/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use arrow::array::Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::UInt64Array;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Map;
use serde_json::Value;

use crate::Ctx;
use crate::local::Collector;

const DRAIN_DELAY: Duration = Duration::from_millis(500);
const ENDPOINT_TELEMETRY_TARGET: &str = "monarch_hyperactor::telemetry::endpoint";
const USER_TELEMETRY_TARGET: &str = "monarch_hyperactor::telemetry";
const QUERY_TIMEOUT: Duration = Duration::from_secs(120);
const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
const ARROW_STREAM_EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

struct IncompleteTraceFile<'a> {
    path: &'a Path,
    complete: bool,
}

impl<'a> IncompleteTraceFile<'a> {
    fn new(path: &'a Path) -> Self {
        Self {
            path,
            complete: false,
        }
    }

    fn mark_complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for IncompleteTraceFile<'_> {
    fn drop(&mut self) {
        if !self.complete {
            let _ = fs::remove_file(self.path);
        }
    }
}

#[derive(Debug, Deserialize)]
struct QueryEnvelope<T> {
    rows: Option<Vec<T>>,
    error: Option<String>,
}

struct EosTrackingReader<R> {
    inner: R,
    tail: [u8; ARROW_STREAM_EOS.len()],
    tail_len: usize,
}

impl<R> EosTrackingReader<R> {
    /// Wrap a reader and retain enough trailing bytes to validate stream completion.
    fn new(inner: R) -> Self {
        Self {
            inner,
            tail: [0; ARROW_STREAM_EOS.len()],
            tail_len: 0,
        }
    }

    /// Return whether the bytes consumed from the reader end with Arrow's EOS marker.
    fn ended_with_eos(&self) -> bool {
        self.tail_len == self.tail.len() && self.tail == ARROW_STREAM_EOS
    }
}

impl<R: Read> Read for EosTrackingReader<R> {
    /// Read from the wrapped stream while retaining its final EOS-sized suffix.
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(buffer)?;
        let bytes = &buffer[..count];
        let tail_len = self.tail.len();

        if bytes.len() >= tail_len {
            self.tail.copy_from_slice(&bytes[bytes.len() - tail_len..]);
            self.tail_len = tail_len;
        } else if !bytes.is_empty() {
            let retained = self.tail_len.min(tail_len - bytes.len());
            self.tail
                .copy_within(self.tail_len - retained..self.tail_len, 0);
            self.tail[retained..retained + bytes.len()].copy_from_slice(bytes);
            self.tail_len = retained + bytes.len();
        }

        Ok(count)
    }
}

#[derive(Debug, Serialize)]
struct QueryRequest<'a> {
    sql: &'a str,
}

#[derive(Debug, Deserialize)]
struct SpanRow {
    process_id: String,
    id: u64,
    name: String,
    target: String,
    fields_json: String,
    start_us: Option<i64>,
    end_us: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SpanMetadataRow {
    process_id: String,
    id: u64,
    name: String,
    target: String,
    fields_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SpanKey {
    process_id: String,
    id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ActorTrack {
    proc_id: String,
    actor_id: String,
}

#[derive(Debug)]
struct PreparedSpan {
    process_id: String,
    id: u64,
    name: String,
    target: String,
    fields: Map<String, Value>,
    track: ActorTrack,
    original_start_us: i64,
    original_end_us: Option<i64>,
    start_clipped: bool,
    end_clipped: bool,
    returns_response: bool,
    start_ns: u64,
    end_ns: Option<u64>,
}

#[derive(Default)]
struct SpanFlowEdits {
    send_flow_ids: Vec<u64>,
    request_terminating_flow_ids: Vec<u64>,
    response_flow_ids: Vec<u64>,
    complete_terminating_flow_ids: Vec<u64>,
}

#[derive(Default)]
struct CorrelatedSpans {
    callers: Vec<usize>,
    receivers: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SpanSkipReason {
    MissingStartTimestamp,
    StartsOutsideProfileWindow,
    NonPositiveDuration,
    InvalidFieldsJson,
    FieldsJsonNotObject,
    MissingActorId,
    ActorIdNotString,
    ActorIdMissingProcessId,
    InvalidStartTimestamp,
    InvalidEndTimestamp,
}

impl SpanSkipReason {
    fn description(self) -> &'static str {
        match self {
            Self::MissingStartTimestamp => "missing a span enter event",
            Self::StartsOutsideProfileWindow => "starting outside the profile window",
            Self::NonPositiveDuration => "without a positive duration",
            Self::InvalidFieldsJson => "with invalid fields_json",
            Self::FieldsJsonNotObject => "whose fields_json is not an object",
            Self::MissingActorId => "missing actor_id",
            Self::ActorIdNotString => "whose actor_id is not a string",
            Self::ActorIdMissingProcessId => "whose actor_id has no process ID",
            Self::InvalidStartTimestamp => "with an invalid start timestamp",
            Self::InvalidEndTimestamp => "with an invalid end timestamp",
        }
    }
}

#[derive(Debug)]
struct TraceSummary {
    span_count: usize,
    actor_count: usize,
    proc_count: usize,
    skipped_counts: BTreeMap<SpanSkipReason, usize>,
}

impl TraceSummary {
    fn skipped_count(&self) -> usize {
        self.skipped_counts.values().sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundaryKind {
    Begin,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Boundary {
    timestamp_ns: u64,
    span_index: usize,
    kind: BoundaryKind,
}

impl Ord for Boundary {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp_ns
            .cmp(&other.timestamp_ns)
            .then_with(|| match (self.kind, other.kind) {
                (BoundaryKind::Begin, BoundaryKind::End) => Ordering::Greater,
                (BoundaryKind::End, BoundaryKind::Begin) => Ordering::Less,
                (BoundaryKind::Begin, BoundaryKind::Begin) => {
                    self.span_index.cmp(&other.span_index)
                }
                (BoundaryKind::End, BoundaryKind::End) => other.span_index.cmp(&self.span_index),
            })
    }
}

impl PartialOrd for Boundary {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Export a completed telemetry interval to a Perfetto trace.
///
/// `output` selects the destination file. When it is absent, the trace is written
/// under `/tmp/$USER/monarch_profiles`. The destination is never overwritten.
pub fn export_profile(
    telemetry_url: &str,
    start_us: i64,
    end_us: i64,
    output: Option<PathBuf>,
) -> Result<PathBuf> {
    if start_us < 0 || end_us <= start_us {
        bail!("profile interval must satisfy 0 <= start_us < end_us");
    }

    let output = resolve_output_path(output, start_us)?;
    if output.exists() {
        bail!("output already exists: {}", output.display());
    }
    export_profile_to_path(telemetry_url, start_us, end_us, output)
}

fn export_profile_to_path(
    telemetry_url: &str,
    start_us: i64,
    end_us: i64,
    output: PathBuf,
) -> Result<PathBuf> {
    std::thread::sleep(DRAIN_DELAY);
    // Each span falls into 1 of 4 cases:
    //
    //         |-- profile window --|
    // 1.             |----|
    // 2.             |--------------
    // 3. |-----------------|
    // 4. |--------------------------
    //
    // 1. Starts inside, ends inside
    // 2. Starts inside, ends after
    // 3. Starts before, ends inside
    // 4. Starts before, ends after
    //
    // `windows_spans` contains cases 1-3 because it is able to match on at least one boundary in the window
    let window_spans = query_rows(telemetry_url, &profile_sql(start_us, end_us))?;
    // `spans_active_at_start` is intended to contain case 4 but also case 3
    let spans_active_at_start = query_spans_active_at_start(telemetry_url, start_us)?;

    // `stitch_spans` deduplicates starts from case 3 that are matched by both `window_spans`
    // and `spans_active_at_start`
    let rows = stitch_spans(window_spans, spans_active_at_start);

    let summary = write_trace_to_output(rows, start_us, end_us, &output)?;

    eprintln!(
        "Wrote {} spans from {} actors on {} procs.",
        summary.span_count, summary.actor_count, summary.proc_count
    );

    let skipped_count = summary.skipped_count();
    if skipped_count > 0 {
        eprintln!("Skipped {skipped_count} rows:");
        for (reason, count) in summary.skipped_counts {
            eprintln!("  {count} {}", reason.description());
        }
    }

    Ok(output)
}

/// Return a telemetry client with the requested I/O timeout.
/// Connections always time out after [`QUERY_TIMEOUT`].
fn telemetry_client(timeout: Option<Duration>) -> Result<Client> {
    Client::builder()
        .no_proxy()
        .connect_timeout(QUERY_TIMEOUT)
        .timeout(timeout)
        .build()
        .context("failed to create telemetry HTTP client")
}

/// Return spans active immediately before `start_us`.
/// Each returned row has its original start and no end.
fn query_spans_active_at_start(telemetry_url: &str, start_us: i64) -> Result<Vec<SpanRow>> {
    if !supports_streaming_queries(telemetry_url)? {
        eprintln!(
            "Telemetry server does not support streaming queries; omitting spans active at the start of the profile window."
        );
        return Ok(Vec::new());
    }

    let spans_active_at_start = span_ids_active_at_start(telemetry_url, start_us)?;

    if spans_active_at_start.is_empty() {
        return Ok(Vec::new());
    }

    let sql = spans_by_id_sql(spans_active_at_start.keys().cloned());

    query_rows::<SpanMetadataRow>(telemetry_url, &sql)?
        .into_iter()
        .map(|span| {
            let key = SpanKey {
                process_id: span.process_id.clone(),
                id: span.id,
            };

            let start_us = spans_active_at_start
                .get(&key)
                .copied()
                .context("span metadata query returned an unexpected span")?;

            Ok(SpanRow {
                process_id: span.process_id,
                id: span.id,
                name: span.name,
                target: span.target,
                fields_json: span.fields_json,
                start_us: Some(start_us),
                end_us: None,
            })
        })
        .collect()
}

/// Return whether the telemetry server supports streamed Arrow query results.
fn supports_streaming_queries(telemetry_url: &str) -> Result<bool> {
    let client = telemetry_client(Some(QUERY_TIMEOUT))?;
    let url = format!("{}/api/query", telemetry_url.trim_end_matches('/'));

    let response = client
        .post(&url)
        .header(ACCEPT, ARROW_STREAM_CONTENT_TYPE)
        .json(&QueryRequest { sql: "SELECT 1" })
        .send()
        .with_context(|| format!("failed to query {url}"))?;

    let status = response.status();
    if status == StatusCode::NOT_ACCEPTABLE || status == StatusCode::NOT_IMPLEMENTED {
        return Ok(false);
    }
    if !status.is_success() {
        let body = response.text().with_context(|| {
            format!("failed to read telemetry query response with status {status}")
        })?;
        bail!("telemetry query failed with {status}: {body}");
    }

    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    let supports_streaming = content_type.starts_with(ARROW_STREAM_CONTENT_TYPE);
    if supports_streaming {
        response
            .bytes()
            .context("failed to finish telemetry streaming capability query")?;
    }

    Ok(supports_streaming)
}

/// Return retained spans active immediately before `start_us`, keyed by span ID.
/// Each value is the span's original enter timestamp.
fn span_ids_active_at_start(telemetry_url: &str, start_us: i64) -> Result<BTreeMap<SpanKey, i64>> {
    let client = telemetry_client(Some(QUERY_TIMEOUT))?;

    let sql = format!(
        r#"
            SELECT process_id, id, timestamp_us, event_type
            FROM span_events
            WHERE timestamp_us < {start_us}
                AND event_type IN ('enter', 'exit')
        "#
    );

    let url = format!("{}/api/query", telemetry_url.trim_end_matches('/'));

    let response = client
        .post(&url)
        .header(ACCEPT, ARROW_STREAM_CONTENT_TYPE)
        .json(&QueryRequest { sql: &sql })
        .send()
        .with_context(|| format!("failed to query {url}"))?;

    let status = response.status();

    if !status.is_success() {
        let body = response.text().with_context(|| {
            format!("failed to read telemetry query response with status {status}")
        })?;
        bail!("telemetry query failed with {status}: {body}");
    }

    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if !content_type.starts_with(ARROW_STREAM_CONTENT_TYPE) {
        bail!(
            "telemetry query returned {content_type:?} instead of an Arrow stream; restart the job with a streaming telemetry server"
        );
    }

    let mut unmatched_enters = HashMap::new();

    let mut unmatched_exits = HashSet::new();

    let mut response = EosTrackingReader::new(response);

    {
        let reader = StreamReader::try_new(&mut response, None)
            .context("failed to decode telemetry Arrow stream")?;

        for batch in reader {
            replay_span_event_batch(batch?, &mut unmatched_enters, &mut unmatched_exits)?;
        }
    }

    if !response.ended_with_eos() {
        bail!("telemetry Arrow stream ended without a completion marker");
    }

    Ok(unmatched_enters.into_iter().collect())
}

/// Update unmatched span entries and exits from one Arrow batch.
/// Rows may arrive in any order and must contain non-null lifecycle columns.
fn replay_span_event_batch(
    batch: RecordBatch,
    unmatched_enters: &mut HashMap<SpanKey, i64>,
    unmatched_exits: &mut HashSet<SpanKey>,
) -> Result<()> {
    let process_ids = typed_column::<StringArray>(&batch, "process_id")?;

    let ids = typed_column::<UInt64Array>(&batch, "id")?;

    let timestamps = typed_column::<Int64Array>(&batch, "timestamp_us")?;

    let event_types = typed_column::<StringArray>(&batch, "event_type")?;

    for index in 0..batch.num_rows() {
        if process_ids.is_null(index)
            || ids.is_null(index)
            || timestamps.is_null(index)
            || event_types.is_null(index)
        {
            bail!("span event row contains null values");
        }

        let key = SpanKey {
            process_id: process_ids.value(index).to_owned(),
            id: ids.value(index),
        };

        match event_types.value(index) {
            "enter" => {
                if !unmatched_exits.remove(&key) {
                    unmatched_enters
                        .entry(key)
                        .and_modify(|timestamp_us| {
                            *timestamp_us = (*timestamp_us).min(timestamps.value(index));
                        })
                        .or_insert_with(|| timestamps.value(index));
                }
            }
            "exit" => {
                if unmatched_enters.remove(&key).is_none() {
                    unmatched_exits.insert(key);
                }
            }
            event_type => bail!("unexpected span event type {event_type:?}"),
        }
    }

    Ok(())
}

/// Return a named Arrow column as `T`, with context for missing or mismatched columns.
fn typed_column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .with_context(|| format!("telemetry Arrow stream is missing column {name:?}"))?
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("telemetry Arrow column {name:?} has an unexpected type"))
}

/// Execute SQL and deserialize the JSON response rows as `T`.
fn query_rows<T: DeserializeOwned>(telemetry_url: &str, sql: &str) -> Result<Vec<T>> {
    let client = telemetry_client(Some(QUERY_TIMEOUT))?;

    let response = {
        let url = format!("{}/api/query", telemetry_url.trim_end_matches('/'));

        client
            .post(&url)
            .json(&QueryRequest { sql })
            .send()
            .with_context(|| format!("failed to query {url}"))
    }?;

    let status = response.status();

    let body = response
        .text()
        .with_context(|| format!("failed to read telemetry query response with status {status}"))?;

    if !status.is_success() {
        bail!("telemetry query failed with {status}: {body}");
    }

    let envelope: QueryEnvelope<T> =
        serde_json::from_str(&body).context("failed to decode telemetry query response")?;

    if let Some(error) = envelope.error {
        bail!("telemetry query failed: {error}");
    }

    Ok(envelope.rows.unwrap_or_default())
}

fn profile_sql(start_us: i64, end_us: i64) -> String {
    format!(
        r#"
WITH window_events AS (
    SELECT process_id, id, timestamp_us, event_type
    FROM span_events
    WHERE timestamp_us >= {start_us}
      AND timestamp_us < {end_us}
      AND event_type IN ('enter', 'exit')
),
profile_events AS (
    SELECT
        process_id,
        id,
        MIN(CASE WHEN event_type = 'enter' THEN timestamp_us END) AS start_us,
        MAX(CASE WHEN event_type = 'exit' THEN timestamp_us END) AS end_us
    FROM window_events
    GROUP BY process_id, id
)
SELECT
    s.process_id,
    s.id,
    s.name,
    s.target,
    s.fields_json,
    e.start_us,
    e.end_us
FROM profile_events e
JOIN spans s ON s.process_id = e.process_id AND s.id = e.id"#
    )
}

/// Build a query for the metadata of a nonempty collection of span IDs.
fn spans_by_id_sql(span_ids: impl IntoIterator<Item = SpanKey>) -> String {
    let span_ids_sql = span_ids
        .into_iter()
        .map(|key| {
            format!(
                "({}, CAST({} AS BIGINT UNSIGNED))",
                sql_string_literal(&key.process_id),
                key.id,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n        ");

    format!(
        r#"
        WITH span_ids (process_id, id) AS (
            VALUES
                {span_ids_sql}
        )
        SELECT
            s.process_id,
            s.id,
            s.name,
            s.target,
            s.fields_json
        FROM span_ids i
        JOIN spans s ON s.process_id = i.process_id AND s.id = i.id
    "#
    )
}

/// Return one row for every span intersecting the profile window.
/// For a span active at the window start that exits inside it, the returned row
/// combines the earlier start with the in-window end.
fn stitch_spans(
    mut window_spans: Vec<SpanRow>,
    spans_active_at_start: Vec<SpanRow>,
) -> Vec<SpanRow> {
    let mut spans_active_at_start = spans_active_at_start
        .into_iter()
        .map(|span| {
            (
                SpanKey {
                    process_id: span.process_id.clone(),
                    id: span.id,
                },
                span,
            )
        })
        .collect::<HashMap<_, _>>();

    for span in &mut window_spans {
        let key = SpanKey {
            process_id: span.process_id.clone(),
            id: span.id,
        };

        if let Some(active_span) = spans_active_at_start.remove(&key)
            && span.start_us.is_none()
        {
            span.start_us = active_span.start_us;
        }
    }

    window_spans.extend(spans_active_at_start.into_values());
    window_spans
}

/// Quote a string as a SQL literal, escaping embedded single quotes.
fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn write_trace_to_output(
    rows: Vec<SpanRow>,
    window_start_us: i64,
    window_end_us: i64,
    output: &Path,
) -> Result<TraceSummary> {
    let output_file = create_output_file(output)?;
    let mut incomplete_output = IncompleteTraceFile::new(output);
    let summary = write_trace(rows, window_start_us, window_end_us, output_file)?;
    incomplete_output.mark_complete();
    Ok(summary)
}

fn write_trace(
    rows: Vec<SpanRow>,
    window_start_us: i64,
    window_end_us: i64,
    output: fs::File,
) -> Result<TraceSummary> {
    let row_count = rows.len();
    let (mut spans, skipped_counts) = rows
        .into_iter()
        .map(|row| prepare_span(row, window_start_us, window_end_us))
        .fold(
            (Vec::with_capacity(row_count), BTreeMap::new()),
            |(mut spans, mut skipped_counts), result| {
                match result {
                    Ok(span) => spans.push(span),
                    Err(reason) => *skipped_counts.entry(reason).or_default() += 1,
                }
                (spans, skipped_counts)
            },
        );

    spans.sort_by(|left, right| {
        left.track
            .cmp(&right.track)
            .then_with(|| left.original_start_us.cmp(&right.original_start_us))
            .then_with(|| {
                right
                    .original_end_us
                    .unwrap_or(i64::MAX)
                    .cmp(&left.original_end_us.unwrap_or(i64::MAX))
            })
            .then_with(|| left.id.cmp(&right.id))
    });

    let proc_ids = spans
        .iter()
        .map(|span| span.track.proc_id.clone())
        .collect::<BTreeSet<_>>();

    let actor_tracks = spans
        .iter()
        .map(|span| span.track.clone())
        .collect::<BTreeSet<_>>();

    let process_count = i32::try_from(proc_ids.len()).context("too many process tracks")?;

    let collector = Collector::from_file(output);
    let mut ctx = Ctx::new(collector);

    let mut process_track_ids = HashMap::new();
    for (pid, proc_id) in (1..=process_count).zip(&proc_ids) {
        let track_id = ctx.new_process_with_name(pid, proc_id.clone());
        process_track_ids.insert(proc_id.clone(), track_id);
    }

    let mut actor_track_ids = HashMap::new();
    for actor_track in &actor_tracks {
        let process_track = process_track_ids
            .get(&actor_track.proc_id)
            .copied()
            .expect("each actor should have a registered process track");

        let track_id = ctx.next_uuid();

        ctx.new_track(track_id)
            .name(&actor_track.actor_id)
            .parent(process_track)
            .consume();

        actor_track_ids.insert(actor_track.clone(), track_id);
    }

    let flow_plan = build_flow_plan(&spans, || ctx.next_uuid());

    for boundary in span_boundaries(&spans) {
        let span = &spans[boundary.span_index];
        let track_id = actor_track_ids
            .get(&span.track)
            .copied()
            .expect("each span should have a registered actor track");

        match boundary.kind {
            BoundaryKind::Begin => {
                let flow_edits = flow_plan.get(&boundary.span_index);
                let mut event = ctx
                    .start_slice(track_id, boundary.timestamp_ns)
                    .name(&span.name);

                event = event.add_annotation("process_id", &Value::from(span.process_id.clone()));
                event = event.add_annotation("span_id", &Value::from(span.id.to_string()));
                event = event.add_annotation("target", &Value::from(span.target.clone()));

                if span.start_clipped {
                    event = event.add_annotation("profile.start_clipped", &Value::Bool(true));
                    event = event.add_annotation(
                        "profile.original_start_us",
                        &Value::from(span.original_start_us),
                    );
                }

                if span.end_clipped {
                    event = event.add_annotation("profile.end_clipped", &Value::Bool(true));
                    if let Some(original_end_us) = span.original_end_us {
                        event = event.add_annotation(
                            "profile.original_end_us",
                            &Value::from(original_end_us),
                        );
                    } else {
                        event =
                            event.add_annotation("profile.end_event_missing", &Value::Bool(true));
                    }
                }
                for (name, value) in &span.fields {
                    event = event.add_annotation(name, value);
                }
                if let Some(flow_edits) = flow_edits {
                    event =
                        event.with_terminating_flow_ids(&flow_edits.request_terminating_flow_ids);
                }
                event.consume();

                if let Some(flow_edits) = flow_edits
                    && !flow_edits.send_flow_ids.is_empty()
                {
                    ctx.instant(track_id, boundary.timestamp_ns)
                        .name("send")
                        .add_annotation(
                            "correlation_id",
                            span.fields
                                .get("correlation_id")
                                .expect("a caller with flows should have a correlation id"),
                        )
                        .with_flow_ids(&flow_edits.send_flow_ids)
                        .consume();
                }
            }
            BoundaryKind::End => {
                let flow_edits = flow_plan.get(&boundary.span_index);
                if let Some(flow_edits) = flow_edits
                    && !flow_edits.complete_terminating_flow_ids.is_empty()
                {
                    ctx.instant(track_id, boundary.timestamp_ns)
                        .name("complete")
                        .add_annotation(
                            "correlation_id",
                            span.fields
                                .get("correlation_id")
                                .expect("a caller with flows should have a correlation id"),
                        )
                        .with_terminating_flow_ids(&flow_edits.complete_terminating_flow_ids)
                        .consume();
                }

                let mut event = ctx.end_slice(track_id, boundary.timestamp_ns);
                if let Some(flow_edits) = flow_edits {
                    event = event.with_flow_ids(&flow_edits.response_flow_ids);
                }
                event.consume();
            }
        }
    }

    let summary = TraceSummary {
        span_count: spans.len(),
        actor_count: actor_tracks.len(),
        proc_count: proc_ids.len(),
        skipped_counts,
    };

    let mut collector = ctx.sink();

    collector.flush()?;

    Ok(summary)
}

fn span_boundaries(spans: &[PreparedSpan]) -> Vec<Boundary> {
    let mut boundaries = spans
        .iter()
        .enumerate()
        .flat_map(|(span_index, span)| {
            std::iter::once(Boundary {
                timestamp_ns: span.start_ns,
                span_index,
                kind: BoundaryKind::Begin,
            })
            .chain(span.end_ns.map(|timestamp_ns| Boundary {
                timestamp_ns,
                span_index,
                kind: BoundaryKind::End,
            }))
        })
        .collect::<Vec<_>>();

    boundaries.sort_unstable();
    boundaries
}

fn build_flow_plan(
    spans: &[PreparedSpan],
    mut next_flow_id: impl FnMut() -> u64,
) -> HashMap<usize, SpanFlowEdits> {
    let mut correlated_spans: HashMap<u64, CorrelatedSpans> = HashMap::new();

    for (span_index, span) in spans.iter().enumerate() {
        let Some(correlation_id) = span.fields.get("correlation_id").and_then(Value::as_u64) else {
            continue;
        };

        let group = correlated_spans.entry(correlation_id).or_default();
        if span.target == ENDPOINT_TELEMETRY_TARGET {
            group.callers.push(span_index);
        } else if span.target == USER_TELEMETRY_TARGET {
            group.receivers.push(span_index);
        }
    }

    let mut flow_plan: HashMap<usize, SpanFlowEdits> = HashMap::new();
    for group in correlated_spans.into_values() {
        let [caller_index] = group.callers.as_slice() else {
            continue;
        };
        let caller = &spans[*caller_index];
        for receiver_index in group.receivers {
            let receiver = &spans[receiver_index];

            if !caller.start_clipped && !receiver.start_clipped {
                let flow_id = next_flow_id();
                flow_plan
                    .entry(*caller_index)
                    .or_default()
                    .send_flow_ids
                    .push(flow_id);
                flow_plan
                    .entry(receiver_index)
                    .or_default()
                    .request_terminating_flow_ids
                    .push(flow_id);
            }

            if caller.returns_response && !caller.end_clipped && !receiver.end_clipped {
                let flow_id = next_flow_id();
                flow_plan
                    .entry(receiver_index)
                    .or_default()
                    .response_flow_ids
                    .push(flow_id);
                flow_plan
                    .entry(*caller_index)
                    .or_default()
                    .complete_terminating_flow_ids
                    .push(flow_id);
            }
        }
    }

    flow_plan
}

fn prepare_span(
    row: SpanRow,
    window_start_us: i64,
    window_end_us: i64,
) -> Result<PreparedSpan, SpanSkipReason> {
    let original_start_us = row.start_us.ok_or(SpanSkipReason::MissingStartTimestamp)?;
    let original_end_us = row.end_us;

    let start_clipped = original_start_us < window_start_us;
    let end_clipped = original_end_us.is_none_or(|end_us| end_us > window_end_us);

    let start_us = original_start_us.max(window_start_us);
    if start_us >= window_end_us {
        return Err(SpanSkipReason::StartsOutsideProfileWindow);
    }

    let end_us = original_end_us.map(|end_us| end_us.min(window_end_us));
    if end_us.is_some_and(|end_us| end_us <= start_us) {
        return Err(SpanSkipReason::NonPositiveDuration);
    }

    let fields = match serde_json::from_str::<Value>(&row.fields_json) {
        Ok(Value::Object(fields)) => fields,
        Ok(_) => return Err(SpanSkipReason::FieldsJsonNotObject),
        Err(_) => return Err(SpanSkipReason::InvalidFieldsJson),
    };
    let actor_id = match fields.get("actor_id") {
        Some(Value::String(actor_id)) => actor_id,
        Some(_) => return Err(SpanSkipReason::ActorIdNotString),
        None => return Err(SpanSkipReason::MissingActorId),
    };
    let track = parse_actor_track(actor_id).ok_or(SpanSkipReason::ActorIdMissingProcessId)?;

    let start_ns = micros_to_nanos(start_us).map_err(|_| SpanSkipReason::InvalidStartTimestamp)?;
    let end_ns = end_us
        .map(micros_to_nanos)
        .transpose()
        .map_err(|_| SpanSkipReason::InvalidEndTimestamp)?;
    let returns_response = row.target == ENDPOINT_TELEMETRY_TARGET
        && matches!(row.name.as_str(), "call" | "call_one" | "choose");
    let name = display_name(&row, &fields);

    Ok(PreparedSpan {
        process_id: row.process_id,
        id: row.id,
        name,
        target: row.target,
        fields,
        track,
        original_start_us,
        original_end_us,
        start_clipped,
        end_clipped,
        returns_response,
        start_ns,
        end_ns,
    })
}

fn display_name(row: &SpanRow, fields: &Map<String, Value>) -> String {
    if row.target == ENDPOINT_TELEMETRY_TARGET {
        let mesh = fields.get("mesh").and_then(Value::as_str);
        let method = fields.get("method").and_then(Value::as_str);
        let call_name = fields.get("call_name").and_then(Value::as_str);

        return match (mesh, method, call_name) {
            (Some(mesh), Some(method), _) => format!("{mesh}.{method}.{}()", row.name),
            (_, _, Some(call_name)) if !call_name.is_empty() => {
                format!("{call_name}.{}()", row.name)
            }
            _ => row.name.clone(),
        };
    }

    fields
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or(&row.name)
        .to_string()
}

fn parse_actor_track(actor_addr: &str) -> Option<ActorTrack> {
    let actor_addr = actor_addr
        .rsplit_once(',')
        .map(|(_, suffix)| suffix)
        .unwrap_or(actor_addr)
        .trim();

    let actor_id = actor_addr
        .split_once('@')
        .map(|(id, _)| id)
        .unwrap_or(actor_addr);
    // ActorId displays as `<actor_uid>.<proc_id>`, so the first component is
    // the actor and the complete remainder is the process ID.
    let (_, proc_id) = actor_id.split_once('.')?;

    Some(ActorTrack {
        proc_id: proc_id.to_string(),
        actor_id: actor_id.to_string(),
    })
}

fn micros_to_nanos(timestamp_us: i64) -> Result<u64> {
    u64::try_from(timestamp_us)
        .context("trace timestamp is before the Unix epoch")?
        .checked_mul(1_000)
        .context("trace timestamp exceeds the Perfetto range")
}

fn resolve_output_path(output: Option<PathBuf>, filename_timestamp_us: i64) -> Result<PathBuf> {
    let output = match output {
        Some(output) => output,
        None => {
            let user = std::env::var("USER")
                .ok()
                .filter(|user| !user.is_empty())
                .context("USER is not set; pass an explicit output path")?;

            PathBuf::from("/tmp")
                .join(user)
                .join("monarch_profiles")
                .join(format!("monarch-profile-{filename_timestamp_us}.pftrace"))
        }
    };

    if output.is_absolute() {
        return Ok(output);
    }

    Ok(std::env::current_dir()
        .context("failed to get the current directory")?
        .join(output))
}

fn create_output_file(output: &Path) -> Result<fs::File> {
    create_output_parent(output)?;

    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
    {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            bail!("output already exists: {}", output.display())
        }
        Err(error) => Err(error).with_context(|| format!("failed to create {}", output.display())),
    }
}

fn create_output_parent(output: &Path) -> Result<()> {
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span_row(end_us: Option<i64>) -> SpanRow {
        SpanRow {
            process_id: "proc".to_owned(),
            id: 1,
            name: "span".to_owned(),
            target: "target".to_owned(),
            fields_json: r#"{"actor_id":"actor.proc"}"#.to_owned(),
            start_us: Some(12),
            end_us,
        }
    }

    #[test]
    fn missing_span_end_remains_open() {
        let span = prepare_span(span_row(None), 10, 20)
            .expect("span without an end should remain renderable");

        assert_eq!(span.start_ns, 12_000);
        assert_eq!(span.end_ns, None);
        assert!(!span.start_clipped);
        assert!(span.end_clipped);
    }

    #[test]
    fn span_without_end_emits_only_begin_boundary() {
        let span = prepare_span(span_row(None), 10, 20)
            .expect("span without an end should remain renderable");

        assert_eq!(
            span_boundaries(&[span]),
            vec![Boundary {
                timestamp_ns: 12_000,
                span_index: 0,
                kind: BoundaryKind::Begin,
            }]
        );
    }
}
