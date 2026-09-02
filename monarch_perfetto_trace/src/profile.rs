/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

use crate::Ctx;
use crate::local::Collector;

const DRAIN_DELAY: Duration = Duration::from_millis(500);
const ENDPOINT_TELEMETRY_TARGET: &str = "monarch_hyperactor::telemetry::endpoint";
const QUERY_TIMEOUT: Duration = Duration::from_secs(120);

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
struct QueryEnvelope {
    #[serde(default)]
    rows: Vec<SpanRow>,
    error: Option<String>,
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
    start_ns: u64,
    end_ns: u64,
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

/// Collect distributed user telemetry for `duration` and write a Perfetto trace.
///
/// `output` selects the destination file. When it is absent, the trace is written
/// under `/tmp/$USER/monarch_profiles`. The destination is checked before the
/// collection interval starts and is never overwritten.
pub fn collect_profile(
    telemetry_url: &str,
    duration: Duration,
    output: Option<PathBuf>,
) -> Result<PathBuf> {
    if duration.is_zero() {
        bail!("--time must be greater than zero");
    }

    let output_timestamp_us = timestamp_us()?;
    let output = resolve_output_path(output, output_timestamp_us)?;
    validate_output_path(&output)?;

    eprintln!(
        "Collecting traces for {}...",
        humantime::format_duration(duration)
    );

    let start_us = timestamp_us()?;
    std::thread::sleep(duration);
    let end_us = timestamp_us()?;

    std::thread::sleep(DRAIN_DELAY);

    let rows = query_spans(telemetry_url, start_us, end_us)?;
    let (span_count, actor_count, proc_count, skipped_count) =
        write_trace_to_output(rows, start_us, end_us, &output)?;

    eprintln!(
        "Wrote {} spans from {} actors on {} procs.",
        span_count, actor_count, proc_count
    );

    if skipped_count > 0 {
        eprintln!("Skipped {skipped_count} rows without usable actor span data.");
    }

    Ok(output)
}

fn query_spans(telemetry_url: &str, start_us: i64, end_us: i64) -> Result<Vec<SpanRow>> {
    let client = Client::builder()
        .no_proxy()
        .timeout(QUERY_TIMEOUT)
        .build()
        .context("failed to create telemetry HTTP client")?;

    let response = {
        let sql = profile_sql(start_us, end_us);
        let url = format!("{}/api/query", telemetry_url.trim_end_matches('/'));

        client
            .post(&url)
            .json(&QueryRequest { sql: &sql })
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

    let envelope: QueryEnvelope =
        serde_json::from_str(&body).context("failed to decode telemetry query response")?;

    if let Some(error) = envelope.error {
        bail!("telemetry query failed: {error}");
    }

    Ok(envelope.rows)
}

fn profile_sql(start_us: i64, end_us: i64) -> String {
    format!(
        "WITH profile_spans AS ( \
         SELECT s.process_id, s.id, s.name, s.target, s.fields_json, \
         MIN(CASE WHEN e.event_type = 'enter' THEN e.timestamp_us END) AS start_us, \
         MAX(CASE WHEN e.event_type = 'exit' THEN e.timestamp_us END) AS end_us \
         FROM spans s LEFT JOIN span_events e \
         ON s.process_id = e.process_id AND s.id = e.id \
         WHERE s.timestamp_us < {end_us} \
         GROUP BY s.process_id, s.id, s.name, s.target, s.fields_json \
         ) SELECT process_id, id, name, target, fields_json, start_us, end_us \
         FROM profile_spans WHERE start_us < {end_us} \
         AND COALESCE(end_us, {end_us}) > {start_us}"
    )
}

fn write_trace_to_output(
    rows: Vec<SpanRow>,
    window_start_us: i64,
    window_end_us: i64,
    output: &Path,
) -> Result<(usize, usize, usize, usize)> {
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
) -> Result<(usize, usize, usize, usize)> {
    let row_count = rows.len();
    let mut spans = rows
        .into_iter()
        .filter_map(|row| prepare_span(row, window_start_us, window_end_us))
        .collect::<Vec<_>>();

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

    let mut boundaries = spans
        .iter()
        .enumerate()
        .flat_map(|(span_index, span)| {
            [
                Boundary {
                    timestamp_ns: span.start_ns,
                    span_index,
                    kind: BoundaryKind::Begin,
                },
                Boundary {
                    timestamp_ns: span.end_ns,
                    span_index,
                    kind: BoundaryKind::End,
                },
            ]
        })
        .collect::<Vec<_>>();

    boundaries.sort_unstable();

    for boundary in boundaries {
        let span = &spans[boundary.span_index];
        let track_id = actor_track_ids
            .get(&span.track)
            .copied()
            .expect("each span should have a registered actor track");

        match boundary.kind {
            BoundaryKind::Begin => {
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
                event.consume();
            }
            BoundaryKind::End => ctx.end_slice(track_id, boundary.timestamp_ns).consume(),
        }
    }

    let summary = (
        spans.len(),
        actor_tracks.len(),
        proc_ids.len(),
        row_count - spans.len(),
    );

    let mut collector = ctx.sink();

    collector.flush()?;

    Ok(summary)
}

fn prepare_span(row: SpanRow, window_start_us: i64, window_end_us: i64) -> Option<PreparedSpan> {
    let original_start_us = row.start_us?;
    let original_end_us = row.end_us;

    let start_clipped = original_start_us < window_start_us;
    let end_clipped = original_end_us.is_none_or(|end_us| end_us > window_end_us);

    let start_us = original_start_us.max(window_start_us);
    if start_us >= window_end_us {
        return None;
    }

    let end_us = original_end_us.unwrap_or(window_end_us).min(window_end_us);
    if end_us <= start_us {
        return None;
    }

    let fields = match serde_json::from_str::<Value>(&row.fields_json) {
        Ok(Value::Object(fields)) => fields,
        Ok(_) | Err(_) => return None,
    };
    let actor_id = fields.get("actor_id").and_then(Value::as_str)?;
    let track = parse_actor_track(actor_id)?;

    let start_ns = micros_to_nanos(start_us).ok()?;
    let end_ns = micros_to_nanos(end_us).ok()?;
    let name = display_name(&row, &fields);

    Some(PreparedSpan {
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

fn timestamp_us() -> Result<i64> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_micros();

    i64::try_from(micros).context("system clock exceeds the telemetry timestamp range")
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

fn validate_output_path(output: &Path) -> Result<()> {
    if output.exists() {
        bail!("output already exists: {}", output.display());
    }
    create_output_parent(output)
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
