/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Local file-based trace reading.
//!
//! Reads `.pftrace` files from the local filesystem.
//!
//! ## Directory Layout
//!
//! ```text
//! {trace_dir}/
//! ├── executions/
//! │   ├── {execution_id}/
//! │   │   ├── {process_name}.pftrace
//! │   │   └── ...
//! │   └── latest -> {execution_id}/   # symlink to most recent
//! ```

use std::collections::HashMap;
use std::fs;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use prost::Message;
use tracing::info;
use tracing_perfetto_sdk_schema::Trace;
use tracing_perfetto_sdk_schema::TracePacket;
use tracing_perfetto_sdk_schema::trace_packet::Data;
use tracing_perfetto_sdk_schema::trace_packet::OptionalTrustedPacketSequenceId;
use tracing_perfetto_sdk_schema::track_event::Type as TrackEventType;

use crate::Sink;

/// Subdirectory name where traces are stored within a trace root.
const MONARCH_TRACES_DIR: &str = "monarch_traces";

/// Returns the default trace root directory: `/tmp/{username}/`.
pub fn default_trace_root() -> PathBuf {
    let username = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    PathBuf::from(format!("/tmp/{}", username))
}

/// Returns the trace directory for a given root: `{root}/monarch_traces/`.
pub fn trace_dir(root: &Path) -> PathBuf {
    root.join(MONARCH_TRACES_DIR)
}

/// Returns the path to the execution directory.
///
/// If `execution_id` is `None`, follows the `latest` symlink.
pub fn execution_dir(trace_dir: &Path, execution_id: Option<&str>) -> Result<PathBuf> {
    let executions_dir = trace_dir.join("executions");

    match execution_id {
        Some(id) => {
            let dir = executions_dir.join(id);
            if !dir.exists() {
                bail!(
                    "Execution '{}' not found in {}",
                    id,
                    executions_dir.display()
                );
            }
            Ok(dir)
        }
        None => {
            // Follow the `latest` symlink
            let latest_link = executions_dir.join("latest");
            if !latest_link.exists() {
                bail!(
                    "No 'latest' symlink found in {}. Specify --execution-id or run a traced execution first.",
                    executions_dir.display()
                );
            }
            let target = fs::read_link(&latest_link).with_context(|| {
                format!(
                    "Failed to read 'latest' symlink at {}",
                    latest_link.display()
                )
            })?;

            // The symlink is relative to the executions directory
            let dir = executions_dir.join(&target);
            if !dir.exists() {
                bail!(
                    "'latest' symlink points to '{}' which does not exist",
                    target.display()
                );
            }
            Ok(dir)
        }
    }
}

/// Returns the execution ID from the `latest` symlink.
pub fn get_latest_execution_id(trace_dir: &Path) -> Result<Option<String>> {
    let latest_link = trace_dir.join("executions").join("latest");
    if !latest_link.exists() {
        return Ok(None);
    }
    Ok(fs::read_link(&latest_link)?.to_str().map(|s| s.to_string()))
}

/// Lists all available execution IDs in the trace directory.
pub fn list_executions(trace_dir: &Path) -> Result<Vec<String>> {
    let executions_dir = trace_dir.join("executions");
    if !executions_dir.exists() {
        return Ok(Vec::new());
    }

    let mut executions = Vec::new();
    for entry in fs::read_dir(&executions_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        // Skip the `latest` symlink
        if name == "latest" {
            continue;
        }
        if entry.file_type()?.is_dir() {
            executions.push(name);
        }
    }

    // Sort by modification time (newest first)
    executions.sort_by(|a, b| {
        let a_time = fs::metadata(executions_dir.join(a))
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

        let b_time = fs::metadata(executions_dir.join(b))
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

        b_time.cmp(&a_time)
    });

    Ok(executions)
}

/// Finds all `.pftrace` files in an execution directory.
fn find_pftrace_files(execution_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for entry in fs::read_dir(execution_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "pftrace") {
            files.push(path);
        }
    }

    files.sort();

    if files.is_empty() {
        bail!("No .pftrace files found in {}", execution_dir.display());
    }

    tracing::info!("Found {} .pftrace files", files.len());
    Ok(files)
}

/// Reads and merges trace files from an execution directory.
///
/// Track UUIDs and packet sequence IDs are rewritten to avoid collisions
/// between processes in the merged trace.
pub fn merge_traces_from_dir<S: Sink>(execution_dir: &Path, sink: &mut S) -> Result<()> {
    let files = find_pftrace_files(execution_dir)?;
    merge_trace_files(&files, sink)
}

fn merge_trace_files<S: Sink>(files: &[PathBuf], sink: &mut S) -> Result<()> {
    for (file_idx, path) in files.iter().enumerate() {
        // We have to offset UUIDs here since within a single process,
        // we generate these by just incrementing a counter.
        let uuid_offset = (file_idx as u64 + 1) * 1_000_000;
        let sequence_id = u32::try_from(file_idx + 1)
            .context("too many trace files to assign unique packet sequence ids")?;

        read_and_rewrite_trace_file(path, uuid_offset, sequence_id, sink)?;
    }

    Ok(())
}

/// Reads a single .pftrace file and sends packets to the sink with rewritten IDs.
///
/// The file may contain multiple concatenated `Trace` messages (protobuf containers).
fn read_and_rewrite_trace_file<S: Sink>(
    path: &Path,
    uuid_offset: u64,
    sequence_id: u32,
    sink: &mut S,
) -> Result<()> {
    let mut file = fs::File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let mut offset = 0;
    while offset < buffer.len() {
        match Trace::decode(&buffer[offset..]) {
            Ok(trace) => {
                let encoded_len = trace.encoded_len();

                for mut packet in trace.packet {
                    rewrite_packet_ids(&mut packet, uuid_offset, sequence_id);
                    sink.consume(packet);
                }

                offset += encoded_len;
            }
            Err(e) => {
                // If we can't decode, try skipping a byte and retrying
                // This handles potential alignment issues or partial writes
                tracing::debug!("Decode error at offset {}: {}", offset, e);
                offset += 1;
            }
        }
    }

    Ok(())
}

fn rewrite_packet_ids(packet: &mut TracePacket, uuid_offset: u64, sequence_id: u32) {
    if packet.optional_trusted_packet_sequence_id.is_some() {
        packet.optional_trusted_packet_sequence_id = Some(
            OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(sequence_id),
        );
    }

    if let Some(ref mut data) = packet.data {
        match data {
            Data::TrackDescriptor(td) => {
                if let Some(ref mut uuid) = td.uuid {
                    *uuid += uuid_offset;
                }
                if let Some(ref mut parent) = td.parent_uuid {
                    *parent += uuid_offset;
                }
            }
            Data::TrackEvent(te) => {
                if let Some(ref mut uuid) = te.track_uuid {
                    *uuid += uuid_offset;
                }
            }
            _ => {}
        }
    }
}

/// A [`Sink`] that writes trace packets to a file, flushing every 100 packets.
pub struct Collector {
    trace: Trace,
    path: String,
}

impl Collector {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_string_lossy().to_string(),
            trace: Trace::default(),
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        let tr = std::mem::take(&mut self.trace);
        let mut file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;

        let bytes = prost::Message::encode_to_vec(&tr);
        file.write_all(&bytes)?;
        file.flush()?;
        Ok(())
    }
}

impl Sink for Collector {
    fn consume(&mut self, packet: TracePacket) {
        self.trace.packet.push(packet);
        if self.trace.packet.len() > 100 {
            self.flush().expect("failed to flush trace packets");
        }
    }
}

/// Resolves trace/execution directories, merges all per-process `.pftrace`
/// files, and writes the unified trace to `output`.
///
/// - `trace_root`: root directory containing `monarch_traces/`. If `None`,
///   defaults to `/tmp/{USER}/`.
/// - `execution_id`: specific execution to use. If `None`, follows the
///   `latest` symlink.
/// - `output`: path to write the merged trace file.
///
/// Returns the resolved execution ID.
pub fn merge_to_file(
    trace_root: Option<&Path>,
    execution_id: Option<&str>,
    output: &Path,
) -> Result<String> {
    let root = trace_root
        .map(PathBuf::from)
        .unwrap_or_else(default_trace_root);
    let tdir = trace_dir(&root);
    info!("Using trace directory: {}", tdir.display());

    let exec_id = match execution_id {
        Some(id) => id.to_string(),
        None => match get_latest_execution_id(&tdir)? {
            Some(id) => {
                info!("Using latest execution: {}", id);
                id
            }
            None => {
                bail!(
                    "No executions found in {}. Run a traced execution first.",
                    tdir.display()
                );
            }
        },
    };

    let edir = execution_dir(&tdir, Some(&exec_id))?;
    info!("Reading from execution: {}", edir.display());

    let files = find_pftrace_files(&edir)?;
    // Retain only correlated packet metadata across files, then stream a second
    // pass while applying the resulting flow edits.
    let mut scanner = CorrelationScanner::default();
    merge_trace_files(&files, &mut scanner)?;
    let flow_plan = scanner.into_flow_plan();

    let mut sink = Collector::new(output);
    {
        let mut injecting_sink = FlowInjectingSink {
            inner: &mut sink,
            flow_plan: &flow_plan,
            packet_index: 0,
        };
        merge_trace_files(&files, &mut injecting_sink)?;
    }
    sink.flush()?;

    Ok(exec_id)
}

const CORRELATION_ID_ANNOTATION: &str = "correlation_id";

fn slice_begin_correlation_id(
    packet: &TracePacket,
    interned_names: &HashMap<u64, String>,
) -> Option<u64> {
    let Data::TrackEvent(track_event) = packet.data.as_ref()? else {
        return None;
    };
    if track_event.r#type != Some(TrackEventType::SliceBegin as i32) {
        return None;
    }
    track_event.debug_annotations.iter().find_map(|ann| {
        let matches = match &ann.name_field {
            Some(tracing_perfetto_sdk_schema::debug_annotation::NameField::Name(name)) => {
                name == CORRELATION_ID_ANNOTATION
            }
            Some(tracing_perfetto_sdk_schema::debug_annotation::NameField::NameIid(iid)) => {
                interned_names
                    .get(iid)
                    .is_some_and(|name| name == CORRELATION_ID_ANNOTATION)
            }
            _ => false,
        };
        if matches {
            match &ann.value {
                Some(tracing_perfetto_sdk_schema::debug_annotation::Value::IntValue(value)) => {
                    Some(*value as u64)
                }
                _ => None,
            }
        } else {
            None
        }
    })
}

/// Derives a distinct Perfetto flow id for the (correlation_id, receiver)
/// pair. Hashing keeps ids for different correlation_ids from overlapping the
/// way `correlation_id + offset` would when two correlation_ids fall within
/// `num_receivers` of each other.
fn flow_id(correlation_id: u64, offset: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    correlation_id.hash(&mut hasher);
    (offset as u64).hash(&mut hasher);
    hasher.finish()
}

#[derive(Default)]
struct FlowEdits {
    flow_ids: Vec<u64>,
    terminating_flow_ids: Vec<u64>,
}

#[derive(Default)]
struct CorrelationScanner {
    packet_index: usize,
    annotation_names: HashMap<u32, HashMap<u64, String>>,
    correlated_packets: HashMap<u64, Vec<(usize, u64)>>,
}

impl CorrelationScanner {
    fn into_flow_plan(mut self) -> HashMap<usize, FlowEdits> {
        let mut flow_plan: HashMap<usize, FlowEdits> = HashMap::new();
        for (correlation_id, packets) in &mut self.correlated_packets {
            // Merge order is file-based, so timestamps determine caller before receiver.
            packets.sort_by_key(|&(index, timestamp)| (timestamp, index));
            let Some(((sender_index, _), receivers)) = packets.split_first() else {
                continue;
            };
            if receivers.is_empty() {
                continue;
            }

            for (offset, &(receiver_index, _)) in receivers.iter().enumerate() {
                let id = flow_id(*correlation_id, offset);
                flow_plan
                    .entry(*sender_index)
                    .or_default()
                    .flow_ids
                    .push(id);
                flow_plan
                    .entry(receiver_index)
                    .or_default()
                    .terminating_flow_ids
                    .push(id);
            }
        }
        flow_plan
    }
}

impl Sink for CorrelationScanner {
    fn consume(&mut self, packet: TracePacket) {
        let index = self.packet_index;
        self.packet_index += 1;

        let sequence_id = packet
            .optional_trusted_packet_sequence_id
            .map(|OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(id)| id);
        if packet.incremental_state_cleared == Some(true)
            && let Some(sequence_id) = sequence_id
        {
            self.annotation_names.remove(&sequence_id);
        }

        if let (Some(sequence_id), Some(interned)) = (sequence_id, packet.interned_data.as_ref()) {
            let names = self.annotation_names.entry(sequence_id).or_default();
            for annotation_name in &interned.debug_annotation_names {
                if let (Some(iid), Some(name)) = (annotation_name.iid, &annotation_name.name) {
                    names.insert(iid, name.clone());
                }
            }
        }

        let empty = HashMap::new();
        let interned_names = sequence_id
            .and_then(|id| self.annotation_names.get(&id))
            .unwrap_or(&empty);
        if let Some(correlation_id) = slice_begin_correlation_id(&packet, interned_names) {
            self.correlated_packets
                .entry(correlation_id)
                .or_default()
                .push((index, packet.timestamp.unwrap_or(u64::MAX)));
        }
    }
}

struct FlowInjectingSink<'a, S> {
    inner: &'a mut S,
    flow_plan: &'a HashMap<usize, FlowEdits>,
    packet_index: usize,
}

impl<S: Sink> Sink for FlowInjectingSink<'_, S> {
    fn consume(&mut self, mut packet: TracePacket) {
        if let (Some(edits), Some(Data::TrackEvent(track_event))) =
            (self.flow_plan.get(&self.packet_index), packet.data.as_mut())
        {
            track_event.flow_ids.extend_from_slice(&edits.flow_ids);
            track_event
                .terminating_flow_ids
                .extend_from_slice(&edits.terminating_flow_ids);
        }
        self.packet_index += 1;
        self.inner.consume(packet);
    }
}
