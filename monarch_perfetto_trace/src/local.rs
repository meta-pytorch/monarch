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
use std::collections::HashSet;
use std::fs;
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
use tracing_perfetto_sdk_schema::TrackEvent;
use tracing_perfetto_sdk_schema::trace_packet::Data;
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

    if files.is_empty() {
        bail!("No .pftrace files found in {}", execution_dir.display());
    }

    tracing::info!("Found {} .pftrace files", files.len());
    Ok(files)
}

/// Reads and merges trace files from an execution directory.
///
/// Track UUIDs are offset to avoid collisions between different processes.
/// Since process_name is globally unique within an execution, we just need
/// a simple per-file offset that's large enough to not collide with the
/// max UUID count within a single process.
pub fn merge_traces_from_dir<S: Sink>(execution_dir: &Path, sink: &mut S) -> Result<()> {
    for (file_idx, path) in find_pftrace_files(execution_dir)?.iter().enumerate() {
        // We have to offset UUIDs here since within a single process,
        // we generate these by just incrementing a counter.
        let uuid_offset = (file_idx as u64 + 1) * 1_000_000;

        read_and_offset_trace_file(path, uuid_offset, sink)?;
    }

    Ok(())
}

/// Reads a single .pftrace file and sends packets to the sink with offset UUIDs.
///
/// The file may contain multiple concatenated `Trace` messages (protobuf containers).
fn read_and_offset_trace_file<S: Sink>(path: &Path, uuid_offset: u64, sink: &mut S) -> Result<()> {
    let mut file = fs::File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let mut offset = 0;
    while offset < buffer.len() {
        match Trace::decode(&buffer[offset..]) {
            Ok(trace) => {
                let encoded_len = trace.encoded_len();

                for mut packet in trace.packet {
                    offset_packet_uuids(&mut packet, uuid_offset);
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

/// Offsets all track UUIDs in a packet by the given amount.
fn offset_packet_uuids(packet: &mut TracePacket, offset: u64) {
    if let Some(ref mut data) = packet.data {
        match data {
            Data::TrackDescriptor(td) => {
                if let Some(ref mut uuid) = td.uuid {
                    *uuid += offset;
                }
                if let Some(ref mut parent) = td.parent_uuid {
                    *parent += offset;
                }
            }
            Data::TrackEvent(te) => {
                if let Some(ref mut uuid) = te.track_uuid {
                    *uuid += offset;
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

    let mut buffer = VecSink::default();
    merge_traces_from_dir(&edir, &mut buffer)?;
    inject_correlation_flows(&mut buffer.packets);

    let mut sink = Collector::new(output);
    for packet in buffer.packets {
        sink.consume(packet);
    }
    sink.flush()?;

    Ok(exec_id)
}

/// Collects packets into a Vec for post-processing before writing.
#[derive(Default)]
struct VecSink {
    packets: Vec<TracePacket>,
}

impl Sink for VecSink {
    fn consume(&mut self, packet: TracePacket) {
        self.packets.push(packet);
    }
}

const CORRELATION_ID_ANNOTATION: &str = "correlation_id";

fn build_annotation_name_table(packets: &[TracePacket]) -> HashMap<u64, String> {
    let mut table = HashMap::new();
    for packet in packets {
        if let Some(ref interned) = packet.interned_data {
            for dan in &interned.debug_annotation_names {
                if let (Some(iid), Some(name)) = (dan.iid, &dan.name) {
                    table.insert(iid, name.clone());
                }
            }
        }
    }
    table
}

fn get_annotation_u64(
    te: &TrackEvent,
    name: &str,
    interned_names: &HashMap<u64, String>,
) -> Option<u64> {
    te.debug_annotations.iter().find_map(|ann| {
        let matches = match &ann.name_field {
            Some(tracing_perfetto_sdk_schema::debug_annotation::NameField::Name(n)) => n == name,
            Some(tracing_perfetto_sdk_schema::debug_annotation::NameField::NameIid(iid)) => {
                interned_names.get(iid).is_some_and(|n| n == name)
            }
            _ => false,
        };
        if matches {
            match &ann.value {
                Some(tracing_perfetto_sdk_schema::debug_annotation::Value::IntValue(v)) => {
                    Some(*v as u64)
                }
                _ => None,
            }
        } else {
            None
        }
    })
}

fn is_slice_begin(te: &TrackEvent) -> bool {
    te.r#type == Some(TrackEventType::SliceBegin as i32)
}

/// Scans merged trace packets for correlation_id annotations and injects
/// Perfetto flow events to draw arrows from sender to receiver spans.
///
/// A "sender" is a SliceBegin with a correlation_id that appears exactly
/// once for that ID (the endpoint span). All other SliceBegins sharing
/// that correlation_id are "receivers".
///
/// Pass 1: count how many receivers exist per correlation_id.
/// Pass 2: stamp `flow_ids` on senders (one per receiver) and
///          `terminating_flow_ids` on receivers (sequential offset).
fn inject_correlation_flows(packets: &mut [TracePacket]) {
    let interned_names = build_annotation_name_table(packets);

    let mut counts: HashMap<u64, usize> = HashMap::new();
    let mut sender_cids: HashSet<u64> = HashSet::new();

    for packet in packets.iter() {
        let Some(Data::TrackEvent(te)) = &packet.data else {
            continue;
        };
        if !is_slice_begin(te) {
            continue;
        }
        let Some(cid) = get_annotation_u64(te, CORRELATION_ID_ANNOTATION, &interned_names) else {
            continue;
        };
        *counts.entry(cid).or_insert(0) += 1;
    }

    let mut receiver_counters: HashMap<u64, u64> = HashMap::new();

    for packet in packets.iter_mut() {
        let Some(Data::TrackEvent(te)) = &mut packet.data else {
            continue;
        };
        if !is_slice_begin(te) {
            continue;
        }
        let Some(cid) = get_annotation_u64(te, CORRELATION_ID_ANNOTATION, &interned_names) else {
            continue;
        };

        let total = match counts.get(&cid) {
            Some(&n) if n > 1 => n - 1,
            _ => continue,
        };

        if sender_cids.insert(cid) {
            te.flow_ids = (0..total).map(|i| cid.wrapping_add(i as u64)).collect();
        } else {
            let idx = receiver_counters.entry(cid).or_insert(0);
            te.terminating_flow_ids = vec![cid.wrapping_add(*idx)];
            *idx += 1;
        }
    }
}
