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

use std::fs;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use prost::Message;
use tracing_perfetto_sdk_schema::Trace;
use tracing_perfetto_sdk_schema::TracePacket;
use tracing_perfetto_sdk_schema::trace_packet::Data;

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
