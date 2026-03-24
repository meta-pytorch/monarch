/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! DataFusion table schemas for py-spy stack trace data.
//!
//! Four normalized tables matching the structures in `hyperactor_mesh::pyspy`:
//! - `pyspy_dumps`: one row per dump (top-level `PySpyResult::Ok` metadata)
//! - `pyspy_stack_traces`: one row per thread (matches `PySpyStackTrace`)
//! - `pyspy_frames`: one row per frame (matches `PySpyFrame`)
//! - `pyspy_local_variables`: one row per local variable (matches `PySpyLocalVariable`)

use record_batch_derive::RecordBatchRow;

use crate::record_batch_sink::RecordBatchBuffer;

/// Row data for the pyspy_dumps table.
#[derive(RecordBatchRow)]
pub struct PySpyDump {
    /// Caller-provided identifier. Uniqueness and semantics are the caller's
    /// responsibility (typically a UUID).
    pub dump_id: String,
    /// Ingestion timestamp, not the py-spy capture time. We record when the
    /// result was stored rather than when the snapshot was taken because the
    /// py-spy JSON does not carry a capture timestamp.
    pub timestamp_us: i64,
    pub pid: i32,
    pub binary: String,
    pub proc_ref: String,
}

/// Row data for the pyspy_stack_traces table.
/// Matches `hyperactor_mesh::pyspy::PySpyStackTrace`.
#[derive(RecordBatchRow)]
pub struct PySpyStackTrace {
    pub dump_id: String,
    pub pid: i32,
    pub thread_id: u64,
    pub thread_name: Option<String>,
    pub os_thread_id: Option<u64>,
    pub active: bool,
    pub owns_gil: bool,
}

/// Row data for the pyspy_frames table.
/// Matches `hyperactor_mesh::pyspy::PySpyFrame`.
#[derive(RecordBatchRow)]
pub struct PySpyFrame {
    pub dump_id: String,
    pub thread_id: u64,
    pub frame_depth: i32,
    pub name: String,
    pub filename: String,
    pub module: Option<String>,
    pub short_filename: Option<String>,
    pub line: i32,
    pub is_entry: bool,
}

/// Row data for the pyspy_local_variables table.
/// Matches `hyperactor_mesh::pyspy::PySpyLocalVariable`.
#[derive(RecordBatchRow)]
pub struct PySpyLocalVariable {
    pub dump_id: String,
    pub thread_id: u64,
    pub frame_depth: i32,
    pub name: String,
    pub addr: u64,
    pub arg: bool,
    pub repr: Option<String>,
}
