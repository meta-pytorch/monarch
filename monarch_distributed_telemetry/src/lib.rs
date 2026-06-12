/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Distributed Telemetry - Three-component architecture
//!
//! 1. DatabaseScanner (Rust): Local MemTable operations, scans with child stream merging
//! 2. DistributedTelemetryActor (Python): Orchestrates children, wraps DatabaseScanner
//! 3. QueryEngine (Rust): DataFusion query execution, creates ports, collects results
//!
//! Data flows directly Rust-to-Rust via PortRef for efficiency.

pub mod database_scanner;
mod entity_batch_sink;
pub mod pyspy_table;
pub mod query_engine;
mod record_batch_sink;
pub mod socket_ingest;

pub use database_scanner::DatabaseScanner;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
pub use entity_batch_sink::EntityBatchSink;
use monarch_record_batch::RecordBatchBuffer;
use monarch_telemetry_schema::entity_tables::ACTOR_STATUS_EVENTS;
use monarch_telemetry_schema::entity_tables::ACTORS;
use monarch_telemetry_schema::entity_tables::ActorBuffer;
use monarch_telemetry_schema::entity_tables::ActorStatusEventBuffer;
use monarch_telemetry_schema::entity_tables::MESHES;
use monarch_telemetry_schema::entity_tables::MESSAGE_STATUS_EVENTS;
use monarch_telemetry_schema::entity_tables::MESSAGES;
use monarch_telemetry_schema::entity_tables::MeshBuffer;
use monarch_telemetry_schema::entity_tables::MessageBuffer;
use monarch_telemetry_schema::entity_tables::MessageStatusEventBuffer;
use monarch_telemetry_schema::entity_tables::SENT_MESSAGES;
use monarch_telemetry_schema::entity_tables::SentMessageBuffer;
pub use monarch_telemetry_schema::serialize_batch;
use monarch_telemetry_schema::trace_tables::EVENTS;
use monarch_telemetry_schema::trace_tables::EventBuffer;
use monarch_telemetry_schema::trace_tables::SPAN_EVENTS;
use monarch_telemetry_schema::trace_tables::SPANS;
use monarch_telemetry_schema::trace_tables::SpanBuffer;
use monarch_telemetry_schema::trace_tables::SpanEventBuffer;
use pyo3::prelude::*;
pub use pyspy_table::PySpyDump;
pub use pyspy_table::PySpyFrame;
pub use pyspy_table::PySpyLocalVariable;
pub use pyspy_table::PySpyStackTrace;
pub use query_engine::QueryEngine;
pub use record_batch_sink::FlushCallback;
pub use record_batch_sink::RecordBatchSink;
pub use record_batch_sink::get_flush_count;
pub use record_batch_sink::reset_flush_count;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use typeuri::Named;

/// Response message for streaming query results.
/// Sent directly over Rust PortRef for efficiency.
/// Completion is signaled by the scan endpoint returning, not via a message.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct QueryResponse {
    /// A batch of data in Arrow IPC format.
    /// Uses Part for zero-copy transfer across the actor system.
    pub data: Part,
}

// ============================================================================
// Serialization helpers
// ============================================================================

/// Helper to convert SystemTime to microseconds since Unix epoch.
pub(crate) fn timestamp_to_micros(timestamp: &std::time::SystemTime) -> i64 {
    timestamp
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

pub(crate) fn serialize_schema(schema: &SchemaRef) -> anyhow::Result<Vec<u8>> {
    let batch = RecordBatch::new_empty(schema.clone());
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buf)
}

// ============================================================================
// Python module registration
// ============================================================================

/// Register the RecordBatchSink with the telemetry system.
/// This will cause trace events to be collected as RecordBatches.
/// Use `get_record_batch_flush_count()` to check how many batches have been flushed.
///
/// This can be called at any time - before or after telemetry initialization.
/// Sinks will start receiving events once the telemetry dispatcher is created.
///
/// Args:
///     batch_size: Number of rows to buffer before flushing each table
#[pyfunction]
fn enable_record_batch_tracing(batch_size: usize) {
    let sink = RecordBatchSink::new_printing(batch_size);
    hyperactor_telemetry::register_sink(Box::new(sink));
}

/// Get the total number of RecordBatches flushed by the sink.
/// This can be used in tests to verify that the sink is receiving events.
#[pyfunction]
fn get_record_batch_flush_count() -> usize {
    get_flush_count()
}

/// Reset the flush counter to zero. Useful for tests.
#[pyfunction]
fn reset_record_batch_flush_count() {
    reset_flush_count()
}

/// Start Unix-socket ingest for a database scanner.
#[pyfunction]
fn _start_socket_ingest(scanner: PyRef<'_, DatabaseScanner>, socket_path: &str) -> PyResult<bool> {
    scanner
        .start_socket_ingest(std::path::Path::new(socket_path))
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))
}

/// Register trace and entity schemas with a database scanner.
#[pyfunction]
fn _register_trace_entity_schemas(scanner: PyRef<'_, DatabaseScanner>) -> PyResult<()> {
    register_table_schema::<SpanBuffer>(&scanner, SPANS)?;
    register_table_schema::<SpanEventBuffer>(&scanner, SPAN_EVENTS)?;
    register_table_schema::<EventBuffer>(&scanner, EVENTS)?;
    register_table_schema::<ActorBuffer>(&scanner, ACTORS)?;
    register_table_schema::<MeshBuffer>(&scanner, MESHES)?;
    register_table_schema::<ActorStatusEventBuffer>(&scanner, ACTOR_STATUS_EVENTS)?;
    register_table_schema::<SentMessageBuffer>(&scanner, SENT_MESSAGES)?;
    register_table_schema::<MessageBuffer>(&scanner, MESSAGES)?;
    register_table_schema::<MessageStatusEventBuffer>(&scanner, MESSAGE_STATUS_EVENTS)?;
    Ok(())
}

fn register_table_schema<B>(scanner: &DatabaseScanner, table_name: &str) -> PyResult<()>
where
    B: RecordBatchBuffer + Default,
{
    scanner
        .register_table(
            table_name,
            B::default()
                .drain_to_record_batch()
                .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?
                .schema(),
        )
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))
}

/// Activate the process-global Unix socket telemetry sink.
#[pyfunction]
fn _set_unix_socket_sink_path(socket_path: &str) -> PyResult<()> {
    hyperactor_telemetry::set_unix_socket_sink_path(socket_path)
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(enable_record_batch_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(get_record_batch_flush_count, module)?)?;
    module.add_function(wrap_pyfunction!(reset_record_batch_flush_count, module)?)?;
    module.add_function(wrap_pyfunction!(_start_socket_ingest, module)?)?;
    module.add_function(wrap_pyfunction!(_register_trace_entity_schemas, module)?)?;
    module.add_function(wrap_pyfunction!(_set_unix_socket_sink_path, module)?)?;
    Ok(())
}
