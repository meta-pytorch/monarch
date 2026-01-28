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

mod record_batch_sink;

use pyo3::prelude::*;
pub use record_batch_sink::FlushCallback;
pub use record_batch_sink::RecordBatchSink;
pub use record_batch_sink::get_flush_count;
pub use record_batch_sink::reset_flush_count;

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

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(enable_record_batch_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(get_record_batch_flush_count, module)?)?;
    module.add_function(wrap_pyfunction!(reset_record_batch_flush_count, module)?)?;
    Ok(())
}
