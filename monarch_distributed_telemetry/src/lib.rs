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

pub use record_batch_sink::FlushCallback;
pub use record_batch_sink::RecordBatchSink;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::array::TimestampMicrosecondArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::expr_to_sql;
use hyperactor::Bind;
use hyperactor::PortId;
use hyperactor::PortRef;
use hyperactor::Unbind;
use hyperactor::mailbox::PortReceiver;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::mailbox::PyMailbox;
use monarch_hyperactor::mailbox::PyPortId;
use monarch_hyperactor::pytokio::PyPythonTask;
use monarch_hyperactor::runtime::get_tokio_runtime;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

// ============================================================================
// Python module registration
// ============================================================================

/// Register the RecordBatchSink with the telemetry system.
/// This will cause trace events to be collected and printed as RecordBatches.
///
/// This can be called at any time - before or after telemetry initialization.
/// Sinks will start receiving events once the telemetry dispatcher is created.
#[pyfunction]
fn enable_record_batch_tracing(batch_size: usize) {
    let sink = RecordBatchSink::new_printing(batch_size);
    hyperactor_telemetry::register_sink(Box::new(sink));
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(enable_record_batch_tracing, module)?)?;
    Ok(())
}
