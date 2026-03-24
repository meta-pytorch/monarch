/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! DatabaseScanner - Local MemTable operations, scans with child stream merging

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use hyperactor::Instance;
use hyperactor::reference;
use monarch_hyperactor::actor::PythonActor;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::mailbox::PyPortId;
use monarch_hyperactor::runtime::get_tokio_runtime;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyModule;
use serde_multipart::Part;

use crate::EntityDispatcher;
use crate::QueryResponse;
use crate::RecordBatchSink;
use crate::pyspy_table::PySpyDumpBuffer;
use crate::pyspy_table::PySpyFrameBuffer;
use crate::pyspy_table::PySpyLocalVariableBuffer;
use crate::pyspy_table::PySpyStackTraceBuffer;
use crate::record_batch_sink::RecordBatchBuffer;
use crate::serialize_batch;
use crate::serialize_schema;
use crate::timestamp_to_micros;

/// Wraps a table's data so we can dynamically push new batches.
/// The MemTable is created on initialization and shared with queries.
pub struct LiveTableData {
    /// The MemTable that queries use
    mem_table: Arc<MemTable>,
}

impl LiveTableData {
    fn new(schema: SchemaRef) -> Self {
        let mem_table = MemTable::try_new(schema, vec![vec![]])
            .expect("failed to create MemTable with empty partition");
        Self {
            mem_table: Arc::new(mem_table),
        }
    }

    /// Push a new batch to the table.
    pub async fn push(&self, batch: RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }

        let partition = &self.mem_table.batches[0];
        let mut guard = partition.write().await;
        guard.push(batch);
    }

    /// Filter the table's data, keeping only rows that match the WHERE clause.
    ///
    /// Holds the write lock for the entire operation to prevent data loss
    /// from concurrent `push()` calls.
    pub async fn apply_retention(
        &self,
        table_name: &str,
        where_clause: &str,
    ) -> anyhow::Result<()> {
        use futures::TryStreamExt;

        let partition = &self.mem_table.batches[0];
        let mut guard = partition.write().await;

        // Drain current batches into a temporary MemTable for querying.
        let current_batches: Vec<RecordBatch> = guard.drain(..).collect();
        let tmp = MemTable::try_new(self.mem_table.schema(), vec![current_batches])?;

        let ctx = SessionContext::new();
        ctx.register_table(table_name, Arc::new(tmp))?;

        let query = format!("SELECT * FROM {table_name} WHERE {where_clause}");
        let df = ctx.sql(&query).await?;
        let filtered: Vec<RecordBatch> = df.execute_stream().await?.try_collect().await?;

        for batch in filtered {
            if batch.num_rows() > 0 {
                guard.push(batch);
            }
        }
        Ok(())
    }

    /// Get the schema.
    pub fn schema(&self) -> SchemaRef {
        self.mem_table.schema()
    }

    /// Get the MemTable for registering with a SessionContext.
    pub fn mem_table(&self) -> Arc<MemTable> {
        self.mem_table.clone()
    }
}

/// Default retention duration: 10 minutes in seconds.
const DEFAULT_RETENTION_SECS: u64 = 10 * 60;

/// Tables that keep only recent data; all others have unlimited retention.
const RETENTION_TABLES: &[&str] = &["sent_messages", "messages", "message_status_events"];

#[pyclass(
    name = "DatabaseScanner",
    module = "monarch._rust_bindings.monarch_distributed_telemetry.database_scanner"
)]
pub struct DatabaseScanner {
    /// Tables stored by name - each holds the schema and shared PartitionData
    table_data: Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
    rank: usize,
    /// Retention window in microseconds.
    retention_us: i64,
    /// Handle to flush the RecordBatchSink for trace events (spans, events)
    sink: Option<RecordBatchSink>,
    /// Handle to flush the EntityDispatcher for entity events (actors, meshes)
    dispatcher: Option<EntityDispatcher>,
}

#[pymethods]
impl DatabaseScanner {
    #[new]
    #[pyo3(signature = (rank, batch_size=1000, retention_secs=DEFAULT_RETENTION_SECS))]
    fn new(rank: usize, batch_size: usize, retention_secs: u64) -> PyResult<Self> {
        let mut scanner = Self {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            rank,
            retention_us: retention_secs as i64 * 1_000_000,
            sink: None,
            dispatcher: None,
        };

        // Create and register a RecordBatchSink for trace events (spans, events)
        let sink = scanner.create_record_batch_sink(batch_size);
        scanner.sink = Some(sink.clone());
        hyperactor_telemetry::register_sink(Box::new(sink));

        // Create and register an EntityDispatcher for entity events (actors, meshes)
        let dispatcher = scanner.create_entity_dispatcher(batch_size);
        scanner.dispatcher = Some(dispatcher.clone());
        hyperactor_telemetry::set_entity_dispatcher(Box::new(dispatcher));

        // Pre-register py-spy tables so QueryEngine discovers them at setup time
        for (name, batch) in [
            (
                "pyspy_dumps",
                PySpyDumpBuffer::default().to_record_batch().unwrap(),
            ),
            (
                "pyspy_stack_traces",
                PySpyStackTraceBuffer::default().to_record_batch().unwrap(),
            ),
            (
                "pyspy_frames",
                PySpyFrameBuffer::default().to_record_batch().unwrap(),
            ),
            (
                "pyspy_local_variables",
                PySpyLocalVariableBuffer::default()
                    .to_record_batch()
                    .unwrap(),
            ),
        ] {
            Self::push_batch_to_tables(&scanner.table_data, name, batch).unwrap();
        }

        Ok(scanner)
    }

    /// Flush any pending trace events and entity events to the tables,
    /// then apply time-based retention policies.
    fn flush(&self) -> PyResult<()> {
        if let Some(ref sink) = self.sink {
            sink.flush()
                .map_err(|e| PyException::new_err(format!("failed to flush sink: {}", e)))?;
        }
        if let Some(ref dispatcher) = self.dispatcher {
            dispatcher
                .flush()
                .map_err(|e| PyException::new_err(format!("failed to flush dispatcher: {}", e)))?;
        }
        self.apply_retention_policies()?;
        Ok(())
    }

    /// Filter a single table, keeping only rows that match the WHERE clause.
    fn apply_retention(&self, table_name: &str, where_clause: &str) -> PyResult<()> {
        let table = {
            let guard = self
                .table_data
                .lock()
                .map_err(|_| PyException::new_err("lock poisoned"))?;
            match guard.get(table_name) {
                Some(t) => t.clone(),
                None => return Ok(()),
            }
        };

        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| {
                handle.block_on(table.apply_retention(table_name, where_clause))
            })
        } else {
            get_tokio_runtime().block_on(table.apply_retention(table_name, where_clause))
        };
        result.map_err(|e| PyException::new_err(e.to_string()))
    }

    /// Get list of table names.
    fn table_names(&self) -> PyResult<Vec<String>> {
        self.flush()?;
        let guard = self
            .table_data
            .lock()
            .map_err(|_| PyException::new_err("lock poisoned"))?;
        Ok(guard.keys().cloned().collect())
    }

    /// Get schema for a table in Arrow IPC format.
    fn schema_for<'py>(&self, py: Python<'py>, table: &str) -> PyResult<Bound<'py, PyBytes>> {
        self.flush()?;
        let guard = self
            .table_data
            .lock()
            .map_err(|_| PyException::new_err("lock poisoned"))?;
        let table_data = guard
            .get(table)
            .ok_or_else(|| PyException::new_err(format!("table '{}' not found", table)))?;
        let schema = table_data.schema();
        let bytes = serialize_schema(&schema).map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Store a py-spy dump result into the pyspy_stacks table.
    fn store_pyspy_dump_py(
        &self,
        dump_id: &str,
        proc_ref: &str,
        pyspy_result_json: &str,
    ) -> PyResult<()> {
        self.store_pyspy_dump(dump_id, proc_ref, pyspy_result_json)
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    /// Perform a scan, sending results directly to the dest port.
    ///
    /// Sends local scan results to `dest` synchronously. The Python caller
    /// is responsible for calling children and waiting for them to complete.
    /// When this method and all child scans return, all data has been sent.
    ///
    /// Args:
    ///     dest: The destination PortId to send results to
    ///     table_name: Name of the table to scan
    ///     projection: Optional list of column indices to project
    ///     limit: Optional row limit
    ///     filter_expr: Optional SQL WHERE clause
    ///
    /// Returns:
    ///     Number of batches sent
    fn scan(
        &self,
        py: Python<'_>,
        dest: &PyPortId,
        table_name: String,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        filter_expr: Option<String>,
    ) -> PyResult<usize> {
        self.flush()?;

        // Get actor instance from context and extract the Rust Instance once
        let actor_module = py.import("monarch.actor")?;
        let ctx = actor_module.call_method0("context")?;
        let actor_instance_obj = ctx.getattr("actor_instance")?;
        let py_instance: PyRef<'_, PyInstance> = actor_instance_obj.extract()?;
        let instance: Instance<PythonActor> = py_instance.clone_for_py();

        // Build destination PortRef once
        let dest_port_id: reference::PortId = dest.clone().into();
        let dest_ref: reference::PortRef<QueryResponse> = reference::PortRef::attest(dest_port_id);

        // Execute scan, streaming batches directly to destination
        self.execute_scan_streaming(
            &table_name,
            projection,
            filter_expr,
            limit,
            &instance,
            &dest_ref,
        )
    }
}

impl DatabaseScanner {
    /// Static method to push a batch to the table_data map.
    /// This can be used from closures that capture the Arc.
    ///
    /// If the batch is empty, creates the table with the schema but doesn't append data.
    fn push_batch_to_tables(
        table_data: &Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        let table = {
            let mut guard = table_data
                .lock()
                .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
            guard
                .entry(table_name.to_string())
                .or_insert_with(|| Arc::new(LiveTableData::new(batch.schema())))
                .clone()
        };

        // Push the batch (push ignores empty batches).
        // Use block_in_place + Handle::current() when called from within a tokio
        // runtime (e.g., from notify_sent_message on a worker thread), otherwise
        // fall back to creating/reusing a runtime via get_tokio_runtime().
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(table.push(batch)));
        } else {
            get_tokio_runtime().block_on(table.push(batch));
        }
        Ok(())
    }

    /// Create a RecordBatchSink that pushes batches to this scanner's tables.
    ///
    /// The sink can be registered with hyperactor_telemetry::register_sink()
    /// to receive trace events and store them as queryable tables.
    pub fn create_record_batch_sink(&self, batch_size: usize) -> RecordBatchSink {
        let table_data = self.table_data.clone();

        RecordBatchSink::new(
            batch_size,
            Box::new(move |table_name, batch| {
                if let Err(e) = Self::push_batch_to_tables(&table_data, table_name, batch) {
                    tracing::error!("Failed to push batch to table {}: {}", table_name, e);
                }
            }),
        )
    }

    /// Create an EntityDispatcher that pushes batches to this scanner's tables.
    ///
    /// The dispatcher can be registered with hyperactor_telemetry::set_entity_dispatcher()
    /// to receive entity events (actors, meshes) and store them as queryable tables.
    pub fn create_entity_dispatcher(&self, batch_size: usize) -> EntityDispatcher {
        let table_data = self.table_data.clone();

        EntityDispatcher::new(
            batch_size,
            Box::new(move |table_name, batch| {
                if let Err(e) = Self::push_batch_to_tables(&table_data, table_name, batch) {
                    tracing::error!("Failed to push batch to table {}: {}", table_name, e);
                }
            }),
        )
    }

    /// Parse a py-spy result JSON and store data in normalized py-spy tables.
    ///
    /// Populates four tables matching the `hyperactor_mesh::pyspy` structs:
    /// - `pyspy_dumps`: one row per dump
    /// - `pyspy_stack_traces`: one row per thread (matches `PySpyStackTrace`)
    /// - `pyspy_frames`: one row per frame (matches `PySpyFrame`)
    /// - `pyspy_local_variables`: one row per local variable (matches `PySpyLocalVariable`)
    ///
    /// Design notes:
    /// - Non-Ok results (`BinaryNotFound`, `Failed`) are silently dropped.
    ///   We intentionally do not record them as structured telemetry today;
    ///   the caller can log or count those cases if needed.
    /// - `dump_id` is caller-provided; uniqueness is the caller's responsibility.
    /// - `timestamp_us` records ingestion time, not py-spy capture time (the
    ///   py-spy JSON carries no capture timestamp).
    /// - We parse via `serde_json::Value` rather than importing the typed
    ///   `PySpyResult` to avoid a crate dependency on `hyperactor_mesh`. The
    ///   tradeoff is that schema drift in the py-spy structs will not be caught
    ///   at compile time.
    pub fn store_pyspy_dump(
        &self,
        dump_id: &str,
        proc_ref: &str,
        pyspy_result_json: &str,
    ) -> anyhow::Result<()> {
        use crate::pyspy_table::PySpyDump;
        use crate::pyspy_table::PySpyDumpBuffer;
        use crate::pyspy_table::PySpyFrame;
        use crate::pyspy_table::PySpyFrameBuffer;
        use crate::pyspy_table::PySpyLocalVariable;
        use crate::pyspy_table::PySpyLocalVariableBuffer;
        use crate::pyspy_table::PySpyStackTrace;
        use crate::pyspy_table::PySpyStackTraceBuffer;
        use crate::record_batch_sink::RecordBatchBuffer;

        let value: serde_json::Value = serde_json::from_str(pyspy_result_json)?;
        let ok = match value.get("Ok") {
            Some(ok) => ok,
            None => return Ok(()),
        };

        let pid = ok.get("pid").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
        let binary = ok
            .get("binary")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let traces = ok.get("stack_traces").and_then(|v| v.as_array());

        let now_us = timestamp_to_micros(&SystemTime::now());

        // Insert dump row
        let mut dump_buf = PySpyDumpBuffer::default();
        dump_buf.insert(PySpyDump {
            dump_id: dump_id.to_string(),
            timestamp_us: now_us,
            pid,
            binary,
            proc_ref: proc_ref.to_string(),
        });
        Self::push_batch_to_tables(&self.table_data, "pyspy_dumps", dump_buf.to_record_batch()?)?;

        // Insert stack trace, frame, and local variable rows
        let mut trace_buf = PySpyStackTraceBuffer::default();
        let mut frame_buf = PySpyFrameBuffer::default();
        let mut local_buf = PySpyLocalVariableBuffer::default();

        if let Some(traces) = traces {
            for trace in traces {
                let thread_id = trace.get("thread_id").and_then(|v| v.as_u64()).unwrap_or(0);

                trace_buf.insert(PySpyStackTrace {
                    dump_id: dump_id.to_string(),
                    pid: trace
                        .get("pid")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(pid as i64) as i32,
                    thread_id,
                    thread_name: trace
                        .get("thread_name")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    os_thread_id: trace.get("os_thread_id").and_then(|v| v.as_u64()),
                    active: trace
                        .get("active")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    owns_gil: trace
                        .get("owns_gil")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                });

                if let Some(frames) = trace.get("frames").and_then(|v| v.as_array()) {
                    for (depth, frame) in frames.iter().enumerate() {
                        frame_buf.insert(PySpyFrame {
                            dump_id: dump_id.to_string(),
                            thread_id,
                            frame_depth: depth as i32,
                            name: frame
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            filename: frame
                                .get("filename")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            module: frame
                                .get("module")
                                .and_then(|v| v.as_str())
                                .map(String::from),
                            short_filename: frame
                                .get("short_filename")
                                .and_then(|v| v.as_str())
                                .map(String::from),
                            line: frame.get("line").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                            is_entry: frame
                                .get("is_entry")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
                        });

                        if let Some(locals) = frame.get("locals").and_then(|v| v.as_array()) {
                            for local in locals {
                                local_buf.insert(PySpyLocalVariable {
                                    dump_id: dump_id.to_string(),
                                    thread_id,
                                    frame_depth: depth as i32,
                                    name: local
                                        .get("name")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                    addr: local.get("addr").and_then(|v| v.as_u64()).unwrap_or(0),
                                    arg: local
                                        .get("arg")
                                        .and_then(|v| v.as_bool())
                                        .unwrap_or(false),
                                    repr: local
                                        .get("repr")
                                        .and_then(|v| v.as_str())
                                        .map(String::from),
                                });
                            }
                        }
                    }
                }
            }
        }

        Self::push_batch_to_tables(
            &self.table_data,
            "pyspy_stack_traces",
            trace_buf.to_record_batch()?,
        )?;
        Self::push_batch_to_tables(
            &self.table_data,
            "pyspy_frames",
            frame_buf.to_record_batch()?,
        )?;
        Self::push_batch_to_tables(
            &self.table_data,
            "pyspy_local_variables",
            local_buf.to_record_batch()?,
        )?;
        Ok(())
    }

    /// Apply retention policies for all configured tables.
    /// Skipped when retention_us is 0 (unlimited).
    fn apply_retention_policies(&self) -> PyResult<()> {
        if self.retention_us == 0 {
            return Ok(());
        }

        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_micros() as i64;
        let cutoff = now_us - self.retention_us;
        let where_clause = format!("timestamp_us > {cutoff}");

        for &table_name in RETENTION_TABLES {
            self.apply_retention(table_name, &where_clause)?;
        }
        Ok(())
    }

    /// Get a clone of the table_data Arc for sharing with sinks.
    pub fn table_data(&self) -> Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>> {
        self.table_data.clone()
    }

    fn execute_scan_streaming(
        &self,
        table_name: &str,
        projection: Option<Vec<usize>>,
        where_clause: Option<String>,
        limit: Option<usize>,
        instance: &Instance<PythonActor>,
        dest_ref: &reference::PortRef<QueryResponse>,
    ) -> PyResult<usize> {
        let rank = self.rank;

        // Get the LiveTableData's MemTable
        let (schema, mem_table) = {
            let guard = self
                .table_data
                .lock()
                .map_err(|_| PyException::new_err("lock poisoned"))?;
            let table_data = guard
                .get(table_name)
                .ok_or_else(|| PyException::new_err(format!("table '{}' not found", table_name)))?;
            (table_data.schema(), table_data.mem_table())
        };

        // Handle empty projection (e.g., for COUNT(*) queries)
        // DataFusion may request 0 columns but we still need row counts
        let is_empty_projection = matches!(&projection, Some(proj) if proj.is_empty());

        // Build a query using DataFusion
        let ctx = SessionContext::new();
        ctx.register_table(table_name, mem_table)
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // Build SELECT clause - for empty projection, use NULL as fake_column
        let columns = match &projection {
            Some(proj) if !proj.is_empty() => {
                let selected: Vec<_> = proj
                    .iter()
                    .filter_map(|&i| schema.fields().get(i).map(|f| f.name().clone()))
                    .collect();
                if selected.is_empty() {
                    "*".into()
                } else {
                    selected.join(", ")
                }
            }
            Some(_) => "NULL as fake_column".into(),
            _ => "*".into(),
        };

        let query = format!(
            "SELECT {} FROM {}{}{}",
            columns,
            table_name,
            where_clause
                .map(|c| format!(" WHERE {}", c))
                .unwrap_or_default(),
            limit.map(|n| format!(" LIMIT {}", n)).unwrap_or_default()
        );

        // Execute and stream batches directly to destination
        let batch_count = get_tokio_runtime()
            .block_on(async {
                use futures::StreamExt;

                let df = ctx.sql(&query).await?;
                let mut stream = df.execute_stream().await?;
                let mut count: usize = 0;

                while let Some(result) = stream.next().await {
                    let batch = result?;

                    // For empty projection, project to empty schema
                    let batch = if is_empty_projection {
                        batch.project(&[])?
                    } else {
                        batch
                    };

                    if let Ok(data) = serialize_batch(&batch) {
                        tracing::info!("Scanner {}: sending batch {}", rank, count);
                        let msg = QueryResponse {
                            data: Part::from(data),
                        };
                        if let Err(e) = dest_ref.send(instance, msg) {
                            tracing::debug!(
                                "Scanner {}: send error for batch {}: {:?}",
                                rank,
                                count,
                                e
                            );
                        }
                        count += 1;
                    }
                }

                tracing::info!(
                    "Scanner {}: local scan complete, sent {} batches",
                    rank,
                    count
                );
                Ok::<usize, datafusion::error::DataFusionError>(count)
            })
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(batch_count)
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<DatabaseScanner>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Array;
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;

    use super::*;

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let col = Int64Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
    }

    async fn row_count(table: &LiveTableData) -> usize {
        table.mem_table.batches[0]
            .read()
            .await
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    #[tokio::test]
    async fn test_empty_batch_ignored() {
        let table = LiveTableData::new(make_batch(&[]).schema());

        table.push(make_batch(&[])).await;
        assert_eq!(row_count(&table).await, 0);
    }

    #[tokio::test]
    async fn test_apply_retention_filters_rows() {
        // Push rows with x values 1..=5, then keep only x >= 3.
        let table = LiveTableData::new(make_batch(&[]).schema());
        table.push(make_batch(&[1, 2, 3, 4, 5])).await;

        table.apply_retention("t", "x >= 3").await.unwrap();

        // 3 rows should remain (3, 4, 5).
        assert_eq!(row_count(&table).await, 3);
    }

    #[tokio::test]
    async fn test_apply_retention_keeps_all() {
        let table = LiveTableData::new(make_batch(&[]).schema());
        table.push(make_batch(&[1, 2, 3])).await;

        table.apply_retention("t", "1=1").await.unwrap();

        assert_eq!(row_count(&table).await, 3);
    }

    #[tokio::test]
    async fn test_concurrent_push_during_retention() {
        // Verify that a push() concurrent with apply_retention() is not lost.
        let table = Arc::new(LiveTableData::new(make_batch(&[]).schema()));
        table.push(make_batch(&[1, 2, 3, 4, 5])).await;

        let table_clone = table.clone();
        let push_handle = tokio::spawn(async move {
            // This push races with apply_retention. The write lock ensures
            // it either completes before or after retention, never lost.
            table_clone.push(make_batch(&[10, 11])).await;
        });

        // Retain only x >= 3 from the original batch.
        table.apply_retention("t", "x >= 3").await.unwrap();
        push_handle.await.unwrap();

        // The pushed batch (10, 11) must survive regardless of ordering.
        // If push ran first: 1,2,3,4,5,10,11 -> retain x>=3 -> 3,4,5,10,11 = 5 rows
        // If push ran after: 1,2,3,4,5 -> retain x>=3 -> 3,4,5 -> push 10,11 = 5 rows
        assert_eq!(row_count(&table).await, 5);
    }

    fn table_row_count(scanner: &DatabaseScanner, table_name: &str) -> usize {
        let guard = scanner.table_data.lock().unwrap();
        match guard.get(table_name) {
            Some(table) => get_tokio_runtime().block_on(async {
                table.mem_table().batches[0]
                    .read()
                    .await
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            }),
            None => 0,
        }
    }

    fn table_batches(scanner: &DatabaseScanner, table_name: &str) -> Vec<RecordBatch> {
        let guard = scanner.table_data.lock().unwrap();
        match guard.get(table_name) {
            Some(table) => get_tokio_runtime()
                .block_on(async { table.mem_table().batches[0].read().await.clone() }),
            None => vec![],
        }
    }

    #[test]
    fn test_store_pyspy_dump_creates_normalized_rows() {
        let scanner = DatabaseScanner {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            rank: 0,
            retention_us: 0,
            sink: None,
            dispatcher: None,
        };

        let json = r#"{
            "Ok": {
                "pid": 1234, "binary": "python3",
                "stack_traces": [{
                    "pid": 1234, "thread_id": 100,
                    "thread_name": "MainThread", "os_thread_id": 5678,
                    "active": true, "owns_gil": true,
                    "frames": [
                        {"name": "inner", "filename": "a.py", "module": "a",
                         "short_filename": "a.py", "line": 10, "locals": [
                            {"name": "x", "addr": 100, "arg": true, "repr": "42"},
                            {"name": "y", "addr": 200, "arg": false, "repr": null}
                         ], "is_entry": false},
                        {"name": "outer", "filename": "a.py", "module": "a",
                         "short_filename": "a.py", "line": 5, "locals": [
                            {"name": "z", "addr": 300, "arg": true, "repr": "'hello'"}
                         ], "is_entry": true}
                    ]
                }],
                "warnings": []
            }
        }"#;

        scanner.store_pyspy_dump("dump-1", "proc[0]", json).unwrap();

        assert_eq!(table_row_count(&scanner, "pyspy_dumps"), 1);
        assert_eq!(table_row_count(&scanner, "pyspy_stack_traces"), 1);
        assert_eq!(table_row_count(&scanner, "pyspy_frames"), 2);
        assert_eq!(table_row_count(&scanner, "pyspy_local_variables"), 3);

        // Verify pyspy_dumps content
        let batches = table_batches(&scanner, "pyspy_dumps");
        let batch = &batches[0];
        let dump_ids = batch
            .column_by_name("dump_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pids = batch
            .column_by_name("pid")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let binaries = batch
            .column_by_name("binary")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let proc_refs = batch
            .column_by_name("proc_ref")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(dump_ids.value(0), "dump-1");
        assert_eq!(pids.value(0), 1234);
        assert_eq!(binaries.value(0), "python3");
        assert_eq!(proc_refs.value(0), "proc[0]");

        // Verify pyspy_stack_traces content
        let batches = table_batches(&scanner, "pyspy_stack_traces");
        let batch = &batches[0];
        let dump_ids = batch
            .column_by_name("dump_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let thread_ids = batch
            .column_by_name("thread_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let thread_names = batch
            .column_by_name("thread_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let os_thread_ids = batch
            .column_by_name("os_thread_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let actives = batch
            .column_by_name("active")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let owns_gils = batch
            .column_by_name("owns_gil")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(dump_ids.value(0), "dump-1");
        assert_eq!(thread_ids.value(0), 100);
        assert_eq!(thread_names.value(0), "MainThread");
        assert_eq!(os_thread_ids.value(0), 5678);
        assert!(actives.value(0), "thread should be active");
        assert!(owns_gils.value(0), "thread should own GIL");

        // Verify pyspy_frames content (2 rows: inner at depth 0, outer at depth 1)
        let batches = table_batches(&scanner, "pyspy_frames");
        let batch = &batches[0];
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let filenames = batch
            .column_by_name("filename")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let depths = batch
            .column_by_name("frame_depth")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let lines = batch
            .column_by_name("line")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let is_entries = batch
            .column_by_name("is_entry")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(names.value(0), "inner");
        assert_eq!(filenames.value(0), "a.py");
        assert_eq!(depths.value(0), 0);
        assert_eq!(lines.value(0), 10);
        assert!(!is_entries.value(0), "inner frame is not entry");
        assert_eq!(names.value(1), "outer");
        assert_eq!(filenames.value(1), "a.py");
        assert_eq!(depths.value(1), 1);
        assert_eq!(lines.value(1), 5);
        assert!(is_entries.value(1), "outer frame is entry");

        // Verify pyspy_local_variables content (3 rows)
        let batches = table_batches(&scanner, "pyspy_local_variables");
        let batch = &batches[0];
        let dump_ids = batch
            .column_by_name("dump_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let thread_ids = batch
            .column_by_name("thread_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let depths = batch
            .column_by_name("frame_depth")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let var_names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let addrs = batch
            .column_by_name("addr")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let args = batch
            .column_by_name("arg")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let reprs = batch
            .column_by_name("repr")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Row 0: x, addr=100, arg=true, repr=Some("42")
        assert_eq!(dump_ids.value(0), "dump-1");
        assert_eq!(thread_ids.value(0), 100);
        assert_eq!(depths.value(0), 0);
        assert_eq!(var_names.value(0), "x");
        assert_eq!(addrs.value(0), 100);
        assert!(args.value(0), "x is an argument");
        assert_eq!(reprs.value(0), "42");
        assert!(!reprs.is_null(0), "x repr should be Some");
        // Row 1: y, addr=200, arg=false, repr=None
        assert_eq!(dump_ids.value(1), "dump-1");
        assert_eq!(thread_ids.value(1), 100);
        assert_eq!(depths.value(1), 0);
        assert_eq!(var_names.value(1), "y");
        assert_eq!(addrs.value(1), 200);
        assert!(!args.value(1), "y is not an argument");
        assert!(reprs.is_null(1), "y repr should be None");
        // Row 2: z, addr=300, arg=true, repr=Some("'hello'")
        assert_eq!(dump_ids.value(2), "dump-1");
        assert_eq!(thread_ids.value(2), 100);
        assert_eq!(depths.value(2), 1);
        assert_eq!(var_names.value(2), "z");
        assert_eq!(addrs.value(2), 300);
        assert!(args.value(2), "z is an argument");
        assert_eq!(reprs.value(2), "'hello'");
        assert!(!reprs.is_null(2), "z repr should be Some");
    }

    #[test]
    fn test_store_pyspy_dump_failed_result_no_rows() {
        let scanner = DatabaseScanner {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            rank: 0,
            retention_us: 0,
            sink: None,
            dispatcher: None,
        };

        let json =
            r#"{"Failed": {"pid": 1, "binary": "py-spy", "exit_code": 1, "stderr": "error"}}"#;
        scanner.store_pyspy_dump("dump-2", "proc[0]", json).unwrap();

        assert_eq!(table_row_count(&scanner, "pyspy_dumps"), 0);
        assert_eq!(table_row_count(&scanner, "pyspy_stack_traces"), 0);
        assert_eq!(table_row_count(&scanner, "pyspy_frames"), 0);
    }

    #[test]
    fn test_store_pyspy_dump_invalid_json_errors() {
        let scanner = DatabaseScanner {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            rank: 0,
            retention_us: 0,
            sink: None,
            dispatcher: None,
        };
        assert!(scanner.store_pyspy_dump("x", "p", "not json").is_err());
    }

    #[test]
    fn test_store_pyspy_dump_multiple_threads() {
        let scanner = DatabaseScanner {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            rank: 0,
            retention_us: 0,
            sink: None,
            dispatcher: None,
        };

        let json = r#"{
            "Ok": {
                "pid": 1, "binary": "python3",
                "stack_traces": [
                    {"pid": 1, "thread_id": 1, "thread_name": "Main", "os_thread_id": 10,
                     "active": true, "owns_gil": true,
                     "frames": [{"name": "f1", "filename": "a.py", "line": 1, "is_entry": false}]},
                    {"pid": 1, "thread_id": 2, "thread_name": "Worker", "os_thread_id": 11,
                     "active": false, "owns_gil": false,
                     "frames": [{"name": "f2", "filename": "b.py", "line": 2, "is_entry": false}]}
                ],
                "warnings": []
            }
        }"#;

        scanner.store_pyspy_dump("dump-3", "proc[0]", json).unwrap();

        assert_eq!(table_row_count(&scanner, "pyspy_dumps"), 1);
        assert_eq!(table_row_count(&scanner, "pyspy_stack_traces"), 2);
        assert_eq!(table_row_count(&scanner, "pyspy_frames"), 2);

        // Verify pyspy_stack_traces content: two threads
        let batches = table_batches(&scanner, "pyspy_stack_traces");
        let batch = &batches[0];
        let thread_ids = batch
            .column_by_name("thread_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let thread_names = batch
            .column_by_name("thread_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let actives = batch
            .column_by_name("active")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let owns_gils = batch
            .column_by_name("owns_gil")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // Thread 1: Main, active, owns GIL
        assert_eq!(thread_ids.value(0), 1);
        assert_eq!(thread_names.value(0), "Main");
        assert!(actives.value(0), "Main thread should be active");
        assert!(owns_gils.value(0), "Main thread should own GIL");
        // Thread 2: Worker, not active, no GIL
        assert_eq!(thread_ids.value(1), 2);
        assert_eq!(thread_names.value(1), "Worker");
        assert!(!actives.value(1), "Worker thread should not be active");
        assert!(!owns_gils.value(1), "Worker thread should not own GIL");

        // Verify pyspy_frames content: f1 on thread 1, f2 on thread 2
        let batches = table_batches(&scanner, "pyspy_frames");
        let batch = &batches[0];
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let frame_thread_ids = batch
            .column_by_name("thread_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let filenames = batch
            .column_by_name("filename")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "f1");
        assert_eq!(frame_thread_ids.value(0), 1);
        assert_eq!(filenames.value(0), "a.py");
        assert_eq!(names.value(1), "f2");
        assert_eq!(frame_thread_ids.value(1), 2);
        assert_eq!(filenames.value(1), "b.py");
    }
}
