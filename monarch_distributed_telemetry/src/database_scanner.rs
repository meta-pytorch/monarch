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
use crate::serialize_batch;
use crate::serialize_schema;

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
    /// Internal method to push a RecordBatch to a table.
    ///
    /// Creates the table if it doesn't exist, using the batch's schema.
    /// If the batch is empty, creates the table with the schema but doesn't append.
    /// This method is used both by the Python push_batch and by the Rust RecordBatchSink.
    pub fn push_batch_internal(&self, table_name: &str, batch: RecordBatch) -> anyhow::Result<()> {
        Self::push_batch_to_tables(&self.table_data, table_name, batch)
    }

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

    use datafusion::arrow::array::Int64Array;
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
}
