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
use datafusion::prelude::SessionConfig;
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

/// Response message for streaming query results.
/// Sent directly over Rust PortRef for efficiency.
/// Completion is signaled by the scan endpoint returning, not via a message.
#[derive(Debug, Clone, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct QueryResponse {
    /// A batch of data in Arrow IPC format.
    pub data: Vec<u8>,
}

// ============================================================================
// Helper to spawn reader task that drains PortReceiver into a channel
// ============================================================================

use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use tokio::sync::mpsc;

/// Spawns a task that reads QueryResponse messages from port_receiver until
/// the completion future resolves with the expected batch count, then waits
/// for exactly that many batches.
///
/// Returns a stream that reads from the channel.
fn create_draining_stream<F>(
    schema: SchemaRef,
    port_receiver: PortReceiver<QueryResponse>,
    completion_future: F,
) -> SendableRecordBatchStream
where
    F: std::future::Future<Output = PyResult<PyObject>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<DFResult<RecordBatch>>(32);

    // Spawn a task that reads messages until we have all expected batches
    get_tokio_runtime().spawn(async move {
        let mut receiver = port_receiver;
        let mut batch_count: usize = 0;
        let mut expected_batches: Option<usize> = None;

        tokio::pin!(completion_future);

        loop {
            // Check if we've received all expected batches
            if let Some(expected) = expected_batches {
                if batch_count >= expected {
                    tracing::info!(
                        "QueryEngine reader: received all {} expected batches",
                        expected
                    );
                    break;
                }
            }

            tokio::select! {
                biased;

                // Check if the scan has completed (only if we don't have expected count yet)
                result = &mut completion_future, if expected_batches.is_none() => {
                    match result {
                        Ok(py_result) => {
                            // Extract the batch count from the Python result
                            // Result is a ValueMesh which is iterable, yielding (rank_dict, count) tuples
                            let count = Python::with_gil(|py| {
                                let bound = py_result.bind(py);
                                let mut total: usize = 0;
                                // Iterate the ValueMesh
                                if let Ok(iter) = bound.iter() {
                                    for item in iter {
                                        if let Ok(tuple) = item {
                                            // Each item is (rank_dict, count) - get second element
                                            if let Ok(count_val) = tuple.get_item(1) {
                                                if let Ok(count) = count_val.extract::<usize>() {
                                                    total += count;
                                                }
                                            }
                                        }
                                    }
                                }
                                total
                            });
                            tracing::info!(
                                "QueryEngine reader: scan completed, expecting {} batches, have {}",
                                count,
                                batch_count
                            );
                            expected_batches = Some(count);
                        }
                        Err(e) => {
                            tracing::error!("QueryEngine reader: scan failed: {:?}", e);
                            let _ = tx.send(Err(datafusion::error::DataFusionError::External(
                                anyhow::anyhow!("Scan failed: {:?}", e).into(),
                            ))).await;
                            break;
                        }
                    }
                }

                // Receive data from the port
                recv_result = receiver.recv() => {
                    match recv_result {
                        Ok(QueryResponse { data }) => {
                            match deserialize_batch(&data) {
                                Ok(Some(batch)) => {
                                    batch_count += 1;
                                    if tx.send(Ok(batch)).await.is_err() {
                                        tracing::info!(
                                            "QueryEngine reader: consumer dropped, continuing to drain"
                                        );
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(datafusion::error::DataFusionError::External(e.into())))
                                        .await;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("QueryEngine reader: error receiving: {:?}", e);
                            let _ = tx
                                .send(Err(datafusion::error::DataFusionError::External(
                                    anyhow::anyhow!("Error receiving: {:?}", e).into(),
                                )))
                                .await;
                            break;
                        }
                    }
                }
            }
        }
        tracing::info!("QueryEngine reader: complete, {} batches", batch_count);
    });

    // Convert channel receiver to a stream
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

// ============================================================================
// DatabaseScanner - Rust struct holding MemTables, exposed to Python
// ============================================================================

/// Wraps a table's data so we can dynamically push new batches.
/// The MemTable is created on initialization and shared with queries.
pub struct LiveTableData {
    /// The MemTable that queries use
    mem_table: Arc<MemTable>,
    /// Maximum number of batches to keep (0 = unlimited)
    max_batches: usize,
}

impl LiveTableData {
    fn new(schema: SchemaRef, max_batches: usize) -> Self {
        // Create MemTable with one empty partition
        // try_new requires at least one partition, but the partition can be empty
        let mem_table = MemTable::try_new(schema, vec![vec![]])
            .expect("failed to create MemTable with empty partition");
        Self {
            mem_table: Arc::new(mem_table),
            max_batches,
        }
    }

    /// Push a new batch to the table. If max_batches is reached, removes the oldest.
    /// Empty batches are ignored (no-op).
    pub async fn push(&self, batch: RecordBatch) {
        // Ignore empty batches
        if batch.num_rows() == 0 {
            return;
        }
        // The MemTable has a single partition, push to it
        let partition = &self.mem_table.batches[0];
        let mut guard = partition.write().await;
        if self.max_batches > 0 && guard.len() >= self.max_batches {
            guard.remove(0);
        }
        guard.push(batch);
    }

    /// Get the schema
    pub fn schema(&self) -> SchemaRef {
        self.mem_table.schema()
    }

    /// Get the MemTable for registering with a SessionContext
    pub fn mem_table(&self) -> Arc<MemTable> {
        self.mem_table.clone()
    }
}

#[pyclass(
    name = "DatabaseScanner",
    module = "monarch._rust_bindings.monarch_extension.distributed_telemetry"
)]
pub struct DatabaseScanner {
    /// Tables stored by name - each holds the schema and shared PartitionData
    table_data: Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
    rank: usize,
    max_batches: usize,
    /// Handle to flush the RecordBatchSink (when not using fake data)
    sink: Option<RecordBatchSink>,
}

fn fill_fake_batches(scanner: &DatabaseScanner) -> anyhow::Result<()> {
    use rand::Rng;

    let mut rng = rand::thread_rng();

    // Create hosts table schema
    let hosts_schema = Arc::new(Schema::new(vec![
        Field::new("host_id", DataType::Int32, false),
        Field::new("hostname", DataType::Utf8, false),
        Field::new("datacenter", DataType::Utf8, false),
        Field::new("os", DataType::Utf8, false),
        Field::new("cpu_cores", DataType::Int32, false),
        Field::new("memory_gb", DataType::Int32, false),
    ]));

    // Use random base to avoid duplicate host_ids across actors
    let host_start = rng.gen_range(0..10000) * 10;
    let host_end = host_start + 10;
    let datacenters = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"];
    let os_types = ["ubuntu-22.04", "debian-12", "rhel-9", "amazon-linux-2"];
    let cpu_options = [4, 8, 16, 32, 64];
    let memory_options = [16, 32, 64, 128, 256];

    // Generate hosts data
    let mut host_ids = Vec::new();
    let mut hostnames = Vec::new();
    let mut dcs = Vec::new();
    let mut oses = Vec::new();
    let mut cpus = Vec::new();
    let mut mems = Vec::new();

    for host_id in host_start..host_end {
        host_ids.push(host_id as i32);
        hostnames.push(format!("server-{:05}", host_id));
        dcs.push(datacenters[rng.gen_range(0..datacenters.len())].to_string());
        oses.push(os_types[rng.gen_range(0..os_types.len())].to_string());
        cpus.push(cpu_options[rng.gen_range(0..cpu_options.len())] as i32);
        mems.push(memory_options[rng.gen_range(0..memory_options.len())] as i32);
    }

    let hosts_batch = RecordBatch::try_new(
        hosts_schema.clone(),
        vec![
            Arc::new(Int32Array::from(host_ids.clone())),
            Arc::new(StringArray::from(hostnames)),
            Arc::new(StringArray::from(dcs)),
            Arc::new(StringArray::from(oses)),
            Arc::new(Int32Array::from(cpus)),
            Arc::new(Int32Array::from(mems)),
        ],
    )?;
    scanner.push_batch_internal("hosts", hosts_batch)?;

    // Create metrics table schema
    let metrics_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("host_id", DataType::Int32, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // Generate metrics data
    let metric_names = [
        "cpu_usage",
        "memory_usage",
        "disk_io",
        "network_rx",
        "network_tx",
    ];

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    let mut timestamps = Vec::new();
    let mut metric_host_ids = Vec::new();
    let mut metric_names_col = Vec::new();
    let mut values = Vec::new();

    for i in 0..960 {
        let timestamp_micros = now - (i * 90 * 1_000_000);
        timestamps.push(timestamp_micros);

        let host_id = rng.gen_range(host_start..host_end);
        metric_host_ids.push(host_id as i32);

        let metric_name = metric_names[rng.gen_range(0..metric_names.len())];
        metric_names_col.push(metric_name.to_string());

        let value = match metric_name {
            "cpu_usage" | "memory_usage" => rng.gen_range(0.0..100.0),
            "disk_io" => rng.gen_range(0.0..1000.0),
            _ => rng.gen_range(0.0..10000.0),
        };
        values.push(value);
    }

    let metrics_batch = RecordBatch::try_new(
        metrics_schema.clone(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(Int32Array::from(metric_host_ids)),
            Arc::new(StringArray::from(metric_names_col)),
            Arc::new(Float64Array::from(values)),
        ],
    )?;
    scanner.push_batch_internal("metrics", metrics_batch)?;

    tracing::info!(
        "Worker {}: initialized with hosts {}-{}",
        scanner.rank,
        host_start,
        host_end - 1
    );
    Ok(())
}

fn serialize_schema(schema: &SchemaRef) -> anyhow::Result<Vec<u8>> {
    let batch = RecordBatch::new_empty(schema.clone());
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buf)
}

fn serialize_batch(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

fn deserialize_schema(data: &[u8]) -> anyhow::Result<SchemaRef> {
    let reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    Ok(reader.schema())
}

fn deserialize_batch(data: &[u8]) -> anyhow::Result<Option<RecordBatch>> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    Ok(reader.next().transpose()?)
}

#[pymethods]
impl DatabaseScanner {
    #[new]
    #[pyo3(signature = (rank, use_fake_data=true, max_batches=100, batch_size=1000))]
    fn new(rank: usize, use_fake_data: bool, max_batches: usize, batch_size: usize) -> PyResult<Self> {
        let mut scanner = Self {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            rank,
            max_batches,
            sink: None,
        };

        if use_fake_data {
            fill_fake_batches(&scanner)
                .map_err(|e| PyException::new_err(format!("failed to create fake data: {}", e)))?;
        } else {
            // Register a RecordBatchSink to receive telemetry events
            // Clone the sink before registering so we can call flush() later
            let sink = scanner.create_record_batch_sink(batch_size);
            scanner.sink = Some(sink.clone());
            hyperactor_telemetry::register_sink(Box::new(sink));
        }

        Ok(scanner)
    }

    /// Flush any pending trace events to the tables.
    fn flush(&self) -> PyResult<()> {
        if let Some(ref sink) = self.sink {
            sink.flush()
                .map_err(|e| PyException::new_err(format!("failed to flush sink: {}", e)))?;
        }
        Ok(())
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

        // Get actor instance from context
        let actor_module = py.import("monarch.actor")?;
        let ctx = actor_module.call_method0("context")?;
        let actor_instance_obj = ctx.getattr("actor_instance")?;
        let actor_instance: PyObject = actor_instance_obj.clone().into_pyobject(py)?.unbind();

        // Build destination PortId
        let dest_port_id: PortId = dest.clone().into();

        // Execute scan, streaming batches directly to destination
        self.execute_scan_streaming(
            &table_name,
            projection,
            filter_expr,
            limit,
            &actor_instance,
            &dest_port_id,
        )
    }

}

/// Send a QueryResponse via PortRef, acquiring the GIL to get the actor context.
fn send_query_response(actor_instance: &PyObject, dest_port_id: &PortId, msg: QueryResponse) {
    Python::with_gil(|py| {
        if let Ok(instance) = actor_instance.extract::<PyRef<'_, PyInstance>>(py) {
            let dest_ref: PortRef<QueryResponse> = PortRef::attest(dest_port_id.clone());
            if let Err(e) = dest_ref.send(&**instance, msg) {
                tracing::debug!("send_query_response: send error: {:?}", e);
            }
        }
    })
}

impl DatabaseScanner {
    /// Internal method to push a RecordBatch to a table.
    ///
    /// Creates the table if it doesn't exist, using the batch's schema.
    /// If the batch is empty, creates the table with the schema but doesn't append.
    /// This method is used both by the Python push_batch and by the Rust RecordBatchSink.
    pub fn push_batch_internal(&self, table_name: &str, batch: RecordBatch) -> anyhow::Result<()> {
        Self::push_batch_to_tables(&self.table_data, self.max_batches, table_name, batch)
    }

    /// Static method to push a batch to the table_data map.
    /// This can be used from closures that capture the Arc.
    ///
    /// If the batch is empty, creates the table with the schema but doesn't append data.
    fn push_batch_to_tables(
        table_data: &Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
        max_batches: usize,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        // Get or create the table
        let table = {
            let mut guard = table_data
                .lock()
                .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
            guard
                .entry(table_name.to_string())
                .or_insert_with(|| Arc::new(LiveTableData::new(batch.schema(), max_batches)))
                .clone()
        };

        // Push the batch (push ignores empty batches)
        get_tokio_runtime().block_on(table.push(batch));
        Ok(())
    }

    /// Create a RecordBatchSink that pushes batches to this scanner's tables.
    ///
    /// The sink can be registered with hyperactor_telemetry::register_sink()
    /// to receive trace events and store them as queryable tables.
    pub fn create_record_batch_sink(&self, batch_size: usize) -> RecordBatchSink {
        let table_data = self.table_data.clone();
        let max_batches = self.max_batches;

        RecordBatchSink::new(
            batch_size,
            Box::new(move |table_name, batch| {
                if let Err(e) =
                    Self::push_batch_to_tables(&table_data, max_batches, table_name, batch)
                {
                    tracing::error!("Failed to push batch to table {}: {}", table_name, e);
                }
            }),
        )
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
        actor_instance: &PyObject,
        dest_port_id: &PortId,
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
                        let msg = QueryResponse { data };
                        send_query_response(actor_instance, dest_port_id, msg);
                        count += 1;
                    }
                }

                tracing::info!("Scanner {}: local scan complete, sent {} batches", rank, count);
                Ok::<usize, datafusion::error::DataFusionError>(count)
            })
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(batch_count)
    }
}

// ============================================================================
// QueryEngine - Rust struct that takes Python actor, runs DataFusion queries
// ============================================================================

struct DistributedTableProvider {
    table_name: String,
    schema: SchemaRef,
    actor: PyObject,
    mailbox: Arc<StdMutex<PyMailbox>>,
}

impl std::fmt::Debug for DistributedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedTableProvider")
            .field("table_name", &self.table_name)
            .finish()
    }
}

fn expr_to_sql_string(expr: &Expr) -> Option<String> {
    expr_to_sql(expr).ok().map(|sql| sql.to_string())
}

#[async_trait::async_trait]
impl TableProvider for DistributedTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|e| {
                if expr_to_sql_string(e).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let where_clauses: Vec<String> = filters.iter().filter_map(expr_to_sql_string).collect();
        let where_clause = if where_clauses.is_empty() {
            None
        } else {
            Some(where_clauses.join(" AND "))
        };

        let output_schema = match projection {
            Some(proj) => Arc::new(datafusion::arrow::datatypes::Schema::new(
                proj.iter()
                    .filter_map(|&i| self.schema.fields().get(i).cloned())
                    .collect::<Vec<_>>(),
            )),
            None => self.schema.clone(),
        };

        // Clone actor with GIL since we're in an async context
        let actor = Python::with_gil(|py| self.actor.clone_ref(py));

        Ok(Arc::new(DistributedExec {
            table_name: self.table_name.clone(),
            schema: output_schema.clone(),
            projection: projection.cloned(),
            where_clause,
            limit,
            actor,
            mailbox: self.mailbox.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(output_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        }))
    }
}

struct DistributedExec {
    table_name: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    where_clause: Option<String>,
    limit: Option<usize>,
    actor: PyObject,
    mailbox: Arc<StdMutex<PyMailbox>>,
    properties: PlanProperties,
}

impl std::fmt::Debug for DistributedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedExec")
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec: table={}", self.table_name)
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = self.schema.clone();

        // Open a port and start the distributed scan
        let (receiver, completion_future) = Python::with_gil(
            |py| -> anyhow::Result<(
                PortReceiver<QueryResponse>,
                std::pin::Pin<
                    Box<dyn std::future::Future<Output = PyResult<PyObject>> + Send + 'static>,
                >,
            )> {
                let mailbox = self
                    .mailbox
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
                let (handle, receiver) = mailbox.get_inner().open_port::<QueryResponse>();
                let dest_port_ref = handle.bind();
                let dest_port_id: PyPortId = dest_port_ref.port_id().clone().into();

                // Call actor.scan.call(dest, table, proj, limit, filter) to get a Future
                let scan = self.actor.getattr(py, "scan")?;
                let future_obj = scan.call_method1(
                    py,
                    "call",
                    (
                        dest_port_id,
                        self.table_name.clone(),
                        self.projection.clone(),
                        self.limit,
                        self.where_clause.clone(),
                    ),
                )?;

                // Extract the PythonTask from the Future object
                // Future._status is an _Unawaited(coro) where coro is a PythonTask
                let status = future_obj.getattr(py, "_status")?;
                // _Unawaited is a NamedTuple with .coro attribute
                let python_task_obj = status.getattr(py, "coro")?;
                let mut python_task: PyRefMut<'_, PyPythonTask> = python_task_obj.extract(py)?;
                let completion_future = python_task.take_task()?;

                Ok((receiver, completion_future))
            },
        )
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        Ok(create_draining_stream(schema, receiver, completion_future))
    }
}

#[pyclass(
    name = "QueryEngine",
    module = "monarch._rust_bindings.monarch_extension.distributed_telemetry"
)]
pub struct QueryEngine {
    session: Arc<StdMutex<Option<SessionContext>>>,
    actor: PyObject,
    mailbox: Arc<StdMutex<PyMailbox>>,
}

#[pymethods]
impl QueryEngine {
    /// Create a new QueryEngine.
    ///
    /// Args:
    ///     actor: A singleton DistributedTelemetryActor (ActorMesh) to query
    #[new]
    fn new(py: Python<'_>, actor: PyObject) -> PyResult<Self> {
        // Get mailbox from context (must be called from within an actor context)
        let actor_module = py.import("monarch.actor")?;
        let ctx = actor_module.call_method0("context")?;
        let actor_instance_obj = ctx.getattr("actor_instance")?;
        let mailbox_obj = actor_instance_obj.getattr("_mailbox")?;
        let mailbox: PyMailbox = mailbox_obj.extract()?;

        let engine = Self {
            session: Arc::new(StdMutex::new(None)),
            actor,
            mailbox: Arc::new(StdMutex::new(mailbox)),
        };
        engine.setup_tables(py)?;
        Ok(engine)
    }

    fn __repr__(&self) -> String {
        "<QueryEngine>".into()
    }

    /// Execute a SQL query and return results as Arrow IPC bytes.
    fn query<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyBytes>> {
        let session_guard = self
            .session
            .lock()
            .map_err(|_| PyException::new_err("lock poisoned"))?;
        let ctx = session_guard
            .as_ref()
            .ok_or_else(|| PyException::new_err("QueryEngine not initialized"))?
            .clone();
        drop(session_guard);

        // Release the GIL and run the async query on the shared monarch runtime.
        let results: Vec<RecordBatch> = py
            .allow_threads(|| {
                get_tokio_runtime().block_on(async {
                    let df = ctx.sql(&sql).await?;
                    df.collect().await
                })
            })
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // Serialize all results as a single Arrow IPC stream
        let schema = results
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        for batch in &results {
            writer
                .write(batch)
                .map_err(|e| PyException::new_err(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(PyBytes::new(py, &buf))
    }
}

impl QueryEngine {
    fn setup_tables(&self, py: Python<'_>) -> PyResult<()> {
        // Get table names from the actor mesh via endpoint call
        // table_names is an endpoint, so we get it then call .call().get().item()
        let tables: Vec<String> = self
            .actor
            .getattr(py, "table_names")?
            .call_method0(py, "call")?
            .call_method0(py, "get")?
            .call_method0(py, "item")?
            .extract(py)?;

        let config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);

        for table_name in &tables {
            // Get schema from actor via endpoint call
            let schema_bytes: Vec<u8> = self
                .actor
                .getattr(py, "schema_for")?
                .call_method1(py, "call", (table_name,))?
                .call_method0(py, "get")?
                .call_method0(py, "item")?
                .extract(py)?;

            let schema = deserialize_schema(&schema_bytes).map_err(|e| {
                PyException::new_err(format!("Failed to deserialize schema: {}", e))
            })?;

            let provider = DistributedTableProvider {
                table_name: table_name.clone(),
                schema,
                actor: self.actor.clone_ref(py),
                mailbox: self.mailbox.clone(),
            };

            ctx.register_table(table_name, Arc::new(provider))
                .map_err(|e| PyException::new_err(e.to_string()))?;
        }

        *self
            .session
            .lock()
            .map_err(|_| PyException::new_err("lock poisoned"))? = Some(ctx);
        Ok(())
    }
}

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
    module.add_class::<DatabaseScanner>()?;
    module.add_class::<QueryEngine>()?;
    module.add_function(wrap_pyfunction!(enable_record_batch_tracing, module)?)?;
    Ok(())
}
