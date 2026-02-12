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
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use hyperactor::Instance;
use hyperactor::PortId;
use hyperactor::PortRef;
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
    module = "monarch._rust_bindings.monarch_distributed_telemetry.database_scanner"
)]
pub struct DatabaseScanner {
    /// Tables stored by name - each holds the schema and shared PartitionData
    table_data: Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
    rank: usize,
    max_batches: usize,
    /// Handle to flush the RecordBatchSink for trace events (spans, events)
    sink: Option<RecordBatchSink>,
    /// Handle to flush the EntityDispatcher for entity events (actors, meshes)
    dispatcher: Option<EntityDispatcher>,
}

fn fill_fake_batches(scanner: &DatabaseScanner) -> anyhow::Result<()> {
    use rand::Rng;

    let mut rng = rand::rng();

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
    let host_start = rng.random_range(0..10000) * 10;
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
        host_ids.push(host_id);
        hostnames.push(format!("server-{:05}", host_id));
        dcs.push(datacenters[rng.random_range(0..datacenters.len())].to_string());
        oses.push(os_types[rng.random_range(0..os_types.len())].to_string());
        cpus.push(cpu_options[rng.random_range(0..cpu_options.len())] as i32);
        mems.push(memory_options[rng.random_range(0..memory_options.len())]);
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

        let host_id = rng.random_range(host_start..host_end);
        metric_host_ids.push(host_id);

        let metric_name = metric_names[rng.random_range(0..metric_names.len())];
        metric_names_col.push(metric_name.to_string());

        let value = match metric_name {
            "cpu_usage" | "memory_usage" => rng.random_range(0.0..100.0),
            "disk_io" => rng.random_range(0.0..1000.0),
            _ => rng.random_range(0.0..10000.0),
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

#[pymethods]
impl DatabaseScanner {
    #[new]
    #[pyo3(signature = (rank, use_fake_data=true, max_batches=100, batch_size=1000))]
    fn new(
        rank: usize,
        use_fake_data: bool,
        max_batches: usize,
        batch_size: usize,
    ) -> PyResult<Self> {
        let mut scanner = Self {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            rank,
            max_batches,
            sink: None,
            dispatcher: None,
        };

        if use_fake_data {
            fill_fake_batches(&scanner)
                .map_err(|e| PyException::new_err(format!("failed to create fake data: {}", e)))?;
        } else {
            // Create and register a RecordBatchSink for trace events (spans, events)
            let sink = scanner.create_record_batch_sink(batch_size);
            scanner.sink = Some(sink.clone());
            hyperactor_telemetry::register_sink(Box::new(sink));

            // Create and register an EntityDispatcher for entity events (actors, meshes)
            let dispatcher = scanner.create_entity_dispatcher(batch_size);
            scanner.dispatcher = Some(dispatcher.clone());
            hyperactor_telemetry::set_entity_dispatcher(Box::new(dispatcher));
        }

        Ok(scanner)
    }

    /// Flush any pending trace events and entity events to the tables.
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

        // Get actor instance from context and extract the Rust Instance once
        let actor_module = py.import("monarch.actor")?;
        let ctx = actor_module.call_method0("context")?;
        let actor_instance_obj = ctx.getattr("actor_instance")?;
        let py_instance: PyRef<'_, PyInstance> = actor_instance_obj.extract()?;
        let instance: Instance<PythonActor> = py_instance.clone_for_py();

        // Build destination PortRef once
        let dest_port_id: PortId = dest.clone().into();
        let dest_ref: PortRef<QueryResponse> = PortRef::attest(dest_port_id);

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

    /// Create an EntityDispatcher that pushes batches to this scanner's tables.
    ///
    /// The dispatcher can be registered with hyperactor_telemetry::set_entity_dispatcher()
    /// to receive entity events (actors, meshes) and store them as queryable tables.
    pub fn create_entity_dispatcher(&self, batch_size: usize) -> EntityDispatcher {
        let table_data = self.table_data.clone();
        let max_batches = self.max_batches;

        EntityDispatcher::new(
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
        instance: &Instance<PythonActor>,
        dest_ref: &PortRef<QueryResponse>,
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
