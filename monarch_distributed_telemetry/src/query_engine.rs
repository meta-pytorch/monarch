/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! QueryEngine - DataFusion query execution, creates ports, collects results

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
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
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::expr_to_sql;
use hyperactor::Instance;
use hyperactor::context::Mailbox as MailboxTrait;
use hyperactor::mailbox::PortReceiver;
use monarch_hyperactor::actor::PythonActor;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::mailbox::PyPortId;
use monarch_hyperactor::pytokio::PyPythonTask;
use monarch_hyperactor::runtime::get_tokio_runtime;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyModule;
use tokio::sync::mpsc;

use crate::QueryResponse;

// ============================================================================
// Deserialization helpers
// ============================================================================

fn deserialize_schema(data: &[u8]) -> anyhow::Result<SchemaRef> {
    let reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    Ok(reader.schema())
}

fn deserialize_batch(data: &[u8]) -> anyhow::Result<Option<RecordBatch>> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    Ok(reader.next().transpose()?)
}

// ============================================================================
// Helper to spawn reader task that drains PortReceiver into a channel
// ============================================================================

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
                                if let Ok(iter) = bound.try_iter() {
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
                            match deserialize_batch(&data.into_bytes()) {
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

struct DistributedTableProvider {
    table_name: String,
    schema: SchemaRef,
    actor: PyObject,
    /// Actor instance for creating ports
    instance: Instance<PythonActor>,
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

        // Clone actor and instance for the execution plan
        let (actor, instance) =
            Python::with_gil(|py| (self.actor.clone_ref(py), self.instance.clone_for_py()));

        Ok(Arc::new(DistributedExec {
            table_name: self.table_name.clone(),
            schema: output_schema.clone(),
            projection: projection.cloned(),
            where_clause,
            limit,
            actor,
            instance,
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
    instance: Instance<PythonActor>,
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

        // Open port using the instance captured at scan time
        let (handle, receiver) = self.instance.mailbox().open_port::<QueryResponse>();
        let dest_port_ref = handle.bind();

        // Start the distributed scan
        let completion_future = Python::with_gil(
            |py| -> anyhow::Result<
                std::pin::Pin<
                    Box<dyn std::future::Future<Output = PyResult<PyObject>> + Send + 'static>,
                >,
            > {
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

                Ok(completion_future)
            },
        )
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        Ok(create_draining_stream(schema, receiver, completion_future))
    }
}

#[pyclass(
    name = "QueryEngine",
    module = "monarch._rust_bindings.monarch_distributed_telemetry.query_engine"
)]
pub struct QueryEngine {
    session: SessionContext,
}

#[pymethods]
impl QueryEngine {
    /// Create a new QueryEngine.
    ///
    /// Args:
    ///     actor: A singleton DistributedTelemetryActor (ActorMesh) to query
    #[new]
    fn new(py: Python<'_>, actor: PyObject) -> PyResult<Self> {
        // Get actor instance from current Python context
        let actor_module = py.import("monarch.actor")?;
        let ctx = actor_module.call_method0("context")?;
        let actor_instance_obj = ctx.getattr("actor_instance")?;
        let py_instance: PyRef<'_, PyInstance> = actor_instance_obj.extract()?;
        let instance: Instance<PythonActor> = py_instance.clone_for_py();

        let session = Self::setup_tables(py, &actor, instance)?;
        Ok(Self { session })
    }

    fn __repr__(&self) -> String {
        "<QueryEngine>".into()
    }

    /// Execute a SQL query and return results as Arrow IPC bytes.
    fn query<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyBytes>> {
        let session_ctx = self.session.clone();

        // Release the GIL and run the async query on the shared monarch runtime.
        let results: Vec<RecordBatch> = py
            .allow_threads(|| {
                get_tokio_runtime().block_on(async {
                    let df = session_ctx.sql(&sql).await?;
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
    fn setup_tables(
        py: Python<'_>,
        actor: &PyObject,
        instance: Instance<PythonActor>,
    ) -> PyResult<SessionContext> {
        // Get table names from the actor mesh via endpoint call
        // table_names is an endpoint, so we get it then call .call().get().item()
        let tables: Vec<String> = actor
            .getattr(py, "table_names")?
            .call_method0(py, "call")?
            .call_method0(py, "get")?
            .call_method0(py, "item")?
            .extract(py)?;

        let config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);

        for table_name in &tables {
            // Get schema from actor via endpoint call
            let schema_bytes: Vec<u8> = actor
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
                actor: actor.clone_ref(py),
                instance: instance.clone_for_py(),
            };

            ctx.register_table(table_name, Arc::new(provider))
                .map_err(|e| PyException::new_err(e.to_string()))?;
        }

        Ok(ctx)
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<QueryEngine>()?;
    Ok(())
}
