/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! QueryEngine - DataFusion query execution and streaming results

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_pyarrow::PyArrowType;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::record_batch::RecordBatchReader;
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
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
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::expr_to_sql;
use futures::StreamExt;
use hyperactor::ActorAddr;
use hyperactor::Instance;
use hyperactor::config::MESSAGE_DELIVERY_TIMEOUT;
use hyperactor::context::Mailbox as MailboxTrait;
use hyperactor::mailbox::PortReceiver;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use monarch_gil::GilSite;
use monarch_gil::monarch_with_gil;
use monarch_gil::monarch_with_gil_blocking;
use monarch_hyperactor::actor::PythonActor;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::endpoint::ActorEndpoint;
use monarch_hyperactor::endpoint::ReplyFuture;
use monarch_hyperactor::mailbox::PyPortId;
use monarch_hyperactor::runtime::get_tokio_runtime;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use tokio::sync::mpsc;

use crate::QueryResponse;
use crate::pushdown::CollectorTables;
use crate::pushdown::PushDownCollectorAggregates;

declare_attrs! {
    /// Run the collector-local part of each telemetry aggregate on the
    /// collectors, which return partial results instead of rows.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_TELEMETRY_AGGREGATE_PUSHDOWN".to_string()),
        Some("telemetry_aggregate_pushdown".to_string()),
    ))
    pub attr TELEMETRY_AGGREGATE_PUSHDOWN: bool = true;
}

enum QueryStreamMessage {
    Batch(RecordBatch),
    Error(DataFusionError),
    Finished,
}

struct StreamingRecordBatchReader {
    schema: SchemaRef,
    receiver: mpsc::Receiver<QueryStreamMessage>,
    finished: bool,
}

impl Iterator for StreamingRecordBatchReader {
    type Item = ArrowResult<RecordBatch>;

    /// Return the next query batch, waiting until one is available.
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.receiver.blocking_recv() {
            Some(QueryStreamMessage::Batch(batch)) => Some(Ok(batch)),
            Some(QueryStreamMessage::Error(error)) => {
                self.finished = true;
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
            Some(QueryStreamMessage::Finished) => {
                self.finished = true;
                None
            }
            None => {
                self.finished = true;
                Some(Err(ArrowError::ExternalError(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "query stream ended without a completion message",
                    ),
                ))))
            }
        }
    }
}

impl RecordBatchReader for StreamingRecordBatchReader {
    /// Return the schema established before query execution begins streaming.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ============================================================================
// Deserialization helpers
// ============================================================================

fn deserialize_schema(data: &[u8]) -> anyhow::Result<SchemaRef> {
    let reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    Ok(reader.schema())
}

fn deserialize_batch(data: &[u8]) -> anyhow::Result<RecordBatch> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    reader
        .next()
        .ok_or_else(|| anyhow::anyhow!("scan response carried no batch"))?
        .map_err(Into::into)
}

// ============================================================================
// Distributed scan
// ============================================================================

type BatchSender = mpsc::UnboundedSender<DFResult<RecordBatch>>;

/// Forwards one distributed scan's batches to its output stream.
///
/// A collector knows how many batches it posted only once its scan finishes,
/// and the root posts every collector's count in one [`QueryResponse::Counts`],
/// so the stream ends once every collector in it has delivered its count.
/// Batches from collectors the counts leave out (timed out or failed) are
/// dropped from then on: a late collector can neither stand in for batches
/// another collector still owes nor add rows after the stream ends.
///
/// Otherwise, a batch that cannot be read, or one more batch than a collector
/// reported, fails the query: either way the result no longer matches what the
/// collectors scanned. Malformed counts fail it too rather than panicking: a
/// panic would only end this task, which the reader sees as a normal end of
/// stream.
struct ScanTracker {
    sender: Option<BatchSender>,
    counts: Counts,
}

/// Per-collector batch counts, before and after the root's counts arrive.
enum Counts {
    /// Batches received from each collector so far.
    Received(HashMap<ActorAddr, usize>),
    /// Batches each collector in the root's counts still owes.
    Outstanding(HashMap<ActorAddr, usize>),
}

impl ScanTracker {
    fn new(sender: BatchSender) -> Self {
        Self {
            sender: Some(sender),
            counts: Counts::Received(HashMap::new()),
        }
    }

    fn on_batch(&mut self, source: &ActorAddr, batch: DFResult<RecordBatch>) {
        let batch = match (&self.counts, batch) {
            // The root's counts left this collector out: it timed out or failed.
            (Counts::Outstanding(outstanding), _) if !outstanding.contains_key(source) => return,
            (_, Err(error)) => {
                self.fail(&format!("unreadable batch from {source}: {error}"));
                return;
            }
            (_, Ok(batch)) => batch,
        };

        match &mut self.counts {
            Counts::Received(received) => *received.entry(source.clone()).or_default() += 1,
            Counts::Outstanding(outstanding) => {
                let remaining = outstanding
                    .get_mut(source)
                    .expect("should be outstanding: sources left out of the counts return above");
                if *remaining == 0 {
                    self.fail(&format!("{source} sent more batches than it reported"));
                    return;
                }
                *remaining -= 1;
            }
        }

        if let Some(sender) = &self.sender {
            // A dropped receiver means the query stopped reading; keep
            // draining the port until the scan completes.
            let _ = sender.send(Ok(batch));
        }
        self.close_if_finished();
    }

    fn on_counts(&mut self, batch_counts: Vec<(ActorAddr, usize)>) {
        let Counts::Received(received) = &self.counts else {
            self.fail("received the root's counts twice");
            return;
        };
        let mut outstanding = HashMap::new();
        for (source, count) in batch_counts {
            let delivered = received.get(&source).copied().unwrap_or(0);
            let Some(remaining) = count.checked_sub(delivered) else {
                self.fail(&format!(
                    "{source} sent {delivered} batches but reported {count}"
                ));
                return;
            };
            if outstanding.contains_key(&source) {
                self.fail(&format!("the counts list {source} more than once"));
                return;
            }
            outstanding.insert(source, remaining);
        }
        self.counts = Counts::Outstanding(outstanding);
        self.close_if_finished();
    }

    /// Fail the scan because the query root's counts, or the batches they
    /// report, did not arrive within `timeout`.
    fn expire(&mut self, timeout: Duration) {
        let message = match &self.counts {
            Counts::Received(_) => {
                format!("batch counts not delivered within {timeout:?} of the scan reply")
            }
            Counts::Outstanding(outstanding) => {
                let owed = outstanding
                    .iter()
                    .filter(|(_, remaining)| **remaining > 0)
                    .map(|(source, remaining)| format!("{source} owes {remaining}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("batches not delivered within {timeout:?} of the scan reply: {owed}")
            }
        };
        tracing::error!("distributed scan: {message}");
        self.fail(&message);
    }

    fn fail(&mut self, message: &str) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(DataFusionError::External(message.into())));
        }
    }

    fn close_if_finished(&mut self) {
        if let Counts::Outstanding(outstanding) = &self.counts
            && outstanding.values().all(|&remaining| remaining == 0)
        {
            self.sender = None;
        }
    }

    fn is_finished(&self) -> bool {
        self.sender.is_none()
    }
}

/// Spawns a task that forwards the scan's batches from `port_receiver` until
/// every collector in the root's [`QueryResponse::Counts`] has delivered the
/// batches it reported. `completion_future` resolves when the root's scan
/// endpoint returns, and fails the scan if the endpoint failed. If the counts
/// or their batches have not all arrived within twice
/// [`MESSAGE_DELIVERY_TIMEOUT`] of the reply, the scan fails.
///
/// Returns a stream that reads from the task.
fn create_draining_stream<F>(
    schema: SchemaRef,
    port_receiver: PortReceiver<QueryResponse>,
    completion_future: F,
) -> SendableRecordBatchStream
where
    F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let (sender, receiver) = mpsc::unbounded_channel();
    let mut tracker = ScanTracker::new(sender);
    // The root posts its counts, and collectors their batches, before replying,
    // but the delivery timeout bounds each transport hop, not the whole path
    // through host gateways.
    let delivery_timeout = hyperactor_config::global::get(MESSAGE_DELIVERY_TIMEOUT) * 2;

    get_tokio_runtime().spawn(async move {
        let mut port_receiver = port_receiver;
        tokio::pin!(completion_future);
        let mut replied = false;
        // Armed when the reply arrives; the root posts its counts before
        // replying. Until then this branch is disabled.
        let delivery_deadline = tokio::time::sleep(delivery_timeout);
        tokio::pin!(delivery_deadline);

        while !tracker.is_finished() {
            tokio::select! {
                biased;

                result = &mut completion_future, if !replied => {
                    replied = true;
                    delivery_deadline
                        .as_mut()
                        .reset(tokio::time::Instant::now() + delivery_timeout);
                    if let Err(error) = result {
                        tracing::error!("distributed scan failed: {error}");
                        tracker.fail(&format!("scan failed: {error}"));
                    }
                }

                message = port_receiver.recv() => match message {
                    Ok(QueryResponse::Batch { source, data }) => {
                        let batch = deserialize_batch(&data.into_bytes())
                            .map_err(|error| DataFusionError::External(error.into()));
                        tracker.on_batch(&source, batch);
                    }
                    Ok(QueryResponse::Counts(batch_counts)) => tracker.on_counts(batch_counts),
                    Err(error) => {
                        tracing::error!("distributed scan: error receiving: {error:?}");
                        tracker.fail(&format!("error receiving: {error:?}"));
                    }
                },

                // Last, so batches already queued on the port count first.
                () = &mut delivery_deadline, if replied => tracker.expire(delivery_timeout),
            }
        }
    });

    let stream = futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Upper bound on the partitions a distributed query runs on.
///
/// Each distributed scan arrives as one partition. DataFusion repartitions it
/// so joins and aggregates over millions of rows run in parallel; past this
/// bound, extra partitions mostly add repartitioning overhead and take
/// workers from the shared control-plane runtime.
const MAX_QUERY_PARTITIONS: usize = 16;

fn query_target_partitions() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |cores| cores.get())
        .min(MAX_QUERY_PARTITIONS)
}

struct DistributedTableProvider {
    table_name: String,
    schema: SchemaRef,
    actor: Py<PyAny>,
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

/// The collectors a query engine scans.
struct Collectors {
    actor: Py<PyAny>,
    instance: Instance<PythonActor>,
}

impl std::fmt::Debug for Collectors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Collectors").finish()
    }
}

impl CollectorTables for Collectors {
    fn is_collector_table(&self, provider: &dyn TableProvider) -> bool {
        provider.as_any().is::<DistributedTableProvider>()
    }

    fn sql_table(&self, sql: String, schema: SchemaRef) -> Arc<dyn TableProvider> {
        let (actor, instance) = monarch_with_gil_blocking(GilSite::Convert, |py| {
            (self.actor.clone_ref(py), self.instance.clone_for_py())
        });
        Arc::new(CollectorSqlTable {
            sql,
            schema,
            actor,
            instance,
        })
    }
}

/// The concatenated results of a query run on every collector.
struct CollectorSqlTable {
    sql: String,
    schema: SchemaRef,
    actor: Py<PyAny>,
    instance: Instance<PythonActor>,
}

impl std::fmt::Debug for CollectorSqlTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectorSqlTable")
            .field("sql", &self.sql)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for CollectorSqlTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        collector_scan(
            &self.actor,
            &self.instance,
            &format!("({}) AS pushed", self.sql),
            &self.schema,
            projection,
            None,
            limit,
        )
    }
}

/// The column a collector returns for a projection of no columns.
const ROWS_COLUMN: &str = "fake_column";

/// Quote `name` as a SQL identifier.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The query each collector runs for a scan of `from`, and the schema of its
/// rows. Collectors answer a projection of no columns with one null column,
/// which carries the row count.
fn scan_sql(
    from: &str,
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    where_clause: Option<&str>,
    limit: Option<usize>,
) -> DFResult<(String, SchemaRef)> {
    let (columns, schema) = match projection {
        Some(projection) if projection.is_empty() => (
            format!("NULL AS {ROWS_COLUMN}"),
            Arc::new(Schema::new(vec![Field::new(
                ROWS_COLUMN,
                DataType::Null,
                true,
            )])),
        ),
        Some(projection) => {
            let schema = Arc::new(schema.project(projection)?);
            let columns: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| quote_identifier(field.name()))
                .collect();
            (columns.join(", "), schema)
        }
        None => ("*".to_string(), schema.clone()),
    };
    let mut sql = format!("SELECT {columns} FROM {from}");
    if let Some(where_clause) = where_clause {
        sql.push_str(&format!(" WHERE {where_clause}"));
    }
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    Ok((sql, schema))
}

/// Plan a scan of `from` that runs on every collector and streams the rows to
/// this root.
fn collector_scan(
    actor: &Py<PyAny>,
    instance: &Instance<PythonActor>,
    from: &str,
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    where_clause: Option<&str>,
    limit: Option<usize>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let (sql, schema) = scan_sql(from, schema, projection, where_clause, limit)?;
    let (actor, instance) = monarch_with_gil_blocking(GilSite::Convert, |py| {
        (actor.clone_ref(py), instance.clone_for_py())
    });
    let exec: Arc<dyn ExecutionPlan> = Arc::new(DistributedExec {
        sql,
        schema: schema.clone(),
        actor,
        instance,
        properties: PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ),
    });
    if projection.is_some_and(|projection| projection.is_empty()) {
        return Ok(Arc::new(ProjectionExec::try_new(
            Vec::<ProjectionExpr>::new(),
            exec,
        )?));
    }
    Ok(exec)
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
        let where_clauses: Vec<String> = filters
            .iter()
            .filter_map(expr_to_sql_string)
            .map(|filter| format!("({filter})"))
            .collect();
        let where_clause = if where_clauses.is_empty() {
            None
        } else {
            Some(where_clauses.join(" AND "))
        };

        collector_scan(
            &self.actor,
            &self.instance,
            &quote_identifier(&self.table_name),
            &self.schema,
            projection,
            where_clause.as_deref(),
            limit,
        )
    }
}

struct DistributedExec {
    /// The query each collector runs over its own tables.
    sql: String,
    schema: SchemaRef,
    actor: Py<PyAny>,
    instance: Instance<PythonActor>,
    properties: PlanProperties,
}

impl std::fmt::Debug for DistributedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedExec")
            .field("sql", &self.sql)
            .finish()
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec: sql={}", self.sql)
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
        let reply = monarch_with_gil_blocking(
            GilSite::EndpointDispatch,
            |py| -> anyhow::Result<ReplyFuture> {
                let dest_port_id: PyPortId = dest_port_ref.port_addr().clone().into();

                // Send to every scan actor now. The draining task below awaits
                // the reply alongside the batch receiver.
                let scan = self
                    .actor
                    .bind(py)
                    .getattr("query_scan")?
                    .cast_into::<ActorEndpoint>()
                    .map_err(PyErr::from)?;
                let args = (dest_port_id, self.sql.clone()).into_pyobject(py)?;
                Ok(scan.borrow().call_future(py, &args, None)?)
            },
        )
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        // The root posts its counts to the port, so the reply only reports
        // whether its scan endpoint failed.
        let completion_future = async move {
            match reply.await {
                Ok(_) => Ok(()),
                Err(error) => Err(anyhow::anyhow!(
                    monarch_with_gil(GilSite::Convert, |_| error.to_string()).await
                )),
            }
        };

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
    /// Create a DataFusion engine for local Arrow tables, without actor RPCs.
    #[staticmethod]
    fn local() -> Self {
        Self {
            session: SessionContext::new_with_config(
                SessionConfig::new()
                    .with_information_schema(true)
                    .with_target_partitions(1),
            ),
        }
    }

    /// Register or replace a local table from an Arrow IPC stream.
    fn register_local_table(&self, name: String, arrow_ipc_bytes: Vec<u8>) -> PyResult<()> {
        let reader = StreamReader::try_new(std::io::Cursor::new(arrow_ipc_bytes), None)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        let schema = reader.schema();
        let batches = reader
            .collect::<ArrowResult<Vec<_>>>()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| PyException::new_err(e.to_string()))?;
        self.session
            .deregister_table(&name)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        self.session
            .register_table(name, Arc::new(table))
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }

    /// Create a new QueryEngine.
    ///
    /// Args:
    ///     actor: A singleton TelemetryActor actor mesh to query
    #[new]
    fn new(py: Python<'_>, actor: Py<PyAny>) -> PyResult<Self> {
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

    /// Execute SQL and return an Arrow reader that produces batches on demand.
    /// Dropping the reader cancels the remaining query work.
    fn query(
        &self,
        py: Python<'_>,
        sql: String,
    ) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let session_ctx = self.session.clone();
        let (schema, mut stream) = py
            .detach(|| -> DFResult<(SchemaRef, SendableRecordBatchStream)> {
                get_tokio_runtime().block_on(async {
                    let df = session_ctx.sql(&sql).await?;
                    let schema = Arc::clone(df.schema().inner());
                    let stream = df.execute_stream().await?;
                    Ok((schema, stream))
                })
            })
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let (sender, receiver) = mpsc::channel(2);

        get_tokio_runtime().spawn(async move {
            loop {
                tokio::select! {
                    _ = sender.closed() => break,
                    batch = stream.next() => match batch {
                        Some(Ok(batch)) => {
                            if sender.send(QueryStreamMessage::Batch(batch)).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(error)) => {
                            let _ = sender.send(QueryStreamMessage::Error(error)).await;
                            break;
                        }
                        None => {
                            let _ = sender.send(QueryStreamMessage::Finished).await;
                            break;
                        }
                    }
                }
            }
        });

        let reader = StreamingRecordBatchReader {
            schema,
            receiver,
            finished: false,
        };
        Ok(PyArrowType(Box::new(reader)))
    }
}

impl QueryEngine {
    fn setup_tables(
        py: Python<'_>,
        actor: &Py<PyAny>,
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

        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_target_partitions(query_target_partitions());
        let mut state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();
        if hyperactor_config::global::get(TELEMETRY_AGGREGATE_PUSHDOWN) {
            state = state.with_optimizer_rule(Arc::new(PushDownCollectorAggregates::new(
                Arc::new(Collectors {
                    actor: actor.clone_ref(py),
                    instance: instance.clone_for_py(),
                }),
            )));
        }
        let ctx = SessionContext::new_with_state(state.build());

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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::compute::concat_batches;
    use datafusion::datasource::MemTable;
    use hyperactor::ProcAddr;
    use hyperactor::channel::ChannelAddr;

    use super::*;

    /// A fresh collector address; each call returns a distinct one.
    fn collector(name: &str) -> ActorAddr {
        ProcAddr::instance(ChannelAddr::Local(0), name).actor_addr(name)
    }

    fn unreadable() -> DFResult<RecordBatch> {
        Err(DataFusionError::External("bad batch".into()))
    }

    /// Assert that the stream ends with an error after the batches before it.
    fn assert_failed(receiver: &mut mpsc::UnboundedReceiver<DFResult<RecordBatch>>) {
        while let Ok(item) = receiver.try_recv() {
            if item.is_err() {
                assert!(
                    matches!(
                        receiver.try_recv(),
                        Err(mpsc::error::TryRecvError::Disconnected)
                    ),
                    "the error should end the stream"
                );
                return;
            }
        }
        panic!("the stream should have ended with an error");
    }

    fn batch(value: i64) -> DFResult<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        Ok(RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap())
    }

    fn tracker() -> (ScanTracker, mpsc::UnboundedReceiver<DFResult<RecordBatch>>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (ScanTracker::new(sender), receiver)
    }

    /// Drain the stream, returning its values if it has ended, or `None` if
    /// it is still open.
    fn drain(receiver: &mut mpsc::UnboundedReceiver<DFResult<RecordBatch>>) -> Option<Vec<i64>> {
        let mut values = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(batch) => values.push(
                    batch
                        .unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                ),
                Err(mpsc::error::TryRecvError::Disconnected) => return Some(values),
                Err(mpsc::error::TryRecvError::Empty) => return None,
            }
        }
    }

    #[test]
    fn test_tracker_waits_for_every_reported_count() {
        let (mut tracker, mut receiver) = tracker();
        let (a, b) = (collector("a"), collector("b"));

        tracker.on_batch(&b, batch(1));
        tracker.on_counts(vec![(a.clone(), 1), (b.clone(), 2)]);
        tracker.on_batch(&b, batch(2));
        assert!(!receiver.is_closed(), "a still owes its one batch");

        tracker.on_batch(&a, batch(3));
        assert_eq!(drain(&mut receiver), Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_tracker_late_collector_cannot_stand_in_for_another() {
        // The old protocol waited for a total batch count, so a late batch from
        // a timed-out collector could stand in for one still owed by another.
        let (mut tracker, mut receiver) = tracker();
        let (slow, root) = (collector("slow"), collector("root"));

        tracker.on_batch(&slow, batch(98));
        tracker.on_counts(vec![(root.clone(), 2)]);
        tracker.on_batch(&slow, batch(99));
        tracker.on_batch(&slow, unreadable());
        tracker.on_batch(&root, batch(1));
        assert!(!receiver.is_closed(), "root still owes a batch");

        tracker.on_batch(&root, batch(2));
        // A batch that arrived before the counts stays, as best-effort
        // coverage; anything after the counts left `slow` out is dropped,
        // unreadable or not.
        assert_eq!(drain(&mut receiver), Some(vec![98, 1, 2]));
    }

    #[test]
    fn test_tracker_unreadable_batch_fails_without_counting() {
        let (mut tracker, mut receiver) = tracker();
        let root = collector("root");

        tracker.on_counts(vec![(root.clone(), 2)]);
        tracker.on_batch(&root, batch(1));
        tracker.on_batch(&root, unreadable());

        assert_failed(&mut receiver);
    }

    #[test]
    fn test_tracker_unreadable_batch_before_counts_fails() {
        let (mut tracker, mut receiver) = tracker();

        tracker.on_batch(&collector("root"), unreadable());

        assert_failed(&mut receiver);
    }

    #[test]
    fn test_tracker_extra_batch_after_counts_fails() {
        let (mut tracker, mut receiver) = tracker();
        let (a, b) = (collector("a"), collector("b"));

        tracker.on_counts(vec![(a.clone(), 1), (b.clone(), 1)]);
        tracker.on_batch(&a, batch(1));
        tracker.on_batch(&a, batch(2));

        assert_failed(&mut receiver);
    }

    #[test]
    fn test_tracker_extra_batch_before_counts_fails() {
        let (mut tracker, mut receiver) = tracker();
        let root = collector("root");

        tracker.on_batch(&root, batch(1));
        tracker.on_batch(&root, batch(2));
        tracker.on_counts(vec![(root.clone(), 1)]);

        assert_failed(&mut receiver);
    }

    #[test]
    fn test_tracker_expire_names_collectors_still_owing() {
        let (mut tracker, mut receiver) = tracker();
        let (a, b) = (collector("a"), collector("b"));

        tracker.on_counts(vec![(a.clone(), 1), (b.clone(), 2)]);
        tracker.on_batch(&a, batch(1));
        tracker.on_batch(&b, batch(2));
        tracker.expire(Duration::from_secs(30));

        assert!(tracker.is_finished());
        let message = std::iter::from_fn(|| receiver.try_recv().ok())
            .find_map(Result::err)
            .expect("the stream should end with an error")
            .to_string();
        assert!(message.contains(&format!("{b} owes 1")), "{message}");
        assert!(
            !message.contains(&a.to_string()),
            "a delivered everything: {message}"
        );
    }

    #[test]
    fn test_tracker_expire_without_counts_says_so() {
        let (mut tracker, mut receiver) = tracker();

        tracker.on_batch(&collector("root"), batch(1));
        tracker.expire(Duration::from_secs(30));

        assert!(tracker.is_finished());
        let message = std::iter::from_fn(|| receiver.try_recv().ok())
            .find_map(Result::err)
            .expect("the stream should end with an error")
            .to_string();
        assert!(message.contains("batch counts not delivered"), "{message}");
    }

    #[test]
    fn test_tracker_failure_ends_stream_with_an_error() {
        let (mut tracker, mut receiver) = tracker();

        tracker.fail("scan failed");

        assert_failed(&mut receiver);
    }

    #[test]
    fn test_tracker_empty_counts_end_stream() {
        let (mut tracker, mut receiver) = tracker();

        tracker.on_batch(&collector("slow"), batch(1));
        tracker.on_counts(vec![]);

        assert_eq!(drain(&mut receiver), Some(vec![1]));
    }

    #[test]
    fn test_tracker_duplicate_collector_in_counts_fails() {
        let (mut tracker, mut receiver) = tracker();
        let root = collector("root");

        tracker.on_counts(vec![(root.clone(), 1), (root, 1)]);

        assert_failed(&mut receiver);
    }

    fn scan_test_context() -> (SessionContext, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        (ctx, schema)
    }

    async fn run_scan_sql(
        projection: Option<&Vec<usize>>,
        where_clause: Option<&str>,
        limit: Option<usize>,
    ) -> (SchemaRef, Vec<RecordBatch>) {
        let (ctx, schema) = scan_test_context();
        let (sql, schema) = scan_sql(
            &quote_identifier("t"),
            &schema,
            projection,
            where_clause,
            limit,
        )
        .unwrap();
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        for batch in &batches {
            assert_eq!(batch.schema(), schema, "{sql}");
        }
        (schema, batches)
    }

    #[tokio::test]
    async fn test_scan_sql_applies_filter_projection_and_limit() {
        let (schema, batches) = run_scan_sql(Some(&vec![1]), Some("(x >= 2)"), Some(2)).await;
        let batch = concat_batches(&schema, &batches).unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[20, 30]);
    }

    #[tokio::test]
    async fn test_scan_sql_of_no_columns_returns_the_rows() {
        let (_, batches) = run_scan_sql(Some(&vec![]), Some("(x >= 2)"), None).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[test]
    fn test_scan_sql_rejects_out_of_range_projection() {
        let (_, schema) = scan_test_context();
        assert!(scan_sql("t", &schema, Some(&vec![2]), None, None).is_err());
    }

    #[test]
    fn test_quote_identifier_escapes_quotes() {
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }
}
