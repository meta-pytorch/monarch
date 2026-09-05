/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! DatabaseScanner - Local MemTable operations, scans with child stream merging

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;
use hyperactor as reference;
use hyperactor::Endpoint as _;
use hyperactor::Instance;
use monarch_hyperactor::actor::PythonActor;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::mailbox::PyPortId;
use monarch_hyperactor::runtime::get_tokio_runtime;
use monarch_record_batch::RecordBatchBuffer;
use monarch_telemetry_schema::deserialize_one_batch;
use monarch_telemetry_schema::entity_tables::MESSAGE_STATUS_EVENTS;
use monarch_telemetry_schema::entity_tables::MESSAGES;
use monarch_telemetry_schema::entity_tables::SENT_MESSAGES;
use monarch_telemetry_schema::metric_tables::METRIC_GAUGES;
use monarch_telemetry_schema::metric_tables::METRIC_HISTOGRAMS;
use monarch_telemetry_schema::metric_tables::METRIC_SUMS;
use monarch_telemetry_schema::trace_tables::EVENTS;
use monarch_telemetry_schema::trace_tables::SPAN_EVENTS;
use monarch_telemetry_schema::trace_tables::SPANS;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyModule;
use serde_multipart::Part;
use tokio::task::AbortHandle;

use crate::QueryResponse;
use crate::pyspy_table::PySpyDumpBuffer;
use crate::pyspy_table::PySpyFrameBuffer;
use crate::pyspy_table::PySpyLocalVariableBuffer;
use crate::pyspy_table::PySpyStackTraceBuffer;
use crate::serialize_batch;
use crate::serialize_schema;
use crate::timestamp_to_micros;

/// Target rows per stored batch.
///
/// Ingest appends one `RecordBatch` per write, so without compaction a table
/// accumulates one small batch per event indefinitely. Batch count -- not row
/// count -- drives the cost of everything downstream: DataFusion plans over the
/// partition, the retention rewrite walks it, and each batch a scan emits is a
/// separate Arrow IPC serialization and hyperactor message. Merging the
/// trailing run of small batches once it reaches this many rows keeps the
/// partition proportional to rows stored rather than to writes performed.
/// A row trigger alone bounds the uncompacted tail: `push` rejects empty
/// batches, so the pending run never exceeds this many batches either.
const COMPACT_TARGET_ROWS: usize = 8192;

/// Wraps a table's data so we can dynamically push new batches.
/// The MemTable is created on initialization and shared with queries.
pub struct LiveTableData {
    /// The MemTable that queries use
    mem_table: Arc<MemTable>,
    /// Batches appended since the last compaction, and their total rows.
    ///
    /// Only ever read or written while holding the partition write lock, which
    /// is what keeps them consistent with the partition itself.
    pending_batches: AtomicUsize,
    pending_rows: AtomicUsize,
}

impl LiveTableData {
    fn new(schema: SchemaRef) -> Self {
        let mem_table = MemTable::try_new(schema, vec![vec![]])
            .expect("failed to create MemTable with empty partition");
        Self {
            mem_table: Arc::new(mem_table),
            pending_batches: AtomicUsize::new(0),
            pending_rows: AtomicUsize::new(0),
        }
    }

    /// Push a new batch to the table, compacting the trailing run when it grows
    /// large enough to be worth merging.
    pub async fn push(&self, batch: RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }

        let partition = &self.mem_table.batches[0];
        let mut guard = partition.write().await;
        let rows = batch.num_rows();
        guard.push(batch);

        let pending = self.pending_batches.fetch_add(1, Ordering::Relaxed) + 1;
        let pending_rows = self.pending_rows.fetch_add(rows, Ordering::Relaxed) + rows;
        if pending_rows >= COMPACT_TARGET_ROWS {
            self.compact_tail(&mut guard, pending);
        }
    }

    /// Merge the last `tail_len` batches of the partition into one.
    ///
    /// Resets the pending counters whether or not the merge happens, so a batch
    /// that cannot be concatenated does not wedge compaction permanently.
    fn compact_tail(&self, partition: &mut Vec<RecordBatch>, tail_len: usize) {
        self.pending_batches.store(0, Ordering::Relaxed);
        self.pending_rows.store(0, Ordering::Relaxed);

        let start = partition.len().saturating_sub(tail_len);
        if partition.len() - start < 2 {
            return;
        }

        let schema = partition[start].schema();
        match concat_batches(&schema, partition[start..].iter()) {
            Ok(merged) => {
                partition.truncate(start);
                partition.push(merged);
            }
            Err(error) => {
                tracing::warn!("telemetry batch compaction failed: {error}");
            }
        }
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

        // The rewrite emits coalesced batches, so nothing is left pending.
        self.pending_batches.store(0, Ordering::Relaxed);
        self.pending_rows.store(0, Ordering::Relaxed);
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

/// Opaque handle to the shared table storage.
///
/// External crates receive this capability via
/// [`DatabaseScanner::table_store()`]. The raw storage map is not
/// part of the public API.
///
/// # Table-store invariants (TS-*)
///
/// - **TS-1 (opaque capability):** External crates do not receive
///   the raw `Arc<StdMutex<HashMap<...>>>`.
/// - **TS-2 (behavior parity):** [`TableStore::ingest_batch`]
///   preserves existing ingestion semantics (ID-1 through ID-6).
/// - **TS-3 (read capability minimality):** [`table_names`](Self::table_names)
///   and [`table_provider`](Self::table_provider) expose only what
///   downstream query setup needs. Callers receive
///   `Arc<dyn TableProvider>`, not the backing `MemTable`.
/// - **TS-4 (ownership preserved):** Storage ownership remains in
///   `monarch_distributed_telemetry`. `TableStore` is a handle, not
///   an independent store.
#[derive(Clone)]
pub struct TableStore {
    inner: Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
}

impl TableStore {
    /// Create an empty standalone table store.
    ///
    /// Useful for testing or standalone ingestion scenarios where
    /// the full [`DatabaseScanner`] lifecycle is not needed.
    pub fn new_empty() -> Self {
        Self {
            inner: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Register an empty table with an authoritative schema.
    pub fn register_table(&self, table_name: &str, schema: SchemaRef) -> anyhow::Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;

        match guard.get(table_name) {
            Some(table) if table.schema() != schema => {
                anyhow::bail!("schema mismatch for registered table {table_name}")
            }
            Some(_) => Ok(()),
            None => {
                guard.insert(table_name.to_string(), Arc::new(LiveTableData::new(schema)));
                Ok(())
            }
        }
    }

    /// Ingest a `RecordBatch` into a named table (TS-2).
    ///
    /// Async so callers in async contexts can await directly without
    /// hitting the `block_in_place` bridge in `push_batch_to_tables`.
    ///
    /// See the ID-* invariants on
    /// `DatabaseScanner::push_batch_to_tables` for behavioral
    /// guarantees (this method preserves the same semantics).
    pub async fn ingest_batch(&self, table_name: &str, batch: RecordBatch) -> anyhow::Result<()> {
        let table = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
            guard
                .entry(table_name.to_string())
                .or_insert_with(|| Arc::new(LiveTableData::new(batch.schema())))
                .clone()
        };
        table.push(batch).await;
        Ok(())
    }

    /// Push a batch to a table that must already be registered.
    pub async fn push_to_registered(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        let table = {
            let guard = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
            guard
                .get(table_name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown registered table {table_name}"))?
        };

        if table.schema() != batch.schema() {
            anyhow::bail!("schema mismatch for registered table {table_name}");
        }

        table.push(batch).await;
        Ok(())
    }

    /// Return sorted table names currently in storage (TS-3).
    pub fn table_names(&self) -> anyhow::Result<Vec<String>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        let mut names: Vec<String> = guard.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    /// Return a [`TableProvider`] for a named table, or `None` if
    /// the table does not exist (TS-3).
    ///
    /// The returned provider can be registered directly with a
    /// DataFusion `SessionContext`. Callers do not see the backing
    /// storage type.
    pub fn table_provider(
        &self,
        table_name: &str,
    ) -> anyhow::Result<Option<Arc<dyn TableProvider>>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        Ok(guard
            .get(table_name)
            .map(|t| t.mem_table() as Arc<dyn TableProvider>))
    }
}

/// Default retention duration: one hour in seconds.
const DEFAULT_RETENTION_SECS: u64 = 60 * 60;

/// Ordered tables that keep only recent data; trace definitions are filtered first.
const RETENTION_TABLES: &[&str] = &[
    SPANS,
    SPAN_EVENTS,
    EVENTS,
    SENT_MESSAGES,
    MESSAGES,
    MESSAGE_STATUS_EVENTS,
    METRIC_GAUGES,
    METRIC_SUMS,
    METRIC_HISTOGRAMS,
];

/// Bounds for routine retention sweeps.
const MIN_RETENTION_INTERVAL: Duration = Duration::from_secs(30);
const MAX_RETENTION_INTERVAL: Duration = Duration::from_secs(5 * 60);
const RETENTION_INTERVAL_DIVISOR: i64 = 10;

fn retention_interval_us(retention_us: i64) -> i64 {
    let min_interval_us = MIN_RETENTION_INTERVAL.as_micros() as i64;
    let max_interval_us = MAX_RETENTION_INTERVAL.as_micros() as i64;
    let proportional_interval_us = retention_us / RETENTION_INTERVAL_DIVISOR;
    retention_us.min(proportional_interval_us.clamp(min_interval_us, max_interval_us))
}

/// Target rows per batch streamed back to the query root.
///
/// Every batch a scan emits costs an Arrow IPC serialization plus a hyperactor
/// message, so scan cost tracks *batch* count, not row count.
///
/// [`COMPACT_TARGET_ROWS`] bounds fragmentation in storage, but that is not
/// enough on its own: the tail is only merged once the pending run reaches the
/// target, so between merges it holds up to that many rows spread across as
/// many batches as there were ingests. An unfiltered scan emits one batch per
/// stored batch and so pays for the whole tail -- measured at 242 batches for
/// 9,180 rows, 623 for 10,582, and up to 1,752, against a max of 2 once
/// coalesced.
///
/// This applies specifically to *unfiltered* scans. A filter is pushed down to
/// the collector, where DataFusion inserts a `CoalesceBatchesExec` after the
/// `FilterExec`, so filtered output already arrives in one batch regardless of
/// selectivity. A bare table read has no such operator. The dashboard's hot
/// queries -- `COUNT(*)`, `GROUP BY`, latest-status aggregates over a whole
/// table -- are exactly the unfiltered kind.
///
/// 8192 matches DataFusion's `execution.batch_size` default, so coalesced
/// batches arrive at the query root already the size its operators expect.
const SCAN_TARGET_BATCH_ROWS: usize = 8192;

/// Bypass coalescing for batches larger than this.
///
/// Without a bypass every row is copied into per-column accumulators and
/// re-split to exactly the target, so batches that are already large would be
/// rebuilt for no benefit.
///
/// Half the target, matching every DataFusion call site
/// (`CoalesceBatchesExec`, hash join, sort-merge join). It must be strictly
/// below the target: arrow bypasses only when `rows > limit`, so a limit equal
/// to the target would still rebuild exactly-target-sized batches -- and that
/// is precisely the size compaction seals and the retention rewrite emits, via
/// DataFusion's `execution.batch_size` default. The cost of the lower bound is
/// that a batch between half and one target is emitted as-is rather than being
/// merged with its neighbours; storage produces at most one such batch per
/// table (a retention rewrite's remainder), so this trades one extra message
/// for never rebuilding the common case.
const SCAN_BYPASS_COALESCE_ROWS: usize = SCAN_TARGET_BATCH_ROWS / 2;

/// Build the coalescer that groups a scan's output batches.
///
/// A large batch arriving on a non-empty buffer flushes the buffer first, so
/// row order is preserved whether or not a batch bypasses.
fn scan_coalescer(schema: SchemaRef) -> BatchCoalescer {
    BatchCoalescer::new(schema, SCAN_TARGET_BATCH_ROWS)
        .with_biggest_coalesce_batch_size(Some(SCAN_BYPASS_COALESCE_ROWS))
}

/// Schema of the batches a scan emits.
///
/// `BatchCoalescer` needs its schema up front, so the zero-column projection
/// that COUNT(*) uses has to be applied here as well as to each batch.
fn scan_output_schema(stream_schema: SchemaRef, is_empty_projection: bool) -> DFResult<SchemaRef> {
    if is_empty_projection {
        return Ok(Arc::new(stream_schema.project(&[])?));
    }
    Ok(stream_schema)
}

/// Serialize `batch` and post it to `dest_ref`.
///
/// A serialization failure fails the scan rather than skipping the batch.
/// Skipping matched the previous per-batch behaviour, but a coalesced batch
/// carries up to [`SCAN_TARGET_BATCH_ROWS`] rows, and dropping it would leave
/// the query silently short of that many rows while still reporting success --
/// the batch count the root waits for is only incremented on a successful post,
/// so nothing downstream would notice.
fn post_batch(
    batch: &RecordBatch,
    instance: &Instance<PythonActor>,
    dest_ref: &reference::PortRef<QueryResponse>,
) -> DFResult<()> {
    let data = serialize_batch(batch)
        .map_err(|error| datafusion::error::DataFusionError::External(error.into()))?;
    dest_ref.post(
        instance,
        QueryResponse {
            data: Part::from(data),
        },
    );
    Ok(())
}

fn build_scan_dataframe(
    ctx: &SessionContext,
    table: Arc<dyn TableProvider>,
    projection: Option<&[usize]>,
    where_clause: Option<&str>,
    limit: Option<usize>,
) -> DFResult<DataFrame> {
    let schema = table.schema();
    let mut dataframe = ctx.read_table(table)?;
    if let Some(where_clause) = where_clause {
        let filter = dataframe.parse_sql_expr(where_clause)?;
        dataframe = dataframe.filter(filter)?;
    }

    dataframe = match projection {
        Some([]) => dataframe.select_exprs(&["NULL as fake_column"])?,
        Some(projection) => {
            let columns = projection
                .iter()
                .map(|&index| {
                    schema
                        .fields()
                        .get(index)
                        .map(|field| col(field.name()))
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "projection index {index} out of range"
                            ))
                        })
                })
                .collect::<DFResult<Vec<_>>>()?;
            dataframe.select(columns)?
        }
        None => dataframe,
    };

    dataframe.limit(0, limit)
}

#[pyclass(
    name = "DatabaseScanner",
    module = "monarch._rust_bindings.monarch_distributed_telemetry.database_scanner"
)]
pub struct DatabaseScanner {
    /// Tables stored by name - each holds the schema and shared PartitionData
    table_data: Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
    scan_session: SessionContext,
    rank: usize,
    /// Retention window in microseconds.
    retention_us: i64,
    /// Socket ingest tasks owned by this scanner.
    ///
    /// Keeping the handles here keeps the listener tasks alive for the scanner
    /// lifetime. Dropping the scanner drops each handle, which aborts the
    /// corresponding background task.
    socket_ingest_handles: StdMutex<Vec<crate::socket_ingest::IngestServerHandle>>,
    /// Collector-side retention task owned by this scanner.
    ///
    /// Producer-side `UnixSocketSink` flushes only move frames into storage;
    /// retention policy belongs here where the table store is owned.
    retention_task: StdMutex<Option<AbortHandle>>,
}

#[pymethods]
impl DatabaseScanner {
    #[new]
    #[pyo3(signature = (rank, retention_secs=DEFAULT_RETENTION_SECS))]
    fn new(rank: usize, retention_secs: u64) -> PyResult<Self> {
        let scanner = Self {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            scan_session: SessionContext::new(),
            rank,
            retention_us: retention_secs as i64 * 1_000_000,
            socket_ingest_handles: StdMutex::new(Vec::new()),
            retention_task: StdMutex::new(None),
        };

        // Pre-register py-spy tables so QueryEngine discovers them at setup time
        for (name, batch) in [
            (
                "pyspy_dumps",
                PySpyDumpBuffer::default().drain_to_record_batch().unwrap(),
            ),
            (
                "pyspy_stack_traces",
                PySpyStackTraceBuffer::default()
                    .drain_to_record_batch()
                    .unwrap(),
            ),
            (
                "pyspy_frames",
                PySpyFrameBuffer::default().drain_to_record_batch().unwrap(),
            ),
            (
                "pyspy_local_variables",
                PySpyLocalVariableBuffer::default()
                    .drain_to_record_batch()
                    .unwrap(),
            ),
        ] {
            Self::push_batch_to_tables(&scanner.table_data, name, batch).unwrap();
        }

        Ok(scanner)
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
        let guard = self
            .table_data
            .lock()
            .map_err(|_| PyException::new_err("lock poisoned"))?;
        Ok(guard.keys().cloned().collect())
    }

    /// Get schema for a table in Arrow IPC format.
    fn schema_for<'py>(&self, py: Python<'py>, table: &str) -> PyResult<Bound<'py, PyBytes>> {
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

    /// Decode one snapshot Arrow IPC stream and append it to a registered table.
    fn ingest_snapshot_batch(&self, table_name: &str, arrow_ipc_bytes: &[u8]) -> PyResult<()> {
        let batch = decode_snapshot_batch(table_name, arrow_ipc_bytes)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        self.push_to_registered_blocking(table_name, batch)
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
        // Get actor instance from context and extract the Rust Instance once
        let actor_module = py.import("monarch.actor")?;
        let ctx = actor_module.call_method0("context")?;
        let actor_instance_obj = ctx.getattr("actor_instance")?;
        let py_instance: PyRef<'_, PyInstance> = actor_instance_obj.extract()?;
        let instance: Instance<PythonActor> = py_instance.clone_for_py();

        // Build destination PortRef once
        let dest_port_id: reference::PortAddr = dest.clone().into();
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
    /// Register an empty table with an authoritative schema.
    pub fn register_table(&self, table_name: &str, schema: SchemaRef) -> anyhow::Result<()> {
        self.table_store().register_table(table_name, schema)
    }

    /// Push a batch to a table that must already be registered.
    pub async fn push_to_registered(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        self.table_store()
            .push_to_registered(table_name, batch)
            .await
    }

    /// Push a batch to a registered table from synchronous callers.
    pub fn push_to_registered_blocking(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        let store = self.table_store();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| {
                handle.block_on(store.push_to_registered(table_name, batch))
            })
        } else {
            get_tokio_runtime().block_on(store.push_to_registered(table_name, batch))
        }
    }

    /// Start Unix-socket ingest for this scanner.
    pub fn start_socket_ingest(&self, socket_path: &Path) -> anyhow::Result<()> {
        let listener = crate::socket_ingest::bind_ingest_socket(socket_path)?;
        let handle = crate::socket_ingest::run_ingest_server(listener, self.table_store())?;
        self.start_periodic_retention()?;
        self.socket_ingest_handles
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?
            .push(handle);
        Ok(())
    }

    fn start_periodic_retention(&self) -> anyhow::Result<()> {
        if self.retention_us == 0 {
            return Ok(());
        }

        // Socket ingest can append directly into `TableStore` without a
        // scanner query/flush. Keep retention as low-frequency collector
        // maintenance instead of coupling DataFusion filtering to producer
        // flushes or per-frame ingest.
        let mut guard = self
            .retention_task
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        if guard.is_some() {
            return Ok(());
        }

        *guard = Some(Self::spawn_periodic_retention_task(
            self.table_data.clone(),
            self.retention_us,
        ));
        Ok(())
    }

    fn spawn_periodic_retention_task(
        table_data: Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
        retention_us: i64,
    ) -> AbortHandle {
        let interval = Duration::from_micros(retention_interval_us(retention_us) as u64);
        let handle = get_tokio_runtime().spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let now_us = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock before unix epoch")
                    .as_micros() as i64;
                let where_clause = Self::retention_where_clause(retention_us, now_us);
                if let Err(error) =
                    Self::apply_retention_policies_to_tables(&table_data, &where_clause).await
                {
                    tracing::warn!("periodic telemetry retention failed: {error}");
                }
            }
        });
        handle.abort_handle()
    }

    async fn apply_retention_policies_to_tables(
        table_data: &Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
        where_clause: &str,
    ) -> anyhow::Result<()> {
        for &table_name in RETENTION_TABLES {
            let table = table_data
                .lock()
                .map_err(|_| anyhow::anyhow!("lock poisoned"))?
                .get(table_name)
                .cloned();
            if let Some(table) = table {
                table.apply_retention(table_name, where_clause).await?;
            }
        }
        Ok(())
    }

    fn retention_where_clause(retention_us: i64, now_us: i64) -> String {
        let cutoff = now_us - retention_us;
        format!("timestamp_us > {cutoff}")
    }

    #[cfg(test)]
    fn spawn_triggered_retention_task(
        table_data: Arc<StdMutex<HashMap<String, Arc<LiveTableData>>>>,
        retention_us: i64,
        mut receiver: tokio::sync::mpsc::Receiver<(i64, tokio::sync::oneshot::Sender<()>)>,
    ) -> AbortHandle {
        let handle = get_tokio_runtime().spawn(async move {
            while let Some((now_us, ack)) = receiver.recv().await {
                let where_clause = Self::retention_where_clause(retention_us, now_us);
                let _ = Self::apply_retention_policies_to_tables(&table_data, &where_clause).await;
                let _ = ack.send(());
            }
        });
        handle.abort_handle()
    }

    /// Push a batch into the named table in `table_data`.
    ///
    /// # Ingestion invariants (ID-*)
    ///
    /// - **ID-1 (create on first batch):** If `table_name` is absent,
    ///   a new `LiveTableData` is created from `batch.schema()`.
    /// - **ID-2 (empty batch registers schema):** An empty batch
    ///   creates the table entry and preserves the schema —
    ///   `LiveTableData::push` is a no-op for zero rows, but the
    ///   `entry().or_insert_with()` runs unconditionally.
    /// - **ID-3 (append on existing table):** A non-empty batch for
    ///   an existing table appends rows.
    /// - **ID-4 (error surface):** Lock poisoning propagates as
    ///   `Err`. `push()` itself is infallible.
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

    /// Parse a py-spy result JSON and store data in normalized py-spy tables.
    ///
    /// Populates four tables matching the `hyperactor_mesh::pyspy` structs:
    /// - `pyspy_dumps`: one row per dump, including warnings as JSON
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
        use monarch_record_batch::RecordBatchBuffer;

        use crate::pyspy_table::PySpyDump;
        use crate::pyspy_table::PySpyDumpBuffer;
        use crate::pyspy_table::PySpyFrame;
        use crate::pyspy_table::PySpyFrameBuffer;
        use crate::pyspy_table::PySpyLocalVariable;
        use crate::pyspy_table::PySpyLocalVariableBuffer;
        use crate::pyspy_table::PySpyStackTrace;
        use crate::pyspy_table::PySpyStackTraceBuffer;

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
        let capture_mode = ok
            .get("capture_mode")
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing py-spy capture_mode"))?;
        if !matches!(capture_mode, "python_only" | "native" | "native_all") {
            anyhow::bail!("invalid py-spy capture_mode: {capture_mode}");
        }
        let warnings = ok
            .get("warnings")
            .and_then(|value| value.as_array())
            .map(Vec::as_slice)
            .unwrap_or_default();
        let warnings_json = serde_json::to_string(warnings)?;
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
            capture_mode: capture_mode.to_string(),
            warnings_json,
        });
        Self::push_batch_to_tables(
            &self.table_data,
            "pyspy_dumps",
            dump_buf.drain_to_record_batch()?,
        )?;

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
            trace_buf.drain_to_record_batch()?,
        )?;
        Self::push_batch_to_tables(
            &self.table_data,
            "pyspy_frames",
            frame_buf.drain_to_record_batch()?,
        )?;
        Self::push_batch_to_tables(
            &self.table_data,
            "pyspy_local_variables",
            local_buf.drain_to_record_batch()?,
        )?;
        Ok(())
    }

    /// Return an opaque [`TableStore`] handle for external callers.
    pub fn table_store(&self) -> TableStore {
        TableStore {
            inner: self.table_data.clone(),
        }
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
        let mem_table = {
            let guard = self
                .table_data
                .lock()
                .map_err(|_| PyException::new_err("lock poisoned"))?;
            let table_data = guard
                .get(table_name)
                .ok_or_else(|| PyException::new_err(format!("table '{}' not found", table_name)))?;
            table_data.mem_table()
        };

        // Handle empty projection (e.g., for COUNT(*) queries)
        // DataFusion may request 0 columns but we still need row counts
        let is_empty_projection = matches!(&projection, Some(proj) if proj.is_empty());

        // Execute and stream batches directly to destination
        let batch_count = get_tokio_runtime()
            .block_on(async {
                use futures::StreamExt;

                let df = build_scan_dataframe(
                    &self.scan_session,
                    mem_table,
                    projection.as_deref(),
                    where_clause.as_deref(),
                    limit,
                )?;
                let mut stream = df.execute_stream().await?;
                let mut coalescer =
                    scan_coalescer(scan_output_schema(stream.schema(), is_empty_projection)?);
                let mut count: usize = 0;
                let mut rows: usize = 0;

                while let Some(result) = stream.next().await {
                    let batch = result?;

                    // For empty projection, project to empty schema
                    let batch = if is_empty_projection {
                        batch.project(&[])?
                    } else {
                        batch
                    };

                    coalescer.push_batch(batch)?;
                    while let Some(coalesced) = coalescer.next_completed_batch() {
                        post_batch(&coalesced, instance, dest_ref)?;
                        count += 1;
                        rows += coalesced.num_rows();
                    }
                }
                coalescer.finish_buffered_batch()?;
                while let Some(coalesced) = coalescer.next_completed_batch() {
                    post_batch(&coalesced, instance, dest_ref)?;
                    count += 1;
                    rows += coalesced.num_rows();
                }

                // Counted after posting, so the log never reports rows that
                // failed to reach the root.
                tracing::info!(
                    "Scanner {}: local scan complete, sent {} batches ({} rows)",
                    rank,
                    count,
                    rows
                );
                Ok::<usize, datafusion::error::DataFusionError>(count)
            })
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(batch_count)
    }
}

impl Drop for DatabaseScanner {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.retention_task.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
    }
}

fn decode_snapshot_batch(table_name: &str, payload: &[u8]) -> anyhow::Result<RecordBatch> {
    // Snapshot batches permit zero rows, unlike the socket-ingest frame path;
    // `deserialize_one_batch` already enforces the one-batch-per-stream contract.
    deserialize_one_batch(payload)
        .with_context(|| format!("decoding snapshot batch for table {table_name}"))
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
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::arrow::record_batch::RecordBatch;
    use monarch_telemetry_schema::entity_tables::ACTORS;
    use monarch_telemetry_schema::entity_tables::Actor;
    use monarch_telemetry_schema::entity_tables::ActorBuffer;
    use monarch_telemetry_schema::entity_tables::MESSAGE_STATUS_EVENTS;
    use monarch_telemetry_schema::entity_tables::MESSAGES;
    use monarch_telemetry_schema::entity_tables::Message;
    use monarch_telemetry_schema::entity_tables::MessageBuffer;
    use monarch_telemetry_schema::entity_tables::MessageStatusEvent;
    use monarch_telemetry_schema::entity_tables::MessageStatusEventBuffer;
    use monarch_telemetry_schema::entity_tables::SENT_MESSAGES;
    use monarch_telemetry_schema::entity_tables::SentMessage;
    use monarch_telemetry_schema::entity_tables::SentMessageBuffer;
    use monarch_telemetry_schema::metric_tables::MetricGauge;
    use monarch_telemetry_schema::metric_tables::MetricGaugeBuffer;
    use monarch_telemetry_schema::metric_tables::MetricHistogram;
    use monarch_telemetry_schema::metric_tables::MetricHistogramBuffer;
    use monarch_telemetry_schema::metric_tables::MetricSum;
    use monarch_telemetry_schema::metric_tables::MetricSumBuffer;
    use monarch_telemetry_schema::trace_tables::Event;
    use monarch_telemetry_schema::trace_tables::EventBuffer;
    use monarch_telemetry_schema::trace_tables::Span;
    use monarch_telemetry_schema::trace_tables::SpanBuffer;
    use monarch_telemetry_schema::trace_tables::SpanEvent;
    use monarch_telemetry_schema::trace_tables::SpanEventBuffer;

    use super::*;

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let col = Int64Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
    }

    fn make_other_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Int64, false)]));
        let col = Int64Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
    }

    fn serialize_batches(batches: &[RecordBatch]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batches[0].schema()).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        buf
    }

    async fn row_count(table: &LiveTableData) -> usize {
        table.mem_table.batches[0]
            .read()
            .await
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    async fn batch_count(table: &LiveTableData) -> usize {
        table.mem_table.batches[0].read().await.len()
    }

    fn test_scanner(retention_us: i64) -> DatabaseScanner {
        DatabaseScanner {
            table_data: Arc::new(StdMutex::new(HashMap::new())),
            scan_session: SessionContext::new(),
            rank: 0,
            retention_us,
            socket_ingest_handles: StdMutex::new(Vec::new()),
            retention_task: StdMutex::new(None),
        }
    }

    async fn ingest_batch(scanner: &DatabaseScanner, table_name: &str, batch: RecordBatch) {
        scanner
            .table_store()
            .ingest_batch(table_name, batch)
            .await
            .unwrap();
    }

    async fn ingest_retention_rows(scanner: &DatabaseScanner, old_us: i64, fresh_us: i64) {
        let mut sent = SentMessageBuffer::default();
        sent.insert(SentMessage {
            id: 1,
            timestamp_us: old_us,
            sender_actor_id: 10,
            actor_mesh_id: 20,
            view_json: "{}".to_string(),
            shape_json: "{}".to_string(),
        });
        sent.insert(SentMessage {
            id: 2,
            timestamp_us: fresh_us,
            sender_actor_id: 11,
            actor_mesh_id: 21,
            view_json: "{}".to_string(),
            shape_json: "{}".to_string(),
        });
        ingest_batch(
            scanner,
            SENT_MESSAGES,
            sent.drain_to_record_batch().unwrap(),
        )
        .await;

        let mut messages = MessageBuffer::default();
        messages.insert(Message {
            id: 3,
            timestamp_us: old_us,
            from_actor_id: 10,
            to_actor_id: 30,
            endpoint: Some("old".to_string()),
            port_index: None,
        });
        messages.insert(Message {
            id: 4,
            timestamp_us: fresh_us,
            from_actor_id: 11,
            to_actor_id: 31,
            endpoint: Some("fresh".to_string()),
            port_index: Some(4),
        });
        ingest_batch(scanner, MESSAGES, messages.drain_to_record_batch().unwrap()).await;

        let mut statuses = MessageStatusEventBuffer::default();
        statuses.insert(MessageStatusEvent {
            id: 5,
            timestamp_us: old_us,
            message_id: 3,
            status: "queued".to_string(),
        });
        statuses.insert(MessageStatusEvent {
            id: 6,
            timestamp_us: fresh_us,
            message_id: 4,
            status: "complete".to_string(),
        });
        ingest_batch(
            scanner,
            MESSAGE_STATUS_EVENTS,
            statuses.drain_to_record_batch().unwrap(),
        )
        .await;

        let mut actors = ActorBuffer::default();
        actors.insert(Actor {
            id: 7,
            timestamp_us: old_us,
            mesh_id: 70,
            rank: 0,
            full_name: "old".to_string(),
            display_name: None,
        });
        actors.insert(Actor {
            id: 8,
            timestamp_us: fresh_us,
            mesh_id: 80,
            rank: 1,
            full_name: "fresh".to_string(),
            display_name: Some("fresh".to_string()),
        });
        ingest_batch(scanner, ACTORS, actors.drain_to_record_batch().unwrap()).await;

        let mut spans = SpanBuffer::default();
        for (id, timestamp_us, parent_id) in [(9, old_us, None), (10, fresh_us, Some(9))] {
            spans.insert(Span {
                process_id: "test",
                id,
                name: format!("span-{id}"),
                target: "test".to_string(),
                level: "INFO".to_string(),
                fields_json: "{}".to_string(),
                timestamp_us,
                parent_id,
                thread_name: "test".to_string(),
                file: None,
                line: None,
            });
        }
        ingest_batch(scanner, SPANS, spans.drain_to_record_batch().unwrap()).await;

        let mut span_events = SpanEventBuffer::default();
        for (timestamp_us, event_type) in [(old_us, "enter"), (fresh_us, "close")] {
            span_events.insert(SpanEvent {
                process_id: "test",
                id: 9,
                timestamp_us,
                event_type: event_type.to_string(),
            });
        }
        ingest_batch(
            scanner,
            SPAN_EVENTS,
            span_events.drain_to_record_batch().unwrap(),
        )
        .await;

        let mut events = EventBuffer::default();
        for (name, timestamp_us) in [("old", old_us), ("fresh", fresh_us)] {
            events.insert(Event {
                name: name.to_string(),
                target: "test".to_string(),
                level: "INFO".to_string(),
                fields_json: "{}".to_string(),
                timestamp_us,
                parent_span: Some(9),
                thread_id: "1".to_string(),
                thread_name: "test".to_string(),
                module_path: None,
                file: None,
                line: None,
            });
        }
        ingest_batch(scanner, EVENTS, events.drain_to_record_batch().unwrap()).await;

        let mut gauges = MetricGaugeBuffer::default();
        gauges.insert(MetricGauge {
            name: "old_gauge".to_string(),
            timestamp_us: old_us,
            start_timestamp_us: None,
            scope_name: "test".to_string(),
            unit: "1".to_string(),
            attributes_json: "{}".to_string(),
            resource_attributes_json: "{}".to_string(),
            value_f64: Some(1.0),
            value_i64: None,
            value_u64: None,
        });
        gauges.insert(MetricGauge {
            name: "fresh_gauge".to_string(),
            timestamp_us: fresh_us,
            start_timestamp_us: None,
            scope_name: "test".to_string(),
            unit: "1".to_string(),
            attributes_json: "{}".to_string(),
            resource_attributes_json: "{}".to_string(),
            value_f64: Some(2.0),
            value_i64: None,
            value_u64: None,
        });
        ingest_batch(
            scanner,
            METRIC_GAUGES,
            gauges.drain_to_record_batch().unwrap(),
        )
        .await;

        let mut sums = MetricSumBuffer::default();
        sums.insert(MetricSum {
            name: "old_sum".to_string(),
            timestamp_us: old_us,
            start_timestamp_us: old_us - 1,
            scope_name: "test".to_string(),
            unit: "1".to_string(),
            temporality: "delta".to_string(),
            is_monotonic: true,
            attributes_json: "{}".to_string(),
            resource_attributes_json: "{}".to_string(),
            sum_f64: None,
            sum_i64: None,
            sum_u64: Some(1),
        });
        sums.insert(MetricSum {
            name: "fresh_sum".to_string(),
            timestamp_us: fresh_us,
            start_timestamp_us: fresh_us - 1,
            scope_name: "test".to_string(),
            unit: "1".to_string(),
            temporality: "delta".to_string(),
            is_monotonic: true,
            attributes_json: "{}".to_string(),
            resource_attributes_json: "{}".to_string(),
            sum_f64: None,
            sum_i64: None,
            sum_u64: Some(2),
        });
        ingest_batch(scanner, METRIC_SUMS, sums.drain_to_record_batch().unwrap()).await;

        let mut histograms = MetricHistogramBuffer::default();
        histograms.insert(MetricHistogram {
            name: "old_histogram".to_string(),
            timestamp_us: old_us,
            start_timestamp_us: old_us - 1,
            scope_name: "test".to_string(),
            unit: "ms".to_string(),
            temporality: "delta".to_string(),
            attributes_json: "{}".to_string(),
            resource_attributes_json: "{}".to_string(),
            count: 1,
            sum_f64: Some(1.0),
            sum_i64: None,
            sum_u64: None,
            min_f64: Some(1.0),
            min_i64: None,
            min_u64: None,
            max_f64: Some(1.0),
            max_i64: None,
            max_u64: None,
            bounds_json: "[]".to_string(),
            bucket_counts_json: "[1]".to_string(),
        });
        histograms.insert(MetricHistogram {
            name: "fresh_histogram".to_string(),
            timestamp_us: fresh_us,
            start_timestamp_us: fresh_us - 1,
            scope_name: "test".to_string(),
            unit: "ms".to_string(),
            temporality: "delta".to_string(),
            attributes_json: "{}".to_string(),
            resource_attributes_json: "{}".to_string(),
            count: 1,
            sum_f64: Some(2.0),
            sum_i64: None,
            sum_u64: None,
            min_f64: None,
            min_i64: None,
            min_u64: None,
            max_f64: None,
            max_i64: None,
            max_u64: None,
            bounds_json: "[]".to_string(),
            bucket_counts_json: "[1]".to_string(),
        });
        ingest_batch(
            scanner,
            METRIC_HISTOGRAMS,
            histograms.drain_to_record_batch().unwrap(),
        )
        .await;
    }

    async fn table_row_count_async(scanner: &DatabaseScanner, table_name: &str) -> usize {
        let table = scanner.table_data.lock().unwrap().get(table_name).cloned();
        match table {
            Some(table) => row_count(&table).await,
            None => 0,
        }
    }

    async fn table_u64_values_async(
        scanner: &DatabaseScanner,
        table_name: &str,
        column_name: &str,
    ) -> Vec<u64> {
        let table = scanner
            .table_data
            .lock()
            .expect("table map should be available")
            .get(table_name)
            .expect("table should exist")
            .clone();
        let batches = table.mem_table.batches[0].read().await;
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(column_name)
                    .expect("column should exist")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("column should be UInt64")
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    async fn table_i64_values_async(
        scanner: &DatabaseScanner,
        table_name: &str,
        column_name: &str,
    ) -> Vec<i64> {
        let table = scanner
            .table_data
            .lock()
            .expect("table map should be available")
            .get(table_name)
            .expect("table should exist")
            .clone();
        let batches = table.mem_table.batches[0].read().await;
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(column_name)
                    .expect("column should exist")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column should be Int64")
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
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
    async fn test_build_scan_dataframe_applies_filter_projection_and_limit() {
        let table = LiveTableData::new(make_batch(&[]).schema());
        table.push(make_batch(&[1, 2, 3, 4])).await;
        let schema = table.schema();
        let ctx = SessionContext::new();

        let dataframe =
            build_scan_dataframe(&ctx, table.mem_table(), Some(&[0]), Some("x >= 2"), Some(2))
                .unwrap();
        let batches = dataframe.collect().await.unwrap();
        let batch = concat_batches(&schema, &batches).unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(values.values(), &[2, 3]);
    }

    #[test]
    fn test_build_scan_dataframe_rejects_out_of_range_projection() {
        let table = LiveTableData::new(make_batch(&[]).schema());
        let ctx = SessionContext::new();

        let error = build_scan_dataframe(&ctx, table.mem_table(), Some(&[1]), None, None)
            .expect_err("out-of-range projection should fail");

        assert!(
            matches!(
                &error,
                datafusion::error::DataFusionError::Plan(message)
                    if message == "projection index 1 out of range"
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn test_push_to_registered_appends_registered_table() {
        let store = TableStore::new_empty();
        store.register_table("t", make_batch(&[]).schema()).unwrap();

        store
            .push_to_registered("t", make_batch(&[1, 2]))
            .await
            .unwrap();

        let table = store.inner.lock().unwrap().get("t").unwrap().clone();
        assert_eq!(row_count(&table).await, 2);
    }

    #[tokio::test]
    async fn test_push_to_registered_rejects_unknown_table() {
        let store = TableStore::new_empty();

        let err = store
            .push_to_registered("missing", make_batch(&[1]))
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("unknown registered table missing"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_push_to_registered_rejects_schema_mismatch() {
        let store = TableStore::new_empty();
        store.register_table("t", make_batch(&[]).schema()).unwrap();

        let err = store
            .push_to_registered("t", make_other_batch(&[1]))
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("schema mismatch for registered table t"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_ingest_snapshot_batch_accepts_zero_rows() {
        let scanner = test_scanner(0);
        scanner
            .register_table("t", make_batch(&[]).schema())
            .unwrap();
        let payload = crate::serialize_batch(&make_batch(&[])).unwrap();

        scanner
            .ingest_snapshot_batch("t", &payload)
            .expect("zero-row snapshot batches are valid");

        assert_eq!(table_row_count(&scanner, "t"), 0);
    }

    #[test]
    fn test_ingest_snapshot_batch_appends_registered_table() {
        let scanner = test_scanner(0);
        scanner
            .register_table("t", make_batch(&[]).schema())
            .unwrap();
        let payload = crate::serialize_batch(&make_batch(&[1, 2, 3])).unwrap();

        scanner.ingest_snapshot_batch("t", &payload).unwrap();

        assert_eq!(table_row_count(&scanner, "t"), 3);
    }

    #[test]
    fn test_ingest_snapshot_batch_rejects_schema_mismatch() {
        let scanner = test_scanner(0);
        scanner
            .register_table("t", make_batch(&[]).schema())
            .unwrap();
        let payload = crate::serialize_batch(&make_other_batch(&[1])).unwrap();

        assert!(scanner.ingest_snapshot_batch("t", &payload).is_err());
        assert_eq!(table_row_count(&scanner, "t"), 0);
    }

    #[test]
    fn test_ingest_snapshot_batch_rejects_multiple_batches() {
        let scanner = test_scanner(0);
        scanner
            .register_table("t", make_batch(&[]).schema())
            .unwrap();
        let payload = serialize_batches(&[make_batch(&[1]), make_batch(&[2])]);

        let err = decode_snapshot_batch("t", &payload).unwrap_err();

        let chain = format!("{err:#}");
        assert!(
            chain.contains("snapshot batch for table t")
                && chain.contains("multiple record batches"),
            "unexpected error: {chain}"
        );
        assert!(scanner.ingest_snapshot_batch("t", &payload).is_err());
        assert_eq!(table_row_count(&scanner, "t"), 0);
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

    #[tokio::test]
    async fn test_periodic_retention_task_filters_retention_tables() {
        let scanner = test_scanner(10_000_000);
        let now_us = 100_000_000;
        let old_us = now_us - 20_000_000;
        let fresh_us = now_us - 5_000_000;
        ingest_retention_rows(&scanner, old_us, fresh_us).await;

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let handle = DatabaseScanner::spawn_triggered_retention_task(
            scanner.table_data.clone(),
            scanner.retention_us,
            receiver,
        );
        let (ack_sender, ack_receiver) = tokio::sync::oneshot::channel();
        sender.send((now_us, ack_sender)).await.unwrap();
        ack_receiver.await.unwrap();
        handle.abort();

        assert_eq!(table_row_count_async(&scanner, SENT_MESSAGES).await, 1);
        assert_eq!(table_row_count_async(&scanner, MESSAGES).await, 1);
        assert_eq!(
            table_row_count_async(&scanner, MESSAGE_STATUS_EVENTS).await,
            1
        );
        assert_eq!(
            table_u64_values_async(&scanner, SPANS, "id").await,
            vec![10]
        );
        assert_eq!(
            table_u64_values_async(&scanner, SPAN_EVENTS, "id").await,
            vec![9]
        );
        assert_eq!(
            table_i64_values_async(&scanner, EVENTS, "timestamp_us").await,
            vec![fresh_us]
        );
        assert_eq!(table_row_count_async(&scanner, METRIC_GAUGES).await, 1);
        assert_eq!(table_row_count_async(&scanner, METRIC_SUMS).await, 1);
        assert_eq!(table_row_count_async(&scanner, METRIC_HISTOGRAMS).await, 1);
        assert_eq!(table_row_count_async(&scanner, ACTORS).await, 2);
    }

    #[test]
    fn test_trace_retention_runs_spans_first() {
        assert_eq!(
            &RETENTION_TABLES[..3],
            &[SPANS, SPAN_EVENTS, EVENTS],
            "span definitions must be filtered before dependent trace rows"
        );
    }

    #[test]
    fn test_retention_interval_scales_with_window() {
        assert_eq!(retention_interval_us(0), 0);
        assert_eq!(retention_interval_us(10_000_000), 10_000_000);
        assert_eq!(retention_interval_us(5 * 60_000_000), 30_000_000);
        assert_eq!(retention_interval_us(10 * 60_000_000), 60_000_000);
        assert_eq!(retention_interval_us(60 * 60_000_000), 300_000_000);
    }

    #[test]
    fn test_retention_secs_zero_starts_no_task() {
        let scanner = test_scanner(0);

        scanner.start_periodic_retention().unwrap();

        assert!(scanner.retention_task.lock().unwrap().is_none());
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
        let scanner = test_scanner(0);

        let json = r#"{
            "Ok": {
                "pid": 1234, "binary": "python3",
                "capture_mode": "native",
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
                "warnings": ["--native-all unsupported; fell back to --native"]
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
        let capture_modes = batch
            .column_by_name("capture_mode")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let warnings_json = batch
            .column_by_name("warnings_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(dump_ids.value(0), "dump-1");
        assert_eq!(pids.value(0), 1234);
        assert_eq!(binaries.value(0), "python3");
        assert_eq!(proc_refs.value(0), "proc[0]");
        assert_eq!(capture_modes.value(0), "native");
        assert_eq!(
            warnings_json.value(0),
            r#"["--native-all unsupported; fell back to --native"]"#
        );

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
        let scanner = test_scanner(0);

        let json =
            r#"{"Failed": {"pid": 1, "binary": "py-spy", "exit_code": 1, "stderr": "error"}}"#;
        scanner.store_pyspy_dump("dump-2", "proc[0]", json).unwrap();

        assert_eq!(table_row_count(&scanner, "pyspy_dumps"), 0);
        assert_eq!(table_row_count(&scanner, "pyspy_stack_traces"), 0);
        assert_eq!(table_row_count(&scanner, "pyspy_frames"), 0);
    }

    #[test]
    fn test_store_pyspy_dump_invalid_json_errors() {
        let scanner = test_scanner(0);
        assert!(scanner.store_pyspy_dump("x", "p", "not json").is_err());
    }

    #[test]
    fn test_store_pyspy_dump_invalid_capture_mode_errors() {
        let scanner = test_scanner(0);
        let json = r#"{"Ok": {"capture_mode": "unknown"}}"#;

        assert!(scanner.store_pyspy_dump("x", "p", json).is_err());
        assert_eq!(table_row_count(&scanner, "pyspy_dumps"), 0);
    }

    #[test]
    fn test_store_pyspy_dump_multiple_threads() {
        let scanner = test_scanner(0);

        let json = r#"{
            "Ok": {
                "pid": 1, "binary": "python3",
                "capture_mode": "python_only",
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

    // --- ingest_batch tests ---
    // These reference the ID-* invariants defined on ingest_batch.

    // ID-1, ID-2: empty batch creates the table with schema but 0
    // rows.
    #[tokio::test]
    async fn test_ingest_batch_creates_table_for_empty_batch() {
        let store = TableStore::new_empty();
        let empty = make_batch(&[]);

        store.ingest_batch("t", empty.clone()).await.unwrap();

        let names = store.table_names().unwrap();
        assert!(names.contains(&"t".to_owned()), "ID-1: table should exist");
        assert_eq!(
            query_row_count("t", store.table_provider("t").unwrap().unwrap()).await,
            0,
            "ID-2: 0 rows"
        );
    }

    // ID-1, ID-3: non-empty batch creates table and appends rows.
    #[tokio::test]
    async fn test_ingest_batch_appends_non_empty_batch() {
        let store = TableStore::new_empty();

        store
            .ingest_batch("t", make_batch(&[1, 2, 3]))
            .await
            .unwrap();

        assert_eq!(
            query_row_count("t", store.table_provider("t").unwrap().unwrap()).await,
            3
        );
    }

    // ID-3: two batches to the same table accumulate rows.
    #[tokio::test]
    async fn test_ingest_batch_reuses_existing_table() {
        let store = TableStore::new_empty();

        store.ingest_batch("t", make_batch(&[1, 2])).await.unwrap();
        store
            .ingest_batch("t", make_batch(&[3, 4, 5]))
            .await
            .unwrap();

        assert_eq!(
            store.table_names().unwrap().len(),
            1,
            "ID-3: still one table"
        );
        assert_eq!(
            query_row_count("t", store.table_provider("t").unwrap().unwrap()).await,
            5
        );
    }

    // ID-2, ID-3: empty batch registers schema, then non-empty batch
    // appends rows using the same schema.
    #[tokio::test]
    async fn test_ingest_batch_empty_then_non_empty() {
        let store = TableStore::new_empty();

        // Register schema with empty batch.
        store.ingest_batch("t", make_batch(&[])).await.unwrap();
        assert_eq!(
            query_row_count("t", store.table_provider("t").unwrap().unwrap()).await,
            0
        );

        // Append rows.
        store
            .ingest_batch("t", make_batch(&[10, 20]))
            .await
            .unwrap();
        assert_eq!(
            query_row_count("t", store.table_provider("t").unwrap().unwrap()).await,
            2
        );
    }

    // --- TableStore tests ---
    // These reference the TS-* invariants defined on TableStore.

    /// Register a provider in a fresh SessionContext and return the
    /// row count from `SELECT * FROM {table_name}`.
    async fn query_row_count(table_name: &str, provider: Arc<dyn TableProvider>) -> usize {
        let ctx = SessionContext::new();
        ctx.register_table(table_name, provider).unwrap();
        let df = ctx
            .sql(&format!("SELECT * FROM {table_name}"))
            .await
            .unwrap();
        df.collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    // TS-2, TS-3: ingest via TableStore, register the returned
    // table_provider in a SessionContext, and query it. Proves the
    // opaque handle is sufficient for downstream query setup.
    #[tokio::test]
    async fn test_table_store_ingest_and_query() {
        let store = TableStore::new_empty();

        store
            .ingest_batch("t", make_batch(&[10, 20, 30]))
            .await
            .unwrap();

        let provider = store
            .table_provider("t")
            .unwrap()
            .expect("TS-3: table_provider should return Some");

        assert_eq!(
            query_row_count("t", provider).await,
            3,
            "TS-3: query should return ingested rows"
        );
    }

    // TS-3: table_names returns all ingested table names, sorted.
    #[tokio::test]
    async fn test_table_store_table_names() {
        let store = TableStore::new_empty();

        store.ingest_batch("beta", make_batch(&[1])).await.unwrap();
        store.ingest_batch("alpha", make_batch(&[2])).await.unwrap();

        let names = store.table_names().unwrap();
        assert_eq!(names, vec!["alpha", "beta"], "TS-3: names should be sorted");
    }

    // TS-2 (ID-2 passthrough): empty batch registers schema via
    // TableStore. Proves the table is visible through table_names
    // and table_provider without re-proving row-count internals.
    #[tokio::test]
    async fn test_table_store_empty_batch_registers() {
        let store = TableStore::new_empty();

        store.ingest_batch("t", make_batch(&[])).await.unwrap();

        assert!(
            store.table_names().unwrap().contains(&"t".to_owned()),
            "TS-2: empty batch should register table name"
        );
        assert!(
            store.table_provider("t").unwrap().is_some(),
            "TS-2: empty batch should make table_provider available"
        );
    }

    // TS-3: table_provider for unknown table returns None.
    #[test]
    fn test_table_store_missing_table() {
        let store = TableStore::new_empty();

        assert!(
            store.table_provider("missing").unwrap().is_none(),
            "TS-3: unknown table should return None"
        );
    }

    // Ingest appends one batch per write; compaction must keep the partition
    // proportional to rows stored, not to writes performed. Without it a table
    // reaches tens of thousands of few-row batches, and batch count is what
    // drives scan, retention, and planning cost.
    #[tokio::test]
    async fn test_push_compacts_small_batches() {
        let table = LiveTableData::new(make_batch(&[1]).schema());
        let writes = COMPACT_TARGET_ROWS * 3;

        for i in 0..writes {
            table.push(make_batch(&[i as i64])).await;
        }

        assert_eq!(
            row_count(&table).await,
            writes,
            "compaction must not drop rows"
        );
        // Three full targets compact to 3 batches, plus at most one open tail.
        assert!(
            batch_count(&table).await <= 4,
            "expected <=4 compacted batches for {writes} single-row writes, got {}",
            batch_count(&table).await
        );
    }

    // Compaction merges by position, so the stored order must survive it.
    #[tokio::test]
    async fn test_compaction_preserves_row_order() {
        let table = LiveTableData::new(make_batch(&[1]).schema());
        let writes = COMPACT_TARGET_ROWS + 10;

        for i in 0..writes {
            table.push(make_batch(&[i as i64])).await;
        }

        let guard = table.mem_table.batches[0].read().await;
        let values: Vec<i64> = guard
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        let expected: Vec<i64> = (0..writes as i64).collect();
        assert_eq!(values, expected, "compaction must preserve write order");
    }

    // `push` rejects empty batches, so pending rows always reach the target
    // before the pending run can exceed that many batches. This is what lets a
    // row-only trigger bound the tail.
    #[tokio::test]
    async fn test_uncompacted_tail_is_bounded() {
        let table = LiveTableData::new(make_batch(&[1]).schema());

        // One row short of a second compaction: the worst case tail.
        for i in 0..(COMPACT_TARGET_ROWS * 2 - 1) {
            table.push(make_batch(&[i as i64])).await;
        }

        assert!(
            batch_count(&table).await <= COMPACT_TARGET_ROWS,
            "tail must stay bounded by the row target, got {}",
            batch_count(&table).await
        );
    }

    // Retention rewrites the whole partition, so the pending counters must be
    // cleared or compaction would merge against a stale tail length.
    #[tokio::test]
    async fn test_retention_resets_pending_counters() {
        let table = LiveTableData::new(make_batch(&[1]).schema());
        for i in 0..10 {
            table.push(make_batch(&[i])).await;
        }
        assert!(table.pending_batches.load(Ordering::Relaxed) > 0);

        table.apply_retention("t", "1=1").await.unwrap();

        assert_eq!(table.pending_batches.load(Ordering::Relaxed), 0);
        assert_eq!(table.pending_rows.load(Ordering::Relaxed), 0);
        assert_eq!(row_count(&table).await, 10, "retention kept every row");
    }

    /// Drive the coalescer exactly as `execute_scan_streaming` does, through the
    /// same `scan_output_schema` and `scan_coalescer` the scan path uses,
    /// returning the batches that would have been posted.
    fn run_coalescer(input: Vec<RecordBatch>, is_empty_projection: bool) -> Vec<RecordBatch> {
        // `input` carries the stream's own schema, unprojected, so the schema
        // and the per-batch projection are derived here the same way the scan
        // loop derives them.
        let schema = scan_output_schema(input[0].schema(), is_empty_projection).unwrap();
        let mut coalescer = scan_coalescer(schema);
        let mut posted = Vec::new();
        for batch in input {
            let batch = if is_empty_projection {
                batch.project(&[]).unwrap()
            } else {
                batch
            };
            coalescer.push_batch(batch).unwrap();
            while let Some(coalesced) = coalescer.next_completed_batch() {
                posted.push(coalesced);
            }
        }
        coalescer.finish_buffered_batch().unwrap();
        while let Some(coalesced) = coalescer.next_completed_batch() {
            posted.push(coalesced);
        }
        posted
    }

    fn values_of(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    // The transport groups scan output into few large messages: one message per
    // SCAN_TARGET_BATCH_ROWS rather than one per stored batch. This is the whole
    // point of the change -- a 290k-row table previously streamed 68,983
    // messages.
    #[test]
    fn test_scan_coalescer_emits_one_batch_per_target() {
        let rows = SCAN_TARGET_BATCH_ROWS * 2 + 5;
        let input: Vec<RecordBatch> = (0..rows).map(|i| make_batch(&[i as i64])).collect();

        let posted = run_coalescer(input, false);

        assert_eq!(
            posted.len(),
            3,
            "{rows} rows should post 2 full batches plus a remainder"
        );
        assert_eq!(
            posted.iter().map(|b| b.num_rows()).sum::<usize>(),
            rows,
            "coalescing must not drop rows"
        );
        assert_eq!(
            values_of(&posted),
            (0..rows as i64).collect::<Vec<_>>(),
            "coalescing must preserve scan order"
        );
        assert!(
            posted[0].num_rows() >= SCAN_TARGET_BATCH_ROWS,
            "flushed batches should reach the target"
        );
    }

    // A batch that is already exactly the target size must be passed through,
    // not rebuilt. arrow's bypass triggers on `size > limit`, so a limit equal
    // to the target would copy every exactly-target-sized batch -- and that is
    // precisely the size compaction seals and the retention rewrite emits, via
    // DataFusion's `execution.batch_size` default. Pointer identity on the
    // column is what distinguishes a pass-through from a rebuild.
    #[test]
    fn test_exact_target_size_batch_is_not_copied() {
        let values: Vec<i64> = (0..SCAN_TARGET_BATCH_ROWS as i64).collect();
        let batch = make_batch(&values);
        assert_eq!(batch.num_rows(), SCAN_TARGET_BATCH_ROWS);
        let original = batch.column(0).clone();

        let posted = run_coalescer(vec![batch], false);

        assert_eq!(posted.len(), 1, "one batch in, one batch out");
        assert_eq!(posted[0].num_rows(), SCAN_TARGET_BATCH_ROWS);
        assert!(
            Arc::ptr_eq(&original, posted[0].column(0)),
            "an exactly-target-sized batch must bypass coalescing, not be rebuilt"
        );
    }

    // A scan that yields only empty batches must post nothing, and must not
    // leave them buffered.
    #[test]
    fn test_scan_coalescer_drops_empty_batches() {
        let input: Vec<RecordBatch> = (0..100).map(|_| make_batch(&[])).collect();

        let posted = run_coalescer(input, false);

        assert!(posted.is_empty(), "empty batches should post no messages");
    }

    // The final partial batch must still be emitted; losing it would silently
    // truncate every result smaller than the target.
    #[test]
    fn test_scan_coalescer_flushes_remainder() {
        let posted = run_coalescer(vec![make_batch(&[1, 2]), make_batch(&[3])], false);

        assert_eq!(posted.len(), 1, "a short scan posts a single batch");
        assert_eq!(values_of(&posted), vec![1, 2, 3]);
    }

    // COUNT(*) scans project to zero columns, where the row count lives in
    // RecordBatch metadata rather than in any array. Coalescing and then the
    // Arrow IPC roundtrip must both carry it: if either dropped it, every
    // COUNT(*) in the dashboard would silently return 0.
    #[test]
    fn test_empty_projection_survives_coalesce_and_ipc_roundtrip() {
        // Batches go in with columns, as they arrive from the stream; the
        // zero-column projection is applied by the scan path being exercised.
        let input = vec![make_batch(&[1, 2]), make_batch(&[3])];

        let posted = run_coalescer(input, true);

        assert_eq!(posted.len(), 1);
        assert_eq!(posted[0].num_columns(), 0, "projection yields no columns");
        assert_eq!(
            posted[0].num_rows(),
            3,
            "2+1 zero-column rows coalesce to 3"
        );

        let wire = serialize_batch(&posted[0]).expect("serialize zero-column batch");
        let decoded = deserialize_one_batch(&wire).expect("decode zero-column batch");

        assert_eq!(
            decoded.num_rows(),
            3,
            "row count must survive the wire, or COUNT(*) reads 0"
        );
    }

    // Coalescing preserves every row when batches carry columns.
    #[test]
    fn test_concat_preserves_rows() {
        let batches = [
            make_batch(&[1, 2]),
            make_batch(&[3]),
            make_batch(&[4, 5, 6]),
        ];

        let merged = concat_batches(&batches[0].schema(), batches.iter()).unwrap();

        assert_eq!(merged.num_rows(), 6, "3 batches of 2+1+3 rows merge to 6");
        let values = merged
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            values.values(),
            &[1, 2, 3, 4, 5, 6],
            "coalescing must preserve row order across batches"
        );
    }
}
