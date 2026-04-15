/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Snapshot capture service.
//!
//! [`SnapshotService`] owns the capture pipeline as a single
//! operation and publishes to configured sinks (live [`TableStore`],
//! durable bundle, or both).
//!
//! The service captures once via BFS, drains to `RecordBatch` pairs
//! once via [`drain_to_batches`], and publishes the same batches to
//! whichever sinks are active.
//!
//! # Usage
//!
//! Both [`SnapshotService::capture`] and [`spawn_periodic_capture`]
//! take a *resolver* — a closure `Fn(&NodeRef) ->
//! Future<Result<NodePayload>>` that resolves a single node reference
//! via the mesh admin. In production this calls
//! `MeshAdminAgent::resolve`; in tests it can be a stub backed by a
//! `HashMap`.
//!
//! For [`spawn_periodic_capture`], a *resolver factory* `Fn() ->
//! resolver` is passed instead, producing a fresh resolver per tick.
//!
//! **One-shot capture** — capture a mesh snapshot on demand:
//!
//! ```ignore
//! // Build a resolver that calls MeshAdminAgent::resolve for
//! // each NodeRef.
//! let resolve = |node_ref: &NodeRef| {
//!     let admin_ref = admin_ref.clone();
//!     let ref_string = node_ref.to_string();
//!     async move {
//!         let resp = admin_ref.resolve(instance, ref_string).await?;
//!         resp.0.map_err(|e| anyhow::anyhow!("{}", e))
//!     }
//! };
//!
//! let service = SnapshotService::new(Some(table_store));
//! let result = service.capture(resolve, None).await?;
//! println!("{} nodes captured", result.node_counts.nodes);
//! ```
//!
//! [`capture`](SnapshotService::capture) is the full pipeline: BFS
//! traversal, drain to `RecordBatch` pairs, publish to all active
//! sinks. At least one sink (`table_store` or `export_root`) must be
//! active.
//!
//! **Periodic capture** — run the capture pipeline on a timer:
//!
//! ```ignore
//! // Factory produces a fresh resolver per tick.
//! let make_resolve = || {
//!     let admin_ref = admin_ref.clone();
//!     move |node_ref: &NodeRef| {
//!         let admin_ref = admin_ref.clone();
//!         let ref_string = node_ref.to_string();
//!         async move {
//!             let resp = admin_ref.resolve(instance, ref_string).await?;
//!             resp.0.map_err(|e| anyhow::anyhow!("{}", e))
//!         }
//!     }
//! };
//!
//! let service = SnapshotService::new(Some(table_store));
//! let cancel = CancellationToken::new();
//! let handle = spawn_periodic_capture(
//!     service.clone(),
//!     Duration::from_secs(30),
//!     cancel.clone(),
//!     make_resolve,
//! )?;
//!
//! // ... later, shut down:
//! cancel.cancel();
//! handle.await?;
//! ```
//!
//! [`spawn_periodic_capture`] reuses the same capture pipeline but is
//! live-ingest only (`export_root` is always `None`). Overlapping
//! ticks are skipped, not queued. Capture errors are logged and do
//! not stop the timer.
//!
//! # Service invariants (SV-*)
//!
//! - **SV-1 (sink required):** `capture` returns `Err` when both
//!   `table_store` and `export_root` are `None`.
//! - **SV-2 (single capture):** Each `capture` call performs exactly
//!   one BFS traversal and one `drain_to_batches`.
//! - **SV-3 (table-store publication):** When `table_store` is
//!   `Some`, all 9 tables are ingested (delegates to PS-1..PS-7).
//!   Publication is not atomic — a failure partway through may leave
//!   some tables ingested and others not.
//! - **SV-4 (counts before drain):** [`NodeCounts`] is computed from
//!   [`SnapshotData`] before `drain_to_batches` consumes it.
//! - **SV-5 (metadata correctness):** [`CaptureResult`] contains the
//!   snapshot ID generated for this capture, `snapshot_ts` from the
//!   snapshot row produced by `capture_snapshot`, accurate node
//!   counts, non-negative wall-clock duration, and `bundle_path`
//!   when a bundle was exported.
//! - **SV-6 (bundle path derivation):** When `export_root` is `Some`,
//!   the service derives the bundle directory as
//!   `{export_root}/snapshot-{snapshot_id}/` after generating the
//!   snapshot ID. The two cannot diverge (delegates to BN-7).
//! - **SV-7 (cross-sink non-atomicity):** When both sinks are active,
//!   the operation is not atomic across sinks. If `TableStore` ingest
//!   succeeds and bundle writing fails (or vice versa), you have
//!   partial success. The sinks are independent and the service
//!   reports the error.
//!
//! # Periodic-trigger invariants (PT-*)
//!
//! - **PT-1 (positive interval):** Zero interval rejected before
//!   spawn.
//! - **PT-2 (live sink required):** `table_store.is_some()` required.
//! - **PT-3 (delayed first fire):** First capture after one full
//!   interval.
//! - **PT-4 (single in-flight):** Overlapping ticks skipped via CAS.
//!   `in_flight` is consulted only by the periodic loop; on-demand
//!   `capture` calls do not check it.
//! - **PT-5 (cancellation boundary):** Stops future ticks; does not
//!   interrupt in-flight capture.
//! - **PT-6 (failure resilience):** Capture `Err` logged, loop
//!   continues.
//! - **PT-7 (live-ingest only):** Always `export_root = None`.

use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeRef;
use monarch_distributed_telemetry::database_scanner::TableStore;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::bundle::write_bundle;
use crate::capture::SnapshotData;
use crate::capture::capture_snapshot;
use crate::push::drain_to_batches;

/// Snapshot capture service.
///
/// Owns the capture-and-publish pipeline. Captures once per call and
/// publishes to configured sinks.
#[derive(Clone)]
pub struct SnapshotService {
    /// `None` before telemetry integration, `Some` after. When
    /// present, captures are ingested into live storage.
    table_store: Option<TableStore>,
    /// Overlap guard for the periodic trigger. CAS to acquire, reset
    /// on completion.
    in_flight: Arc<AtomicBool>,
}

impl SnapshotService {
    /// Create a new snapshot service.
    ///
    /// When `table_store` is `Some`, captured snapshots are ingested
    /// into live telemetry storage. When `None`, only bundle export
    /// is available as a sink.
    pub fn new(table_store: Option<TableStore>) -> Self {
        Self {
            table_store,
            in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Capture a mesh snapshot and publish to configured sinks.
    ///
    /// Captures once via BFS, drains to `RecordBatch` pairs once,
    /// then publishes to whichever sinks are active:
    /// - If `self.table_store` is `Some`, ingest into live storage.
    /// - If `export_root` is `Some`, write a durable bundle to
    ///   `{export_root}/snapshot-{snapshot_id}/`.
    ///
    /// At least one sink must be active (`table_store` or
    /// `export_root`), otherwise the capture has no destination and
    /// returns an error (SV-1).
    pub async fn capture<F, Fut>(
        &self,
        resolve: F,
        export_root: Option<&Path>,
    ) -> anyhow::Result<CaptureResult>
    where
        F: Fn(&NodeRef) -> Fut,
        Fut: Future<Output = anyhow::Result<NodePayload>>,
    {
        // SV-1: at least one destination must be active.
        if self.table_store.is_none() && export_root.is_none() {
            anyhow::bail!(
                "snapshot capture requires at least one active sink \
                 (table_store or export_root)"
            );
        }

        let snapshot_id = Uuid::new_v4().to_string();
        let t0 = Instant::now();
        let data = capture_snapshot(&snapshot_id, &resolve).await?;
        // SV-4: counts computed before drain consumes data.
        let node_counts = NodeCounts::from_data(&data);
        let snapshot_ts = data.snapshot.snapshot_ts;

        // SV-2: drain once, publish to all active sinks from the
        // same batches.
        let batches = drain_to_batches(data)?;

        if let Some(ref store) = self.table_store {
            for (name, batch) in &batches {
                store.ingest_batch(name, batch.clone()).await?;
            }
        }

        // SV-6: service owns snapshot_id and derives directory name.
        let bundle_path = match export_root {
            Some(root) => {
                let dir = root.join(format!("snapshot-{}", snapshot_id));
                write_bundle(&dir, &batches)?;
                Some(dir)
            }
            None => None,
        };

        Ok(CaptureResult {
            snapshot_id,
            snapshot_ts,
            node_counts,
            capture_duration_ms: t0.elapsed().as_secs_f64() * 1000.0,
            bundle_path,
        })
    }
}

/// Private drop guard that resets `in_flight` to `false` on all exit
/// paths (success, error, or early return).
struct InFlightGuard<'a>(&'a AtomicBool);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Execute one periodic capture tick.
///
/// Owns the CAS overlap check (PT-4), drop guard, capture call
/// (PT-7: always `export_root = None`), and error logging (PT-6).
/// Returns `true` if a capture was attempted (CAS succeeded),
/// `false` if skipped due to overlap.
///
/// This is the unit of per-tick behavior, factored out of the
/// spawned timer loop so it can be tested deterministically.
async fn run_periodic_tick<F, Fut>(service: &SnapshotService, resolve: F) -> bool
where
    F: Fn(&NodeRef) -> Fut,
    Fut: Future<Output = anyhow::Result<NodePayload>>,
{
    // PT-4: CAS to acquire overlap guard.
    if service
        .in_flight
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        tracing::warn!("periodic capture skipped: previous capture still in flight");
        return false;
    }
    let _guard = InFlightGuard(&service.in_flight);

    // PT-7: always None for export_root.
    match service.capture(resolve, None).await {
        Ok(_result) => {}
        // PT-6: log and continue.
        Err(e) => tracing::warn!("periodic capture failed: {:#}", e),
    }
    true
}

/// Tick source for the periodic capture loop.
///
/// Production uses [`IntervalTick`]; tests use [`NotifyTick`].
trait TickSource {
    /// Wait for the next tick.
    fn tick(&mut self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Production tick source backed by `tokio::time::Interval`.
struct IntervalTick {
    interval: tokio::time::Interval,
}

impl TickSource for IntervalTick {
    fn tick(&mut self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {
            self.interval.tick().await;
        })
    }
}

/// Per-tick completion callback.
///
/// Called after each tick is fully processed (capture completed or
/// skipped). Production passes [`NoOpDone`]; tests pass
/// [`NotifyDone`] to synchronize without `yield_now`.
trait OnTickDone {
    fn done(&self);
}

/// Production no-op completion signal.
struct NoOpDone;
impl OnTickDone for NoOpDone {
    fn done(&self) {}
}

/// Private loop driver for periodic capture.
///
/// Separates tick scheduling from tick handling so tests can drive
/// ticks manually. Production passes an [`IntervalTick`]; tests
/// pass a [`NotifyTick`]. The `on_tick_done` callback fires after
/// each tick is fully processed.
///
/// The loop uses `biased` select with cancellation first (PT-5).
async fn run_periodic_loop<MkResolve, F, Fut>(
    service: SnapshotService,
    cancel: CancellationToken,
    make_resolve: MkResolve,
    mut ticks: impl TickSource,
    on_tick_done: impl OnTickDone,
) where
    MkResolve: Fn() -> F,
    F: Fn(&NodeRef) -> Fut,
    Fut: Future<Output = anyhow::Result<NodePayload>>,
{
    loop {
        tokio::select! {
            biased;

            // PT-5: cancellation checked first via biased select.
            _ = cancel.cancelled() => {
                break;
            }

            _ = ticks.tick() => {
                run_periodic_tick(&service, make_resolve()).await;
                on_tick_done.done();
            }
        }
    }
}

/// Spawn a periodic snapshot capture timer.
///
/// Returns `Err` immediately if `interval` is zero (PT-1) or the
/// service has no `table_store` (PT-2). On success, returns a
/// `JoinHandle` for the spawned timer task. The task owns a cloned
/// `SnapshotService` by value and shares the same `in_flight` guard
/// and `TableStore` as the original.
pub fn spawn_periodic_capture<MkResolve, F, Fut>(
    service: SnapshotService,
    interval: Duration,
    cancel: CancellationToken,
    make_resolve: MkResolve,
) -> anyhow::Result<JoinHandle<()>>
where
    MkResolve: Fn() -> F + Send + Sync + 'static,
    F: Fn(&NodeRef) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<NodePayload>> + Send + 'static,
{
    // PT-1: reject zero interval.
    anyhow::ensure!(
        !interval.is_zero(),
        "periodic capture interval must be non-zero"
    );

    // PT-2: reject if no table_store.
    anyhow::ensure!(
        service.table_store.is_some(),
        "periodic capture requires a table_store"
    );

    let handle = tokio::spawn(async move {
        // PT-3: first fire after one full interval.
        let start = tokio::time::Instant::now() + interval;
        let mut timer = tokio::time::interval_at(start, interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        run_periodic_loop(
            service,
            cancel,
            make_resolve,
            IntervalTick { interval: timer },
            NoOpDone,
        )
        .await;
    });

    Ok(handle)
}

/// Result metadata from a snapshot capture operation.
#[derive(Debug, Clone, Serialize)]
pub struct CaptureResult {
    /// Unique identifier for the snapshot (UUID v4).
    pub snapshot_id: String,
    /// Capture timestamp in microseconds since epoch.
    pub snapshot_ts: i64,
    /// Summary counts of captured entities.
    pub node_counts: NodeCounts,
    /// Wall-clock capture duration in milliseconds.
    pub capture_duration_ms: f64,
    /// Filesystem path to the bundle directory, if a bundle was
    /// exported. `None` when `export_root` was not provided.
    pub bundle_path: Option<PathBuf>,
}

/// Summary counts of entities in a captured snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCounts {
    pub nodes: usize,
    pub children: usize,
    pub root_nodes: usize,
    pub host_nodes: usize,
    pub proc_nodes: usize,
    pub actor_nodes: usize,
    pub actor_failures: usize,
    pub resolution_errors: usize,
}

impl NodeCounts {
    /// Compute counts from a [`SnapshotData`] before it is consumed.
    pub fn from_data(data: &SnapshotData) -> Self {
        Self {
            nodes: data.nodes.len(),
            children: data.children.len(),
            root_nodes: data.root_nodes.len(),
            host_nodes: data.host_nodes.len(),
            proc_nodes: data.proc_nodes.len(),
            actor_nodes: data.actor_nodes.len(),
            actor_failures: data.actor_failures.len(),
            resolution_errors: data.resolution_errors.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use hyperactor::channel::ChannelAddr;
    use hyperactor::reference::ProcId;
    use hyperactor_mesh::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
    use hyperactor_mesh::introspect::NodeProperties;
    use hyperactor_mesh::introspect::NodeRef;

    use super::*;
    use crate::schema::*;

    // --- Fixtures ---

    const PROC_NAME: &str = "worker";
    /// Used both as the name component when constructing actor IDs
    /// and as the `actor_type` string in test fixtures.
    const ACTOR_TYPE: &str = "test_actor";

    fn test_proc_id() -> ProcId {
        ProcId::with_name(ChannelAddr::Local(0), PROC_NAME)
    }

    /// Build a stub resolver backed by a `HashMap`.
    ///
    /// Returns `std::future::Ready` to avoid lifetime issues with
    /// `Fn(&NodeRef) -> Fut` — same pattern as `capture.rs` tests.
    fn stub_resolver(
        payloads: HashMap<NodeRef, NodePayload>,
    ) -> impl Fn(&NodeRef) -> std::future::Ready<anyhow::Result<NodePayload>> {
        move |node_ref: &NodeRef| {
            let result = payloads
                .get(node_ref)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown ref: {}", node_ref));
            std::future::ready(result)
        }
    }

    /// Build a minimal mesh topology: root → host → proc → actor.
    fn minimal_mesh_payloads() -> HashMap<NodeRef, NodePayload> {
        let proc_id = test_proc_id();
        let host_actor_id = proc_id.actor_id(HOST_MESH_AGENT_ACTOR_NAME);
        let actor_id = proc_id.actor_id(ACTOR_TYPE);

        let host_ref = NodeRef::Host(host_actor_id.clone());
        let proc_ref = NodeRef::Proc(proc_id.clone());
        let actor_ref = NodeRef::Actor(actor_id.clone());

        let now = SystemTime::now();

        let mut payloads = HashMap::new();

        payloads.insert(
            NodeRef::Root,
            NodePayload {
                identity: NodeRef::Root,
                properties: NodeProperties::Root {
                    num_hosts: 1,
                    started_at: now,
                    started_by: "test".to_owned(),
                    system_children: vec![],
                },
                children: vec![host_ref.clone()],
                parent: None,
                as_of: now,
            },
        );

        payloads.insert(
            host_ref.clone(),
            NodePayload {
                identity: host_ref.clone(),
                properties: NodeProperties::Host {
                    addr: "10.0.0.1".to_owned(),
                    num_procs: 1,
                    system_children: vec![],
                },
                children: vec![proc_ref.clone()],
                parent: Some(NodeRef::Root),
                as_of: now,
            },
        );

        payloads.insert(
            proc_ref.clone(),
            NodePayload {
                identity: proc_ref.clone(),
                properties: NodeProperties::Proc {
                    proc_name: PROC_NAME.to_owned(),
                    num_actors: 1,
                    system_children: vec![],
                    stopped_children: vec![],
                    stopped_retention_cap: 100,
                    is_poisoned: false,
                    failed_actor_count: 0,
                },
                children: vec![actor_ref.clone()],
                parent: Some(host_ref),
                as_of: now,
            },
        );

        payloads.insert(
            actor_ref.clone(),
            NodePayload {
                identity: actor_ref.clone(),
                properties: NodeProperties::Actor {
                    actor_status: "running".to_owned(),
                    actor_type: ACTOR_TYPE.to_owned(),
                    messages_processed: 42,
                    created_at: Some(now),
                    last_message_handler: Some("handle_msg".to_owned()),
                    total_processing_time_us: 5000,
                    flight_recorder: None,
                    is_system: false,
                    failure_info: None,
                },
                children: vec![],
                parent: Some(proc_ref),
                as_of: now,
            },
        );

        payloads
    }

    // --- NodeCounts tests (SV-4) ---

    // SV-4: counts reflect source data accurately.
    #[test]
    fn test_node_counts_from_data_populated() {
        let data = SnapshotData {
            snapshot: SnapshotRow {
                snapshot_id: "nc1".to_owned(),
                snapshot_ts: 1_000_000,
            },
            nodes: vec![
                NodeRow {
                    snapshot_id: "nc1".to_owned(),
                    node_id: "root".to_owned(),
                    node_kind: "root".to_owned(),
                    as_of: 1_000_000,
                },
                NodeRow {
                    snapshot_id: "nc1".to_owned(),
                    node_id: "actor1".to_owned(),
                    node_kind: "actor".to_owned(),
                    as_of: 1_000_000,
                },
            ],
            children: vec![ChildRow {
                snapshot_id: "nc1".to_owned(),
                parent_id: "root".to_owned(),
                child_id: "actor1".to_owned(),
                child_sort_key: 0,
                is_system: false,
                is_stopped: false,
            }],
            root_nodes: vec![RootNodeRow {
                snapshot_id: "nc1".to_owned(),
                node_id: "root".to_owned(),
                num_hosts: 0,
                started_at: 1_000_000,
                started_by: "test".to_owned(),
            }],
            host_nodes: vec![],
            proc_nodes: vec![],
            actor_nodes: vec![ActorNodeRow {
                snapshot_id: "nc1".to_owned(),
                node_id: "actor1".to_owned(),
                actor_status: "running".to_owned(),
                actor_type: "test".to_owned(),
                messages_processed: 0,
                created_at: None,
                last_message_handler: None,
                total_processing_time_us: 0,
                is_system: false,
            }],
            actor_failures: vec![],
            resolution_errors: vec![],
        };

        let counts = NodeCounts::from_data(&data);
        assert_eq!(
            counts,
            NodeCounts {
                nodes: 2,
                children: 1,
                root_nodes: 1,
                host_nodes: 0,
                proc_nodes: 0,
                actor_nodes: 1,
                actor_failures: 0,
                resolution_errors: 0,
            }
        );
    }

    // SV-4: zero-length vectors produce zero counts.
    #[test]
    fn test_node_counts_from_data_empty() {
        let data = SnapshotData {
            snapshot: SnapshotRow {
                snapshot_id: "nc2".to_owned(),
                snapshot_ts: 1_000_000,
            },
            nodes: vec![],
            children: vec![],
            root_nodes: vec![],
            host_nodes: vec![],
            proc_nodes: vec![],
            actor_nodes: vec![],
            actor_failures: vec![],
            resolution_errors: vec![],
        };

        let counts = NodeCounts::from_data(&data);
        assert_eq!(
            counts,
            NodeCounts {
                nodes: 0,
                children: 0,
                root_nodes: 0,
                host_nodes: 0,
                proc_nodes: 0,
                actor_nodes: 0,
                actor_failures: 0,
                resolution_errors: 0,
            }
        );
    }

    // SV-5: NodeCounts serializes correctly for CaptureResult JSON.
    #[test]
    fn test_node_counts_serializes_to_json() {
        let counts = NodeCounts {
            nodes: 10,
            children: 15,
            root_nodes: 1,
            host_nodes: 2,
            proc_nodes: 3,
            actor_nodes: 4,
            actor_failures: 0,
            resolution_errors: 0,
        };
        let json = serde_json::to_string(&counts).unwrap();
        assert!(json.contains("\"nodes\":10"));
        assert!(json.contains("\"actor_failures\":0"));
    }

    // --- CaptureResult tests (SV-5) ---

    // SV-5: CaptureResult JSON contains all expected fields.
    #[test]
    fn test_capture_result_serializes_to_json() {
        let result = CaptureResult {
            snapshot_id: "test-snap".to_owned(),
            snapshot_ts: 1_000_000,
            node_counts: NodeCounts {
                nodes: 4,
                children: 3,
                root_nodes: 1,
                host_nodes: 1,
                proc_nodes: 1,
                actor_nodes: 1,
                actor_failures: 0,
                resolution_errors: 0,
            },
            capture_duration_ms: 42.5,
            bundle_path: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"snapshot_id\":\"test-snap\""));
        assert!(json.contains("\"snapshot_ts\":1000000"));
        assert!(json.contains("\"capture_duration_ms\":42.5"));
        assert!(json.contains("\"node_counts\":{"));
        assert!(json.contains("\"bundle_path\":null"));
    }

    // --- SnapshotService::capture tests ---

    // SV-2, SV-3, SV-4, SV-5: capture with table_store populates the
    // store and returns correct metadata.
    #[tokio::test]
    async fn test_capture_with_table_store() {
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));

        let result = service.capture(resolve, None).await.unwrap();

        // CaptureResult metadata.
        assert!(!result.snapshot_id.is_empty());
        assert!(result.snapshot_ts > 0);
        assert!(result.capture_duration_ms >= 0.0);
        assert!(result.bundle_path.is_none());
        assert_eq!(result.node_counts.nodes, 4); // root, host, proc, actor
        assert_eq!(result.node_counts.children, 3); // root→host, host→proc, proc→actor
        assert_eq!(result.node_counts.root_nodes, 1);
        assert_eq!(result.node_counts.host_nodes, 1);
        assert_eq!(result.node_counts.proc_nodes, 1);
        assert_eq!(result.node_counts.actor_nodes, 1);
        assert_eq!(result.node_counts.actor_failures, 0);
        assert_eq!(result.node_counts.resolution_errors, 0);

        // Verify store is populated — all 9 tables registered.
        let names = store.table_names().unwrap();
        assert_eq!(names.len(), 9);

        // Verify data is queryable — snapshot row exists.
        let ctx = datafusion::prelude::SessionContext::new();
        if let Some(provider) = store.table_provider("snapshots").unwrap() {
            ctx.register_table("snapshots", provider).unwrap();
        }
        let df = ctx
            .sql(&format!(
                "SELECT snapshot_id FROM snapshots WHERE snapshot_id = '{}'",
                result.snapshot_id
            ))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    // SV-1: capture with no sinks errors.
    #[tokio::test]
    async fn test_capture_no_sinks_errors() {
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);
        let service = SnapshotService::new(None);

        let err = service.capture(resolve, None).await.unwrap_err();
        assert!(
            err.to_string().contains("at least one active sink"),
            "unexpected error: {}",
            err,
        );
    }

    // SV-6, BN-7: capture with export_root creates a bundle directory
    // named snapshot-{snapshot_id}.
    #[tokio::test]
    async fn test_capture_with_export_root() {
        let dir = tempfile::tempdir().unwrap();
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);
        let service = SnapshotService::new(None);

        let result = service.capture(resolve, Some(dir.path())).await.unwrap();

        // BN-7: bundle_path derives from export_root + snapshot_id.
        let bundle_path = result.bundle_path.as_ref().unwrap();
        assert_eq!(
            bundle_path.file_name().unwrap().to_str().unwrap(),
            format!("snapshot-{}", result.snapshot_id),
        );
        assert!(bundle_path.exists());
        assert!(bundle_path.join("manifest.json").exists());
    }

    // SV-7: both sinks active — table_store populated AND bundle
    // written.
    #[tokio::test]
    async fn test_capture_both_sinks() {
        let dir = tempfile::tempdir().unwrap();
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));

        let result = service.capture(resolve, Some(dir.path())).await.unwrap();

        // Table store populated.
        assert_eq!(store.table_names().unwrap().len(), 9);

        // Bundle written.
        let bundle_path = result.bundle_path.as_ref().unwrap();
        assert!(bundle_path.join("manifest.json").exists());

        // Both agree on snapshot_id.
        let raw = std::fs::read_to_string(bundle_path.join("manifest.json")).unwrap();
        let manifest: crate::bundle::BundleManifest = serde_json::from_str(&raw).unwrap();
        assert_eq!(manifest.snapshot_id, result.snapshot_id);
    }

    // --- Periodic trigger tests (PT-*) ---
    //
    // Test split:
    // - PT-1, PT-2: direct spawn_periodic_capture precondition tests
    // - PT-3, PT-5: deterministic loop tests via run_periodic_loop
    //   with NotifyTick (manual tick source)
    // - PT-4, PT-6, PT-7: direct run_periodic_tick tests

    type PinFut = std::pin::Pin<Box<dyn Future<Output = anyhow::Result<NodePayload>> + Send>>;

    /// Test tick source backed by `tokio::sync::Notify`.
    /// Each `notify_one()` on the held `Arc<Notify>` fires one tick.
    struct NotifyTick(Arc<tokio::sync::Notify>);

    impl TickSource for NotifyTick {
        fn tick(&mut self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            let n = self.0.clone();
            Box::pin(async move { n.notified().await })
        }
    }

    /// Test completion signal. The loop calls `done()` after each
    /// tick is fully processed; tests await `done_signal.notified()`
    /// to synchronize deterministically.
    struct NotifyDone(Arc<tokio::sync::Notify>);

    impl OnTickDone for NotifyDone {
        fn done(&self) {
            self.0.notify_one();
        }
    }

    /// Build a resolver factory for tests that exercise
    /// spawn_periodic_capture with a real timer (production timer
    /// PT-3 test) and for the PT-5 in-flight cancellation test.
    fn periodic_resolver_factory(
        payloads: HashMap<NodeRef, NodePayload>,
        counter: Arc<std::sync::atomic::AtomicUsize>,
        gate: Option<Arc<tokio::sync::Notify>>,
    ) -> impl Fn() -> Box<dyn Fn(&NodeRef) -> PinFut + Send + Sync> + Send + Sync + 'static {
        move || {
            let payloads = payloads.clone();
            let counter = counter.clone();
            let gate = gate.clone();
            Box::new(move |node_ref: &NodeRef| {
                let result = payloads
                    .get(node_ref)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("unknown ref: {}", node_ref));
                let is_root = *node_ref == NodeRef::Root;
                let counter = counter.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    if is_root {
                        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if let Some(g) = gate {
                            g.notified().await;
                        }
                    }
                    result
                })
            })
        }
    }

    // --- PT-1, PT-2: spawn precondition tests (sync) ---

    /// Dummy resolver for precondition tests where the resolver is
    /// never called.
    fn unused_resolver(_: &NodeRef) -> std::future::Ready<anyhow::Result<NodePayload>> {
        std::future::ready(Err(anyhow::anyhow!("unused")))
    }

    // PT-1: zero interval rejected.
    #[test]
    fn test_periodic_rejects_zero_interval() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store));
        let cancel = CancellationToken::new();

        let err = spawn_periodic_capture(service, Duration::ZERO, cancel, || unused_resolver);
        assert!(err.is_err(), "PT-1: should reject zero interval");
        assert!(
            err.unwrap_err().to_string().contains("non-zero"),
            "PT-1: error should mention non-zero",
        );
    }

    // PT-2: no table_store rejected.
    #[test]
    fn test_periodic_rejects_no_store() {
        let service = SnapshotService::new(None);
        let cancel = CancellationToken::new();

        let err =
            spawn_periodic_capture(service, Duration::from_secs(1), cancel, || unused_resolver);
        assert!(err.is_err(), "PT-2: should reject no store");
        assert!(
            err.unwrap_err().to_string().contains("table_store"),
            "PT-2: error should mention table_store",
        );
    }

    // PT-3: no capture before a tick is sent; exactly one after. Uses
    // run_periodic_loop with NotifyTick and NotifyDone for
    // deterministic synchronization.
    #[tokio::test]
    async fn test_periodic_delayed_first_fire() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));
        let cancel = CancellationToken::new();
        let payloads = minimal_mesh_payloads();

        let tick_signal = Arc::new(tokio::sync::Notify::new());
        let done_signal = Arc::new(tokio::sync::Notify::new());

        let handle = tokio::spawn({
            let cancel = cancel.clone();
            let payloads = payloads.clone();
            let tick_signal = tick_signal.clone();
            let done_signal = done_signal.clone();
            async move {
                run_periodic_loop(
                    service,
                    cancel,
                    || stub_resolver(payloads.clone()),
                    NotifyTick(tick_signal),
                    NotifyDone(done_signal),
                )
                .await;
            }
        });

        // No tick sent yet — no capture should have occurred. The
        // loop is blocked on NotifyTick, so the store is empty.
        assert_eq!(
            store.table_names().unwrap().len(),
            0,
            "PT-3: no capture before tick"
        );

        // Send one tick and await the completion signal.
        tick_signal.notify_one();
        done_signal.notified().await;

        assert_eq!(
            store.table_names().unwrap().len(),
            9,
            "PT-3: one capture after tick"
        );

        cancel.cancel();
        handle.await.unwrap();
    }

    // PT-3 (production timer): spawn_periodic_capture with a real
    // interval does not fire before one full interval. This is the
    // one test that exercises the interval_at(now + interval,
    // interval) construction in spawn_periodic_capture itself.
    #[tokio::test]
    async fn test_periodic_production_timer_delay() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));
        let cancel = CancellationToken::new();
        let payloads = minimal_mesh_payloads();
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let factory = periodic_resolver_factory(payloads, counter.clone(), None);
        let interval = Duration::from_millis(200);

        let handle = spawn_periodic_capture(service, interval, cancel.clone(), factory).unwrap();

        // Well before the first interval — no capture.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "PT-3: no capture before first interval (production timer)",
        );

        // Wait long enough for the first tick to fire and capture
        // to complete.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            counter.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "PT-3: at least one capture after interval (production timer)",
        );

        cancel.cancel();
        handle.await.unwrap();
    }

    // --- PT-4, PT-6, PT-7: direct run_periodic_tick tests ---
    //
    // These test the per-tick helper directly — no tokio::spawn, no
    // timers, fully deterministic.

    // PT-4: CAS skip when in_flight is already set.
    #[tokio::test]
    async fn test_periodic_tick_skips_when_in_flight() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);

        // Pre-set in_flight to simulate an ongoing capture.
        service.in_flight.store(true, Ordering::Release);

        let attempted = run_periodic_tick(&service, resolve).await;
        assert!(!attempted, "PT-4: tick should be skipped when in_flight");

        // Store should be empty — no capture ran.
        assert_eq!(store.table_names().unwrap().len(), 0);

        // in_flight should still be true (guard did not run).
        assert!(service.in_flight.load(Ordering::Acquire));
    }

    // PT-4: CAS succeeds and guard resets in_flight after capture.
    #[tokio::test]
    async fn test_periodic_tick_captures_and_resets_guard() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);

        let attempted = run_periodic_tick(&service, resolve).await;
        assert!(attempted, "PT-4: tick should attempt capture");

        // Store should be populated.
        assert_eq!(store.table_names().unwrap().len(), 9);

        // in_flight should be reset to false.
        assert!(!service.in_flight.load(Ordering::Acquire));
    }

    // PT-6: resolver error does not prevent the tick from completing
    // and the guard is still reset.
    #[tokio::test]
    async fn test_periodic_tick_survives_resolver_error() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));

        let resolve = |_: &NodeRef| std::future::ready(Err(anyhow::anyhow!("simulated failure")));

        let attempted = run_periodic_tick(&service, resolve).await;
        assert!(
            attempted,
            "PT-6: tick should attempt capture even if it fails",
        );

        // in_flight should be reset to false despite the error.
        assert!(!service.in_flight.load(Ordering::Acquire));

        // Store should be empty — capture failed before ingestion.
        assert_eq!(store.table_names().unwrap().len(), 0);

        // PT-6 continued: a later tick on the same service succeeds
        // after an earlier failure.
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);
        let attempted = run_periodic_tick(&service, resolve).await;
        assert!(attempted, "PT-6: second tick should succeed");
        assert_eq!(store.table_names().unwrap().len(), 9);
    }

    // PT-7: run_periodic_tick always passes export_root = None.
    #[tokio::test]
    async fn test_periodic_tick_no_bundle_export() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));
        let payloads = minimal_mesh_payloads();
        let resolve = stub_resolver(payloads);

        run_periodic_tick(&service, resolve).await;

        // Verify store was populated (live ingest happened).
        assert_eq!(store.table_names().unwrap().len(), 9);

        // PT-7 is structural: run_periodic_tick calls
        // service.capture(resolve, None). No bundle directory
        // was created.
    }

    // --- PT-5: deterministic loop tests via run_periodic_loop ---

    // PT-5 (idle): cancel before any tick, loop exits, zero captures.
    #[tokio::test]
    async fn test_periodic_cancel_while_idle() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));
        let cancel = CancellationToken::new();
        let payloads = minimal_mesh_payloads();
        let tick_signal = Arc::new(tokio::sync::Notify::new());

        // Cancel immediately — before any tick is sent.
        cancel.cancel();

        run_periodic_loop(
            service,
            cancel,
            || stub_resolver(payloads.clone()),
            NotifyTick(tick_signal),
            NoOpDone,
        )
        .await;

        assert_eq!(
            store.table_names().unwrap().len(),
            0,
            "PT-5: no captures after cancel while idle",
        );
    }

    // PT-5 (in-flight): cancel during a gated capture, capture
    // finishes, then the loop exits. No second capture.
    //
    // Synchronization:
    // - started_signal: resolver notifies when root resolution begins
    // - resolver_gate: test releases to let the capture finish
    // - done_signal: loop notifies when tick is fully processed
    #[tokio::test]
    async fn test_periodic_cancel_during_inflight() {
        let store = TableStore::new_empty();
        let service = SnapshotService::new(Some(store.clone()));
        let cancel = CancellationToken::new();
        let payloads = minimal_mesh_payloads();
        let started_signal = Arc::new(tokio::sync::Notify::new());
        let resolver_gate = Arc::new(tokio::sync::Notify::new());
        let tick_signal = Arc::new(tokio::sync::Notify::new());
        let done_signal = Arc::new(tokio::sync::Notify::new());

        let handle = tokio::spawn({
            let cancel = cancel.clone();
            let payloads = payloads.clone();
            let started_signal = started_signal.clone();
            let resolver_gate = resolver_gate.clone();
            let tick_signal = tick_signal.clone();
            let done_signal = done_signal.clone();
            async move {
                run_periodic_loop(
                    service,
                    cancel,
                    move || {
                        let payloads = payloads.clone();
                        let started_signal = started_signal.clone();
                        let resolver_gate = resolver_gate.clone();
                        move |node_ref: &NodeRef| {
                            let result = payloads
                                .get(node_ref)
                                .cloned()
                                .ok_or_else(|| anyhow::anyhow!("unknown ref: {}", node_ref));
                            let is_root = *node_ref == NodeRef::Root;
                            let started_signal = started_signal.clone();
                            let resolver_gate = resolver_gate.clone();
                            Box::pin(async move {
                                if is_root {
                                    started_signal.notify_one();
                                    resolver_gate.notified().await;
                                }
                                result
                            }) as PinFut
                        }
                    },
                    NotifyTick(tick_signal),
                    NotifyDone(done_signal),
                )
                .await;
            }
        });

        // Send one tick — capture starts, blocks on resolver_gate.
        tick_signal.notify_one();
        // Wait for the resolver to signal that root resolution began.
        started_signal.notified().await;

        // Cancel while capture is in-flight.
        cancel.cancel();

        // Task should not have exited — capture is blocked on gate.
        assert!(
            !handle.is_finished(),
            "PT-5: task still running while gated"
        );

        // Release the capture. The loop completes the tick (fires
        // done_signal), then sees cancellation and exits.
        resolver_gate.notify_one();
        done_signal.notified().await;
        handle.await.unwrap();

        // Verify the capture actually ingested data.
        assert_eq!(store.table_names().unwrap().len(), 9);
    }
}
