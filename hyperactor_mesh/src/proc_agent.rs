/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The mesh agent actor manages procs in ProcMeshes.

// EnumAsInner generates code that triggers a false positive
// unused_assignments lint on struct variant fields. #[allow] on the
// enum itself doesn't propagate into derive-macro-generated code, so
// the suppression must be at module scope.
#![allow(unused_assignments)]

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorEnvironment;
use hyperactor::ActorHandle;
use hyperactor::Addr;
use hyperactor::Client;
use hyperactor::Context;
use hyperactor::Data;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortAddr;
use hyperactor::PortHandle;
use hyperactor::PortRef;
use hyperactor::RemoteEndpoint as _;
use hyperactor::actor::handle_undeliverable_message;
use hyperactor::actor::remote::Remote;
use hyperactor::id::Label;
use hyperactor::id::Uid;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::UndeliverableReason;
use hyperactor::proc::Proc;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_cast::cast_actor::CAST_ACTOR_NAME;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::client_root::CLIENT_ROOT;
use crate::client_root::ClientRootApi;
use crate::client_root::ClientRootError;
use crate::client_root::ClientRootRef;
use crate::client_root::EnsureClientRootService;
use crate::client_root::EnsureClientRootServiceReply;
use crate::config_dump::ConfigDump;
use crate::config_dump::ConfigDumpResult;
use crate::introspect::ProcessMemoryStats;
use crate::mesh_id::ResourceId;
use crate::pyspy::PySpyDump;
use crate::pyspy::PySpyProfile;
use crate::pyspy::PySpyProfileWorker;
use crate::pyspy::PySpyWorker;
use crate::resource;

/// Actor name used when spawning the proc agent on user procs.
pub const PROC_AGENT_ACTOR_NAME: &str = "proc_agent";

declare_attrs! {
    /// Whether to self kill actors, procs, and hosts whose owner is not
    /// reachable. `None` disables orphan cleanup entirely; `Some(d)` sets the
    /// keepalive expiry to `d`.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ORPHAN_TIMEOUT".to_string()),
        Some("mesh_orphan_timeout".to_string()),
    ))
    pub attr MESH_ORPHAN_TIMEOUT: Option<Duration> = Some(Duration::from_secs(60));

    /// Interval at which each ProcAgent republishes introspection
    /// on a periodic timer and emits the
    /// `process.memory.rss_bytes` / `process.memory.vm_bytes`
    /// Scuba/OTLP gauges. Linux only — has no effect on other
    /// platforms. `Duration::ZERO` disables the periodic timer
    /// (gauges then never fire); non-periodic republishes (boot,
    /// post-spawn, supervision-event coalesce) still publish
    /// introspect attrs but do not emit gauges.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_PROCESS_MEMORY_METRIC_INTERVAL".to_string()),
        Some("process_memory_metric_interval".to_string()),
    ))
    pub attr PROCESS_MEMORY_METRIC_INTERVAL: Duration = Duration::from_secs(300);

    /// Header tag for StreamState subscriber messages. When present on an
    /// undeliverable envelope, ProcAgent removes the dead subscriber instead
    /// of treating it as an error.
    pub(crate) attr STREAM_STATE_SUBSCRIBER: bool;
}

/// Deferred republish of introspect properties.
///
/// Carries an `emit_memory_metrics` flag distinguishing two senders:
///
/// - `emit_memory_metrics: false` — sent from the supervision event
///   handler with a delay so the supervision handler returns
///   immediately without blocking the ProcAgent message loop.
///   Multiple rapid supervision events (e.g., 4 actors failing
///   simultaneously via broadcast) coalesce into a single republish
///   via the `introspect_dirty` flag. Without this, calling
///   `publish_introspect_properties` inline in the supervision
///   handler starves `GetRankStatus` polls from the
///   `ActorMeshController`, preventing `__supervise__` from firing
///   within the test timeout. See D94960791 for the root cause
///   analysis.
///
/// - `emit_memory_metrics: true` — sent from `Actor::init` and
///   re-armed by the handler at the `PROCESS_MEMORY_METRIC_INTERVAL`
///   cadence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
struct RepublishIntrospect {
    emit_memory_metrics: bool,
}
wirevalue::register_type!(RepublishIntrospect);

/// Collect live actor children and system actor children from the
/// proc's instance DashMap using `all_instance_keys()` with point
/// lookups. This avoids the convoy starvation from `all_actor_ids()`
/// which holds shard read locks while doing heavy per-entry work.
/// See S12 in `introspect` module doc.
fn collect_live_children(
    proc: &hyperactor::Proc,
) -> (
    Vec<hyperactor::introspect::IntrospectRef>,
    Vec<crate::introspect::NodeRef>,
) {
    let all_keys = proc.all_instance_keys();
    let mut children = Vec::with_capacity(all_keys.len());
    let mut system_children = Vec::new();
    for id in all_keys {
        if let Some(cell) = proc.get_instance_by_id(&id) {
            let actor_addr = cell.actor_addr().clone();
            if cell.is_system() {
                system_children.push(crate::introspect::NodeRef::Actor(actor_addr.clone()));
            }
            children.push(hyperactor::introspect::IntrospectRef::Actor(actor_addr));
        }
    }
    (children, system_children)
}

/// Actor state used for v1 API.
#[derive(Debug)]
struct ActorInstanceState {
    create_rank: usize,
    spawn: Result<ActorAddr, anyhow::Error>,
    /// True once a stop signal has been sent. This does *not* mean the actor
    /// has reached a terminal state — that is determined by observing
    /// supervision events.
    stop_initiated: bool,
    /// The supervision event observed for this actor, if it has reached
    /// terminal state.
    supervision_event: Option<ActorSupervisionEvent>,
    /// Streaming subscribers that receive `RankedState<ActorState>` on every
    /// state change, paired with the actor's rank in that subscriber's view.
    /// Dead subscribers are removed via undeliverable handling.
    subscribers: Vec<(usize, PortRef<resource::RankedState<ActorState>>)>,
    /// The time at which the actor should be considered expired if no further
    /// keepalive is received. `None` meaning it will never expire.
    expiry_time: Option<std::time::SystemTime>,
    /// Monotonic generation counter, incremented on every state-mutating
    /// operation (spawn, stop, supervision event). Used for last-writer-wins
    /// ordering in the mesh controller.
    generation: u64,
    /// Pending `WaitRankStatus` callers: each entry is the minimum status
    /// threshold, the delivered view rank at which to position the reply
    /// overlay, and the reply port to send once the threshold is met.
    pending_wait_status: Vec<(resource::Status, usize, PortRef<crate::StatusOverlay>)>,
}

/// Identity of a root-owned client-root service. `actor_states` (keyed by the
/// service's instance `ResourceId`) remains the sole lifecycle registry; this
/// records only what a repeat ensure must match exactly (CROOT-3, CROOT-9).
#[derive(Debug)]
struct ServiceEntry {
    /// The registered remote actor type name.
    actor_type: String,
    /// The exact serialized `RemoteSpawn` parameter bytes.
    params: Data,
    /// The service actor's fresh instance id, whose `actor_states` entry stores
    /// the spawn result.
    id: ResourceId,
}

impl ActorInstanceState {
    /// Derive the resource status from spawn result, stop initiation,
    /// and the observed supervision event.
    fn status(&self) -> resource::Status {
        match &self.spawn {
            Err(e) => resource::Status::Failed(e.to_string()),
            Ok(_) => match &self.supervision_event {
                Some(event) if event.is_error() => resource::Status::Failed(format!("{}", event)),
                Some(_) => resource::Status::Stopped,
                None if self.stop_initiated => resource::Status::Stopping,
                None => resource::Status::Running,
            },
        }
    }

    /// True if the actor has reached a terminal state (stopped or failed),
    /// or if it never successfully spawned.
    fn is_terminal(&self) -> bool {
        match &self.spawn {
            Err(_) => true,
            Ok(_) => self.supervision_event.is_some(),
        }
    }

    /// True if the supervision event is an error.
    fn has_errors(&self) -> bool {
        self.supervision_event
            .as_ref()
            .is_some_and(|e| e.is_error())
    }

    /// Build the `State<ActorState>` for this instance, suitable for
    /// replies and subscriber notifications.
    fn to_state(&self, id: &ResourceId) -> resource::State<ActorState> {
        let status = self.status();
        let actor_state = self.spawn.as_ref().ok().map(|actor_id| ActorState {
            actor_id: actor_id.clone(),
            create_rank: self.create_rank,
            supervision_events: self.supervision_event.clone().into_iter().collect(),
        });
        resource::State {
            id: id.clone(),
            status,
            state: actor_state,
            generation: self.generation,
            timestamp: std::time::SystemTime::now(),
        }
    }

    /// Notify all observers that this actor's status has changed:
    /// streaming subscribers get the full state, and one-shot
    /// `WaitRankStatus` waiters whose threshold is now met get replied
    /// to and removed.
    fn notify_status_changed(&mut self, cx: &impl hyperactor::context::Actor, id: &ResourceId) {
        // Streaming subscribers (persistent).
        let state = self.to_state(id);
        for (view_rank, subscriber) in &self.subscribers {
            let mut headers = Flattrs::new();
            headers.set(STREAM_STATE_SUBSCRIBER, true);
            subscriber.post_with_headers(
                cx,
                headers,
                resource::RankedState {
                    rank: resource::Rank::new(*view_rank),
                    state: state.clone(),
                },
            );
        }

        // One-shot waiters (predicated). Each retains the reply rank it was
        // stashed with (RSP-3), positioned in the caller's view (RSP-1).
        let status = self.status();
        self.pending_wait_status
            .retain(|(min_status, rank, reply)| {
                if status >= *min_status {
                    let overlay = crate::StatusOverlay::try_from_runs(vec![(
                        *rank..(*rank + 1),
                        status.clone(),
                    )])
                    .expect("valid single-run overlay");
                    let _ = reply.post(cx, overlay);
                    false
                } else {
                    true
                }
            });
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, Named)]
pub(crate) struct SelfCheck {}

/// A mesh agent is responsible for managing procs in a [`ProcMesh`].
///
/// ## Supervision event ingestion (remote)
///
/// `ProcAgent` is the *process/rank-local* sink for
/// `ActorSupervisionEvent`s produced by the runtime (actor failures,
/// routing failures, undeliverables, etc.).
///
/// We **export** `ActorSupervisionEvent` as a handler so that other
/// procs—most importantly the process-global root client created by
/// `context()`—can forward undeliverables as supervision
/// events to the *currently active* mesh.
///
/// Without exporting this handler, `ActorSupervisionEvent` cannot be
/// addressed via `ActorAddr`/`PortAddr` across processes, and the
/// global-root-client undeliverable → supervision pipeline would
/// degrade to log-only behavior (events become undeliverable again or
/// are dropped).
///
/// See GC-1 in `global_context` module doc.
#[hyperactor::export(
    handlers=[
        ActorSupervisionEvent,
        resource::CreateOrUpdate<ActorSpec>,
        resource::Stop,
        resource::StopAll,
        resource::GetState<ActorState>,
        resource::StreamState<ActorState>,
        resource::KeepaliveGetState<ActorState>,
        resource::GetRankStatus,
        resource::WaitRankStatus,
        RepublishIntrospect,
        PySpyDump,
        PySpyProfile,
        ConfigDump,
    ]
)]
pub struct ProcAgent {
    proc: Proc,
    remote: Remote,
    /// Actors created and tracked through the resource behavior.
    actor_states: HashMap<ResourceId, ActorInstanceState>,
    /// If true, and supervisor is None, record supervision events to be reported
    /// to owning actors later.
    record_supervision_events: bool,
    /// True when supervision events have arrived but introspect
    /// properties haven't been republished yet.
    introspect_dirty: bool,
    /// If set, the shutdown handler will send the exit code through this
    /// channel instead of calling process::exit directly, allowing the
    /// caller to perform graceful shutdown (e.g. draining the mailbox server).
    shutdown_tx: Option<tokio::sync::oneshot::Sender<i32>>,
    /// True once a StopAll message has been received. When set, the
    /// supervision event handler checks whether all actors have reached
    /// terminal state and, if so, triggers process shutdown.
    stopping_all: bool,
    /// If set, check for expired actors whose keepalive has lapsed.
    mesh_orphan_timeout: Option<Duration>,
    /// Root-owned client-root services keyed by validated static service name.
    /// Populated only on the one root ProcAgent that binds `ClientRootApi`;
    /// empty (and unreachable) on worker ProcAgents (CROOT-1, CROOT-4).
    client_root_services: HashMap<Label, ServiceEntry>,
}

impl ProcAgent {
    pub(crate) fn boot_v1(
        proc: Proc,
        shutdown_tx: Option<tokio::sync::oneshot::Sender<i32>>,
    ) -> Result<ActorHandle<Self>, anyhow::Error> {
        let cast_handle = proc.spawn_with_uid(
            Uid::singleton(Label::strip(CAST_ACTOR_NAME)),
            hyperactor_cast::cast_actor::CastActor::default(),
        )?;
        cast_handle.bind::<hyperactor_cast::cast_actor::CastActor>();

        let orphan_timeout = hyperactor_config::global::get(MESH_ORPHAN_TIMEOUT);
        let agent = ProcAgent {
            proc: proc.clone(),
            remote: Remote::collect(),
            actor_states: HashMap::new(),
            record_supervision_events: true,
            introspect_dirty: false,
            shutdown_tx,
            stopping_all: false,
            mesh_orphan_timeout: orphan_timeout,
            client_root_services: HashMap::new(),
        };
        proc.spawn_with_uid::<Self>(
            Uid::singleton(Label::new(PROC_AGENT_ACTOR_NAME).unwrap()),
            agent,
        )
    }

    /// Returns true when every tracked actor has a terminal supervision event
    /// (or failed to spawn). Used to determine when shutdown can proceed
    /// after a StopAll.
    fn all_actors_terminal(&self) -> bool {
        self.actor_states.values().all(|state| state.is_terminal())
    }

    /// Trigger process shutdown. Flushes the forwarder first so that
    /// supervision events reach their destinations, then sends through
    /// `shutdown_tx` if available, otherwise calls `process::exit`.
    async fn shutdown(&mut self) {
        let has_errors = self.actor_states.values().any(|state| state.has_errors());
        let exit_code = if has_errors { 1 } else { 0 };

        let flush_timeout =
            hyperactor_config::global::get(hyperactor::config::FORWARDER_FLUSH_TIMEOUT);
        match tokio::time::timeout(flush_timeout, self.proc.flush()).await {
            Ok(Err(err)) => {
                tracing::warn!("forwarder flush failed during shutdown: {}", err);
            }
            Err(_elapsed) => {
                tracing::warn!("forwarder flush timed out during shutdown");
            }
            Ok(Ok(())) => {}
        }

        // Stop and join the mailbox server (no-op if this proc was
        // not created with one). Pending receive-side acks are
        // flushed before the underlying channel server is torn down.
        self.proc.join_mailbox_server().await;

        tracing::info!(
            "shutting down process after all actors reached terminal state (exit_code={})",
            exit_code,
        );

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(exit_code);
            return;
        }
        std::process::exit(exit_code);
    }

    /// Send a stop signal to an actor on this proc. This is fire-and-forget;
    /// it does not wait for the actor to reach terminal status.
    fn stop_actor_by_id(&self, actor_id: &ActorAddr, reason: &str) {
        tracing::info!(
            name = "StopActor",
            %actor_id,
            actor_name = actor_id.log_name(),
            %reason,
        );
        self.proc.stop_actor(actor_id.id(), reason.to_string());
    }

    /// Publish the current proc properties and children list for
    /// introspection. See S12 in `introspect` module doc.
    fn publish_introspect_properties(
        &self,
        cx: &impl hyperactor::context::Actor,
    ) -> ProcessMemoryStats {
        let (mut children, mut system_children) = collect_live_children(&self.proc);

        // Terminated actors appear as children but don't inflate
        // the actor count. Track them in stopped_children so the
        // TUI can filter/gray without per-child fetches.
        let mut stopped_children: Vec<crate::introspect::NodeRef> = Vec::new();
        for id in self.proc.all_terminated_actor_ids() {
            let child_ref = hyperactor::introspect::IntrospectRef::Actor(id.clone());
            let node_ref = crate::introspect::NodeRef::Actor(id.clone());
            stopped_children.push(node_ref.clone());
            if let Some(snapshot) = self.proc.terminated_snapshot(&id) {
                let snapshot_attrs: hyperactor_config::Attrs =
                    serde_json::from_str(&snapshot.attrs).unwrap_or_default();
                if snapshot_attrs
                    .get(hyperactor::introspect::IS_SYSTEM)
                    .copied()
                    .unwrap_or(false)
                {
                    system_children.push(node_ref);
                }
            }
            if !children.contains(&child_ref) {
                children.push(child_ref);
            }
        }

        let stopped_retention_cap =
            hyperactor_config::global::get(hyperactor::config::TERMINATED_SNAPSHOT_RETENTION);

        // FI-5: is_poisoned iff failed_actor_count > 0.
        let failed_actor_count = self
            .actor_states
            .values()
            .filter(|s| s.has_errors())
            .count();

        // Attrs-based introspection.
        let num_live = children.len();
        let mut attrs = hyperactor_config::Attrs::new();
        attrs.set(crate::introspect::NODE_TYPE, "proc".to_string());
        attrs.set(
            crate::introspect::PROC_NAME,
            self.proc
                .proc_addr()
                .label()
                .map(|l| l.as_str().to_string())
                .unwrap_or_else(|| self.proc.proc_addr().id().to_string()),
        );
        attrs.set(crate::introspect::NUM_ACTORS, num_live);
        attrs.set(hyperactor::introspect::CHILDREN, children);
        attrs.set(crate::introspect::SYSTEM_CHILDREN, system_children);
        attrs.set(crate::introspect::STOPPED_CHILDREN, stopped_children);
        attrs.set(
            crate::introspect::STOPPED_RETENTION_CAP,
            stopped_retention_cap,
        );
        attrs.set(crate::introspect::IS_POISONED, failed_actor_count > 0);
        attrs.set(crate::introspect::FAILED_ACTOR_COUNT, failed_actor_count);

        // PD-* proc debug stats intentionally join two signal classes:
        // hosting-process memory for the OS process that owns this
        // proc, and proc-local queue pressure aggregated over live
        // actors only.
        let memory = crate::introspect::ProcessMemoryStats::read_from_procfs();
        memory.to_attrs(&mut attrs);

        // Proc-wide total from runtime accounting path (O(1)).
        let queue_total = self.proc.queue_depth_total();
        attrs.set(crate::introspect::ACTOR_WORK_QUEUE_DEPTH_TOTAL, queue_total);

        // Per-actor max still needs the per-actor scan (PD-4: live actors only).
        let mut queue_max: u64 = 0;
        for actor_id in self.proc.all_instance_keys() {
            if let Some(cell) = self.proc.get_instance_by_id(&actor_id) {
                queue_max = queue_max.max(cell.queue_depth());
            }
        }
        attrs.set(crate::introspect::ACTOR_WORK_QUEUE_DEPTH_MAX, queue_max);

        // Retained queue-pressure evidence (PD-6, PD-7).
        attrs.set(
            crate::introspect::ACTOR_WORK_QUEUE_DEPTH_HIGH_WATER_MARK,
            self.proc.queue_depth_high_water_mark(),
        );
        attrs.set(
            crate::introspect::LAST_NONZERO_QUEUE_DEPTH_AGE_MS,
            self.proc.last_nonzero_queue_depth_age_ms(),
        );

        cx.instance().publish_attrs(attrs);

        memory
    }
}

#[async_trait]
impl Actor for ProcAgent {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        self.proc.set_supervision_coordinator(this.port().bind())?;
        let _ = self.publish_introspect_properties(this);

        // Resolve terminated actor snapshots via QueryChild so that
        // dead actors remain directly queryable by reference.
        let proc = self.proc.clone();
        let self_id = this.self_addr().clone();
        this.set_query_child_handler(move |child_ref| {
            use hyperactor::introspect::IntrospectResult;

            if let Addr::Actor(actor_ref) = child_ref
                && let Some(snapshot) = proc.terminated_snapshot(actor_ref)
            {
                return snapshot;
            }

            // PA-1 (ProcAgent path): proc-node children used by
            // admin/TUI must be computed from live proc state at query
            // time, not solely from cached published_properties.
            // Therefore a direct proc.spawn_with_label() actor must appear on the
            // next QueryChild(Addr::Proc) response without an
            // extra publish event. See
            // test_query_child_proc_returns_live_children.
            if let Addr::Proc(proc_ref) = child_ref
                && *proc_ref == proc.proc_addr()
            {
                let (mut children, mut system_children) = collect_live_children(&proc);

                let mut stopped_children: Vec<crate::introspect::NodeRef> = Vec::new();
                for id in proc.all_terminated_actor_ids() {
                    let child_ref = hyperactor::introspect::IntrospectRef::Actor(id.clone());
                    let node_ref = crate::introspect::NodeRef::Actor(id.clone());
                    stopped_children.push(node_ref.clone());
                    if let Some(snapshot) = proc.terminated_snapshot(&id) {
                        let snapshot_attrs: hyperactor_config::Attrs =
                            serde_json::from_str(&snapshot.attrs).unwrap_or_default();
                        if snapshot_attrs
                            .get(hyperactor::introspect::IS_SYSTEM)
                            .copied()
                            .unwrap_or(false)
                        {
                            system_children.push(node_ref);
                        }
                    }
                    if !children.contains(&child_ref) {
                        children.push(child_ref);
                    }
                }

                let stopped_retention_cap = hyperactor_config::global::get(
                    hyperactor::config::TERMINATED_SNAPSHOT_RETENTION,
                );

                let (is_poisoned, failed_actor_count) = proc
                    .get_instance(&self_id)
                    .and_then(|cell| cell.published_attrs())
                    .map(|attrs| {
                        let is_poisoned = attrs
                            .get(crate::introspect::IS_POISONED)
                            .copied()
                            .unwrap_or(false);
                        let failed_actor_count = attrs
                            .get(crate::introspect::FAILED_ACTOR_COUNT)
                            .copied()
                            .unwrap_or(0);
                        (is_poisoned, failed_actor_count)
                    })
                    .unwrap_or((false, 0));

                // Build attrs for this proc node.
                let num_live = children.len();
                let mut attrs = hyperactor_config::Attrs::new();
                attrs.set(crate::introspect::NODE_TYPE, "proc".to_string());
                attrs.set(
                    crate::introspect::PROC_NAME,
                    proc_ref
                        .label()
                        .map(|l| l.as_str().to_string())
                        .unwrap_or_else(|| proc_ref.id().to_string()),
                );
                attrs.set(crate::introspect::NUM_ACTORS, num_live);
                attrs.set(crate::introspect::SYSTEM_CHILDREN, system_children);
                attrs.set(crate::introspect::STOPPED_CHILDREN, stopped_children);
                attrs.set(
                    crate::introspect::STOPPED_RETENTION_CAP,
                    stopped_retention_cap,
                );
                attrs.set(crate::introspect::IS_POISONED, is_poisoned);
                attrs.set(crate::introspect::FAILED_ACTOR_COUNT, failed_actor_count);

                // PD-*: include proc debug stats in QueryChild
                // to prevent resolution drift from the publish path.
                let memory = crate::introspect::ProcessMemoryStats::read_from_procfs();
                memory.to_attrs(&mut attrs);
                attrs.set(
                    crate::introspect::ACTOR_WORK_QUEUE_DEPTH_TOTAL,
                    proc.queue_depth_total(),
                );
                let mut queue_max: u64 = 0;
                for aid in proc.all_instance_keys() {
                    if let Some(cell) = proc.get_instance_by_id(&aid) {
                        queue_max = queue_max.max(cell.queue_depth());
                    }
                }
                attrs.set(crate::introspect::ACTOR_WORK_QUEUE_DEPTH_MAX, queue_max);
                attrs.set(
                    crate::introspect::ACTOR_WORK_QUEUE_DEPTH_HIGH_WATER_MARK,
                    proc.queue_depth_high_water_mark(),
                );
                attrs.set(
                    crate::introspect::LAST_NONZERO_QUEUE_DEPTH_AGE_MS,
                    proc.last_nonzero_queue_depth_age_ms(),
                );

                let attrs_json = serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());

                return IntrospectResult {
                    identity: hyperactor::introspect::IntrospectRef::Proc(proc_ref.clone()),
                    attrs: attrs_json,
                    children,
                    parent: None,
                    as_of: std::time::SystemTime::now(),
                };
            }

            {
                let mut error_attrs = hyperactor_config::Attrs::new();
                error_attrs.set(hyperactor::introspect::ERROR_CODE, "not_found".to_string());
                error_attrs.set(
                    hyperactor::introspect::ERROR_MESSAGE,
                    format!("child {} not found", child_ref),
                );
                let identity = match child_ref {
                    Addr::Proc(p) => hyperactor::introspect::IntrospectRef::Proc(p.clone()),
                    Addr::Actor(a) => hyperactor::introspect::IntrospectRef::Actor(a.clone()),
                    Addr::Port(p) => hyperactor::introspect::IntrospectRef::Actor(p.actor_addr()),
                };
                IntrospectResult {
                    identity,
                    attrs: serde_json::to_string(&error_attrs).unwrap_or_else(|_| "{}".to_string()),
                    children: Vec::new(),
                    parent: None,
                    as_of: std::time::SystemTime::now(),
                }
            }
        });

        if let Some(delay) = &self.mesh_orphan_timeout {
            this.post_after(this, SelfCheck::default(), *delay);
        }
        if cfg!(target_os = "linux") {
            let interval = hyperactor_config::global::get(PROCESS_MEMORY_METRIC_INTERVAL);
            if !interval.is_zero() {
                this.post_after(
                    this,
                    RepublishIntrospect {
                        emit_memory_metrics: true,
                    },
                    interval,
                );
            }
        }
        Ok(())
    }

    async fn handle_undeliverable_message(
        &mut self,
        cx: &Instance<Self>,
        reason: UndeliverableReason,
        envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        let Some(returned) = envelope.as_message() else {
            return handle_undeliverable_message(cx, reason, envelope);
        };
        if let Some(true) = returned.headers().get(STREAM_STATE_SUBSCRIBER) {
            let dest_port_id: PortAddr = returned.dest().clone();
            let port = PortRef::<resource::RankedState<ActorState>>::attest(dest_port_id);
            // Remove this subscriber from whichever actor instance holds it.
            for instance in self.actor_states.values_mut() {
                instance.subscribers.retain(|(_, s)| s != &port);
            }
            Ok(())
        } else {
            handle_undeliverable_message(cx, reason, envelope)
        }
    }

    async fn handle_invalid_reference(
        &mut self,
        cx: &Instance<Self>,
        invalid: hyperactor::mailbox::InvalidReference,
        envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        let Some(returned) = envelope.as_message() else {
            return hyperactor::actor::handle_invalid_reference(cx, invalid, envelope);
        };
        if let Some(true) = returned.headers().get(STREAM_STATE_SUBSCRIBER) {
            let dest_port_id: PortAddr = returned.dest().clone();
            let port = PortRef::<resource::RankedState<ActorState>>::attest(dest_port_id);
            for instance in self.actor_states.values_mut() {
                instance.subscribers.retain(|(_, s)| s != &port);
            }
            Ok(())
        } else {
            hyperactor::actor::handle_invalid_reference(cx, invalid, envelope)
        }
    }
}

#[async_trait]
impl Handler<ActorSupervisionEvent> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        event: ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        if self.record_supervision_events {
            if event.is_error() {
                tracing::warn!(
                    name = "SupervisionEvent",
                    proc_id = %self.proc.proc_addr(),
                    %event,
                    "recording supervision error",
                );
            } else {
                tracing::debug!(
                    name = "SupervisionEvent",
                    proc_id = %self.proc.proc_addr(),
                    %event,
                    "recording non-error supervision event",
                );
            }
            // Record the event in the actor's instance state and notify subscribers.
            if let Some((id, instance)) = self.actor_states.iter_mut().find(|(_, s)| {
                s.spawn
                    .as_ref()
                    .ok()
                    .is_some_and(|actor_id| actor_id.id() == event.actor_id.id())
            }) {
                instance.supervision_event = Some(event.clone());
                instance.generation += 1;
                let id = id.clone();
                instance.notify_status_changed(cx, &id);
            }
            // Defer republish so introspection picks up is_poisoned /
            // failed_actor_count without blocking the message loop.
            // Multiple rapid events coalesce into one republish.
            if !self.introspect_dirty {
                self.introspect_dirty = true;
                cx.post_after(
                    cx,
                    RepublishIntrospect {
                        emit_memory_metrics: false,
                    },
                    std::time::Duration::from_millis(100),
                );
            }

            // If StopAll was requested, check whether all actors have now
            // reached terminal state. If so, shut down the process.
            if self.stopping_all && self.all_actors_terminal() {
                self.shutdown().await;
            }
        }
        if !self.record_supervision_events && event.is_error() {
            // If there is no supervisor, and nothing is recording these, crash
            // the whole process on error events.
            tracing::error!(
                name = "supervision_event_transmit_failed",
                proc_id = %cx.self_addr().proc_addr(),
                %event,
                "could not propagate supervision event, crashing",
            );

            // We should have a custom "crash" function here, so that this works
            // in testing of the LocalAllocator, etc.
            std::process::exit(1);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RepublishIntrospect> for ProcAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: RepublishIntrospect) -> anyhow::Result<()> {
        self.introspect_dirty = false;
        let memory = self.publish_introspect_properties(cx);
        if msg.emit_memory_metrics {
            let proc_id = self.proc.proc_addr().to_string();
            let pid = std::process::id() as i64;
            if let Some(rss) = memory.process_rss_bytes {
                crate::metrics::PROCESS_RSS_BYTES.record(
                    rss as f64,
                    hyperactor_telemetry::kv_pairs!(
                        "proc_id" => proc_id.clone(),
                        "pid" => pid,
                    ),
                );
            }
            if let Some(vm) = memory.process_vm_size_bytes {
                crate::metrics::PROCESS_VM_SIZE_BYTES.record(
                    vm as f64,
                    hyperactor_telemetry::kv_pairs!(
                        "proc_id" => proc_id,
                        "pid" => pid,
                    ),
                );
            }
            let interval = hyperactor_config::global::get(PROCESS_MEMORY_METRIC_INTERVAL);
            if !interval.is_zero() {
                cx.post_after(
                    cx,
                    RepublishIntrospect {
                        emit_memory_metrics: true,
                    },
                    interval,
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<PySpyDump> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: PySpyDump,
    ) -> Result<(), anyhow::Error> {
        PySpyWorker::spawn_and_forward(cx, message.opts, message.result)
    }
}

#[async_trait]
impl Handler<PySpyProfile> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: PySpyProfile,
    ) -> Result<(), anyhow::Error> {
        PySpyProfileWorker::spawn_and_forward(cx, message.request, message.result)
    }
}

#[async_trait]
impl Handler<ConfigDump> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: ConfigDump,
    ) -> Result<(), anyhow::Error> {
        let entries = hyperactor_config::global::config_entries();
        // Reply is best-effort: the caller may have timed out and dropped
        // the once-port.  That must not crash this actor.
        let _ = message.result.post(cx, ConfigDumpResult { entries });
        Ok(())
    }
}

// Implement the resource behavior for managing actors:

/// Actor spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct ActorSpec {
    /// registered actor type
    pub actor_type: String,
    /// serialized parameters
    pub params_data: Data,
    /// The persistent environment to store on the spawned actor's instance.
    /// Copied from the spawning context's instance and serialized per spawn
    /// (AENV-3).
    pub actor_environment: ActorEnvironment,
}
wirevalue::register_type!(ActorSpec);

/// Actor state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct ActorState {
    /// The actor's ID.
    pub actor_id: ActorAddr,
    /// The actor's dense rank in the view it was first created over. Stable for
    /// the actor's lifetime and independent of any later view that reuses the
    /// actor, so it is not the actor's rank in an overlapping or sliced view.
    pub create_rank: usize,
    // TODO status: ActorStatus,
    pub supervision_events: Vec<ActorSupervisionEvent>,
}
wirevalue::register_type!(ActorState);

impl ProcAgent {
    /// Create a root-tracked actor and record its state under `id`, returning
    /// whether a spawn was attempted.
    ///
    /// On a poisoned proc (an actor with an error supervision event) it records a
    /// failed state without spawning and returns `false`. Otherwise it attempts
    /// the spawn — merging the spec's persistent environment with `headers` (the
    /// transient constructor headers) only for `RemoteSpawn::new` — and returns
    /// `true` whether or not `gspawn` itself succeeded; a spawn failure is cached
    /// in `actor_states`, which remains the sole lifecycle authority. Callers that
    /// need the actual spawn result read it back from `actor_states` (see
    /// `service_result`). Shared by the mesh `CreateOrUpdate` path and the
    /// client-root ensure path.
    async fn create_actor(
        &mut self,
        id: ResourceId,
        create_rank: usize,
        spec: ActorSpec,
        headers: Flattrs,
    ) -> bool {
        let poisoned = self.actor_states.values().any(|s| s.has_errors());
        let spawn = if poisoned {
            Err(anyhow::anyhow!(
                "Cannot spawn new actors on mesh with supervision events"
            ))
        } else {
            let ActorSpec {
                actor_type,
                params_data,
                actor_environment,
            } = spec;
            self.remote
                .gspawn(
                    &self.proc,
                    &actor_type,
                    id.uid().clone(),
                    params_data,
                    actor_environment,
                    headers,
                )
                .await
        };
        self.actor_states.insert(
            id,
            ActorInstanceState {
                create_rank,
                spawn,
                stop_initiated: false,
                supervision_event: None,
                subscribers: Vec::new(),
                expiry_time: None,
                generation: 1,
                pending_wait_status: Vec::new(),
            },
        );
        !poisoned
    }

    /// Create or reuse the statically named, root-owned service and return its
    /// address. Runs entirely inside the serial ProcAgent handler, so concurrent
    /// identical ensures collapse: the first records the entry and the rest
    /// observe it (CROOT-3, CROOT-5).
    async fn ensure_client_root_service(
        &mut self,
        cx: &Context<'_, Self>,
        service_name: Label,
        actor_type: String,
        params: Data,
    ) -> Result<ActorAddr, ClientRootError> {
        let conflict = || ClientRootError::ConflictingSpec {
            name: service_name.as_str().to_string(),
        };

        // Reuse an existing service only on an exact (type, params) match; a
        // service that has since gone terminal fails closed in `service_result`.
        if let Some(entry) = self.client_root_services.get(&service_name) {
            if entry.actor_type != actor_type || entry.params != params {
                return Err(conflict());
            }
            return Self::service_result(&self.actor_states, &entry.id, &service_name);
        }

        // Refuse to create a new service once shutdown has begun: a StopAll has
        // already snapshotted and stopped the live actors, so a service created
        // afterwards would never be stopped and would strand shutdown.
        if self.stopping_all {
            return Err(ClientRootError::Unavailable {
                name: service_name.as_str().to_string(),
                reason: "the client root is shutting down".to_string(),
            });
        }

        // A fresh random instance id (CROOT-3): this avoids the predictable
        // collision a name-derived id would create with a mesh actor or a
        // preoccupying actor. The loop guards the negligible random-uid collision
        // against the ids already present in `actor_states`.
        let id = loop {
            let candidate = ResourceId::instance(service_name.clone());
            if !self.actor_states.contains_key(&candidate) {
                break candidate;
            }
        };

        // The created service inherits the same client-root capability so its own
        // descendants stay in this root (CROOT-7).
        let self_root = ClientRootRef::from_ref(
            hyperactor::context::Actor::instance(cx).bind::<ClientRootApi>(),
        );
        let mut service_env = hyperactor::ActorEnvironment::default();
        service_env
            .set(CLIENT_ROOT, self_root)
            .map_err(|e| ClientRootError::Spawn {
                name: service_name.as_str().to_string(),
                message: e.to_string(),
            })?;
        let spec = ActorSpec {
            actor_type: actor_type.clone(),
            params_data: params.clone(),
            actor_environment: service_env,
        };
        // Root-owned construction uses empty transient headers: the requester's
        // message headers are request-scoped and must not leak into a long-lived,
        // root-owned service that outlives the requester (AENV-4).
        let spawn_attempted = self.create_actor(id.clone(), 0, spec, Flattrs::new()).await;

        // Record the identity so later ensures reuse it; the spawn result
        // (including a cached failure) lives in `actor_states`.
        self.client_root_services.insert(
            service_name.clone(),
            ServiceEntry {
                actor_type,
                params,
                id: id.clone(),
            },
        );
        // Reflect the new service in the root's introspect view, matching the
        // mesh create path (whenever a spawn was attempted).
        if spawn_attempted {
            let _ = self.publish_introspect_properties(cx);
        }
        Self::service_result(&self.actor_states, &id, &service_name)
    }

    /// Read the recorded spawn result for a service's reserved id.
    fn service_result(
        actor_states: &HashMap<ResourceId, ActorInstanceState>,
        id: &ResourceId,
        service_name: &Label,
    ) -> Result<ActorAddr, ClientRootError> {
        let Some(state) = actor_states.get(id) else {
            return Err(ClientRootError::Spawn {
                name: service_name.as_str().to_string(),
                message: "service actor state is missing".to_string(),
            });
        };
        // A cached spawn failure (including a poisoned-proc failure) is returned
        // as-is; proc poisoning is latched, so it is never retried.
        let addr = state.spawn.as_ref().map_err(|e| ClientRootError::Spawn {
            name: service_name.as_str().to_string(),
            message: e.to_string(),
        })?;
        // Fail closed if the service spawned but has since gone terminal
        // (stopped, failed, or stopping): reuse must never hand back a dead ref.
        match state.status() {
            resource::Status::Running => Ok(addr.clone()),
            other => Err(ClientRootError::Unavailable {
                name: service_name.as_str().to_string(),
                reason: format!("service is {}", other),
            }),
        }
    }
}

#[async_trait]
impl Handler<resource::CreateOrUpdate<ActorSpec>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<ActorSpec>,
    ) -> anyhow::Result<()> {
        if self.actor_states.contains_key(&create_or_update.id) {
            // There is no update.
            return Ok(());
        }
        // Once shutdown has begun, a new actor would escape the StopAll snapshot
        // and strand shutdown; do not create it.
        if self.stopping_all {
            return Ok(());
        }
        let create_rank = create_or_update.rank.unwrap();
        // Publish introspect whenever a spawn was attempted (non-poisoned proc),
        // matching the prior behavior; on a poisoned proc the helper records a
        // failed state without spawning and does not publish.
        let spawn_attempted = self
            .create_actor(
                create_or_update.id.clone(),
                create_rank,
                create_or_update.spec,
                cx.headers().clone(),
            )
            .await;
        if spawn_attempted {
            let _ = self.publish_introspect_properties(cx);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<EnsureClientRootService> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: EnsureClientRootService,
    ) -> anyhow::Result<()> {
        let EnsureClientRootService {
            service_name,
            actor_type,
            params,
            mut reply,
        } = message;
        // A departed requester must not fault the root: an undeliverable reply is
        // dropped rather than returned to this ProcAgent (CROOT-11). Expected
        // failures travel in the typed reply, and the handler always returns Ok.
        reply.return_undeliverable(false);
        let result = self
            .ensure_client_root_service(cx, service_name, actor_type, params)
            .await;
        reply.post(cx, EnsureClientRootServiceReply(result));
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::Stop> for ProcAgent {
    async fn handle(&mut self, cx: &Context<Self>, message: resource::Stop) -> anyhow::Result<()> {
        let actor_id = match self.actor_states.get_mut(&message.id) {
            Some(actor_state) => {
                let id = actor_state.spawn.as_ref().ok().cloned();
                if id.is_some() && !actor_state.stop_initiated {
                    actor_state.stop_initiated = true;
                    actor_state.generation += 1;
                    actor_state.notify_status_changed(cx, &message.id);
                    id
                } else {
                    None
                }
            }
            None => None,
        };
        if let Some(actor_id) = actor_id {
            self.stop_actor_by_id(&actor_id, &message.reason);
        }

        Ok(())
    }
}

/// Handles `StopAll` by sending stop signals to all child actors.
/// Process shutdown is deferred until all actors have reached terminal
/// state, as observed through supervision events.
#[async_trait]
impl Handler<resource::StopAll> for ProcAgent {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        message: resource::StopAll,
    ) -> anyhow::Result<()> {
        self.stopping_all = true;

        // Send stop signals to all actors that haven't been stopped yet.
        let to_stop: Vec<ActorAddr> = self
            .actor_states
            .values_mut()
            .filter_map(|state| {
                if state.stop_initiated {
                    return None;
                }
                state.stop_initiated = true;
                state.spawn.as_ref().ok().cloned()
            })
            .collect();

        for actor_id in &to_stop {
            self.stop_actor_by_id(actor_id, &message.reason);
        }

        // If there are no actors to stop, shut down immediately.
        if self.all_actors_terminal() {
            self.shutdown().await;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<resource::GetRankStatus> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_rank_status: resource::GetRankStatus,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;

        // Position the overlay at the rank the request carries (the recipient's
        // rank in the caller's view), not the actor's first-creation rank (RSP-1);
        // an absent actor yields an empty overlay (RSP-4).
        let overlay = match self.actor_states.get(&get_rank_status.id) {
            Some(state) => {
                let rank = get_rank_status.rank.unwrap();
                StatusOverlay::try_from_runs(vec![(rank..(rank + 1), state.status())])
                    .expect("valid single-run overlay")
            }
            None => StatusOverlay::new(),
        };
        get_rank_status.reply.post(cx, overlay);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::WaitRankStatus> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: resource::WaitRankStatus,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;

        // The request carries the reply rank (the recipient's rank in the
        // caller's view) (RSP-1); a deferred waiter retains it, since the cast
        // context is gone by flush time (RSP-3). An absent actor replies with an
        // empty overlay (RSP-4).
        let Some(status) = self.actor_states.get(&msg.id).map(|state| state.status()) else {
            let _ = msg.reply.post(cx, StatusOverlay::new());
            return Ok(());
        };
        let rank = msg.rank.unwrap();

        // If already at or past the requested threshold, reply immediately.
        if status >= msg.min_status {
            let overlay = StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                .expect("valid single-run overlay");
            let _ = msg.reply.post(cx, overlay);
            return Ok(());
        }

        // Otherwise, stash the waiter with its reply rank. It will be flushed
        // when the status changes (supervision event or stop).
        if let Some(state) = self.actor_states.get_mut(&msg.id) {
            state
                .pending_wait_status
                .push((msg.min_status, rank, msg.reply));
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::GetState<ActorState>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_state: resource::GetState<ActorState>,
    ) -> anyhow::Result<()> {
        let state = match self.actor_states.get(&get_state.id) {
            Some(instance) => instance.to_state(&get_state.id),
            None => resource::State {
                id: get_state.id.clone(),
                status: resource::Status::NotExist,
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        };

        get_state.reply.post(cx, state);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::StreamState<ActorState>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        stream_state: resource::StreamState<ActorState>,
    ) -> anyhow::Result<()> {
        // The cast layer fills each recipient's rank in the subscriber's view.
        // Retain it because later notifications are direct posts, outside the
        // subscription cast context.
        let view_rank = stream_state.subscriber_rank.unwrap();

        let state = match self.actor_states.get_mut(&stream_state.id) {
            Some(instance) => {
                let state = instance.to_state(&stream_state.id);
                instance
                    .subscribers
                    .push((view_rank, stream_state.subscriber.clone()));
                state
            }
            None => resource::State {
                id: stream_state.id.clone(),
                status: resource::Status::NotExist,
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        };

        // Send the current state immediately at the subscriber's view rank.
        let mut headers = Flattrs::new();
        headers.set(STREAM_STATE_SUBSCRIBER, true);
        stream_state.subscriber.post_with_headers(
            cx,
            headers,
            resource::RankedState {
                rank: resource::Rank::new(view_rank),
                state,
            },
        );
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::KeepaliveGetState<ActorState>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: resource::KeepaliveGetState<ActorState>,
    ) -> anyhow::Result<()> {
        // Same impl as GetState, but additionally update the expiry time on the actor.
        if let Ok(instance_state) =
            self.actor_states
                .get_mut(&message.get_state.id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "attempting to register a keepalive for an actor that doesn't exist: {}",
                        message.get_state.id
                    )
                })
        {
            instance_state.expiry_time = Some(message.expires_after);
        }

        // Forward the rest of the impl to GetState.
        <Self as Handler<resource::GetState<ActorState>>>::handle(self, cx, message.get_state).await
    }
}

/// A local handler to get a new client instance on the proc.
/// This is used to create root client instances.
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct NewClientInstance {
    #[reply]
    pub client_instance: PortHandle<Client>,
}

#[async_trait]
impl Handler<NewClientInstance> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        NewClientInstance { client_instance }: NewClientInstance,
    ) -> anyhow::Result<()> {
        let client = self.proc.client("client");
        client_instance.post(cx, client);
        Ok(())
    }
}

/// A handler to get a clone of the proc managed by this agent.
/// This is used to obtain the local proc from a host mesh.
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct GetProc {
    #[reply]
    pub proc: PortHandle<Proc>,
}

#[async_trait]
impl Handler<GetProc> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetProc { proc }: GetProc,
    ) -> anyhow::Result<()> {
        proc.post(cx, self.proc.clone());
        Ok(())
    }
}

#[async_trait]
impl Handler<SelfCheck> for ProcAgent {
    async fn handle(&mut self, cx: &Context<Self>, _: SelfCheck) -> anyhow::Result<()> {
        // Check each actor's expiry time. If the current time is past the expiry,
        // stop the actor. This allows automatic cleanup when a controller disappears
        // but owned resources remain. It is important that this check runs on the
        // same proc as the child actor itself, since the controller could be dead or
        // disconnected.
        let Some(duration) = &self.mesh_orphan_timeout else {
            return Ok(());
        };
        let duration = *duration;
        let now = std::time::SystemTime::now();

        // Collect expired actors before mutating, since stop_actor borrows &mut self.
        let expired: Vec<(ResourceId, ActorAddr)> = self
            .actor_states
            .iter()
            .filter_map(|(id, state)| {
                let expiry = state.expiry_time?;
                // If a stop was already initiated we don't need to do it again.
                if now > expiry
                    && !state.stop_initiated
                    && let Ok(actor_id) = &state.spawn
                {
                    return Some((id.clone(), actor_id.clone()));
                }
                None
            })
            .collect();

        if !expired.is_empty() {
            tracing::info!(
                "stopping {} orphaned actors past their keepalive expiry",
                expired.len(),
            );
        }

        for (id, actor_id) in expired {
            if let Some(state) = self.actor_states.get_mut(&id) {
                state.stop_initiated = true;
            }
            self.stop_actor_by_id(&actor_id, "orphaned");
        }

        // Reschedule.
        cx.post_after(cx, SelfCheck::default(), duration);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hyperactor::ActorRef;

    use super::*;

    // A no-op actor used to test direct proc-level spawning.
    #[derive(Debug, Default, Serialize, Deserialize)]
    #[hyperactor::export(handlers = [])]
    struct ExtraActor;
    impl hyperactor::Actor for ExtraActor {}
    hyperactor::register_spawnable!(ExtraActor);
    // Verifies that QueryChild(Addr::Proc) on a ProcAgent returns
    // a live IntrospectResult whose children reflect actors spawned
    // directly on the proc — i.e. via proc.spawn_with_label(), which bypasses the
    // gspawn message handler and therefore never triggers
    // publish_introspect_properties.
    //
    // Exercises PA-1 (see mesh_admin module doc).
    //
    // Regression guard for the bug introduced in 9a08d559: removing
    // handle_introspect left publish_introspect_properties as the only
    // update path, which missed supervision-spawned actors (e.g. every
    // sieve actor after sieve[0]). See also
    // mesh_admin::tests::test_proc_children_reflect_directly_spawned_actors.
    #[tokio::test]
    async fn test_query_child_proc_returns_live_children() {
        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::introspect::IntrospectMessage;
        use hyperactor::introspect::IntrospectResult;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "test_proc".to_string()).unwrap();
        let agent_handle = ProcAgent::boot_v1(proc.clone(), None).unwrap();

        // Wait for ProcAgent to finish init.
        agent_handle
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        // Client instance for opening reply ports.
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let agent_id: ActorAddr = proc.proc_addr().actor_addr(PROC_AGENT_ACTOR_NAME);
        let port = agent_id.introspect_port();

        // Helper: send QueryChild(Proc) and return the payload with a
        // timeout so a misrouted reply fails fast rather than hanging.
        let query = |client: &hyperactor::Client| {
            let (reply_port, reply_rx) = client.open_once_port::<IntrospectResult>();
            port.post(
                client,
                IntrospectMessage::QueryChild {
                    child_ref: Addr::Proc(proc.proc_addr().clone()),
                    reply: reply_port.bind(),
                },
            );
            reply_rx
        };
        let recv = |rx: hyperactor::mailbox::OncePortReceiver<IntrospectResult>| async move {
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("QueryChild(Proc) timed out — reply never delivered")
                .expect("reply channel closed")
        };

        // Initial query: ProcAgent itself should appear in children.
        let payload = recv(query(&client)).await;
        // Verify this is a proc node by checking attrs contain node_type=proc.
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&payload.attrs).expect("valid attrs JSON");
        assert_eq!(
            attrs.get(crate::introspect::NODE_TYPE).map(String::as_str),
            Some("proc"),
            "expected node_type=proc in attrs, got {:?}",
            payload.attrs
        );
        assert!(
            payload
                .children
                .iter()
                .any(|c| c.to_string().contains(PROC_AGENT_ACTOR_NAME)),
            "initial children {:?} should contain proc_agent",
            payload.children
        );
        let initial_count = payload.children.len();

        // Spawn an actor directly on the proc, bypassing ProcAgent's
        // gspawn message handler. This is how supervision-spawned
        // actors (e.g. sieve children) are created.
        proc.spawn_with_label("extra_actor", ExtraActor);

        // Second query: extra_actor must appear without any republish.
        let payload2 = recv(query(&client)).await;
        let attrs2: hyperactor_config::Attrs =
            serde_json::from_str(&payload2.attrs).expect("valid attrs JSON");
        assert_eq!(
            attrs2.get(crate::introspect::NODE_TYPE).map(String::as_str),
            Some("proc"),
            "expected node_type=proc in attrs, got {:?}",
            payload2.attrs
        );
        assert!(
            payload2
                .children
                .iter()
                .any(|c| c.to_string().contains("extra_actor")),
            "after direct spawn, children {:?} should contain extra_actor",
            payload2.children
        );
        assert!(
            payload2.children.len() > initial_count,
            "expected at least {} children after direct spawn, got {:?}",
            initial_count + 1,
            payload2.children
        );
    }

    // Exercises S12 (see introspect module doc): introspection must
    // not impair actor liveness. Rapidly spawns and stops
    // actors while concurrently querying QueryChild(Addr::Proc).
    // The spawn/stop loop must complete within the timeout and the
    // iteration count must match -- if DashMap convoy starvation
    // blocks the proc, the timeout fires and the test fails.
    #[tokio::test]
    async fn test_rapid_spawn_stop_does_not_stall_proc_agent() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::introspect::IntrospectMessage;
        use hyperactor::introspect::IntrospectResult;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "test_proc".to_string()).unwrap();
        let agent_handle = ProcAgent::boot_v1(proc.clone(), None).unwrap();

        agent_handle
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let agent_id: ActorAddr = proc.proc_addr().actor_addr(PROC_AGENT_ACTOR_NAME);
        let port = agent_id.introspect_port();

        // Concurrent query task: send QueryChild(Proc) every 10ms.
        let query_client_proc =
            Proc::direct(ChannelTransport::Unix.any(), "query_client".to_string()).unwrap();
        let query_client = query_client_proc.client("qc");
        let query_port = port.clone();
        let query_proc_id = proc.proc_addr().clone();
        let query_count = Arc::new(AtomicUsize::new(0));
        let query_count_clone = query_count.clone();
        let query_task = tokio::spawn(async move {
            loop {
                let (reply_port, reply_rx) = query_client.open_once_port::<IntrospectResult>();
                query_port.post(
                    &query_client,
                    IntrospectMessage::QueryChild {
                        child_ref: Addr::Proc(query_proc_id.clone()),
                        reply: reply_port.bind(),
                    },
                );
                match tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx.recv()).await
                {
                    Ok(Ok(_)) => {
                        query_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {} // Transient failures expected during churn
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        // Rapid spawn/stop loop with liveness timeout.
        const ITERATIONS: usize = 200;
        let mut completed = 0usize;
        let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for i in 0..ITERATIONS {
                let name = format!("churn_{}", i);
                let handle = proc.spawn_with_label(&name, ExtraActor);
                let actor_id = handle.actor_addr().clone();
                if let Some(mut status) = proc.stop_actor(actor_id.id(), "churn".to_string()) {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        status.wait_for(ActorStatus::is_terminal),
                    )
                    .await;
                }
                completed += 1;
            }
        })
        .await;

        query_task.abort();
        let _ = query_task.await; // Join to suppress noisy panic on drop.

        assert!(
            result.is_ok(),
            "spawn/stop loop stalled after {completed}/{ITERATIONS} iterations — \
             DashMap convoy starvation likely"
        );
        assert_eq!(
            completed, ITERATIONS,
            "expected {ITERATIONS} completed iterations, got {completed}"
        );
        assert!(
            query_count.load(Ordering::Relaxed) > 0,
            "concurrent QueryChild queries never succeeded — query task may not have run"
        );

        // Final consistency check: QueryChild should still work.
        let (reply_port, reply_rx) = client.open_once_port::<IntrospectResult>();
        port.post(
            &client,
            IntrospectMessage::QueryChild {
                child_ref: Addr::Proc(proc.proc_addr().clone()),
                reply: reply_port.bind(),
            },
        );
        let final_payload =
            tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx.recv())
                .await
                .expect("final QueryChild timed out")
                .expect("final QueryChild channel closed");
        let attrs: hyperactor_config::Attrs =
            serde_json::from_str(&final_payload.attrs).expect("valid attrs JSON");
        assert_eq!(
            attrs.get(crate::introspect::NODE_TYPE).map(String::as_str),
            Some("proc"),
        );
    }

    #[tokio::test]
    async fn test_stream_state_and_unsubscribe() {
        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;

        use crate::resource::CreateOrUpdateClient;
        use crate::resource::GetStateClient;
        use crate::resource::StopClient;
        use crate::resource::StreamStateClient;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "test_proc".to_string()).unwrap();
        let agent_handle = ProcAgent::boot_v1(proc.clone(), None).unwrap();
        agent_handle
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        let client = proc.client("client");
        let agent_ref: ActorRef<ProcAgent> = agent_handle.bind();

        // A missing actor has no `ActorState` payload, so the streamed message
        // itself must retain the subscription rank.
        let missing_name =
            ResourceId::singleton(hyperactor::id::Label::new("missing-actor").unwrap());
        let (missing_port, mut missing_rx) =
            client.open_port::<resource::RankedState<ActorState>>();
        agent_ref
            .stream_state(
                &client,
                missing_name,
                resource::Rank::new(6),
                missing_port.bind(),
            )
            .await
            .unwrap();
        let missing = missing_rx.recv().await.expect("missing state reply");
        assert_eq!(missing.rank.unwrap(), 6);
        assert_eq!(missing.state.status, resource::Status::NotExist);
        assert_eq!(missing.state.state, None);

        let actor_type = hyperactor::actor::remote::Remote::collect()
            .name_of::<ExtraActor>()
            .unwrap()
            .to_string();
        let actor_params =
            bincode::serde::encode_to_vec(&ExtraActor, bincode::config::legacy()).unwrap();
        let actor_name = ResourceId::singleton(hyperactor::id::Label::new("test-actor").unwrap());

        // 1. Spawn an actor via CreateOrUpdate.
        agent_ref
            .create_or_update(
                &client,
                actor_name.clone(),
                resource::Rank::new(0),
                ActorSpec {
                    actor_type: actor_type.clone(),
                    params_data: actor_params.clone(),
                    actor_environment: ActorEnvironment::default(),
                },
            )
            .await
            .unwrap();

        // 2. Subscribe to state updates.
        let subscriber_rank = 7;
        let (sub_port, mut sub_rx) = client.open_port::<resource::RankedState<ActorState>>();
        agent_ref
            .stream_state(
                &client,
                actor_name.clone(),
                resource::Rank::new(subscriber_rank),
                sub_port.bind(),
            )
            .await
            .unwrap();

        // 3. Should receive the initial state (Running).
        let initial = sub_rx.recv().await.expect("subscriber channel error");
        assert_eq!(initial.rank.unwrap(), subscriber_rank);
        assert_eq!(initial.state.status, resource::Status::Running);
        assert!(initial.state.state.is_some());

        // 4. Send Stop — should receive Stopping.
        agent_ref
            .stop(&client, actor_name.clone(), "test".to_string())
            .await
            .unwrap();

        let stopping = sub_rx.recv().await.expect("subscriber channel error");
        assert_eq!(stopping.rank.unwrap(), subscriber_rank);
        assert_eq!(stopping.state.status, resource::Status::Stopping);

        // 5. Wait for the Stopped supervision event update.
        let stopped = sub_rx.recv().await.expect("subscriber channel error");
        assert_eq!(stopped.rank.unwrap(), subscriber_rank);
        assert_eq!(stopped.state.status, resource::Status::Stopped);

        // 6. Test implicit unsubscription via undeliverable.
        let actor_name_2 =
            ResourceId::singleton(hyperactor::id::Label::new("test-actor-2").unwrap());
        agent_ref
            .create_or_update(
                &client,
                actor_name_2.clone(),
                resource::Rank::new(1),
                ActorSpec {
                    actor_type: actor_type.clone(),
                    params_data: actor_params.clone(),
                    actor_environment: ActorEnvironment::default(),
                },
            )
            .await
            .unwrap();

        let (sub_port_2, mut sub_rx_2) = client.open_port::<resource::RankedState<ActorState>>();
        agent_ref
            .stream_state(
                &client,
                actor_name_2.clone(),
                resource::Rank::new(8),
                sub_port_2.bind(),
            )
            .await
            .unwrap();

        let initial_2 = sub_rx_2.recv().await.expect("subscriber 2 channel error");
        assert_eq!(initial_2.rank.unwrap(), 8);
        assert_eq!(initial_2.state.status, resource::Status::Running);

        // Drop the receiver so the next send bounces as undeliverable.
        drop(sub_rx_2);

        // Stop the second actor — triggers notify_status_changed to the
        // dead subscriber. ProcAgent should handle the undeliverable
        // gracefully.
        agent_ref
            .stop(
                &client,
                actor_name_2.clone(),
                "test unsubscribe".to_string(),
            )
            .await
            .unwrap();

        // Wait for actor_2 to reach terminal state via a new stream subscription.
        let (sub_port_3, mut sub_rx_3) = client.open_port::<resource::RankedState<ActorState>>();
        agent_ref
            .stream_state(
                &client,
                actor_name_2.clone(),
                resource::Rank::new(9),
                sub_port_3.bind(),
            )
            .await
            .unwrap();
        loop {
            let state = sub_rx_3.recv().await.expect("subscriber 3 channel error");
            assert_eq!(state.rank.unwrap(), 9);
            if state.state.status.is_terminating() {
                break;
            }
        }

        // Verify ProcAgent is still alive after the undeliverable was handled.
        let state = agent_ref
            .get_state(&client, actor_name_2.clone())
            .await
            .unwrap();
        assert!(
            state.status.is_terminating(),
            "expected terminating status, got {:?}",
            state.status,
        );
    }

    // ── PD-4/PD-5: live proc-agent queue pressure test ────────

    // A blocking actor for inducing queue pressure. Uses a shared
    // Notify for the block/unblock protocol since actor messages
    // must be Serialize + Clone.
    #[derive(Debug, Default, Serialize, Deserialize)]
    #[hyperactor::export(handlers = [BlockMsg])]
    struct BlockActor {
        #[serde(skip)]
        gate: Option<Arc<tokio::sync::Notify>>,
    }
    impl hyperactor::Actor for BlockActor {}

    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        Named,
        hyperactor::Handler,
        hyperactor::HandleClient
    )]
    enum BlockMsg {
        /// Block until the shared Notify fires.
        Block(),
        /// No-op message to queue behind a blocked Block.
        Noop(),
    }
    wirevalue::register_type!(BlockMsg);

    #[async_trait::async_trait]
    #[hyperactor::handle(BlockMsg)]
    impl BlockMsgHandler for BlockActor {
        async fn block(&mut self, _cx: &hyperactor::Context<Self>) -> Result<(), anyhow::Error> {
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            Ok(())
        }
        async fn noop(&mut self, _cx: &hyperactor::Context<Self>) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    // PD-4/PD-5: QueryChild(Proc) returns non-zero queue stats
    // while actors are under induced pressure. This proves the
    // live proc-agent introspection path carries the queue depth
    // signal that the TUI depends on.
    //
    // Queue depth is an instantaneous snapshot at query time,
    // not backlog history.
    #[tokio::test]
    async fn test_query_child_proc_queue_depth_under_pressure() {
        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::introspect::IntrospectMessage;
        use hyperactor::introspect::IntrospectResult;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "qd_proc".to_string()).unwrap();
        let agent_handle = ProcAgent::boot_v1(proc.clone(), None).unwrap();

        agent_handle
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        let client_proc =
            Proc::direct(ChannelTransport::Unix.any(), "qd_client".to_string()).unwrap();
        let client = client_proc.client("client");

        // Spawn a blocking actor with a shared gate.
        let gate = Arc::new(tokio::sync::Notify::new());
        let blocker = proc.spawn(BlockActor {
            gate: Some(Arc::clone(&gate)),
        });

        // Block the actor and queue additional work behind it.
        blocker.block(&client).await.unwrap();
        // Give the actor time to enter the handler.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        blocker.noop(&client).await.unwrap();
        blocker.noop(&client).await.unwrap();

        // QueryChild(Proc) — same aggregation logic as mesh-admin
        // resolution.
        let agent_id: ActorAddr = proc.proc_addr().actor_addr(PROC_AGENT_ACTOR_NAME);
        let port = agent_id.introspect_port();

        // Poll until queue stats are non-zero.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let (reply_port, reply_rx) = client.open_once_port::<IntrospectResult>();
            port.post(
                &client,
                IntrospectMessage::QueryChild {
                    child_ref: Addr::Proc(proc.proc_addr().clone()),
                    reply: reply_port.bind(),
                },
            );
            let payload = tokio::time::timeout(std::time::Duration::from_secs(3), reply_rx.recv())
                .await
                .expect("QueryChild timed out")
                .expect("reply channel closed");

            let attrs: hyperactor_config::Attrs =
                serde_json::from_str(&payload.attrs).expect("valid attrs JSON");

            let total = attrs
                .get(crate::introspect::ACTOR_WORK_QUEUE_DEPTH_TOTAL)
                .copied()
                .unwrap_or(0);
            let max = attrs
                .get(crate::introspect::ACTOR_WORK_QUEUE_DEPTH_MAX)
                .copied()
                .unwrap_or(0);

            if total > 0 {
                assert!(max > 0, "max should be > 0 when total is {total}");
                assert!(max <= total, "PD-1: max ({max}) <= total ({total})");
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for non-zero queue depth in QueryChild(Proc)",
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Unblock the actor.
        gate.notify_one();
    }

    hyperactor_config::attrs::declare_attrs! {
        attr AENV_SPEC_TAG: u64;
        attr AENV_SPEC_LABEL: String;
    }

    // AENV-3: the actor environment travels with `ActorSpec` through the same
    // positional codec used for every remote spawn, and `ActorSpec` equality
    // composes the environment's semantic (order-independent) equality rather
    // than a raw-buffer comparison.
    #[test]
    fn actor_environment_serializes_in_actor_spec() {
        let mut env = ActorEnvironment::default();
        env.set(AENV_SPEC_TAG, 7u64).expect("insert tag");
        env.set(AENV_SPEC_LABEL, "root".to_string())
            .expect("insert label");

        let spec = ActorSpec {
            actor_type: "probe".to_string(),
            params_data: vec![1, 2, 3],
            actor_environment: env.clone(),
        };

        let bytes = bincode::serde::encode_to_vec(&spec, bincode::config::legacy()).unwrap();
        let restored: ActorSpec =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy())
                .map(|(v, _)| v)
                .unwrap();

        assert_eq!(restored.actor_environment.get(AENV_SPEC_TAG), Some(7u64));
        assert_eq!(
            restored.actor_environment.get(AENV_SPEC_LABEL),
            Some("root".to_string())
        );
        assert_eq!(restored.actor_environment, env);
        // Derived `ActorSpec` equality composes the semantic environment
        // equality.
        assert_eq!(restored, spec);
    }

    // ----- client-root capability proofs -----

    // Boot a root ProcAgent and bind the client-root API on it.
    async fn boot_client_root() -> (
        hyperactor::Proc,
        ActorHandle<ProcAgent>,
        crate::client_root::ClientRootRef,
    ) {
        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string()).unwrap();
        let agent = ProcAgent::boot_v1(proc.clone(), None).unwrap();
        agent
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();
        let client_root = crate::client_root::ClientRootRef::bind(&agent);
        (proc, agent, client_root)
    }

    // Proof 1: the capability serde-round-trips its bound reference, while its
    // display/Debug are redacted and textual parse is rejected (CROOT-2).
    #[tokio::test]
    async fn client_root_ref_redacts_and_round_trips() {
        use hyperactor_config::AttrValue;

        use crate::client_root::ClientRootRef;

        let (_proc, _agent, client_root) = boot_client_root().await;

        let bytes = bincode::serde::encode_to_vec(&client_root, bincode::config::legacy()).unwrap();
        let restored: ClientRootRef =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy())
                .map(|(v, _)| v)
                .unwrap();
        assert_eq!(
            restored, client_root,
            "capability must round-trip via serde"
        );

        assert_eq!(client_root.display(), "<redacted>");
        assert_eq!(format!("{:?}", client_root), "ClientRootRef(<redacted>)");
        assert!(
            ClientRootRef::parse("anything").is_err(),
            "a bearer capability must not be reconstructible from text"
        );
    }

    // Proof 10: binding the API twice on the same root returns the same
    // reference and adds no registration/activation state (CROOT-1).
    #[tokio::test]
    async fn client_root_bind_is_idempotent() {
        use crate::client_root::ClientRootRef;

        let (_proc, agent, first) = boot_client_root().await;
        let second = ClientRootRef::bind(&agent);
        assert_eq!(first, second);
    }

    // Proofs 3/5: ensuring the same service returns one root-owned actor, and a
    // requester exiting does not stop it — a later requester observes the same
    // address.
    #[tokio::test]
    async fn client_root_ensure_is_root_owned_and_single_identity() {
        use crate::client_root::ClientRootService;

        let (proc, _agent, client_root) = boot_client_root().await;
        let service = ClientRootService::<ExtraActor>::declare("probe-service");

        let caller_a = proc.client("caller-a");
        let first = service
            .ensure(&caller_a, &client_root, ())
            .await
            .expect("first ensure creates the service");
        // The first requester context is gone; a later ensure still reuses the
        // one service (CROOT-3). Departed-requester survival and the in-flight
        // drop are covered by `client_root_departed_requester_does_not_fault_root`.
        drop(caller_a);

        let caller_b = proc.client("caller-b");
        let second = service
            .ensure(&caller_b, &client_root, ())
            .await
            .expect("second ensure reuses the service after requester exit");
        assert_eq!(
            first.actor_addr(),
            second.actor_addr(),
            "identical ensures must resolve to the one root-owned service (CROOT-3)"
        );
    }

    // A second registered actor type, distinct from `ExtraActor`, so a repeat
    // ensure under the same name with a different type conflicts.
    #[derive(Debug, Default, Serialize, Deserialize)]
    #[hyperactor::export(handlers = [])]
    struct OtherActor;
    impl hyperactor::Actor for OtherActor {}
    hyperactor::register_spawnable!(OtherActor);

    // CROOT-3: the same service name with a different actor type conflicts and
    // surfaces as a transparent `Error::ClientRootError(ConflictingSpec)`.
    #[tokio::test]
    async fn client_root_conflicting_type_is_rejected() {
        use crate::client_root::ClientRootError;
        use crate::client_root::ClientRootService;

        let (proc, _agent, client_root) = boot_client_root().await;
        let caller = proc.client("caller");

        ClientRootService::<ExtraActor>::declare("shared")
            .ensure(&caller, &client_root, ())
            .await
            .expect("first ensure creates the service");

        let err = ClientRootService::<OtherActor>::declare("shared")
            .ensure(&caller, &client_root, ())
            .await
            .expect_err("a different actor type for the same name must conflict");
        assert!(
            matches!(
                err,
                crate::Error::ClientRootError(ClientRootError::ConflictingSpec { .. })
            ),
            "expected ClientRootError::ConflictingSpec, got {err:?}"
        );
    }

    // H5 / CROOT-3: the service actor uses a fresh instance id, not a
    // deterministic `client-root-<name>` singleton, avoiding the predictable
    // collision a name-derived id would create with a mesh actor.
    #[tokio::test]
    async fn client_root_service_uses_instance_id() {
        use crate::client_root::ClientRootService;

        let (proc, _agent, client_root) = boot_client_root().await;
        let caller = proc.client("caller");
        let svc = ClientRootService::<ExtraActor>::declare("probe")
            .ensure(&caller, &client_root, ())
            .await
            .expect("ensure creates the service");
        assert!(
            !svc.actor_addr().is_root(),
            "a client-root service must use a fresh instance uid, not a singleton id"
        );
    }

    // A spawnable actor whose remote constructor increments a global counter, so
    // a test can prove single-flight *construction*, not just a single identity.
    static COUNTING_ACTOR_BUILDS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    #[derive(Debug)]
    #[hyperactor::export(handlers = [])]
    #[hyperactor::spawnable]
    struct CountingActor;
    impl hyperactor::Actor for CountingActor {}

    #[async_trait::async_trait]
    impl hyperactor::RemoteSpawn for CountingActor {
        type Params = ();
        async fn new(_params: (), _environment: Flattrs) -> anyhow::Result<Self> {
            COUNTING_ACTOR_BUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Self)
        }
    }

    // Proof 2: concurrent identical ensures collapse through the serial root
    // ProcAgent to a single service identity AND a single construction.
    #[tokio::test]
    async fn client_root_concurrent_ensures_collapse() {
        use std::sync::atomic::Ordering;

        use crate::client_root::ClientRootService;

        COUNTING_ACTOR_BUILDS.store(0, Ordering::SeqCst);
        let (proc, _agent, client_root) = boot_client_root().await;
        let ensure = |name: &'static str| {
            let caller = proc.client(name);
            let client_root = &client_root;
            async move {
                ClientRootService::<CountingActor>::declare("concurrent")
                    .ensure(&caller, client_root, ())
                    .await
                    .expect("ensure succeeds")
                    .actor_addr()
                    .clone()
            }
        };

        let (a, b, c, d) = tokio::join!(
            ensure("caller-0"),
            ensure("caller-1"),
            ensure("caller-2"),
            ensure("caller-3"),
        );
        assert_eq!(a, b, "concurrent ensures must resolve to one service");
        assert_eq!(a, c, "concurrent ensures must resolve to one service");
        assert_eq!(a, d, "concurrent ensures must resolve to one service");
        assert_eq!(
            COUNTING_ACTOR_BUILDS.load(Ordering::SeqCst),
            1,
            "single-flight: identical concurrent ensures must construct once"
        );
    }

    // H3: `service_result` fails closed for a service that has gone terminal or
    // never spawned; only a running service returns its address.
    #[tokio::test]
    async fn client_root_service_result_fails_closed_on_terminal() {
        use hyperactor::actor::ActorStatus;

        use crate::client_root::ClientRootError;

        let (_proc, agent, _client_root) = boot_client_root().await;
        let addr = agent.bind::<ProcAgent>().actor_addr().clone();
        let name = Label::new("svc").unwrap();
        let id = ResourceId::instance(name.clone());

        let mk = |spawn: Result<ActorAddr, anyhow::Error>,
                  stop_initiated: bool,
                  supervision_event: Option<ActorSupervisionEvent>| {
            ActorInstanceState {
                create_rank: 0,
                spawn,
                stop_initiated,
                supervision_event,
                subscribers: Vec::new(),
                expiry_time: None,
                generation: 1,
                pending_wait_status: Vec::new(),
            }
        };

        // Running -> Ok(addr).
        let mut states = HashMap::new();
        states.insert(id.clone(), mk(Ok(addr.clone()), false, None));
        assert_eq!(
            ProcAgent::service_result(&states, &id, &name).expect("running service resolves"),
            addr
        );

        // Stopping (stop initiated, no event yet) -> Unavailable.
        let mut states = HashMap::new();
        states.insert(id.clone(), mk(Ok(addr.clone()), true, None));
        assert!(matches!(
            ProcAgent::service_result(&states, &id, &name),
            Err(ClientRootError::Unavailable { .. })
        ));

        // Terminal via a supervision event -> Unavailable.
        let mut states = HashMap::new();
        let event = ActorSupervisionEvent::new(
            addr.clone(),
            None,
            ActorStatus::Stopped("done".into()),
            None,
        );
        states.insert(id.clone(), mk(Ok(addr.clone()), false, Some(event)));
        assert!(matches!(
            ProcAgent::service_result(&states, &id, &name),
            Err(ClientRootError::Unavailable { .. })
        ));

        // Cached spawn failure -> Spawn.
        let mut states = HashMap::new();
        states.insert(id.clone(), mk(Err(anyhow::anyhow!("boom")), false, None));
        assert!(matches!(
            ProcAgent::service_result(&states, &id, &name),
            Err(ClientRootError::Spawn { .. })
        ));

        // Missing state -> Spawn.
        let states: HashMap<ResourceId, ActorInstanceState> = HashMap::new();
        assert!(matches!(
            ProcAgent::service_result(&states, &id, &name),
            Err(ClientRootError::Spawn { .. })
        ));
    }

    // CROOT-6: reading the capability from an environment that lacks it fails
    // closed with the typed `MissingClientRoot`, not a silent absence.
    #[tokio::test]
    async fn client_root_from_env_absent_fails_closed() {
        use crate::client_root::ClientRootRef;

        let (proc, _agent, _client_root) = boot_client_root().await;
        let caller = proc.client("no-capability");
        let environment = hyperactor::context::Actor::instance(&caller).actor_environment();
        assert!(
            matches!(
                ClientRootRef::from_env(environment),
                Err(crate::Error::MissingClientRoot)
            ),
            "a caller without the capability must fail closed with MissingClientRoot"
        );
    }

    // Boot a root ProcAgent with a shutdown channel so a StopAll signals the
    // exit code instead of calling `process::exit`.
    async fn boot_client_root_with_shutdown() -> (
        hyperactor::Proc,
        ActorHandle<ProcAgent>,
        crate::client_root::ClientRootRef,
        tokio::sync::oneshot::Receiver<i32>,
    ) {
        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string()).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let agent = ProcAgent::boot_v1(proc.clone(), Some(tx)).unwrap();
        agent
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();
        let client_root = crate::client_root::ClientRootRef::bind(&agent);
        (proc, agent, client_root, rx)
    }

    // A root-owned service that reports the client-root capability it reads back
    // from its own environment, so a test can compare its exact identity.
    #[derive(Debug, Serialize, Deserialize, Named)]
    struct ClientRootObservation(Option<crate::client_root::ClientRootRef>);
    wirevalue::register_type!(ClientRootObservation);

    #[derive(Debug, Serialize, Deserialize, Named)]
    struct ObserveClientRoot {
        reply: hyperactor::OncePortRef<ClientRootObservation>,
    }
    wirevalue::register_type!(ObserveClientRoot);

    #[derive(Debug, Default, Serialize, Deserialize)]
    #[hyperactor::export(handlers = [ObserveClientRoot])]
    struct EnvProbeActor;
    impl hyperactor::Actor for EnvProbeActor {}
    hyperactor::register_spawnable!(EnvProbeActor);

    #[async_trait::async_trait]
    impl Handler<ObserveClientRoot> for EnvProbeActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: ObserveClientRoot,
        ) -> anyhow::Result<()> {
            let environment = hyperactor::context::Actor::instance(cx).actor_environment();
            let observed = crate::client_root::ClientRootRef::from_env(environment).ok();
            msg.reply.post(cx, ClientRootObservation(observed));
            Ok(())
        }
    }

    // H1 / CROOT-11: a requester that departs before the reply lands must not
    // fault the root. The reply is posted to a dropped once-port, which without
    // `return_undeliverable(false)` would return to the root's fatal
    // `handle_invalid_reference`. The root must still serve a later request.
    #[tokio::test]
    async fn client_root_departed_requester_does_not_fault_root() {
        use std::time::Duration;

        use hyperactor::ActorRef;
        use hyperactor::Endpoint;

        use crate::client_root::ClientRootApi;
        use crate::client_root::ClientRootService;

        let (proc, _agent, client_root) = boot_client_root().await;

        {
            let caller = proc.client("ephemeral");
            let (reply_handle, reply_receiver) =
                caller.open_once_port::<EnsureClientRootServiceReply>();
            let reply = reply_handle.bind();
            // Drop the receiver BEFORE posting so the reply is guaranteed
            // undeliverable; otherwise the root could answer before the port is
            // gone and the fatal-return path would never be exercised.
            drop(reply_receiver);
            let actor_type = Remote::collect()
                .name_of::<ExtraActor>()
                .unwrap()
                .to_string();
            let params = bincode::serde::encode_to_vec((), bincode::config::legacy()).unwrap();
            let api = ActorRef::<ClientRootApi>::attest(client_root.addr().clone());
            api.post(
                &caller,
                EnsureClientRootService {
                    service_name: Label::new("ephemeral-svc").unwrap(),
                    actor_type,
                    params,
                    reply,
                },
            );
            drop(caller);
        }

        // Two sequential survivor ensures. The undeliverable reply-return is
        // enqueued to the root while it handles the request above, so by the time
        // the second ensure completes the root has processed it; a fatal return
        // would fail (or hang) one of these rather than slip past a single fast
        // reply.
        let survivor = proc.client("survivor");
        for name in ["after-departure-1", "after-departure-2"] {
            tokio::time::timeout(
                Duration::from_secs(15),
                ClientRootService::<ExtraActor>::declare(name).ensure(&survivor, &client_root, ()),
            )
            .await
            .expect("root must stay responsive after a departed requester")
            .expect("root must serve a new ensure after a departed requester");
        }
    }

    // A stop-gated service: its handler blocks until released, so a test can hold
    // a proc open across a StopAll and then observe shutdown run to completion.
    // The gate is process-global (statics) because a remote-spawned actor cannot
    // carry a non-serializable field.
    static GATE_ENTERED: tokio::sync::Notify = tokio::sync::Notify::const_new();
    static GATE_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();

    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        Named,
        hyperactor::Handler,
        hyperactor::HandleClient
    )]
    enum GateMsg {
        Block(),
    }
    wirevalue::register_type!(GateMsg);

    #[derive(Debug, Default, Serialize, Deserialize)]
    #[hyperactor::export(handlers = [GateMsg])]
    struct GateActor;
    impl hyperactor::Actor for GateActor {}
    hyperactor::register_spawnable!(GateActor);

    #[async_trait::async_trait]
    #[hyperactor::handle(GateMsg)]
    impl GateMsgHandler for GateActor {
        async fn block(&mut self, _cx: &Context<Self>) -> anyhow::Result<()> {
            GATE_ENTERED.notify_one();
            GATE_RELEASE.notified().await;
            Ok(())
        }
    }

    // H2: once shutdown has begun a NEW ensure is refused, so a service cannot
    // escape the StopAll snapshot and strand shutdown. A gated keeper holds the
    // proc open across StopAll; releasing it lets shutdown run to completion.
    #[tokio::test]
    async fn client_root_ensure_rejected_during_shutdown() {
        use std::time::Duration;

        use hyperactor::Endpoint;

        use crate::client_root::ClientRootError;
        use crate::client_root::ClientRootService;

        let (proc, agent, client_root, shutdown_rx) = boot_client_root_with_shutdown().await;

        // A gated keeper never reaches terminal state while blocked, so StopAll
        // defers shutdown until we release it.
        let keeper = ClientRootService::<GateActor>::declare("keeper")
            .ensure(&proc.client("keeper-caller"), &client_root, ())
            .await
            .expect("keeper service is created before shutdown");
        keeper.post(&proc.client("gate-driver"), GateMsg::Block());
        GATE_ENTERED.notified().await;

        // Begin shutdown; the blocked keeper defers it. A new ensure is refused.
        let control = proc.client("control");
        agent.bind::<ProcAgent>().post(
            &control,
            resource::StopAll {
                reason: "test shutdown".to_string(),
            },
        );
        let err = tokio::time::timeout(
            Duration::from_secs(15),
            ClientRootService::<ExtraActor>::declare("too-late").ensure(&control, &client_root, ()),
        )
        .await
        .expect("agent must answer while the keeper defers shutdown")
        .expect_err("a new ensure during shutdown must be rejected");
        assert!(
            matches!(
                err,
                crate::Error::ClientRootError(ClientRootError::Unavailable { .. })
            ),
            "ensure during shutdown must be Unavailable, got {err:?}"
        );

        // Release the keeper; it terminates and shutdown completes, delivering a
        // clean exit code through `shutdown_rx`.
        GATE_RELEASE.notify_one();
        let code = tokio::time::timeout(Duration::from_secs(30), shutdown_rx)
            .await
            .expect("shutdown must complete once the last actor terminates")
            .expect("shutdown channel delivered an exit code");
        assert_eq!(code, 0, "clean shutdown exit code");
    }

    // CROOT-7: a service spawned through ensure inherits the client-root
    // capability in its environment and reads it back with `from_env`.
    #[tokio::test]
    async fn client_root_service_observes_inherited_capability() {
        use std::time::Duration;

        use hyperactor::Endpoint;

        use crate::client_root::ClientRootService;

        let (proc, _agent, client_root) = boot_client_root().await;
        let caller = proc.client("caller");
        let probe = ClientRootService::<EnvProbeActor>::declare("env-probe")
            .ensure(&caller, &client_root, ())
            .await
            .expect("ensure creates the probe service");

        let (reply_handle, reply_receiver) = caller.open_once_port::<ClientRootObservation>();
        probe.post(
            &caller,
            ObserveClientRoot {
                reply: reply_handle.bind(),
            },
        );
        let ClientRootObservation(observed) =
            tokio::time::timeout(Duration::from_secs(15), reply_receiver.recv())
                .await
                .expect("probe must reply")
                .expect("reply channel is open");
        assert_eq!(
            observed,
            Some(client_root.clone()),
            "a service must inherit the exact client-root capability, not merely some capability"
        );
    }
}
