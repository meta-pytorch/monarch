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
use std::mem::take;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::time::Duration;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Data;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortHandle;
use hyperactor::RefClient;
use hyperactor::Unbind;
use hyperactor::actor::handle_undeliverable_message;
use hyperactor::actor::remote::Remote;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::IntoBoxedMailboxSender;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::proc::Proc;
use hyperactor::reference as hyperactor_reference;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::Name;
use crate::pyspy::PySpyOpts;
use crate::pyspy::PySpyWorker;
use crate::resource;

/// Actor name used when spawning the proc agent on user procs.
pub const PROC_AGENT_ACTOR_NAME: &str = "proc_agent";

declare_attrs! {
    /// Whether to self kill actors, procs, and hosts whose owner is not reachable.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ORPHAN_TIMEOUT".to_string()),
        Some("mesh_orphan_timeout".to_string()),
    ))
    pub attr MESH_ORPHAN_TIMEOUT: Duration = Duration::from_secs(60);

    /// Header tag for StreamState subscriber messages. When present on an
    /// undeliverable envelope, ProcAgent removes the dead subscriber instead
    /// of treating it as an error.
    attr STREAM_STATE_SUBSCRIBER: bool;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Named)]
pub enum GspawnResult {
    Success {
        rank: usize,
        actor_id: hyperactor_reference::ActorId,
    },
    Error(String),
}
wirevalue::register_type!(GspawnResult);

/// Request a py-spy stack dump from this process.
///
/// The ProcAgent runs inside the target OS process (1:1 mapping).
/// py-spy attaches to `std::process::id()` to capture Python stacks.
/// See PS-1 in `introspect` module doc.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct PySpyDump {
    /// Include per-thread stacks.
    pub threads: bool,
    /// Include native C/C++ frames for threads that have Python frames
    /// (`--native`).
    pub native: bool,
    /// Include native C/C++ frames for all threads, even those without
    /// Python frames (`--native-all`).
    pub native_all: bool,
    /// Use nonblocking mode (py-spy reads without pausing the target).
    pub nonblocking: bool,
    /// Reply port for the result.
    #[reply]
    pub result: hyperactor_reference::OncePortRef<crate::pyspy::PySpyResult>,
}
wirevalue::register_type!(PySpyDump);

/// Deferred republish of introspect properties.
///
/// Sent as a zero-delay self-message from the supervision event
/// handler so it returns immediately without blocking the ProcAgent
/// message loop. Multiple rapid supervision events (e.g., 4 actors
/// failing simultaneously via broadcast) coalesce into a single
/// republish via the `introspect_dirty` flag.
///
/// Without this, calling `publish_introspect_properties` inline in
/// the supervision handler starves `GetRankStatus` polls from the
/// `ActorMeshController`, preventing `__supervise__` from firing
/// within the test timeout. See D94960791 for the root cause
/// analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
struct RepublishIntrospect;
wirevalue::register_type!(RepublishIntrospect);

/// Collect live actor children and system actor children from the
/// proc's instance DashMap using `all_instance_keys()` with point
/// lookups. This avoids the convoy starvation from `all_actor_ids()`
/// which holds shard read locks while doing heavy per-entry work.
/// See S12 in `introspect` module doc.
fn collect_live_children(proc: &hyperactor::Proc) -> (Vec<String>, Vec<String>) {
    let all_keys = proc.all_instance_keys();
    let mut children = Vec::with_capacity(all_keys.len());
    let mut system_children = Vec::new();
    for id in all_keys {
        if let Some(cell) = proc.get_instance(&id) {
            let ref_str = id.to_string();
            if cell.is_system() {
                system_children.push(ref_str.clone());
            }
            children.push(ref_str);
        }
    }
    (children, system_children)
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Handler,
    HandleClient,
    RefClient,
    Named
)]
pub(crate) enum MeshAgentMessage {
    /// Configure the proc in the mesh.
    Configure {
        /// The rank of this proc in the mesh.
        rank: usize,
        /// The forwarder to send messages to unknown destinations.
        forwarder: ChannelAddr,
        /// The supervisor port to which the agent should report supervision events.
        supervisor: Option<hyperactor_reference::PortRef<ActorSupervisionEvent>>,
        /// An address book to use for direct dialing.
        address_book: HashMap<hyperactor_reference::ProcId, ChannelAddr>,
        /// The agent should write its rank to this port when it successfully
        /// configured.
        configured: hyperactor_reference::PortRef<usize>,
        /// If true, and supervisor is None, record supervision events to be reported
        record_supervision_events: bool,
    },

    Status {
        /// The status of the proc.
        /// To be replaced with fine-grained lifecycle status,
        /// and to use aggregation.
        status: hyperactor_reference::PortRef<(usize, bool)>,
    },

    /// Spawn an actor on the proc to the provided name.
    Gspawn {
        /// registered actor type
        actor_type: String,
        /// spawned actor name
        actor_name: String,
        /// serialized parameters
        params_data: Data,
        /// reply port; the proc should send its rank to indicated a spawned actor
        status_port: hyperactor_reference::PortRef<GspawnResult>,
    },
}

/// Internal configuration state of the mesh agent.
#[derive(Debug, EnumAsInner, Default)]
enum State {
    UnconfiguredV0 {
        sender: ReconfigurableMailboxSender,
    },

    ConfiguredV0 {
        sender: ReconfigurableMailboxSender,
        rank: usize,
        supervisor: Option<hyperactor_reference::PortRef<ActorSupervisionEvent>>,
    },

    V1,

    #[default]
    Invalid,
}

impl State {
    fn rank(&self) -> Option<usize> {
        match self {
            State::ConfiguredV0 { rank, .. } => Some(*rank),
            _ => None,
        }
    }

    fn supervisor(&self) -> Option<hyperactor_reference::PortRef<ActorSupervisionEvent>> {
        match self {
            State::ConfiguredV0 { supervisor, .. } => supervisor.clone(),
            _ => None,
        }
    }
}

/// Actor state used for v1 API.
#[derive(Debug)]
struct ActorInstanceState {
    create_rank: usize,
    spawn: Result<hyperactor_reference::ActorId, anyhow::Error>,
    /// True once a stop signal has been sent. This does *not* mean the actor
    /// has reached a terminal state — that is determined by observing
    /// supervision events.
    stop_initiated: bool,
    /// The supervision event observed for this actor, if it has reached
    /// terminal state.
    supervision_event: Option<ActorSupervisionEvent>,
    /// Streaming subscribers that receive `State<ActorState>` on every
    /// state change. Dead subscribers are removed via undeliverable handling.
    subscribers: Vec<hyperactor_reference::PortRef<resource::State<ActorState>>>,
    /// The time at which the actor should be considered expired if no further
    /// keepalive is received. `None` meaning it will never expire.
    expiry_time: Option<std::time::SystemTime>,
    /// Monotonic generation counter, incremented on every state-mutating
    /// operation (spawn, stop, supervision event). Used for last-writer-wins
    /// ordering in the mesh controller.
    generation: u64,
    /// Pending `WaitRankStatus` callers: each entry is the minimum
    /// status threshold and the reply port to send once the threshold
    /// is met.
    pending_wait_status: Vec<(
        resource::Status,
        hyperactor_reference::PortRef<crate::StatusOverlay>,
    )>,
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
    fn to_state(&self, name: &Name) -> resource::State<ActorState> {
        let status = self.status();
        let actor_state = self.spawn.as_ref().ok().map(|actor_id| ActorState {
            actor_id: actor_id.clone(),
            create_rank: self.create_rank,
            supervision_events: self.supervision_event.clone().into_iter().collect(),
        });
        resource::State {
            name: name.clone(),
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
    fn notify_status_changed(&mut self, cx: &impl hyperactor::context::Actor, name: &Name) {
        // Streaming subscribers (persistent).
        let state = self.to_state(name);
        for subscriber in &self.subscribers {
            let mut headers = Flattrs::new();
            headers.set(STREAM_STATE_SUBSCRIBER, true);
            if let Err(e) = subscriber.send_with_headers(cx, headers, state.clone()) {
                tracing::warn!(
                    "failed to send state update to subscriber {}: {}",
                    subscriber.port_id(),
                    e,
                );
            }
        }

        // One-shot waiters (predicated).
        let status = self.status();
        self.pending_wait_status.retain(|(min_status, reply)| {
            if status >= *min_status {
                let rank = self.create_rank;
                let overlay =
                    crate::StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status.clone())])
                        .expect("valid single-run overlay");
                let _ = reply.send(cx, overlay);
                false
            } else {
                true
            }
        });
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    Named,
    Bind,
    Unbind
)]
struct SelfCheck {}

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
/// addressed via `ActorRef`/`PortRef` across processes, and the
/// global-root-client undeliverable → supervision pipeline would
/// degrade to log-only behavior (events become undeliverable again or
/// are dropped).
///
/// See GC-1 in `global_context` module doc.
#[hyperactor::export(
    handlers=[
        MeshAgentMessage,
        ActorSupervisionEvent,
        resource::CreateOrUpdate<ActorSpec> { cast = true },
        resource::Stop { cast = true },
        resource::StopAll { cast = true },
        resource::GetState<ActorState> { cast = true },
        resource::StreamState<ActorState> { cast = true },
        resource::KeepaliveGetState<ActorState> { cast = true },
        resource::GetRankStatus { cast = true },
        resource::WaitRankStatus { cast = true },
        RepublishIntrospect { cast = true },
        PySpyDump,
    ]
)]
pub struct ProcAgent {
    proc: Proc,
    remote: Remote,
    state: State,
    /// Actors created and tracked through the resource behavior.
    actor_states: HashMap<Name, ActorInstanceState>,
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
}

impl ProcAgent {
    #[hyperactor::observe_result("MeshAgent")]
    pub(crate) async fn bootstrap(
        proc_id: hyperactor_reference::ProcId,
    ) -> Result<(Proc, ActorHandle<Self>), anyhow::Error> {
        let sender = ReconfigurableMailboxSender::new();
        let proc = Proc::configured(proc_id.clone(), BoxedMailboxSender::new(sender.clone()));

        let agent = ProcAgent {
            proc: proc.clone(),
            remote: Remote::collect(),
            state: State::UnconfiguredV0 { sender },
            actor_states: HashMap::new(),
            record_supervision_events: false,
            introspect_dirty: false,
            shutdown_tx: None,
            stopping_all: false,
            // v0 procs don't have an owner they can check for, so they should
            // never try to kill the children.
            mesh_orphan_timeout: None,
        };
        let handle = proc.spawn::<Self>("mesh", agent)?;
        Ok((proc, handle))
    }

    pub(crate) fn boot_v1(
        proc: Proc,
        shutdown_tx: Option<tokio::sync::oneshot::Sender<i32>>,
    ) -> Result<ActorHandle<Self>, anyhow::Error> {
        // We can't use Option<Duration> directly in config attrs because AttrValue
        // is not implemented for Option<Duration>. So we use a zero timeout to
        // indicate no timeout.
        let orphan_timeout = hyperactor_config::global::get(MESH_ORPHAN_TIMEOUT);
        let orphan_timeout = if orphan_timeout.is_zero() {
            None
        } else {
            Some(orphan_timeout)
        };
        let agent = ProcAgent {
            proc: proc.clone(),
            remote: Remote::collect(),
            state: State::V1,
            actor_states: HashMap::new(),
            record_supervision_events: true,
            introspect_dirty: false,
            shutdown_tx,
            stopping_all: false,
            mesh_orphan_timeout: orphan_timeout,
        };
        proc.spawn::<Self>(PROC_AGENT_ACTOR_NAME, agent)
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
    fn stop_actor_by_id(&self, actor_id: &hyperactor_reference::ActorId, reason: &str) {
        tracing::info!(
            name = "StopActor",
            %actor_id,
            actor_name = actor_id.name(),
            %reason,
        );
        self.proc.stop_actor(actor_id, reason.to_string());
    }

    /// Publish the current proc properties and children list for
    /// introspection. See S12 in `introspect` module doc.
    fn publish_introspect_properties(&self, cx: &impl hyperactor::context::Actor) {
        let (mut children, mut system_children) = collect_live_children(&self.proc);

        // Terminated actors appear as children but don't inflate
        // the actor count. Track them in stopped_children so the
        // TUI can filter/gray without per-child fetches.
        let mut stopped_children: Vec<String> = Vec::new();
        for id in self.proc.all_terminated_actor_ids() {
            let ref_str = id.to_string();
            stopped_children.push(ref_str.clone());
            // Terminated system actors must also appear in
            // system_children for correct filtering.
            if let Some(snapshot) = self.proc.terminated_snapshot(&id) {
                let snapshot_attrs: hyperactor_config::Attrs =
                    serde_json::from_str(&snapshot.attrs).unwrap_or_default();
                if snapshot_attrs
                    .get(hyperactor::introspect::IS_SYSTEM)
                    .copied()
                    .unwrap_or(false)
                {
                    system_children.push(ref_str.clone());
                }
            }
            if !children.contains(&ref_str) {
                children.push(ref_str);
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
            self.proc.proc_id().to_string(),
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
        cx.instance().publish_attrs(attrs);
    }
}

#[async_trait]
impl Actor for ProcAgent {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        self.proc.set_supervision_coordinator(this.port())?;
        self.publish_introspect_properties(this);

        // Resolve terminated actor snapshots via QueryChild so that
        // dead actors remain directly queryable by reference.
        let proc = self.proc.clone();
        let self_id = this.self_id().clone();
        this.set_query_child_handler(move |child_ref| {
            use hyperactor::introspect::IntrospectResult;

            if let hyperactor::reference::Reference::Actor(id) = child_ref {
                if let Some(snapshot) = proc.terminated_snapshot(id) {
                    return snapshot;
                }
            }

            // PA-1 (ProcAgent path): proc-node children used by
            // admin/TUI must be computed from live proc state at query
            // time, not solely from cached published_properties.
            // Therefore a direct proc.spawn() actor must appear on the
            // next QueryChild(Reference::Proc) response without an
            // extra publish event. See
            // test_query_child_proc_returns_live_children.
            if let hyperactor::reference::Reference::Proc(proc_id) = child_ref {
                if proc_id == proc.proc_id() {
                    let (mut children, mut system_children) = collect_live_children(&proc);

                    let mut stopped_children: Vec<String> = Vec::new();
                    for id in proc.all_terminated_actor_ids() {
                        let ref_str = id.to_string();
                        stopped_children.push(ref_str.clone());
                        if let Some(snapshot) = proc.terminated_snapshot(&id) {
                            let snapshot_attrs: hyperactor_config::Attrs =
                                serde_json::from_str(&snapshot.attrs).unwrap_or_default();
                            if snapshot_attrs
                                .get(hyperactor::introspect::IS_SYSTEM)
                                .copied()
                                .unwrap_or(false)
                            {
                                system_children.push(ref_str.clone());
                            }
                        }
                        if !children.contains(&ref_str) {
                            children.push(ref_str);
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
                    attrs.set(crate::introspect::PROC_NAME, proc_id.to_string());
                    attrs.set(crate::introspect::NUM_ACTORS, num_live);
                    attrs.set(crate::introspect::SYSTEM_CHILDREN, system_children);
                    attrs.set(crate::introspect::STOPPED_CHILDREN, stopped_children);
                    attrs.set(
                        crate::introspect::STOPPED_RETENTION_CAP,
                        stopped_retention_cap,
                    );
                    attrs.set(crate::introspect::IS_POISONED, is_poisoned);
                    attrs.set(crate::introspect::FAILED_ACTOR_COUNT, failed_actor_count);
                    let attrs_json =
                        serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());

                    return IntrospectResult {
                        identity: proc_id.to_string(),
                        attrs: attrs_json,
                        children,
                        parent: None,
                        as_of: humantime::format_rfc3339_millis(std::time::SystemTime::now())
                            .to_string(),
                    };
                }
            }

            {
                let mut error_attrs = hyperactor_config::Attrs::new();
                error_attrs.set(hyperactor::introspect::ERROR_CODE, "not_found".to_string());
                error_attrs.set(
                    hyperactor::introspect::ERROR_MESSAGE,
                    format!("child {} not found", child_ref),
                );
                IntrospectResult {
                    identity: String::new(),
                    attrs: serde_json::to_string(&error_attrs).unwrap_or_else(|_| "{}".to_string()),
                    children: Vec::new(),
                    parent: None,
                    as_of: humantime::format_rfc3339_millis(std::time::SystemTime::now())
                        .to_string(),
                }
            }
        });

        if let Some(delay) = &self.mesh_orphan_timeout {
            this.self_message_with_delay(SelfCheck::default(), *delay)?;
        }
        Ok(())
    }

    async fn handle_undeliverable_message(
        &mut self,
        cx: &Instance<Self>,
        envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        if let Some(true) = envelope.0.headers().get(STREAM_STATE_SUBSCRIBER) {
            let dest_port_id = envelope.0.dest().clone();
            let port =
                hyperactor_reference::PortRef::<resource::State<ActorState>>::attest(dest_port_id);
            // Remove this subscriber from whichever actor instance holds it.
            for instance in self.actor_states.values_mut() {
                instance.subscribers.retain(|s| s != &port);
            }
            Ok(())
        } else {
            handle_undeliverable_message(cx, envelope)
        }
    }
}

#[async_trait]
#[hyperactor::handle(MeshAgentMessage)]
impl MeshAgentMessageHandler for ProcAgent {
    async fn configure(
        &mut self,
        cx: &Context<Self>,
        rank: usize,
        forwarder: ChannelAddr,
        supervisor: Option<hyperactor_reference::PortRef<ActorSupervisionEvent>>,
        address_book: HashMap<hyperactor_reference::ProcId, ChannelAddr>,
        configured: hyperactor_reference::PortRef<usize>,
        record_supervision_events: bool,
    ) -> Result<(), anyhow::Error> {
        anyhow::ensure!(
            self.state.is_unconfigured_v0(),
            "mesh agent cannot be (re-)configured"
        );
        self.record_supervision_events = record_supervision_events;

        let client = MailboxClient::new(channel::dial(forwarder)?);
        let router =
            DialMailboxRouter::new_with_default_direct_addressed_remote_only(client.into_boxed());

        for (proc_id, addr) in address_book {
            router.bind(proc_id.into(), addr);
        }

        let sender = take(&mut self.state).into_unconfigured_v0().unwrap();
        assert!(sender.configure(router.into_boxed()));

        // This is a bit suboptimal: ideally we'd set the supervisor first, to correctly report
        // any errors that occur during configuration. However, these should anyway be correctly
        // caught on process exit.
        self.state = State::ConfiguredV0 {
            sender,
            rank,
            supervisor,
        };
        configured.send(cx, rank)?;

        Ok(())
    }

    async fn gspawn(
        &mut self,
        cx: &Context<Self>,
        actor_type: String,
        actor_name: String,
        params_data: Data,
        status_port: hyperactor_reference::PortRef<GspawnResult>,
    ) -> Result<(), anyhow::Error> {
        anyhow::ensure!(
            self.state.is_configured_v0(),
            "mesh agent is not v0 configured"
        );
        let actor_id = match self
            .remote
            .gspawn(
                &self.proc,
                &actor_type,
                &actor_name,
                params_data,
                cx.headers().clone(),
            )
            .await
        {
            Ok(id) => id,
            Err(err) => {
                status_port.send(cx, GspawnResult::Error(format!("gspawn failed: {}", err)))?;
                return Err(anyhow::anyhow!("gspawn failed"));
            }
        };
        status_port.send(
            cx,
            GspawnResult::Success {
                rank: self.state.rank().unwrap(),
                actor_id,
            },
        )?;
        self.publish_introspect_properties(cx);
        Ok(())
    }

    async fn status(
        &mut self,
        cx: &Context<Self>,
        status_port: hyperactor_reference::PortRef<(usize, bool)>,
    ) -> Result<(), anyhow::Error> {
        match &self.state {
            State::ConfiguredV0 { rank, .. } => {
                // v0 path: configured with a concrete rank
                status_port.send(cx, (*rank, true))?;
                Ok(())
            }
            State::UnconfiguredV0 { .. } => {
                // v0 path but not configured yet
                Err(anyhow::anyhow!(
                    "status unavailable: v0 agent not configured (waiting for Configure)"
                ))
            }
            State::V1 => {
                // v1/owned path does not support status (no rank semantics)
                Err(anyhow::anyhow!(
                    "status unsupported in v1/owned path (no rank)"
                ))
            }
            State::Invalid => Err(anyhow::anyhow!(
                "status unavailable: agent in invalid state"
            )),
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
                    proc_id = %self.proc.proc_id(),
                    %event,
                    "recording supervision error",
                );
            } else {
                tracing::debug!(
                    name = "SupervisionEvent",
                    proc_id = %self.proc.proc_id(),
                    %event,
                    "recording non-error supervision event",
                );
            }
            // Record the event in the actor's instance state and notify subscribers.
            if let Some((name, instance)) = self
                .actor_states
                .iter_mut()
                .find(|(_, s)| s.spawn.as_ref().ok() == Some(&event.actor_id))
            {
                instance.supervision_event = Some(event.clone());
                instance.generation += 1;
                let name = name.clone();
                instance.notify_status_changed(cx, &name);
            }
            // Defer republish so introspection picks up is_poisoned /
            // failed_actor_count without blocking the message loop.
            // Multiple rapid events coalesce into one republish.
            if !self.introspect_dirty {
                self.introspect_dirty = true;
                let _ = cx.self_message_with_delay(
                    RepublishIntrospect,
                    std::time::Duration::from_millis(100),
                );
            }

            // If StopAll was requested, check whether all actors have now
            // reached terminal state. If so, shut down the process.
            if self.stopping_all && self.all_actors_terminal() {
                self.shutdown().await;
            }
        }
        if let Some(supervisor) = self.state.supervisor() {
            supervisor.send(cx, event)?;
        } else if !self.record_supervision_events && event.is_error() {
            // If there is no supervisor, and nothing is recording these, crash
            // the whole process on error events.
            tracing::error!(
                name = "supervision_event_transmit_failed",
                proc_id = %cx.self_id().proc_id(),
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
    async fn handle(&mut self, cx: &Context<Self>, _: RepublishIntrospect) -> anyhow::Result<()> {
        if self.introspect_dirty {
            self.introspect_dirty = false;
            self.publish_introspect_properties(cx);
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
        let opts = PySpyOpts {
            threads: message.threads,
            native: message.native,
            native_all: message.native_all,
            nonblocking: message.nonblocking,
        };
        PySpyWorker::spawn_and_forward(cx, opts, message.result)
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
}
wirevalue::register_type!(ActorSpec);

/// Actor state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct ActorState {
    /// The actor's ID.
    pub actor_id: hyperactor_reference::ActorId,
    /// The rank of the proc that created the actor. This is before any slicing.
    pub create_rank: usize,
    // TODO status: ActorStatus,
    pub supervision_events: Vec<ActorSupervisionEvent>,
}
wirevalue::register_type!(ActorState);

#[async_trait]
impl Handler<resource::CreateOrUpdate<ActorSpec>> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<ActorSpec>,
    ) -> anyhow::Result<()> {
        if self.actor_states.contains_key(&create_or_update.name) {
            // There is no update.
            return Ok(());
        }
        let create_rank = create_or_update.rank.unwrap();
        // If any actor on this proc has error supervision events,
        // we disallow spawning new actors on it, as this proc may be in an
        // invalid state.
        if self.actor_states.values().any(|s| s.has_errors()) {
            self.actor_states.insert(
                create_or_update.name.clone(),
                ActorInstanceState {
                    spawn: Err(anyhow::anyhow!(
                        "Cannot spawn new actors on mesh with supervision events"
                    )),
                    create_rank,
                    stop_initiated: false,
                    supervision_event: None,
                    subscribers: Vec::new(),
                    expiry_time: None,
                    generation: 1,
                    pending_wait_status: Vec::new(),
                },
            );
            return Ok(());
        }

        let ActorSpec {
            actor_type,
            params_data,
        } = create_or_update.spec;
        self.actor_states.insert(
            create_or_update.name.clone(),
            ActorInstanceState {
                create_rank,
                spawn: self
                    .remote
                    .gspawn(
                        &self.proc,
                        &actor_type,
                        &create_or_update.name.to_string(),
                        params_data,
                        cx.headers().clone(),
                    )
                    .await,
                stop_initiated: false,
                supervision_event: None,
                subscribers: Vec::new(),
                expiry_time: None,
                generation: 1,
                pending_wait_status: Vec::new(),
            },
        );

        self.publish_introspect_properties(cx);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::Stop> for ProcAgent {
    async fn handle(&mut self, cx: &Context<Self>, message: resource::Stop) -> anyhow::Result<()> {
        let actor_id = match self.actor_states.get_mut(&message.name) {
            Some(actor_state) => {
                let id = actor_state.spawn.as_ref().ok().cloned();
                if id.is_some() && !actor_state.stop_initiated {
                    actor_state.stop_initiated = true;
                    actor_state.generation += 1;
                    actor_state.notify_status_changed(cx, &message.name);
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
        let to_stop: Vec<hyperactor_reference::ActorId> = self
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
        use crate::resource::Status;

        let (rank, status) = match self.actor_states.get(&get_rank_status.name) {
            Some(state) => (state.create_rank, state.status()),
            None => (usize::MAX, Status::NotExist),
        };

        // Send a sparse overlay update. If rank is unknown, emit an
        // empty overlay.
        let overlay = if rank == usize::MAX {
            StatusOverlay::new()
        } else {
            StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                .expect("valid single-run overlay")
        };
        let result = get_rank_status.reply.send(cx, overlay);
        // Ignore errors, because returning Err from here would cause the ProcAgent
        // to be stopped, which would prevent querying and spawning other actors.
        // This only means some actor that requested the state of an actor failed to receive it.
        if let Err(e) = result {
            tracing::warn!(
                actor = %cx.self_id(),
                "failed to send GetRankStatus reply to {} due to error: {}",
                get_rank_status.reply.port_id().actor_id(),
                e
            );
        }
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
        use crate::resource::Status;

        let (rank, status) = match self.actor_states.get(&msg.name) {
            Some(state) => (state.create_rank, state.status()),
            None => (usize::MAX, Status::NotExist),
        };

        // If already at or past the requested threshold, reply immediately.
        if status >= msg.min_status || rank == usize::MAX {
            let overlay = if rank == usize::MAX {
                StatusOverlay::new()
            } else {
                StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                    .expect("valid single-run overlay")
            };
            let _ = msg.reply.send(cx, overlay);
            return Ok(());
        }

        // Otherwise, stash the waiter. It will be flushed when the
        // status changes (supervision event or stop).
        if let Some(state) = self.actor_states.get_mut(&msg.name) {
            state.pending_wait_status.push((msg.min_status, msg.reply));
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
        let state = match self.actor_states.get(&get_state.name) {
            Some(instance) => instance.to_state(&get_state.name),
            None => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::NotExist,
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        };

        let result = get_state.reply.send(cx, state);
        if let Err(e) = result {
            tracing::warn!(
                actor = %cx.self_id(),
                "failed to send GetState reply to {} due to error: {}",
                get_state.reply.port_id().actor_id(),
                e
            );
        }
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
        let state = match self.actor_states.get_mut(&stream_state.name) {
            Some(instance) => {
                let state = instance.to_state(&stream_state.name);
                instance.subscribers.push(stream_state.subscriber.clone());
                state
            }
            None => resource::State {
                name: stream_state.name.clone(),
                status: resource::Status::NotExist,
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        };

        // Send the current state immediately.
        let mut headers = Flattrs::new();
        headers.set(STREAM_STATE_SUBSCRIBER, true);
        if let Err(e) = stream_state
            .subscriber
            .send_with_headers(cx, headers, state)
        {
            tracing::warn!(
                actor = %cx.self_id(),
                "failed to send initial StreamState to {}: {}",
                stream_state.subscriber.port_id().actor_id(),
                e,
            );
        }
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
        if let Ok(instance_state) = self
            .actor_states
            .get_mut(&message.get_state.name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "attempting to register a keepalive for an actor that doesn't exist: {}",
                    message.get_state.name
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
    pub client_instance: PortHandle<Instance<()>>,
}

#[async_trait]
impl Handler<NewClientInstance> for ProcAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        NewClientInstance { client_instance }: NewClientInstance,
    ) -> anyhow::Result<()> {
        let (instance, _handle) = self.proc.instance("client")?;
        client_instance.send(cx, instance)?;
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
        proc.send(cx, self.proc.clone())?;
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
        let duration = duration.clone();
        let now = std::time::SystemTime::now();

        // Collect expired actors before mutating, since stop_actor borrows &mut self.
        let expired: Vec<(Name, hyperactor_reference::ActorId)> = self
            .actor_states
            .iter()
            .filter_map(|(name, state)| {
                let expiry = state.expiry_time?;
                // If a stop was already initiated we don't need to do it again.
                if now > expiry && !state.stop_initiated {
                    if let Ok(actor_id) = &state.spawn {
                        return Some((name.clone(), actor_id.clone()));
                    }
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

        for (name, actor_id) in expired {
            if let Some(state) = self.actor_states.get_mut(&name) {
                state.stop_initiated = true;
            }
            self.stop_actor_by_id(&actor_id, "orphaned");
        }

        // Reschedule.
        cx.self_message_with_delay(SelfCheck::default(), duration)?;
        Ok(())
    }
}

/// A mailbox sender that initially queues messages, and then relays them to
/// an underlying sender once configured.
#[derive(Clone)]
pub(crate) struct ReconfigurableMailboxSender {
    state: Arc<RwLock<ReconfigurableMailboxSenderState>>,
}

impl std::fmt::Debug for ReconfigurableMailboxSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Not super helpful, but we definitely don't wan to acquire any locks
        // in a Debug formatter.
        f.debug_struct("ReconfigurableMailboxSender").finish()
    }
}

/// A capability wrapper granting access to the configured mailbox
/// sender.
///
/// This type exists to tie the lifetime of any `&BoxedMailboxSender`
/// reference to a lock guard, so the underlying state cannot be
/// reconfigured while the reference is in use.
///
/// A **read** guard is sufficient because we only need to *observe*
/// and borrow the configured sender, not mutate state. While a
/// `RwLockReadGuard` is held, `configure()` cannot acquire the write
/// lock, so the state cannot transition from `Configured(..)` to any
/// other variant during the guard’s lifetime.
pub(crate) struct ReconfigurableMailboxSenderInner<'a> {
    guard: RwLockReadGuard<'a, ReconfigurableMailboxSenderState>,
}

impl<'a> ReconfigurableMailboxSenderInner<'a> {
    pub(crate) fn as_configured(&self) -> Option<&BoxedMailboxSender> {
        self.guard.as_configured()
    }
}

type Post = (MessageEnvelope, PortHandle<Undeliverable<MessageEnvelope>>);

#[derive(EnumAsInner, Debug)]
enum ReconfigurableMailboxSenderState {
    Queueing(Mutex<Vec<Post>>),
    Configured(BoxedMailboxSender),
}

impl ReconfigurableMailboxSender {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ReconfigurableMailboxSenderState::Queueing(
                Mutex::new(Vec::new()),
            ))),
        }
    }

    /// Configure this mailbox with the provided sender. This will first
    /// enqueue any pending messages onto the sender; future messages are
    /// posted directly to the configured sender.
    pub(crate) fn configure(&self, sender: BoxedMailboxSender) -> bool {
        // Hold the write lock until all queued messages are flushed.
        let mut state = self.state.write().unwrap();
        if state.is_configured() {
            return false;
        }

        // Install the configured sender exactly once.
        let queued = std::mem::replace(
            &mut *state,
            ReconfigurableMailboxSenderState::Configured(sender),
        );

        // Borrow the configured sender from the state (stable while
        // we hold the lock).
        let configured_sender = state.as_configured().expect("just configured");

        // Flush the old queue while still holding the write lock.
        for (envelope, return_handle) in queued.into_queueing().unwrap().into_inner().unwrap() {
            configured_sender.post(envelope, return_handle);
        }

        true
    }

    pub(crate) fn as_inner<'a>(
        &'a self,
    ) -> Result<ReconfigurableMailboxSenderInner<'a>, anyhow::Error> {
        let state = self.state.read().unwrap();
        if state.is_configured() {
            Ok(ReconfigurableMailboxSenderInner { guard: state })
        } else {
            Err(anyhow::anyhow!("cannot get inner sender: not configured"))
        }
    }
}

#[async_trait]
impl MailboxSender for ReconfigurableMailboxSender {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match &*self.state.read().unwrap() {
            ReconfigurableMailboxSenderState::Queueing(queue) => {
                queue.lock().unwrap().push((envelope, return_handle));
            }
            ReconfigurableMailboxSenderState::Configured(sender) => {
                sender.post(envelope, return_handle);
            }
        }
    }

    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match &*self.state.read().unwrap() {
            ReconfigurableMailboxSenderState::Queueing(queue) => {
                queue.lock().unwrap().push((envelope, return_handle));
            }
            ReconfigurableMailboxSenderState::Configured(sender) => {
                sender.post_unchecked(envelope, return_handle);
            }
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        let sender = match &*self.state.read().unwrap() {
            ReconfigurableMailboxSenderState::Queueing(_) => return Ok(()),
            ReconfigurableMailboxSenderState::Configured(sender) => sender.clone(),
        };
        sender.flush().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use hyperactor::mailbox::BoxedMailboxSender;
    use hyperactor::mailbox::Mailbox;
    use hyperactor::mailbox::MailboxSender;
    use hyperactor::mailbox::MessageEnvelope;
    use hyperactor::mailbox::PortHandle;
    use hyperactor::mailbox::Undeliverable;
    use hyperactor::testing::ids::test_actor_id;
    use hyperactor::testing::ids::test_port_id;
    use hyperactor_config::Flattrs;

    use super::*;

    #[derive(Debug, Clone)]
    struct QueueingMailboxSender {
        messages: Arc<Mutex<Vec<MessageEnvelope>>>,
    }

    impl QueueingMailboxSender {
        fn new() -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_messages(&self) -> Vec<MessageEnvelope> {
            self.messages.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl MailboxSender for QueueingMailboxSender {
        fn post_unchecked(
            &self,
            envelope: MessageEnvelope,
            _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        ) {
            self.messages.lock().unwrap().push(envelope);
        }
    }

    // Helper function to create a test message envelope
    fn envelope(data: u64) -> MessageEnvelope {
        MessageEnvelope::serialize(
            test_actor_id("world_0", "sender"),
            test_port_id("world_0", "receiver", 1),
            &data,
            Flattrs::new(),
        )
        .unwrap()
    }

    fn return_handle() -> PortHandle<Undeliverable<MessageEnvelope>> {
        let mbox = Mailbox::new_detached(test_actor_id("0", "test"));
        let (port, _receiver) = mbox.open_port::<Undeliverable<MessageEnvelope>>();
        port
    }

    #[test]
    fn test_queueing_before_configure() {
        let sender = ReconfigurableMailboxSender::new();

        let test_sender = QueueingMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(test_sender.clone());

        let return_handle = return_handle();
        sender.post(envelope(1), return_handle.clone());
        sender.post(envelope(2), return_handle.clone());

        assert_eq!(test_sender.get_messages().len(), 0);

        sender.configure(boxed_sender);

        let messages = test_sender.get_messages();
        assert_eq!(messages.len(), 2);

        assert_eq!(messages[0].deserialized::<u64>().unwrap(), 1);
        assert_eq!(messages[1].deserialized::<u64>().unwrap(), 2);
    }

    #[test]
    fn test_direct_delivery_after_configure() {
        // Create a ReconfigurableMailboxSender
        let sender = ReconfigurableMailboxSender::new();

        let test_sender = QueueingMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(test_sender.clone());
        sender.configure(boxed_sender);

        let return_handle = return_handle();
        sender.post(envelope(3), return_handle.clone());
        sender.post(envelope(4), return_handle.clone());

        let messages = test_sender.get_messages();
        assert_eq!(messages.len(), 2);

        assert_eq!(messages[0].deserialized::<u64>().unwrap(), 3);
        assert_eq!(messages[1].deserialized::<u64>().unwrap(), 4);
    }

    #[test]
    fn test_multiple_configurations() {
        let sender = ReconfigurableMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(QueueingMailboxSender::new());

        assert!(sender.configure(boxed_sender.clone()));
        assert!(!sender.configure(boxed_sender));
    }

    #[test]
    fn test_mixed_queueing_and_direct_delivery() {
        let sender = ReconfigurableMailboxSender::new();

        let test_sender = QueueingMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(test_sender.clone());

        let return_handle = return_handle();
        sender.post(envelope(5), return_handle.clone());
        sender.post(envelope(6), return_handle.clone());

        sender.configure(boxed_sender);

        sender.post(envelope(7), return_handle.clone());
        sender.post(envelope(8), return_handle.clone());

        let messages = test_sender.get_messages();
        assert_eq!(messages.len(), 4);

        assert_eq!(messages[0].deserialized::<u64>().unwrap(), 5);
        assert_eq!(messages[1].deserialized::<u64>().unwrap(), 6);
        assert_eq!(messages[2].deserialized::<u64>().unwrap(), 7);
        assert_eq!(messages[3].deserialized::<u64>().unwrap(), 8);
    }

    // A no-op actor used to test direct proc-level spawning.
    #[derive(Debug, Default, Serialize, Deserialize)]
    #[hyperactor::export(handlers = [])]
    struct ExtraActor;
    impl hyperactor::Actor for ExtraActor {}
    hyperactor::remote!(ExtraActor);
    // Verifies that QueryChild(Reference::Proc) on a ProcAgent returns
    // a live IntrospectResult whose children reflect actors spawned
    // directly on the proc — i.e. via proc.spawn(), which bypasses the
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
        use hyperactor::reference as hyperactor_reference;

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
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let agent_id = proc.proc_id().actor_id(PROC_AGENT_ACTOR_NAME, 0);
        let port =
            hyperactor_reference::PortRef::<IntrospectMessage>::attest_message_port(&agent_id);

        // Helper: send QueryChild(Proc) and return the payload with a
        // timeout so a misrouted reply fails fast rather than hanging.
        let query = |client: &hyperactor::Instance<()>| {
            let (reply_port, reply_rx) = client.open_once_port::<IntrospectResult>();
            port.send(
                client,
                IntrospectMessage::QueryChild {
                    child_ref: hyperactor_reference::Reference::Proc(proc.proc_id().clone()),
                    reply: reply_port.bind(),
                },
            )
            .unwrap();
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
                .any(|c| c.contains(PROC_AGENT_ACTOR_NAME)),
            "initial children {:?} should contain proc_agent",
            payload.children
        );
        let initial_count = payload.children.len();

        // Spawn an actor directly on the proc, bypassing ProcAgent's
        // gspawn message handler. This is how supervision-spawned
        // actors (e.g. sieve children) are created.
        proc.spawn("extra_actor", ExtraActor).unwrap();

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
            payload2.children.iter().any(|c| c.contains("extra_actor")),
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
    // actors while concurrently querying QueryChild(Reference::Proc).
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
        use hyperactor::reference as hyperactor_reference;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "test_proc".to_string()).unwrap();
        let agent_handle = ProcAgent::boot_v1(proc.clone(), None).unwrap();

        agent_handle
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let agent_id = proc.proc_id().actor_id(PROC_AGENT_ACTOR_NAME, 0);
        let port =
            hyperactor_reference::PortRef::<IntrospectMessage>::attest_message_port(&agent_id);

        // Concurrent query task: send QueryChild(Proc) every 10ms.
        let query_client_proc =
            Proc::direct(ChannelTransport::Unix.any(), "query_client".to_string()).unwrap();
        let (query_client, _qc_handle) = query_client_proc.instance("qc").unwrap();
        let query_port = port.clone();
        let query_proc_id = proc.proc_id().clone();
        let query_count = Arc::new(AtomicUsize::new(0));
        let query_count_clone = query_count.clone();
        let query_task = tokio::spawn(async move {
            loop {
                let (reply_port, reply_rx) = query_client.open_once_port::<IntrospectResult>();
                if query_port
                    .send(
                        &query_client,
                        IntrospectMessage::QueryChild {
                            child_ref: hyperactor_reference::Reference::Proc(query_proc_id.clone()),
                            reply: reply_port.bind(),
                        },
                    )
                    .is_err()
                {
                    break;
                }
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
                let handle = proc.spawn(&name, ExtraActor).unwrap();
                let actor_id = handle.actor_id().clone();
                if let Some(mut status) = proc.stop_actor(&actor_id, "churn".to_string()) {
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
        port.send(
            &client,
            IntrospectMessage::QueryChild {
                child_ref: hyperactor_reference::Reference::Proc(proc.proc_id().clone()),
                reply: reply_port.bind(),
            },
        )
        .unwrap();
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

        let (client, _client_handle) = proc.instance("client").unwrap();
        let agent_ref: hyperactor_reference::ActorRef<ProcAgent> = agent_handle.bind();

        let actor_type = hyperactor::actor::remote::Remote::collect()
            .name_of::<ExtraActor>()
            .unwrap()
            .to_string();
        let actor_params = bincode::serialize(&ExtraActor).unwrap();
        let actor_name = Name::Reserved("test_actor".to_string());

        // 1. Spawn an actor via CreateOrUpdate.
        agent_ref
            .create_or_update(
                &client,
                actor_name.clone(),
                resource::Rank::new(0),
                ActorSpec {
                    actor_type: actor_type.clone(),
                    params_data: actor_params.clone(),
                },
            )
            .await
            .unwrap();

        // 2. Subscribe to state updates.
        let (sub_port, mut sub_rx) = client.open_port::<resource::State<ActorState>>();
        agent_ref
            .stream_state(&client, actor_name.clone(), sub_port.bind())
            .await
            .unwrap();

        // 3. Should receive the initial state (Running).
        let initial = sub_rx.recv().await.expect("subscriber channel error");
        assert_eq!(initial.status, resource::Status::Running);
        assert!(initial.state.is_some());

        // 4. Send Stop — should receive Stopping.
        agent_ref
            .stop(&client, actor_name.clone(), "test".to_string())
            .await
            .unwrap();

        let stopping = sub_rx.recv().await.expect("subscriber channel error");
        assert_eq!(stopping.status, resource::Status::Stopping);

        // 5. Wait for the Stopped supervision event update.
        let stopped = sub_rx.recv().await.expect("subscriber channel error");
        assert_eq!(stopped.status, resource::Status::Stopped);

        // 6. Test implicit unsubscription via undeliverable.
        let actor_name_2 = Name::Reserved("test_actor_2".to_string());
        agent_ref
            .create_or_update(
                &client,
                actor_name_2.clone(),
                resource::Rank::new(1),
                ActorSpec {
                    actor_type: actor_type.clone(),
                    params_data: actor_params.clone(),
                },
            )
            .await
            .unwrap();

        let (sub_port_2, mut sub_rx_2) = client.open_port::<resource::State<ActorState>>();
        agent_ref
            .stream_state(&client, actor_name_2.clone(), sub_port_2.bind())
            .await
            .unwrap();

        let initial_2 = sub_rx_2.recv().await.expect("subscriber 2 channel error");
        assert_eq!(initial_2.status, resource::Status::Running);

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
        let (sub_port_3, mut sub_rx_3) = client.open_port::<resource::State<ActorState>>();
        agent_ref
            .stream_state(&client, actor_name_2.clone(), sub_port_3.bind())
            .await
            .unwrap();
        loop {
            let state = sub_rx_3.recv().await.expect("subscriber 3 channel error");
            if state.status.is_terminating() {
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
}
