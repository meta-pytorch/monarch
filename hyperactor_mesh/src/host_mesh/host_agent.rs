/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The mesh agent actor that manages a host.

// EnumAsInner generates code that triggers a false positive
// unused_assignments lint on struct variant fields. #[allow] on the
// enum itself doesn't propagate into derive-macro-generated code, so
// the suppression must be at module scope.
#![allow(unused_assignments)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::pin::Pin;
use std::sync::OnceLock;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use futures::future::Either;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Addr;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortHandle;
use hyperactor::PortRef;
use hyperactor::Proc;
use hyperactor::ProcAddr;
use hyperactor::RefClient;
use hyperactor::RemoteEndpoint as _;
use hyperactor::Uid;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::ActorStoppingReason;
use hyperactor::context;
use hyperactor::id::Label;
use hyperactor::mailbox::MailboxSender as _;
use hyperactor::value_mesh::ValueOverlay;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::Attrs;
use ndslice::view::Region;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use typeuri::Named;

use crate::StatusOverlay;
use crate::bootstrap;
use crate::bootstrap::BootstrapCommand;
use crate::bootstrap::BootstrapProcConfig;
use crate::bootstrap::BootstrapProcManager;
use crate::bootstrap::ProcBind;
use crate::config_dump::ConfigDump;
use crate::config_dump::ConfigDumpResult;
use crate::host::Host;
use crate::host::HostError;
use crate::host::HostShutdownHandles;
use crate::host::LOCAL_PROC_NAME;
use crate::host::LocalProcManager;
use crate::host::SERVICE_PROC_NAME;
use crate::host::SingleTerminate;
use crate::mesh_id::HostMeshId;
use crate::mesh_id::ProcMeshId;
use crate::mesh_id::ResourceId;
use crate::proc_agent::ProcAgent;
use crate::pyspy::PySpyDump;
use crate::pyspy::PySpyProfile;
use crate::pyspy::PySpyProfileWorker;
use crate::pyspy::PySpyWorker;
use crate::resource;
use crate::resource::ProcSpec;
use crate::resource::Status;

pub(crate) type ProcManagerSpawnFuture =
    Pin<Box<dyn Future<Output = anyhow::Result<ActorHandle<ProcAgent>>> + Send>>;
pub(crate) type ProcManagerSpawnFn = Box<dyn Fn(Proc) -> ProcManagerSpawnFuture + Send + Sync>;

/// Represents the different ways a [`Host`] can be managed by an agent.
///
/// A host can either:
/// - [`Process`] — a host running as an external OS process, managed by
///   [`BootstrapProcManager`].
/// - [`Local`] — a host running in-process, managed by
///   [`LocalProcManager`] with a custom spawn function.
///
/// This abstraction lets the same `HostAgent` work across both
/// out-of-process and in-process execution modes.
#[derive(EnumAsInner)]
pub(crate) enum HostAgentMode {
    Process {
        host: Host<BootstrapProcManager>,
        /// If set, the ShutdownHost handler sends the active transport handles
        /// back to the bootstrap loop once shutdown is complete, so the caller
        /// can drain them and exit.
        shutdown_tx: Option<tokio::sync::oneshot::Sender<HostShutdownHandles>>,
    },
    Local(Host<LocalProcManager<ProcManagerSpawnFn>>),
}

impl HostAgentMode {
    pub(crate) fn addr(&self) -> &hyperactor::channel::ChannelAddr {
        #[allow(clippy::match_same_arms)]
        match self {
            HostAgentMode::Process { host, .. } => host.addr(),
            HostAgentMode::Local(host) => host.addr(),
        }
    }

    pub(crate) fn system_proc(&self) -> &Proc {
        #[allow(clippy::match_same_arms)]
        match self {
            HostAgentMode::Process { host, .. } => host.system_proc(),
            HostAgentMode::Local(host) => host.system_proc(),
        }
    }

    pub(crate) fn local_proc(&self) -> &Proc {
        #[allow(clippy::match_same_arms)]
        match self {
            HostAgentMode::Process { host, .. } => host.local_proc(),
            HostAgentMode::Local(host) => host.local_proc(),
        }
    }

    /// Non-blocking stop: send the stop signal and spawn a background
    /// task for cleanup. Returns immediately without blocking the
    /// actor.
    async fn request_stop(
        &self,
        cx: &impl context::Actor,
        proc: &ProcAddr,
        timeout: Duration,
        reason: &str,
    ) {
        match self {
            HostAgentMode::Process { host, .. } => {
                host.manager().request_stop(cx, proc, timeout, reason).await;
            }
            HostAgentMode::Local(host) => {
                host.manager().request_stop(proc, timeout, reason).await;
            }
        }
    }

    /// Query a proc's lifecycle state, returning both the coarse
    /// `resource::Status` used by the resource protocol and the
    /// detailed `bootstrap::ProcStatus` (when available) for callers
    /// that need process-level detail such as PIDs or exit codes.
    async fn proc_status(
        &self,
        proc_id: &ProcAddr,
    ) -> (resource::Status, Option<bootstrap::ProcStatus>) {
        match self {
            HostAgentMode::Process { host, .. } => match host.manager().status(proc_id).await {
                Some(proc_status) => (proc_status.clone().into(), Some(proc_status)),
                None => (resource::Status::Unknown, None),
            },
            HostAgentMode::Local(host) => {
                let status = match host.manager().local_proc_status(proc_id).await {
                    Some(crate::host::LocalProcStatus::Stopping) => resource::Status::Stopping,
                    Some(crate::host::LocalProcStatus::Stopped) => resource::Status::Stopped,
                    None => resource::Status::Running,
                };
                (status, None)
            }
        }
    }

    /// The bootstrap command used by the process manager, if any.
    fn bootstrap_command(&self) -> Option<BootstrapCommand> {
        match self {
            HostAgentMode::Process { host, .. } => Some(host.manager().command().clone()),
            HostAgentMode::Local(_) => None,
        }
    }
}

/// Derive the proc resource name for the proc at the given mesh `rank`.
///
/// `rank` is the proc's absolute rank in the proc mesh — its position in the
/// `host_extent ⊕ per_host` region, i.e. [`crate::proc_mesh::ProcRef::create_rank`].
/// This is the proc's first-class, mesh-level identity; the `(host, per-host
/// slot)` layout is an implementation detail of how procs are placed and is
/// deliberately NOT part of the name. Both the caller and the receiving
/// [`HostAgent`] derive the same name from `rank`, so the caller can construct
/// [`crate::proc_mesh::ProcRef`]s before the casted `CreateOrUpdate<ProcSpec>`
/// messages arrive. The id is a stable function of the proc mesh id and rank,
/// so every proc in the mesh has a distinct id.
pub(crate) fn proc_name(proc_mesh_id: &ProcMeshId, rank: usize) -> ResourceId {
    let label = Label::strip(&format!(
        "{}-{}",
        proc_mesh_id
            .display_label()
            .map(|label| label.as_str())
            .unwrap_or("unnamed"),
        rank
    ));

    match proc_mesh_id.uid() {
        Uid::Singleton(_) => ResourceId::singleton(label),
        Uid::Instance(_, _) => {
            let mut hasher = DefaultHasher::new();
            proc_mesh_id.hash(&mut hasher);
            rank.hash(&mut hasher);
            ResourceId::new(
                Uid::Instance(hasher.finish(), Some(label.clone())),
                Some(label),
            )
        }
    }
}

pub(crate) fn proc_slots(
    proc_mesh_id: &ProcMeshId,
    host_rank: usize,
    num_per_host: usize,
) -> impl Iterator<Item = (usize, usize, ResourceId)> + '_ {
    (0..num_per_host).map(move |per_host_rank| {
        let rank = num_per_host * host_rank + per_host_rank;
        (per_host_rank, rank, proc_name(proc_mesh_id, rank))
    })
}

#[derive(Debug)]
pub(crate) struct ProcMetadata {
    rank: usize,
    host_mesh_id: Option<HostMeshId>,
    /// The proc mesh this proc belongs to. Used to scope per-mesh queries like
    /// `StreamState`, since a host agent can hold procs from multiple meshes.
    /// Always set for procs spawned through a proc mesh (the cast `SpawnProcs`
    /// path populates it via `ProcSpec`). `None` only for procs created off that
    /// path (e.g. the point-to-point `CreateOrUpdate` used by tests/admin),
    /// which belong to no queryable mesh and are intentionally excluded from
    /// per-mesh queries.
    proc_mesh_id: Option<ProcMeshId>,
}

mod completion_action {
    use super::Duration;

    /// Deferred action for a proc that is still being created.
    ///
    /// Actions only strengthen from keep to stop, drain, or shutdown. Each
    /// pending proc counted by a drain or shutdown must complete its accounting
    /// exactly once, and the lifecycle reply must wait for that accounting.
    #[derive(Debug)]
    pub(crate) struct CompletionAction(State);

    #[derive(Debug)]
    #[expect(
        dead_code,
        reason = "pending proc creation is activated by a later change"
    )]
    enum State {
        Keep,
        Stop { timeout: Duration, reason: String },
        Drain,
        Shutdown,
    }

    impl CompletionAction {
        pub(super) fn is_keep(&self) -> bool {
            matches!(&self.0, State::Keep)
        }

        pub(super) fn request_stop(&mut self, timeout: Duration, reason: String) {
            if self.is_keep() {
                self.0 = State::Stop { timeout, reason };
            }
        }

        /// Returns whether the requesting drain must account for this proc.
        /// An existing drain action can be shared by another drain, but a
        /// shutdown already owns the proc's completion.
        pub(super) fn request_drain(&mut self) -> bool {
            if matches!(&self.0, State::Shutdown) {
                return false;
            }

            self.0 = State::Drain;
            true
        }

        pub(super) fn request_shutdown(&mut self) {
            self.0 = State::Shutdown;
        }
    }

    impl super::ProcCreationState {
        pub(super) fn pending(
            metadata: super::ProcMetadata,
            expiry_time: Option<std::time::SystemTime>,
        ) -> Self {
            Self::Pending {
                metadata,
                on_completion: CompletionAction(State::Keep),
                expiry_time,
            }
        }
    }
}

pub(crate) use completion_action::CompletionAction;

#[derive(Debug)]
pub(crate) enum ProcCreationState {
    Pending {
        metadata: ProcMetadata,
        on_completion: CompletionAction,
        expiry_time: Option<std::time::SystemTime>,
    },
    Created {
        metadata: ProcMetadata,
        proc_id: ProcAddr,
        mesh_agent: ActorRef<ProcAgent>,
        /// "Owner is alive" deadline communicated by the controller via
        /// `KeepaliveGetState`. The host's `SelfCheck` reaper compares against this
        /// and tears down procs whose owner has stopped extending the keepalive.
        expiry_time: Option<std::time::SystemTime>,
    },
    Failed {
        metadata: ProcMetadata,
        error: HostError,
    },
}

impl ProcCreationState {
    fn metadata(&self) -> &ProcMetadata {
        match self {
            Self::Pending { metadata, .. }
            | Self::Created { metadata, .. }
            | Self::Failed { metadata, .. } => metadata,
        }
    }

    fn from_spawn_result(
        metadata: ProcMetadata,
        expiry_time: Option<std::time::SystemTime>,
        result: Result<(ProcAddr, ActorRef<ProcAgent>), HostError>,
    ) -> Self {
        match result {
            Ok((proc_id, mesh_agent)) => Self::Created {
                metadata,
                proc_id,
                mesh_agent,
                expiry_time,
            },
            Err(error) => Self::Failed { metadata, error },
        }
    }

    fn set_expiry_time(&mut self, expires_after: std::time::SystemTime) {
        match self {
            Self::Pending { expiry_time, .. } | Self::Created { expiry_time, .. } => {
                *expiry_time = Some(expires_after);
            }
            Self::Failed { .. } => {}
        }
    }

    fn rank(&self) -> usize {
        self.metadata().rank
    }

    fn host_mesh_id(&self) -> Option<&HostMeshId> {
        self.metadata().host_mesh_id.as_ref()
    }

    fn proc_mesh_id(&self) -> Option<&ProcMeshId> {
        self.metadata().proc_mesh_id.as_ref()
    }
}

struct PendingDrain {
    /// Number of matching proc spawns that were pending when the drain began.
    /// Each counted spawn decrements this exactly once. Reaching zero completes
    /// the drain and posts every reply exactly once.
    remaining: usize,
    failures: Vec<String>,
    timeout: Duration,
    max_in_flight: usize,
    replies: Vec<PendingDrainReply>,
}

struct PendingDrainReply {
    rank: usize,
    reply: PortRef<crate::StatusOverlay>,
}

#[derive(Default)]
struct PendingDrains {
    /// A full drain subsumes the selective drains, which remain in the map so
    /// that every selective request still receives its reply.
    full: Option<PendingDrain>,
    selective: HashMap<HostMeshId, PendingDrain>,
}

impl PendingDrains {
    fn blocks_create(&self, host_mesh_id: Option<&HostMeshId>) -> bool {
        self.full.is_some()
            || host_mesh_id.is_some_and(|host_mesh_id| self.selective.contains_key(host_mesh_id))
    }

    fn take_all(&mut self) -> impl Iterator<Item = PendingDrain> {
        let full = self.full.take();
        let selective = std::mem::take(&mut self.selective);
        full.into_iter().chain(selective.into_values())
    }
}

impl PendingDrain {
    fn post_replies(self, cx: &Context<'_, HostAgent>) {
        let status = if self.failures.is_empty() {
            resource::Status::Stopped
        } else {
            resource::Status::Failed(self.failures.join("; "))
        };

        self.post_replies_with_status(cx, status);
    }

    fn post_replies_with_status(self, cx: &Context<'_, HostAgent>, status: resource::Status) {
        for PendingDrainReply { rank, reply } in self.replies {
            let overlay =
                crate::StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status.clone())])
                    .expect("valid single-run overlay");
            reply.post(cx, overlay);
        }
    }
}

struct PendingShutdown {
    remaining: usize,
    timeout: Duration,
    max_in_flight: usize,
    acknowledgements: Vec<(usize, PortRef<usize>)>,
    drain_replies: Vec<PendingDrainReply>,
}

/// Actor name used when spawning the host mesh agent on the system proc.
pub const HOST_MESH_AGENT_ACTOR_NAME: &str = "host_agent";

/// Lifecycle state of the host managed by [`HostAgent`].
enum HostAgentState {
    /// Waiting for a client to attach. The host is idle and ready
    /// to accept new proc spawn requests.
    Detached(HostAgentMode),
    /// Actively running procs for an attached client.
    Attached(HostAgentMode),
    /// Procs are being drained by a DrainWorker. The host has been
    /// temporarily moved to the worker. The host agent remains
    /// responsive; min_proc_status() returns Stopping.
    Draining,
    /// Host fully shut down.
    Shutdown,
}

/// A mesh agent is responsible for managing a host in a [`HostMesh`],
/// through the resource behaviors defined in [`crate::resource`].
/// Self-notification sent by bridge tasks when a proc's status changes.
/// Not exported or registered — only used internally via `PortHandle`.
#[derive(Debug, Serialize, Deserialize, Named)]
struct ProcStatusChanged {
    id: ResourceId,
}

/// Sent by DrainWorker back to HostAgent when draining completes.
/// Not exported — delivered locally via PortHandle (no serialization).
struct DrainComplete(HostAgentMode);

/// Child actor whose only job is to run `host.terminate_children()` in
/// its `init()`, return the host and ack to the parent via DrainComplete,
/// and exit. Runs on the same proc as the host agent so it gets its
/// own `Instance` (required by `terminate_children`).
#[hyperactor::export(handlers = [])]
struct DrainWorker {
    host: Option<HostAgentMode>,
    timeout: Duration,
    max_in_flight: usize,
    done_notify: PortHandle<DrainComplete>,
}

#[async_trait]
impl Actor for DrainWorker {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        if let Some(host) = self.host.as_mut() {
            match host {
                HostAgentMode::Process { host, .. } => {
                    host.terminate_children(
                        this,
                        self.timeout,
                        self.max_in_flight.clamp(1, 256),
                        "drain host",
                    )
                    .await;
                }
                HostAgentMode::Local(host) => {
                    host.terminate_children(this, self.timeout, self.max_in_flight, "drain host")
                        .await;
                }
            }
        }

        // Restore the host before the parent reports drain completion.
        if let Some(host) = self.host.take() {
            let _ = self.done_notify.post(this, DrainComplete(host));
        }

        Ok(())
    }
}

impl fmt::Debug for DrainWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DrainWorker")
            .field("timeout", &self.timeout)
            .field("max_in_flight", &self.max_in_flight)
            .finish()
    }
}

#[hyperactor::export(
    handlers=[
        resource::CreateOrUpdate<ProcSpec>,
        SpawnProcs,
        WaitProcs,
        resource::Stop,
        resource::GetState<ProcState>,
        resource::KeepaliveGetState<ProcState>,
        GetHostProcStates,
        resource::StreamState<ProcState>,
        resource::GetRankStatus,
        resource::WaitRankStatus,
        resource::List,
        ShutdownHost,
        DrainHost,
        SetClientConfig,
        ProcStatusChanged,
        PySpyDump,
        PySpyProfile,
        ConfigDump,
        crate::proc_agent::SelfCheck,
    ]
)]
pub struct HostAgent {
    state: HostAgentState,
    pub(crate) created: HashMap<ResourceId, ProcCreationState>,
    pending_drains: PendingDrains,
    pending_shutdown: Option<PendingShutdown>,
    /// Pending `WaitRankStatus` waiters, keyed by resource name.
    /// Each entry is `(min_status, rank, reply_port)`. Only touched
    /// from `&mut self` handlers.
    pending_proc_waiters:
        HashMap<ResourceId, Vec<(resource::Status, usize, PortRef<crate::StatusOverlay>)>>,
    /// Procs that already have an active bridge task watching their status.
    watching: HashSet<ResourceId>,
    /// Port handle for sending `ProcStatusChanged` to self. Set in `init()`.
    proc_status_port: Option<PortHandle<ProcStatusChanged>>,
    /// Lazily initialized ProcAgent on the host's local proc.
    /// Boots on first [`GetLocalProc`] (LP-1 — see
    /// `crate::host::LOCAL_PROC_NAME`).
    local_mesh_agent: OnceLock<anyhow::Result<ActorHandle<ProcAgent>>>,
}

impl HostAgent {
    /// Create a host mesh agent for a process-backed host.
    pub(crate) fn new_process(
        host: Host<BootstrapProcManager>,
        shutdown_tx: Option<tokio::sync::oneshot::Sender<HostShutdownHandles>>,
    ) -> Self {
        Self::new(HostAgentMode::Process { host, shutdown_tx })
    }

    /// Create a host mesh agent for an in-process host.
    pub fn new_local(host: Host<LocalProcManager<ProcManagerSpawnFn>>) -> Self {
        Self::new(HostAgentMode::Local(host))
    }

    fn new(host: HostAgentMode) -> Self {
        Self {
            state: HostAgentState::Detached(host),
            created: HashMap::new(),
            pending_drains: PendingDrains::default(),
            pending_shutdown: None,
            pending_proc_waiters: HashMap::new(),
            watching: HashSet::new(),
            proc_status_port: None,
            local_mesh_agent: OnceLock::new(),
        }
    }

    /// Wait until the agent has completed `init` and can receive external
    /// messages through the host gateway.
    pub async fn wait_initialized(handle: &ActorHandle<Self>) -> anyhow::Result<()> {
        let mut status = handle.status();
        loop {
            let current = status.borrow_and_update().clone();
            match current {
                ActorStatus::Idle | ActorStatus::Processing(_, _) => return Ok(()),
                ActorStatus::Failed(err) => anyhow::bail!("host agent init failed: {err}"),
                ActorStatus::Stopped(reason) => anyhow::bail!("host agent stopped: {reason}"),
                ActorStatus::Stopping(ActorStoppingReason::Zombie(reason)) => {
                    anyhow::bail!("host agent zombie: {reason}")
                }
                ActorStatus::Unknown
                | ActorStatus::Created
                | ActorStatus::Initializing
                | ActorStatus::Client
                | ActorStatus::Stopping(_) => {}
            }
            if status.changed().await.is_err() {
                anyhow::bail!("host agent status channel closed before init completed");
            }
        }
    }

    /// Minimum status floor derived from the host agent's lifecycle.
    /// Procs on this host cannot be healthier than this.
    fn min_proc_status(&self) -> resource::Status {
        if self.pending_shutdown.is_some() {
            return resource::Status::Stopping;
        }
        match &self.state {
            HostAgentState::Detached(_) | HostAgentState::Attached(_) => resource::Status::Running,
            HostAgentState::Draining => resource::Status::Stopping,
            HostAgentState::Shutdown => resource::Status::Stopped,
        }
    }

    fn host(&self) -> Option<&HostAgentMode> {
        match &self.state {
            HostAgentState::Detached(h) | HostAgentState::Attached(h) => Some(h),
            _ => None,
        }
    }

    fn host_mut(&mut self) -> Option<&mut HostAgentMode> {
        match &mut self.state {
            HostAgentState::Detached(h) | HostAgentState::Attached(h) => Some(h),
            _ => None,
        }
    }

    /// Terminate all tracked children on the host and clear proc state.
    ///
    /// The host, system proc, mailbox server, and HostAgent all stay
    /// alive — only user procs are killed. After this returns the host
    /// is ready to accept new spawn requests with the same proc names.
    async fn drain(
        &mut self,
        cx: &Context<'_, Self>,
        timeout: std::time::Duration,
        max_in_flight: usize,
    ) {
        if let Some(host_mode) = self.host_mut() {
            match host_mode {
                HostAgentMode::Process { host, .. } => {
                    let summary = host
                        .terminate_children(cx, timeout, max_in_flight.clamp(1, 256), "stop host")
                        .await;
                    tracing::info!(?summary, "terminated children on host");
                }
                HostAgentMode::Local(host) => {
                    let summary = host
                        .terminate_children(cx, timeout, max_in_flight, "stop host")
                        .await;
                    tracing::info!(?summary, "terminated children on local host");
                }
            }
        }
        self.created.clear();
    }

    /// Selectively stop procs belonging to a specific host mesh.
    /// Only procs whose `host_mesh_id` matches `filter` are stopped;
    /// all other procs are left running.
    async fn drain_by_mesh_name(
        &mut self,
        cx: &Context<'_, Self>,
        timeout: std::time::Duration,
        filter: Option<&HostMeshId>,
    ) {
        let matching_ids: Vec<ResourceId> = self
            .created
            .iter()
            .filter(|(_, state)| {
                state.host_mesh_id() == filter
                    && matches!(
                        state,
                        ProcCreationState::Created { .. } | ProcCreationState::Failed { .. }
                    )
            })
            .map(|(id, _)| id.clone())
            .collect();

        if let Some(host_mode) = self.host() {
            for id in &matching_ids {
                if let Some(ProcCreationState::Created { proc_id, .. }) = self.created.get(id) {
                    match host_mode {
                        HostAgentMode::Process { host, .. } => {
                            let _ = host
                                .terminate_proc(cx, proc_id, timeout, "selective drain")
                                .await;
                        }
                        HostAgentMode::Local(host) => {
                            let _ = host
                                .terminate_proc(cx, proc_id, timeout, "selective drain")
                                .await;
                        }
                    }
                }
            }
        }

        // Remove drained entries and associated state so that
        // future spawns with the same proc names get fresh watch bridges.
        for id in &matching_ids {
            self.created.remove(id);
            self.watching.remove(id);
            self.pending_proc_waiters.remove(id);
        }

        tracing::info!(
            count = matching_ids.len(),
            filter = ?filter,
            "selectively drained procs",
        );
    }

    /// Publish the current host properties and child list for
    /// introspection. Called from init and after each state change
    /// (proc created/stopped).
    fn publish_introspect_properties(&self, cx: &Instance<Self>) {
        let host = match self.host() {
            Some(h) => h,
            None => return, // host shut down or stopping
        };

        let addr = host.addr().to_string();
        let mut children: Vec<hyperactor::introspect::IntrospectRef> = Vec::new();
        let system_children: Vec<crate::introspect::NodeRef> = Vec::new(); // LC-2

        // Procs are not system — only actors are. Both service and
        // local appear as regular children; 's' in the TUI toggles
        // actor visibility, not proc visibility.
        children.push(hyperactor::introspect::IntrospectRef::Proc(
            host.system_proc().proc_addr().clone(),
        ));
        children.push(hyperactor::introspect::IntrospectRef::Proc(
            host.local_proc().proc_addr().clone(),
        ));

        // User procs.
        for state in self.created.values() {
            if let ProcCreationState::Created { proc_id, .. } = state {
                children.push(hyperactor::introspect::IntrospectRef::Proc(proc_id.clone()));
            }
        }

        let num_procs = children.len();

        let mut attrs = hyperactor_config::Attrs::new();
        attrs.set(crate::introspect::NODE_TYPE, "host".to_string());
        attrs.set(crate::introspect::ADDR, addr);
        attrs.set(crate::introspect::NUM_PROCS, num_procs);
        attrs.set(hyperactor::introspect::CHILDREN, children);
        attrs.set(crate::introspect::SYSTEM_CHILDREN, system_children);
        // PD-*: hosting-process memory stats. This is the same
        // hosting OS process signal surfaced on the proc path, but
        // the host path does not attempt to publish proc-local queue
        // pressure.
        let memory = crate::introspect::ProcessMemoryStats::read_from_procfs();
        memory.to_attrs(&mut attrs);
        cx.publish_attrs(attrs);
    }
}

#[async_trait]
impl Actor for HostAgent {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.bind::<Self>();
        if matches!(self.host().unwrap(), HostAgentMode::Process { .. }) {
            let (directory, file) = hyperactor_telemetry::log_file_path(
                hyperactor_telemetry::env::Env::current(),
                None,
            )
            .unwrap();
            eprintln!(
                "Monarch internal logs are being written to {}/{}.log; execution id {}",
                directory,
                file,
                hyperactor_telemetry::env::execution_id(),
            );
        }
        this.set_system();
        self.publish_introspect_properties(this);

        // Register callback for QueryChild — resolves system procs
        // that are not independently addressable actors.
        let host = self.host().expect("host present");
        let system_proc = host.system_proc().clone();
        let local_proc = host.local_proc().clone();
        let self_id = this.self_addr().clone();
        this.set_query_child_handler(move |child_ref| {
            use hyperactor::introspect::IntrospectResult;

            let proc = match child_ref {
                Addr::Proc(proc_ref) => {
                    if *proc_ref == system_proc.proc_addr() {
                        Some((&system_proc, SERVICE_PROC_NAME))
                    } else if *proc_ref == local_proc.proc_addr() {
                        Some((&local_proc, LOCAL_PROC_NAME))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            match proc {
                Some((proc, label)) => {
                    // Use all_instance_keys() instead of
                    // all_actor_ids() to avoid holding DashMap shard
                    // read locks while doing Weak::upgrade() +
                    // watch::borrow() + is_terminal() per entry.
                    // Under rapid actor churn the per-entry work in
                    // all_actor_ids() causes convoy starvation with
                    // concurrent insert/remove operations, stalling
                    // the spawn/exit path. all_instance_keys() just
                    // clones keys — microseconds per shard. Actor
                    // addresses and the is_system check use individual
                    // point lookups outside the iteration. Stale keys
                    // are harmless: if the point lookup fails, the actor
                    // has already gone away.
                    let all_keys = proc.all_instance_keys();
                    let mut actors: Vec<hyperactor::introspect::IntrospectRef> =
                        Vec::with_capacity(all_keys.len());
                    let mut system_actors: Vec<crate::introspect::NodeRef> = Vec::new();
                    for id in all_keys {
                        if let Some(cell) = proc.get_instance_by_id(&id) {
                            let actor_addr = cell.actor_addr().clone();
                            if cell.is_system() {
                                system_actors
                                    .push(crate::introspect::NodeRef::Actor(actor_addr.clone()));
                            }
                            actors.push(hyperactor::introspect::IntrospectRef::Actor(actor_addr));
                        }
                    }
                    let mut attrs = hyperactor_config::Attrs::new();
                    attrs.set(crate::introspect::NODE_TYPE, "proc".to_string());
                    attrs.set(crate::introspect::PROC_NAME, label.to_string());
                    attrs.set(crate::introspect::NUM_ACTORS, actors.len());
                    attrs.set(crate::introspect::SYSTEM_CHILDREN, system_actors.clone());
                    // PD-*: include proc debug stats so QueryChild
                    // results carry real signal. Memory from procfs,
                    // queue stats from the Proc's runtime accounting.
                    let memory = crate::introspect::ProcessMemoryStats::read_from_procfs();
                    memory.to_attrs(&mut attrs);
                    attrs.set(
                        crate::introspect::ACTOR_WORK_QUEUE_DEPTH_TOTAL,
                        proc.queue_depth_total(),
                    );
                    // Per-actor max from the live actor scan.
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
                    let attrs_json =
                        serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());

                    IntrospectResult {
                        identity: hyperactor::introspect::IntrospectRef::Proc(
                            proc.proc_addr().clone(),
                        ),
                        attrs: attrs_json,
                        children: actors,
                        parent: Some(hyperactor::introspect::IntrospectRef::Actor(
                            self_id.clone(),
                        )),
                        as_of: std::time::SystemTime::now(),
                    }
                }
                None => {
                    let mut error_attrs = hyperactor_config::Attrs::new();
                    error_attrs.set(hyperactor::introspect::ERROR_CODE, "not_found".to_string());
                    error_attrs.set(
                        hyperactor::introspect::ERROR_MESSAGE,
                        format!("child {} not found", child_ref),
                    );
                    let identity = match child_ref {
                        Addr::Proc(p) => hyperactor::introspect::IntrospectRef::Proc(p.clone()),
                        Addr::Actor(a) => hyperactor::introspect::IntrospectRef::Actor(a.clone()),
                        Addr::Port(p) => {
                            hyperactor::introspect::IntrospectRef::Actor(p.actor_addr())
                        }
                    };
                    IntrospectResult {
                        identity,
                        attrs: serde_json::to_string(&error_attrs)
                            .unwrap_or_else(|_| "{}".to_string()),
                        children: Vec::new(),
                        parent: None,
                        as_of: std::time::SystemTime::now(),
                    }
                }
            }
        });

        self.proc_status_port = Some(this.port::<ProcStatusChanged>());

        // Kick off the SelfCheck reaper if the orphan timeout is configured.
        // The reaper walks `created` looking for procs whose owner stopped
        // extending the keepalive and tears them down.
        if let Some(delay) = hyperactor_config::global::get(crate::proc_agent::MESH_ORPHAN_TIMEOUT)
        {
            this.post_after(this, crate::proc_agent::SelfCheck::default(), delay);
        }

        Ok(())
    }
}

impl fmt::Debug for HostAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostAgent")
            .field("host", &"..")
            .field("created", &self.created)
            .finish()
    }
}

/// Cast to every HostAgent to spawn a mesh's per-host proc slots in one message.
/// Identity is positional: each recipient derives its procs' ids/ranks from its
/// stamped `rank` plus `proc_mesh_id`/`num_per_host`, since a shared cast payload
/// can't carry a per-host `id` (unlike `CreateOrUpdate`).
#[derive(
    Serialize,
    Deserialize,
    Clone,
    Debug,
    Named,
    Handler,
    RefClient,
    HandleClient
)]
pub struct SpawnProcs {
    /// This host's ordinal within the cast region, stamped by the cast layer.
    pub rank: resource::Rank,
    /// Proc mesh id used to deterministically derive per-host proc ids.
    pub proc_mesh_id: ProcMeshId,
    /// Number of procs to spawn on this host.
    pub num_per_host: usize,
    /// Config values to set on each spawned proc's global config, at the
    /// `ClientOverride` layer.
    pub client_config_override: Attrs,
    /// The HostMesh that owns these procs (used by `DrainHost` for selective
    /// drain).
    pub host_mesh_id: Option<HostMeshId>,
    /// Bootstrap command to use when no per-rank override applies.
    pub default_bootstrap_command: Option<BootstrapCommand>,
    /// Optional per-proc CPU/NUMA binding, indexed by per-host slot. `HostMesh::spawn`
    /// already rejects a length that doesn't match the per-host extent, so the
    /// handler is deliberately tolerant of a missing slot (including an index
    /// past the vec's length): it spawns with no binding rather than panicking.
    pub proc_bind: Option<Vec<ProcBind>>,
    /// Optional per-rank bootstrap overrides, indexed by absolute proc rank
    /// (`num_per_host * host_rank + per_host_rank`).
    pub bootstrap_commands: Option<Vec<Option<BootstrapCommand>>>,
}
wirevalue::register_type!(SpawnProcs);

#[async_trait]
impl Handler<SpawnProcs> for HostAgent {
    #[tracing::instrument("HostAgent::SpawnProcs", level = "info", skip_all, fields(host_rank, num = spawn.num_per_host))]
    async fn handle(&mut self, cx: &Context<Self>, spawn: SpawnProcs) -> anyhow::Result<()> {
        let host_rank = spawn
            .rank
            .0
            .expect("cast layer stamps the rank before delivery");

        tracing::Span::current().record("host_rank", host_rank);

        for (per_host_rank, rank, id) in
            proc_slots(&spawn.proc_mesh_id, host_rank, spawn.num_per_host)
        {
            let bootstrap_command = spawn
                .bootstrap_commands
                .as_ref()
                .and_then(|commands| commands.get(rank).cloned().flatten())
                .or_else(|| spawn.default_bootstrap_command.clone());

            let proc_bind = spawn
                .proc_bind
                .as_ref()
                .and_then(|binds| binds.get(per_host_rank).cloned());

            if let Err(error) = <Self as Handler<resource::CreateOrUpdate<ProcSpec>>>::handle(
                self,
                cx,
                resource::CreateOrUpdate {
                    id,
                    rank: resource::Rank::new(rank),
                    spec: ProcSpec {
                        client_config_override: spawn.client_config_override.clone(),
                        proc_bind,
                        bootstrap_command,
                        host_mesh_id: spawn.host_mesh_id.clone(),
                        proc_mesh_id: Some(spawn.proc_mesh_id.clone()),
                    },
                },
            )
            .await
            {
                tracing::error!(%error, "failed to request proc creation");
                break;
            }
        }

        Ok(())
    }
}

/// Cast after [`SpawnProcs`] to report when each derived proc reaches
/// [`Status::Running`]. Hyperactor delivers the two casts in order for each
/// sender-destination stream, so an absent proc means its create was rejected.
#[derive(
    Serialize,
    Deserialize,
    Clone,
    Debug,
    Named,
    Handler,
    RefClient,
    HandleClient
)]
pub struct WaitProcs {
    /// This host's ordinal within the cast region, stamped by the cast layer.
    pub rank: resource::Rank,
    /// Proc mesh id used to derive the same proc ids as [`SpawnProcs`].
    pub proc_mesh_id: ProcMeshId,
    /// Number of procs spawned on this host.
    pub num_per_host: usize,
    /// Sparse readiness updates for the caller's status barrier.
    pub status_reply: PortRef<crate::StatusOverlay>,
}
wirevalue::register_type!(WaitProcs);

#[async_trait]
impl Handler<WaitProcs> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, wait: WaitProcs) -> anyhow::Result<()> {
        let host_rank = wait
            .rank
            .0
            .expect("cast layer stamps the rank before delivery");
        let mut failed = Vec::new();

        for (_, rank, id) in proc_slots(&wait.proc_mesh_id, host_rank, wait.num_per_host) {
            if self.created.contains_key(&id) {
                if let Err(error) = <Self as Handler<resource::WaitRankStatus>>::handle(
                    self,
                    cx,
                    resource::WaitRankStatus {
                        id,
                        rank: resource::Rank::new(rank),
                        min_status: Status::Running,
                        reply: wait.status_reply.clone(),
                    },
                )
                .await
                {
                    failed.push((
                        rank..(rank + 1),
                        Status::Failed(format!("failed to wait for proc status: {error}")),
                    ));
                }
            } else {
                failed.push((
                    rank..(rank + 1),
                    Status::Failed("proc creation was not accepted".to_string()),
                ));
            }
        }

        if !failed.is_empty() {
            let overlay = crate::StatusOverlay::try_from_runs(failed)
                .expect("proc slots produce ordered non-overlapping ranks");
            wait.status_reply.post(cx, overlay);
        }

        Ok(())
    }
}

// Point-to-point proc spawn by carried `id`. The shared per-proc creation path:
// invoked directly (e.g. unit/admin tests) and once per slot by the `SpawnProcs`
// cast handler, which derives each proc's id/rank from the host's stamped rank.
#[async_trait]
impl Handler<resource::CreateOrUpdate<ProcSpec>> for HostAgent {
    #[tracing::instrument("HostAgent::CreateOrUpdate", level = "info", skip_all, fields(id=%create_or_update.id))]
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<ProcSpec>,
    ) -> anyhow::Result<()> {
        if self.created.contains_key(&create_or_update.id) {
            // Already requested or completed: there is no update.
            return Ok(());
        }
        if self.pending_shutdown.is_some() {
            tracing::warn!(
                id = %create_or_update.id,
                "ignoring CreateOrUpdate: HostAgent is shutting down"
            );
            return Ok(());
        }
        if self
            .pending_drains
            .blocks_create(create_or_update.spec.host_mesh_id.as_ref())
        {
            tracing::warn!(
                id = %create_or_update.id,
                "ignoring CreateOrUpdate: HostAgent is draining"
            );
            return Ok(());
        }

        let host = match self.host() {
            Some(h) => h,
            None => {
                tracing::warn!(
                    id = %create_or_update.id,
                    "ignoring CreateOrUpdate: HostAgent has already shut down"
                );
                return Ok(());
            }
        };
        let rank = create_or_update.rank.unwrap();
        let id = create_or_update.id.clone();
        let spawn = match host {
            HostAgentMode::Process { host, .. } => Either::Left(host.spawn(
                id.to_string(),
                BootstrapProcConfig {
                    create_rank: rank,
                    client_config_override: create_or_update.spec.client_config_override.clone(),
                    proc_bind: create_or_update.spec.proc_bind.clone(),
                    bootstrap_command: create_or_update.spec.bootstrap_command.clone(),
                },
            )),
            HostAgentMode::Local(host) => Either::Right(host.spawn(id.to_string(), ())),
        };

        let was_empty = self.created.is_empty();
        self.created.insert(
            id.clone(),
            ProcCreationState::pending(
                ProcMetadata {
                    rank,
                    host_mesh_id: create_or_update.spec.host_mesh_id.clone(),
                    proc_mesh_id: create_or_update.spec.proc_mesh_id.clone(),
                },
                None,
            ),
        );

        let created = spawn.await;

        if let Err(e) = &created {
            tracing::error!("failed to spawn proc {}: {}", id, e);
        }
        let (metadata, expiry_time) = match self
            .created
            .remove(&id)
            .expect("synchronous proc creation remains pending until spawn completes")
        {
            ProcCreationState::Pending {
                metadata,
                on_completion,
                expiry_time,
            } if on_completion.is_keep() => (metadata, expiry_time),
            ProcCreationState::Pending { .. }
            | ProcCreationState::Created { .. }
            | ProcCreationState::Failed { .. } => {
                unreachable!("HostAgent cannot process another request during synchronous spawn")
            }
        };
        self.created.insert(
            id.clone(),
            ProcCreationState::from_spawn_result(metadata, expiry_time, created),
        );

        // Transition Detached → Attached on first proc creation.
        if was_empty && let HostAgentState::Detached(_) = &self.state {
            let host = match std::mem::replace(&mut self.state, HostAgentState::Shutdown) {
                HostAgentState::Detached(h) => h,
                _ => unreachable!(),
            };
            self.state = HostAgentState::Attached(host);
        }

        // A WaitRankStatus stashed before this proc existed carries its own reply
        // rank (RSP-3); starting a status watch bridge for it needs the proc_id.
        let proc_id = self.created.get(&id).and_then(|state| match state {
            ProcCreationState::Created { proc_id, .. } => Some(proc_id.clone()),
            ProcCreationState::Pending { .. } | ProcCreationState::Failed { .. } => None,
        });

        // Bridge status changes to any pending waiters, then flush once now.
        if self.pending_proc_waiters.contains_key(&id) {
            if let Some(proc_id) = &proc_id {
                self.start_watch_bridge(&id, proc_id).await;
            }
            self.flush_proc_waiters(cx, &id).await;
        }

        self.publish_introspect_properties(cx);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::Stop> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, message: resource::Stop) -> anyhow::Result<()> {
        tracing::info!(
            name = "HostMeshAgentStatus",
            proc_id = %message.id,
            reason = %message.reason,
            "stopping proc"
        );
        let timeout = hyperactor_config::global::get(hyperactor::config::PROCESS_EXIT_TIMEOUT);

        if let Some(ProcCreationState::Pending { on_completion, .. }) =
            self.created.get_mut(&message.id)
        {
            on_completion.request_stop(timeout, message.reason.clone());
            self.flush_proc_waiters(cx, &message.id).await;
            self.publish_introspect_properties(cx);
            return Ok(());
        }

        let host = match self.host() {
            Some(h) => h,
            None => {
                // Host already shut down; all procs are terminated.
                tracing::debug!(
                    proc_id = %message.id,
                    "ignoring Stop: HostAgent has already shut down"
                );
                return Ok(());
            }
        };

        if let Some(ProcCreationState::Created { proc_id, .. }) = self.created.get(&message.id) {
            host.request_stop(cx, proc_id, timeout, &message.reason)
                .await;
        }

        // Status may have changed to Stopping; notify pending waiters.
        self.flush_proc_waiters(cx, &message.id).await;

        self.publish_introspect_properties(cx);
        Ok(())
    }
}

impl HostAgent {
    /// The `(rank, status)` for a created proc, clamped to the host's minimum
    /// status. `rank == usize::MAX` means the proc is unknown to this host.
    async fn proc_rank_status(&self, id: &ResourceId) -> (usize, Status) {
        match self.created.get(id) {
            Some(ProcCreationState::Pending {
                metadata,
                on_completion,
                ..
            }) => (
                metadata.rank,
                if on_completion.is_keep() {
                    Status::Initializing
                } else {
                    Status::Stopping
                },
            ),
            Some(ProcCreationState::Created {
                metadata, proc_id, ..
            }) => {
                let raw_status = match self.host() {
                    Some(host) => host.proc_status(proc_id).await.0,
                    None => resource::Status::Unknown,
                };
                (metadata.rank, raw_status.clamp_min(self.min_proc_status()))
            }
            Some(ProcCreationState::Failed { metadata, error }) => {
                (metadata.rank, Status::Failed(error.to_string()))
            }
            None => (usize::MAX, Status::NotExist),
        }
    }
}

#[async_trait]
impl Handler<resource::GetRankStatus> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_rank_status: resource::GetRankStatus,
    ) -> anyhow::Result<()> {
        // `proc_rank_status` returns `usize::MAX` for a proc unknown to this
        // host, which still yields an empty overlay (RSP-4). A *known* proc is
        // positioned at the rank the request carries (the caller's view), not
        // the stored creation rank (RSP-1).
        let (found_rank, status) = self.proc_rank_status(&get_rank_status.id).await;
        let overlay = if found_rank == usize::MAX {
            StatusOverlay::new()
        } else {
            let rank = get_rank_status.rank.unwrap();
            StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                .expect("valid single-run overlay")
        };
        get_rank_status.reply.post(cx, overlay);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::WaitRankStatus> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: resource::WaitRankStatus,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;
        use crate::resource::Status;

        // Position from the rank the request carries (the caller's view), not the
        // stored creation rank (RSP-1). A deferred / pre-create waiter retains it
        // (RSP-3).
        let rank = msg.rank.unwrap();
        match self.created.get(&msg.id) {
            Some(ProcCreationState::Created { proc_id, .. }) => {
                let status = match self.host() {
                    Some(host) => host.proc_status(proc_id).await.0,
                    None => Status::Stopped,
                };

                // If already at or past the requested threshold, reply immediately.
                if status >= msg.min_status {
                    let overlay = StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                        .expect("valid single-run overlay");
                    let _ = msg.reply.post(cx, overlay);
                    return Ok(());
                }

                // Stash the waiter and start a bridge if we don't have one yet.
                self.pending_proc_waiters
                    .entry(msg.id.clone())
                    .or_default()
                    .push((msg.min_status, rank, msg.reply));

                let proc_id = proc_id.clone();
                self.start_watch_bridge(&msg.id, &proc_id).await;
            }
            Some(ProcCreationState::Failed { error, .. }) => {
                // Creation failed — reply immediately with Failed status.
                let overlay = StatusOverlay::try_from_runs(vec![(
                    rank..(rank + 1),
                    Status::Failed(error.to_string()),
                )])
                .expect("valid single-run overlay");
                let _ = msg.reply.post(cx, overlay);
            }
            Some(ProcCreationState::Pending { .. }) | None => {
                // Proc isn't ready yet. Stash the waiter with the rank the
                // request carries (RSP-3); proc creation starts the watch bridge.
                self.pending_proc_waiters
                    .entry(msg.id.clone())
                    .or_default()
                    .push((msg.min_status, rank, msg.reply));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<ProcStatusChanged> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: ProcStatusChanged) -> anyhow::Result<()> {
        self.flush_proc_waiters(cx, &msg.id).await;
        Ok(())
    }
}

impl HostAgent {
    /// Flush pending `WaitRankStatus` waiters whose threshold is now satisfied.
    async fn flush_proc_waiters(&mut self, cx: &Context<'_, Self>, id: &ResourceId) {
        use crate::StatusOverlay;
        use crate::resource::Status;

        let status = match self.created.get(id) {
            Some(ProcCreationState::Pending { on_completion, .. }) => {
                if on_completion.is_keep() {
                    Status::Initializing
                } else {
                    Status::Stopping
                }
            }
            Some(ProcCreationState::Created { proc_id, .. }) => match self.host() {
                Some(host) => host.proc_status(proc_id).await.0,
                None => Status::Stopped,
            },
            Some(ProcCreationState::Failed { error, .. }) => Status::Failed(error.to_string()),
            None => {
                // Proc not created yet, nothing to flush.
                return;
            }
        };

        let Some(waiters) = self.pending_proc_waiters.get_mut(id) else {
            return;
        };

        let remaining = std::mem::take(waiters);
        for (min_status, rank, reply) in remaining {
            if status >= min_status {
                // Each waiter keeps the reply rank it registered with (RSP-3),
                // positioned in the caller's view (RSP-1).
                let overlay =
                    StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status.clone())])
                        .expect("valid single-run overlay");
                let _ = reply.post(cx, overlay);
            } else {
                waiters.push((min_status, rank, reply));
            }
        }

        if waiters.is_empty() {
            self.pending_proc_waiters.remove(id);
        }
    }

    /// Start a bridge task that watches a proc's status channel and sends
    /// `ProcStatusChanged` to self on each change. At most one bridge per proc.
    async fn start_watch_bridge(&mut self, id: &ResourceId, proc_id: &ProcAddr) {
        if self.watching.contains(id) {
            return;
        }
        self.watching.insert(id.clone());

        let port = match &self.proc_status_port {
            Some(p) => p.clone(),
            None => return,
        };

        match self.host() {
            Some(HostAgentMode::Process { host, .. }) => {
                if let Some(rx) = host.manager().watch(proc_id).await {
                    start_proc_watch(port, rx, id.clone(), |s| s.clone().into());
                }
            }
            Some(HostAgentMode::Local(host)) => {
                if let Some(rx) = host.manager().watch(proc_id).await {
                    start_proc_watch(port, rx, id.clone(), |s| (*s).into());
                }
            }
            None => {}
        }
    }
}

/// Spawn a bridge task that watches a proc's status channel and sends
/// `ProcStatusChanged` to the actor via the given `PortHandle`.
fn start_proc_watch<S>(
    port: PortHandle<ProcStatusChanged>,
    mut rx: tokio::sync::watch::Receiver<S>,
    id: ResourceId,
    to_status: impl Fn(&S) -> resource::Status + Send + 'static,
) where
    S: Send + Sync + 'static,
{
    // TODO: replace Instance::self_client() with a proper mechanism
    // for sending to port handles without an actor context.
    let client = Instance::<()>::self_client();
    tokio::spawn(async move {
        loop {
            match rx.changed().await {
                Ok(()) => {
                    let status = to_status(&*rx.borrow());
                    let terminated = status.is_terminated();
                    let _ = port.post(client, ProcStatusChanged { id: id.clone() });
                    if terminated {
                        return;
                    }
                }
                Err(_) => {
                    let _ = port.post(client, ProcStatusChanged { id: id.clone() });
                    return;
                }
            }
        }
    });
}

#[derive(
    Serialize,
    Deserialize,
    Clone,
    Debug,
    Named,
    Handler,
    RefClient,
    HandleClient
)]
pub struct ShutdownHost {
    /// Grace window: send SIGTERM and wait this long before
    /// escalating.
    pub timeout: std::time::Duration,
    /// Max number of children to terminate concurrently on this host.
    pub max_in_flight: usize,
    /// This host's ordinal within the shutdown cast region, stamped by the
    /// cast layer. The host echoes it back via `ack` so the caller can tell
    /// exactly which hosts acknowledged shutdown.
    pub rank: resource::Rank,
    /// Direct reply carrying this host's rank once shutdown work is done.
    ///
    /// Intentionally must NOT be split/tree-reduced: `ShutdownHost` makes each
    /// host exit right after acking, so a tree-reduced ack would stall — the
    /// node that fans in peers' acks tears down before they arrive. Replying
    /// directly to the caller survives the responders exiting. The caller binds
    /// this port `.unsplit()` so the cast layer leaves it alone, and collects
    /// one direct reply per host (see `HostMeshRef::cast_shutdown`).
    pub ack: PortRef<usize>,
}
wirevalue::register_type!(ShutdownHost);

/// Drain user procs on this host but keep the host, service proc,
/// and networking alive. Used during mesh stop/shutdown so that
/// forwarder flushes can still reach remote hosts.
///
/// If `host_mesh_id` is `Some`, only procs belonging to that mesh
/// are stopped (selective drain). If `None`, all procs are
/// terminated (full drain).
#[derive(
    Serialize,
    Deserialize,
    Clone,
    Debug,
    Named,
    Handler,
    RefClient,
    HandleClient
)]
pub struct DrainHost {
    pub timeout: std::time::Duration,
    pub max_in_flight: usize,
    pub host_mesh_id: Option<HostMeshId>,
    /// The recipient's ordinal within the drain cast region, stamped by
    /// the cast layer. Used to position this host's status overlay.
    pub rank: resource::Rank,
    /// Streaming status reply. Each host reports a single-rank `Stopped`
    /// overlay once it has drained; the caller reduces these into a
    /// `StatusMesh` barrier and can detect hosts that never reported.
    pub reply: PortRef<crate::StatusOverlay>,
}
wirevalue::register_type!(DrainHost);

#[async_trait]
impl Handler<DrainHost> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: DrainHost) -> anyhow::Result<()> {
        let rank = msg.rank.unwrap();
        if let Some(shutdown) = self.pending_shutdown.as_mut() {
            shutdown.drain_replies.push(PendingDrainReply {
                rank,
                reply: msg.reply,
            });
            return Ok(());
        }

        let active_drain = match msg.host_mesh_id.as_ref() {
            Some(host_mesh_id) => self.pending_drains.selective.get_mut(host_mesh_id),
            None => self.pending_drains.full.as_mut(),
        };
        if let Some(drain) = active_drain {
            drain.replies.push(PendingDrainReply {
                rank,
                reply: msg.reply,
            });
            return Ok(());
        }

        // CreateOrUpdate still waits for Host::spawn, so a drain cannot observe
        // an in-progress proc creation in this revision.
        let remaining = self.mark_requests_for_drain(msg.host_mesh_id.as_ref());
        let drain = PendingDrain {
            remaining,
            failures: Vec::new(),
            timeout: msg.timeout,
            max_in_flight: msg.max_in_flight,
            replies: vec![PendingDrainReply {
                rank,
                reply: msg.reply,
            }],
        };

        match msg.host_mesh_id {
            Some(host_mesh_id) => {
                if self.pending_drains.full.is_some() {
                    let replaced = self.pending_drains.selective.insert(host_mesh_id, drain);
                    debug_assert!(replaced.is_none(), "duplicate selective drain was joined");
                    return Ok(());
                }

                self.drain_by_mesh_name(cx, msg.timeout, Some(&host_mesh_id))
                    .await;

                if remaining == 0 {
                    drain.post_replies(cx);
                } else {
                    let replaced = self.pending_drains.selective.insert(host_mesh_id, drain);
                    debug_assert!(replaced.is_none(), "duplicate selective drain was joined");
                }
            }
            None => {
                if remaining == 0 && self.pending_drains.selective.is_empty() {
                    self.start_full_drain(cx, drain);
                } else {
                    debug_assert!(
                        self.pending_drains.full.is_none(),
                        "duplicate full drain joined"
                    );
                    self.pending_drains.full = Some(drain);
                }
            }
        }

        Ok(())
    }
}

impl HostAgent {
    fn start_full_drain(&mut self, cx: &Context<'_, Self>, drain: PendingDrain) {
        if drain.remaining != 0 {
            unreachable!("full drain requires no remaining proc creations");
        }
        let timeout = drain.timeout;
        let max_in_flight = drain.max_in_flight;

        let host = match std::mem::replace(&mut self.state, HostAgentState::Draining) {
            HostAgentState::Attached(h) => h,
            other @ (HostAgentState::Detached(_) | HostAgentState::Draining) => {
                // Nothing to drain — report immediately.
                self.state = other;
                drain.post_replies(cx);
                return;
            }
            HostAgentState::Shutdown => {
                self.state = HostAgentState::Shutdown;
                drain.post_replies(cx);
                return;
            }
        };

        debug_assert!(
            self.pending_drains.full.is_none(),
            "duplicate full drain was joined"
        );
        self.pending_drains.full = Some(drain);

        // Do not clear `self.created` here. The DrainWorker terminates procs
        // asynchronously, and status queries must still find their entries.
        let done_port = cx.port::<DrainComplete>();

        cx.spawn_with_label(
            "drain_worker",
            DrainWorker {
                host: Some(host),
                timeout,
                max_in_flight,
                done_notify: done_port,
            },
        );
    }
    fn mark_requests_for_drain(&mut self, filter: Option<&HostMeshId>) -> usize {
        let mut remaining = 0;
        for state in self.created.values_mut() {
            if filter.is_some_and(|filter| state.host_mesh_id() != Some(filter)) {
                continue;
            }

            if let ProcCreationState::Pending { on_completion, .. } = state
                && on_completion.request_drain()
            {
                remaining += 1;
            }
        }
        remaining
    }
}

#[async_trait]
impl Handler<DrainComplete> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: DrainComplete) -> anyhow::Result<()> {
        self.state = HostAgentState::Detached(msg.0);
        self.created.clear();
        for drain in self.pending_drains.take_all() {
            drain.post_replies(cx);
        }

        if self
            .pending_shutdown
            .as_ref()
            .is_some_and(|shutdown| shutdown.remaining == 0)
        {
            let shutdown = self
                .pending_shutdown
                .take()
                .expect("pending shutdown exists");
            self.finish_shutdown(cx, shutdown).await;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<ShutdownHost> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: ShutdownHost) -> anyhow::Result<()> {
        let rank = msg.rank.unwrap();
        if matches!(&self.state, HostAgentState::Shutdown) {
            msg.ack.post(cx, rank);
            return Ok(());
        }

        if let Some(shutdown) = self.pending_shutdown.as_mut() {
            shutdown.acknowledgements.push((rank, msg.ack));
            return Ok(());
        }

        for drain in self.pending_drains.take_all() {
            drain.post_replies_with_status(
                cx,
                resource::Status::Failed("host shutdown superseded drain".to_string()),
            );
        }

        // CreateOrUpdate still waits for Host::spawn, so shutdown cannot
        // observe an in-progress proc creation in this revision.
        let remaining = self.mark_requests_for_shutdown();
        self.pending_shutdown = Some(PendingShutdown {
            remaining,
            timeout: msg.timeout,
            max_in_flight: msg.max_in_flight,
            acknowledgements: vec![(rank, msg.ack)],
            drain_replies: Vec::new(),
        });

        if remaining == 0 {
            let shutdown = self
                .pending_shutdown
                .take()
                .expect("pending shutdown exists");
            self.finish_shutdown(cx, shutdown).await;
        }

        Ok(())
    }
}

impl HostAgent {
    async fn finish_shutdown(&mut self, cx: &Context<'_, Self>, shutdown: PendingShutdown) {
        if matches!(&self.state, HostAgentState::Draining) {
            self.pending_shutdown = Some(shutdown);
            return;
        }

        let PendingShutdown {
            remaining: 0,
            timeout,
            max_in_flight,
            acknowledgements,
            drain_replies,
        } = shutdown
        else {
            unreachable!("shutdown cannot finish while proc creation is pending");
        };

        // Terminate children BEFORE acking, so the caller's networking
        // stays alive while children flush their forwarders during
        // teardown. If we ack first, the caller proceeds to tear down
        // the host proc's networking while children are still running,
        // causing their forwarder flushes to hang until
        // MESSAGE_DELIVERY_TIMEOUT expires.
        if !self.created.is_empty() {
            self.drain(cx, timeout, max_in_flight).await;
        }

        for PendingDrainReply { rank, reply } in drain_replies {
            let overlay = crate::StatusOverlay::try_from_runs(vec![(
                rank..(rank + 1),
                resource::Status::Stopped,
            )])
            .expect("valid single-run overlay");
            reply.post(cx, overlay);
        }

        // Drop the host and signal the bootstrap loop to drain the
        // mailbox and exit.
        match std::mem::replace(&mut self.state, HostAgentState::Shutdown) {
            HostAgentState::Detached(HostAgentMode::Process {
                mut host,
                shutdown_tx: Some(tx),
            })
            | HostAgentState::Attached(HostAgentMode::Process {
                mut host,
                shutdown_tx: Some(tx),
            }) => {
                // Keep the host's outbound gateway alive until the direct
                // shutdown acknowledgments have been flushed. The bootstrap
                // task may stop the frontend as soon as it receives the
                // handle below.
                for (rank, ack) in acknowledgements {
                    ack.post(cx, rank);
                }
                let flush_timeout =
                    hyperactor_config::global::get(hyperactor::config::FORWARDER_FLUSH_TIMEOUT);
                match tokio::time::timeout(flush_timeout, host.gateway().flush()).await {
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "gateway flush failed during host shutdown");
                    }
                    Err(_elapsed) => {
                        tracing::warn!("gateway flush timed out during host shutdown");
                    }
                    Ok(Ok(())) => {}
                }
                tracing::info!(
                    proc_id = %cx.self_addr().proc_addr(),
                    actor_id = %cx.self_addr(),
                    "host is shut down, sending transport handles to bootstrap for draining"
                );
                let handles = host.take_shutdown_handles();
                if let Err(handles) = tx.send(handles) {
                    drop(handles);
                }
            }
            HostAgentState::Detached(HostAgentMode::Local(mut host))
            | HostAgentState::Attached(HostAgentMode::Local(mut host)) => {
                let procs = [host.local_proc().clone(), host.system_proc().clone()];
                let shutdown_client = Proc::global().client("local_host_shutdown");
                // Continue outside the system proc so the final ack can be sent
                // after that proc has stopped.
                tokio::spawn(async move {
                    for mut proc in procs {
                        let _ = proc.destroy_and_wait(timeout, "local host shutdown").await;
                    }
                    host.shutdown_servers().await;
                    for (rank, ack) in acknowledgements {
                        ack.post(&shutdown_client, rank);
                    }
                });
            }
            _ => {
                for (rank, ack) in acknowledgements {
                    ack.post(cx, rank);
                }
            }
        }
    }

    fn mark_requests_for_shutdown(&mut self) -> usize {
        let mut remaining = 0;
        for state in self.created.values_mut() {
            if let ProcCreationState::Pending { on_completion, .. } = state {
                on_completion.request_shutdown();
                remaining += 1;
            }
        }
        remaining
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Named, Serialize, Deserialize)]
pub struct ProcState {
    pub proc_id: ProcAddr,
    pub create_rank: usize,
    pub mesh_agent: ActorRef<ProcAgent>,
    pub bootstrap_command: Option<BootstrapCommand>,
    pub proc_status: Option<bootstrap::ProcStatus>,
}
wirevalue::register_type!(ProcState);

impl HostAgent {
    /// Build the `State<ProcState>` for a single proc `id` from `created`.
    /// Shared by the `GetState` and `GetHostProcStates` handlers.
    async fn proc_state(&self, id: &ResourceId) -> resource::State<ProcState> {
        match self.created.get(id) {
            Some(state) => self.proc_state_from(id, state).await,
            None => resource::State {
                id: id.clone(),
                status: resource::Status::NotExist,
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        }
    }

    /// Like [`Self::proc_state`], but for an already-borrowed `ProcCreationState`
    /// so callers iterating `self.created` avoid a second lookup for the same
    /// entry.
    async fn proc_state_from(
        &self,
        id: &ResourceId,
        state: &ProcCreationState,
    ) -> resource::State<ProcState> {
        match state {
            ProcCreationState::Pending { on_completion, .. } => resource::State {
                id: id.clone(),
                status: if on_completion.is_keep() {
                    resource::Status::Initializing
                } else {
                    resource::Status::Stopping
                },
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
            ProcCreationState::Created {
                metadata,
                proc_id,
                mesh_agent,
                ..
            } => {
                let (raw_status, proc_status, bootstrap_command) = match self.host() {
                    Some(host) => {
                        let (status, proc_status) = host.proc_status(proc_id).await;
                        (status, proc_status, host.bootstrap_command())
                    }
                    None => (resource::Status::Unknown, None, None),
                };
                let status = raw_status.clamp_min(self.min_proc_status());
                resource::State {
                    id: id.clone(),
                    status,
                    state: Some(ProcState {
                        proc_id: proc_id.clone(),
                        create_rank: metadata.rank,
                        mesh_agent: mesh_agent.clone(),
                        bootstrap_command,
                        proc_status,
                    }),
                    generation: 0,
                    timestamp: std::time::SystemTime::now(),
                }
            }
            ProcCreationState::Failed { error, .. } => resource::State {
                id: id.clone(),
                status: resource::Status::Failed(error.to_string()),
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        }
    }
}

#[async_trait]
impl Handler<resource::GetState<ProcState>> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_state: resource::GetState<ProcState>,
    ) -> anyhow::Result<()> {
        let state = self.proc_state(&get_state.id).await;
        get_state.reply.post(cx, state);
        Ok(())
    }
}

/// Query the state of a proc mesh's procs, cast to the host agents backing that
/// mesh. The caller casts ONE of these carrying the queried `region` (the mesh
/// may be sliced, so the region need not be a dense `0..n`). The cast reaches
/// every routing host, but each rank's proc lives on exactly one host, so each
/// `HostAgent` that owns at least one selected proc reports those procs in a
/// single batch. Hosts that own no selected procs do not reply.
///
/// The bound `reply` port is split by the cast tree, so replies reduce up the
/// tree (to cast actor 0) instead of every host dialing the caller directly.
/// One batched reply per owning host means the caller sees `O(hosts)` reply
/// messages instead of `O(procs)`.
///
/// `GetState<ProcState>` cannot be cast this way because it carries a
/// fully-resolved id; this message resolves ids host-side instead.
///
/// If `keepalive` is `Some`, each proc's expiry is extended (same
/// orphan-protection semantics as `KeepaliveGetState`).
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct GetHostProcStates {
    pub proc_mesh_id: ProcMeshId,
    /// The (possibly sliced) region being queried. Each host keeps the procs it
    /// owns for this mesh whose global rank lies in the region, tested with
    /// `Slice::contains` (offset/stride-aware) — so a sliced or host-offset
    /// region resolves correctly without relying on the recipient's stamped rank.
    pub region: Region,
    pub keepalive: Option<std::time::SystemTime>,
    /// Sparse overlay of the ranks this host owns for the mesh. The caller opens
    /// an accumulator port seeded with a full-region template, so per-host
    /// overlays reduce up the cast tree into the complete proc-state mesh (see
    /// `ProcMeshRef::states`).
    pub reply: hyperactor::PortRef<ValueOverlay<resource::State<ProcState>>>,
}
wirevalue::register_type!(GetHostProcStates);

#[async_trait]
impl Handler<GetHostProcStates> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: GetHostProcStates,
    ) -> anyhow::Result<()> {
        let selects = |state: &ProcCreationState| {
            state.proc_mesh_id() == Some(&message.proc_mesh_id)
                && message.region.slice().contains(state.rank())
        };

        // Bump keepalive (if requested) in a separate mutable pass, so the read
        // loop can borrow `&self` via `proc_state` (mirrors `KeepaliveGetState`).
        if let Some(expires_after) = message.keepalive {
            for state in self.created.values_mut() {
                if selects(state) {
                    state.set_expiry_time(expires_after);
                }
            }
        }

        // Build a sparse overlay of just the ranks this host owns for the mesh,
        // keyed by each proc's *base index within the queried region* — not its
        // absolute rank. The caller's `ValueMesh` addresses cells by base rank,
        // so on a sliced region (e.g. gpus 2..4 → ranks {2,3,6,7}) the absolute
        // rank would land in the wrong cell or out of bounds. The caller's
        // accumulator merges these per-host overlays into the full proc-state
        // mesh; a host owning no selected procs simply posts nothing.
        let mut runs = Vec::new();
        for (id, state) in self.created.iter() {
            if selects(state) {
                let base = message.region.slice().index(state.rank())?;
                runs.push((base..(base + 1), self.proc_state_from(id, state).await));
            }
        }

        if !runs.is_empty() {
            // Runs are single-rank at distinct ranks; sort so the overlay's
            // sorted/non-overlapping normalization invariant holds.
            runs.sort_by_key(|(range, _)| range.start);
            message.reply.post(cx, ValueOverlay::try_from_runs(runs)?);
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<crate::proc_agent::SelfCheck> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        _: crate::proc_agent::SelfCheck,
    ) -> anyhow::Result<()> {
        // Walk procs and tear down any whose owner-supplied keepalive has
        // lapsed. Mirrors the proc-agent reaper but at host scope: we
        // address the same problem (a controller/client died abruptly)
        // for proc-level cleanup so the host doesn't leak children.
        let Some(duration) = hyperactor_config::global::get(crate::proc_agent::MESH_ORPHAN_TIMEOUT)
        else {
            return Ok(());
        };
        let now = std::time::SystemTime::now();
        let timeout = hyperactor_config::global::get(hyperactor::config::PROCESS_EXIT_TIMEOUT);

        let expired: Vec<ResourceId> = self
            .created
            .iter()
            .filter_map(|(id, state)| {
                let ProcCreationState::Created { expiry_time, .. } = state else {
                    return None;
                };
                let expiry = *expiry_time.as_ref()?;
                if now > expiry { Some(id.clone()) } else { None }
            })
            .collect();

        if !expired.is_empty() {
            tracing::info!(
                "stopping {} orphaned procs past their keepalive expiry",
                expired.len(),
            );
        }

        for id in expired {
            if let Some(ProcCreationState::Created { proc_id, .. }) = self.created.get(&id) {
                let proc_id = proc_id.clone();
                if let Some(host) = self.host() {
                    host.request_stop(cx, &proc_id, timeout, "orphaned").await;
                }
                // Don't reap repeatedly while teardown is in flight.
                if let Some(ProcCreationState::Created { expiry_time, .. }) =
                    self.created.get_mut(&id)
                {
                    *expiry_time = None;
                }
            }
        }

        cx.post_after(cx, crate::proc_agent::SelfCheck::default(), duration);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::List> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, list: resource::List) -> anyhow::Result<()> {
        list.reply.post(cx, self.created.keys().cloned().collect());
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::KeepaliveGetState<ProcState>> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: resource::KeepaliveGetState<ProcState>,
    ) -> anyhow::Result<()> {
        // Record the new expiry so the periodic SelfCheck reaper knows the
        // owner is still alive. If the owner stops extending the keepalive
        // (e.g. its process dies abruptly), the proc will be reaped past
        // `expires_after`.
        if let Some(state) = self.created.get_mut(&message.get_state.id) {
            state.set_expiry_time(message.expires_after);
        }
        <Self as Handler<resource::GetState<ProcState>>>::handle(self, cx, message.get_state).await
    }
}

#[async_trait]
impl Handler<resource::StreamState<ProcState>> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        stream_state: resource::StreamState<ProcState>,
    ) -> anyhow::Result<()> {
        // One cast delivers a single StreamState per host agent. Stream a state
        // for each proc this host owns that belongs to the subscribing mesh.
        // TODO: register `subscriber` for ongoing updates.
        let mut headers = Flattrs::new();
        headers.set(crate::proc_agent::STREAM_STATE_SUBSCRIBER, true);

        for (id, proc) in self.created.iter() {
            // Skip procs that don't belong to the subscribing proc mesh.
            if proc
                .proc_mesh_id()
                .is_none_or(|mesh| mesh.resource_id() != &stream_state.id)
            {
                continue;
            }

            let state = self.proc_state_from(id, proc).await;

            stream_state.subscriber.post_with_headers(
                cx,
                headers.clone(),
                resource::RankedState {
                    rank: resource::Rank::new(proc.rank()),
                    state,
                },
            );
        }
        Ok(())
    }
}

/// Push client configuration overrides to this host agent's process.
///
/// The attrs are installed as `Source::ClientOverride` (lowest explicit
/// priority), so the host's own env vars and file config take precedence.
/// This message is idempotent — sending the same attrs twice replaces
/// the layer wholesale.
///
/// Request-reply: the reply acts as a barrier confirming the config
/// is installed. The fatal-on-failure / best-effort policy is the
/// caller's contract, not this message's; for the canonical
/// attach-time contract see the HM-* invariants in `host_mesh.rs`.
#[derive(
    Debug,
    Clone,
    Named,
    Handler,
    RefClient,
    HandleClient,
    Serialize,
    Deserialize
)]
pub struct SetClientConfig {
    pub attrs: Attrs,
    /// This host's ordinal within the config-push cast region, stamped by the
    /// cast layer. Used to position this host's install ack overlay.
    pub rank: resource::Rank,
    /// Streaming install ack. Each host posts a single-rank overlay at its
    /// ordinal once it has installed the config; the caller reduces these into
    /// a `StatusMesh` barrier and can name exactly which hosts (if any) never
    /// acknowledged (HM-4). `StatusMesh` is used here only as a per-rank
    /// presence/ack barrier — the status value itself is not meaningful (see
    /// the handler).
    pub reply: PortRef<crate::StatusOverlay>,
}
wirevalue::register_type!(SetClientConfig);

#[async_trait]
impl Handler<SetClientConfig> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: SetClientConfig) -> anyhow::Result<()> {
        let rank = msg.rank.0.expect("rank should be stamped before delivery");
        // Use `set` (not `create_or_merge`) because `push_config` always
        // sends a complete `propagatable_attrs()` snapshot. Replacing the
        // layer wholesale is intentional and idempotent.
        hyperactor_config::global::set(
            hyperactor_config::global::Source::ClientOverride,
            msg.attrs,
        );
        tracing::debug!("installed client config override on host agent");
        // Ack as a single-rank overlay at this host's ordinal. `StatusMesh` is
        // reused here purely as a per-rank presence/ack barrier, not as a
        // lifecycle signal: there is no "config installed" status, so we pick
        // `Running` only because the barrier just needs any value distinct from
        // the `NotExist` seed to mark this host as having acknowledged. A
        // purpose-built `ValueMesh<2-state>` would model this more honestly;
        // this reuses the already-registered `StatusMesh` reducer instead.
        let installed_overlay = crate::StatusOverlay::try_from_runs(vec![(
            rank..(rank + 1),
            resource::Status::Running,
        )])
        .expect("valid single-run overlay");

        msg.reply.post(cx, installed_overlay);

        Ok(())
    }
}

/// Boot the ProcAgent on the host's local proc (LP-1).
///
/// The local proc starts empty; this message activates it by spawning
/// a `ProcAgent` (once, via `OnceLock`). Called by
/// `monarch_hyperactor::bootstrap_host` when setting up the Python
/// `this_proc()` singleton.
///
/// See also: `crate::host::LOCAL_PROC_NAME`.
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct GetLocalProc {
    #[reply]
    pub proc_mesh_agent: PortHandle<ActorHandle<ProcAgent>>,
}

#[async_trait]
impl Handler<GetLocalProc> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetLocalProc { proc_mesh_agent }: GetLocalProc,
    ) -> anyhow::Result<()> {
        let host = self
            .host()
            .ok_or_else(|| anyhow::anyhow!("HostAgent has already shut down"))?;
        let agent = self
            .local_mesh_agent
            .get_or_init(|| ProcAgent::boot_v1(host.local_proc().clone(), None));

        match agent {
            Err(e) => anyhow::bail!("error booting local proc: {}", e),
            Ok(agent) => proc_mesh_agent.post(cx, agent.clone()),
        };

        Ok(())
    }
}

#[async_trait]
impl Handler<PySpyDump> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: PySpyDump,
    ) -> Result<(), anyhow::Error> {
        PySpyWorker::spawn_and_forward(cx, message.opts, message.result)
    }
}

#[async_trait]
impl Handler<PySpyProfile> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: PySpyProfile,
    ) -> Result<(), anyhow::Error> {
        PySpyProfileWorker::spawn_and_forward(cx, message.request, message.result)
    }
}

#[async_trait]
impl Handler<ConfigDump> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: ConfigDump,
    ) -> Result<(), anyhow::Error> {
        let entries = hyperactor_config::global::config_entries();
        message.result.post(cx, ConfigDumpResult { entries });
        Ok(())
    }
}

#[cfg(all(test, fbcode_build))]
mod tests {
    use std::assert_matches;
    use std::sync::Arc;

    use hyperactor::ActorAddr;
    use hyperactor::Client;
    use hyperactor::PortReceiver;
    use hyperactor::Proc;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::id::Label;
    use hyperactor::id::Uid;
    use timed_test::async_timed_test;

    use super::*;
    use crate::bootstrap::ProcStatus;
    use crate::host::LocalProcManager;
    use crate::mesh_id::ResourceId;
    use crate::resource::CreateOrUpdateClient;
    use crate::resource::GetRankStatusClient;
    use crate::resource::GetStateClient;
    use crate::resource::Rank;
    use crate::resource::WaitRankStatusClient;

    /// Assert `overlay` holds exactly one run covering `rank..rank+1`. The
    /// reply must land at the message rank the caller requested, not at any
    /// rank the proc was created under.
    fn assert_overlay_at_rank(overlay: &crate::StatusOverlay, rank: usize) {
        assert_eq!(overlay.len(), 1, "expected exactly one run: {overlay:?}");
        let (range, _) = overlay.runs().next().unwrap();
        assert_eq!(*range, rank..rank + 1, "overlay positioned at wrong rank");
    }

    /// Like [`assert_overlay_at_rank`], but also require the single run to carry
    /// a `Failed` status.
    fn assert_overlay_failed_at_rank(overlay: &crate::StatusOverlay, rank: usize) {
        assert_eq!(overlay.len(), 1, "expected exactly one run: {overlay:?}");
        let (range, status) = overlay.runs().next().unwrap();
        assert_eq!(*range, rank..rank + 1, "overlay positioned at wrong rank");
        assert_matches!(status, resource::Status::Failed(_));
    }

    async fn spawn_local_host_agent(spawn: ProcManagerSpawnFn) -> ActorHandle<HostAgent> {
        let host = Host::new(LocalProcManager::new(spawn), ChannelTransport::Unix.any())
            .await
            .expect("local host should start");

        let host_agent = host
            .system_proc()
            .clone()
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_local(host),
            )
            .expect("host agent should start");

        HostAgent::wait_initialized(&host_agent)
            .await
            .expect("host agent should initialize");

        host_agent
    }

    #[derive(Debug)]
    struct DrainBlockingActor {
        stop_started: Arc<tokio::sync::Notify>,
        release_stop: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Actor for DrainBlockingActor {
        async fn handle_stop(
            &mut self,
            this: &Instance<Self>,
            mode: hyperactor::actor::StopMode,
            reason: &str,
        ) -> anyhow::Result<()> {
            let this = this.clone_for_py();
            let release_stop = Arc::clone(&self.release_stop);
            let reason = reason.to_string();
            this.close();
            self.stop_started.notify_one();
            tokio::spawn(async move {
                release_stop.notified().await;
                match mode {
                    hyperactor::actor::StopMode::Stop => this.exit(&reason).unwrap(),
                    hyperactor::actor::StopMode::DrainAndStop => {
                        this.exit_after_drain(&reason).unwrap()
                    }
                }
            });
            Ok(())
        }
    }

    /// Spawn a Proc, wait for it to be running, request a drain
    async fn start_blocked_full_drain(
        client_name: &str,
    ) -> (
        ActorHandle<HostAgent>,
        Client,
        Arc<tokio::sync::Notify>,
        PortReceiver<crate::StatusOverlay>,
    ) {
        let stop_started = Arc::new(tokio::sync::Notify::new());
        let release_stop = Arc::new(tokio::sync::Notify::new());

        let spawn: ProcManagerSpawnFn = {
            let stop_started_for_spawn = Arc::clone(&stop_started);
            let release_stop_for_spawn = Arc::clone(&release_stop);

            Box::new(move |proc: Proc| {
                proc.spawn(DrainBlockingActor {
                    stop_started: Arc::clone(&stop_started_for_spawn),
                    release_stop: Arc::clone(&release_stop_for_spawn),
                });
                Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None)))
            })
        };
        let host_agent = spawn_local_host_agent(spawn).await;

        let client = Proc::direct(ChannelTransport::Unix.any(), client_name.to_string())
            .unwrap()
            .client("client");
        let proc_id = ResourceId::instance(Label::new("proc").unwrap());
        let (running_reply, mut running_rx) = client.open_port::<crate::StatusOverlay>();

        host_agent
            .wait_rank_status(
                &client,
                proc_id.clone(),
                resource::Rank::new(0),
                resource::Status::Running,
                running_reply.bind(),
            )
            .await
            .expect("running waiter should be accepted");

        host_agent
            .create_or_update(
                &client,
                proc_id,
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .expect("proc should start");

        let running_status = running_rx
            .recv()
            .await
            .expect("proc should report running before drain");

        assert_overlay_at_rank(&running_status, 0);
        assert_eq!(
            running_status.runs().next().unwrap().1,
            resource::Status::Running,
        );

        let (drain_reply, drain_rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .try_post(
                &client,
                DrainHost {
                    timeout: Duration::from_secs(20),
                    max_in_flight: 1,
                    host_mesh_id: None,
                    rank: resource::Rank::new(0),
                    reply: drain_reply.bind(),
                },
            )
            .expect("drain request should be delivered");
        stop_started.notified().await;

        (host_agent, client, release_stop, drain_rx)
    }

    // RSP-* coverage map for the HostAgent rank-status tests. Each deliberately
    // uses `message_rank != creation_rank`, so a handler that read the creation
    // rank would fail:
    // - RSP-1 (position at the message rank, not creation rank):
    //   `test_wait_rank_status_already_running` (immediate WaitRankStatus and
    //   GetRankStatus on a known proc), `test_wait_rank_status_stop`,
    //   `test_wait_rank_status_before_proc_exists`, `test_spawn_procs_many_per_host`;
    //   `test_rank_status_created_error` covers the created-error branch (Failed
    //   overlay at the message rank).
    // - RSP-2 (direct delivery supplies the rank explicitly): every test above
    //   calls the generated client with `Rank::new(...)`; the cast half
    //   (`Rank::default()` late-bound in transit) is covered by the library
    //   regression in `proc_mesh.rs`.
    // - RSP-3 (deferred / pre-create waiter retains its registration rank):
    //   `test_wait_rank_status_stop` (deferred until stop),
    //   `test_wait_rank_status_before_proc_exists` (registered before the proc
    //   exists, then the proc is created at a different rank; the waiter keeps its
    //   own rank rather than adopting the creation rank), and
    //   `test_rank_status_created_error` (registered before a failed creation).
    // - RSP-4 (absence yields an empty overlay): the unknown-proc GetRankStatus
    //   assertion in `test_wait_rank_status_already_running`.

    #[tokio::test]
    async fn test_basic() {
        let bootstrap_command = BootstrapCommand::test();
        let expected_bootstrap_command = bootstrap_command.clone();
        let host = Host::new(
            BootstrapProcManager::new(bootstrap_command).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let id = ResourceId::instance(Label::new("proc1").unwrap());

        // First, create the proc, then query its state:

        host_agent
            .create_or_update(
                &client,
                id.clone(),
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .unwrap();
        // The host advertises spawned procs with a
        // `Via(proc_uid, Addr(host_addr))` location so its gateway can
        // peel and forward to the child's serving address. Construct
        // the expected proc_addr the same way.
        let expected_location =
            hyperactor::Location::from(host_addr.clone()).with_via(id.uid().clone());
        let expected_proc_addr = ProcAddr::new(id.proc_id(), expected_location);
        assert_matches!(
            host_agent.get_state(&client, id.clone()).await.unwrap(),
            resource::State {
                id: resource_id,
                status: resource::Status::Running,
                state: Some(ProcState {
                    // The proc itself should be direct addressed, with its name directly.
                    proc_id,
                    // The mesh agent should run in the same proc, under the name
                    // "proc_agent".
                    mesh_agent,
                    bootstrap_command: actual_bootstrap_command,
                    proc_status: Some(ProcStatus::Ready { started_at: _, addr: _, agent: proc_status_mesh_agent}),
                    ..
                }),
                ..
            } if id == resource_id
              && proc_id == expected_proc_addr
              && mesh_agent == ActorRef::attest(expected_proc_addr.actor_addr(crate::proc_agent::PROC_AGENT_ACTOR_NAME))
              && actual_bootstrap_command == Some(expected_bootstrap_command)
              && mesh_agent == proc_status_mesh_agent
        );
    }

    #[tokio::test]
    async fn shutdown_local_host_stops_system_actors_before_ack() {
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let host = Host::new(LocalProcManager::new(spawn), ChannelTransport::Unix.any())
            .await
            .unwrap();
        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_local(host),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();
        let cast_actor = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::strip(hyperactor_cast::cast_actor::CAST_ACTOR_NAME)),
                hyperactor_cast::cast_actor::CastActor::default(),
            )
            .unwrap();

        let client_proc = Proc::direct(
            ChannelTransport::Unix.any(),
            "shutdown_test_client".to_string(),
        )
        .unwrap();
        let client = client_proc.client("shutdown_test");
        let (ack, mut ack_rx) = client.open_port::<usize>();
        host_agent
            .try_post(
                &client,
                ShutdownHost {
                    timeout: Duration::from_secs(5),
                    max_in_flight: 1,
                    rank: Rank::new(0),
                    ack: ack.bind().unsplit(),
                },
            )
            .unwrap();

        assert_eq!(ack_rx.recv().await.unwrap(), 0);
        assert!(
            host_agent.status().borrow().is_terminal(),
            "local HostAgent must stop before acknowledging shutdown"
        );
        assert!(
            cast_actor.status().borrow().is_terminal(),
            "local CastActor must stop before acknowledging shutdown"
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn shutdown_waits_for_active_drain_worker() {
        let (host_agent, client, release_stop, mut drain_rx) =
            start_blocked_full_drain("shutdown_during_drain_client").await;

        let (ack, mut ack_rx) = client.open_port::<usize>();
        host_agent
            .try_post(
                &client,
                ShutdownHost {
                    timeout: Duration::from_secs(5),
                    max_in_flight: 1,
                    rank: resource::Rank::new(0),
                    ack: ack.bind().unsplit(),
                },
            )
            .expect("shutdown request should be delivered");

        let drain_status = drain_rx
            .recv()
            .await
            .expect("shutdown should supersede the active drain");
        assert_overlay_failed_at_rank(&drain_status, 0);
        assert_eq!(
            ack_rx
                .try_recv()
                .expect("shutdown ack port should remain open"),
            None,
            "shutdown was acknowledged before DrainComplete",
        );

        release_stop.notify_one();
        assert_eq!(
            ack_rx
                .recv()
                .await
                .expect("shutdown should be acknowledged"),
            0,
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn drain_during_pending_shutdown_waits_for_completion() {
        let (host_agent, client, release_stop, mut active_drain_rx) =
            start_blocked_full_drain("drain_during_shutdown_client").await;

        let (ack, mut ack_rx) = client.open_port::<usize>();
        host_agent
            .try_post(
                &client,
                ShutdownHost {
                    timeout: Duration::from_secs(5),
                    max_in_flight: 1,
                    rank: resource::Rank::new(0),
                    ack: ack.bind().unsplit(),
                },
            )
            .expect("shutdown request should be delivered");

        let active_drain_status = active_drain_rx
            .recv()
            .await
            .expect("shutdown should supersede the active drain");
        assert_overlay_failed_at_rank(&active_drain_status, 0);

        let (drain_reply, mut drain_rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .try_post(
                &client,
                DrainHost {
                    timeout: Duration::from_secs(20),
                    max_in_flight: 1,
                    host_mesh_id: None,
                    rank: resource::Rank::new(0),
                    reply: drain_reply.bind(),
                },
            )
            .expect("drain request should be delivered during shutdown");
        assert_eq!(
            drain_rx
                .try_recv()
                .expect("drain reply port should remain open"),
            None,
            "drain completed before shutdown finished terminating procs",
        );

        release_stop.notify_one();
        let drain_status = drain_rx
            .recv()
            .await
            .expect("drain during shutdown should complete");
        assert_overlay_at_rank(&drain_status, 0);
        assert_eq!(
            drain_status.runs().next().unwrap().1,
            resource::Status::Stopped,
        );
        assert_eq!(
            ack_rx
                .recv()
                .await
                .expect("shutdown should be acknowledged"),
            0,
        );
    }

    /// Immediate status queries on one host fixture: WaitRankStatus and
    /// GetRankStatus on a running proc reply immediately, each positioning the
    /// overlay at the message rank (not the proc's creation rank); GetRankStatus
    /// for an unknown proc replies with an empty overlay. These share a single
    /// created proc to avoid standing up extra host/proc fixtures under parallel
    /// test execution.
    #[tokio::test]
    async fn test_wait_rank_status_already_running() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let id = ResourceId::instance(Label::new("proc1").unwrap());
        // Create at rank 0, but query at different message ranks so a handler that
        // read the creation rank instead of the message rank would fail.
        host_agent
            .create_or_update(
                &client,
                id.clone(),
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .unwrap();

        // Proc is Running; WaitRankStatus for Running replies immediately, at its
        // own message rank.
        let wait_rank = 7;
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .wait_rank_status(
                &client,
                id.clone(),
                resource::Rank::new(wait_rank),
                resource::Status::Running,
                port.bind(),
            )
            .await
            .unwrap();
        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out")
            .expect("reply channel closed");
        assert_overlay_at_rank(&overlay, wait_rank);

        // GetRankStatus for the same known proc positions at its own message rank.
        let get_rank = 4;
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .get_rank_status(&client, id, resource::Rank::new(get_rank), port.bind())
            .await
            .unwrap();
        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out")
            .expect("reply channel closed");
        assert_overlay_at_rank(&overlay, get_rank);

        // GetRankStatus for an unknown proc replies with an empty overlay,
        // regardless of the message rank.
        let unknown = ResourceId::instance(Label::new("never-created").unwrap());
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .get_rank_status(&client, unknown, resource::Rank::new(4), port.bind())
            .await
            .unwrap();
        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out")
            .expect("reply channel closed");
        assert!(
            overlay.is_empty(),
            "expected empty overlay for unknown proc: {overlay:?}",
        );
    }

    /// WaitRankStatus for Stopped, then stop the proc — reply should
    /// arrive only after the proc actually stops.
    #[tokio::test]
    async fn test_wait_rank_status_stop() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let id = ResourceId::instance(Label::new("proc1").unwrap());
        // Create at rank 0, defer at a different message rank; the deferred
        // waiter must retain the message rank through to its flush on stop.
        let message_rank = 5;
        host_agent
            .create_or_update(
                &client,
                id.clone(),
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .unwrap();

        // Wait for Stopped — should not reply yet.
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .wait_rank_status(
                &client,
                id.clone(),
                resource::Rank::new(message_rank),
                resource::Status::Stopped,
                port.bind(),
            )
            .await
            .unwrap();

        // Stop the proc.
        crate::resource::StopClient::stop(&host_agent, &client, id, "test".to_string())
            .await
            .unwrap();

        // Now the reply should arrive.
        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out — proc did not reach Stopped")
            .expect("reply channel closed");
        assert_overlay_at_rank(&overlay, message_rank);
    }

    /// WaitRankStatus sent before the proc is created — the waiter is stashed
    /// and replied to once CreateOrUpdate runs. The waiter's message rank is
    /// chosen before creation and survives unchanged even though the proc is
    /// later created at a different rank: the reply is positioned at the waiter's
    /// own rank, never the creation rank.
    #[tokio::test]
    async fn test_wait_rank_status_before_proc_exists() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let id = ResourceId::instance(Label::new("proc1").unwrap());
        // Waiter rank is chosen now, before the proc exists.
        let message_rank = 9;

        // Wait for Running on a proc that doesn't exist yet.
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .wait_rank_status(
                &client,
                id.clone(),
                resource::Rank::new(message_rank),
                resource::Status::Running,
                port.bind(),
            )
            .await
            .unwrap();

        // Now create the proc at a different rank — the stashed waiter is
        // flushed once the proc is Running, keeping its own message rank.
        host_agent
            .create_or_update(&client, id, resource::Rank::new(0), ProcSpec::default())
            .await
            .unwrap();

        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out — waiter was not flushed after CreateOrUpdate")
            .expect("reply channel closed");
        assert_overlay_at_rank(&overlay, message_rank);
    }

    /// A failed creation flushes a waiter registered before the creation attempt,
    /// and subsequent status queries report the cached failure immediately. Each
    /// reply is positioned at the rank carried by its request.
    #[tokio::test]
    async fn test_rank_status_created_error() {
        // A local, in-process host whose proc spawn deterministically fails, so
        // CreateOrUpdate caches `created: Err(..)` for the requested proc.
        let spawn: ProcManagerSpawnFn = Box::new(|_proc| {
            Box::pin(std::future::ready(Err::<ActorHandle<ProcAgent>, _>(
                anyhow::anyhow!("test failure"),
            )))
        });
        let host_agent = spawn_local_host_agent(spawn).await;

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let id = ResourceId::instance(Label::new("proc1").unwrap());
        // Register a waiter before the proc exists. It must be retained through
        // the failed creation attempt and resolved with the cached failure.
        let deferred_rank = 8;
        let (port, mut deferred_rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .wait_rank_status(
                &client,
                id.clone(),
                resource::Rank::new(deferred_rank),
                resource::Status::Running,
                port.bind(),
            )
            .await
            .unwrap();

        // Creation is attempted at rank 0; the spawn fails and is cached. The
        // client call still returns Ok because status queries carry the failure.
        host_agent
            .create_or_update(
                &client,
                id.clone(),
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .unwrap();

        let overlay = tokio::time::timeout(Duration::from_secs(30), deferred_rx.recv())
            .await
            .expect("pre-create waiter was not resolved after creation failed")
            .expect("reply channel closed");
        assert_overlay_failed_at_rank(&overlay, deferred_rank);

        // WaitRankStatus for a known-but-failed proc replies immediately (no
        // deferral) with a Failed overlay at the message rank.
        let wait_rank = 6;
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .wait_rank_status(
                &client,
                id.clone(),
                resource::Rank::new(wait_rank),
                resource::Status::Running,
                port.bind(),
            )
            .await
            .unwrap();
        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out")
            .expect("reply channel closed");
        assert_overlay_failed_at_rank(&overlay, wait_rank);

        // GetRankStatus for the same failed proc reports Failed at its own
        // message rank.
        let get_rank = 3;
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .get_rank_status(&client, id, resource::Rank::new(get_rank), port.bind())
            .await
            .unwrap();
        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out")
            .expect("reply channel closed");
        assert_overlay_failed_at_rank(&overlay, get_rank);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn duplicate_selective_drains_each_receive_completion() {
        let (spawn_started_tx, mut spawn_started_rx) = tokio::sync::watch::channel(false);
        let (release_spawn_tx, release_spawn_rx) = tokio::sync::watch::channel(false);
        let spawn: ProcManagerSpawnFn = Box::new(move |proc| {
            let spawn_started_tx = spawn_started_tx.clone();
            let mut release_spawn_rx = release_spawn_rx.clone();
            Box::pin(async move {
                spawn_started_tx
                    .send(true)
                    .expect("spawn-start receiver remains open");
                release_spawn_rx
                    .wait_for(|release| *release)
                    .await
                    .expect("spawn-release sender remains open");
                ProcAgent::boot_v1(proc, None)
            })
        });
        let host_agent = spawn_local_host_agent(spawn).await;

        let client_proc =
            Proc::direct(ChannelTransport::Unix.any(), "drain_client".to_string()).unwrap();
        let client = client_proc.client("drain_client");
        let host_mesh_id = HostMeshId::instance(Label::new("mesh-a").unwrap());
        let proc_id = ResourceId::instance(Label::new("proc-a").unwrap());
        let agent_ref: ActorRef<HostAgent> = host_agent.bind();

        agent_ref.post(
            &client,
            resource::CreateOrUpdate {
                id: proc_id,
                rank: resource::Rank::new(0),
                spec: ProcSpec {
                    host_mesh_id: Some(host_mesh_id.clone()),
                    ..Default::default()
                },
            },
        );
        spawn_started_rx
            .wait_for(|started| *started)
            .await
            .expect("spawn-start sender remains open");

        let (first_reply, mut first_reply_rx) = client.open_port::<crate::StatusOverlay>();
        let (second_reply, mut second_reply_rx) = client.open_port::<crate::StatusOverlay>();
        agent_ref.post(
            &client,
            DrainHost {
                timeout: Duration::from_secs(5),
                max_in_flight: 16,
                host_mesh_id: Some(host_mesh_id.clone()),
                rank: resource::Rank::new(3),
                reply: first_reply.bind(),
            },
        );
        agent_ref.post(
            &client,
            DrainHost {
                timeout: Duration::from_secs(5),
                max_in_flight: 16,
                host_mesh_id: Some(host_mesh_id),
                rank: resource::Rank::new(7),
                reply: second_reply.bind(),
            },
        );

        release_spawn_tx
            .send(true)
            .expect("spawn-release receivers remain open");

        let first = first_reply_rx.recv().await.expect("first drain reply");
        assert_overlay_at_rank(&first, 3);
        assert_eq!(
            first.runs().next().unwrap().1,
            resource::Status::Stopped,
            "first drain should complete successfully",
        );
        let second = second_reply_rx.recv().await.expect("second drain reply");
        assert_overlay_at_rank(&second, 7);
        assert_eq!(
            second.runs().next().unwrap().1,
            resource::Status::Stopped,
            "second drain should complete successfully",
        );
    }

    /// DrainHost with a host_mesh_id filter only stops procs
    /// belonging to that mesh; procs from other meshes are unaffected.
    #[tokio::test]
    async fn test_drain_scoped_to_host_mesh_id() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let mesh_a = HostMeshId::instance(Label::new("mesh-a").unwrap());
        let mesh_b = HostMeshId::instance(Label::new("mesh-b").unwrap());
        let proc_a_id = ResourceId::instance(Label::new("proc-a").unwrap());
        let proc_b_id = ResourceId::instance(Label::new("proc-b").unwrap());

        // Create proc_a belonging to mesh_a.
        let spec_a = ProcSpec {
            host_mesh_id: Some(mesh_a.clone()),
            ..Default::default()
        };
        host_agent
            .create_or_update(&client, proc_a_id.clone(), resource::Rank::new(0), spec_a)
            .await
            .unwrap();

        // Create proc_b belonging to mesh_b.
        let spec_b = ProcSpec {
            host_mesh_id: Some(mesh_b.clone()),
            ..Default::default()
        };
        host_agent
            .create_or_update(&client, proc_b_id.clone(), resource::Rank::new(1), spec_b)
            .await
            .unwrap();

        // Both should be Running.
        assert_matches!(
            host_agent
                .get_state(&client, proc_a_id.clone())
                .await
                .unwrap(),
            resource::State {
                status: resource::Status::Running,
                ..
            }
        );
        assert_matches!(
            host_agent
                .get_state(&client, proc_b_id.clone())
                .await
                .unwrap(),
            resource::State {
                status: resource::Status::Running,
                ..
            }
        );

        // Drain only mesh_a.
        let (drain_reply, mut drain_rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .drain_host(
                &client,
                Duration::from_secs(5),
                16,
                Some(mesh_a.clone()),
                resource::Rank::new(0),
                drain_reply.bind(),
            )
            .await
            .unwrap();
        // Wait for the host to report drained before asserting.
        drain_rx.recv().await.unwrap();

        // proc_a should be gone (removed from created).
        assert_matches!(
            host_agent
                .get_state(&client, proc_a_id.clone())
                .await
                .unwrap(),
            resource::State {
                status: resource::Status::NotExist,
                ..
            }
        );

        // proc_b should still be Running.
        assert_matches!(
            host_agent
                .get_state(&client, proc_b_id.clone())
                .await
                .unwrap(),
            resource::State {
                status: resource::Status::Running,
                ..
            }
        );
    }

    /// DrainHost with host_mesh_id=None drains all procs regardless
    /// of their mesh affiliation (backwards compatibility).
    #[tokio::test]
    async fn test_drain_none_drains_all() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let mesh_a = HostMeshId::instance(Label::new("mesh-a").unwrap());
        let mesh_b = HostMeshId::instance(Label::new("mesh-b").unwrap());
        let proc_a_id = ResourceId::instance(Label::new("proc-a").unwrap());
        let proc_b_id = ResourceId::instance(Label::new("proc-b").unwrap());

        let spec_a = ProcSpec {
            host_mesh_id: Some(mesh_a),
            ..Default::default()
        };
        host_agent
            .create_or_update(&client, proc_a_id.clone(), resource::Rank::new(0), spec_a)
            .await
            .unwrap();

        let spec_b = ProcSpec {
            host_mesh_id: Some(mesh_b),
            ..Default::default()
        };
        host_agent
            .create_or_update(&client, proc_b_id.clone(), resource::Rank::new(1), spec_b)
            .await
            .unwrap();

        // Drain all (no filter).
        let (drain_reply, mut drain_rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .drain_host(
                &client,
                Duration::from_secs(5),
                16,
                None,
                resource::Rank::new(0),
                drain_reply.bind(),
            )
            .await
            .unwrap();
        // Wait for the host to report drained before asserting.
        drain_rx.recv().await.unwrap();

        // Both should be gone.
        assert_matches!(
            host_agent.get_state(&client, proc_a_id).await.unwrap(),
            resource::State {
                status: resource::Status::NotExist,
                ..
            }
        );
        assert_matches!(
            host_agent.get_state(&client, proc_b_id).await.unwrap(),
            resource::State {
                status: resource::Status::NotExist,
                ..
            }
        );
    }

    // PD-6/PD-8 regression: QueryChild(Proc) on the service proc
    // returns non-zero queue stats after the host_agent has handled
    // messages. Guards against the bug where the HostAgent closure
    // defaulted queue stats to zero because it predated Proc-level
    // queue accessors.
    #[tokio::test]
    async fn test_service_proc_query_child_has_queue_stats() {
        use hyperactor::introspect::IntrospectMessage;
        use hyperactor::introspect::IntrospectResult;

        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new_process(host, None),
            )
            .unwrap();
        HostAgent::wait_initialized(&host_agent).await.unwrap();

        let client_proc =
            Proc::direct(ChannelTransport::Unix.any(), "qd_client".to_string()).unwrap();
        let client = client_proc.client("client");

        // Spawn a proc so the host_agent processes at least one
        // CreateOrUpdate message, which goes through the work queue.
        let name = ResourceId::instance(Label::new("qd_test_proc").unwrap());
        host_agent
            .create_or_update(
                &client,
                name.clone(),
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .unwrap();

        // The host_agent has now processed messages on the service
        // proc. Query the service proc's introspection.
        let agent_ref = system_proc
            .proc_addr()
            .actor_addr(HOST_MESH_AGENT_ACTOR_NAME);
        let agent_id: ActorAddr = agent_ref;
        let port = agent_id.introspect_port();

        // Poll until we see non-zero watermark (evidence of queue
        // traffic since startup).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let (reply_port, reply_rx) = client.open_once_port::<IntrospectResult>();
            port.post(
                &client,
                IntrospectMessage::QueryChild {
                    child_ref: Addr::Proc(system_proc.proc_addr().clone()),
                    reply: reply_port.bind(),
                },
            );
            let payload = tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx.recv())
                .await
                .expect("QueryChild timed out")
                .expect("reply channel closed");

            let attrs: hyperactor_config::Attrs =
                serde_json::from_str(&payload.attrs).expect("valid attrs JSON");

            let hwm = attrs
                .get(crate::introspect::ACTOR_WORK_QUEUE_DEPTH_HIGH_WATER_MARK)
                .copied()
                .unwrap_or(0);
            let last_nonzero: Option<u64> = attrs
                .get(crate::introspect::LAST_NONZERO_QUEUE_DEPTH_AGE_MS)
                .copied()
                .flatten();

            if hwm > 0 {
                // The service proc's watermark should reflect
                // the messages the host_agent processed.
                assert!(
                    last_nonzero.is_some(),
                    "last-nonzero should be Some when watermark is {hwm}",
                );
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for service proc watermark > 0",
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// A single `SpawnProcs` message at host rank 0 fans out into
    /// `num_per_host` procs, each created under the id derived from
    /// `proc_name(&proc_mesh_id, rank)`. All of them should come up Running.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_procs_many_per_host() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn_with_uid(
                Uid::singleton(Label::new(HOST_MESH_AGENT_ACTOR_NAME).unwrap()),
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let client = client_proc.client("client");

        let proc_mesh_id = ProcMeshId::singleton(Label::new("spawn-many").unwrap());
        let num_per_host = 4;
        let (spawn_status_reply, mut spawn_status_rx) = client.open_port::<crate::StatusOverlay>();

        // Send the command and wait messages in the same order used by
        // HostMesh::spawn.
        let agent_ref: ActorRef<HostAgent> = host_agent.bind();
        agent_ref.post(
            &client,
            SpawnProcs {
                rank: resource::Rank::new(0),
                proc_mesh_id: proc_mesh_id.clone(),
                num_per_host,
                client_config_override: Attrs::new(),
                host_mesh_id: None,
                default_bootstrap_command: None,
                proc_bind: None,
                bootstrap_commands: None,
            },
        );
        agent_ref.post(
            &client,
            WaitProcs {
                rank: resource::Rank::new(0),
                proc_mesh_id: proc_mesh_id.clone(),
                num_per_host,
                status_reply: spawn_status_reply.bind(),
            },
        );

        let mut reported = vec![false; num_per_host];
        for _ in 0..num_per_host {
            let overlay = spawn_status_rx
                .recv()
                .await
                .expect("WaitProcs status reply channel closed");
            assert_eq!(overlay.len(), 1, "expected one proc status per reply");
            let (range, status) = overlay.runs().next().expect("status overlay has a run");
            assert_eq!(range.end, range.start + 1);
            assert_eq!(status, &resource::Status::Running);
            reported[range.start] = true;
        }
        assert!(reported.into_iter().all(|reported| reported));
    }
}
