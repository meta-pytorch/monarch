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
use std::fmt;
use std::pin::Pin;
use std::sync::OnceLock;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortHandle;
use hyperactor::Proc;
use hyperactor::RefClient;
use hyperactor::channel::ChannelTransport;
use hyperactor::context;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::LOCAL_PROC_NAME;
use hyperactor::host::LocalProcManager;
use hyperactor::host::SERVICE_PROC_NAME;
use hyperactor::host::SingleTerminate;
use hyperactor::mailbox::MailboxServerHandle;
use hyperactor::reference as hyperactor_reference;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::Attrs;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use typeuri::Named;

use crate::Name;
use crate::bootstrap;
use crate::bootstrap::BootstrapCommand;
use crate::bootstrap::BootstrapProcConfig;
use crate::bootstrap::BootstrapProcManager;
use crate::config_dump::ConfigDump;
use crate::config_dump::ConfigDumpResult;
use crate::proc_agent::ProcAgent;
use crate::pyspy::PySpyDump;
use crate::pyspy::PySpyProfile;
use crate::pyspy::PySpyProfileWorker;
use crate::pyspy::PySpyWorker;
use crate::resource;
use crate::resource::ProcSpec;

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
pub enum HostAgentMode {
    Process {
        host: Host<BootstrapProcManager>,
        /// If set, the ShutdownHost handler sends the frontend mailbox server
        /// handle back to the bootstrap loop via this channel once shutdown is
        /// complete, so the caller can drain it and exit.
        shutdown_tx: Option<tokio::sync::oneshot::Sender<MailboxServerHandle>>,
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
        proc: &hyperactor_reference::ProcId,
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
        proc_id: &hyperactor_reference::ProcId,
    ) -> (resource::Status, Option<bootstrap::ProcStatus>) {
        match self {
            HostAgentMode::Process { host, .. } => match host.manager().status(proc_id).await {
                Some(proc_status) => (proc_status.clone().into(), Some(proc_status)),
                None => (resource::Status::Unknown, None),
            },
            HostAgentMode::Local(host) => {
                let status = match host.manager().local_proc_status(proc_id).await {
                    Some(hyperactor::host::LocalProcStatus::Stopping) => resource::Status::Stopping,
                    Some(hyperactor::host::LocalProcStatus::Stopped) => resource::Status::Stopped,
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

#[derive(Debug)]
pub(crate) struct ProcCreationState {
    pub(crate) rank: usize,
    pub(crate) host_mesh_name: Option<crate::Name>,
    pub(crate) created: Result<
        (
            hyperactor_reference::ProcId,
            hyperactor_reference::ActorRef<ProcAgent>,
        ),
        HostError,
    >,
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
    name: Name,
}

/// Sent by DrainWorker back to HostAgent when draining completes.
/// Not exported — delivered locally via PortHandle (no serialization).
struct DrainComplete {
    host: HostAgentMode,
    ack: hyperactor_reference::PortRef<()>,
}

/// Child actor whose only job is to run `host.terminate_children()` in
/// its `init()`, return the host and ack to the parent via DrainComplete,
/// and exit. Runs on the same proc as the host agent so it gets its
/// own `Instance` (required by `terminate_children`).
#[hyperactor::export(handlers = [])]
struct DrainWorker {
    host: Option<HostAgentMode>,
    timeout: Duration,
    max_in_flight: usize,
    ack: Option<hyperactor_reference::PortRef<()>>,
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

        // Bundle host + ack into DrainComplete so the parent sends the ack
        // AFTER restoring state (prevents race with ShutdownHost).
        if let (Some(host), Some(ack)) = (self.host.take(), self.ack.take()) {
            let _ = self.done_notify.send(this, DrainComplete { host, ack });
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
        resource::Stop,
        resource::GetState<ProcState>,
        resource::GetRankStatus { cast = true },
        resource::WaitRankStatus { cast = true },
        resource::List,
        ShutdownHost,
        DrainHost,
        SetClientConfig,
        ProcStatusChanged,
        PySpyDump,
        PySpyProfile,
        ConfigDump,
    ]
)]
pub struct HostAgent {
    state: HostAgentState,
    pub(crate) created: HashMap<Name, ProcCreationState>,
    /// Pending `WaitRankStatus` waiters, keyed by resource name.
    /// Each entry is `(min_status, rank, reply_port)`. Only touched
    /// from `&mut self` handlers.
    pending_proc_waiters: HashMap<
        Name,
        Vec<(
            resource::Status,
            usize,
            hyperactor_reference::PortRef<crate::StatusOverlay>,
        )>,
    >,
    /// Procs that already have an active bridge task watching their status.
    watching: HashSet<Name>,
    /// Port handle for sending `ProcStatusChanged` to self. Set in `init()`.
    proc_status_port: Option<PortHandle<ProcStatusChanged>>,
    /// Lazily initialized ProcAgent on the host's local proc.
    /// Boots on first [`GetLocalProc`] (LP-1 — see
    /// `hyperactor::host::LOCAL_PROC_NAME`).
    local_mesh_agent: OnceLock<anyhow::Result<ActorHandle<ProcAgent>>>,
    /// Handle to the host's frontend mailbox server, set during `init` after
    /// `this.bind::<Self>()` ensures the actor port is registered before the
    /// mailbox starts routing messages. Sent back to the bootstrap loop via
    /// `shutdown_tx` when the host shuts down so the caller can
    /// drain it.
    mailbox_handle: Option<MailboxServerHandle>,
}

impl HostAgent {
    /// Create a new host mesh agent running in the provided mode.
    pub fn new(host: HostAgentMode) -> Self {
        Self {
            state: HostAgentState::Detached(host),
            created: HashMap::new(),
            pending_proc_waiters: HashMap::new(),
            watching: HashSet::new(),
            proc_status_port: None,
            local_mesh_agent: OnceLock::new(),
            mailbox_handle: None,
        }
    }

    /// Minimum status floor derived from the host agent's lifecycle.
    /// Procs on this host cannot be healthier than this.
    fn min_proc_status(&self) -> resource::Status {
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
    /// Only procs whose `host_mesh_name` matches `filter` are stopped;
    /// all other procs are left running.
    async fn drain_by_mesh_name(
        &mut self,
        cx: &Context<'_, Self>,
        timeout: std::time::Duration,
        filter: Option<&crate::Name>,
    ) {
        let matching_names: Vec<crate::Name> = self
            .created
            .iter()
            .filter(|(_, state)| state.host_mesh_name.as_ref() == filter)
            .map(|(name, _)| name.clone())
            .collect();

        if let Some(host_mode) = self.host() {
            for name in &matching_names {
                if let Some(ProcCreationState {
                    created: Ok((proc_id, _)),
                    ..
                }) = self.created.get(name)
                {
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
        for name in &matching_names {
            self.created.remove(name);
            self.watching.remove(name);
            self.pending_proc_waiters.remove(name);
        }

        tracing::info!(
            count = matching_names.len(),
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
            host.system_proc().proc_id().clone(),
        ));
        children.push(hyperactor::introspect::IntrospectRef::Proc(
            host.local_proc().proc_id().clone(),
        ));

        // User procs.
        for state in self.created.values() {
            if let Ok((proc_id, _agent_ref)) = &state.created {
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
        // Serve the host now that the agent is initialized. Make sure our port is
        // bound before serving.
        this.bind::<Self>();
        match self.host_mut().unwrap() {
            HostAgentMode::Process { host, .. } => {
                self.mailbox_handle = host.serve();
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
            HostAgentMode::Local(host) => {
                host.serve();
            }
        };
        this.set_system();
        self.publish_introspect_properties(this);

        // Register callback for QueryChild — resolves system procs
        // that are not independently addressable actors.
        let host = self.host().expect("host present");
        let system_proc = host.system_proc().clone();
        let local_proc = host.local_proc().clone();
        let self_id = this.self_id().clone();
        this.set_query_child_handler(move |child_ref| {
            use hyperactor::introspect::IntrospectResult;

            let proc = match child_ref {
                hyperactor::reference::Reference::Proc(proc_id) => {
                    if *proc_id == *system_proc.proc_id() {
                        Some((&system_proc, SERVICE_PROC_NAME))
                    } else if *proc_id == *local_proc.proc_id() {
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
                    // clones keys — microseconds per shard. The
                    // is_system check uses individual point lookups
                    // outside the iteration. Stale keys (terminal
                    // actors) may appear but are harmless — the TUI
                    // handles "not found" gracefully.
                    let all_keys = proc.all_instance_keys();
                    let mut actors: Vec<hyperactor::introspect::IntrospectRef> =
                        Vec::with_capacity(all_keys.len());
                    let mut system_actors: Vec<crate::introspect::NodeRef> = Vec::new();
                    for id in all_keys {
                        if proc.get_instance(&id).is_some_and(|cell| cell.is_system()) {
                            system_actors.push(crate::introspect::NodeRef::Actor(id.clone()));
                        }
                        actors.push(hyperactor::introspect::IntrospectRef::Actor(id));
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
                        if let Some(cell) = proc.get_instance(&aid) {
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
                            proc.proc_id().clone(),
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
                        hyperactor::reference::Reference::Proc(id) => {
                            hyperactor::introspect::IntrospectRef::Proc(id.clone())
                        }
                        hyperactor::reference::Reference::Actor(id) => {
                            hyperactor::introspect::IntrospectRef::Actor(id.clone())
                        }
                        hyperactor::reference::Reference::Port(id) => {
                            hyperactor::introspect::IntrospectRef::Actor(id.actor_id().clone())
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

#[async_trait]
impl Handler<resource::CreateOrUpdate<ProcSpec>> for HostAgent {
    #[tracing::instrument("HostAgent::CreateOrUpdate", level = "info", skip_all, fields(name=%create_or_update.name))]
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<ProcSpec>,
    ) -> anyhow::Result<()> {
        if self.created.contains_key(&create_or_update.name) {
            // Already created: there is no update.
            return Ok(());
        }

        let host = match self.host_mut() {
            Some(h) => h,
            None => {
                tracing::warn!(
                    name = %create_or_update.name,
                    "ignoring CreateOrUpdate: HostAgent has already shut down"
                );
                return Ok(());
            }
        };
        let created = match host {
            HostAgentMode::Process { host, .. } => {
                host.spawn(
                    create_or_update.name.clone().to_string(),
                    BootstrapProcConfig {
                        create_rank: create_or_update.rank.unwrap(),
                        client_config_override: create_or_update
                            .spec
                            .client_config_override
                            .clone(),
                        proc_bind: create_or_update.spec.proc_bind.clone(),
                        bootstrap_command: create_or_update.spec.bootstrap_command.clone(),
                    },
                )
                .await
            }
            HostAgentMode::Local(host) => {
                host.spawn(create_or_update.name.clone().to_string(), ())
                    .await
            }
        };

        let rank = create_or_update.rank.unwrap();

        if let Err(e) = &created {
            tracing::error!("failed to spawn proc {}: {}", create_or_update.name, e);
        }
        let was_empty = self.created.is_empty();
        self.created.insert(
            create_or_update.name.clone(),
            ProcCreationState {
                rank,
                host_mesh_name: create_or_update.spec.host_mesh_name.clone(),
                created,
            },
        );

        // Transition Detached → Attached on first proc creation.
        if was_empty {
            if let HostAgentState::Detached(_) = &self.state {
                let host = match std::mem::replace(&mut self.state, HostAgentState::Shutdown) {
                    HostAgentState::Detached(h) => h,
                    _ => unreachable!(),
                };
                self.state = HostAgentState::Attached(host);
            }
        }

        // If any WaitRankStatus messages arrived before this proc
        // existed, their waiters were stashed with a sentinel rank.
        // Now that we know the real rank, fix them up and start a
        // watch bridge.
        // Extract the proc_id before mutably borrowing pending_proc_waiters.
        let proc_id = self
            .created
            .get(&create_or_update.name)
            .and_then(|s| s.created.as_ref().ok())
            .map(|(pid, _)| pid.clone());

        if let Some(waiters) = self.pending_proc_waiters.get_mut(&create_or_update.name) {
            for (_, waiter_rank, _) in waiters.iter_mut() {
                if *waiter_rank == usize::MAX {
                    *waiter_rank = rank;
                }
            }
        }

        // Start a bridge and send ourselves an initial check.
        if self
            .pending_proc_waiters
            .contains_key(&create_or_update.name)
        {
            if let Some(proc_id) = &proc_id {
                self.start_watch_bridge(&create_or_update.name, proc_id)
                    .await;
            }
            self.notify_proc_status_changed(&create_or_update.name);
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
            proc_name = %message.name,
            reason = %message.reason,
            "stopping proc"
        );
        let host = match self.host() {
            Some(h) => h,
            None => {
                // Host already shut down; all procs are terminated.
                tracing::debug!(
                    proc_name = %message.name,
                    "ignoring Stop: HostAgent has already shut down"
                );
                return Ok(());
            }
        };
        let timeout = hyperactor_config::global::get(hyperactor::config::PROCESS_EXIT_TIMEOUT);

        if let Some(ProcCreationState {
            created: Ok((proc_id, _)),
            ..
        }) = self.created.get(&message.name)
        {
            host.request_stop(cx, proc_id, timeout, &message.reason)
                .await;
        }

        // Status may have changed to Stopping; notify pending waiters.
        self.notify_proc_status_changed(&message.name);

        self.publish_introspect_properties(cx);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::GetRankStatus> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_rank_status: resource::GetRankStatus,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;
        use crate::resource::Status;

        let (rank, status) = match self.created.get(&get_rank_status.name) {
            Some(ProcCreationState {
                rank,
                created: Ok((proc_id, _mesh_agent)),
                ..
            }) => {
                let raw_status = match self.host() {
                    Some(host) => host.proc_status(proc_id).await.0,
                    None => resource::Status::Unknown,
                };
                (*rank, raw_status.clamp_min(self.min_proc_status()))
            }
            Some(ProcCreationState {
                rank,
                created: Err(e),
                ..
            }) => (*rank, Status::Failed(e.to_string())),
            None => (usize::MAX, Status::NotExist),
        };

        let overlay = if rank == usize::MAX {
            StatusOverlay::new()
        } else {
            StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                .expect("valid single-run overlay")
        };
        let result = get_rank_status.reply.send(cx, overlay);
        // Ignore errors, because returning Err from here would cause the HostAgent
        // to be stopped, which would take down the entire host. This only means
        // some actor that requested the rank status failed to receive it.
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
impl Handler<resource::WaitRankStatus> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: resource::WaitRankStatus,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;
        use crate::resource::Status;

        match self.created.get(&msg.name) {
            Some(ProcCreationState {
                rank,
                created: Ok((proc_id, _)),
                ..
            }) => {
                let rank = *rank;
                let status = match self.host() {
                    Some(host) => host.proc_status(proc_id).await.0,
                    None => Status::Stopped,
                };

                // If already at or past the requested threshold, reply immediately.
                if status >= msg.min_status {
                    let overlay = StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
                        .expect("valid single-run overlay");
                    let _ = msg.reply.send(cx, overlay);
                    return Ok(());
                }

                // Stash the waiter and start a bridge if we don't have one yet.
                self.pending_proc_waiters
                    .entry(msg.name.clone())
                    .or_default()
                    .push((msg.min_status, rank, msg.reply));

                let proc_id = proc_id.clone();
                self.start_watch_bridge(&msg.name, &proc_id).await;
            }
            Some(ProcCreationState {
                rank,
                created: Err(e),
                ..
            }) => {
                // Creation failed — reply immediately with Failed status.
                let overlay = StatusOverlay::try_from_runs(vec![(
                    *rank..(*rank + 1),
                    Status::Failed(e.to_string()),
                )])
                .expect("valid single-run overlay");
                let _ = msg.reply.send(cx, overlay);
            }
            None => {
                // Proc doesn't exist yet. Stash the waiter with a
                // sentinel rank; CreateOrUpdate will fill it in and
                // start the watch bridge.
                self.pending_proc_waiters
                    .entry(msg.name.clone())
                    .or_default()
                    .push((msg.min_status, usize::MAX, msg.reply));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<ProcStatusChanged> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: ProcStatusChanged) -> anyhow::Result<()> {
        use crate::StatusOverlay;
        use crate::resource::Status;

        let status = match self.created.get(&msg.name) {
            Some(ProcCreationState {
                created: Ok((proc_id, _)),
                ..
            }) => match self.host() {
                Some(host) => host.proc_status(proc_id).await.0,
                None => Status::Stopped,
            },
            Some(ProcCreationState {
                created: Err(_), ..
            }) => {
                // Already replied with Failed when they were stashed.
                return Ok(());
            }
            None => {
                // Proc not created yet, nothing to flush.
                return Ok(());
            }
        };

        let Some(waiters) = self.pending_proc_waiters.get_mut(&msg.name) else {
            return Ok(());
        };

        let remaining = std::mem::take(waiters);
        for (min_status, rank, reply) in remaining {
            if status >= min_status {
                let overlay =
                    StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status.clone())])
                        .expect("valid single-run overlay");
                let _ = reply.send(cx, overlay);
            } else {
                waiters.push((min_status, rank, reply));
            }
        }

        if waiters.is_empty() {
            self.pending_proc_waiters.remove(&msg.name);
        }

        Ok(())
    }
}

impl HostAgent {
    /// Send a `ProcStatusChanged` self-notification for the given proc name.
    fn notify_proc_status_changed(&self, name: &Name) {
        if let Some(port) = &self.proc_status_port {
            let client = Instance::<()>::self_client();
            let _ = port.send(client, ProcStatusChanged { name: name.clone() });
        }
    }

    /// Start a bridge task that watches a proc's status channel and sends
    /// `ProcStatusChanged` to self on each change. At most one bridge per proc.
    async fn start_watch_bridge(&mut self, name: &Name, proc_id: &hyperactor_reference::ProcId) {
        if self.watching.contains(name) {
            return;
        }
        self.watching.insert(name.clone());

        let port = match &self.proc_status_port {
            Some(p) => p.clone(),
            None => return,
        };

        match self.host() {
            Some(HostAgentMode::Process { host, .. }) => {
                if let Some(rx) = host.manager().watch(proc_id).await {
                    start_proc_watch(port, rx, name.clone(), |s| s.clone().into());
                }
            }
            Some(HostAgentMode::Local(host)) => {
                if let Some(rx) = host.manager().watch(proc_id).await {
                    start_proc_watch(port, rx, name.clone(), |s| (*s).into());
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
    name: Name,
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
                    let _ = port.send(client, ProcStatusChanged { name: name.clone() });
                    if terminated {
                        return;
                    }
                }
                Err(_) => {
                    let _ = port.send(client, ProcStatusChanged { name: name.clone() });
                    return;
                }
            }
        }
    });
}

#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient, HandleClient)]
pub struct ShutdownHost {
    /// Grace window: send SIGTERM and wait this long before
    /// escalating.
    pub timeout: std::time::Duration,
    /// Max number of children to terminate concurrently on this host.
    pub max_in_flight: usize,
    /// Ack that the agent finished shutdown work (best-effort).
    #[reply]
    pub ack: hyperactor::reference::PortRef<()>,
}
wirevalue::register_type!(ShutdownHost);

/// Drain user procs on this host but keep the host, service proc,
/// and networking alive. Used during mesh stop/shutdown so that
/// forwarder flushes can still reach remote hosts.
///
/// If `host_mesh_name` is `Some`, only procs belonging to that mesh
/// are stopped (selective drain). If `None`, all procs are
/// terminated (full drain).
#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient, HandleClient)]
pub struct DrainHost {
    pub timeout: std::time::Duration,
    pub max_in_flight: usize,
    pub host_mesh_name: Option<crate::Name>,
    #[reply]
    pub ack: hyperactor::reference::PortRef<()>,
}
wirevalue::register_type!(DrainHost);

#[async_trait]
impl Handler<DrainHost> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: DrainHost) -> anyhow::Result<()> {
        if msg.host_mesh_name.is_some() {
            // Selective drain: stop only procs belonging to the named mesh.
            self.drain_by_mesh_name(cx, msg.timeout, msg.host_mesh_name.as_ref())
                .await;
            msg.ack.send(cx, ())?;
            return Ok(());
        }

        // Full drain: terminate all children.
        let host = match std::mem::replace(&mut self.state, HostAgentState::Draining) {
            HostAgentState::Attached(h) => h,
            other @ (HostAgentState::Detached(_) | HostAgentState::Draining) => {
                // Nothing to drain — ack immediately.
                self.state = other;
                msg.ack.send(cx, ())?;
                return Ok(());
            }
            HostAgentState::Shutdown => {
                self.state = HostAgentState::Shutdown;
                msg.ack.send(cx, ())?;
                return Ok(());
            }
        };

        // Do NOT clear `self.created` here: the DrainWorker
        // terminates procs asynchronously, and concurrent GetState /
        // GetRankStatus queries must still find the entries. With the
        // host in Draining state (`self.host()` returns None), those
        // handlers already report Status::Stopped for every known
        // proc, which is the correct answer while draining is
        // in progress.

        let done_port = cx.port::<DrainComplete>();

        cx.spawn_with_name(
            "drain_worker",
            DrainWorker {
                host: Some(host),
                timeout: msg.timeout,
                max_in_flight: msg.max_in_flight,
                ack: Some(msg.ack),
                done_notify: done_port,
            },
        )?;

        Ok(())
    }
}

#[async_trait]
impl Handler<DrainComplete> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: DrainComplete) -> anyhow::Result<()> {
        self.state = HostAgentState::Detached(msg.host);
        self.created.clear();
        msg.ack.send(cx, ())?;
        Ok(())
    }
}

#[async_trait]
impl Handler<ShutdownHost> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: ShutdownHost) -> anyhow::Result<()> {
        // Terminate children BEFORE acking, so the caller's networking
        // stays alive while children flush their forwarders during
        // teardown. If we ack first, the caller proceeds to tear down
        // the host proc's networking while children are still running,
        // causing their forwarder flushes to hang until
        // MESSAGE_DELIVERY_TIMEOUT expires.
        if !self.created.is_empty() {
            self.drain(cx, msg.timeout, msg.max_in_flight).await;
        }

        // Ack after children are terminated so the caller does not
        // tear down the host's networking prematurely.
        msg.ack.send(cx, ())?;

        // Drop the host and signal the bootstrap loop to drain the
        // mailbox and exit.
        match std::mem::replace(&mut self.state, HostAgentState::Shutdown) {
            HostAgentState::Detached(HostAgentMode::Process {
                shutdown_tx: Some(tx),
                ..
            })
            | HostAgentState::Attached(HostAgentMode::Process {
                shutdown_tx: Some(tx),
                ..
            }) => {
                tracing::info!(
                    proc_id = %cx.self_id().proc_id(),
                    actor_id = %cx.self_id(),
                    "host is shut down, sending mailbox handle to bootstrap for draining"
                );
                if let Some(handle) = self.mailbox_handle.take() {
                    let _ = tx.send(handle);
                }
            }
            _ => {}
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Named, Serialize, Deserialize)]
pub struct ProcState {
    pub proc_id: hyperactor_reference::ProcId,
    pub create_rank: usize,
    pub mesh_agent: hyperactor_reference::ActorRef<ProcAgent>,
    pub bootstrap_command: Option<BootstrapCommand>,
    pub proc_status: Option<bootstrap::ProcStatus>,
}
wirevalue::register_type!(ProcState);

#[async_trait]
impl Handler<resource::GetState<ProcState>> for HostAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_state: resource::GetState<ProcState>,
    ) -> anyhow::Result<()> {
        let state = match self.created.get(&get_state.name) {
            Some(ProcCreationState {
                rank,
                created: Ok((proc_id, mesh_agent)),
                ..
            }) => {
                let (raw_status, proc_status, bootstrap_command) = match self.host() {
                    Some(host) => {
                        let (status, proc_status) = host.proc_status(proc_id).await;
                        (status, proc_status, host.bootstrap_command())
                    }
                    None => (resource::Status::Unknown, None, None),
                };
                let status = raw_status.clamp_min(self.min_proc_status());
                resource::State {
                    name: get_state.name.clone(),
                    status,
                    state: Some(ProcState {
                        proc_id: proc_id.clone(),
                        create_rank: *rank,
                        mesh_agent: mesh_agent.clone(),
                        bootstrap_command,
                        proc_status,
                    }),
                    generation: 0,
                    timestamp: std::time::SystemTime::now(),
                }
            }
            Some(ProcCreationState {
                created: Err(e), ..
            }) => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::Failed(e.to_string()),
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
            None => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::NotExist,
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        };

        let result = get_state.reply.send(cx, state);
        // Ignore errors, because returning Err from here would cause the HostAgent
        // to be stopped, which would take down the entire host. This only means
        // some actor that requested the state of a proc failed to receive it.
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
impl Handler<resource::List> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, list: resource::List) -> anyhow::Result<()> {
        list.reply
            .send(cx, self.created.keys().cloned().collect())?;
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
/// is installed. The caller should await with a timeout and treat
/// timeout as best-effort (log warning, continue).
#[derive(Debug, Named, Handler, RefClient, HandleClient, Serialize, Deserialize)]
pub struct SetClientConfig {
    pub attrs: Attrs,
    #[reply]
    pub done: hyperactor_reference::PortRef<()>,
}
wirevalue::register_type!(SetClientConfig);

#[async_trait]
impl Handler<SetClientConfig> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: SetClientConfig) -> anyhow::Result<()> {
        // Use `set` (not `create_or_merge`) because `push_config` always
        // sends a complete `propagatable_attrs()` snapshot. Replacing the
        // layer wholesale is intentional and idempotent.
        hyperactor_config::global::set(
            hyperactor_config::global::Source::ClientOverride,
            msg.attrs,
        );
        tracing::debug!("installed client config override on host agent");
        msg.done.send(cx, ())?;
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
/// See also: `hyperactor::host::LOCAL_PROC_NAME`.
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
            Ok(agent) => proc_mesh_agent.send(cx, agent.clone())?,
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
        // Reply is best-effort: the caller may have timed out and dropped
        // the once-port.  That must not crash this actor.
        if let Err(e) = message.result.send(cx, ConfigDumpResult { entries }) {
            tracing::warn!("HostAgent: ConfigDump reply undeliverable (caller timed out): {e}",);
        }
        Ok(())
    }
}

/// A trampoline actor that spawns a [`Host`], and sends a reference to the
/// corresponding [`HostAgent`] to the provided reply port.
///
/// This is used to bootstrap host meshes from proc meshes.
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers=[GetHostMeshAgent]
)]
pub(crate) struct HostMeshAgentProcMeshTrampoline {
    host_mesh_agent: ActorHandle<HostAgent>,
    reply_port: hyperactor_reference::PortRef<hyperactor_reference::ActorRef<HostAgent>>,
}

#[async_trait]
impl Actor for HostMeshAgentProcMeshTrampoline {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.reply_port.send(this, self.host_mesh_agent.bind())?;
        Ok(())
    }
}

#[async_trait]
impl hyperactor::RemoteSpawn for HostMeshAgentProcMeshTrampoline {
    type Params = (
        ChannelTransport,
        hyperactor_reference::PortRef<hyperactor_reference::ActorRef<HostAgent>>,
        Option<BootstrapCommand>,
        bool, /* local? */
    );

    async fn new(
        (transport, reply_port, command, local): Self::Params,
        _environment: Flattrs,
    ) -> anyhow::Result<Self> {
        let host = if local {
            let spawn: ProcManagerSpawnFn =
                Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
            let manager = LocalProcManager::new(spawn);
            let host = Host::new(manager, transport.any()).await?;
            HostAgentMode::Local(host)
        } else {
            let command = match command {
                Some(command) => command,
                None => BootstrapCommand::current()?,
            };
            tracing::info!("booting host with proc command {:?}", command);
            let manager = BootstrapProcManager::new(command).unwrap();
            let host = Host::new(manager, transport.any()).await?;
            HostAgentMode::Process {
                host,
                shutdown_tx: None,
            }
        };

        let system_proc = host.system_proc().clone();
        let host_mesh_agent =
            system_proc.spawn(HOST_MESH_AGENT_ACTOR_NAME, HostAgent::new(host))?;

        Ok(Self {
            host_mesh_agent,
            reply_port,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient)]
pub struct GetHostMeshAgent {
    #[reply]
    pub host_mesh_agent: hyperactor_reference::PortRef<hyperactor_reference::ActorRef<HostAgent>>,
}
wirevalue::register_type!(GetHostMeshAgent);

#[async_trait]
impl Handler<GetHostMeshAgent> for HostMeshAgentProcMeshTrampoline {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_host_mesh_agent: GetHostMeshAgent,
    ) -> anyhow::Result<()> {
        get_host_mesh_agent
            .host_mesh_agent
            .send(cx, self.host_mesh_agent.bind())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use hyperactor::Proc;
    use hyperactor::channel::ChannelTransport;

    use super::*;
    use crate::bootstrap::ProcStatus;
    use crate::resource::CreateOrUpdateClient;
    use crate::resource::GetStateClient;
    use crate::resource::StopClient;
    use crate::resource::WaitRankStatusClient;

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_basic() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let name = Name::new("proc1").unwrap();

        // First, create the proc, then query its state:

        host_agent
            .create_or_update(
                &client,
                name.clone(),
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .unwrap();
        assert_matches!(
            host_agent.get_state(&client, name.clone()).await.unwrap(),
            resource::State {
                name: resource_name,
                status: resource::Status::Running,
                state: Some(ProcState {
                    // The proc itself should be direct addressed, with its name directly.
                    proc_id,
                    // The mesh agent should run in the same proc, under the name
                    // "proc_agent".
                    mesh_agent,
                    bootstrap_command,
                    proc_status: Some(ProcStatus::Ready { started_at: _, addr: _, agent: proc_status_mesh_agent}),
                    ..
                }),
                ..
            } if name == resource_name
              && proc_id == hyperactor_reference::ProcId::with_name(host_addr.clone(), name.to_string())
              && mesh_agent == hyperactor_reference::ActorRef::attest(hyperactor_reference::ProcId::with_name(host_addr.clone(), name.to_string()).actor_id(crate::proc_agent::PROC_AGENT_ACTOR_NAME, 0)) && bootstrap_command == Some(BootstrapCommand::test())
              && mesh_agent == proc_status_mesh_agent
        );
    }

    /// WaitRankStatus on a running proc replies immediately with Running.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_wait_rank_status_already_running() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let name = Name::new("proc1").unwrap();
        host_agent
            .create_or_update(
                &client,
                name.clone(),
                resource::Rank::new(0),
                ProcSpec::default(),
            )
            .await
            .unwrap();

        // Proc is Running; wait for Running should reply immediately.
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .wait_rank_status(&client, name, resource::Status::Running, port.bind())
            .await
            .unwrap();

        let overlay = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("reply timed out")
            .expect("reply channel closed");
        assert!(!overlay.is_empty(), "expected non-empty overlay");
    }

    /// WaitRankStatus for Stopped, then stop the proc — reply should
    /// arrive only after the proc actually stops.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_wait_rank_status_stop() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let name = Name::new("proc1").unwrap();
        host_agent
            .create_or_update(
                &client,
                name.clone(),
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
                name.clone(),
                resource::Status::Stopped,
                port.bind(),
            )
            .await
            .unwrap();

        // Stop the proc.
        host_agent
            .stop(&client, name, "test".to_string())
            .await
            .unwrap();

        // Now the reply should arrive.
        let overlay = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("reply timed out — proc did not reach Stopped")
            .expect("reply channel closed");
        assert!(!overlay.is_empty(), "expected non-empty overlay");
    }

    /// WaitRankStatus sent before the proc is created — the waiter is
    /// stashed and replied to once CreateOrUpdate runs.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_wait_rank_status_before_proc_exists() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let name = Name::new("proc1").unwrap();

        // Wait for Running on a proc that doesn't exist yet.
        let (port, mut rx) = client.open_port::<crate::StatusOverlay>();
        host_agent
            .wait_rank_status(
                &client,
                name.clone(),
                resource::Status::Running,
                port.bind(),
            )
            .await
            .unwrap();

        // Now create the proc — the stashed waiter should get its
        // sentinel rank fixed and be flushed once the proc is Running.
        host_agent
            .create_or_update(&client, name, resource::Rank::new(0), ProcSpec::default())
            .await
            .unwrap();

        let overlay = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("reply timed out — waiter was not flushed after CreateOrUpdate")
            .expect("reply channel closed");
        assert!(!overlay.is_empty(), "expected non-empty overlay");
    }

    /// DrainHost with a host_mesh_name filter only stops procs
    /// belonging to that mesh; procs from other meshes are unaffected.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_drain_scoped_to_host_mesh_name() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let mesh_a = crate::Name::new("mesh_a").unwrap();
        let mesh_b = crate::Name::new("mesh_b").unwrap();
        let proc_a = crate::Name::new("proc_a").unwrap();
        let proc_b = crate::Name::new("proc_b").unwrap();

        // Create proc_a belonging to mesh_a.
        let mut spec_a = ProcSpec::default();
        spec_a.host_mesh_name = Some(mesh_a.clone());
        host_agent
            .create_or_update(&client, proc_a.clone(), resource::Rank::new(0), spec_a)
            .await
            .unwrap();

        // Create proc_b belonging to mesh_b.
        let mut spec_b = ProcSpec::default();
        spec_b.host_mesh_name = Some(mesh_b.clone());
        host_agent
            .create_or_update(&client, proc_b.clone(), resource::Rank::new(1), spec_b)
            .await
            .unwrap();

        // Both should be Running.
        assert_matches!(
            host_agent.get_state(&client, proc_a.clone()).await.unwrap(),
            resource::State {
                status: resource::Status::Running,
                ..
            }
        );
        assert_matches!(
            host_agent.get_state(&client, proc_b.clone()).await.unwrap(),
            resource::State {
                status: resource::Status::Running,
                ..
            }
        );

        // Drain only mesh_a.
        host_agent
            .drain_host(&client, Duration::from_secs(5), 16, Some(mesh_a.clone()))
            .await
            .unwrap();

        // proc_a should be gone (removed from created).
        assert_matches!(
            host_agent.get_state(&client, proc_a.clone()).await.unwrap(),
            resource::State {
                status: resource::Status::NotExist,
                ..
            }
        );

        // proc_b should still be Running.
        assert_matches!(
            host_agent.get_state(&client, proc_b.clone()).await.unwrap(),
            resource::State {
                status: resource::Status::Running,
                ..
            }
        );
    }

    /// DrainHost with host_mesh_name=None drains all procs regardless
    /// of their mesh affiliation (backwards compatibility).
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_drain_none_drains_all() {
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let mesh_a = crate::Name::new("mesh_a").unwrap();
        let mesh_b = crate::Name::new("mesh_b").unwrap();
        let proc_a = crate::Name::new("proc_a").unwrap();
        let proc_b = crate::Name::new("proc_b").unwrap();

        let mut spec_a = ProcSpec::default();
        spec_a.host_mesh_name = Some(mesh_a);
        host_agent
            .create_or_update(&client, proc_a.clone(), resource::Rank::new(0), spec_a)
            .await
            .unwrap();

        let mut spec_b = ProcSpec::default();
        spec_b.host_mesh_name = Some(mesh_b);
        host_agent
            .create_or_update(&client, proc_b.clone(), resource::Rank::new(1), spec_b)
            .await
            .unwrap();

        // Drain all (no filter).
        host_agent
            .drain_host(&client, Duration::from_secs(5), 16, None)
            .await
            .unwrap();

        // Both should be gone.
        assert_matches!(
            host_agent.get_state(&client, proc_a).await.unwrap(),
            resource::State {
                status: resource::Status::NotExist,
                ..
            }
        );
        assert_matches!(
            host_agent.get_state(&client, proc_b).await.unwrap(),
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
    #[cfg(fbcode_build)]
    async fn test_service_proc_query_child_has_queue_stats() {
        use hyperactor::actor::ActorStatus;
        use hyperactor::introspect::IntrospectMessage;
        use hyperactor::introspect::IntrospectResult;
        use hyperactor::reference as hyperactor_reference;

        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Process {
                    host,
                    shutdown_tx: None,
                }),
            )
            .unwrap();

        // Wait for HostAgent to finish init.
        host_agent
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        let client_proc =
            Proc::direct(ChannelTransport::Unix.any(), "qd_client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Spawn a proc so the host_agent processes at least one
        // CreateOrUpdate message, which goes through the work queue.
        let name = Name::new("qd_test_proc").unwrap();
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
        let agent_id = system_proc
            .proc_id()
            .actor_id(HOST_MESH_AGENT_ACTOR_NAME, 0);
        let port =
            hyperactor_reference::PortRef::<IntrospectMessage>::attest_message_port(&agent_id);

        // Poll until we see non-zero watermark (evidence of queue
        // traffic since startup).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let (reply_port, reply_rx) = client.open_once_port::<IntrospectResult>();
            port.send(
                &client,
                IntrospectMessage::QueryChild {
                    child_ref: hyperactor_reference::Reference::Proc(system_proc.proc_id().clone()),
                    reply: reply_port.bind(),
                },
            )
            .unwrap();
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
}
