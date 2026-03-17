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
use std::str::FromStr;
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
use hyperactor::context::Mailbox as _;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::LOCAL_PROC_NAME;
use hyperactor::host::LocalProcManager;
use hyperactor::host::SERVICE_PROC_NAME;
use hyperactor::mailbox::MailboxServerHandle;
use hyperactor::mailbox::PortSender as _;
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
use crate::mesh_admin::MeshAdminMessageClient;
use crate::proc_agent::ProcAgent;
use crate::resource;
use crate::resource::ProcSpec;

/// Typed host-node identifier for mesh admin navigation.
///
/// Wraps an [`ActorId`] (the `HostAgent`'s actor id) and
/// serializes with a `host:` prefix so that the admin resolver can
/// distinguish host-level references from plain actor references.
/// The same `HostAgent` `ActorId` can appear as both a host
/// (from root's children) and as an actor (from a proc's children);
/// `HostId` makes the host case unambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostId(pub hyperactor_reference::ActorId);

/// Prefix used by [`HostId`] for display/parse round-tripping.
const HOST_ID_PREFIX: &str = "host:";

impl fmt::Display for HostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{HOST_ID_PREFIX}{}", self.0)
    }
}

impl FromStr for HostId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = s
            .strip_prefix(HOST_ID_PREFIX)
            .ok_or_else(|| anyhow::anyhow!("not a host reference: {}", s))?;
        let actor_id: hyperactor_reference::ActorId = inner
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid actor id in host ref '{}': {}", s, e))?;
        Ok(HostId(actor_id))
    }
}

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

/// A mesh agent is responsible for managing a host in a [`HostMesh`],
/// through the resource behaviors defined in [`crate::resource`].
/// Self-notification sent by bridge tasks when a proc's status changes.
/// Not exported or registered — only used internally via `PortHandle`.
#[derive(Debug, Serialize, Deserialize, Named)]
struct ProcStatusChanged {
    name: Name,
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
        SpawnMeshAdmin,
        SetClientConfig,
        ProcStatusChanged,
    ]
)]
pub struct HostAgent {
    pub(crate) host: Option<HostAgentMode>,
    pub(crate) created: HashMap<Name, ProcCreationState>,
    /// Pending `WaitRankStatus` waiters, keyed by resource name.
    /// Each entry is `(min_status, rank, reply_port)`. Only touched
    /// from `&mut self` handlers.
    pending_proc_waiters:
        HashMap<Name, Vec<(resource::Status, usize, hyperactor_reference::PortRef<crate::StatusOverlay>)>>,
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
    /// `shutdown_tx` when the host shuts down so the caller can drain it.
    mailbox_handle: Option<MailboxServerHandle>,
}

impl HostAgent {
    /// Create a new host mesh agent running in the provided mode.
    pub fn new(host: HostAgentMode) -> Self {
        Self {
            host: Some(host),
            created: HashMap::new(),
            pending_proc_waiters: HashMap::new(),
            watching: HashSet::new(),
            proc_status_port: None,
            local_mesh_agent: OnceLock::new(),
            mailbox_handle: None,
        }
    }

    /// Publish the current host properties and children list for
    /// introspection. Called from init and after each state change
    /// (proc created/stopped).
    fn publish_introspect_properties(&self, cx: &Instance<Self>) {
        let host = match self.host.as_ref() {
            Some(h) => h,
            None => return, // host shut down
        };

        let addr = host.addr().to_string();
        let mut children = Vec::new();
        let system_children = Vec::new();

        // Procs are not system — only actors are. Both service and
        // local appear as regular children; 's' in the TUI toggles
        // actor visibility, not proc visibility.
        let sys_ref = host.system_proc().proc_id().to_string();
        let local_ref = host.local_proc().proc_id().to_string();
        children.push(sys_ref);
        children.push(local_ref);

        // User procs.
        for state in self.created.values() {
            if let Ok((proc_id, _agent_ref)) = &state.created {
                children.push(proc_id.to_string());
            }
        }

        let num_procs = children.len();

        let mut attrs = hyperactor_config::Attrs::new();
        attrs.set(crate::introspect::NODE_TYPE, "host".to_string());
        attrs.set(crate::introspect::ADDR, addr);
        attrs.set(crate::introspect::NUM_PROCS, num_procs);
        attrs.set(hyperactor::introspect::CHILDREN, children);
        attrs.set(crate::introspect::SYSTEM_CHILDREN, system_children);
        cx.publish_attrs(attrs);
    }
}

#[async_trait]
impl Actor for HostAgent {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Serve the host now that the agent is initialized. Make sure our port is
        // bound before serving.
        this.bind::<Self>();
        match self.host.as_mut().unwrap() {
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
        let host = self.host.as_ref().expect("host present");
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
                    let mut actors = Vec::with_capacity(all_keys.len());
                    let mut system_actors = Vec::new();
                    for id in all_keys {
                        let ref_str = id.to_string();
                        if proc.get_instance(&id).is_some_and(|cell| cell.is_system()) {
                            system_actors.push(ref_str.clone());
                        }
                        actors.push(ref_str);
                    }
                    // Build attrs for this proc node.
                    let mut attrs = hyperactor_config::Attrs::new();
                    attrs.set(crate::introspect::NODE_TYPE, "proc".to_string());
                    attrs.set(crate::introspect::PROC_NAME, label.to_string());
                    attrs.set(crate::introspect::NUM_ACTORS, actors.len());
                    attrs.set(crate::introspect::SYSTEM_CHILDREN, system_actors.clone());
                    let attrs_json =
                        serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());

                    IntrospectResult {
                        identity: proc.proc_id().to_string(),
                        attrs: attrs_json,
                        children: actors,
                        parent: Some(HostId(self_id.clone()).to_string()),
                        as_of: humantime::format_rfc3339_millis(std::time::SystemTime::now())
                            .to_string(),
                    }
                }
                None => {
                    let mut error_attrs = hyperactor_config::Attrs::new();
                    error_attrs.set(hyperactor::introspect::ERROR_CODE, "not_found".to_string());
                    error_attrs.set(
                        hyperactor::introspect::ERROR_MESSAGE,
                        format!("child {} not found", child_ref),
                    );
                    IntrospectResult {
                        identity: String::new(),
                        attrs: serde_json::to_string(&error_attrs)
                            .unwrap_or_else(|_| "{}".to_string()),
                        children: Vec::new(),
                        parent: None,
                        as_of: humantime::format_rfc3339_millis(std::time::SystemTime::now())
                            .to_string(),
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

        let host = self.host.as_mut().expect("host present");
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
        self.created.insert(
            create_or_update.name.clone(),
            ProcCreationState {
                rank,
                created,
            },
        );

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
        if self.pending_proc_waiters.contains_key(&create_or_update.name) {
            if let Some(proc_id) = &proc_id {
                self.start_watch_bridge(&create_or_update.name, proc_id).await;
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
        let host = self
            .host
            .as_ref()
            .ok_or(anyhow::anyhow!("HostAgent has already shut down"))?;
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
            }) => {
                let status = match self.host.as_ref() {
                    Some(host) => host.proc_status(proc_id).await.0,
                    None => Status::Stopped,
                };
                (*rank, status)
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
            }) => {
                let rank = *rank;
                let status = match self.host.as_ref() {
                    Some(host) => host.proc_status(proc_id).await.0,
                    None => Status::Stopped,
                };

                // If already at or past the requested threshold, reply immediately.
                if status >= msg.min_status {
                    let overlay =
                        StatusOverlay::try_from_runs(vec![(rank..(rank + 1), status)])
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
                let overlay =
                    StatusOverlay::try_from_runs(vec![(*rank..(*rank + 1), Status::Failed(e.to_string()))])
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
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: ProcStatusChanged,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;
        use crate::resource::Status;

        let status = match self.created.get(&msg.name) {
            Some(ProcCreationState {
                created: Ok((proc_id, _)),
                ..
            }) => match self.host.as_ref() {
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
    async fn start_watch_bridge(
        &mut self,
        name: &Name,
        proc_id: &hyperactor_reference::ProcId,
    ) {
        if self.watching.contains(name) {
            return;
        }
        self.watching.insert(name.clone());

        let port = match &self.proc_status_port {
            Some(p) => p.clone(),
            None => return,
        };

        match self.host.as_ref() {
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

#[async_trait]
impl Handler<ShutdownHost> for HostAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: ShutdownHost) -> anyhow::Result<()> {
        // Ack immediately so caller can stop waiting.
        let (return_handle, mut return_receiver) = cx.mailbox().open_port();
        cx.mailbox()
            .serialize_and_send(&msg.ack, (), return_handle)?;

        let mut shutdown_tx = None;
        if let Some(host_mode) = self.host.take() {
            match host_mode {
                HostAgentMode::Process {
                    host,
                    shutdown_tx: tx,
                } => {
                    let summary = host
                        .terminate_children(
                            cx,
                            msg.timeout,
                            msg.max_in_flight.clamp(1, 256),
                            "shutdown host",
                        )
                        .await;
                    tracing::info!(?summary, "terminated children on host");
                    shutdown_tx = tx;
                }
                HostAgentMode::Local(host) => {
                    let summary = host
                        .terminate_children(cx, msg.timeout, msg.max_in_flight, "shutdown host")
                        .await;
                    tracing::info!(?summary, "terminated children on local host");
                }
            }
        }

        // If message is returned, it means it ack was not sent successfully.
        if return_receiver.recv().await.is_ok() {
            tracing::warn!("failed to send ack");
        }

        // Drop the host to release any resources that somehow survived.
        let _ = self.host.take();

        if let Some(tx) = shutdown_tx {
            tracing::info!(
                proc_id = %cx.self_id().proc_id(),
                actor_id = %cx.self_id(),
                "host is shut down, sending mailbox handle to bootstrap for draining"
            );
            if let Some(handle) = self.mailbox_handle.take() {
                let _ = tx.send(handle);
            }
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
            }) => {
                let (status, proc_status, bootstrap_command) = match self.host.as_ref() {
                    Some(host) => {
                        let (status, proc_status) = host.proc_status(proc_id).await;
                        (status, proc_status, host.bootstrap_command())
                    }
                    None => (resource::Status::Stopped, None, None),
                };
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

/// Message to spawn a [`MeshAdminAgent`] on this host's system proc.
///
/// The handler spawns the admin agent, queries its HTTP address via
/// `GetAdminAddr`, and replies with the address string.
#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient, HandleClient)]
pub struct SpawnMeshAdmin {
    /// All hosts in the mesh as `(address, agent_ref)` pairs. Passed
    /// through to [`MeshAdminAgent::new`] so the admin can fan out
    /// introspection queries to every host.
    pub hosts: Vec<(String, hyperactor_reference::ActorRef<HostAgent>)>,

    /// `ActorId` of the process-global root client, exposed as a
    /// child node in the admin introspection tree. `None` if no root
    /// client is available.
    pub root_client_actor_id: Option<hyperactor_reference::ActorId>,

    /// Explicit bind address for the admin HTTP server. When `None`,
    /// the server reads `MESH_ADMIN_ADDR` from config.
    pub admin_addr: Option<std::net::SocketAddr>,

    /// Reply port for the admin HTTP address string (e.g.
    /// `"myhost.facebook.com:8080"`).
    #[reply]
    pub addr: hyperactor::reference::PortRef<String>,
}
wirevalue::register_type!(SpawnMeshAdmin);

#[async_trait]
impl Handler<SpawnMeshAdmin> for HostAgent {
    /// Spawns a [`MeshAdminAgent`] on this host's system proc, waits
    /// for its HTTP server to bind, and replies with the listen
    /// address.
    async fn handle(&mut self, cx: &Context<Self>, msg: SpawnMeshAdmin) -> anyhow::Result<()> {
        let proc = self
            .host
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("host is not available"))?
            .system_proc();

        let agent_handle = proc.spawn(
            crate::mesh_admin::MESH_ADMIN_ACTOR_NAME,
            crate::mesh_admin::MeshAdminAgent::new(
                msg.hosts,
                msg.root_client_actor_id,
                msg.admin_addr,
            ),
        )?;
        let response = agent_handle.get_admin_addr(cx).await?;
        let addr_str = response
            .addr
            .ok_or_else(|| anyhow::anyhow!("mesh admin agent did not report an address"))?;

        msg.addr.send(cx, addr_str)?;
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
        let agent = self.local_mesh_agent.get_or_init(|| {
            ProcAgent::boot_v1(self.host.as_ref().unwrap().local_proc().clone(), None)
        });

        match agent {
            Err(e) => anyhow::bail!("error booting local proc: {}", e),
            Ok(agent) => proc_mesh_agent.send(cx, agent.clone())?,
        };

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
}
