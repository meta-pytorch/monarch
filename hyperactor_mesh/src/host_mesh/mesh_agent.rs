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
use std::fmt;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::OnceLock;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortHandle;
use hyperactor::PortRef;
use hyperactor::Proc;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::channel::ChannelTransport;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use hyperactor::context::Mailbox as _;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::LocalProcManager;
use hyperactor::mailbox::PortSender as _;
use hyperactor_config::Flattrs;
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
/// Wraps an [`ActorId`] (the `HostMeshAgent`'s actor id) and
/// serializes with a `host:` prefix so that the admin resolver can
/// distinguish host-level references from plain actor references.
/// The same `HostMeshAgent` `ActorId` can appear as both a host
/// (from root's children) and as an actor (from a proc's children);
/// `HostId` makes the host case unambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostId(pub ActorId);

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
        let actor_id: ActorId = inner
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
        exit_on_shutdown: bool,
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
        proc: &ProcId,
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
        proc_id: &ProcId,
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
    pub(crate) created: Result<(ProcId, ActorRef<ProcAgent>), HostError>,
}

/// Actor name used when spawning the host mesh agent on the system proc.
pub const HOST_MESH_AGENT_ACTOR_NAME: &str = "agent";

/// A mesh agent is responsible for managing a host in a [`HostMesh`],
/// through the resource behaviors defined in [`crate::resource`].
#[hyperactor::export(
    handlers=[
        resource::CreateOrUpdate<ProcSpec>,
        resource::Stop,
        resource::GetState<ProcState>,
        resource::GetRankStatus { cast = true },
        resource::List,
        ShutdownHost,
        SpawnMeshAdmin,
    ]
)]
pub struct HostMeshAgent {
    pub(crate) host: Option<HostAgentMode>,
    pub(crate) created: HashMap<Name, ProcCreationState>,
    /// Stores the lazily initialized proc mesh agent for the local proc.
    local_mesh_agent: OnceLock<anyhow::Result<ActorHandle<ProcAgent>>>,
}

impl HostMeshAgent {
    /// Create a new host mesh agent running in the provided mode.
    pub fn new(host: HostAgentMode) -> Self {
        Self {
            host: Some(host),
            created: HashMap::new(),
            local_mesh_agent: OnceLock::new(),
        }
    }

    /// Publish the current host properties and children list for
    /// introspection. Called from init and after each state change
    /// (proc created/stopped).
    fn publish_introspect_properties(&self, cx: &Instance<Self>) {
        use hyperactor::introspect::PublishedPropertiesKind;

        let host = match self.host.as_ref() {
            Some(h) => h,
            None => return, // host shut down
        };

        let addr = host.addr().to_string();
        let mut children = Vec::new();
        let mut system_children = Vec::new();

        // System procs — plain ProcId strings, no prefix needed.
        // The admin server's resolve_proc_node handles routing via
        // QueryChild to the host agent.
        let sys_ref = host.system_proc().proc_id().to_string();
        let local_ref = host.local_proc().proc_id().to_string();
        system_children.push(sys_ref.clone());
        system_children.push(local_ref.clone());
        children.push(sys_ref);
        children.push(local_ref);

        // User procs.
        for state in self.created.values() {
            if let Ok((proc_id, _agent_ref)) = &state.created {
                children.push(proc_id.to_string());
            }
        }

        let num_procs = children.len();
        cx.publish_properties(PublishedPropertiesKind::Host {
            addr,
            num_procs,
            children,
            system_children,
        });
    }
}

#[async_trait]
impl Actor for HostMeshAgent {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Serve the host now that the agent is initialized. Make sure our port is
        // bound before serving.
        this.bind::<Self>();
        match self.host.as_mut().unwrap() {
            HostAgentMode::Process { host, .. } => {
                host.serve();
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
            use hyperactor::introspect::NodePayload;
            use hyperactor::introspect::NodeProperties;
            use hyperactor::reference::Reference;

            let proc = match child_ref {
                Reference::Proc(proc_id) => {
                    if *proc_id == *system_proc.proc_id() {
                        Some((&system_proc, "service"))
                    } else if *proc_id == *local_proc.proc_id() {
                        Some((&local_proc, "local"))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            match proc {
                Some((proc, label)) => {
                    let all_ids = proc.all_actor_ids();
                    let mut actors = Vec::with_capacity(all_ids.len());
                    let mut system_actors = Vec::new();
                    for id in all_ids {
                        let ref_str = id.to_string();
                        if proc.get_instance(&id).is_some_and(|cell| cell.is_system()) {
                            system_actors.push(ref_str.clone());
                        }
                        actors.push(ref_str);
                    }
                    NodePayload {
                        identity: proc.proc_id().to_string(),
                        properties: NodeProperties::Proc {
                            proc_name: label.to_string(),
                            num_actors: actors.len(),
                            is_system: true,
                            system_children: system_actors,
                            stopped_children: Vec::new(),
                            stopped_retention_cap: 0,
                            is_poisoned: false,
                            failed_actor_count: 0,
                        },
                        children: actors,
                        parent: Some(HostId(self_id.clone()).to_string()),
                        as_of: humantime::format_rfc3339_millis(RealClock.system_time_now())
                            .to_string(),
                    }
                }
                None => NodePayload {
                    identity: String::new(),
                    properties: NodeProperties::Error {
                        code: "not_found".into(),
                        message: format!("child {} not found", child_ref),
                    },
                    children: Vec::new(),
                    parent: None,
                    as_of: humantime::format_rfc3339_millis(RealClock.system_time_now())
                        .to_string(),
                },
            }
        });

        Ok(())
    }
}

impl fmt::Debug for HostMeshAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostMeshAgent")
            .field("host", &"..")
            .field("created", &self.created)
            .finish()
    }
}

#[async_trait]
impl Handler<resource::CreateOrUpdate<ProcSpec>> for HostMeshAgent {
    #[tracing::instrument("HostMeshAgent::CreateOrUpdate", level = "info", skip_all, fields(name=%create_or_update.name))]
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

        if let Err(e) = &created {
            tracing::error!("failed to spawn proc {}: {}", create_or_update.name, e);
        }
        self.created.insert(
            create_or_update.name.clone(),
            ProcCreationState {
                rank: create_or_update.rank.unwrap(),
                created,
            },
        );

        self.publish_introspect_properties(cx);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::Stop> for HostMeshAgent {
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
            .ok_or(anyhow::anyhow!("HostMeshAgent has already shut down"))?;
        let timeout = hyperactor_config::global::get(hyperactor::config::PROCESS_EXIT_TIMEOUT);

        if let Some(ProcCreationState {
            created: Ok((proc_id, _)),
            ..
        }) = self.created.get(&message.name)
        {
            host.request_stop(cx, proc_id, timeout, &message.reason)
                .await;
        }

        self.publish_introspect_properties(cx);
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::GetRankStatus> for HostMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_rank_status: resource::GetRankStatus,
    ) -> anyhow::Result<()> {
        use crate::StatusOverlay;
        use crate::resource::Status;

        let host = self.host.as_ref().expect("host present");
        let (rank, status) = match self.created.get(&get_rank_status.name) {
            Some(ProcCreationState {
                rank,
                created: Ok((proc_id, _mesh_agent)),
            }) => {
                let (status, _) = host.proc_status(proc_id).await;
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
        // Ignore errors, because returning Err from here would cause the HostMeshAgent
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

#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient, HandleClient)]
pub struct ShutdownHost {
    /// Grace window: send SIGTERM and wait this long before
    /// escalating.
    pub timeout: std::time::Duration,
    /// Max number of children to terminate concurrently on this host.
    pub max_in_flight: usize,
    /// Ack that the agent finished shutdown work (best-effort).
    #[reply]
    pub ack: hyperactor::PortRef<()>,
}
wirevalue::register_type!(ShutdownHost);

#[async_trait]
impl Handler<ShutdownHost> for HostMeshAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: ShutdownHost) -> anyhow::Result<()> {
        // Ack immediately so caller can stop waiting.
        let (return_handle, mut return_receiver) = cx.mailbox().open_port();
        cx.mailbox()
            .serialize_and_send(&msg.ack, (), return_handle)?;

        let mut should_exit = false;
        if let Some(host_mode) = self.host.take() {
            match host_mode {
                HostAgentMode::Process {
                    host,
                    exit_on_shutdown,
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
                    should_exit = exit_on_shutdown;
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

        if should_exit {
            tracing::info!(
                proc_id = %cx.self_id().proc_id(),
                actor_id = %cx.self_id(),
                "host is shut down, exiting this process"
            );
            std::process::exit(0);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Named, Serialize, Deserialize)]
pub struct ProcState {
    pub proc_id: ProcId,
    pub create_rank: usize,
    pub mesh_agent: ActorRef<ProcAgent>,
    pub bootstrap_command: Option<BootstrapCommand>,
    pub proc_status: Option<bootstrap::ProcStatus>,
}
wirevalue::register_type!(ProcState);

#[async_trait]
impl Handler<resource::GetState<ProcState>> for HostMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_state: resource::GetState<ProcState>,
    ) -> anyhow::Result<()> {
        let host = self.host.as_ref().expect("host present");
        let state = match self.created.get(&get_state.name) {
            Some(ProcCreationState {
                rank,
                created: Ok((proc_id, mesh_agent)),
            }) => {
                let (status, proc_status) = host.proc_status(proc_id).await;
                resource::State {
                    name: get_state.name.clone(),
                    status,
                    state: Some(ProcState {
                        proc_id: proc_id.clone(),
                        create_rank: *rank,
                        mesh_agent: mesh_agent.clone(),
                        bootstrap_command: host.bootstrap_command(),
                        proc_status,
                    }),
                }
            }
            Some(ProcCreationState {
                created: Err(e), ..
            }) => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::Failed(e.to_string()),
                state: None,
            },
            None => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::NotExist,
                state: None,
            },
        };

        let result = get_state.reply.send(cx, state);
        // Ignore errors, because returning Err from here would cause the HostMeshAgent
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
impl Handler<resource::List> for HostMeshAgent {
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
    pub hosts: Vec<(String, ActorRef<HostMeshAgent>)>,

    /// `ActorId` of the process-global root client, exposed as a
    /// child node in the admin introspection tree. `None` if no root
    /// client is available.
    pub root_client_actor_id: Option<ActorId>,

    /// Fixed port for the admin HTTP server. When `Some`, the server
    /// binds to `[::]:<port>`; when `None`, an ephemeral port is
    /// chosen.
    pub admin_port: Option<u16>,

    /// Reply port for the admin HTTP address string (e.g.
    /// `"myhost.facebook.com:8080"`).
    #[reply]
    pub addr: hyperactor::PortRef<String>,
}
wirevalue::register_type!(SpawnMeshAdmin);

#[async_trait]
impl Handler<SpawnMeshAdmin> for HostMeshAgent {
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
                msg.admin_port,
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

/// A local-only message to access the "local" proc on the host.
/// This is used to bootstrap the root mesh process client on the
/// local singleton host mesh.
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct GetLocalProc {
    #[reply]
    pub proc_mesh_agent: PortHandle<ActorHandle<ProcAgent>>,
}

#[async_trait]
impl Handler<GetLocalProc> for HostMeshAgent {
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
/// corresponding [`HostMeshAgent`] to the provided reply port.
///
/// This is used to bootstrap host meshes from proc meshes.
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers=[GetHostMeshAgent]
)]
pub(crate) struct HostMeshAgentProcMeshTrampoline {
    host_mesh_agent: ActorHandle<HostMeshAgent>,
    reply_port: PortRef<ActorRef<HostMeshAgent>>,
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
        PortRef<ActorRef<HostMeshAgent>>,
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
                exit_on_shutdown: false,
            }
        };

        let system_proc = host.system_proc().clone();
        let host_mesh_agent =
            system_proc.spawn(HOST_MESH_AGENT_ACTOR_NAME, HostMeshAgent::new(host))?;

        Ok(Self {
            host_mesh_agent,
            reply_port,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient)]
pub struct GetHostMeshAgent {
    #[reply]
    pub host_mesh_agent: PortRef<ActorRef<HostMeshAgent>>,
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
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
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
            } if name == resource_name
              && proc_id == ProcId::Direct(host_addr.clone(), name.to_string())
              && mesh_agent == ActorRef::attest(ProcId::Direct(host_addr.clone(), name.to_string()).actor_id(crate::proc_agent::PROC_AGENT_ACTOR_NAME, 0)) && bootstrap_command == Some(BootstrapCommand::test())
              && mesh_agent == proc_status_mesh_agent
        );
    }
}
