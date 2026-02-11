/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The mesh agent actor that manages a host.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
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
use hyperactor::actor::remote::Remote;
use hyperactor::channel::ChannelTransport;
use hyperactor::context;
use hyperactor::context::Mailbox as _;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::LocalProcManager;
use hyperactor::host::SingleTerminate;
use hyperactor::mailbox::PortSender as _;
use hyperactor_config::Attrs;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use typeuri::Named;

use crate::bootstrap;
use crate::bootstrap::BootstrapCommand;
use crate::bootstrap::BootstrapProcConfig;
use crate::bootstrap::BootstrapProcManager;
use crate::proc_launcher::ProcLauncher;
use crate::proc_launcher::ProcLauncherError;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::resource;
use crate::resource::ProcSpec;
use crate::v1::Name;
use crate::v1::host_mesh::host_admin::HostAdminQueryMessage;

pub(crate) type ProcManagerSpawnFuture =
    Pin<Box<dyn Future<Output = anyhow::Result<ActorHandle<ProcMeshAgent>>> + Send>>;
pub(crate) type ProcManagerSpawnFn = Box<dyn Fn(Proc) -> ProcManagerSpawnFuture + Send + Sync>;

/// The well-known name of the host mesh agent actor.
pub static HOST_AGENT: &str = "host_agent";

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

    /// Get the system proc for this host.
    ///
    /// The system proc is where infrastructure actors (like proc
    /// launchers) are spawned.
    pub fn system_proc(&self) -> &Proc {
        #[allow(clippy::match_same_arms)]
        match self {
            HostAgentMode::Process { host, .. } => host.system_proc(),
            HostAgentMode::Local(host) => host.system_proc(),
        }
    }

    /// Get the local proc for this host.
    pub fn local_proc(&self) -> &Proc {
        #[allow(clippy::match_same_arms)]
        match self {
            HostAgentMode::Process { host, .. } => host.local_proc(),
            HostAgentMode::Local(host) => host.local_proc(),
        }
    }

    async fn terminate_proc(
        &self,
        cx: &impl context::Actor,
        proc: &ProcId,
        timeout: Duration,
        reason: &str,
    ) -> Result<(Vec<ActorId>, Vec<ActorId>), anyhow::Error> {
        #[allow(clippy::match_same_arms)]
        match self {
            HostAgentMode::Process { host, .. } => {
                host.terminate_proc(cx, proc, timeout, reason).await
            }
            HostAgentMode::Local(host) => host.terminate_proc(cx, proc, timeout, reason).await,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ProcCreationState {
    pub(crate) rank: usize,
    pub(crate) created: Result<(ProcId, ActorRef<ProcMeshAgent>), HostError>,
    pub(crate) stopped: bool,
}

/// A mesh agent is responsible for managing a host iny a [`HostMesh`],
/// through the resource behaviors defined in [`crate::resource`].
#[hyperactor::export(
    handlers=[
        resource::CreateOrUpdate<ProcSpec>,
        resource::Stop,
        resource::GetState<ProcState>,
        resource::GetRankStatus { cast = true },
        resource::List,
        ShutdownHost,
        HostAdminQueryMessage,
        GspawnOnSystemProc
    ]
)]
pub struct HostMeshAgent {
    pub(crate) host: Option<HostAgentMode>,
    pub(crate) created: HashMap<Name, ProcCreationState>,
    /// Stores the lazily initialized proc mesh agent for the local proc.
    local_mesh_agent: OnceLock<anyhow::Result<ActorHandle<ProcMeshAgent>>>,
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

    /// Install a custom proc launcher.
    ///
    /// Only valid for Process mode hosts. Must be called before any spawn
    /// operation that would initialize the default launcher.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The host is in Local mode (does not support custom launchers)
    /// - The host has been shut down
    /// - A launcher has already been installed
    pub fn set_proc_launcher(
        &mut self,
        launcher: Arc<dyn ProcLauncher>,
    ) -> Result<(), ProcLauncherError> {
        match self.host.as_mut() {
            Some(HostAgentMode::Process { host, .. }) => host.manager().set_launcher(launcher),
            Some(HostAgentMode::Local(_)) => Err(ProcLauncherError::Other(
                "Local mode does not support custom proc launchers".into(),
            )),
            None => Err(ProcLauncherError::Other("Host has been shut down".into())),
        }
    }

    /// Get the system proc for spawning infrastructure actors.
    ///
    /// Returns `None` if the host has been shut down.
    pub fn system_proc(&self) -> Option<&Proc> {
        self.host.as_ref().map(|h| h.system_proc())
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
        _cx: &Context<Self>,
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
                stopped: false,
            },
        );

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
            .as_mut()
            .ok_or(anyhow::anyhow!("HostMeshAgent has already shut down"))?;
        let manager = host.as_process().map(|(h, _)| h.manager());
        let timeout = hyperactor_config::global::get(hyperactor::config::PROCESS_EXIT_TIMEOUT);
        // We don't remove the proc from the state map, instead we just store
        // its state as Stopped.
        let proc = self.created.get_mut(&message.name);
        if let Some(ProcCreationState {
            created: Ok((proc_id, _)),
            stopped,
            ..
        }) = proc
        {
            let proc_status = match manager {
                Some(manager) => manager.status(proc_id).await,
                None => None,
            };
            // Fetch status from the ProcStatus object if it's available
            // for more details.
            // This prevents trying to kill a process that is already dead.
            let should_stop = if let Some(status) = &proc_status {
                resource::Status::from(status.clone()).is_healthy()
            } else {
                !*stopped
            };
            if should_stop {
                host.terminate_proc(&cx, proc_id, timeout, &message.reason)
                    .await?;
                *stopped = true;
            }
        }

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
        use crate::resource::Status;
        use crate::v1::StatusOverlay;

        let manager = self
            .host
            .as_mut()
            .and_then(|h| h.as_process())
            .map(|(h, _)| h.manager());
        let (rank, status) = match self.created.get(&get_rank_status.name) {
            Some(ProcCreationState {
                rank,
                created: Ok((proc_id, _mesh_agent)),
                stopped,
            }) => {
                let proc_status = match manager {
                    Some(manager) => manager.status(proc_id).await,
                    None => None,
                };
                // Fetch status from the ProcStatus object if it's available
                // for more details.
                let status = if let Some(status) = &proc_status {
                    status.clone().into()
                } else if *stopped {
                    resource::Status::Stopped
                } else {
                    resource::Status::Running
                };
                (*rank, status)
            }
            // If the creation failed, show as Failed instead of Stopped even if
            // the proc was stopped.
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
    pub mesh_agent: ActorRef<ProcMeshAgent>,
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
        let manager: Option<&BootstrapProcManager> = self
            .host
            .as_mut()
            .and_then(|h| h.as_process())
            .map(|(h, _)| h.manager());
        let state = match self.created.get(&get_state.name) {
            Some(ProcCreationState {
                rank,
                created: Ok((proc_id, mesh_agent)),
                stopped,
            }) => {
                let proc_status = match manager {
                    Some(manager) => manager.status(proc_id).await,
                    None => None,
                };
                // Fetch status from the ProcStatus object if it's available
                // for more details.
                let status = if let Some(status) = &proc_status {
                    status.clone().into()
                } else if *stopped {
                    resource::Status::Stopped
                } else {
                    resource::Status::Running
                };
                resource::State {
                    name: get_state.name.clone(),
                    status,
                    state: Some(ProcState {
                        proc_id: proc_id.clone(),
                        create_rank: *rank,
                        mesh_agent: mesh_agent.clone(),
                        bootstrap_command: manager.map(|m| m.command().clone()),
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

/// A local-only message to access the "local" proc on the host.
/// This is used to bootstrap the root mesh process client on the
/// local singleton host mesh.
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct GetLocalProc {
    #[reply]
    pub proc_mesh_agent: PortHandle<ActorHandle<ProcMeshAgent>>,
}

#[async_trait]
impl Handler<GetLocalProc> for HostMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetLocalProc { proc_mesh_agent }: GetLocalProc,
    ) -> anyhow::Result<()> {
        let agent = self.local_mesh_agent.get_or_init(|| {
            ProcMeshAgent::boot_v1(self.host.as_ref().unwrap().local_proc().clone())
        });

        match agent {
            Err(e) => anyhow::bail!("error booting local proc: {}", e),
            Ok(agent) => proc_mesh_agent.send(cx, agent.clone())?,
        };

        Ok(())
    }
}

/// A local-only message to install a custom proc launcher.
///
/// This is used by monarch_hyperactor to install Python-based proc
/// launchers. The message is not serializable and can only be sent
/// within the same process.
#[derive(hyperactor::Handler, hyperactor::HandleClient)]
pub struct SetProcLauncher {
    /// The proc launcher to install.
    pub launcher: Arc<dyn ProcLauncher>,
    /// Reply port to signal completion or error.
    #[reply]
    pub result: PortHandle<Result<(), String>>,
}

impl fmt::Debug for SetProcLauncher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SetProcLauncher")
            .field("launcher", &"<dyn ProcLauncher>")
            .field("result", &self.result)
            .finish()
    }
}

#[async_trait]
impl Handler<SetProcLauncher> for HostMeshAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: SetProcLauncher) -> anyhow::Result<()> {
        let result = self
            .set_proc_launcher(msg.launcher)
            .map_err(|e| e.to_string());
        msg.result.send(cx, result)?;
        Ok(())
    }
}

/// Spawn an actor on the system proc by type name (idempotent).
///
/// Uses the actor registry ([`Remote`]) to find the actor type by
/// name and spawn it on the system proc. If an actor with the given
/// name already exists, returns its ID instead of failing. This makes
/// the operation safe to retry.
///
/// This is used by monarch_hyperactor to spawn infrastructure actors
/// like `ProcLauncherInstaller` without compile-time type
/// dependencies.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Named,
    hyperactor::Handler,
    hyperactor::HandleClient
)]
pub struct GspawnOnSystemProc {
    /// The fully-qualified type name of the actor to spawn (e.g.,
    /// "monarch_hyperactor::proc_launcher_installer::ProcLauncherInstaller").
    pub actor_type: String,
    /// The name to give the spawned actor.
    pub actor_name: String,
    /// Serialized parameters for the actor (bincode-encoded).
    pub params_data: Vec<u8>,
    /// Reply port for the result.
    #[reply]
    pub result: PortRef<Result<ActorId, String>>,
}

#[async_trait]
impl Handler<GspawnOnSystemProc> for HostMeshAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: GspawnOnSystemProc) -> anyhow::Result<()> {
        let result = match &self.host {
            Some(host) => {
                let system_proc = host.system_proc();
                let remote = Remote::collect();

                match remote
                    .gspawn(
                        system_proc,
                        &msg.actor_type,
                        &msg.actor_name,
                        msg.params_data,
                        cx.headers().clone(),
                    )
                    .await
                {
                    Ok(actor_id) => Ok(actor_id),
                    Err(e) => {
                        // Check if this is an "already spawned" error
                        // (idempotent)
                        let err_str = e.to_string();
                        if err_str.contains("has already been spawned") {
                            // Return the existing actor ID
                            // (idempotent behavior)
                            let existing_id = system_proc.proc_id().actor_id(&msg.actor_name, 0);
                            Ok(existing_id)
                        } else {
                            Err(err_str)
                        }
                    }
                }
            }
            None => Err("Host has been shut down".to_string()),
        };
        msg.result.send(cx, result)?;
        Ok(())
    }
}

/// Test-only message to force default proc launcher initialization.
///
/// This triggers the same lock-in as a spawn would, without needing
/// to set up the full spawn pipeline. Used by tests to verify that
/// `SetProcLauncher` fails after default initialization.
#[cfg(test)]
#[derive(Debug, hyperactor::Handler, hyperactor::HandleClient)]
pub struct ForceDefaultProcLauncherInit {
    /// Reply port to signal completion or error.
    #[reply]
    pub result: PortHandle<Result<(), String>>,
}

#[cfg(test)]
#[async_trait]
impl Handler<ForceDefaultProcLauncherInit> for HostMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: ForceDefaultProcLauncherInit,
    ) -> anyhow::Result<()> {
        let result = match &self.host {
            Some(HostAgentMode::Process { host, .. }) => {
                // Force the get_or_init path - this locks in the
                // default
                host.manager().launcher();
                Ok(())
            }
            Some(HostAgentMode::Local(_)) => {
                Err("Local mode does not have a BootstrapProcManager".into())
            }
            None => Err("Host has been shut down".into()),
        };
        msg.result.send(cx, result)?;
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
        _environment: Attrs,
    ) -> anyhow::Result<Self> {
        let host = if local {
            let spawn: ProcManagerSpawnFn =
                Box::new(|proc| Box::pin(std::future::ready(ProcMeshAgent::boot_v1(proc))));
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
        let host_mesh_agent = system_proc.spawn(HOST_AGENT, HostMeshAgent::new(host))?;

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

// Test utilities (NOT under #[cfg(test)] - must be in bootstrap
// binary image)
//
// This can't be defined under a `#[cfg(test)]` because there needs to
// be an entry in the spawnable actor registry in the executable
// 'hyperactor_mesh_test_bootstrap' for the GspawnOnSystemProc tests.

/// Test utilities for mesh agent tests.
pub mod test_utils {
    use std::sync::Arc;

    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::ActorId;
    use hyperactor::ActorRef;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::PortRef;
    use serde::Deserialize;
    use serde::Serialize;
    use typeuri::Named;

    use super::HostMeshAgent;
    use super::SetProcLauncherClient;
    use crate::proc_launcher::ProcLauncher;

    /// A minimal test actor for system proc infrastructure tests.
    ///
    /// This actor is registered with `spawn = true` so it can be
    /// spawned via `Remote::collect().gspawn()`. It derives `Default`
    /// which triggers the blanket `RemoteSpawn` impl with `Params =
    /// ()`.
    #[derive(Debug, Default)]
    #[hyperactor::export(spawn = true, handlers = [InstallLauncherViaDowncast])]
    pub struct SystemProcTestActor {
        pub initialized: bool,
    }

    impl Actor for SystemProcTestActor {}

    /// Message to test the downcast_handle pattern for local message
    /// sending.
    ///
    /// This message triggers `SystemProcTestActor` to:
    /// 1. Use `ActorRef::attest` to create a reference to
    ///    `HostMeshAgent`
    /// 2. Use `downcast_handle` to get an `ActorHandle`
    /// 3. Create a `DummyLauncher` locally
    /// 4. Send `SetProcLauncher` via the handle
    ///
    /// This validates the pattern that `ProcLauncherInstaller` will use.
    #[derive(
        Debug,
        Serialize,
        Deserialize,
        Named,
        hyperactor::Handler,
        hyperactor::HandleClient,
        hyperactor::RefClient
    )]
    pub struct InstallLauncherViaDowncast {
        /// The ActorId of the HostMeshAgent to send to.
        pub host_agent_id: ActorId,
        /// Reply port for the result.
        #[reply]
        pub result: PortRef<Result<(), String>>,
    }

    #[async_trait]
    impl Handler<InstallLauncherViaDowncast> for SystemProcTestActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: InstallLauncherViaDowncast,
        ) -> anyhow::Result<()> {
            // 1. Create ActorRef from the provided ActorId
            let agent_ref: ActorRef<HostMeshAgent> = ActorRef::attest(msg.host_agent_id.clone());

            // 2. Downcast to ActorHandle (only works if same proc)
            let result = match agent_ref.downcast_handle(cx) {
                Some(agent_handle) => {
                    // 3. Create a launcher locally.
                    //
                    // In the real ProcLauncherInstaller, this would be:
                    //   let python_actor = PythonActor::new(pickled_actor_class)?;
                    //   let handle = python_actor.spawn(cx)?;
                    //   handle.send(cx, init_message)?;  // Initialize with user's launcher
                    //   let launcher = Arc::new(ActorProcLauncher::new(handle, mailbox, instance));
                    //
                    // For this test, we use a dummy:
                    let launcher: Arc<dyn ProcLauncher> = Arc::new(TestDummyLauncher);

                    // 4. Send SetProcLauncher via the handle
                    agent_handle
                        .set_proc_launcher(cx, launcher)
                        .await
                        .map_err(|e| format!("send failed: {}", e))
                        .and_then(|r| r)
                }
                None => Err("downcast_handle failed: HostMeshAgent not on same proc".to_string()),
            };

            msg.result.send(cx, result)?;
            Ok(())
        }
    }

    /// A minimal dummy launcher for testing the downcast_handle pattern.
    #[derive(Debug)]
    struct TestDummyLauncher;

    #[async_trait]
    impl ProcLauncher for TestDummyLauncher {
        async fn launch(
            &self,
            _proc_id: &hyperactor::ProcId,
            _opts: crate::proc_launcher::LaunchOptions,
        ) -> Result<crate::proc_launcher::LaunchResult, crate::proc_launcher::ProcLauncherError>
        {
            unimplemented!("TestDummyLauncher::launch should not be called in this test")
        }

        async fn terminate(
            &self,
            _proc_id: &hyperactor::ProcId,
            _timeout: std::time::Duration,
        ) -> Result<(), crate::proc_launcher::ProcLauncherError> {
            Ok(())
        }

        async fn kill(
            &self,
            _proc_id: &hyperactor::ProcId,
        ) -> Result<(), crate::proc_launcher::ProcLauncherError> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;
    use std::time::Duration;

    use hyperactor::ActorRef;
    use hyperactor::Proc;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;

    use super::*;
    use crate::bootstrap::ProcStatus;
    use crate::proc_launcher::LaunchOptions;
    use crate::proc_launcher::LaunchResult;
    use crate::proc_launcher::ProcExitKind;
    use crate::proc_launcher::ProcExitResult;
    use crate::proc_launcher::ProcLauncher;
    use crate::proc_launcher::ProcLauncherError;
    use crate::proc_launcher::StdioHandling;
    use crate::proc_mesh::mesh_agent::PROC_AGENT;
    use crate::resource::CreateOrUpdateClient;
    use crate::resource::GetStateClient;

    /// A dummy proc launcher for testing. Does not actually launch
    /// anything.
    #[allow(dead_code)]
    struct DummyLauncher {
        marker: u64,
    }

    impl DummyLauncher {
        fn new(marker: u64) -> Self {
            Self { marker }
        }
    }

    #[async_trait]
    impl ProcLauncher for DummyLauncher {
        async fn launch(
            &self,
            _proc_id: &hyperactor::ProcId,
            _opts: LaunchOptions,
        ) -> Result<LaunchResult, ProcLauncherError> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(ProcExitResult {
                kind: ProcExitKind::Exited { code: 0 },
                stderr_tail: Some(vec![]),
            });
            Ok(LaunchResult {
                pid: None,
                started_at: RealClock.system_time_now(),
                stdio: StdioHandling::ManagedByLauncher,
                exit_rx: rx,
            })
        }

        async fn terminate(
            &self,
            _proc_id: &hyperactor::ProcId,
            _timeout: Duration,
        ) -> Result<(), ProcLauncherError> {
            Ok(())
        }

        async fn kill(&self, _proc_id: &hyperactor::ProcId) -> Result<(), ProcLauncherError> {
            Ok(())
        }
    }

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
                HOST_AGENT,
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
              && mesh_agent == ActorRef::attest(ProcId::Direct(host_addr.clone(), name.to_string()).actor_id(PROC_AGENT, 0)) && bootstrap_command == Some(BootstrapCommand::test())
              && mesh_agent == proc_status_mesh_agent
        );
    }

    // Tests that HostMeshAgent can receive and process a
    // SetProcLauncher message, successfully installing a custom proc
    // launcher.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_set_proc_launcher() {
        // Create host and spawn HostMeshAgent
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                "agent",
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
                }),
            )
            .unwrap();

        // Create a client to send messages
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Create a custom launcher
        let custom_launcher: Arc<dyn ProcLauncher> = Arc::new(DummyLauncher::new(42));

        // Send SetProcLauncher message
        let result = host_agent
            .set_proc_launcher(&client, custom_launcher)
            .await
            .expect("send should succeed");

        // Verify the launcher was installed successfully
        assert!(
            result.is_ok(),
            "set_proc_launcher should succeed on fresh host: {:?}",
            result
        );
    }

    /// SetProcLauncher fails if called twice.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_set_proc_launcher_twice_fails() {
        // Create host and spawn HostMeshAgent
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                "agent",
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // First install should succeed
        let first: Arc<dyn ProcLauncher> = Arc::new(DummyLauncher::new(1));
        let result1 = host_agent
            .set_proc_launcher(&client, first)
            .await
            .expect("send should succeed");
        assert!(result1.is_ok(), "first install should succeed");

        // Second install should fail
        let second: Arc<dyn ProcLauncher> = Arc::new(DummyLauncher::new(2));
        let result2 = host_agent
            .set_proc_launcher(&client, second)
            .await
            .expect("send should succeed");
        assert!(result2.is_err(), "second install should fail");

        // Verify error message
        let err_msg = result2.unwrap_err();
        assert!(
            err_msg.contains("already initialized"),
            "error should mention 'already initialized', got: {}",
            err_msg
        );
    }

    /// SetProcLauncher fails after default initialization.
    ///
    /// This pins the "default locks it in" invariant: once launcher()
    /// is called (e.g., by a spawn), set_proc_launcher() must fail.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_set_proc_launcher_after_default_init_fails() {
        use super::ForceDefaultProcLauncherInitClient;

        // Create host and spawn HostMeshAgent
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                "agent",
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Force default initialization (simulates what spawn would do)
        let init_result = host_agent
            .force_default_proc_launcher_init(&client)
            .await
            .expect("send should succeed");
        assert!(init_result.is_ok(), "force init should succeed");

        // Now try to install custom - should fail
        let custom: Arc<dyn ProcLauncher> = Arc::new(DummyLauncher::new(99));
        let result = host_agent
            .set_proc_launcher(&client, custom)
            .await
            .expect("send should succeed");

        assert!(
            result.is_err(),
            "set_proc_launcher should fail after default init"
        );

        let err_msg = result.unwrap_err();
        assert!(
            err_msg.contains("already initialized"),
            "error should mention 'already initialized', got: {}",
            err_msg
        );
        assert!(
            err_msg.contains("before first spawn"),
            "error should mention 'before first spawn', got: {}",
            err_msg
        );
    }

    // GspawnOnSystemProc Tests
    //
    // Tests for spawning actors on the system proc via the Remote
    // registry.

    use hyperactor::actor::remote::Remote;

    use super::GspawnOnSystemProcClient;
    use super::test_utils::SystemProcTestActor;

    // Get the fully-qualified type name for SystemProcTestActor from the
    // registry.
    fn system_proc_test_actor_type() -> String {
        let remote = Remote::collect();
        remote
            .name_of::<SystemProcTestActor>()
            .expect("SystemProcTestActor should be registered")
            .to_string()
    }

    // GspawnOnSystemProc successfully spawns an actor.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_gspawn_on_system_proc_happy_path() {
        // Create host and spawn HostMeshAgent
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                "agent",
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Spawn an actor via GspawnOnSystemProc
        let actor_type = system_proc_test_actor_type();
        let params_data = bincode::serialize(&()).unwrap();

        let result = host_agent
            .gspawn_on_system_proc(&client, actor_type, "test_actor".to_string(), params_data)
            .await
            .expect("send should succeed");

        // Should succeed and return an ActorId
        assert!(result.is_ok(), "gspawn should succeed: {:?}", result);
        let actor_id = result.unwrap();
        assert_eq!(
            actor_id.name(),
            "test_actor",
            "actor should have the requested name"
        );
    }

    /// GspawnOnSystemProc is idempotent - second spawn returns
    /// existing ID.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_gspawn_on_system_proc_idempotent() {
        // Create host and spawn HostMeshAgent
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                "agent",
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let actor_type = system_proc_test_actor_type();
        let params_data = bincode::serialize(&()).unwrap();

        // First spawn
        let result1 = host_agent
            .gspawn_on_system_proc(
                &client,
                actor_type.clone(),
                "idempotent_actor".to_string(),
                params_data.clone(),
            )
            .await
            .expect("send should succeed");
        assert!(result1.is_ok(), "first spawn should succeed: {:?}", result1);
        let actor_id1 = result1.unwrap();

        // Second spawn with same name - should return the same ID (idempotent)
        let result2 = host_agent
            .gspawn_on_system_proc(
                &client,
                actor_type,
                "idempotent_actor".to_string(),
                params_data,
            )
            .await
            .expect("send should succeed");
        assert!(
            result2.is_ok(),
            "second spawn should succeed (idempotent): {:?}",
            result2
        );
        let actor_id2 = result2.unwrap();

        // Both should return the same actor ID
        assert_eq!(
            actor_id1, actor_id2,
            "idempotent spawn should return same actor ID"
        );
    }

    /// GspawnOnSystemProc fails for unknown actor type.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_gspawn_on_system_proc_bad_type() {
        // Create host and spawn HostMeshAgent
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                "agent",
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Try to spawn a non-existent actor type
        let result = host_agent
            .gspawn_on_system_proc(
                &client,
                "nonexistent::FakeActor".to_string(),
                "fake_actor".to_string(),
                vec![],
            )
            .await
            .expect("send should succeed");

        // Should fail with an error
        assert!(result.is_err(), "gspawn with bad type should fail");
        let err_msg = result.unwrap_err();
        assert!(
            err_msg.contains("not registered"),
            "error should indicate actor type not registered, got: {}",
            err_msg
        );
    }

    /// Test that an actor on the system proc can use downcast_handle
    /// to send SetProcLauncher to HostMeshAgent.
    ///
    /// This  validates the pattern that  `ProcLauncherInstaller` will
    /// use:
    /// 1. Spawn via GspawnOnSystemProc
    /// 2. Use ActorRef::attest + downcast_handle to get HostMeshAgent
    ///   handle
    /// 3. Send local-only SetProcLauncher message via the handle
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_system_proc_downcast_handle_set_proc_launcher() {
        use super::test_utils::*;

        // Create host and spawn HostMeshAgent
        let host = Host::new(
            BootstrapProcManager::new(BootstrapCommand::test()).unwrap(),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn(
                "agent",
                HostMeshAgent::new(HostAgentMode::Process {
                    host,
                    exit_on_shutdown: false,
                }),
            )
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Spawn SystemProcTestActor on the system proc (same proc as
        // HostMeshAgent)
        let actor_type = system_proc_test_actor_type();
        let params_data = bincode::serialize(&()).unwrap();

        let result = host_agent
            .gspawn_on_system_proc(
                &client,
                actor_type,
                "launcher_installer".to_string(),
                params_data,
            )
            .await
            .expect("gspawn send should succeed");

        assert!(result.is_ok(), "gspawn should succeed: {:?}", result);
        let test_actor_id = result.unwrap();

        // Get ActorRef to the spawned test actor
        let test_actor_ref: ActorRef<super::test_utils::SystemProcTestActor> =
            ActorRef::attest(test_actor_id);

        // Send InstallLauncherViaDowncast to the test actor. This
        // will trigger the downcast_handle + set_proc_launcher flow.
        let install_result = test_actor_ref
            .install_launcher_via_downcast(&client, host_agent.actor_id().clone())
            .await
            .expect("install_launcher_via_downcast send should succeed");

        // Verify the downcast_handle + SetProcLauncher succeeded
        assert!(
            install_result.is_ok(),
            "downcast_handle + set_proc_launcher should succeed: {:?}",
            install_result
        );
    }
}
