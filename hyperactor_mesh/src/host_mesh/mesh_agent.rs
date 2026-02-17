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
use hyperactor::context;
use hyperactor::context::Mailbox as _;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::LocalProcManager;
use hyperactor::host::SingleTerminate;
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
use crate::host_mesh::host_admin::HostAdminQueryMessage;
use crate::mesh_agent::ProcMeshAgent;
use crate::resource;
use crate::resource::ProcSpec;

pub(crate) type ProcManagerSpawnFuture =
    Pin<Box<dyn Future<Output = anyhow::Result<ActorHandle<ProcMeshAgent>>> + Send>>;
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
        HostAdminQueryMessage
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
        use crate::StatusOverlay;
        use crate::resource::Status;

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
        let host_mesh_agent = system_proc.spawn("agent", HostMeshAgent::new(host))?;

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
                "agent",
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
                    // "agent".
                    mesh_agent,
                    bootstrap_command,
                    proc_status: Some(ProcStatus::Ready { started_at: _, addr: _, agent: proc_status_mesh_agent}),
                    ..
                }),
            } if name == resource_name
              && proc_id == ProcId::Direct(host_addr.clone(), name.to_string())
              && mesh_agent == ActorRef::attest(ProcId::Direct(host_addr.clone(), name.to_string()).actor_id("agent", 0)) && bootstrap_command == Some(BootstrapCommand::test())
              && mesh_agent == proc_status_mesh_agent
        );
    }
}
