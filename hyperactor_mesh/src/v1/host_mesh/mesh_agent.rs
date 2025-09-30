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

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::Proc;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::channel::ChannelTransport;
use hyperactor::host::Host;
use hyperactor::host::HostError;
use hyperactor::host::LocalProcManager;
use serde::Deserialize;
use serde::Serialize;

use crate::bootstrap;
use crate::bootstrap::BootstrapCommand;
use crate::bootstrap::BootstrapProcManager;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::resource;
use crate::v1::Name;

type ProcManagerSpawnFuture =
    Pin<Box<dyn Future<Output = anyhow::Result<ActorHandle<ProcMeshAgent>>> + Send>>;
type ProcManagerSpawnFn = Box<dyn Fn(Proc) -> ProcManagerSpawnFuture + Send + Sync>;

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
    Process(Host<BootstrapProcManager>),
    Local(Host<LocalProcManager<ProcManagerSpawnFn>>),
}

impl HostAgentMode {
    fn system_proc(&self) -> &Proc {
        #[allow(clippy::match_same_arms)]
        match self {
            HostAgentMode::Process(host) => host.system_proc(),
            HostAgentMode::Local(host) => host.system_proc(),
        }
    }
}

/// A mesh agent is responsible for managing a host iny a [`HostMesh`],
/// through the resource behaviors defined in [`crate::resource`].
#[hyperactor::export(handlers=[resource::CreateOrUpdate<()>, resource::GetState<ProcState>, ShutdownHost])]
pub struct HostMeshAgent {
    host: Option<HostAgentMode>,
    created: HashMap<Name, Result<(ProcId, ActorRef<ProcMeshAgent>), HostError>>,
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
impl Actor for HostMeshAgent {
    type Params = HostAgentMode;

    async fn new(host: HostAgentMode) -> anyhow::Result<Self> {
        Ok(Self {
            host: Some(host),
            created: HashMap::new(),
        })
    }
}

#[async_trait]
impl Handler<resource::CreateOrUpdate<()>> for HostMeshAgent {
    #[tracing::instrument("HostMeshAgent::CreateOrUpdate", level = "info", skip_all, fields(name=%create_or_update.name))]
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<()>,
    ) -> anyhow::Result<()> {
        if self.created.contains_key(&create_or_update.name) {
            // There is no update.
            return Ok(());
        }

        let host = self.host.as_mut().expect("host present");
        let created = match host {
            HostAgentMode::Process(host) => {
                host.spawn(create_or_update.name.clone().to_string()).await
            }
            HostAgentMode::Local(host) => {
                host.spawn(create_or_update.name.clone().to_string()).await
            }
        };
        let ok = created.is_ok();
        if let Err(e) = &created {
            tracing::error!("failed to spawn proc {}: {}", create_or_update.name, e);
        }
        self.created.insert(create_or_update.name.clone(), created);
        create_or_update.reply.send(cx, ok)?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient)]
pub struct ShutdownHost {
    #[reply]
    pub ack: hyperactor::PortRef<()>,
}

#[async_trait]
impl Handler<ShutdownHost> for HostMeshAgent {
    async fn handle(&mut self, cx: &Context<Self>, msg: ShutdownHost) -> anyhow::Result<()> {
        // Prove we ran and let caller wait until this returns.
        msg.ack.send(cx, ())?;

        // Take ownership and drop the host; this drops the embedded
        // BootstrapProcManager (which sends SIGKILL to any children
        // it has spawned).
        if let Some(host_mode) = self.host.take() {
            drop(host_mode);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Named, Serialize, Deserialize)]
pub struct ProcState {
    pub proc_id: ProcId,
    pub mesh_agent: ActorRef<ProcMeshAgent>,
    pub bootstrap_command: Option<BootstrapCommand>,
    pub proc_status: Option<bootstrap::ProcStatus>,
}

#[async_trait]
impl Handler<resource::GetState<ProcState>> for HostMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_state: resource::GetState<ProcState>,
    ) -> anyhow::Result<()> {
        let manager = self
            .host
            .as_mut()
            .expect("host")
            .as_process()
            .map(Host::manager);
        let state = match self.created.get(&get_state.name) {
            Some(Ok((proc_id, mesh_agent))) => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::Running,
                state: Some(ProcState {
                    proc_id: proc_id.clone(),
                    mesh_agent: mesh_agent.clone(),
                    bootstrap_command: manager.map(|m| m.command().clone()),
                    proc_status: match manager {
                        Some(manager) => Some(manager.status(proc_id).await.unwrap()),
                        None => None,
                    },
                }),
            },
            Some(Err(e)) => resource::State {
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

        get_state.reply.send(cx, state)?;
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
    type Params = (
        ChannelTransport,
        PortRef<ActorRef<HostMeshAgent>>,
        Option<BootstrapCommand>,
        bool, /* local? */
    );

    async fn new((transport, reply_port, command, local): Self::Params) -> anyhow::Result<Self> {
        let host = if local {
            let spawn: ProcManagerSpawnFn = Box::new(|proc| Box::pin(ProcMeshAgent::boot_v1(proc)));
            let manager = LocalProcManager::new(spawn);
            let (host, _) = Host::serve(manager, transport.any()).await?;
            HostAgentMode::Local(host)
        } else {
            let command = match command {
                Some(command) => command,
                None => BootstrapCommand::current()?,
            };
            tracing::info!("booting host with proc command {:?}", command);
            let manager = BootstrapProcManager::new(command);
            let (host, _) = Host::serve(manager, transport.any()).await?;
            HostAgentMode::Process(host)
        };

        let host_mesh_agent = host
            .system_proc()
            .clone()
            .spawn::<HostMeshAgent>("agent", host)
            .await?;

        Ok(Self {
            host_mesh_agent,
            reply_port,
        })
    }

    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.reply_port.send(this, self.host_mesh_agent.bind())?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Named, Handler, RefClient)]
pub struct GetHostMeshAgent {
    #[reply]
    pub host_mesh_agent: PortRef<ActorRef<HostMeshAgent>>,
}

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
    async fn test_basic() {
        let (host, _handle) = Host::serve(
            BootstrapProcManager::new(BootstrapCommand::test()),
            ChannelTransport::Unix.any(),
        )
        .await
        .unwrap();

        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent = system_proc
            .spawn::<HostMeshAgent>("agent", HostAgentMode::Process(host))
            .await
            .unwrap();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string())
            .await
            .unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        let name = Name::new("proc1");

        // First, create the proc, then query its state:

        assert!(
            host_agent
                .create_or_update(&client, name.clone(), ())
                .await
                .unwrap()
        );
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
                    proc_status: Some(ProcStatus::Ready { pid: _, started_at: _, addr: _, agent: proc_status_mesh_agent}),
                }),
            } if name == resource_name
              && proc_id == ProcId::Direct(host_addr.clone(), name.to_string())
              && mesh_agent == ActorRef::attest(ProcId::Direct(host_addr.clone(), name.to_string()).actor_id("agent", 0)) && bootstrap_command == Some(BootstrapCommand::test())
              && mesh_agent == proc_status_mesh_agent
        );
    }
}
