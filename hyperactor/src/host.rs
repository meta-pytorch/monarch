/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines [`Host`], which represents all the procs running on a host.
//! The procs themselves are managed by an implementation of [`ProcManager`], which may,
//! for example, fork new processes for each proc, or spawn them in the same process
//! for testing purposes.
//!
//! The primary purpose of a host is to manage the lifecycle of these procs, and to
//! serve as a single front-end for all the procs on a host, multiplexing network
//! channels.
//!
//! ## Channel muxing
//!
//! A [`Host`] maintains a single frontend address, through which all procs are accessible
//! through direct addressing: the id of each proc is the `ProcId::Direct(frontend_addr, proc_name)`.
//! In the following, the frontend address is denoted by `*`. The host listens on `*` and
//! multiplexes messages based on the proc name. When spawning procs, the host maintains
//! backend channels with separate addresses. In the diagram `#` is the backend address of
//! the host, while `#n` is the backend address for proc *n*. The host forwards messages
//! to the appropriate backend channel, while procs forward messages to the host backend
//! channel at `#`.
//!
//! ```text
//!                      ┌────────────┐
//!                  ┌───▶  proc *,1  │
//!                  │ #1└────────────┘
//!                  │                 
//!  ┌──────────┐    │   ┌────────────┐
//!  │   Host   │◀───┼───▶  proc *,2  │
//! *└──────────┘#   │ #2└────────────┘
//!                  │                 
//!                  │   ┌────────────┐
//!                  └───▶  proc *,3  │
//!                    #3└────────────┘
//! ```

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::Actor;
use crate::PortHandle;
use crate::Proc;
use crate::ProcId;
use crate::actor::Binds;
use crate::actor::RemoteActor;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::channel::Rx;
use crate::channel::Tx;
use crate::mailbox::BoxableMailboxSender;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::MailboxClient;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxServer;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::Undeliverable;

/// The type of error produced by host operations.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// A channel error occurred during a host operation.
    #[error(transparent)]
    ChannelError(#[from] ChannelError),

    /// The named proc already exists and cannot be spawned.
    #[error("proc '{0}' already exists")]
    ProcExists(String),

    /// Failures occuring while spawning a subprocess.
    #[error("proc '{0}' failed to spawn process: {1}")]
    ProcessSpawnFailure(ProcId, #[source] std::io::Error),

    /// Failures occuring while spawning a management actor in a proc.
    #[error("proc '{0}' failed to spawn actor: {1}")]
    ActorSpawnFailure(String, #[source] anyhow::Error),

    /// An input parameter was missing.
    #[error("parameter '{0}' missing: {1}")]
    MissingParameter(String, std::env::VarError),

    /// An input parameter was invalid.
    #[error("parameter '{0}' invalid: {1}")]
    InvalidParameter(String, anyhow::Error),
}

/// A host, managing the lifecycle of several procs, and their backend
/// routing, as described in this module's documentation.
pub struct Host {
    procs: HashMap<String, ChannelAddr>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    router: DialMailboxRouter,
    manager: Box<dyn ProcManager>,
    system_proc: Proc,
}

impl Host {
    /// Serve a host using the provided ProcManager, on the provided `addr`.
    /// On success, the host will multiplex messages for procs on the host
    /// on the address of the host.
    pub async fn serve(
        manager: Box<dyn ProcManager>,
        addr: ChannelAddr,
    ) -> Result<(Self, MailboxServerHandle), HostError> {
        let (frontend_addr, frontend_rx) = channel::serve(addr).await?;

        // We set up a cascade of routers: first, the outer router supports
        // sending to the the system proc, while the dial router manages dialed
        // connections.
        let router = DialMailboxRouter::new();

        // Establish a backend channel on the preferred transport. We currently simply
        // serve the same router on both.
        let (backend_addr, backend_rx) =
            channel::serve(ChannelAddr::any(manager.transport())).await?;

        // Set up a system proc. This is often used to manage the host itself.
        let system_proc_id = ProcId::Direct(frontend_addr.clone(), "system".to_string());
        let system_proc = Proc::new(system_proc_id, router.boxed());

        let host = Host {
            procs: HashMap::new(),
            frontend_addr,
            backend_addr,
            router: router.clone(),
            manager,
            system_proc: system_proc.clone(),
        };

        let router = ProcOrDial {
            proc: system_proc,
            router,
        };

        // Serve the same router on both frontend and backend addresses.
        let _backend_handle = router.clone().serve(backend_rx);
        let frontend_handle = router.serve(frontend_rx);

        Ok((host, frontend_handle))
    }

    /// The address which accepts messages destined for this host.
    pub fn addr(&self) -> &ChannelAddr {
        &self.frontend_addr
    }

    /// The system proc associated with this host.
    pub fn system_proc(&self) -> &Proc {
        &self.system_proc
    }

    /// Spawn a new process with the given `name`. On success, the proc has been
    /// spawned, and is reachable through the returned, direct-addressed ProcId,
    /// which will be `ProcId::Direct(self.addr(), name)`.
    pub async fn spawn(&mut self, name: String) -> Result<ProcId, HostError> {
        if self.procs.contains_key(&name) {
            return Err(HostError::ProcExists(name));
        }

        let proc_id = ProcId::Direct(self.frontend_addr.clone(), name.clone());
        let addr = self
            .manager
            .spawn(proc_id.clone(), self.backend_addr.clone())
            .await?;

        self.router.bind(proc_id.clone().into(), addr.clone());
        self.procs.insert(name, addr);
        Ok(proc_id)
    }
}

/// A router used to route to the system proc, or else fall back to
/// the dial mailbox router.
#[derive(Debug, Clone)]
struct ProcOrDial {
    proc: Proc,
    router: DialMailboxRouter,
}

impl MailboxSender for ProcOrDial {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if envelope.dest().actor_id().proc_id() == self.proc.proc_id() {
            self.proc.post(envelope, return_handle);
        } else {
            self.router.post(envelope, return_handle)
        }
    }
}

/// A trait describing a manager of procs, responsible for bootstrapping
/// procs on a host, and managing their lifetimes.
#[async_trait]
pub trait ProcManager {
    /// The preferred transport for this ProcManager.
    /// In practice this will be [`ChannelTransport::Local`]
    /// for testing, and [`ChannelTransport::Unix`] for external
    /// processes.
    fn transport(&self) -> ChannelTransport;

    /// Spawn a new proc with the provided proc id. The proc
    /// should use the provided forwarder address for messages
    /// destined outside of the proc. The returned address accepts
    /// messages destined for the proc.
    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
    ) -> Result<ChannelAddr, HostError>;

    // TODO: full lifecycle management; perhaps mimick the Command API.
}

/// A ProcManager that spawns into local (in-process) procs. Used for testing.
pub struct LocalProcManager {
    procs: Arc<Mutex<HashMap<ProcId, Proc>>>,
}

#[async_trait]
impl ProcManager for LocalProcManager {
    fn transport(&self) -> ChannelTransport {
        ChannelTransport::Local
    }

    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
    ) -> Result<ChannelAddr, HostError> {
        let transport = forwarder_addr.transport();
        let proc = Proc::new(
            proc_id.clone(),
            MailboxClient::dial(forwarder_addr)?.into_boxed(),
        );
        let (proc_addr, rx) = channel::serve(ChannelAddr::any(transport)).await?;
        self.procs
            .lock()
            .await
            .insert(proc_id.clone(), proc.clone());
        let _handle = proc.clone().serve(rx);
        Ok(proc_addr)
    }
}

/// A ProcManager that manages each proc as a separate process.
/// It follows a simple protocol:
///
/// Each process is launched with the following environment variables:
/// - HYPERACTOR_HOST_BACKEND_ADDR: the backend address to which all messages are forwarded,
/// - HYPERACTOR_HOST_PROC_ID: the proc id to assign the launched proc, and
/// - HYPERACTOR_HOST_CALLBACK_ADDR: the channel address with which to return the proc's address
///
/// The launched proc should also spawn an actor to manage it - the details of this are
/// implementation dependent, and outside the scope of the process manager.
///
/// The function [`ProcessProcManager::boot_proc`] provides a convenient implementation of the
/// protocol.
pub struct ProcessProcManager {
    cmd: Arc<Mutex<Command>>,
}

impl ProcessProcManager {
    /// Create a new ProcessProcManager that runs the provided command.
    pub fn new(cmd: Command) -> Self {
        Self {
            cmd: Arc::new(Mutex::new(cmd)),
        }
    }

    /// Boot a process. Should be called from processes spawned by the process manager.
    /// `boot_proc` will spawn the provided actor type (with parameters) onto the newly
    /// created Proc, and bind its handler. This allows the user to install an agent to
    /// manage the proc itself.
    pub async fn boot_proc<A: Actor + RemoteActor + Binds<A>>(
        name: &str,
        params: A::Params,
    ) -> Result<Proc, HostError> {
        let backend_addr: ChannelAddr = Self::parse_env("HYPERACTOR_HOST_BACKEND_ADDR")?;
        let proc_id: ProcId = Self::parse_env("HYPERACTOR_HOST_PROC_ID")?;
        let callback_addr: ChannelAddr = Self::parse_env("HYPERACTOR_HOST_CALLBACK_ADDR")?;
        let backend_transport = backend_addr.transport();
        let proc = Proc::new(proc_id, MailboxClient::dial(backend_addr)?.into_boxed());

        let handle = proc
            .spawn::<A>(name, params)
            .await
            .map_err(|e| HostError::ActorSpawnFailure(name.to_string(), e))?;
        handle.bind::<A>(); // since we are not gspawning

        // Finally serve the proc on the same transport as the backend address,
        // and call back.
        let (proc_addr, proc_rx) = channel::serve(ChannelAddr::any(backend_transport)).await?;
        proc.clone().serve(proc_rx);
        channel::dial(callback_addr)?
            .send(proc_addr)
            .await
            .map_err(ChannelError::from)?;

        Ok(proc)
    }

    fn parse_env<T, E>(key: &str) -> Result<T, HostError>
    where
        T: FromStr<Err = E>,
        E: Into<anyhow::Error>,
    {
        std::env::var(key)
            .map_err(|e| HostError::MissingParameter(key.to_string(), e))?
            .parse()
            .map_err(|e: E| HostError::InvalidParameter(key.to_string(), e.into()))
    }
}

#[async_trait]
impl ProcManager for ProcessProcManager {
    fn transport(&self) -> ChannelTransport {
        ChannelTransport::Unix
    }

    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
    ) -> Result<ChannelAddr, HostError> {
        let mut cmd = self.cmd.lock().await;

        let (callback_addr, mut callback_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix)).await?;

        cmd.env("HYPERACTOR_HOST_BACKEND_ADDR", forwarder_addr.to_string());
        cmd.env("HYPERACTOR_HOST_PROC_ID", proc_id.to_string());
        cmd.env("HYPERACTOR_HOST_CALLBACK_ADDR", callback_addr.to_string());

        // TODO: retain, manage lifecycle
        let _process = cmd
            .spawn()
            .map_err(|e| HostError::ProcessSpawnFailure(proc_id, e))?;

        // Now wait for the callback, providing the address:
        Ok(callback_rx.recv().await?)
    }
}

/// Testing support for hosts. This is linked outside of cfg(test)
/// as it is needed by an external binary.
pub mod testing {
    use async_trait::async_trait;

    use crate as hyperactor;
    use crate::Actor;
    use crate::ActorId;
    use crate::Context;
    use crate::Handler;
    use crate::OncePortRef;

    /// Just a simple actor, available in both the bootstrap binary as well as
    /// hyperactor tests.
    #[derive(Debug, Default, Actor)]
    #[hyperactor::export(handlers = [OncePortRef<ActorId>])]
    pub struct EchoActor;

    #[async_trait]
    impl Handler<OncePortRef<ActorId>> for EchoActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            reply: OncePortRef<ActorId>,
        ) -> Result<(), anyhow::Error> {
            reply.send(cx, cx.self_id().clone())?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::testing::EchoActor;
    use super::*;
    use crate::ActorRef;
    use crate::channel::ChannelTransport;
    use crate::context::Mailbox;

    #[tokio::test]
    async fn test_basic() {
        let procs = Arc::new(Mutex::new(HashMap::new()));
        let (mut host, _handle) = Host::serve(
            Box::new(LocalProcManager {
                procs: Arc::clone(&procs),
            }),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let proc_id1 = host.spawn("proc1".to_string()).await.unwrap();
        assert_eq!(
            proc_id1,
            ProcId::Direct(host.addr().clone(), "proc1".to_string())
        );
        assert!(procs.lock().await.contains_key(&proc_id1));

        let proc_id2 = host.spawn("proc2".to_string()).await.unwrap();
        assert!(procs.lock().await.contains_key(&proc_id2));

        let proc1 = procs.lock().await.get(&proc_id1).unwrap().clone();
        let proc2 = procs.lock().await.get(&proc_id2).unwrap().clone();

        // Make sure they can talk to each other:
        let (instance1, _handle) = proc1.instance("client").unwrap();
        let (instance2, _handle) = proc2.instance("client").unwrap();

        let (port, mut rx) = instance1.mailbox().open_port();

        port.bind().send(&instance2, "hello".to_string()).unwrap();
        assert_eq!(rx.recv().await.unwrap(), "hello".to_string());

        // Make sure that the system proc is also wired in correctly.
        let (system_actor, _handle) = host.system_proc().instance("test").unwrap();

        // system->proc
        port.bind()
            .send(&system_actor, "hello from the system proc".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the system proc".to_string()
        );

        // system->system
        let (port, mut rx) = system_actor.mailbox().open_port();
        port.bind()
            .send(&system_actor, "hello from the system".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the system".to_string()
        );

        // proc->system
        port.bind()
            .send(&instance1, "hello from the instance1".to_string())
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            "hello from the instance1".to_string()
        );
    }

    #[tokio::test]
    async fn test_process_proc_manager() {
        hyperactor_telemetry::initialize_logging(crate::clock::ClockKind::default());

        let process_manager = ProcessProcManager::new(Command::new(
            buck_resources::get("monarch/hyperactor/bootstrap").unwrap(),
        ));
        let (mut host, _handle) = Host::serve(
            Box::new(process_manager),
            ChannelAddr::any(ChannelTransport::Unix),
        )
        .await
        .unwrap();

        let proc1 = host.spawn("proc1".to_string()).await.unwrap();
        let proc2 = host.spawn("proc2".to_string()).await.unwrap();

        let echo_actor_1: ActorRef<EchoActor> = ActorRef::attest(proc1.actor_id("echo", 0));
        let echo_actor_2: ActorRef<EchoActor> = ActorRef::attest(proc2.actor_id("echo", 0));

        // These are always direct addressed, so we can reach them with our own proc.
        let test_proc = Proc::direct(
            ChannelAddr::any(host.addr().transport()),
            "test".to_string(),
        )
        .await
        .unwrap();
        let (test_instance, _handle) = test_proc.instance("test").unwrap();

        let (port, rx) = test_instance.mailbox().open_once_port();
        echo_actor_1.send(&test_instance, port.bind()).unwrap();
        assert_eq!(rx.recv().await.unwrap(), *echo_actor_1.actor_id());
    }
}
