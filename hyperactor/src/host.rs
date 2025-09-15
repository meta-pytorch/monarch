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

use futures::Future;

use crate::Actor;
use crate::ActorHandle;
use crate::ActorRef;
use crate::PortHandle;
use crate::Proc;
use crate::ProcId;
use crate::actor::Binds;
use crate::actor::RemoteActor;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::Tx;
use crate::host_types::HostError;
use crate::mailbox::BoxableMailboxSender;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender;
use crate::mailbox::MailboxClient;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxServer;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::Undeliverable;
use crate::proc_manager::ProcManager;

/// A host, managing the lifecycle of several procs, and their backend
/// routing, as described in this module's documentation.
pub struct Host<M> {
    procs: HashMap<String, ChannelAddr>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    router: DialMailboxRouter,
    manager: M,
    system_proc: Proc,
}

impl<M: ProcManager> Host<M> {
    /// Serve a host using the provided ProcManager, on the provided `addr`.
    /// On success, the host will multiplex messages for procs on the host
    /// on the address of the host.
    pub async fn serve(
        manager: M,
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
    pub async fn spawn(&mut self, name: String) -> Result<(ProcId, ActorRef<M::Agent>), HostError> {
        if self.procs.contains_key(&name) {
            return Err(HostError::ProcExists(name));
        }

        let proc_id = ProcId::Direct(self.frontend_addr.clone(), name.clone());
        let (addr, agent_ref) = self
            .manager
            .spawn(proc_id.clone(), self.backend_addr.clone())
            .await?;

        self.router.bind(proc_id.clone().into(), addr.clone());
        self.procs.insert(name, addr);
        Ok((proc_id, agent_ref))
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

/// Boot a process in a ProcessProcManager<A>. Should be called from processes spawned
/// by the process manager. `boot_proc` will spawn the provided actor type (with parameters)
/// onto the newly created Proc, and bind its handler. This allows the user to install an agent to
/// manage the proc itself.
pub async fn boot_proc<A, S, F>(spawn: S) -> Result<Proc, HostError>
where
    A: Actor + RemoteActor + Binds<A>,
    S: FnOnce(Proc) -> F,
    F: Future<Output = Result<ActorHandle<A>, anyhow::Error>>,
{
    let backend_addr: ChannelAddr = parse_env("HYPERACTOR_HOST_BACKEND_ADDR")?;
    let proc_id: ProcId = parse_env("HYPERACTOR_HOST_PROC_ID")?;
    let callback_addr: ChannelAddr = parse_env("HYPERACTOR_HOST_CALLBACK_ADDR")?;
    let backend_transport = backend_addr.transport();
    let proc = Proc::new(
        proc_id.clone(),
        MailboxClient::dial(backend_addr)?.into_boxed(),
    );

    let agent_handle = spawn(proc.clone())
        .await
        .map_err(|e| HostError::AgentSpawnFailure(proc_id, e))?;

    // Finally serve the proc on the same transport as the backend address,
    // and call back.
    let (proc_addr, proc_rx) = channel::serve(ChannelAddr::any(backend_transport)).await?;
    proc.clone().serve(proc_rx);
    channel::dial(callback_addr)?
        .send((proc_addr, agent_handle.bind::<A>()))
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

    use tokio::process::Command;

    use super::testing::EchoActor;
    use super::*;
    use crate::channel::ChannelTransport;
    use crate::context::Mailbox;
    use crate::host_types::HostError;
    use crate::proc_manager::LocalProcManager;
    use crate::proc_manager::ProcessProcManager;

    #[tokio::test]
    async fn test_basic() {
        let proc_manager = LocalProcManager::<()>::new(());
        let procs = Arc::clone(&proc_manager.procs());
        let (mut host, _handle) =
            Host::serve(proc_manager, ChannelAddr::any(ChannelTransport::Local))
                .await
                .unwrap();

        let (proc_id1, _ref) = host.spawn("proc1".to_string()).await.unwrap();
        assert_eq!(
            proc_id1,
            ProcId::Direct(host.addr().clone(), "proc1".to_string())
        );
        assert!(procs.lock().await.contains_key(&proc_id1));

        let (proc_id2, _ref) = host.spawn("proc2".to_string()).await.unwrap();
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

        // EchoActor is "agent" used to test connectivity.
        let process_manager = ProcessProcManager::<EchoActor>::new(Command::new(
            buck_resources::get("monarch/hyperactor/bootstrap").unwrap(),
        ));
        let (mut host, _handle) =
            Host::serve(process_manager, ChannelAddr::any(ChannelTransport::Unix))
                .await
                .unwrap();

        // (1) Spawn and check invariants.
        assert!(matches!(host.addr().transport(), ChannelTransport::Unix));
        let (proc1, echo1) = host.spawn("proc1".to_string()).await.unwrap();
        let (proc2, echo2) = host.spawn("proc2".to_string()).await.unwrap();
        assert_eq!(echo1.actor_id().proc_id(), &proc1);
        assert_eq!(echo2.actor_id().proc_id(), &proc2);

        // (2) Duplicate name rejection.
        let dup = host.spawn("proc1".to_string()).await;
        assert!(matches!(dup, Err(HostError::ProcExists(_))));

        // (3) Create a standalone client proc and verify echo1 agent responds.
        // Request: client proc -> host frontend/router -> echo1 (proc1).
        // Reply:   echo1 (proc1) -> host backend -> host router -> client port.
        // This confirms that an external proc (created via
        // `Proc::direct`) can address a child proc through the host,
        // and receive a correct reply.
        let client = Proc::direct(
            ChannelAddr::any(host.addr().transport()),
            "test".to_string(),
        )
        .await
        .unwrap();
        let (client_inst, _h) = client.instance("test").unwrap();
        let (port, rx) = client_inst.mailbox().open_once_port();
        echo1.send(&client_inst, port.bind()).unwrap();
        #[allow(clippy::disallowed_methods)]
        let id = tokio::time::timeout(tokio::time::Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id, *echo1.actor_id());

        // (4) Child <-> external client request -> reply:
        // Request: client proc (standalone via `Proc::direct`) ->
        //          host frontend/router -> echo2 (proc2).
        // Reply:   echo2 (proc2) -> host backend -> host router ->
        //          client port (standalone proc).
        // This exercises cross-proc routing between a child and an
        // external client under the same host.
        let (port2, rx2) = client_inst.mailbox().open_once_port();
        echo2.send(&client_inst, port2.bind()).unwrap();
        #[allow(clippy::disallowed_methods)]
        let id2 = tokio::time::timeout(tokio::time::Duration::from_secs(5), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id2, *echo2.actor_id());

        // (5) System -> child request -> cross-proc reply:
        // Request: system proc -> host router (frontend) -> echo1
        //          (proc1, child).
        // Reply: echo1 (proc1) -> proc1 forwarder -> host backend ->
        //        host router -> client proc direct addr (Proc::direct) ->
        //        client port.
        // Because `client_inst` runs in its own proc, the reply
        // traverses the host (not local delivery within proc1).
        let (sys_inst, _h) = host.system_proc().instance("sys-client").unwrap();
        let (port3, rx3) = client_inst.mailbox().open_once_port();
        // Send from system -> child via a message that ultimately
        // replies to client's port
        echo1.send(&sys_inst, port3.bind()).unwrap();
        #[allow(clippy::disallowed_methods)]
        let id3 = tokio::time::timeout(tokio::time::Duration::from_secs(5), rx3.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id3, *echo1.actor_id());
    }
}
