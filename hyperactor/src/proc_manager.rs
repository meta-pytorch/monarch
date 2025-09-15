/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ProcManager trait and its concrete implementations.
//!
//! A `ProcManager` is responsible for creating and managing procs
//! under a [`Host`](crate::host::Host). It defines the contract for
//! how procs are bootstrapped, how their lifetime is managed, and how
//! the host can reach their mailbox.
//!
//! Current implementations:
//! - [`LocalProcManager`] spawns procs in-process, mainly for
//!   testing.
//! - [`ProcessProcManager`] spawns each proc as a separate OS
//!   process, using the environment-variable bootstrap protocol.
//!
//! Future extensions will add richer lifecycle control (shutdown,
//! restart, status queries) and observability hooks.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::Actor;
use crate::ActorRef;
use crate::Proc;
use crate::ProcId;
use crate::actor::Binds;
use crate::actor::RemoteActor;
use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;
use crate::channel::Rx;
use crate::channel::{self};
use crate::host_types::HostError;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::MailboxClient;
use crate::mailbox::MailboxServer;

/// A trait describing a manager of procs, responsible for bootstrapping
/// procs on a host, and managing their lifetimes. The manager spawns an
/// `Agent`-typed actor on each proc, responsible for managing the proc.
#[async_trait]
pub trait ProcManager {
    /// The type of agent actor launched on the proc.
    type Agent: Actor + RemoteActor;

    /// The preferred transport for this ProcManager.
    /// In practice this will be [`ChannelTransport::Local`]
    /// for testing, and [`ChannelTransport::Unix`] for external
    /// processes.
    fn transport(&self) -> ChannelTransport;

    /// Spawn a new proc with the provided proc id. The proc
    /// should use the provided forwarder address for messages
    /// destined outside of the proc. The returned address accepts
    /// messages destined for the proc.
    ///
    /// An agent actor is also spawned, and the corresponding actor
    /// ref is returned.
    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
    ) -> Result<(ChannelAddr, ActorRef<Self::Agent>), HostError>;

    // TODO: full lifecycle management; perhaps mimick the Command API.
}

/// A ProcManager that spawns into local (in-process) procs. Used for
/// testing.
pub struct LocalProcManager<A: Actor> {
    procs: Arc<Mutex<HashMap<ProcId, Proc>>>,
    params: A::Params,
}

impl<A: Actor> LocalProcManager<A> {
    /// Create a new in-process proc manager with the given agent
    /// params.
    pub fn new(params: A::Params) -> Self {
        Self {
            procs: Arc::new(Mutex::new(HashMap::new())),
            params,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn procs(&self) -> Arc<Mutex<HashMap<ProcId, Proc>>> {
        Arc::clone(&self.procs)
    }
}

#[async_trait]
impl<A> ProcManager for LocalProcManager<A>
where
    A: Actor + RemoteActor + Binds<A>,
    A::Params: Sync + Clone,
{
    type Agent = A;

    fn transport(&self) -> ChannelTransport {
        ChannelTransport::Local
    }

    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
    ) -> Result<(ChannelAddr, ActorRef<A>), HostError> {
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
        let agent_handle = proc
            .spawn("agent", self.params.clone())
            .await
            .map_err(|e| HostError::AgentSpawnFailure(proc_id, e))?;
        Ok((proc_addr, agent_handle.bind()))
    }
}

/// A ProcManager that manages each proc as a separate process.
/// It follows a simple protocol:
///
/// Each process is launched with the following environment variables:
/// - `HYPERACTOR_HOST_BACKEND_ADDR`: the backend address to which all messages are forwarded,
/// - `HYPERACTOR_HOST_PROC_ID`: the proc id to assign the launched proc, and
/// - `HYPERACTOR_HOST_CALLBACK_ADDR`: the channel address with which to return the proc's address
///
/// The launched proc should also spawn an actor to manage it - the details of this are
/// implementation dependent, and outside the scope of the process manager.
///
/// The function [`boot_proc`] provides a convenient implementation of the
/// protocol.
pub struct ProcessProcManager<A> {
    cmd: Arc<Mutex<Command>>,
    children: Arc<Mutex<HashMap<ProcId, Child>>>,
    _phantom: PhantomData<A>,
}

impl<A> ProcessProcManager<A> {
    /// Create a new ProcessProcManager that runs the provided
    /// command.
    pub fn new(cmd: Command) -> Self {
        Self {
            cmd: Arc::new(Mutex::new(cmd)),
            children: Arc::new(Mutex::new(HashMap::new())),
            _phantom: PhantomData,
        }
    }
}

impl<A> Drop for ProcessProcManager<A> {
    fn drop(&mut self) {
        // When the manager is dropped, `children` is dropped, which
        // drops each `Child` handle. With `kill_on_drop(true)`, the OS
        // will SIGKILL the processes. Nothing else to do here.
    }
}

#[async_trait]
impl<A: Actor + RemoteActor> ProcManager for ProcessProcManager<A> {
    type Agent = A;

    fn transport(&self) -> ChannelTransport {
        ChannelTransport::Unix
    }

    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
    ) -> Result<(ChannelAddr, ActorRef<A>), HostError> {
        let mut cmd = self.cmd.lock().await;

        let (callback_addr, mut callback_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Unix)).await?;

        cmd.env("HYPERACTOR_HOST_BACKEND_ADDR", forwarder_addr.to_string());
        cmd.env("HYPERACTOR_HOST_PROC_ID", proc_id.to_string());
        cmd.env("HYPERACTOR_HOST_CALLBACK_ADDR", callback_addr.to_string());

        // Lifetime strategy: mark the child with
        // `kill_on_drop(true)` so the OS will send SIGKILL if the
        // handle is dropped and retain the `Child` in
        // `self.children`, tying its lifetime to the manager/host.
        //
        // This is the simplest viable policy to avoid orphaned
        // subprocesses in CI; more sophisticated lifecycle control
        // (graceful shutdown, restart) will be layered on later.

        // Kill the child when its handle is dropped.
        cmd.kill_on_drop(true);

        let child = cmd
            .spawn()
            .map_err(|e| HostError::ProcessSpawnFailure(proc_id.clone(), e))?;

        // Retain the handle so it lives for the life of the
        // manager/host.
        {
            let mut children = self.children.lock().await;
            children.insert(proc_id.clone(), child);
        }

        // Now wait for the callback, providing the address.
        Ok(callback_rx.recv().await?)
    }
}
