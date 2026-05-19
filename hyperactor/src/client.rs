/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Client caller contexts.

use std::fmt;

use crate as hyperactor;
use crate::Actor;
use crate::ActorAddr;
use crate::ActorRef;
use crate::Data;
use crate::Message;
use crate::RemoteMessage;
use crate::actor::ActorHandle;
use crate::actor::ActorStatus;
use crate::actor::AnyActorHandle;
use crate::actor::Binds;
use crate::context;
use crate::context::Mailbox as MailboxContext;
use crate::id::Uid;
use crate::mailbox::Mailbox;
use crate::mailbox::OncePortHandle;
use crate::mailbox::OncePortReceiver;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::ordering::Sequencer;
use crate::proc::HandlerPorts;
use crate::proc::Instance;
use crate::proc::Proc;

/// Actor marker type used for client caller contexts.
#[derive(Debug, Default)]
#[hyperactor::export]
pub struct ClientActor;

impl Actor for ClientActor {}

impl Binds<ClientActor> for () {
    fn bind(_ports: &HandlerPorts<ClientActor>) {}
}

/// A scoped caller context.
///
/// Dropping a client closes its mailbox. Messages already accepted into
/// port receivers remain available; later deliveries to the client's ports
/// fail as ordinary closed-mailbox deliveries.
pub struct Client {
    instance: Instance<ClientActor>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("self_addr", self.self_addr())
            .finish()
    }
}

impl Client {
    pub(crate) fn new(instance: Instance<ClientActor>) -> Self {
        Self { instance }
    }

    /// This client's actor address.
    pub fn self_addr(&self) -> &ActorAddr {
        self.instance.self_addr()
    }

    /// The proc that owns this client.
    pub fn proc(&self) -> &Proc {
        self.instance.proc()
    }

    /// Open a new port that accepts `M`-typed messages.
    pub fn open_port<M: Message>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        self.instance.open_port()
    }

    /// Open a new one-shot port that accepts an `M`-typed message.
    pub fn open_once_port<M: Message>(&self) -> (OncePortHandle<M>, OncePortReceiver<M>) {
        self.instance.open_once_port()
    }

    /// Bind a handler port to this client.
    pub fn bind_handler_port<M: RemoteMessage>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        let (handle, receiver) = self.instance.open_port();
        handle.bind_handler_port();
        (handle, receiver)
    }

    /// Bind this client as an actor ref.
    pub fn bind<R: Binds<ClientActor>>(&self) -> ActorRef<R> {
        self.instance.bind()
    }

    /// Create a new direct child instance.
    pub fn child(&self) -> anyhow::Result<(Instance<()>, ActorHandle<()>)> {
        self.instance.child()
    }

    /// Spawn an actor as this client's child.
    pub fn spawn<A: Actor>(&self, actor: A) -> anyhow::Result<ActorHandle<A>> {
        self.instance.spawn(actor)
    }

    /// Spawn a named actor as this client's child.
    pub fn spawn_with_name<A: Actor>(
        &self,
        name: &str,
        actor: A,
    ) -> anyhow::Result<ActorHandle<A>> {
        self.instance.spawn_with_name(name, actor)
    }

    /// Spawn a registered actor as this client's child.
    pub async fn gspawn(&self, actor_type: &str, params: Data) -> anyhow::Result<AnyActorHandle> {
        self.instance.gspawn(actor_type, params).await
    }

    /// Spawn a registered actor as this client's child using an explicit uid.
    pub async fn gspawn_uid(
        &self,
        actor_type: &str,
        uid: Uid,
        params: Data,
    ) -> anyhow::Result<AnyActorHandle> {
        self.instance.gspawn_uid(actor_type, uid, params).await
    }

    /// Return this client's sequencer.
    pub fn sequencer(&self) -> &Sequencer {
        self.instance.sequencer()
    }

    /// The client's lifecycle status.
    pub fn status(&self) -> tokio::sync::watch::Receiver<ActorStatus> {
        self.instance.status()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.instance.close_client("client dropped");
    }
}

impl context::Mailbox for Client {
    fn mailbox(&self) -> &Mailbox {
        MailboxContext::mailbox(&self.instance)
    }
}

impl context::Mailbox for &Client {
    fn mailbox(&self) -> &Mailbox {
        MailboxContext::mailbox(&self.instance)
    }
}

impl context::Actor for Client {
    type A = ClientActor;

    fn instance(&self) -> &Instance<ClientActor> {
        &self.instance
    }
}

impl context::Actor for &Client {
    type A = ClientActor;

    fn instance(&self) -> &Instance<ClientActor> {
        &self.instance
    }
}
