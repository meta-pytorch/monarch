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

    /// Create a new direct child client.
    pub fn child(&self) -> Client {
        self.instance.child_client()
    }

    /// Spawn a child actor with a fresh uid labeled from the actor type.
    pub fn spawn<A: Actor>(&self, actor: A) -> anyhow::Result<ActorHandle<A>> {
        self.instance.spawn(actor)
    }

    /// Spawn a child actor with a fresh uid carrying a display label.
    pub fn spawn_with_label<A: Actor>(
        &self,
        label: &str,
        actor: A,
    ) -> anyhow::Result<ActorHandle<A>> {
        self.instance.spawn_with_label(label, actor)
    }

    /// Spawn a child actor using an explicit uid.
    pub fn spawn_with_uid<A: Actor>(&self, uid: Uid, actor: A) -> anyhow::Result<ActorHandle<A>> {
        self.instance.spawn_with_uid(uid, actor)
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

#[cfg(test)]
mod tests {
    use crate::Proc;

    #[test]
    fn client_ids_are_fresh_instances() {
        let proc = Proc::isolated();

        let first = proc.client("caller");
        let second = proc.client("caller");
        let anonymous = proc.client("");

        assert!(first.self_addr().id().uid().is_instance());
        assert!(second.self_addr().id().uid().is_instance());
        assert!(anonymous.self_addr().id().uid().is_instance());
        assert_eq!(
            first.self_addr().id().label().map(|label| label.as_str()),
            Some("caller")
        );
        assert_eq!(anonymous.self_addr().id().label(), None);
        assert_ne!(first.self_addr().id(), second.self_addr().id());
    }

    #[tokio::test]
    async fn dropping_client_closes_mailbox_but_preserves_accepted_messages() {
        let proc = Proc::isolated();
        let receiver = proc.client("receiver");
        let sender = proc.client("sender");
        let (port, mut rx) = receiver.open_port::<u64>();

        port.try_send(&sender, 1).unwrap();
        drop(receiver);

        assert_eq!(rx.recv().await.unwrap(), 1);
        assert!(port.try_send(&sender, 2).is_err());
        assert_eq!(rx.try_recv().unwrap(), None);
    }
}
