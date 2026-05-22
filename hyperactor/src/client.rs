/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Client caller contexts.

use crate as hyperactor;
use crate::Actor;
use crate::ActorAddr;
use crate::Message;
use crate::RemoteMessage;
use crate::actor::Binds;
use crate::context;
use crate::context::Mailbox as MailboxContext;
use crate::mailbox::Mailbox;
use crate::mailbox::OncePortHandle;
use crate::mailbox::OncePortReceiver;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::proc::HandlerPorts;
use crate::proc::Instance;

/// Actor marker type used for client caller contexts.
#[derive(Debug, Default)]
#[hyperactor::export]
pub struct ClientActor;

impl Actor for ClientActor {}

/// A scoped caller context.
///
/// Dropping a client closes its mailbox. Messages already accepted into
/// port receivers remain available; later deliveries to the client's ports
/// fail as ordinary closed-mailbox deliveries.
pub struct Client {
    instance: Instance<ClientActor>,
    lifecycle: Arc<()>,
}

impl Client {
    /// This client's actor address.
    pub fn self_addr(&self) -> &ActorAddr {
        self.instance.self_addr()
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
}

impl Drop for Client {
    fn drop(&mut self) {
        if Arc::strong_count(&self.lifecycle) == 1 {
            self.instance.close_client("client dropped");
        }
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone_for_py(),
            lifecycle: Arc::clone(&self.lifecycle),
        }
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
