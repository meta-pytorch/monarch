/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines traits that are used as context arguments to various
//! hyperactor APIs; usually [`crate::context::Actor`], implemented by
//! [`crate::proc::Context`] (provided to actor handlers) and [`crate::proc::Instance`],
//! representing a running actor instance.
//!
//! Context traits are sealed, and thus can only be implemented by data types in the
//! core hyperactor crate.

use async_trait::async_trait;

use crate::ActorHandle;
use crate::ActorId;
use crate::ActorRef;
use crate::Instance;
use crate::PortId;
use crate::Proc;
use crate::accum::ReducerSpec;
use crate::actor::RemoteActor;
use crate::attrs::Attrs;
use crate::cap;
use crate::data::Serialized;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;

/// A mailbox context provides a mailbox.
pub trait Mailbox: crate::private::Sealed {
    /// The mailbox associated with this context
    fn mailbox(&self) -> &crate::Mailbox;
}

/// A typed actor context, providing both a [`Mailbox`] and an [`Instance`].
///
/// Note: Send and Sync markers are here only temporarily in order to bridge
/// the transition to the context types, away from the [`crate::cap`] module.
#[async_trait]
pub trait Actor: Mailbox + Send + Sync {
    /// The type of actor associated with this context.
    type A: crate::Actor;

    /// The instance associated with this context.
    fn instance(&self) -> &Instance<Self::A>;
}

// The following are forwarding traits, used to ease transition to the new
// context types.

impl<T: Mailbox + Send + Sync> cap::sealed::CanOpenPort for T {
    fn mailbox(&self) -> &crate::Mailbox {
        <Self as Mailbox>::mailbox(self)
    }
}

impl<T: Mailbox + Send + Sync> cap::sealed::CanSplitPort for T {
    fn split(&self, port_id: PortId, reducer_spec: Option<ReducerSpec>) -> anyhow::Result<PortId> {
        self.mailbox().split(port_id, reducer_spec)
    }
}

/// Only actors CanSend because they need a return port.
impl<T: Actor + Send + Sync> cap::sealed::CanSend for T {
    fn post(&self, dest: PortId, headers: Attrs, data: Serialized) {
        let envelope = MessageEnvelope::new(self.actor_id().clone(), dest, data, headers);
        MailboxSender::post(self.mailbox(), envelope, self.instance().port());
    }
    fn actor_id(&self) -> &ActorId {
        self.mailbox().actor_id()
    }
}

#[async_trait]
impl<T: Actor + Send + Sync> cap::sealed::CanSpawn for T {
    async fn spawn<C: crate::Actor>(&self, params: C::Params) -> anyhow::Result<ActorHandle<C>> {
        self.instance().spawn(params).await
    }
}

impl<T: Actor + Send + Sync> cap::sealed::CanResolveActorRef for T {
    fn resolve_actor_ref<R: RemoteActor + crate::Actor>(
        &self,
        actor_ref: &ActorRef<R>,
    ) -> Option<ActorHandle<R>> {
        self.instance().proc().resolve_actor_ref(actor_ref)
    }
}

impl<T: Actor + Send + Sync> cap::HasProc for T {
    fn proc(&self) -> &Proc {
        self.instance().proc()
    }
}
