/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Capabilities used in various public APIs.

/// CanSend is a capabilty to confers the right of the holder to send
/// messages to actors. CanSend is sealed and may only be implemented
/// and accessed by this crate.
pub trait CanSend: sealed::CanSend {}
impl<T: sealed::CanSend> CanSend for T {}

/// CanOpenPort is a capability that confers the ability of hte holder to
/// open local ports, which can then be used to receive messages.
pub trait CanOpenPort: sealed::CanOpenPort {}
impl<T: sealed::CanOpenPort> CanOpenPort for T {}

/// CanOpenPort is a capability that confers the ability of the holder to
/// split ports.
pub trait CanSplitPort: sealed::CanSplitPort {}
impl<T: sealed::CanSplitPort> CanSplitPort for T {}

/// CanSpawn is a capability that confers the ability to spawn a child
/// actor.
pub trait CanSpawn: sealed::CanSpawn {}
impl<T: sealed::CanSpawn> CanSpawn for T {}

/// CanResolveActorRef is a capability that confers the ability to resolve
/// an ActorRef to a local ActorHandle if the actor is available locally.
pub trait CanResolveActorRef: sealed::CanResolveActorRef {}
impl<T: sealed::CanResolveActorRef> CanResolveActorRef for T {}

/// HasProc is a capability that returns the Proc associated with the caller.
/// Full access to the proc is quite powerful: we should consider some kind of
/// "read only" proc trait.
pub trait HasProc {
    /// The proc associated with the caller.
    fn proc(&self) -> &crate::Proc;
}

pub(crate) mod sealed {
    use async_trait::async_trait;

    use crate::ActorId;
    use crate::ActorRef;
    use crate::PortId;
    use crate::accum::ReducerSpec;
    use crate::actor::Actor;
    use crate::actor::ActorHandle;
    use crate::actor::RemoteActor;
    use crate::attrs::Attrs;
    use crate::data::Serialized;
    use crate::mailbox::Mailbox;

    pub trait CanSend: Send + Sync {
        fn post(&self, dest: PortId, headers: Attrs, data: Serialized);
        fn actor_id(&self) -> &ActorId;
    }

    pub trait CanOpenPort: Send + Sync {
        fn mailbox(&self) -> &Mailbox;
    }

    pub trait CanSplitPort: Send + Sync {
        fn split(
            &self,
            port_id: PortId,
            reducer_spec: Option<ReducerSpec>,
        ) -> anyhow::Result<PortId>;
    }

    #[async_trait]
    pub trait CanSpawn: Send + Sync {
        async fn spawn<A: Actor>(&self, params: A::Params) -> anyhow::Result<ActorHandle<A>>;
    }

    pub trait CanResolveActorRef: Send + Sync {
        fn resolve_actor_ref<A: RemoteActor>(
            &self,
            actor_ref: &ActorRef<A>,
        ) -> Option<ActorHandle<A>>;
    }
}
