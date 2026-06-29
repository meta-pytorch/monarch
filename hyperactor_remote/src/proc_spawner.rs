/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Remote proc spawning protocol.
//!
//! [`ProcSpawnerEndpoint`] is the proc-level counterpart to
//! [`crate::ActorSpawnerEndpoint`]. A proc-spawner endpoint accepts [`SpawnProc`],
//! starts a new proc, creates a proc-local [`SpawnActor`] endpoint, and links that
//! actor-spawn endpoint back to the caller through the same remote supervision
//! machinery used for remote actor spawns.

use hyperactor::ActorRef;
use hyperactor::Endpoint;
use hyperactor::Label;
use hyperactor::Location;
use hyperactor::OncePortHandle;
use hyperactor::ProcAddr;
use hyperactor::ProcId;
use hyperactor::Uid;
use hyperactor::context;

use crate::KeepaliveLink;
use crate::SpawnProc;
use crate::SupervisionOptions;
use crate::Supervisor;
use crate::actor_spawner::SpawnActor;

pub mod unix;

// Behavior implemented by actors that expose proc spawning.
hyperactor::behavior!(ProcSpawner, SpawnProc);

/// Convenience methods for endpoints that accept [`SpawnProc`] requests.
pub trait ProcSpawnerEndpoint {
    /// Spawn a proc with a fresh proc uid.
    fn spawn_proc(&self, cx: &impl context::Actor) -> anyhow::Result<ActorRef<SpawnActor>>
    where
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnProc>,
    {
        self.spawn_proc_uid(cx, Uid::anonymous())
    }

    /// Spawn a proc with an explicit proc uid.
    fn spawn_proc_uid(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
    ) -> anyhow::Result<ActorRef<SpawnActor>>
    where
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnProc>,
    {
        self.spawn_proc_uid_with_link(cx, uid, KeepaliveLink::default())
    }

    /// Spawn a proc and report when it becomes reachable.
    ///
    /// The returned [`ActorRef`] names the proc's actor-spawn endpoint, but the
    /// proc has to launch and join before that endpoint can route — messages
    /// sent earlier are dropped. A bare readiness signal is posted to `ready`
    /// once the proc has joined, so callers that need to use the endpoint can
    /// wait for that signal instead of guessing with a delay. The endpoint
    /// address is already known from the synchronously returned [`ActorRef`], so
    /// `ready` carries no payload.
    fn spawn_proc_uid_with_ready(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
        ready: OncePortHandle<()>,
    ) -> anyhow::Result<ActorRef<SpawnActor>>
    where
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnProc>,
    {
        self.spawn_proc_uid_with_link_and_ready(cx, uid, KeepaliveLink::default(), Some(ready))
    }

    /// Spawn a proc with an explicit proc uid and liveness link.
    fn spawn_proc_uid_with_link(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
        liveness: KeepaliveLink,
    ) -> anyhow::Result<ActorRef<SpawnActor>>
    where
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnProc>,
    {
        self.spawn_proc_uid_with_link_and_ready(cx, uid, liveness, None)
    }

    /// Spawn a proc with an explicit proc uid, liveness link, and optional
    /// readiness port. This is the general form behind the other `spawn_proc*`
    /// methods; see [`spawn_proc_uid_with_ready`](Self::spawn_proc_uid_with_ready)
    /// for the readiness semantics.
    fn spawn_proc_uid_with_link_and_ready(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
        liveness: KeepaliveLink,
        ready: Option<OncePortHandle<()>>,
    ) -> anyhow::Result<ActorRef<SpawnActor>>
    where
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnProc>,
    {
        anyhow::ensure!(uid.is_instance(), "spawned procs cannot be singletons");

        let actor_spawner = spawned_actor_spawner(self, &uid);
        let proc_spawner = self.clone();
        cx.instance().spawn(Supervisor::bootstrap_uid(
            liveness,
            SupervisionOptions::default(),
            Uid::anonymous(),
            actor_spawner.actor_addr().clone(),
            ready,
            move |cx, supervise| {
                proc_spawner.post(cx, SpawnProc { uid, supervise });
                Ok(())
            },
        ));
        Ok(actor_spawner)
    }
}

impl<T> ProcSpawnerEndpoint for T where for<'a> &'a T: Endpoint<SpawnProc> {}

/// Well-known singleton name of a proc's actor-spawn endpoint.
pub(crate) const ACTOR_SPAWNER_NAME: &str = "spawner";

/// The singleton uid of a proc's actor-spawn endpoint. The spawner
/// attests the endpoint ref at this uid, and the child proc spawns its
/// `ActorSpawner` under it, so the two agree without a round trip.
pub(crate) fn actor_spawner_uid() -> Uid {
    Uid::singleton(Label::strip(ACTOR_SPAWNER_NAME))
}

pub(crate) fn actor_spawner_ref(proc_addr: &ProcAddr) -> ActorRef<SpawnActor> {
    ActorRef::attest(proc_addr.actor_addr_uid(actor_spawner_uid()))
}

fn spawned_proc_addr(spawner: impl Endpoint<SpawnProc>, uid: &Uid) -> ProcAddr {
    let spawner_addr = spawner.endpoint_location().actor_addr().proc_addr();
    spawned_proc_addr_for_spawner_addr(&spawner_addr, uid)
}

pub(crate) fn spawned_proc_addr_for_spawner_addr(spawner: &ProcAddr, uid: &Uid) -> ProcAddr {
    let location = append_inner_via(spawner.location().clone(), uid.clone());
    ProcAddr::new(ProcId::new(uid.clone(), None), location)
}

fn spawned_actor_spawner(spawner: impl Endpoint<SpawnProc>, uid: &Uid) -> ActorRef<SpawnActor> {
    actor_spawner_ref(&spawned_proc_addr(spawner, uid))
}

fn append_inner_via(location: Location, uid: Uid) -> Location {
    match location {
        Location::Addr(addr) => Location::Addr(addr).with_via(uid),
        Location::Via(via, inner) => Location::Via(via, Box::new(append_inner_via(*inner, uid))),
    }
}

#[cfg(test)]
mod tests {
    use hyperactor::Label;
    use hyperactor::channel::ChannelAddr;

    use super::*;

    #[test]
    fn spawned_proc_addr_preserves_spawner_via_route() {
        let spawner_uid = Uid::instance(Label::new("spawner").unwrap());
        let outer = Uid::instance(Label::new("outer").unwrap());
        let inner = Uid::instance(Label::new("inner").unwrap());
        let child = Uid::instance(Label::new("child").unwrap());
        let terminal = ChannelAddr::Local(123);
        let spawner = ProcAddr::new(
            ProcId::new(spawner_uid, None),
            Location::from(terminal.clone())
                .with_via(inner.clone())
                .with_via(outer.clone()),
        );

        let spawned = spawned_proc_addr_for_spawner_addr(&spawner, &child);

        let (outer_uid, location) = spawned
            .location()
            .as_via()
            .expect("outer via must be preserved");
        assert_eq!(outer_uid, &outer);
        let (inner_uid, location) = location
            .as_via()
            .expect("inner via must be preserved before child");
        assert_eq!(inner_uid, &inner);
        let (child_uid, location) = location
            .as_via()
            .expect("child via must be appended inside spawner route");
        assert_eq!(child_uid, &child);
        assert_eq!(location.as_addr(), Some(&terminal));
    }
}
