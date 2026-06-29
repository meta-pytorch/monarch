/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Actor interface for remotely spawning actors.
//!
//! [`ActorSpawner`] is a spawn factory actor. A proc that wants to accept
//! dynamic actor spawns runs an `ActorSpawner` locally and exposes an
//! [`ActorRef<ActorSpawner>`] to callers, for example through a token
//! rendezvous. A caller can post [`SpawnActor`] directly, but most callers
//! should use [`ActorSpawnerEndpoint`]. The extension trait builds the spawn
//! request, starts the local supervisor that owns the remote actor lifetime,
//! and returns the [`ActorRef`] for the actor that will be created on the
//! remote proc.
//!
//! Spawning through [`ActorSpawnerEndpoint`] makes the spawning actor the
//! supervisor of the spawned remote actor. Logically, the remote actor becomes
//! part of the spawner's supervision tree even though it runs on another proc:
//!
//! ```text
//! caller proc                         remote proc
//!
//! caller actor
//! `- spawned actor A  -------- runs on -------->  ActorSpawner's proc
//! ```
//!
//! The implementation realizes that logical edge with a caller-side supervisor,
//! a remote worker, and a cross-proc session. Boxes are actors. Arrow labels
//! are protocol messages.
//!
//! ```text
//! caller proc                                      remote proc
//!
//! caller actor
//! `- [Supervisor] -- SpawnActor ---------------->  [ActorSpawner]
//!                                                `- [Worker]
//!                                                   `- [spawned actor A]
//!
//! [Supervisor] <-------- SupervisorEvent -------- [Worker]
//! ```
//!
//! The caller-side [`Supervisor`] is spawned as a child of the actor that
//! requested the remote spawn. Its bootstrap posts [`SpawnActor`] to the remote
//! [`ActorSpawner`]. The remote [`ActorSpawner`] spawns a [`Worker`] child and asks it
//! to deserialize the [`Gspawn`] specification, spawn the requested actor as its
//! own child, and attach it to the caller-side supervisor. The caller-side
//! supervisor also owns the private liveness machinery that lets either proc
//! observe when the other side disappears.
//!
//! The resulting lifetime is rooted in the caller's actor tree. If the caller
//! tree stops, `Supervisor` forwards a stop request to `Worker`, and `Worker`
//! stops the spawned actor. If the spawned actor fails, `Worker` reports the
//! supervision event to `Supervisor`, which re-raises it into the caller's
//! supervision tree. If the supervisor session fails, the surviving side turns
//! that into a terminal supervision event instead of leaving an orphaned remote
//! actor.
//!
//! A typical caller only needs the extension trait. The returned [`ActorRef`]
//! names where the actor will run, but the remote proc has to spawn and link it
//! before it can route — messages sent earlier are dropped. Spawn with a
//! readiness port and wait for the signal before messaging the actor:
//!
//! ```rust,ignore
//! use hyperactor::ActorRef;
//! use hyperactor::Context;
//! use hyperactor::Handler;
//! use hyperactor::Label;
//! use hyperactor::RemoteSpawn;
//! use hyperactor::Uid;
//! use hyperactor_remote::ActorSpawner;
//! use hyperactor_remote::ActorSpawnerEndpoint;
//!
//! struct Driver {
//!     actor_spawner: ActorRef<ActorSpawner>,
//! }
//!
//! impl Handler<Start> for Driver {
//!     async fn handle(&mut self, cx: &Context<Self>, _start: Start) -> anyhow::Result<()> {
//!         let (ready, ready_rx) = cx.open_once_port::<()>();
//!         let calculator = self.actor_spawner.spawn_uid_with_ready::<Calculator>(
//!             cx,
//!             Uid::instance(Label::new("calculator").unwrap()),
//!             CalculatorParams { initial_value: 0 },
//!             ready,
//!         )?;
//!         // The returned ref names the calculator, but it cannot route until
//!         // the remote proc spawns and links it. Wait for the readiness signal
//!         // first, then message the actor we already hold a ref to.
//!         ready_rx.recv().await?;
//!         calculator.post(cx, Add { lhs: 40, rhs: 2 });
//!         Ok(())
//!     }
//! }
//!
//! # struct Start;
//! # struct Calculator;
//! # struct CalculatorParams {
//! #     initial_value: i64,
//! # }
//! # struct Add {
//! #     lhs: i64,
//! #     rhs: i64,
//! # }
//! # impl RemoteSpawn for Calculator {
//! #     type Params = CalculatorParams;
//! # }
//! ```

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint;
use hyperactor::Handler;
use hyperactor::OncePortHandle;
use hyperactor::RemoteSpawn;
use hyperactor::Uid;
use hyperactor::context;

use crate::Gspawn;
use crate::KeepaliveLink;
use crate::SpawnActor as SpawnActorMessage;
use crate::SupervisionOptions;
use crate::Supervisor;
use crate::Worker;
use crate::supervision::GspawnAndSupervise;

// Actor-spawn interface exposed by an actor spawner.
hyperactor::behavior!(SpawnActor, SpawnActorMessage);

/// Remote actor that accepts dynamic remote spawn requests.
#[derive(Debug, Default)]
#[hyperactor::export(SpawnActorMessage)]
#[hyperactor::spawnable]
pub struct ActorSpawner;

#[async_trait]
impl Actor for ActorSpawner {}

#[async_trait]
impl Handler<SpawnActorMessage> for ActorSpawner {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: SpawnActorMessage,
    ) -> anyhow::Result<()> {
        let SpawnActorMessage { gspawn, supervise } = message;
        let worker = context::Actor::instance(cx).spawn(Worker::new());
        worker.post(cx, GspawnAndSupervise::new(gspawn, supervise));
        Ok(())
    }
}

hyperactor::assert_behaves!(ActorSpawner as SpawnActor);

/// Convenience methods for endpoints that accept [`SpawnActor`] requests.
pub trait ActorSpawnerEndpoint {
    /// Spawn a registered actor with a fresh actor uid.
    fn spawn<A>(&self, cx: &impl context::Actor, params: A::Params) -> anyhow::Result<ActorRef<A>>
    where
        A: RemoteSpawn,
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnActorMessage>,
    {
        self.spawn_uid::<A>(cx, Uid::anonymous(), params)
    }

    /// Spawn a registered actor with an explicit actor uid.
    fn spawn_uid<A>(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
        params: A::Params,
    ) -> anyhow::Result<ActorRef<A>>
    where
        A: RemoteSpawn,
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnActorMessage>,
    {
        self.spawn_uid_with_link::<A>(cx, uid, params, KeepaliveLink::default())
    }

    /// Spawn a registered actor and report when it becomes reachable.
    ///
    /// The returned [`ActorRef`] names where the actor will run, but the remote
    /// proc has to spawn and link it before it can route — messages sent earlier
    /// are dropped. A bare readiness signal is posted to `ready` once the actor
    /// has linked, so callers that need to message it can wait for that signal
    /// instead of guessing with a delay. The address is already known from the
    /// synchronously returned [`ActorRef`], so `ready` carries no payload.
    fn spawn_uid_with_ready<A>(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
        params: A::Params,
        ready: OncePortHandle<()>,
    ) -> anyhow::Result<ActorRef<A>>
    where
        A: RemoteSpawn,
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnActorMessage>,
    {
        self.spawn_uid_with_link_and_ready::<A>(
            cx,
            uid,
            params,
            KeepaliveLink::default(),
            Some(ready),
        )
    }

    /// Spawn a registered actor with an explicit actor uid and liveness link.
    fn spawn_uid_with_link<A>(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
        params: A::Params,
        liveness: KeepaliveLink,
    ) -> anyhow::Result<ActorRef<A>>
    where
        A: RemoteSpawn,
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnActorMessage>,
    {
        self.spawn_uid_with_link_and_ready::<A>(cx, uid, params, liveness, None)
    }

    /// Spawn a registered actor with an explicit actor uid, liveness link, and
    /// optional readiness port. This is the general form behind the other
    /// `spawn*` methods; see [`spawn_uid_with_ready`](Self::spawn_uid_with_ready)
    /// for the readiness semantics.
    fn spawn_uid_with_link_and_ready<A>(
        &self,
        cx: &impl context::Actor,
        uid: Uid,
        params: A::Params,
        liveness: KeepaliveLink,
        ready: Option<OncePortHandle<()>>,
    ) -> anyhow::Result<ActorRef<A>>
    where
        A: RemoteSpawn,
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnActorMessage>,
    {
        // We can safely compute the resulting actor ref: by definition it
        // will be on the same proc as the actor spawner itself, and we use a
        // Uid to identify it.
        anyhow::ensure!(
            uid.is_instance(),
            "ActorSpawner-spawned actors cannot be singletons"
        );
        let actor_ref = ActorRef::attest(
            self.endpoint_location()
                .actor_addr()
                .proc_addr()
                .actor_addr_uid(uid.clone()),
        );
        let actor_spawner = self.clone();
        let gspawn = Gspawn::for_actor_uid::<A>(uid, params)?;
        cx.instance().spawn(Supervisor::bootstrap_uid(
            liveness,
            SupervisionOptions::default(),
            Uid::anonymous(),
            actor_ref.actor_addr().clone(),
            ready,
            move |cx, supervise| {
                actor_spawner.post(cx, SpawnActorMessage { gspawn, supervise });
                Ok(())
            },
        ));
        Ok(actor_ref)
    }
}

impl<T> ActorSpawnerEndpoint for T where for<'a> &'a T: Endpoint<SpawnActorMessage> {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::ActorAddr;
    use hyperactor::ActorHandle;
    use hyperactor::Context;
    use hyperactor::Endpoint as _;
    use hyperactor::Handler;
    use hyperactor::Instance;
    use hyperactor::Label;
    use hyperactor::OncePortHandle;
    use hyperactor::PortRef;
    use hyperactor::Proc;
    use hyperactor::RemoteSpawn;
    use hyperactor::Uid;
    use hyperactor::supervision::ActorSupervisionEvent;
    use hyperactor_config::Flattrs;
    use serde::Deserialize;
    use serde::Serialize;
    use typeuri::Named;

    use super::*;
    use crate::Gspawn;
    use crate::KeepaliveLink;
    use crate::RemoteActorDisposition;
    use crate::Supervise;
    use crate::SupervisionOptions;
    use crate::SupervisorEvent;

    #[derive(Clone, Debug, Serialize, Deserialize, Named)]
    struct Fail;
    wirevalue::register_type!(Fail);

    #[derive(Debug)]
    #[hyperactor::export(Fail)]
    struct TestChild;

    #[async_trait]
    impl Actor for TestChild {}

    #[async_trait]
    impl RemoteSpawn for TestChild {
        type Params = ();

        async fn new(_params: (), _environment: Flattrs) -> anyhow::Result<Self> {
            Ok(Self)
        }
    }

    #[async_trait]
    impl Handler<Fail> for TestChild {
        async fn handle(&mut self, _cx: &Context<Self>, _message: Fail) -> anyhow::Result<()> {
            anyhow::bail!("test child failed")
        }
    }

    hyperactor::register_spawnable!(TestChild);

    #[derive(Debug)]
    #[hyperactor::export(handlers = [])]
    struct FailingChild;

    #[async_trait]
    impl Actor for FailingChild {
        async fn init(&mut self, _this: &Instance<Self>) -> anyhow::Result<()> {
            anyhow::bail!("failing child init")
        }
    }

    #[async_trait]
    impl RemoteSpawn for FailingChild {
        type Params = ();

        async fn new(_params: (), _environment: Flattrs) -> anyhow::Result<Self> {
            Ok(Self)
        }
    }

    hyperactor::register_spawnable!(FailingChild);

    #[derive(Debug)]
    struct PlainSpawner {
        actor_spawner: ActorHandle<ActorSpawner>,
        spawned: PortRef<ActorAddr>,
        events: PortRef<ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for PlainSpawner {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let actor_ref = self.actor_spawner.spawn_uid::<FailingChild>(
                this,
                Uid::instance(Label::new("failing_child").unwrap()),
                (),
            )?;
            self.spawned.post(this, actor_ref.actor_addr().clone());
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.post(this, event.clone());
            this.exit("supervision event observed")?;
            Ok(true)
        }
    }

    #[derive(Debug)]
    struct ReadySpawner {
        actor_spawner: ActorHandle<ActorSpawner>,
        spawned: PortRef<ActorAddr>,
        ready: Option<OncePortHandle<()>>,
    }

    #[async_trait]
    impl Actor for ReadySpawner {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let ready = self.ready.take().expect("ready spawner initialized once");
            let actor_ref = self.actor_spawner.spawn_uid_with_ready::<TestChild>(
                this,
                Uid::instance(Label::new("ready_child").unwrap()),
                (),
                ready,
            )?;
            self.spawned.post(this, actor_ref.actor_addr().clone());
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            _this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            // Absorb teardown events so cleanup doesn't surface as a root failure.
            Ok(!event.is_error())
        }
    }

    fn encoded_unit() -> Vec<u8> {
        bincode::serde::encode_to_vec((), bincode::config::legacy()).unwrap()
    }

    async fn spawn_test_child(
        actor_spawner: &ActorHandle<ActorSpawner>,
        client: &hyperactor::Client,
        supervisor: PortRef<SupervisorEvent>,
        session_id: Uid,
    ) {
        let (_link_handle, liveness) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn_supervisor(client)
                .unwrap();
        actor_spawner.post(
            client,
            SpawnActorMessage {
                gspawn: Gspawn::for_actor_uid::<TestChild>(
                    Uid::instance(Label::new("test_child").unwrap()),
                    (),
                )
                .unwrap(),
                supervise: Supervise {
                    session_id,
                    supervisor,
                    parent: client.self_addr().clone(),
                    liveness,
                    options: SupervisionOptions::default(),
                },
            },
        );
    }

    #[tokio::test]
    async fn test_spawn_actor_endpoint_works_from_plain_actor() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let actor_spawner = proc.spawn(ActorSpawner);
        let (spawned, mut spawned_rx) = client.open_port::<ActorAddr>();
        let (events, mut events_rx) = client.open_port::<ActorSupervisionEvent>();
        let spawner = proc.spawn(PlainSpawner {
            actor_spawner: actor_spawner.clone(),
            spawned: spawned.bind(),
            events: events.bind(),
        });

        let spawned_actor = tokio::time::timeout(Duration::from_secs(5), spawned_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), events_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(event.is_error());
        assert_eq!(event.actor_id, spawned_actor);

        actor_spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), spawner)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), actor_spawner)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_spawn_uid_with_ready_signals_when_child_links() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let actor_spawner = proc.spawn(ActorSpawner);
        let (spawned, mut spawned_rx) = client.open_port::<ActorAddr>();
        let (ready, ready_rx) = client.open_once_port::<()>();
        let spawner = proc.spawn(ReadySpawner {
            actor_spawner: actor_spawner.clone(),
            spawned: spawned.bind(),
            ready: Some(ready),
        });

        let _attested = tokio::time::timeout(Duration::from_secs(5), spawned_rx.recv())
            .await
            .unwrap()
            .unwrap();
        // The readiness port fires once the child links and becomes reachable.
        // It carries no payload — the caller already holds the address from the
        // synchronously returned ref — so receiving it is the reachability signal.
        tokio::time::timeout(Duration::from_secs(5), ready_rx.recv())
            .await
            .unwrap()
            .unwrap();

        spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), spawner)
            .await
            .unwrap();
        actor_spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), actor_spawner)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_actor_spawner_worker_reports_child_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let actor_spawner = proc.spawn(ActorSpawner);
        let (supervisor, mut supervisor_rx) = client.open_port::<SupervisorEvent>();
        let session_id = Uid::instance(Label::new("session").unwrap());

        spawn_test_child(
            &actor_spawner,
            &client,
            supervisor.bind(),
            session_id.clone(),
        )
        .await;
        let linked = tokio::time::timeout(Duration::from_secs(5), supervisor_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (session_id, child) = match linked {
            SupervisorEvent::Linked {
                session_id, child, ..
            } => (session_id, child),
            message => panic!("expected linked message, got {:?}", message),
        };
        PortRef::<Fail>::attest_handler_port(&child).post(&client, Fail);

        let event = tokio::time::timeout(Duration::from_secs(5), supervisor_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            SupervisorEvent::SupervisionEvent {
                session_id: event_session_id,
                event: ActorSupervisionEvent { actor_id, .. },
                disposition: RemoteActorDisposition::Terminal,
            } if event_session_id == session_id && actor_id == child => {}
            message => panic!("expected terminal supervision event, got {:?}", message),
        }

        actor_spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), actor_spawner)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_actor_spawner_rejects_unknown_actor_type() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let actor_spawner = proc.spawn(ActorSpawner);
        let (supervisor, mut supervisor_rx) = client.open_port::<SupervisorEvent>();
        let (_link_handle, liveness) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn_supervisor(&client)
                .unwrap();
        let session_id = Uid::instance(Label::new("session").unwrap());

        actor_spawner.post(
            &client,
            SpawnActorMessage {
                gspawn: Gspawn::with_uid("unknown::Actor", Uid::anonymous(), encoded_unit()),
                supervise: Supervise {
                    session_id: session_id.clone(),
                    supervisor: supervisor.bind(),
                    parent: client.self_addr().clone(),
                    liveness,
                    options: SupervisionOptions::default(),
                },
            },
        );

        let rejected = tokio::time::timeout(Duration::from_secs(5), supervisor_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match rejected {
            SupervisorEvent::SuperviseRejected {
                session_id: rejected_session_id,
                reason,
            } if rejected_session_id == session_id
                && reason.contains("actor type unknown::Actor not registered") => {}
            message => panic!("expected supervise rejection, got {:?}", message),
        }

        actor_spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), actor_spawner)
            .await
            .unwrap();
    }
}
