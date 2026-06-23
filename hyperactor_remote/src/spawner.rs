/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Actor interface for remotely spawning actors.
//!
//! [`RemoteSpawner`] is a spawn factory actor. A proc that wants to accept
//! dynamic actor spawns runs a `RemoteSpawner` locally and exposes an
//! [`ActorRef<RemoteSpawner>`] to callers, for example through a token
//! rendezvous. A caller can post [`SpawnActor`] directly, but most callers
//! should use [`RemoteSpawnerEndpoint`]. The extension trait builds the spawn
//! request, starts the local supervisor that owns the remote actor lifetime,
//! and returns the [`ActorRef`] for the actor that will be created on the
//! remote proc.
//!
//! Spawning through [`RemoteSpawnerEndpoint`] makes the spawning actor the
//! supervisor of the spawned remote actor. Logically, the remote actor becomes
//! part of the spawner's supervision tree even though it runs on another proc:
//!
//! ```text
//! caller proc                         remote proc
//!
//! caller actor
//! `- spawned actor A  -------- runs on -------->  RemoteSpawner's proc
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
//! `- [Supervisor] -- SpawnActor ---------------->  [RemoteSpawner]
//!                                                `- [Worker]
//!                                                   `- [spawned actor A]
//!
//! [Supervisor] <-------- SupervisorEvent -------- [Worker]
//! ```
//!
//! The caller-side [`Supervisor`] is spawned as a child of the actor that
//! requested the remote spawn. Its bootstrap posts [`SpawnActor`] to the remote
//! [`RemoteSpawner`]. The remote [`RemoteSpawner`] spawns a [`Worker`] child and asks it
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
//! A typical caller only needs the extension trait:
//!
//! ```rust,ignore
//! use hyperactor::ActorRef;
//! use hyperactor::Context;
//! use hyperactor::Handler;
//! use hyperactor::Label;
//! use hyperactor::RemoteSpawn;
//! use hyperactor::Uid;
//! use hyperactor_remote::RemoteSpawner;
//! use hyperactor_remote::RemoteSpawnerEndpoint;
//!
//! struct Driver {
//!     remote_spawner: ActorRef<RemoteSpawner>,
//! }
//!
//! impl Handler<Start> for Driver {
//!     async fn handle(&mut self, cx: &Context<Self>, _start: Start) -> anyhow::Result<()> {
//!         let calculator: ActorRef<Calculator> = self.remote_spawner.spawn_uid::<Calculator>(
//!             cx,
//!             Uid::instance(Label::new("calculator").unwrap()),
//!             CalculatorParams { initial_value: 0 },
//!         )?;
//!
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

use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint;
use hyperactor::Handler;
use hyperactor::RemoteSpawn;
use hyperactor::Uid;
use hyperactor::context;

use crate::Gspawn;
use crate::KeepaliveLink;
use crate::SpawnActor;
use crate::SupervisionOptions;
use crate::Supervisor;
use crate::Worker;
use crate::supervision::GspawnAndSupervise;

/// Remote actor that accepts dynamic remote spawn requests.
#[derive(Debug, Default)]
#[hyperactor::export(SpawnActor)]
pub struct RemoteSpawner;

#[async_trait]
impl Actor for RemoteSpawner {}

#[async_trait]
impl Handler<SpawnActor> for RemoteSpawner {
    async fn handle(&mut self, cx: &Context<Self>, message: SpawnActor) -> anyhow::Result<()> {
        let SpawnActor { gspawn, supervise } = message;
        let worker = context::Actor::instance(cx).spawn(Worker::new());
        worker.post(cx, GspawnAndSupervise::new(gspawn, supervise));
        Ok(())
    }
}

/// Convenience methods for endpoints that accept [`SpawnActor`] requests.
pub trait RemoteSpawnerEndpoint {
    /// Spawn a registered actor with a fresh actor uid.
    fn spawn<A>(&self, cx: &impl context::Actor, params: A::Params) -> anyhow::Result<ActorRef<A>>
    where
        A: RemoteSpawn,
        Self: Clone + Send + 'static,
        for<'a> &'a Self: Endpoint<SpawnActor>,
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
        for<'a> &'a Self: Endpoint<SpawnActor>,
    {
        self.spawn_uid_with_link::<A>(
            cx,
            uid,
            params,
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60)),
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
        for<'a> &'a Self: Endpoint<SpawnActor>,
    {
        // We can safely compute the resulting actor ref: by definition it
        // will be on the same proc as the remote spawner itself, and we use a
        // Uid to identify it.
        anyhow::ensure!(
            uid.is_instance(),
            "RemoteSpawner-spawned actors cannot be singletons"
        );
        let actor_ref = ActorRef::attest(
            self.endpoint_location()
                .actor_addr()
                .proc_addr()
                .actor_addr_uid(uid.clone()),
        );
        let remote_spawner = self.clone();
        let gspawn = Gspawn::for_actor_uid::<A>(uid, params)?;
        cx.instance().spawn(Supervisor::bootstrap_uid(
            liveness,
            SupervisionOptions::default(),
            Uid::anonymous(),
            actor_ref.actor_addr().clone(),
            move |cx, supervise| {
                remote_spawner.post(cx, SpawnActor { gspawn, supervise });
                Ok(())
            },
        ));
        Ok(actor_ref)
    }
}

impl<T> RemoteSpawnerEndpoint for T where for<'a> &'a T: Endpoint<SpawnActor> {}

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
        remote_spawner: ActorHandle<RemoteSpawner>,
        spawned: PortRef<ActorAddr>,
        events: PortRef<ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for PlainSpawner {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let actor_ref = self.remote_spawner.spawn_uid::<FailingChild>(
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

    fn encoded_unit() -> Vec<u8> {
        bincode::serde::encode_to_vec((), bincode::config::legacy()).unwrap()
    }

    async fn spawn_test_child(
        remote_spawner: &ActorHandle<RemoteSpawner>,
        client: &hyperactor::Client,
        supervisor: PortRef<SupervisorEvent>,
        session_id: Uid,
    ) {
        let (_link_handle, liveness) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn_supervisor(client)
                .unwrap();
        remote_spawner.post(
            client,
            SpawnActor {
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
        let remote_spawner = proc.spawn(RemoteSpawner);
        let (spawned, mut spawned_rx) = client.open_port::<ActorAddr>();
        let (events, mut events_rx) = client.open_port::<ActorSupervisionEvent>();
        let spawner = proc.spawn(PlainSpawner {
            remote_spawner: remote_spawner.clone(),
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

        remote_spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), spawner)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), remote_spawner)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_remote_spawner_worker_reports_child_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let remote_spawner = proc.spawn(RemoteSpawner);
        let (supervisor, mut supervisor_rx) = client.open_port::<SupervisorEvent>();
        let session_id = Uid::instance(Label::new("session").unwrap());

        spawn_test_child(
            &remote_spawner,
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

        remote_spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), remote_spawner)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_remote_spawner_rejects_unknown_actor_type() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let remote_spawner = proc.spawn(RemoteSpawner);
        let (supervisor, mut supervisor_rx) = client.open_port::<SupervisorEvent>();
        let (_link_handle, liveness) =
            KeepaliveLink::new(Duration::from_secs(60), Duration::from_secs(60))
                .spawn_supervisor(&client)
                .unwrap();
        let session_id = Uid::instance(Label::new("session").unwrap());

        remote_spawner.post(
            &client,
            SpawnActor {
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

        remote_spawner.stop("test").unwrap();
        tokio::time::timeout(Duration::from_secs(5), remote_spawner)
            .await
            .unwrap();
    }
}
