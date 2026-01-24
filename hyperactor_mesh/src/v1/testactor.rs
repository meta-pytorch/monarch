/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines a test actor. It is defined in a separate module
//! (outside of [`crate::v1::testing`]) to ensure that it is compiled into
//! the bootstrap binary, which is not built in test mode (and anyway, test mode
//! does not work across crate boundaries)

#[cfg(test)]
use std::collections::HashSet;
use std::collections::VecDeque;
use std::ops::Deref;
#[cfg(test)]
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::Unbind;
use hyperactor::clock::Clock as _;
use hyperactor::clock::RealClock;
#[cfg(test)]
use hyperactor::context;
#[cfg(test)]
use hyperactor::mailbox;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::global::Source;
use ndslice::Point;
#[cfg(test)]
use ndslice::ViewExt as _;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::comm::multicast::CastInfo;
use crate::supervision::MeshFailure;
use crate::v1::ActorMesh;
#[cfg(test)]
use crate::v1::ActorMeshRef;
use crate::v1::Name;
use crate::v1::ProcMeshRef;
#[cfg(test)]
use crate::v1::testing;

/// A simple test actor used by various unit tests.
#[derive(Default, Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        GetActorId { cast = true },
        GetCastInfo { cast = true },
        CauseSupervisionEvent { cast = true },
        Forward,
        GetConfigAttrs { cast = true },
        SetConfigAttrs { cast = true },
    ]
)]
pub struct TestActor;

impl Actor for TestActor {}

/// A message that returns the recipient actor's id.
#[derive(Debug, Clone, Named, Bind, Unbind, Serialize, Deserialize)]
pub struct GetActorId(#[binding(include)] pub PortRef<ActorId>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SupervisionEventType {
    Panic,
    SigSEGV,
    ProcessExit(i32),
}

/// A message that causes a supervision event. The one argument determines what
/// kind of supervision event it'll be.
#[derive(Debug, Clone, Named, Bind, Unbind, Serialize, Deserialize)]
pub struct CauseSupervisionEvent {
    pub kind: SupervisionEventType,
    pub send_to_children: bool,
}

impl CauseSupervisionEvent {
    fn cause_event(&self) -> ! {
        match self.kind {
            SupervisionEventType::Panic => {
                panic!("for testing");
            }
            SupervisionEventType::SigSEGV => {
                tracing::error!("exiting with SIGSEGV");
                // SAFETY: This is for testing code that explicitly causes a SIGSEGV.
                unsafe { std::ptr::null_mut::<i32>().write(42) };
                // While the above should always segfault, we need a hard exit
                // for the compiler's sake.
                panic!("should have segfaulted");
            }
            SupervisionEventType::ProcessExit(code) => {
                tracing::error!("exiting process {} with code {}", std::process::id(), code);
                std::process::exit(code);
            }
        }
    }
}

#[async_trait]
impl Handler<GetActorId> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetActorId(reply): GetActorId,
    ) -> Result<(), anyhow::Error> {
        reply.send(cx, cx.self_id().clone())?;
        Ok(())
    }
}

#[async_trait]
impl Handler<CauseSupervisionEvent> for TestActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        msg: CauseSupervisionEvent,
    ) -> Result<(), anyhow::Error> {
        msg.cause_event();
    }
}

/// A test actor that handles supervision events.
/// It should be the parent of TestActor who can panic or cause a SIGSEGV.
#[derive(Default, Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [ActorSupervisionEvent],
)]
pub struct TestActorWithSupervisionHandling;

#[async_trait]
impl Actor for TestActorWithSupervisionHandling {
    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        tracing::error!("supervision event: {:?}", event);
        // Swallow the supervision error to avoid crashing the process.
        Ok(true)
    }
}

#[async_trait]
impl Handler<ActorSupervisionEvent> for TestActorWithSupervisionHandling {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        _msg: ActorSupervisionEvent,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// A test actor that sleeps when it receives a Duration message.
/// Used for testing timeout and abort behavior.
#[derive(Default, Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [std::time::Duration],
)]
pub struct SleepActor;

impl Actor for SleepActor {}

#[async_trait]
impl Handler<std::time::Duration> for SleepActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        duration: std::time::Duration,
    ) -> Result<(), anyhow::Error> {
        RealClock.sleep(duration).await;
        Ok(())
    }
}

/// A message to forward to a visit list of ports.
/// Each port removes the next entry, and adds it to the
/// 'visited' list.
#[derive(Debug, Clone, Named, Bind, Unbind, Serialize, Deserialize)]
pub struct Forward {
    pub to_visit: VecDeque<PortRef<Forward>>,
    pub visited: Vec<PortRef<Forward>>,
}

#[async_trait]
impl Handler<Forward> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        Forward {
            mut to_visit,
            mut visited,
        }: Forward,
    ) -> Result<(), anyhow::Error> {
        let Some(this) = to_visit.pop_front() else {
            anyhow::bail!("unexpected forward chain termination");
        };
        visited.push(this);
        let next = to_visit.front().cloned();
        anyhow::ensure!(next.is_some(), "unexpected forward chain termination");
        next.unwrap().send(cx, Forward { to_visit, visited })?;
        Ok(())
    }
}

/// Just return the cast info of the sender.
#[derive(
    Debug,
    Clone,
    Named,
    Bind,
    Unbind,
    Serialize,
    Deserialize,
    Handler,
    RefClient
)]
pub struct GetCastInfo {
    /// Originating actor, point, sender.
    #[reply]
    pub cast_info: PortRef<(Point, ActorRef<TestActor>, ActorId)>,
}

#[async_trait]
impl Handler<GetCastInfo> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetCastInfo { cast_info }: GetCastInfo,
    ) -> Result<(), anyhow::Error> {
        cast_info.send(cx, (cx.cast_point(), cx.bind(), cx.sender().clone()))?;
        Ok(())
    }
}

#[derive(Debug)]
#[hyperactor::export(spawn = true)]
pub struct FailingCreateTestActor;

#[async_trait]
impl Actor for FailingCreateTestActor {}

#[async_trait]
impl hyperactor::RemoteSpawn for FailingCreateTestActor {
    type Params = ();

    async fn new(
        _params: Self::Params,
    ) -> Result<Self, hyperactor::internal_macro_support::anyhow::Error> {
        Err(anyhow::anyhow!("test failure"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct SetConfigAttrs(pub Vec<u8>);

#[async_trait]
impl Handler<SetConfigAttrs> for TestActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        SetConfigAttrs(attrs): SetConfigAttrs,
    ) -> Result<(), anyhow::Error> {
        let attrs = bincode::deserialize(&attrs)?;
        hyperactor_config::global::set(Source::Runtime, attrs);
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct GetConfigAttrs(pub PortRef<Vec<u8>>);

#[async_trait]
impl Handler<GetConfigAttrs> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetConfigAttrs(reply): GetConfigAttrs,
    ) -> Result<(), anyhow::Error> {
        let attrs = bincode::serialize(&hyperactor_config::global::attrs())?;
        reply.send(cx, attrs)?;
        Ok(())
    }
}

/// A message to request the next supervision event delivered to WrapperActor.
/// Replies with None if no supervision event is encountered within a timeout
/// (10 seconds).
#[derive(Clone, Debug, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct NextSupervisionFailure(pub PortRef<Option<MeshFailure>>);

/// A small wrapper to handle supervision messages so they don't
/// need to reach the client. This just wraps and forwards all messages to TestActor.
/// The supervision events are sent back to "supervisor".
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        CauseSupervisionEvent { cast = true },
        MeshFailure { cast = true },
        NextSupervisionFailure { cast = true },
    ]
)]
pub struct WrapperActor {
    proc_mesh: ProcMeshRef,
    // Needs to be a mesh so we own this actor and have a controller for it.
    mesh: Option<ActorMesh<TestActor>>,
    supervisor: PortRef<MeshFailure>,
    test_name: Name,
}

#[async_trait]
impl hyperactor::RemoteSpawn for WrapperActor {
    type Params = (ProcMeshRef, PortRef<MeshFailure>, Name);

    async fn new(
        (proc_mesh, supervisor, test_name): Self::Params,
    ) -> Result<Self, hyperactor::internal_macro_support::anyhow::Error> {
        Ok(Self {
            proc_mesh,
            mesh: None,
            supervisor,
            test_name,
        })
    }
}

#[async_trait]
impl Actor for WrapperActor {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.mesh = Some(
            self.proc_mesh
                .spawn_with_name(this, self.test_name.clone(), &(), None, false)
                .await?,
        );
        Ok(())
    }
}

#[async_trait]
impl Handler<CauseSupervisionEvent> for WrapperActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: CauseSupervisionEvent,
    ) -> Result<(), anyhow::Error> {
        // No reply to wait for.
        if msg.send_to_children {
            // Send only to children, don't cause the event itself.
            self.mesh
                .as_ref()
                .unwrap()
                .cast(cx, msg)
                .map_err(|e| e.into())
        } else {
            msg.cause_event()
        }
    }
}

#[async_trait]
impl Handler<NextSupervisionFailure> for WrapperActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: NextSupervisionFailure,
    ) -> Result<(), anyhow::Error> {
        let mesh = if let Some(mesh) = self.mesh.as_ref() {
            mesh.deref()
        } else {
            msg.0.send(cx, None)?;
            return Ok(());
        };
        let failure = match RealClock
            .timeout(
                tokio::time::Duration::from_secs(20),
                mesh.next_supervision_event(cx),
            )
            .await
        {
            Ok(Ok(failure)) => Some(failure),
            // Any error in next_supervision_event is treated the same.
            Ok(Err(_)) => None,
            // If we timeout, send back None.
            Err(_) => None,
        };
        msg.0.send(cx, failure)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<MeshFailure> for WrapperActor {
    async fn handle(&mut self, cx: &Context<Self>, msg: MeshFailure) -> Result<(), anyhow::Error> {
        // All supervision events are considered handled so they don't bubble up
        // to the client (who isn't listening for MeshFailure).
        tracing::info!("got supervision event from child: {}", msg);
        // Send to a port so the client can view the messages.
        // Ignore the error if there is one.
        let _ = self.supervisor.send(cx, msg.clone());
        Ok(())
    }
}

#[cfg(test)]
/// Asserts that the provided actor mesh has the expected shape,
/// and all actors are assigned the correct ranks. We also test
/// slicing the mesh.
pub async fn assert_mesh_shape(actor_mesh: ActorMesh<TestActor>) {
    let instance = testing::instance();
    // Verify casting to the root actor mesh
    assert_casting_correctness(&actor_mesh, instance).await;

    // Just pick the first dimension. Slice half of it off.
    // actor_mesh.extent().
    let label = actor_mesh.extent().labels()[0].clone();
    let size = actor_mesh.extent().sizes()[0] / 2;

    // Verify casting to the sliced actor mesh
    let sliced_actor_mesh = actor_mesh.range(&label, 0..size).unwrap();
    assert_casting_correctness(&sliced_actor_mesh, instance).await;
}

#[cfg(test)]
/// Cast to the actor mesh, and verify that all actors are reached.
pub async fn assert_casting_correctness(
    actor_mesh: &ActorMeshRef<TestActor>,
    instance: &impl context::Actor,
) {
    let (port, mut rx) = mailbox::open_port(instance);
    actor_mesh.cast(instance, GetActorId(port.bind())).unwrap();

    let mut expected_actor_ids: HashSet<_> = actor_mesh
        .values()
        .map(|actor_ref| actor_ref.actor_id().clone())
        .collect();

    while !expected_actor_ids.is_empty() {
        let actor_id = rx.recv().await.unwrap();
        assert!(
            expected_actor_ids.remove(&actor_id),
            "got {actor_id}, expect {expected_actor_ids:?}"
        );
    }

    // No more messages
    RealClock.sleep(Duration::from_secs(1)).await;
    let result = rx.try_recv();
    assert!(result.as_ref().unwrap().is_none(), "got {result:?}");
}
