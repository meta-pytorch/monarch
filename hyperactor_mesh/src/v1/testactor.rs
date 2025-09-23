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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::Unbind;
use hyperactor::clock::Clock as _;
use hyperactor::clock::RealClock;
use hyperactor::mailbox;
use hyperactor::supervision::ActorSupervisionEvent;
use ndslice::Point;
use ndslice::ViewExt;
use serde::Deserialize;
use serde::Serialize;

use crate::comm::multicast::CastInfo;
use crate::v1::ActorMesh;
use crate::v1::ActorMeshRef;
#[cfg(test)]
use crate::v1::testing;

/// A simple test actor used by various unit tests.
#[derive(Actor, Default, Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        GetActorId { cast = true },
        GetCastInfo { cast = true },
        CauseSupervisionEvent { cast = true },
        Forward,
    ]
)]
pub struct TestActor;

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
pub struct CauseSupervisionEvent(pub SupervisionEventType);

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
        match msg.0 {
            SupervisionEventType::Panic => {
                panic!("for testing");
            }
            SupervisionEventType::SigSEGV => {
                // SAFETY: This is for testing code that explicitly causes a SIGSEGV.
                unsafe { std::ptr::null_mut::<i32>().write(42) };
            }
            SupervisionEventType::ProcessExit(code) => {
                std::process::exit(code);
            }
        }
        Ok(())
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
    type Params = ();

    async fn new(_params: Self::Params) -> Result<Self, hyperactor::anyhow::Error> {
        Ok(Self {})
    }

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
        cast_info.send(cx, (cx.cast_info(), cx.bind(), cx.sender().clone()))?;
        Ok(())
    }
}

#[cfg(test)]
/// Asserts that the provided actor mesh has the expected shape,
/// and all actors are assigned the correct ranks. We also test
/// slicing the mesh.
pub async fn assert_mesh_shape(actor_mesh: ActorMesh<TestActor>) {
    let instance = testing::instance().await;
    // Verify casting to the root actor mesh
    {
        let (port, mut rx) = mailbox::open_port(&instance);
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

    // Just pick the first dimension. Slice half of it off.
    // actor_mesh.extent().
    let label = actor_mesh.extent().labels()[0].clone();
    let size = actor_mesh.extent().sizes()[0] / 2;

    // Verify casting to the sliced actor mesh
    let sliced_actor_mesh = actor_mesh.range(&label, 0..size).unwrap();
    {
        let (port, mut rx) = mailbox::open_port(instance);
        sliced_actor_mesh
            .cast(instance, GetActorId(port.bind()))
            .unwrap();

        let mut expected_actor_ids: HashSet<_> = sliced_actor_mesh
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
}
