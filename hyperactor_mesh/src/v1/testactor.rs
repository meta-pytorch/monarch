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
use std::collections::HashMap;
use std::collections::VecDeque;
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
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::Unbind;
#[cfg(test)]
use hyperactor::clock::Clock as _;
#[cfg(test)]
use hyperactor::clock::RealClock;
#[cfg(test)]
use hyperactor::mailbox;
use hyperactor::proc::SEQ_INFO;
use hyperactor::proc::SeqInfo;
use hyperactor::supervision::ActorSupervisionEvent;
use ndslice::Point;
#[cfg(test)]
use ndslice::ViewExt as _;
use serde::Deserialize;
use serde::Serialize;
#[cfg(test)]
use uuid::Uuid;

use crate::comm::multicast::CastInfo;
#[cfg(test)]
use crate::v1::ActorMesh;
#[cfg(test)]
use crate::v1::ActorMeshRef;
#[cfg(test)]
use crate::v1::testing;

/// A simple test actor used by various unit tests.
#[derive(Actor, Default, Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        () { cast = true },
        GetActorId { cast = true },
        GetCastInfo { cast = true },
        CauseSupervisionEvent { cast = true },
        Forward,
    ]
)]
pub struct TestActor;

/// A message that returns the recipient actor's id and cast message's seq info.
#[derive(Debug, Clone, Named, Bind, Unbind, Serialize, Deserialize)]
pub struct GetActorId(#[binding(include)] pub PortRef<(ActorId, Option<SeqInfo>)>);

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
impl Handler<()> for TestActor {
    async fn handle(&mut self, cx: &Context<Self>, _: ()) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<GetActorId> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetActorId(reply): GetActorId,
    ) -> Result<(), anyhow::Error> {
        let seq_info = cx.headers().get(SEQ_INFO).cloned();
        reply.send(cx, (cx.self_id().clone(), seq_info))?;
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
        cast_info.send(cx, (cx.cast_point(), cx.bind(), cx.sender().clone()))?;
        Ok(())
    }
}

#[derive(Default, Debug)]
#[hyperactor::export(spawn = true)]
pub struct FailingCreateTestActor;

#[async_trait]
impl Actor for FailingCreateTestActor {
    type Params = ();

    async fn new(params: Self::Params) -> Result<Self, hyperactor::anyhow::Error> {
        Err(anyhow::anyhow!("test failure"))
    }
}

#[cfg(test)]
/// Asserts that the provided actor mesh has the expected shape,
/// and all actors are assigned the correct ranks. We also test
/// slicing the mesh.
pub async fn assert_mesh_shape(actor_mesh: ActorMesh<TestActor>) {
    let instance = testing::instance().await;
    // Verify casting to the root actor mesh
    assert_casting_correctness(&actor_mesh, instance, None).await;

    // Just pick the first dimension. Slice half of it off.
    // actor_mesh.extent().
    let label = actor_mesh.extent().labels()[0].clone();
    let size = actor_mesh.extent().sizes()[0] / 2;

    // Verify casting to the sliced actor mesh
    let sliced_actor_mesh = actor_mesh.range(&label, 0..size).unwrap();
    assert_casting_correctness(&sliced_actor_mesh, instance, None).await;
}

#[cfg(test)]
/// Cast to the actor mesh, and verify that all actors are reached, and the
/// sequence numbers, if provided, are correct.
pub async fn assert_casting_correctness(
    actor_mesh: &ActorMeshRef<TestActor>,
    instance: &Instance<()>,
    expected_seqs: Option<(Uuid, Vec<u64>)>,
) {
    let (port, mut rx) = mailbox::open_port(&instance);
    actor_mesh.cast(&instance, GetActorId(port.bind())).unwrap();
    let expected_actor_ids = actor_mesh
        .values()
        .map(|actor_ref| actor_ref.actor_id().clone())
        .collect::<Vec<_>>();
    let mut expected: HashMap<&ActorId, Option<SeqInfo>> = match expected_seqs {
        None => expected_actor_ids
            .iter()
            .map(|actor_id| (actor_id, None))
            .collect(),
        Some((session_id, seqs)) => expected_actor_ids
            .iter()
            .zip(
                seqs.into_iter()
                    .map(|seq| Some(SeqInfo::Session { session_id, seq })),
            )
            .collect(),
    };

    while !expected.is_empty() {
        let (actor_id, rcved) = rx.recv().await.unwrap();
        let rcv_seq_info = rcved.unwrap();
        let removed = expected.remove(&actor_id);
        assert!(
            removed.is_some(),
            "got {actor_id}, expect {expected_actor_ids:?}"
        );
        if let Some(expected) = removed.unwrap() {
            assert_eq!(expected, rcv_seq_info, "got different seq for {actor_id}");
        }
    }

    // No more messages
    RealClock.sleep(Duration::from_secs(1)).await;
    let result = rx.try_recv();
    assert!(result.as_ref().unwrap().is_none(), "got {result:?}");
}
