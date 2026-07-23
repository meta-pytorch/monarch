/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines a test actor. It is defined in a separate module
//! (outside of [`crate::testing`]) to ensure that it is compiled into
//! the bootstrap binary, which is not built in test mode (and anyway, test mode
//! does not work across crate boundaries)

#[cfg(test)]
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Deref;
#[cfg(test)]
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::RefClient;
#[cfg(test)]
use hyperactor::context;
use hyperactor::ordering::SEQ_INFO;
use hyperactor::ordering::SeqInfo;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::global::Source;
use ndslice::Point;
#[cfg(test)]
use ndslice::ViewExt as _;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;
#[cfg(test)]
use uuid::Uuid;

use crate::ActorMesh;
#[cfg(test)]
use crate::ActorMeshRef;
use crate::ProcMeshRef;
use crate::casting::CAST_POINT;
use crate::casting::CastInfo;
use crate::mesh_id::ActorMeshId;
use crate::supervision::MeshFailure;
#[cfg(test)]
use crate::testing;

/// A simple test actor used by various unit tests.
#[derive(Default, Debug)]
#[hyperactor::export(
    (),
    GetActorId,
    GetCastInfo,
    GetResourceRank,
    CauseSupervisionEvent,
    Forward,
    GetConfigAttrs,
    SetConfigAttrs,
)]
#[hyperactor::spawnable]
pub struct TestActor;

impl Actor for TestActor {}

/// A message that returns the recipient actor's id and cast message's seq info.
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct GetActorId(pub hyperactor::PortRef<(hyperactor::ActorAddr, Option<SeqInfo>)>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SupervisionEventType {
    Panic,
    SigSEGV,
    ProcessExit(i32),
}

/// A message that causes a supervision event. The one argument determines what
/// kind of supervision event it'll be.
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
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
impl Handler<()> for TestActor {
    async fn handle(&mut self, _cx: &Context<Self>, _: ()) -> Result<(), anyhow::Error> {
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
        let seq_info = cx.headers().get(SEQ_INFO);
        reply.post(cx, (cx.self_addr().clone(), seq_info));
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
#[hyperactor::export(ActorSupervisionEvent)]
#[hyperactor::spawnable]
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
#[hyperactor::export(std::time::Duration)]
#[hyperactor::spawnable]
pub struct SleepActor;

impl Actor for SleepActor {}

#[async_trait]
impl Handler<std::time::Duration> for SleepActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        duration: std::time::Duration,
    ) -> Result<(), anyhow::Error> {
        tokio::time::sleep(duration).await;
        Ok(())
    }
}

/// A message to forward to a visit list of ports.
/// Each port removes the next entry, and adds it to the
/// 'visited' list.
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct Forward {
    pub to_visit: VecDeque<hyperactor::PortRef<Forward>>,
    pub visited: Vec<hyperactor::PortRef<Forward>>,
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
        next.unwrap().post(cx, Forward { to_visit, visited });
        Ok(())
    }
}

/// Just return the cast info of the sender.
#[derive(Debug, Clone, Named, Serialize, Deserialize, Handler, RefClient)]
pub struct GetCastInfo {
    /// Originating actor, point, sender.
    #[reply]
    pub cast_info: hyperactor::PortRef<(Point, ActorRef<TestActor>, hyperactor::ActorAddr)>,
}

#[async_trait]
impl Handler<GetCastInfo> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetCastInfo { cast_info }: GetCastInfo,
    ) -> Result<(), anyhow::Error> {
        cast_info.post(cx, (cx.cast_point(), cx.bind(), cx.sender().clone()));
        Ok(())
    }
}

#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct GetResourceRank {
    pub rank: crate::resource::Rank,
    pub reply: hyperactor::PortRef<(Point, Option<usize>)>,
}

#[async_trait]
impl Handler<GetResourceRank> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetResourceRank { rank, reply }: GetResourceRank,
    ) -> Result<(), anyhow::Error> {
        reply.post(cx, (cx.cast_point(), rank.0));

        Ok(())
    }
}

#[derive(Debug)]
#[hyperactor::export]
#[hyperactor::spawnable]
pub struct FailingCreateTestActor;

#[async_trait]
impl Actor for FailingCreateTestActor {}

#[async_trait]
impl hyperactor::RemoteSpawn for FailingCreateTestActor {
    type Params = ();

    async fn new(
        _params: Self::Params,
        _environment: Flattrs,
    ) -> Result<Self, hyperactor::internal_macro_support::anyhow::Error> {
        Err(anyhow::anyhow!("test failure"))
    }
}

declare_attrs! {
    /// Persistent sentinel used by actor-environment transport tests.
    pub attr ACTOR_ENVIRONMENT_TEST_TAG: u64;
}

/// What an [`ActorEnvironmentProbe`] observed at construction and after its
/// native instance was installed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Named)]
pub struct ActorEnvironmentObservation {
    pub label: String,
    pub persistent_tag: Option<u64>,
    pub constructor_tag: Option<u64>,
    pub constructor_point: Option<Point>,
    pub stored_point: Option<Point>,
    pub proc_addr: String,
}
wirevalue::register_type!(ActorEnvironmentObservation);

/// Construction parameters for [`ActorEnvironmentProbe`].
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub struct ActorEnvironmentProbeParams {
    pub label: String,
    pub reply: hyperactor::PortRef<ActorEnvironmentObservation>,
    pub nested: Option<(ProcMeshRef, String, String)>,
}
wirevalue::register_type!(ActorEnvironmentProbeParams);

/// A remote-spawn probe that optionally performs one nested ProcMesh spawn.
#[derive(Debug)]
#[hyperactor::export(handlers = [])]
pub struct ActorEnvironmentProbe {
    label: String,
    reply: hyperactor::PortRef<ActorEnvironmentObservation>,
    nested: Option<(ProcMeshRef, String, String)>,
    constructor_tag: Option<u64>,
    constructor_point: Option<Point>,
}

#[async_trait]
impl Actor for ActorEnvironmentProbe {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.reply.post(
            this,
            ActorEnvironmentObservation {
                label: self.label.clone(),
                persistent_tag: this.actor_environment().get(ACTOR_ENVIRONMENT_TEST_TAG),
                constructor_tag: self.constructor_tag,
                constructor_point: self.constructor_point.clone(),
                stored_point: this.actor_environment().get(CAST_POINT),
                proc_addr: this.proc().proc_addr().to_string(),
            },
        );

        if let Some((proc_mesh, name, label)) = self.nested.take() {
            proc_mesh
                .spawn_controllerless_service::<Self, _>(
                    this,
                    &name,
                    &ActorEnvironmentProbeParams {
                        label,
                        reply: self.reply.clone(),
                        nested: None,
                    },
                )
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl hyperactor::RemoteSpawn for ActorEnvironmentProbe {
    type Params = ActorEnvironmentProbeParams;

    async fn new(params: Self::Params, environment: Flattrs) -> anyhow::Result<Self> {
        Ok(Self {
            label: params.label,
            reply: params.reply,
            nested: params.nested,
            constructor_tag: environment.get(ACTOR_ENVIRONMENT_TEST_TAG),
            constructor_point: environment.get(CAST_POINT),
        })
    }
}

hyperactor::register_spawnable!(ActorEnvironmentProbe);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub struct SetConfigAttrs(pub Vec<u8>);

#[async_trait]
impl Handler<SetConfigAttrs> for TestActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        SetConfigAttrs(attrs): SetConfigAttrs,
    ) -> Result<(), anyhow::Error> {
        let attrs =
            bincode::serde::decode_from_slice(&attrs, bincode::config::legacy()).map(|(v, _)| v)?;
        hyperactor_config::global::set(Source::Runtime, attrs);
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub struct GetConfigAttrs(pub hyperactor::PortRef<Vec<u8>>);

#[async_trait]
impl Handler<GetConfigAttrs> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetConfigAttrs(reply): GetConfigAttrs,
    ) -> Result<(), anyhow::Error> {
        let attrs = bincode::serde::encode_to_vec(
            hyperactor_config::global::attrs(),
            bincode::config::legacy(),
        )?;
        reply.post(cx, attrs);
        Ok(())
    }
}

/// A message to request the next supervision event delivered to WrapperActor.
/// Replies with None if no supervision event is encountered within a timeout
/// (10 seconds).
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub struct NextSupervisionFailure(pub hyperactor::PortRef<Option<MeshFailure>>);

/// A small wrapper to handle supervision messages so they don't
/// need to reach the client. This just wraps and forwards all messages to TestActor.
/// The supervision events are sent back to "supervisor".
#[derive(Debug)]
#[hyperactor::export(CauseSupervisionEvent, MeshFailure, NextSupervisionFailure)]
#[hyperactor::spawnable]
pub struct WrapperActor {
    proc_mesh: ProcMeshRef,
    // Needs to be a mesh so we own this actor and have a controller for it.
    mesh: Option<ActorMesh<TestActor>>,
    supervisor: hyperactor::PortRef<MeshFailure>,
    test_name: ActorMeshId,
}

#[async_trait]
impl hyperactor::RemoteSpawn for WrapperActor {
    type Params = (ProcMeshRef, hyperactor::PortRef<MeshFailure>, ActorMeshId);

    async fn new(
        (proc_mesh, supervisor, test_name): Self::Params,
        _environment: Flattrs,
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
            msg.0.post(cx, None);
            return Ok(());
        };
        let failure = match tokio::time::timeout(
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
        msg.0.post(cx, failure);
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
        let _ = self.supervisor.post(cx, msg.clone());
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
    instance: &impl context::Actor,
    expected_seqs: Option<(Uuid, Vec<u64>)>,
) {
    let (port, mut rx) = instance.mailbox().open_port();
    actor_mesh.cast(instance, GetActorId(port.bind())).unwrap();
    let expected_actor_ids = actor_mesh
        .values()
        .map(|actor_ref| actor_ref.actor_addr().clone())
        .collect::<Vec<_>>();
    let mut expected: HashMap<&hyperactor::ActorAddr, Option<SeqInfo>> = match expected_seqs {
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    let result = rx.try_recv();
    assert!(result.as_ref().unwrap().is_none(), "got {result:?}");
}
