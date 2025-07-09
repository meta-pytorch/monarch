/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)] // until used publically

use std::ops::Deref;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::GangId;
use hyperactor::GangRef;
use hyperactor::Message;
use hyperactor::Named;
use hyperactor::PortHandle;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::Unbind;
use hyperactor::WorldId;
use hyperactor::actor::RemoteActor;
use hyperactor::attrs::Attrs;
use hyperactor::cap;
use hyperactor::mailbox::MailboxSenderError;
use hyperactor::mailbox::PortReceiver;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor::supervision::ActorSupervisionEvent;
use ndslice::Range;
use ndslice::Selection;
use ndslice::Shape;
use ndslice::ShapeError;
use ndslice::Slice;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::CommActor;
use crate::Mesh;
use crate::comm::multicast::CastMessage;
use crate::comm::multicast::CastMessageEnvelope;
use crate::comm::multicast::DestinationPort;
use crate::comm::multicast::Uslice;
use crate::comm::multicast::set_cast_info_on_headers;
use crate::metrics;
use crate::proc_mesh::ProcMesh;
use crate::reference::ActorMeshId;
use crate::reference::ActorMeshRef;
use crate::reference::ProcMeshId;

/// Common implementation for ActorMeshes and ActorMeshRefs to cast an [`M`]-typed message
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
pub(crate) fn actor_mesh_cast<M: Castable + Clone, A>(
    caps: &impl cap::CanSend,
    actor_mesh_id: ActorMeshId,
    actor_mesh_shape: &Shape,
    _proc_mesh_shape: &Shape,
    actor_name: &str,
    sender: &ActorId,
    comm_actor_ref: &ActorRef<CommActor>,
    selection: Selection,
    message: M,
) -> Result<(), CastError>
where
    A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
{
    let _ = metrics::ACTOR_MESH_CAST_DURATION.start(hyperactor::kv_pairs!(
        "message_type" => M::typename(),
        "message_variant" => message.arm().unwrap_or_default(),
    ));

    let slice = actor_mesh_shape.slice().clone();
    let message = CastMessageEnvelope::new(
        actor_mesh_id,
        sender.clone(),
        DestinationPort::new::<A, M>(actor_name.to_string()),
        actor_mesh_shape.clone(),
        message,
        None, // TODO: reducer typehash
    )?;

    comm_actor_ref.send(
        caps,
        CastMessage {
            dest: Uslice { slice, selection },
            message,
        },
    )?;

    Ok(())
}

/// A mesh of actors, all of which reside on the same [`ProcMesh`].
pub trait ActorMesh: Mesh<Id = ActorMeshId> {
    /// The type of actor in the mesh.
    type Actor: RemoteActor;

    /// Cast an [`M`]-typed message to the ranks selected by `sel`
    /// in this ActorMesh.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
    fn cast<M: Castable + Clone>(&self, selection: Selection, message: M) -> Result<(), CastError>
    where
        Self::Actor: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
    {
        actor_mesh_cast::<M, Self::Actor>(
            self.proc_mesh().client(),
            self.id(),
            self.shape(),
            self.proc_mesh().shape(),
            self.name(),
            self.proc_mesh().client().actor_id(),
            self.proc_mesh().comm_actor(),
            selection,
            message,
        )
    }

    /// The ProcMesh on top of which this actor mesh is spawned.
    fn proc_mesh(&self) -> &ProcMesh;

    /// The name global name of actors in this mesh.
    fn name(&self) -> &str;

    fn world_id(&self) -> &WorldId {
        self.proc_mesh().world_id()
    }

    /// Iterate over all `ActorRef<Self::Actor>` in this mesh.
    fn iter_actor_refs(&self) -> impl Iterator<Item = ActorRef<Self::Actor>> {
        let gang: GangRef<Self::Actor> =
            GangId(self.proc_mesh().world_id().clone(), self.name().to_string()).into();
        self.shape().slice().iter().map(move |rank| gang.rank(rank))
    }

    /// Get a serializeable reference to this mesh similar to ActorHandle::bind
    fn bind(&self) -> ActorMeshRef<Self::Actor> {
        ActorMeshRef::attest(
            ActorMeshId(
                ProcMeshId(self.world_id().to_string()),
                self.name().to_string(),
            ),
            self.shape().clone(),
            self.proc_mesh().shape().clone(),
            self.proc_mesh().comm_actor().clone(),
        )
    }
}

/// Abstracts over shared and borrowed references to a [`ProcMesh`].
/// Given a shared ProcMesh, we can obtain a [`ActorMesh<'static, _>`]
/// for it, useful when lifetime must be managed dynamically.
enum ProcMeshRef<'a> {
    /// The reference is shared without requiring a reference.
    Shared(Box<dyn Deref<Target = ProcMesh> + Sync + Send>),
    /// The reference is borrowed with a parameterized
    /// lifetime.
    Borrowed(&'a ProcMesh),
}

impl Deref for ProcMeshRef<'_> {
    type Target = ProcMesh;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Shared(p) => p,
            Self::Borrowed(p) => p, // p: &ProcMesh
        }
    }
}

/// A mesh of actor instances. ActorMeshes are obtained by spawning an
/// actor on a [`ProcMesh`].
pub struct RootActorMesh<'a, A: RemoteActor> {
    proc_mesh: ProcMeshRef<'a>,
    name: String,
    pub(crate) ranks: Vec<ActorRef<A>>, // temporary until we remove `ArcActorMesh`.
    actor_supervision_rx: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
}

impl<'a, A: RemoteActor> RootActorMesh<'a, A> {
    pub(crate) fn new(
        proc_mesh: &'a ProcMesh,
        name: String,
        actor_supervision_rx: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
        ranks: Vec<ActorRef<A>>,
    ) -> Self {
        Self {
            proc_mesh: ProcMeshRef::Borrowed(proc_mesh),
            name,
            ranks,
            actor_supervision_rx,
        }
    }

    pub(crate) fn new_shared<D: Deref<Target = ProcMesh> + Send + Sync + 'static>(
        proc_mesh: D,
        name: String,
        actor_supervision_rx: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
        ranks: Vec<ActorRef<A>>,
    ) -> Self {
        Self {
            proc_mesh: ProcMeshRef::Shared(Box::new(proc_mesh)),
            name,
            ranks,
            actor_supervision_rx,
        }
    }

    /// Open a port on this ActorMesh.
    pub(crate) fn open_port<M: Message>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        self.proc_mesh.client().open_port()
    }

    /// Until the selection logic is more powerful, we need a way to
    /// replicate the send patterns that the worker actor mesh actually does.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
    pub fn cast_slices<M: RemoteMessage + Clone>(
        &self,
        sel: Vec<Slice>,
        message: M,
    ) -> Result<(), CastError>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
    {
        let _ = metrics::ACTOR_MESH_CAST_DURATION.start(hyperactor::kv_pairs!(
            "message_type" => M::typename(),
            "message_variant" => message.arm().unwrap_or_default(),
        ));
        for ref slice in sel {
            for rank in slice.iter() {
                let mut headers = Attrs::new();
                set_cast_info_on_headers(
                    &mut headers,
                    rank,
                    self.shape().clone(),
                    self.proc_mesh.client().actor_id().clone(),
                );
                self.ranks[rank]
                    .send_with_headers(self.proc_mesh.client(), headers, message.clone())
                    .map_err(|err| CastError::MailboxSenderError(rank, err))?;
            }
        }
        Ok(())
    }

    /// An event stream of proc events. Each ProcMesh can produce only one such
    /// stream, returning None after the first call.
    pub async fn next(&mut self) -> Option<ActorSupervisionEvent> {
        let result = self.actor_supervision_rx.recv().await;
        match result.as_ref() {
            Some(event) => {
                tracing::debug!("Received supervision event: {event:?}");
            }
            None => {
                tracing::info!(
                    "Supervision stream for actor mesh {} was closed!",
                    self.name
                );
            }
        };
        result
    }
}

#[async_trait]
impl<'a, A: RemoteActor> Mesh for RootActorMesh<'a, A> {
    type Node = ActorRef<A>;
    type Id = ActorMeshId;
    type Sliced<'b>
        = SlicedActorMesh<'b, A>
    where
        'a: 'b;

    fn shape(&self) -> &Shape {
        self.proc_mesh.shape()
    }

    fn select<R: Into<Range>>(
        &self,
        label: &str,
        range: R,
    ) -> Result<Self::Sliced<'_>, ShapeError> {
        Ok(SlicedActorMesh(self, self.shape().select(label, range)?))
    }

    fn get(&self, rank: usize) -> Option<ActorRef<A>> {
        self.ranks.get(rank).cloned()
    }

    fn id(&self) -> Self::Id {
        ActorMeshId(self.proc_mesh.id(), self.name.clone())
    }
}

impl<A: RemoteActor> ActorMesh for RootActorMesh<'_, A> {
    type Actor = A;

    fn proc_mesh(&self) -> &ProcMesh {
        &self.proc_mesh
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct SlicedActorMesh<'a, A: RemoteActor>(&'a RootActorMesh<'a, A>, Shape);

impl<'a, A: RemoteActor> SlicedActorMesh<'a, A> {
    pub fn new(actor_mesh: &'a RootActorMesh<'a, A>, shape: Shape) -> Self {
        Self(actor_mesh, shape)
    }

    pub fn shape(&self) -> &Shape {
        &self.1
    }
}

#[async_trait]
impl<A: RemoteActor> Mesh for SlicedActorMesh<'_, A> {
    type Node = ActorRef<A>;
    type Id = ActorMeshId;
    type Sliced<'b>
        = SlicedActorMesh<'b, A>
    where
        Self: 'b;

    fn shape(&self) -> &Shape {
        &self.1
    }

    fn select<R: Into<Range>>(
        &self,
        label: &str,
        range: R,
    ) -> Result<Self::Sliced<'_>, ShapeError> {
        Ok(Self(self.0, self.1.select(label, range)?))
    }

    fn get(&self, _index: usize) -> Option<ActorRef<A>> {
        unimplemented!()
    }

    fn id(&self) -> Self::Id {
        self.0.id()
    }
}

impl<A: RemoteActor> ActorMesh for SlicedActorMesh<'_, A> {
    type Actor = A;

    fn proc_mesh(&self) -> &ProcMesh {
        &self.0.proc_mesh
    }

    fn name(&self) -> &str {
        &self.0.name
    }
}

/// The type of error of casting operations.
#[derive(Debug, thiserror::Error)]
pub enum CastError {
    #[error("invalid selection {0}: {1}")]
    InvalidSelection(Selection, ShapeError),

    #[error("send on rank {0}: {1}")]
    MailboxSenderError(usize, MailboxSenderError),

    #[error(transparent)]
    RootMailboxSenderError(#[from] MailboxSenderError),

    #[error(transparent)]
    ShapeError(#[from] ShapeError),

    #[error(transparent)]
    SerializationError(#[from] bincode::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

// This has to be compiled outside of test mode because the bootstrap binary
// is not built in test mode, and requires access to TestActor.
pub(crate) mod test_util {
    use std::collections::VecDeque;

    use anyhow::ensure;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::PortRef;

    use super::*;
    use crate::comm::multicast::CastInfo;

    // This can't be defined under a `#[cfg(test)]` because there needs to
    // be an entry in the spawnable actor registry in the executable
    // 'hyperactor_mesh_test_bootstrap' for the `tests::process` actor
    // mesh test suite.
    #[derive(Debug)]
    #[hyperactor::export(
        spawn = true,
        handlers = [
            Echo { cast = true },
            GetRank { cast = true },
            Error { cast = true },
            Relay,
        ],
    )]
    pub struct TestActor;

    #[async_trait]
    impl Actor for TestActor {
        type Params = ();

        async fn new(_params: Self::Params) -> Result<Self, anyhow::Error> {
            Ok(Self)
        }
    }

    /// Request message to retrieve the actor's rank.
    ///
    /// The `bool` in the tuple controls the outcome of the handler:
    /// - If `true`, the handler will send the rank and return
    ///   `Ok(())`.
    /// - If `false`, the handler will still send the rank, but return
    ///   an error (`Err(...)`).
    ///
    /// This is useful for testing both successful and failing
    /// responses from a single message type.
    #[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
    pub struct GetRank(pub bool, #[binding(include)] pub PortRef<usize>);

    #[async_trait]
    impl Handler<GetRank> for TestActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            GetRank(ok, reply): GetRank,
        ) -> Result<(), anyhow::Error> {
            let (rank, _) = cx.cast_info()?;
            reply.send(cx, rank)?;
            anyhow::ensure!(ok, "intentional error!"); // If `!ok` exit with `Err()`.
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
    pub struct Echo(pub String, #[binding(include)] pub PortRef<String>);

    #[async_trait]
    impl Handler<Echo> for TestActor {
        async fn handle(&mut self, cx: &Context<Self>, message: Echo) -> Result<(), anyhow::Error> {
            let Echo(message, reply_port) = message;
            reply_port.send(cx, message)?;
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
    pub struct Error(pub String);

    #[async_trait]
    impl Handler<Error> for TestActor {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
            Error(error): Error,
        ) -> Result<(), anyhow::Error> {
            Err(anyhow::anyhow!("{}", error))
        }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone)]
    pub struct Relay(pub usize, pub VecDeque<PortRef<Relay>>);

    #[async_trait]
    impl Handler<Relay> for TestActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            Relay(count, mut hops): Relay,
        ) -> Result<(), anyhow::Error> {
            ensure!(!hops.is_empty(), "relay must have at least one hop");
            let next = hops.pop_front().unwrap();
            next.send(cx, Relay(count + 1, hops))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hyperactor::ActorId;
    use hyperactor::PortRef;
    use hyperactor::ProcId;
    use hyperactor::WorldId;
    use hyperactor::attrs::Attrs;

    use super::*;
    use crate::proc_mesh::ProcEvent;

    // These tests are parametric over allocators.
    #[macro_export]
    macro_rules! actor_mesh_test_suite {
        ($allocator:expr_2021) => {
            use std::assert_matches::assert_matches;

            use ndslice::shape;
            use $crate::alloc::AllocSpec;
            use $crate::alloc::Allocator;
            use $crate::assign::Ranks;
            use $crate::sel_from_shape;
            use $crate::sel;
            use $crate::proc_mesh::SharedSpawnable;
            use std::collections::VecDeque;
            use hyperactor::data::Serialized;

            use super::*;
            use super::test_util::*;

            #[tokio::test]
            async fn test_basic() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        shape: shape! { replica = 4 },
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();

                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let actor_mesh: RootActorMesh<TestActor> = proc_mesh.spawn("echo", &()).await.unwrap();
                let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
                actor_mesh
                    .cast(sel!(*), Echo("Hello".to_string(), reply_handle.bind()))
                    .unwrap();
                for _ in 0..4 {
                    assert_eq!(&reply_receiver.recv().await.unwrap(), "Hello");
                }
            }

            #[tokio::test]
            async fn test_ping_pong() {
                use hyperactor::test_utils::pingpong::PingPongActor;
                use hyperactor::test_utils::pingpong::PingPongMessage;
                use hyperactor::test_utils::pingpong::PingPongActorParams;

                let alloc = $allocator
                    .allocate(AllocSpec {
                        shape: shape! { replica = 2  },
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();
                let mesh = ProcMesh::allocate(alloc).await.unwrap();

                let (undeliverable_msg_tx, _) = mesh.client().open_port();
                let ping_pong_actor_params = PingPongActorParams::new(undeliverable_msg_tx.bind(), None);
                let actor_mesh: RootActorMesh<PingPongActor> = mesh
                    .spawn::<PingPongActor>("ping-pong", &ping_pong_actor_params)
                    .await
                    .unwrap();

                let ping: ActorRef<PingPongActor> = actor_mesh.get(0).unwrap();
                let pong: ActorRef<PingPongActor> = actor_mesh.get(1).unwrap();
                let (done_tx, done_rx) = mesh.client().open_once_port();
                ping.send(mesh.client(), PingPongMessage(4, pong.clone(), done_tx.bind())).unwrap();

                assert!(done_rx.recv().await.unwrap());
            }

            #[tokio::test]
            async fn test_pingpong_full_mesh() {
                use hyperactor::test_utils::pingpong::PingPongActor;
                use hyperactor::test_utils::pingpong::PingPongActorParams;
                use hyperactor::test_utils::pingpong::PingPongMessage;

                use futures::future::join_all;

                const X: usize = 3;
                const Y: usize = 3;
                const Z: usize = 3;
                let alloc = $allocator
                    .allocate(AllocSpec {
                        shape: shape! { x = X, y = Y, z = Z },
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();

                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let (undeliverable_tx, _undeliverable_rx) = proc_mesh.client().open_port();
                let params = PingPongActorParams::new(undeliverable_tx.bind(), None);
                let actor_mesh = proc_mesh.spawn::<PingPongActor>("pingpong", &params).await.unwrap();
                let slice = actor_mesh.shape().slice();

                let mut futures = Vec::new();
                for rank in slice.iter() {
                    let actor = actor_mesh.get(rank).unwrap();
                    let coords = (&slice.coordinates(rank).unwrap()[..]).try_into().unwrap();
                    let sizes = (&slice.sizes())[..].try_into().unwrap();
                    let neighbors = ndslice::utils::stencil::moore_neighbors::<3>();
                    for neighbor_coords in ndslice::utils::apply_stencil(&coords, sizes, &neighbors) {
                        if let Ok(neighbor_rank) = slice.location(&neighbor_coords) {
                            let neighbor = actor_mesh.get(neighbor_rank).unwrap();
                            let (done_tx, done_rx) = proc_mesh.client().open_once_port();
                            actor
                                .send(
                                    proc_mesh.client(),
                                    PingPongMessage(4, neighbor.clone(), done_tx.bind()),
                                )
                                .unwrap();
                            futures.push(done_rx.recv());
                        }
                    }
                }
                let results = join_all(futures).await;
                assert_eq!(results.len(), 316); // 5180 messages
                for result in results {
                    assert_eq!(result.unwrap(), true);
                }
            }

            #[tokio::test]
            async fn test_cast() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        shape: shape! { replica = 2, host = 2, gpu = 8 },
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();

                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let actor_mesh: RootActorMesh<TestActor> = proc_mesh.spawn("echo", &()).await.unwrap();
                let dont_simulate_error = true;
                let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
                actor_mesh
                    .cast(sel!(*), GetRank(dont_simulate_error, reply_handle.bind()))
                    .unwrap();
                let mut ranks = Ranks::new(actor_mesh.shape().slice().len());
                while !ranks.is_full() {
                    let rank = reply_receiver.recv().await.unwrap();
                    assert!(ranks.insert(rank, rank).is_none(), "duplicate rank {rank}");
                }
                // Retrieve all GPUs on replica=0, host=0
                let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
                actor_mesh
                    .cast(
                        sel_from_shape!(actor_mesh.shape(), replica = 0, host = 0),
                        GetRank(dont_simulate_error, reply_handle.bind()),
                    )
                    .unwrap();
                let mut ranks = Ranks::new(8);
                while !ranks.is_full() {
                    let rank = reply_receiver.recv().await.unwrap();
                    assert!(ranks.insert(rank, rank).is_none(), "duplicate rank {rank}");
                }
            }

            #[tokio::test]
            async fn test_inter_actor_comms() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        // Sizes intentionally small to keep the time
                        // required for this test in the process case
                        // reasonable (< 60s).
                        shape: shape! { replica = 2, host = 2, gpu = 8 },
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();

                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();
                let actor_mesh: RootActorMesh<TestActor> = proc_mesh.spawn("echo", &()).await.unwrap();

                // Bounce the message through all actors and return it to the sender (us).
                let mut hops: VecDeque<_> = actor_mesh.iter().map(|actor| actor.port()).collect();
                let (handle, mut rx) = proc_mesh.client().open_port();
                hops.push_back(handle.bind());
                hops.pop_front()
                    .unwrap()
                    .send(proc_mesh.client(), Relay(0, hops))
                    .unwrap();
                assert_matches!(
                    rx.recv().await.unwrap(),
                    Relay(count, hops)
                        if count == actor_mesh.shape().slice().len()
                        && hops.is_empty());
            }

            #[tokio::test]
            async fn test_inter_proc_mesh_comms() {
                let mut meshes = Vec::new();
                for _ in 0..2 {
                    let alloc = $allocator
                        .allocate(AllocSpec {
                            shape: shape! { replica = 1 },
                            constraints: Default::default(),
                        })
                        .await
                        .unwrap();

                    let proc_mesh = Arc::new(ProcMesh::allocate(alloc).await.unwrap());
                    let proc_mesh_clone = Arc::clone(&proc_mesh);
                    let actor_mesh : RootActorMesh<TestActor> = proc_mesh_clone.spawn("echo", &()).await.unwrap();
                    meshes.push((proc_mesh, actor_mesh));
                }

                let mut hops: VecDeque<_> = meshes
                    .iter()
                    .flat_map(|(_proc_mesh, actor_mesh)| actor_mesh.iter())
                    .map(|actor| actor.port())
                    .collect();
                let num_hops = hops.len();

                let client = meshes[0].0.client();
                let (handle, mut rx) = client.open_port();
                hops.push_back(handle.bind());
                hops.pop_front()
                    .unwrap()
                    .send(client, Relay(0, hops))
                    .unwrap();
                assert_matches!(
                    rx.recv().await.unwrap(),
                    Relay(count, hops)
                        if count == num_hops
                        && hops.is_empty());
            }

            #[timed_test::async_timed_test(timeout_secs = 60)]
            async fn test_actor_mesh_cast() {
                // Verify a full broadcast in the mesh. Send a message
                // to every actor and check each actor receives it.

                use $crate::sel;
                use $crate::comm::test_utils::TestActor as CastTestActor;
                use $crate::comm::test_utils::TestActorParams as CastTestActorParams;
                use $crate::comm::test_utils::TestMessage as CastTestMessage;

                let shape = shape! {replica = 4, host = 4, gpu = 4 };
                let num_actors = shape.slice().len();
                let alloc = $allocator
                    .allocate(AllocSpec {
                        shape,
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();

                let proc_mesh = ProcMesh::allocate(alloc).await.unwrap();

                let (tx, mut rx) = hyperactor::mailbox::open_port(proc_mesh.client());
                let params = CastTestActorParams{ forward_port: tx.bind() };
                let actor_mesh: RootActorMesh<CastTestActor> = proc_mesh.spawn("actor", &params).await.unwrap();

                actor_mesh.cast(sel!(*), CastTestMessage::Forward("abc".to_string())).unwrap();

                for _ in 0..num_actors {
                    assert_eq!(rx.recv().await.unwrap(), CastTestMessage::Forward("abc".to_string()));
                }
            }

            #[tokio::test]
            async fn test_delivery_failure() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        shape: shape! { replica = 1  },
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();

                let name = alloc.name().to_string();
                let mut mesh = ProcMesh::allocate(alloc).await.unwrap();
                let mut events = mesh.events().unwrap();

                // Send a message to a non-existent actor (the proc however exists).
                let unmonitored_reply_to = mesh.client().open_port::<usize>().0.bind();
                let bad_actor = ActorRef::<TestActor>::attest(ActorId(ProcId(WorldId(name.clone()), 0), "foo".into(), 0));
                bad_actor.send(mesh.client(), GetRank(true, unmonitored_reply_to)).unwrap();

                // The message will be returned!
                assert_matches!(
                    events.next().await.unwrap(),
                    ProcEvent::Crashed(0, reason) if reason.contains("failed: message not delivered")
                );

                // TODO: Stop the proc.
            }

            #[tokio::test]
            async fn test_send_with_headers() {
                let alloc = $allocator
                    .allocate(AllocSpec {
                        shape: shape! { replica = 1  },
                        constraints: Default::default(),
                    })
                    .await
                    .unwrap();

                let mesh = ProcMesh::allocate(alloc).await.unwrap();
                let (reply_port_handle, mut reply_port_receiver) = mesh.client().open_port::<usize>();
                let reply_port = reply_port_handle.bind();

                let actor_mesh: RootActorMesh<TestActor> = mesh.spawn("test", &()).await.unwrap();
                let actor_ref = actor_mesh.get(0).unwrap();
                let mut headers = Attrs::new();
                set_cast_info_on_headers(&mut headers, 0, Shape::unity(), mesh.client().actor_id().clone());
                actor_ref.send_with_headers(mesh.client(), headers.clone(), GetRank(true, reply_port.clone())).unwrap();
                assert_eq!(0, reply_port_receiver.recv().await.unwrap());

                set_cast_info_on_headers(&mut headers, 1, Shape::unity(), mesh.client().actor_id().clone());
                actor_ref.port()
                    .send_with_headers(mesh.client(), headers.clone(), GetRank(true, reply_port.clone()))
                    .unwrap();
                assert_eq!(1, reply_port_receiver.recv().await.unwrap());

                set_cast_info_on_headers(&mut headers, 2, Shape::unity(), mesh.client().actor_id().clone());
                actor_ref.actor_id()
                    .port_id(GetRank::port())
                    .send_with_headers(
                        mesh.client(),
                        &Serialized::serialize(&GetRank(true, reply_port)).unwrap(),
                        headers
                    );
                assert_eq!(2, reply_port_receiver.recv().await.unwrap());
                // TODO: Stop the proc.
            }
        }
    }

    mod local {
        use crate::alloc::local::LocalAllocator;

        actor_mesh_test_suite!(LocalAllocator);

        #[tokio::test]
        async fn test_send_failure() {
            use hyperactor::test_utils::pingpong::PingPongActor;
            use hyperactor::test_utils::pingpong::PingPongActorParams;
            use hyperactor::test_utils::pingpong::PingPongMessage;

            use crate::alloc::ProcStopReason;
            use crate::proc_mesh::ProcEvent;

            let config = hyperactor::config::global::lock();
            let _guard = config.override_key(
                hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
                tokio::time::Duration::from_secs(1),
            );

            let alloc = LocalAllocator
                .allocate(AllocSpec {
                    shape: shape! { replica = 2  },
                    constraints: Default::default(),
                })
                .await
                .unwrap();
            let monkey = alloc.chaos_monkey();
            let mut mesh = ProcMesh::allocate(alloc).await.unwrap();
            let mut events = mesh.events().unwrap();

            let ping_pong_actor_params = PingPongActorParams::new(
                PortRef::attest_message_port(mesh.client().actor_id()),
                None,
            );
            let actor_mesh: RootActorMesh<PingPongActor> = mesh
                .spawn::<PingPongActor>("ping-pong", &ping_pong_actor_params)
                .await
                .unwrap();

            let ping: ActorRef<PingPongActor> = actor_mesh.get(0).unwrap();
            let pong: ActorRef<PingPongActor> = actor_mesh.get(1).unwrap();

            // Kill ping.
            monkey(0, ProcStopReason::Killed(0, false));
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Stopped(0, ProcStopReason::Killed(0, false))
            );

            // Try to send a message to 'ping'. Since 'ping's mailbox
            // is stopped, the send will timeout and fail.
            let (unmonitored_done_tx, _) = mesh.client().open_once_port();
            ping.send(
                mesh.client(),
                PingPongMessage(1, pong.clone(), unmonitored_done_tx.bind()),
            )
            .unwrap();

            // The message will be returned!
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("failed: message not delivered")
            );

            // Get 'pong' to send 'ping' a message. Since 'ping's
            // mailbox is stopped, the send will timeout and fail.
            let (unmonitored_done_tx, _) = mesh.client().open_once_port();
            pong.send(
                mesh.client(),
                PingPongMessage(1, ping.clone(), unmonitored_done_tx.bind()),
            )
            .unwrap();

            // The message will be returned!
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("failed: message not delivered")
            );
        }

        #[tokio::test]
        async fn test_cast_failure() {
            use crate::alloc::ProcStopReason;
            use crate::proc_mesh::ProcEvent;
            use crate::sel;

            let alloc = LocalAllocator
                .allocate(AllocSpec {
                    shape: shape! { replica = 1  },
                    constraints: Default::default(),
                })
                .await
                .unwrap();

            let stop = alloc.stopper();
            let mut mesh = ProcMesh::allocate(alloc).await.unwrap();
            let mut events = mesh.events().unwrap();

            let actor_mesh = mesh
                .spawn::<TestActor>("reply-then-fail", &())
                .await
                .unwrap();

            // `GetRank` with `false` means exit with error after
            // replying with rank.
            let (reply_handle, mut reply_receiver) = actor_mesh.open_port();
            actor_mesh
                .cast(sel!(*), GetRank(false, reply_handle.bind()))
                .unwrap();
            let rank = reply_receiver.recv().await.unwrap();
            assert_eq!(rank, 0);

            // The above is expected to trigger a proc crash.
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("intentional error!")
            );

            // Cast the message.
            let (reply_handle, _) = actor_mesh.open_port();
            actor_mesh
                .cast(sel!(*), GetRank(false, reply_handle.bind()))
                .unwrap();

            // The message will be returned!
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Crashed(0, reason) if reason.contains("failed: message not delivered")
            );

            // Stop the mesh.
            stop();
            assert_matches!(
                events.next().await.unwrap(),
                ProcEvent::Stopped(0, ProcStopReason::Stopped),
            );
            assert!(events.next().await.is_none());
        }
    } // mod local

    mod process {
        use tokio::process::Command;

        use crate::alloc::process::ProcessAllocator;

        #[cfg(fbcode_build)] // we use an external binary, produced by buck
        actor_mesh_test_suite!(ProcessAllocator::new(Command::new(
            buck_resources::get("monarch/hyperactor_mesh/bootstrap").unwrap()
        )));
    }
}
