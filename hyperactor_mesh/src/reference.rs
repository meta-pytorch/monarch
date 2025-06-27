/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::Ord;
use std::cmp::PartialOrd;
use std::hash::Hash;
use std::marker::PhantomData;

use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::WorldId;
use hyperactor::actor::RemoteActor;
use hyperactor::cap;
use hyperactor::channel::ChannelAddr;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use ndslice::Selection;
use ndslice::Shape;
use serde::Deserialize;
use serde::Serialize;

use crate::CommActor;
use crate::ProcMesh;
use crate::actor_mesh::Cast;
use crate::actor_mesh::CastError;
use crate::actor_mesh::actor_mesh_cast;
use crate::proc_mesh::mesh_agent::MeshAgent;

#[macro_export]
macro_rules! mesh_id {
    ($proc_mesh:ident) => {
        $crate::reference::ProcMeshId(stringify!($proc_mesh).to_string(), "0".into())
    };
    ($proc_mesh:ident . $actor_mesh:ident) => {
        $crate::reference::ActorMeshId(
            $crate::reference::ProcMeshId(stringify!($proc_mesh).to_string()),
            stringify!($proc_mesh).to_string(),
        )
    };
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    Named
)]
pub struct ProcMeshId(pub String);

/// Actor Mesh ID.  Tuple of the ProcMesh ID and Actor Mesh ID.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    Named
)]
pub struct ActorMeshId(pub ProcMeshId, pub String);

/// Types references to Actor Meshes.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActorMeshRef<A: RemoteActor> {
    pub(crate) mesh_id: ActorMeshId,
    shape: Shape,
    /// The shape of the underlying Proc Mesh.
    proc_mesh_shape: Shape,
    phantom: PhantomData<A>,
}

impl<A: RemoteActor> ActorMeshRef<A> {
    /// The caller guarantees that the provided mesh ID is also a valid,
    /// typed reference.  This is usually invoked to provide a guarantee
    /// that an externally-provided mesh ID (e.g., through a command
    /// line argument) is a valid reference.
    pub(crate) fn attest(mesh_id: ActorMeshId, shape: Shape, proc_mesh_shape: Shape) -> Self {
        Self {
            mesh_id,
            shape,
            proc_mesh_shape,
            phantom: PhantomData,
        }
    }

    /// The Actor Mesh ID corresponding with this reference.
    pub fn mesh_id(&self) -> &ActorMeshId {
        &self.mesh_id
    }

    /// Shape of the Actor Mesh.
    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    /// Shape of the underlying Proc Mesh.
    fn proc_mesh_shape(&self) -> &Shape {
        &self.proc_mesh_shape
    }

    fn name(&self) -> &str {
        &self.mesh_id.1
    }

    /// Cast an [`M`]-typed message to the ranks selected by `sel`
    /// in this ActorMesh.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
    pub fn cast<M: Castable + Clone>(
        &self,
        caps: &(impl cap::CanSend + cap::CanOpenPort),
        selection: Selection,
        message: M,
    ) -> Result<(), CastError>
    where
        A: RemoteHandles<Cast<M>> + RemoteHandles<IndexedErasedUnbound<Cast<M>>>,
    {
        let world_id = WorldId(self.mesh_id.0.0.clone());
        let comm_actor_id = ActorId(ProcId(world_id, 0), "comm".to_string(), 0);

        actor_mesh_cast::<M, A>(
            caps,
            self.shape(),
            self.proc_mesh_shape(),
            self.name(),
            caps.mailbox().actor_id(),
            &ActorRef::<CommActor>::attest(comm_actor_id),
            selection,
            message,
        )?;

        Ok(())
    }
}

impl<A: RemoteActor> Clone for ActorMeshRef<A> {
    fn clone(&self) -> Self {
        Self {
            mesh_id: self.mesh_id.clone(),
            shape: self.shape.clone(),
            proc_mesh_shape: self.proc_mesh_shape.clone(),
            phantom: PhantomData,
        }
    }
}

impl<A: RemoteActor> PartialEq for ActorMeshRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.mesh_id == other.mesh_id && self.shape == other.shape
    }
}

impl<A: RemoteActor> Eq for ActorMeshRef<A> {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcMeshRef {
    mesh_id: ProcMeshId,
    shape: Shape,
    ranks: Vec<(ProcId, (ChannelAddr, ActorRef<MeshAgent>))>,
}

impl ProcMeshRef {
    pub(crate) fn attest(
        mesh_id: ProcMeshId,
        shape: Shape,
        ranks: Vec<(ProcId, (ChannelAddr, ActorRef<MeshAgent>))>,
    ) -> Self {
        Self {
            mesh_id,
            shape,
            ranks,
        }
    }

    /// Spawn an `ActorMesh` by launching the same actor type on all
    /// agents, using the **same** parameters instance for every
    /// actor.
    ///
    /// - `actor_name`: Name for all spawned actors.
    /// - `params`: Reference to the parameter struct, reused for all
    ///   actors.
    ///
    /// This will return an `ActorMeshRef` that
    /// can be used to cast messages to `Actors` in the mesh.
    pub async fn spawn<A: Actor + RemoteActor>(
        &self,
        caps: &(impl cap::CanSend + cap::CanOpenPort),
        actor_name: &str,
        params: &A::Params,
    ) -> Result<ActorMeshRef<A>, anyhow::Error>
    where
        A::Params: RemoteMessage,
    {
        ProcMesh::spawn_on_procs::<A>(caps, self.agents(), actor_name, params).await?;

        Ok(ActorMeshRef::attest(
            ActorMeshId(self.mesh_id.clone(), actor_name.to_string()),
            self.shape.clone(),
            self.shape.clone(),
        ))
    }

    fn agents(&self) -> impl Iterator<Item = ActorRef<MeshAgent>> + '_ {
        self.ranks.iter().map(|(_, (_, agent))| agent.clone())
    }

    /// The `ProcMeshId`` of the `ProcMesh`
    pub fn mesh_id(&self) -> &ProcMeshId {
        &self.mesh_id
    }

    /// The shape of the `ProcMesh`.
    pub fn shape(&self) -> &Shape {
        &self.shape
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::Handler;
    use hyperactor::Instance;
    use hyperactor::PortRef;
    use hyperactor::message::Bind;
    use hyperactor::message::Bindings;
    use hyperactor::message::Unbind;
    use hyperactor_mesh_macros::sel;
    use ndslice::shape;

    use super::*;
    use crate::Mesh;
    use crate::ProcMesh;
    use crate::RootActorMesh;
    use crate::actor_mesh::ActorMesh;
    use crate::alloc::AllocSpec;
    use crate::alloc::Allocator;
    use crate::alloc::LocalAllocator;

    fn shape() -> Shape {
        shape! { replica = 4 }
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone)]
    struct MeshPingPongMessage(
        /*ttl:*/ u64,
        ActorMeshRef<MeshPingPongActor>,
        /*completed port:*/ PortRef<bool>,
    );

    #[derive(Debug, Clone)]
    #[hyperactor::export(
        spawn = true,
        handlers = [
            Cast<MeshPingPongMessage>,
            IndexedErasedUnbound<Cast<MeshPingPongMessage>>
        ]
    )]
    struct MeshPingPongActor {
        mesh_ref: ActorMeshRef<MeshPingPongActor>,
    }

    #[derive(Debug, Serialize, Deserialize, Named, Clone)]
    struct MeshPingPongActorParams {
        mesh_id: ActorMeshId,
        shape: Shape,
        proc_mesh_shape: Shape,
    }

    #[async_trait]
    impl Actor for MeshPingPongActor {
        type Params = MeshPingPongActorParams;

        async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
            Ok(Self {
                mesh_ref: ActorMeshRef::attest(
                    params.mesh_id,
                    params.shape,
                    params.proc_mesh_shape,
                ),
            })
        }
    }

    #[async_trait]
    impl Handler<Cast<MeshPingPongMessage>> for MeshPingPongActor {
        async fn handle(
            &mut self,
            this: &Instance<Self>,
            Cast {
                message: MeshPingPongMessage(ttl, sender_mesh, done_tx),
                ..
            }: Cast<MeshPingPongMessage>,
        ) -> Result<(), anyhow::Error> {
            if ttl == 0 {
                done_tx.send(this, true)?;
                return Ok(());
            }
            let msg = MeshPingPongMessage(ttl - 1, self.mesh_ref.clone(), done_tx);
            sender_mesh.cast(this, sel!(?), msg)?;
            Ok(())
        }
    }

    impl Unbind for MeshPingPongMessage {
        fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
            self.2.unbind(bindings)
        }
    }

    impl Bind for MeshPingPongMessage {
        fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
            self.2.bind(bindings)
        }
    }

    #[tokio::test]
    async fn test_inter_mesh_ping_pong() {
        let alloc_ping = LocalAllocator
            .allocate(AllocSpec {
                shape: shape(),
                constraints: Default::default(),
            })
            .await
            .unwrap();
        let alloc_pong = LocalAllocator
            .allocate(AllocSpec {
                shape: shape(),
                constraints: Default::default(),
            })
            .await
            .unwrap();
        let ping_proc_mesh = ProcMesh::allocate(alloc_ping).await.unwrap();
        let ping_mesh: RootActorMesh<MeshPingPongActor> = ping_proc_mesh
            .spawn(
                "ping",
                &MeshPingPongActorParams {
                    mesh_id: ActorMeshId(
                        ProcMeshId(ping_proc_mesh.world_id().to_string()),
                        "ping".to_string(),
                    ),
                    shape: ping_proc_mesh.shape().clone(),
                    proc_mesh_shape: ping_proc_mesh.shape().clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(ping_proc_mesh.shape(), ping_mesh.shape());

        let pong_proc_mesh = ProcMesh::allocate(alloc_pong).await.unwrap();
        let pong_mesh: RootActorMesh<MeshPingPongActor> = pong_proc_mesh
            .spawn(
                "pong",
                &MeshPingPongActorParams {
                    mesh_id: ActorMeshId(
                        ProcMeshId(pong_proc_mesh.world_id().to_string()),
                        "pong".to_string(),
                    ),
                    shape: pong_proc_mesh.shape().clone(),
                    proc_mesh_shape: pong_proc_mesh.shape().clone(),
                },
            )
            .await
            .unwrap();

        let ping_mesh_ref: ActorMeshRef<MeshPingPongActor> = ping_mesh.bind();
        let pong_mesh_ref: ActorMeshRef<MeshPingPongActor> = pong_mesh.bind();

        let (done_tx, mut done_rx) = ping_proc_mesh.client().open_port::<bool>();
        ping_mesh_ref
            .cast(
                ping_proc_mesh.client(),
                sel!(?),
                MeshPingPongMessage(10, pong_mesh_ref, done_tx.bind()),
            )
            .unwrap();

        assert!(done_rx.recv().await.unwrap());
    }

    #[tokio::test]
    async fn test_inter_mesh_ping_pong_proc_mesh_ref() {
        let alloc_ping = LocalAllocator
            .allocate(AllocSpec {
                shape: shape(),
                constraints: Default::default(),
            })
            .await
            .unwrap();
        let alloc_pong = LocalAllocator
            .allocate(AllocSpec {
                shape: shape(),
                constraints: Default::default(),
            })
            .await
            .unwrap();
        let ping_proc_mesh = ProcMesh::allocate(alloc_ping).await.unwrap();
        let ping_proc_mesh_ref = ping_proc_mesh.bind();
        let ping_mesh_ref: ActorMeshRef<MeshPingPongActor> = ping_proc_mesh_ref
            .spawn(
                ping_proc_mesh.client(),
                "ping",
                &MeshPingPongActorParams {
                    mesh_id: ActorMeshId(
                        ProcMeshId(ping_proc_mesh.world_id().to_string()),
                        "ping".to_string(),
                    ),
                    shape: ping_proc_mesh.shape().clone(),
                    proc_mesh_shape: ping_proc_mesh.shape().clone(),
                },
            )
            .await
            .unwrap();

        let pong_proc_mesh = ProcMesh::allocate(alloc_pong).await.unwrap();
        let pong_proc_mesh_ref = pong_proc_mesh.bind();
        let pong_mesh_ref: ActorMeshRef<MeshPingPongActor> = pong_proc_mesh_ref
            .spawn(
                pong_proc_mesh.client(),
                "pong",
                &MeshPingPongActorParams {
                    mesh_id: ActorMeshId(
                        ProcMeshId(pong_proc_mesh.world_id().to_string()),
                        "pong".to_string(),
                    ),
                    shape: pong_proc_mesh.shape().clone(),
                    proc_mesh_shape: pong_proc_mesh.shape().clone(),
                },
            )
            .await
            .unwrap();

        let (done_tx, mut done_rx) = ping_proc_mesh.client().open_port::<bool>();
        ping_mesh_ref
            .cast(
                ping_proc_mesh.client(),
                sel!(?),
                MeshPingPongMessage(10, pong_mesh_ref, done_tx.bind()),
            )
            .unwrap();

        assert!(done_rx.recv().await.unwrap());
    }
}
