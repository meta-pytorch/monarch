/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::type_name;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::RemoteMessage;
use hyperactor::actor::RemoteActor;
use hyperactor::actor::remote::Remote;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::context;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxServer;
use ndslice::Extent;
use ndslice::view;
use ndslice::view::MapIntoRefExt;
use ndslice::view::Region;
use serde::Deserialize;
use serde::Serialize;

use crate::alloc::Alloc;
use crate::alloc::AllocExt;
use crate::alloc::AllocatedProc;
use crate::assign::Ranks;
use crate::proc_mesh::mesh_agent::GspawnResult;
use crate::proc_mesh::mesh_agent::MeshAgentMessageClient;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::v1;
use crate::v1::ActorMesh;
use crate::v1::Error;
use crate::v1::Name;
use crate::v1::ValueMesh;

/// A reference to a single [`hyperactor::Proc`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcRef {
    proc_id: ProcId,
    /// The rank of this proc at creation.
    create_rank: usize,
    /// The agent managing this proc.
    agent: ActorRef<ProcMeshAgent>,
}

impl ProcRef {
    /// Pings the proc, returning whether it is alive. This will be replaced by a
    /// finer-grained lifecycle status in the near future.
    #[allow(dead_code)]
    async fn status(&self, cx: &impl context::Actor) -> v1::Result<bool> {
        let (port, mut rx) = cx.mailbox().open_port();
        self.agent
            .status(cx, port.bind())
            .await
            .map_err(|e| Error::CallError(self.agent.actor_id().clone(), e))?;
        loop {
            let (rank, status) = rx
                .recv()
                .await
                .map_err(|e| Error::CallError(self.agent.actor_id().clone(), e.into()))?;
            if rank == self.create_rank {
                break Ok(status);
            }
        }
    }

    pub(crate) fn actor_id(&self, name: &Name) -> ActorId {
        self.proc_id.actor_id(name.to_string(), 0)
    }

    pub(crate) fn attest<A: RemoteActor>(&self, name: &Name) -> ActorRef<A> {
        ActorRef::attest(self.actor_id(name))
    }
}

/// A mesh of processes.
#[derive(Named)]
pub struct ProcMesh {
    name: Name,
    allocation: ProcMeshAllocation,
}

impl ProcMesh {
    /// Freeze this proc mesh in its current state, returning a stable
    /// reference that may be serialized.
    pub fn freeze(&self) -> ProcMeshRef {
        let region = self.allocation.extent().clone().into();
        match &self.allocation {
            ProcMeshAllocation::Allocated { ranks, .. } => {
                ProcMeshRef::new(self.name.clone(), region, Arc::clone(ranks)).unwrap()
            }
        }
    }

    /// Allocate a new ProcMeshRef from the provided alloc.
    /// Allocate does not require an owning actor because references are not owned.
    /// Allocate a new ProcMesh from the provided alloc.
    pub async fn allocate(
        cx: &impl context::Actor,
        mut alloc: impl Alloc + Send + Sync + 'static,
        name: &str,
    ) -> v1::Result<Self> {
        let running = alloc.initialize().await?;

        // Wire the newly created mesh into the proc, so that it is routable.
        // We route all of the relevant prefixes into the proc's forwarder,
        // and serve it on the alloc's transport.
        //
        // This will be removed with direct addressing.
        let proc = cx.instance().proc();

        // First make sure we can serve the proc:
        let (proc_channel_addr, rx) = channel::serve(ChannelAddr::any(alloc.transport())).await?;
        proc.clone().serve(rx);

        let router = proc
            .forwarder()
            .downcast_ref::<DialMailboxRouter>()
            .ok_or(Error::UnroutableMesh())?;
        // Route all of the allocated procs:
        for AllocatedProc { proc_id, addr, .. } in running.iter() {
            if proc_id.is_direct() {
                continue;
            }
            router.bind(proc_id.clone().into(), addr.clone());
        }

        // Set up the mesh agents. Since references are not owned, we don't supervise it.
        // Instead, we just let procs die when they have unhandled supervision events.
        let address_book: HashMap<_, _> = running
            .iter()
            .map(
                |AllocatedProc {
                     addr, mesh_agent, ..
                 }| { (mesh_agent.actor_id().proc_id().clone(), addr.clone()) },
            )
            .collect();

        let (config_handle, mut config_receiver) = cx.mailbox().open_port();
        for (rank, AllocatedProc { mesh_agent, .. }) in running.iter().enumerate() {
            mesh_agent
                .configure(
                    cx,
                    rank,
                    proc_channel_addr.clone(),
                    None, // no supervisor; we just crash
                    address_book.clone(),
                    config_handle.bind(),
                )
                .await
                .map_err(Error::ConfigurationError)?;
        }
        let mut completed = Ranks::new(running.len());
        while !completed.is_full() {
            let rank = config_receiver
                .recv()
                .await
                .map_err(|err| Error::ConfigurationError(err.into()))?;
            if completed.insert(rank, rank).is_some() {
                tracing::warn!("multiple completions received for rank {}", rank);
            }
        }

        let ranks = running
            .into_iter()
            .enumerate()
            .map(|(create_rank, allocated)| ProcRef {
                proc_id: allocated.proc_id,
                create_rank,
                agent: allocated.mesh_agent,
            })
            .collect();

        Ok(Self {
            name: Name::new(name),
            allocation: ProcMeshAllocation::Allocated {
                alloc: Box::new(alloc),
                ranks: Arc::new(ranks),
            },
        })
    }
}

/// Represents different ways ProcMeshes can be allocated.
enum ProcMeshAllocation {
    /// A mesh that has been allocated from an `Alloc`.
    Allocated {
        // We have to hold on to the alloc for the duration of the mesh lifetime.
        // The procmesh inherits the alloc's extent.
        alloc: Box<dyn Alloc + Send + Sync + 'static>,

        // The allocated ranks.
        ranks: Arc<Vec<ProcRef>>,
    },
}

impl ProcMeshAllocation {
    fn extent(&self) -> &Extent {
        match self {
            ProcMeshAllocation::Allocated { alloc, .. } => alloc.extent(),
        }
    }
}

impl fmt::Debug for ProcMeshAllocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcMeshAllocation::Allocated { ranks, .. } => f
                .debug_struct("ProcMeshAllocation")
                .field("alloc", &"<dyn Alloc>")
                .field("ranks", ranks)
                .finish(),
        }
    }
}

/// A reference to a ProcMesh, consisting of a set of ranked [`ProcRef`]s,
/// arranged into a region. ProcMeshes named, uniquely identifying the
/// ProcMesh from which the reference was derived.
///
/// ProcMeshes can be sliced to create new ProcMeshes with a subset of the
/// original ranks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct ProcMeshRef {
    name: Name,
    region: Region,
    ranks: Arc<Vec<ProcRef>>,
}

impl ProcMeshRef {
    /// Create a new ProcMeshRef from the given name, region, and ranks.
    fn new(name: Name, region: Region, ranks: Arc<Vec<ProcRef>>) -> v1::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(v1::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        Ok(Self {
            name,
            region,
            ranks,
        })
    }

    /// Maps over all of the ProcRefs in the mesh, returning a new
    /// ValueMesh with the mapped values. This is infallible because
    /// the mapping is 1:1 with the ranks.
    fn mapped<F, R>(&self, f: F) -> ValueMesh<R>
    where
        F: Fn(&ProcRef) -> R,
    {
        ValueMesh::new_unchecked(self.region.clone(), self.ranks.iter().map(f).collect())
    }

    /// The current statuses of procs in this mesh.
    #[allow(dead_code)]
    async fn status(&self, cx: &impl context::Actor) -> v1::Result<ValueMesh<bool>> {
        let vm: ValueMesh<_> = self.map_into_ref(|proc_ref| {
            let proc_ref = proc_ref.clone();
            async move { proc_ref.status(cx).await }
        });
        vm.join().await.transpose()
    }

    /// Spawn an actor on all of the procs in this mesh, returning a new ActorMesh.
    #[allow(dead_code)]
    async fn spawn<A: Actor + RemoteActor>(
        &self,
        cx: &impl context::Actor,
        name: &str,
        params: &A::Params,
    ) -> v1::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
    {
        let remote = Remote::collect();
        let actor_type = remote
            .name_of::<A>()
            .ok_or(Error::ActorTypeNotRegistered(type_name::<A>().to_string()))?
            .to_string();

        let name = Name::new(name);
        let serialized_params = bincode::serialize(params)?;

        let (completed_handle, mut completed_receiver) = cx.mailbox().open_port();
        for proc_ref in self.ranks.iter() {
            proc_ref
                .agent
                .gspawn(
                    cx,
                    actor_type.clone(),
                    name.clone().to_string(),
                    serialized_params.clone(),
                    completed_handle.bind(),
                )
                .await
                .map_err(|e| Error::CallError(proc_ref.agent.actor_id().clone(), e))?;
        }

        let mut completed = Ranks::new(self.ranks.len());
        while !completed.is_full() {
            let result = completed_receiver.recv().await?;
            match result {
                GspawnResult::Success { rank, .. } if rank >= self.ranks.len() => {
                    tracing::error!("ignoring invalid rank {}", rank);
                }
                GspawnResult::Success { rank, actor_id } => {
                    if completed.insert(rank, actor_id.clone()).is_some() {
                        tracing::error!("multiple completions received for rank {}", rank);
                    }

                    let expected_actor_id = self.ranks.get(rank).unwrap().actor_id(&name);
                    if actor_id != expected_actor_id {
                        return Err(Error::GspawnError(
                            name,
                            format!(
                                "expected actor id {} for rank {}; got {}",
                                expected_actor_id, rank, actor_id
                            ),
                        ));
                    }
                }
                GspawnResult::Error(error_msg) => return Err(Error::GspawnError(name, error_msg)),
            }
        }

        Ok(ActorMesh::new(self.clone(), name))
    }
}

impl view::Ranked for ProcMeshRef {
    type Item = ProcRef;

    fn region(&self) -> &Region {
        &self.region
    }

    fn get(&self, rank: usize) -> Option<ProcRef> {
        self.ranks.get(rank).cloned()
    }

    fn sliced(&self, region: Region, nodes: impl Iterator<Item = ProcRef>) -> Self {
        Self::new(self.name.clone(), region, Arc::new(nodes.collect())).unwrap()
    }
}

impl view::RankedRef for ProcMeshRef {
    fn get_ref(&self, rank: usize) -> Option<&Self::Item> {
        self.ranks.get(rank)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::mailbox;
    use ndslice::ViewExt;
    use ndslice::extent;
    use timed_test::async_timed_test;

    use crate::v1::ActorMeshRef;
    use crate::v1::testactor;
    use crate::v1::testing;

    #[tokio::test]
    async fn test_proc_mesh_allocate() {
        let (mesh, actor, router) = testing::local_proc_mesh(extent!(replica = 4)).await;
        let mesh_ref = mesh.freeze();
        assert_eq!(mesh_ref.extent(), extent!(replica = 4));
        assert_eq!(mesh_ref.ranks.len(), 4);
        assert!(!router.prefixes().is_empty());

        // All of the agents are alive, and reachable (both ways).
        for proc_ref in mesh_ref.values() {
            assert!(proc_ref.status(&actor).await.unwrap());
        }

        // Same on the proc mesh:
        assert!(
            mesh_ref
                .status(&actor)
                .await
                .unwrap()
                .values()
                .all(|status| status)
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance();

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            let actor_mesh: ActorMeshRef<testactor::TestActor> = proc_mesh
                .freeze()
                .spawn(&instance, "test", &())
                .await
                .unwrap()
                .freeze();

            // Verify casting to the root actor mesh
            {
                let (port, mut rx) = mailbox::open_port(&instance);
                actor_mesh
                    .cast(&instance, testactor::GetActorId(port.bind()))
                    .unwrap();

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

            // Verify casting to the sliced actor mesh
            let sliced_actor_mesh = actor_mesh.range("replicas", 1..3).unwrap();
            {
                let (port, mut rx) = mailbox::open_port(&instance);
                sliced_actor_mesh
                    .cast(&instance, testactor::GetActorId(port.bind()))
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
    }
}
