/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::type_name;
use std::collections::HashMap;
use std::sync::Arc;

use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::RemoteMessage;
use hyperactor::actor::RemoteActor;
use hyperactor::actor::remote::Remote;
use hyperactor::cap;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::mailbox;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxServer;
use ndslice::view;
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
    async fn status(&self, caps: &(impl cap::CanSend + cap::CanOpenPort)) -> v1::Result<bool> {
        let (port, mut rx) = mailbox::open_port(caps);
        self.agent
            .status(caps, port.bind())
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
    fn new(name: Name, region: Region, ranks: Vec<ProcRef>) -> v1::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(v1::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        Ok(Self {
            name,
            region,
            ranks: Arc::new(ranks),
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
    async fn status(
        &self,
        caps: &(impl cap::CanSend + cap::CanOpenPort),
    ) -> v1::Result<ValueMesh<bool>> {
        self.mapped(|proc_ref| {
            let proc_ref = proc_ref.clone();
            async move { proc_ref.status(caps).await }
        })
        .join()
        .await
        .transpose()
    }

    /// Allocate a new ProcMeshRef from the provided alloc.
    /// Allocate does not require an owning actor because references are not owned.
    pub async fn allocate(
        caps: &(impl cap::CanOpenPort + cap::CanSend + cap::HasProc),
        mut alloc: impl Alloc + Send + Sync + 'static,
        name: &str,
    ) -> v1::Result<Self> {
        let running = alloc.initialize().await?;

        // Wire the newly created mesh into the proc, so that it is routable.
        // We route all of the relevant prefixes into the proc's forwarder,
        // and serve it on the alloc's transport.
        //
        // This will be removed with direct addressing.
        let proc = caps.proc();
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

        let (config_handle, mut config_receiver) = mailbox::open_port(caps);
        for (rank, AllocatedProc { mesh_agent, .. }) in running.iter().enumerate() {
            mesh_agent
                .configure(
                    caps,
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
            region: alloc.extent().clone().into(),
            ranks: Arc::new(ranks),
        })
    }

    /// Spawn an actor on all of the procs in this mesh, returning a new ActorMesh.
    async fn spawn<A: Actor + RemoteActor>(
        &self,
        caps: &(impl cap::CanSend + cap::CanOpenPort),
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

        let (completed_handle, mut completed_receiver) = mailbox::open_port(caps);
        for proc_ref in self.ranks.iter() {
            proc_ref
                .agent
                .gspawn(
                    caps,
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
        Self::new(self.name.clone(), region, nodes.collect()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::Instance;
    use hyperactor::PortRef;
    use hyperactor::Proc;
    use hyperactor::id;
    use hyperactor::mailbox::BoxableMailboxSender;
    use ndslice::Extent;
    use ndslice::ViewExt;
    use ndslice::extent;

    use super::*;
    use crate::alloc::AllocSpec;
    use crate::alloc::Allocator;
    use crate::alloc::LocalAllocator;
    use crate::v1::ActorMeshRef;

    async fn local_proc_mesh(extent: Extent) -> (ProcMeshRef, Instance<()>, DialMailboxRouter) {
        let router = DialMailboxRouter::new();
        let proc = Proc::new(id!(test[0]), router.boxed());
        let (actor, _handle) = proc.instance("controller").unwrap();

        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent,
                constraints: Default::default(),
                proc_name: None,
            })
            .await
            .unwrap();
        (
            ProcMeshRef::allocate(&actor, alloc, "test").await.unwrap(),
            actor,
            router,
        )
    }

    #[tokio::test]
    async fn test_proc_mesh_allocate() {
        let (mesh, actor, router) = local_proc_mesh(extent!(replica = 4)).await;
        assert_eq!(mesh.extent(), extent!(replica = 4));
        assert_eq!(mesh.ranks.len(), 4);
        assert!(!router.prefixes().is_empty());

        // All of the agents are alive, and reachable (both ways).
        for proc_ref in mesh.values() {
            assert!(proc_ref.status(&actor).await.unwrap());
        }

        // Same on the proc mesh:
        assert!(
            mesh.status(&actor)
                .await
                .unwrap()
                .values()
                .all(|status| status)
        );
    }

    #[tokio::test]
    async fn test_spawn_actor() {
        #[derive(Actor, Default, Debug)]
        #[hyperactor::export(
            spawn = true,
            handlers = [
                PortRef<ActorId>,
            ]
        )]
        struct EchoActor;

        #[async_trait]
        impl Handler<PortRef<ActorId>> for EchoActor {
            async fn handle(
                &mut self,
                cx: &Context<Self>,
                reply: PortRef<ActorId>,
            ) -> Result<(), anyhow::Error> {
                reply.send(cx, cx.self_id().clone())?;
                Ok(())
            }
        }

        let (proc_mesh, actor, _router) = local_proc_mesh(extent!(replica = 4)).await;
        let actor_mesh: ActorMeshRef<EchoActor> =
            proc_mesh.spawn(&actor, "test", &()).await.unwrap().freeze();

        let (port, mut rx) = mailbox::open_port(&actor);
        actor_mesh.cast(&actor, port.bind()).unwrap();

        let mut expected_actor_ids: HashSet<_> = actor_mesh
            .values()
            .map(|actor_ref| actor_ref.actor_id().clone())
            .collect();

        while !expected_actor_ids.is_empty() {
            let actor_id = rx.recv().await.unwrap();
            assert!(expected_actor_ids.remove(&actor_id));
        }
    }
}
