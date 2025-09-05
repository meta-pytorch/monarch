/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::Arc;

use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
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
use crate::proc_mesh::mesh_agent::MeshAgentMessageClient;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::v1;
use crate::v1::Error;
use crate::v1::Name;

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

    /// Allocate a new ProcMeshRef from the provided alloc.
    /// Allocate does not require an owning actor because references are not owned.
    async fn allocate(
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
}

impl view::Ranked for ProcMeshRef {
    type Item = ProcRef;

    fn region(&self) -> &Region {
        &self.region
    }

    fn ranks(&self) -> &[ProcRef] {
        &self.ranks
    }

    fn sliced<'a>(&self, region: Region, nodes: impl Iterator<Item = &'a ProcRef>) -> Self {
        Self::new(self.name.clone(), region, nodes.cloned().collect()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use hyperactor::Proc;
    use hyperactor::id;
    use hyperactor::mailbox::BoxableMailboxSender;
    use ndslice::ViewExt;
    use ndslice::extent;

    use super::*;
    use crate::alloc::AllocSpec;
    use crate::alloc::Allocator;
    use crate::alloc::LocalAllocator;

    #[tokio::test]
    async fn test_proc_mesh_allocate() {
        let router = DialMailboxRouter::new();
        let proc = Proc::new(id!(test[0]), router.boxed());
        let (actor, _handle) = proc.instance("controller").unwrap();

        let alloc = LocalAllocator
            .allocate(AllocSpec {
                extent: extent!(replica = 4),
                constraints: Default::default(),
                proc_name: None,
            })
            .await
            .unwrap();

        let mesh = ProcMeshRef::allocate(&actor, alloc, "test").await.unwrap();
        assert_eq!(mesh.extent(), extent!(replica = 4));
        assert_eq!(mesh.ranks.len(), 4);
        assert!(!router.prefixes().is_empty());

        // All of the agents are alive, and reachable (both ways).
        for proc_ref in mesh.values() {
            assert!(proc_ref.status(&actor).await.unwrap());
        }
    }
}
