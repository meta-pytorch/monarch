/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Mailbox;
use hyperactor::RemoteMessage;
use hyperactor::WorldId;
use hyperactor::actor::RemoteActor;
use hyperactor::actor::remote::Remote;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::mailbox;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxRouter;
use hyperactor::mailbox::MailboxServer;
use hyperactor::proc::Proc;
use hyperactor::reference::ProcId;
use hyperactor::reference::Reference;
use ndslice::Range;
use ndslice::Shape;
use ndslice::ShapeError;

use crate::Mesh;
use crate::actor_mesh::ActorMesh;
use crate::alloc::Alloc;
use crate::alloc::AllocatorError;
use crate::alloc::ProcState;
use crate::alloc::ProcStopReason;
use crate::assign::Ranks;
use crate::proc_mesh::mesh_agent::MeshAgent;
use crate::proc_mesh::mesh_agent::MeshAgentMessageClient;

pub mod mesh_agent;

use std::sync::OnceLock;

/// A global router shared by all meshes managed in this process;
/// this allows different meshes to communicate with each other.
///
/// This is definitely a "good enough for now" solution; in the future,
/// we'll likely have some form of truly global registration for meshes,
/// also benefitting tooling, etc.
fn global_router() -> &'static MailboxRouter {
    static GLOBAL_ROUTER: OnceLock<MailboxRouter> = OnceLock::new();
    GLOBAL_ROUTER.get_or_init(MailboxRouter::new)
}

/// A ProcMesh maintains a mesh of procs whose lifecycles are managed by
/// an allocator.
pub struct ProcMesh {
    // The underlying alloc. It is None if it has been transferred to
    // a proc event observer.
    alloc: Option<Box<dyn Alloc + Send + Sync>>,
    shape: Shape,
    ranks: Vec<(ProcId, (ChannelAddr, ActorRef<MeshAgent>))>,
    #[allow(dead_code)] // will be used in subsequent diff
    client_proc: Proc,
    client: Mailbox,
}

impl ProcMesh {
    /// Allocate a new ProcMesh from the provided allocator. Allocate returns
    /// after the mesh has been successfully (and fully) allocated, returning
    /// early on any allocation failure.
    pub async fn allocate(
        mut alloc: impl Alloc + Send + Sync + 'static,
    ) -> Result<Self, AllocatorError> {
        // We wait for the full allocation to be running before returning the mesh.
        let shape = alloc.shape().clone();

        let mut proc_ids = Ranks::new(shape.slice().len());
        let mut running = Ranks::new(shape.slice().len());

        while !running.is_full() {
            let Some(state) = alloc.next().await else {
                // Alloc finished before it was fully allocated.
                return Err(AllocatorError::Incomplete(shape));
            };

            match state {
                ProcState::Created { proc_id, coords } => {
                    let rank = shape
                        .slice()
                        .location(&coords)
                        .map_err(|err| AllocatorError::Other(err.into()))?;
                    if let Some(old_proc_id) = proc_ids.insert(rank, proc_id.clone()) {
                        tracing::warn!("rank {rank} reassigned from {old_proc_id} to {proc_id}");
                    }
                    tracing::info!("proc {} rank {}: created", proc_id, rank);
                }
                ProcState::Running {
                    proc_id,
                    mesh_agent,
                    addr,
                } => {
                    let Some(rank) = proc_ids.rank(&proc_id) else {
                        tracing::warn!("proc id {proc_id} running, but not created");
                        continue;
                    };

                    if let Some((old_addr, old_mesh_agent)) =
                        running.insert(*rank, (addr.clone(), mesh_agent.clone()))
                    {
                        tracing::warn!(
                            "duplicate running notifications for {proc_id}, addr:{addr}, mesh_agent:{mesh_agent}, old addr:{old_addr}, old mesh_agent:{old_mesh_agent}"
                        )
                    }
                    tracing::info!(
                        "proc {} rank {}: running at addr:{addr} mesh_agent:{mesh_agent}",
                        proc_id,
                        rank
                    );
                }
                ProcState::Stopped { proc_id, reason } => {
                    if let Some(rank) = proc_ids.unassign(proc_id.clone()) {
                        let _ = running.remove(rank);
                        tracing::info!("proc {} rank {}: stopped: {}", proc_id, rank, reason);
                    }
                }
            }
        }

        // We collect all the ranks at this point of completion, so that we can
        // avoid holding Rcs across awaits.
        let running: Vec<_> = running.into_iter().map(Option::unwrap).collect();

        // All procs are running, so we now configure them.
        let mut world_ids = HashSet::new();

        let (router_channel_addr, router_rx) = channel::serve(ChannelAddr::any(alloc.transport()))
            .await
            .map_err(|err| AllocatorError::Other(err.into()))?;
        let router = DialMailboxRouter::new_with_default(
            router_channel_addr.clone(),
            global_router().boxed(),
        );
        for (rank, (addr, _agent)) in running.iter().enumerate() {
            let proc_id = proc_ids.get(rank).unwrap().clone();
            router.bind(Reference::Proc(proc_id.clone()), addr.clone());
            // Work around for Allocs that have more than one world.
            world_ids.insert(proc_id.world_id().clone());
        }
        router
            .clone()
            .serve(router_rx, mailbox::monitored_return_handle());

        // Set up a client proc for the mesh itself, so that we can attach ourselves
        // to it, and communicate with the agents. We wire it into the same router as
        // everything else, so now the whole mesh should be able to communicate.
        let client_proc_id = ProcId(WorldId(format!("{}_manager", alloc.world_id().name())), 0);
        let (client_proc_addr, client_rx) = channel::serve(ChannelAddr::any(alloc.transport()))
            .await
            .map_err(|err| AllocatorError::Other(err.into()))?;
        let client_proc = Proc::new(
            client_proc_id.clone(),
            BoxedMailboxSender::new(router.clone()),
        );
        client_proc
            .clone()
            .serve(client_rx, mailbox::monitored_return_handle());
        router.bind(client_proc_id.clone().into(), client_proc_addr);

        // Bind this router to the global router, to enable cross-mesh routing.
        // TODO: unbind this when we incorporate mesh destruction too.
        for world_id in world_ids {
            global_router().bind(world_id.clone().into(), router.clone());
        }
        global_router().bind(alloc.world_id().clone().into(), router.clone());
        global_router().bind(client_proc_id.into(), router.clone());

        // Now, configure the full mesh, so that the local agents are wired up to
        // our router.
        let client = client_proc.attach("client")?;

        let (config_handle, mut config_receiver) = client.open_port();
        for (rank, (_, agent)) in running.iter().enumerate() {
            agent
                .configure(
                    &client,
                    rank,
                    router_channel_addr.clone(),
                    config_handle.bind(),
                )
                .await?;
        }
        let mut completed = Ranks::new(shape.slice().len());
        while !completed.is_full() {
            let rank = config_receiver
                .recv()
                .await
                .map_err(|err| AllocatorError::Other(err.into()))?;
            if completed.insert(rank, rank).is_some() {
                tracing::warn!("multiple completions received for rank {}", rank);
            }
        }

        let shape = alloc.shape().clone();

        Ok(Self {
            alloc: Some(Box::new(alloc)),
            shape,
            ranks: proc_ids
                .into_iter()
                .map(Option::unwrap)
                .zip(running.into_iter())
                .collect(),
            client_proc,
            client,
        })
    }

    pub async fn spawn<A: Actor + RemoteActor>(
        &self,
        actor_name: &str,
        params: &A::Params,
    ) -> Result<ActorMesh<'_, A>, anyhow::Error>
    where
        A::Params: RemoteMessage,
    {
        Ok(ActorMesh::new(
            self,
            self.spawn_to_ranks(actor_name, params).await?,
        ))
    }

    async fn spawn_to_ranks<A: Actor + RemoteActor>(
        &self,
        actor_name: &str,
        params: &A::Params,
    ) -> Result<Vec<ActorRef<A>>, anyhow::Error>
    where
        A::Params: RemoteMessage,
    {
        let remote = Remote::collect();
        let actor_type = remote
            .name_of::<A>()
            .ok_or(anyhow::anyhow!("actor not registered"))?
            .to_string();

        let (completed_handle, mut completed_receiver) = self.client.open_port();
        for (_proc_id, (_addr, agent)) in self.ranks.iter() {
            agent
                .gspawn(
                    &self.client,
                    actor_type.clone(),
                    actor_name.to_string(),
                    bincode::serialize(params)?,
                    completed_handle.bind(),
                )
                .await?;
        }
        let mut completed = Ranks::new(self.ranks.len());
        while !completed.is_full() {
            let (rank, actor_id) = completed_receiver.recv().await?;
            if completed.insert(rank, actor_id).is_some() {
                tracing::warn!("multiple completions received for rank {}", rank);
            }
        }

        // `Ranks` really should have some way to convert into a "completed" rank
        // in a one-shot way; the API here is too awkward otherwise.
        Ok(completed
            .into_iter()
            .map(Option::unwrap)
            .map(ActorRef::attest)
            .collect())
    }

    /// A client used to communicate with any member of this mesh.
    pub fn client(&self) -> &Mailbox {
        &self.client
    }

    /// An event stream of proc events. Each ProcMesh can produce only one such
    /// stream, returning None after the first call.
    pub fn events(&mut self) -> Option<ProcEvents> {
        self.alloc.take().map(|alloc| ProcEvents {
            alloc,
            ranks: self
                .ranks
                .iter()
                .enumerate()
                .map(|(rank, (proc_id, _))| (proc_id.clone(), rank))
                .collect(),
        })
    }
}

/// Proc lifecycle events.
#[derive(Debug)]
pub enum ProcEvent {
    /// The proc of the given rank was stopped with the provided reason.
    Stopped(usize, ProcStopReason),
}

/// An event stream of [`ProcEvent`]
// TODO: consider using streams for this.
pub struct ProcEvents {
    alloc: Box<dyn Alloc + Send + Sync>,
    ranks: HashMap<ProcId, usize>,
}

impl ProcEvents {
    /// Get the next lifecycle event. The stream is closed when this method
    /// returns `None`.
    pub async fn next(&mut self) -> Option<ProcEvent> {
        loop {
            let Some(alloc_event) = self.alloc.next().await else {
                break None;
            };

            let ProcState::Stopped { proc_id, reason } = alloc_event else {
                // Ignore non-stopped events for now.
                continue;
            };

            let Some(rank) = self.ranks.get(&proc_id) else {
                tracing::warn!("received stop event for unmapped proc {}", proc_id);
                continue;
            };

            break Some(ProcEvent::Stopped(*rank, reason));
        }
    }
}

/// Spawns from shared ([`Arc`]) proc meshes, providing [`ActorMesh`]es with
/// static lifetimes.
#[async_trait]
pub trait SharedSpawnable {
    async fn spawn<A: Actor + RemoteActor>(
        &self,
        actor_name: &str,
        params: &A::Params,
    ) -> Result<ActorMesh<'static, A>, anyhow::Error>
    where
        A::Params: RemoteMessage;
}

#[async_trait]
impl SharedSpawnable for Arc<ProcMesh> {
    async fn spawn<A: Actor + RemoteActor>(
        &self,
        actor_name: &str,
        params: &A::Params,
    ) -> Result<ActorMesh<'static, A>, anyhow::Error>
    where
        A::Params: RemoteMessage,
    {
        Ok(ActorMesh::new_shared(
            Arc::clone(self),
            self.spawn_to_ranks(actor_name, params).await?,
        ))
    }
}

#[async_trait]
impl Mesh for ProcMesh {
    type Node = ProcId;
    type Sliced<'a> = SlicedProcMesh<'a>;

    fn shape(&self) -> &Shape {
        &self.shape
    }

    fn select<R: Into<Range>>(
        &self,
        label: &str,
        range: R,
    ) -> Result<Self::Sliced<'_>, ShapeError> {
        Ok(SlicedProcMesh(self, self.shape().select(label, range)?))
    }

    fn get(&self, rank: usize) -> Option<ProcId> {
        Some(self.ranks[rank].0.clone())
    }
}

impl fmt::Display for ProcMesh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ shape: {} }}", self.shape())
    }
}

impl fmt::Debug for ProcMesh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcMesh")
            .field("shape", &self.shape())
            .field("ranks", &self.ranks)
            .field("client_proc", &self.client_proc)
            .field("client", &self.client)
            // Skip the alloc field since it doesn't implement Debug
            .finish()
    }
}

pub struct SlicedProcMesh<'a>(&'a ProcMesh, Shape);

#[async_trait]
impl Mesh for SlicedProcMesh<'_> {
    type Node = ProcId;
    type Sliced<'b>
        = SlicedProcMesh<'b>
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

    fn get(&self, _index: usize) -> Option<ProcId> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use ndslice::shape;

    use super::*;
    use crate::alloc::AllocSpec;
    use crate::alloc::Allocator;
    use crate::alloc::local::LocalAllocator;

    #[tokio::test]
    async fn test_basic() {
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                shape: shape! { replica = 4 },
                constraints: Default::default(),
            })
            .await
            .unwrap();

        let name = alloc.name().to_string();
        let mesh = ProcMesh::allocate(alloc).await.unwrap();

        assert_eq!(mesh.get(0).unwrap().world_name(), &name);
    }

    #[tokio::test]
    async fn test_propagate_lifecycle_events() {
        let alloc = LocalAllocator
            .allocate(AllocSpec {
                shape: shape! { replica = 4 },
                constraints: Default::default(),
            })
            .await
            .unwrap();

        let monkey = alloc.chaos_monkey();
        let mut mesh = ProcMesh::allocate(alloc).await.unwrap();
        let mut events = mesh.events().unwrap();

        monkey(1, ProcStopReason::Killed(1, false));
        assert_matches!(
            events.next().await.unwrap(),
            ProcEvent::Stopped(1, ProcStopReason::Killed(1, false))
        );

        // todo: allow meshes to be stopped

        // assert_matches!(
        //     events.next().await.unwrap(),
        //     ProcEvent::Stopped(0, ProcStopReason::Stopped)
        // );

        // assert!(events.next().await.is_none());
    }
}
