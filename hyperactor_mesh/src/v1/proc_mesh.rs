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
use std::ops::Deref;
use std::sync::Arc;

use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::RemoteMessage;
use hyperactor::actor::Referable;
use hyperactor::actor::remote::Remote;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::context;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxServer;
use ndslice::Extent;
use ndslice::ViewExt as _;
use ndslice::view;
use ndslice::view::CollectMeshExt;
use ndslice::view::MapIntoExt;
use ndslice::view::Ranked;
use ndslice::view::Region;
use serde::Deserialize;
use serde::Serialize;

use crate::CommActor;
use crate::alloc::Alloc;
use crate::alloc::AllocExt;
use crate::alloc::AllocatedProc;
use crate::assign::Ranks;
use crate::comm::CommActorMode;
use crate::proc_mesh::mesh_agent;
use crate::proc_mesh::mesh_agent::ActorState;
use crate::proc_mesh::mesh_agent::MeshAgentMessage;
use crate::proc_mesh::mesh_agent::MeshAgentMessageClient;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::proc_mesh::mesh_agent::ReconfigurableMailboxSender;
use crate::resource;
use crate::resource::RankedValues;
use crate::v1;
use crate::v1::ActorMesh;
use crate::v1::ActorMeshRef;
use crate::v1::Error;
use crate::v1::HostMeshRef;
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
    pub(crate) fn new(proc_id: ProcId, create_rank: usize, agent: ActorRef<ProcMeshAgent>) -> Self {
        Self {
            proc_id,
            create_rank,
            agent,
        }
    }

    /// Pings the proc, returning whether it is alive. This will be replaced by a
    /// finer-grained lifecycle status in the near future.
    pub(crate) async fn status(&self, cx: &impl context::Actor) -> v1::Result<bool> {
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

    /// Get the supervision events for one actor with the given name.
    #[allow(dead_code)]
    async fn actor_state(
        &self,
        cx: &impl context::Actor,
        name: Name,
    ) -> v1::Result<resource::State<ActorState>> {
        let (port, mut rx) = cx.mailbox().open_port::<resource::State<ActorState>>();
        self.agent
            .send(
                cx,
                resource::GetState::<ActorState> {
                    name: name.clone(),
                    reply: port.bind(),
                },
            )
            .map_err(|e| Error::CallError(self.agent.actor_id().clone(), e.into()))?;
        let state = rx
            .recv()
            .await
            .map_err(|e| Error::CallError(self.agent.actor_id().clone(), e.into()))?;
        if let Some(ref inner) = state.state {
            let rank = inner.create_rank;
            if rank == self.create_rank {
                Ok(state)
            } else {
                Err(Error::CallError(
                    self.agent.actor_id().clone(),
                    anyhow::anyhow!(
                        "Rank on mesh agent not matching for Actor {}: returned {}, expected {}",
                        name,
                        rank,
                        self.create_rank
                    ),
                ))
            }
        } else {
            Err(Error::CallError(
                self.agent.actor_id().clone(),
                anyhow::anyhow!("Actor {} does not exist", name),
            ))
        }
    }

    pub(crate) fn actor_id(&self, name: &Name) -> ActorId {
        self.proc_id.actor_id(name.to_string(), 0)
    }

    /// Generic bound: `A: Referable` - required because we return
    /// an `ActorRef<A>`.
    pub(crate) fn attest<A: Referable>(&self, name: &Name) -> ActorRef<A> {
        ActorRef::attest(self.actor_id(name))
    }
}

/// A mesh of processes.
#[allow(dead_code)]
#[derive(Debug)]
pub struct ProcMesh {
    name: Name,
    allocation: ProcMeshAllocation,
    comm_actor_name: Option<Name>,
    current_ref: ProcMeshRef,
}

impl ProcMesh {
    async fn create(
        cx: &impl context::Actor,
        name: Name,
        allocation: ProcMeshAllocation,
        spawn_comm_actor: bool,
    ) -> v1::Result<Self> {
        let comm_actor_name = if spawn_comm_actor {
            Some(Name::new("comm"))
        } else {
            None
        };

        let region = allocation.extent().clone().into();
        let ranks = allocation.ranks();
        let root_comm_actor = comm_actor_name.as_ref().map(|name| {
            ActorRef::attest(
                ranks
                    .first()
                    .expect("root mesh cannot be empty")
                    .actor_id(name),
            )
        });
        let current_ref = ProcMeshRef::new(
            name.clone(),
            region,
            ranks,
            None, // this is the root mesh
            None, // comm actor is not alive yet
        )
        .unwrap();

        let mut proc_mesh = Self {
            name,
            allocation,
            comm_actor_name: comm_actor_name.clone(),
            current_ref,
        };

        if let Some(comm_actor_name) = comm_actor_name {
            // CommActor satisfies `Actor + Referable`, so it can be
            // spawned and safely referenced via ActorRef<CommActor>.
            let comm_actor_mesh = proc_mesh
                .spawn_with_name::<CommActor>(cx, comm_actor_name, &Default::default())
                .await?;
            let address_book: HashMap<_, _> = comm_actor_mesh
                .iter()
                .map(|(point, actor_ref)| (point.rank(), actor_ref))
                .collect();
            // Now that we have all of the spawned comm actors, kick them all into
            // mesh mode.
            for (rank, comm_actor) in &address_book {
                comm_actor
                    .send(cx, CommActorMode::Mesh(*rank, address_book.clone()))
                    .map_err(|e| Error::SendingError(comm_actor.actor_id().clone(), Box::new(e)))?
            }

            // The comm actor is now set up and ready to go.
            proc_mesh.current_ref.root_comm_actor = root_comm_actor;
        }

        Ok(proc_mesh)
    }

    pub(crate) async fn create_owned_unchecked(
        cx: &impl context::Actor,
        name: Name,
        extent: Extent,
        hosts: HostMeshRef,
        ranks: Vec<ProcRef>,
    ) -> v1::Result<Self> {
        Self::create(
            cx,
            name,
            ProcMeshAllocation::Owned {
                hosts,
                extent,
                ranks: Arc::new(ranks),
            },
            true,
        )
        .await
    }

    /// Allocate a new ProcMesh from the provided alloc.
    /// Allocate does not require an owning actor because references are not owned.
    /// Allocate a new ProcMesh from the provided alloc.
    pub async fn allocate(
        cx: &impl context::Actor,
        mut alloc: Box<dyn Alloc + Send + Sync + 'static>,
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
        let (proc_channel_addr, rx) = channel::serve(ChannelAddr::any(alloc.transport()))?;
        proc.clone().serve(rx);

        let bind_allocated_procs = |router: &DialMailboxRouter| {
            // Route all of the allocated procs:
            for AllocatedProc { proc_id, addr, .. } in running.iter() {
                if proc_id.is_direct() {
                    continue;
                }
                router.bind(proc_id.clone().into(), addr.clone());
            }
        };

        // Temporary for backward compatibility with ranked procs and v0 API.
        // Proc meshes can be allocated either using the root client proc (which
        // has a DialMailboxRouter forwarder) or a mesh agent proc (which has a
        // ReconfigurableMailboxSender forwarder with an inner DialMailboxRouter).
        if let Some(router) = proc.forwarder().downcast_ref() {
            bind_allocated_procs(router);
        } else if let Some(router) = proc
            .forwarder()
            .downcast_ref::<ReconfigurableMailboxSender>()
        {
            bind_allocated_procs(
                router
                    .as_inner()
                    .map_err(|_| Error::UnroutableMesh())?
                    .as_configured()
                    .ok_or(Error::UnroutableMesh())?
                    .downcast_ref()
                    .ok_or(Error::UnroutableMesh())?,
            );
        } else {
            return Err(Error::UnroutableMesh());
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
            let message = MeshAgentMessage::Configure {
                rank,
                forwarder: proc_channel_addr.clone(),
                supervisor: None, // no supervisor; we just crash
                address_book: address_book.clone(),
                configured: config_handle.bind(),
                record_supervision_events: true,
            };
            mesh_agent
                .send(cx, message)
                .map_err(|e| Error::ConfigurationError(e.into()))?;
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

        let ranks: Vec<_> = running
            .into_iter()
            .enumerate()
            .map(|(create_rank, allocated)| ProcRef {
                proc_id: allocated.proc_id,
                create_rank,
                agent: allocated.mesh_agent,
            })
            .collect();

        Self::create(
            cx,
            Name::new(name),
            ProcMeshAllocation::Allocated {
                alloc,
                ranks: Arc::new(ranks),
            },
            true, // alloc-based meshes support comm actors
        )
        .await
    }
}

impl Deref for ProcMesh {
    type Target = ProcMeshRef;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
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

    /// An owned allocation: this ProcMesh fully owns the set of ranks.
    Owned {
        /// The host mesh from which the proc mesh was spawned.
        hosts: HostMeshRef,
        // This is purely for storage: `hosts.extent()` returns a computed (by value)
        // extent.
        extent: Extent,
        /// A proc reference for each rank in the mesh.
        ranks: Arc<Vec<ProcRef>>,
    },
}

impl ProcMeshAllocation {
    fn extent(&self) -> &Extent {
        match self {
            ProcMeshAllocation::Allocated { alloc, .. } => alloc.extent(),
            ProcMeshAllocation::Owned { extent, .. } => extent,
        }
    }

    fn ranks(&self) -> Arc<Vec<ProcRef>> {
        Arc::clone(match self {
            ProcMeshAllocation::Allocated { ranks, .. } => ranks,
            ProcMeshAllocation::Owned { ranks, .. } => ranks,
        })
    }
}

impl fmt::Debug for ProcMeshAllocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcMeshAllocation::Allocated { ranks, .. } => f
                .debug_struct("ProcMeshAllocation::Allocated")
                .field("alloc", &"<dyn Alloc>")
                .field("ranks", ranks)
                .finish(),
            ProcMeshAllocation::Owned {
                hosts,
                ranks,
                extent: _,
            } => f
                .debug_struct("ProcMeshAllocation::Owned")
                .field("hosts", hosts)
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
    // Temporary: used to fit v1 ActorMesh with v0's casting implementation. This
    // should be removed after we remove the v0 code.
    // The root region of this mesh. None means this mesh itself is the root.
    pub(crate) root_region: Option<Region>,
    // Temporary: used to fit v1 ActorMesh with v0's casting implementation. This
    // should be removed after we remove the v0 code.
    // v0 casting requires root mesh rank 0 as the 1st hop, so we need to provide
    // it here. For v1, this can be removed since v1 can use any rank.
    pub(crate) root_comm_actor: Option<ActorRef<CommActor>>,
}

impl ProcMeshRef {
    /// Create a new ProcMeshRef from the given name, region, ranks, and so on.
    fn new(
        name: Name,
        region: Region,
        ranks: Arc<Vec<ProcRef>>,
        root_region: Option<Region>,
        root_comm_actor: Option<ActorRef<CommActor>>,
    ) -> v1::Result<Self> {
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
            root_region,
            root_comm_actor,
        })
    }

    pub(crate) fn root_comm_actor(&self) -> Option<&ActorRef<CommActor>> {
        self.root_comm_actor.as_ref()
    }

    /// The current statuses of procs in this mesh.
    pub async fn status(&self, cx: &impl context::Actor) -> v1::Result<ValueMesh<bool>> {
        let vm: ValueMesh<_> = self.map_into(|proc_ref| {
            let proc_ref = proc_ref.clone();
            async move { proc_ref.status(cx).await }
        });
        vm.join().await.transpose()
    }

    fn agent_mesh(&self) -> ActorMeshRef<ProcMeshAgent> {
        let agent_name = self.ranks.first().unwrap().agent.actor_id().name();
        // This name must match the ProcMeshAgent name, which can change depending on the allocator.
        ActorMeshRef::new(Name::new_reserved(agent_name), self.clone())
    }

    /// The supervision events of procs in this mesh.
    pub async fn actor_states(
        &self,
        cx: &impl context::Actor,
        name: Name,
    ) -> v1::Result<ValueMesh<resource::State<ActorState>>> {
        let agent_mesh = self.agent_mesh();
        let (port, mut rx) = cx.mailbox().open_port::<resource::State<ActorState>>();
        // TODO: Use accumulation to get back a single value (representing whether
        // *any* of the actors failed) instead of a mesh.
        agent_mesh.cast(
            cx,
            resource::GetState::<ActorState> {
                name: name.clone(),
                reply: port.bind(),
            },
        )?;
        let expected = self.ranks.len();
        let mut states = Vec::with_capacity(expected);
        for _ in 0..expected {
            let state = rx.recv().await?;
            match state.state {
                Some(ref inner) => {
                    states.push((inner.create_rank, state));
                }
                None => {
                    return Err(Error::NotExist(state.name));
                }
            }
        }
        // Sort by rank, so that the resulting mesh is ordered.
        states.sort_by_key(|(rank, _)| *rank);
        let vm = states
            .into_iter()
            .map(|(_, state)| state)
            .collect_mesh::<ValueMesh<_>>(self.region.clone())?;
        Ok(vm)
    }

    /// Spawn an actor on all of the procs in this mesh, returning a
    /// new ActorMesh.
    ///
    /// Bounds:
    /// - `A: Actor` - the actor actually runs inside each proc.
    /// - `A: Referable` - so we can return typed `ActorRef<A>`s
    ///   inside the `ActorMesh`.
    /// - `A::Params: RemoteMessage` - spawn parameters must be
    ///   serializable and routable.
    pub async fn spawn<A: Actor + Referable>(
        &self,
        cx: &impl context::Actor,
        name: &str,
        params: &A::Params,
    ) -> v1::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
    {
        self.spawn_with_name(cx, Name::new(name), params).await
    }

    /// Spawn an actor on all procs in this mesh under the given
    /// [`Name`], returning a new `ActorMesh`.
    ///
    /// This is the underlying implementation used by [`spawn`]; it
    /// differs only in that the actor name is passed explicitly
    /// rather than as a `&str`.
    ///
    /// Bounds:
    /// - `A: Actor` - the actor actually runs inside each proc.
    /// - `A: Referable` - so we can return typed `ActorRef<A>`s
    ///   inside the `ActorMesh`.
    /// - `A::Params: RemoteMessage` — spawn parameters must be
    ///   serializable and routable.
    pub(crate) async fn spawn_with_name<A: Actor + Referable>(
        &self,
        cx: &impl context::Actor,
        name: Name,
        params: &A::Params,
    ) -> v1::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
    {
        let remote = Remote::collect();
        // `Referable` ensures the type `A` is registered with
        // `Remote`.
        let actor_type = remote
            .name_of::<A>()
            .ok_or(Error::ActorTypeNotRegistered(type_name::<A>().to_string()))?
            .to_string();

        let serialized_params = bincode::serialize(params)?;

        self.agent_mesh().cast(
            cx,
            resource::CreateOrUpdate::<mesh_agent::ActorSpec> {
                name: name.clone(),
                rank: Default::default(),
                spec: mesh_agent::ActorSpec {
                    actor_type: actor_type.clone(),
                    params_data: serialized_params.clone(),
                },
            },
        )?;

        let (port, mut rx) = cx.mailbox().open_accum_port(RankedValues::default());

        self.agent_mesh().cast(
            cx,
            resource::GetRankStatus {
                name: name.clone(),
                reply: port.bind(),
            },
        )?;

        // Wait for everyone to report back.
        // TODO: move out of critical path
        let statuses = loop {
            let statuses = rx.recv().await?;
            if statuses.rank(self.ranks.len()) == self.ranks.len() {
                break statuses;
            }
        };

        let failed: Vec<_> = statuses
            .iter()
            .filter_map(|(ranks, status)| {
                if status.is_terminating() {
                    Some(ranks.clone())
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        if !failed.is_empty() {
            return Err(Error::GspawnError(
                name,
                format!("failed ranks: {:?}", failed,),
            ));
        }

        Ok(ActorMesh::new(self.clone(), name))
    }
}

impl view::Ranked for ProcMeshRef {
    type Item = ProcRef;

    fn region(&self) -> &Region {
        &self.region
    }

    fn get(&self, rank: usize) -> Option<&Self::Item> {
        self.ranks.get(rank)
    }
}

impl view::RankedSliceable for ProcMeshRef {
    fn sliced(&self, region: Region) -> Self {
        debug_assert!(region.is_subset(view::Ranked::region(self)));
        let ranks = self
            .region()
            .remap(&region)
            .unwrap()
            .map(|index| self.get(index).unwrap().clone())
            .collect();
        Self::new(
            self.name.clone(),
            region,
            Arc::new(ranks),
            Some(self.root_region.as_ref().unwrap_or(&self.region).clone()),
            self.root_comm_actor.clone(),
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::ops::Deref;

    use hyperactor::Proc;
    use hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER;
    use hyperactor::config::ENABLE_NATIVE_V1_CASTING;
    use hyperactor::context;
    use hyperactor::id;
    use hyperactor::mailbox::BoxableMailboxSender;
    use hyperactor::mailbox::DialMailboxRouter;
    use ndslice::ViewExt;
    use ndslice::extent;
    use timed_test::async_timed_test;
    use uuid::Uuid;

    use super::*;
    use crate::v1;
    use crate::v1::testactor;
    use crate::v1::testing;

    #[tokio::test]
    async fn test_proc_mesh_allocate() {
        let (mesh, actor, router) = testing::local_proc_mesh(extent!(replica = 4)).await;
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

    async fn execute_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance().await;

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            testactor::assert_mesh_shape(proc_mesh.spawn(instance, "test", &()).await.unwrap())
                .await;
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_actor_v1_casting() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        execute_spawn_actor().await;
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_spawn_actor_v0_casting() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, false);
        execute_spawn_actor().await;
    }

    // * Spawn an actor mesh, and then
    // * do a random numbers of cast to it to bump the seq numbers for all
    //   actors participating in the cast.
    //     * This is to avoid the test mistakenly passing.
    async fn spawm_for_seq_test(
        cx: &impl context::Actor,
        proc_mesh: &ProcMeshRef,
    ) -> ActorMesh<testactor::TestActor> {
        let actor_mesh = proc_mesh
            .spawn::<testactor::TestActor>(cx, "test", &())
            .await
            .unwrap();

        let (instance, _) = cx
            .instance()
            .proc()
            .instance(&format!("random_casts_{}", Uuid::now_v7()))
            .unwrap();
        let n = fastrand::u64(3..10);
        for _ in 0..n {
            actor_mesh.cast(&instance, ()).unwrap();
        }
        actor_mesh
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_seq_from_same_sender_to_different_meshes() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());
        let instance = testing::instance().await;
        let session_id = instance.sequencer().session_id();

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            let proc_mesh_ref = proc_mesh.deref();

            // Sequence numbers are scoped based on the (client, dest) pair.
            // So casts to different meshes from the same client instance would
            // result in seq 1 for all casts.
            let handles = (0..3)
                .map(|_| {
                    let proc_mesh_ref_clone = proc_mesh_ref.clone();
                    tokio::spawn(async move {
                        let actor_mesh = spawm_for_seq_test(instance, &proc_mesh_ref_clone).await;
                        let expected_seqs = vec![1; 8];
                        testactor::assert_casting_correctness(
                            &actor_mesh,
                            instance,
                            Some((session_id, expected_seqs)),
                        )
                        .await;
                    })
                })
                .collect::<Vec<_>>();
            futures::future::join_all(handles).await;
        }
    }

    // Verify that the seq numbers are assigned correctly when we cast to
    // different views of the same root mesh.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_seq_from_same_sender_to_difference_views() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance().await;
        let session_id = instance.sequencer().session_id();

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            let actor_mesh = spawm_for_seq_test(instance, &proc_mesh).await;

            // First cast. The seq should be 1 for all actors.
            let expected_seqs = vec![1; 8];
            testactor::assert_casting_correctness(
                &actor_mesh,
                instance,
                Some((session_id, expected_seqs)),
            )
            .await;

            // Verify casting to the sliced actor mesh
            let sliced_actor_mesh = actor_mesh.range("replicas", 1..3).unwrap();
            // Second cast. The seq should be 2 for actors in the sliced mesh.
            let expected_seqs = vec![2; 4];
            testactor::assert_casting_correctness(
                &sliced_actor_mesh,
                instance,
                Some((session_id, expected_seqs)),
            )
            .await;

            // Verify casting to a different sliced actor mesh
            let sliced_actor_mesh = actor_mesh.range("replicas", 0..2).unwrap();
            // For actors in the previous sliced mesh, the seq should be 3 since
            // this is the third cast for them. For other actors, the seq should
            // be 2.
            let expected_seqs = vec![2, 2, 3, 3];
            testactor::assert_casting_correctness(
                &sliced_actor_mesh,
                instance,
                Some((session_id, expected_seqs)),
            )
            .await;
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_seq_from_different_senders() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());
        let proc = Proc::new(id!(test[0]), DialMailboxRouter::new().boxed());
        let (instance, _) = proc.instance("test_client").unwrap();
        let (first_instance, _) = proc.instance("first_client").unwrap();
        let (second_instance, _) = proc.instance("second_client").unwrap();
        let (third_instance, _) = proc.instance("third_client").unwrap();

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            let actor_mesh = spawm_for_seq_test(&instance, &proc_mesh).await;

            // Sequence numbers are calculated based on the sequencer, i.e. the
            // client name. So three casts would result in seq 1 for all actors.
            for inst in [&first_instance, &second_instance, &third_instance] {
                let expected_seqs = vec![1; 8];
                let session_id = inst.sequencer().session_id();
                testactor::assert_casting_correctness(
                    &actor_mesh,
                    inst,
                    Some((session_id, expected_seqs)),
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn test_failing_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance().await;

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            let err = proc_mesh
                .spawn::<testactor::FailingCreateTestActor>(instance, "testfail", &())
                .await
                .unwrap_err();
            assert_matches!(err, v1::Error::GspawnError(_, _))
        }
    }
}
