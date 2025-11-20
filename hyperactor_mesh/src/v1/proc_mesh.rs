/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::type_name;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::panic::Location;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::RemoteMessage;
use hyperactor::accum::ReducerOpts;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::actor::remote::Remote;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::config;
use hyperactor::config::CONFIG;
use hyperactor::config::ConfigAttr;
use hyperactor::context;
use hyperactor::declare_attrs;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxServer;
use hyperactor::supervision::ActorSupervisionEvent;
use ndslice::Extent;
use ndslice::ViewExt as _;
use ndslice::view;
use ndslice::view::CollectMeshExt;
use ndslice::view::MapIntoExt;
use ndslice::view::Ranked;
use ndslice::view::Region;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Notify;
use tracing::Instrument;

use crate::CommActor;
use crate::alloc::Alloc;
use crate::alloc::AllocExt;
use crate::alloc::AllocatedProc;
use crate::assign::Ranks;
use crate::comm::CommActorMode;
use crate::proc_mesh::mesh_agent;
use crate::proc_mesh::mesh_agent::ActorState;
use crate::proc_mesh::mesh_agent::MeshAgentMessageClient;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::proc_mesh::mesh_agent::ReconfigurableMailboxSender;
use crate::resource;
use crate::resource::GetRankStatus;
use crate::resource::Status;
use crate::v1;
use crate::v1::ActorMesh;
use crate::v1::ActorMeshRef;
use crate::v1::Error;
use crate::v1::HostMeshRef;
use crate::v1::Name;
use crate::v1::ValueMesh;
use crate::v1::host_mesh::mesh_agent::ProcState;
use crate::v1::host_mesh::mesh_to_rankedvalues_with_default;
use crate::v1::mesh_controller::ActorMeshController;

declare_attrs! {
    /// The maximum idle time between updates while spawning actor
    /// meshes.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_ACTOR_SPAWN_MAX_IDLE".to_string()),
        py_name: None,
    })
    pub attr ACTOR_SPAWN_MAX_IDLE: Duration = Duration::from_secs(30);

    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESH_GET_ACTOR_STATE_MAX_IDLE".to_string()),
        py_name: None,
    })
    pub attr GET_ACTOR_STATE_MAX_IDLE: Duration = Duration::from_secs(60);
}

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

    pub fn proc_id(&self) -> &ProcId {
        &self.proc_id
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
#[derive(Debug)]
pub struct ProcMesh {
    #[allow(dead_code)]
    name: Name,
    allocation: ProcMeshAllocation,
    #[allow(dead_code)]
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
        let host_mesh = allocation.hosts();
        let current_ref = ProcMeshRef::new(
            name.clone(),
            region,
            ranks,
            host_mesh.cloned(),
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

    fn alloc_counter() -> &'static AtomicUsize {
        static C: OnceLock<AtomicUsize> = OnceLock::new();
        C.get_or_init(|| AtomicUsize::new(0))
    }

    /// Allocate a new ProcMesh from the provided alloc.
    /// Allocate does not require an owning actor because references are not owned.
    #[track_caller]
    pub async fn allocate(
        cx: &impl context::Actor,
        alloc: Box<dyn Alloc + Send + Sync + 'static>,
        name: &str,
    ) -> v1::Result<Self> {
        let caller = Location::caller();
        Self::allocate_inner(cx, alloc, Name::new(name), caller).await
    }

    // Use allocate_inner to set field mesh_name in span
    #[hyperactor::instrument(fields(proc_mesh=name.to_string()))]
    async fn allocate_inner(
        cx: &impl context::Actor,
        mut alloc: Box<dyn Alloc + Send + Sync + 'static>,
        name: Name,
        caller: &'static Location<'static>,
    ) -> v1::Result<Self> {
        let alloc_id = Self::alloc_counter().fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!(
            name = "ProcMeshStatus",
            status = "Allocate::Attempt",
            %caller,
            alloc_id,
            shape = ?alloc.shape(),
            "allocating proc mesh"
        );

        let running = alloc
            .initialize()
            .instrument(tracing::info_span!(
                "ProcMeshStatus::Allocate::Initialize",
                alloc_id,
                proc_mesh = %name
            ))
            .await?;

        // Wire the newly created mesh into the proc, so that it is routable.
        // We route all of the relevant prefixes into the proc's forwarder,
        // and serve it on the alloc's transport.
        //
        // This will be removed with direct addressing.
        let proc = cx.instance().proc();

        // First make sure we can serve the proc:
        let proc_channel_addr = {
            let _guard =
                tracing::info_span!("allocate_serve_proc", proc_id = %proc.proc_id()).entered();
            let (addr, rx) = channel::serve(ChannelAddr::any(alloc.transport()))?;
            proc.clone().serve(rx);
            tracing::info!(
                name = "ProcMeshStatus",
                status = "Allocate::ChannelServe",
                proc_mesh = %name,
                %addr,
                "proc started listening on addr: {addr}"
            );
            addr
        };

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
            mesh_agent
                .configure(
                    cx,
                    rank,
                    proc_channel_addr.clone(),
                    None, // no supervisor; we just crash
                    address_book.clone(),
                    config_handle.bind(),
                    true,
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

        let ranks: Vec<_> = running
            .into_iter()
            .enumerate()
            .map(|(create_rank, allocated)| ProcRef {
                proc_id: allocated.proc_id,
                create_rank,
                agent: allocated.mesh_agent,
            })
            .collect();

        let stop = Arc::new(Notify::new());
        let extent = alloc.extent().clone();
        let alloc_name = alloc.world_id().to_string();

        {
            let stop = Arc::clone(&stop);

            tokio::spawn(
                async move {
                    loop {
                        tokio::select! {
                            _ = stop.notified() => {
                                // If we are explicitly stopped, the alloc is torn down.
                                if let Err(error) = alloc.stop_and_wait().await {
                                    tracing::error!(
                                        name = "ProcMeshStatus",
                                        alloc_name = %alloc.world_id(),
                                        status = "FailedToStopAlloc",
                                        %error,
                                    );
                                }
                                break;
                            }
                            // We are mostly just using this to drive allocation events.
                            proc_state = alloc.next() => {
                                match proc_state {
                                    // The alloc was stopped.
                                    None => break,
                                    Some(proc_state) => {
                                        tracing::debug!(
                                            alloc_name = %alloc.world_id(),
                                            "unmonitored allocation event: {}", proc_state);
                                    }
                                }

                            }
                        }
                    }
                }
                .instrument(tracing::info_span!("alloc_monitor")),
            );
        }

        let mesh = Self::create(
            cx,
            name,
            ProcMeshAllocation::Allocated {
                alloc_name,
                stop,
                extent,
                ranks: Arc::new(ranks),
            },
            true, // alloc-based meshes support comm actors
        )
        .await;
        match &mesh {
            Ok(_) => tracing::info!(name = "ProcMeshStatus", status = "Allocate::Created"),
            Err(error) => {
                tracing::info!(name = "ProcMeshStatus", status = "Allocate::Failed", %error)
            }
        }
        mesh
    }

    /// Detach the proc mesh from the lifetime of `self`, and return its reference.
    #[cfg(test)]
    pub(crate) fn detach(self) -> ProcMeshRef {
        // This also keeps the ProcMeshAllocation::Allocated alloc task alive.
        self.current_ref.clone()
    }

    /// Stop this mesh gracefully.
    pub async fn stop(&mut self, cx: &impl context::Actor) -> anyhow::Result<()> {
        let region = self.region.clone();
        match &mut self.allocation {
            ProcMeshAllocation::Allocated {
                stop, alloc_name, ..
            } => {
                stop.notify_one();
                tracing::info!(
                    name = "ProcMeshStatus",
                    proc_mesh = %self.name,
                    alloc_name,
                    status = "StoppingAlloc",
                    "sending stop to alloc {alloc_name}; check its log for stop status",
                );
                Ok(())
            }
            ProcMeshAllocation::Owned { hosts, .. } => {
                let procs = self.current_ref.proc_ids().collect::<Vec<ProcId>>();
                // We use the proc mesh region rather than the host mesh region
                // because the host agent stores one entry per proc, not per host.
                hosts.stop_proc_mesh(cx, &self.name, procs, region).await
            }
        }
    }
}

impl fmt::Display for ProcMesh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.current_ref)
    }
}

impl Deref for ProcMesh {
    type Target = ProcMeshRef;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
    }
}

impl Drop for ProcMesh {
    fn drop(&mut self) {
        tracing::info!(
            name = "ProcMeshStatus",
            proc_mesh = %self.name,
            status = "Dropped",
        );
    }
}

/// Represents different ways ProcMeshes can be allocated.
enum ProcMeshAllocation {
    /// A mesh that has been allocated from an `Alloc`.
    Allocated {
        // The name of the alloc from which this mesh was allocated.
        alloc_name: String,

        // A cancellation token used to stop the task keeping the alloc alive.
        stop: Arc<Notify>,

        extent: Extent,

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
            ProcMeshAllocation::Allocated { extent, .. } => extent,
            ProcMeshAllocation::Owned { extent, .. } => extent,
        }
    }

    fn ranks(&self) -> Arc<Vec<ProcRef>> {
        Arc::clone(match self {
            ProcMeshAllocation::Allocated { ranks, .. } => ranks,
            ProcMeshAllocation::Owned { ranks, .. } => ranks,
        })
    }

    fn hosts(&self) -> Option<&HostMeshRef> {
        match self {
            ProcMeshAllocation::Allocated { .. } => None,
            ProcMeshAllocation::Owned { hosts, .. } => Some(hosts),
        }
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
/// arranged into a region. ProcMeshes are named, uniquely identifying the
/// ProcMesh from which the reference was derived.
///
/// ProcMeshes can be sliced to create new ProcMeshes with a subset of the
/// original ranks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct ProcMeshRef {
    name: Name,
    region: Region,
    ranks: Arc<Vec<ProcRef>>,
    // Some if this was spawned from a host mesh, else none.
    host_mesh: Option<HostMeshRef>,
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
    #[allow(clippy::result_large_err)]
    fn new(
        name: Name,
        region: Region,
        ranks: Arc<Vec<ProcRef>>,
        host_mesh: Option<HostMeshRef>,
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
            host_mesh,
            root_region,
            root_comm_actor,
        })
    }

    pub(crate) fn root_comm_actor(&self) -> Option<&ActorRef<CommActor>> {
        self.root_comm_actor.as_ref()
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub fn host_mesh_name(&self) -> Option<&Name> {
        self.host_mesh.as_ref().map(|h| h.name())
    }

    /// Returns the HostMeshRef that this ProcMeshRef might be backed by.
    /// Returns None if this ProcMeshRef is backed by an Alloc instead of a host mesh.
    pub fn hosts(&self) -> Option<&HostMeshRef> {
        self.host_mesh.as_ref()
    }

    /// The current statuses of procs in this mesh.
    pub async fn status(&self, cx: &impl context::Actor) -> v1::Result<ValueMesh<bool>> {
        let vm: ValueMesh<_> = self.map_into(|proc_ref| {
            let proc_ref = proc_ref.clone();
            async move { proc_ref.status(cx).await }
        });
        vm.join().await.transpose()
    }

    pub(crate) fn agent_mesh(&self) -> ActorMeshRef<ProcMeshAgent> {
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
        let timeout = config::global::get(GET_ACTOR_STATE_MAX_IDLE);
        for _ in 0..expected {
            // The agent runs on the same process as the running actor, so if some
            // fatal event caused the process to crash (e.g. OOM, signal, process exit),
            // the agent will be unresponsive.
            // We handle this by setting a timeout on the recv, and if we don't get a
            // message we assume the agent is dead and return a failed state.
            let state = RealClock.timeout(timeout, rx.recv()).await;
            if let Ok(state) = state {
                // Handle non-timeout receiver error.
                let state = state?;
                match state.state {
                    Some(ref inner) => {
                        states.push((inner.create_rank, state));
                    }
                    None => {
                        return Err(Error::NotExist(state.name));
                    }
                }
            } else {
                tracing::error!(
                    "timeout waiting for a message after {:?} from proc mesh agent in mesh {}",
                    timeout,
                    agent_mesh
                );
                // Timeout error, stop reading from the receiver and send back what we have so far,
                // padding with failed states.
                let all_ranks = (0..self.ranks.len()).collect::<HashSet<_>>();
                let completed_ranks = states.iter().map(|(rank, _)| *rank).collect::<HashSet<_>>();
                let mut leftover_ranks = all_ranks.difference(&completed_ranks).collect::<Vec<_>>();
                assert_eq!(leftover_ranks.len(), expected - states.len());
                while states.len() < expected {
                    let rank = *leftover_ranks
                        .pop()
                        .expect("leftover ranks should not be empty");
                    let agent = agent_mesh.get(rank).expect("agent should exist");
                    let agent_id = agent.actor_id().clone();
                    states.push((
                        // We populate with any ranks leftover at the time of the timeout.
                        rank,
                        resource::State {
                            name: name.clone(),
                            status: resource::Status::Timeout(timeout),
                            // We don't know the ActorId that used to live on this rank.
                            // But we do know the mesh agent id, so we'll use that.
                            state: Some(ActorState {
                                actor_id: agent_id.clone(),
                                create_rank: rank,
                                supervision_events: vec![ActorSupervisionEvent::new(
                                    agent_id,
                                    None,
                                    ActorStatus::Stopped,
                                    None,
                                )],
                            }),
                        },
                    ));
                }
                break;
            }
        }
        // Ensure that all ranks have replied. Note that if the mesh is sliced,
        // not all create_ranks may be in the mesh.
        // Sort by rank, so that the resulting mesh is ordered.
        states.sort_by_key(|(rank, _)| *rank);
        let vm = states
            .into_iter()
            .map(|(_, state)| state)
            .collect_mesh::<ValueMesh<_>>(self.region.clone())?;
        Ok(vm)
    }

    pub async fn proc_states(
        &self,
        cx: &impl context::Actor,
    ) -> v1::Result<Option<ValueMesh<resource::State<ProcState>>>> {
        let names = self.proc_ids().collect::<Vec<ProcId>>();
        if let Some(host_mesh) = &self.host_mesh {
            Ok(Some(
                host_mesh
                    .proc_states(cx, names, self.region.clone())
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Returns an iterator over the proc ids in this mesh.
    pub(crate) fn proc_ids(&self) -> impl Iterator<Item = ProcId> {
        self.ranks.iter().map(|proc_ref| proc_ref.proc_id.clone())
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

    /// Spawn a 'service' actor. Service actors are *singletons*, using
    /// reserved names. The provided name is used verbatim as the actor's
    /// name, and thus it may be persistently looked up by constructing
    /// the appropriate name.
    ///
    /// Note: avoid using service actors if possible; the mechanism will
    /// be replaced by an actor registry.
    pub async fn spawn_service<A: Actor + Referable>(
        &self,
        cx: &impl context::Actor,
        name: &str,
        params: &A::Params,
    ) -> v1::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
    {
        self.spawn_with_name(cx, Name::new_reserved(name), params)
            .await
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
    /// - `A::Params: RemoteMessage` - spawn parameters must be
    ///   serializable and routable.
    #[hyperactor::instrument(fields(
        host_mesh=self.host_mesh_name().map(|n| n.to_string()),
        proc_mesh=self.name.to_string(),
        actor_mesh=name.to_string(),
    ))]
    pub(crate) async fn spawn_with_name<A: Actor + Referable>(
        &self,
        cx: &impl context::Actor,
        name: Name,
        params: &A::Params,
    ) -> v1::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
    {
        tracing::info!(
            name = "ProcMeshStatus",
            status = "ActorMesh::Spawn::Attempt",
        );
        tracing::info!(name = "ActorMeshStatus", status = "Spawn::Attempt");
        let result = self.spawn_with_name_inner(cx, name, params).await;
        match &result {
            Ok(_) => {
                tracing::info!(
                    name = "ProcMeshStatus",
                    status = "ActorMesh::Spawn::Success",
                );
                tracing::info!(name = "ActorMeshStatus", status = "Spawn::Success");
            }
            Err(error) => {
                tracing::error!(name = "ProcMeshStatus", status = "ActorMesh::Spawn::Failed", %error);
                tracing::error!(name = "ActorMeshStatus", status = "Spawn::Failed", %error);
            }
        }
        result
    }

    async fn spawn_with_name_inner<A: Actor + Referable>(
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
        let agent_mesh = self.agent_mesh();

        agent_mesh.cast(
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

        let region = self.region().clone();
        // Open an accum port that *receives overlays* and *emits full
        // meshes*.
        //
        // NOTE: Mailbox initializes the accumulator state via
        // `Default`, which is an *empty* ValueMesh (0 ranks). Our
        // Accumulator<ValueMesh<T>> implementation detects this on
        // the first update and replaces it with the caller-supplied
        // template (the `self` passed into open_accum_port), which we
        // seed here as "full NotExist over the target region".
        let (port, rx) = cx.mailbox().open_accum_port_opts(
            // Initial state for the accumulator: full mesh seeded to
            // NotExist.
            crate::v1::StatusMesh::from_single(region.clone(), Status::NotExist),
            Some(ReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
            }),
        );

        let mut reply = port.bind();
        // If this proc dies or some other issue renders the reply undeliverable,
        // the reply does not need to be returned to the sender.
        reply.return_undeliverable(false);
        // Send a message to all ranks. They reply with overlays to
        // `port`.
        agent_mesh.cast(
            cx,
            resource::GetRankStatus {
                name: name.clone(),
                reply,
            },
        )?;

        let start_time = RealClock.now();

        // Wait for all ranks to report a terminal or running status.
        // If any proc reports a failure (via supervision) or the mesh
        // times out, `wait()` returns Err with the final snapshot.
        //
        // `rx` is the accumulator output stream: each time reduced
        // overlays are applied, it emits a new StatusMesh snapshot.
        // `wait()` loops on it, deciding when the stream is
        // "complete" (no more NotExist) or times out.
        let mesh = match GetRankStatus::wait(
            rx,
            self.ranks.len(),
            config::global::get(ACTOR_SPAWN_MAX_IDLE),
            region.clone(), // fallback
        )
        .await
        {
            Ok(statuses) => {
                // Spawn succeeds only if no rank has reported a
                // supervision/terminal state. This preserves the old
                // `first_terminating().is_none()` semantics.
                let has_terminating = statuses.values().any(|s| s.is_terminating());
                if !has_terminating {
                    Ok(ActorMesh::new(self.clone(), name))
                } else {
                    let legacy = mesh_to_rankedvalues_with_default(
                        &statuses,
                        Status::NotExist,
                        Status::is_not_exist,
                        self.ranks.len(),
                    );
                    Err(Error::ActorSpawnError { statuses: legacy })
                }
            }
            Err(complete) => {
                // Fill remaining ranks with a timeout status, now
                // handled via the legacy shim.
                let elapsed = start_time.elapsed();
                let legacy = mesh_to_rankedvalues_with_default(
                    &complete,
                    Status::Timeout(elapsed),
                    Status::is_not_exist,
                    self.ranks.len(),
                );
                Err(Error::ActorSpawnError { statuses: legacy })
            }
        }?;
        // Spawn a unique mesh manager for each actor mesh, so the type of the
        // mesh can be preserved.
        let _controller: ActorHandle<ActorMeshController<A>> =
            ActorMeshController::<A>::spawn(cx, mesh.deref().clone())
                .await
                .map_err(|e| Error::ControllerActorSpawnError(mesh.name().clone(), e))?;
        Ok(mesh)
    }

    /// Send stop actors message to all mesh agents for a specific mesh name
    #[hyperactor::instrument(fields(
        host_mesh = self.host_mesh_name().map(|n| n.to_string()),
        proc_mesh = self.name.to_string(),
        actor_mesh = mesh_name.to_string(),
    ))]
    pub(crate) async fn stop_actor_by_name(
        &self,
        cx: &impl context::Actor,
        mesh_name: Name,
    ) -> v1::Result<()> {
        tracing::info!(name = "ProcMeshStatus", status = "ActorMesh::Stop::Attempt");
        tracing::info!(name = "ActorMeshStatus", status = "Stop::Attempt");
        let result = self.stop_actor_by_name_inner(cx, mesh_name).await;
        match &result {
            Ok(_) => {
                tracing::info!(name = "ProcMeshStatus", status = "ActorMesh::Stop::Success");
                tracing::info!(name = "ActorMeshStatus", status = "Stop::Success");
            }
            Err(error) => {
                tracing::error!(name = "ProcMeshStatus", status = "ActorMesh::Stop::Failed", %error);
                tracing::error!(name = "ActorMeshStatus", status = "Stop::Failed", %error);
            }
        }
        result
    }

    async fn stop_actor_by_name_inner(
        &self,
        cx: &impl context::Actor,
        mesh_name: Name,
    ) -> v1::Result<()> {
        let region = self.region().clone();
        let agent_mesh = self.agent_mesh();
        agent_mesh.cast(
            cx,
            resource::Stop {
                name: mesh_name.clone(),
            },
        )?;

        // Open an accum port that *receives overlays* and *emits full
        // meshes*.
        //
        // NOTE: Mailbox initializes the accumulator state via
        // `Default`, which is an *empty* ValueMesh (0 ranks). Our
        // Accumulator<ValueMesh<T>> implementation detects this on
        // the first update and replaces it with the caller-supplied
        // template (the `self` passed into open_accum_port), which we
        // seed here as "full NotExist over the target region".
        let (port, rx) = cx.mailbox().open_accum_port_opts(
            // Initial state for the accumulator: full mesh seeded to
            // NotExist.
            crate::v1::StatusMesh::from_single(region.clone(), Status::NotExist),
            Some(ReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
            }),
        );
        agent_mesh.cast(
            cx,
            resource::GetRankStatus {
                name: mesh_name,
                reply: port.bind(),
            },
        )?;
        let start_time = RealClock.now();

        // Reuse actor spawn idle time.
        let max_idle_time = config::global::get(ACTOR_SPAWN_MAX_IDLE);
        match GetRankStatus::wait(
            rx,
            self.ranks.len(),
            max_idle_time,
            region.clone(), // fallback mesh if nothing arrives
        )
        .await
        {
            Ok(statuses) => {
                // Check that all actors are in some terminal state.
                // Failed is ok, because one of these actors may have failed earlier
                // and we're trying to stop the others.
                let all_stopped = statuses.values().all(|s| s.is_terminating());
                if all_stopped {
                    Ok(())
                } else {
                    let legacy = mesh_to_rankedvalues_with_default(
                        &statuses,
                        Status::NotExist,
                        Status::is_not_exist,
                        self.ranks.len(),
                    );
                    Err(Error::ActorStopError { statuses: legacy })
                }
            }
            Err(complete) => {
                // Fill remaining ranks with a timeout status via the
                // legacy shim.
                let legacy = mesh_to_rankedvalues_with_default(
                    &complete,
                    Status::Timeout(start_time.elapsed()),
                    Status::is_not_exist,
                    self.ranks.len(),
                );
                Err(Error::ActorStopError { statuses: legacy })
            }
        }
    }
}

impl fmt::Display for ProcMeshRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{{{}}}", self.name, self.region)
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
            self.host_mesh.clone(),
            Some(self.root_region.as_ref().unwrap_or(&self.region).clone()),
            self.root_comm_actor.clone(),
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use ndslice::ViewExt;
    use ndslice::extent;
    use timed_test::async_timed_test;

    use crate::resource::RankedValues;
    use crate::resource::Status;
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

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance().await;

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            testactor::assert_mesh_shape(proc_mesh.spawn(instance, "test", &()).await.unwrap())
                .await;
        }
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_failing_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance().await;

        for proc_mesh in testing::proc_meshes(&instance, extent!(replicas = 4, hosts = 2)).await {
            let err = proc_mesh
                .spawn::<testactor::FailingCreateTestActor>(instance, "testfail", &())
                .await
                .unwrap_err();
            let statuses = err.into_actor_spawn_error().unwrap();
            assert_eq!(
                statuses,
                RankedValues::from((0..8, Status::Failed("test failure".to_string()))),
            );
        }
    }
}
