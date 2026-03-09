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
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use hyperactor::Actor;
use hyperactor::Handler;
use hyperactor::RemoteMessage;
use hyperactor::RemoteSpawn;
use hyperactor::accum::StreamingReducerOpts;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::actor::remote::Remote;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use hyperactor::reference as hyperactor_reference;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use ndslice::Extent;
use ndslice::ViewExt as _;
use ndslice::view;
use ndslice::view::CollectMeshExt;
use ndslice::view::Ranked;
use ndslice::view::Region;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::ActorMesh;
use crate::ActorMeshRef;
use crate::CommActor;
use crate::Error;
use crate::HostMeshRef;
use crate::Name;
use crate::ValueMesh;
use crate::comm::CommMeshConfig;
use crate::host_mesh::host_agent::ProcState;
use crate::host_mesh::mesh_to_rankedvalues_with_default;
use crate::mesh_controller::ActorMeshController;
use crate::proc_agent;
use crate::proc_agent::ActorState;
use crate::proc_agent::ProcAgent;
use crate::resource;
use crate::resource::GetRankStatus;
use crate::resource::Status;
use crate::supervision::MeshFailure;

declare_attrs! {
    /// The maximum idle time between updates while spawning actor
    /// meshes.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ACTOR_SPAWN_MAX_IDLE".to_string()),
        Some("actor_spawn_max_idle".to_string()),
    ))
    pub attr ACTOR_SPAWN_MAX_IDLE: Duration = Duration::from_secs(30);

    /// The maximum idle time between updates while waiting for a response to GetState
    /// from ProcAgent.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_GET_ACTOR_STATE_MAX_IDLE".to_string()),
        Some("get_actor_state_max_idle".to_string()),
    ))
    pub attr GET_ACTOR_STATE_MAX_IDLE: Duration = Duration::from_secs(30);
}

/// Name used for the mesh communication actor spawned on each user proc.
///
/// The `CommActor` enables proc-to-proc mesh messaging and is always
/// present as a system actor (`system_children`) on every proc mesh member.
pub const COMM_ACTOR_NAME: &str = "comm";

/// A reference to a single [`hyperactor::Proc`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcRef {
    proc_id: hyperactor_reference::ProcId,
    /// The rank of this proc at creation.
    create_rank: usize,
    /// The agent managing this proc.
    agent: hyperactor_reference::ActorRef<ProcAgent>,
}

impl ProcRef {
    /// Create a new proc ref from the provided id, create rank and agent.
    pub fn new(
        proc_id: hyperactor_reference::ProcId,
        create_rank: usize,
        agent: hyperactor_reference::ActorRef<ProcAgent>,
    ) -> Self {
        Self {
            proc_id,
            create_rank,
            agent,
        }
    }

    pub fn proc_id(&self) -> &hyperactor_reference::ProcId {
        &self.proc_id
    }

    pub(crate) fn actor_id(&self, name: &Name) -> hyperactor_reference::ActorId {
        self.proc_id.actor_id(name.to_string(), 0)
    }

    /// Generic bound: `A: Referable` - required because we return
    /// an `ActorRef<A>`.
    pub(crate) fn attest<A: Referable>(&self, name: &Name) -> hyperactor_reference::ActorRef<A> {
        hyperactor_reference::ActorRef::attest(self.actor_id(name))
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
    async fn create<C: context::Actor>(
        cx: &C,
        name: Name,
        allocation: ProcMeshAllocation,
        spawn_comm_actor: bool,
    ) -> crate::Result<Self>
    where
        C::A: Handler<MeshFailure>,
    {
        let comm_actor_name = if spawn_comm_actor {
            Some(Name::new(COMM_ACTOR_NAME).unwrap())
        } else {
            None
        };

        let region = allocation.extent().clone().into();
        let ranks = allocation.ranks();

        // Set the global supervision sink to the first ProcAgent's
        // supervision event handler. Last-mesh-wins semantics: if a
        // previous mesh installed a sink, it is replaced.
        if let Some(first) = ranks.first() {
            crate::global_context::set_global_supervision_sink(
                first.agent.port::<ActorSupervisionEvent>(),
            );
        }

        let host_mesh = allocation.hosts();
        let current_ref =
            ProcMeshRef::new(name.clone(), region, ranks, host_mesh.cloned()).unwrap();

        // Notify telemetry that the ProcAgent mesh was created.
        {
            let name_str = name.to_string();
            let mesh_id_hash = hyperactor_telemetry::hash_to_u64(&name_str);

            let (parent_mesh_id, parent_view_json) = match host_mesh {
                Some(hm) => (
                    Some(hyperactor_telemetry::hash_to_u64(&hm.name().to_string())),
                    serde_json::to_string(hm.region()).ok(),
                ),
                None => (None, None),
            };

            hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
                id: mesh_id_hash,
                timestamp: RealClock.system_time_now(),
                class: "Proc".to_string(),
                given_name: name.name().to_string(),
                full_name: name_str,
                shape_json: serde_json::to_string(&current_ref.region.extent()).unwrap_or_default(),
                parent_mesh_id,
                parent_view_json,
            });

            // Notify telemetry of each ProcAgent actor in this mesh.
            // These are skipped in Proc::spawn_inner. mesh_id directly points to proc mesh.
            let now = RealClock.system_time_now();
            for rank in current_ref.ranks.iter() {
                let actor_id = rank.agent.actor_id();

                hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                    id: hyperactor_telemetry::hash_to_u64(actor_id),
                    timestamp: now,
                    mesh_id: mesh_id_hash,
                    rank: rank.create_rank as u64,
                    full_name: actor_id.to_string(),
                    display_name: None,
                });
            }
        }

        let proc_mesh = Self {
            name,
            allocation,
            comm_actor_name: comm_actor_name.clone(),
            current_ref,
        };

        if let Some(comm_actor_name) = comm_actor_name {
            // CommActor satisfies `Actor + Referable`, so it can be
            // spawned and safely referenced via ActorRef<CommActor>.
            // It is a system actor that should not have a controller managing it.
            let comm_actor_mesh: ActorMesh<CommActor> = proc_mesh
                .spawn_with_name(cx, comm_actor_name, &Default::default(), None, true)
                .await?;
            let address_book: HashMap<_, _> = comm_actor_mesh
                .iter()
                .map(|(point, actor_ref)| (point.rank(), actor_ref))
                .collect();
            // Now that we have all of the spawned comm actors, kick them all into
            // mesh mode.
            for (rank, comm_actor) in &address_book {
                comm_actor
                    .send(cx, CommMeshConfig::new(*rank, address_book.clone()))
                    .map_err(|e| Error::SendingError(comm_actor.actor_id().clone(), Box::new(e)))?
            }
        }

        Ok(proc_mesh)
    }

    pub(crate) async fn create_owned_unchecked<C: context::Actor>(
        cx: &C,
        name: Name,
        extent: Extent,
        hosts: HostMeshRef,
        ranks: Vec<ProcRef>,
    ) -> crate::Result<Self>
    where
        C::A: Handler<MeshFailure>,
    {
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

    /// Stop this mesh gracefully.
    pub async fn stop(&mut self, cx: &impl context::Actor, reason: String) -> anyhow::Result<()> {
        let region = self.region.clone();
        match &mut self.allocation {
            ProcMeshAllocation::Owned { hosts, .. } => {
                let procs = self
                    .current_ref
                    .proc_ids()
                    .collect::<Vec<hyperactor_reference::ProcId>>();
                hosts
                    .stop_proc_mesh(cx, &self.name, procs, region, reason)
                    .await
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn ranks(&self) -> Arc<Vec<ProcRef>> {
        self.allocation.ranks()
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

/// Represents how a ProcMesh was allocated.
enum ProcMeshAllocation {
    /// An owned allocation: this ProcMesh fully owns the set of ranks.
    Owned {
        /// The host mesh from which the proc mesh was spawned.
        hosts: HostMeshRef,
        extent: Extent,
        /// A proc reference for each rank in the mesh.
        ranks: Arc<Vec<ProcRef>>,
    },
}

impl ProcMeshAllocation {
    fn extent(&self) -> &Extent {
        match self {
            ProcMeshAllocation::Owned { extent, .. } => extent,
        }
    }

    fn ranks(&self) -> Arc<Vec<ProcRef>> {
        match self {
            ProcMeshAllocation::Owned { ranks, .. } => Arc::clone(ranks),
        }
    }

    fn hosts(&self) -> Option<&HostMeshRef> {
        match self {
            ProcMeshAllocation::Owned { hosts, .. } => Some(hosts),
        }
    }
}

impl fmt::Debug for ProcMeshAllocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
}
wirevalue::register_type!(ProcMeshRef);

impl ProcMeshRef {
    /// Create a new ProcMeshRef from the given name, region, ranks, and host mesh.
    #[allow(clippy::result_large_err)]
    fn new(
        name: Name,
        region: Region,
        ranks: Arc<Vec<ProcRef>>,
        host_mesh: Option<HostMeshRef>,
    ) -> crate::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(crate::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        Ok(Self {
            name,
            region,
            ranks,
            host_mesh,
        })
    }

    /// Create a singleton ProcMeshRef, given the provided ProcRef and name.
    pub fn new_singleton(name: Name, proc_ref: ProcRef) -> Self {
        Self {
            name,
            region: Extent::unity().into(),
            ranks: Arc::new(vec![proc_ref]),
            host_mesh: None,
        }
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

    pub(crate) fn agent_mesh(&self) -> ActorMeshRef<ProcAgent> {
        let agent_name = self.ranks.first().unwrap().agent.actor_id().name();
        // This name must match the ProcAgent name, which can change depending on the allocator.
        // Since we control the agent_name, it is guaranteed to be a valid mesh identifier.
        // No controller for the ProcAgent mesh.
        ActorMeshRef::new(Name::new_reserved(agent_name).unwrap(), self.clone(), None)
    }

    /// Query the state of all actors in this mesh matching "name".
    pub async fn actor_states(
        &self,
        cx: &impl context::Actor,
        name: Name,
    ) -> crate::Result<ValueMesh<resource::State<ActorState>>> {
        self.actor_states_with_keepalive(cx, name, None).await
    }

    /// Query the state of all actors in this mesh matching "name".
    /// If keepalive is Some, use a message that indicates to the recipient
    /// that the owner of the mesh is still alive, along with the expiry time
    /// after which the actor should be considered orphaned. Else, use a normal
    /// state query.
    pub(crate) async fn actor_states_with_keepalive(
        &self,
        cx: &impl context::Actor,
        name: Name,
        keepalive: Option<std::time::SystemTime>,
    ) -> crate::Result<ValueMesh<resource::State<ActorState>>> {
        let agent_mesh = self.agent_mesh();
        let (port, mut rx) = cx.mailbox().open_port::<resource::State<ActorState>>();
        // TODO: Use accumulation to get back a single value (representing whether
        // *any* of the actors failed) instead of a mesh.
        let get_state = resource::GetState::<ActorState> {
            name: name.clone(),
            reply: port.bind(),
        };
        if let Some(expires_after) = keepalive {
            agent_mesh.cast(
                cx,
                resource::KeepaliveGetState {
                    expires_after,
                    get_state,
                },
            )?;
        } else {
            agent_mesh.cast(cx, get_state)?;
        }
        let expected = self.ranks.len();
        let mut states = Vec::with_capacity(expected);
        let timeout = hyperactor_config::global::get(GET_ACTOR_STATE_MAX_IDLE);
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
                                    ActorStatus::generic_failure(format!(
                                        "timeout waiting for message from proc mesh agent while querying for \"{}\". The process likely crashed",
                                        name,
                                    )),
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
    ) -> crate::Result<Option<ValueMesh<resource::State<ProcState>>>> {
        let names = self
            .proc_ids()
            .collect::<Vec<hyperactor_reference::ProcId>>();
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
    pub(crate) fn proc_ids(&self) -> impl Iterator<Item = hyperactor_reference::ProcId> {
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
    pub async fn spawn<A: RemoteSpawn, C: context::Actor>(
        &self,
        cx: &C,
        name: &str,
        params: &A::Params,
    ) -> crate::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
        C::A: Handler<MeshFailure>,
    {
        // Spawning from a string is never a system actor.
        self.spawn_with_name(cx, Name::new(name)?, params, None, false)
            .await
    }

    /// Spawn a 'service' actor. Service actors are *singletons*, using
    /// reserved names. The provided name is used verbatim as the actor's
    /// name, and thus it may be persistently looked up by constructing
    /// the appropriate name.
    ///
    /// Note: avoid using service actors if possible; the mechanism will
    /// be replaced by an actor registry.
    pub async fn spawn_service<A: RemoteSpawn, C: context::Actor>(
        &self,
        cx: &C,
        name: &str,
        params: &A::Params,
    ) -> crate::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
        C::A: Handler<MeshFailure>,
    {
        self.spawn_with_name(cx, Name::new_reserved(name)?, params, None, false)
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
    /// - `C::A: Handler<MeshFailure>` - in order to spawn actors,
    ///   the actor must accept messages of type `MeshFailure`. This
    ///   is delivered when the actors spawned in the mesh have a failure that
    ///   isn't handled.
    #[hyperactor::instrument(fields(
        host_mesh=self.host_mesh_name().map(|n| n.to_string()),
        proc_mesh=self.name.to_string(),
        actor_name=name.to_string(),
    ))]
    pub async fn spawn_with_name<A: RemoteSpawn, C: context::Actor>(
        &self,
        cx: &C,
        name: Name,
        params: &A::Params,
        supervision_display_name: Option<String>,
        is_system_actor: bool,
    ) -> crate::Result<ActorMesh<A>>
    where
        A::Params: RemoteMessage,
        C::A: Handler<MeshFailure>,
    {
        tracing::info!(
            name = "ProcMeshStatus",
            status = "ActorMesh::Spawn::Attempt",
        );
        tracing::info!(name = "ActorMeshStatus", status = "Spawn::Attempt");
        let result = self
            .spawn_with_name_inner(cx, name, params, supervision_display_name, is_system_actor)
            .await;
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

    async fn spawn_with_name_inner<A: RemoteSpawn, C: context::Actor>(
        &self,
        cx: &C,
        name: Name,
        params: &A::Params,
        supervision_display_name: Option<String>,
        is_system_actor: bool,
    ) -> crate::Result<ActorMesh<A>>
    where
        C::A: Handler<MeshFailure>,
    {
        let remote = Remote::collect();
        // `RemoteSpawn` + `remote!(A)` ensure that `A` has a
        // `SpawnableActor` entry in this registry, so
        // `name_of::<A>()` can resolve its global type name.
        let actor_type = remote
            .name_of::<A>()
            .ok_or(Error::ActorTypeNotRegistered(type_name::<A>().to_string()))?
            .to_string();

        let serialized_params = bincode::serialize(params)?;
        let agent_mesh = self.agent_mesh();

        agent_mesh.cast(
            cx,
            resource::CreateOrUpdate::<proc_agent::ActorSpec> {
                name: name.clone(),
                rank: Default::default(),
                spec: proc_agent::ActorSpec {
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
            crate::StatusMesh::from_single(region.clone(), Status::NotExist),
            StreamingReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
                initial_update_interval: None,
            },
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
        let (statuses, mut mesh) = match GetRankStatus::wait(
            rx,
            self.ranks.len(),
            hyperactor_config::global::get(ACTOR_SPAWN_MAX_IDLE),
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
                    Ok((statuses, ActorMesh::new(self.clone(), name.clone(), None)))
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
        // We don't need controllers for a system actor like the CommActor.
        if !is_system_actor {
            // Spawn a unique mesh manager for each actor mesh, so the type of the
            // mesh can be preserved.
            let controller: ActorMeshController<A> = ActorMeshController::new(
                mesh.deref().clone(),
                supervision_display_name.clone(),
                Some(cx.instance().port().bind()),
                statuses,
            );
            // AI-3: controller name must include mesh identity for
            // proc-wide ActorId uniqueness. A fixed base name alone
            // collides across parents because pid allocation is
            // parent-scoped.
            let controller_name = format!(
                "{}_{}",
                crate::mesh_controller::ACTOR_MESH_CONTROLLER_NAME,
                mesh.name()
            );
            let controller = controller
                .spawn_with_name(cx, &controller_name)
                .map_err(|e| Error::ControllerActorSpawnError(mesh.name().clone(), e))?;
            // Controller and ActorMesh both depend on references from each other, break
            // the cycle by setting the controller after the fact.
            mesh.set_controller(Some(controller.bind()));
        }
        // Notify telemetry that an actor mesh was created.
        {
            let name_str = mesh.name().to_string();

            // Hash the actor mesh name. This is used as mesh_id for both
            // the MeshEvent and the per-actor ActorEvents below.
            let mesh_id_hash = hyperactor_telemetry::hash_to_u64(&name_str);

            // Hash the proc mesh name for parent_mesh_id.
            let parent_mesh_id_hash = hyperactor_telemetry::hash_to_u64(&self.name().to_string());

            hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
                id: mesh_id_hash,
                timestamp: RealClock.system_time_now(),
                class: actor_type,
                given_name: mesh.name().name().to_string(),
                full_name: name_str,
                shape_json: serde_json::to_string(&self.region().extent()).unwrap_or_default(),
                parent_mesh_id: Some(parent_mesh_id_hash),
                parent_view_json: serde_json::to_string(self.region()).ok(),
            });

            // Notify telemetry of each actor in this mesh. The rank is
            // the actor's position within the actor mesh (not the proc's
            // create_rank, which reflects the original unsliced mesh).
            let now = RealClock.system_time_now();
            for (rank, proc_ref) in self.ranks.iter().enumerate() {
                let display_name = supervision_display_name.as_ref().map(|sdn| {
                    let point = self.region().extent().point_of_rank(rank).unwrap();
                    crate::actor_display_name(sdn, &point)
                });
                let actor_id = proc_ref.actor_id(&name);
                hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                    id: hyperactor_telemetry::hash_to_u64(&actor_id),
                    timestamp: now,
                    mesh_id: mesh_id_hash,
                    rank: rank as u64,
                    full_name: actor_id.to_string(),
                    display_name,
                });
            }
        }

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
        reason: String,
    ) -> crate::Result<ValueMesh<Status>> {
        tracing::info!(name = "ProcMeshStatus", status = "ActorMesh::Stop::Attempt");
        tracing::info!(name = "ActorMeshStatus", status = "Stop::Attempt");
        let result = self.stop_actor_by_name_inner(cx, mesh_name, reason).await;
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
        reason: String,
    ) -> crate::Result<ValueMesh<Status>> {
        let region = self.region().clone();
        let agent_mesh = self.agent_mesh();
        agent_mesh.cast(
            cx,
            resource::Stop {
                name: mesh_name.clone(),
                reason,
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
            crate::StatusMesh::from_single(region.clone(), Status::NotExist),
            StreamingReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
                initial_update_interval: None,
            },
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
        let max_idle_time = hyperactor_config::global::get(ACTOR_SPAWN_MAX_IDLE);
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
                    Ok(statuses)
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
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use hyperactor::Instance;
    use ndslice::extent;
    use timed_test::async_timed_test;

    use crate::resource::RankedValues;
    use crate::resource::Status;
    use crate::testactor;
    use crate::testing;

    #[async_timed_test(timeout_secs = 30)]
    #[cfg(fbcode_build)]
    async fn test_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance();

        let mut hm = testing::host_mesh(4).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 2))
            .await
            .unwrap();
        let actor_mesh = proc_mesh.spawn(instance, "test", &()).await.unwrap();
        testactor::assert_mesh_shape(actor_mesh).await;

        let _ = hm.shutdown(instance).await;
    }

    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_failing_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

        let instance = testing::instance();

        let mut hm = testing::host_mesh(4).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 2))
            .await
            .unwrap();
        let err = proc_mesh
            .spawn::<testactor::FailingCreateTestActor, Instance<testing::TestRootClient>>(
                instance,
                "testfail",
                &(),
            )
            .await
            .unwrap_err();
        let statuses = err.into_actor_spawn_error().unwrap();
        assert_eq!(
            statuses,
            RankedValues::from((0..8, Status::Failed("test failure".to_string()))),
        );

        let _ = hm.shutdown(instance).await;
    }
}
