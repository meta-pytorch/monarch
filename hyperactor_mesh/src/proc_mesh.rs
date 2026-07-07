/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::type_name;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use hyperactor::ActorAddr;
use hyperactor::ActorRef;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::ProcAddr;
use hyperactor::RemoteMessage;
use hyperactor::RemoteSpawn;
use hyperactor::accum::StreamingReducerOpts;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::remote::Remote;
use hyperactor::context;
use hyperactor::id::Label;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_telemetry::hash_to_u64;
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
use crate::Error;
use crate::HostMeshRef;
use crate::ValueMesh;
use crate::host_mesh::GET_PROC_STATE_MAX_IDLE;
use crate::host_mesh::host_agent::GetHostProcStates;
use crate::host_mesh::host_agent::ProcState;
use crate::host_mesh::mesh_to_rankedvalues_with_default;
use crate::mesh_controller::ActorMeshControlPlane;
use crate::mesh_controller::ActorMeshController;
use crate::mesh_id::ActorMeshId;
use crate::mesh_id::ProcMeshId;
use crate::mesh_id::ResourceId;
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

/// Returns the telemetry `meshes.id` value for an actor mesh.
pub fn telemetry_actor_mesh_id(proc_mesh_id: &ProcMeshId, actor_mesh_id: &ActorMeshId) -> u64 {
    hash_to_u64(&(proc_mesh_id, actor_mesh_id))
}

/// A reference to a single [`hyperactor::Proc`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcRef {
    proc_id: ProcAddr,
    /// The rank of this proc at creation.
    create_rank: usize,
    /// The agent managing this proc.
    agent: ActorRef<ProcAgent>,
}

impl ProcRef {
    /// Create a new proc ref from the provided id, create rank and agent.
    pub fn new(proc_id: ProcAddr, create_rank: usize, agent: ActorRef<ProcAgent>) -> Self {
        Self {
            proc_id,
            create_rank,
            agent,
        }
    }

    pub fn proc_addr(&self) -> &ProcAddr {
        &self.proc_id
    }

    pub(crate) fn actor_addr(&self, id: &ActorMeshId) -> ActorAddr {
        self.proc_id.actor_addr_uid(id.uid().clone())
    }
}

/// A mesh of processes.
#[derive(Debug)]
pub struct ProcMesh {
    #[allow(dead_code)]
    id: ProcMeshId,
    current_ref: ProcMeshRef,
    controller: Option<ActorRef<crate::mesh_controller::ProcMeshController>>,
}

impl ProcMesh {
    pub(crate) fn create(
        id: ProcMeshId,
        extent: Extent,
        hosts: HostMeshRef,
        ranks: Vec<ProcRef>,
    ) -> crate::Result<Self> {
        let region = extent.into();
        let ranks = Arc::new(ranks);

        // Set the global supervision sink to the first ProcAgent's
        // supervision event handler. Last-mesh-wins semantics: if a
        // previous mesh installed a sink, it is replaced.
        if let Some(first) = ranks.first() {
            crate::global_context::set_global_supervision_sink(
                first.agent.port::<ActorSupervisionEvent>(),
            );
        }

        let current_ref = ProcMeshRef::new(id.clone(), region, ranks, Some(hosts)).unwrap();

        // Notify telemetry that the ProcAgent mesh was created.
        {
            let name_str = id.to_string();
            let mesh_id_hash = hash_to_u64(&id);

            let hm = current_ref
                .host_mesh
                .as_ref()
                .expect("ProcMesh always has a host mesh");
            let parent_mesh_id = hash_to_u64(hm.id());
            let parent_view_json = serde_json::to_string(hm.region())
                .unwrap_or_else(|e| format!("encountered error when serializing region: {}", e));

            hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
                id: mesh_id_hash,
                timestamp: std::time::SystemTime::now(),
                class: "Proc".to_string(),
                given_name: id
                    .display_label()
                    .map(|l| l.as_str())
                    .unwrap_or("unnamed")
                    .to_string(),
                full_name: name_str,
                shape_json: serde_json::to_string(&current_ref.region.extent()).unwrap_or_default(),
                parent_mesh_id: Some(parent_mesh_id),
                parent_view_json: Some(parent_view_json),
            });

            // Notify telemetry of each ProcAgent actor in this mesh.
            // These are skipped in Proc::spawn_inner. mesh_id directly points to proc mesh.
            let now = std::time::SystemTime::now();
            for rank in current_ref.ranks.iter() {
                let actor_addr = rank.agent.actor_addr();

                hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                    id: hyperactor_telemetry::hash_to_u64(actor_addr.id()),
                    timestamp: now,
                    mesh_id: mesh_id_hash,
                    rank: rank.create_rank as u64,
                    full_name: actor_addr.to_string(),
                    display_name: None,
                });
            }
        }

        Ok(Self {
            id,
            current_ref,
            controller: None,
        })
    }

    /// Set or clear the controller actor managing this mesh.
    pub(crate) fn set_controller(
        &mut self,
        controller: Option<ActorRef<crate::mesh_controller::ProcMeshController>>,
    ) {
        self.controller = controller;
    }

    /// Stop this mesh gracefully.
    ///
    /// If a `ProcMeshController` is present (owned meshes spawned from a host
    /// mesh), the stop is delegated to the controller via `resource::Stop`;
    /// the controller's handler awaits `HostMeshRef::stop_proc_mesh`, which
    /// casts `Stop` + `WaitRankStatus{min_status: Stopped}` to the
    /// HostAgents and waits up to `PROC_STOP_MAX_IDLE` for every proc to
    /// reach `Stopped`. We then serialize behind that handler with a
    /// `GetState` to read the final statuses out of the controller's
    /// `health_state`.
    pub async fn stop(&mut self, cx: &impl context::Actor, reason: String) -> anyhow::Result<()> {
        if let Some(controller) = self.controller.take() {
            let id = self.id.resource_id().clone();
            controller.post(
                cx,
                resource::Stop {
                    id: id.clone(),
                    reason,
                },
            );

            // The controller processes messages serially, so by the time it
            // gets to this `GetState`, its `health_state.statuses` already
            // reflects the outcome of `stop_proc_mesh` (Stopping, Stopped,
            // Failed, or Timeout on `PROC_STOP_MAX_IDLE` exhaustion).
            let (port, mut rx) = cx.mailbox().open_port();
            controller.post(
                cx,
                resource::GetState::<resource::mesh::State<()>> {
                    id: id.clone(),
                    reply: port.bind(),
                },
            );

            let statuses = rx.recv().await?;
            let Some(state) = &statuses.state else {
                anyhow::bail!(
                    "non-existent state in GetState reply from controller: {}",
                    controller.actor_addr()
                );
            };
            // `is_terminating` accepts Stopping, Stopped, Failed, and
            // Timeout. The controller's Stop handler has already awaited
            // (or timed out) the underlying HostAgent wait, so any rank
            // still in Running here means the controller never processed
            // the stop for that rank.
            let all_stopped = state.statuses.values().all(|s| s.is_terminating());
            if !all_stopped {
                anyhow::bail!(
                    "proc mesh {} not all procs reached terminating state after stop: {:?}",
                    id,
                    state.statuses,
                );
            }
            return Ok(());
        }

        let region = self.region.clone();
        let procs = self.current_ref.proc_ids().collect::<Vec<ProcAddr>>();
        // We use the proc mesh region rather than the host mesh region
        // because the host agent stores one entry per proc, not per host.
        self.current_ref
            .host_mesh
            .as_ref()
            .expect("ProcMesh always has a host mesh")
            .stop_proc_mesh(cx, &self.id, procs, region, reason)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
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
            proc_mesh = %self.id,
            status = "Dropped",
        );
    }
}

/// A reference to a ProcMesh, consisting of a set of ranked [`ProcRef`]s,
/// arranged into a region. ProcMeshes are named, uniquely identifying the
/// ProcMesh from which the reference was derived.
///
/// ProcMeshes can be sliced to create new ProcMeshes with a subset of the
/// original ranks.
///
/// `ProcMeshRef::sliced` is intentionally pure. A sliced proc mesh can still
/// expose dense `ProcRef`s, while the backing `ProcAgent` `ActorMeshRef`
/// carries a lazy cast-domain descriptor that installs itself on first cast.
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct ProcMeshRef {
    id: ProcMeshId,
    region: Region,
    ranks: Arc<Vec<ProcRef>>,
    /// Actor mesh for the `ProcAgent`s backing this proc mesh view.
    ///
    /// `ProcMeshRef::sliced` derives a sliced agent mesh with a lazy cast
    /// descriptor. The first cast through that view installs the descriptor on
    /// the caller's sender stream.
    ///
    /// The `ProcMeshRef` itself keeps dense `ProcRef`s so it can later
    /// materialize this field from the current view without consulting the
    /// parent mesh or using a temporary proc.
    proc_agent_mesh: ActorMeshRef<ProcAgent>,
    // Some if this was spawned from a host mesh, else none.
    host_mesh: Option<HostMeshRef>,
}
wirevalue::register_type!(ProcMeshRef);

// The proc-agent actor mesh is derived from `ranks`, so it is not part of
// `ProcMeshRef` identity.
impl PartialEq for ProcMeshRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.region == other.region
            && self.ranks == other.ranks
            && self.host_mesh == other.host_mesh
    }
}

impl Eq for ProcMeshRef {}

impl std::hash::Hash for ProcMeshRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.region.hash(state);
        self.ranks.hash(state);
        self.host_mesh.hash(state);
    }
}

impl ProcMeshRef {
    /// Create a new ProcMeshRef from the given id, region, ranks, and so on.
    #[allow(clippy::result_large_err)]
    fn new(
        id: ProcMeshId,
        region: Region,
        ranks: Arc<Vec<ProcRef>>,
        host_mesh: Option<HostMeshRef>,
    ) -> crate::Result<Self> {
        if ranks.is_empty() {
            return Err(crate::Error::ConfigurationError(anyhow::anyhow!(
                "empty proc meshes are not supported"
            )));
        }
        if region.num_ranks() != ranks.len() {
            return Err(crate::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        let proc_agent_mesh = Self::proc_agent_mesh_ref(&id, &region, &ranks)?;
        Ok(Self {
            id,
            region,
            ranks,
            proc_agent_mesh,
            host_mesh,
        })
    }

    /// Create a singleton ProcMeshRef, given the provided ProcRef and id.
    /// This is used to support creating local singleton proc meshes to support `this_proc()`
    /// in python client actors.
    pub fn new_singleton(id: ProcMeshId, proc_ref: ProcRef) -> crate::Result<Self> {
        let region: Region = Extent::unity().into();
        let ranks = Arc::new(vec![proc_ref]);
        let proc_agent_mesh = Self::proc_agent_mesh_ref(&id, &region, &ranks)?;
        Ok(Self {
            id,
            region,
            ranks,
            proc_agent_mesh,
            host_mesh: None,
        })
    }

    pub fn id(&self) -> &ProcMeshId {
        &self.id
    }

    pub fn host_mesh_id(&self) -> Option<&crate::mesh_id::HostMeshId> {
        self.host_mesh.as_ref().map(|h| h.id())
    }

    /// Returns the HostMeshRef that owns this ProcMeshRef, if any.
    pub fn hosts(&self) -> Option<&HostMeshRef> {
        self.host_mesh.as_ref()
    }

    pub(crate) fn agent_mesh(&self) -> &ActorMeshRef<ProcAgent> {
        &self.proc_agent_mesh
    }

    fn proc_agent_mesh_ref(
        proc_mesh_id: &ProcMeshId,
        region: &Region,
        ranks: &[ProcRef],
    ) -> crate::Result<ActorMeshRef<ProcAgent>> {
        let agent_label = ranks
            .first()
            .unwrap()
            .agent
            .actor_addr()
            .label()
            .cloned()
            .unwrap_or_else(|| Label::new(proc_agent::PROC_AGENT_ACTOR_NAME).unwrap());
        let id = ActorMeshId::singleton(agent_label);

        let members = Arc::new(
            ranks
                .iter()
                .map(|rank| rank.agent.actor_addr().clone())
                .collect_mesh::<ValueMesh<_>>(region.clone())
                .map_err(|error| crate::Error::ConfigurationError(error.into()))?,
        );

        Ok(ActorMeshRef::new(
            id,
            Some(proc_mesh_id.clone()),
            region.clone(),
            None,
            members,
        ))
    }

    /// Query the state of all actors in this mesh matching the given id.
    pub async fn actor_states(
        &self,
        cx: &impl context::Actor,
        id: ActorMeshId,
    ) -> crate::Result<ValueMesh<resource::State<ActorState>>> {
        self.actor_states_with_keepalive(cx, id, None).await
    }

    /// Query the state of all actors in this mesh matching the given id.
    /// If keepalive is Some, use a message that indicates to the recipient
    /// that the owner of the mesh is still alive, along with the expiry time
    /// after which the actor should be considered orphaned. Else, use a normal
    /// state query.
    pub(crate) async fn actor_states_with_keepalive(
        &self,
        cx: &impl context::Actor,
        id: ActorMeshId,
        keepalive: Option<std::time::SystemTime>,
    ) -> crate::Result<ValueMesh<resource::State<ActorState>>> {
        let (port, mut rx) = cx.mailbox().open_port::<resource::State<ActorState>>();
        let mut port = port.bind();
        // If this proc dies or some other issue renders the reply undeliverable,
        // the reply does not need to be returned to the sender.
        port.return_undeliverable(false);
        // TODO: Use accumulation to get back a single value (representing whether
        // *any* of the actors failed) instead of a mesh.
        let get_state = resource::GetState::<ActorState> {
            id: id.resource_id().clone(),
            reply: port,
        };
        if let Some(expires_after) = keepalive {
            self.proc_agent_mesh.cast(
                cx,
                resource::KeepaliveGetState {
                    expires_after,
                    get_state,
                },
            )?;
        } else {
            self.proc_agent_mesh.cast(cx, get_state)?;
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
            let state = tokio::time::timeout(timeout, rx.recv()).await;
            if let Ok(state) = state {
                // Handle non-timeout receiver error.
                let state = state?;
                match state.state {
                    Some(ref inner) => {
                        states.push((inner.create_rank, state));
                    }
                    None => {
                        return Err(Error::NotExist(state.id));
                    }
                }
            } else {
                tracing::error!(
                    "timeout waiting for a message after {:?} from proc mesh agent in mesh {}",
                    timeout,
                    self.proc_agent_mesh
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
                    let agent = self.proc_agent_mesh.get(rank).expect("agent should exist");
                    let agent_id = agent.actor_addr().clone();
                    states.push((
                        // We populate with any ranks leftover at the time of the timeout.
                        rank,
                        resource::State {
                            id: id.resource_id().clone(),
                            status: resource::Status::Timeout(timeout),
                            // We don't know the ActorAddr that used to live on this rank.
                            // But we do know the mesh agent id, so we'll use that.
                            // Use u64::MAX so this synthetic state always wins
                            // last-writer-wins ordering against real streamed updates.
                            generation: u64::MAX,
                            timestamp: std::time::SystemTime::now(),
                            state: Some(ActorState {
                                actor_id: agent_id.clone(),
                                create_rank: rank,
                                supervision_events: vec![ActorSupervisionEvent::new(
                                    agent_id,
                                    None,
                                    ActorStatus::generic_failure(format!(
                                        "timeout waiting for message from proc mesh agent while querying for \"{}\". The process likely crashed",
                                        id,
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

    /// Get the state of every proc in this proc mesh.
    ///
    /// Casts a single `GetHostProcStates` to the routing host-agent mesh
    /// carrying this mesh's selected global ranks (the mesh may be sliced, so
    /// they need not be a dense `0..n`). The cast may reach hosts that own no
    /// selected procs, but each HostAgent filters locally and only hosts with
    /// matching ranks reply. Replies reduce up the cast tree (fanning in at cast
    /// actor 0) instead of every host dialing this caller. When `keepalive` is
    /// `Some`, each proc's expiry is extended (orphan protection, as with
    /// `KeepaliveGetState`). Returns `None` when this proc mesh is not backed by
    /// a host mesh (local/in-process meshes). On timeout, ranks whose host did
    /// not reply are padded with a `Timeout` state.
    #[allow(clippy::result_large_err)]
    pub async fn states(
        &self,
        cx: &impl context::Actor,
        keepalive: Option<std::time::SystemTime>,
    ) -> crate::Result<Option<ValueMesh<resource::State<ProcState>>>> {
        // Only meaningful when this proc mesh is backed by a host mesh.
        let Some(host_mesh) = self.host_mesh.as_ref() else {
            return Ok(None);
        };
        let region = self.region.clone();
        let timeout = hyperactor_config::global::get(GET_PROC_STATE_MAX_IDLE);

        // Per-rank template seeded with `Timeout` placeholders. Hosts overlay
        // the ranks they own, so any rank never reported keeps its placeholder
        // and the result is a complete mesh with no post-hoc padding.
        let template: ValueMesh<resource::State<ProcState>> = self
            .ranks
            .iter()
            .map(|proc_ref| resource::State {
                id: ResourceId::new(
                    proc_ref.proc_id.uid().clone(),
                    proc_ref.proc_id.label().cloned(),
                ),
                status: resource::Status::Timeout(timeout),
                state: None,
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            })
            .collect_mesh::<ValueMesh<_>>(region.clone())?;
        // Snapshot returned if no host replies before the idle timeout.
        let fallback = template.clone();

        // Accumulator port: receives sparse per-host overlays and emits the
        // merged full mesh (right-wins). The host mesh is a routing
        // over-approximation for sliced proc meshes; HostAgents that own
        // selected ranks post an overlay, others stay silent.
        let (port, rx) = cx.mailbox().open_accum_port_opts(
            template,
            StreamingReducerOpts {
                max_update_interval: Some(Duration::from_millis(50)),
                initial_update_interval: None,
            },
        );

        host_mesh.agent_mesh().cast(
            cx,
            GetHostProcStates {
                proc_mesh_id: self.id.clone(),
                region: region.clone(),
                keepalive,
                reply: port.bind(),
            },
        )?;

        // Wait until every rank has reported (moved off its `Timeout`
        // placeholder) or we idle out. Either way the mesh is complete: ranks
        // whose host never replied stay `Timeout`, and a failed proc surfaces as
        // its `Failed` state rather than an error.
        let mesh =
            match resource::wait_mesh(rx, timeout, fallback, |s: &resource::State<ProcState>| {
                !matches!(s.status, resource::Status::Timeout(_))
            })
            .await
            {
                Ok(mesh) | Err(mesh) => mesh,
            };

        Ok(Some(mesh))
    }

    /// Returns an iterator over the proc ids in this mesh.
    pub(crate) fn proc_ids(&self) -> impl Iterator<Item = ProcAddr> {
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
        let id = ActorMeshId::instance(Label::strip(name));
        self.spawn_with_name(cx, id, params, None, false).await
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
        let id = ActorMeshId::singleton(Label::strip(name));
        self.spawn_with_name(cx, id, params, None, false).await
    }

    /// Spawn an actor on all procs in this mesh under the given
    /// [`ActorMeshId`](crate::mesh_id::ActorMeshId), returning a new `ActorMesh`.
    ///
    /// This is the underlying implementation used by [`spawn`]; it
    /// differs only in that the actor mesh id is passed explicitly
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
        host_mesh=self.host_mesh_id().map(|id| id.to_string()),
        proc_mesh=self.id.to_string(),
        actor_name=name.to_string(),
    ))]
    pub async fn spawn_with_name<A: RemoteSpawn, C: context::Actor>(
        &self,
        cx: &C,
        name: ActorMeshId,
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
        actor_mesh_id: ActorMeshId,
        params: &A::Params,
        supervision_display_name: Option<String>,
        is_system_actor: bool,
    ) -> crate::Result<ActorMesh<A>>
    where
        C::A: Handler<MeshFailure>,
    {
        let remote = Remote::collect();
        // `RemoteSpawn` + `register_spawnable!(A)` ensure that `A` has a
        // `SpawnableActor` entry in this registry, so
        // `name_of::<A>()` can resolve its global type name.
        let actor_type = remote
            .name_of::<A>()
            .ok_or(Error::ActorTypeNotRegistered(type_name::<A>().to_string()))?
            .to_string();

        let serialized_params = bincode::serde::encode_to_vec(params, bincode::config::legacy())?;
        self.proc_agent_mesh.cast(
            cx,
            resource::CreateOrUpdate::<proc_agent::ActorSpec> {
                id: actor_mesh_id.resource_id().clone(),
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
        self.proc_agent_mesh.cast(
            cx,
            resource::GetRankStatus {
                id: actor_mesh_id.resource_id().clone(),
                reply,
            },
        )?;

        let start_time = tokio::time::Instant::now();

        // Wait for all ranks to report a terminal or running status.
        // If any proc reports a failure (via supervision) or the mesh
        // times out, `wait()` returns Err with the final snapshot.
        //
        // `rx` is the accumulator output stream: each time reduced
        // overlays are applied, it emits a new StatusMesh snapshot.
        // `wait()` loops on it, deciding when the stream is
        // "complete" (no more NotExist) or times out.
        let statuses = match GetRankStatus::wait(
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
                    Ok(statuses)
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

        let actor_mesh_members = Arc::new(
            self.ranks
                .iter()
                .map(|rank| rank.actor_addr(&actor_mesh_id))
                .collect_mesh::<ValueMesh<_>>(self.region().clone())
                .map_err(|error| crate::Error::ConfigurationError(error.into()))?,
        );

        let mut mesh = ActorMesh::new(
            self.clone(),
            actor_mesh_id.clone(),
            None,
            actor_mesh_members,
        );
        // System actors are managed by their owning runtime, not an
        // ActorMeshController.
        if !is_system_actor {
            // Spawn a unique mesh manager for each actor mesh, so the type of the
            // mesh can be preserved.
            let controller: ActorMeshController<A> = ActorMeshController::new(
                ActorMeshControlPlane::new(mesh.deref().clone(), self.clone()),
                supervision_display_name.clone(),
                Some(cx.instance().port().bind()),
                statuses,
            );
            // hyperactor::proc AI-3: controller name must include mesh
            // identity for proc-wide ActorAddr uniqueness. A fixed base name alone
            // collides across parents because pid allocation is
            // parent-scoped.
            let controller_name = format!(
                "{}_{}",
                crate::mesh_controller::ACTOR_MESH_CONTROLLER_NAME,
                mesh.id()
            );
            let controller = cx.spawn_with_label(&controller_name, controller);
            // Controller and ActorMesh both depend on references from each other, break
            // the cycle by setting the controller after the fact.
            mesh.set_controller(Some(controller.bind()));
        }
        // Notify telemetry that an actor mesh was created.
        {
            let id_str = mesh.id().to_string();

            // Hash the proc mesh id for parent_mesh_id.
            let parent_mesh_id_hash = hash_to_u64(self.id());
            let mesh_id_hash = telemetry_actor_mesh_id(self.id(), mesh.id());

            hyperactor_telemetry::notify_mesh_created(hyperactor_telemetry::MeshEvent {
                id: mesh_id_hash,
                timestamp: std::time::SystemTime::now(),
                class: supervision_display_name
                    .as_deref()
                    .and_then(python_class_from_supervision_name)
                    .unwrap_or(actor_type),
                given_name: mesh
                    .id()
                    .display_label()
                    .map(|l| l.as_str())
                    .unwrap_or("unnamed")
                    .to_string(),
                full_name: id_str,
                shape_json: serde_json::to_string(&self.region().extent()).unwrap_or_default(),
                parent_mesh_id: Some(parent_mesh_id_hash),
                parent_view_json: serde_json::to_string(self.region()).ok(),
            });

            // Notify telemetry of each actor in this mesh. The rank is
            // the actor's position within the actor mesh (not the proc's
            // create_rank, which reflects the original unsliced mesh).
            let now = std::time::SystemTime::now();
            for (rank, proc_ref) in self.ranks.iter().enumerate() {
                let display_name = supervision_display_name.as_ref().map(|sdn| {
                    let point = self.region().extent().point_of_rank(rank).unwrap();
                    crate::actor_display_name(sdn, &point)
                });
                let actor_addr = proc_ref.actor_addr(&actor_mesh_id);
                hyperactor_telemetry::notify_actor_created(hyperactor_telemetry::ActorEvent {
                    id: hyperactor_telemetry::hash_to_u64(actor_addr.id()),
                    timestamp: now,
                    mesh_id: mesh_id_hash,
                    rank: rank as u64,
                    full_name: actor_addr.to_string(),
                    display_name,
                });
            }
        }

        Ok(mesh)
    }

    /// Send stop actors message to all mesh agents for a specific actor mesh id.
    #[hyperactor::instrument(fields(
        host_mesh = self.host_mesh_id().map(|id| id.to_string()),
        proc_mesh = self.id.to_string(),
        actor_mesh = actor_mesh_id.to_string(),
    ))]
    pub(crate) async fn stop_actor_by_id(
        &self,
        cx: &impl context::Actor,
        actor_mesh_id: ActorMeshId,
        reason: String,
    ) -> crate::Result<ValueMesh<Status>> {
        tracing::info!(name = "ProcMeshStatus", status = "ActorMesh::Stop::Attempt");
        tracing::info!(name = "ActorMeshStatus", status = "Stop::Attempt");
        let result = self.stop_actor_by_id_inner(cx, actor_mesh_id, reason).await;
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

    async fn stop_actor_by_id_inner(
        &self,
        cx: &impl context::Actor,
        actor_mesh_id: ActorMeshId,
        reason: String,
    ) -> crate::Result<ValueMesh<Status>> {
        let region = self.region().clone();
        self.proc_agent_mesh.cast(
            cx,
            resource::Stop {
                id: actor_mesh_id.resource_id().clone(),
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
        // Use WaitRankStatus instead of GetRankStatus so agents defer
        // their reply until the actor reaches terminal state, rather
        // than replying immediately with Stopping.
        self.proc_agent_mesh.cast(
            cx,
            resource::WaitRankStatus {
                id: actor_mesh_id.resource_id().clone(),
                min_status: Status::Stopped,
                reply: port.bind(),
            },
        )?;
        let start_time = tokio::time::Instant::now();

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
                // Check that all actors are in a terminating state (Stopping
                // or beyond). Failed is ok, because one of these actors may
                // have failed earlier and we're trying to stop the others.
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
        write!(f, "{}{{{}}}", self.id, self.region)
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
    /// Return a pure slice of this proc mesh.
    ///
    /// The returned `ProcMeshRef` contains the selected dense `ProcRef`s, and
    /// its `proc_agent_mesh` carries a lazy actor-mesh slice descriptor.
    fn sliced(&self, region: Region) -> Self {
        debug_assert!(region.is_subset(view::Ranked::region(self)));
        let ranks = self
            .region()
            .remap(&region)
            .unwrap()
            .map(|index| self.get(index).unwrap().clone())
            .collect::<Vec<_>>();

        Self {
            id: self.id.clone(),
            proc_agent_mesh: self.proc_agent_mesh.sliced(region.clone()),
            region,
            ranks: Arc::new(ranks),
            host_mesh: self.host_mesh.clone(),
        }
    }
}

/// Extract a Python class display name from a supervision display name.
///
/// The supervision display name format is `{instance}.<{module}.{ClassName} {mesh_name}>`.
/// Returns `"Python<ClassName>"` if the format matches, `None` otherwise.
///
/// Scope note: this function is used only by telemetry
/// (`MeshEvent.class`), which needs the Python class as a
/// structured string and has no structured carrier today. It is
/// not on the supervision rendering path.
///
/// TODO: retained only because the telemetry path needs a
/// structured Python-class string and this is the only available
/// source. A follow-up should add a structured carrier (e.g. an
/// `actor_class` field on `ActorSupervisionEvent`, or a dedicated
/// telemetry-side field) and delete this function.
fn python_class_from_supervision_name(sdn: &str) -> Option<String> {
    let inner = sdn.rsplit_once('<')?.1.strip_suffix('>')?;
    let qualified = inner.split_whitespace().next()?;
    let class_name = qualified.rsplit_once('.')?.1;
    Some(format!("Python<{class_name}>"))
}

#[cfg(test)]
mod tests {
    #[cfg(fbcode_build)]
    use std::ops::Deref;
    #[cfg(fbcode_build)]
    use std::time::Duration;

    #[cfg(fbcode_build)]
    use hyperactor::Instance;
    #[cfg(fbcode_build)]
    use hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER;
    #[cfg(fbcode_build)]
    use ndslice::ViewExt as _;
    #[cfg(fbcode_build)]
    use ndslice::extent;
    #[cfg(fbcode_build)]
    use timed_test::assert_no_process_leak;
    #[cfg(fbcode_build)]
    use timed_test::async_timed_test;
    #[cfg(fbcode_build)]
    use uuid::Uuid;

    #[cfg(fbcode_build)]
    use crate::ActorMesh;
    #[cfg(fbcode_build)]
    use crate::comm::ENABLE_NATIVE_V1_CASTING;
    #[cfg(fbcode_build)]
    use crate::host_mesh::PROC_SPAWN_MAX_IDLE;
    #[cfg(fbcode_build)]
    use crate::resource::RankedValues;
    #[cfg(fbcode_build)]
    use crate::resource::Status;
    #[cfg(fbcode_build)]
    use crate::testactor;
    #[cfg(fbcode_build)]
    use crate::testing;

    #[cfg(fbcode_build)]
    async fn execute_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor_telemetry::DefaultTelemetryClock {});

        let instance = testing::instance();

        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 1), None, None)
            .await
            .unwrap();
        let actor_mesh = proc_mesh.spawn(instance, "test", &()).await.unwrap();
        testactor::assert_mesh_shape(actor_mesh).await;

        let _ = hm.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 120)]
    #[cfg(fbcode_build)]
    async fn test_spawn_actor_v1_casting() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        let _guard3 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(120));
        let _guard4 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(120),
        );
        execute_spawn_actor().await;
    }

    #[async_timed_test(timeout_secs = 120)]
    #[cfg(fbcode_build)]
    async fn test_spawn_actor_v1_casting_p2p() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        let _guard3 = config.override_key(crate::config::V1_CAST_POINT_TO_POINT_THRESHOLD, 1024);
        let _guard4 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(120));
        let _guard5 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(120),
        );
        execute_spawn_actor().await;
    }

    #[async_timed_test(timeout_secs = 120)]
    #[cfg(fbcode_build)]
    async fn test_spawn_actor_v0_casting() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, false);
        let _guard2 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(120));
        let _guard3 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(120),
        );
        execute_spawn_actor().await;
    }

    /// Spawn an actor mesh, then do a random number of casts to bump the seq
    /// numbers for all actors participating in the cast. This avoids the test
    /// mistakenly passing.
    #[cfg(fbcode_build)]
    async fn spawn_for_seq_test(
        cx: &Instance<testing::TestRootClient>,
        proc_mesh: &super::ProcMeshRef,
    ) -> ActorMesh<testactor::TestActor> {
        let actor_mesh: ActorMesh<testactor::TestActor> =
            proc_mesh.spawn(cx, "test", &()).await.unwrap();

        let instance = cx
            .proc()
            .client(&format!("random_casts_{}", Uuid::now_v7()));
        let n = 1;
        for _ in 0..n {
            actor_mesh.cast(&instance, ()).unwrap();
        }
        println!(
            "did {} casts with sequencer session id {}",
            n,
            instance.sequencer().session_id()
        );
        actor_mesh
    }

    #[async_timed_test(timeout_secs = 60)]
    #[cfg(fbcode_build)]
    async fn test_seq_from_same_sender_to_different_meshes() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        let _guard3 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _guard4 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        hyperactor_telemetry::initialize_logging_for_test();
        let instance = testing::instance();
        let session_id = instance.sequencer().session_id();

        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 1), None, None)
            .await
            .unwrap();
        let proc_mesh_ref = proc_mesh.deref();

        // Sequence numbers are scoped based on the (client, dest) pair.
        // So casts to different meshes from the same client instance would
        // result in seq 1 for all casts.
        let handles = (0..3)
            .map(|_| {
                let proc_mesh_ref_clone = proc_mesh_ref.clone();
                tokio::spawn(async move {
                    let actor_mesh = spawn_for_seq_test(instance, &proc_mesh_ref_clone).await;
                    let expected_seqs = vec![1; 2];
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

        let _ = hm.shutdown(instance).await;
    }

    /// Verify that the seq numbers are assigned correctly when we cast to
    /// different views of the same root mesh.
    #[async_timed_test(timeout_secs = 60)]
    #[cfg(fbcode_build)]
    async fn test_seq_from_same_sender_to_different_views() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        let _guard3 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _guard4 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        hyperactor_telemetry::initialize_logging_for_test();

        let instance = testing::instance();
        let session_id = instance.sequencer().session_id();

        let mut hm = testing::host_mesh(3).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 1), None, None)
            .await
            .unwrap();

        let actor_mesh = spawn_for_seq_test(instance, &proc_mesh).await;

        // First cast. The seq should be 1 for all actors.
        let expected_seqs = vec![1; 3];
        testactor::assert_casting_correctness(
            &actor_mesh,
            instance,
            Some((session_id, expected_seqs)),
        )
        .await;

        // Verify casting to the sliced actor mesh
        let sliced_actor_mesh = actor_mesh.range("hosts", 1..3).unwrap();
        // Second cast. The seq should be 2 for actors in the sliced mesh.
        let expected_seqs = vec![2; 2];
        testactor::assert_casting_correctness(
            &sliced_actor_mesh,
            instance,
            Some((session_id, expected_seqs)),
        )
        .await;

        // Verify casting to a different sliced actor mesh
        let sliced_actor_mesh = actor_mesh.range("hosts", 0..2).unwrap();
        // For actors in the previous sliced mesh, the seq should be 3 since
        // this is the third cast for them. For other actors, the seq should
        // be 2.
        let expected_seqs = vec![2, 3];
        testactor::assert_casting_correctness(
            &sliced_actor_mesh,
            instance,
            Some((session_id, expected_seqs)),
        )
        .await;

        let _ = hm.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    #[cfg(fbcode_build)]
    async fn test_seq_from_different_senders() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(ENABLE_NATIVE_V1_CASTING, true);
        let _guard2 = config.override_key(ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);
        let _guard3 = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _guard4 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        hyperactor_telemetry::initialize_logging_for_test();

        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;

        let proc = Proc::direct(ChannelTransport::Unix.any(), "test_0".to_string()).unwrap();
        let instance = proc
            .actor_instance::<testing::TestRootClient>("test_client")
            .unwrap()
            .instance;
        let first_instance = proc
            .actor_instance::<testing::TestRootClient>("first_client")
            .unwrap()
            .instance;
        let second_instance = proc
            .actor_instance::<testing::TestRootClient>("second_client")
            .unwrap()
            .instance;
        let third_instance = proc
            .actor_instance::<testing::TestRootClient>("third_client")
            .unwrap()
            .instance;

        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 1), None, None)
            .await
            .unwrap();

        let actor_mesh = spawn_for_seq_test(&instance, &proc_mesh).await;

        // Sequence numbers are calculated based on the sequencer, i.e. the
        // client name. So three casts would result in seq 1 for all actors.
        for inst in [&first_instance, &second_instance, &third_instance] {
            let expected_seqs = vec![1; 2];
            let session_id = inst.sequencer().session_id();
            testactor::assert_casting_correctness(
                &actor_mesh,
                inst,
                Some((session_id, expected_seqs)),
            )
            .await;
        }

        let _ = hm.shutdown(&instance).await;
    }

    #[cfg(fbcode_build)]
    #[assert_no_process_leak]
    #[tokio::test]
    async fn test_failing_spawn_actor() {
        hyperactor_telemetry::initialize_logging(hyperactor_telemetry::DefaultTelemetryClock {});

        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _guard2 = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        let instance = testing::instance();

        let mut hm = testing::host_mesh(1).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 1), None, None)
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
            RankedValues::from((0..1, Status::Failed("test failure".to_string()))),
        );

        let _ = hm.shutdown(instance).await;
    }

    #[async_timed_test(timeout_secs = 60)]
    #[cfg(fbcode_build)]
    async fn test_spawn_actor_on_proc_mesh_slice_only_spawns_slice_members() {
        let instance = testing::instance();

        let mut hm = testing::host_mesh(2).await;
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 2), None, None)
            .await
            .unwrap();
        let host1 = proc_mesh.range("hosts", 1..2).unwrap();
        let actor_name = crate::mesh_id::ActorMeshId::instance(
            hyperactor::id::Label::new("slice_only").unwrap(),
        );

        let actor_mesh = host1
            .spawn_with_name::<testactor::TestActor, _>(
                instance,
                actor_name.clone(),
                &(),
                None,
                false,
            )
            .await
            .unwrap();
        testactor::assert_casting_correctness(&actor_mesh, instance, None).await;

        let slice_states = host1
            .actor_states(instance, actor_name.clone())
            .await
            .unwrap();
        assert_eq!(slice_states.extent(), host1.extent());

        let err = proc_mesh
            .actor_states(instance, actor_name.clone())
            .await
            .unwrap_err();
        let expected_name = actor_name.into();
        match err {
            crate::Error::NotExist(name) if name == expected_name => {}
            other => panic!("expected NotExist for {expected_name}, got {other:?}"),
        }

        let _ = hm.shutdown(instance).await;
    }

    /// `proc_states` must resolve exactly the procs of the (possibly sliced)
    /// mesh it is called on — gpu-dim slices, host-dim slices, and
    /// slice-of-slice — rather than re-deriving a dense `0..n` from per-host
    /// counts or from the recipient's stamped (sliced-ordinal) rank.
    #[cfg(fbcode_build)]
    async fn assert_proc_states_match_slice<C: hyperactor::context::Actor>(
        mesh: &super::ProcMeshRef,
        cx: &C,
    ) {
        // The slice's own procs, by their original global rank.
        let mut expected: Vec<usize> = mesh.ranks.iter().map(|r| r.create_rank).collect();
        expected.sort();

        let states = mesh
            .states(cx, None)
            .await
            .unwrap()
            .expect("host-backed mesh yields Some");

        assert_eq!(states.extent(), mesh.extent());

        let mut got: Vec<usize> = states
            .values()
            .map(|s| {
                assert!(
                    !matches!(
                        s.status,
                        Status::NotExist | Status::Failed(_) | Status::Timeout(_)
                    ),
                    "rank should resolve to a live proc, got status {:?}",
                    s.status
                );
                s.state.expect("live proc carries ProcState").create_rank
            })
            .collect();

        got.sort();

        assert_eq!(
            got, expected,
            "proc_states must return exactly this (sub)mesh's procs"
        );
    }

    #[async_timed_test(timeout_secs = 60)]
    #[cfg(fbcode_build)]
    async fn test_proc_states_on_sliced_mesh() {
        let instance = testing::instance();
        let mut hm = testing::host_mesh(2).await;
        // 2 hosts x 4 gpus, host-major: global rank = host * 4 + gpu.
        let proc_mesh = hm
            .spawn(&instance, "test", extent!(gpus = 4), None, None)
            .await
            .unwrap();

        // Full mesh: ranks 0..8.
        assert_proc_states_match_slice(&proc_mesh, instance).await;
        // gpu-dim slice (gpus 2..4): ranks {2,3,6,7} — the original mis-derivation.
        assert_proc_states_match_slice(&proc_mesh.range("gpus", 2..4).unwrap(), instance).await;
        // host-dim slice (host 1): ranks {4,5,6,7} — the stamped-ordinal trap.
        assert_proc_states_match_slice(&proc_mesh.range("hosts", 1..2).unwrap(), instance).await;
        // slice-of-slice (host 1, gpus 2..4): ranks {6,7} — pins base-rank composition.
        assert_proc_states_match_slice(
            &proc_mesh
                .range("gpus", 2..4)
                .unwrap()
                .range("hosts", 1..2)
                .unwrap(),
            instance,
        )
        .await;

        let _ = hm.shutdown(instance).await;
    }

    #[test]
    fn test_python_class_from_supervision_name() {
        use super::python_class_from_supervision_name;

        assert_eq!(
            python_class_from_supervision_name("instance0.<my_module.MyWorker test_mesh>"),
            Some("Python<MyWorker>".to_string()),
        );
        assert_eq!(
            python_class_from_supervision_name(
                "instance0.<package.submodule.TrainingActor mesh_0>"
            ),
            Some("Python<TrainingActor>".to_string()),
        );
        // No angle brackets — not a Python supervision name.
        assert_eq!(python_class_from_supervision_name("plain_name"), None,);
        // Malformed: missing dot-qualified class name.
        assert_eq!(
            python_class_from_supervision_name("instance0.<NoModule mesh>"),
            None,
        );
    }
}
