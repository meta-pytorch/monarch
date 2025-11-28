/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! System actor manages a system.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::SystemTime;

use async_trait::async_trait;
use dashmap::DashMap;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::PortHandle;
use hyperactor::PortRef;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::WorldId;
use hyperactor::actor::Handler;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::sim::SimAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::ClockKind;
use hyperactor::id;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MailboxSenderError;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::PortSender;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::mailbox_admin_message::MailboxAdminMessage;
use hyperactor::mailbox::monitored_return_handle;
use hyperactor::proc::Proc;
use hyperactor::reference::Index;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use tokio::time::Instant;

use super::proc_actor::ProcMessage;
use crate::proc_actor::Environment;
use crate::proc_actor::ProcActor;
use crate::proc_actor::ProcStopResult;
use crate::supervision::ProcStatus;
use crate::supervision::ProcSupervisionMessage;
use crate::supervision::ProcSupervisionState;
use crate::supervision::WorldSupervisionMessage;
use crate::supervision::WorldSupervisionState;

/// A snapshot of a single proc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorldSnapshotProcInfo {
    /// The labels of the proc.
    pub labels: HashMap<String, String>,
}

impl From<&ProcInfo> for WorldSnapshotProcInfo {
    fn from(proc_info: &ProcInfo) -> Self {
        Self {
            labels: proc_info.labels.clone(),
        }
    }
}

/// A snapshot view of a world in the system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorldSnapshot {
    /// The host procs used to spawn procs in this world. Some caveats:
    ///   1. The host procs are actually not in this world but in a different
    ///      "shadow" world. The shadow world's ID can be told from the host
    ///      ProcId.
    ///   2. Not all host procs are captured here. This field only captures the
    ///      hosts that joined before the world were created.
    pub host_procs: HashSet<ProcId>,

    /// The procs in this world.
    pub procs: HashMap<ProcId, WorldSnapshotProcInfo>,

    /// The status of the world.
    pub status: WorldStatus,

    /// Labels attached to this world. They can be used later to query
    /// world(s) using system snapshot api.
    pub labels: HashMap<String, String>,
}

impl WorldSnapshot {
    fn from_world_filtered(world: &World, filter: &SystemSnapshotFilter) -> Self {
        WorldSnapshot {
            host_procs: world.state.host_map.keys().map(|h| &h.0).cloned().collect(),
            procs: world
                .state
                .procs
                .iter()
                .map_while(|(k, v)| filter.proc_matches(v).then_some((k.clone(), v.into())))
                .collect(),
            status: world.state.status.clone(),
            labels: world.labels.clone(),
        }
    }
}

/// A snapshot view of the system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named)]
pub struct SystemSnapshot {
    /// Snapshots of all the worlds in this system.
    pub worlds: HashMap<WorldId, WorldSnapshot>,
    /// Execution ID of the system.
    pub execution_id: String,
}

/// A filter used to filter the snapshot view of the system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named, Default)]
pub struct SystemSnapshotFilter {
    /// The world ids to filter. Empty list matches all.
    pub worlds: Vec<WorldId>,
    /// World labels to filter. Empty matches all.
    pub world_labels: HashMap<String, String>,
    /// Proc labels to filter. Empty matches all.
    pub proc_labels: HashMap<String, String>,
}

impl SystemSnapshotFilter {
    /// Create an empty filter that matches everything.
    pub fn all() -> Self {
        Self {
            worlds: Vec::new(),
            world_labels: HashMap::new(),
            proc_labels: HashMap::new(),
        }
    }

    /// Whether the filter matches the given world.
    fn world_matches(&self, world: &World) -> bool {
        if !self.worlds.is_empty() && !self.worlds.contains(&world.world_id) {
            return false;
        }
        Self::labels_match(&self.world_labels, &world.labels)
    }

    fn proc_matches(&self, proc_info: &ProcInfo) -> bool {
        Self::labels_match(&self.proc_labels, &proc_info.labels)
    }

    /// Whether the filter matches the given proc labels.
    fn labels_match(
        filter_labels: &HashMap<String, String>,
        labels: &HashMap<String, String>,
    ) -> bool {
        filter_labels.is_empty()
            || filter_labels
                .iter()
                .all(|(k, v)| labels.contains_key(k) && labels.get(k).unwrap() == v)
    }
}

/// Update the states of worlds, specifically checking if they are unhealthy.
/// Evict the world if it is unhealthy for too long.
#[derive(Debug, Clone, PartialEq)]
struct MaintainWorldHealth;

/// The proc's lifecyle management mode.
#[derive(Named, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProcLifecycleMode {
    /// Proc is detached, its lifecycle isn't managed by the system.
    Detached,
    /// Proc's lifecycle is managed by the system, supervision is enabled for the proc.
    ManagedBySystem,
    /// The proc manages the lifecyle of the system, supervision is enabled for the proc.
    /// System goes down when the proc stops.
    ManagingSystem,
}

impl ProcLifecycleMode {
    /// Whether the lifecycle mode indicates whether proc is managed by/managing system or not.
    pub fn is_managed(&self) -> bool {
        matches!(
            self,
            ProcLifecycleMode::ManagedBySystem | ProcLifecycleMode::ManagingSystem
        )
    }
}

/// System messages.
#[derive(
    hyperactor::Handler,
    HandleClient,
    RefClient,
    Named,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq
)]
pub enum SystemMessage {
    /// Join the system at the given proc id.
    Join {
        /// The world that is being joined.
        world_id: WorldId,
        /// The proc id that is joining.
        proc_id: ProcId,
        /// Reference to the proc actor managing the proc.
        proc_message_port: PortRef<ProcMessage>,
        /// The channel address used to communicate with the proc.
        proc_addr: ChannelAddr,
        /// Arbitrary name/value pairs that can be used to identify the proc.
        labels: HashMap<String, String>,
        /// The lifecyle mode of the proc.
        lifecycle_mode: ProcLifecycleMode,
    },

    /// Create a new world or update an existing world.
    UpsertWorld {
        /// The world id.
        world_id: WorldId,
        /// The shape of the world.
        shape: Shape,
        /// The number of procs per host.
        num_procs_per_host: usize,
        /// How to spawn procs in the world.
        env: Environment,
        /// Arbitrary name/value pairs that can be used to identify the world.
        labels: HashMap<String, String>,
    },

    /// Return a snapshot view of this system. Used for debugging.
    #[log_level(debug)]
    Snapshot {
        /// The filter used to filter the snapshot view.
        filter: SystemSnapshotFilter,
        /// Used to return the snapshot view to the caller.
        #[reply]
        ret: OncePortRef<SystemSnapshot>,
    },

    /// Start the shutdown process of everything in this system. It tries to
    /// shutdown all the procs first, and then the system actor itself.
    ///
    /// Note this shutdown sequence is best effort, yet not guaranteed. It is
    /// possible the system actor/proc might already stop, while the remote
    /// procs are still in the middle of shutting down.
    Stop {
        /// List of worlds to stop. If provided, only the procs belonging to
        /// the list of worlds are stopped, otherwise all worlds are stopped
        /// including the system proc itself.
        worlds: Option<Vec<WorldId>>,
        /// The timeout used by ProcActor to stop the proc.
        proc_timeout: Duration,
        /// Used to return success to the caller.
        reply_port: OncePortRef<()>,
    },
}

/// Errors that can occur inside a system actor.
#[derive(thiserror::Error, Debug)]
pub enum SystemActorError {
    /// A proc is trying to join before a world is created
    #[error("procs cannot join uncreated world {0}")]
    UnknownWorldId(WorldId),

    /// Spawn procs failed
    #[error("failed to spawn procs")]
    SpawnProcsFailed(#[from] MailboxSenderError),

    /// Host ID does not start with valid prefix.
    #[error("invalid host {0}: does not start with prefix '{SHADOW_PREFIX}'")]
    InvalidHostPrefix(HostId),

    /// A host is trying to join the world which already has a joined host with the same ID.
    #[error("host ID {0} already exists in world")]
    DuplicatedHostId(HostId),

    /// Trying to get the actor ref for a host that doesn't exist in a world.
    #[error("host {0} does not exist in world")]
    HostNotExist(HostId),
}

/// TODO: add missing doc
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Shape {
    /// A definite N-dimensional shape of the world, the semantics of the shape can be defined
    /// by the user (TODO: implement this), e.g. in a shape like [3, 2, 2], user will be able
    /// to express things like dim 0: ai zone, dim 1: rack, dim 2: host.
    Definite(Vec<usize>),
    /// Shape is unknown.
    Unknown,
}

/// TODO: Toss this world implementation away once we have
/// a more clearly defined allocation API.
/// Currently, each world in a system has two worlds beneath:
/// the actual world and the shadow world. The shadow world
/// is a world that is used to maintain hosts which in turn
/// spawn procs for the world.
/// This is needed in order to support the current scheduler implementation
/// which does not support per-proc scheduling.
///
/// That means, each host is a proc in the shadow world. Each host proc spawns
/// a number of procs for the actual world.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct World {
    /// The world id.
    world_id: WorldId,
    /// TODO: add misssing doc
    scheduler_params: SchedulerParams,
    /// Artbitrary labels attached to the world.
    labels: HashMap<String, String>,
    /// The state of the world.
    state: WorldState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Host {
    num_procs_assigned: usize,
    proc_message_port: PortRef<ProcMessage>,
    host_rank: usize,
}

impl Host {
    fn new(proc_message_port: PortRef<ProcMessage>, host_rank: usize) -> Self {
        Self {
            num_procs_assigned: 0,
            proc_message_port,
            host_rank,
        }
    }

    fn get_assigned_procs(
        &mut self,
        world_id: &WorldId,
        scheduler_params: &mut SchedulerParams,
    ) -> Vec<ProcId> {
        // Get Host from hosts given host_id else return empty vec
        let mut proc_ids = Vec::new();

        // The number of hosts that will be assigned a total of scheduler_params.num_procs_per_host
        // procs. If scheduler_params.num_procs() is 31 and scheduler_params.num_procs_per_host is 8,
        // then num_saturated_hosts == 3 even though total number of hosts will be 4.
        let num_saturated_hosts =
            scheduler_params.num_procs() / scheduler_params.num_procs_per_host;
        // If num_saturated_hosts is less than total hosts, then the final host_rank will be equal
        // to num_saturated_hosts, and should not be assigned the full scheduler_params.num_procs_per_host.
        // Instead, we should only assign the remaining procs. So if num_procs is 31, num_procs_per_host is 8,
        // then host_rank 3 should only be assigned 7 procs.
        let num_scheduled = if self.host_rank == num_saturated_hosts {
            scheduler_params.num_procs() % scheduler_params.num_procs_per_host
        } else {
            scheduler_params.num_procs_per_host
        };

        scheduler_params.num_procs_scheduled += num_scheduled;

        for _ in 0..num_scheduled {
            // Compute each proc id (which will become the RANK env var on each worker)
            // based on host_rank, which is (optionally) assigned to each host at bootstrap
            // time according to a sorted hostname file.
            //
            // More precisely, when a host process starts up, it gets its host rank from some
            // global source of truth common to all host nodes. This source of truth could be
            // a file or an env var. In order to be consistent with the SPMD world, assuming
            // num_procs_per_host == N, we would want worker ranks 0 through N-1 on host 0;
            // ranks N through 2N-1 on host 1; etc. So, for host H, we assign proc ids in the
            // interval [H*N, (H+1)*N).
            let rank =
                self.host_rank * scheduler_params.num_procs_per_host + self.num_procs_assigned;
            let proc_id = ProcId::Ranked(world_id.clone(), rank);
            proc_ids.push(proc_id);
            self.num_procs_assigned += 1;
        }

        proc_ids
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchedulerParams {
    shape: Shape,
    num_procs_scheduled: usize,
    num_procs_per_host: usize,
    next_rank: Index,
    env: Environment,
}

impl SchedulerParams {
    fn num_procs(&self) -> usize {
        match &self.shape {
            Shape::Definite(v) => v.iter().product(),
            Shape::Unknown => unimplemented!(),
        }
    }
}

/// A world id that is used to identify a host.
pub type HostWorldId = WorldId;
static SHADOW_PREFIX: &str = "host";

/// A host id that is used to identify a host.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord
)]
pub struct HostId(ProcId);
impl HostId {
    /// Creates a new HostId from a proc_id.
    pub fn new(proc_id: ProcId) -> Result<Self, anyhow::Error> {
        if !proc_id
            .world_name()
            .expect("proc must be ranked for world_name check")
            .starts_with(SHADOW_PREFIX)
        {
            anyhow::bail!(
                "proc_id {} is not a valid HostId because it does not start with {}",
                proc_id,
                SHADOW_PREFIX
            )
        }
        Ok(Self(proc_id))
    }
}

impl TryFrom<ProcId> for HostId {
    type Error = anyhow::Error;

    fn try_from(proc_id: ProcId) -> Result<Self, anyhow::Error> {
        if !proc_id
            .world_name()
            .expect("proc must be ranked for world_name check")
            .starts_with(SHADOW_PREFIX)
        {
            anyhow::bail!(
                "proc_id {} is not a valid HostId because it does not start with {}",
                proc_id,
                SHADOW_PREFIX
            )
        }
        Ok(Self(proc_id))
    }
}

impl std::fmt::Display for HostId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

type HostMap = HashMap<HostId, Host>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ProcInfo {
    port_ref: PortRef<ProcMessage>,
    labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct WorldState {
    host_map: HostMap,
    procs: HashMap<ProcId, ProcInfo>,
    status: WorldStatus,
}

/// A world status represents the different phases of a world.
#[derive(Debug, Clone, Serialize, Deserialize, EnumAsInner, PartialEq)]
pub enum WorldStatus {
    /// Waiting for the world to be created. Accumulate joined hosts or procs while we're waiting.
    AwaitingCreation,

    /// World is created and enough procs based on the scheduler parameter.
    /// All procs in the world are without failures.
    Live,

    /// World is created but it does not have enough procs or some procs are failing.
    /// [`SystemTime`] contains the time when the world became unhealthy.
    // Use SystemTime instead of Instant to avoid the issue of serialization.
    Unhealthy(SystemTime),
}

impl Display for WorldStatus {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            WorldStatus::AwaitingCreation => write!(f, "Awaiting Creation"),
            WorldStatus::Live => write!(f, "Live"),
            WorldStatus::Unhealthy(_) => write!(f, "Unhealthy"),
        }
    }
}

impl WorldState {
    /// Gets the mutable ref to host_map.
    fn get_host_map_mut(&mut self) -> &mut HostMap {
        &mut self.host_map
    }

    /// Gets the ref to host_map.
    fn get_host_map(&self) -> &HostMap {
        &self.host_map
    }
}

impl World {
    fn new(
        world_id: WorldId,
        shape: Shape,
        state: WorldState,
        num_procs_per_host: usize,
        env: Environment,
        labels: HashMap<String, String>,
    ) -> Result<Self, anyhow::Error> {
        if world_id.name().starts_with(SHADOW_PREFIX) {
            anyhow::bail!(
                "world name {} cannot start with {}!",
                world_id,
                SHADOW_PREFIX
            )
        }
        tracing::info!("creating world {}", world_id,);
        Ok(Self {
            world_id,
            scheduler_params: SchedulerParams {
                shape,
                num_procs_per_host,
                num_procs_scheduled: 0,
                next_rank: 0,
                env,
            },
            state,
            labels,
        })
    }

    fn get_real_world_id(proc_world_id: &WorldId) -> WorldId {
        WorldId(
            proc_world_id
                .name()
                .strip_prefix(SHADOW_PREFIX)
                .unwrap_or(proc_world_id.name())
                .to_string(),
        )
    }

    fn is_host_world(world_id: &WorldId) -> bool {
        world_id.name().starts_with(SHADOW_PREFIX)
    }

    fn get_port_ref_from_host(
        &self,
        host_id: &HostId,
    ) -> Result<PortRef<ProcMessage>, SystemActorError> {
        let host_map = self.state.get_host_map();
        // Get Host from hosts given proc_id
        match host_map.get(host_id) {
            Some(h) => Ok(h.proc_message_port.clone()),
            None => Err(SystemActorError::HostNotExist(host_id.clone())),
        }
    }

    /// Adds procs to the world.
    fn add_proc(
        &mut self,
        proc_id: ProcId,
        proc_message_port: PortRef<ProcMessage>,
        labels: HashMap<String, String>,
    ) -> Result<(), SystemActorError> {
        self.state.procs.insert(
            proc_id,
            ProcInfo {
                port_ref: proc_message_port,
                labels,
            },
        );
        if self.state.status.is_unhealthy()
            && self.state.procs.len() >= self.scheduler_params.num_procs()
        {
            self.state.status = WorldStatus::Live;
            tracing::info!(
                "world {}: ready to serve with {} procs",
                self.world_id,
                self.state.procs.len()
            );
        }
        Ok(())
    }

    /// 1. Adds a host to the hosts map.
    /// 2. Create executor procs for the host.
    /// 3. Run necessary programs
    async fn on_host_join(
        &mut self,
        host_id: HostId,
        proc_message_port: PortRef<ProcMessage>,
        router: &DialMailboxRouter,
    ) -> Result<(), SystemActorError> {
        let mut host_entry = match self.state.host_map.entry(host_id.clone()) {
            Entry::Occupied(_) => {
                return Err(SystemActorError::DuplicatedHostId(host_id));
            }
            Entry::Vacant(entry) => entry.insert_entry(Host::new(
                proc_message_port.clone(),
                host_id
                    .0
                    .rank()
                    .expect("host proc must be ranked for rank access"),
            )),
        };

        if self.state.status == WorldStatus::AwaitingCreation {
            return Ok(());
        }

        let proc_ids = host_entry
            .get_mut()
            .get_assigned_procs(&self.world_id, &mut self.scheduler_params);

        router.serialize_and_send(
            &proc_message_port,
            ProcMessage::SpawnProc {
                env: self.scheduler_params.env.clone(),
                world_id: self.world_id.clone(),
                proc_ids,
                world_size: self.scheduler_params.num_procs(),
            },
            monitored_return_handle(),
        )?;
        Ok(())
    }

    fn get_hosts_to_procs(&mut self) -> Result<HashMap<HostId, Vec<ProcId>>, SystemActorError> {
        // A map from host ID to scheduled proc IDs on this host.
        let mut host_proc_map: HashMap<HostId, Vec<ProcId>> = HashMap::new();
        let host_map = self.state.get_host_map_mut();
        // Iterate over each entry in self.hosts
        for (host_id, host) in host_map {
            // Had to clone hosts in order to call schedule_procs
            if host.num_procs_assigned == self.scheduler_params.num_procs_per_host {
                continue;
            }
            let host_procs = host.get_assigned_procs(&self.world_id, &mut self.scheduler_params);
            if host_procs.is_empty() {
                continue;
            }
            host_proc_map.insert(host_id.clone(), host_procs);
        }
        Ok(host_proc_map)
    }

    async fn on_create(&mut self, router: &DialMailboxRouter) -> Result<(), anyhow::Error> {
        let host_procs_map = self.get_hosts_to_procs()?;
        for (host_id, procs_ids) in host_procs_map {
            if procs_ids.is_empty() {
                continue;
            }

            // REFACTOR(marius): remove
            let world_id = procs_ids
                .first()
                .unwrap()
                .clone()
                .into_ranked()
                .expect("proc must be ranked for world_id access")
                .0
                .clone();
            // Open port ref
            tracing::info!("spawning procs for host {:?}", host_id);
            router.serialize_and_send(
                // Get host proc!
                &self.get_port_ref_from_host(&host_id)?,
                ProcMessage::SpawnProc {
                    env: self.scheduler_params.env.clone(),
                    world_id,
                    // REFACTOR(marius): remove
                    proc_ids: procs_ids,
                    world_size: self.scheduler_params.num_procs(),
                },
                monitored_return_handle(),
            )?;
        }
        Ok(())
    }
}

/// A mailbox router that forwards messages to their destinations and
/// additionally reports the destination address back to the sender’s
/// [`ProcActor`], allowing it to cache the address for future use.
#[derive(Debug, Clone)]
pub struct ReportingRouter {
    router: DialMailboxRouter,
    /// A record of cached addresses from dst_proc_id to HashSet(src_proc_id)
    /// Right now only the proc_ids are recorded for updating purpose.
    /// We can also cache the address here in the future.
    address_cache: Arc<DashMap<ProcId, HashSet<ProcId>>>,
}

impl MailboxSender for ReportingRouter {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let ReportingRouter { router, .. } = self;
        self.post_update_address(&envelope);
        router.post_unchecked(envelope, return_handle);
    }
}

impl ReportingRouter {
    fn new() -> Self {
        Self {
            router: DialMailboxRouter::new(),
            address_cache: Arc::new(DashMap::new()),
        }
    }
    fn post_update_address(&self, envelope: &MessageEnvelope) {
        let system_proc_id = id!(system[0]);
        // These are edge cases that are at unlikely to come up in a
        // well ordered system but in the event that they do we skip
        // sending update address messages:
        // - The sender ID is "unknown" (it makes no sense to remember
        //   the address of an unknown sender)
        // - The sender world is "user", which doesn't have a ProcActor running
        //   to process the address update message.
        // - The sender is the system (the system knows all addresses)
        // - The destination is the system (every proc knows the
        //   system address)
        // - The sender and the destination are on the same proc (it
        //   doesn't make sense to be dialing connections between them).
        if envelope.sender().proc_id() == &id!(unknown[0])
            || envelope.sender().proc_id().world_id() == Some(&id!(user))
            || envelope.sender().proc_id() == &system_proc_id
            || envelope.dest().actor_id().proc_id() == &system_proc_id
            || envelope.sender().proc_id() == envelope.dest().actor_id().proc_id()
        {
            return;
        }
        let (dst_proc_id, dst_proc_addr) = self.dest_proc_id_and_address(envelope);
        let Some(dst_proc_addr) = dst_proc_addr else {
            tracing::warn!("unknown address for {}", &dst_proc_id);
            return;
        };

        let sender_proc_id = envelope.sender().proc_id();
        self.upsert_address_cache(sender_proc_id, &dst_proc_id);
        // Sim addresses have a concept of directionality. When we notify a proc of an address we should
        // use the proc's address as the source for the sim address.
        let sender_address = self.router.lookup_addr(envelope.sender());
        let dst_proc_addr =
            if let (Some(ChannelAddr::Sim(sender_sim_addr)), ChannelAddr::Sim(dest_sim_addr)) =
                (sender_address, &dst_proc_addr)
            {
                ChannelAddr::Sim(
                    SimAddr::new_with_src(
                        // source is the sender
                        sender_sim_addr.addr().clone(),
                        // dest remains unchanged
                        dest_sim_addr.addr().clone(),
                    )
                    .unwrap(),
                )
            } else {
                dst_proc_addr
            };
        self.serialize_and_send(
            &self.proc_port_ref(sender_proc_id),
            MailboxAdminMessage::UpdateAddress {
                proc_id: dst_proc_id,
                addr: dst_proc_addr,
            },
            monitored_return_handle(),
        )
        .expect("unexpected serialization failure")
    }

    /// broadcasts the address of the proc if there's any stale record that has been sent
    /// out to senders before.
    fn broadcast_addr(&self, dst_proc_id: &ProcId, dst_proc_addr: ChannelAddr) {
        if let Some(r) = self.address_cache.get(dst_proc_id) {
            for sender_proc_id in r.value() {
                tracing::info!(
                    "broadcasting address change to {} for {}: {}",
                    sender_proc_id,
                    dst_proc_id,
                    dst_proc_addr
                );
                self.serialize_and_send(
                    &self.proc_port_ref(sender_proc_id),
                    MailboxAdminMessage::UpdateAddress {
                        proc_id: dst_proc_id.clone(),
                        addr: dst_proc_addr.clone(),
                    },
                    monitored_return_handle(),
                )
                .expect("unexpected serialization failure")
            }
        }
    }

    fn upsert_address_cache(&self, src_proc_id: &ProcId, dst_proc_id: &ProcId) {
        self.address_cache
            .entry(dst_proc_id.clone())
            .and_modify(|src_proc_ids| {
                src_proc_ids.insert(src_proc_id.clone());
            })
            .or_insert({
                let mut set = HashSet::new();
                set.insert(src_proc_id.clone());
                set
            });
    }

    fn dest_proc_id_and_address(
        &self,
        envelope: &MessageEnvelope,
    ) -> (ProcId, Option<ChannelAddr>) {
        let dest_proc_port_id = envelope.dest();
        let dest_proc_actor_id = dest_proc_port_id.actor_id();
        let dest_proc_id = dest_proc_actor_id.proc_id();
        let dest_proc_addr = self.router.lookup_addr(dest_proc_actor_id);
        (dest_proc_id.clone(), dest_proc_addr)
    }

    fn proc_port_ref(&self, proc_id: &ProcId) -> PortRef<MailboxAdminMessage> {
        let proc_actor_id = ActorId(proc_id.clone(), "proc".to_string(), 0);
        let proc_actor_ref = ActorRef::<ProcActor>::attest(proc_actor_id);
        proc_actor_ref.port::<MailboxAdminMessage>()
    }
}

/// TODO: add misssing doc
#[derive(Debug, Clone)]
pub struct SystemActorParams {
    mailbox_router: ReportingRouter,

    /// The duration to declare an actor dead if no supervision update received.
    supervision_update_timeout: Duration,

    /// The duration to evict an unhealthy world, after which a world fails supervision states.
    world_eviction_timeout: Duration,
}

impl SystemActorParams {
    /// Create a new system actor params.
    pub fn new(supervision_update_timeout: Duration, world_eviction_timeout: Duration) -> Self {
        Self {
            mailbox_router: ReportingRouter::new(),
            supervision_update_timeout,
            world_eviction_timeout,
        }
    }
}

/// A map of all alive procs with their proc ids as the key, the value is the supervision info of this proc.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SystemSupervisionState {
    // A map from world id to world supervision state.
    supervision_map: HashMap<WorldId, WorldSupervisionInfo>,
    // Supervision expiration duration.
    supervision_update_timeout: Duration,
}

// Used to record when procs sent their last heartbeats.
#[derive(Debug, Clone, Default)]
struct HeartbeatRecord {
    // This index is used to efficiently find expired procs.
    // T208419148: Handle btree_index initialization during system actor recovery
    btree_index: BTreeSet<(Instant, ProcId)>,

    // Last time when proc was updated.
    proc_last_update_time: HashMap<ProcId, Instant>,
}

impl HeartbeatRecord {
    // Update this proc's heartbeat record with timestamp as "now".
    fn update(&mut self, proc_id: &ProcId, clock: &impl Clock) {
        // Remove previous entry in btree_index if exists.
        if let Some(update_time) = self.proc_last_update_time.get(proc_id) {
            self.btree_index
                .remove(&(update_time.clone(), proc_id.clone()));
        }

        // Insert new entry into btree_index.
        let now = clock.now();
        self.proc_last_update_time
            .insert(proc_id.clone(), now.clone());
        self.btree_index.insert((now.clone(), proc_id.clone()));
    }

    // Find all the procs with expired heartbeat, and mark them as expired in
    // WorldSupervisionState.
    fn mark_expired_procs(
        &self,
        state: &mut WorldSupervisionState,
        clock: &impl Clock,
        supervision_update_timeout: Duration,
    ) {
        // Update procs' live status.
        let now = clock.now();
        self.btree_index
            .iter()
            .take_while(|(last_update_time, _)| {
                now > *last_update_time + supervision_update_timeout
            })
            .for_each(|(_, proc_id)| {
                if let Some(proc_state) = state
                    .procs
                    .get_mut(&proc_id.rank().expect("proc must be ranked for rank access"))
                {
                    match proc_state.proc_health {
                        ProcStatus::Alive => proc_state.proc_health = ProcStatus::Expired,
                        // Do not overwrite the health of a proc already known to be unhealthy.
                        _ => (),
                    }
                }
            });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorldSupervisionInfo {
    state: WorldSupervisionState,

    // The lifecycle mode of the proc.
    lifecycle_mode: HashMap<ProcId, ProcLifecycleMode>,

    #[serde(skip)]
    heartbeat_record: HeartbeatRecord,
}

impl WorldSupervisionInfo {
    fn new() -> Self {
        Self {
            state: WorldSupervisionState {
                procs: HashMap::new(),
            },
            lifecycle_mode: HashMap::new(),
            heartbeat_record: HeartbeatRecord::default(),
        }
    }
}

impl SystemSupervisionState {
    fn new(supervision_update_timeout: Duration) -> Self {
        Self {
            supervision_map: HashMap::new(),
            supervision_update_timeout,
        }
    }

    /// Create a proc supervision entry.
    fn create(
        &mut self,
        proc_state: ProcSupervisionState,
        lifecycle_mode: ProcLifecycleMode,
        clock: &impl Clock,
    ) {
        if World::is_host_world(&proc_state.world_id) {
            return;
        }

        let world = self
            .supervision_map
            .entry(proc_state.world_id.clone())
            .or_insert_with(WorldSupervisionInfo::new);
        world
            .lifecycle_mode
            .insert(proc_state.proc_id.clone(), lifecycle_mode);

        self.update(proc_state, clock);
    }

    /// Update a proc supervision entry.
    fn update(&mut self, proc_state: ProcSupervisionState, clock: &impl Clock) {
        if World::is_host_world(&proc_state.world_id) {
            return;
        }

        let world = self
            .supervision_map
            .entry(proc_state.world_id.clone())
            .or_insert_with(WorldSupervisionInfo::new);

        world.heartbeat_record.update(&proc_state.proc_id, clock);

        // Update supervision map.
        if let Some(info) = world.state.procs.get_mut(
            &proc_state
                .proc_id
                .rank()
                .expect("proc must be ranked for proc state update"),
        ) {
            match info.proc_health {
                ProcStatus::Alive => info.proc_health = proc_state.proc_health,
                // Do not overwrite the health of a proc already known to be unhealthy.
                _ => (),
            }
            info.failed_actors.extend(proc_state.failed_actors);
        } else {
            world.state.procs.insert(
                proc_state
                    .proc_id
                    .rank()
                    .expect("proc must be ranked for rank access"),
                proc_state,
            );
        }
    }

    /// Report the given proc's supervision state. If the proc is not in the map, do nothing.
    fn report(&mut self, proc_state: ProcSupervisionState, clock: &impl Clock) {
        if World::is_host_world(&proc_state.world_id) {
            return;
        }

        let proc_id = proc_state.proc_id.clone();
        match self.supervision_map.entry(proc_state.world_id.clone()) {
            Entry::Occupied(mut world_supervision_info) => {
                match world_supervision_info
                    .get_mut()
                    .state
                    .procs
                    .entry(proc_id.rank().expect("proc must be ranked for rank access"))
                {
                    Entry::Occupied(_) => {
                        self.update(proc_state, clock);
                    }
                    Entry::Vacant(_) => {
                        tracing::error!("supervision not enabled for proc {}", &proc_id);
                    }
                }
            }
            Entry::Vacant(_) => {
                tracing::error!("supervision not enabled for proc {}", &proc_id);
            }
        }
    }

    /// Get procs of a world with expired supervision updates, as well as procs with
    /// actor failures.
    fn get_world_with_failures(
        &mut self,
        world_id: &WorldId,
        clock: &impl Clock,
    ) -> Option<WorldSupervisionState> {
        if let Some(world) = self.supervision_map.get_mut(world_id) {
            world.heartbeat_record.mark_expired_procs(
                &mut world.state,
                clock,
                self.supervision_update_timeout,
            );
            // Get procs with failures.
            let mut world_state_copy = world.state.clone();
            // Only return failed procs if there is any
            world_state_copy
                .procs
                .retain(|_, proc_state| !proc_state.is_healthy());
            return Some(world_state_copy);
        }
        None
    }

    fn is_world_healthy(&mut self, world_id: &WorldId, clock: &impl Clock) -> bool {
        self.get_world_with_failures(world_id, clock)
            .is_none_or(|state| WorldSupervisionState::is_healthy(&state))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorldStoppingState {
    stopping_procs: HashSet<ProcId>,
    stopped_procs: HashSet<ProcId>,
}

/// A message to stop the system actor.
#[derive(Debug, Clone, PartialEq, EnumAsInner)]
enum SystemStopMessage {
    StopSystemActor,
    EvictWorlds(Vec<WorldId>),
}

/// The system actor manages the whole system. It is responsible for
/// managing the systems' worlds, and for managing their constituent
/// procs. The system actor also provides a central mailbox that can
/// route messages to any live actor in the system.
#[derive(Debug, Clone)]
#[hyperactor::export(
    handlers = [
        SystemMessage,
        ProcSupervisionMessage,
        WorldSupervisionMessage,
    ],
)]
pub struct SystemActor {
    params: SystemActorParams,
    supervision_state: SystemSupervisionState,
    worlds: HashMap<WorldId, World>,
    // A map from request id to stop state for inflight stop requests.
    worlds_to_stop: HashMap<WorldId, WorldStoppingState>,
    shutting_down: bool,
}

/// The well known ID of the world that hosts the system actor, it is always `system`.
pub static SYSTEM_WORLD: LazyLock<WorldId> = LazyLock::new(|| id!(system));

/// The well known ID of the system actor, it is always `system[0].root`.
static SYSTEM_ACTOR_ID: LazyLock<ActorId> = LazyLock::new(|| id!(system[0].root));

/// The ref corresponding to the well known [`ID`].
pub static SYSTEM_ACTOR_REF: LazyLock<ActorRef<SystemActor>> =
    LazyLock::new(|| ActorRef::attest(id!(system[0].root)));

impl SystemActor {
    /// Adds a new world that's awaiting creation to the worlds.
    fn add_new_world(&mut self, world_id: WorldId) -> Result<(), anyhow::Error> {
        let world_state = WorldState {
            host_map: HashMap::new(),
            procs: HashMap::new(),
            status: WorldStatus::AwaitingCreation,
        };
        let world = World::new(
            world_id.clone(),
            Shape::Unknown,
            world_state,
            0,
            Environment::Local,
            HashMap::new(),
        )?;
        self.worlds.insert(world_id.clone(), world);
        Ok(())
    }

    fn router(&self) -> &ReportingRouter {
        &self.params.mailbox_router
    }

    /// Bootstrap the system actor. This will create a proc, spawn the actor
    /// on that proc, and then return the actor handle and the corresponding
    /// proc.
    pub async fn bootstrap(
        params: SystemActorParams,
    ) -> Result<(ActorHandle<SystemActor>, Proc), anyhow::Error> {
        Self::bootstrap_with_clock(params, ClockKind::default()).await
    }

    /// Bootstrap the system actor with a specified clock.This will create a proc, spawn the actor
    /// on that proc, and then return the actor handle and the corresponding
    /// proc.
    pub async fn bootstrap_with_clock(
        params: SystemActorParams,
        clock: ClockKind,
    ) -> Result<(ActorHandle<SystemActor>, Proc), anyhow::Error> {
        let system_proc = Proc::new_with_clock(
            SYSTEM_ACTOR_ID.proc_id().clone(),
            BoxedMailboxSender::new(params.mailbox_router.clone()),
            clock,
        );
        let actor_handle = system_proc
            .spawn::<SystemActor>(SYSTEM_ACTOR_ID.name(), params)
            .await?;

        Ok((actor_handle, system_proc))
    }

    /// Evict a single world
    fn evict_world(&mut self, world_id: &WorldId) {
        self.worlds.remove(world_id);
        self.supervision_state.supervision_map.remove(world_id);
        // Remove all the addresses starting with the world_id as the prefix.
        self.params
            .mailbox_router
            .router
            .unbind(&world_id.clone().into());
    }
}

#[async_trait]
impl Actor for SystemActor {
    type Params = SystemActorParams;

    async fn new(params: SystemActorParams) -> Result<Self, anyhow::Error> {
        let supervision_update_timeout = params.supervision_update_timeout.clone();
        Ok(Self {
            params,
            supervision_state: SystemSupervisionState::new(supervision_update_timeout),
            worlds: HashMap::new(),
            worlds_to_stop: HashMap::new(),
            shutting_down: false,
        })
    }

    async fn init(&mut self, cx: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Start to periodically check the unhealthy worlds.
        cx.self_message_with_delay(MaintainWorldHealth {}, Duration::from_secs(0))?;
        Ok(())
    }

    async fn handle_undeliverable_message(
        &mut self,
        _cx: &Instance<Self>,
        Undeliverable(envelope): Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        let to = envelope.dest().clone();
        let from = envelope.sender().clone();
        tracing::info!(
            "a message from {} to {} was undeliverable and returned to the system actor",
            from,
            to,
        );

        // The channel to the receiver's proc is lost or can't be
        // established. Update the proc's supervision status
        // accordingly.
        let proc_id = to.actor_id().proc_id();
        let world_id = proc_id
            .world_id()
            .expect("proc must be ranked for world_id access");
        if let Some(world) = &mut self.supervision_state.supervision_map.get_mut(world_id) {
            if let Some(proc) = world
                .state
                .procs
                .get_mut(&proc_id.rank().expect("proc must be ranked for rank access"))
            {
                match proc.proc_health {
                    ProcStatus::Alive => proc.proc_health = ProcStatus::ConnectionFailure,
                    // Do not overwrite the health of a proc already
                    // known to be unhealthy.
                    _ => (),
                }
            } else {
                tracing::error!(
                    "can't update proc {} status because there isn't one",
                    proc_id
                );
            }
        } else {
            tracing::error!(
                "can't update world {} status because there isn't one",
                world_id
            );
        }
        Ok(())
    }
}

///
/// +------+  spawns   +----+  joins   +-----+
/// | Proc |<----------|Host|--------->|World|
/// +------+           +----+          +-----+
///    |                                   ^
///    |          joins                    |
///    +-----------------------------------+
/// When bootstrapping the system,
///   1. hosts will join the world,
///   2. hosts will spawn (worker) procs,
///   3. procs will join the world
#[async_trait]
#[hyperactor::forward(SystemMessage)]
impl SystemMessageHandler for SystemActor {
    async fn join(
        &mut self,
        cx: &Context<Self>,
        world_id: WorldId,
        proc_id: ProcId,
        proc_message_port: PortRef<ProcMessage>,
        channel_addr: ChannelAddr,
        labels: HashMap<String, String>,
        lifecycle_mode: ProcLifecycleMode,
    ) -> Result<(), anyhow::Error> {
        tracing::info!("received join for proc {} in world {}", proc_id, world_id);
        // todo: check that proc_id is a user id
        self.router()
            .router
            .bind(proc_id.clone().into(), channel_addr.clone());

        self.router().broadcast_addr(&proc_id, channel_addr.clone());

        // TODO: handle potential undeliverable message return
        self.router().serialize_and_send(
            &proc_message_port,
            ProcMessage::Joined(),
            monitored_return_handle(),
        )?;

        if lifecycle_mode.is_managed() {
            self.supervision_state.create(
                ProcSupervisionState {
                    world_id: world_id.clone(),
                    proc_id: proc_id.clone(),
                    proc_addr: channel_addr.clone(),
                    proc_health: ProcStatus::Alive,
                    failed_actors: Vec::new(),
                },
                lifecycle_mode.clone(),
                cx.clock(),
            );
        }

        // If the proc's life cycle is not managed by system actor, system actor
        // doesn't need to track it in its "worlds" field.
        if lifecycle_mode != ProcLifecycleMode::ManagedBySystem {
            tracing::info!("ignoring join for proc {} in world {}", proc_id, world_id);
            return Ok(());
        }

        let world_id = World::get_real_world_id(&world_id);
        if !self.worlds.contains_key(&world_id) {
            self.add_new_world(world_id.clone())?;
        }
        let world = self
            .worlds
            .get_mut(&world_id)
            .ok_or(anyhow::anyhow!("failed to get world from map"))?;

        match HostId::try_from(proc_id.clone()) {
            Ok(host_id) => {
                tracing::info!("{}: adding host {}", world_id, host_id);
                return world
                    .on_host_join(
                        host_id,
                        proc_message_port,
                        &self.params.mailbox_router.router,
                    )
                    .await
                    .map_err(anyhow::Error::from);
            }
            // If it is not a host ID, it must be a regular proc ID. e.g.
            // worker procs spawned by the host proc actor.
            Err(_) => {
                tracing::info!("proc {} joined to world {}", &proc_id, &world_id,);
                // TODO(T207602936) add reconciliation machine to make sure
                // 1. only add procs that are created by the host
                // 2. retry upon failed proc creation by host.
                if let Err(e) = world.add_proc(proc_id.clone(), proc_message_port, labels) {
                    tracing::warn!(
                        "failed to add proc {} to world {}: {}",
                        &proc_id,
                        &world_id,
                        e
                    );
                }
            }
        };
        Ok(())
    }

    async fn upsert_world(
        &mut self,
        cx: &Context<Self>,
        world_id: WorldId,
        shape: Shape,
        num_procs_per_host: usize,
        env: Environment,
        labels: HashMap<String, String>,
    ) -> Result<(), anyhow::Error> {
        tracing::info!("received upsert_world for world {}!", world_id);
        match self.worlds.get_mut(&world_id) {
            Some(world) => {
                tracing::info!("found existing world {}!", world_id);
                match &world.state.status {
                    WorldStatus::AwaitingCreation => {
                        world.scheduler_params.shape = shape;
                        world.scheduler_params.num_procs_per_host = num_procs_per_host;
                        world.scheduler_params.env = env;
                        world.state = WorldState {
                            host_map: world.state.host_map.clone(),
                            procs: world.state.procs.clone(),
                            status: if world.state.procs.len() < world.scheduler_params.num_procs()
                                || !self
                                    .supervision_state
                                    .is_world_healthy(&world_id, cx.clock())
                            {
                                WorldStatus::Unhealthy(cx.clock().system_time_now())
                            } else {
                                WorldStatus::Live
                            },
                        };
                        for (k, v) in labels {
                            if world.labels.contains_key(&k) {
                                anyhow::bail!("cannot overwrite world label: {}", k);
                            }
                            world.labels.insert(k.clone(), v.clone());
                        }
                    }
                    _ => {
                        anyhow::bail!("cannot modify world {}: already exists", world.world_id)
                    }
                }

                world.on_create(&self.params.mailbox_router.router).await?;
                tracing::info!(
                    "modified parameters to world {} with shape: {:?} and labels {:?}",
                    &world.world_id,
                    world.scheduler_params.shape,
                    world.labels
                );
            }
            None => {
                let world = World::new(
                    world_id.clone(),
                    shape.clone(),
                    WorldState {
                        host_map: HashMap::new(),
                        procs: HashMap::new(),
                        status: WorldStatus::Unhealthy(cx.clock().system_time_now()),
                    },
                    num_procs_per_host,
                    env,
                    labels,
                )?;
                tracing::info!("new world {} added with shape: {:?}", world_id, &shape);
                self.worlds.insert(world_id, world);
            }
        };
        Ok(())
    }

    async fn snapshot(
        &mut self,
        _cx: &Context<Self>,
        filter: SystemSnapshotFilter,
    ) -> Result<SystemSnapshot, anyhow::Error> {
        let world_snapshots = self
            .worlds
            .iter()
            .filter(|(_, world)| filter.world_matches(world))
            .map(|(world_id, world)| {
                (
                    world_id.clone(),
                    WorldSnapshot::from_world_filtered(world, &filter),
                )
            })
            .collect();
        Ok(SystemSnapshot {
            worlds: world_snapshots,
            execution_id: hyperactor_telemetry::env::execution_id(),
        })
    }

    async fn stop(
        &mut self,
        cx: &Context<Self>,
        worlds: Option<Vec<WorldId>>,
        proc_timeout: Duration,
        reply_port: OncePortRef<()>,
    ) -> Result<(), anyhow::Error> {
        // TODO: this needn't be async

        match &worlds {
            Some(world_ids) => {
                tracing::info!("stopping worlds: {:?}", world_ids);
            }
            None => {
                tracing::info!("stopping system actor and all worlds");
                self.shutting_down = true;
            }
        }

        // If there's no worlds left to stop, shutdown now.
        if self.worlds.is_empty() && self.shutting_down {
            cx.stop()?;
            reply_port.send(cx, ())?;
            return Ok(());
        }

        let mut world_ids = vec![];
        match &worlds {
            Some(worlds) => {
                // Stop only the specified worlds
                world_ids.extend(worlds.clone().into_iter().collect::<Vec<_>>());
            }
            None => {
                // Stop all worlds
                world_ids.extend(
                    self.worlds
                        .keys()
                        .filter(|x| x.name() != "user")
                        .cloned()
                        .collect::<Vec<_>>(),
                );
            }
        }

        for world_id in &world_ids {
            if self.worlds_to_stop.contains_key(world_id) || !self.worlds.contains_key(world_id) {
                // The world is being stopped already.
                continue;
            }
            self.worlds_to_stop.insert(
                world_id.clone(),
                WorldStoppingState {
                    stopping_procs: HashSet::new(),
                    stopped_procs: HashSet::new(),
                },
            );
        }

        let all_procs = self
            .worlds
            .iter()
            .filter(|(world_id, _)| match &worlds {
                Some(worlds_ids) => worlds_ids.contains(world_id),
                None => true,
            })
            .flat_map(|(_, world)| {
                world
                    .state
                    .host_map
                    .iter()
                    .map(|(host_id, host)| (host_id.0.clone(), host.proc_message_port.clone()))
                    .chain(
                        world
                            .state
                            .procs
                            .iter()
                            .map(|(proc_id, info)| (proc_id.clone(), info.port_ref.clone())),
                    )
                    .collect::<Vec<_>>()
            })
            .collect::<HashMap<_, _>>();

        // Send Stop message to all processes known to the system. This is a best
        // effort, because the message might fail to deliver due to network
        // partition.
        for (proc_id, port) in all_procs.into_iter() {
            let stopping_state = self
                .worlds_to_stop
                .get_mut(&World::get_real_world_id(
                    proc_id
                        .world_id()
                        .expect("proc must be ranked for world_id access"),
                ))
                .unwrap();
            if !stopping_state.stopping_procs.insert(proc_id) {
                continue;
            }

            // This is a hack. Due to T214365263, SystemActor cannot get reply
            // from a 2-way message when that message is sent from its handler.
            // As a result, we set the reply to a handle port, so that reply
            // can be processed as a separate message. See Handler<ProcStopResult>
            // for how the received reply is further processed.
            let reply_to = cx.port::<ProcStopResult>().bind().into_once();
            port.send(
                cx,
                ProcMessage::Stop {
                    timeout: proc_timeout,
                    reply_to,
                },
            )?;
        }

        let stop_msg = match &worlds {
            Some(_) => SystemStopMessage::EvictWorlds(world_ids.clone()),
            None => SystemStopMessage::StopSystemActor {},
        };

        // Schedule a message to stop the system actor itself.
        cx.self_message_with_delay(stop_msg, Duration::from_secs(8))?;

        reply_port.send(cx, ())?;
        Ok(())
    }
}

#[async_trait]
impl Handler<MaintainWorldHealth> for SystemActor {
    async fn handle(&mut self, cx: &Context<Self>, _: MaintainWorldHealth) -> anyhow::Result<()> {
        // TODO: this needn't be async

        // Find the world with the oldest unhealthy time so we can schedule the next check.
        let mut next_check_delay = self.params.world_eviction_timeout;
        tracing::debug!("Checking world state. Got {} worlds", self.worlds.len());

        for world in self.worlds.values_mut() {
            if world.state.status == WorldStatus::AwaitingCreation {
                continue;
            }

            let Some(state) = self
                .supervision_state
                .get_world_with_failures(&world.world_id, cx.clock())
            else {
                tracing::debug!("world {} does not have failures, skipping.", world.world_id);
                continue;
            };

            if state.is_healthy() {
                tracing::debug!(
                    "world {} with procs {:?} is healthy, skipping.",
                    world.world_id,
                    state
                        .procs
                        .values()
                        .map(|p| p.proc_id.clone())
                        .collect::<Vec<_>>()
                );
                continue;
            }
            // Some procs are not healthy, check if any of the proc should manage the system.
            for (_, proc_state) in state.procs.iter() {
                if proc_state.proc_health == ProcStatus::Alive {
                    tracing::debug!("proc {} is still alive.", proc_state.proc_id);
                    continue;
                }
                if self
                    .supervision_state
                    .supervision_map
                    .get(&world.world_id)
                    .and_then(|world| world.lifecycle_mode.get(&proc_state.proc_id))
                    .map_or(true, |mode| *mode != ProcLifecycleMode::ManagingSystem)
                {
                    tracing::debug!(
                        "proc {} with state {} does not manage system.",
                        proc_state.proc_id,
                        proc_state.proc_health
                    );
                    continue;
                }

                tracing::error!(
                    "proc {}  is unhealthy, stop the system as the proc manages the system",
                    proc_state.proc_id
                );

                // The proc has expired heartbeating and it manages the lifecycle of system, schedule system stop
                let (tx, _) = cx.open_once_port::<()>();
                cx.port().send(SystemMessage::Stop {
                    worlds: None,
                    proc_timeout: Duration::from_secs(5),
                    reply_port: tx.bind(),
                })?;
            }

            if world.state.status == WorldStatus::Live {
                world.state.status = WorldStatus::Unhealthy(cx.clock().system_time_now());
            }

            match &world.state.status {
                WorldStatus::Unhealthy(last_unhealthy_time) => {
                    let elapsed = last_unhealthy_time
                        .elapsed()
                        .inspect_err(|err| {
                            tracing::error!(
                                "failed to get elapsed time for unhealthy world {}: {}",
                                world.world_id,
                                err
                            )
                        })
                        .unwrap_or_else(|_| Duration::from_secs(0));

                    if elapsed < self.params.world_eviction_timeout {
                        // We can live a bit longer still.
                        next_check_delay = std::cmp::min(
                            next_check_delay,
                            self.params.world_eviction_timeout - elapsed,
                        );
                    } else {
                        next_check_delay = Duration::from_secs(0);
                    }
                }
                _ => {
                    tracing::error!(
                        "find a failed world {} with healthy state {}",
                        world.world_id,
                        world.state.status
                    );
                    continue;
                }
            }
        }
        cx.self_message_with_delay(MaintainWorldHealth {}, next_check_delay)?;

        Ok(())
    }
}

#[async_trait]
impl Handler<ProcSupervisionMessage> for SystemActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: ProcSupervisionMessage,
    ) -> anyhow::Result<()> {
        match msg {
            ProcSupervisionMessage::Update(state, reply_port) => {
                self.supervision_state.report(state, cx.clock());
                let _ = reply_port.send(cx, ());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<WorldSupervisionMessage> for SystemActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: WorldSupervisionMessage,
    ) -> anyhow::Result<()> {
        match msg {
            WorldSupervisionMessage::State(world_id, reply_port) => {
                let world_state = self
                    .supervision_state
                    .get_world_with_failures(&world_id, cx.clock());
                // TODO: handle potential undeliverable message return
                let _ = reply_port.send(cx, world_state);
            }
        }
        Ok(())
    }
}

// Temporary solution to allow SystemMessage::Stop receive replies from 2-way
// messages. Can be remove after T214365263 is implemented.
#[async_trait]
impl Handler<ProcStopResult> for SystemActor {
    async fn handle(&mut self, cx: &Context<Self>, msg: ProcStopResult) -> anyhow::Result<()> {
        fn stopping_proc_msg<'a>(sprocs: impl Iterator<Item = &'a ProcId>) -> String {
            let sprocs = sprocs.collect::<Vec<_>>();
            if sprocs.is_empty() {
                return "no procs left".to_string();
            }
            let msg = sprocs
                .iter()
                .take(3)
                .map(|proc_id| proc_id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            if sprocs.len() > 3 {
                format!("remaining procs: {} and {} more", msg, sprocs.len() - 3)
            } else {
                format!("remaining procs: {}", msg)
            }
        }
        let mut world_stopped = false;
        let world_id = &msg
            .proc_id
            .clone()
            .into_ranked()
            .expect("proc must be ranked for world_id access")
            .0;
        if let Some(stopping_state) = self.worlds_to_stop.get_mut(world_id) {
            stopping_state.stopped_procs.insert(msg.proc_id.clone());
            tracing::debug!(
                "received stop response from {}: {} stopped actors, {} aborted actors: {}",
                msg.proc_id,
                msg.actors_stopped,
                msg.actors_aborted,
                stopping_proc_msg(
                    stopping_state
                        .stopping_procs
                        .difference(&stopping_state.stopped_procs)
                ),
            );
            world_stopped =
                stopping_state.stopping_procs.len() == stopping_state.stopped_procs.len();
        } else {
            tracing::warn!(
                "received stop response from {} but no inflight stopping request is found, possibly late response",
                msg.proc_id
            );
        }

        if world_stopped {
            self.evict_world(world_id);
            self.worlds_to_stop.remove(world_id);
        }

        if self.shutting_down && self.worlds.is_empty() {
            cx.stop()?;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<SystemStopMessage> for SystemActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: SystemStopMessage,
    ) -> anyhow::Result<()> {
        match message {
            SystemStopMessage::EvictWorlds(world_ids) => {
                for world_id in &world_ids {
                    if self.worlds_to_stop.contains_key(world_id) {
                        tracing::warn!(
                            "Waiting for world to stop timed out, evicting world anyways: {:?}",
                            world_id
                        );
                        self.evict_world(world_id);
                    }
                }
            }
            SystemStopMessage::StopSystemActor => {
                if self.worlds_to_stop.is_empty() {
                    tracing::warn!(
                        "waiting for all worlds to stop timed out, stopping the system actor and evicting the these worlds anyways: {:?}",
                        self.worlds_to_stop.keys()
                    );
                } else {
                    tracing::warn!(
                        "waiting for all worlds to stop timed out, stopping the system actor"
                    );
                }

                cx.stop()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use anyhow::Result;
    use hyperactor::PortId;
    use hyperactor::actor::ActorStatus;
    use hyperactor::channel;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::Rx;
    use hyperactor::channel::TcpMode;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::data::Serialized;
    use hyperactor::mailbox::Mailbox;
    use hyperactor::mailbox::MailboxServer;
    use hyperactor::mailbox::MessageEnvelope;
    use hyperactor::mailbox::PortHandle;
    use hyperactor::mailbox::PortReceiver;
    use hyperactor::simnet;
    use hyperactor::test_utils::pingpong::PingPongActorParams;
    use hyperactor_config::Attrs;

    use super::*;
    use crate::System;

    struct MockHostActor {
        local_proc_id: ProcId,
        local_proc_addr: ChannelAddr,
        local_proc_message_port: PortHandle<ProcMessage>,
        local_proc_message_receiver: PortReceiver<ProcMessage>,
    }

    async fn spawn_mock_host_actor(proc_world_id: WorldId, host_id: usize) -> MockHostActor {
        // Set up a local actor.
        let local_proc_id = ProcId::Ranked(
            WorldId(format!("{}{}", SHADOW_PREFIX, proc_world_id.name())),
            host_id,
        );
        let (local_proc_addr, local_proc_rx) =
            channel::serve::<MessageEnvelope>(ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let local_proc_mbox = Mailbox::new_detached(local_proc_id.actor_id("test".to_string(), 0));
        let (local_proc_message_port, local_proc_message_receiver) = local_proc_mbox.open_port();
        let _local_proc_serve_handle = local_proc_mbox.clone().serve(local_proc_rx);
        MockHostActor {
            local_proc_id,
            local_proc_addr,
            local_proc_message_port,
            local_proc_message_receiver,
        }
    }

    // V0-specific test - no V1 equivalent. Unit test for
    // SystemSupervisionState which tracks proc health and failed
    // actors centrally at world level. Tests heartbeat timeout
    // detection (marks procs expired if no heartbeat within timeout)
    // and failed actor aggregation. V1 does not have centralized
    // supervision state - V1 uses local supervision where actors
    // handle ActorSupervisionEvent locally rather than reporting to a
    // central SystemActor for world-level health monitoring.
    #[tokio::test]
    async fn test_supervision_state() {
        let mut sv = SystemSupervisionState::new(Duration::from_secs(1));
        let world_id = id!(world);
        let proc_id_0 = world_id.proc_id(0);
        let clock = ClockKind::Real(RealClock);
        sv.create(
            ProcSupervisionState {
                world_id: world_id.clone(),
                proc_addr: ChannelAddr::any(ChannelTransport::Local),
                proc_id: proc_id_0.clone(),
                proc_health: ProcStatus::Alive,
                failed_actors: Vec::new(),
            },
            ProcLifecycleMode::ManagedBySystem,
            &clock,
        );
        let actor_id = id!(world[1].actor);
        let proc_id_1 = actor_id.proc_id();
        sv.create(
            ProcSupervisionState {
                world_id: world_id.clone(),
                proc_addr: ChannelAddr::any(ChannelTransport::Local),
                proc_id: proc_id_1.clone(),
                proc_health: ProcStatus::Alive,
                failed_actors: Vec::new(),
            },
            ProcLifecycleMode::ManagedBySystem,
            &clock,
        );
        let world_id = id!(world);

        let unknown_world_id = id!(unknow_world);
        let failures = sv.get_world_with_failures(&unknown_world_id, &clock);
        assert!(failures.is_none());

        // No supervision expiration yet.
        let failures = sv.get_world_with_failures(&world_id, &clock);
        assert!(failures.is_some());
        assert_eq!(failures.unwrap().procs.len(), 0);

        // One proc expired.
        RealClock.sleep(Duration::from_secs(2)).await;
        sv.report(
            ProcSupervisionState {
                world_id: world_id.clone(),
                proc_addr: ChannelAddr::any(ChannelTransport::Local),
                proc_id: proc_id_1.clone(),
                proc_health: ProcStatus::Alive,
                failed_actors: Vec::new(),
            },
            &clock,
        );
        let failures = sv.get_world_with_failures(&world_id, &clock);
        let procs = failures.unwrap().procs;
        assert_eq!(procs.len(), 1);
        assert!(
            procs.contains_key(
                &proc_id_0
                    .rank()
                    .expect("proc must be ranked for rank access")
            )
        );

        // Actor failure happened to proc_1
        sv.report(
            ProcSupervisionState {
                world_id: world_id.clone(),
                proc_addr: ChannelAddr::any(ChannelTransport::Local),
                proc_id: proc_id_1.clone(),
                proc_health: ProcStatus::Alive,
                failed_actors: [(
                    actor_id.clone(),
                    ActorStatus::generic_failure("Actor failed"),
                )]
                .to_vec(),
            },
            &clock,
        );

        let failures = sv.get_world_with_failures(&world_id, &clock);
        let procs = failures.unwrap().procs;
        assert_eq!(procs.len(), 2);
        assert!(
            procs.contains_key(
                &proc_id_0
                    .rank()
                    .expect("proc must be ranked for rank access")
            )
        );
        assert!(
            procs.contains_key(
                &proc_id_1
                    .rank()
                    .expect("proc must be ranked for rank access")
            )
        );
    }

    // V0-specific test - no V1 equivalent. Tests SystemActor world
    // orchestration where hosts can join before world is created.
    // Flow: hosts send Join messages → queued by SystemActor →
    // UpsertWorld defines world topology → SystemActor sends
    // SpawnProc messages telling each host which procs to spawn.
    // Verifies correct proc assignment across hosts. V1 does not have
    // this orchestration model - V1 uses coordinated ProcMesh
    // allocation where meshes are allocated in one operation, not
    // assembled from hosts independently joining a central
    // SystemActor.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_host_join_before_world() {
        // Spins up a new world with 2 hosts, with 3 procs each.
        let params = SystemActorParams::new(Duration::from_secs(10), Duration::from_secs(10));
        let (system_actor_handle, _system_proc) = SystemActor::bootstrap(params).await.unwrap();

        // Use a local proc actor to join the system.
        let mut host_actors: Vec<MockHostActor> = Vec::new();

        let world_name = "test".to_string();
        let world_id = WorldId(world_name.clone());
        host_actors.push(spawn_mock_host_actor(world_id.clone(), 0).await);
        host_actors.push(spawn_mock_host_actor(world_id.clone(), 1).await);

        for host_actor in host_actors.iter_mut() {
            // Join the world.
            system_actor_handle
                .send(SystemMessage::Join {
                    proc_id: host_actor.local_proc_id.clone(),
                    world_id: world_id.clone(),
                    proc_message_port: host_actor.local_proc_message_port.bind(),
                    proc_addr: host_actor.local_proc_addr.clone(),
                    labels: HashMap::new(),
                    lifecycle_mode: ProcLifecycleMode::ManagedBySystem,
                })
                .unwrap();

            // We should get a joined message.
            // and a spawn proc message.
            assert_matches!(
                host_actor.local_proc_message_receiver.recv().await.unwrap(),
                ProcMessage::Joined()
            );
        }

        // Create a new world message and send to system actor
        let num_procs = 6;
        let shape = Shape::Definite(vec![2, 3]);
        system_actor_handle
            .send(SystemMessage::UpsertWorld {
                world_id: world_id.clone(),
                shape,
                num_procs_per_host: 3,
                env: Environment::Local,
                labels: HashMap::new(),
            })
            .unwrap();

        let mut all_procs: Vec<ProcId> = Vec::new();
        for host_actor in host_actors.iter_mut() {
            let m = host_actor.local_proc_message_receiver.recv().await.unwrap();
            match m {
                ProcMessage::SpawnProc {
                    env,
                    world_id,
                    mut proc_ids,
                    world_size,
                } => {
                    assert_eq!(world_id, WorldId(world_name.clone()));
                    assert_eq!(env, Environment::Local);
                    assert_eq!(world_size, num_procs);
                    all_procs.append(&mut proc_ids);
                }
                _ => std::panic!("Unexpected message type!"),
            }
        }
        // Check if all proc ids from 0 to num_procs - 1 are in the list
        assert_eq!(all_procs.len(), num_procs);
        all_procs.sort();
        for (i, proc) in all_procs.iter().enumerate() {
            assert_eq!(*proc, ProcId::Ranked(WorldId(world_name.clone()), i));
        }
    }

    // V0-specific test - no V1 equivalent. Tests SystemActor world
    // orchestration where world is created before hosts join (reverse
    // order of test_host_join_before_world). Flow: UpsertWorld
    // defines topology → hosts send Join messages → SystemActor
    // immediately sends SpawnProc messages. Tests that join order
    // doesn't matter. V1 does not have this orchestration model - V1
    // uses coordinated ProcMesh allocation where meshes are allocated
    // in one operation.
    #[tokio::test]
    async fn test_host_join_after_world() {
        // Spins up a new world with 2 hosts, with 3 procs each.
        let params = SystemActorParams::new(Duration::from_secs(10), Duration::from_secs(10));
        let (system_actor_handle, _system_proc) = SystemActor::bootstrap(params).await.unwrap();

        // Create a new world message and send to system actor
        let world_name = "test".to_string();
        let world_id = WorldId(world_name.clone());
        let num_procs = 6;
        let shape = Shape::Definite(vec![2, 3]);
        system_actor_handle
            .send(SystemMessage::UpsertWorld {
                world_id: world_id.clone(),
                shape,
                num_procs_per_host: 3,
                env: Environment::Local,
                labels: HashMap::new(),
            })
            .unwrap();

        // Use a local proc actor to join the system.
        let mut host_actors: Vec<MockHostActor> = Vec::new();

        host_actors.push(spawn_mock_host_actor(world_id.clone(), 0).await);
        host_actors.push(spawn_mock_host_actor(world_id.clone(), 1).await);

        for host_actor in host_actors.iter_mut() {
            // Join the world.
            system_actor_handle
                .send(SystemMessage::Join {
                    proc_id: host_actor.local_proc_id.clone(),
                    world_id: world_id.clone(),
                    proc_message_port: host_actor.local_proc_message_port.bind(),
                    proc_addr: host_actor.local_proc_addr.clone(),
                    labels: HashMap::new(),
                    lifecycle_mode: ProcLifecycleMode::ManagedBySystem,
                })
                .unwrap();

            // We should get a joined message.
            // and a spawn proc message.
            assert_matches!(
                host_actor.local_proc_message_receiver.recv().await.unwrap(),
                ProcMessage::Joined()
            );
        }

        let mut all_procs: Vec<ProcId> = Vec::new();
        for host_actor in host_actors.iter_mut() {
            let m = host_actor.local_proc_message_receiver.recv().await.unwrap();
            match m {
                ProcMessage::SpawnProc {
                    env,
                    world_id,
                    mut proc_ids,
                    world_size,
                } => {
                    assert_eq!(world_id, WorldId(world_name.clone()));
                    assert_eq!(env, Environment::Local);
                    assert_eq!(world_size, num_procs);
                    all_procs.append(&mut proc_ids);
                }
                _ => std::panic!("Unexpected message type!"),
            }
        }
        // Check if all proc ids from 0 to num_procs - 1 are in the list
        assert_eq!(all_procs.len(), num_procs);
        all_procs.sort();
        for (i, proc) in all_procs.iter().enumerate() {
            assert_eq!(*proc, ProcId::Ranked(WorldId(world_name.clone()), i));
        }
    }

    // V0-specific test - no V1 equivalent. Unit test for
    // SystemSnapshotFilter which filters worlds by name and labels
    // when querying SystemActor. Tests world_matches() and
    // labels_match() logic. V1 does not have SystemActor or
    // SystemSnapshot - V1 uses mesh-based iteration and state queries
    // instead.
    #[test]
    fn test_snapshot_filter() {
        let test_world = World::new(
            WorldId("test_world".to_string()),
            Shape::Definite(vec![1]),
            WorldState {
                host_map: HashMap::new(),
                procs: HashMap::new(),
                status: WorldStatus::Live,
            },
            1,
            Environment::Local,
            HashMap::from([("foo".to_string(), "bar".to_string())]),
        )
        .unwrap();
        // match all
        let filter = SystemSnapshotFilter::all();
        assert!(filter.world_matches(&test_world));
        assert!(SystemSnapshotFilter::labels_match(
            &HashMap::new(),
            &HashMap::from([("foo".to_string(), "bar".to_string())])
        ));
        // specific match
        let mut filter = SystemSnapshotFilter::all();
        filter.worlds = vec![WorldId("test_world".to_string())];
        assert!(filter.world_matches(&test_world));
        filter.worlds = vec![WorldId("unknow_world".to_string())];
        assert!(!filter.world_matches(&test_world));
        assert!(SystemSnapshotFilter::labels_match(
            &HashMap::from([("foo".to_string(), "baz".to_string())]),
            &HashMap::from([("foo".to_string(), "baz".to_string())]),
        ));
        assert!(!SystemSnapshotFilter::labels_match(
            &HashMap::from([("foo".to_string(), "bar".to_string())]),
            &HashMap::from([("foo".to_string(), "baz".to_string())]),
        ));
    }

    // V0-specific test - no V1 equivalent. Tests SystemActor
    // supervision behavior when mailbox server crashes: undeliverable
    // messages are handled AND system supervision detects the
    // unhealthy world state. V1 does not have SystemActor or world
    // supervision. V1 undeliverable message handling (without
    // supervision) is tested in
    // hyperactor_mesh/src/v1/actor_mesh.rs::test_undeliverable_message_return.
    #[tokio::test]
    async fn test_undeliverable_message_return() {
        // System can't send a message to a remote actor because the
        // proc connection is lost.
        use hyperactor::mailbox::MailboxClient;
        use hyperactor::test_utils::pingpong::PingPongActor;
        use hyperactor::test_utils::pingpong::PingPongMessage;

        use crate::System;
        use crate::proc_actor::ProcActor;
        use crate::supervision::ProcSupervisor;

        // Use temporary config for this test
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_secs(1),
        );

        // Serve a system. Undeliverable messages encountered by the
        // mailbox server are returned to the system actor.
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            Duration::from_secs(2), // supervision update timeout
            Duration::from_secs(2), // duration to evict an unhealthy world
        )
        .await
        .unwrap();
        let system_actor_handle = server_handle.system_actor_handle();
        let mut system = System::new(server_handle.local_addr().clone());
        let client = system.attach().await.unwrap();

        // At this point there are no worlds.
        let snapshot = system_actor_handle
            .snapshot(&client, SystemSnapshotFilter::all())
            .await
            .unwrap();
        assert_eq!(snapshot.worlds.len(), 0);

        // Create one.
        let world_id = id!(world);
        system_actor_handle
            .send(SystemMessage::UpsertWorld {
                world_id: world_id.clone(),
                shape: Shape::Definite(vec![1]),
                num_procs_per_host: 1,
                env: Environment::Local,
                labels: HashMap::new(),
            })
            .unwrap();

        // Now we should know a world.
        let snapshot = system_actor_handle
            .snapshot(&client, SystemSnapshotFilter::all())
            .await
            .unwrap();
        assert_eq!(snapshot.worlds.len(), 1);
        // Check it's the world we think it is.
        assert!(snapshot.worlds.contains_key(&world_id));
        // It starts out unhealthy (todo: understand why).
        assert!(matches!(
            snapshot.worlds.get(&world_id).unwrap().status,
            WorldStatus::Unhealthy(_)
        ));

        // Build a supervisor.
        let supervisor = system.attach().await.unwrap();
        let (_sup_tx, _sup_rx) = supervisor.bind_actor_port::<ProcSupervisionMessage>();
        let sup_ref = ActorRef::<ProcSupervisor>::attest(supervisor.self_id().clone());

        // Construct a system sender.
        let system_sender = BoxedMailboxSender::new(MailboxClient::new(
            channel::dial(server_handle.local_addr().clone()).unwrap(),
        ));
        // Construct a proc forwarder in terms of the system sender.
        let proc_forwarder =
            BoxedMailboxSender::new(DialMailboxRouter::new_with_default(system_sender));

        // Bootstrap proc 'world[0]', join the system.
        let proc_0 = Proc::new(world_id.proc_id(0), proc_forwarder.clone());
        let _proc_actor_0 = ProcActor::bootstrap_for_proc(
            proc_0.clone(),
            world_id.clone(),
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_millis(300), // supervision update interval
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let proc_0_client = proc_0.attach("client").unwrap();
        let (proc_0_undeliverable_tx, _proc_0_undeliverable_rx) = proc_0_client.open_port();

        // Bootstrap a second proc 'world[1]', join the system.
        let proc_1 = Proc::new(world_id.proc_id(1), proc_forwarder.clone());
        let proc_actor_1 = ProcActor::bootstrap_for_proc(
            proc_1.clone(),
            world_id.clone(),
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_millis(300), // supervision update interval
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let proc_1_client = proc_1.attach("client").unwrap();
        let (proc_1_undeliverable_tx, mut _proc_1_undeliverable_rx) = proc_1_client.open_port();

        // Spawn two actors 'ping' and 'pong' where 'ping' runs on
        // 'world[0]' and 'pong' on 'world[1]' (that is, not on the
        // same proc).
        let ping_params = PingPongActorParams::new(Some(proc_0_undeliverable_tx.bind()), None);
        let ping_handle = proc_0
            .spawn::<PingPongActor>("ping", ping_params)
            .await
            .unwrap();
        let pong_params = PingPongActorParams::new(Some(proc_1_undeliverable_tx.bind()), None);
        let pong_handle = proc_1
            .spawn::<PingPongActor>("pong", pong_params)
            .await
            .unwrap();

        // Now kill pong's mailbox server making message delivery
        // between procs impossible.
        proc_actor_1.mailbox.stop("from testing");
        proc_actor_1.mailbox.await.unwrap().unwrap();

        // That in itself shouldn't be a problem. Check the world
        // health now.
        let snapshot = system_actor_handle
            .snapshot(&client, SystemSnapshotFilter::all())
            .await
            .unwrap();
        assert_eq!(snapshot.worlds.len(), 1);
        assert!(snapshot.worlds.contains_key(&world_id));
        assert_eq!(
            snapshot.worlds.get(&world_id).unwrap().status,
            WorldStatus::Live
        );

        // Have 'ping' send 'pong' a message.
        let ttl = 1_u64;
        let (game_over, on_game_over) = proc_0_client.open_once_port::<bool>();
        ping_handle
            .send(PingPongMessage(ttl, pong_handle.bind(), game_over.bind()))
            .unwrap();

        // We expect message delivery failure prevents the game from
        // ending within the timeout.
        assert!(
            RealClock
                .timeout(tokio::time::Duration::from_secs(4), on_game_over.recv())
                .await
                .is_err()
        );

        // By supervision, we expect the world should have
        // transitioned to unhealthy.
        let snapshot = system_actor_handle
            .snapshot(&client, SystemSnapshotFilter::all())
            .await
            .unwrap();
        assert_eq!(snapshot.worlds.len(), 1);
        assert!(matches!(
            snapshot.worlds.get(&world_id).unwrap().status,
            WorldStatus::Unhealthy(_)
        ));
    }

    // V0-specific test - no V1 equivalent. Tests SystemActor stop
    // when system is empty (no worlds). Sends SystemMessage::Stop to
    // central SystemActor which coordinates shutdown of all worlds.
    // V1 does not have a central SystemActor - V1 uses mesh-level
    // stop operations (ProcMesh::stop(), HostMesh::shutdown()) where
    // you stop individual meshes rather than a system-wide
    // coordinator.
    #[tokio::test]
    async fn test_stop_fast() -> Result<()> {
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            Duration::from_secs(2), // supervision update timeout
            Duration::from_secs(2), // duration to evict an unhealthy world
        )
        .await?;
        let system_actor_handle = server_handle.system_actor_handle();
        let mut system = System::new(server_handle.local_addr().clone());
        let client = system.attach().await?;

        // Create a new world message and send to system actor
        let (client_tx, client_rx) = client.open_once_port::<()>();
        system_actor_handle.send(SystemMessage::Stop {
            worlds: None,
            proc_timeout: Duration::from_secs(5),
            reply_port: client_tx.bind(),
        })?;
        client_rx.recv().await?;

        // Check that it has stopped.
        let mut sys_status_rx = system_actor_handle.status();
        {
            let received = sys_status_rx.borrow_and_update();
            assert_eq!(*received, ActorStatus::Stopped);
        }

        Ok(())
    }

    // V0-specific test - no V1 equivalent. Tests ReportingRouter's
    // UpdateAddress behavior in simnet mode. When messages are sent,
    // post_update_address() sends MailboxAdminMessage::UpdateAddress
    // to update address caches with simnet source routing info. V1
    // does not have ReportingRouter or dynamic address updates - V1
    // uses static/direct addressing.
    #[tokio::test]
    async fn test_update_sim_address() {
        simnet::start();

        let src_id = id!(proc[0].actor);
        let src_addr = ChannelAddr::Sim(SimAddr::new("unix!@src".parse().unwrap()).unwrap());
        let dst_addr = ChannelAddr::Sim(SimAddr::new("unix!@dst".parse().unwrap()).unwrap());
        let (_, mut rx) = channel::serve::<MessageEnvelope>(src_addr.clone()).unwrap();

        let router = ReportingRouter::new();

        router
            .router
            .bind(src_id.proc_id().clone().into(), src_addr);
        router.router.bind(id!(proc[1]).into(), dst_addr);

        router.post_update_address(&MessageEnvelope::new(
            src_id,
            PortId(id!(proc[1].actor), 9999u64),
            Serialized::serialize(&1u64).unwrap(),
            Attrs::new(),
        ));

        let envelope = rx.recv().await.unwrap();
        let admin_msg = envelope
            .data()
            .deserialized::<MailboxAdminMessage>()
            .unwrap();
        let MailboxAdminMessage::UpdateAddress {
            addr: ChannelAddr::Sim(addr),
            ..
        } = admin_msg
        else {
            panic!("Expected sim address");
        };

        assert_eq!(addr.src().clone().unwrap().to_string(), "unix:@src");
        assert_eq!(addr.addr().to_string(), "unix:@dst");
    }
}
