/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::time::SystemTime;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::RemoteEndpoint as _;
use hyperactor::Unbind;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::actor::handle_undeliverable_message;
use hyperactor::context;
use hyperactor::kv_pairs;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::RemoteMessage;
use hyperactor::mailbox::Undeliverable;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_telemetry::declare_static_counter;
use ndslice::ViewExt;
use ndslice::view::CollectMeshExt;
use ndslice::view::Point;
use ndslice::view::Ranked;
use opentelemetry::metrics::Counter;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use typeuri::Named;

use crate::ValueMesh;
use crate::actor_mesh::ActorMeshRef;
use crate::bootstrap::ProcStatus;
use crate::casting::CAST_ACTOR_MESH_ID;
use crate::casting::update_undeliverable_envelope_for_casting;
use crate::mesh_id::ResourceId;
use crate::proc_agent::ActorState;
use crate::proc_agent::MESH_ORPHAN_TIMEOUT;
use crate::proc_mesh::ProcMeshRef;
use crate::resource;
use crate::supervision::MeshFailure;
use crate::supervision::Unhealthy;

/// Actor name for `ActorMeshController` when spawned as a named child.
pub const ACTOR_MESH_CONTROLLER_NAME: &str = "actor_mesh_controller";

declare_attrs! {
    /// Time between checks of actor states to create supervision events for
    /// owners. The longer this is, the longer it will take to detect a failure
    /// and report it to all subscribers; however, shorter intervals will send
    /// more frequent messages and heartbeats just to see everything is still running.
    /// The default is chosen to balance these two objectives.
    /// This also controls how frequently the healthy heartbeat is sent out to
    /// subscribers if there are no failures encountered.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_SUPERVISION_POLL_FREQUENCY".to_string()),
        None,
    ))
    pub attr SUPERVISION_POLL_FREQUENCY: Duration = Duration::from_secs(10);
}

declare_static_counter!(
    ACTOR_MESH_CONTROLLER_SUPERVISION_STALLS,
    "actor.actor_mesh_controller.num_stalls"
);

declare_static_counter!(
    PROC_MESH_CONTROLLER_SUPERVISION_STALLS,
    "actor.proc_mesh_controller.num_stalls"
);

/// Aggregated health and subscriber bookkeeping for a single
/// `ResourceController`. Tracks the most recently observed status of every
/// rank in the controlled mesh, the latched unhealthy event (if any), the
/// owner port (notified on failures), and the set of streaming subscribers
/// (notified on both stop and failure events). The generation counter on
/// each status entry provides last-writer-wins ordering between streamed
/// and polled updates.
#[derive(Debug)]
pub struct HealthState {
    /// The status of each rank in the controlled mesh, paired with the
    /// generation counter from the most recent update. The generation is
    /// used for last-writer-wins ordering between streamed and polled updates.
    statuses: HashMap<Point, (resource::Status, u64)>,
    /// The latched unhealthy event for the mesh, if any. Once set, this is
    /// surfaced to new subscribers on subscribe and to `GetState` callers.
    unhealthy_event: Option<Unhealthy>,
    /// Per-rank supervision events for ranks that have crashed. Used to build
    /// region-scoped failure reports.
    crashed_ranks: HashMap<usize, ActorSupervisionEvent>,
    /// The single owner of the controlled mesh, notified on failure events
    /// (but not on clean stops).
    owner: Option<hyperactor::PortRef<MeshFailure>>,
    /// Streaming subscribers, notified on both stop and failure events as
    /// well as periodic heartbeats.
    subscribers: HashSet<hyperactor::PortRef<Option<MeshFailure>>>,
}

impl HealthState {
    fn new(
        statuses: HashMap<Point, resource::Status>,
        owner: Option<hyperactor::PortRef<MeshFailure>>,
    ) -> Self {
        Self {
            statuses: statuses
                .into_iter()
                .map(|(point, status)| (point, (status, 0)))
                .collect(),
            unhealthy_event: None,
            crashed_ranks: HashMap::new(),
            owner,
            subscribers: HashSet::new(),
        }
    }

    /// Try to update the status at `point`. Returns `true` if the status
    /// was newly inserted or changed; `false` if dominated by a higher
    /// generation or unchanged.
    fn maybe_update(&mut self, point: Point, status: resource::Status, generation: u64) -> bool {
        use std::collections::hash_map::Entry;
        match self.statuses.entry(point) {
            Entry::Occupied(mut entry) => {
                let (old_status, old_gen) = entry.get();
                // Once a resource enters a terminating state (including Stopping),
                // its status is frozen — later updates are ignored.
                if old_status.is_terminating() || *old_gen > generation {
                    return false;
                }
                let changed = *old_status != status;
                *entry.get_mut() = (status, generation);
                changed
            }
            Entry::Vacant(entry) => {
                entry.insert((status, generation));
                true
            }
        }
    }

    /// True when every tracked rank has reached a terminating status.
    fn all_terminating(&self) -> bool {
        self.statuses.values().all(|(s, _)| s.is_terminating())
    }

    /// True when at least one tracked rank has reached a terminating status.
    fn any_terminating(&self) -> bool {
        self.statuses.values().any(|(s, _)| s.is_terminating())
    }

    /// Apply status updates from polled resource states and invoke `on_change`
    /// for each rank whose status actually changed. The point passed to
    /// `on_change` is the created rank, *not* the rank of the possibly sliced
    /// input mesh. Returns `true` if `on_change` reported at least one
    /// notification (used to decide whether a heartbeat is needed).
    pub(crate) fn apply_updates_and_notify<S: Clone + 'static>(
        &mut self,
        states: &ValueMesh<resource::State<S>>,
        mut on_change: impl FnMut(resource::State<S>, &mut HealthState) -> bool,
    ) -> bool {
        let mut did_notify = false;
        for (point, state) in states.iter() {
            let status = state.status.clone();
            let generation = state.generation;
            if self.maybe_update(point, status, generation) && on_change(state, self) {
                did_notify = true;
            }
        }
        did_notify
    }
}

/// Outcome of the mesh-specific polling phase inside `CheckState`.
pub enum PollResult {
    /// An error or early condition was handled internally; just reschedule.
    Reschedule,
    /// States were polled and processed. `did_notify` is true if at least
    /// one subscriber/owner notification was sent.
    Processed { did_notify: bool },
}

/// Compute the keepalive expiry from `MESH_ORPHAN_TIMEOUT`, or `None` if
/// the timeout is disabled.
fn compute_keepalive() -> Option<SystemTime> {
    hyperactor_config::global::get(MESH_ORPHAN_TIMEOUT).map(|d| SystemTime::now() + d)
}

/// Subscribe me to updates about a mesh. If a duplicate is subscribed, only a single
/// message is sent.
/// Will send None if there are no failures on the mesh periodically. This guarantees
/// the listener that the controller is still alive. Make sure to filter such events
/// out as not useful.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct Subscribe(pub hyperactor::PortRef<Option<MeshFailure>>);
wirevalue::register_type!(Subscribe);

/// Unsubscribe me to future updates about a mesh. Should be the same port used in
/// the Subscribe message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct Unsubscribe(pub hyperactor::PortRef<Option<MeshFailure>>);
wirevalue::register_type!(Unsubscribe);

/// Query the number of active supervision subscribers on this controller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct GetSubscriberCount(#[binding(include)] pub hyperactor::PortRef<usize>);
wirevalue::register_type!(GetSubscriberCount);

/// Check state of the actors in the mesh. This is used as a self message to
/// periodically check.
/// Stores the next time we expect to start running a check state message.
/// Used to check for stalls in message handling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct CheckState(pub SystemTime);
wirevalue::register_type!(CheckState);

declare_attrs! {
    /// If present in a message header, the message is from an ActorMeshController
    /// to a subscriber and can be safely dropped if it is returned as undeliverable.
    pub attr ACTOR_MESH_SUBSCRIBER_MESSAGE: bool;
}

fn send_subscriber_message(
    cx: &impl context::Actor,
    subscriber: &hyperactor::PortRef<Option<MeshFailure>>,
    message: MeshFailure,
) {
    let mut headers = Flattrs::new();
    headers.set(ACTOR_MESH_SUBSCRIBER_MESSAGE, true);
    subscriber.post_with_headers(cx, headers, Some(message.clone()));
    tracing::info!(event = %message, "sent supervision failure message to subscriber {}", subscriber.port_addr());
}

/// Like send_state_change, but when there was no state change that occurred.
/// Will send a None message to subscribers, and there is no state to change.
/// Is not sent to the owner, because the owner is only watching for failures.
/// Should be called once every so often so subscribers can discern the difference
/// between "no messages because no errors" and "no messages because controller died".
/// Without sending these hearbeats, subscribers will assume the mesh is dead.
fn send_heartbeat(cx: &impl context::Actor, health_state: &HealthState) {
    tracing::debug!(
        num_subscribers = health_state.subscribers.len(),
        "sending heartbeat to subscribers",
    );

    for subscriber in health_state.subscribers.iter() {
        let mut headers = Flattrs::new();
        headers.set(ACTOR_MESH_SUBSCRIBER_MESSAGE, true);
        subscriber.post_with_headers(cx, headers, None);
    }
}

/// Sends a MeshFailure to the owner and subscribers of this mesh,
/// and changes the health state stored unhealthy_event.
/// Owners are sent a message only for Failure events, not for Stopped events.
/// Subscribers are sent both Stopped and Failure events.
fn send_state_change(
    cx: &impl context::Actor,
    rank: usize,
    event: ActorSupervisionEvent,
    mesh_name: &ResourceId,
    is_proc_stopped: bool,
    health_state: &mut HealthState,
) {
    // This does not include the Stopped status, which is a state that occurs when the
    // user calls stop() on a proc or actor mesh.
    let is_failed = event.is_error();
    if is_failed {
        tracing::warn!(
            name = "SupervisionEvent",
            actor_mesh = %mesh_name,
            %event,
            "detected supervision error on monitored mesh: name={mesh_name}",
        );
    } else {
        tracing::debug!(
            name = "SupervisionEvent",
            actor_mesh = %mesh_name,
            %event,
            "detected non-error supervision event on monitored mesh: name={mesh_name}",
        );
    }

    let failure_message = MeshFailure {
        actor_mesh_name: Some(mesh_name.to_string()),
        event: event.clone(),
        crashed_ranks: vec![rank],
    };
    health_state.crashed_ranks.insert(rank, event.clone());
    health_state.unhealthy_event = Some(if is_proc_stopped {
        Unhealthy::StreamClosed(failure_message.clone())
    } else {
        Unhealthy::Crashed(failure_message.clone())
    });
    // Send a notification to the owning actor of this mesh, if there is one.
    // Don't send a message to the owner for non-failure events such as "stopped".
    // Those events are always initiated by the owner, who don't need to be
    // told that they were stopped.
    if is_failed {
        if let Some(owner) = &health_state.owner {
            owner.post(cx, failure_message.clone());
            tracing::info!(actor_mesh = %mesh_name, %event, "sent supervision failure message to owner {}", owner.port_addr());
        }
    }
    // Subscribers get all messages, even for non-failures like Stopped, because
    // they need to know if the owner stopped the mesh.
    for subscriber in health_state.subscribers.iter() {
        send_subscriber_message(cx, subscriber, failure_message.clone());
    }
}

fn actor_state_to_supervision_events(
    state: resource::State<ActorState>,
) -> (usize, Vec<ActorSupervisionEvent>) {
    let (rank, actor_id, events) = match state.state {
        Some(inner) => (
            inner.create_rank,
            Some(inner.actor_id),
            inner.supervision_events.clone(),
        ),
        None => (0, None, vec![]),
    };
    let events = match state.status {
        // If the actor was killed, it might not have a Failed status
        // or supervision events, and it can't tell us which rank
        resource::Status::NotExist | resource::Status::Stopped | resource::Status::Timeout(_) => {
            // it was.
            if !events.is_empty() {
                events
            } else {
                vec![ActorSupervisionEvent::new(
                    actor_id.expect("actor_id is None"),
                    None,
                    ActorStatus::Stopped(
                        format!(
                            "actor status is {}; actor may have been killed",
                            state.status
                        )
                        .to_string(),
                    ),
                    None,
                )]
            }
        }
        resource::Status::Failed(_) => events,
        // All other states are successful.
        _ => vec![],
    };
    (rank, events)
}

/// Map a process-level [`ProcStatus`] to an actor-level [`ActorStatus`].
///
/// When the supervision poll discovers that a process is terminating, this
/// function decides whether to treat it as a clean stop or a failure.
/// Notably, [`ProcStatus::Stopping`] (SIGTERM sent, process not yet exited)
/// is mapped to [`ActorStatus::Stopped`] rather than [`ActorStatus::Failed`]
/// so that a graceful shutdown in progress does not trigger unhandled
/// supervision errors.
fn proc_status_to_actor_status(proc_status: Option<ProcStatus>) -> ActorStatus {
    match proc_status {
        Some(ProcStatus::Stopped { exit_code: 0, .. }) => {
            ActorStatus::Stopped("process exited cleanly".to_string())
        }
        Some(ProcStatus::Stopped { exit_code, .. }) => {
            ActorStatus::Failed(ActorErrorKind::Generic(format!(
                "the process this actor was running on exited with non-zero code {}",
                exit_code
            )))
        }
        // Stopping is a transient state during graceful shutdown. Treat it the
        // same as a clean stop rather than a failure.
        Some(ProcStatus::Stopping { .. }) => {
            ActorStatus::Stopped("process is stopping".to_string())
        }
        // Conservatively treat lack of status as stopped
        None => ActorStatus::Stopped("no status received from process".to_string()),
        Some(status) => ActorStatus::Failed(ActorErrorKind::Generic(format!(
            "the process this actor was running on failed: {}",
            status
        ))),
    }
}

fn format_system_time(time: SystemTime) -> String {
    let datetime: chrono::DateTime<chrono::Local> = time.into();
    datetime.to_rfc3339()
}

/// Log a warning and bump `counter` if the supervision loop is running late.
///
/// "Late" means the current wall-clock time exceeds `expected_time` by more
/// than one full poll interval, i.e. 2x the expected period.
fn check_stall(expected_time: SystemTime, actor_id: &hyperactor::ActorId, counter: &Counter<u64>) {
    if SystemTime::now()
        <= expected_time + hyperactor_config::global::get(SUPERVISION_POLL_FREQUENCY)
    {
        return;
    }
    let expected_time = format_system_time(expected_time);
    counter.add(
        1,
        kv_pairs!("actor_id" => actor_id.to_string(), "expected_time" => expected_time.clone()),
    );
    tracing::warn!(
        %actor_id,
        "Handler<CheckState> is stalled, expected at {}",
        expected_time,
    );
}

/// Mesh-specific behavior required by the generic `ResourceController`.
///
/// Each variant of resource mesh (actor, proc) implements this trait to
/// provide the details that cannot be shared by the generic controller:
/// the state type carried in `resource::State<_>`, how to query or stream
/// that state from the underlying agents, how to stop the resources, and
/// how to notify observers when the state changes.
#[async_trait]
pub trait Controlled: Clone + Debug + Send + Sync + 'static {
    /// Inner payload carried in `resource::State<Self::StateInner>`.
    type StateInner: RemoteMessage + Clone + Debug + 'static;

    /// Counter bumped when the supervision loop detects a stall.
    fn stall_counter() -> &'static Counter<u64>;

    /// The mesh's resource identifier.
    fn id(&self) -> &ResourceId;

    /// The region of ranks in this mesh.
    fn region(&self) -> &ndslice::Region;

    /// Subscribe the given port to `StreamState<StateInner>` updates from
    /// the underlying agents.
    fn subscribe_to_stream(
        &self,
        cx: &impl context::Actor,
        subscriber: hyperactor::PortRef<resource::State<Self::StateInner>>,
    ) -> anyhow::Result<()>;

    /// Forward a `WaitRankStatus` message to the underlying agents.
    fn forward_wait_rank_status(
        &self,
        cx: &impl context::Actor,
        msg: resource::WaitRankStatus,
    ) -> anyhow::Result<()>;

    /// Mesh-specific polling step for the supervision loop. Implementations
    /// may do pre-checks (such as the actor mesh's proc-aliveness check)
    /// before querying rank states; updates to `health_state` happen
    /// in-place. `supervision_display_name` is used for synthesised
    /// supervision events (e.g., when a proc dies).
    async fn poll_states(
        &self,
        cx: &impl context::Actor,
        supervision_display_name: &str,
        health_state: &mut HealthState,
    ) -> PollResult;

    /// Process a single streamed or polled state. Updates the health state
    /// and notifies owner/subscribers as appropriate. Returns `true` if a
    /// notification was emitted (used to suppress heartbeats).
    fn process_state(
        &self,
        cx: &impl context::Actor,
        state: resource::State<Self::StateInner>,
        health_state: &mut HealthState,
    ) -> bool;

    /// Perform the mesh-specific stop: issue stop messages to the underlying
    /// agents and, where appropriate, update `health_state` and notify
    /// subscribers. The caller has already taken the monitor and logged.
    async fn handle_stop_request(
        &self,
        cx: &impl context::Actor,
        supervision_display_name: &str,
        reason: String,
        health_state: &mut HealthState,
    ) -> anyhow::Result<()>;

    /// Stop this mesh on controller cleanup (when `Stop` was not received
    /// but the actor is shutting down).
    async fn cleanup_stop(&self, cx: &impl context::Actor, reason: String) -> anyhow::Result<()>;
}

/// Generic controller for a mesh of resources. Currently instantiated as
/// `ActorMeshController<A> = ResourceController<ActorMeshRef<A>>`. All
/// shared behavior lives here; mesh-specific behavior is delegated through
/// the `Controlled` trait.
///
/// `resource::mesh::Spec<()>` and `resource::mesh::State<()>` (instead of
/// `Spec<T::Spec>` / `State<T::StateInner>`) are used because the
/// controller participates in the mesh `resource` protocol only at the
/// outer layer: callers of `GetState` on the controller want the
/// per-rank statuses and the mesh-wide status that `resource::mesh::State`
/// already carries, not the inner `T::StateInner` payload (which is
/// available rank-by-rank via the `resource::State<T::StateInner>` stream).
/// The unit type is the explicit "no extra payload" choice.
#[hyperactor::export(
    handlers=[
        Subscribe,
        Unsubscribe,
        GetSubscriberCount,
        CheckState,
        resource::WaitRankStatus,
        resource::CreateOrUpdate<resource::mesh::Spec<()>>,
        resource::GetState<resource::mesh::State<()>>,
        resource::Stop,
        resource::State<T::StateInner>,
    ]
)]
pub struct ResourceController<T: Controlled> {
    mesh: T,
    /// Supervision display name used in telemetry and fake supervision
    /// events. If `None`, falls back to `mesh.id()`.
    supervision_display_name: Option<String>,
    /// Shared health state for the monitor and responding to queries.
    health_state: HealthState,
    /// The monitor which continuously runs in the background to refresh
    /// state. If None, the controller has stopped monitoring.
    monitor: Option<()>,
}

/// Controller for an actor mesh.
pub type ActorMeshController<A> = ResourceController<ActorMeshRef<A>>;

impl<T: Controlled> ResourceController<T> {
    /// Create a new controller over the given mesh.
    pub(crate) fn new(
        mesh: T,
        supervision_display_name: Option<String>,
        owner: Option<hyperactor::PortRef<MeshFailure>>,
        initial_statuses: ValueMesh<resource::Status>,
    ) -> Self {
        Self {
            mesh,
            supervision_display_name,
            health_state: HealthState::new(initial_statuses.iter().collect(), owner),
            monitor: None,
        }
    }

    /// The display name to use for supervision events and telemetry.
    pub(crate) fn supervision_display_name(&self) -> String {
        self.supervision_display_name
            .clone()
            .unwrap_or_else(|| self.mesh.id().to_string())
    }

    /// Schedule the next `CheckState` self-message if the monitor is active.
    ///
    /// `send_fn` bridges the type gap: the caller passes a closure that
    /// captures the typed `Instance`/`Context` and calls
    /// `post_after`.
    fn schedule_next_check(&self, send_fn: impl FnOnce(CheckState, Duration)) {
        if self.monitor.is_some() {
            let delay = hyperactor_config::global::get(SUPERVISION_POLL_FREQUENCY);
            send_fn(CheckState(SystemTime::now() + delay), delay);
        }
    }

    /// Derive the mesh-level status from health state and monitor presence.
    fn mesh_status(&self) -> resource::Status {
        if let Some(Unhealthy::Crashed(e)) = &self.health_state.unhealthy_event {
            resource::Status::Failed(e.to_string())
        } else if let Some(Unhealthy::StreamClosed(_)) = &self.health_state.unhealthy_event {
            resource::Status::Stopped
        } else if self.monitor.is_none() {
            resource::Status::Stopped
        } else {
            resource::Status::Running
        }
    }

    /// Build and send the `GetState<resource::mesh::State<()>>` response.
    fn handle_get_state_msg(
        &self,
        cx: &impl context::Actor,
        message: resource::GetState<resource::mesh::State<()>>,
    ) -> anyhow::Result<()> {
        let status = self.mesh_status();
        let mut statuses = self
            .health_state
            .statuses
            .iter()
            .map(|(p, (s, _))| (p.clone(), s.clone()))
            .collect::<Vec<_>>();
        statuses.sort_by_key(|(p, _)| p.rank());
        let statuses: ValueMesh<resource::Status> =
            statuses
                .into_iter()
                .map(|(_, s)| s)
                .collect_mesh::<ValueMesh<_>>(self.mesh.region().clone())?;
        let state = resource::mesh::State {
            statuses,
            state: (),
        };
        message.reply.post(
            cx,
            resource::State {
                id: message.id,
                status,
                state: Some(state),
                generation: 0,
                timestamp: SystemTime::now(),
            },
        );
        Ok(())
    }

    /// Drop the monitor if every tracked rank has reached a terminal status.
    fn stop_if_all_terminating(&mut self) {
        if self.health_state.all_terminating() {
            self.monitor.take();
        }
    }

    async fn handle_check_state(
        &mut self,
        cx: &Context<'_, Self>,
        expected_time: SystemTime,
    ) -> anyhow::Result<()>
    where
        resource::State<T::StateInner>: RemoteMessage,
    {
        if self.monitor.is_none() {
            return Ok(());
        }
        check_stall(expected_time, cx.self_addr().id(), T::stall_counter());

        let display = self.supervision_display_name();
        let result = self
            .mesh
            .poll_states(cx, &display, &mut self.health_state)
            .await;

        match result {
            PollResult::Reschedule => {
                self.schedule_next_check(|msg, delay| cx.post_after(cx, msg, delay));
            }
            PollResult::Processed { did_notify } => {
                // Suppress heartbeats once any rank is terminating: the mesh is on
                // its way down and subscribers will get a real state-change message
                // for the terminal transition.
                if !did_notify && !self.health_state.any_terminating() {
                    send_heartbeat(cx, &self.health_state);
                }
                if !self.health_state.all_terminating() {
                    self.schedule_next_check(|msg, delay| cx.post_after(cx, msg, delay));
                } else {
                    self.monitor.take();
                }
            }
        }
        Ok(())
    }
}

impl<T: Controlled> Debug for ResourceController<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceController")
            .field("mesh", &self.mesh)
            .field("health_state", &self.health_state)
            .field("monitor", &self.monitor)
            .finish()
    }
}

impl<T: Controlled> resource::mesh::Mesh for ResourceController<T> {
    type Spec = ();
    type State = ();
}

#[async_trait]
impl<T: Controlled> Actor for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();

        // Subscribe to streaming state updates from the underlying agents so
        // the controller receives state changes in real time, complementing
        // the existing polling loop. Avoid binding the handle here: the
        // controller's exported ports are bound when the mesh installs the
        // ActorRef after spawn. Binding the same handle twice panics.
        //
        // This must happen before starting the monitor so that the first
        // CheckState does not race the initial StreamState cast.
        //
        // TODO(SF, 2026-03-32, T261106175): follow up in hyperactor on bind
        // semantics here. `cx.port()` plus later actor-ref export currently
        // hits `bind()` -> `bind_handler_port()` on the same handle, and
        // `bind_handler_port()` still panics on an already-bound handle. This
        // workaround uses `attest_handler_port(...)` to avoid the eager
        // bind, but the longer-term fix is to clarify whether that bind
        // path should be idempotent and eliminate the need for attestation
        // here.
        let subscriber =
            hyperactor::PortRef::<resource::State<T::StateInner>>::attest_handler_port(
                &this.self_addr().clone(),
            )
            .unsplit();
        self.mesh.subscribe_to_stream(this, subscriber)?;

        // Start the monitor task.
        self.monitor = Some(());
        self.schedule_next_check(|msg, delay| this.post_after(this, msg, delay));

        let owner = if let Some(owner) = &self.health_state.owner {
            owner.to_string()
        } else {
            String::from("None")
        };
        tracing::info!(
            actor_id = %this.self_addr(),
            %owner,
            "started resource controller for {}",
            self.mesh.id()
        );
        Ok(())
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        if self.monitor.take().is_some() {
            tracing::info!(
                actor_id = %this.self_addr(),
                mesh = %self.mesh.id(),
                "starting cleanup for ResourceController, stopping mesh",
            );
            self.mesh
                .cleanup_stop(this, "resource controller cleanup".to_string())
                .await?;
        }
        Ok(())
    }

    async fn handle_undeliverable_message(
        &mut self,
        cx: &Instance<Self>,
        mut envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        envelope = update_undeliverable_envelope_for_casting(envelope);
        let Some(returned) = envelope.as_message() else {
            return handle_undeliverable_message(cx, envelope);
        };
        if let Some(true) = returned.headers().get(ACTOR_MESH_SUBSCRIBER_MESSAGE) {
            // Remove from the subscriber list (if it existed) so we don't
            // send to this subscriber again.
            // NOTE: The only part of the port that is used for equality checks is
            // the port id, so create a new one just for the comparison.
            let dest_port_id = returned.dest().clone();
            let port = hyperactor::PortRef::<Option<MeshFailure>>::attest(dest_port_id);
            let did_exist = self.health_state.subscribers.remove(&port);
            if did_exist {
                tracing::debug!(
                    actor_id = %cx.self_addr(),
                    num_subscribers = self.health_state.subscribers.len(),
                    "ResourceController: removed subscriber {} from mesh controller",
                    port.port_addr()
                );
            }
            Ok(())
        } else if returned.headers().get(CAST_ACTOR_MESH_ID).is_some() {
            // A cast message we sent (e.g. StreamState or KeepaliveGetState)
            // was returned by the CommActor because it could not be forwarded.
            // This is expected when the network session is broken. Log and
            // continue — the supervision polling loop will detect the failure.
            tracing::warn!(
                actor_id = %cx.self_addr(),
                dest = %returned.dest(),
                "ResourceController: ignoring undeliverable cast message",
            );
            Ok(())
        } else {
            handle_undeliverable_message(cx, envelope)
        }
    }
}

#[async_trait]
impl<T: Controlled> Handler<Subscribe> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn handle(&mut self, cx: &Context<Self>, message: Subscribe) -> anyhow::Result<()> {
        // If there are any crashed ranks, replay a failure event so the new
        // subscriber learns about the current health state. We send a single
        // message with all crashed ranks so the subscriber's filter can check
        // overlap with its slice region. This avoids the watch-channel
        // coalescing problem (sending per-rank messages would lose all but
        // the last one).
        if let Some(unhealthy) = &self.health_state.unhealthy_event {
            let msg = match unhealthy {
                Unhealthy::StreamClosed(msg) | Unhealthy::Crashed(msg) => msg,
            };
            let mut replay_msg = msg.clone();
            replay_msg.crashed_ranks = self.health_state.crashed_ranks.keys().copied().collect();
            send_subscriber_message(cx, &message.0, replay_msg);
        }
        let port_id = message.0.port_addr().clone();
        if self.health_state.subscribers.insert(message.0) {
            tracing::debug!(
                actor_id = %cx.self_addr(),
                num_subscribers = self.health_state.subscribers.len(),
                "added subscriber {} to mesh controller",
                port_id
            );
        }
        Ok(())
    }
}

#[async_trait]
impl<T: Controlled> Handler<Unsubscribe> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn handle(&mut self, cx: &Context<Self>, message: Unsubscribe) -> anyhow::Result<()> {
        if self.health_state.subscribers.remove(&message.0) {
            tracing::debug!(
                actor_id = %cx.self_addr(),
                num_subscribers = self.health_state.subscribers.len(),
                "removed subscriber {} from mesh controller",
                message.0.port_addr()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl<T: Controlled> Handler<GetSubscriberCount> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: GetSubscriberCount,
    ) -> anyhow::Result<()> {
        message.0.post(cx, self.health_state.subscribers.len());
        Ok(())
    }
}

#[async_trait]
impl<T: Controlled> Handler<resource::CreateOrUpdate<resource::mesh::Spec<()>>>
    for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    /// Currently a no-op as there's nothing to create or update, but allows
    /// `ResourceController` to implement the resource mesh behavior.
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        _message: resource::CreateOrUpdate<resource::mesh::Spec<()>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<T: Controlled> Handler<resource::GetState<resource::mesh::State<()>>> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: resource::GetState<resource::mesh::State<()>>,
    ) -> anyhow::Result<()> {
        self.handle_get_state_msg(cx, message)
    }
}

#[async_trait]
impl<T: Controlled> Handler<resource::Stop> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn handle(&mut self, cx: &Context<Self>, message: resource::Stop) -> anyhow::Result<()> {
        let mesh_name = self.mesh.id().clone();
        tracing::info!(
            name = "ResourceControllerStatus",
            %mesh_name,
            reason = %message.reason,
            "stopping mesh"
        );
        if self.monitor.take().is_none() {
            tracing::debug!(
                actor_id = %cx.self_addr(),
                %mesh_name,
                "duplicate stop request, mesh is already stopped",
            );
            return Ok(());
        }
        let display = self.supervision_display_name();
        self.mesh
            .handle_stop_request(cx, &display, message.reason, &mut self.health_state)
            .await
    }
}

#[async_trait]
impl<T: Controlled> Handler<resource::WaitRankStatus> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    /// Forward WaitRankStatus to the underlying agents. Each agent replies
    /// directly to the caller's accumulator port when its resource reaches
    /// the requested status.
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: resource::WaitRankStatus,
    ) -> anyhow::Result<()> {
        self.mesh.forward_wait_rank_status(cx, msg)
    }
}

#[async_trait]
impl<T: Controlled> Handler<CheckState> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        CheckState(expected_time): CheckState,
    ) -> Result<(), anyhow::Error> {
        self.handle_check_state(cx, expected_time).await
    }
}

#[async_trait]
impl<T: Controlled> Handler<resource::State<T::StateInner>> for ResourceController<T>
where
    resource::State<T::StateInner>: RemoteMessage,
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        state: resource::State<T::StateInner>,
    ) -> anyhow::Result<()> {
        self.mesh.process_state(cx, state, &mut self.health_state);
        self.stop_if_all_terminating();
        Ok(())
    }
}

/// `Controlled` implementation for an actor mesh.
#[async_trait]
impl<A: Referable> Controlled for ActorMeshRef<A> {
    type StateInner = ActorState;

    fn stall_counter() -> &'static Counter<u64> {
        &ACTOR_MESH_CONTROLLER_SUPERVISION_STALLS
    }

    fn id(&self) -> &ResourceId {
        ActorMeshRef::id(self).resource_id()
    }

    fn region(&self) -> &ndslice::Region {
        ndslice::view::Ranked::region(self)
    }

    fn subscribe_to_stream(
        &self,
        cx: &impl context::Actor,
        subscriber: hyperactor::PortRef<resource::State<ActorState>>,
    ) -> anyhow::Result<()> {
        self.proc_mesh().agent_mesh().cast(
            cx,
            resource::StreamState::<ActorState> {
                id: ActorMeshRef::id(self).resource_id().clone(),
                subscriber,
            },
        )?;
        Ok(())
    }

    fn forward_wait_rank_status(
        &self,
        cx: &impl context::Actor,
        msg: resource::WaitRankStatus,
    ) -> anyhow::Result<()> {
        self.proc_mesh().agent_mesh().cast(cx, msg)?;
        Ok(())
    }

    async fn poll_states(
        &self,
        cx: &impl context::Actor,
        supervision_display_name: &str,
        health_state: &mut HealthState,
    ) -> PollResult {
        let mesh_name = Controlled::id(self);

        // Actor-specific: first check if the proc mesh is dead before
        // trying to query their agents.
        let proc_states = self.proc_mesh().proc_states(cx, None).await;
        if let Err(e) = proc_states {
            send_state_change(
                cx,
                0,
                ActorSupervisionEvent::new(
                    cx.instance().self_addr().clone(),
                    None,
                    ActorStatus::generic_failure(format!(
                        "unable to query for proc states: {:?}",
                        e
                    )),
                    None,
                ),
                mesh_name,
                false,
                health_state,
            );
            return PollResult::Reschedule;
        }
        if let Some(proc_states) = proc_states.unwrap() {
            // Check if the proc mesh is still alive.
            if let Some((point, state)) = proc_states
                .iter()
                .find(|(_rank, state)| state.status.is_terminating())
            {
                // TODO: allow "actor supervision event" to be general, and
                // make the proc failure the cause. It is a hack to try to determine
                // the correct status based on process exit status.
                let actor_status =
                    proc_status_to_actor_status(state.state.and_then(|s| s.proc_status));
                let display = crate::actor_display_name(supervision_display_name, &point);
                send_state_change(
                    cx,
                    point.rank(),
                    ActorSupervisionEvent::new(
                        // Attribute this to the monitored actor, even if the underlying
                        // cause is a proc_failure. We propagate the cause explicitly.
                        self.get(point.rank()).unwrap().actor_addr().clone(),
                        Some(display),
                        actor_status,
                        None,
                    ),
                    mesh_name,
                    true,
                    health_state,
                );
                return PollResult::Reschedule;
            }
        }

        // Query resource states with keepalive.
        let actor_states = self
            .actor_states_with_keepalive(cx, compute_keepalive())
            .await;
        match actor_states {
            Err(e) => {
                send_state_change(
                    cx,
                    0,
                    ActorSupervisionEvent::new(
                        cx.instance().self_addr().clone(),
                        Some(supervision_display_name.to_string()),
                        ActorStatus::generic_failure(format!(
                            "unable to query for actor states: {:?}",
                            e
                        )),
                        None,
                    ),
                    mesh_name,
                    false,
                    health_state,
                );
                PollResult::Reschedule
            }
            Ok(states) => {
                let did_notify =
                    health_state.apply_updates_and_notify(&states, |state, health_state| {
                        let (rank, events) = actor_state_to_supervision_events(state);
                        if events.is_empty() {
                            return false;
                        }
                        send_state_change(
                            cx,
                            rank,
                            events[0].clone(),
                            mesh_name,
                            false,
                            health_state,
                        );
                        true
                    });
                PollResult::Processed { did_notify }
            }
        }
    }

    fn process_state(
        &self,
        cx: &impl context::Actor,
        state: resource::State<ActorState>,
        health_state: &mut HealthState,
    ) -> bool {
        let (rank, events) = actor_state_to_supervision_events(state.clone());
        let Ok(point) = Controlled::region(self).extent().point_of_rank(rank) else {
            return false;
        };

        let changed = health_state.maybe_update(point, state.status, state.generation);

        if changed && !events.is_empty() {
            send_state_change(
                cx,
                rank,
                events[0].clone(),
                Controlled::id(self),
                false,
                health_state,
            );
            true
        } else {
            false
        }
    }

    async fn handle_stop_request(
        &self,
        cx: &impl context::Actor,
        _supervision_display_name: &str,
        reason: String,
        health_state: &mut HealthState,
    ) -> anyhow::Result<()> {
        let mesh_name = Controlled::id(self);
        tracing::info!(
            actor_id = %cx.instance().self_addr(),
            actor_mesh = %mesh_name,
            "forwarding stop request from ActorMeshController to proc mesh"
        );

        // Let the client know that the controller has stopped. Since the monitor
        // is cancelled, it will not alert the owner or the subscribers.
        // We use a placeholder rank to get an actor id, but really there should
        // be a stop event for every rank in the mesh. Since every rank has the
        // same owner, we assume the rank doesn't matter, and the owner can just
        // assume the stop happened on all actors.
        let rank = 0usize;
        let event = ActorSupervisionEvent::new(
            self.get(rank)
                .expect("mesh must have at least one rank")
                .actor_addr()
                .clone(),
            None,
            ActorStatus::Stopped("ActorMeshController received explicit stop request".to_string()),
            None,
        );
        let failure_message = MeshFailure {
            actor_mesh_name: Some(mesh_name.to_string()),
            event,
            crashed_ranks: vec![],
        };
        health_state.unhealthy_event = Some(Unhealthy::StreamClosed(failure_message.clone()));
        // We don't send a message to the owner on stops, because only the owner
        // can request a stop. We just send to subscribers instead, as they did
        // not request the stop themselves.
        for subscriber in health_state.subscribers.iter() {
            send_subscriber_message(cx, subscriber, failure_message.clone());
        }

        // max_rank and extent are only needed for the deprecated RankedValues.
        // TODO: add cmp::Ord to Point for a max() impl.
        let max_rank = health_state.statuses.keys().map(|p| p.rank()).max();
        let extent = health_state
            .statuses
            .keys()
            .next()
            .map(|p| p.extent().clone());

        // Cannot use "ActorMesh::stop" as it tries to message the controller.
        let result = self
            .proc_mesh()
            .stop_actor_by_id(cx, ActorMeshRef::id(self).clone(), reason)
            .await;

        match result {
            Ok(statuses) => {
                // All stops successful, set actor status on health state.
                for (rank, status) in statuses.iter() {
                    health_state
                        .statuses
                        .entry(rank)
                        .and_modify(move |s| *s = (status, u64::MAX));
                }
            }
            Err(crate::Error::ActorStopError { statuses }) => {
                if let Some(max_rank) = max_rank {
                    let extent = extent.expect("no actors in mesh");
                    for (rank, status) in statuses.materialized_iter(max_rank).enumerate() {
                        *health_state
                            .statuses
                            .get_mut(&extent.point_of_rank(rank).expect("illegal rank"))
                            .unwrap() = (status.clone(), u64::MAX);
                    }
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }

        tracing::info!(
            actor_id = %cx.instance().self_addr(),
            actor_mesh = %mesh_name,
            "stopped mesh"
        );
        Ok(())
    }

    async fn cleanup_stop(&self, cx: &impl context::Actor, reason: String) -> anyhow::Result<()> {
        self.proc_mesh()
            .stop_actor_by_id(cx, ActorMeshRef::id(self).clone(), reason)
            .await?;
        Ok(())
    }
}

/// Controller for a proc mesh.
pub(crate) type ProcMeshController = ResourceController<ProcMeshRef>;

/// `Controlled` implementation for a proc mesh.
#[async_trait]
impl Controlled for ProcMeshRef {
    type StateInner = crate::host_mesh::host_agent::ProcState;

    fn stall_counter() -> &'static Counter<u64> {
        &PROC_MESH_CONTROLLER_SUPERVISION_STALLS
    }

    fn id(&self) -> &ResourceId {
        ProcMeshRef::id(self).resource_id()
    }

    fn region(&self) -> &ndslice::Region {
        ndslice::view::Ranked::region(self)
    }

    fn subscribe_to_stream(
        &self,
        cx: &impl context::Actor,
        subscriber: hyperactor::PortRef<resource::State<Self::StateInner>>,
    ) -> anyhow::Result<()> {
        // Send one StreamState per proc to its host agent.
        for proc_id in self.proc_ids() {
            let proc_resource_id = ResourceId::new(proc_id.uid().clone(), proc_id.label().cloned());
            let host = crate::host_mesh::HostRef(proc_id.addr().clone());
            host.mesh_agent().post(
                cx,
                resource::StreamState::<Self::StateInner> {
                    id: proc_resource_id,
                    subscriber: subscriber.clone(),
                },
            );
        }
        Ok(())
    }

    fn forward_wait_rank_status(
        &self,
        cx: &impl context::Actor,
        msg: resource::WaitRankStatus,
    ) -> anyhow::Result<()> {
        for proc_id in self.proc_ids() {
            let host = crate::host_mesh::HostRef(proc_id.addr().clone());
            host.mesh_agent().post(cx, msg.clone());
        }
        Ok(())
    }

    async fn poll_states(
        &self,
        cx: &impl context::Actor,
        supervision_display_name: &str,
        health_state: &mut HealthState,
    ) -> PollResult {
        let mesh_name = Controlled::id(self);

        let proc_states = self.proc_states(cx, compute_keepalive()).await;
        match proc_states {
            Err(e) => {
                send_state_change(
                    cx,
                    0,
                    ActorSupervisionEvent::new(
                        cx.instance().self_addr().clone(),
                        Some(supervision_display_name.to_string()),
                        ActorStatus::generic_failure(format!(
                            "unable to query for proc states: {:?}",
                            e
                        )),
                        None,
                    ),
                    mesh_name,
                    false,
                    health_state,
                );
                PollResult::Reschedule
            }
            Ok(None) => PollResult::Processed { did_notify: false },
            Ok(Some(states)) => {
                let did_notify =
                    health_state.apply_updates_and_notify(&states, |state, health_state| {
                        self.notify_proc_state_change(
                            cx,
                            supervision_display_name,
                            state,
                            health_state,
                        )
                    });
                PollResult::Processed { did_notify }
            }
        }
    }

    fn process_state(
        &self,
        cx: &impl context::Actor,
        state: resource::State<Self::StateInner>,
        health_state: &mut HealthState,
    ) -> bool {
        let Ok(point) = Controlled::region(self).extent().point_of_rank(
            state
                .state
                .as_ref()
                .map(|s| s.create_rank)
                .unwrap_or(usize::MAX),
        ) else {
            return false;
        };
        let changed = health_state.maybe_update(point, state.status.clone(), state.generation);
        if !changed {
            return false;
        }
        let display = Controlled::id(self).to_string();
        self.notify_proc_state_change(cx, &display, state, health_state)
    }

    async fn handle_stop_request(
        &self,
        cx: &impl context::Actor,
        _supervision_display_name: &str,
        reason: String,
        health_state: &mut HealthState,
    ) -> anyhow::Result<()> {
        let mesh_name = Controlled::id(self);
        tracing::info!(
            actor_id = %cx.instance().self_addr(),
            proc_mesh = %mesh_name,
            "ProcMeshController stopping proc mesh"
        );
        // Marker so subscribers know the mesh is being torn down on request.
        let event = ActorSupervisionEvent::new(
            cx.instance().self_addr().clone(),
            None,
            ActorStatus::Stopped("ProcMeshController received explicit stop request".to_string()),
            None,
        );
        let failure_message = MeshFailure {
            actor_mesh_name: Some(mesh_name.to_string()),
            event,
            crashed_ranks: vec![],
        };
        health_state.unhealthy_event = Some(Unhealthy::StreamClosed(failure_message.clone()));
        for subscriber in health_state.subscribers.iter() {
            send_subscriber_message(cx, subscriber, failure_message.clone());
        }

        let names = self.proc_ids().collect::<Vec<hyperactor::ProcAddr>>();
        let region = Ranked::region(self).clone();
        let Some(hosts) = self.hosts() else {
            return Ok(());
        };
        // stop_proc_mesh waits for every rank to reach a terminating state
        // before returning Ok, so we can apply its returned StatusMesh
        // verbatim. On error we still got per-rank statuses for whatever
        // ranks the host agents reported on; apply those too so health
        // state stays as accurate as we can make it.
        let max_rank = health_state.statuses.keys().map(|p| p.rank()).max();
        let extent = health_state
            .statuses
            .keys()
            .next()
            .map(|p| p.extent().clone());
        match hosts
            .stop_proc_mesh(cx, self.id(), names, region, reason)
            .await
        {
            Ok(statuses) => {
                for (rank, status) in statuses.iter() {
                    health_state
                        .statuses
                        .entry(rank)
                        .and_modify(move |s| *s = (status, u64::MAX));
                }
                Ok(())
            }
            Err(crate::Error::ProcMeshStopError { statuses }) => {
                if let (Some(max_rank), Some(extent)) = (max_rank, extent) {
                    for (rank, status) in statuses.materialized_iter(max_rank).enumerate() {
                        if let Ok(point) = extent.point_of_rank(rank) {
                            health_state
                                .statuses
                                .entry(point)
                                .and_modify(|s| *s = (status.clone(), u64::MAX));
                        }
                    }
                }
                Err(crate::Error::ProcMeshStopError { statuses }.into())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn cleanup_stop(&self, cx: &impl context::Actor, reason: String) -> anyhow::Result<()> {
        let names = self.proc_ids().collect::<Vec<hyperactor::ProcAddr>>();
        let region = Ranked::region(self).clone();
        if let Some(hosts) = self.hosts() {
            hosts
                .stop_proc_mesh(cx, self.id(), names, region, reason)
                .await?;
        }
        Ok(())
    }
}

impl ProcMeshRef {
    /// Translate a polled or streamed `State<ProcState>` into a supervision
    /// event on this proc-mesh controller. Returns `true` if a notification
    /// was sent (which suppresses the heartbeat path).
    fn notify_proc_state_change(
        &self,
        cx: &impl context::Actor,
        supervision_display_name: &str,
        state: resource::State<crate::host_mesh::host_agent::ProcState>,
        health_state: &mut HealthState,
    ) -> bool {
        let create_rank = state.state.as_ref().map(|s| s.create_rank);
        let actor_status = proc_status_to_actor_status(state.state.and_then(|s| s.proc_status));
        let event = ActorSupervisionEvent::new(
            cx.instance().self_addr().clone(),
            Some(supervision_display_name.to_string()),
            actor_status,
            None,
        );
        let rank = create_rank
            .and_then(|r| {
                ndslice::view::Ranked::region(self)
                    .extent()
                    .point_of_rank(r)
                    .ok()
            })
            .map(|p| p.rank())
            .unwrap_or(0);
        send_state_change(cx, rank, event, Controlled::id(self), true, health_state);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::time::Duration;

    use hyperactor::actor::ActorStatus;
    use hyperactor::id::Label;
    use ndslice::Extent;
    use ndslice::ViewExt;

    use super::SUPERVISION_POLL_FREQUENCY;
    use super::proc_status_to_actor_status;
    use crate::ActorMesh;
    use crate::bootstrap::ProcStatus;
    use crate::host_mesh::PROC_SPAWN_MAX_IDLE;
    use crate::mesh_id::ActorMeshId;
    use crate::mesh_id::HostMeshId;
    use crate::proc_agent::MESH_ORPHAN_TIMEOUT;
    use crate::resource;
    use crate::supervision::MeshFailure;
    use crate::test_utils::local_host_mesh;
    use crate::testactor;
    use crate::testing;

    /// Wraps a host mesh's shutdown guard and the spawned host child
    /// processes so tests can simulate an unclean host crash by killing
    /// the children directly rather than asking an in-mesh actor to
    /// `process::exit`, which can also tear down the test binary.
    #[cfg(fbcode_build)]
    struct TestHostMesh {
        guard: crate::host_mesh::HostMeshShutdownGuard,
        children: Vec<tokio::process::Child>,
    }

    #[cfg(fbcode_build)]
    impl TestHostMesh {
        async fn kill_hosts(&mut self) {
            for child in &mut self.children {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
            self.children.clear();
        }
    }

    #[cfg(fbcode_build)]
    impl std::ops::Deref for TestHostMesh {
        type Target = crate::host_mesh::HostMeshShutdownGuard;

        fn deref(&self) -> &Self::Target {
            &self.guard
        }
    }

    #[cfg(fbcode_build)]
    impl std::ops::DerefMut for TestHostMesh {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.guard
        }
    }

    /// Verify that actors spawned without a controller are cleaned up
    /// when their keepalive expiry lapses. We:
    ///   1. Enable the orphan timeout on the `ProcMeshAgent`.
    ///   2. Spawn actors as *system actors* (no `ActorMeshController`).
    ///   3. Send a single keepalive with a short expiry time.
    ///   4. Wait for the expiry to pass and `SelfCheck` to fire.
    ///   5. Assert that the actors are now stopped.
    #[tokio::test]
    async fn test_orphaned_actors_are_cleaned_up() {
        let config = hyperactor_config::global::lock();
        // Short orphan timeout so SelfCheck fires frequently.
        let _orphan = config.override_key(MESH_ORPHAN_TIMEOUT, Some(Duration::from_secs(1)));

        let instance = testing::instance();
        let host_mesh = local_host_mesh(2).await;
        let proc_mesh = host_mesh
            .spawn(instance, "test", Extent::unity(), None, None)
            .await
            .unwrap();

        let actor_name = ActorMeshId::unique(Label::new("orphan_test").unwrap());
        // Spawn as a system actor so no controller is created. This lets us
        // control keepalive messages directly without the controller
        // interfering.
        let actor_mesh: ActorMesh<testactor::TestActor> = proc_mesh
            .spawn_with_name(instance, actor_name.clone(), &(), None, true)
            .await
            .unwrap();
        assert!(
            actor_mesh.deref().extent().num_ranks() > 0,
            "should have spawned at least one actor"
        );

        // Send a keepalive with a short expiry. This is what the
        // ActorMeshController would normally do on each supervision poll.
        let states = proc_mesh
            .actor_states_with_keepalive(
                instance,
                actor_name.clone(),
                Some(std::time::SystemTime::now() + Duration::from_secs(2)),
            )
            .await
            .unwrap();
        // All actors should be running right now.
        for state in states.values() {
            assert_eq!(
                state.status,
                resource::Status::Running,
                "actor should be running before expiry"
            );
        }

        // Poll until all actors are stopped, rather than sleeping a
        // fixed duration. The expiry is 2s and SelfCheck fires every 1s,
        // so this should converge quickly, but we allow a generous timeout
        // for slow CI environments.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let states = proc_mesh
                .actor_states(instance, actor_name.clone())
                .await
                .unwrap();
            if states
                .values()
                .all(|s| s.status == resource::Status::Stopped)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for actors to be stopped after keepalive expiry"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Create a multi-process host mesh that propagates the current
    /// process's config overrides to child processes via Bootstrap.
    #[cfg(fbcode_build)]
    async fn host_mesh_with_config(n: usize) -> TestHostMesh {
        use hyperactor::channel::ChannelTransport;
        use tokio::process::Command;

        let program = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");
        let mut host_addrs = vec![];
        let mut children = Vec::new();
        for _ in 0..n {
            host_addrs.push(ChannelTransport::Unix.any());
        }

        for host in host_addrs.iter() {
            let mut cmd = Command::new(program.clone());
            let boot = crate::Bootstrap::Host {
                addr: host.clone(),
                command: None,
                config: Some(hyperactor_config::global::attrs()),
                exit_on_shutdown: false,
            };
            boot.to_env(&mut cmd);
            cmd.kill_on_drop(false);
            // SAFETY: pre_exec sets PR_SET_PDEATHSIG so the child is
            // cleaned up if the parent (test) process dies.
            unsafe {
                cmd.pre_exec(crate::bootstrap::install_pdeathsig_kill);
            }
            children.push(cmd.spawn().unwrap());
        }

        let host_mesh = crate::HostMeshRef::from_hosts(
            HostMeshId::unique(Label::new("test").unwrap()),
            host_addrs,
        );
        TestHostMesh {
            guard: crate::host_mesh::HostMesh::take(host_mesh).shutdown_guard(),
            children,
        }
    }

    /// Verify that actors are cleaned up via the orphan timeout when the
    /// `ActorMeshController`'s host process crashes. Unlike the system-actor
    /// test above, this spawns actors through a real controller (via
    /// `WrapperActor`) and then kills the controller's host process
    /// uncleanly. The agents on the surviving proc mesh detect the expired
    /// keepalive and stop the actors.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_orphaned_actors_cleaned_up_on_controller_crash() {
        let config = hyperactor_config::global::lock();
        let _orphan = config.override_key(MESH_ORPHAN_TIMEOUT, Some(Duration::from_secs(2)));
        let _poll = config.override_key(SUPERVISION_POLL_FREQUENCY, Duration::from_secs(1));
        let _proc_spawn = config.override_key(PROC_SPAWN_MAX_IDLE, Duration::from_secs(60));
        let _host_spawn = config.override_key(
            hyperactor::config::HOST_SPAWN_READY_TIMEOUT,
            Duration::from_secs(60),
        );

        let instance = testing::instance();
        let num_replicas = 1;

        // Host mesh for the test actors (these survive the crash).
        // host_mesh_with_config propagates config overrides to child
        // processes via Bootstrap, so agents boot with
        // MESH_ORPHAN_TIMEOUT=2s and start the SelfCheck loop.
        let mut actor_hm = host_mesh_with_config(num_replicas).await;
        let actor_proc_mesh = actor_hm
            .spawn(instance, "actors", Extent::unity(), None, None)
            .await
            .unwrap();

        // Host mesh for the wrapper + controller (will be killed).
        let mut controller_hm = host_mesh_with_config(1).await;
        let controller_proc_mesh = controller_hm
            .spawn(instance, "controller", Extent::unity(), None, None)
            .await
            .unwrap();

        let child_name = ActorMeshId::unique(Label::new("orphan_child").unwrap());

        // Supervision port required by WrapperActor params.
        let (supervision_port, _supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();

        // Spawn WrapperActor on controller_proc_mesh. Its init() spawns
        // ActorMesh<TestActor> on actor_proc_mesh with a real
        // ActorMeshController co-located on the controller's process.
        let _wrapper_mesh: ActorMesh<testactor::WrapperActor> = controller_proc_mesh
            .spawn(
                instance,
                "wrapper",
                &(
                    actor_proc_mesh.deref().clone(),
                    supervisor,
                    child_name.clone(),
                ),
            )
            .await
            .unwrap();

        // Give the controller time to run at least one CheckState cycle
        // (polling every 1s). This is what sends `KeepaliveGetState` to
        // each agent, and is what arms the agent's `expiry_time` so the
        // agent's `SelfCheck` reaper can cull the actors after the
        // controller dies. Polling for `Running` is not enough: actors
        // reach `Running` at spawn time before the controller's first
        // poll, and if we kill the controller in that window the agents
        // never received a keepalive and the orphan timeout never trips.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let states = actor_proc_mesh
            .actor_states(instance, child_name.clone())
            .await
            .unwrap();
        for state in states.values() {
            assert_eq!(
                state.status,
                resource::Status::Running,
                "actor should be running before controller crash"
            );
        }

        // Kill the controller's host process uncleanly. The TestActors on
        // actor_proc_mesh survive. Killing the host (rather than asking the
        // wrapper actor to `process::exit`) is critical: the wrapper runs
        // in this same test binary's address space when the host mesh is
        // co-located, so an in-process exit would also tear down the test
        // runner.
        controller_hm.kill_hosts().await;

        // Poll until all actors are stopped via the orphan timeout. The
        // configured timeout is 2s and `SelfCheck` fires every 2s, so this
        // converges quickly; allow generous slack for slow CI environments.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let states = actor_proc_mesh
                .actor_states(instance, child_name.clone())
                .await
                .unwrap();
            if states
                .values()
                .all(|s| s.status == resource::Status::Stopped)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for actors to be stopped after controller crash and orphan timeout"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let _ = actor_hm.shutdown(instance).await;
    }

    #[test]
    fn test_proc_status_to_actor_status_stopped_cleanly() {
        let status = proc_status_to_actor_status(Some(ProcStatus::Stopped {
            exit_code: 0,
            stderr_tail: vec![],
        }));
        assert!(
            matches!(status, ActorStatus::Stopped(ref msg) if msg.contains("cleanly")),
            "expected Stopped, got {:?}",
            status
        );
    }

    #[test]
    fn test_proc_status_to_actor_status_nonzero_exit() {
        let status = proc_status_to_actor_status(Some(ProcStatus::Stopped {
            exit_code: 1,
            stderr_tail: vec![],
        }));
        assert!(
            matches!(status, ActorStatus::Failed(_)),
            "expected Failed, got {:?}",
            status
        );
    }

    #[test]
    fn test_proc_status_to_actor_status_stopping_is_not_a_failure() {
        let status = proc_status_to_actor_status(Some(ProcStatus::Stopping {
            started_at: std::time::SystemTime::now(),
        }));
        assert!(
            matches!(status, ActorStatus::Stopped(ref msg) if msg.contains("stopping")),
            "expected Stopped, got {:?}",
            status
        );
    }

    #[test]
    fn test_proc_status_to_actor_status_none() {
        let status = proc_status_to_actor_status(None);
        assert!(
            matches!(status, ActorStatus::Stopped(_)),
            "expected Stopped, got {:?}",
            status
        );
    }

    #[test]
    fn test_proc_status_to_actor_status_killed() {
        let status = proc_status_to_actor_status(Some(ProcStatus::Killed {
            signal: 9,
            core_dumped: false,
        }));
        assert!(
            matches!(status, ActorStatus::Failed(_)),
            "expected Failed, got {:?}",
            status
        );
    }

    #[test]
    fn test_proc_status_to_actor_status_failed() {
        let status = proc_status_to_actor_status(Some(ProcStatus::Failed {
            reason: "oom".to_string(),
        }));
        assert!(
            matches!(status, ActorStatus::Failed(_)),
            "expected Failed, got {:?}",
            status
        );
    }
}
