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

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Unbind;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::actor::handle_undeliverable_message;
use hyperactor::context;
use hyperactor::kv_pairs;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::reference as hyperactor_reference;
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
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use typeuri::Named;

use crate::Name;
use crate::ValueMesh;
use crate::actor_mesh::ActorMeshRef;
use crate::bootstrap::ProcStatus;
use crate::casting::CAST_ACTOR_MESH_ID;
use crate::casting::update_undeliverable_envelope_for_casting;
use crate::host_mesh::HostMeshRef;
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

#[derive(Debug)]
struct HealthState {
    /// The status of each actor in the controlled mesh, paired with the
    /// generation counter from the most recent update. The generation is
    /// used for last-writer-wins ordering between streamed and polled updates.
    statuses: HashMap<Point, (resource::Status, u64)>,
    unhealthy_event: Option<Unhealthy>,
    crashed_ranks: HashMap<usize, ActorSupervisionEvent>,
    // The unique owner of this actor.
    owner: Option<hyperactor_reference::PortRef<MeshFailure>>,
    /// A set of subscribers to send messages to when events are encountered.
    subscribers: HashSet<hyperactor_reference::PortRef<Option<MeshFailure>>>,
}

impl HealthState {
    fn new(
        statuses: HashMap<Point, resource::Status>,
        owner: Option<hyperactor_reference::PortRef<MeshFailure>>,
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
}

/// Subscribe me to updates about a mesh. If a duplicate is subscribed, only a single
/// message is sent.
/// Will send None if there are no failures on the mesh periodically. This guarantees
/// the listener that the controller is still alive. Make sure to filter such events
/// out as not useful.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct Subscribe(pub hyperactor_reference::PortRef<Option<MeshFailure>>);

/// Unsubscribe me to future updates about a mesh. Should be the same port used in
/// the Subscribe message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct Unsubscribe(pub hyperactor_reference::PortRef<Option<MeshFailure>>);

/// Query the number of active supervision subscribers on this controller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct GetSubscriberCount(#[binding(include)] pub hyperactor_reference::PortRef<usize>);

/// Check state of the actors in the mesh. This is used as a self message to
/// periodically check.
/// Stores the next time we expect to start running a check state message.
/// Used to check for stalls in message handling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct CheckState(pub std::time::SystemTime);

/// The implementation of monitoring works as follows:
/// * ActorMesh and ActorMeshRef subscribe for updates from this controller,
///   which aggregates events from all owned actors.
/// * The monitor continuously polls for new events. When new events are
///   found, it sends messages to all subscribers
/// * In addition to sending to subscribers, the owner is an automatic subscriber
///   that also has to handle the events.
#[hyperactor::export(handlers = [
    Subscribe,
    Unsubscribe,
    GetSubscriberCount,
    resource::State<ActorState>,
    resource::CreateOrUpdate<resource::mesh::Spec<()>> { cast = true },
    resource::GetState<resource::mesh::State<()>> { cast = true },
    resource::Stop { cast = true },
])]
pub struct ActorMeshController<A>
where
    A: Referable,
{
    mesh: ActorMeshRef<A>,
    supervision_display_name: String,
    // Shared health state for the monitor and responding to queries.
    health_state: HealthState,
    // The monitor which continuously runs in the background to refresh the state
    // of actors.
    // If None, the actor it monitors has already stopped.
    monitor: Option<()>,
}

impl<A: Referable> resource::mesh::Mesh for ActorMeshController<A> {
    type Spec = ();
    type State = ();
}

impl<A: Referable> ActorMeshController<A> {
    /// Create a new mesh controller based on the provided reference.
    pub(crate) fn new(
        mesh: ActorMeshRef<A>,
        supervision_display_name: Option<String>,
        port: Option<hyperactor_reference::PortRef<MeshFailure>>,
        initial_statuses: ValueMesh<resource::Status>,
    ) -> Self {
        let supervision_display_name =
            supervision_display_name.unwrap_or_else(|| mesh.name().to_string());
        Self {
            mesh,
            supervision_display_name,
            health_state: HealthState::new(initial_statuses.iter().collect(), port),
            monitor: None,
        }
    }

    async fn stop(
        &self,
        cx: &impl context::Actor,
        reason: String,
    ) -> crate::Result<ValueMesh<resource::Status>> {
        // Cannot use "ActorMesh::stop" as it tries to message the controller, which is this actor.
        self.mesh
            .proc_mesh()
            .stop_actor_by_name(cx, self.mesh.name().clone(), reason)
            .await
    }

    fn self_check_state_message(&self, cx: &Instance<Self>) -> Result<(), ActorError> {
        // Only schedule a self message if the monitor has not been dropped.
        if self.monitor.is_some() {
            // Save when we expect the next check state message, so we can automatically
            // detect stalls as they accumulate.
            let delay = hyperactor_config::global::get(SUPERVISION_POLL_FREQUENCY);
            cx.self_message_with_delay(CheckState(std::time::SystemTime::now() + delay), delay)
        } else {
            Ok(())
        }
    }
}

declare_attrs! {
    /// If present in a message header, the message is from an ActorMeshController
    /// to a subscriber and can be safely dropped if it is returned as undeliverable.
    pub attr ACTOR_MESH_SUBSCRIBER_MESSAGE: bool;
}

fn send_subscriber_message(
    cx: &impl context::Actor,
    subscriber: &hyperactor_reference::PortRef<Option<MeshFailure>>,
    message: MeshFailure,
) {
    let mut headers = Flattrs::new();
    headers.set(ACTOR_MESH_SUBSCRIBER_MESSAGE, true);
    if let Err(error) = subscriber.send_with_headers(cx, headers, Some(message.clone())) {
        tracing::warn!(
            event = %message,
            "failed to send supervision event to subscriber {}: {}",
            subscriber.port_id(),
            error
        );
    } else {
        tracing::info!(event = %message, "sent supervision failure message to subscriber {}", subscriber.port_id());
    }
}

impl<A: Referable> Debug for ActorMeshController<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshController")
            .field("mesh", &self.mesh)
            .field("health_state", &self.health_state)
            .field("monitor", &self.monitor)
            .finish()
    }
}

#[async_trait]
impl<A: Referable> Actor for ActorMeshController<A> {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        // Start the monitor task.
        // There's a shared monitor for all whole mesh ref. Note that slices do
        // not share the health state. This is fine because requerying a slice
        // of a mesh will still return any failed state.
        self.monitor = Some(());
        self.self_check_state_message(this)?;

        // Subscribe to streaming state updates from all ProcAgents so the
        // controller receives state changes in real time, complementing the
        // existing polling loop.
        self.mesh.proc_mesh().agent_mesh().cast(
            this,
            resource::StreamState::<ActorState> {
                name: self.mesh.name().clone(),
                // All ProcAgents send updates directly to this port
                // so that failures along the comm tree path does not
                // affect clean shutdowns.
                subscriber: this.port().bind().unsplit(),
            },
        )?;

        let owner = if let Some(owner) = &self.health_state.owner {
            owner.to_string()
        } else {
            String::from("None")
        };
        tracing::info!(actor_id = %this.self_id(), %owner, "started mesh controller for {}", self.mesh.name());
        Ok(())
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // If the monitor hasn't been dropped yet, send a stop message to the
        // proc mesh.
        if self.monitor.take().is_some() {
            tracing::info!(actor_id = %this.self_id(), actor_mesh = %self.mesh.name(), "starting cleanup for ActorMeshController, stopping actor mesh");
            self.stop(this, "actor mesh controller cleanup".to_string())
                .await?;
        }
        Ok(())
    }

    async fn handle_undeliverable_message(
        &mut self,
        cx: &Instance<Self>,
        mut envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        // Update the destination in case this was a casting message.
        envelope = update_undeliverable_envelope_for_casting(envelope);
        if let Some(true) = envelope.0.headers().get(ACTOR_MESH_SUBSCRIBER_MESSAGE) {
            // Remove from the subscriber list (if it existed) so we don't
            // send to this subscriber again.
            // NOTE: The only part of the port that is used for equality checks is
            // the port id, so create a new one just for the comparison.
            let dest_port_id = envelope.0.dest().clone();
            let port = hyperactor_reference::PortRef::<Option<MeshFailure>>::attest(dest_port_id);
            let did_exist = self.health_state.subscribers.remove(&port);
            if did_exist {
                tracing::debug!(
                    actor_id = %cx.self_id(),
                    num_subscribers = self.health_state.subscribers.len(),
                    "ActorMeshController: handle_undeliverable_message: removed subscriber {} from mesh controller",
                    port.port_id()
                );
            }
            Ok(())
        } else if envelope.0.headers().get(CAST_ACTOR_MESH_ID).is_some() {
            // A cast message we sent (e.g. StreamState or KeepaliveGetState)
            // was returned by the CommActor because it could not be forwarded.
            // This is expected when the network session is broken. Log and
            // continue — the supervision polling loop will detect the failure.
            tracing::warn!(
                actor_id = %cx.self_id(),
                dest = %envelope.0.dest(),
                "ActorMeshController: ignoring undeliverable cast message",
            );
            Ok(())
        } else {
            handle_undeliverable_message(cx, envelope)
        }
    }
}

#[async_trait]
impl<A: Referable> Handler<Subscribe> for ActorMeshController<A> {
    async fn handle(&mut self, cx: &Context<Self>, message: Subscribe) -> anyhow::Result<()> {
        // If we can't send a message to a subscriber, the subscriber might be gone.
        // That shouldn't cause this actor to exit.
        // This is handled by the handle_undeliverable_message method.
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
        let port_id = message.0.port_id().clone();
        if self.health_state.subscribers.insert(message.0) {
            tracing::debug!(actor_id = %cx.self_id(), num_subscribers = self.health_state.subscribers.len(), "added subscriber {} to mesh controller", port_id);
        }
        Ok(())
    }
}

#[async_trait]
impl<A: Referable> Handler<Unsubscribe> for ActorMeshController<A> {
    async fn handle(&mut self, cx: &Context<Self>, message: Unsubscribe) -> anyhow::Result<()> {
        if self.health_state.subscribers.remove(&message.0) {
            tracing::debug!(actor_id = %cx.self_id(), num_subscribers = self.health_state.subscribers.len(), "removed subscriber {} from mesh controller", message.0.port_id());
        }
        Ok(())
    }
}

#[async_trait]
impl<A: Referable> Handler<GetSubscriberCount> for ActorMeshController<A> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: GetSubscriberCount,
    ) -> anyhow::Result<()> {
        message.0.send(cx, self.health_state.subscribers.len())?;
        Ok(())
    }
}

#[async_trait]
impl<A: Referable> Handler<resource::CreateOrUpdate<resource::mesh::Spec<()>>>
    for ActorMeshController<A>
{
    /// Currently a no-op as there's nothing to create or update, but allows
    /// ActorMeshController to implement the resource mesh behavior.
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        _message: resource::CreateOrUpdate<resource::mesh::Spec<()>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<A: Referable> Handler<resource::GetState<resource::mesh::State<()>>>
    for ActorMeshController<A>
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: resource::GetState<resource::mesh::State<()>>,
    ) -> anyhow::Result<()> {
        let status = if let Some(Unhealthy::Crashed(e)) = &self.health_state.unhealthy_event {
            resource::Status::Failed(e.to_string())
        } else if let Some(Unhealthy::StreamClosed(_)) = &self.health_state.unhealthy_event {
            resource::Status::Stopped
        } else {
            resource::Status::Running
        };
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
        message.reply.send(
            cx,
            resource::State {
                name: message.name,
                status,
                state: Some(state),
                generation: 0,
                timestamp: std::time::SystemTime::now(),
            },
        )?;
        Ok(())
    }
}

#[async_trait]
impl<A: Referable> Handler<resource::Stop> for ActorMeshController<A> {
    async fn handle(&mut self, cx: &Context<Self>, message: resource::Stop) -> anyhow::Result<()> {
        let mesh = &self.mesh;
        let mesh_name = mesh.name();
        tracing::info!(
            name = "ActorMeshControllerStatus",
            %mesh_name,
            reason = %message.reason,
            "stopping actor mesh"
        );
        // Run the drop on the monitor loop. The actors will not change state
        // after this point, because they will be stopped.
        // This message is idempotent because multiple stops only send out one
        // set of messages to subscribers.
        if self.monitor.take().is_none() {
            tracing::debug!(actor_id = %cx.self_id(), actor_mesh = %mesh_name, "duplicate stop request, actor mesh is already stopped");
            return Ok(());
        }
        tracing::info!(actor_id = %cx.self_id(), actor_mesh = %mesh_name, "forwarding stop request from ActorMeshController to proc mesh");

        // Let the client know that the controller has stopped. Since the monitor
        // is cancelled, it will not alert the owner or the subscribers.
        // We use a placeholder rank to get an actor id, but really there should
        // be a stop event for every rank in the mesh. Since every rank has the
        // same owner, we assume the rank doesn't matter, and the owner can just
        // assume the stop happened on all actors.
        let rank = 0usize;
        let event = ActorSupervisionEvent::new(
            // Use an actor id from the mesh.
            mesh.get(rank).unwrap().actor_id().clone(),
            None,
            ActorStatus::Stopped("ActorMeshController received explicit stop request".to_string()),
            None,
        );
        let failure_message = MeshFailure {
            actor_mesh_name: Some(mesh_name.to_string()),
            event,
            crashed_ranks: vec![],
        };
        self.health_state.unhealthy_event = Some(Unhealthy::StreamClosed(failure_message.clone()));
        // We don't send a message to the owner on stops, because only the owner
        // can request a stop. We just send to subscribers instead, as they did
        // not request the stop themselves.
        for subscriber in self.health_state.subscribers.iter() {
            send_subscriber_message(cx, subscriber, failure_message.clone());
        }

        // max_rank and extent are only needed for the deprecated RankedValues.
        // TODO: add cmp::Ord to Point for a max() impl.
        let max_rank = self.health_state.statuses.keys().map(|p| p.rank()).max();
        let extent = self
            .health_state
            .statuses
            .keys()
            .next()
            .map(|p| p.extent().clone());
        // Send a stop message to the ProcAgent for these actors.
        match self.stop(cx, message.reason.clone()).await {
            Ok(statuses) => {
                // All stops successful, set actor status on health state.
                for (rank, status) in statuses.iter() {
                    self.health_state
                        .statuses
                        .entry(rank)
                        .and_modify(move |s| *s = (status, u64::MAX));
                }
            }
            // An ActorStopError means some actors didn't reach the stopped state.
            Err(crate::Error::ActorStopError { statuses }) => {
                // If there are no states yet, nothing to update.
                if let Some(max_rank) = max_rank {
                    let extent = extent.expect("no actors in mesh");
                    for (rank, status) in statuses.materialized_iter(max_rank).enumerate() {
                        *self
                            .health_state
                            .statuses
                            .get_mut(&extent.point_of_rank(rank).expect("illegal rank"))
                            .unwrap() = (status.clone(), u64::MAX);
                    }
                }
            }
            // Other error types should be reported as supervision errors.
            Err(e) => {
                return Err(e.into());
            }
        }

        tracing::info!(actor_id = %cx.self_id(), actor_mesh = %mesh_name, "stopped mesh");
        Ok(())
    }
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
        if let Err(e) = subscriber.send_with_headers(cx, headers, None) {
            tracing::warn!(subscriber = %subscriber.port_id(), "error sending heartbeat message: {:?}", e);
        }
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
    mesh_name: &Name,
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
            if let Err(error) = owner.send(cx, failure_message.clone()) {
                tracing::warn!(
                    name = "SupervisionEvent",
                    actor_mesh = %mesh_name,
                    %event,
                    %error,
                    "failed to send supervision event to owner {}: {}. dropping event",
                    owner.port_id(),
                    error
                );
            } else {
                tracing::info!(actor_mesh = %mesh_name, %event, "sent supervision failure message to owner {}", owner.port_id());
            }
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
        Some(ProcStatus::Stopped { exit_code, .. }) => ActorStatus::Failed(
            ActorErrorKind::Generic(format!("process exited with non-zero code {}", exit_code)),
        ),
        // Stopping is a transient state during graceful shutdown. Treat it the
        // same as a clean stop rather than a failure.
        Some(ProcStatus::Stopping { .. }) => {
            ActorStatus::Stopped("process is stopping".to_string())
        }
        // Conservatively treat lack of status as stopped
        None => ActorStatus::Stopped("no status received from process".to_string()),
        Some(status) => ActorStatus::Failed(ActorErrorKind::Generic(format!(
            "process failure: {}",
            status
        ))),
    }
}

#[async_trait]
impl<A: Referable> Handler<resource::State<ActorState>> for ActorMeshController<A> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        state: resource::State<ActorState>,
    ) -> anyhow::Result<()> {
        let (rank, events) = actor_state_to_supervision_events(state.clone());
        let point = self.mesh.region().extent().point_of_rank(rank)?;

        let changed = self
            .health_state
            .maybe_update(point, state.status, state.generation);

        if changed && !events.is_empty() {
            send_state_change(
                cx,
                rank,
                events[0].clone(),
                self.mesh.name(),
                false,
                &mut self.health_state,
            );
        }

        if self
            .health_state
            .statuses
            .values()
            .all(|(s, _)| s.is_terminating())
        {
            self.monitor.take();
        }
        Ok(())
    }
}

fn format_system_time(time: std::time::SystemTime) -> String {
    let datetime: chrono::DateTime<chrono::Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

#[async_trait]
impl<A: Referable> Handler<CheckState> for ActorMeshController<A> {
    /// Checks actor states and reschedules as a self-message.
    ///
    /// When any actor in this mesh changes state,
    /// including once for the initial state of all actors, send a message to the
    /// owners and subscribers of this mesh.
    /// The receivers will get a MeshFailure. The created rank is
    /// the original rank of the actor on the mesh, not the rank after
    /// slicing.
    ///
    /// * SUPERVISION_POLL_FREQUENCY controls how frequently to poll.
    /// * self-messaging stops when self.monitor is set to None.
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        CheckState(expected_time): CheckState,
    ) -> Result<(), anyhow::Error> {
        // A delayed CheckState may arrive after Stop has already dropped
        // the monitor. Discard it — there is nothing left to poll.
        if self.monitor.is_none() {
            return Ok(());
        }

        // This implementation polls every "time_between_checks" duration, checking
        // for changes in the actor states. It can be improved in two ways:
        // 1. Use accumulation, to get *any* actor with a change in state, not *all*
        //    actors.
        // 2. Use a push-based mode instead of polling.
        // Wait in between checking to avoid using too much network.

        // Check for stalls in the supervision loop. These delays can cause the
        // subscribers to think the controller is dead.
        // Allow a little slack time to avoid logging for innocuous delays.
        // If it's greater than 2x the expected time, log a warning.
        if std::time::SystemTime::now()
            > expected_time + hyperactor_config::global::get(SUPERVISION_POLL_FREQUENCY)
        {
            // Current time is included by default in the log message.
            let expected_time = format_system_time(expected_time);
            // Track in both metrics and tracing.
            ACTOR_MESH_CONTROLLER_SUPERVISION_STALLS.add(1, kv_pairs!("actor_id" => cx.self_id().to_string(), "expected_time" => expected_time.clone()));
            tracing::warn!(
                actor_id = %cx.self_id(),
                "Handler<CheckState> is being stalled, expected at {}",
                expected_time,
            );
        }
        let mesh = &self.mesh;
        let supervision_display_name = &self.supervision_display_name;
        // First check if the proc mesh is dead before trying to query their agents.
        let proc_states = mesh.proc_mesh().proc_states(cx).await;
        if let Err(e) = proc_states {
            send_state_change(
                cx,
                0,
                ActorSupervisionEvent::new(
                    cx.self_id().clone(),
                    None,
                    ActorStatus::generic_failure(format!(
                        "unable to query for proc states: {:?}",
                        e
                    )),
                    None,
                ),
                mesh.name(),
                false,
                &mut self.health_state,
            );
            self.self_check_state_message(cx)?;
            return Ok(());
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
                let display_name = crate::actor_display_name(supervision_display_name, &point);
                send_state_change(
                    cx,
                    point.rank(),
                    ActorSupervisionEvent::new(
                        // Attribute this to the monitored actor, even if the underlying
                        // cause is a proc_failure. We propagate the cause explicitly.
                        mesh.get(point.rank()).unwrap().actor_id().clone(),
                        Some(format!("{} was running on a process which", display_name)),
                        actor_status,
                        None,
                    ),
                    mesh.name(),
                    true,
                    &mut self.health_state,
                );
                self.self_check_state_message(cx)?;
                return Ok(());
            }
        }

        // Now that we know the proc mesh is alive, check for actor state changes.
        let orphan_timeout = hyperactor_config::global::get(MESH_ORPHAN_TIMEOUT);
        let keepalive = if orphan_timeout.is_zero() {
            None
        } else {
            Some(std::time::SystemTime::now() + orphan_timeout)
        };
        let events = mesh.actor_states_with_keepalive(cx, keepalive).await;
        if let Err(e) = events {
            send_state_change(
                cx,
                0,
                ActorSupervisionEvent::new(
                    cx.self_id().clone(),
                    Some(supervision_display_name.clone()),
                    ActorStatus::generic_failure(format!(
                        "unable to query for actor states: {:?}",
                        e
                    )),
                    None,
                ),
                mesh.name(),
                false,
                &mut self.health_state,
            );
            self.self_check_state_message(cx)?;
            return Ok(());
        }
        // If there was any state change, we don't need to send a heartbeat.
        let mut did_send_state_change = false;
        // True if any rank is in a terminal status. Once that is true, no more
        // heartbeats are sent.
        let mut is_terminal = false;
        // This returned point is the created rank, *not* the rank of
        // the possibly sliced input mesh.
        for (point, state) in events.unwrap().iter() {
            let changed = self.health_state.maybe_update(
                point.clone(),
                state.status.clone(),
                state.generation,
            );
            // If the status of any rank is terminal, we don't want to send
            // a heartbeat message.
            if !is_terminal {
                if let Some((s, _)) = self.health_state.statuses.get(&point) {
                    if s.is_terminating() {
                        is_terminal = true;
                    }
                }
            }
            if !changed {
                continue;
            }
            let (rank, events) = actor_state_to_supervision_events(state.clone());
            if events.is_empty() {
                continue;
            }
            did_send_state_change = true;
            send_state_change(
                cx,
                rank,
                events[0].clone(),
                mesh.name(),
                false,
                &mut self.health_state,
            );
        }
        if !did_send_state_change && !is_terminal {
            // No state change, but subscribers need to be sent a message
            // every so often so they know the controller is still alive.
            // Send a "no state change" message.
            // Only if the last state for any actor in this mesh is not a terminal state.
            send_heartbeat(cx, &self.health_state);
        }

        // If all ranks are in a terminal state, we don't need to continue checking,
        // as statuses cannot change.
        // Any new subscribers will get an immediate message saying the mesh is stopped.
        let all_ranks_terminal = self
            .health_state
            .statuses
            .values()
            .all(|(s, _)| s.is_terminating());
        if !all_ranks_terminal {
            // Schedule a self send after a waiting period.
            self.self_check_state_message(cx)?;
        } else {
            // There's no need to send a stop message during cleanup if all the
            // ranks are already terminal.
            self.monitor.take();
        }
        return Ok(());
    }
}

#[derive(Debug)]
#[hyperactor::export]
pub(crate) struct ProcMeshController {
    mesh: ProcMeshRef,
}

impl ProcMeshController {
    /// Create a new proc controller based on the provided reference.
    pub(crate) fn new(mesh: ProcMeshRef) -> Self {
        Self { mesh }
    }
}

#[async_trait]
impl Actor for ProcMeshController {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        Ok(())
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // Cannot use "ProcMesh::stop" as it's only defined on ProcMesh, not ProcMeshRef.
        let names = self
            .mesh
            .proc_ids()
            .collect::<Vec<hyperactor_reference::ProcId>>();
        let region = self.mesh.region().clone();
        if let Some(hosts) = self.mesh.hosts() {
            hosts
                .stop_proc_mesh(
                    this,
                    self.mesh.name(),
                    names,
                    region,
                    "proc mesh controller cleanup".to_string(),
                )
                .await
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
#[hyperactor::export]
pub(crate) struct HostMeshController {
    mesh: HostMeshRef,
}

impl HostMeshController {
    /// Create a new host controller based on the provided reference.
    pub(crate) fn new(mesh: HostMeshRef) -> Self {
        Self { mesh }
    }
}

#[async_trait]
impl Actor for HostMeshController {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        Ok(())
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // Cannot use "HostMesh::shutdown" as it's only defined on HostMesh, not HostMeshRef.
        for host in self.mesh.values() {
            if let Err(e) = host.shutdown(this).await {
                tracing::warn!(host = %host, error = %e, "host shutdown failed");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::time::Duration;

    use hyperactor::actor::ActorStatus;
    use ndslice::Extent;
    use ndslice::ViewExt;

    use super::SUPERVISION_POLL_FREQUENCY;
    use super::proc_status_to_actor_status;
    use crate::ActorMesh;
    use crate::Name;
    use crate::bootstrap::ProcStatus;
    use crate::proc_agent::MESH_ORPHAN_TIMEOUT;
    use crate::resource;
    use crate::supervision::MeshFailure;
    use crate::test_utils::local_host_mesh;
    use crate::testactor;
    use crate::testing;

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
        let _orphan = config.override_key(MESH_ORPHAN_TIMEOUT, Duration::from_secs(1));

        let instance = testing::instance();
        let host_mesh = local_host_mesh(2).await;
        let proc_mesh = host_mesh
            .spawn(instance, "test", Extent::unity(), None)
            .await
            .unwrap();

        let actor_name = Name::new("orphan_test").unwrap();
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

        // Wait long enough for the expiry to pass and at least one
        // SelfCheck cycle to fire. With MESH_ORPHAN_TIMEOUT = 1s and
        // expiry in 2s, by around 4s at least two SelfCheck cycles will
        // have elapsed after the expiry.
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Query again, this time *without* a keepalive so we don't
        // extend the expiry.
        let states = proc_mesh
            .actor_states(instance, actor_name.clone())
            .await
            .unwrap();
        for state in states.values() {
            assert_eq!(
                state.status,
                resource::Status::Stopped,
                "actor should be stopped after keepalive expiry"
            );
        }
    }

    /// Create a multi-process host mesh that propagates the current
    /// process's config overrides to child processes via Bootstrap.
    #[cfg(fbcode_build)]
    async fn host_mesh_with_config(n: usize) -> crate::host_mesh::HostMeshShutdownGuard {
        use hyperactor::channel::ChannelTransport;
        use tokio::process::Command;

        let program = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");
        let mut host_addrs = vec![];
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
            cmd.spawn().unwrap();
        }

        let host_mesh = crate::HostMeshRef::from_hosts(Name::new("test").unwrap(), host_addrs);
        crate::host_mesh::HostMesh::take(host_mesh).shutdown_guard()
    }

    /// Verify that actors are cleaned up via the orphan timeout when the
    /// `ActorMeshController`'s process crashes. Unlike the system-actor test
    /// above, this spawns actors through a real controller (via `WrapperActor`)
    /// and then kills the controller's process uncleanly with `ProcessExit`.
    /// The agents on the surviving proc mesh detect the expired keepalive
    /// and stop the actors.
    #[tokio::test]
    #[cfg(fbcode_build)]
    async fn test_orphaned_actors_cleaned_up_on_controller_crash() {
        let config = hyperactor_config::global::lock();
        let _orphan = config.override_key(MESH_ORPHAN_TIMEOUT, Duration::from_secs(2));
        let _poll = config.override_key(SUPERVISION_POLL_FREQUENCY, Duration::from_secs(1));

        let instance = testing::instance();
        let num_replicas = 2;

        // Host mesh for the test actors (these survive the crash).
        // host_mesh_with_config propagates config overrides to child
        // processes via Bootstrap, so agents boot with
        // MESH_ORPHAN_TIMEOUT=2s and start the SelfCheck loop.
        let mut actor_hm = host_mesh_with_config(num_replicas).await;
        let actor_proc_mesh = actor_hm
            .spawn(instance, "actors", Extent::unity(), None)
            .await
            .unwrap();

        // Host mesh for the wrapper + controller (will be killed).
        let mut controller_hm = host_mesh_with_config(1).await;
        let controller_proc_mesh = controller_hm
            .spawn(instance, "controller", Extent::unity(), None)
            .await
            .unwrap();

        let child_name = Name::new("orphan_child").unwrap();

        // Supervision port required by WrapperActor params.
        let (supervision_port, _supervision_receiver) = instance.open_port::<MeshFailure>();
        let supervisor = supervision_port.bind();

        // Spawn WrapperActor on controller_proc_mesh. Its init() spawns
        // ActorMesh<TestActor> on actor_proc_mesh with a real
        // ActorMeshController co-located on the controller's process.
        let wrapper_mesh: ActorMesh<testactor::WrapperActor> = controller_proc_mesh
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
        // (polling every 1s) so it sends KeepaliveGetState to the agents.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify actors are running before the crash.
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

        // Kill the controller's process uncleanly. send_to_children: false
        // means only the WrapperActor's process exits; the TestActors on
        // actor_proc_mesh survive.
        wrapper_mesh
            .cast(
                instance,
                testactor::CauseSupervisionEvent {
                    kind: testactor::SupervisionEventType::ProcessExit(1),
                    send_to_children: false,
                },
            )
            .unwrap();

        // Wait for:
        //  - keepalive expiry (2s from last CheckState)
        //  - at least one SelfCheck cycle (every 2s)
        //  - margin for processing
        tokio::time::sleep(Duration::from_secs(8)).await;

        // Actors should now be stopped via the orphan timeout.
        let states = actor_proc_mesh
            .actor_states(instance, child_name.clone())
            .await
            .unwrap();
        for state in states.values() {
            assert_eq!(
                state.status,
                resource::Status::Stopped,
                "actor should be stopped after controller crash and orphan timeout"
            );
        }

        let _ = actor_hm.shutdown(instance).await;
        let _ = controller_hm.shutdown(instance).await;
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
