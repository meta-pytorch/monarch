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
use hyperactor::PortRef;
use hyperactor::ProcId;
use hyperactor::Unbind;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::actor::handle_undeliverable_message;
use hyperactor::context;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Attrs;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
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
use crate::casting::update_undeliverable_envelope_for_casting;
use crate::host_mesh::HostMeshRef;
use crate::mesh_agent::ActorState;
use crate::proc_mesh::ProcMeshRef;
use crate::resource;
use crate::supervision::MeshFailure;
use crate::supervision::Unhealthy;

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

#[derive(Debug)]
struct HealthState {
    /// The status of each actor in the controlled mesh.
    /// TODO: replace with ValueMesh?
    statuses: HashMap<Point, resource::Status>,
    unhealthy_event: Option<Unhealthy>,
    crashed_ranks: HashMap<usize, ActorSupervisionEvent>,
    // The unique owner of this actor.
    owner: Option<PortRef<MeshFailure>>,
    /// A set of subscribers to send messages to when events are encountered.
    subscribers: HashSet<PortRef<Option<MeshFailure>>>,
}

impl HealthState {
    fn new(
        statuses: HashMap<Point, resource::Status>,
        owner: Option<PortRef<MeshFailure>>,
    ) -> Self {
        Self {
            statuses,
            unhealthy_event: None,
            crashed_ranks: HashMap::new(),
            owner,
            subscribers: HashSet::new(),
        }
    }
}

/// Subscribe me to updates about a mesh. If a duplicate is subscribed, only a single
/// message is sent.
/// Will send None if there are no failures on the mesh periodically. This guarantees
/// the listener that the controller is still alive. Make sure to filter such events
/// out as not useful.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct Subscribe(pub PortRef<Option<MeshFailure>>);

/// Unsubscribe me to future updates about a mesh. Should be the same port used in
/// the Subscribe message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct Unsubscribe(pub PortRef<Option<MeshFailure>>);

/// Check state of the actors in the mesh. This is used as a self message to
/// periodically check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct CheckState();

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
        port: Option<PortRef<MeshFailure>>,
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
            cx.self_message_with_delay(
                CheckState {},
                hyperactor_config::global::get(SUPERVISION_POLL_FREQUENCY),
            )
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
    subscriber: &PortRef<Option<MeshFailure>>,
    message: MeshFailure,
) {
    let mut headers = Attrs::new();
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
        // Start the monitor task.
        // There's a shared monitor for all whole mesh ref. Note that slices do
        // not share the health state. This is fine because requerying a slice
        // of a mesh will still return any failed state.
        self.monitor = Some(());
        self.self_check_state_message(this)?;
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
            let port = PortRef::<Option<MeshFailure>>::attest(dest_port_id);
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
        match &self.health_state.unhealthy_event {
            None => {}
            // For an adverse event like stopped or crashed, send a notification
            // immediately. This represents an initial bad state, if subscribing
            // to an already-dead mesh.
            Some(Unhealthy::StreamClosed(msg)) => {
                send_subscriber_message(cx, &message.0, msg.clone());
            }
            Some(Unhealthy::Crashed(msg)) => {
                send_subscriber_message(cx, &message.0, msg.clone());
            }
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
        let statuses = &self.health_state.statuses;
        let mut statuses = statuses.clone().into_iter().collect::<Vec<_>>();
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
            // Rank = none means it affects the whole mesh.
            rank: None,
            event,
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
        // Send a stop message to the ProcMeshAgent for these actors.
        match self.stop(cx, message.reason.clone()).await {
            Ok(statuses) => {
                // All stops successful, set actor status on health state.
                for (rank, status) in statuses.iter() {
                    self.health_state
                        .statuses
                        .entry(rank)
                        .and_modify(move |s| *s = status);
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
                            .unwrap() = status.clone();
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
        let mut headers = Attrs::new();
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
        rank: Some(rank),
        event: event.clone(),
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
    async fn handle(&mut self, cx: &Context<Self>, _: CheckState) -> Result<(), anyhow::Error> {
        // This implementation polls every "time_between_checks" duration, checking
        // for changes in the actor states. It can be improved in two ways:
        // 1. Use accumulation, to get *any* actor with a change in state, not *all*
        //    actors.
        // 2. Use a push-based mode instead of polling.
        // Wait in between checking to avoid using too much network.
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
                let actor_status = match state.state.and_then(|s| s.proc_status) {
                    Some(ProcStatus::Stopped { exit_code, .. }) => {
                        ActorStatus::Stopped(format!("process exited with code {}", exit_code))
                    }
                    // Conservatively treat lack of status as stopped
                    None => ActorStatus::Stopped("no status received from process".to_string()),
                    Some(status) => ActorStatus::Failed(ActorErrorKind::Generic(format!(
                        "process failure: {}",
                        status
                    ))),
                };
                let display_name = if !point.is_empty() {
                    let coords_display = point.format_as_dict();
                    if let Some(pos) = supervision_display_name.rfind('>') {
                        format!(
                            "{}{}{}",
                            &supervision_display_name[..pos],
                            coords_display,
                            &supervision_display_name[pos..]
                        )
                    } else {
                        format!("{}{}", supervision_display_name, coords_display)
                    }
                } else {
                    supervision_display_name.clone()
                };
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
        let events = mesh.actor_states(cx).await;
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
            let mut is_new = false;
            let entry = self
                .health_state
                .statuses
                .entry(point.clone())
                .or_insert_with(|| {
                    tracing::debug!(
                        "PythonActorMeshImpl: received initial state: point={:?}, state={:?}",
                        point,
                        state
                    );
                    let (_rank, events) = actor_state_to_supervision_events(state.clone());
                    // Wait for next event if the change in state produced no supervision events.
                    if !events.is_empty() {
                        is_new = true;
                    }
                    state.status.clone()
                });
            // If the status of any rank is terminal, we don't want to send
            // a heartbeat message.
            if !is_terminal && entry.is_terminating() {
                is_terminal = true;
            }
            // If this actor is new, or the state changed, send a message to the owner.
            let (rank, event) = if is_new {
                let (rank, events) = actor_state_to_supervision_events(state.clone());
                if events.is_empty() {
                    continue;
                }
                (rank, events[0].clone())
            } else if *entry != state.status {
                tracing::debug!(
                    "PythonActorMeshImpl: received state change event: point={:?}, old_state={:?}, new_state={:?}",
                    point,
                    entry,
                    state
                );
                let (rank, events) = actor_state_to_supervision_events(state.clone());
                if events.is_empty() {
                    continue;
                }
                *entry = state.status;
                (rank, events[0].clone())
            } else {
                continue;
            };
            did_send_state_change = true;
            send_state_change(cx, rank, event, mesh.name(), false, &mut self.health_state);
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
            .all(|s| s.is_terminating());
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
    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // Cannot use "ProcMesh::stop" as it's only defined on ProcMesh, not ProcMeshRef.
        let names = self.mesh.proc_ids().collect::<Vec<ProcId>>();
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
