/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::clone::Clone;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::RemoteMessage;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Referable;
use hyperactor::actor::RemotableActor;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh::bootstrap::ProcStatus;
use hyperactor_mesh::dashmap::DashMap;
use hyperactor_mesh::proc_mesh::mesh_agent::ActorState;
use hyperactor_mesh::resource;
use hyperactor_mesh::v1::Name;
use hyperactor_mesh::v1::actor_mesh::ActorMesh;
use hyperactor_mesh::v1::actor_mesh::ActorMeshRef;
use ndslice::Point;
use ndslice::Region;
use ndslice::Selection;
use ndslice::Slice;
use ndslice::ViewExt;
use ndslice::selection::structurally_equal;
use ndslice::view::Ranked;
use ndslice::view::RankedSliceable;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PySystemExit;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor_mesh::ActorMeshProtocol;
use crate::actor_mesh::PyActorSupervisionEvent;
use crate::actor_mesh::PythonActorMesh;
use crate::context::ContextInstance;
use crate::context::PyInstance;
use crate::instance_dispatch;
use crate::proc::PyActorId;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;
use crate::runtime::get_tokio_runtime;
use crate::shape::PyRegion;
use crate::supervision::MeshFailure;
use crate::supervision::SupervisionError;
use crate::supervision::SupervisionFailureMessage;
use crate::supervision::Unhealthy;

struct RootHealthState {
    unhealthy_event: std::sync::Mutex<Unhealthy<ActorSupervisionEvent>>,
    crashed_ranks: DashMap<usize, ActorSupervisionEvent>,
}

impl std::fmt::Debug for RootHealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootHealthState")
            .field("unhealthy_event", &self.unhealthy_event)
            .field("crashed_ranks", &self.crashed_ranks)
            .finish()
    }
}

impl RootHealthState {
    fn new() -> Self {
        Self {
            unhealthy_event: std::sync::Mutex::new(Unhealthy::SoFarSoGood),
            crashed_ranks: DashMap::new(),
        }
    }
}

#[derive(Debug)]
struct SupervisionMonitor {
    cancel: CancellationToken,
    receiver: watch::Receiver<Option<PyErr>>,
}

impl Drop for SupervisionMonitor {
    fn drop(&mut self) {
        // The task is continuously polling for events on this mesh, but when
        // the mesh is no longer available we can stop querying it.
        self.cancel.cancel();
    }
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) struct PyActorMesh {
    mesh: ActorMesh<PythonActor>,
    health_state: Arc<RootHealthState>,
    monitor: Arc<Mutex<Option<SupervisionMonitor>>>,
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMeshRef",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) struct PyActorMeshRef {
    mesh: ActorMeshRef<PythonActor>,
    health_state: Arc<RootHealthState>,
    monitor: Arc<Mutex<Option<SupervisionMonitor>>>,
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "PythonActorMeshImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) enum PythonActorMeshImpl {
    Owned(PyActorMesh),
    Ref(PyActorMeshRef),
}

impl PythonActorMeshImpl {
    /// Get a new owned [`PythonActorMeshImpl`].
    pub(crate) fn new_owned(inner: ActorMesh<PythonActor>) -> Self {
        let health_state = Arc::new(RootHealthState::new());
        PythonActorMeshImpl::Owned(PyActorMesh {
            mesh: inner,
            health_state,
            monitor: Arc::new(Mutex::new(None)),
        })
    }

    /// Get a new ref-based [`PythonActorMeshImpl`].
    pub(crate) fn new_ref(inner: ActorMeshRef<PythonActor>) -> Self {
        let health_state = Arc::new(RootHealthState::new());
        PythonActorMeshImpl::Ref(PyActorMeshRef {
            mesh: inner,
            health_state,
            monitor: Arc::new(Mutex::new(None)),
        })
    }

    fn mesh_ref(&self) -> ActorMeshRef<PythonActor> {
        match self {
            PythonActorMeshImpl::Owned(inner) => (*inner.mesh).clone(),
            PythonActorMeshImpl::Ref(inner) => inner.mesh.clone(),
        }
    }

    fn health_state(&self) -> &Arc<RootHealthState> {
        match self {
            PythonActorMeshImpl::Owned(inner) => &inner.health_state,
            PythonActorMeshImpl::Ref(inner) => &inner.health_state,
        }
    }

    fn monitor(&self) -> &Arc<Mutex<Option<SupervisionMonitor>>> {
        match self {
            PythonActorMeshImpl::Owned(inner) => &inner.monitor,
            PythonActorMeshImpl::Ref(inner) => &inner.monitor,
        }
    }

    fn make_monitor<F>(
        &self,
        instance: PyInstance,
        unhandled: F,
        supervision_display_name: String,
    ) -> SupervisionMonitor
    where
        F: Fn(MeshFailure) + Send + 'static,
    {
        match self {
            // Owned meshes send a local message to themselves for the failures.
            PythonActorMeshImpl::Owned(inner) => Self::create_monitor(
                instance,
                (*inner.mesh).clone(),
                inner.health_state.clone(),
                true,
                unhandled,
                supervision_display_name,
            ),
            // Ref meshes send no message, they are only used to generate
            // the v0 style exception.
            PythonActorMeshImpl::Ref(inner) => Self::create_monitor(
                instance,
                inner.mesh.clone(),
                inner.health_state.clone(),
                false,
                unhandled,
                supervision_display_name,
            ),
        }
    }

    /// Get a supervision receiver for this mesh. The passed in monitor object
    /// must outlive the returned receiver, or else the sender may be dropped
    /// and the receiver will get a closed channel.
    fn supervision_receiver<F>(
        &self,
        instance: &PyInstance,
        monitor: &Arc<Mutex<Option<SupervisionMonitor>>>,
        unhandled: F,
        supervision_display_name: Option<String>,
    ) -> watch::Receiver<Option<PyErr>>
    where
        F: Fn(MeshFailure) + Send + 'static,
    {
        let mut guard = monitor.lock().unwrap();
        guard.get_or_insert_with(move || {
            let instance = Python::with_gil(|_py| instance.clone());
            self.make_monitor(
                instance,
                unhandled,
                supervision_display_name.unwrap_or_default(),
            )
        });
        let monitor = guard.as_ref().unwrap();
        monitor.receiver.clone()
    }

    fn unhandled_fault_hook<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        py.import("monarch.actor")?.getattr("unhandled_fault_hook")
    }

    fn get_unhandled(&self, instance: &PyInstance) -> Box<dyn Fn(MeshFailure) + Send + 'static> {
        let is_client = matches!(instance.context_instance(), ContextInstance::Client(_));
        match self {
            PythonActorMeshImpl::Owned(_) => {
                if is_client {
                    Box::new(move |failure| {
                        Python::with_gil(|py| {
                            let unhandled = Self::unhandled_fault_hook(py)
                                .expect("failed to fetch unhandled_fault_hook");
                            let pyfailure = failure
                                .clone()
                                .into_pyobject(py)
                                .expect("failed to turn PyErr into PyObject");
                            let result = unhandled.call1((pyfailure,));
                            // Handle SystemExit and actually exit the process.
                            // It is normally just an exception.
                            if let Err(e) = result {
                                if e.is_instance_of::<PySystemExit>(py) {
                                    let code = e
                                        .into_bound_py_any(py)
                                        .unwrap()
                                        .getattr("code")
                                        .unwrap()
                                        .extract::<i32>()
                                        .unwrap();
                                    tracing::error!(
                                        name = "ActorMeshStatus",
                                        status = "SupervisionError::UnhandledFaultHook",
                                        actor_name = failure.mesh_name,
                                        event = %failure.event,
                                        rank = failure.rank,
                                        "unhandled event reached unhandled_fault_hook: {}, which is exiting the process with code {}",
                                        failure,
                                        code
                                    );
                                    std::process::exit(code);
                                } else {
                                    // The callback raised some other exception, and there's
                                    // no way to handle it. Just exit the process anyways
                                    tracing::error!(
                                        name = "ActorMeshStatus",
                                        status = "SupervisionError::UnhandledFaultHook",
                                        actor_name = failure.mesh_name,
                                        event = %failure.event,
                                        rank = failure.rank,
                                        "unhandled event reached unhandled_fault_hook: {}, which raised an exception: {:?}. \
                                        Exiting the process with code 1",
                                        failure,
                                        e,
                                    );
                                    std::process::exit(1);
                                }
                            } else {
                                tracing::warn!(
                                    name = "ActorMeshStatus",
                                    status = "SupervisionError::UnhandledFaultHook",
                                    actor_name = failure.mesh_name,
                                    event = %failure.event,
                                    rank = failure.rank,
                                    "unhandled event reached unhandled_fault_hook: {}, but that function produced no exception or crash. Ignoring the error",
                                    failure
                                );
                            }
                        });
                    })
                } else {
                    Box::new(|_| {
                        // Never called if not client.
                    })
                }
            }
            PythonActorMeshImpl::Ref(_inner) => Box::new(|_| {
                // Never called if not owned.
            }),
        }
    }

    fn create_monitor<F>(
        instance: PyInstance,
        mesh: ActorMeshRef<PythonActor>,
        health_state: Arc<RootHealthState>,
        is_owned: bool,
        unhandled: F,
        supervision_display_name: String,
    ) -> SupervisionMonitor
    where
        F: Fn(MeshFailure) + Send + 'static,
    {
        // There's a shared monitor for all whole mesh ref. Note that slices do
        // not share the health state. This is fine because requerying a slice
        // of a mesh will still return any failed state.
        let (sender, receiver) = watch::channel(None);
        let cancel = CancellationToken::new();
        let canceled = cancel.clone();
        let _task = get_tokio_runtime().spawn(async move {
            // 3 seconds is chosen to not penalize short-lived successful calls,
            // while still able to catch issues before they look like a hang or timeout.
            let time_between_checks = tokio::time::Duration::from_secs(3);
            match instance.context_instance() {
                ContextInstance::Client(cx_instance) => {
                    actor_states_monitor(
                        cx_instance,
                        mesh,
                        // owner is always None if the owning instance is a client,
                        // because nothing can handle the message.
                        None,
                        is_owned,
                        unhandled,
                        health_state,
                        time_between_checks,
                        sender,
                        canceled,
                        supervision_display_name.clone(),
                    )
                    .await;
                }
                ContextInstance::PythonActor(cx_instance) => {
                    // Only make a handle if is_owned is true, we don't want
                    // to send messages to the ref holder.
                    let owner = if is_owned {
                        Some(cx_instance.handle())
                    } else {
                        None
                    };
                    actor_states_monitor(
                        cx_instance,
                        mesh,
                        owner,
                        is_owned,
                        unhandled,
                        health_state,
                        time_between_checks,
                        sender,
                        canceled,
                        supervision_display_name,
                    )
                    .await;
                }
            };
        });
        SupervisionMonitor { cancel, receiver }
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
                    ActorStatus::Stopped,
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

fn send_state_change<F>(
    rank: usize,
    event: ActorSupervisionEvent,
    actor_mesh_name: &Name,
    owner: &Option<ActorHandle<PythonActor>>,
    is_owned: bool,
    is_proc_stopped: bool,
    unhandled: &F,
    health_state: &Arc<RootHealthState>,
    sender: &watch::Sender<Option<PyErr>>,
) where
    F: Fn(MeshFailure),
{
    let failure = MeshFailure::new(actor_mesh_name, rank, event.clone());
    // Any supervision event that is not a failure should not generate
    // call "unhandled".
    // This includes the Stopped status, which is a state that occurs when the
    // user calls stop() on a proc or actor mesh.
    // It is not being terminated due to a failure. In this state, new messages
    // should not be sent, but we don't call unhandled when it is detected.
    let is_failed = event.is_error();
    if is_failed {
        tracing::warn!(
            name = "ActorMeshStatus",
            status = "SupervisionError",
            %event,
            "detected supervision error on monitored mesh: name={actor_mesh_name}",
        );
    } else {
        tracing::debug!(
            name = "ActorMeshStatus",
            status = "SupervisionEvent",
            %event,
            "detected non-error supervision event on monitored mesh: name={actor_mesh_name}",
        );
    }

    // Send a notification to the owning actor of this mesh, if there is one.
    if let Some(owner) = owner {
        if let Err(error) = owner.send(SupervisionFailureMessage {
            actor_mesh_name: actor_mesh_name.to_string(),
            rank,
            event: event.clone(),
        }) {
            tracing::warn!(
                name = "ActorMeshStatus",
                status = "SupervisionError",
                %event,
                %error,
                "failed to send supervision event to owner {}: {}. dropping event",
                owner.actor_id(),
                error
            );
        }
    } else if is_owned && is_failed {
        // The mesh has an owner, but it is not a PythonActor, so it must be the client.
        // Call the unhandled function to let the client control what to do.
        unhandled(failure);
    }
    let mut inner_unhealthy_event = health_state
        .unhealthy_event
        .lock()
        .expect("unhealthy_event lock poisoned");
    health_state.crashed_ranks.insert(rank, event.clone());
    *inner_unhealthy_event = if is_proc_stopped {
        Unhealthy::StreamClosed
    } else {
        Unhealthy::Crashed(event.clone())
    };
    let event_actor_id = event.actor_id.clone();
    let py_event = PyActorSupervisionEvent::from(event.clone());
    let pyerr = PyErr::new::<SupervisionError, _>(format!(
        "Actor {} exited because of the following reason: {}",
        event_actor_id,
        py_event
            .__repr__()
            .expect("repr failed on PyActorSupervisionEvent")
    ));
    sender.send(Some(pyerr)).expect("receiver closed");
}

/// Returns a watchable receiver for actor states.
///
/// The receiver will be notified when any actor in this mesh changes state,
/// including once for the initial state of all actors.
/// The caller can filter the state transition notifications themselves.
/// The receiver will get a tuple of (created rank, old state, new state). The
/// created rank is the original rank of the actor on the mesh, not the rank after
/// slicing.
///
/// * is_owned is true if this monitor is running on the owning instance. When true,
///   a message will be sent to "owner" if it is not None. If owner is None,
///   then a panic will be raised instead to crash the client.
/// * time_between_tasks 1trols how frequently to poll.
#[hyperactor::instrument_infallible(fields(
    host_mesh=actor_mesh.proc_mesh().host_mesh_name().map(|n| n.to_string()),
    proc_mesh=actor_mesh.proc_mesh().name().to_string(),
    actor_name=actor_mesh.name().to_string(),
))]
async fn actor_states_monitor<A, F>(
    cx: &impl context::Actor,
    actor_mesh: ActorMeshRef<A>,
    owner: Option<ActorHandle<PythonActor>>,
    is_owned: bool,
    unhandled: F,
    health_state: Arc<RootHealthState>,
    time_between_checks: tokio::time::Duration,
    sender: watch::Sender<Option<PyErr>>,
    canceled: CancellationToken,
    supervision_display_name: String,
) where
    A: Actor + RemotableActor + Referable,
    A::Params: RemoteMessage,
    F: Fn(MeshFailure),
{
    // This implementation polls every "time_between_checks" duration, checking
    // for changes in the actor states. It can be improved in two ways:
    // 1. Use accumulation, to get *any* actor with a change in state, not *all*
    //    actors.
    // 2. Use a push-based mode instead of polling.
    let mut existing_states: HashMap<Point, resource::State<ActorState>> = HashMap::new();
    loop {
        // Wait in between checking to avoid using too much network.
        tokio::select! {
            _ = RealClock.sleep(time_between_checks) => (),
            _ = canceled.cancelled() => break,
        }
        // First check if the proc mesh is dead before trying to query their agents.
        let proc_states = actor_mesh.proc_mesh().proc_states(cx).await;
        if let Err(e) = proc_states {
            send_state_change(
                0,
                ActorSupervisionEvent::new(
                    cx.instance().self_id().clone(),
                    None,
                    ActorStatus::generic_failure(format!(
                        "unable to query for proc states: {:?}",
                        e
                    )),
                    None,
                ),
                actor_mesh.name(),
                &owner,
                is_owned,
                false,
                &unhandled,
                &health_state,
                &sender,
            );
            continue;
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
                    Some(ProcStatus::Stopped { .. })
                    // SIGTERM
                    | Some(ProcStatus::Killed { signal: 15, .. })
                    // Conservatively treat lack of status as stopped
                    | None => ActorStatus::Stopped,

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
                    point.rank(),
                    ActorSupervisionEvent::new(
                        // Attribute this to the monitored actor, even if the underlying
                        // cause is a proc_failure. We propagate the cause explicitly.
                        actor_mesh.get(point.rank()).unwrap().actor_id().clone(),
                        Some(display_name),
                        actor_status,
                        None,
                    ),
                    actor_mesh.name(),
                    &owner,
                    is_owned,
                    true,
                    &unhandled,
                    &health_state,
                    &sender,
                );
                continue;
            }
        }

        // Now that we know the proc mesh is alive, check for actor state changes.
        let events = actor_mesh.actor_states(cx).await;
        if let Err(e) = events {
            send_state_change(
                0,
                ActorSupervisionEvent::new(
                    cx.instance().self_id().clone(),
                    Some(supervision_display_name.clone()),
                    ActorStatus::generic_failure(format!(
                        "unable to query for actor states: {:?}",
                        e
                    )),
                    None,
                ),
                actor_mesh.name(),
                &owner,
                is_owned,
                false,
                &unhandled,
                &health_state,
                &sender,
            );
            continue;
        }
        // This returned point is the created rank, *not* the rank of
        // the possibly sliced input mesh.
        for (point, state) in events.unwrap().iter() {
            let entry = existing_states.entry(point.clone()).or_insert_with(|| {
                tracing::debug!(
                    "PythonActorMeshImpl: received initial state: point={:?}, state={:?}",
                    point,
                    state
                );
                let (rank, events) = actor_state_to_supervision_events(state.clone());
                // Wait for next event if the change in state produced no supervision events.
                if events.is_empty() {
                    return state.clone();
                }
                // If this actor is new, send a message to the owner.
                send_state_change(
                    rank,
                    events[0].clone(),
                    actor_mesh.name(),
                    &owner,
                    is_owned,
                    false,
                    &unhandled,
                    &health_state,
                    &sender,
                );
                state.clone()
            });
            if entry.status != state.status {
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
                send_state_change(
                    rank,
                    events[0].clone(),
                    actor_mesh.name(),
                    &owner,
                    is_owned,
                    false,
                    &unhandled,
                    &health_state,
                    &sender,
                );
                *entry = state;
            }
        }
    }
}

impl ActorMeshProtocol for PythonActorMeshImpl {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        // First check if the mesh is already dead before sending out any messages
        // to a possibly undeliverable actor.
        let mesh_ref = self.mesh_ref();

        let health_state = self.health_state();
        let region = Ranked::region(&mesh_ref);
        match &*health_state
            .unhealthy_event
            .lock()
            .unwrap_or_else(|e| e.into_inner())
        {
            Unhealthy::StreamClosed => {
                return Err(SupervisionError::new_err(
                    "actor mesh is stopped due to proc mesh shutdown".to_string(),
                ));
            }
            Unhealthy::Crashed(event) => {
                return Err(SupervisionError::new_err(format!(
                    "Actor {} is unhealthy with reason: {}",
                    event.actor_id, event.actor_status
                )));
            }
            Unhealthy::SoFarSoGood => {
                // Further check crashed ranks in case those were updated from another
                // slice of the same mesh.
                if let Some(event) = region.slice().iter().find_map(|rank| {
                    health_state
                        .crashed_ranks
                        .get(&rank)
                        .map(|entry| entry.value().clone())
                }) {
                    return Err(SupervisionError::new_err(format!(
                        "Actor {} is unhealthy with reason: {}",
                        event.actor_id, event.actor_status
                    )));
                }
            }
        }
        <ActorMeshRef<PythonActor> as ActorMeshProtocol>::cast(
            &mesh_ref, message, selection, instance,
        )
    }

    fn supervision_event(&self, instance: &PyInstance) -> PyResult<Option<PyShared>> {
        // Make a clone so each endpoint can get the same supervision events.
        let unhandled = self.get_unhandled(instance);
        let monitor = self.monitor().clone();
        let mut receiver = self.supervision_receiver(instance, &monitor, unhandled, None);
        PyPythonTask::new(async move {
            receiver.changed().await.map_err(|e| {
                PyValueError::new_err(format!("Waiting for supervision event change: {}", e))
            })?;
            let event = receiver.borrow();
            let result = if let Some(pyerr) = event.as_ref() {
                Err(Python::with_gil(move |py| pyerr.clone_ref(py)))
            } else {
                tracing::error!("Received None on watch channel for supervision events");
                Ok(())
            };
            // Make sure the task is kept alive until after the receiver has been
            // read from. If it is dropped too early the sender will close before
            // the receiver is read.
            // The &self is not sufficient to keep the task alive, because this
            // future may outlive the PythonActorMeshImpl!
            drop(monitor);
            result
        })?
        .spawn_abortable()
        .map(Some)
    }

    fn start_supervision(
        &self,
        instance: &PyInstance,
        supervision_display_name: String,
    ) -> PyResult<()> {
        // Fetch the receiver once, this will initialize the monitor task.
        let unhandled = self.get_unhandled(instance);
        let monitor = self.monitor().clone();
        self.supervision_receiver(
            instance,
            &monitor,
            unhandled,
            Some(supervision_display_name),
        );
        Ok(())
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        // The sliced mesh will not share the health state as the original mesh.
        assert!(region.as_inner().is_subset(self.mesh_ref().region()));
        Ok(Box::new(PythonActorMeshImpl::new_ref(
            self.mesh_ref().sliced(region.as_inner().clone()),
        )))
    }

    fn stop(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        let (slf, instance) = Python::with_gil(|_py| (self.clone(), instance.clone()));
        match slf {
            PythonActorMeshImpl::Owned(mesh) => PyPythonTask::new(async move {
                instance_dispatch!(instance, |cx_instance| {
                    mesh.mesh
                        .stop(cx_instance)
                        .await
                        .map_err(|err| PyValueError::new_err(err.to_string()))?
                });
                Ok(())
            }),
            PythonActorMeshImpl::Ref(_) => Err(PyErr::new::<PyNotImplementedError, _>(
                "Cannot call stop on an ActorMeshRef, requires an owned ActorMesh",
            )),
        }
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.mesh_ref().__reduce__(py)
    }
}

impl ActorMeshProtocol for ActorMeshRef<PythonActor> {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        if structurally_equal(&selection, &Selection::All(Box::new(Selection::True))) {
            instance_dispatch!(instance, |cx_instance| {
                self.cast(cx_instance, message.clone())
                    .map_err(|err| PyException::new_err(err.to_string()))?;
            });
        } else if structurally_equal(&selection, &Selection::Any(Box::new(Selection::True))) {
            let region = Ranked::region(self);
            let random_rank = fastrand::usize(0..region.num_ranks());
            let offset = region
                .slice()
                .get(random_rank)
                .map_err(anyhow::Error::from)?;
            let singleton_region = Region::new(
                Vec::new(),
                Slice::new(offset, Vec::new(), Vec::new()).map_err(anyhow::Error::from)?,
            );
            instance_dispatch!(instance, |cx_instance| {
                self.sliced(singleton_region)
                    .cast(cx_instance, message.clone())
                    .map_err(|err| PyException::new_err(err.to_string()))?;
            });
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "invalid selection: {:?}",
                selection
            )));
        }

        Ok(())
    }

    fn supervision_event(&self, _instance: &PyInstance) -> PyResult<Option<PyShared>> {
        Err(PyErr::new::<PyNotImplementedError, _>(
            "This should never be called on ActorMeshRef directly",
        ))
    }

    fn start_supervision(
        &self,
        _instance: &PyInstance,
        _supervision_display_name: String,
    ) -> PyResult<()> {
        Err(PyErr::new::<PyNotImplementedError, _>(
            "This should never be called on ActorMeshRef directly",
        ))
    }

    /// Stop the actor mesh asynchronously.
    fn stop(&self, _instance: &PyInstance) -> PyResult<PyPythonTask> {
        Err(PyErr::new::<PyNotImplementedError, _>(
            "This cannot be used on ActorMeshRef, only on owned ActorMesh",
        ))
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        let sliced = self.sliced(region.as_inner().clone());
        Ok(Box::new(sliced))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let bytes =
            bincode::serialize(self).map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let module = py
            .import("monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh")
            .unwrap();
        let from_bytes = module.getattr("py_actor_mesh_from_bytes").unwrap();
        Ok((from_bytes, py_bytes))
    }
}

#[pymethods]
impl PythonActorMeshImpl {
    fn get(&self, rank: usize) -> PyResult<Option<PyActorId>> {
        Ok(self
            .mesh_ref()
            .get(rank)
            .map(|r| ActorRef::into_actor_id(r.clone()))
            .map(PyActorId::from))
    }

    fn __repr__(&self) -> String {
        format!("PythonActorMeshImpl({:?})", self.mesh_ref())
    }
}

#[pyfunction]
fn py_actor_mesh_from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<PythonActorMesh> {
    let r: PyResult<ActorMeshRef<PythonActor>> = bincode::deserialize(bytes.as_bytes())
        .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()));
    r.map(|r| PythonActorMesh::from_impl(Box::new(PythonActorMeshImpl::new_ref(r))))
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PythonActorMeshImpl>()?;
    let f = wrap_pyfunction!(py_actor_mesh_from_bytes, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh",
    )?;
    hyperactor_mod.add_function(f)?;
    Ok(())
}
