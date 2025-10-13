/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::Arc;

use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::RemoteMessage;
use hyperactor::actor::Referable;
use hyperactor::actor::RemotableActor;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh::dashmap::DashMap;
use hyperactor_mesh::proc_mesh::mesh_agent::ActorState;
use hyperactor_mesh::resource;
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
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor_mesh::ActorMeshProtocol;
use crate::actor_mesh::PythonActorMesh;
use crate::context::PyInstance;
use crate::instance_dispatch;
use crate::proc::PyActorId;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;
use crate::runtime::get_tokio_runtime;
use crate::shape::PyRegion;
use crate::supervision::SupervisionError;
use crate::supervision::Unhealthy;

#[derive(Debug)]
struct RootHealthState {
    unhealthy_event: std::sync::Mutex<Unhealthy<ActorSupervisionEvent>>,
    crashed_ranks: DashMap<usize, ActorSupervisionEvent>,
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) struct PyActorMesh(ActorMesh<PythonActor>, Arc<RootHealthState>);

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMeshRef",
    module = "monarch._rust_bindings.monarch_hyperactor.v1.actor_mesh"
)]
pub(crate) struct PyActorMeshRef(ActorMeshRef<PythonActor>, Arc<RootHealthState>);

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
        let health_state = Arc::new(RootHealthState {
            unhealthy_event: std::sync::Mutex::new(Unhealthy::SoFarSoGood),
            crashed_ranks: DashMap::new(),
        });
        PythonActorMeshImpl::Owned(PyActorMesh(inner, health_state))
    }

    /// Get a new ref-based [`PythonActorMeshImpl`].
    pub(crate) fn new_ref(inner: ActorMeshRef<PythonActor>) -> Self {
        let health_state = Arc::new(RootHealthState {
            unhealthy_event: std::sync::Mutex::new(Unhealthy::SoFarSoGood),
            crashed_ranks: DashMap::new(),
        });
        PythonActorMeshImpl::Ref(PyActorMeshRef(inner, health_state))
    }

    fn mesh_ref(&self) -> ActorMeshRef<PythonActor> {
        match self {
            PythonActorMeshImpl::Owned(inner) => (*inner.0).clone(),
            PythonActorMeshImpl::Ref(inner) => inner.0.clone(),
        }
    }

    fn health_state(&self) -> &Arc<RootHealthState> {
        match self {
            PythonActorMeshImpl::Owned(inner) => &inner.1,
            PythonActorMeshImpl::Ref(inner) => &inner.1,
        }
    }
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
/// * time_between_tasks controls how frequently to poll.
#[allow(dead_code)]
pub fn actor_states_monitor<A, C>(
    mesh: ActorMeshRef<A>,
    cx: Instance<C>,
    sender: PortRef<(
        Point,
        Option<resource::State<ActorState>>,
        resource::State<ActorState>,
    )>,
    time_between_checks: tokio::time::Duration,
) -> tokio::task::JoinHandle<()>
where
    A: Actor + RemotableActor + Referable,
    C: Actor + RemotableActor + std::fmt::Debug,
    A::Params: RemoteMessage,
    C::Params: RemoteMessage,
{
    // This implementation polls every "time_between_checks" duration, checking
    // for changes in the actor states. It can be improved in two ways:
    // 1. Use accumulation, to get *any* actor with a change in state, not *all*
    //    actors.
    // 2. Use a push-based mode instead of polling.
    get_tokio_runtime().spawn(async move {
        let mut existing_states: HashMap<Point, resource::State<ActorState>> = HashMap::new();
        let send = |point,
                    old_state: Option<resource::State<ActorState>>,
                    new_state: resource::State<ActorState>| {
            sender
                .send(&cx, (point, old_state.clone(), new_state.clone()))
                .expect("Unsuccessful send of ActorStateChangeMessage to sender");
        };
        loop {
            // Wait in between checking to avoid using too much network.
            RealClock.sleep(time_between_checks).await;
            match mesh.actor_states(&cx).await {
                Ok(events) => {
                    // This returned point is the created rank, *not* the rank of
                    // the possibly sliced input mesh.
                    for (point, state) in events.iter() {
                        let entry = existing_states.entry(point.clone()).or_insert_with(|| {
                            // If this actor is new, send a message to the owner.
                            send(point.clone(), None, state.clone());
                            state.clone()
                        });
                        if entry.status != state.status {
                            send(point.clone(), Some(entry.clone()), state.clone());
                            *entry = state;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "error in task accessing actor_states on {:?}: {:?}",
                        mesh,
                        e
                    );
                    break;
                }
            };
        }
    })
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
        // TODO: Fix performance issue with many actor_states_monitor being spawned.
        Ok(None)
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        // The sliced mesh will not share the health state as the original mesh.
        assert!(region.as_inner().is_subset(self.mesh_ref().region()));
        Ok(Box::new(PythonActorMeshImpl::new_ref(
            self.mesh_ref().sliced(region.as_inner().clone()),
        )))
    }

    fn stop<'py>(&self) -> PyResult<PyPythonTask> {
        Err(PyErr::new::<PyNotImplementedError, _>(
            "stop is not implemented yet for v1::PythonActorMeshImpl",
        ))
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
