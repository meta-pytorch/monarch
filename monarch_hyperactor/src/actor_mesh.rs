/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Weak;

use futures::future::FutureExt;
use futures::future::Shared;
use hyperactor::ActorRef;
use hyperactor::actor::ActorStatus;
use hyperactor::id;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::actor_mesh::ActorSupervisionEvents;
use hyperactor_mesh::dashmap::DashMap;
use hyperactor_mesh::reference::ActorMeshRef;
use hyperactor_mesh::sel;
use hyperactor_mesh::shared_cell::SharedCell;
use hyperactor_mesh::shared_cell::SharedCellRef;
use ndslice::Selection;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::context::PyInstance;
use crate::instance_dispatch;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PyMailbox;
use crate::proc::PyActorId;
use crate::proc_mesh::Keepalive;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PyShared;
use crate::runtime::get_tokio_runtime;
use crate::shape::PyRegion;
use crate::supervision::SupervisionError;
use crate::supervision::Unhealthy;

/// Trait defining the common interface for actor mesh, mesh ref and actor mesh implementations.
/// This corresponds to the Python ActorMeshProtocol ABC.
pub(crate) trait ActorMeshProtocol: Send + Sync {
    /// Cast a message to actors selected by the given selection using the specified mailbox.
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()>;

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)>;

    /// Get supervision events for this actor mesh.
    /// Returns None by default for implementations that don't support supervision events.
    fn supervision_event(&self) -> PyResult<Option<PyShared>> {
        Ok(None)
    }

    /// Stop the actor mesh asynchronously.
    /// Default implementation raises NotImplementedError for types that don't support stopping.
    fn stop(&self) -> PyResult<PyPythonTask> {
        Err(PyNotImplementedError::new_err(format!(
            "stop() is not supported for {}",
            std::any::type_name::<Self>()
        )))
    }

    /// Initialize the actor mesh asynchronously.
    /// Default implementation returns None (no initialization needed).
    fn initialized(&self) -> PyResult<PyPythonTask> {
        PyPythonTask::new(async { Ok(None::<()>) })
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>>;
}

/// This just forwards to the rust trait that can implement these bindings
#[pyclass(
    name = "PythonActorMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.actor_mesh"
)]
pub(crate) struct PythonActorMesh {
    inner: Box<dyn ActorMeshProtocol>,
}

impl PythonActorMesh {
    pub(crate) fn new<F>(f: F, supervised: bool) -> Self
    where
        F: Future<Output = PyResult<Box<dyn ActorMeshProtocol>>> + Send + 'static,
    {
        PythonActorMesh {
            inner: Box::new(AsyncActorMesh::new_queue(f, supervised)),
        }
    }
    pub(crate) fn from_impl(inner: Box<dyn ActorMeshProtocol>) -> Self {
        PythonActorMesh { inner }
    }
}

pub(crate) fn to_hy_sel(selection: &str) -> PyResult<Selection> {
    match selection {
        "choose" => Ok(sel!(?)),
        "all" => Ok(sel!(*)),
        _ => Err(PyErr::new::<PyValueError, _>(format!(
            "Invalid selection: {}",
            selection
        ))),
    }
}

#[pymethods]
impl PythonActorMesh {
    fn cast(
        &self,
        message: &PythonMessage,
        selection: &str,
        instance: &PyInstance,
    ) -> PyResult<()> {
        let sel = to_hy_sel(selection)?;
        self.inner.cast(message.clone(), sel, instance)
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<PythonActorMesh> {
        let inner = self.inner.new_with_region(region)?;
        Ok(PythonActorMesh { inner })
    }

    fn supervision_event(&self) -> PyResult<Option<PyShared>> {
        self.inner.supervision_event()
    }

    fn stop(&self) -> PyResult<PyPythonTask> {
        self.inner.stop()
    }

    fn initialized(&self) -> PyResult<PyPythonTask> {
        self.inner.initialized()
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.inner.__reduce__(py)
    }

    #[staticmethod]
    fn from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<PythonActorMesh> {
        let r: PyResult<PythonActorMeshRef> = bincode::deserialize(bytes.as_bytes())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()));
        r.map(|r| PythonActorMesh { inner: Box::new(r) })
    }
}

#[pyclass(
    name = "PythonActorMeshImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.actor_mesh"
)]
pub(crate) struct PythonActorMeshImpl {
    inner: SharedCell<RootActorMesh<'static, PythonActor>>,
    #[allow(dead_code)]
    client: PyMailbox, // Never read.
    _keepalive: Keepalive,
    monitor: tokio::task::JoinHandle<()>,
    health_state: Arc<RootHealthState>,
}

impl PythonActorMeshImpl {
    /// Create a new [`PythonActorMesh`] with a monitor that will observe supervision
    /// errors for this mesh, and update its state properly.
    pub(crate) fn new(
        inner: SharedCell<RootActorMesh<'static, PythonActor>>,
        client: PyMailbox,
        keepalive: Keepalive,
        events: ActorSupervisionEvents,
    ) -> Self {
        let (user_monitor_sender, _) =
            tokio::sync::broadcast::channel::<Option<ActorSupervisionEvent>>(1);
        let health_state = Arc::new(RootHealthState {
            user_monitor_sender,
            unhealthy_event: std::sync::Mutex::new(Unhealthy::SoFarSoGood),
            crashed_ranks: DashMap::new(),
        });
        let monitor = tokio::spawn(Self::actor_mesh_monitor(events, health_state.clone()));
        PythonActorMeshImpl {
            inner,
            client,
            _keepalive: keepalive,
            monitor,
            health_state,
        }
    }
    /// Monitor of the actor mesh. It processes supervision errors for the mesh, and keeps mesh
    /// health state up to date.
    async fn actor_mesh_monitor(
        mut events: ActorSupervisionEvents,
        health_state: Arc<RootHealthState>,
    ) {
        loop {
            let event = events.next().await;
            tracing::debug!("actor_mesh_monitor received supervision event: {event:?}");
            {
                let mut inner_unhealthy_event = health_state.unhealthy_event.lock().unwrap();
                match &event {
                    None => *inner_unhealthy_event = Unhealthy::StreamClosed,
                    Some(event) => {
                        health_state
                            .crashed_ranks
                            .insert(event.actor_id.rank(), event.clone());
                        *inner_unhealthy_event = Unhealthy::Crashed(event.clone())
                    }
                }
            }

            // Ignore the sender error when there is no receiver,
            // which happens when there is no active requests to this
            // mesh.
            let ret = health_state.user_monitor_sender.send(event.clone());
            tracing::debug!("actor_mesh_monitor user_sender send: {ret:?}");

            if event.is_none() {
                // The mesh is stopped, so we can stop the monitor.
                break;
            }
        }
    }

    fn try_inner(&self) -> PyResult<SharedCellRef<RootActorMesh<'static, PythonActor>>> {
        self.inner.borrow().map_err(|_| {
            SupervisionError::new_err("`PythonActorMesh` has already been stopped".to_string())
        })
    }

    fn bind(&self) -> PyResult<PythonActorMeshRef> {
        let mesh = self.try_inner()?;
        let root_health_state = Some(Arc::downgrade(&self.health_state));
        Ok(PythonActorMeshRef {
            inner: mesh.bind(),
            root_health_state,
        })
    }
}

impl ActorMeshProtocol for PythonActorMeshImpl {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        let unhealthy_event = self
            .health_state
            .unhealthy_event
            .lock()
            .expect("failed to acquire unhealthy_event lock");

        match &*unhealthy_event {
            Unhealthy::SoFarSoGood => (),
            Unhealthy::Crashed(event) => {
                return Err(SupervisionError::new_err(format!(
                    "Actor {:?} is unhealthy with reason: {}",
                    event.actor_id, event.actor_status
                )));
            }
            Unhealthy::StreamClosed => {
                return Err(SupervisionError::new_err(
                    "actor mesh is stopped due to proc mesh shutdown".to_string(),
                ));
            }
        }

        instance_dispatch!(instance, |cx_instance| {
            self.try_inner()?
                .cast(cx_instance, selection, message)
                .map_err(|err| PyException::new_err(err.to_string()))?;
        });
        Ok(())
    }

    fn supervision_event(&self) -> PyResult<Option<PyShared>> {
        let mut receiver = self.health_state.user_monitor_sender.subscribe();
        PyPythonTask::new(async move {
            let event = receiver.recv().await;
            let event = match event {
                Ok(Some(event)) => PyActorSupervisionEvent::from(event.clone()),
                Ok(None) | Err(_) => PyActorSupervisionEvent::from(ActorSupervisionEvent::new(
                    // Dummy actor as placeholder to indicate the whole mesh is stopped
                    // TODO(albertli): remove this when pushing all supervision logic to rust.
                    id!(default[0].actor[0]),
                    ActorStatus::Failed("actor mesh is stopped due to proc mesh shutdown".into()),
                    None,
                    None,
                )),
            };
            Ok(PyErr::new::<SupervisionError, _>(format!(
                "Actor {:?} exited because of the following reason: {}",
                event.actor_id(),
                event.__repr__()?
            )))
        })
        .map(|mut x| x.spawn().map(Some))?
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        self.bind()?.new_with_region(region)
    }

    fn stop<'py>(&self) -> PyResult<PyPythonTask> {
        let actor_mesh = self.inner.clone();
        PyPythonTask::new(async move {
            let actor_mesh = actor_mesh
                .take()
                .await
                .map_err(|_| PyRuntimeError::new_err("`ActorMesh` has already been stopped"))?;
            actor_mesh.stop().await.map_err(|err| {
                PyException::new_err(format!("Failed to stop actor mesh: {}", err))
            })?;
            Ok(())
        })
    }
    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.bind()?.__reduce__(py)
    }
}

#[pymethods]
impl PythonActorMeshImpl {
    fn get_supervision_event(&self) -> PyResult<Option<PyActorSupervisionEvent>> {
        let unhealthy_event = self
            .health_state
            .unhealthy_event
            .lock()
            .expect("failed to acquire unhealthy_event lock");

        match &*unhealthy_event {
            Unhealthy::SoFarSoGood => Ok(None),
            Unhealthy::StreamClosed => {
                Ok(Some(PyActorSupervisionEvent::from(
                    ActorSupervisionEvent::new(
                        // Dummy actor as placeholder to indicate the whole mesh is stopped
                        // TODO(albertli): remove this when pushing all supervision logic to rust.
                        id!(default[0].actor[0]),
                        ActorStatus::Failed(
                            "actor mesh is stopped due to proc mesh shutdown".into(),
                        ),
                        None,
                        None,
                    ),
                )))
            }
            Unhealthy::Crashed(event) => Ok(Some(PyActorSupervisionEvent::from(event.clone()))),
        }
    }

    fn supervision_event(&self) -> PyResult<Option<PyShared>> {
        ActorMeshProtocol::supervision_event(self)
    }
    fn stop(&self) -> PyResult<PyPythonTask> {
        ActorMeshProtocol::stop(self)
    }
    // Consider defining a "PythonActorRef", which carries specifically
    // a reference to python message actors.
    fn get(&self, rank: usize) -> PyResult<Option<PyActorId>> {
        Ok(self
            .try_inner()?
            .get(rank)
            .map(ActorRef::into_actor_id)
            .map(PyActorId::from))
    }

    #[getter]
    fn stopped(&self) -> PyResult<bool> {
        Ok(self.inner.borrow().is_err())
    }
}

#[derive(Debug)]
struct RootHealthState {
    user_monitor_sender: tokio::sync::broadcast::Sender<Option<ActorSupervisionEvent>>,
    unhealthy_event: std::sync::Mutex<Unhealthy<ActorSupervisionEvent>>,
    crashed_ranks: DashMap<usize, ActorSupervisionEvent>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PythonActorMeshRef {
    inner: ActorMeshRef<PythonActor>,
    #[serde(skip)]
    // If the reference has been serialized and sent over the wire
    // we no longer have access to the underlying mesh's state
    root_health_state: Option<Weak<RootHealthState>>,
}

impl ActorMeshProtocol for PythonActorMeshRef {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        if let Some(root_health_state) = &self.root_health_state {
            // MeshRef has not been serialized and sent over the wire so we can actually validate
            // if the underlying mesh still exists
            if let Some(root_health_state) = root_health_state.upgrade() {
                // iterate through all crashed ranks in the root mesh and take first rank
                // that is in the sliced mesh
                match self.inner.shape().slice().iter().find_map(|rank| {
                    root_health_state
                        .crashed_ranks
                        .get(&rank)
                        .map(|entry| entry.value().clone())
                }) {
                    Some(event) => {
                        return Err(SupervisionError::new_err(format!(
                            "Actor {:?} is unhealthy with reason: {}",
                            event.actor_id, event.actor_status
                        )));
                    }
                    None => {
                        if matches!(
                            &*root_health_state
                                .unhealthy_event
                                .lock()
                                .unwrap_or_else(|e| e.into_inner()),
                            Unhealthy::StreamClosed
                        ) {
                            return Err(SupervisionError::new_err(
                                "actor mesh is stopped due to proc mesh shutdown".to_string(),
                            ));
                        }
                    }
                }
            } else {
                return Err(SupervisionError::new_err(
                    "actor mesh is stopped due to proc mesh shutdown".to_string(),
                ));
            }
        }

        instance_dispatch!(instance, |cx_instance| {
            self.inner
                .cast(cx_instance, selection, message)
                .map_err(|err| PyException::new_err(err.to_string()))?;
        });
        Ok(())
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        let sliced = self
            .inner
            .new_with_shape(region.as_inner().into())
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        Ok(Box::new(Self {
            inner: sliced,
            root_health_state: self.root_health_state.clone(),
        }))
    }

    fn supervision_event(&self) -> PyResult<Option<PyShared>> {
        match self.root_health_state.as_ref().and_then(|x| x.upgrade()) {
            Some(root_health_state) => {
                let mut receiver = root_health_state.user_monitor_sender.subscribe();
                let slice = self.inner.shape().slice().clone();
                PyPythonTask::new(async move {
                    while let Ok(Some(event)) =  receiver.recv().await {
                        if slice.iter().any(|rank| rank == event.actor_id.rank()) {
                            return Ok(PyErr::new::<SupervisionError, _>(format!(
                                "Actor {:?} exited because of the following reason: {}",
                                event.actor_id, event.actor_status
                            )));
                        }
                    }
                    Ok(PyErr::new::<SupervisionError, _>(format!(
                        "Actor {:?} exited because of the following reason: actor mesh is stopped due to proc mesh shutdown",
                        id!(default[0].actor[0])
                    )))
                })
                .map(|mut x| x.spawn().map(Some))?
            }
            None => Ok(None),
        }
    }
    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let bytes =
            bincode::serialize(self).map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let module = py
            .import("monarch._rust_bindings.monarch_hyperactor.actor_mesh")
            .unwrap();
        let from_bytes = module
            .getattr("PythonActorMesh")
            .unwrap()
            .getattr("from_bytes")
            .unwrap();
        Ok((from_bytes, py_bytes))
    }
}

impl Drop for PythonActorMeshImpl {
    fn drop(&mut self) {
        if let Ok(mesh) = self.inner.borrow() {
            tracing::debug!("Dropping PythonActorMesh: {}", mesh.name());
        } else {
            tracing::debug!(
                "Dropping stopped PythonActorMesh. The underlying mesh is already stopped."
            );
        }
        self.monitor.abort();
    }
}
struct ClonePyErr {
    inner: PyErr,
}

impl From<ClonePyErr> for PyErr {
    fn from(value: ClonePyErr) -> PyErr {
        value.inner
    }
}
impl From<PyErr> for ClonePyErr {
    fn from(inner: PyErr) -> ClonePyErr {
        ClonePyErr { inner }
    }
}

impl Clone for ClonePyErr {
    fn clone(&self) -> Self {
        Python::with_gil(|py| self.inner.clone_ref(py).into())
    }
}

type ActorMeshResult = Result<Arc<dyn ActorMeshProtocol>, ClonePyErr>;
pub(crate) struct AsyncActorMesh {
    mesh: Shared<Pin<Box<dyn Future<Output = ActorMeshResult> + Send>>>,
    queue: UnboundedSender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    supervised: bool,
}

impl AsyncActorMesh {
    pub(crate) fn new_queue<F>(f: F, supervised: bool) -> AsyncActorMesh
    where
        F: Future<Output = PyResult<Box<dyn ActorMeshProtocol>>> + Send + 'static,
    {
        let (queue, mut recv) = unbounded_channel();

        get_tokio_runtime().spawn(async move {
            loop {
                let r = recv.recv().await;
                if let Some(r) = r {
                    r.await;
                } else {
                    return;
                }
            }
        });
        AsyncActorMesh::new(queue, supervised, f)
    }
    fn new<F>(
        queue: UnboundedSender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
        supervised: bool,
        f: F,
    ) -> AsyncActorMesh
    where
        F: Future<Output = PyResult<Box<dyn ActorMeshProtocol>>> + Send + 'static,
    {
        let mesh = async { Ok(Arc::from(f.await?)) }.boxed().shared();
        AsyncActorMesh {
            mesh,
            queue,
            supervised,
        }
    }

    fn push<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.queue.send(f.boxed()).unwrap();
    }
}

impl ActorMeshProtocol for AsyncActorMesh {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        let mesh = self.mesh.clone();
        let instance = instance.clone();
        self.push(async move {
            let port = match &message.kind {
                PythonMessageKind::CallMethod { response_port, .. } => response_port.clone(),
                _ => None,
            };
            let result = async { mesh.await?.cast(message, selection, &instance) }.await;
            match (port, result) {
                (Some(p), Err(pyerr)) => Python::with_gil(|py: Python<'_>| {
                    let port_ref = match p {
                        EitherPortRef::Once(p) => p.into_bound_py_any(py),
                        EitherPortRef::Unbounded(p) => p.into_bound_py_any(py),
                    }
                    .unwrap();
                    let port = py
                        .import("monarch._src.actor.actor_mesh")
                        .unwrap()
                        .call_method1("Port", (port_ref, instance._mailbox(), 0))
                        .unwrap();
                    port.call_method1("exception", (pyerr.value(py),)).unwrap();
                }),
                _ => (),
            }
        });
        Ok(())
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        let mesh = self.mesh.clone();
        let region = region.clone();
        Ok(Box::new(AsyncActorMesh::new(
            self.queue.clone(),
            self.supervised,
            async move { mesh.await?.new_with_region(&region) },
        )))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let mesh = self.mesh.clone();
        let mesh = py.allow_threads(|| get_tokio_runtime().block_on(mesh));
        mesh?.__reduce__(py)
    }

    fn supervision_event(&self) -> PyResult<Option<PyShared>> {
        if !self.supervised {
            return Ok(None);
        }
        let mesh = self.mesh.clone();
        PyPythonTask::new(async {
            let mut event = mesh.await?.supervision_event()?.unwrap();
            event.task()?.take_task()?.await
        })
        .map(|mut x| x.spawn().map(Some))?
    }

    fn stop(&self) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        PyPythonTask::new(async {
            let task = mesh.await?.stop()?.take_task()?;
            task.await
        })
    }

    fn initialized<'py>(&self) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        PyPythonTask::new(async {
            mesh.await?;
            Ok(None::<()>)
        })
    }
}

#[pyclass(
    name = "ActorSupervisionEvent",
    module = "monarch._rust_bindings.monarch_hyperactor.actor_mesh"
)]
#[derive(Debug)]
pub struct PyActorSupervisionEvent {
    inner: ActorSupervisionEvent,
}

#[pymethods]
impl PyActorSupervisionEvent {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<PyActorSupervisionEvent: {}>", self.inner))
    }

    #[getter]
    fn actor_id(&self) -> PyResult<PyActorId> {
        Ok(PyActorId::from(self.inner.actor_id.clone()))
    }

    #[getter]
    fn actor_status(&self) -> PyResult<String> {
        Ok(self.inner.actor_status.to_string())
    }
}

impl From<ActorSupervisionEvent> for PyActorSupervisionEvent {
    fn from(event: ActorSupervisionEvent) -> Self {
        PyActorSupervisionEvent { inner: event }
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PythonActorMesh>()?;
    hyperactor_mod.add_class::<PythonActorMeshImpl>()?;
    hyperactor_mod.add_class::<PyActorSupervisionEvent>()?;
    Ok(())
}
