/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use futures::future;
use futures::future::FutureExt;
use futures::future::Shared;
use hyperactor::ActorRef;
use hyperactor::Instance;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh::sel;
use hyperactor_mesh::selection::Selection;
use hyperactor_mesh::v1::actor_mesh::ActorMesh;
use hyperactor_mesh::v1::actor_mesh::ActorMeshRef;
use monarch_types::py_global;
use monarch_types::py_module_add_function;
use ndslice::Region;
use ndslice::Slice;
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
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::context::PyInstance;
use crate::mailbox::EitherPortRef;
use crate::proc::PyActorId;
use crate::pytokio::PendingPickle;
use crate::pytokio::PyPythonTask;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on;
use crate::shape::PyRegion;
use crate::supervision::PySupervisor;
use crate::supervision::Supervisable;
use crate::supervision::SupervisionError;

py_global!(
    is_pending_pickle_allowed,
    "monarch._src.actor.pickle",
    "is_pending_pickle_allowed"
);

/// Trait defining the common interface for actor mesh, mesh ref and actor mesh implementations.
/// This corresponds to the Python ActorMeshProtocol ABC.
#[async_trait]
pub(crate) trait ActorMeshProtocol: Supervisable + Send + Sync {
    /// Cast a message to actors selected by the given selection using the specified mailbox.
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()>;

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)>;

    /// Stop the actor mesh asynchronously.
    /// Default implementation raises NotImplementedError for types that don't support stopping.
    fn stop(&self, _instance: &PyInstance) -> PyResult<PyPythonTask> {
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
#[derive(Clone)]
pub(crate) struct PythonActorMesh {
    inner: Arc<dyn ActorMeshProtocol>,
}

impl PythonActorMesh {
    pub(crate) fn new<F>(f: F, supervised: bool) -> Self
    where
        F: Future<Output = PyResult<Box<dyn ActorMeshProtocol>>> + Send + 'static,
    {
        let f = async move { Ok(Arc::from(f.await?)) }.boxed().shared();
        PythonActorMesh {
            inner: Arc::new(AsyncActorMesh::new_queue(f, supervised)),
        }
    }

    pub(crate) fn from_impl(inner: Arc<dyn ActorMeshProtocol>) -> Self {
        PythonActorMesh { inner }
    }
}

#[async_trait]
impl Supervisable for PythonActorMesh {
    async fn next_supervision_event(&self, instance: &Instance<PythonActor>) -> Option<PyErr> {
        self.inner.next_supervision_event(instance).await
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
    #[hyperactor::instrument]
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
        Ok(PythonActorMesh {
            inner: Arc::from(inner),
        })
    }

    fn start_supervision(
        &self,
        instance: &PyInstance,
        supervision_display_name: String,
    ) -> PyResult<()> {
        self.inner
            .start_supervision(instance, supervision_display_name)
    }

    fn stop(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        self.inner.stop(instance)
    }

    fn initialized(&self) -> PyResult<PyPythonTask> {
        self.inner.initialized()
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.inner.__reduce__(py)
    }

    /// Returns a Supervisor that can be used to monitor actor health.
    ///
    /// This is used by endpoint operations to race supervision events
    /// against message receipt.
    fn as_supervisor(&self) -> PySupervisor {
        PySupervisor::new(self.clone())
    }
}

#[derive(Debug)]
pub(crate) struct ClonePyErr {
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
        monarch_with_gil_blocking(|py| self.inner.clone_ref(py).into())
    }
}

type ActorMeshResult = Result<Arc<dyn ActorMeshProtocol>, ClonePyErr>;
type ActorMeshFut = Shared<Pin<Box<dyn Future<Output = ActorMeshResult> + Send + 'static>>>;

pub(crate) struct AsyncActorMesh {
    mesh: ActorMeshFut,
    queue: UnboundedSender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    supervised: bool,
}

impl AsyncActorMesh {
    pub(crate) fn new_queue(f: ActorMeshFut, supervised: bool) -> AsyncActorMesh {
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

    fn new(
        queue: UnboundedSender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
        supervised: bool,
        f: ActorMeshFut,
    ) -> AsyncActorMesh {
        AsyncActorMesh {
            mesh: f,
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

    pub(crate) fn from_impl(mesh: Arc<dyn ActorMeshProtocol>) -> Self {
        let fut = future::ready(Ok::<Arc<dyn ActorMeshProtocol>, ClonePyErr>(mesh))
            .boxed()
            .shared();
        // Poll the future so that its result can be observed without blocking the tokio runtime.
        let _ = futures::executor::block_on(fut.clone());
        Self::new_queue(fut, true)
    }
}

#[async_trait]
impl ActorMeshProtocol for AsyncActorMesh {
    fn cast(
        &self,
        mut message: PythonMessage,
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
            let result = async {
                if let Some(pickle_state) = message.pending_pickle_state.take() {
                    message.message = pickle_state.resolve(message.message.into_bytes()).await?;
                }
                mesh.await?.cast(message, selection, &instance)
            }
            .await;
            if let (Some(p), Err(pyerr)) = (port, result) {
                let _ = monarch_with_gil(|py: Python<'_>| {
                    let port_ref = match p {
                        EitherPortRef::Once(p) => p.into_bound_py_any(py),
                        EitherPortRef::Unbounded(p) => p.into_bound_py_any(py),
                    }
                    .unwrap();
                    let port = py
                        .import("monarch._src.actor.actor_mesh")
                        .unwrap()
                        .call_method1("Port", (port_ref, instance, 0))
                        .unwrap();
                    port.call_method1("exception", (pyerr.value(py),)).unwrap();
                    Ok::<_, PyErr>(())
                })
                .await;
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
            async move { Ok(Arc::from(mesh.await?.new_with_region(&region)?)) }
                .boxed()
                .shared(),
        )))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let fut = self.mesh.clone();
        match fut.peek().cloned() {
            Some(mesh) => mesh?.__reduce__(py),
            None => {
                if !is_pending_pickle_allowed(py).call0()?.is_truthy()? {
                    return signal_safe_block_on(py, fut)??.__reduce__(py);
                }

                let ident = py
                    .import("monarch._rust_bindings.monarch_hyperactor.actor_mesh")?
                    .getattr("py_identity")?;
                let fut = self.mesh.clone();
                Ok((
                    ident,
                    (PendingPickle::from_future(
                        async move {
                            let mesh = PythonActorMesh::from_impl(fut.await?);
                            monarch_with_gil(|py| mesh.into_py_any(py)).await
                        }
                        .boxed(),
                    )?,)
                        .into_bound_py_any(py)?,
                ))
            }
        }
    }

    fn stop(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        let instance = monarch_with_gil_blocking(|_py| instance.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push(async move {
            let result = async move { mesh.await?.stop(&instance)?.take_task()?.await }.await;
            if tx.send(result).is_err() {
                panic!("oneshot failed");
            }
        });
        PyPythonTask::new(async move { rx.await.map_err(anyhow::Error::from)? })
    }

    fn initialized(&self) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        PyPythonTask::new(async {
            mesh.await?;
            Ok(None::<()>)
        })
    }
}

#[async_trait]
impl Supervisable for AsyncActorMesh {
    async fn next_supervision_event(&self, instance: &Instance<PythonActor>) -> Option<PyErr> {
        if !self.supervised {
            return None;
        }
        let mesh = self.mesh.clone();
        match mesh.await {
            Ok(mesh) => mesh.next_supervision_event(instance).await,
            Err(e) => Some(e.into()),
        }
    }

    fn start_supervision(
        &self,
        instance: &PyInstance,
        supervision_display_name: String,
    ) -> PyResult<()> {
        if !self.supervised {
            return Ok(());
        }
        let mesh = self.mesh.clone();
        let instance = monarch_with_gil_blocking(|_py| instance.clone());
        self.push(async move {
            let mesh = mesh.await;
            if let Ok(mesh) = mesh {
                mesh.start_supervision(&instance, supervision_display_name)
                    .unwrap();
            }
        });
        Ok(())
    }
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.actor_mesh"
)]
pub(crate) struct PyActorMesh {
    mesh: ActorMesh<PythonActor>,
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "PyActorMeshRef",
    module = "monarch._rust_bindings.monarch_hyperactor.actor_mesh"
)]
pub(crate) struct PyActorMeshRef {
    mesh: ActorMeshRef<PythonActor>,
}

#[derive(Debug, Clone)]
#[pyclass(
    name = "PythonActorMeshImpl",
    module = "monarch._rust_bindings.monarch_hyperactor.actor_mesh"
)]
pub(crate) enum PythonActorMeshImpl {
    Owned(PyActorMesh),
    Ref(PyActorMeshRef),
}

impl PythonActorMeshImpl {
    /// Get a new owned [`PythonActorMeshImpl`].
    pub(crate) fn new_owned(inner: ActorMesh<PythonActor>) -> Self {
        PythonActorMeshImpl::Owned(PyActorMesh { mesh: inner })
    }

    /// Get a new ref-based [`PythonActorMeshImpl`].
    pub(crate) fn new_ref(inner: ActorMeshRef<PythonActor>) -> Self {
        PythonActorMeshImpl::Ref(PyActorMeshRef { mesh: inner })
    }

    fn mesh_ref(&self) -> ActorMeshRef<PythonActor> {
        match self {
            PythonActorMeshImpl::Owned(inner) => (*inner.mesh).clone(),
            PythonActorMeshImpl::Ref(inner) => inner.mesh.clone(),
        }
    }
}

#[async_trait]
impl Supervisable for PythonActorMeshImpl {
    async fn next_supervision_event(&self, instance: &Instance<PythonActor>) -> Option<PyErr> {
        let mesh = self.mesh_ref();
        match mesh.next_supervision_event(instance).await {
            Ok(supervision_failure) => {
                let event = supervision_failure.event;
                Some(SupervisionError::new_err(format!(
                    "Actor {} exited because of the following reason: {}",
                    event.actor_id, event,
                )))
            }
            Err(e) => Some(SupervisionError::new_err(e.to_string())),
        }
    }
}

#[async_trait]
impl ActorMeshProtocol for PythonActorMeshImpl {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        let mesh_ref = self.mesh_ref();

        <ActorMeshRef<PythonActor> as ActorMeshProtocol>::cast(
            &mesh_ref, message, selection, instance,
        )
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        assert!(region.as_inner().is_subset(self.mesh_ref().region()));
        Ok(Box::new(PythonActorMeshImpl::new_ref(
            self.mesh_ref().sliced(region.as_inner().clone()),
        )))
    }

    fn stop(&self, instance: &PyInstance) -> PyResult<PyPythonTask> {
        let (slf, instance) = monarch_with_gil_blocking(|_py| (self.clone(), instance.clone()));
        match slf {
            PythonActorMeshImpl::Owned(mut mesh) => PyPythonTask::new(async move {
                mesh.mesh
                    .stop(instance.deref())
                    .await
                    .map_err(|err| PyValueError::new_err(err.to_string()))
            }),
            PythonActorMeshImpl::Ref(_) => Err(PyNotImplementedError::new_err(
                "Cannot call stop on an ActorMeshRef, requires an owned ActorMesh",
            )),
        }
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.mesh_ref().__reduce__(py)
    }
}

#[async_trait]
impl Supervisable for ActorMeshRef<PythonActor> {
    async fn next_supervision_event(&self, _instance: &Instance<PythonActor>) -> Option<PyErr> {
        panic!("This should never be called on ActorMeshRef directly");
    }

    fn start_supervision(
        &self,
        _instance: &PyInstance,
        _supervision_display_name: String,
    ) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "This should never be called on ActorMeshRef directly",
        ))
    }
}

#[async_trait]
impl ActorMeshProtocol for ActorMeshRef<PythonActor> {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &PyInstance,
    ) -> PyResult<()> {
        if structurally_equal(&selection, &Selection::All(Box::new(Selection::True))) {
            self.cast(instance.deref(), message.clone())
                .map_err(|err| PyException::new_err(err.to_string()))?;
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
            self.sliced(singleton_region)
                .cast(instance.deref(), message.clone())
                .map_err(|err| PyException::new_err(err.to_string()))?;
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "invalid selection: {:?}",
                selection
            )));
        }

        Ok(())
    }

    /// Stop the actor mesh asynchronously.
    fn stop(&self, _instance: &PyInstance) -> PyResult<PyPythonTask> {
        Err(PyNotImplementedError::new_err(
            "This cannot be used on ActorMeshRef, only on owned ActorMesh",
        ))
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn ActorMeshProtocol>> {
        let sliced = self.sliced(region.as_inner().clone());
        Ok(Box::new(sliced))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let bytes = bincode::serialize(self).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let module = py
            .import("monarch._rust_bindings.monarch_hyperactor.actor_mesh")
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
    let r: PyResult<ActorMeshRef<PythonActor>> =
        bincode::deserialize(bytes.as_bytes()).map_err(|e| PyValueError::new_err(e.to_string()));
    r.map(|r| AsyncActorMesh::from_impl(Arc::new(PythonActorMeshImpl::new_ref(r))))
        .map(|r| PythonActorMesh::from_impl(Arc::from(r)))
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
    pub(crate) fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<PyActorSupervisionEvent: {}>", self.inner))
    }

    #[getter]
    pub(crate) fn actor_id(&self) -> PyResult<PyActorId> {
        Ok(PyActorId::from(self.inner.actor_id.clone()))
    }

    #[getter]
    pub(crate) fn actor_status(&self) -> PyResult<String> {
        Ok(self.inner.actor_status.to_string())
    }
}

impl From<ActorSupervisionEvent> for PyActorSupervisionEvent {
    fn from(event: ActorSupervisionEvent) -> Self {
        PyActorSupervisionEvent { inner: event }
    }
}

#[pyfunction]
fn py_identity(obj: Py<PyAny>) -> PyResult<Py<PyAny>> {
    Ok(obj)
}

/// Holds the GIL for the specified number of seconds without releasing it.
///
/// This is a test utility function that spawns a background thread which
/// acquires the GIL using Rust's Python::with_gil and holds it for the
/// specified duration using thread::sleep. Unlike Python code which
/// periodically releases the GIL, this function holds it continuously.
///
/// We intentionally use `std::thread::sleep` here (not `Clock::sleep` or async sleep)
/// because the purpose is to simulate a blocking operation that holds the GIL without
/// releasing it. Using an async sleep would release the GIL periodically, defeating
/// the purpose of this test utility.
///
/// Args:
///     delay_secs: Seconds to wait before acquiring the GIL
///     hold_secs: Seconds to hold the GIL
#[pyfunction]
#[pyo3(name = "hold_gil_for_test", signature = (delay_secs, hold_secs))]
#[allow(clippy::disallowed_methods)] // Intentional: we need blocking sleep to hold the GIL
pub fn hold_gil_for_test(delay_secs: f64, hold_secs: f64) {
    thread::spawn(move || {
        // Wait before grabbing the GIL (blocking sleep is fine here, we're in a spawned thread)
        #[allow(clippy::disallowed_methods)]
        thread::sleep(Duration::from_secs_f64(delay_secs));
        // Acquire and hold the GIL - MUST use blocking sleep to keep GIL held
        Python::with_gil(|_py| {
            tracing::info!("start holding the gil...");
            #[allow(clippy::disallowed_methods)]
            thread::sleep(Duration::from_secs_f64(hold_secs));
            tracing::info!("end holding the gil...");
        });
    });
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    py_module_add_function!(
        hyperactor_mod,
        "monarch._rust_bindings.monarch_hyperactor.actor_mesh",
        py_identity
    );
    py_module_add_function!(
        hyperactor_mod,
        "monarch._rust_bindings.monarch_hyperactor.actor_mesh",
        py_actor_mesh_from_bytes
    );
    py_module_add_function!(
        hyperactor_mod,
        "monarch._rust_bindings.monarch_hyperactor.actor_mesh",
        hold_gil_for_test
    );
    hyperactor_mod.add_class::<PythonActorMesh>()?;
    hyperactor_mod.add_class::<PythonActorMeshImpl>()?;
    hyperactor_mod.add_class::<PyActorSupervisionEvent>()?;
    Ok(())
}
