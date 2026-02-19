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
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::actor_mesh::ActorMeshRef;
use hyperactor_mesh::sel;
use monarch_types::py_global;
use monarch_types::py_module_add_function;
use ndslice::Region;
use ndslice::Slice;
use ndslice::selection::Selection;
use ndslice::selection::structurally_equal;
use ndslice::view::Ranked;
use ndslice::view::RankedSliceable;
use pyo3::IntoPyObjectExt;
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
use crate::proc::PyActorId;
use crate::pytokio::PendingPickle;
use crate::pytokio::PyPythonTask;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on;
use crate::shape::PyRegion;
use crate::supervision::Supervisable;
use crate::supervision::SupervisionError;

py_global!(
    is_pending_pickle_allowed,
    "monarch._src.actor.pickle",
    "is_pending_pickle_allowed"
);
py_global!(_pickle, "monarch._src.actor.actor_mesh", "_pickle");

/// Trait defining the common interface for actor mesh, mesh ref and actor mesh implementations.
/// This corresponds to the Python ActorMeshProtocol ABC.
pub(crate) trait ActorMeshProtocol: Send + Sync {
    /// Cast a message to actors selected by the given selection using the specified mailbox.
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()>;

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)>;

    /// Stop the actor mesh asynchronously.
    /// Default implementation raises NotImplementedError for types that don't support stopping.
    fn stop(&self, _instance: &PyInstance, _reason: String) -> PyResult<PyPythonTask> {
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
}

pub(crate) trait SupervisableActorMesh: ActorMeshProtocol + Supervisable {
    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn SupervisableActorMesh>>;
}

/// This just forwards to the rust trait that can implement these bindings
#[pyclass(
    name = "PythonActorMesh",
    module = "monarch._rust_bindings.monarch_hyperactor.actor_mesh"
)]
#[derive(Clone)]
pub(crate) struct PythonActorMesh {
    inner: Arc<dyn SupervisableActorMesh>,
}

impl PythonActorMesh {
    pub(crate) fn new<F>(f: F, supervised: bool) -> Self
    where
        F: Future<Output = PyResult<Box<dyn SupervisableActorMesh>>> + Send + 'static,
    {
        let f = async move { Ok(Arc::from(f.await?)) }.boxed().shared();
        PythonActorMesh {
            inner: Arc::new(AsyncActorMesh::new_queue(f, supervised)),
        }
    }

    pub(crate) fn from_impl(inner: Arc<dyn SupervisableActorMesh>) -> Self {
        PythonActorMesh { inner }
    }

    pub(crate) fn get_inner(&self) -> Arc<dyn SupervisableActorMesh> {
        self.inner.clone()
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
    #[tracing::instrument(level = "debug", skip_all)]
    #[pyo3(name = "cast")]
    fn py_cast(
        &self,
        message: &PythonMessage,
        selection: &str,
        instance: &PyInstance,
    ) -> PyResult<()> {
        let sel = to_hy_sel(selection)?;
        self.inner.cast(message.clone(), sel, instance.deref())
    }

    fn new_with_region(&self, region: &PyRegion) -> PyResult<PythonActorMesh> {
        let inner = self.inner.new_with_region(region)?;
        Ok(PythonActorMesh {
            inner: Arc::from(inner),
        })
    }

    fn stop(&self, instance: &PyInstance, reason: String) -> PyResult<PyPythonTask> {
        self.inner.stop(instance, reason)
    }

    fn initialized(&self) -> PyResult<PyPythonTask> {
        self.inner.initialized()
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.inner.__reduce__(py)
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

type ActorMeshResult = Result<Arc<dyn SupervisableActorMesh>, ClonePyErr>;
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

        let mesh = AsyncActorMesh::new(queue, supervised, f);
        // Eagerly trigger the mesh initialization by pushing an init task onto
        // the queue. This ensures actors are spawned immediately rather than
        // waiting for the first endpoint call, which is critical for:
        // 1. Tests/code that wait for supervision events from actor __init__
        //    failures without making any endpoint calls.
        // 2. Ensuring all meshes on a proc are spawned before any errors occur,
        //    preventing spawn rejections due to stale supervision events.
        let f = mesh.mesh.clone();
        mesh.push(async move {
            let _ = f.await;
        });
        mesh
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

    pub(crate) fn from_impl(mesh: Arc<dyn SupervisableActorMesh>) -> Self {
        let fut = future::ready(Ok::<Arc<dyn SupervisableActorMesh>, ClonePyErr>(mesh))
            .boxed()
            .shared();
        // Poll the future so that its result can be observed without blocking the tokio runtime.
        let _ = futures::executor::block_on(fut.clone());
        Self::new_queue(fut, true)
    }
}

impl ActorMeshProtocol for AsyncActorMesh {
    fn cast(
        &self,
        mut message: PythonMessage,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        let mesh = self.mesh.clone();
        let instance = instance.clone_for_py();
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
            if let (Some(mut port_ref), Err(pyerr)) = (port, result) {
                let _ = monarch_with_gil(|py: Python<'_>| {
                    port_ref
                        .send(
                            &instance,
                            PythonMessage::new(
                                PythonMessageKind::Exception { rank: Some(0) },
                                _pickle(py).call1((pyerr,))?,
                                None,
                            )?,
                        )
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
                        .unwrap();
                    Ok::<_, PyErr>(())
                })
                .await;
            }
        });
        Ok(())
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

    fn stop(&self, instance: &PyInstance, reason: String) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        let instance = monarch_with_gil_blocking(|_py| instance.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push(async move {
            let result =
                async move { mesh.await?.stop(&instance, reason)?.take_task()?.await }.await;
            if tx.send(result).is_err() {
                panic!("oneshot failed");
            }
        });
        PyPythonTask::new(async move { rx.await.map_err(anyhow::Error::from)? })
    }

    fn initialized<'py>(&self) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        PyPythonTask::new(async {
            mesh.await?;
            Ok(None::<()>)
        })
    }
}

#[async_trait]
impl Supervisable for AsyncActorMesh {
    async fn supervision_event(&self, instance: &Instance<PythonActor>) -> Option<PyErr> {
        if !self.supervised {
            return None;
        }
        let mesh = self.mesh.clone();
        match mesh.await {
            Ok(mesh) => mesh.supervision_event(instance).await,
            Err(e) => Some(e.into()),
        }
    }
}

impl SupervisableActorMesh for AsyncActorMesh {
    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn SupervisableActorMesh>> {
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

    fn mesh_ref(&self) -> &ActorMeshRef<PythonActor> {
        match self {
            PythonActorMeshImpl::Owned(inner) => &inner.mesh,
            PythonActorMeshImpl::Ref(inner) => &inner.mesh,
        }
    }
}

#[async_trait]
impl Supervisable for PythonActorMeshImpl {
    async fn supervision_event(&self, instance: &Instance<PythonActor>) -> Option<PyErr> {
        let mesh = self.mesh_ref();
        match mesh.next_supervision_event(instance).await {
            Ok(supervision_failure) => Some(SupervisionError::new_err_from(supervision_failure)),
            Err(e) => Some(PyValueError::new_err(e.to_string())),
        }
    }
}

impl ActorMeshProtocol for PythonActorMeshImpl {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        <ActorMeshRef<PythonActor> as ActorMeshProtocol>::cast(
            self.mesh_ref(),
            message,
            selection,
            instance,
        )
    }

    fn stop(&self, instance: &PyInstance, reason: String) -> PyResult<PyPythonTask> {
        let (slf, instance) = monarch_with_gil_blocking(|_py| (self.clone(), instance.clone()));
        match slf {
            PythonActorMeshImpl::Owned(mut mesh) => PyPythonTask::new(async move {
                mesh.mesh
                    .stop(instance.deref(), reason)
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

impl SupervisableActorMesh for PythonActorMeshImpl {
    fn new_with_region(&self, region: &PyRegion) -> PyResult<Box<dyn SupervisableActorMesh>> {
        assert!(region.as_inner().is_subset(self.mesh_ref().region()));
        Ok(Box::new(PythonActorMeshImpl::new_ref(
            self.mesh_ref().sliced(region.as_inner().clone()),
        )))
    }
}

// Convert a hyperactor_mesh::Error to a Python exception. hyperactor_mesh::Error::Supervision becomes a SupervisionError,
// all others become a RuntimeError.
fn cast_error_to_py_error(err: hyperactor_mesh::Error) -> PyErr {
    if let hyperactor_mesh::Error::Supervision(failure) = err {
        SupervisionError::new_err_from(*failure)
    } else {
        PyRuntimeError::new_err(err.to_string())
    }
}

impl ActorMeshProtocol for ActorMeshRef<PythonActor> {
    fn cast(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        if structurally_equal(&selection, &Selection::All(Box::new(Selection::True))) {
            self.cast(instance, message.clone())
                .map_err(cast_error_to_py_error)?;
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
                .cast(instance, message.clone())
                .map_err(cast_error_to_py_error)?;
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "invalid selection: {:?}",
                selection
            )));
        }

        Ok(())
    }

    /// Stop the actor mesh asynchronously.
    fn stop(&self, _instance: &PyInstance, _reason: String) -> PyResult<PyPythonTask> {
        Err(PyNotImplementedError::new_err(
            "This cannot be used on ActorMeshRef, only on owned ActorMesh",
        ))
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
/// acquires the GIL using Rust's Python::attach and holds it for the
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
        Python::attach(|_py| {
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

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;
    use std::time::Duration;

    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::Instance;
    use hyperactor::Proc;
    use hyperactor::actor::Signal;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::mailbox;
    use hyperactor::mailbox::PortReceiver;
    use hyperactor::proc::WorkCell;
    use hyperactor::supervision::ActorSupervisionEvent;
    use hyperactor_mesh::ProcMesh;
    use hyperactor_mesh::alloc::AllocSpec;
    use hyperactor_mesh::alloc::Allocator;
    use hyperactor_mesh::alloc::LocalAllocator;
    use hyperactor_mesh::mesh_controller::GetSubscriberCount;
    use hyperactor_mesh::supervision::MeshFailure;
    use monarch_types::PickledPyObject;
    use ndslice::extent;
    use pyo3::Python;
    use tokio::sync::mpsc;

    use super::*;
    use crate::actor::PythonActor;
    use crate::actor::PythonActorParams;

    /// Minimal root-client actor for test infrastructure.
    /// Handles MeshFailure by panicking (test failure).
    #[derive(Debug)]
    struct TestClient {
        signal_rx: PortReceiver<Signal>,
        supervision_rx: PortReceiver<ActorSupervisionEvent>,
        work_rx: mpsc::UnboundedReceiver<WorkCell<Self>>,
    }

    impl Actor for TestClient {}

    #[async_trait]
    impl Handler<MeshFailure> for TestClient {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
            msg: MeshFailure,
        ) -> Result<(), anyhow::Error> {
            panic!("unexpected supervision failure in test: {}", msg);
        }
    }

    impl TestClient {
        fn run(mut self, instance: &'static Instance<Self>) {
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        work = self.work_rx.recv() => {
                            match work {
                                Some(work) => {
                                    let _ = work.handle(&mut self, instance).await;
                                }
                                None => break,
                            }
                        }
                        _ = self.signal_rx.recv() => {}
                        Ok(event) = self.supervision_rx.recv() => {
                            let _ = instance
                                .handle_supervision_event(&mut self, event)
                                .await;
                        }
                    }
                }
            });
        }
    }

    fn init_test_instance() -> &'static Instance<TestClient> {
        static INSTANCE: OnceLock<Instance<TestClient>> = OnceLock::new();
        let proc = Proc::direct(ChannelTransport::Unix.any(), "test_proc".to_string()).unwrap();
        let (actor, _handle, supervision_rx, signal_rx, work_rx) =
            proc.actor_instance("test_client").unwrap();

        INSTANCE
            .set(actor)
            .map_err(|_| "already initialized")
            .unwrap();
        let instance = INSTANCE.get().unwrap();

        TestClient {
            signal_rx,
            supervision_rx,
            work_rx,
        }
        .run(instance);

        instance
    }

    fn test_instance() -> &'static Instance<TestClient> {
        static INSTANCE: OnceLock<&'static Instance<TestClient>> = OnceLock::new();
        INSTANCE.get_or_init(init_test_instance)
    }

    /// Verify that calling `supervision_event` repeatedly through a
    /// [`PythonActorMesh`] does not increase the subscriber count on the
    /// controller.  This guards against a regression where each call
    /// would create a new supervision subscriber.
    #[tokio::test]
    async fn test_subscriber_count_stable_across_supervision_calls() {
        crate::pytokio::ensure_python();

        let instance = test_instance();

        let proc_mesh = ProcMesh::allocate(
            instance,
            Box::new(
                LocalAllocator
                    .allocate(AllocSpec {
                        extent: extent!(replicas = 2),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: ChannelTransport::Local,
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap(),
            ),
            "test",
        )
        .await
        .unwrap();

        // Create a minimal Python class and pickle it so we can spawn
        // PythonActor instances (mirroring PyProcMesh::spawn_async).
        // The class must live in __main__'s globals for pickle to find it.
        let pickled_type = Python::attach(|py| {
            py.run(c"class MinimalActor: pass", None, None).unwrap();

            PickledPyObject::pickle(
                &py.import("__main__")
                    .unwrap()
                    .getattr("MinimalActor")
                    .unwrap(),
            )
            .unwrap()
        });

        let actor_mesh = proc_mesh
            .spawn::<PythonActor, _>(
                instance,
                "test_actors",
                &PythonActorParams::new(pickled_type, None),
            )
            .await
            .unwrap();

        let controller = actor_mesh.controller().as_ref().unwrap().clone();

        // Wrap using the production code path from PyProcMesh::spawn_async.
        let mesh_impl =
            async move { Ok::<_, PyErr>(Box::new(PythonActorMeshImpl::new_owned(actor_mesh))) };
        let python_actor_mesh = PythonActorMesh::new(
            async move {
                let mesh_impl: Box<dyn SupervisableActorMesh> = mesh_impl.await?;
                Ok(mesh_impl)
            },
            true,
        );

        // Instance<PythonActor> required by the Supervisable trait
        // signature. Only used for subscription routing inside
        // next_supervision_event.
        let (py_instance, ..) = Proc::direct(ChannelTransport::Unix.any(), "py_proc".to_string())
            .unwrap()
            .actor_instance::<PythonActor>("py_client")
            .unwrap();

        // Query the subscriber count from the controller.
        let (port, mut rx) = mailbox::open_port::<usize>(instance);
        controller
            .send(instance, GetSubscriberCount(port.bind()))
            .unwrap();
        let initial_count = RealClock
            .timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for subscriber count")
            .expect("channel closed");
        assert_eq!(initial_count, 0, "should have 0 subscribers initially");

        // Call supervision_event through the PythonActorMesh multiple
        // times, racing against a short timeout each time.  The mesh is
        // healthy so no event fires; we just want to trigger the lazy
        // subscriber initialization repeatedly.
        for _ in 0..5 {
            tokio::select! {
                _ = python_actor_mesh.inner.supervision_event(&py_instance) => {
                    panic!("unexpected supervision event on healthy mesh");
                }
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }

        // After 5 calls from the same context, there should be exactly 1
        // subscriber (created lazily on the first call, reused thereafter).
        let (port, mut rx) = mailbox::open_port::<usize>(instance);
        controller
            .send(instance, GetSubscriberCount(port.bind()))
            .unwrap();
        let after_count = RealClock
            .timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for subscriber count")
            .expect("channel closed");
        assert_eq!(
            after_count, 1,
            "subscriber count should be exactly 1, not growing with each call"
        );

        // Do 5 more calls to confirm it stays stable.
        for _ in 0..5 {
            tokio::select! {
                _ = python_actor_mesh.inner.supervision_event(&py_instance) => {
                    panic!("unexpected supervision event on healthy mesh");
                }
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }

        let (port, mut rx) = mailbox::open_port::<usize>(instance);
        controller
            .send(instance, GetSubscriberCount(port.bind()))
            .unwrap();
        let final_count = RealClock
            .timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for subscriber count")
            .expect("channel closed");
        assert_eq!(
            final_count, 1,
            "subscriber count should still be 1 after repeated calls"
        );
    }
}
