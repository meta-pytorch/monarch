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
use hyperactor::Instance;
use hyperactor::RemoteEndpoint as _;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::actor_mesh::ActorMeshRef;
use monarch_types::py_global;
use monarch_types::py_module_add_function;
use ndslice::view::Ranked;
use ndslice::view::RankedSliceable;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyTuple;
use rand::RngExt as _;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tracing::Instrument;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::context::PyInstance;
use crate::pickle::PendingMessage;
use crate::proc::PyActorAddr;
use crate::pytokio::PyPythonTask;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::shape::PyRegion;
use crate::supervision::Supervisable;
use crate::supervision::SupervisionError;

py_global!(_pickle, "monarch._src.actor.actor_mesh", "_pickle");

py_global!(
    shared_class,
    "monarch._rust_bindings.monarch_hyperactor.pytokio",
    "Shared"
);

/// Closed actor-mesh selection surface used by public send APIs.
///
/// Public Python APIs expose only `"all"` and `"choose"`. Keep that
/// invariant explicit here so arbitrary `ndslice::Selection` values stay out
/// of the public mesh-casting surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllOrChoose {
    All,
    Choose,
}

impl AllOrChoose {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Choose => "choose",
        }
    }
}

/// Trait defining the common interface for actor mesh, mesh ref and actor mesh implementations.
/// This corresponds to the Python ActorMeshProtocol ABC.
pub(crate) trait ActorMeshProtocol: Send + Sync {
    /// Cast a message to actors selected by the given selection using the specified mailbox.
    fn cast(
        &self,
        message: PythonMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()>;

    /// Cast a message, merging caller-supplied envelope headers into
    /// the outbound request. Implementations that reach the real
    /// envelope emission site override this to thread `caller_headers`
    /// through `hyperactor_mesh::ActorMeshRef::cast_with_headers`;
    /// the default collapses to the non-headers path for impls that
    /// have no envelope access.
    fn cast_with_headers(
        &self,
        message: PythonMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        _caller_headers: hyperactor_config::Flattrs,
    ) -> PyResult<()> {
        self.cast(message, selection, instance)
    }

    /// Cast a pending message (which may contain unresolved async values) to actors.
    ///
    /// The default implementation blocks on resolving the message and then calls cast.
    /// AsyncActorMesh overrides this with an optimized async implementation.
    fn cast_unresolved(
        &self,
        message: PendingMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        let message = get_tokio_runtime().block_on(message.resolve())?;
        self.cast(message, selection, instance)
    }

    /// Async counterpart of `cast_with_headers`. The default
    /// resolves the pending message synchronously and delegates;
    /// `AsyncActorMesh` overrides this to resolve asynchronously
    /// and route through `cast_with_headers`.
    fn cast_unresolved_with_headers(
        &self,
        message: PendingMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        caller_headers: hyperactor_config::Flattrs,
    ) -> PyResult<()> {
        let message = get_tokio_runtime().block_on(message.resolve())?;
        self.cast_with_headers(message, selection, instance, caller_headers)
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)>;

    /// The serializable reference for this mesh, for the out-of-band `refs`
    /// table. Required so every impl chooses: hold a resolved ref and return
    /// it, or (a pending mesh) have none and error explicitly.
    fn mesh_ref(&self) -> PyResult<ActorMeshRef<PythonActor>>;

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

    /// The name of the mesh.
    fn name(&self) -> PyResult<PyPythonTask>;
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

pub(crate) fn to_all_or_choose(selection: &str) -> PyResult<AllOrChoose> {
    match selection {
        "choose" => Ok(AllOrChoose::Choose),
        "all" => Ok(AllOrChoose::All),
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
        let sel = to_all_or_choose(selection)?;
        self.inner.cast(message.clone(), sel, instance.deref())
    }

    #[hyperactor::instrument]
    pub(crate) fn cast_unresolved(
        &self,
        message: &mut PendingMessage,
        selection: &str,
        instance: &PyInstance,
    ) -> PyResult<()> {
        let sel = to_all_or_choose(selection)?;
        let message = message.take()?;
        self.inner.cast_unresolved(message, sel, instance)
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

    fn name(&self) -> PyResult<PyPythonTask> {
        self.inner.name()
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
        monarch_with_gil_blocking(GilSite::Convert, |py| self.inner.clone_ref(py).into())
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
        _message: PythonMessage,
        _selection: AllOrChoose,
        _instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        panic!("not implemented")
    }

    fn cast_unresolved(
        &self,
        message: PendingMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        self.cast_unresolved_with_headers(
            message,
            selection,
            instance,
            hyperactor_config::Flattrs::new(),
        )
    }

    fn cast_unresolved_with_headers(
        &self,
        message: PendingMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        caller_headers: hyperactor_config::Flattrs,
    ) -> PyResult<()> {
        let mesh = self.mesh.clone();
        let instance = instance.clone_for_py();
        let port = match &message.kind {
            PythonMessageKind::CallMethod { response_port, .. } => response_port.clone(),
            _ => None,
        };
        self.push(
            async move {
                let result = async {
                    let resolved = match message.try_resolve_now()? {
                        Ok(resolved) => resolved,
                        Err(message) => message.resolve().await?,
                    };
                    mesh.await?
                        .cast_with_headers(resolved, selection, &instance, caller_headers)
                }
                .await;
                if let (Some(mut port_ref), Err(pyerr)) = (port, result) {
                    let _ = monarch_with_gil(GilSite::Traceback, |py: Python<'_>| {
                        let exception_str = crate::logging::format_traceback(py, &pyerr);
                        tracing::error!(
                            actor_id = instance.self_addr().to_string(),
                            "error occurred during cast unresolved: {}",
                            exception_str
                        );

                        // Endpoint calls create a response port: the
                        // PortRef is sent to the remote worker (to send
                        // results back), and collect_valuemesh owns the
                        // PortReceiver. If mesh.cast() fails here, we try
                        // to send the exception back to the caller via
                        // the PortRef ourselves. But a supervision event
                        // can cause collect_valuemesh to drop the
                        // PortReceiver (removing the port from the
                        // mailbox) before we get here. Disable
                        // return-undeliverable so a delivery failure
                        // doesn't bounce back and crash the root client.
                        //
                        // TODO: Tie the lifetime of this queued work to
                        // the PortReceiver (e.g. a cancellation token set
                        // on drop) so we can distinguish
                        // supervision-caused failures — where the caller
                        // already knows — from other cast errors where
                        // the caller actually needs this exception.

                        port_ref.set_return_undeliverable(false);

                        let mut state = crate::pickle::pickle(
                            py,
                            pyerr.into_value(py).into_any(),
                            false,
                            false,
                        )?;
                        let _ = port_ref.post(
                            &instance,
                            PythonMessage::new_from_buf(
                                PythonMessageKind::Exception { rank: Some(0) },
                                state.take_inner()?.take_buffer(),
                            ),
                        );

                        Ok::<_, PyErr>(())
                    })
                    .await;
                }
            }
            .instrument(tracing::debug_span!("AsyncActorMesh::cast")),
        );
        Ok(())
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let fut = self.mesh.clone();
        match fut.peek().cloned() {
            Some(mesh) => mesh?.__reduce__(py),
            None => {
                let observer = async move { Ok(PythonActorMesh::from_impl(fut.await?)) };
                // Test-only: consumes a caller-thread probe, if one is armed, and
                // moves it into the spawned observer. Adds no field, branch,
                // callback or synchronization point to the production build.
                #[cfg(test)]
                let observer = tests::probe_reduction_observer(observer);
                let shared = PyPythonTask::new(observer)?.spawn_abortable()?;
                let shared = Py::new(py, shared)?;
                if crate::pickle::reserve_mesh_reference_if_active(shared.clone_ref(py)) {
                    let pop_fn = py
                        .import("monarch._rust_bindings.monarch_hyperactor.pickle")?
                        .getattr("pop_mesh_reference")?;
                    return Ok((pop_fn, PyTuple::empty(py).into_any()));
                }
                // Get Shared.block_on as an unbound method
                let block_on = shared_class(py).getattr("block_on")?;
                let args = PyTuple::new(py, [shared])?;
                Ok((block_on, args.into_any()))
            }
        }
    }

    fn mesh_ref(&self) -> PyResult<ActorMeshRef<PythonActor>> {
        // A pending mesh has no serializable ref of its own; the reserve/fill
        // slot carries it out-of-band. This is a backstop: the happy path never
        // asks a pending mesh for a ref.
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "pending actor mesh has no serializable ref; it is carried via the reserve/fill slot",
        ))
    }

    fn stop(&self, instance: &PyInstance, reason: String) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        let instance = monarch_with_gil_blocking(GilSite::Stop, |_py| instance.clone());
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

    fn name(&self) -> PyResult<PyPythonTask> {
        let mesh = self.mesh.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push(async move {
            let result = async move { mesh.await?.name()?.take_task()?.await }.await;
            if tx.send(result).is_err() {
                panic!("oneshot failed");
            }
        });
        PyPythonTask::new(async move { rx.await.map_err(anyhow::Error::from)? })
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
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        <ActorMeshRef<PythonActor> as ActorMeshProtocol>::cast(
            self.mesh_ref(),
            message,
            selection,
            instance,
        )
    }

    fn cast_with_headers(
        &self,
        message: PythonMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        caller_headers: hyperactor_config::Flattrs,
    ) -> PyResult<()> {
        <ActorMeshRef<PythonActor> as ActorMeshProtocol>::cast_with_headers(
            self.mesh_ref(),
            message,
            selection,
            instance,
            caller_headers,
        )
    }

    fn stop(&self, instance: &PyInstance, reason: String) -> PyResult<PyPythonTask> {
        let (slf, instance) =
            monarch_with_gil_blocking(GilSite::Stop, |_py| (self.clone(), instance.clone()));
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

    fn mesh_ref(&self) -> PyResult<ActorMeshRef<PythonActor>> {
        // `self.mesh_ref()` resolves to the inherent borrow-returning method.
        Ok(PythonActorMeshImpl::mesh_ref(self).clone())
    }

    fn name(&self) -> PyResult<PyPythonTask> {
        let name = self
            .mesh_ref()
            .as_managed()
            .expect("Python actor mesh names require a managed mesh")
            .id()
            .to_string();
        PyPythonTask::new(async move { Ok(name) })
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

fn cast_error_to_py_error(err: hyperactor_mesh::Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

impl ActorMeshProtocol for ActorMeshRef<PythonActor> {
    fn cast(
        &self,
        message: PythonMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        <Self as ActorMeshProtocol>::cast_with_headers(
            self,
            message,
            selection,
            instance,
            hyperactor_config::Flattrs::new(),
        )
    }

    fn cast_with_headers(
        &self,
        message: PythonMessage,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        caller_headers: hyperactor_config::Flattrs,
    ) -> PyResult<()> {
        match selection {
            AllOrChoose::All => ActorMeshRef::<PythonActor>::cast_with_headers(
                self,
                instance,
                &caller_headers,
                message,
            )
            .map_err(cast_error_to_py_error),
            AllOrChoose::Choose => match self {
                ActorMeshRef::Managed(mesh) => mesh
                    .cast_choose_with_headers(instance, &caller_headers, message)
                    .map_err(cast_error_to_py_error),
                ActorMeshRef::Data(mesh) => {
                    let num_ranks = mesh.region().num_ranks();
                    if num_ranks > 0 {
                        let actor = mesh
                            .get(rand::rng().random_range(0..num_ranks))
                            .expect("selected rank should exist in dense data mesh");
                        actor.post_with_headers(instance, caller_headers, message);
                    }
                    Ok(())
                }
            },
        }
    }

    /// Stop the actor mesh asynchronously.
    fn stop(&self, _instance: &PyInstance, _reason: String) -> PyResult<PyPythonTask> {
        Err(PyNotImplementedError::new_err(
            "This cannot be used on ActorMeshRef, only on owned ActorMesh",
        ))
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        if crate::pickle::push_mesh_reference_if_active(crate::actor::MeshRef::Actor(Box::new(
            self.clone(),
        ))) {
            let pop_fn = py
                .import("monarch._rust_bindings.monarch_hyperactor.pickle")?
                .getattr("pop_mesh_reference")?;
            return Ok((pop_fn, pyo3::types::PyTuple::empty(py).into_any()));
        }
        let bytes = bincode::serde::encode_to_vec(self, bincode::config::legacy())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let py_bytes = (PyBytes::new(py, &bytes),).into_bound_py_any(py).unwrap();
        let module = py
            .import("monarch._rust_bindings.monarch_hyperactor.actor_mesh")
            .unwrap();
        let from_bytes = module.getattr("py_actor_mesh_from_bytes").unwrap();
        Ok((from_bytes, py_bytes))
    }

    fn mesh_ref(&self) -> PyResult<ActorMeshRef<PythonActor>> {
        Ok(self.clone())
    }

    fn name(&self) -> PyResult<PyPythonTask> {
        let name = self
            .as_managed()
            .expect("Python actor mesh names require a managed mesh")
            .id()
            .to_string();
        PyPythonTask::new(async move { Ok(name) })
    }
}

#[pymethods]
impl PythonActorMeshImpl {
    fn get(&self, rank: usize) -> PyResult<Option<PyActorAddr>> {
        Ok(self
            .mesh_ref()
            .get(rank)
            .map(|r| hyperactor::ActorRef::into_actor_addr(r.clone()))
            .map(PyActorAddr::from))
    }

    fn __repr__(&self) -> String {
        format!("PythonActorMeshImpl({:?})", self.mesh_ref())
    }
}

#[pyfunction]
fn py_actor_mesh_from_bytes(bytes: &Bound<'_, PyBytes>) -> PyResult<PythonActorMesh> {
    let r: PyResult<ActorMeshRef<PythonActor>> =
        bincode::serde::decode_from_slice(bytes.as_bytes(), bincode::config::legacy())
            .map(|(v, _)| v)
            .map_err(|e| PyValueError::new_err(e.to_string()));
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
    pub(crate) fn actor_id(&self) -> PyResult<PyActorAddr> {
        Ok(PyActorAddr::from(self.inner.actor_id.clone()))
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
pub fn hold_gil_for_test(delay_secs: f64, hold_secs: f64) {
    thread::spawn(move || {
        // Wait before grabbing the GIL (blocking sleep is fine here, we're in a spawned thread)
        thread::sleep(Duration::from_secs_f64(delay_secs));
        // Acquire and hold the GIL - MUST use blocking sleep to keep GIL held
        monarch_with_gil_blocking(GilSite::Test, |_py| {
            tracing::info!("start holding the gil...");
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
    use std::any::Any;
    use std::cell::RefCell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc::Receiver;
    use std::sync::mpsc::SyncSender;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use hyperactor::ActorRef;
    use hyperactor::Proc;
    use hyperactor::ProcAddr;
    use hyperactor::ProcId;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::id::Label;
    use hyperactor::id::Uid;
    use hyperactor_mesh::host_mesh::HostMeshRef;
    use hyperactor_mesh::mesh_id::ActorMeshId;
    use hyperactor_mesh::mesh_id::HostMeshId;
    use ndslice::extent;
    use pyo3::PyTypeInfo;
    use tokio::sync::watch;

    use super::*;

    /// The text both name wrappers assert on when handed a data mesh.
    const MANAGED_ONLY_NAME: &str = "Python actor mesh names require a managed mesh";

    /// A mesh id no implementation could return by accident: `instance` draws a
    /// fresh uid, so a hard-coded name cannot match it.
    fn fixture_mesh_id() -> ActorMeshId {
        ActorMeshId::instance(Label::new("mesh_name_fixture").expect("test label should be valid"))
    }

    /// A managed `ActorMeshRef<PythonActor>` carrying `id`, obtained through the
    /// public deserializer.
    ///
    /// `ActorMeshRef::new_managed` is not public outside `hyperactor_mesh`.
    /// Starting a real mesh merely to obtain this value would introduce runtime
    /// resources, so this fixture instead uses the public serde representation.
    /// `HostMeshRef::from_hosts` is a synchronous constructor whose embedded
    /// host-agent mesh is managed with no controller. The actor type occurs in
    /// the managed wire form only in that absent controller field, which makes
    /// the serialized value safe to decode as `ActorMeshRef<PythonActor>`. No
    /// host is contacted and the local address is never connected.
    ///
    /// `ActorMeshRefRepr::Managed` is externally tagged and serializes as the
    /// four-element tuple `(id, proc_mesh_id, controller, cast_domain)`. The
    /// `HostMeshRef` constructor supplies a valid value for every element, but
    /// always uses the fixed `host_agent` id. The fixture replaces tuple element
    /// zero only; otherwise a name implementation incorrectly hard-coded to
    /// `host_agent` would satisfy the test.
    ///
    /// The field lookup, variant lookup, and arity assertion below deliberately
    /// make this dependency on the wire format explicit. A field, tag, or tuple
    /// layout change fails before mutation, and the post-decode assertion proves
    /// that the public deserializer retained the caller-selected id.
    fn managed_mesh_ref(id: &ActorMeshId) -> ActorMeshRef<PythonActor> {
        let host_mesh = HostMeshRef::from_hosts(
            HostMeshId::singleton(Label::strip("managed_name_fixture")),
            vec![ChannelAddr::Local(1)],
        );
        let mut wire = serde_json::to_value(&host_mesh).expect("HostMeshRef should serialize");
        let payload = wire
            .get_mut("host_agent_mesh")
            .and_then(|mesh| mesh.get_mut("Managed"))
            .and_then(serde_json::Value::as_array_mut)
            .expect("the host agent mesh should encode as the managed variant");
        assert_eq!(
            payload.len(),
            4,
            "the managed payload should be the (id, proc mesh, controller, cast domain) tuple"
        );
        payload[0] = serde_json::to_value(id).expect("ActorMeshId should serialize");

        let mesh_ref: ActorMeshRef<PythonActor> =
            serde_json::from_value(wire["host_agent_mesh"].clone())
                .expect("the host agent mesh should decode as an ActorMeshRef");
        assert_eq!(
            mesh_ref
                .as_managed()
                .expect("the fixture must decode as the managed variant")
                .id(),
            id,
            "the fixture must carry the caller-selected id, not the constructor default"
        );
        mesh_ref
    }

    /// A data `ActorMeshRef<PythonActor>`: the variant both name wrappers
    /// reject. Its single member is never messaged.
    fn data_mesh_ref() -> ActorMeshRef<PythonActor> {
        let proc_addr = ProcAddr::new(
            ProcId::new(
                Uid::Instance(1, None),
                Some(Label::new("local").expect("test label should be valid")),
            ),
            ChannelAddr::Local(1).into(),
        );
        let member: ActorRef<PythonActor> = ActorRef::attest(proc_addr.actor_addr("member"));
        ActorMeshRef::try_new_data(extent!(members = 1).into(), vec![member])
            .expect("a one-rank data mesh should be valid")
    }

    /// Drive a wrapper task to completion and return the string it yields.
    fn drive_name(task: PyResult<PyPythonTask>) -> String {
        let mut task = task.expect("name() should return a task");
        let value = get_tokio_runtime()
            .block_on(task.take_task().expect("a fresh task is not consumed"))
            .expect("the name task should resolve");
        monarch_with_gil_blocking(GilSite::Test, |py| {
            value
                .extract::<String>(py)
                .expect("name() should resolve to a string")
        })
    }

    fn panic_message(payload: &(dyn Any + Send)) -> String {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_string())
            })
            .unwrap_or_else(|| "<non-string panic payload>".to_string())
    }

    // Both managed name wrappers yield the mesh id, and discarding a wrapper
    // consumes nothing: a later wrapper reports the same id.
    //
    // What these assertions establish and what they do not. They prove the
    // value is repeatable across a discarded observation. They do NOT prove
    // dynamically that the id is read at call time rather than inside the
    // returned future; an implementation that cloned the mesh and read the id
    // when driven would pass too. Call-time computation is source-grounded --
    // both bodies compute the string and then move it into
    // `PyPythonTask::new(async move { Ok(name) })` -- and its synchronous half
    // is witnessed dynamically by the data-mesh panic below, which happens
    // before any task exists. Nor is the returned task first-poll ready:
    // `PyPythonTask::new` converts its result through `monarch_with_gil`, which
    // may wait on the process-global GIL lock, so observation is not free.
    #[test]
    fn discarded_managed_name_wrappers_leave_exact_name_repeatable() {
        pyo3::Python::initialize();
        let id = fixture_mesh_id();
        let mesh_ref = managed_mesh_ref(&id);
        let mesh_impl = PythonActorMeshImpl::new_ref(mesh_ref.clone());
        let expected = id.to_string();

        drop(
            <ActorMeshRef<PythonActor> as ActorMeshProtocol>::name(&mesh_ref)
                .expect("the ref wrapper should return a task"),
        );
        drop(
            <PythonActorMeshImpl as ActorMeshProtocol>::name(&mesh_impl)
                .expect("the impl wrapper should return a task"),
        );

        assert_eq!(
            drive_name(<ActorMeshRef<PythonActor> as ActorMeshProtocol>::name(
                &mesh_ref
            )),
            expected,
            "discarding a ref-wrapper task must not disturb the next one"
        );
        assert_eq!(
            drive_name(<PythonActorMeshImpl as ActorMeshProtocol>::name(&mesh_impl)),
            expected,
            "discarding an impl-wrapper task must not disturb the next one"
        );
    }

    // KNOWN-BAD CURRENT BEHAVIOR, recorded so a later conversion decides
    // deliberately rather than by accident: a data mesh has no name, and both
    // wrappers assert instead of returning a task that fails. The panic happens
    // in the call itself, so no task ever reaches the caller.
    #[test]
    fn data_name_wrappers_panic_before_returning_a_task() {
        pyo3::Python::initialize();
        let mesh_ref = data_mesh_ref();
        let mesh_impl = PythonActorMeshImpl::new_ref(mesh_ref.clone());

        let from_ref = catch_unwind(AssertUnwindSafe(|| {
            <ActorMeshRef<PythonActor> as ActorMeshProtocol>::name(&mesh_ref).map(|_| ())
        }));
        let from_impl = catch_unwind(AssertUnwindSafe(|| {
            <PythonActorMeshImpl as ActorMeshProtocol>::name(&mesh_impl).map(|_| ())
        }));

        for (outcome, which) in [(from_ref, "ref"), (from_impl, "impl")] {
            let payload = outcome
                .err()
                .unwrap_or_else(|| panic!("the {which} wrapper must panic on a data mesh"));
            let message = panic_message(payload.as_ref());
            assert!(
                message.contains(MANAGED_ONLY_NAME),
                "the {which} wrapper must fail the managed-mesh assertion, got: {message}"
            );
        }
    }

    // The trait-default `initialized` wrapper resolves to None and leaves the
    // mesh usable, on both implementors that inherit it.
    //
    // What this establishes and what it does not. The absence of domain work is
    // a property of the default's source body, `async { Ok(None::<()>) }`, not
    // something these assertions measure: driving through `block_on` would also
    // pass if the future yielded or did unrelated non-mutating work first, and
    // `PyPythonTask::new` converts its result through `monarch_with_gil`, which
    // may wait on the process-global GIL lock. So this is not a first-poll-ready
    // claim. Re-reading the name afterwards shows the mesh is still usable; it
    // is not a work counter.
    //
    // No production caller reaches this default today. Normal `PythonActorMesh`
    // construction and reconstruction use `AsyncActorMesh`, which overrides it.
    //
    // One alternate route exists. When the mesh is still pending, and only on
    // the non-reserved branch, `AsyncActorMesh::__reduce__` returns
    // `(Shared.block_on, (shared,))` over an observer that builds a wrapper
    // directly around the resolved implementation. Ordinary pickle reduces that
    // value again into an async-wrapped reference, but a caller that invokes
    // `Shared.block_on` itself can retain the bare wrapper and make
    // `PythonActorMesh::initialized` dispatch here. Under active Monarch
    // pickling the callable is `pop_mesh_reference` instead, which reconstructs
    // async-wrapped, so that branch cannot expose the wrapper at all.
    //
    // The route is source-grounded rather than exercised below, and it does not
    // depend on which arm resolved: neither `PythonActorMeshImpl` nor
    // `ActorMeshRef` overrides `initialized`. Both inheriting implementors are
    // exercised directly because no production caller was found.
    #[test]
    fn default_initialized_wrappers_return_none_and_leave_mesh_usable() {
        pyo3::Python::initialize();
        let id = fixture_mesh_id();
        let mesh_ref = managed_mesh_ref(&id);
        let mesh_impl = PythonActorMeshImpl::new_ref(mesh_ref.clone());

        drop(
            <ActorMeshRef<PythonActor> as ActorMeshProtocol>::initialized(&mesh_ref)
                .expect("the ref default should return a task"),
        );
        drop(
            <PythonActorMeshImpl as ActorMeshProtocol>::initialized(&mesh_impl)
                .expect("the impl default should return a task"),
        );

        for (task, which) in [
            (
                <ActorMeshRef<PythonActor> as ActorMeshProtocol>::initialized(&mesh_ref),
                "ref",
            ),
            (
                <PythonActorMeshImpl as ActorMeshProtocol>::initialized(&mesh_impl),
                "impl",
            ),
        ] {
            let mut task = task.expect("initialized() should return a task");
            let value = get_tokio_runtime()
                .block_on(task.take_task().expect("a fresh task is not consumed"))
                .expect("the default initialized task should resolve");
            assert!(
                monarch_with_gil_blocking(GilSite::Test, |py| value.is_none(py)),
                "the default {which} initialized wrapper must resolve to None"
            );
        }

        assert_eq!(
            drive_name(<PythonActorMeshImpl as ActorMeshProtocol>::name(&mesh_impl)),
            id.to_string(),
            "observing the default initialized wrapper must leave the mesh usable"
        );
    }

    // Controlled actor-mesh fixture

    /// What `ControlledActorMesh::__reduce__` reconstructs to. `str` is a real
    /// builtin, so the reduce tuple survives a genuine `pickle.dumps`/`loads`
    /// round trip; `py_identity` could not, since its declared module is not
    /// installed in a Rust unit binary.
    const CONTROLLED_MARKER: &str = "controlled-actor-mesh-reconstructed";

    /// Signals for one reduction observer: the future built inside
    /// `AsyncActorMesh::__reduce__`.
    struct ReductionProbe {
        entered: SyncSender<()>,
        dropped: SyncSender<()>,
    }

    thread_local! {
        /// Armed by a test immediately before it calls `__reduce__`, and
        /// consumed there. Caller-thread scoped: a process-global slot would be
        /// unusable in a binary that runs tests in parallel.
        static REDUCTION_PROBE: RefCell<Option<ReductionProbe>> = const { RefCell::new(None) };
    }

    /// Restores the previous probe when dropped, so a run that takes the ready
    /// branch -- or fails before `__reduce__` -- cannot leak an armed probe into
    /// the next test on this thread. Serial test mode puts every test on the
    /// main thread, so the leak is reachable without the guard.
    struct ArmedProbe {
        previous: Option<ReductionProbe>,
    }

    impl Drop for ArmedProbe {
        fn drop(&mut self) {
            let previous = self.previous.take();
            REDUCTION_PROBE.with(|slot| *slot.borrow_mut() = previous);
        }
    }

    #[must_use]
    fn arm_reduction_probe(probe: ReductionProbe) -> ArmedProbe {
        let previous = REDUCTION_PROBE.with(|slot| slot.borrow_mut().replace(probe));
        ArmedProbe { previous }
    }

    /// Wraps the reduction observer, reporting its first poll and its
    /// destruction. With no probe armed it is a transparent pass-through.
    pub(super) struct ProbedObserver {
        inner: Option<Pin<Box<dyn Future<Output = PyResult<PythonActorMesh>> + Send>>>,
        probe: Option<ReductionProbe>,
        entered: bool,
        completed: bool,
    }

    pub(super) fn probe_reduction_observer<F>(inner: F) -> ProbedObserver
    where
        F: Future<Output = PyResult<PythonActorMesh>> + Send + 'static,
    {
        ProbedObserver {
            inner: Some(Box::pin(inner)),
            probe: REDUCTION_PROBE.with(|slot| slot.borrow_mut().take()),
            entered: false,
            completed: false,
        }
    }

    impl Future for ProbedObserver {
        type Output = PyResult<PythonActorMesh>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let polled = match this.inner.as_mut() {
                Some(inner) => inner.as_mut().poll(cx),
                None => Poll::Pending,
            };
            if polled.is_ready() {
                this.completed = true;
            }
            // Signalled after the inner poll returns, so the entry signal
            // witnesses an actual poll of the observer rather than mere entry
            // into this wrapper.
            if !this.entered {
                this.entered = true;
                if let Some(probe) = &this.probe {
                    let _ = probe.entered.try_send(());
                }
            }
            polled
        }
    }

    impl Drop for ProbedObserver {
        fn drop(&mut self) {
            // Destroy the wrapped future -- and with it the `Shared` clone it
            // captured -- before acknowledging. `Drop::drop` runs ahead of field
            // destruction, so signalling here without the explicit take would
            // leave open the cancellation race this probe exists to close.
            drop(self.inner.take());
            // Only a destruction while still pending is cancellation. Firing on
            // normal completion too would let an observer that simply finished
            // satisfy an assertion about abandonment.
            if let Some(probe) = self.probe.take()
                && !self.completed
            {
                let _ = probe.dropped.try_send(());
            }
        }
    }

    /// Release gate for a controlled root initialization.
    struct Gate {
        release: watch::Sender<bool>,
        entered: SyncSender<()>,
    }

    impl Gate {
        fn new(entered: SyncSender<()>) -> (Arc<Self>, watch::Receiver<bool>) {
            let (release, rx) = watch::channel(false);
            (Arc::new(Self { release, entered }), rx)
        }

        fn open(&self) {
            let _ = self.release.send(true);
        }
    }

    /// The fixed result a controlled operation yields once it is released.
    #[derive(Default)]
    enum OpOutcome {
        #[default]
        Succeed,
        /// Resolves to a `PyValueError` carrying exactly this message.
        Fail(&'static str),
    }

    /// Opt-in control over one `ControlledActorMesh` operation.
    ///
    /// `Default` is no entry signal, no gate, immediate success. It is not
    /// literally inert: `run` always increments `completed`, so a defaulted
    /// operation still performs one sequentially consistent atomic increment
    /// that nothing reads. Every externally visible property -- value, error, and timing --
    /// matches returning the value directly, which is what lets a caller that
    /// does not care about ordering ignore the control entirely.
    #[derive(Default)]
    struct OpControl {
        /// `None` leaves the operation ungated.
        release: Option<watch::Receiver<bool>>,
        /// `None` emits no entry acknowledgement.
        entered: Option<SyncSender<()>>,
        /// Incremented once per released run, after the wait and before the
        /// outcome is produced.
        completed: Arc<AtomicUsize>,
        outcome: OpOutcome,
    }

    impl OpControl {
        /// Gated: acknowledges entry, then waits for `gate` before completing.
        fn gated(gate: &Arc<Gate>, release: watch::Receiver<bool>, outcome: OpOutcome) -> Self {
            Self {
                release: Some(release),
                entered: Some(gate.entered.clone()),
                completed: Arc::new(AtomicUsize::new(0)),
                outcome,
            }
        }

        /// Ungated but still counted, for a subcase that needs a fixed outcome
        /// without an ordering rendezvous.
        fn immediate(outcome: OpOutcome) -> Self {
            Self {
                outcome,
                ..Self::default()
            }
        }

        fn completions(&self) -> usize {
            self.completed.load(Ordering::SeqCst)
        }

        /// Acknowledge entry, wait for release, count the completion, then
        /// yield the fixed outcome.
        ///
        /// The acknowledgement is emitted before the wait, so an observer of it
        /// learns that the caller reached this operation -- not that it
        /// finished. The counter is incremented after the wait, so a reader
        /// that has not synchronized with completion can legitimately see zero.
        async fn run(&self) -> PyResult<()> {
            if let Some(entered) = &self.entered {
                let _ = entered.try_send(());
            }
            if let Some(release) = &self.release {
                let mut release = release.clone();
                while !*release.borrow() {
                    if release.changed().await.is_err() {
                        break;
                    }
                }
            }
            self.completed.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                OpOutcome::Succeed => Ok(()),
                OpOutcome::Fail(message) => Err(PyValueError::new_err(message)),
            }
        }
    }

    /// The per-operation controls a controlled mesh carries. Both default to no
    /// gate, no entry signal and immediate success -- externally
    /// indistinguishable from returning the value directly, though `run` still
    /// performs its unread completion increment. Both are shared with every
    /// slice derived from the mesh.
    #[derive(Default, Clone)]
    struct ControlledOps {
        name: Arc<OpControl>,
        stop: Arc<OpControl>,
    }

    /// A closed `SupervisableActorMesh`: fixed results and internal gates only,
    /// never Python callables, coroutines or caller-supplied work.
    struct ControlledActorMesh {
        name: String,
        slices: Arc<AtomicUsize>,
        ops: ControlledOps,
    }

    impl ActorMeshProtocol for ControlledActorMesh {
        fn cast(
            &self,
            _message: PythonMessage,
            _selection: AllOrChoose,
            _instance: &Instance<PythonActor>,
        ) -> PyResult<()> {
            unreachable!("the controlled mesh has no cast path")
        }

        fn __reduce__<'py>(
            &self,
            py: Python<'py>,
        ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
            // Reached by the bare-pickle test: `reduce_shared` resolves the
            // observer to a `PythonActorMesh` over this implementation and
            // pickle then recurses into it. Deliberately not production's
            // `self.mesh_ref().__reduce__(py)`, which would need a managed ref
            // and exercise bincode to no purpose here.
            let ctor = py.get_type::<pyo3::types::PyString>().into_any();
            let args = PyTuple::new(py, [CONTROLLED_MARKER])?;
            Ok((ctor, args.into_any()))
        }

        fn mesh_ref(&self) -> PyResult<ActorMeshRef<PythonActor>> {
            // Mirrors the backstop `AsyncActorMesh::mesh_ref` uses for a mesh
            // with no serializable ref of its own. Nothing here needs it.
            Err(PyRuntimeError::new_err(
                "the controlled mesh has no serializable reference",
            ))
        }

        fn name(&self) -> PyResult<PyPythonTask> {
            let name = self.name.clone();
            let ops = self.ops.name.clone();
            PyPythonTask::new(async move {
                ops.run().await?;
                Ok(name)
            })
        }

        /// Resolves to `()`, mirroring the owned production arm, whose
        /// `ActorMesh::stop` returns `Result<()>`. On the Python side that is
        /// an empty tuple, not `None`: pyo3 defines `IntoPyObject for ()` as
        /// `PyTuple::empty`. Without this method the trait default returns
        /// `PyNotImplementedError` synchronously, and a queued caller's `?`
        /// would short-circuit before any inner work ran.
        fn stop(&self, _instance: &PyInstance, _reason: String) -> PyResult<PyPythonTask> {
            let ops = self.ops.stop.clone();
            PyPythonTask::new(async move {
                ops.run().await?;
                Ok(())
            })
        }
    }

    #[async_trait]
    impl Supervisable for ControlledActorMesh {
        async fn supervision_event(&self, _instance: &Instance<PythonActor>) -> Option<PyErr> {
            std::future::pending().await
        }
    }

    impl SupervisableActorMesh for ControlledActorMesh {
        fn new_with_region(&self, _region: &PyRegion) -> PyResult<Box<dyn SupervisableActorMesh>> {
            self.slices.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(ControlledActorMesh {
                name: format!("{}-slice", self.name),
                slices: self.slices.clone(),
                // Shared, not defaulted: a slice that reverted to the default
                // control would have no gate and no entry signal, making any
                // ordering measurement taken through it vacuous.
                ops: self.ops.clone(),
            }))
        }
    }

    /// The controlled root future: reports entry, waits for the gate, then
    /// resolves to a `ControlledActorMesh`.
    fn controlled_root(
        gate: &Arc<Gate>,
        mut release: watch::Receiver<bool>,
        slices: Option<Arc<AtomicUsize>>,
        ops: ControlledOps,
    ) -> ActorMeshFut {
        let entered = gate.entered.clone();
        let slices = slices.unwrap_or_default();
        async move {
            let _ = entered.try_send(());
            while !*release.borrow() {
                if release.changed().await.is_err() {
                    break;
                }
            }
            Ok::<Arc<dyn SupervisableActorMesh>, ClonePyErr>(Arc::new(ControlledActorMesh {
                name: "controlled".to_string(),
                slices,
                ops,
            }))
        }
        .boxed()
        .shared()
    }

    /// The production queue loop, with its `JoinHandle` retained.
    ///
    /// `AsyncActorMesh::new_queue` spawns exactly this loop and discards the
    /// handle; keeping it is the only difference, and production is not
    /// modified.
    fn test_queue() -> (
        UnboundedSender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let (queue, mut recv) = unbounded_channel();
        let driver = get_tokio_runtime().spawn(async move {
            loop {
                let r = recv.recv().await;
                if let Some(r) = r {
                    r.await;
                } else {
                    return;
                }
            }
        });
        (queue, driver)
    }

    /// Neither release permission nor a `dumps` return has claimed the phase.
    const PHASE_RUNNING: u8 = 0;
    /// The helper observed the blocking acknowledgement first.
    const PHASE_RELEASE_PERMITTED: u8 = 1;
    /// `pickle.dumps` returned first, which the bare-pickle test rejects.
    const PHASE_DUMPS_RETURNED: u8 = 2;

    /// Bound shared by every wait here: long enough that a healthy run never
    /// reaches it, short enough that a stranded one fails instead of hanging.
    const WAIT: Duration = Duration::from_secs(30);

    fn signal() -> (SyncSender<()>, Receiver<()>) {
        std::sync::mpsc::sync_channel(1)
    }

    /// Bounded wait; returns false on timeout so a test can clean up before
    /// asserting rather than unwinding with gates still shut.
    fn awaited(rx: &Receiver<()>) -> bool {
        rx.recv_timeout(WAIT).is_ok()
    }

    /// Drive a wrapper task under a bound. Returns `None` on timeout so a
    /// stranded `Shared` fails a named assertion rather than blocking forever
    /// and taking the whole test binary with it.
    fn drive_bounded(task: PyResult<PyPythonTask>) -> Option<Py<PyAny>> {
        let mut task = task.expect("the wrapper should return a task");
        let fut = task.take_task().expect("a fresh task is not consumed");
        get_tokio_runtime()
            .block_on(async { tokio::time::timeout(WAIT, fut).await })
            .ok()
            .map(|resolved| resolved.expect("the task should resolve"))
    }

    fn bounded_name(task: PyResult<PyPythonTask>) -> Option<String> {
        drive_bounded(task).map(|value| {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                value
                    .extract::<String>(py)
                    .expect("name() should resolve to a string")
            })
        })
    }

    /// Owns every release gate and the retained queue driver for one test.
    ///
    /// The success path calls `join`, which drops the driver handle and waits
    /// for the loop to exit once its senders close. `Drop` is the failure path
    /// only: on unwind it opens every gate still shut and aborts a driver still
    /// held, so an assertion failure cannot leave the runtime wedged. `Drop`
    /// cannot await, which is why it aborts rather than joins -- `abort()` only
    /// requests cancellation, so the guarantee is a bounded return, not that no
    /// task outlives the test.
    ///
    /// Ownership of the operation gates is what makes that promise reachable: a
    /// gate held only by the test body stays shut on an unwinding path, and the
    /// future waiting on it never wakes.
    struct Teardown {
        gate: Arc<Gate>,
        op_gates: Vec<Arc<Gate>>,
        driver: Option<tokio::task::JoinHandle<()>>,
    }

    impl Teardown {
        fn new(gate: Arc<Gate>, driver: tokio::task::JoinHandle<()>) -> Self {
            Self {
                gate,
                op_gates: Vec::new(),
                driver: Some(driver),
            }
        }

        /// Hand an operation gate to the teardown so every exit path opens it.
        #[must_use]
        fn with_op_gate(mut self, gate: Arc<Gate>) -> Self {
            self.op_gates.push(gate);
            self
        }

        /// Open the root gate and every operation gate. Idempotent.
        fn open_all(&self) {
            self.gate.open();
            for gate in &self.op_gates {
                gate.open();
            }
        }

        /// True only if the loop returned normally. A panicked or aborted task
        /// yields `Ok(Err(JoinError))`, which the outer `is_ok()` alone would
        /// accept -- and `AsyncActorMesh::name`/`stop` can panic a driver.
        fn join(&mut self) -> bool {
            let Some(driver) = self.driver.as_mut() else {
                return false;
            };
            let outcome =
                get_tokio_runtime().block_on(async { tokio::time::timeout(WAIT, driver).await });
            match outcome {
                // Only a normal return counts. A panicked or aborted task
                // yields `Ok(Err(JoinError))`, which an outer `is_ok()` alone
                // would accept -- and `AsyncActorMesh::name`/`stop` can panic a
                // driver.
                Ok(result) => {
                    self.driver.take();
                    result.is_ok()
                }
                // Wedged. Request cancellation and wait once more rather than
                // dropping the handle, which would only detach. The second wait
                // can itself elapse, so this attempts a bounded reap; it does
                // not establish one.
                Err(_) => {
                    if let Some(driver) = self.driver.take() {
                        driver.abort();
                        let _ = get_tokio_runtime()
                            .block_on(async { tokio::time::timeout(WAIT, driver).await });
                    }
                    false
                }
            }
        }

        /// `join` for a caller already inside a runtime.
        async fn join_async(&mut self) -> bool {
            let Some(driver) = self.driver.as_mut() else {
                return false;
            };
            match tokio::time::timeout(WAIT, driver).await {
                Ok(result) => {
                    self.driver.take();
                    result.is_ok()
                }
                Err(_) => {
                    if let Some(driver) = self.driver.take() {
                        driver.abort();
                        let _ = tokio::time::timeout(WAIT, driver).await;
                    }
                    false
                }
            }
        }

        /// The inverse of `join`: the panic payload if the loop panicked, and
        /// `None` otherwise.
        ///
        /// A sibling rather than a relaxation of `join`, which must keep
        /// rejecting `Ok(Err(JoinError))` for the assertions that depend on it.
        /// Returning the payload rather than a bool is what lets a caller check
        /// the `oneshot failed` boundary instead of merely "something panicked".
        /// A normal return yields `None`, so a driver that exited cleanly cannot
        /// satisfy an expected-panic assertion.
        fn join_expecting_panic(&mut self) -> Option<String> {
            let driver = self.driver.as_mut()?;
            let outcome =
                get_tokio_runtime().block_on(async { tokio::time::timeout(WAIT, driver).await });
            match outcome {
                Ok(Err(join_error)) if join_error.is_panic() => {
                    self.driver.take();
                    Some(panic_message(join_error.into_panic().as_ref()))
                }
                Ok(_) => {
                    self.driver.take();
                    None
                }
                // Wedged. Attempt bounded reaping rather than detaching; the
                // second wait can itself elapse, so this requests cancellation
                // and returns under a bound rather than proving a reap.
                Err(_) => {
                    if let Some(driver) = self.driver.take() {
                        driver.abort();
                        let _ = get_tokio_runtime()
                            .block_on(async { tokio::time::timeout(WAIT, driver).await });
                    }
                    None
                }
            }
        }

        /// `join_expecting_panic` for a caller already inside a runtime.
        async fn join_expecting_panic_async(&mut self) -> Option<String> {
            let driver = self.driver.as_mut()?;
            let outcome = tokio::time::timeout(WAIT, driver).await;
            match outcome {
                Ok(Err(join_error)) if join_error.is_panic() => {
                    self.driver.take();
                    Some(panic_message(join_error.into_panic().as_ref()))
                }
                Ok(_) => {
                    self.driver.take();
                    None
                }
                // Wedged. Attempt bounded reaping rather than detaching; the
                // second await can itself elapse, so this requests cancellation
                // and returns under a bound rather than proving a reap.
                Err(_) => {
                    if let Some(driver) = self.driver.take() {
                        driver.abort();
                        let _ = tokio::time::timeout(WAIT, driver).await;
                    }
                    None
                }
            }
        }
    }

    impl Drop for Teardown {
        fn drop(&mut self) {
            self.open_all();
            if let Some(driver) = self.driver.take() {
                driver.abort();
            }
        }
    }

    // Ordinary bare pickle of a pending mesh blocks until initialization
    // resolves.
    //
    // The release is driven by an acknowledgement emitted inside
    // `PyShared::block_on` once it has committed to blocking, not by the
    // controlled future's first poll: `__reduce__` calls `spawn_abortable()`
    // before returning, so that first poll happens while pickle is still on the
    // outer reducer, and releasing there would let the producer resolve in time
    // for `reduce_shared`'s ready fast path -- passing this test without ever
    // blocking.
    //
    // A plain `#[test]`: `PyShared::block_on` enters `signal_safe_block_on` for
    // a pending value, and calling that from inside a Tokio runtime
    // characterizes nested-runtime failure instead of bare-pickle behavior.
    #[test]
    fn pending_bare_actor_mesh_pickle_waits_for_release() {
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            PyPythonTask::install_test_module(py).expect("the pytokio module should install")
        });

        let (entered_tx, entered_rx) = signal();
        let (gate, release) = Gate::new(entered_tx);
        let (queue, driver) = test_queue();
        // No init item is pushed: the reduction observer is the only driver.
        let mesh = AsyncActorMesh::new(
            queue,
            true,
            controlled_root(&gate, release, None, ControlledOps::default()),
        );
        let mut teardown = Teardown::new(gate.clone(), driver);
        let mesh = monarch_with_gil_blocking(GilSite::Test, |py| {
            Py::new(py, PythonActorMesh::from_impl(Arc::new(mesh)))
                .expect("the mesh wrapper should construct")
        });

        // A CAS rather than a sampled flag: exactly one of "release was
        // permitted" and "dumps returned" claims the phase, so a return racing
        // the read cannot satisfy the assertion below. This guards that `dumps`
        // did not return before release permission; it does not show where the
        // acknowledgement hook sits. That the hook fires after `block_on`'s own
        // pending poll and before `signal_safe_block_on` is source-grounded,
        // not established here.
        let phase = Arc::new(AtomicU8::new(PHASE_RUNNING));
        let (ack_tx, ack_rx) = signal();
        let helper_gate = gate.clone();
        let helper_phase = phase.clone();
        let helper = std::thread::spawn(move || {
            let acknowledged = ack_rx.recv_timeout(WAIT).is_ok();
            let release_won = helper_phase
                .compare_exchange(
                    PHASE_RUNNING,
                    PHASE_RELEASE_PERMITTED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok();
            // Opened on every path, timeout included, so the pickling call can
            // never be stranded.
            helper_gate.open();
            (acknowledged, release_won)
        });

        let blob = monarch_with_gil_blocking(GilSite::Test, |py| {
            let _routing = PyPythonTask::route_pending_block_on(ack_tx);
            let pickle = py.import("pickle").expect("pickle should import");
            let blob = pickle
                .call_method1("dumps", (mesh.clone_ref(py),))
                .expect("pickling a pending mesh should succeed once released")
                .unbind();
            let _ = phase.compare_exchange(
                PHASE_RUNNING,
                PHASE_DUMPS_RETURNED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            blob
        });

        let (acknowledged, release_won) =
            helper.join().expect("the helper thread should not panic");

        let restored = monarch_with_gil_blocking(GilSite::Test, |py| {
            let pickle = py.import("pickle").expect("pickle should import");
            pickle
                .call_method1("loads", (blob.bind(py),))
                .expect("the pickled mesh should load")
                .extract::<String>()
                .expect("the fixture reconstructs to its marker")
        });

        // Dropped with the GIL held: a `Py<_>` released without it only queues
        // the decref, so the wrapper -- and the queue sender inside it -- would
        // stay alive and the driver would never see its senders close.
        monarch_with_gil_blocking(GilSite::Test, move |_py| {
            drop(mesh);
            drop(blob);
        });
        let joined = teardown.join();

        assert!(
            awaited(&entered_rx),
            "the controlled root must have been polled by the reduction observer"
        );
        assert!(
            acknowledged,
            "pickling must reach the pending branch of PyShared::block_on"
        );
        assert!(
            release_won,
            "pickle.dumps must not return before release permission is granted"
        );
        assert_eq!(
            restored, CONTROLLED_MARKER,
            "the recursive controlled reducer must have run after the blocking branch"
        );
        assert!(
            joined,
            "the queue driver must return once its senders close"
        );
    }

    /// Call the pending reducer and prove the pending branch exactly: the real
    /// `Shared.block_on` descriptor plus exactly one real `Shared` argument.
    fn pending_reduce(py: Python<'_>, mesh: &dyn ActorMeshProtocol) -> Py<PyAny> {
        let (callable, args) = mesh.__reduce__(py).expect("__reduce__ should succeed");
        let expected = shared_class(py)
            .getattr("block_on")
            .expect("Shared.block_on should resolve");
        assert!(
            callable.is(&expected),
            "a pending mesh must reduce through the real Shared.block_on descriptor"
        );
        let args = args
            .cast_into::<PyTuple>()
            .expect("the reducer arguments should be a tuple");
        assert_eq!(args.len(), 1, "exactly one observer argument is expected");
        let observer = args.get_item(0).expect("the tuple has one item");
        assert!(
            observer
                .is_instance(&shared_class(py))
                .expect("instance check should succeed"),
            "the reducer argument must be a real Shared"
        );
        args.into_any().unbind()
    }

    // Dropping a transient reduction observer, and an unpolled readiness
    // observer, cancels neither. The root keeps its own queued initializer, so
    // it completes on its own and stays usable.
    //
    // The completion signal is emitted by the queued initializer *after* its
    // `f.await` returns, and is awaited before any replacement observer is
    // created. Without that ordering the fresh `initialized()` could be what
    // drove initialization and the test would prove nothing.
    #[test]
    fn dropped_root_observers_do_not_cancel_initialization() {
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            PyPythonTask::install_test_module(py).expect("the pytokio module should install")
        });

        let (entered_tx, _entered_rx) = signal();
        let (gate, release) = Gate::new(entered_tx);
        let (queue, driver) = test_queue();
        let mesh = AsyncActorMesh::new(
            queue,
            true,
            controlled_root(&gate, release, None, ControlledOps::default()),
        );
        let mut teardown = Teardown::new(gate.clone(), driver);

        // The independent root initializer, equivalent to the one `new_queue`
        // pushes, plus the completion signal after `f.await` returns. Enqueued
        // here rather than taken from `new_queue` because the test needs the
        // driver's `JoinHandle`, which `new_queue` discards.
        //
        // Evidence boundary: this pins that abandonment does not cancel an
        // already-enqueued initializer. That production supplies that item is
        // source-grounded -- `new_queue` pushes `async { let _ = f.await; }` --
        // and is not what these assertions establish.
        let (completed_tx, completed_rx) = signal();
        let root = mesh.mesh.clone();
        mesh.push(async move {
            let _ = root.await;
            let _ = completed_tx.try_send(());
        });

        let (probe_entered_tx, probe_entered_rx) = signal();
        let (probe_dropped_tx, probe_dropped_rx) = signal();
        let _probe = arm_reduction_probe(ReductionProbe {
            entered: probe_entered_tx,
            dropped: probe_dropped_tx,
        });
        let tuple = monarch_with_gil_blocking(GilSite::Test, |py| pending_reduce(py, &mesh));

        let observed = awaited(&probe_entered_rx);
        // Dropped with the GIL held. Releasing a `Py<_>` without it only queues
        // the decref, so the tuple's `Shared` would not be freed, its
        // abort-on-drop would not fire, and the acknowledgement below would wait
        // on a future that is still alive.
        monarch_with_gil_blocking(GilSite::Test, move |_py| drop(tuple));
        let acknowledged = awaited(&probe_dropped_rx);

        // A second, never-polled observer.
        drop(
            mesh.initialized()
                .expect("initialized() should return a task"),
        );

        gate.open();
        let completed = awaited(&completed_rx);

        // Only drive replacements once the prerequisite holds. Falling through
        // on a failed prerequisite would block on a `Shared` that can never
        // resolve, turning the regression this test exists to catch into a hang
        // that kills the whole binary.
        let readiness = completed.then(|| drive_bounded(mesh.initialized()));
        let name = completed.then(|| bounded_name(mesh.name()));

        drop(mesh);
        let joined = teardown.join();

        assert!(
            observed,
            "the reduction observer must be polled before it is dropped"
        );
        assert!(
            acknowledged,
            "the reduction observer must be destroyed while still pending"
        );
        assert!(
            completed,
            "the queue-held root initializer must complete on its own after release"
        );
        let readiness = readiness.flatten().expect("readiness must resolve in time");
        assert!(
            monarch_with_gil_blocking(GilSite::Test, |py| readiness.is_none(py)),
            "a replacement readiness observer resolves to None"
        );
        assert_eq!(
            name.flatten().as_deref(),
            Some("controlled"),
            "the resolved implementation stays usable"
        );
        assert!(
            joined,
            "the queue driver must return once its senders close"
        );
    }

    // A slice holds a derived computation with no queue item of its own, so
    // discarding its observers leaves it dormant and re-observable.
    //
    // Boundary of this claim: `new_with_region` enqueues no independent
    // initializer, but a later `name`, `stop` or cast enqueues an await and
    // `supervision_event` awaits the derived `Shared` directly. These assertions
    // therefore prove dormancy only after the named observers are discarded and
    // while no other mesh operation occurs. They do not prove that an
    // `initialized` observer is the only thing that can ever drive a slice.
    #[test]
    fn dropped_slice_observers_leave_the_slice_reobservable() {
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            PyPythonTask::install_test_module(py).expect("the pytokio module should install")
        });

        let slices = Arc::new(AtomicUsize::new(0));
        let (entered_tx, _entered_rx) = signal();
        let (gate, release) = Gate::new(entered_tx);
        let (queue, driver) = test_queue();
        let mesh = AsyncActorMesh::new(
            queue,
            true,
            controlled_root(
                &gate,
                release,
                Some(slices.clone()),
                ControlledOps::default(),
            ),
        );
        let mut teardown = Teardown::new(gate.clone(), driver);

        // The independent root initializer, equivalent to the one `new_queue`
        // pushes, plus the completion signal after `f.await` returns. Enqueued
        // here rather than taken from `new_queue` because the test needs the
        // driver's `JoinHandle`, which `new_queue` discards.
        //
        // Evidence boundary: this pins that abandonment does not cancel an
        // already-enqueued initializer. That production supplies that item is
        // source-grounded -- `new_queue` pushes `async { let _ = f.await; }` --
        // and is not what these assertions establish.
        let (completed_tx, completed_rx) = signal();
        let root = mesh.mesh.clone();
        mesh.push(async move {
            let _ = root.await;
            let _ = completed_tx.try_send(());
        });

        let region: PyRegion = ndslice::Region::from(extent!(slices = 1)).into();
        let slice = mesh
            .new_with_region(&region)
            .expect("slicing a pending mesh should succeed");

        let (probe_entered_tx, probe_entered_rx) = signal();
        let (probe_dropped_tx, probe_dropped_rx) = signal();
        let _probe = arm_reduction_probe(ReductionProbe {
            entered: probe_entered_tx,
            dropped: probe_dropped_tx,
        });
        let tuple =
            monarch_with_gil_blocking(GilSite::Test, |py| pending_reduce(py, slice.as_ref()));

        let observed = awaited(&probe_entered_rx);
        monarch_with_gil_blocking(GilSite::Test, move |_py| drop(tuple));
        let acknowledged = awaited(&probe_dropped_rx);

        drop(
            slice
                .initialized()
                .expect("initialized() should return a task"),
        );

        gate.open();
        let completed = awaited(&completed_rx);
        let dormant = slices.load(Ordering::SeqCst);

        let readiness = completed.then(|| drive_bounded(slice.initialized()));
        let driven = slices.load(Ordering::SeqCst);
        let name = completed.then(|| bounded_name(slice.name()));

        drop(slice);
        drop(mesh);
        let joined = teardown.join();

        assert!(
            observed,
            "the slice reduction observer must be polled before it is dropped"
        );
        assert!(
            acknowledged,
            "the slice reduction observer must be destroyed while still pending"
        );
        assert!(completed, "the root must complete on its own after release");
        assert_eq!(
            dormant, 0,
            "the slice has no queue item, so discarding its observers leaves it undriven"
        );
        let readiness = readiness.flatten().expect("readiness must resolve in time");
        assert!(
            monarch_with_gil_blocking(GilSite::Test, |py| readiness.is_none(py)),
            "a replacement slice observer resolves to None"
        );
        assert_eq!(
            driven, 1,
            "the replacement observer drives the slice exactly once"
        );
        assert_eq!(
            name.flatten().as_deref(),
            Some("controlled-slice"),
            "the retained slice stays usable"
        );
        assert!(
            joined,
            "the queue driver must return once its senders close"
        );
    }

    // Queued name and stop characterization

    /// The fixed message a controlled operation fails with, so a test can
    /// assert the exact error text rather than merely that an error arrived.
    const CONTROLLED_FAILURE: &str = "controlled operation failed";

    /// The message the `Ref` arm rejects with. Pinned here so a wording change
    /// in production fails this test rather than silently weakening it.
    const REF_STOP_REJECTION: &str =
        "Cannot call stop on an ActorMeshRef, requires an owned ActorMesh";

    /// A real `PyInstance` over an isolated proc.
    ///
    /// `actor_instance` spawns detached introspect tasks, so every caller must
    /// be a `#[tokio::test]`: cleanup is the per-test runtime being dropped, and
    /// building this on the shared runtime would leak those tasks for the life
    /// of the test binary.
    fn isolated_py_instance(name: &str) -> PyInstance {
        let instance = Proc::isolated()
            .actor_instance::<PythonActor>(name)
            .expect("an isolated proc should host a client instance")
            .instance;
        PyInstance::from(instance)
    }

    /// A controlled mesh over a retained queue driver.
    ///
    /// The teardown owns the root gate, every operation gate passed here, and
    /// the driver, so an unwinding assertion cannot strand a future behind a
    /// gate that only the test body held.
    fn controlled_fixture(
        ops: ControlledOps,
        op_gates: &[Arc<Gate>],
    ) -> (AsyncActorMesh, Arc<Gate>, Receiver<()>, Teardown) {
        let (root_entered_tx, root_entered_rx) = signal();
        let (root_gate, release) = Gate::new(root_entered_tx);
        let (queue, driver) = test_queue();
        let mesh =
            AsyncActorMesh::new(queue, true, controlled_root(&root_gate, release, None, ops));
        let mut teardown = Teardown::new(root_gate.clone(), driver);
        for gate in op_gates {
            teardown = teardown.with_op_gate(gate.clone());
        }
        (mesh, root_gate, root_entered_rx, teardown)
    }

    /// Drive the queued closure as far as the inner operation, reporting what
    /// was observed instead of asserting it.
    ///
    /// Returns `(root entered, operation entered)`. Both waits are bounded, and
    /// neither is asserted here: a failed wait must not unwind while the gates
    /// are shut and the driver is live, because `Teardown::Drop` only requests
    /// cancellation. Callers collect this evidence, run their cleanup, and
    /// assert afterwards.
    ///
    /// The order is fixed and load-bearing. This seam builds the mesh with
    /// `AsyncActorMesh::new`, which enqueues no initializer, so nothing polls
    /// the root future until a queued operation awaits it. With the root gate
    /// still shut the closure parks at that await and never reaches the inner
    /// method, so the operation acknowledgement would never arrive. Opening the
    /// root gate only after the root acknowledgement is what makes the
    /// operation acknowledgement mean "entered the inner method".
    fn reach_inner_operation(
        root_entered: &Receiver<()>,
        root_gate: &Arc<Gate>,
        op_entered: &Receiver<()>,
    ) -> (bool, bool) {
        let reached_root = awaited(root_entered);
        root_gate.open();
        (reached_root, awaited(op_entered))
    }

    /// Destroy the observer the call actually returned, reporting whether one
    /// was returned at all.
    ///
    /// The outer observer is constructed *after* its work is enqueued, and
    /// `PyPythonTask::new` is fallible -- it propagates `current_traceback()?`.
    /// A construction failure therefore drops the receiver too, so the queued
    /// work still runs, still fails at `tx.send`, and still panics the driver.
    /// Without proving construction succeeded, "the caller discarded a returned
    /// observer" and "the observer was never returned" satisfy the same
    /// assertions, and they are different public boundaries.
    ///
    /// No GIL scope: `PyPythonTask` owns `Option<PythonTask>`, which owns the
    /// boxed future directly, so dropping this Rust value destroys the future
    /// and the `rx` it captured immediately. Only the task's optional
    /// `Py<PyAny>` traceback defers a decref, and nothing here depends on when
    /// that happens.
    fn discard_observer(task: PyResult<PyPythonTask>) -> Result<(), PyErr> {
        match task {
            Ok(observer) => {
                drop(observer);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Everything driving a wrapper task can produce, as data rather than as a
    /// panic. Nothing here asserts: a subcase collects an outcome, runs its
    /// cleanup, and only then decides whether it is the expected one.
    enum DriveOutcome {
        /// The call returned an error instead of an observer.
        NotConstructed(PyErr),
        /// The observer existed but its future had already been taken.
        TaskConsumed,
        /// The observer did not resolve within `WAIT`.
        TimedOut,
        Resolved(PyResult<Py<PyAny>>),
    }

    impl DriveOutcome {
        /// The resolved result, or a description of why there is not one.
        fn resolved(self, context: &str) -> PyResult<Py<PyAny>> {
            match self {
                DriveOutcome::Resolved(result) => result,
                DriveOutcome::NotConstructed(err) => {
                    panic!("{context}: the wrapper returned no observer: {err:?}")
                }
                DriveOutcome::TaskConsumed => {
                    panic!("{context}: the observer's future was already taken")
                }
                DriveOutcome::TimedOut => panic!("{context}: the observer did not resolve"),
            }
        }
    }

    /// Drive a wrapper task under a bound without asserting anything.
    fn drive_evidence(task: PyResult<PyPythonTask>) -> DriveOutcome {
        let mut task = match task {
            Ok(task) => task,
            Err(err) => return DriveOutcome::NotConstructed(err),
        };
        let Ok(fut) = task.take_task() else {
            return DriveOutcome::TaskConsumed;
        };
        match get_tokio_runtime().block_on(async { tokio::time::timeout(WAIT, fut).await }) {
            Ok(resolved) => DriveOutcome::Resolved(resolved),
            Err(_) => DriveOutcome::TimedOut,
        }
    }

    /// `drive_evidence` for a caller already inside a runtime.
    async fn drive_evidence_async(task: PyResult<PyPythonTask>) -> DriveOutcome {
        let mut task = match task {
            Ok(task) => task,
            Err(err) => return DriveOutcome::NotConstructed(err),
        };
        let Ok(fut) = task.take_task() else {
            return DriveOutcome::TaskConsumed;
        };
        match tokio::time::timeout(WAIT, fut).await {
            Ok(resolved) => DriveOutcome::Resolved(resolved),
            Err(_) => DriveOutcome::TimedOut,
        }
    }

    /// Enqueue a barrier behind the work already queued and wait for it.
    ///
    /// The queue runs one item at a time to completion, so observing the
    /// barrier proves the preceding closure finished -- including its
    /// `tx.send`. That is a rendezvous with the send, not a delay.
    ///
    /// Sends on the queue directly rather than through `push`, whose
    /// `send(..).unwrap()` would panic before the caller's cleanup if the
    /// driver had already died. A closed queue is reported, not raised.
    fn queue_barrier(mesh: &AsyncActorMesh) -> bool {
        let (passed_tx, passed_rx) = signal();
        let queued = mesh
            .queue
            .send(
                async move {
                    let _ = passed_tx.try_send(());
                }
                .boxed(),
            )
            .is_ok();
        queued && awaited(&passed_rx)
    }

    /// Require an error to be exactly `E`, by type identity rather than by
    /// subclass or message, and to carry exactly `message`.
    fn assert_exact_error<E: PyTypeInfo>(err: &PyErr, message: &str, context: &str) {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(
                err.get_type(py).is(py.get_type::<E>()),
                "{context}: expected exactly {}, got {}",
                std::any::type_name::<E>(),
                err.get_type(py),
            );
            assert_eq!(
                err.value(py).to_string(),
                message,
                "{context}: unexpected error message"
            );
        });
    }

    // `AsyncActorMesh::name` enqueues its work when it is called, not when the
    // returned observer is awaited, and reports through a one-shot channel. A
    // retained observer receives the inner value, or the inner error verbatim.
    //
    // The queue panics when the receiver has been destroyed at the moment
    // `tx.send` runs. That is narrower than "never observed": tokio's
    // `oneshot::Sender::send` fails only if the receiver has been deallocated,
    // so an observer that is merely unpolled still takes delivery, and dropping
    // it afterwards loses the buffered value without touching the driver. Both
    // sides are covered below.
    //
    // This panic is current behavior being characterized, not behavior the
    // migration should preserve.
    //
    // Every subcase collects evidence, opens its gates, closes the queue and
    // joins the driver, and only then asserts. Unwinding earlier would leave a
    // gate shut and the driver merely abort-requested, since `Teardown::Drop`
    // cannot await.
    //
    // A plain `#[test]`: no subcase needs a `PyInstance`, so nothing here
    // requires a per-test runtime and the sync helpers apply.
    #[test]
    fn async_name_runs_before_a_dropped_observer_panics_the_queue() {
        pyo3::Python::initialize();

        // Retained observer, success: the inner value reaches the caller.
        {
            let name_ops = Arc::new(OpControl::immediate(OpOutcome::Succeed));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: name_ops.clone(),
                    stop: Arc::default(),
                },
                &[],
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::name(&mesh);
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let outcome = drive_evidence(task);

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join();

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            let value = outcome
                .resolved("retained name success")
                .expect("the controlled name should succeed");
            let observed = monarch_with_gil_blocking(GilSite::Test, move |py| {
                value
                    .extract::<String>(py)
                    .expect("name() should resolve to a string")
            });
            assert_eq!(
                observed, "controlled",
                "a retained observer must receive the inner name"
            );
            assert_eq!(
                name_ops.completions(),
                1,
                "the inner operation must run exactly once"
            );
            assert!(joined, "the driver must return once its senders close");
        }

        // Retained observer, error: the inner error reaches the caller intact.
        // It must be read through a retained observer, because once the
        // receiver is gone the queue panic replaces the inner result and a
        // discarded observer can never be the error oracle.
        {
            let name_ops = Arc::new(OpControl::immediate(OpOutcome::Fail(CONTROLLED_FAILURE)));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: name_ops.clone(),
                    stop: Arc::default(),
                },
                &[],
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::name(&mesh);
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let outcome = drive_evidence(task);

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join();

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            let err = outcome
                .resolved("retained name error")
                .expect_err("the fixed-error control must surface as an error");
            assert_exact_error::<PyValueError>(&err, CONTROLLED_FAILURE, "retained name error");
            assert_eq!(
                name_ops.completions(),
                1,
                "the failing inner operation must still run exactly once"
            );
            assert!(joined, "the driver must return once its senders close");
        }

        // Unpolled observer destroyed *after* the send: no panic.
        //
        // The complement of the case below, and what limits the panic claim to
        // receiver destruction before `tx.send`. The barrier is a rendezvous
        // with the send, not a delay: the queue is serial, so observing an item
        // enqueued behind the name closure proves that closure finished.
        {
            let name_ops = Arc::new(OpControl::immediate(OpOutcome::Succeed));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: name_ops.clone(),
                    stop: Arc::default(),
                },
                &[],
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::name(&mesh);
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let sent = queue_barrier(&mesh);
            // Never polled, and destroyed only after the send succeeded.
            let construction = discard_observer(task);

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join();

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            assert!(sent, "the barrier should prove the name closure completed");
            assert!(
                construction.is_ok(),
                "the outer observer must have been constructed: {:?}",
                construction.err()
            );
            assert_eq!(
                name_ops.completions(),
                1,
                "the inner operation must run exactly once"
            );
            assert!(
                joined,
                "destroying an unpolled observer after a successful send must not panic the driver"
            );
        }

        // Observer destroyed while the operation is still pending: the work
        // completes, then the report fails.
        {
            let (op_entered_tx, op_entered) = signal();
            let (op_gate, op_release) = Gate::new(op_entered_tx);
            let name_ops = Arc::new(OpControl::gated(&op_gate, op_release, OpOutcome::Succeed));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: name_ops.clone(),
                    stop: Arc::default(),
                },
                std::slice::from_ref(&op_gate),
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::name(&mesh);
            let (reached_root, entered_op) =
                reach_inner_operation(&root_entered, &root_gate, &op_entered);
            let construction = discard_observer(task);

            teardown.open_all();
            // Join before reading the counter. `Gate::open` is a `watch` send
            // with no rendezvous, so it returns before the released future has
            // been polled; a counter read here could legitimately see zero. The
            // join is the ordering: the driver terminates only after the queued
            // closure reached its send, which is after the increment.
            let payload = teardown.join_expecting_panic();
            drop(mesh);

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            assert!(
                entered_op,
                "the queued closure should enter the inner operation"
            );
            assert!(
                construction.is_ok(),
                "the outer observer must have been constructed: {:?}",
                construction.err()
            );
            let payload = payload.expect("discarding the observer must panic the driver");
            assert!(
                payload.contains("oneshot failed"),
                "the driver must fail at the report boundary, got: {payload}"
            );
            assert_eq!(
                name_ops.completions(),
                1,
                "the inner operation must have completed once, and the panic must not re-run it"
            );
        }
    }

    // `AsyncActorMesh::stop` has the same queued shape as `name`, plus a
    // `GilSite::Stop` instance clone taken before the work is enqueued. A
    // retained observer receives the success value -- an empty tuple, since the
    // owned production arm wraps `ActorMesh::stop`, whose `Ok` type is `()`,
    // and pyo3 converts the unit type with `IntoPyObject for () ->
    // PyTuple::empty` -- or the inner error verbatim. Destroying the receiver
    // while the operation is still pending panics the driver at `tx.send`.
    //
    // This panic is current behavior being characterized, not behavior the
    // migration should preserve.
    //
    // A `#[tokio::test]`: `stop` needs a real `PyInstance`, and `actor_instance`
    // spawns detached introspect tasks that the per-test runtime reaps. That in
    // turn rules out the `block_on` helpers, so this test uses the async
    // siblings throughout.
    #[tokio::test]
    async fn async_stop_runs_before_a_dropped_observer_panics_the_queue() {
        pyo3::Python::initialize();
        let instance = isolated_py_instance("stop_characterization_client");

        // Retained observer, success: the caller sees the unit conversion.
        {
            let stop_ops = Arc::new(OpControl::immediate(OpOutcome::Succeed));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: Arc::default(),
                    stop: stop_ops.clone(),
                },
                &[],
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::stop(
                &mesh,
                &instance,
                "characterization".to_string(),
            );
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let outcome = drive_evidence_async(task).await;

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join_async().await;

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            let value = outcome
                .resolved("retained stop success")
                .expect("the controlled stop should succeed");
            monarch_with_gil_blocking(GilSite::Test, move |py| {
                // An empty tuple, not None: pyo3 converts the unit type with
                // `IntoPyObject for () -> PyTuple::empty`, and the fixture
                // mirrors production's `Result<()>` exactly.
                let bound = value.bind(py);
                let tuple = bound
                    .cast::<PyTuple>()
                    .expect("the unit result should convert to a tuple");
                assert!(
                    tuple.is_empty(),
                    "a successful owned stop resolves to the unit conversion, the empty tuple"
                );
            });
            assert_eq!(
                stop_ops.completions(),
                1,
                "the inner operation must run exactly once"
            );
            assert!(joined, "the driver must return once its senders close");
        }

        // Retained observer, error: the inner error reaches the caller intact.
        {
            let stop_ops = Arc::new(OpControl::immediate(OpOutcome::Fail(CONTROLLED_FAILURE)));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: Arc::default(),
                    stop: stop_ops.clone(),
                },
                &[],
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::stop(
                &mesh,
                &instance,
                "characterization".to_string(),
            );
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let outcome = drive_evidence_async(task).await;

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join_async().await;

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            let err = outcome
                .resolved("retained stop error")
                .expect_err("the fixed-error control must surface as an error");
            assert_exact_error::<PyValueError>(&err, CONTROLLED_FAILURE, "retained stop error");
            assert_eq!(
                stop_ops.completions(),
                1,
                "the failing inner operation must still run exactly once"
            );
            assert!(joined, "the driver must return once its senders close");
        }

        // Unpolled observer destroyed after the send: no panic. See the name
        // test's equivalent subcase for why the barrier is a rendezvous.
        {
            let stop_ops = Arc::new(OpControl::immediate(OpOutcome::Succeed));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: Arc::default(),
                    stop: stop_ops.clone(),
                },
                &[],
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::stop(
                &mesh,
                &instance,
                "characterization".to_string(),
            );
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let sent = queue_barrier(&mesh);
            let construction = discard_observer(task);

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join_async().await;

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            assert!(sent, "the barrier should prove the stop closure completed");
            assert!(
                construction.is_ok(),
                "the outer observer must have been constructed: {:?}",
                construction.err()
            );
            assert_eq!(
                stop_ops.completions(),
                1,
                "the inner operation must run exactly once"
            );
            assert!(
                joined,
                "destroying an unpolled observer after a successful send must not panic the driver"
            );
        }

        // Observer destroyed while the operation is still pending.
        {
            let (op_entered_tx, op_entered) = signal();
            let (op_gate, op_release) = Gate::new(op_entered_tx);
            let stop_ops = Arc::new(OpControl::gated(&op_gate, op_release, OpOutcome::Succeed));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: Arc::default(),
                    stop: stop_ops.clone(),
                },
                std::slice::from_ref(&op_gate),
            );

            let task = <AsyncActorMesh as ActorMeshProtocol>::stop(
                &mesh,
                &instance,
                "characterization".to_string(),
            );
            let (reached_root, entered_op) =
                reach_inner_operation(&root_entered, &root_gate, &op_entered);
            let construction = discard_observer(task);

            teardown.open_all();
            let payload = teardown.join_expecting_panic_async().await;
            drop(mesh);

            assert!(
                reached_root,
                "the queued closure should reach root initialization"
            );
            assert!(
                entered_op,
                "the queued closure should enter the inner operation"
            );
            assert!(
                construction.is_ok(),
                "the outer observer must have been constructed: {:?}",
                construction.err()
            );
            let payload = payload.expect("discarding the observer must panic the driver");
            assert!(
                payload.contains("oneshot failed"),
                "the driver must fail at the report boundary, got: {payload}"
            );
            assert_eq!(
                stop_ops.completions(),
                1,
                "the inner operation must have completed once, and the panic must not re-run it"
            );
        }
    }

    // Asking a `PythonActorMeshImpl::Ref` to stop is rejected by the inner arm
    // itself, with no task returned: there is nothing to observe, nothing to
    // drop, and no queue involved.
    //
    // The assertions cover what is returned. "No task was constructed" is read
    // off the source -- the `Ref` arm returns before reaching any
    // `PyPythonTask::new` -- and is not proven here, because an `Err` return
    // cannot distinguish "never constructed" from "constructed and dropped".
    // The arm is also not side-effect free: `stop` clones `self` and the
    // instance under `GilSite::Stop` before the match, so this rejection still
    // takes the GIL once.
    //
    // This is the inner arm only. A deserialized public ref is wrapped in
    // `AsyncActorMesh`, so `ActorMesh.stop()` on one still enqueues and surfaces
    // this error through an outer observer instead of at the call.
    //
    // A `#[tokio::test]` for the same reason as the queued stop test: the
    // signature takes a `&PyInstance`.
    #[tokio::test]
    async fn actor_mesh_impl_ref_stop_rejects_before_returning_a_task() {
        pyo3::Python::initialize();
        // Built outside the assertion boundary, so a fixture failure cannot
        // satisfy the rejection assertion below.
        let mesh_impl = PythonActorMeshImpl::new_ref(data_mesh_ref());
        let instance = isolated_py_instance("ref_stop_characterization_client");

        let rejected = <PythonActorMeshImpl as ActorMeshProtocol>::stop(
            &mesh_impl,
            &instance,
            "characterization".to_string(),
        );

        let Err(err) = rejected else {
            panic!("the Ref arm must reject rather than return a task");
        };
        assert_exact_error::<PyNotImplementedError>(&err, REF_STOP_REJECTION, "Ref stop rejection");
    }
}
