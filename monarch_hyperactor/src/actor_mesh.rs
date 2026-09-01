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
                let shared =
                    PyPythonTask::new(async move { Ok(PythonActorMesh::from_impl(fut.await?)) })?
                        .spawn_abortable()?;
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
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

    use hyperactor::ActorRef;
    use hyperactor::ProcAddr;
    use hyperactor::ProcId;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::id::Label;
    use hyperactor::id::Uid;
    use hyperactor_mesh::host_mesh::HostMeshRef;
    use hyperactor_mesh::mesh_id::ActorMeshId;
    use hyperactor_mesh::mesh_id::HostMeshId;
    use ndslice::extent;

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
    // No Python-visible call reaches this default today, though not because
    // every `PythonActorMesh` wraps an `AsyncActorMesh`. The pending
    // `__reduce__` branch builds one directly over the resolved
    // `PythonActorMeshImpl`, which inherits this default; that object is only
    // ever consumed by the pickler, never handed to Python code that calls
    // `initialized()`. Both inheriting implementors are exercised directly here
    // because neither has a caller to exercise them through.
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
}
