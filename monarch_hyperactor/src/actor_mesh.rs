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
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::future::Shared;
use hyperactor::Instance;
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
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tracing::Instrument;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::context::PyInstance;
use crate::handle::PyHandle;
use crate::handle::WouldBlockRuntime;
use crate::pickle::PendingMeshFill;
use crate::pickle::PendingMessage;
use crate::proc::PyActorAddr;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::is_in_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on;
use crate::shape::PyRegion;
use crate::supervision::Supervisable;
use crate::supervision::SupervisionError;

py_global!(_pickle, "monarch._src.actor.actor_mesh", "_pickle");

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

    // HOT PATH: Be mindful of performance when making changes here.
    // To test how performance is affected by a change, run the RPC benchmarks in
    // `monarch/python/benches/`.
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

    /// The serializable reference for a mesh that already exists, without
    /// waiting. The default returns [`Self::mesh_ref`]. A pending mesh returns
    /// its created mesh's reference, its creation error, or `ValueError` while
    /// it is still being created.
    fn ready_mesh_ref(&self) -> PyResult<ActorMeshRef<PythonActor>> {
        self.mesh_ref()
    }

    /// Return a future that completes when the mesh has stopped.
    ///
    /// The default implementation rejects the call immediately.
    /// Mesh types that support stopping override this method.
    fn stop(
        &self,
        _instance: &PyInstance,
        _reason: String,
    ) -> PyResult<BoxFuture<'static, PyResult<()>>> {
        Err(PyNotImplementedError::new_err(format!(
            "stop() is not supported for {}",
            std::any::type_name::<Self>()
        )))
    }

    /// Return a future that completes when the mesh has been created.
    ///
    /// The default returns an already-completed future: the mesh exists.
    /// Pending meshes override this method to wait for creation.
    /// The completion value is exposed to Python as None.
    fn initialized(&self) -> BoxFuture<'static, PyResult<Option<()>>> {
        future::ready(Ok(None)).boxed()
    }

    /// Return a future that reports the mesh's name once creation has finished.
    fn name(&self) -> BoxFuture<'static, PyResult<String>>;
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

    /// Start stopping the mesh and return a Handle that reports the outcome.
    /// A mesh type that does not support stopping may reject the call before
    /// returning a Handle. Dropping the Handle does not cancel an accepted stop.
    fn stop(&self, instance: &PyInstance, reason: String) -> PyResult<PyHandle> {
        self.inner.stop(instance, reason).map(PyHandle::spawn)
    }

    /// Start waiting for mesh creation and return a Handle that completes
    /// with Python None. Dropping the Handle does not cancel creation.
    fn initialized(&self) -> PyHandle {
        PyHandle::spawn(self.inner.initialized())
    }

    /// Return a Handle for the mesh's name, waiting for creation if necessary.
    /// The wait starts with this call; dropping the Handle does not cancel it.
    fn name(&self) -> PyHandle {
        PyHandle::spawn(self.inner.name())
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
/// Shared completion of actor-mesh creation.
///
/// The mesh and its observers hold clones of the same future, so creation
/// runs once and each observer receives the same result.
pub(crate) type ActorMeshFut =
    Shared<Pin<Box<dyn Future<Output = ActorMeshResult> + Send + 'static>>>;

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

    // HOT PATH: Be mindful of performance when making changes here.
    // To test how performance is affected by a change, run the RPC benchmarks in
    // `monarch/python/benches/`.
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
                if crate::pickle::reserve_mesh_reference_if_active(PendingMeshFill::Actor(
                    fut.clone(),
                )) {
                    let pop_fn = py
                        .import("monarch._rust_bindings.monarch_hyperactor.pickle")?
                        .getattr("pop_mesh_reference")?;
                    return Ok((pop_fn, PyTuple::empty(py).into_any()));
                }
                // Bare pickling must wait before returning bytes; it cannot
                // hand an unresolved mesh to the eventual unpickler.
                if is_in_tokio_runtime() {
                    return Err(WouldBlockRuntime::new_err(
                        "cannot pickle a pending actor mesh from a Tokio runtime",
                    ));
                }
                let mesh = signal_safe_block_on(py, fut)??;
                mesh.__reduce__(py)
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

    fn ready_mesh_ref(&self) -> PyResult<ActorMeshRef<PythonActor>> {
        match self.mesh.peek().cloned() {
            Some(mesh) => mesh?.mesh_ref(),
            None => Err(PyValueError::new_err(
                "the actor mesh is still being created; wait for `initialized` first",
            )),
        }
    }

    /// Queue the stop after work already queued for this mesh, and return a
    /// future for its result.
    ///
    /// The queued work waits for mesh creation, then stops the mesh. Dropping
    /// the returned future does not cancel that work.
    fn stop(
        &self,
        instance: &PyInstance,
        reason: String,
    ) -> PyResult<BoxFuture<'static, PyResult<()>>> {
        // The queued work owns these clones so it can outlive this call.
        let mesh = self.mesh.clone();
        let instance = monarch_with_gil_blocking(GilSite::Stop, |_py| instance.clone());
        // The queue runs the stop; this channel reports its outcome to the
        // caller's future. Observing that future does not drive the stop.
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.push(async move {
            // If a step fails, `?` exits this inner async block with an error.
            // Awaiting the block binds either Ok or Err to `result`, after which
            // the outer block calls tx.send(result). No additional task is spawned.
            let result = async move {
                // Wait for mesh creation; a creation failure becomes the result.
                let mesh = mesh.await?;
                // Request the stop. A mesh reference rejects it here; an owned
                // mesh returns a future that performs the stop.
                let completion = mesh.stop(&instance, reason)?;
                // Run that future until stopping succeeds or fails.
                completion.await
            }
            .await;
            // The future returned by stop owns rx. If that future has been
            // dropped, tx.send fails. Ignore the error rather than panic and
            // prevent later queued work from running.
            let _ = tx.send(result);
        });
        Ok(async move { rx.await.map_err(anyhow::Error::from)? }.boxed())
    }

    /// Return a future that waits for mesh creation, then for the created mesh
    /// to start, without adding a queue item. Creation or start errors fail
    /// the future; success is exposed to Python as None.
    fn initialized(&self) -> BoxFuture<'static, PyResult<Option<()>>> {
        let mesh = self.mesh.clone();
        async move { mesh.await?.initialized().await }.boxed()
    }

    /// Return a future that waits for mesh creation, then reads its name.
    /// This does not add a queue item; creation or name errors fail the future.
    fn name(&self) -> BoxFuture<'static, PyResult<String>> {
        let mesh = self.mesh.clone();
        async move { mesh.await?.name().await }.boxed()
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
    /// The instance that spawned the mesh, used to wait for it to start.
    instance: Arc<Instance<PythonActor>>,
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
    pub(crate) fn new_owned(
        inner: ActorMesh<PythonActor>,
        instance: Instance<PythonActor>,
    ) -> Self {
        PythonActorMeshImpl::Owned(PyActorMesh {
            mesh: inner,
            instance: Arc::new(instance),
        })
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

    /// Return a future that stops an owned mesh.
    ///
    /// A mesh reference is rejected immediately with NotImplementedError.
    /// Native stop failures make the returned future fail with ValueError.
    fn stop(
        &self,
        instance: &PyInstance,
        reason: String,
    ) -> PyResult<BoxFuture<'static, PyResult<()>>> {
        let (slf, instance) =
            monarch_with_gil_blocking(GilSite::Stop, |_py| (self.clone(), instance.clone()));
        match slf {
            PythonActorMeshImpl::Owned(mut mesh) => Ok(async move {
                // The Rust-level stop only sends the stop; Python's `stop()`
                // also waits for the actors to terminate.
                mesh.mesh
                    .stop(instance.deref(), reason)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                mesh.mesh
                    .wait_for_terminal(instance.deref())
                    .await
                    .map_err(|err| PyValueError::new_err(err.to_string()))
            }
            .boxed()),
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

    /// Return a future that waits for every actor of an owned mesh to start.
    /// A mesh reference does not own its actors and completes immediately.
    fn initialized(&self) -> BoxFuture<'static, PyResult<Option<()>>> {
        let PythonActorMeshImpl::Owned(PyActorMesh { mesh, instance }) = self.clone() else {
            return future::ready(Ok(None)).boxed();
        };
        async move {
            mesh.wait_started(&*instance)
                .await
                .map_err(anyhow::Error::from)?;
            Ok(None)
        }
        .boxed()
    }

    /// Return the mesh reference's ID as its name, for both owned and
    /// reference meshes. The delegated method reads the ID immediately
    /// and returns an already-completed future.
    fn name(&self) -> BoxFuture<'static, PyResult<String>> {
        <ActorMeshRef<PythonActor> as ActorMeshProtocol>::name(self.mesh_ref())
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
                    mesh.cast_choose_with_headers(instance, caller_headers, message);
                    Ok(())
                }
            },
        }
    }

    /// Reject the call immediately with NotImplementedError: stopping
    /// requires an owned mesh, not a reference. No future is returned.
    fn stop(
        &self,
        _instance: &PyInstance,
        _reason: String,
    ) -> PyResult<BoxFuture<'static, PyResult<()>>> {
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

    /// Read the mesh ID immediately and return it as the name in an
    /// already-completed future. No creation wait or queue item is needed.
    fn name(&self) -> BoxFuture<'static, PyResult<String>> {
        let name = self.id().to_string();
        future::ready(Ok(name)).boxed()
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
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc::Receiver;
    use std::sync::mpsc::SyncSender;
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
    use pyo3::types::PyDict;
    use tokio::sync::watch;

    use super::*;
    use crate::actor::MeshRef;
    use crate::pickle::PendingMessage;
    use crate::pickle::PicklingState;

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
            vec![hyperactor::ProcAddr::instance(
                ChannelAddr::Local(1),
                "service",
            )],
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

    /// A one-rank data `ActorMeshRef<PythonActor>` carrying `id`. Its single
    /// member is never messaged.
    fn data_mesh_ref(id: &ActorMeshId) -> ActorMeshRef<PythonActor> {
        let proc_addr = ProcAddr::new(
            ProcId::new(
                Uid::Instance(1, None),
                Some(Label::new("local").expect("test label should be valid")),
            ),
            ChannelAddr::Local(1).into(),
        );
        let member: ActorRef<PythonActor> = ActorRef::attest(proc_addr.actor_addr("member"));
        ActorMeshRef::try_new_data(id.clone(), extent!(members = 1).into(), vec![member])
            .expect("a one-rank data mesh should be valid")
    }

    /// Drive the native name future to completion.
    fn drive_name(future: BoxFuture<'static, PyResult<String>>) -> String {
        get_tokio_runtime()
            .block_on(future)
            .expect("the name future should resolve")
    }

    // Both managed name wrappers yield the mesh id, and discarding a wrapper
    // consumes nothing: a later wrapper reports the same id.
    //
    // What these assertions establish and what they do not. They prove the
    // value is repeatable across a discarded observation. They do NOT prove
    // dynamically that the id is read at call time rather than inside the
    // returned future; an implementation that cloned the mesh and read the id
    // when driven would pass too. Call-time computation is source-grounded --
    // both bodies compute the string and return a ready Rust future. The
    // Python binding converts the result separately through `PyHandle::spawn`.
    #[test]
    fn discarded_managed_name_wrappers_leave_exact_name_repeatable() {
        pyo3::Python::initialize();
        let id = fixture_mesh_id();
        let mesh_ref = managed_mesh_ref(&id);
        let mesh_impl = PythonActorMeshImpl::new_ref(mesh_ref.clone());
        let expected = id.to_string();

        drop(<ActorMeshRef<PythonActor> as ActorMeshProtocol>::name(
            &mesh_ref,
        ));
        drop(<PythonActorMeshImpl as ActorMeshProtocol>::name(&mesh_impl));

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

    // Both data name wrappers yield the mesh id, as on the managed path.
    #[test]
    fn data_name_wrappers_report_mesh_id() {
        pyo3::Python::initialize();
        let id = fixture_mesh_id();
        let mesh_ref = data_mesh_ref(&id);
        let mesh_impl = PythonActorMeshImpl::new_ref(mesh_ref.clone());
        let expected = id.to_string();

        assert_eq!(
            drive_name(<ActorMeshRef<PythonActor> as ActorMeshProtocol>::name(
                &mesh_ref
            )),
            expected,
            "the ref wrapper must report the data mesh id"
        );
        assert_eq!(
            drive_name(<PythonActorMeshImpl as ActorMeshProtocol>::name(&mesh_impl)),
            expected,
            "the impl wrapper must report the data mesh id"
        );
    }

    // The trait-default `initialized` wrapper resolves to None and leaves the
    // mesh usable, on both implementors that inherit it.
    //
    // What this establishes and what it does not. The absence of domain work is
    // a property of the default's source body, `future::ready(Ok(None))`, not
    // something these assertions measure: driving through `block_on` would also
    // pass if the future yielded or did unrelated non-mutating work first, and
    // `PyHandle::spawn` converts its result through `monarch_with_gil`, which
    // may wait on the process-global GIL lock. So this is not a first-poll-ready
    // claim. Re-reading the name afterwards shows the mesh is still usable; it
    // is not a work counter.
    //
    // No production caller reaches this default today. Normal `PythonActorMesh`
    // construction and reconstruction use `AsyncActorMesh`, which overrides it.
    //
    // Neither `PythonActorMeshImpl` nor `ActorMeshRef` overrides `initialized`.
    // Both inheriting implementors are exercised directly because no
    // production caller was found.
    #[test]
    fn default_initialized_wrappers_return_none_and_leave_mesh_usable() {
        pyo3::Python::initialize();
        let id = fixture_mesh_id();
        let mesh_ref = managed_mesh_ref(&id);
        let mesh_impl = PythonActorMeshImpl::new_ref(mesh_ref.clone());

        drop(<ActorMeshRef<PythonActor> as ActorMeshProtocol>::initialized(&mesh_ref));
        drop(<PythonActorMeshImpl as ActorMeshProtocol>::initialized(
            &mesh_impl,
        ));

        for (future, which) in [
            (
                <ActorMeshRef<PythonActor> as ActorMeshProtocol>::initialized(&mesh_ref),
                "ref",
            ),
            (
                <PythonActorMeshImpl as ActorMeshProtocol>::initialized(&mesh_impl),
                "impl",
            ),
        ] {
            let handle = PyHandle::spawn(future);
            let value = get_tokio_runtime()
                .block_on(handle.wait_future())
                .expect("the default initialized Handle should resolve");
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
            // Reached by the bare-pickle test after the native blocking wait
            // resolves to this implementation. Deliberately not production's
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

        fn name(&self) -> BoxFuture<'static, PyResult<String>> {
            let name = self.name.clone();
            let ops = self.ops.name.clone();
            async move {
                ops.run().await?;
                Ok(name)
            }
            .boxed()
        }

        /// Resolves to `()`, mirroring the owned production arm, whose
        /// `ActorMesh::stop` returns `Result<()>`. On the Python side that is
        /// an empty tuple, not `None`: pyo3 defines `IntoPyObject for ()` as
        /// `PyTuple::empty`. Without this method the trait default returns
        /// `PyNotImplementedError` synchronously, and a queued caller's `?`
        /// would short-circuit before any inner work ran.
        fn stop(
            &self,
            _instance: &PyInstance,
            _reason: String,
        ) -> PyResult<BoxFuture<'static, PyResult<()>>> {
            let ops = self.ops.stop.clone();
            Ok(async move {
                ops.run().await?;
                Ok(())
            }
            .boxed())
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

    /// Drive a native future under a bound. Returns `None` on timeout so a
    /// stranded `Shared` fails a named assertion rather than blocking forever
    /// and taking the whole test binary with it.
    fn drive_bounded<T>(future: BoxFuture<'static, PyResult<T>>) -> Option<T> {
        get_tokio_runtime()
            .block_on(async { tokio::time::timeout(WAIT, future).await })
            .ok()
            .map(|resolved| resolved.expect("the future should resolve"))
    }

    fn bounded_name(future: BoxFuture<'static, PyResult<String>>) -> Option<String> {
        drive_bounded(future)
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
        /// accept.
        fn join(&mut self) -> bool {
            let Some(driver) = self.driver.as_mut() else {
                return false;
            };
            let outcome =
                get_tokio_runtime().block_on(async { tokio::time::timeout(WAIT, driver).await });
            match outcome {
                // Only a normal return counts. A panicked or aborted task
                // yields `Ok(Err(JoinError))`, which an outer `is_ok()` alone
                // would accept.
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
    // Nothing polls the controlled root before the reducer commits to its
    // blocking wait, so the root's first poll is the helper thread's signal to
    // release creation. Returning the pickle before that release fails.
    //
    // A plain `#[test]`: the reducer enters `signal_safe_block_on` for
    // a pending value, and calling that from inside a Tokio runtime
    // characterizes nested-runtime failure instead of bare-pickle behavior.
    #[test]
    fn pending_bare_actor_mesh_pickle_waits_for_release() {
        pyo3::Python::initialize();

        let (entered_tx, entered_rx) = signal();
        let (gate, release) = Gate::new(entered_tx);
        let (queue, driver) = test_queue();
        // No init item is pushed: the reducer's blocking wait is the only driver.
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
        // did not return before release permission. That the root's first poll
        // happens inside `signal_safe_block_on` is source-grounded, not
        // established here.
        let phase = Arc::new(AtomicU8::new(PHASE_RUNNING));
        let helper_gate = gate.clone();
        let helper_phase = phase.clone();
        let helper = std::thread::spawn(move || {
            let acknowledged = entered_rx.recv_timeout(WAIT).is_ok();
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
            acknowledged,
            "the reducer's blocking wait must poll the controlled root"
        );
        assert!(
            release_won,
            "pickle.dumps must not return before release permission is granted"
        );
        assert_eq!(
            restored, CONTROLLED_MARKER,
            "the resolved controlled reducer must have run after the blocking branch"
        );
        assert!(
            joined,
            "the queue driver must return once its senders close"
        );
    }

    /// Install the real pickle bindings in the native test binary without
    /// replacing any modules an earlier test installed.
    fn install_pickle_test_module(py: Python<'_>) -> PyResult<()> {
        let modules = py
            .import("sys")?
            .getattr("modules")?
            .cast_into::<PyDict>()?;
        let mut path = String::new();
        let mut parent: Option<Bound<'_, PyModule>> = None;
        for part in "monarch._rust_bindings.monarch_hyperactor.pickle".split('.') {
            if !path.is_empty() {
                path.push('.');
            }
            path.push_str(part);
            let module = match modules.get_item(&path)? {
                Some(existing) => existing.cast_into::<PyModule>()?,
                None => {
                    let module = PyModule::new(py, &path)?;
                    modules.set_item(&path, &module)?;
                    module
                }
            };
            if let Some(parent) = &parent
                && parent.getattr(part).is_err()
            {
                parent.setattr(part, &module)?;
            }
            parent = Some(module);
        }
        let module = parent.expect("the module path has components");
        if module.getattr("pop_mesh_reference").is_err() {
            crate::pickle::register_python_bindings(&module)?;
        }
        Ok(())
    }

    /// Reserve through the real pending reducer and retain its fill for a
    /// directly driven resolve. The tuple must contain the slot-pop sentinel,
    /// rather than an observer hidden in its arguments.
    fn pending_reduce(py: Python<'_>, mesh: &dyn ActorMeshProtocol) -> PicklingState {
        install_pickle_test_module(py).expect("the pickle bindings should install");
        let (state, (callable, args)) = PicklingState::collect_mesh_references_for_test(|| {
            mesh.__reduce__(py).expect("__reduce__ should succeed")
        });
        let expected = py
            .import("monarch._rust_bindings.monarch_hyperactor.pickle")
            .expect("the pickle module should import")
            .getattr("pop_mesh_reference")
            .expect("the slot-pop sentinel should resolve");
        assert!(
            callable.is(&expected),
            "a reserved pending mesh must reduce through the real slot-pop sentinel"
        );
        let args = args
            .cast_into::<PyTuple>()
            .expect("the reducer arguments should be a tuple");
        assert!(args.is_empty(), "a slot pop takes no observer argument");
        state
    }

    // Refusal must happen before the blocking helper or any poll of creation,
    // rather than panic inside a nested Runtime::block_on.
    #[tokio::test]
    async fn pending_bare_actor_mesh_pickle_refuses_on_tokio_before_polling() {
        pyo3::Python::initialize();
        let (mesh, _gate, entered, mut teardown) =
            controlled_fixture(ControlledOps::default(), &[]);
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let error = mesh
                .__reduce__(py)
                .expect_err("pending bare pickle must refuse on Tokio");
            assert!(error.is_instance_of::<WouldBlockRuntime>(py));
        });
        let undriven = matches!(
            entered.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        drop(mesh);
        let joined = teardown.join_async().await;
        assert!(undriven, "refusal must not poll the mesh creation future");
        assert!(joined, "the unused queue driver must exit");
    }

    // The first creation cannot finish until the second is polled. A serial
    // fill driver would time out, not merely return a different slot order.
    #[tokio::test]
    async fn reserved_actor_fills_are_polled_concurrently_and_keep_their_slots() {
        pyo3::Python::initialize();
        let first_ref = managed_mesh_ref(&fixture_mesh_id());
        let second_ref = data_mesh_ref(&fixture_mesh_id());
        let (release, waiting) = tokio::sync::oneshot::channel();
        let first_result = first_ref.clone();
        let second_result = second_ref.clone();
        let (queue, driver) = test_queue();
        let (unused_entered, _) = signal();
        let (gate, _) = Gate::new(unused_entered);
        let mut teardown = Teardown::new(gate, driver);
        let first = AsyncActorMesh::new(
            queue.clone(),
            true,
            async move {
                waiting
                    .await
                    .expect("the second fill must release the first");
                Ok::<Arc<dyn SupervisableActorMesh>, ClonePyErr>(Arc::new(
                    PythonActorMeshImpl::new_ref(first_result),
                ))
            }
            .boxed()
            .shared(),
        );
        let second = AsyncActorMesh::new(
            queue,
            true,
            async move {
                release.send(()).expect("the first fill is still waiting");
                Ok::<Arc<dyn SupervisableActorMesh>, ClonePyErr>(Arc::new(
                    PythonActorMeshImpl::new_ref(second_result),
                ))
            }
            .boxed()
            .shared(),
        );
        let state = monarch_with_gil_blocking(GilSite::Test, |py| {
            install_pickle_test_module(py).expect("the pickle bindings should install");
            PicklingState::collect_mesh_references_for_test(|| {
                first
                    .__reduce__(py)
                    .expect("the first mesh should reserve a slot");
                second
                    .__reduce__(py)
                    .expect("the second mesh should reserve a slot");
            })
            .0
        });
        let message = PendingMessage::new(PythonMessageKind::Result { rank: None }, state);
        let resolved = tokio::time::timeout(WAIT, message.resolve()).await;
        drop(first);
        drop(second);
        let joined = teardown.join_async().await;
        let resolved = resolved
            .expect("both fills must be polled concurrently")
            .expect("both actor fills should resolve");
        assert_eq!(
            resolved.refs,
            vec![
                MeshRef::Actor(Box::new(first_ref)),
                MeshRef::Actor(Box::new(second_ref)),
            ]
        );
        assert!(joined, "the queue driver must exit");
    }

    // Dropping a pending fill's resolution does not cancel initialization.
    // The root keeps its own queued initializer, so
    // it completes on its own and stays usable.
    //
    // The completion signal is emitted by the queued initializer *after* its
    // `f.await` returns, and is awaited before any replacement observer is
    // created. Without that ordering the fresh `initialized()` could be what
    // drove initialization and the test would prove nothing.
    #[test]
    fn dropped_root_observers_do_not_cancel_initialization() {
        pyo3::Python::initialize();

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

        let state = monarch_with_gil_blocking(GilSite::Test, |py| pending_reduce(py, &mesh));
        let mut resolve = Box::pin(state.resolve());
        // The first poll of `resolve` polls every fill, so a pending result
        // means the actor fill has started waiting on creation.
        let pending = get_tokio_runtime()
            .block_on(async { futures::poll!(&mut resolve) })
            .is_pending();
        // Destroys the actor fill here, before the creation gate opens.
        drop(resolve);

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

        assert!(pending, "resolution must still be pending when dropped");
        assert!(
            completed,
            "the queue-held root initializer must complete on its own after release"
        );
        let readiness = readiness.flatten().expect("readiness must resolve in time");
        assert!(
            readiness.is_none(),
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
    // initializer, but a later `stop` or cast enqueues an await, and name,
    // readiness and `supervision_event` await the derived `Shared` directly. These assertions
    // therefore prove dormancy only after the named observers are discarded and
    // while no other mesh operation occurs. They do not prove that an
    // `initialized` observer is the only thing that can ever drive a slice.
    #[test]
    fn dropped_slice_observers_leave_the_slice_reobservable() {
        pyo3::Python::initialize();

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

        let state =
            monarch_with_gil_blocking(GilSite::Test, |py| pending_reduce(py, slice.as_ref()));
        let mut resolve = Box::pin(state.resolve());
        let pending = get_tokio_runtime()
            .block_on(async { futures::poll!(&mut resolve) })
            .is_pending();
        drop(resolve);

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
            pending,
            "slice resolution must still be pending when dropped"
        );
        assert!(completed, "the root must complete on its own after release");
        assert_eq!(
            dormant, 0,
            "the slice has no queue item, so discarding its observers leaves it undriven"
        );
        let readiness = readiness.flatten().expect("readiness must resolve in time");
        assert!(
            readiness.is_none(),
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

    // Name and stop completion and abandonment

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

    /// Drive the producer as far as the inner operation, reporting what
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
    /// the root future until a queued stop or a name Handle awaits it. With the
    /// root gate still shut the producer parks there and never reaches the inner
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

    /// Drop the returned completion future or Handle. For a queued stop,
    /// dropping the Rust future immediately closes its oneshot receiver.
    fn discard_observer<T>(observer: PyResult<T>) -> PyResult<()> {
        observer.map(drop)
    }

    /// Everything observing a Handle can produce, as data rather than as a
    /// panic. Nothing here asserts: a subcase collects an outcome, runs its
    /// cleanup, and only then decides whether it is the expected one.
    enum DriveOutcome {
        /// The call returned an error instead of an observer.
        NotConstructed(PyErr),
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
                DriveOutcome::TimedOut => panic!("{context}: the observer did not resolve"),
            }
        }
    }

    /// Observe a Handle under a bound without asserting before cleanup.
    fn drive_evidence(handle: PyResult<PyHandle>) -> DriveOutcome {
        get_tokio_runtime().block_on(drive_evidence_async(handle))
    }

    /// `drive_evidence` for a caller already inside a runtime.
    async fn drive_evidence_async(handle: PyResult<PyHandle>) -> DriveOutcome {
        let handle = match handle {
            Ok(handle) => handle,
            Err(err) => return DriveOutcome::NotConstructed(err),
        };
        match tokio::time::timeout(WAIT, handle.wait_future()).await {
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

    // Name waits directly on the mesh, outside the cast/stop queue. A native
    // Handle drives that wait even when its last observer is dropped. A
    // retained observer receives the inner value or error verbatim.
    //
    // Every subcase collects evidence, opens its gates, closes the queue and
    // joins the driver before asserting. Name completion is acknowledged
    // separately: a queue barrier cannot prove off-queue work has finished.
    //
    // A plain `#[test]`: no subcase needs a `PyInstance`, so nothing here
    // requires a per-test runtime and the sync helpers apply.
    #[test]
    fn async_name_survives_a_dropped_observer_and_leaves_queue_usable() {
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

            let handle = PyHandle::spawn(<AsyncActorMesh as ActorMeshProtocol>::name(&mesh));
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let outcome = drive_evidence(Ok(handle));

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join();

            assert!(
                reached_root,
                "the name producer should reach root initialization"
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
        {
            let name_ops = Arc::new(OpControl::immediate(OpOutcome::Fail(CONTROLLED_FAILURE)));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: name_ops.clone(),
                    stop: Arc::default(),
                },
                &[],
            );

            let handle = PyHandle::spawn(<AsyncActorMesh as ActorMeshProtocol>::name(&mesh));
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let outcome = drive_evidence(Ok(handle));

            teardown.open_all();
            drop(mesh);
            let joined = teardown.join();

            assert!(
                reached_root,
                "the name producer should reach root initialization"
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

        // Last Handle dropped after completion: the result need not be read.
        {
            let name_ops = Arc::new(OpControl::immediate(OpOutcome::Succeed));
            let (mesh, root_gate, root_entered, mut teardown) = controlled_fixture(
                ControlledOps {
                    name: name_ops.clone(),
                    stop: Arc::default(),
                },
                &[],
            );
            let (completed_tx, completed_rx) = signal();
            let future = mesh.name();
            let handle = PyHandle::spawn(async move {
                let result = future.await;
                let _ = completed_tx.try_send(());
                result
            });
            let reached_root = awaited(&root_entered);
            root_gate.open();
            let completed = awaited(&completed_rx);
            drop(handle);
            let usable = queue_barrier(&mesh);
            teardown.open_all();
            drop(mesh);
            let joined = teardown.join();

            assert!(reached_root, "the name producer must reach initialization");
            assert!(completed, "name must complete before its Handle is dropped");
            assert_eq!(name_ops.completions(), 1);
            assert!(usable, "later queue work must still run");
            assert!(joined, "the queue driver must return normally");
        }

        // Last Handle dropped while name is pending. The queue can serve work
        // before the name gate opens, and name still completes after release.
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
            let (completed_tx, completed_rx) = signal();
            let future = mesh.name();
            let handle = PyHandle::spawn(async move {
                let result = future.await;
                let _ = completed_tx.try_send(());
                result
            });
            let (reached_root, entered_op) =
                reach_inner_operation(&root_entered, &root_gate, &op_entered);
            drop(handle);
            let usable = queue_barrier(&mesh);
            teardown.open_all();
            let completed = awaited(&completed_rx);
            drop(mesh);
            let joined = teardown.join();

            assert!(reached_root, "the name producer must reach initialization");
            assert!(
                entered_op,
                "name must be pending when its Handle is dropped"
            );
            assert!(usable, "name must not hold up the cast/stop queue");
            assert!(completed, "unobserved name must complete after release");
            assert_eq!(name_ops.completions(), 1);
            assert!(joined, "the queue driver must return normally");
        }
    }

    // Stop keeps its queue position and its pre-enqueue instance clone under
    // `GilSite::Stop`. A retained Handle receives the inner error verbatim or
    // the current owned-stop result: an empty tuple, since pyo3 converts Rust
    // `()` through `PyTuple::empty`.
    //
    // Dropping the returned Rust future closes the oneshot receiver, but the
    // queued stop must still finish and permit later queue work. Test that
    // inner boundary directly; a dropped Handle alone would leave its producer
    // holding the receiver and would not exercise the failed `tx.send`.
    //
    // A `#[tokio::test]`: `stop` needs a real `PyInstance`, and `actor_instance`
    // spawns detached introspect tasks that the per-test runtime reaps. That in
    // turn rules out the `block_on` helpers, so this test uses the async
    // siblings throughout.
    #[tokio::test]
    async fn async_stop_survives_a_dropped_observer_and_leaves_queue_usable() {
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
            let outcome = drive_evidence_async(task.map(PyHandle::spawn)).await;

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
            let outcome = drive_evidence_async(task.map(PyHandle::spawn)).await;

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

        // Completion future dropped after the send. The serial queue barrier
        // proves the stop closure, including its send, has finished.
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
            let sent = queue_barrier(&mesh);
            drop(mesh);
            let joined = teardown.join_async().await;

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
            assert!(
                sent,
                "later queue work must run after the closed-receiver send"
            );
            assert!(joined, "the queue driver must return normally");
            assert_eq!(
                stop_ops.completions(),
                1,
                "the inner stop must finish exactly once despite losing its observer"
            );
        }
    }

    // Asking a `PythonActorMeshImpl::Ref` to stop is rejected by the inner arm
    // itself, with no future returned: there is nothing to observe, nothing to
    // drop, and no queue involved.
    //
    // The assertions cover what is returned. "No future was returned" is read
    // off the source -- the `Ref` arm returns before constructing its future --
    // and is not proven here, because an `Err` return
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
    async fn actor_mesh_impl_ref_stop_rejects_before_returning_a_future() {
        pyo3::Python::initialize();
        // Built outside the assertion boundary, so a fixture failure cannot
        // satisfy the rejection assertion below.
        let mesh_impl = PythonActorMeshImpl::new_ref(data_mesh_ref(&fixture_mesh_id()));
        let instance = isolated_py_instance("ref_stop_characterization_client");

        let rejected = <PythonActorMeshImpl as ActorMeshProtocol>::stop(
            &mesh_impl,
            &instance,
            "characterization".to_string(),
        );

        let Err(err) = rejected else {
            panic!("the Ref arm must reject rather than return a future");
        };
        assert_exact_error::<PyNotImplementedError>(&err, REF_STOP_REJECTION, "Ref stop rejection");
    }

    // A pending mesh offers no reference until its creation future has been
    // computed, and then offers the created mesh's reference or its error.
    #[tokio::test]
    async fn ready_mesh_ref_follows_creation() {
        pyo3::Python::initialize();
        let (queue, driver) = test_queue();
        let (unused_entered, _) = signal();
        let (gate, _) = Gate::new(unused_entered);
        let mut teardown = Teardown::new(gate, driver);

        let pending = AsyncActorMesh::new(
            queue.clone(),
            true,
            future::pending::<Result<Arc<dyn SupervisableActorMesh>, ClonePyErr>>()
                .boxed()
                .shared(),
        );
        let still_creating = pending
            .ready_mesh_ref()
            .expect_err("a mesh still being created has no reference");

        let created_ref = managed_mesh_ref(&fixture_mesh_id());
        let created_result = created_ref.clone();
        let created = AsyncActorMesh::new(
            queue.clone(),
            true,
            async move {
                Ok::<Arc<dyn SupervisableActorMesh>, ClonePyErr>(Arc::new(
                    PythonActorMeshImpl::new_ref(created_result),
                ))
            }
            .boxed()
            .shared(),
        );
        created
            .initialized()
            .await
            .expect("creation should succeed");

        let failed = AsyncActorMesh::new(
            queue,
            true,
            future::ready(Err::<Arc<dyn SupervisableActorMesh>, ClonePyErr>(
                PyRuntimeError::new_err("creation failed").into(),
            ))
            .boxed()
            .shared(),
        );
        failed
            .initialized()
            .await
            .expect_err("creation should fail");

        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(still_creating.is_instance_of::<PyValueError>(py));
            assert_eq!(
                created
                    .ready_mesh_ref()
                    .expect("a created mesh has a reference"),
                created_ref
            );
            let error = failed
                .ready_mesh_ref()
                .expect_err("a failed creation has no reference");
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert!(error.to_string().contains("creation failed"), "{error}");
        });

        drop(pending);
        drop(created);
        drop(failed);
        assert!(teardown.join_async().await, "the queue driver must exit");
    }
}
