/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use hyperactor::Instance;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::mailbox::PortReceiver;
use hyperactor_mesh::sel;
use monarch_types::py_global;
use ndslice::Extent;
use ndslice::Selection;
use ndslice::Shape;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use serde_multipart::Part;

use crate::actor::MethodSpecifier;
use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::actor_mesh::ActorMeshProtocol;
use crate::actor_mesh::PythonActorMesh;
use crate::buffers::FrozenBuffer;
use crate::context::PyInstance;
use crate::mailbox::PyMailbox;
use crate::mailbox::PythonPortRef;
use crate::metrics::ENDPOINT_CALL_ERROR;
use crate::metrics::ENDPOINT_CALL_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_CALL_ONE_ERROR;
use crate::metrics::ENDPOINT_CALL_ONE_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_CALL_ONE_THROUGHPUT;
use crate::metrics::ENDPOINT_CALL_THROUGHPUT;
use crate::metrics::ENDPOINT_CHOOSE_ERROR;
use crate::metrics::ENDPOINT_CHOOSE_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_CHOOSE_THROUGHPUT;
use crate::metrics::ENDPOINT_STREAM_ERROR;
use crate::metrics::ENDPOINT_STREAM_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_STREAM_THROUGHPUT;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PythonTask;
use crate::shape::PyExtent;
use crate::shape::PyShape;
use crate::supervision::PySupervisor;
use crate::supervision::SupervisionError;
use crate::value_mesh::PyValueMesh;

py_global!(pickle_unflatten, "monarch._src.actor.pickle", "unflatten");
py_global!(get_context, "monarch._src.actor.actor_mesh", "context");
py_global!(
    create_endpoint_message,
    "monarch._src.actor.actor_mesh",
    "_create_endpoint_message"
);
py_global!(make_future, "monarch._src.actor.future", "Future");

/// An infinite iterator that yields the same mailbox value forever.
/// Used as local_state for unflatten, replacing Python's itertools.repeat(mailbox).
#[pyclass(
    name = "RepeatMailbox",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
struct RepeatMailbox {
    mailbox: PyObject,
}

#[pymethods]
impl RepeatMailbox {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self) -> PyObject {
        self.mailbox.clone()
    }
}

#[tracing::instrument(skip_all)]
fn unflatten<'py>(
    py: Python<'py>,
    part: &Part,
    mailbox: impl Into<PyMailbox>,
) -> PyResult<Bound<'py, PyAny>> {
    let unflatten_fn = pickle_unflatten(py);
    let mailbox: PyMailbox = mailbox.into();
    let mailbox_iter = RepeatMailbox {
        mailbox: mailbox.into_pyobject(py)?.unbind().into(),
    };

    unflatten_fn.call1((
        FrozenBuffer {
            inner: part.to_bytes(),
        }
        .into_pyobject(py)?,
        mailbox_iter,
    ))
}

/// The type of endpoint operation being performed.
///
/// Used to select the appropriate telemetry metrics for each operation type.
#[pyclass(
    name = "EndpointAdverb",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
#[derive(Clone, Copy, Debug)]
pub enum EndpointAdverb {
    Call,
    CallOne,
    Choose,
    Stream,
}

/// RAII guard for recording endpoint call telemetry.
///
/// Records latency on drop, similar to Python's `@_with_telemetry` decorator.
/// Call `mark_error()` before dropping to also record an error.
pub struct EndpointTelemetry {
    start: tokio::time::Instant,
    method_name: String,
    actor_count: usize,
    adverb: EndpointAdverb,
    error_occurred: Cell<bool>,
}

impl EndpointTelemetry {
    fn new(
        start: tokio::time::Instant,
        method_name: String,
        actor_count: usize,
        adverb: EndpointAdverb,
    ) -> Self {
        let attributes = hyperactor_telemetry::kv_pairs!(
            "method" => method_name.clone()
        );
        match adverb {
            EndpointAdverb::Call => {
                ENDPOINT_CALL_THROUGHPUT.add(1, attributes);
            }
            EndpointAdverb::CallOne => {
                ENDPOINT_CALL_ONE_THROUGHPUT.add(1, attributes);
            }
            EndpointAdverb::Choose => {
                ENDPOINT_CHOOSE_THROUGHPUT.add(1, attributes);
            }
            EndpointAdverb::Stream => {
                // Throughput already recorded once at stream creation in py_stream_collector
            }
        }

        Self {
            start,
            method_name,
            actor_count,
            adverb,
            error_occurred: Cell::new(false),
        }
    }

    fn mark_error(&self) {
        self.error_occurred.set(true);
    }
}

impl Drop for EndpointTelemetry {
    fn drop(&mut self) {
        let actor_count_str = self.actor_count.to_string();
        let attributes = hyperactor_telemetry::kv_pairs!(
            "method" => self.method_name.clone(),
            "actor_count" => actor_count_str
        );
        tracing::info!(message = "response received", method = self.method_name);

        let duration_us = self.start.elapsed().as_micros();

        match self.adverb {
            EndpointAdverb::Call => {
                ENDPOINT_CALL_LATENCY_US_HISTOGRAM.record(duration_us as f64, attributes);
            }
            EndpointAdverb::CallOne => {
                ENDPOINT_CALL_ONE_LATENCY_US_HISTOGRAM.record(duration_us as f64, attributes);
            }
            EndpointAdverb::Choose => {
                ENDPOINT_CHOOSE_LATENCY_US_HISTOGRAM.record(duration_us as f64, attributes);
            }
            EndpointAdverb::Stream => {
                ENDPOINT_STREAM_LATENCY_US_HISTOGRAM.record(duration_us as f64, attributes);
            }
        }

        if self.error_occurred.get() {
            match self.adverb {
                EndpointAdverb::Call => {
                    ENDPOINT_CALL_ERROR.add(1, attributes);
                }
                EndpointAdverb::CallOne => {
                    ENDPOINT_CALL_ONE_ERROR.add(1, attributes);
                }
                EndpointAdverb::Choose => {
                    ENDPOINT_CHOOSE_ERROR.add(1, attributes);
                }
                EndpointAdverb::Stream => {
                    ENDPOINT_STREAM_ERROR.add(1, attributes);
                }
            }
        }
    }
}

fn supervision_error_to_pyerr(err: PyErr, qualified_endpoint_name: &Option<String>) -> PyErr {
    match qualified_endpoint_name {
        Some(endpoint) => {
            Python::with_gil(|py| SupervisionError::set_endpoint_on_err(py, err, endpoint.clone()))
        }
        None => err,
    }
}

enum RaceResult {
    Message(PythonMessage),
    SupervisionError(PyErr),
    RecvError(String),
}

async fn collect_value(
    rx: &mut PortReceiver<PythonMessage>,
    supervisor: &Option<PySupervisor>,
    instance: &Instance<PythonActor>,
    qualified_endpoint_name: &Option<String>,
) -> PyResult<(Part, Option<usize>)> {
    let race_result = match supervisor {
        Some(sup) => {
            tokio::select! {
                biased;
                result = sup.next_supervision_event(instance) => {
                    match result {
                        Some(err) => RaceResult::SupervisionError(err),
                        None => {
                            match rx.recv().await {
                                Ok(msg) => RaceResult::Message(msg),
                                Err(e) => RaceResult::RecvError(e.to_string()),
                            }
                        }
                    }
                }
                msg = rx.recv() => {
                    match msg {
                        Ok(m) => RaceResult::Message(m),
                        Err(e) => RaceResult::RecvError(e.to_string()),
                    }
                }
            }
        }
        _ => match rx.recv().await {
            Ok(msg) => RaceResult::Message(msg),
            Err(e) => RaceResult::RecvError(e.to_string()),
        },
    };

    match race_result {
        RaceResult::Message(PythonMessage {
            kind: PythonMessageKind::Result { rank, .. },
            message,
            ..
        }) => Ok((message, rank)),
        RaceResult::Message(PythonMessage {
            kind: PythonMessageKind::Exception { .. },
            message,
            ..
        }) => Python::with_gil(|py| {
            Err(PyErr::from_value(unflatten(
                py,
                &message,
                instance.mailbox_for_py().clone(),
            )?))
        }),
        RaceResult::Message(msg) => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unexpected message kind {:?}",
            msg.kind
        ))),
        RaceResult::RecvError(e) => Err(pyo3::exceptions::PyEOFError::new_err(format!(
            "Port closed: {}",
            e
        ))),
        RaceResult::SupervisionError(err) => {
            Err(supervision_error_to_pyerr(err, qualified_endpoint_name))
        }
    }
}

async fn collect_valuemesh(
    extent: Extent,
    mut rx: PortReceiver<PythonMessage>,
    method_name: String,
    supervisor: Option<PySupervisor>,
    instance: &Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
    adverb: EndpointAdverb,
) -> PyResult<Py<PyAny>> {
    let start = RealClock.now();

    let expected_count = extent.num_ranks();

    let telemetry = EndpointTelemetry::new(start, method_name.clone(), expected_count, adverb);

    let mut results: Vec<Option<Part>> = vec![None; expected_count];

    for _ in 0..expected_count {
        match collect_value(&mut rx, &supervisor, instance, &qualified_endpoint_name).await {
            Ok((message, rank)) => {
                results[rank.expect("RankedPort receiver got a message without a rank")] =
                    Some(message);
            }
            Err(e) => {
                telemetry.mark_error();
                return Err(e);
            }
        }
    }

    Python::with_gil(|py| {
        Ok(PyValueMesh::build_dense_from_extent(
            &extent,
            results
                .into_iter()
                .map(|msg| {
                    let m = msg.expect("all responses should be filled");
                    unflatten(py, &m, instance.mailbox_for_py().clone()).map(|obj| obj.unbind())
                })
                .collect::<PyResult<_>>()?,
        )?
        .into_pyobject(py)?
        .into_any()
        .unbind())
    })
}

fn valuemesh_collector(
    extent: Extent,
    receiver: PortReceiver<PythonMessage>,
    method_name: String,
    supervisor: Option<PySupervisor>,
    instance: Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
    adverb: EndpointAdverb,
) -> PyResult<PyPythonTask> {
    Ok(PythonTask::new(async move {
        collect_valuemesh(
            extent,
            receiver,
            method_name,
            supervisor,
            &instance,
            qualified_endpoint_name,
            adverb,
        )
        .await
    })?
    .into())
}

/// Create a task that opens a port and processes multiple responses into a ValueMesh.
///
/// Returns a tuple of (PortRef, PythonTask) where:
/// - PortRef: The port reference to send to actors for responses
/// - PythonTask: A task that awaits all responses and returns a ValueMesh
#[pyfunction]
#[pyo3(name = "valuemesh_collector")]
fn py_valuemesh_collector(
    extent: PyExtent,
    method_name: String,
    instance: PyInstance,
    adverb: EndpointAdverb,
) -> PyResult<(PythonPortRef, PyPythonTask)> {
    let instance = instance.into_instance();
    let (p, r) = instance.open_port::<PythonMessage>();

    let task = valuemesh_collector(extent.into(), r, method_name, None, instance, None, adverb)?;
    Ok((PythonPortRef { inner: p.bind() }, task))
}

fn value_collector(
    mut receiver: PortReceiver<PythonMessage>,
    method_name: String,
    supervisor: Option<PySupervisor>,
    instance: Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
    adverb: EndpointAdverb,
) -> PyResult<PyPythonTask> {
    Ok(PythonTask::new(async move {
        let start = RealClock.now();

        let telemetry = EndpointTelemetry::new(start, method_name.clone(), 1, adverb);

        match collect_value(
            &mut receiver,
            &supervisor,
            &instance,
            &qualified_endpoint_name,
        )
        .await
        {
            Ok((message, _)) => Python::with_gil(|py| {
                unflatten(py, &message, instance.mailbox_for_py().clone()).map(|obj| obj.unbind())
            }),
            Err(e) => {
                telemetry.mark_error();
                Err(e)
            }
        }
    })?
    .into())
}

/// Create a task that opens a port and processes a single response.
///
/// Returns a tuple of (PortRef, PythonTask) where:
/// - PortRef: The port reference to send to an actor for the response
/// - PythonTask: A task that awaits the single response and returns the unpickled value
#[pyfunction]
#[pyo3(name = "value_collector")]
fn py_value_collector(
    method_name: String,
    instance: PyInstance,
    adverb: EndpointAdverb,
) -> PyResult<(PythonPortRef, PyPythonTask)> {
    let instance = instance.into_instance();
    let (p, r) = instance.open_port::<PythonMessage>();

    let task = value_collector(r, method_name, None, instance, None, adverb)?;
    Ok((PythonPortRef { inner: p.bind() }, task))
}

/// A streaming iterator that yields futures for each response from actors.
///
/// Implements Python's iterator protocol (`__iter__`/`__next__`) to yield
/// `Future` objects that resolve to individual actor responses.
#[pyclass(
    name = "ValueStream",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
pub struct PyValueStream {
    receiver: Arc<tokio::sync::Mutex<PortReceiver<PythonMessage>>>,
    supervisor: Option<PySupervisor>,
    instance: Instance<PythonActor>,
    remaining: AtomicUsize,
    method_name: String,
    qualified_endpoint_name: Option<String>,
    start: tokio::time::Instant,
    actor_count: usize,
    future_class: PyObject,
}

#[pymethods]
impl PyValueStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Ok(None);
        }
        self.remaining.store(remaining - 1, Ordering::Relaxed);

        let receiver = self.receiver.clone();
        let supervisor = self.supervisor.clone();
        let instance = self.instance.clone_for_py();
        let qualified_endpoint_name = self.qualified_endpoint_name.clone();
        let start = self.start;
        let method_name = self.method_name.clone();
        let actor_count = self.actor_count;

        let task: PyPythonTask = PythonTask::new(async move {
            let telemetry =
                EndpointTelemetry::new(start, method_name, actor_count, EndpointAdverb::Stream);

            let mut rx_guard = receiver.lock().await;

            match collect_value(
                &mut rx_guard,
                &supervisor,
                &instance,
                &qualified_endpoint_name,
            )
            .await
            {
                Ok((message, _)) => Python::with_gil(|py| {
                    unflatten(py, &message, instance.mailbox_for_py().clone())
                        .map(|obj| obj.unbind())
                }),
                Err(e) => {
                    telemetry.mark_error();
                    Err(e)
                }
            }
        })?
        .into();

        let kwargs = PyDict::new(py);
        kwargs.set_item("coro", task)?;
        let future = self.future_class.call(py, (), Some(&kwargs))?;
        Ok(Some(future))
    }
}

fn stream_collector(
    extent: Extent,
    receiver: PortReceiver<PythonMessage>,
    method_name: String,
    supervisor: Option<PySupervisor>,
    instance: Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
    future_class: PyObject,
) -> PyValueStream {
    let actor_count = extent.num_ranks();
    let start = RealClock.now();

    let attributes = hyperactor_telemetry::kv_pairs!(
        "method" => method_name.clone()
    );
    ENDPOINT_STREAM_THROUGHPUT.add(1, attributes);

    PyValueStream {
        receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
        supervisor,
        instance,
        remaining: AtomicUsize::new(actor_count),
        method_name,
        qualified_endpoint_name,
        start,
        actor_count,
        future_class,
    }
}

/// Create a streaming iterator for processing multiple responses incrementally.
///
/// Returns a tuple of (PortRef, ValueStream) where:
/// - PortRef: The port reference to send to actors for responses
/// - ValueStream: An iterator that yields Future objects for each response
#[pyfunction]
#[pyo3(name = "stream_collector")]
fn py_stream_collector(
    extent: PyExtent,
    method_name: String,
    instance: PyInstance,
    py: Python<'_>,
) -> PyResult<(PythonPortRef, PyValueStream)> {
    let instance = instance.into_instance();
    let (p, r) = instance.open_port::<PythonMessage>();

    let stream = stream_collector(
        extent.into(),
        r,
        method_name,
        None,
        instance,
        None,
        make_future(py).unbind(),
    );
    Ok((PythonPortRef { inner: p.bind() }, stream))
}

/// Trait for types that can send endpoint messages to targets.
pub(crate) trait EndpointBackend: Send + Sync {
    /// Send a message to targets with the given selection.
    fn send(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()>;

    /// Get the extent/shape of this endpoint's targets.
    fn extent(&self) -> Extent;

    /// Get a supervisor for health monitoring.
    fn supervisor(&self) -> PySupervisor;
}

/// Backend that sends to actors via ActorMeshProtocol.
pub(crate) struct ActorMeshBackend {
    inner: Arc<dyn ActorMeshProtocol>,
    shape: Shape,
}

impl ActorMeshBackend {
    pub fn new(inner: Arc<dyn ActorMeshProtocol>, shape: Shape) -> Self {
        Self { inner, shape }
    }
}

impl EndpointBackend for ActorMeshBackend {
    fn send(
        &self,
        message: PythonMessage,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        self.inner.cast(message, selection, instance)
    }

    fn extent(&self) -> Extent {
        self.shape.extent()
    }

    fn supervisor(&self) -> PySupervisor {
        PythonActorMesh::from_impl(self.inner.clone()).as_supervisor()
    }
}

fn get_current_instance(py: Python<'_>) -> PyResult<Instance<PythonActor>> {
    let context = get_context(py).call0()?;
    let py_instance: PyRef<PyInstance> = context.getattr("actor_instance")?.extract()?;
    Ok(py_instance.clone().into_instance())
}

fn open_response_port(
    instance: &Instance<PythonActor>,
) -> (PythonPortRef, PortReceiver<PythonMessage>) {
    let (p, receiver) = instance.mailbox_for_py().open_port::<PythonMessage>();
    (PythonPortRef { inner: p.bind() }, receiver)
}

fn wrap_in_future(py: Python<'_>, task: PyPythonTask) -> PyResult<PyObject> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("coro", task)?;
    let future = make_future(py).call((), Some(&kwargs))?;
    Ok(future.unbind())
}

#[pyclass(
    name = "ActorEndpoint",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
pub struct ActorEndpoint {
    backend: ActorMeshBackend,
    method: MethodSpecifier,
    mesh_name: String,
    signature: Option<PyObject>,
    proc_mesh: Option<PyObject>,
}

impl ActorEndpoint {
    fn create_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<&PythonPortRef>,
    ) -> PyResult<PythonMessage> {
        let port_ref_py: PyObject = match port_ref {
            Some(pr) => pr.clone().into_pyobject(py)?.unbind().into(),
            None => py.None(),
        };

        create_endpoint_message(py)
            .call1((
                self.method.clone(),
                self.signature
                    .as_ref()
                    .map_or_else(|| py.None(), |s| s.clone_ref(py)),
                args,
                kwargs
                    .map_or_else(|| PyDict::new(py), |d| d.clone())
                    .into_any(),
                port_ref_py,
                self.proc_mesh
                    .as_ref()
                    .map_or_else(|| py.None(), |p| p.clone_ref(py)),
            ))?
            .extract()
    }
}

#[pymethods]
impl ActorEndpoint {
    /// Create a new ActorEndpoint.
    ///
    /// Args:
    ///     actor_mesh: The ActorMeshProtocol to send messages to
    ///     method: The MethodSpecifier for the endpoint
    ///     shape: The shape of the actor mesh
    ///     mesh_name: The name of the mesh
    ///     signature: Optional Python inspect.Signature for argument validation
    ///     proc_mesh: Optional ProcMesh reference for pending pickle support
    #[new]
    #[pyo3(signature = (actor_mesh, method, shape, mesh_name, signature=None, proc_mesh=None))]
    fn new(
        actor_mesh: PythonActorMesh,
        method: MethodSpecifier,
        shape: PyShape,
        mesh_name: String,
        signature: Option<PyObject>,
        proc_mesh: Option<PyObject>,
    ) -> Self {
        Self {
            backend: ActorMeshBackend::new(actor_mesh.get_inner(), shape.get_inner().clone()),
            method,
            mesh_name,
            signature,
            proc_mesh,
        }
    }

    fn qualified_endpoint_name(&self) -> String {
        format!("{}.{}()", self.mesh_name, self.method.name())
    }

    /// Call the endpoint on all actors and collect all responses into a ValueMesh.
    ///
    /// Returns a Future that resolves to a ValueMesh containing all responses.
    #[pyo3(signature = (*args, **kwargs))]
    fn call<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<PyObject> {
        let instance = get_current_instance(py)?;
        let (port_ref, receiver) = open_response_port(&instance);

        let message = self.create_message(py, args, kwargs, Some(&port_ref))?;

        self.backend.send(message, sel!(*), &instance)?;

        let task = valuemesh_collector(
            self.backend.extent(),
            receiver,
            self.method.name().to_string(),
            Some(self.backend.supervisor()),
            instance.clone_for_py(),
            Some(self.qualified_endpoint_name()),
            EndpointAdverb::Call,
        )?;

        wrap_in_future(py, task)
    }

    /// Load balanced sends a message to one chosen actor and awaits a result.
    ///
    /// Load balanced RPC-style entrypoint for request/response messaging.
    ///
    /// Returns a Future that resolves to the single response.
    #[pyo3(signature = (*args, **kwargs))]
    fn choose<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<PyObject> {
        let instance = get_current_instance(py)?;
        let (port_ref, receiver) = open_response_port(&instance);

        let message = self.create_message(py, args, kwargs, Some(&port_ref))?;

        self.backend.send(message, sel!(?), &instance)?;

        let task = value_collector(
            receiver,
            self.method.name().to_string(),
            Some(self.backend.supervisor()),
            instance.clone_for_py(),
            Some(self.qualified_endpoint_name()),
            EndpointAdverb::Choose,
        )?;

        wrap_in_future(py, task)
    }

    /// Call the endpoint on exactly one actor (the mesh must have exactly one actor).
    ///
    /// Returns a Future that resolves to the single response.
    /// Raises an error if the mesh has more than one actor.
    #[pyo3(signature = (*args, **kwargs))]
    fn call_one<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<PyObject> {
        let extent = self.backend.extent();
        let num_ranks = extent.num_ranks();
        if num_ranks != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "call_one requires exactly 1 actor, but mesh has {}",
                num_ranks
            )));
        }

        let instance = get_current_instance(py)?;
        let (port_ref, receiver) = open_response_port(&instance);

        let message = self.create_message(py, args, kwargs, Some(&port_ref))?;

        self.backend.send(message, sel!(*), &instance)?;

        let task = value_collector(
            receiver,
            self.method.name().to_string(),
            Some(self.backend.supervisor()),
            instance.clone_for_py(),
            Some(self.qualified_endpoint_name()),
            EndpointAdverb::CallOne,
        )?;

        wrap_in_future(py, task)
    }

    /// Call the endpoint on all actors and return an iterator of Futures.
    ///
    /// Returns a ValueStream that yields Futures for each response as they arrive.
    #[pyo3(signature = (*args, **kwargs))]
    fn stream<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<PyObject> {
        let instance = get_current_instance(py)?;
        let (port_ref, receiver) = open_response_port(&instance);

        let message = self.create_message(py, args, kwargs, Some(&port_ref))?;

        self.backend.send(message, sel!(*), &instance)?;

        let stream = stream_collector(
            self.backend.extent(),
            receiver,
            self.method.name().to_string(),
            Some(self.backend.supervisor()),
            instance.clone_for_py(),
            Some(self.qualified_endpoint_name()),
            make_future(py).unbind(),
        );

        Ok(stream.into_pyobject(py)?.unbind().into())
    }

    /// Send a message to all actors without waiting for responses (fire-and-forget).
    #[pyo3(signature = (*args, **kwargs))]
    fn broadcast<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<()> {
        let instance = get_current_instance(py)?;

        let message = self.create_message(py, args, kwargs, None)?;

        self.backend.send(message, sel!(*), &instance)?;

        Ok(())
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(py_valuemesh_collector, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.endpoint",
    )?;
    module.add_function(f)?;

    let f = wrap_pyfunction!(py_value_collector, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.endpoint",
    )?;
    module.add_function(f)?;

    let f = wrap_pyfunction!(py_stream_collector, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.endpoint",
    )?;
    module.add_function(f)?;

    module.add_class::<PyValueStream>()?;
    module.add_class::<EndpointAdverb>()?;
    module.add_class::<RepeatMailbox>()?;
    module.add_class::<ActorEndpoint>()?;

    Ok(())
}
