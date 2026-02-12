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
use monarch_types::py_module_add_function;
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
use crate::actor_mesh::PythonActorMesh;
use crate::actor_mesh::to_hy_sel;
use crate::buffers::FrozenBuffer;
use crate::context::PyInstance;
use crate::mailbox::PyMailbox;
use crate::mailbox::PythonPortRef;
use crate::metrics::ENDPOINT_BROADCAST_ERROR;
use crate::metrics::ENDPOINT_BROADCAST_THROUGHPUT;
use crate::metrics::ENDPOINT_CALL_ERROR;
use crate::metrics::ENDPOINT_CALL_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_CALL_ONE_ERROR;
use crate::metrics::ENDPOINT_CALL_ONE_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_CALL_ONE_THROUGHPUT;
use crate::metrics::ENDPOINT_CALL_THROUGHPUT;
use crate::metrics::ENDPOINT_CHOOSE_ERROR;
use crate::metrics::ENDPOINT_CHOOSE_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_CHOOSE_THROUGHPUT;
use crate::metrics::ENDPOINT_MESSAGE_SIZE_HISTOGRAM;
use crate::metrics::ENDPOINT_STREAM_ERROR;
use crate::metrics::ENDPOINT_STREAM_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_STREAM_THROUGHPUT;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PythonTask;
use crate::shape::PyExtent;
use crate::shape::PyShape;
use crate::supervision::PySupervisionMonitor;
use crate::supervision::SupervisionError;
use crate::value_mesh::PyValueMesh;

py_global!(pickle_unflatten, "monarch._src.actor.pickle", "unflatten");
py_global!(get_context, "monarch._src.actor.actor_mesh", "context");
py_global!(
    create_endpoint_message,
    "monarch._src.actor.actor_mesh",
    "_create_endpoint_message"
);
py_global!(
    dispatch_actor_rref,
    "monarch._src.actor.actor_mesh",
    "_dispatch_actor_rref"
);
py_global!(make_future, "monarch._src.actor.future", "Future");

/// An infinite iterator that yields the same mailbox value forever.
/// Used as local_state for unflatten, replacing Python's itertools.repeat(mailbox).
#[pyclass(
    name = "RepeatMailbox",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
struct RepeatMailbox {
    mailbox: Py<PyAny>,
}

#[pymethods]
impl RepeatMailbox {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self) -> Py<PyAny> {
        self.mailbox.clone()
    }
}

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
#[derive(Clone, Copy, Debug)]
enum EndpointAdverb {
    Call,
    CallOne,
    Choose,
    Stream,
}

fn to_endpoint_adverb(adverb: &str) -> PyResult<EndpointAdverb> {
    match adverb {
        "call_one" => Ok(EndpointAdverb::CallOne),
        "choose" => Ok(EndpointAdverb::Choose),
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "only literals ['call_one', 'choose'] expected to be received from Python, got: {}",
            adverb
        ))),
    }
}

/// RAII guard for recording endpoint call telemetry.
///
/// Records latency on drop, similar to Python's `@_with_telemetry` decorator.
/// Call `mark_error()` before dropping to also record an error.
pub struct RecordEndpointGuard {
    start: tokio::time::Instant,
    method_name: String,
    actor_count: usize,
    adverb: EndpointAdverb,
    error_occurred: Cell<bool>,
}

impl RecordEndpointGuard {
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

impl Drop for RecordEndpointGuard {
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
            Python::attach(|py| SupervisionError::set_endpoint_on_err(py, err, endpoint.clone()))
        }
        None => err,
    }
}

async fn collect_value(
    rx: &mut PortReceiver<PythonMessage>,
    supervision_monitor: &Option<PySupervisionMonitor>,
    instance: &Instance<PythonActor>,
    qualified_endpoint_name: &Option<String>,
) -> PyResult<(Part, Option<usize>)> {
    enum RaceResult {
        Message(PythonMessage),
        SupervisionError(PyErr),
        RecvError(String),
    }

    let race_result = match supervision_monitor {
        Some(sup) => {
            tokio::select! {
                biased;
                result = sup.supervision_event(instance) => {
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
        }) => Python::attach(|py| {
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
    supervision_monitor: Option<PySupervisionMonitor>,
    instance: &Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
) -> PyResult<Py<PyAny>> {
    let start = RealClock.now();

    let expected_count = extent.num_ranks();

    let record_guard = RecordEndpointGuard::new(
        start,
        method_name.clone(),
        expected_count,
        EndpointAdverb::Call,
    );

    let mut results: Vec<Option<Part>> = vec![None; expected_count];

    for _ in 0..expected_count {
        match collect_value(
            &mut rx,
            &supervision_monitor,
            instance,
            &qualified_endpoint_name,
        )
        .await
        {
            Ok((message, rank)) => {
                results[rank.expect("RankedPort receiver got a message without a rank")] =
                    Some(message);
            }
            Err(e) => {
                record_guard.mark_error();
                return Err(e);
            }
        }
    }

    Python::attach(|py| {
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
    supervision_monitor: Option<PySupervisionMonitor>,
    instance: PyInstance,
    qualified_endpoint_name: Option<String>,
) -> PyResult<(PythonPortRef, PyPythonTask)> {
    let instance = instance.into_instance();
    let (p, r) = instance.open_port::<PythonMessage>();

    Ok((
        PythonPortRef { inner: p.bind() },
        PythonTask::new(async move {
            collect_valuemesh(
                extent.into(),
                r,
                method_name,
                supervision_monitor,
                &instance,
                qualified_endpoint_name,
            )
            .await
        })?
        .into(),
    ))
}

fn value_collector(
    mut receiver: PortReceiver<PythonMessage>,
    method_name: String,
    supervision_monitor: Option<PySupervisionMonitor>,
    instance: Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
    adverb: EndpointAdverb,
) -> PyResult<PyPythonTask> {
    Ok(PythonTask::new(async move {
        let start = RealClock.now();

        let record_guard = RecordEndpointGuard::new(start, method_name, 1, adverb);

        match collect_value(
            &mut receiver,
            &supervision_monitor,
            &instance,
            &qualified_endpoint_name,
        )
        .await
        {
            Ok((message, _)) => Python::attach(|py| {
                unflatten(py, &message, instance.mailbox_for_py().clone()).map(|obj| obj.unbind())
            }),
            Err(e) => {
                record_guard.mark_error();
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
    supervision_monitor: Option<PySupervisionMonitor>,
    instance: PyInstance,
    qualified_endpoint_name: Option<String>,
    adverb: &str,
) -> PyResult<(PythonPortRef, PyPythonTask)> {
    let adverb = to_endpoint_adverb(adverb)?;
    let mailbox = instance.mailbox_for_py();
    let (p, r) = mailbox.open_port::<PythonMessage>();
    let instance = instance.into_instance();
    let task = value_collector(
        r,
        method_name,
        supervision_monitor,
        instance,
        qualified_endpoint_name,
        adverb,
    )?;
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
    /// Supervisor for monitoring actor health during streaming.
    supervision_monitor: Option<PySupervisionMonitor>,
    instance: Instance<PythonActor>,
    remaining: AtomicUsize,
    method_name: String,
    qualified_endpoint_name: Option<String>,
    start: tokio::time::Instant,
    actor_count: usize,
    future_class: Py<PyAny>,
}

#[pymethods]
impl PyValueStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Ok(None);
        }
        self.remaining.store(remaining - 1, Ordering::Relaxed);

        let receiver = self.receiver.clone();
        let supervision_monitor = self.supervision_monitor.clone();
        let instance = self.instance.clone_for_py();
        let qualified_endpoint_name = self.qualified_endpoint_name.clone();
        let start = self.start;
        let method_name = self.method_name.clone();
        let actor_count = self.actor_count;

        let task: PyPythonTask = PythonTask::new(async move {
            let record_guard =
                RecordEndpointGuard::new(start, method_name, actor_count, EndpointAdverb::Stream);

            let mut rx_guard = receiver.lock().await;

            match collect_value(
                &mut rx_guard,
                &supervision_monitor,
                &instance,
                &qualified_endpoint_name,
            )
            .await
            {
                Ok((message, _)) => Python::attach(|py| {
                    unflatten(py, &message, instance.mailbox_for_py().clone())
                        .map(|obj| obj.unbind())
                }),
                Err(e) => {
                    record_guard.mark_error();
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

/// Create a streaming iterator for processing multiple responses incrementally.
///
/// Returns a tuple of (PortRef, ValueStream) where:
/// - PortRef: The port reference to send to actors for responses
/// - ValueStream: An iterator that yields PythonTask objects for each response
#[pyfunction]
#[pyo3(name = "stream_collector")]
fn py_stream_collector(
    py: Python<'_>,
    extent: PyExtent,
    method_name: String,
    supervision_monitor: Option<PySupervisionMonitor>,
    instance: PyInstance,
    qualified_endpoint_name: Option<String>,
) -> PyResult<(PythonPortRef, PyValueStream)> {
    let start = RealClock.now();
    let extent: Extent = extent.into();

    let actor_count = extent.num_ranks();
    let instance = instance.into_instance();
    let (p, r) = instance.open_port::<PythonMessage>();

    let attributes = hyperactor_telemetry::kv_pairs!(
        "method" => method_name.clone()
    );
    ENDPOINT_STREAM_THROUGHPUT.add(1, attributes);

    let future_class = make_future(py).unbind();

    Ok((
        PythonPortRef { inner: p.bind() },
        PyValueStream {
            receiver: Arc::new(tokio::sync::Mutex::new(r)),
            supervision_monitor,
            instance,
            remaining: AtomicUsize::new(actor_count),
            method_name,
            qualified_endpoint_name,
            start,
            actor_count,
            future_class,
        },
    ))
}

fn wrap_in_future(py: Python<'_>, task: PyPythonTask) -> PyResult<Py<PyAny>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("coro", task)?;
    let future = make_future(py).call((), Some(&kwargs))?;
    Ok(future.unbind())
}

/// Trait that defines the core operations an endpoint must provide.
/// Both ActorEndpoint and RemoteEndpoint implement this trait.
pub(crate) trait Endpoint {
    /// Get the extent of the endpoint's targets.
    fn get_extent(&self, py: Python<'_>) -> PyResult<Extent>;

    /// Get the method name for this endpoint.
    fn get_method_name(&self) -> &str;

    /// Create and send a message with the given args/kwargs.
    fn send_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<&PythonPortRef>,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()>;

    /// Get the supervision_monitor for this endpoint (if any).
    fn get_supervision_monitor(&self) -> Option<PySupervisionMonitor>;

    /// Get the qualified endpoint name for error messages (if any).
    fn get_qualified_name(&self) -> Option<String>;

    fn get_current_instance(&self, py: Python<'_>) -> PyResult<Instance<PythonActor>> {
        let context = get_context(py).call0()?;
        let py_instance: PyRef<PyInstance> = context.getattr("actor_instance")?.extract()?;
        Ok(py_instance.clone().into_instance())
    }

    fn open_response_port(
        &self,
        instance: &Instance<PythonActor>,
    ) -> (PythonPortRef, PortReceiver<PythonMessage>) {
        let (p, receiver) = instance.mailbox_for_py().open_port::<PythonMessage>();
        (PythonPortRef { inner: p.bind() }, receiver)
    }

    /// Call the endpoint on all actors and collect all responses into a ValueMesh.
    fn call<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let extent = self.get_extent(py)?;
        let method_name = self.get_method_name().to_string();

        let instance = self.get_current_instance(py)?;
        let (port_ref, receiver) = self.open_response_port(&instance);

        let supervision_monitor = self.get_supervision_monitor();
        let qualified_endpoint_name = self.get_qualified_name();

        self.send_message(py, args, kwargs, Some(&port_ref), sel!(*), &instance)?;

        let instance_for_task = instance.clone_for_py();
        let task: PyPythonTask = PythonTask::new(async move {
            collect_valuemesh(
                extent,
                receiver,
                method_name,
                supervision_monitor,
                &instance_for_task,
                qualified_endpoint_name,
            )
            .await
        })?
        .into();

        wrap_in_future(py, task)
    }

    /// Load balanced sends a message to one chosen actor and awaits a result.
    fn choose<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let method_name = self.get_method_name();

        let instance = self.get_current_instance(py)?;
        let (port_ref, receiver) = self.open_response_port(&instance);

        self.send_message(py, args, kwargs, Some(&port_ref), sel!(?), &instance)?;

        let task = value_collector(
            receiver,
            method_name.to_string(),
            self.get_supervision_monitor(),
            instance.clone_for_py(),
            self.get_qualified_name(),
            EndpointAdverb::Choose,
        )?;

        wrap_in_future(py, task)
    }

    /// Call the endpoint on exactly one actor (the mesh must have exactly one actor).
    fn call_one<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let extent = self.get_extent(py)?;
        let method_name = self.get_method_name();

        if extent.num_ranks() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "call_one requires exactly 1 actor, but mesh has {}",
                extent.num_ranks()
            )));
        }

        let instance = self.get_current_instance(py)?;
        let (port_ref, receiver) = self.open_response_port(&instance);

        self.send_message(py, args, kwargs, Some(&port_ref), sel!(*), &instance)?;

        let task = value_collector(
            receiver,
            method_name.to_string(),
            self.get_supervision_monitor(),
            instance.clone_for_py(),
            self.get_qualified_name(),
            EndpointAdverb::CallOne,
        )?;

        wrap_in_future(py, task)
    }

    /// Call the endpoint on all actors and return an iterator of Futures.
    fn stream<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let extent = self.get_extent(py)?;
        let method_name = self.get_method_name().to_string();

        let instance = self.get_current_instance(py)?;
        let (port_ref, receiver) = self.open_response_port(&instance);

        self.send_message(py, args, kwargs, Some(&port_ref), sel!(*), &instance)?;

        let actor_count = extent.num_ranks();
        let start = RealClock.now();
        let supervision_monitor = self.get_supervision_monitor();
        let qualified_endpoint_name = self.get_qualified_name();
        let future_class = make_future(py).unbind();

        let attributes = hyperactor_telemetry::kv_pairs!(
            "method" => method_name.clone()
        );
        ENDPOINT_STREAM_THROUGHPUT.add(1, attributes);

        let stream = PyValueStream {
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
            supervision_monitor,
            instance: instance.clone_for_py(),
            remaining: AtomicUsize::new(actor_count),
            method_name,
            qualified_endpoint_name,
            start,
            actor_count,
            future_class,
        };

        Ok(stream.into_pyobject(py)?.unbind().into())
    }

    /// Send a message to all actors without waiting for responses (fire-and-forget).
    fn broadcast<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<()> {
        let instance = self.get_current_instance(py)?;
        let method_name = self.get_method_name();
        let attributes = hyperactor_telemetry::kv_pairs!(
            "method" => method_name.to_string()
        );

        match self.send_message(py, args, kwargs, None, sel!(*), &instance) {
            Ok(()) => {
                ENDPOINT_BROADCAST_THROUGHPUT.add(1, attributes);
                Ok(())
            }
            Err(e) => {
                ENDPOINT_BROADCAST_ERROR.add(1, attributes);
                Err(e)
            }
        }
    }
}

#[pyclass(
    name = "ActorEndpoint",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
pub struct ActorEndpoint {
    inner: PythonActorMesh,
    shape: Shape,
    method: MethodSpecifier,
    mesh_name: String,
    signature: Option<Py<PyAny>>,
    proc_mesh: Option<Py<PyAny>>,
    propagator: Option<Py<PyAny>>,
}

impl ActorEndpoint {
    fn create_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<&PythonPortRef>,
    ) -> PyResult<PythonMessage> {
        let port_ref_py: Py<PyAny> = match port_ref {
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

impl Endpoint for ActorEndpoint {
    fn get_extent(&self, _py: Python<'_>) -> PyResult<Extent> {
        Ok(self.shape.extent())
    }

    fn get_method_name(&self) -> &str {
        self.method.name()
    }

    fn send_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<&PythonPortRef>,
        selection: Selection,
        instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        let message = self.create_message(py, args, kwargs, port_ref)?;
        let attributes = hyperactor_telemetry::kv_pairs!(
            "method" => self.method.name().to_string()
        );
        ENDPOINT_MESSAGE_SIZE_HISTOGRAM.record(message.message.len() as f64, attributes);
        self.inner.cast(message, selection, instance)
    }

    fn get_supervision_monitor(&self) -> Option<PySupervisionMonitor> {
        Some(self.inner.get_supervision_monitor())
    }

    fn get_qualified_name(&self) -> Option<String> {
        Some(format!("{}.{}()", self.mesh_name, self.method.name()))
    }
}

#[pymethods]
impl ActorEndpoint {
    /// Create a new ActorEndpoint.
    #[new]
    #[pyo3(signature = (actor_mesh, method, shape, mesh_name, signature=None, proc_mesh=None, propagator=None))]
    fn new(
        actor_mesh: PythonActorMesh,
        method: MethodSpecifier,
        shape: PyShape,
        mesh_name: String,
        signature: Option<Py<PyAny>>,
        proc_mesh: Option<Py<PyAny>>,
        propagator: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            inner: actor_mesh,
            shape: shape.get_inner().clone(),
            method,
            mesh_name,
            signature,
            proc_mesh,
            propagator,
        }
    }

    /// Get the method specifier (used by actor_rref for tensor dispatch).
    #[getter]
    fn _name(&self) -> MethodSpecifier {
        self.method.clone()
    }

    /// Get the signature (used for argument checking in _dispatch_actor_rref).
    #[getter]
    fn _signature(&self, py: Python<'_>) -> Py<PyAny> {
        self.signature
            .clone()
            .unwrap_or_else(|| py.None().into_any())
    }

    /// Get the actor mesh (used by actor_rref for sending messages).
    #[getter]
    fn _actor_mesh(&self) -> PythonActorMesh {
        self.inner.clone()
    }

    /// Propagation method for tensor shape inference.
    /// Delegates to Python _do_propagate helper.
    fn _propagate<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyAny>,
        kwargs: &Bound<'py, PyAny>,
        fake_args: &Bound<'py, PyAny>,
        fake_kwargs: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let do_propagate = py
            .import("monarch._src.actor.endpoint")?
            .getattr("_do_propagate")?;
        let propagator = self
            .propagator
            .as_ref()
            .map(|p| p.clone_ref(py).into_bound(py))
            .unwrap_or_else(|| py.None().into_bound(py));
        let cache = PyDict::new(py);
        do_propagate
            .call1((&propagator, args, kwargs, fake_args, fake_kwargs, cache))?
            .extract()
    }

    /// Propagation for fetch operations.
    /// Returns None if no propagator is provided, otherwise calls _propagate.
    fn _fetch_propagate<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyAny>,
        kwargs: &Bound<'py, PyAny>,
        fake_args: &Bound<'py, PyAny>,
        fake_kwargs: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        if self.propagator.is_none() {
            return Ok(py.None());
        }
        self._propagate(py, args, kwargs, fake_args, fake_kwargs)
    }

    /// Propagation for pipe operations.
    /// Requires an explicit callable propagator.
    fn _pipe_propagate<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyAny>,
        kwargs: &Bound<'py, PyAny>,
        fake_args: &Bound<'py, PyAny>,
        fake_kwargs: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        // Check if propagator is callable
        let is_callable = self
            .propagator
            .as_ref()
            .map(|p| p.bind(py).is_callable())
            .unwrap_or(false);
        if !is_callable {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Must specify explicit callable for pipe",
            ));
        }
        self._propagate(py, args, kwargs, fake_args, fake_kwargs)
    }

    /// Get the rref result by calling the Python dispatch helper.
    #[pyo3(signature = (*args, **kwargs))]
    fn rref<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let kwargs_dict = kwargs.map_or_else(|| PyDict::new(py), |d| d.clone());

        // Call _dispatch_actor_rref(endpoint, args, kwargs)
        let result = dispatch_actor_rref(py).call1((slf.into_pyobject(py)?, args, kwargs_dict))?;

        Ok(result.unbind())
    }

    /// Call the endpoint on all actors and collect all responses into a ValueMesh.
    #[pyo3(signature = (*args, **kwargs), name = "call")]
    fn py_call<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.call(py, args, kwargs)
    }

    /// Load balanced sends a message to one chosen actor and awaits a result.
    #[pyo3(signature = (*args, **kwargs), name = "choose")]
    fn py_choose<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.choose(py, args, kwargs)
    }

    /// Call the endpoint on exactly one actor (the mesh must have exactly one actor).
    #[pyo3(signature = (*args, **kwargs), name = "call_one")]
    fn py_call_one<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.call_one(py, args, kwargs)
    }

    /// Call the endpoint on all actors and return an iterator of Futures.
    #[pyo3(signature = (*args, **kwargs), name = "stream")]
    fn py_stream<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.stream(py, args, kwargs)
    }

    /// Send a message to all actors without waiting for responses (fire-and-forget).
    #[pyo3(signature = (*args, **kwargs), name = "broadcast")]
    fn py_broadcast<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<()> {
        self.broadcast(py, args, kwargs)
    }

    /// Send a message with optional port for response (used by actor_mesh.send).
    fn _send<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: &Bound<'py, PyDict>,
        port: Option<&PythonPortRef>,
        selection: &str,
    ) -> PyResult<()> {
        let instance = self.get_current_instance(py)?;
        let sel = to_hy_sel(selection)?;
        self.send_message(py, args, Some(kwargs), port, sel, &instance)
    }
}

/// A Rust wrapper for Python's RemoteImpl endpoint.
///
/// This allows us to implement the adverb methods (call, choose, call_one, stream, broadcast)
/// in Rust while delegating the actual send logic to the Python RemoteImpl._send() method.
#[pyclass(
    name = "Remote",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
pub struct Remote {
    /// The wrapped Python RemoteImpl object
    inner: Py<PyAny>,
}

impl Endpoint for Remote {
    fn get_extent(&self, py: Python<'_>) -> PyResult<Extent> {
        let extent: PyExtent = self.inner.call_method0(py, "_get_extent")?.extract(py)?;
        Ok(extent.into())
    }

    fn get_method_name(&self) -> &str {
        "unknown"
    }

    fn send_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<&PythonPortRef>,
        selection: Selection,
        _instance: &Instance<PythonActor>,
    ) -> PyResult<()> {
        let send_kwargs = PyDict::new(py);
        match port_ref {
            Some(pr) => send_kwargs.set_item("port", pr.clone())?,
            None => send_kwargs.set_item("port", py.None())?,
        }

        let selection_str = match selection {
            Selection::All(inner) if matches!(*inner, Selection::True) => "all",
            Selection::Any(inner) if matches!(*inner, Selection::True) => "choose",
            _ => {
                panic!("only sel!(*) and sel!(?) should be provided as selection for send_message")
            }
        };

        send_kwargs.set_item("selection", selection_str)?;

        let kwargs_dict = kwargs.map_or_else(|| PyDict::new(py), |d| d.clone());
        self.inner
            .call_method(py, "_send", (args.clone(), kwargs_dict), Some(&send_kwargs))?;

        Ok(())
    }

    fn get_supervision_monitor(&self) -> Option<PySupervisionMonitor> {
        None // Remote endpoints don't have supervision_monitors
    }

    fn get_qualified_name(&self) -> Option<String> {
        None // Remote endpoints don't have qualified names
    }
}

#[pymethods]
impl Remote {
    /// Create a new Remote wrapping a Python RemoteImpl object.
    #[new]
    fn new(remote: Py<PyAny>) -> Self {
        Self { inner: remote }
    }

    /// Call the endpoint on all actors and collect all responses into a ValueMesh.
    #[pyo3(signature = (*args, **kwargs), name = "call")]
    fn py_call<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.call(py, args, kwargs)
    }

    /// Load balanced sends a message to one chosen actor and awaits a result.
    #[pyo3(signature = (*args, **kwargs), name = "choose")]
    fn py_choose<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.choose(py, args, kwargs)
    }

    /// Call the endpoint on exactly one actor (the mesh must have exactly one actor).
    #[pyo3(signature = (*args, **kwargs), name = "call_one")]
    fn py_call_one<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.call_one(py, args, kwargs)
    }

    /// Call the endpoint on all actors and return an iterator of Futures.
    #[pyo3(signature = (*args, **kwargs), name = "stream")]
    fn py_stream<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.stream(py, args, kwargs)
    }

    /// Send a message to all actors without waiting for responses (fire-and-forget).
    #[pyo3(signature = (*args, **kwargs), name = "broadcast")]
    fn py_broadcast<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<()> {
        self.broadcast(py, args, kwargs)
    }

    /// Get the rref result by calling the wrapped Remote's rref method.
    #[pyo3(signature = (*args, **kwargs))]
    fn rref<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let kwargs_dict = kwargs.map_or_else(|| PyDict::new(py), |d| d.clone());
        self.inner.call_method(py, "rref", args, Some(&kwargs_dict))
    }

    /// Get the call name by delegating to the wrapped Remote's _call_name.
    fn _call_name(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.call_method0(py, "_call_name")
    }

    /// Get the maybe_resolvable property from the wrapped RemoteImpl.
    #[getter]
    fn _maybe_resolvable(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.getattr(py, "_maybe_resolvable")
    }

    /// Get the resolvable property from the wrapped RemoteImpl.
    #[getter]
    fn _resolvable(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.getattr(py, "_resolvable")
    }

    /// Get the remote_impl from the wrapped RemoteImpl.
    /// This is needed for function_to_import_path() in function.py to work correctly.
    #[getter]
    fn _remote_impl(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.getattr(py, "_remote_impl")
    }

    /// Propagation method for tensor shape inference.
    /// Delegates to the wrapped Remote's _propagate.
    fn _propagate<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyAny>,
        kwargs: &Bound<'py, PyAny>,
        fake_args: &Bound<'py, PyAny>,
        fake_kwargs: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.inner
            .call_method1(py, "_propagate", (args, kwargs, fake_args, fake_kwargs))
    }

    /// Propagation for fetch operations.
    /// Delegates to the wrapped Remote's _fetch_propagate.
    fn _fetch_propagate<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyAny>,
        kwargs: &Bound<'py, PyAny>,
        fake_args: &Bound<'py, PyAny>,
        fake_kwargs: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.inner.call_method1(
            py,
            "_fetch_propagate",
            (args, kwargs, fake_args, fake_kwargs),
        )
    }

    /// Propagation for pipe operations.
    /// Delegates to the wrapped Remote's _pipe_propagate.
    fn _pipe_propagate<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyAny>,
        kwargs: &Bound<'py, PyAny>,
        fake_args: &Bound<'py, PyAny>,
        fake_kwargs: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.inner.call_method1(
            py,
            "_pipe_propagate",
            (args, kwargs, fake_args, fake_kwargs),
        )
    }

    /// Send a message with optional port for response.
    /// Delegates to the wrapped RemoteImpl's _send.
    fn _send<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: &Bound<'py, PyDict>,
        port: Option<Py<PyAny>>,
        selection: &str,
    ) -> PyResult<()> {
        self.inner.call_method(
            py,
            "_send",
            (args, kwargs),
            Some(&{
                let d = PyDict::new(py);
                d.set_item("port", port.unwrap_or_else(|| py.None()))?;
                d.set_item("selection", selection)?;
                d
            }),
        )?;
        Ok(())
    }

    /// Make RemoteEndpoint callable - delegates to rref() like Remote.__call__.
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.rref(py, args, kwargs)
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    py_module_add_function!(
        module,
        "monarch._rust_bindings.monarch_hyperactor.endpoint",
        py_valuemesh_collector
    );

    py_module_add_function!(
        module,
        "monarch._rust_bindings.monarch_hyperactor.endpoint",
        py_value_collector
    );

    py_module_add_function!(
        module,
        "monarch._rust_bindings.monarch_hyperactor.endpoint",
        py_stream_collector
    );

    module.add_class::<PyValueStream>()?;
    module.add_class::<RepeatMailbox>()?;
    module.add_class::<ActorEndpoint>()?;
    module.add_class::<Remote>()?;

    Ok(())
}
