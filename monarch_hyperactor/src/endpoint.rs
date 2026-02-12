/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;

use hyperactor::Instance;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::mailbox::PortReceiver;
use monarch_types::py_global;
use monarch_types::py_module_add_function;
use ndslice::Extent;
use pyo3::prelude::*;
use serde_multipart::Part;

use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
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
use crate::pytokio::PyPythonTask;
use crate::pytokio::PythonTask;
use crate::shape::PyExtent;
use crate::supervision::PySupervisionMonitor;
use crate::supervision::SupervisionError;
use crate::value_mesh::PyValueMesh;

py_global!(pickle_unflatten, "monarch._src.actor.pickle", "unflatten");
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

    module.add_class::<RepeatMailbox>()?;

    Ok(())
}
