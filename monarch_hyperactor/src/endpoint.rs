/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use hyperactor::ActorAddr;
use hyperactor::Instance;
use hyperactor::accum::Accumulator;
use hyperactor::accum::CommReducer;
use hyperactor::accum::ReducerFactory;
use hyperactor::accum::ReducerSpec;
use hyperactor::id::Label;
use hyperactor::mailbox::OncePortReceiver;
use hyperactor::mailbox::PortReceiver;
use hyperactor_mesh::value_mesh::ValueOverlay;
use hyperactor_mesh::value_mesh::rle;
use monarch_types::py_global;
use ndslice::Extent;
use ndslice::Shape;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use serde_multipart::Part;
use typeuri::Named;

use crate::actor::MeshRef;
use crate::actor::MethodSpecifier;
use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::actor::PythonResponseMessage;
use crate::actor_mesh::AllOrChoose;
use crate::actor_mesh::PythonActorMesh;
use crate::actor_mesh::SupervisableActorMesh;
use crate::actor_mesh::to_all_or_choose;
use crate::context::PyInstance;
use crate::handle::PyHandle;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PythonOncePortRef;
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
use crate::metrics::EndpointAttrs;
use crate::metrics::UNKNOWN;
use crate::pickle::PendingMessage;
use crate::pickle::PicklingState;
use crate::pytokio::current_traceback;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::runtime::signal_safe_block_on;
use crate::shape::PyExtent;
use crate::shape::PyShape;
use crate::supervision::Supervisable;
use crate::supervision::SupervisionError;
use crate::value_mesh::PyValueMesh;

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

/// The type of endpoint operation being performed.
///
/// Used to select the appropriate telemetry metrics for each operation type.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EndpointAdverb {
    Call,
    CallOne,
    Choose,
    Stream,
    Broadcast,
}

impl EndpointAdverb {
    fn as_str(self) -> &'static str {
        match self {
            Self::Call => "call",
            Self::CallOne => "call_one",
            Self::Choose => "choose",
            Self::Stream => "stream",
            Self::Broadcast => "broadcast",
        }
    }
}

/// RAII guard for recording endpoint call telemetry.
///
/// Records latency on drop, similar to Python's `@_with_telemetry` decorator.
/// Call `mark_error()` before dropping to also record an error.
pub struct RecordEndpointGuard {
    start: tokio::time::Instant,
    attrs: Arc<EndpointAttrs>,
    adverb: EndpointAdverb,
    error_occurred: Cell<bool>,
}

impl RecordEndpointGuard {
    fn new(start: tokio::time::Instant, attrs: Arc<EndpointAttrs>, adverb: EndpointAdverb) -> Self {
        let attributes = attrs.as_slice();
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
            EndpointAdverb::Stream | EndpointAdverb::Broadcast => {
                // Throughput already recorded at the call site
            }
        }

        Self {
            start,
            attrs,
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
        let attributes = self.attrs.as_slice();
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
            EndpointAdverb::Broadcast => {}
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
                EndpointAdverb::Broadcast => {}
            }
        }
    }
}

/// Send-safe RAII guard for an OTEL-style endpoint span.
///
/// We only need endpoint spans for telemetry slices, not for `tracing` context
/// propagation. So this guard emits synthetic trace events directly into the
/// unified telemetry dispatcher instead of holding a real `tracing::Span`
/// across `.await` points.
pub(crate) struct SpanGuard {
    id: u64,
}

impl SpanGuard {
    fn actor_endpoint(
        name: &'static str,
        actor_id: &ActorAddr,
        mesh: &str,
        method: &str,
        correlation_id: u64,
    ) -> Self {
        Self {
            id: hyperactor_telemetry::start_user_span(
                name,
                hyperactor_telemetry::sinks::perfetto::ENDPOINT_TELEMETRY_TARGET,
                [
                    (
                        "actor_id",
                        hyperactor_telemetry::trace_dispatcher::FieldValue::Str(
                            actor_id.to_string(),
                        ),
                    ),
                    (
                        "mesh",
                        hyperactor_telemetry::trace_dispatcher::FieldValue::Str(mesh.to_string()),
                    ),
                    (
                        "method",
                        hyperactor_telemetry::trace_dispatcher::FieldValue::Str(method.to_string()),
                    ),
                    (
                        "correlation_id",
                        hyperactor_telemetry::trace_dispatcher::FieldValue::U64(correlation_id),
                    ),
                ],
            ),
        }
    }

    fn remote(
        name: &'static str,
        actor_id: &ActorAddr,
        call_name: &str,
        correlation_id: u64,
    ) -> Self {
        Self {
            id: hyperactor_telemetry::start_user_span(
                name,
                hyperactor_telemetry::sinks::perfetto::ENDPOINT_TELEMETRY_TARGET,
                [
                    (
                        "actor_id",
                        hyperactor_telemetry::trace_dispatcher::FieldValue::Str(
                            actor_id.to_string(),
                        ),
                    ),
                    (
                        "call_name",
                        hyperactor_telemetry::trace_dispatcher::FieldValue::Str(
                            call_name.to_string(),
                        ),
                    ),
                    (
                        "correlation_id",
                        hyperactor_telemetry::trace_dispatcher::FieldValue::U64(correlation_id),
                    ),
                ],
            ),
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        hyperactor_telemetry::end_user_span(self.id);
    }
}

/// Identify which endpoint call failed when supervision reports an actor failure.
///
/// `call()`, `call_one()` and `choose()` start reply collectors (Rust tasks that
/// wait for replies and produce the returned Future's result). A supervision
/// failure can end that wait before a reply arrives. This helper adds the called
/// endpoint's name to the Python exception so the caller can see which call
/// failed. If no name is available, or `err` is not a `SupervisionError`, return
/// the original error unchanged.
///
/// This is async so the collector can wait its turn to acquire the GIL through
/// `monarch_with_gil`. Inspecting and annotating the Python exception needs the
/// GIL; using `monarch_with_gil_blocking` would let every waiting collector block
/// a Tokio worker. `monarch_with_gil` queues those collectors asynchronously,
/// leaving their workers available for other work. The collector at the front
/// can still block acquiring the GIL; annotation runs without an `await` once
/// the GIL is held. This helper does not spawn a task.
async fn supervision_error_to_pyerr(err: PyErr, qualified_endpoint_name: &Option<String>) -> PyErr {
    match qualified_endpoint_name {
        Some(endpoint) => {
            let endpoint = endpoint.clone();
            monarch_with_gil(GilSite::Supervise, move |py| {
                SupervisionError::set_endpoint_on_err(py, err, endpoint)
            })
            .await
        }
        None => err,
    }
}

async fn collect_value(
    rx: &mut PortReceiver<PythonMessage>,
    supervision_monitor: &Option<Arc<dyn Supervisable>>,
    instance: &Instance<PythonActor>,
    qualified_endpoint_name: &Option<String>,
) -> PyResult<(Part, Vec<MeshRef>, Option<usize>)> {
    enum RaceResult {
        Message(Box<PythonMessage>),
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
                                Ok(msg) => RaceResult::Message(Box::new(msg)),
                                Err(e) => RaceResult::RecvError(e.to_string()),
                            }
                        }
                    }
                }
                msg = rx.recv() => {
                    match msg {
                        Ok(m) => RaceResult::Message(Box::new(m)),
                        Err(e) => RaceResult::RecvError(e.to_string()),
                    }
                }
            }
        }
        _ => match rx.recv().await {
            Ok(msg) => RaceResult::Message(Box::new(msg)),
            Err(e) => RaceResult::RecvError(e.to_string()),
        },
    };

    match race_result {
        RaceResult::Message(boxed) => {
            let PythonMessage {
                kind,
                message,
                refs,
            } = *boxed;
            match kind {
                PythonMessageKind::Result { rank, .. } => Ok((message, refs, rank)),
                PythonMessageKind::Exception { .. } => {
                    monarch_with_gil(GilSite::Traceback, move |py| {
                        let mesh_references: VecDeque<Option<MeshRef>> =
                            refs.into_iter().map(Some).collect();
                        let mut state =
                            PicklingState::from_parts(message, VecDeque::new(), mesh_references);
                        Err(PyErr::from_value(
                            state.unpickle_with_receiver(py, instance)?.into_bound(py),
                        ))
                    })
                    .await
                }
                other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unexpected message kind {:?}",
                    other
                ))),
            }
        }
        RaceResult::RecvError(e) => Err(pyo3::exceptions::PyEOFError::new_err(format!(
            "Port closed: {}",
            e
        ))),
        RaceResult::SupervisionError(err) => {
            Err(supervision_error_to_pyerr(err, qualified_endpoint_name).await)
        }
    }
}

// HOT PATH: Be mindful of performance when making changes here.
// To test how performance is affected by a change, run the RPC benchmarks in
// `monarch/python/benches/`.
#[tracing::instrument(level = "debug", skip_all)]
async fn collect_valuemesh(
    extent: Extent,
    rx: OncePortReceiver<PythonMessage>,
    attrs: Arc<EndpointAttrs>,
    supervision_monitor: Option<Arc<dyn Supervisable>>,
    instance: &Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
) -> PyResult<Py<PyAny>> {
    let start = tokio::time::Instant::now();

    let expected_count = extent.num_ranks();

    let record_guard = RecordEndpointGuard::new(start, attrs, EndpointAdverb::Call);

    enum RaceResult {
        Collected(Box<PythonMessage>),
        SupervisionError(PyErr),
        RecvError(String),
    }

    let race_result = match &supervision_monitor {
        Some(sup) => {
            tokio::select! {
                biased;
                result = sup.supervision_event(instance) => {
                    match result {
                        Some(err) => RaceResult::SupervisionError(err),
                        None => RaceResult::RecvError(
                            "supervision monitor closed unexpectedly".to_string()
                        ),
                    }
                }
                batch = rx.recv() => {
                    match batch {
                        Ok(b) => RaceResult::Collected(Box::new(b)),
                        Err(e) => RaceResult::RecvError(e.to_string()),
                    }
                }
            }
        }
        None => match rx.recv().await {
            Ok(batch) => RaceResult::Collected(Box::new(batch)),
            Err(e) => RaceResult::RecvError(e.to_string()),
        },
    };

    match race_result {
        RaceResult::Collected(boxed) => {
            let msg = *boxed;
            let overlay = msg.into_overlay().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to extract overlay from collected responses: {e}"
                ))
            })?;
            // Move `record_guard` into the Send closure: the guard's `Cell<bool>` is not Sync.
            monarch_with_gil(GilSite::ReplyConvert, move |py| {
                // Out-of-band mesh refs reunite only while the `PicklingState` is
                // live, i.e. during decode, so a ref-carrying response must be
                // decoded here (eagerly, at accumulation): a lazy decode on access
                // would have no state to reunite against, the REFS-1 failure. But
                // decoding *every* response here would revert D96180139, which made
                // valuemesh values unpickle lazily on access so a large
                // OnceBuffer-accumulated `.call()` does not pay one big unpickle
                // burst at the end of collection.
                //
                // Reconcile the two by gating on refs. A `.call()` fans one
                // endpoint over the mesh, so the batch is uniform: either every
                // response carries refs (the endpoint returns a mesh, the minority)
                // or none does (plain values, the common case). With no refs we
                // keep the raw parts and let them unpickle on access (D96180139,
                // preserved); only with refs present do we decode eagerly to
                // reunite them.
                let has_refs = overlay.runs().any(|(_, payload)| {
                    let (PythonResponseMessage::Result { refs, .. }
                    | PythonResponseMessage::Exception { refs, .. }) = payload;
                    !refs.is_empty()
                });

                if !has_refs {
                    let mut parts = Vec::with_capacity(expected_count);
                    for (range, payload) in overlay.runs() {
                        match payload {
                            PythonResponseMessage::Result { part, .. } => {
                                parts.extend(range.clone().map(|_| part.clone()));
                            }
                            PythonResponseMessage::Exception { .. } => {
                                record_guard.mark_error();
                                return Err(PyErr::from_value(
                                    payload.decode(py, instance)?.into_bound(py),
                                ));
                            }
                        }
                    }
                    return Ok(PyValueMesh::build_from_parts(&extent, parts)?
                        .into_pyobject(py)?
                        .into_any()
                        .unbind());
                }

                let mut objects: Vec<Py<PyAny>> = Vec::with_capacity(expected_count);
                for (range, payload) in overlay.runs() {
                    match payload {
                        PythonResponseMessage::Result { .. } => {
                            let obj = payload.decode(py, instance)?;
                            objects.extend(range.clone().map(|_| obj.clone_ref(py)));
                        }
                        PythonResponseMessage::Exception { .. } => {
                            record_guard.mark_error();
                            return Err(PyErr::from_value(
                                payload.decode(py, instance)?.into_bound(py),
                            ));
                        }
                    }
                }
                Ok(PyValueMesh::build_from_objects(&extent, objects)?
                    .into_pyobject(py)?
                    .into_any()
                    .unbind())
            })
            .await
        }
        RaceResult::RecvError(e) => {
            record_guard.mark_error();
            Err(pyo3::exceptions::PyEOFError::new_err(format!(
                "Port closed: {}",
                e
            )))
        }
        RaceResult::SupervisionError(err) => {
            record_guard.mark_error();
            Err(supervision_error_to_pyerr(err, &qualified_endpoint_name).await)
        }
    }
}

/// Collect one reply, retaining its response port and caller span until completion
/// or drop. Collection metrics start when the future is first polled.
async fn value_collector_future(
    mut receiver: PortReceiver<PythonMessage>,
    attrs: Arc<EndpointAttrs>,
    supervision_monitor: Option<Arc<dyn Supervisable>>,
    instance: Instance<PythonActor>,
    qualified_endpoint_name: Option<String>,
    adverb: EndpointAdverb,
    span_guard: SpanGuard,
) -> PyResult<Py<PyAny>> {
    let _span_guard = span_guard;
    let start = tokio::time::Instant::now();

    let record_guard = RecordEndpointGuard::new(start, attrs, adverb);

    match collect_value(
        &mut receiver,
        &supervision_monitor,
        &instance,
        &qualified_endpoint_name,
    )
    .await
    {
        Ok((message, refs, _)) => {
            monarch_with_gil(GilSite::ReplyConvert, move |py| {
                let mesh_references: VecDeque<Option<MeshRef>> =
                    refs.into_iter().map(Some).collect();
                let mut state =
                    PicklingState::from_parts(message, VecDeque::new(), mesh_references);
                state.unpickle_with_receiver(py, &instance)
            })
            .await
        }
        Err(e) => {
            record_guard.mark_error();
            Err(e)
        }
    }
}

/// Defers a stream reply's conversion, or a port receive, until the item is
/// observed.
///
/// Replies arrive in the stream's shared queue independently of observation.
/// First observation begins polling the stored Rust future. It takes the
/// next reply, waiting if necessary, and unpickles it into a Python value.
/// An unobserved item leaves the queue untouched.
///
/// A port receive is held the same way: first observation takes the next
/// message from the port and unpickles it, and an unobserved receive takes
/// nothing. When observation spawns a receive, the task stops once nobody can
/// observe it (HDL-23), so an abandoned receive releases its port receiver.
///
/// `await`, `as_asyncio()` and timed `get()` spawn a Tokio task whose worker
/// polls the future. Untimed `get()` called from Python's main thread also
/// spawns a Tokio task, while the main thread waits and checks for signals.
///
/// Untimed `get()` called from another Python thread, such as a sync actor's
/// driver, uses Tokio's `runtime.block_on` to poll the future on that calling
/// thread. It does not spawn a Tokio task for the future. The GIL is released
/// while waiting and reacquired on the calling thread for unpickling.
///
/// Transitional (SF, 2026-10-14): T292850732 will let ordinary Handles hold
/// undecoded replies.
#[pyclass(
    name = "PendingReply",
    module = "monarch._rust_bindings.monarch_hyperactor.endpoint"
)]
pub(crate) struct PyPendingReply {
    /// The unstarted Rust future and the item's optional creation traceback.
    /// The first observer takes both. This mutex protects that one-time
    /// transfer, not the reply queue, and is released before the future runs.
    collector: Mutex<Option<(ReplyFuture, Option<Py<PyAny>>)>>,
    /// Whether a spawned collector stops once nobody can observe it. Port
    /// receives set it; stream items do not.
    stop_when_unobserved: bool,
    /// The `ValueError` text for a second take, naming the kind of item.
    taken_message: &'static str,
}

const STREAM_ITEM_TAKEN: &str = "this stream item was already taken by an earlier get(); if that get() was interrupted, it will never produce a result";
const PORT_RECEIVE_TAKEN: &str = "this receive was already taken by an earlier get(); if that get() was interrupted, it will never produce a result";

impl PyPendingReply {
    /// Store a stream item's unstarted collector and the optional stack
    /// captured at creation.
    fn for_stream_item(
        collector: impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
        traceback: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            collector: Mutex::new(Some((Box::pin(collector), traceback))),
            stop_when_unobserved: false,
            taken_message: STREAM_ITEM_TAKEN,
        }
    }

    /// Store a port receive's unstarted future and the optional stack
    /// captured at creation. A spawned receive stops once nobody can
    /// observe it.
    pub(crate) fn for_port_receive(
        receive: impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
        traceback: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            collector: Mutex::new(Some((Box::pin(receive), traceback))),
            stop_when_unobserved: true,
            taken_message: PORT_RECEIVE_TAKEN,
        }
    }

    /// Give the first observer ownership of the collector and its traceback.
    ///
    /// Only the collector moves here. Its result becomes shared, through a Handle,
    /// only when the first observer goes through `spawn_handle()` (`await`,
    /// `as_asyncio()`, `get(timeout=…)`). An untimed `get()` instead hands the
    /// collector to `block_on()`, which runs it without a Handle: inline on the
    /// calling thread, or, on Python's main thread, on Tokio while the caller
    /// waits and checks for signals. The `Future` records the result only after
    /// `block_on()` returns. Until then no shared result exists, so:
    ///
    /// - another observer of the same item, for example on another thread, finds
    ///   the collector already taken and gets this error, even though the first
    ///   `get()` will still return the value;
    /// - if the first `get()` is interrupted, its result is discarded, and later
    ///   observers get this error. An interrupted `get()` is an error, not
    ///   something to resume.
    ///
    /// An observer can also land here in the brief interval between
    /// `spawn_handle()` returning and the returned Handle being stored in
    /// `Future._status`.
    ///
    /// This window belongs to lazy items, not to this type: any holder that runs
    /// the collector without first publishing a Handle has it. T292877484
    /// tracks closing it by giving each item a Handle core on first observation.
    /// T292850732 removes it altogether: once Handles can hold undecoded replies,
    /// stream items and port receives become ordinary Handles, and every observer
    /// shares one result.
    fn take(&self) -> PyResult<(ReplyFuture, Option<Py<PyAny>>)> {
        self.collector
            .lock()
            .expect("the collector lock is held only while taking its value")
            .take()
            .ok_or_else(|| PyValueError::new_err(self.taken_message))
    }
}

#[pymethods]
impl PyPendingReply {
    /// Spawn the item's stored Rust future as a Tokio task and return a Handle
    /// for its result. The future takes the next reply from the stream's queue,
    /// or the next message from the port, waiting if necessary, and unpickles it
    /// into a Python value.
    ///
    /// Handle observers share the same value or error. If the task fails after
    /// every observer has gone, its optional creation traceback is included in
    /// the error log. A port receive's task instead stops once nobody can
    /// observe it (HDL-23).
    ///
    /// Raises `ValueError` if the item's future has already been taken.
    pub(crate) fn spawn_handle(&self) -> PyResult<PyHandle> {
        let (collector, traceback) = self.take()?;
        if self.stop_when_unobserved {
            return Ok(PyHandle::spawn_pyobject_while_observed(
                collector, traceback,
            ));
        }
        Ok(PyHandle::spawn_pyobject_with_traceback(
            collector, traceback,
        ))
    }

    /// Return one item's Python value synchronously, without a Handle.
    /// Used by untimed `Future.get()`.
    ///
    /// On Python's main thread, spawn the stored future as a Tokio task while
    /// the caller waits and checks for signals. On other Python threads,
    /// Tokio's `block_on` polls it on the calling thread without spawning a task.
    /// Release the GIL while waiting; unpickling reacquires it.
    ///
    /// The Python `Future` stores the result only after this call returns.
    /// Until then, another observer gets the consumed-item error. An interrupted
    /// call discards its result; see `Self::take` for these observation windows.
    pub(crate) fn block_on(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (collector, _traceback) = slf.take()?;
        // Release the PyO3 borrow before the wait releases the GIL.
        // The extracted future is now owned by this call.
        drop(slf);
        signal_safe_block_on(py, collector)?
    }
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
    supervision_monitor: Option<Arc<dyn Supervisable>>,
    instance: Instance<PythonActor>,
    remaining: AtomicUsize,
    attrs: Arc<EndpointAttrs>,
    qualified_endpoint_name: Option<String>,
    start: tokio::time::Instant,
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
        // Reserve this item's slot now. The item takes a reply from the queue
        // only when its Future is first observed, so an item dropped without
        // being observed still uses up its slot: once `remaining` reaches zero
        // the stream ends, and one reply stays in the queue, unread, until the
        // stream is dropped. The user docs for `stream()` and `ValueStream`
        // warn about this. It belongs to lazy stream items, not to this
        // carrier: T292850732 makes items take their replies eagerly, which
        // removes it.
        self.remaining.store(remaining - 1, Ordering::Relaxed);

        let receiver = self.receiver.clone();
        let supervision_monitor = self.supervision_monitor.clone();
        let instance = self.instance.clone_for_py();
        let qualified_endpoint_name = self.qualified_endpoint_name.clone();
        let start = self.start;
        let attrs = self.attrs.clone();

        // Construct a future without executing its body or spawning a task.
        // When the item is first observed, the body takes the next reply from
        // the stream's queue, waiting if necessary, and unpickles it.
        let collector = async move {
            let record_guard = RecordEndpointGuard::new(start, attrs, EndpointAdverb::Stream);

            let mut rx_guard = receiver.lock().await;

            match collect_value(
                &mut rx_guard,
                &supervision_monitor,
                &instance,
                &qualified_endpoint_name,
            )
            .await
            {
                Ok((message, refs, _)) => monarch_with_gil_blocking(GilSite::ReplyConvert, |py| {
                    let mesh_references: VecDeque<Option<MeshRef>> =
                        refs.into_iter().map(Some).collect();
                    let mut state =
                        PicklingState::from_parts(message, VecDeque::new(), mesh_references);
                    state.unpickle_with_receiver(py, &instance)
                }),
                Err(e) => {
                    record_guard.mark_error();
                    Err(e)
                }
            }
        };
        // Keep the unstarted future and its optional creation traceback together.
        // Capture the stack here, where the item is created, not when it is observed.
        let reply = PyPendingReply::for_stream_item(collector, current_traceback()?);

        let future = self
            .future_class
            .call_method1(py, "_from_pending", (reply,))?;
        Ok(Some(future))
    }
}

/// Give Python callers a public `Future` for an endpoint call's result.
///
/// `call()`, `call_one()` and `choose()` have already sent the request and
/// scheduled a Rust future to collect replies on Tokio. The input Handle
/// observes that future's result. `Future._from_handle` stores the Handle in a
/// Python `Future`, giving callers `.get()` and `await`.
///
/// Return the Python `Future` as `Py<PyAny>`, or propagate a Python error if
/// wrapping fails. Dropping the Future, or failing to construct it, does not
/// cancel the running reply collection.
fn wrap_in_future(py: Python<'_>, handle: PyHandle) -> PyResult<Py<PyAny>> {
    let future = make_future(py).call_method1("_from_handle", (handle,))?;
    Ok(future.unbind())
}

/// Trait that defines the core operations an endpoint must provide.
/// Both ActorEndpoint and RemoteEndpoint implement this trait.
pub(crate) trait Endpoint {
    /// Get the extent of the endpoint's targets.
    fn get_extent(&self, py: Python<'_>) -> PyResult<Extent>;

    /// Get the method name for this endpoint.
    fn get_method_name(&self) -> &str;

    /// The attributes every metric recorded for this endpoint carries. Built
    /// once per endpoint; the adverbs hand a clone to the tasks that outlive
    /// the borrow of `self`.
    fn metric_attrs(&self) -> &Arc<EndpointAttrs>;

    /// Create and send a message with the given args/kwargs.
    fn send_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<EitherPortRef>,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        correlation_id: Option<u64>,
    ) -> PyResult<()>;

    /// Like `send_message` but stamps `caller_headers` onto the
    /// outgoing request envelope. Implementations that can carry
    /// headers override this; the default delegates to `send_message`
    /// and drops them.
    fn send_message_with_headers<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<EitherPortRef>,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        _caller_headers: hyperactor_config::Flattrs,
        correlation_id: Option<u64>,
    ) -> PyResult<()> {
        self.send_message(
            py,
            args,
            kwargs,
            port_ref,
            selection,
            instance,
            correlation_id,
        )
    }

    /// Build the operation-context envelope headers to stamp on an
    /// outgoing request for this endpoint invocation. The result is
    /// empty when the endpoint cannot supply a qualified name
    /// (e.g. `RemoteEndpoint`'s `get_qualified_name` returns `None`),
    /// in which case callers see the unchanged dispatch surface.
    fn build_operation_context_headers(
        &self,
        adverb: EndpointAdverb,
    ) -> hyperactor_config::Flattrs {
        let adverb_str = adverb.as_str();
        let attrs = crate::operation_context::build_operation_context_attrs(
            self.get_qualified_name(),
            Some(adverb_str),
        );
        let mut headers = hyperactor_config::Flattrs::new();
        crate::operation_context::stamp_operation_context(&mut headers, &attrs);
        headers
    }

    /// Get the supervision_monitor for this endpoint (if any).
    fn get_supervision_monitor(&self) -> Option<Arc<dyn Supervisable>>;

    /// Get the qualified endpoint name for error messages (if any).
    fn get_qualified_name(&self) -> Option<String>;

    /// Open an OTEL-style span for an endpoint invocation. Each impl attaches
    /// the fields the perfetto sink needs to synthesize the display name
    /// (`{mesh}.{method}.{adverb}` for ActorEndpoint, `{call_name}.{adverb}`
    /// for Remote) and route the slice to an actor-specific track. The adverb is the span name,
    /// so no formatting happens at the call site and the sink formats only when
    /// it renders the slice.
    fn enter_endpoint_span(
        &self,
        adverb: EndpointAdverb,
        actor_id: &ActorAddr,
        correlation_id: u64,
    ) -> SpanGuard;

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

    fn open_reduce_response_port(
        &self,
        instance: &Instance<PythonActor>,
    ) -> (PythonOncePortRef, OncePortReceiver<PythonMessage>) {
        let (p, receiver) = instance
            .mailbox_for_py()
            .open_reduce_port(PythonResponseMessageAccumulator);
        (PythonOncePortRef::from(p.bind()), receiver)
    }

    // HOT PATH: Be mindful of performance when making changes here.
    // To test how performance is affected by a change, run the RPC benchmarks in
    // `monarch/python/benches/`.
    /// Send the call to all actors now and return its owned reply collector.
    #[tracing::instrument(name = "call", level = "debug", skip_all)]
    fn call_future<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static + use<Self>> {
        let instance = self.get_current_instance(py)?;
        let correlation_id: u64 = fastrand::u64(..);
        let span_guard =
            self.enter_endpoint_span(EndpointAdverb::Call, instance.self_addr(), correlation_id);

        let extent = self.get_extent(py)?;
        let attrs = self.metric_attrs().clone();
        let (port_ref, receiver) = self.open_reduce_response_port(&instance);

        let supervision_monitor = self.get_supervision_monitor();
        let qualified_endpoint_name = self.get_qualified_name();

        let caller_headers = self.build_operation_context_headers(EndpointAdverb::Call);
        self.send_message_with_headers(
            py,
            args,
            kwargs,
            Some(EitherPortRef::Once(port_ref)),
            AllOrChoose::All,
            &instance,
            caller_headers,
            Some(correlation_id),
        )?;

        let instance_for_task = instance.clone_for_py();
        Ok(async move {
            let _span_guard = span_guard;
            collect_valuemesh(
                extent,
                receiver,
                attrs,
                supervision_monitor,
                &instance_for_task,
                qualified_endpoint_name,
            )
            .await
        })
    }

    /// Send this endpoint call to every actor in the mesh and start collecting
    /// replies before returning.
    ///
    /// Return a Python Future whose result is a ValueMesh of the actors' return
    /// values. Dropping the Future does not cancel the call or stop collection.
    fn call<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let handle = PyHandle::spawn_pyobject_with_traceback(
            self.call_future(py, args, kwargs)?,
            current_traceback()?,
        );
        wrap_in_future(py, handle)
    }

    /// Send `choose()`'s request and return the Rust future for its result.
    ///
    /// Send the supplied arguments to one actor selected uniformly at random from
    /// the mesh. The future waits for a reply or a supervision failure. Reply
    /// decoding uses the calling actor's instance, captured here.
    ///
    /// `choose()` runs this future on Tokio and returns a Python Future backed by
    /// a Handle observing its result.
    fn choose_future<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static + use<Self>> {
        let instance = self.get_current_instance(py)?;
        let correlation_id: u64 = fastrand::u64(..);
        let span_guard =
            self.enter_endpoint_span(EndpointAdverb::Choose, instance.self_addr(), correlation_id);
        let (port_ref, receiver) = self.open_response_port(&instance);

        let caller_headers = self.build_operation_context_headers(EndpointAdverb::Choose);
        self.send_message_with_headers(
            py,
            args,
            kwargs,
            Some(EitherPortRef::Unbounded(port_ref)),
            AllOrChoose::Choose,
            &instance,
            caller_headers,
            Some(correlation_id),
        )?;

        Ok(value_collector_future(
            receiver,
            self.metric_attrs().clone(),
            self.get_supervision_monitor(),
            instance.clone_for_py(),
            self.get_qualified_name(),
            EndpointAdverb::Choose,
            span_guard,
        ))
    }

    /// Send this endpoint call to one actor chosen uniformly at random from the
    /// mesh, and start collecting its reply before returning.
    ///
    /// Return a Python Future whose result is that actor's return value.
    /// Dropping the Future does not cancel the call or stop collection.
    ///
    /// Selection is independent for each call and does not account for actor load.
    /// Use `call_one()` on a single-actor slice when placement must be deterministic.
    fn choose<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let handle = PyHandle::spawn_pyobject_with_traceback(
            self.choose_future(py, args, kwargs)?,
            current_traceback()?,
        );
        wrap_in_future(py, handle)
    }

    /// Require exactly one actor, send the call now and return its owned reply collector.
    fn call_one_future<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static + use<Self>> {
        let extent = self.get_extent(py)?;

        if extent.num_ranks() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "call_one requires exactly 1 actor, but mesh has {}",
                extent.num_ranks()
            )));
        }

        let instance = self.get_current_instance(py)?;
        let correlation_id: u64 = fastrand::u64(..);
        let span_guard = self.enter_endpoint_span(
            EndpointAdverb::CallOne,
            instance.self_addr(),
            correlation_id,
        );
        let (port_ref, receiver) = self.open_response_port(&instance);

        let caller_headers = self.build_operation_context_headers(EndpointAdverb::CallOne);
        self.send_message_with_headers(
            py,
            args,
            kwargs,
            Some(EitherPortRef::Unbounded(port_ref)),
            AllOrChoose::All,
            &instance,
            caller_headers,
            Some(correlation_id),
        )?;

        Ok(value_collector_future(
            receiver,
            self.metric_attrs().clone(),
            self.get_supervision_monitor(),
            instance.clone_for_py(),
            self.get_qualified_name(),
            EndpointAdverb::CallOne,
            span_guard,
        ))
    }

    /// Require exactly one actor, send now and start collecting its reply.
    /// The returned Future observes completion; dropping it does not stop collection.
    fn call_one<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let handle = PyHandle::spawn_pyobject_with_traceback(
            self.call_one_future(py, args, kwargs)?,
            current_traceback()?,
        );
        wrap_in_future(py, handle)
    }

    /// Call the endpoint on all actors and return an iterator of Futures.
    fn stream<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let extent = self.get_extent(py)?;

        let instance = self.get_current_instance(py)?;
        let (port_ref, receiver) = self.open_response_port(&instance);

        let correlation_id: u64 = fastrand::u64(..);
        // Send-time caller span so the correlation id has a sender for the flow
        // arrow; without it the receiver spans are the only ones carrying the id.
        let _span_guard =
            self.enter_endpoint_span(EndpointAdverb::Stream, instance.self_addr(), correlation_id);
        let caller_headers = self.build_operation_context_headers(EndpointAdverb::Stream);
        self.send_message_with_headers(
            py,
            args,
            kwargs,
            Some(EitherPortRef::Unbounded(port_ref)),
            AllOrChoose::All,
            &instance,
            caller_headers,
            Some(correlation_id),
        )?;

        let actor_count = extent.num_ranks();
        let start = tokio::time::Instant::now();
        let supervision_monitor = self.get_supervision_monitor();
        let qualified_endpoint_name = self.get_qualified_name();
        let future_class = make_future(py).unbind();

        ENDPOINT_STREAM_THROUGHPUT.add(1, self.metric_attrs().as_slice());

        let stream = PyValueStream {
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
            supervision_monitor,
            instance: instance.clone_for_py(),
            remaining: AtomicUsize::new(actor_count),
            attrs: self.metric_attrs().clone(),
            qualified_endpoint_name,
            start,
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
        let correlation_id: u64 = fastrand::u64(..);
        let _span_guard = self.enter_endpoint_span(
            EndpointAdverb::Broadcast,
            instance.self_addr(),
            correlation_id,
        );
        let attributes = self.metric_attrs().as_slice();

        match self.send_message(
            py,
            args,
            kwargs,
            None,
            AllOrChoose::All,
            &instance,
            Some(correlation_id),
        ) {
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
    inner: Arc<dyn SupervisableActorMesh>,
    shape: Shape,
    method: MethodSpecifier,
    mesh_name: String,
    signature: Option<Py<PyAny>>,
    proc_mesh: Option<Py<PyAny>>,
    propagator: Option<Py<PyAny>>,
    /// An endpoint outlives every invocation on it, so building its attributes
    /// once here keeps the per-invocation metrics allocation-free.
    attrs: Arc<EndpointAttrs>,
}

/// A Rust future that waits for endpoint replies and converts them into a
/// Python value, or returns a Python error. Rust callers can await it directly
/// without constructing a Python `Future`.
///
/// For `call`, `call_one` and `choose`, this future owns the reply receiver.
/// Dropping the Rust future closes its reply port; it does not cancel the
/// already-sent request.
///
/// Stream items share the stream's receiver. Dropping an unobserved item
/// leaves queued replies untouched. The reply port closes only when the last
/// owner of that shared receiver drops it.
pub type ReplyFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send + 'static>>;

impl ActorEndpoint {
    /// Send to exactly one actor and return a native future for its reply.
    /// Argument validation and sending happen before this returns. The future owns
    /// the response port; dropping it does not cancel the already-sent call.
    pub fn call_one_future<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<ReplyFuture> {
        Ok(Box::pin(Endpoint::call_one_future(self, py, args, kwargs)?))
    }

    /// Send to all actors and return a native future for their ValueMesh of replies.
    /// Argument validation and sending happen before this returns. The future owns
    /// the response port; dropping it does not cancel the already-sent call.
    pub fn call_future<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<ReplyFuture> {
        Ok(Box::pin(Endpoint::call_future(self, py, args, kwargs)?))
    }

    // HOT PATH: Be mindful of performance when making changes here.
    // To test how performance is affected by a change, run the RPC benchmarks in
    // `monarch/python/benches/`.
    fn create_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<EitherPortRef>,
        correlation_id: Option<u64>,
    ) -> PyResult<PendingMessage> {
        let port_ref_py: Py<PyAny> = match port_ref {
            Some(pr) => pr.clone().into_pyobject(py)?.unbind(),
            None => py.None(),
        };

        let result = create_endpoint_message(py).call1((
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
            correlation_id,
        ))?;
        let mut pending: PyRefMut<'_, PendingMessage> = result.extract()?;
        let message = pending.take()?;

        ENDPOINT_MESSAGE_SIZE_HISTOGRAM.record(message.payload_len() as f64, self.attrs.as_slice());

        Ok(message)
    }
}

impl Endpoint for ActorEndpoint {
    fn get_extent(&self, _py: Python<'_>) -> PyResult<Extent> {
        Ok(self.shape.extent())
    }

    fn get_method_name(&self) -> &str {
        self.method.name()
    }

    fn metric_attrs(&self) -> &Arc<EndpointAttrs> {
        &self.attrs
    }

    fn send_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<EitherPortRef>,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        correlation_id: Option<u64>,
    ) -> PyResult<()> {
        let message = self.create_message(py, args, kwargs, port_ref, correlation_id)?;
        self.inner.cast_unresolved(message, selection, instance)
    }

    fn send_message_with_headers<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<EitherPortRef>,
        selection: AllOrChoose,
        instance: &Instance<PythonActor>,
        caller_headers: hyperactor_config::Flattrs,
        correlation_id: Option<u64>,
    ) -> PyResult<()> {
        let message = self.create_message(py, args, kwargs, port_ref, correlation_id)?;
        self.inner
            .cast_unresolved_with_headers(message, selection, instance, caller_headers)
    }

    fn get_supervision_monitor(&self) -> Option<Arc<dyn Supervisable>> {
        Some(self.inner.clone())
    }

    fn get_qualified_name(&self) -> Option<String> {
        Some(format!("{}.{}()", self.mesh_name, self.method.name()))
    }

    fn enter_endpoint_span(
        &self,
        adverb: EndpointAdverb,
        actor_id: &ActorAddr,
        correlation_id: u64,
    ) -> SpanGuard {
        let mesh = self.mesh_name.as_str();
        let method = self.get_method_name();
        SpanGuard::actor_endpoint(adverb.as_str(), actor_id, mesh, method, correlation_id)
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
        // `mesh_name` is the raw name Python spawned the mesh under, so it takes
        // the same stripping that produced the actors' own label. Only the
        // metric is canonicalized; `mesh_name` keeps the user's spelling for
        // error messages and spans.
        let actor = Label::strip(&mesh_name);
        let attrs = Arc::new(EndpointAttrs::new(method.name(), Some(&actor)));
        Self {
            inner: actor_mesh.get_inner(),
            shape: shape.get_inner().clone(),
            method,
            mesh_name,
            signature,
            proc_mesh,
            propagator,
            attrs,
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
        PythonActorMesh::from_impl(self.inner.clone())
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
            .map_err(Into::into)
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

    /// Sends a message to a randomly selected actor and waits for its result.
    ///
    /// Each call independently selects an actor uniformly at random. Selection does
    /// not account for actor load, so calls are balanced only across many calls. Use
    /// `call_one` on a slice of the mesh when placement must be deterministic.
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
        port: Option<EitherPortRef>,
        selection: &str,
    ) -> PyResult<()> {
        let instance = self.get_current_instance(py)?;
        let sel = to_all_or_choose(selection)?;
        let correlation_id: u64 = fastrand::u64(..);
        tracing::info!(
            target: hyperactor_telemetry::sinks::perfetto::ENDPOINT_TELEMETRY_TARGET,
            actor_id = %instance.self_addr(),
            mesh = self.mesh_name.as_str(),
            method = self.get_method_name(),
            correlation_id,
            "send"
        );
        self.send_message(
            py,
            args,
            Some(kwargs),
            port,
            sel,
            &instance,
            Some(correlation_id),
        )
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
    attrs: Arc<EndpointAttrs>,
}

impl Endpoint for Remote {
    fn get_extent(&self, py: Python<'_>) -> PyResult<Extent> {
        let extent: PyExtent = self.inner.call_method0(py, "_get_extent")?.extract(py)?;
        Ok(extent.into())
    }

    fn get_method_name(&self) -> &str {
        UNKNOWN
    }

    fn metric_attrs(&self) -> &Arc<EndpointAttrs> {
        &self.attrs
    }

    fn send_message<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
        port_ref: Option<EitherPortRef>,
        selection: AllOrChoose,
        _instance: &Instance<PythonActor>,
        correlation_id: Option<u64>,
    ) -> PyResult<()> {
        let send_kwargs = PyDict::new(py);
        match port_ref {
            Some(pr) => send_kwargs.set_item("port", pr.clone())?,
            None => send_kwargs.set_item("port", py.None())?,
        }

        send_kwargs.set_item("selection", selection.as_str())?;
        send_kwargs.set_item("correlation_id", correlation_id)?;

        let kwargs_dict = kwargs.map_or_else(|| PyDict::new(py), |d| d.clone());
        self.inner
            .call_method(py, "_send", (args.clone(), kwargs_dict), Some(&send_kwargs))?;

        Ok(())
    }

    fn get_supervision_monitor(&self) -> Option<Arc<dyn Supervisable>> {
        None // Remote endpoints don't have supervision_monitors
    }

    fn get_qualified_name(&self) -> Option<String> {
        None // Remote endpoints don't have qualified names
    }

    fn enter_endpoint_span(
        &self,
        adverb: EndpointAdverb,
        actor_id: &ActorAddr,
        correlation_id: u64,
    ) -> SpanGuard {
        let call_name = monarch_with_gil_blocking(GilSite::DisplayName, |py| {
            self.inner
                .call_method0(py, "_call_name")
                .ok()
                .and_then(|v| v.extract::<String>(py).ok())
        });
        let call_name = call_name.as_deref().unwrap_or("");
        SpanGuard::remote(adverb.as_str(), actor_id, call_name, correlation_id)
    }
}

#[pymethods]
impl Remote {
    /// Create a new Remote wrapping a Python RemoteImpl object.
    #[new]
    fn new(remote: Py<PyAny>) -> Self {
        let attrs = Arc::new(EndpointAttrs::new(UNKNOWN, None));
        Self {
            inner: remote,
            attrs,
        }
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

    /// Sends a message to a randomly selected actor and waits for its result.
    ///
    /// Each call independently selects an actor uniformly at random. Selection does
    /// not account for actor load, so calls are balanced only across many calls. Use
    /// `call_one` on a slice of the mesh when placement must be deterministic.
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
    module.add_class::<PyValueStream>()?;
    module.add_class::<PyPendingReply>()?;
    module.add_class::<ActorEndpoint>()?;
    module.add_class::<Remote>()?;

    Ok(())
}

#[derive(Named)]
struct PythonResponseMessageReducer;

impl CommReducer for PythonResponseMessageReducer {
    type Update = PythonMessage;

    fn reduce(&self, left: Self::Update, right: Self::Update) -> anyhow::Result<Self::Update> {
        Ok(ValueOverlay::try_from_runs(rle::merge_value_runs(
            left.into_overlay()?.into_runs(),
            right.into_overlay()?.into_runs(),
        ))?
        .into())
    }
}

inventory::submit! {
    ReducerFactory {
        typehash_f: <PythonResponseMessageReducer as Named>::typehash,
        builder_f: |_| Ok(Box::new(PythonResponseMessageReducer)),
    }
}

struct PythonResponseMessageAccumulator;

impl Accumulator for PythonResponseMessageAccumulator {
    type State = PythonMessage;
    type Update = PythonMessage;

    fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()> {
        *state = ValueOverlay::try_from_runs(rle::merge_value_runs(
            std::mem::take(state).into_overlay()?.into_runs(),
            update.into_overlay()?.into_runs(),
        ))?
        .into();

        Ok(())
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        Some(ReducerSpec {
            typehash: <PythonResponseMessageReducer as Named>::typehash(),
            builder_params: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bytes::Bytes;
    use hyperactor::ActorAddr;
    use hyperactor::Endpoint as _;
    use hyperactor::Proc;
    use hyperactor::mailbox::MessageEnvelope;
    use hyperactor::mailbox::PortSender as _;
    use hyperactor::mailbox::Undeliverable;
    use hyperactor::mailbox::headers::OPERATION_ADVERB;
    use hyperactor::mailbox::headers::OPERATION_ENDPOINT;
    use pyo3::PyTypeInfo;
    use pyo3::exceptions::PyValueError;

    use super::*;
    use crate::pytokio::ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK;

    /// Minimal `Endpoint` impl that only serves `get_qualified_name`.
    /// The default `build_operation_context_headers` consults no other
    /// method, so the rest are unreachable.
    struct TestEndpoint {
        qualified_name: Option<String>,
    }

    impl Endpoint for TestEndpoint {
        fn get_extent(&self, _py: Python<'_>) -> PyResult<Extent> {
            unreachable!()
        }
        fn get_method_name(&self) -> &str {
            unreachable!()
        }
        fn metric_attrs(&self) -> &Arc<EndpointAttrs> {
            unreachable!()
        }
        fn send_message<'py>(
            &self,
            _py: Python<'py>,
            _args: &Bound<'py, PyTuple>,
            _kwargs: Option<&Bound<'py, PyDict>>,
            _port_ref: Option<EitherPortRef>,
            _selection: AllOrChoose,
            _instance: &Instance<PythonActor>,
            _correlation_id: Option<u64>,
        ) -> PyResult<()> {
            unreachable!()
        }
        fn get_supervision_monitor(&self) -> Option<Arc<dyn Supervisable>> {
            None
        }
        fn get_qualified_name(&self) -> Option<String> {
            self.qualified_name.clone()
        }
        fn enter_endpoint_span(
            &self,
            _adverb: EndpointAdverb,
            _actor_id: &ActorAddr,
            _correlation_id: u64,
        ) -> SpanGuard {
            unreachable!()
        }
    }

    /// OC-1 request-side producer: each `EndpointAdverb` maps to the
    /// expected wire adverb string, and the qualified endpoint name
    /// from `get_qualified_name()` flows through to `OPERATION_ENDPOINT`.
    #[test]
    fn test_rc1_build_operation_context_headers_stamps_each_adverb() {
        let ep = TestEndpoint {
            qualified_name: Some("training.Philosopher.ping()".to_string()),
        };
        for (adverb, expected_adverb) in [
            (EndpointAdverb::Call, "call"),
            (EndpointAdverb::CallOne, "call_one"),
            (EndpointAdverb::Choose, "choose"),
            (EndpointAdverb::Stream, "stream"),
        ] {
            let headers = ep.build_operation_context_headers(adverb);
            assert_eq!(
                headers.get(OPERATION_ENDPOINT).as_deref(),
                Some("training.Philosopher.ping()"),
                "adverb {:?}: OPERATION_ENDPOINT",
                adverb,
            );
            assert_eq!(
                headers.get(OPERATION_ADVERB).as_deref(),
                Some(expected_adverb),
                "adverb {:?}: OPERATION_ADVERB",
                adverb,
            );
        }
    }

    /// OC-1 request-side producer: when the endpoint has no
    /// qualified name (e.g. `Remote::get_qualified_name` returns
    /// `None`), `OPERATION_ENDPOINT` is omitted while `OPERATION_ADVERB`
    /// is still stamped.
    #[test]
    fn test_rc1_build_operation_context_headers_omits_endpoint_when_no_qualified_name() {
        let ep = TestEndpoint {
            qualified_name: None,
        };
        let headers = ep.build_operation_context_headers(EndpointAdverb::CallOne);
        assert!(
            headers.get(OPERATION_ENDPOINT).is_none(),
            "OPERATION_ENDPOINT must be absent when endpoint has no qualified name",
        );
        assert_eq!(
            headers.get(OPERATION_ADVERB).as_deref(),
            Some("call_one"),
            "OPERATION_ADVERB should still be stamped",
        );
    }

    const WAIT: Duration = Duration::from_secs(30);

    fn test_instance(name: &str) -> Instance<PythonActor> {
        Proc::isolated()
            .actor_instance::<PythonActor>(name)
            .expect("the test actor instance should be created")
            .instance
    }

    fn result_message(rank: Option<usize>) -> PythonMessage {
        PythonMessage {
            kind: PythonMessageKind::Result { rank },
            ..Default::default()
        }
    }

    fn unexpected_message(name: &str) -> (PythonMessage, String) {
        let kind = PythonMessageKind::CallMethod {
            name: MethodSpecifier::ReturnsResponse {
                name: name.to_string(),
            },
            response_port: None,
            correlation_id: None,
        };
        let error = format!("unexpected message kind {kind:?}");
        (
            PythonMessage {
                kind,
                ..Default::default()
            },
            error,
        )
    }

    #[pyclass]
    struct IdentityFuture;

    #[pymethods]
    impl IdentityFuture {
        #[staticmethod]
        fn _from_pending(reply: Py<PyPendingReply>) -> Py<PyPendingReply> {
            reply
        }

        #[staticmethod]
        fn _from_handle(handle: Py<PyHandle>) -> Py<PyHandle> {
            handle
        }
    }

    // The endpoint adverbs resolve `Future._from_handle` through its Python
    // module. The Rust unit-test target does not package that module, so expose
    // only the identity wrapper needed to exercise the real adverbs.
    fn install_future_test_module(py: Python<'_>) -> PyResult<()> {
        let modules = py
            .import("sys")?
            .getattr("modules")?
            .cast_into::<PyDict>()
            .map_err(PyErr::from)?;
        let mut path = String::new();
        let mut parent: Option<Bound<'_, PyModule>> = None;
        for part in "monarch._src.actor.future".split('.') {
            if !path.is_empty() {
                path.push('.');
            }
            path.push_str(part);
            let module = match modules.get_item(&path)? {
                Some(module) => module.cast_into::<PyModule>().map_err(PyErr::from)?,
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
        let module = parent.expect("the dotted module path is not empty");
        if module.getattr("Future").is_err() {
            module.setattr("Future", py.get_type::<IdentityFuture>())?;
        }
        Ok(())
    }

    /// Drive one item's collector inline, completing it before observing another.
    async fn drive_reply(reply: Py<PyPendingReply>) -> PyResult<Py<PyAny>> {
        let collector = monarch_with_gil_blocking(GilSite::Test, |py| reply.borrow(py).take())
            .expect("the item should be unconsumed")
            .0;
        tokio::time::timeout(WAIT, collector)
            .await
            .expect("timed out driving the reply collector")
    }

    fn assert_message_error(error: PyErr, expected: &str) {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(error.get_type(py).is(PyValueError::type_object(py)));
            assert_eq!(error.value(py).to_string(), expected);
        });
    }

    async fn assert_reply_message_error(reply: Py<PyPendingReply>, expected: &str) {
        let error = match drive_reply(reply).await {
            Ok(_) => panic!("the unexpected message kind should be rejected"),
            Err(error) => error,
        };
        assert_message_error(error, expected);
    }

    fn value_stream(
        instance: &Instance<PythonActor>,
        receiver: PortReceiver<PythonMessage>,
        remaining: usize,
    ) -> PyValueStream {
        let future_class = monarch_with_gil_blocking(GilSite::Test, |py| {
            py.get_type::<IdentityFuture>().into_any().unbind()
        });
        PyValueStream {
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
            supervision_monitor: None,
            instance: instance.clone_for_py(),
            remaining: AtomicUsize::new(remaining),
            attrs: Arc::new(EndpointAttrs::new("ownership_test", None)),
            qualified_endpoint_name: None,
            start: tokio::time::Instant::now(),
            future_class,
        }
    }

    /// Construct an item through the real iterator without starting its collector.
    fn next_reply(stream: &PyValueStream) -> Option<Py<PyPendingReply>> {
        monarch_with_gil_blocking(
            GilSite::Test,
            |py| -> PyResult<Option<Py<PyPendingReply>>> {
                stream
                    .__next__(py)?
                    .map(|reply| reply.extract(py).map_err(Into::<PyErr>::into))
                    .transpose()
            },
        )
        .expect("ValueStream.__next__ should construct its pending reply")
    }

    #[tokio::test]
    async fn value_stream_observers_receive_in_observation_order() {
        pyo3::Python::initialize();
        let instance = test_instance("value_stream_observers_receive_in_observation_order");
        let (handle, receiver) = instance.mailbox_for_py().open_port::<PythonMessage>();
        let (first_reply, first_error) = unexpected_message("first reply");
        let (second_reply, second_error) = unexpected_message("second reply");
        handle
            .try_post(&instance, first_reply)
            .expect("the first reply should be queued");
        handle
            .try_post(&instance, second_reply)
            .expect("the second reply should be queued");
        let stream = value_stream(&instance, receiver, 2);

        let first = next_reply(&stream).expect("the first slot should yield an item");
        let second = next_reply(&stream).expect("the second slot should yield an item");

        assert_reply_message_error(second, &first_error).await;
        assert_reply_message_error(first, &second_error).await;
        assert!(
            next_reply(&stream).is_none(),
            "the two constructed observers should exhaust the stream"
        );
    }

    #[tokio::test]
    async fn dropped_value_stream_observer_leaves_reply_but_burns_slot_stranding_tail() {
        pyo3::Python::initialize();
        let instance = test_instance("dropped_value_stream_observer_leaves_reply_but_burns_slot");
        let (handle, receiver) = instance.mailbox_for_py().open_port::<PythonMessage>();
        let (first_reply, first_error) = unexpected_message("first reply");
        let (second_reply, _) = unexpected_message("second reply");
        let second_kind = second_reply.kind.clone();
        handle
            .try_post(&instance, first_reply)
            .expect("the first reply should be queued");
        handle
            .try_post(&instance, second_reply)
            .expect("the second reply should be queued");
        let stream = value_stream(&instance, receiver, 2);

        let discarded = next_reply(&stream).expect("the first slot should yield an item");
        monarch_with_gil_blocking(GilSite::Test, |_py| drop(discarded));

        let retained = next_reply(&stream).expect("the second slot should yield an item");
        assert_reply_message_error(retained, &first_error).await;
        assert!(
            next_reply(&stream).is_none(),
            "discarding an observer should still consume its iteration slot"
        );

        let stranded = stream
            .receiver
            .lock()
            .await
            .try_recv()
            .expect("the shared receiver should remain usable")
            .expect("the final reply should remain queued but unreachable by iteration");
        assert_eq!(stranded.kind, second_kind);
    }

    // Both observation methods must reject a collector already taken. The
    // block_on rejection happens before it could block this Tokio test thread.
    #[tokio::test]
    async fn consumed_stream_item_rejects_both_observation_methods() {
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let reply = Py::new(
                py,
                PyPendingReply::for_stream_item(std::future::ready(Ok(py.None())), None),
            )
            .expect("the pending reply should be allocated");
            drop(
                reply
                    .borrow(py)
                    .take()
                    .expect("the first take should succeed"),
            );

            let expected = "this stream item was already taken by an earlier get(); if that get() was interrupted, it will never produce a result";
            let error = reply
                .borrow(py)
                .spawn_handle()
                .err()
                .expect("spawn_handle should reject the consumed item");
            assert_message_error(error, expected);
            let error = PyPendingReply::block_on(reply.borrow(py), py)
                .expect_err("block_on should reject the consumed item before waiting");
            assert_message_error(error, expected);
        });
    }

    struct RecordingCallEndpoint {
        instance: Instance<PythonActor>,
        attrs: Arc<EndpointAttrs>,
        submissions: AtomicUsize,
        response_ports: Mutex<Vec<EitherPortRef>>,
    }

    impl RecordingCallEndpoint {
        fn new(instance: Instance<PythonActor>) -> Self {
            Self {
                instance,
                attrs: Arc::new(EndpointAttrs::new("ownership_test", None)),
                submissions: AtomicUsize::new(0),
                response_ports: Mutex::new(Vec::new()),
            }
        }

        fn take_recorded_port(&self) -> EitherPortRef {
            self.response_ports
                .lock()
                .expect("the response-port recorder should not be poisoned")
                .pop()
                .expect("call should have supplied a response port")
        }
    }

    impl Endpoint for RecordingCallEndpoint {
        fn get_extent(&self, _py: Python<'_>) -> PyResult<Extent> {
            Ok(Extent::unity())
        }

        fn get_method_name(&self) -> &str {
            "ownership_test"
        }

        fn metric_attrs(&self) -> &Arc<EndpointAttrs> {
            &self.attrs
        }

        fn send_message<'py>(
            &self,
            _py: Python<'py>,
            _args: &Bound<'py, PyTuple>,
            _kwargs: Option<&Bound<'py, PyDict>>,
            port_ref: Option<EitherPortRef>,
            _selection: AllOrChoose,
            _instance: &Instance<PythonActor>,
            _correlation_id: Option<u64>,
        ) -> PyResult<()> {
            let port = port_ref.expect("call and call_one should supply a response port");
            self.response_ports
                .lock()
                .expect("the response-port recorder should not be poisoned")
                .push(port);
            self.submissions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn get_supervision_monitor(&self) -> Option<Arc<dyn Supervisable>> {
            None
        }

        fn get_qualified_name(&self) -> Option<String> {
            Some("ownership_test.call()".to_string())
        }

        fn enter_endpoint_span(
            &self,
            adverb: EndpointAdverb,
            actor_id: &ActorAddr,
            correlation_id: u64,
        ) -> SpanGuard {
            SpanGuard::actor_endpoint(
                adverb.as_str(),
                actor_id,
                "ownership_test_mesh",
                "ownership_test",
                correlation_id,
            )
        }

        fn get_current_instance(&self, _py: Python<'_>) -> PyResult<Instance<PythonActor>> {
            Ok(self.instance.clone_for_py())
        }
    }

    fn call_observer(endpoint: &RecordingCallEndpoint, adverb: EndpointAdverb) -> Py<PyHandle> {
        monarch_with_gil_blocking(GilSite::Test, |py| -> PyResult<Py<PyHandle>> {
            install_future_test_module(py)?;
            let args = PyTuple::empty(py);
            let observer = match adverb {
                EndpointAdverb::Call => endpoint.call(py, &args, None)?,
                EndpointAdverb::CallOne => endpoint.call_one(py, &args, None)?,
                EndpointAdverb::Choose => endpoint.choose(py, &args, None)?,
                _ => unreachable!("this witness covers ordinary reply collectors"),
            };
            observer.extract(py).map_err(Into::<PyErr>::into)
        })
        .expect("call should return an observer")
    }

    /// Exercise the shared construction used by the native methods, without the
    /// Python wrapper. This fixture records sending rather than running an actor.
    fn native_collector(endpoint: &RecordingCallEndpoint, adverb: EndpointAdverb) -> ReplyFuture {
        monarch_with_gil_blocking(GilSite::Test, |py| -> PyResult<ReplyFuture> {
            let args = PyTuple::empty(py);
            match adverb {
                EndpointAdverb::Call => {
                    Ok(Box::pin(Endpoint::call_future(endpoint, py, &args, None)?))
                }
                EndpointAdverb::CallOne => Ok(Box::pin(Endpoint::call_one_future(
                    endpoint, py, &args, None,
                )?)),
                _ => unreachable!("this witness covers call and call_one"),
            }
        })
        .expect("native construction should send and return its collector")
    }

    /// Both collectors own their reply port before the first poll. Successful
    /// delivery is the positive control for the later dropped-port assertion.
    async fn assert_native_collector_ownership(adverb: EndpointAdverb) {
        pyo3::Python::initialize();
        let instance = test_instance(adverb.as_str());
        let endpoint = RecordingCallEndpoint::new(instance.clone_for_py());
        let retained = native_collector(&endpoint, adverb);
        assert_eq!(endpoint.submissions.load(Ordering::SeqCst), 1);
        let retained_port = endpoint.take_recorded_port();
        match retained_port {
            EitherPortRef::Once(PythonOncePortRef { inner: Some(port) }) => {
                let (return_handle, _return_receiver) = instance
                    .mailbox_for_py()
                    .open_port::<Undeliverable<MessageEnvelope>>();
                instance
                    .mailbox_for_py()
                    .serialize_and_send_once(port, result_message(Some(0)), return_handle)
                    .expect("the retained call reply should serialize");
                let collected = tokio::time::timeout(WAIT, retained)
                    .await
                    .expect("the retained call should finish")
                    .expect("the retained call should collect its reply");
                monarch_with_gil_blocking(GilSite::Test, |py| {
                    collected
                        .extract::<Py<PyValueMesh>>(py)
                        .expect("call should return a ValueMesh");
                });
            }
            EitherPortRef::Unbounded(port) => {
                // An unexpected message produces a specific error before unpickling.
                // That exact error proves this reply reached the retained collector.
                let (reply, expected_error) = unexpected_message("native call_one reply");
                port.inner.post(&instance, reply);
                let error = tokio::time::timeout(WAIT, retained)
                    .await
                    .expect("the retained call_one should finish")
                    .expect_err("the unexpected reply kind should fail collection");
                assert_message_error(error, &expected_error);
            }
            _ => panic!("the recorded response port should be populated"),
        }

        let discarded = native_collector(&endpoint, adverb);
        assert_eq!(endpoint.submissions.load(Ordering::SeqCst), 2);
        let dropped_port = endpoint.take_recorded_port();
        drop(discarded);
        let (return_handle, mut return_receiver) = instance
            .mailbox_for_py()
            .open_port::<Undeliverable<MessageEnvelope>>();
        let dropped_destination = match dropped_port {
            EitherPortRef::Once(PythonOncePortRef { inner: Some(port) }) => {
                let destination = port.port_addr().clone();
                instance
                    .mailbox_for_py()
                    .serialize_and_send_once(port, result_message(Some(0)), return_handle)
                    .expect("the discarded call reply should serialize");
                destination
            }
            EitherPortRef::Unbounded(port) => {
                let destination = port.inner.port_addr().clone();
                instance
                    .mailbox_for_py()
                    .serialize_and_send(&port.inner, result_message(None), return_handle)
                    .expect("the discarded call_one reply should serialize");
                destination
            }
            _ => panic!("the recorded response port should be populated"),
        };
        let undeliverable = tokio::time::timeout(WAIT, return_receiver.recv())
            .await
            .expect("the dropped port must report delivery failure within the deadline")
            .expect("the return port should receive the delivery failure");
        let Undeliverable::Returned(envelope) = undeliverable else {
            panic!("the failed response should retain its envelope");
        };
        assert_eq!(envelope.dest(), &dropped_destination);
        assert_eq!(endpoint.submissions.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn native_call_sends_before_poll_and_drop_closes_response_port() {
        assert_native_collector_ownership(EndpointAdverb::Call).await;
    }

    #[tokio::test]
    async fn native_call_one_sends_before_poll_and_drop_closes_response_port() {
        assert_native_collector_ownership(EndpointAdverb::CallOne).await;
    }

    /// Polling does not drive a Handle. Completion here proves collection ran
    /// without an await or get; each delay leaves the GIL free for decoding.
    async fn poll_reply(handle: &Py<PyHandle>) -> PyResult<Py<PyAny>> {
        tokio::time::timeout(WAIT, async {
            loop {
                match monarch_with_gil_blocking(GilSite::Test, |py| {
                    let value = handle.call_method0(py, "poll")?;
                    Ok::<_, PyErr>((!value.is_none(py)).then_some(value))
                }) {
                    Ok(Some(value)) => return Ok(value),
                    Err(error) => return Err(error),
                    Ok(None) => tokio::time::sleep(Duration::from_millis(1)).await,
                }
            }
        })
        .await
        .expect("reply collection must finish without awaiting its Handle")
    }

    /// The multipart codec shares these bytes rather than copying them. Their
    /// final drop acknowledges that the reply and its unobserved result are gone.
    /// A queued reply or an Undeliverable envelope would still own the bytes.
    struct ReplyBytes(Option<tokio::sync::oneshot::Sender<()>>);

    impl AsRef<[u8]> for ReplyBytes {
        fn as_ref(&self) -> &[u8] {
            b"\x80\x04N."
        }
    }

    impl Drop for ReplyBytes {
        fn drop(&mut self) {
            if let Some(consumed) = self.0.take() {
                let _ = consumed.send(());
            }
        }
    }

    async fn assert_python_collector_ownership(adverb: EndpointAdverb) {
        pyo3::Python::initialize();
        let instance = test_instance(adverb.as_str());
        let endpoint = RecordingCallEndpoint::new(instance.clone_for_py());

        let retained = call_observer(&endpoint, adverb);
        assert_eq!(
            endpoint.submissions.load(Ordering::SeqCst),
            1,
            "submission should happen before the adverb returns"
        );
        let retained_port = endpoint.take_recorded_port();
        let (return_handle, _return_receiver) = instance
            .mailbox_for_py()
            .open_port::<Undeliverable<MessageEnvelope>>();
        match retained_port {
            EitherPortRef::Once(PythonOncePortRef { inner: Some(port) }) => {
                instance
                    .mailbox_for_py()
                    .serialize_and_send_once(port, result_message(Some(0)), return_handle)
                    .expect("the retained call reply should serialize");
                let collected = poll_reply(&retained)
                    .await
                    .expect("call should collect its reply");
                monarch_with_gil_blocking(GilSite::Test, |py| {
                    collected
                        .extract::<Py<PyValueMesh>>(py)
                        .expect("call should return a ValueMesh");
                });
            }
            EitherPortRef::Unbounded(port) => {
                let (reply, expected_error) = unexpected_message("eager single reply");
                port.inner.post(&instance, reply);
                let error = poll_reply(&retained)
                    .await
                    .expect_err("the unexpected reply kind should fail collection");
                assert_message_error(error, &expected_error);
            }
            _ => panic!("the recorded response port should be populated"),
        }

        let discarded = call_observer(&endpoint, adverb);
        assert_eq!(
            endpoint.submissions.load(Ordering::SeqCst),
            2,
            "the second operation should also be submitted before observation"
        );
        let dropped_port = endpoint.take_recorded_port();
        monarch_with_gil_blocking(GilSite::Test, |_py| drop(discarded));

        let (consumed, mut acknowledgement) = tokio::sync::oneshot::channel();
        let payload = Part::from(Bytes::from_owner(ReplyBytes(Some(consumed))));
        let (return_handle, mut return_receiver) = instance
            .mailbox_for_py()
            .open_port::<Undeliverable<MessageEnvelope>>();
        // Keep the return port connected until we check for a bounced reply.
        // Each send consumes its own clone of the sender.
        match dropped_port {
            EitherPortRef::Once(PythonOncePortRef { inner: Some(port) }) => {
                let mut reply = result_message(Some(0));
                reply.message = payload;
                instance
                    .mailbox_for_py()
                    .serialize_and_send_once(port, reply, return_handle.clone())
                    .expect("the unobserved call reply should serialize");
            }
            EitherPortRef::Unbounded(port) => {
                let (mut reply, _) = unexpected_message("unobserved single reply");
                reply.message = payload;
                instance
                    .mailbox_for_py()
                    .serialize_and_send(&port.inner, reply, return_handle.clone())
                    .expect("the unobserved single reply should serialize");
            }
            _ => panic!("the recorded response port should be populated"),
        }
        tokio::time::timeout(WAIT, async {
            loop {
                tokio::select! {
                    completion = &mut acknowledgement => break completion,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        // PyO3 may defer destruction of the discarded ValueMesh
                        // until a thread next acquires the GIL.
                        monarch_with_gil_blocking(GilSite::Test, |_py| {});
                    }
                }
            }
        })
        .await
        .expect("the collector must consume the reply after its observer is dropped")
        .expect("reply destruction should acknowledge completion");
        assert!(
            return_receiver
                .try_recv()
                .expect("the return port should remain open")
                .is_none(),
            "an unobserved reply should not bounce back to the replying actor"
        );
        drop(return_handle);
        assert_eq!(endpoint.submissions.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn call_collects_before_observation_and_survives_observer_drop() {
        assert_python_collector_ownership(EndpointAdverb::Call).await;
    }

    #[tokio::test]
    async fn call_one_collects_before_observation_and_survives_observer_drop() {
        assert_python_collector_ownership(EndpointAdverb::CallOne).await;
    }

    #[tokio::test]
    async fn choose_collects_before_observation_and_survives_observer_drop() {
        assert_python_collector_ownership(EndpointAdverb::Choose).await;
    }

    // With ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK set, each adverb keeps its
    // creation traceback, so an unobserved failure still logs where the call
    // was made. Without it, none is captured.
    #[tokio::test]
    async fn adverbs_capture_creation_traceback_only_when_enabled() {
        pyo3::Python::initialize();
        let instance = test_instance("adverbs_capture_creation_traceback_only_when_enabled");
        let endpoint = RecordingCallEndpoint::new(instance.clone_for_py());
        let config = hyperactor_config::global::lock();
        for adverb in [
            EndpointAdverb::Call,
            EndpointAdverb::CallOne,
            EndpointAdverb::Choose,
        ] {
            for enabled in [false, true] {
                let _guard = config.override_key(ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK, enabled);
                let observer = call_observer(&endpoint, adverb);
                let captured = monarch_with_gil_blocking(GilSite::Test, |py| {
                    let captured = observer.borrow(py).has_traceback_for_test();
                    drop(observer);
                    captured
                });
                assert_eq!(
                    captured,
                    enabled,
                    "{} should capture a creation traceback only when enabled",
                    adverb.as_str()
                );
            }
        }
    }

    // Construction captures the same opt-in stack as PythonTask did, and
    // starting the item transfers it to the Handle's unobserved-error log.
    #[tokio::test]
    async fn stream_items_capture_creation_traceback_only_when_enabled() {
        pyo3::Python::initialize();
        let instance = test_instance("stream_items_capture_creation_traceback_only_when_enabled");
        let config = hyperactor_config::global::lock();
        for enabled in [false, true] {
            let _guard = config.override_key(ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK, enabled);
            let (sender, receiver) = instance.mailbox_for_py().open_port::<PythonMessage>();
            let (message, expected) = unexpected_message("traceback reply");
            // Queue the reply before starting the collector, so it cannot be
            // left waiting when this iteration ends.
            sender
                .try_post(&instance, message)
                .expect("the reply should be queued");
            let stream = value_stream(&instance, receiver, 1);
            let reply = next_reply(&stream).expect("the slot should yield an item");
            let observer =
                monarch_with_gil_blocking(GilSite::Test, |py| reply.borrow(py).spawn_handle())
                    .expect("the unconsumed item should start its collector");
            assert_eq!(
                observer.has_traceback_for_test(),
                enabled,
                "a stream item should capture a creation traceback only when enabled"
            );
            let error = tokio::time::timeout(WAIT, observer.wait_future())
                .await
                .expect("the queued reply should complete the collector")
                .expect_err("the unexpected message kind should be rejected");
            assert_message_error(error, &expected);
        }
    }
}
