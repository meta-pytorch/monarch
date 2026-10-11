/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod async_actor;
mod sync_actor;

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::SystemTime;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorEnvironment;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::MessageStatusReporting;
use hyperactor::OncePortHandle;
use hyperactor::PortHandle;
use hyperactor::Proc;
use hyperactor::RemoteSpawn;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Signal;
use hyperactor::context::Actor as ContextActor;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::UndeliverableMessageError;
use hyperactor::mailbox::UndeliverableReason;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use hyperactor_mesh::ProcMeshRef;
use hyperactor_mesh::actor_mesh::ACTOR_MESH_ID;
use hyperactor_mesh::actor_mesh::ActorMeshRef;
use hyperactor_mesh::casting::CAST_POINT;
use hyperactor_mesh::casting::CastInfo;
use hyperactor_mesh::casting::update_undeliverable_envelope_for_casting;
use hyperactor_mesh::host_mesh::HostMeshRef;
use hyperactor_mesh::introspect::ActiveHandler;
use hyperactor_mesh::introspect::EXECUTION;
use hyperactor_mesh::introspect::Execution;
use hyperactor_mesh::supervision::MeshFailure;
use hyperactor_mesh::transport::default_bind_spec;
use hyperactor_mesh::value_mesh::ValueOverlay;
use monarch_types::PickledPyObject;
use monarch_types::SerializablePyErr;
use ndslice::Point;
use ndslice::extent;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::types::PyType;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use typeuri::Named;

use crate::buffers::FrozenBuffer;
use crate::context::PyContext;
use crate::context::PyInstance;
use crate::handle::PyHandleCompleter;
use crate::handle::handle_pair;
// Preserve the crate-local conversion path used by host_mesh.
pub(crate) use crate::handle::to_py_error;
use crate::local_state_broker::BrokerId;
use crate::local_state_broker::LocalStateBrokerMessage;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PyMailbox;
use crate::mailbox::PythonUndeliverableMessageEnvelope;
use crate::pickle::PicklingState;
use crate::pickle::pickle_to_part;
use crate::proc::PyActorAddr;
use crate::pympsc::Sender;
use crate::runtime::GilSite;
use crate::runtime::get_tokio_runtime;
use crate::runtime::mark_actor_driver_thread;
use crate::runtime::mark_actor_event_loop_thread;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::supervision::PyMeshFailure;

/// Whether a Python actor is sync (it declares at least one endpoint, and every
/// endpoint is a plain `def`) or async. Python's `_actor_kind` decides it for a
/// class spawned on a ProcMesh; every other construction route states it
/// (SA-5, defined in `monarch._src.actor.actor_mesh`).
#[pyclass(
    eq,
    eq_int,
    frozen,
    module = "monarch._rust_bindings.monarch_hyperactor.actor"
)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ActorKind {
    /// An actor with at least one endpoint, all declared with `def`.
    #[pyo3(name = "SYNC")]
    Sync,
    /// Any other actor, including one with no endpoints.
    #[pyo3(name = "ASYNC")]
    Async,
}

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum UnflattenArg {
    Mailbox,
    PyObject,
}

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MethodSpecifier {
    /// Call method 'name', send its return value to the response port.
    ReturnsResponse { name: String },
    /// Call method 'name', send the response port as the first argument.
    ExplicitPort { name: String },
    /// Construct the object
    Init {},
}

impl std::fmt::Display for MethodSpecifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[pymethods]
impl MethodSpecifier {
    #[getter(name)]
    fn py_name(&self) -> &str {
        self.name()
    }
}

impl MethodSpecifier {
    pub(crate) fn name(&self) -> &str {
        match self {
            MethodSpecifier::ReturnsResponse { name } => name,
            MethodSpecifier::ExplicitPort { name } => name,
            MethodSpecifier::Init {} => "__init__",
        }
    }
}

/// The payload of a single actor response, without rank information.
///
/// The rank is captured by the overlay's range key, so it is stripped
/// from the value to enable RLE dedup: two ranks returning the same
/// payload will have byte-identical values and can be coalesced into
/// a single run.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub enum PythonResponseMessage {
    Result {
        part: serde_multipart::Part,
        refs: Vec<MeshRef>,
    },
    Exception {
        part: serde_multipart::Part,
        refs: Vec<MeshRef>,
    },
}

wirevalue::register_type!(PythonResponseMessage);
wirevalue::register_type!(ValueOverlay<PythonResponseMessage>);

impl PythonResponseMessage {
    /// Decode this response's payload, reuniting its out-of-band `refs` table
    /// so mesh references reconstruct. Mirrors [`PythonMessage::decode`] for the
    /// accumulated (valuemesh / `.call()`) path.
    /// `instance` is the receiving actor, required so a `Port` in the payload
    /// reconstructs against it rather than falling back to `context()` on a
    /// Tokio worker. Mandatory rather than optional: this is only ever called
    /// from an eager collector, all of which hold the caller instance.
    pub(crate) fn decode(
        &self,
        py: Python<'_>,
        instance: &Instance<PythonActor>,
    ) -> PyResult<Py<PyAny>> {
        let (part, refs) = match self {
            PythonResponseMessage::Result { part, refs }
            | PythonResponseMessage::Exception { part, refs } => (part, refs),
        };
        let mesh_references = refs.iter().cloned().map(Some).collect();
        let mut state = PicklingState::from_parts(part.clone(), VecDeque::new(), mesh_references);
        state.unpickle_with_receiver(py, instance)
    }
}

/// Newtype wrapper around [`ValueOverlay<PythonResponseMessage>`] needed
/// because `PythonMessageKind` is a `#[pyclass]` enum, requiring all variant
/// fields to implement PyO3 traits. `ValueOverlay` is defined in another crate
/// and does not implement `PyClass`.
#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AccumulatedResponses(ValueOverlay<PythonResponseMessage>);

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq)]
pub enum PythonMessageKind {
    #[pyo3(constructor = (name, response_port, correlation_id=None))]
    CallMethod {
        name: MethodSpecifier,
        response_port: Option<EitherPortRef>,
        correlation_id: Option<u64>,
    },
    Result {
        rank: Option<usize>,
    },
    Exception {
        rank: Option<usize>,
    },
    Uninit {},
    #[pyo3(constructor = (name, local_state_broker, id, unflatten_args, correlation_id=None))]
    CallMethodIndirect {
        name: MethodSpecifier,
        local_state_broker: (String, usize),
        id: usize,
        // specify whether the argument to unflatten the local mailbox,
        // or the next argument of the local state.
        unflatten_args: Vec<UnflattenArg>,
        correlation_id: Option<u64>,
    },
    AccumulatedResponses(AccumulatedResponses),
}
wirevalue::register_type!(PythonMessageKind);

impl Default for PythonMessageKind {
    fn default() -> Self {
        PythonMessageKind::Uninit {}
    }
}

/// A serializable reference to a mesh (actor, proc, or host).
///
/// Serialized as a typed multipart part via [`MeshRefRepr`]: under the multipart
/// serializer each `MeshRef` becomes its own typed part, and inline bincode
/// elsewhere.
#[derive(Clone, Debug, Named, PartialEq, Eq)]
pub enum MeshRef {
    Actor(Box<ActorMeshRef<PythonActor>>),
    Proc(Box<ProcMeshRef>),
    Host(Box<HostMeshRef>),
}

/// Wire representation of [`MeshRef`] stored in a typed multipart part.
#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub enum MeshRefRepr {
    Actor(Box<ActorMeshRef<PythonActor>>),
    Proc(Box<ProcMeshRef>),
    Host(Box<HostMeshRef>),
}

impl TryFrom<&MeshRef> for MeshRefRepr {
    type Error = serde_multipart::Error;
    fn try_from(m: &MeshRef) -> serde_multipart::Result<Self> {
        Ok(match m {
            MeshRef::Actor(r) => MeshRefRepr::Actor(r.clone()),
            MeshRef::Proc(r) => MeshRefRepr::Proc(r.clone()),
            MeshRef::Host(r) => MeshRefRepr::Host(r.clone()),
        })
    }
}

impl TryFrom<MeshRefRepr> for MeshRef {
    type Error = serde_multipart::Error;
    fn try_from(r: MeshRefRepr) -> serde_multipart::Result<Self> {
        Ok(match r {
            MeshRefRepr::Actor(r) => MeshRef::Actor(r),
            MeshRefRepr::Proc(r) => MeshRef::Proc(r),
            MeshRefRepr::Host(r) => MeshRef::Host(r),
        })
    }
}

serde_multipart::part_codec! {
    impl MeshRef
    {
        type Repr = MeshRefRepr;
    }
}

impl MeshRef {
    /// Reconstruct the Python mesh wrapper this reference points at.
    ///
    /// Mirrors the `py_*_from_bytes` reconstructors, but takes an
    /// already-deserialized [`MeshRef`] from the message's `refs` table.
    pub(crate) fn reconstruct(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            MeshRef::Proc(r) => {
                Ok(Py::new(py, crate::proc_mesh::PyProcMesh::new_ref(*r))?.into_any())
            }
            MeshRef::Host(r) => {
                Ok(Py::new(py, crate::host_mesh::PyHostMesh::new_ref(*r))?.into_any())
            }
            MeshRef::Actor(r) => {
                let inner = crate::actor_mesh::PythonActorMeshImpl::new_ref(*r);
                let async_mesh = crate::actor_mesh::AsyncActorMesh::from_impl(Arc::new(inner));
                let mesh = crate::actor_mesh::PythonActorMesh::from_impl(Arc::from(async_mesh));
                Ok(Py::new(py, mesh)?.into_any())
            }
        }
    }
}

/// An opaque carrier so a `MeshRef` can ride in `PythonMessage.refs` across
/// the Python boundary (the message getter out, the `PicklingState` ctor in).
#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub struct PyMeshRef {
    pub(crate) inner: MeshRef,
}

impl<'py> IntoPyObject<'py> for MeshRef {
    type Target = PyMeshRef;
    type Output = Bound<'py, PyMeshRef>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(py, PyMeshRef { inner: self })
    }
}

/// Extract the serializable [`MeshRef`] from a resolved mesh wrapper (the
/// inverse of [`MeshRef::reconstruct`]), for the sender-side pending fill.
pub(crate) fn mesh_ref_from_pyobject(value: &Bound<'_, PyAny>) -> PyResult<MeshRef> {
    if let Ok(m) = value.cast::<crate::proc_mesh::PyProcMesh>() {
        return Ok(MeshRef::Proc(Box::new(m.borrow().mesh_ref()?)));
    }
    if let Ok(m) = value.cast::<crate::host_mesh::PyHostMesh>() {
        return Ok(MeshRef::Host(Box::new(m.borrow().mesh_ref().map_err(
            |e| pyo3::exceptions::PyValueError::new_err(e.to_string()),
        )?)));
    }
    if let Ok(m) = value.cast::<crate::actor_mesh::PythonActorMesh>() {
        return Ok(MeshRef::Actor(Box::new(m.borrow().get_inner().mesh_ref()?)));
    }
    Err(pyo3::exceptions::PyRuntimeError::new_err(
        "pending pickle did not resolve to a mesh reference",
    ))
}

#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Clone, Serialize, Deserialize, Named, Default, PartialEq)]
pub struct PythonMessage {
    pub kind: PythonMessageKind,
    pub message: Part,
    /// Mesh references carried out-of-band from the pickled `message`.
    pub refs: Vec<MeshRef>,
}

/// Extract the endpoint method name from a [`PythonMessage`].
fn python_message_endpoint_name(msg: &PythonMessage) -> Option<String> {
    match &msg.kind {
        PythonMessageKind::CallMethod { name, .. }
        | PythonMessageKind::CallMethodIndirect { name, .. } => Some(name.name().to_string()),
        _ => None,
    }
}

// We use manual `submit!` instead of `register_type!` because PythonMessage is a
// struct, so the default `endpoint_name` (which delegates to `arm_unchecked`)
// always returns None. The custom implementation inspects `PythonMessageKind` to
// extract the method name. This registration handles direct (non-cast) dispatch.
wirevalue::submit! {
    wirevalue::TypeInfo {
        typename: <PythonMessage as wirevalue::Named>::typename,
        typehash: <PythonMessage as wirevalue::Named>::typehash,
        typeid: <PythonMessage as wirevalue::Named>::typeid,
        port: <PythonMessage as wirevalue::Named>::port,
        dump: Some(<PythonMessage as wirevalue::NamedDumpable>::dump),
        arm_unchecked: <PythonMessage as wirevalue::Named>::arm_unchecked,
        endpoint_name: |ptr| {
            // SAFETY: ptr points to a PythonMessage.
            let msg = unsafe { &*(ptr as *const PythonMessage) };
            python_message_endpoint_name(msg)
        },
    }
}

impl From<ValueOverlay<PythonResponseMessage>> for PythonMessage {
    fn from(overlay: ValueOverlay<PythonResponseMessage>) -> Self {
        PythonMessage {
            kind: PythonMessageKind::AccumulatedResponses(AccumulatedResponses(overlay)),
            message: Default::default(),
            refs: Vec::new(),
        }
    }
}

impl PythonMessage {
    /// Consume this message and extract a `ValueOverlay<PythonResponseMessage>`.
    ///
    /// Handles both already-collected responses and leaf `Result`/`Exception`
    /// messages by wrapping them in a single-run overlay.
    pub fn into_overlay(self) -> anyhow::Result<ValueOverlay<PythonResponseMessage>> {
        match self.kind {
            PythonMessageKind::AccumulatedResponses(overlay) => Ok(overlay.0),
            PythonMessageKind::Result { rank, .. } => {
                let rank = rank.expect("accumulated response should have a rank");
                let mut overlay = ValueOverlay::new();
                overlay.push_run(
                    rank..rank + 1,
                    PythonResponseMessage::Result {
                        part: self.message,
                        refs: self.refs,
                    },
                )?;
                Ok(overlay)
            }
            PythonMessageKind::Exception { rank, .. } => {
                let rank = rank.expect("accumulated exception should have a rank");
                let mut overlay = ValueOverlay::new();
                overlay.push_run(
                    rank..rank + 1,
                    PythonResponseMessage::Exception {
                        part: self.message,
                        refs: self.refs,
                    },
                )?;
                Ok(overlay)
            }
            other => {
                anyhow::bail!(
                    "unexpected message kind {:?} in collected responses reducer",
                    other
                );
            }
        }
    }
}

struct ResolvedCallMethod {
    method: MethodSpecifier,
    bytes: FrozenBuffer,
    local_state: PendingLocalState,
    mesh_references: Vec<MeshRef>,
    /// Implements PortProtocol
    /// Concretely either a Port, DroppingPort, or LocalPort
    response_port: ResponsePort,
    correlation_id: Option<u64>,
}

enum ResponsePort {
    Dropping,
    Port(Port),
    Local(LocalPort),
}

impl ResponsePort {
    fn into_py_any(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            ResponsePort::Dropping => DroppingPort.into_py_any(py),
            ResponsePort::Port(port) => port.into_py_any(py),
            ResponsePort::Local(port) => port.into_py_any(py),
        }
    }
}

enum PendingLocalState {
    Empty,
    Indirect {
        mailbox: PyMailbox,
        unflatten_args: Vec<UnflattenArg>,
        state: Vec<Py<PyAny>>,
    },
}

impl PendingLocalState {
    fn into_py_any(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            PendingLocalState::Empty => Ok(PyList::empty(py).into_any().unbind()),
            PendingLocalState::Indirect {
                mailbox,
                unflatten_args,
                state,
            } => {
                let mailbox = mailbox.into_bound_py_any(py)?;
                let mut state = state.into_iter();
                let items = unflatten_args.into_iter().map(|arg| match arg {
                    UnflattenArg::Mailbox => mailbox.clone(),
                    UnflattenArg::PyObject => state
                        .next()
                        .expect("local state broker should return an object per PyObject arg")
                        .into_bound(py),
                });
                Ok(PyList::new(py, items)?.into_any().unbind())
            }
        }
    }
}

/// A resolved message queued without taking the GIL. The actor's Python
/// execution thread converts it into a [`QueuedMessage`]. Taking the GIL on the
/// Tokio worker instead can leave the runtime's I/O driver unpolled while
/// another thread holds the GIL, stalling unrelated Rust tasks in the proc
/// (https://github.com/meta-pytorch/monarch/issues/4938).
struct PendingMessage {
    instance: Arc<Py<PyInstance>>,
    rank: Point,
    recording_span: tracing::Span,
    resolved: ResolvedCallMethod,
    telemetry_message_id: Option<u64>,
}

impl<'py> IntoPyObject<'py> for PendingMessage {
    type Target = QueuedMessage;
    type Output = Bound<'py, QueuedMessage>;
    type Error = PyErr;

    /// On failure, reports the message as failed: `handle` has already
    /// returned, and no `QueuedMessage` exists to call `_report_failed`.
    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let telemetry_message_id = self.telemetry_message_id;
        self.into_queued(py)
            .inspect_err(|_| report_message_status(telemetry_message_id, "failed"))
    }
}

impl PendingMessage {
    fn into_queued(self, py: Python<'_>) -> PyResult<Bound<'_, QueuedMessage>> {
        let context = crate::context::PyContext::from_parts(
            self.instance.clone_ref(py),
            self.rank,
            Some(self.recording_span),
        );
        let resolved = self.resolved;
        Bound::new(
            py,
            QueuedMessage {
                context: Py::new(py, context)?,
                method: resolved.method,
                bytes: resolved.bytes,
                local_state: resolved.local_state.into_py_any(py)?,
                refs: resolved.mesh_references.into_py_any(py)?,
                response_port: resolved.response_port.into_py_any(py)?,
                telemetry_message_id: self.telemetry_message_id,
                correlation_id: resolved.correlation_id,
            },
        )
    }
}

/// Message sent through the queue in queue-dispatch mode.
/// Contains pre-resolved components ready for Python consumption.
#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub struct QueuedMessage {
    #[pyo3(get)]
    pub context: Py<crate::context::PyContext>,
    #[pyo3(get)]
    pub method: MethodSpecifier,
    #[pyo3(get)]
    pub bytes: FrozenBuffer,
    #[pyo3(get)]
    pub local_state: Py<PyAny>,
    #[pyo3(get)]
    pub refs: Py<PyAny>,
    #[pyo3(get)]
    pub response_port: Py<PyAny>,
    telemetry_message_id: Option<u64>,
    #[pyo3(get)]
    pub correlation_id: Option<u64>,
}

fn report_message_status(message_id: Option<u64>, status: &str) {
    if let Some(message_id) = message_id {
        hyperactor_telemetry::notify_message_status(hyperactor_telemetry::MessageStatusEvent {
            timestamp: std::time::SystemTime::now(),
            id: hyperactor_telemetry::generate_status_event_id(message_id),
            message_id,
            status: status.to_string(),
        });
    }
}

#[pymethods]
impl QueuedMessage {
    fn _report_complete(&self) {
        report_message_status(self.telemetry_message_id, "complete");
    }

    fn _report_failed(&self) {
        report_message_status(self.telemetry_message_id, "failed");
    }
}

impl PythonMessage {
    pub fn new_from_buf(kind: PythonMessageKind, message: impl Into<Part>) -> Self {
        Self::new_from_buf_with_refs(kind, message, Vec::new())
    }

    pub fn new_from_buf_with_refs(
        kind: PythonMessageKind,
        message: impl Into<Part>,
        refs: Vec<MeshRef>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            refs,
        }
    }

    pub fn into_rank(self, rank: usize) -> Self {
        let rank = Some(rank);
        match self.kind {
            PythonMessageKind::Result { .. } => PythonMessage {
                kind: PythonMessageKind::Result { rank },
                message: self.message,
                refs: self.refs,
            },
            PythonMessageKind::Exception { .. } => PythonMessage {
                kind: PythonMessageKind::Exception { rank },
                message: self.message,
                refs: self.refs,
            },
            _ => panic!("PythonMessage is not a response but {:?}", self),
        }
    }
    async fn resolve_indirect_call(
        self,
        cx: &Context<'_, PythonActor>,
    ) -> anyhow::Result<ResolvedCallMethod> {
        match self.kind {
            PythonMessageKind::CallMethodIndirect {
                name,
                local_state_broker,
                id,
                unflatten_args,
                correlation_id,
            } => {
                let broker = BrokerId::new(local_state_broker).resolve(cx).await;
                let (send, recv) = cx.open_once_port();
                broker.post(cx, LocalStateBrokerMessage::Get(id, send));
                let state = recv.recv().await?;
                Ok(ResolvedCallMethod {
                    method: name,
                    bytes: FrozenBuffer {
                        inner: self.message.into_bytes(),
                    },
                    local_state: PendingLocalState::Indirect {
                        mailbox: cx.mailbox_for_py().clone().into(),
                        unflatten_args,
                        state: state.state,
                    },
                    mesh_references: self.refs,
                    response_port: ResponsePort::Local(LocalPort {
                        instance: cx.into(),
                        inner: Some(state.response_port),
                    }),
                    correlation_id,
                })
            }
            PythonMessageKind::CallMethod {
                name,
                response_port,
                correlation_id,
            } => {
                let method_name = name.name().to_string();
                let response_port = response_port.map_or(ResponsePort::Dropping, |port_ref| {
                    let point = cx.cast_point();
                    // Carry operation context onto the reply: copy
                    // OPERATION_*-marked keys from the inbound
                    // envelope, falling back to the method name when
                    // the caller didn't stamp.
                    let mut reply_headers = hyperactor_config::Flattrs::new();
                    hyperactor_config::attrs::copy_marked_flattrs(
                        &mut reply_headers,
                        cx.headers(),
                        hyperactor_config::attrs::OPERATION_CONTEXT_HEADER,
                    );
                    if reply_headers
                        .get(hyperactor::mailbox::headers::OPERATION_ENDPOINT)
                        .is_none()
                    {
                        reply_headers.set(
                            hyperactor::mailbox::headers::OPERATION_ENDPOINT,
                            format!("{}()", method_name),
                        );
                    }
                    ResponsePort::Port(Port::with_reply_headers(
                        port_ref,
                        cx.instance().clone_for_py(),
                        Some(point.rank()),
                        reply_headers,
                    ))
                });
                Ok(ResolvedCallMethod {
                    method: name,
                    bytes: FrozenBuffer {
                        inner: self.message.into_bytes(),
                    },
                    local_state: PendingLocalState::Empty,
                    mesh_references: self.refs,
                    response_port,
                    correlation_id,
                })
            }
            _ => {
                panic!("unexpected message kind {:?}", self.kind)
            }
        }
    }
}

impl std::fmt::Debug for PythonMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonMessage")
            .field("kind", &self.kind)
            .field(
                "message",
                &wirevalue::HexFmt(&(*self.message.to_bytes())[..]).to_string(),
            )
            .field("refs", &self.refs.len())
            .finish()
    }
}

#[pymethods]
impl PythonMessage {
    #[new]
    #[pyo3(signature = (kind, message, refs))]
    pub fn new(
        kind: PythonMessageKind,
        message: PyRef<'_, FrozenBuffer>,
        refs: &Bound<'_, PyList>,
    ) -> PyResult<Self> {
        let mesh_refs: Vec<MeshRef> = refs
            .iter()
            .map(|item| Ok(item.cast::<PyMeshRef>()?.borrow().inner.clone()))
            .collect::<PyResult<_>>()?;
        Ok(PythonMessage::new_from_buf_with_refs(
            kind,
            message.inner.clone(),
            mesh_refs,
        ))
    }

    #[getter]
    fn kind(&self) -> PythonMessageKind {
        self.kind.clone()
    }

    /// Decode this message's payload, reuniting the out-of-band `refs` table so
    /// the `pop_mesh_reference` sentinels in the pickle stream resolve. The raw
    /// bytes are deliberately not exposed: a payload can only be read back
    /// through here, so a decode can never silently drop mesh references.
    #[pyo3(signature = (local_state=None))]
    fn decode(
        &self,
        py: Python<'_>,
        local_state: Option<&Bound<'_, PyList>>,
    ) -> PyResult<Py<PyAny>> {
        let tensor_engine_references: VecDeque<Py<PyAny>> = local_state
            .map(|list| list.iter().map(|item| item.unbind()).collect())
            .unwrap_or_default();
        let mesh_references: VecDeque<Option<MeshRef>> =
            self.refs.iter().cloned().map(Some).collect();
        let mut state = PicklingState::from_parts(
            self.message.clone(),
            tensor_engine_references,
            mesh_references,
        );
        state.unpickle(py)
    }

    #[getter]
    fn refs(&self) -> Vec<MeshRef> {
        self.refs.clone()
    }
}

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub(super) struct PythonActorHandle {
    pub(super) inner: ActorHandle<PythonActor>,
}

#[pymethods]
impl PythonActorHandle {
    // TODO: do the pickling in rust
    fn send(&self, instance: &PyInstance, message: &PythonMessage) -> PyResult<()> {
        self.inner.post(instance.deref(), message.clone());
        Ok(())
    }

    fn bind(&self) -> PyActorAddr {
        self.inner.bind::<PythonActor>().into_actor_addr().into()
    }
}

// In-flight handler execution tracking for a Python actor -- the producer
// side of the mesh `execution` field. Per-actor Rust state, read GIL-free
// by the introspect seam. Producer invariants (PE-*, monarch_hyperactor-
// local; documented inline, no registry -- not our crate):
//   PE-2: only the real user-method invocation is bracketed (in
//         `_Actor.handle`), never Init/unpickling/plumbing.
//   PE-3: the snapshot reads `Arc` state only (atomic load + `try_lock`);
//         the `Mutex` is held only for an insert/remove, never across user
//         code, so a handler wedged holding the GIL never blocks the
//         snapshot and the read never touches the GIL.
//   PE-4: tokens are >= 1; `0` is a reserved no-op sentinel (returned by
//         the binding when an instance has no tracker), so the
//         unconditional Python `finally` cannot collide it with a real
//         token.

/// EX-4 cap: at most this many distinct in-flight handler names are
/// reported per snapshot; `truncated` is set when exceeded.
const MAX_ACTIVE_HANDLERS: usize = 64;

/// One in-flight handler invocation.
#[derive(Debug)]
struct ActiveEntry {
    name: String,
    started_at: SystemTime,
}

/// Per-actor in-flight handler tracker. Cheap to read concurrently: the
/// count is a lock-free atomic and the per-handler detail sits behind a
/// `try_lock` held only for an insert/remove (never across user code), so
/// a wedged actor stays introspectable (PE-3).
#[derive(Debug)]
pub(crate) struct ExecutionTracker {
    /// Lock-free count of in-flight invocations; always readable.
    active_count: AtomicU64,
    /// Monotonic token source, initialized to 1 so issued tokens are
    /// `>= 1` and `0` stays reserved as the no-op sentinel (PE-4).
    next_token: AtomicU64,
    /// token -> entry for the in-flight invocations.
    handlers: Mutex<HashMap<u64, ActiveEntry>>,
}

/// Aggregate raw in-flight entries into the reported per-handler view:
/// grouped by handler name, oldest-first with a stable tie-break on
/// `name`, capped at `max` (EX-4). Pure, so it can be unit-tested with
/// explicit timestamps.
fn aggregate_active(
    handlers: &HashMap<u64, ActiveEntry>,
    max: usize,
) -> (Vec<ActiveHandler>, bool) {
    let mut by_name: HashMap<&str, (u64, SystemTime)> = HashMap::new();
    for entry in handlers.values() {
        let slot = by_name
            .entry(entry.name.as_str())
            .or_insert((0, entry.started_at));
        slot.0 += 1;
        if entry.started_at < slot.1 {
            slot.1 = entry.started_at;
        }
    }
    let mut out: Vec<ActiveHandler> = by_name
        .into_iter()
        .map(|(name, (active_count, oldest_since))| ActiveHandler {
            name: name.to_string(),
            active_count,
            oldest_since,
        })
        .collect();
    // EX-4: oldest-first, stable tie-break on name.
    out.sort_by(|a, b| {
        a.oldest_since
            .cmp(&b.oldest_since)
            .then_with(|| a.name.cmp(&b.name))
    });
    let truncated = out.len() > max;
    if truncated {
        out.truncate(max);
    }
    (out, truncated)
}

impl ExecutionTracker {
    pub(crate) fn new() -> Self {
        Self {
            active_count: AtomicU64::new(0),
            next_token: AtomicU64::new(1),
            handlers: Mutex::new(HashMap::new()),
        }
    }

    /// Record the start of a handler invocation; returns its token.
    pub(crate) fn start(&self, name: String) -> u64 {
        let token = self.next_token.fetch_add(1, AtomicOrdering::Relaxed);
        self.handlers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                token,
                ActiveEntry {
                    name,
                    started_at: SystemTime::now(),
                },
            );
        self.active_count.fetch_add(1, AtomicOrdering::Relaxed);
        token
    }

    /// Record the end of a handler invocation. Idempotent (a token is
    /// removed at most once) and a no-op for the `0` sentinel (PE-4).
    pub(crate) fn finish(&self, token: u64) {
        if token == 0 {
            return;
        }
        let removed = self
            .handlers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&token)
            .is_some();
        if removed {
            self.active_count.fetch_sub(1, AtomicOrdering::Relaxed);
        }
    }

    /// Point-in-time snapshot for the introspect seam. Never blocks: the
    /// count is read lock-free and the per-handler detail is best-effort
    /// behind `try_lock` (EX-2: a miss yields `complete: false`, it never
    /// drops the field).
    pub(crate) fn snapshot(&self) -> Execution {
        let active_count = self.active_count.load(AtomicOrdering::Relaxed);
        match self.handlers.try_lock() {
            Ok(guard) => {
                let (active_handlers, truncated) = aggregate_active(&guard, MAX_ACTIVE_HANDLERS);
                Execution {
                    active_count,
                    active_handlers,
                    complete: true,
                    truncated,
                }
            }
            Err(_) => Execution {
                active_count,
                active_handlers: Vec::new(),
                complete: false,
                truncated: false,
            },
        }
    }
}

#[cfg(test)]
mod execution_tracker_tests {
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use super::*;

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn aggregates_by_name_oldest_first() {
        let mut h = HashMap::new();
        h.insert(
            1,
            ActiveEntry {
                name: "b".to_string(),
                started_at: at(10),
            },
        );
        h.insert(
            2,
            ActiveEntry {
                name: "a".to_string(),
                started_at: at(20),
            },
        );
        h.insert(
            3,
            ActiveEntry {
                name: "a".to_string(),
                started_at: at(30),
            },
        );
        let (out, truncated) = aggregate_active(&h, MAX_ACTIVE_HANDLERS);
        assert!(!truncated);
        assert_eq!(out.len(), 2);
        // Oldest-first: b (10) before a (20).
        assert_eq!(out[0].name, "b");
        assert_eq!(out[0].active_count, 1);
        assert_eq!(out[0].oldest_since, at(10));
        // "a" aggregates two invocations; oldest_since is the min (20).
        assert_eq!(out[1].name, "a");
        assert_eq!(out[1].active_count, 2);
        assert_eq!(out[1].oldest_since, at(20));
    }

    #[test]
    fn tie_break_on_name_when_same_oldest() {
        let mut h = HashMap::new();
        h.insert(
            1,
            ActiveEntry {
                name: "zebra".to_string(),
                started_at: at(5),
            },
        );
        h.insert(
            2,
            ActiveEntry {
                name: "alpha".to_string(),
                started_at: at(5),
            },
        );
        let (out, _) = aggregate_active(&h, MAX_ACTIVE_HANDLERS);
        assert_eq!(out[0].name, "alpha");
        assert_eq!(out[1].name, "zebra");
    }

    #[test]
    fn truncates_to_n_oldest() {
        let mut h = HashMap::new();
        for i in 0..(MAX_ACTIVE_HANDLERS as u64 + 6) {
            h.insert(
                i,
                ActiveEntry {
                    name: format!("h{:03}", i),
                    started_at: at(i),
                },
            );
        }
        let (out, truncated) = aggregate_active(&h, MAX_ACTIVE_HANDLERS);
        assert!(truncated);
        assert_eq!(out.len(), MAX_ACTIVE_HANDLERS);
        // Prefix of the N oldest.
        assert_eq!(out[0].name, "h000");
        assert_eq!(
            out[MAX_ACTIVE_HANDLERS - 1].name,
            format!("h{:03}", MAX_ACTIVE_HANDLERS - 1)
        );
    }

    #[test]
    fn start_assigns_nonzero_distinct_tokens() {
        let t = ExecutionTracker::new();
        let a = t.start("a".to_string());
        let b = t.start("b".to_string());
        assert!(a >= 1);
        assert!(b >= 1);
        assert_ne!(a, b);
        let snap = t.snapshot();
        assert_eq!(snap.active_count, 2);
        assert!(snap.complete);
        assert_eq!(snap.active_handlers.len(), 2);
    }

    #[test]
    fn finish_is_idempotent_and_zero_is_noop() {
        let t = ExecutionTracker::new();
        let tok = t.start("a".to_string());
        t.finish(tok);
        assert_eq!(t.snapshot().active_count, 0);
        // Double-finish must not underflow the count.
        t.finish(tok);
        assert_eq!(t.snapshot().active_count, 0);
        // The 0 sentinel is a no-op.
        t.finish(0);
        assert_eq!(t.snapshot().active_count, 0);
    }
}

/// An actor for which message handlers are implemented in Python.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        PythonMessage,
        MeshFailure,
    ],
)]
#[hyperactor::spawnable]
pub struct PythonActor {
    /// The Python object that we delegate message handling to.
    actor: Py<PyAny>,
    /// The event-loop or driver state selected by the actor's kind.
    dispatch: Dispatch,
    /// Instance object that we keep across handle calls so that we can store
    /// information from the Init (spawn rank, controller) and provide it to other calls.
    /// The `Arc` lets handlers share it without the GIL.
    instance: Option<Arc<Py<crate::context::PyInstance>>>,
    /// Enqueues messages for the actor's Python dispatch path.
    message_sender: Sender,
    /// Enqueues callbacks for `_callback_loop` or the sync driver's control
    /// plane, and cleanup for the sync driver.
    control_sender: Sender,
    /// Number of enqueued callbacks whose verdict has not been reported yet.
    /// See [`CallbacksPending`].
    callbacks_pending: Arc<AtomicUsize>,
    /// Inherited or assigned construction context, not proof of mesh membership.
    construction_point: OnceLock<Option<Point>>,
    /// Initial message to process during PythonActor::init.
    init_message: Option<PythonMessage>,

    /// Per-actor in-flight handler tracker (producer of the mesh
    /// `execution` field). Read GIL-free by the introspect seam; a clone
    /// of this `Arc` is injected into the actor's `PyInstance` so
    /// `_Actor.handle` can bracket each invocation.
    execution_tracker: Arc<ExecutionTracker>,
}

/// Kind-specific state for running and stopping a [`PythonActor`].
#[derive(Debug)]
enum Dispatch {
    /// Event-loop-backed execution state.
    Async(async_actor::dispatch::State),
    /// Blocking-driver execution state.
    Sync(sync_actor::dispatch::State),
}

/// Named result of selecting an actor's dispatch topology in
/// [`PythonActor::new`].
///
/// Both [`ActorKind`] branches produce the two common senders plus their
/// kind-specific [`Dispatch`] state. Named fields keep the same-typed message
/// and control senders distinct before all three values move into
/// [`PythonActor`].
struct DispatchParts {
    /// Sends endpoint messages to the actor's Python dispatch path.
    message_sender: Sender,
    /// Sends callback work and, for a sync actor, its cleanup request.
    control_sender: Sender,
    /// Owns the receivers and the kind-specific loop or driver lifecycle state.
    dispatch: Dispatch,
}

/// Convert a wake-pipe creation failure into the error type returned across
/// the GIL-bound `PythonActor` constructor.
fn serialize_channel_error(py: Python<'_>, error: nix::Error) -> SerializablePyErr {
    let error = PyRuntimeError::new_err(error.to_string());
    SerializablePyErr::from(py, &error)
}

impl DispatchParts {
    /// Construct the channels and lifecycle state selected by `kind`.
    fn try_new(py: Python<'_>, kind: ActorKind) -> Result<Self, SerializablePyErr> {
        match kind {
            ActorKind::Async => {
                let async_actor::inbox::Inbox {
                    message_sender,
                    control_sender,
                    receiver,
                } = async_actor::inbox::new()
                    .map_err(|error| serialize_channel_error(py, error))?;
                Ok(Self {
                    message_sender,
                    control_sender,
                    dispatch: Dispatch::Async(async_actor::dispatch::State::new(py, receiver)),
                })
            }
            ActorKind::Sync => {
                let sync_actor::inbox::Inbox {
                    message_sender,
                    control_sender,
                    receiver,
                } = sync_actor::inbox::new().map_err(|error| serialize_channel_error(py, error))?;
                Ok(Self {
                    message_sender,
                    control_sender,
                    dispatch: Dispatch::Sync(sync_actor::dispatch::State::new(receiver)),
                })
            }
        }
    }
}

impl PythonActor {
    pub(crate) fn new(
        actor_type: PickledPyObject,
        init_message: Option<PythonMessage>,
        construction_point: Option<Point>,
        kind: ActorKind,
    ) -> Result<Self, anyhow::Error> {
        Ok(monarch_with_gil_blocking(
            GilSite::ActorConstruct,
            |py| -> Result<Self, SerializablePyErr> {
                let unpickled = actor_type.unpickle(py)?;
                let class_type: &Bound<'_, PyType> = unpickled.cast()?;
                let actor: Py<PyAny> = class_type.call0()?.into_py_any(py)?;

                let DispatchParts {
                    message_sender,
                    control_sender,
                    dispatch,
                } = DispatchParts::try_new(py, kind)?;

                Ok(Self {
                    actor,
                    dispatch,
                    instance: None,
                    message_sender,
                    control_sender,
                    callbacks_pending: Arc::new(AtomicUsize::new(0)),
                    construction_point: OnceLock::from(construction_point),
                    init_message,
                    execution_tracker: Arc::new(ExecutionTracker::new()),
                })
            },
        )?)
    }

    /// Get-or-create the actor's cached `PyInstance`, injecting a clone of
    /// the execution tracker (PE-1) so `_Actor.handle` can bracket each
    /// invocation. Its only caller is `init`; `handle_queue` and the
    /// `MeshFailure` handler expect the instance it created.
    fn ensure_py_instance(
        &mut self,
        py: Python<'_>,
        src: impl Into<crate::context::PyInstance>,
    ) -> Py<crate::context::PyInstance> {
        let tracker = self.execution_tracker.clone();
        self.instance
            .get_or_insert_with(|| {
                let mut inst: crate::context::PyInstance = src.into();
                inst.set_execution_tracker(tracker);
                Arc::new(inst.into_pyobject(py).unwrap().into())
            })
            .clone_ref(py)
    }

    /// Bootstrap the root client actor, creating a new proc for it.
    /// This is the legacy entry point that creates its own proc.
    pub(crate) fn bootstrap_client(py: Python<'_>) -> (&'static Instance<Self>, ActorHandle<Self>) {
        static ROOT_CLIENT_INSTANCE: OnceLock<Instance<PythonActor>> = OnceLock::new();

        let client_proc = Proc::direct(
            default_bind_spec().binding_addr(),
            "mesh_root_client_proc".into(),
        )
        .unwrap();

        // The legacy path seeds no inherited capabilities.
        Self::bootstrap_client_inner(
            py,
            client_proc,
            ActorEnvironment::default(),
            &ROOT_CLIENT_INSTANCE,
        )
    }

    /// Bootstrap the client proc, storing the root client instance in given static.
    /// This is passed in because we require storage, as the instance is shared.
    /// This can be simplified when we remove v0.
    ///
    /// `environment` is the root client's persistent
    /// [`ActorEnvironment`](hyperactor::ActorEnvironment): the caller seeds any
    /// inherited capabilities into it (e.g. the client-root reference) and
    /// descendants inherit them through the environment.
    pub(crate) fn bootstrap_client_inner(
        py: Python<'_>,
        client_proc: Proc,
        environment: ActorEnvironment,
        root_client_instance: &'static OnceLock<Instance<PythonActor>>,
    ) -> (&'static Instance<Self>, ActorHandle<Self>) {
        let actor_mesh_mod = py
            .import("monarch._src.actor.actor_mesh")
            .expect("import actor_mesh");
        let root_client_class = actor_mesh_mod
            .getattr("RootClientActor")
            .expect("get RootClientActor");

        let actor_type =
            PickledPyObject::pickle(&actor_mesh_mod.getattr("_Actor").expect("get _Actor"))
                .expect("pickle _Actor");

        let init_frozen_buffer: FrozenBuffer = root_client_class
            .call_method0("_pickled_init_args")
            .expect("call RootClientActor._pickled_init_args")
            .extract()
            .expect("extract FrozenBuffer from _pickled_init_args");
        let init_message = PythonMessage::new_from_buf(
            PythonMessageKind::CallMethod {
                name: MethodSpecifier::Init {},
                response_port: None,
                correlation_id: None,
            },
            init_frozen_buffer,
        );

        let mut actor = PythonActor::new(
            actor_type,
            Some(init_message),
            Some(extent!().point_of_rank(0).unwrap()),
            ActorKind::Async,
        )
        .expect("create client PythonActor");

        let ai = client_proc
            .actor_instance_in_environment(
                root_client_class
                    .getattr("name")
                    .expect("get RootClientActor.name")
                    .extract()
                    .expect("extract RootClientActor.name"),
                environment,
            )
            .expect("root instance create");

        let handle = ai.handle;
        let signal_rx = ai.signal;
        let supervision_rx = ai.supervision;
        let work_rx = ai.work;

        root_client_instance
            .set(ai.instance)
            .map_err(|_| "already initialized root client instance")
            .unwrap();
        let instance = root_client_instance.get().unwrap();

        // The root client PythonActor uses a custom run loop that
        // bypasses Actor::init, so mark it as system explicitly
        // (matching GlobalClientActor::fresh_instance).
        instance.set_system();

        // Bind to ensure the Undeliverable<MessageEnvelope> port is bound.
        let _client_ref = handle.bind::<PythonActor>();

        get_tokio_runtime().spawn(async move {
            // This is gross. Sorry.
            actor.init(instance).await.unwrap();

            let mut signal_rx = signal_rx;
            let mut supervision_rx = supervision_rx;
            let mut work_rx = work_rx;
            let mut need_drain = false;
            let mut err = loop {
                tokio::select! {
                    work = work_rx.recv() => {
                        let work = work.expect("inconsistent work queue state");
                        if let Err(err) = work.handle(&mut actor, instance).await {
                            let kind = ActorErrorKind::processing(err);
                            let err = ActorError {
                                actor_id: Box::new(instance.self_addr().clone()),
                                kind: Box::new(kind),
                            };

                            // Only a `SupervisionOutcome` verdict fails with
                            // `UnhandledSupervisionEvent`. It is `__supervise__`'s
                            // final answer (including an `UnhandledFaultHookException`
                            // raised by `RootClientActor`), so exit rather than
                            // supervising it again, which could loop forever.
                            if matches!(*err.kind, ActorErrorKind::UnhandledSupervisionEvent(_)) {
                                break Some(err);
                            }

                            // Give the actor a chance to handle the error produced
                            // in its own message handler. This is important because
                            // we want an undeliverable message that fails the actor
                            // (via `UndeliverableOutcome`) to create a supervision
                            // event and call __supervise__. This only enqueues the event, so
                            // keep handling messages until the `SupervisionOutcome`
                            // carrying the verdict is delivered.
                            let supervision_event = actor_error_to_event(instance, &actor, err);
                            if let Err(err) = instance.handle_supervision_event(&mut actor, supervision_event).await {
                                break Some(err);
                            }
                        }
                    }
                    signal = signal_rx.recv() => {
                        tracing::info!(actor_id = %instance.self_addr(), "client received signal {signal:?}");
                        match signal {
                            Some(signal@(Signal::Stop(_) | Signal::DrainAndStop(_))) => {
                                need_drain = matches!(signal, Signal::DrainAndStop(_));
                                break None;
                            },
                            Some(Signal::ExitRequested(_)) => break None,
                            Some(Signal::Kill(reason)) => {
                                break Some(ActorError { actor_id: Box::new(instance.self_addr().clone()), kind: Box::new(ActorErrorKind::Aborted(reason)) })
                            },
                            None => {
                                break Some(ActorError {
                                    actor_id: Box::new(instance.self_addr().clone()),
                                    kind: Box::new(ActorErrorKind::SignalChannelClosed),
                                })
                            },
                        }
                    }
                    Some(supervision_event) = supervision_rx.recv() => {
                        if let Err(err) = instance.handle_supervision_event(&mut actor, supervision_event).await {
                            break Some(err);
                        }
                    }
                };
            };
            if need_drain {
                let mut n = 0;
                while let Ok(work) = work_rx.try_recv() {
                    if let Err(e) = work.handle(&mut actor, instance).await {
                        err = Some(ActorError {
                            actor_id: Box::new(instance.self_addr().clone()),
                            kind: Box::new(ActorErrorKind::processing(e)),
                        });
                        break;
                    }
                    n += 1;
                }
                tracing::debug!(actor_id = %instance.self_addr(), "client drained {} messages before stopping", n);
            }
            if let Some(err) = err {
                let event = actor_error_to_event(instance, &actor, err);
                // The proc supervision handler will send to ProcAgent, which
                // just records it in v1. We want to crash instead, as nothing will
                // monitor the client ProcAgent for now.
                tracing::error!(
                    actor_id = %instance.self_addr(),
                    "could not propagate supervision event {} because it reached the global client: signaling KeyboardInterrupt to main thread",
                    event,
                );

                // This is running in a background thread, and thus cannot run
                // Py_FinalizeEx when it exits the process to properly shut down
                // all python objects.
                // We use _thread.interrupt_main to raise a KeyboardInterrupt
                // to the main thread at some point in the future.
                // There is no way to propagate the exception message, but it
                // will at least run proper shutdown code as long as BaseException
                // isn't caught.
                monarch_with_gil_blocking(GilSite::Stop, |py| {
                    // Use _thread.interrupt_main to force the client to exit if it has an
                    // unhandled supervision event.
                    let thread_mod = py.import("_thread").expect("import _thread");
                    let interrupt_main = thread_mod
                        .getattr("interrupt_main")
                        .expect("get interrupt_main");

                    // Ignore any exception from calling interrupt_main
                    if let Err(e) = interrupt_main.call0() {
                        tracing::error!("unable to interrupt main, exiting the process instead: {:?}", e);
                        eprintln!("unable to interrupt main, exiting the process with code 1 instead: {:?}", e);
                        std::process::exit(1);
                    }
                });
            } else {
                tracing::info!(actor_id = %instance.self_addr(), "client stopped");
                instance.change_status(hyperactor::actor::ActorStatus::Stopped("client stopped".into()));
            }
        });

        (root_client_instance.get().unwrap(), handle)
    }
}

fn actor_error_to_event(
    instance: &Instance<PythonActor>,
    actor: &PythonActor,
    err: ActorError,
) -> ActorSupervisionEvent {
    match *err.kind {
        ActorErrorKind::UnhandledSupervisionEvent(event) => *event,
        _ => {
            let status = ActorStatus::generic_failure(err.kind.to_string());
            ActorSupervisionEvent::new(
                instance.self_addr().clone(),
                actor.display_name(),
                status,
                None,
            )
        }
    }
}

pub(crate) fn root_client_actor(py: Python<'_>) -> &'static Instance<PythonActor> {
    static ROOT_CLIENT_ACTOR: OnceLock<&'static Instance<PythonActor>> = OnceLock::new();

    // Release the GIL before waiting on ROOT_CLIENT_ACTOR, because PythonActor::bootstrap_client
    // may release/reacquire the GIL; if thread 0 holds the GIL blocking on ROOT_CLIENT_ACTOR.get_or_init
    // while thread 1 blocks on acquiring the GIL inside PythonActor::bootstrap_client, we get
    // a deadlock.
    py.detach(|| {
        ROOT_CLIENT_ACTOR.get_or_init(|| {
            monarch_with_gil_blocking(GilSite::Bootstrap, |py| {
                let (client, _handle) = PythonActor::bootstrap_client(py);
                client
            })
        })
    })
}

#[async_trait]
impl Actor for PythonActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // PE-1: install the read side eagerly so the actor reports
        // `execution` from its first handled message. The callback runs on
        // the introspect task (off the actor loop) and only reads `Arc`
        // state (PE-3), so it is `Send + Sync`, non-blocking, and infallible.
        let tracker = self.execution_tracker.clone();
        this.set_attrs_snapshot(move || {
            let mut attrs = hyperactor_config::Attrs::new();
            attrs.set(EXECUTION, tracker.snapshot());
            attrs
        });

        monarch_with_gil(GilSite::DispatchInit, |py| {
            let self_instance = self.ensure_py_instance(py, this);
            let actor_mesh_mod = py.import("monarch._src.actor.actor_mesh")?;

            match &mut self.dispatch {
                Dispatch::Async(state) => state.init(
                    py,
                    &actor_mesh_mod,
                    &self.actor,
                    &self_instance,
                    self.callbacks_pending.clone(),
                )?,
                Dispatch::Sync(state) => {
                    state.init(py, &actor_mesh_mod, &self.actor, &self_instance)?
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;

        if let Some(init_message) = self.init_message.take() {
            let construction_point = self.construction_point.get().unwrap().as_ref().expect("PythonActor should never be spawned with init_message unless construction_point is also specified").clone();
            let mut headers = Flattrs::new();
            headers.set(CAST_POINT, construction_point);
            let cx = Context::new(this, headers);
            <Self as Handler<PythonMessage>>::handle(self, &cx, init_message).await?;
        }

        Ok(())
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        err: Option<&ActorError>,
    ) -> anyhow::Result<()> {
        // Calls the "__cleanup__" method on the python instance to allow the actor
        // to control its own cleanup.
        // No headers because this isn't in the context of a message.
        let cx = Context::new(this, Flattrs::new());
        // Turn the ActorError into a representation of the error. We may not
        // have an original exception object or traceback, so we just pass in
        // the message.
        let error = err.map(|e| e.to_string());
        let actor = &self.actor;
        let instance = self.instance.clone();
        let control_sender = &self.control_sender;
        match &mut self.dispatch {
            Dispatch::Async(state) => {
                state
                    .cleanup(actor, instance.as_deref(), &cx, this, error)
                    .await
            }
            Dispatch::Sync(state) => state.cleanup(control_sender, instance, &cx, error).await,
        }
    }

    fn display_name(&self) -> Option<String> {
        self.instance.as_ref().and_then(|instance| {
            monarch_with_gil_blocking(GilSite::DisplayName, |py| {
                instance.bind(py).str().ok().map(|s| s.to_string())
            })
        })
    }

    /// Enqueues the message for `_handle_undeliverable_message` and returns
    /// right away, for the same reasons `handle_supervision_event` does not
    /// wait. The verdict comes back later as an [`UndeliverableOutcome`],
    /// whose handler applies the default handling if the message was not
    /// handled. A message still pending when the actor stops is dropped.
    async fn handle_undeliverable_message(
        &mut self,
        ins: &Instance<Self>,
        reason: UndeliverableReason,
        mut envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        if envelope
            .as_message()
            .is_some_and(|envelope| envelope.sender() != ins.self_addr())
        {
            // This can happen if the sender is comm. Update the envelope.
            envelope = update_undeliverable_envelope_for_casting(envelope);
        }
        let envelope = match envelope {
            Undeliverable::Returned(envelope) => envelope,
            Undeliverable::Report(report) => {
                return Err(UndeliverableMessageError::Report { report }.into());
            }
        };
        assert_eq!(
            envelope.sender(),
            ins.self_addr(),
            "undeliverable message was returned to the wrong actor. \
            Return address = {}, src actor = {}, dest handler port = {}, message type = {}, envelope headers = {}",
            envelope.sender(),
            ins.self_addr(),
            envelope.dest(),
            envelope.data().typename().unwrap_or("unknown"),
            envelope.headers()
        );

        let cx = Context::new(ins, envelope.headers().clone());
        let pending = PendingUndeliverable {
            instance: self
                .instance
                .clone()
                .expect("PythonActor::init should have created the instance"),
            rank: cx.cast_point(),
            recording_span: cx.recording_span(),
            envelope: Undeliverable::Returned(envelope),
            reply: UndeliverableReply {
                port: ins.port(),
                instance: ins.into(),
                reason,
                _in_flight: InFlight::new(&self.callbacks_pending),
            },
        };

        self.control_sender
            .send(pending)
            .map_err(|_| anyhow::anyhow!("failed to send undeliverable message to queue"))?;
        Ok(())
    }

    /// Enqueues the event for `__supervise__` and returns `Ok(true)` right
    /// away: `true` means "accepted", not "handled". The verdict comes back
    /// later as a [`SupervisionOutcome`], whose handler fails the actor if
    /// the event was not handled.
    ///
    /// Waiting for the verdict here is only appropriate when supervision is
    /// bounded in time and does not need this actor's loop, as with Rust
    /// actors. `__supervise__` is arbitrary user code on the actor's asyncio
    /// loop, while this runs on the actor loop, so waiting would stop the
    /// actor from handling messages and signals for as long as user code
    /// runs, deadlock a `__supervise__` that calls its own actor, and keep a
    /// stop from cancelling it.
    ///
    /// Consequences of not waiting:
    /// - The actor keeps handling messages until an unhandled verdict
    ///   arrives.
    /// - Supervision still pending when the actor stops is cancelled along
    ///   with the other asyncio tasks in `cleanup`. This is intentional:
    ///   recovering from a child failure (e.g. respawning a mesh) is
    ///   pointless when the result is about to be stopped with us. It
    ///   includes the events the run loop drains after a handler error, so
    ///   the actor fails with its own error rather than theirs.
    /// - The root client's custom loop must keep running after this returns
    ///   so that it can deliver the `SupervisionOutcome`.
    async fn handle_supervision_event(
        &mut self,
        this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        let cx = Context::new(this, Flattrs::new());
        // Events without labels, such as from a managed mesh's controller, are
        // reported with no mesh name and against the whole mesh.
        let crashed_ranks = event
            .labels
            .get(CAST_POINT)
            .map(|point| point.rank())
            .into_iter()
            .collect();
        self.handle(
            &cx,
            MeshFailure {
                // TODO: Replace with a structural mesh reference once
                // `MeshFailure` and `__supervise__` stop identifying meshes by
                // name.
                actor_mesh_name: event
                    .labels
                    .get(ACTOR_MESH_ID)
                    .map(|mesh_id| mesh_id.to_string()),
                event: event.clone(),
                crashed_ranks,
                // MFCA-4: direct actor-handled supervision conversion, not a
                // controller report.
                reporting_controller: None,
            },
        )
        .await
        .map(|_| true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct PythonActorParams {
    // The pickled actor class to instantiate.
    actor_type: PickledPyObject,
    // Python message to process as part of the actor initialization.
    init_message: Option<PythonMessage>,
    kind: ActorKind,
}

impl PythonActorParams {
    pub(crate) fn new(
        actor_type: PickledPyObject,
        init_message: Option<PythonMessage>,
        kind: ActorKind,
    ) -> Self {
        Self {
            actor_type,
            init_message,
            kind,
        }
    }
}

#[async_trait]
impl RemoteSpawn for PythonActor {
    type Params = PythonActorParams;

    async fn new(
        PythonActorParams {
            actor_type,
            init_message,
            kind,
        }: PythonActorParams,
        environment: &ActorEnvironment,
    ) -> Result<Self, anyhow::Error> {
        let construction_point = environment.get(CAST_POINT);
        Self::new(actor_type, init_message, construction_point, kind)
    }
}

#[async_trait]
impl Handler<PythonMessage> for PythonActor {
    fn message_status_reporting() -> MessageStatusReporting {
        MessageStatusReporting::Deferred
    }

    // HOT PATH: Be mindful of performance when making changes here.
    // To test how performance is affected by a change, run the RPC benchmarks in
    // `monarch/python/benches/`.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn handle(
        &mut self,
        cx: &Context<PythonActor>,
        message: PythonMessage,
    ) -> anyhow::Result<()> {
        self.handle_queue(cx, message).await
    }
}

impl PythonActor {
    // HOT PATH: Be mindful of performance when making changes here.
    // To test how performance is affected by a change, run the RPC benchmarks in
    // `monarch/python/benches/`.
    /// Handle a message using queue dispatch.
    /// Resolves the message on the Rust side and enqueues it for Python to process.
    async fn handle_queue(
        &self,
        cx: &Context<'_, PythonActor>,
        message: PythonMessage,
    ) -> anyhow::Result<()> {
        let resolved = message.resolve_indirect_call(cx).await?;

        let pending = PendingMessage {
            instance: self
                .instance
                .clone()
                .expect("PythonActor::init should have created the instance"),
            rank: cx.cast_point(),
            recording_span: cx.recording_span(),
            resolved,
            telemetry_message_id: cx
                .headers()
                .get(hyperactor::mailbox::headers::TELEMETRY_MESSAGE_ID),
        };

        self.message_sender
            .send(pending)
            .map_err(|_| anyhow::anyhow!("failed to send message to queue"))?;

        Ok(())
    }
}

/// A supervision event enqueued for `_callback_loop`, or for a sync actor's
/// driver, which runs `__supervise__` and reports the verdict with `_handled`
/// or `_raised`.
/// Dropping it without reporting (for example because an async loop was
/// cancelled, sync cleanup won the claim, or the inbox was dropped) produces no
/// verdict.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub struct QueuedSupervision {
    #[pyo3(get)]
    context: Py<crate::context::PyContext>,
    #[pyo3(get)]
    failure: Py<PyMeshFailure>,
    reply: Option<SupervisionReply>,
}

struct SupervisionReply {
    port: PortHandle<SupervisionOutcome>,
    instance: PyInstance,
    failure: MeshFailure,
    display_name: Option<String>,
    _in_flight: InFlight,
}

#[pymethods]
impl QueuedSupervision {
    fn _handled(&mut self, handled: bool) {
        self.report(if handled {
            SupervisionVerdict::Handled
        } else {
            SupervisionVerdict::Unhandled
        });
    }

    fn _raised(&mut self, exc: Bound<'_, PyAny>) {
        self.report(SupervisionVerdict::Raised(
            PyErr::from_value(exc).to_string(),
        ));
    }
}

impl QueuedSupervision {
    fn report(&mut self, verdict: SupervisionVerdict) {
        let SupervisionReply {
            port,
            instance,
            failure,
            display_name,
            _in_flight,
        } = self
            .reply
            .take()
            .expect("supervision verdict reported twice");
        let outcome = SupervisionOutcome {
            failure,
            display_name,
            verdict,
        };
        if let Err(err) = port.try_post(instance.deref(), outcome) {
            tracing::debug!(
                actor_id = %instance.self_addr(),
                "dropping supervision verdict, actor is gone: {}",
                err
            );
        }
    }
}

/// Converted into [`QueuedSupervision`] on the actor's Python execution thread,
/// so the Tokio worker never takes the GIL (see [`PendingMessage`]).
struct PendingSupervision {
    instance: Arc<Py<PyInstance>>,
    rank: Point,
    recording_span: tracing::Span,
    reply: SupervisionReply,
}

impl<'py> IntoPyObject<'py> for PendingSupervision {
    type Target = QueuedSupervision;
    type Output = Bound<'py, QueuedSupervision>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let mut reply = self.reply;
        reply.display_name = self.instance.bind(py).str().ok().map(|s| s.to_string());
        let context = crate::context::PyContext::from_parts(
            self.instance.clone_ref(py),
            self.rank,
            Some(self.recording_span),
        );
        let failure = PyMeshFailure::from(reply.failure.clone());
        Bound::new(
            py,
            QueuedSupervision {
                context: Py::new(py, context)?,
                failure: Py::new(py, failure)?,
                reply: Some(reply),
            },
        )
    }
}

/// Counts a callback as pending until dropped.
#[derive(Debug)]
struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, AtomicOrdering::Relaxed);
        Self(count.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

/// Truthy while any callback is waiting on `_callback_loop`.
/// `_dispatch_loop` yields to the event loop between messages while this is
/// true: `recv` does not yield when a message is already queued, so a
/// backlog of messages would otherwise starve `_callback_loop`.
#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub struct CallbacksPending(Arc<AtomicUsize>);

#[pymethods]
impl CallbacksPending {
    /// A count with no callbacks pending.
    #[new]
    fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }

    fn __bool__(&self) -> bool {
        self.0.load(AtomicOrdering::Relaxed) > 0
    }
}

/// An undeliverable message enqueued for `_callback_loop`, or for a sync
/// actor's driver, which runs `_handle_undeliverable_message` and reports the
/// verdict with `_handled` or `_raised`. Dropping it without reporting (for
/// example because an async loop was cancelled, sync cleanup won the claim, or
/// the inbox was dropped) produces no verdict.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub struct QueuedUndeliverable {
    #[pyo3(get)]
    context: Py<crate::context::PyContext>,
    #[pyo3(get)]
    envelope: Py<PythonUndeliverableMessageEnvelope>,
    reply: Option<UndeliverableReply>,
}

struct UndeliverableReply {
    port: PortHandle<UndeliverableOutcome>,
    instance: PyInstance,
    reason: UndeliverableReason,
    _in_flight: InFlight,
}

#[pymethods]
impl QueuedUndeliverable {
    fn _handled(&mut self, py: Python<'_>, handled: bool) {
        let verdict = if handled {
            UndeliverableVerdict::Handled
        } else {
            match self.envelope.bind(py).try_borrow_mut() {
                Ok(mut envelope) => match envelope.take() {
                    Ok(envelope) => UndeliverableVerdict::Unhandled(Box::new(envelope)),
                    Err(err) => UndeliverableVerdict::Raised(err),
                },
                Err(err) => UndeliverableVerdict::Raised(PyErr::from(err).into()),
            }
        };
        self.report(verdict);
    }

    fn _raised(&mut self, exc: Bound<'_, PyAny>) {
        let err = SerializablePyErr::from(exc.py(), &PyErr::from_value(exc));
        self.report(UndeliverableVerdict::Raised(err.into()));
    }
}

impl QueuedUndeliverable {
    fn report(&mut self, verdict: UndeliverableVerdict) {
        let UndeliverableReply {
            port,
            instance,
            reason,
            _in_flight,
        } = self
            .reply
            .take()
            .expect("undeliverable verdict reported twice");
        if let Err(err) = port.try_post(instance.deref(), UndeliverableOutcome { reason, verdict })
        {
            tracing::debug!(
                actor_id = %instance.self_addr(),
                "dropping undeliverable verdict, actor is gone: {}",
                err
            );
        }
    }
}

/// Converted into [`QueuedUndeliverable`] on the actor's Python execution
/// thread, so the Tokio worker never takes the GIL (see [`PendingMessage`]).
struct PendingUndeliverable {
    instance: Arc<Py<PyInstance>>,
    rank: Point,
    recording_span: tracing::Span,
    envelope: Undeliverable<MessageEnvelope>,
    reply: UndeliverableReply,
}

impl<'py> IntoPyObject<'py> for PendingUndeliverable {
    type Target = QueuedUndeliverable;
    type Output = Bound<'py, QueuedUndeliverable>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let context = crate::context::PyContext::from_parts(
            self.instance.clone_ref(py),
            self.rank,
            Some(self.recording_span),
        );
        let envelope = PythonUndeliverableMessageEnvelope {
            inner: Some(self.envelope),
        };
        Bound::new(
            py,
            QueuedUndeliverable {
                context: Py::new(py, context)?,
                envelope: Py::new(py, envelope)?,
                reply: Some(self.reply),
            },
        )
    }
}

/// The verdict of `_handle_undeliverable_message` on a
/// [`QueuedUndeliverable`], which the actor posts to itself.
#[derive(Debug)]
pub struct UndeliverableOutcome {
    reason: UndeliverableReason,
    verdict: UndeliverableVerdict,
}

#[derive(Debug)]
enum UndeliverableVerdict {
    Handled,
    Unhandled(Box<Undeliverable<MessageEnvelope>>),
    Raised(anyhow::Error),
}

#[async_trait]
impl Handler<UndeliverableOutcome> for PythonActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        outcome: UndeliverableOutcome,
    ) -> anyhow::Result<()> {
        match outcome.verdict {
            UndeliverableVerdict::Handled => Ok(()),
            UndeliverableVerdict::Unhandled(envelope) => {
                hyperactor::actor::handle_undeliverable_message(cx, outcome.reason, *envelope)
            }
            UndeliverableVerdict::Raised(err) => Err(err),
        }
    }
}

/// The verdict of `__supervise__` on a [`QueuedSupervision`], which the
/// actor posts to itself.
#[derive(Debug)]
pub struct SupervisionOutcome {
    failure: MeshFailure,
    display_name: Option<String>,
    verdict: SupervisionVerdict,
}

#[derive(Debug)]
enum SupervisionVerdict {
    Handled,
    Unhandled,
    /// `__supervise__` raised the formatted exception.
    Raised(String),
}

#[async_trait]
impl Handler<MeshFailure> for PythonActor {
    async fn handle(&mut self, cx: &Context<Self>, message: MeshFailure) -> anyhow::Result<()> {
        // If the message is not about a failure, don't call __supervise__.
        // This includes messages like "stop", because those are not errors that
        // need to be propagated.
        if !message.event.actor_status.is_failed() {
            tracing::info!(
                "ignoring non-failure supervision event from child: {}",
                message
            );
            return Ok(());
        }

        // Enqueue instead of waiting for `__supervise__`, for the reasons
        // given on `Actor::handle_supervision_event`. The verdict arrives as
        // a `SupervisionOutcome`.
        let pending = PendingSupervision {
            instance: self
                .instance
                .clone()
                .expect("PythonActor::init should have created the instance"),
            rank: cx.cast_point(),
            recording_span: cx.recording_span(),
            reply: SupervisionReply {
                port: cx.port(),
                instance: cx.into(),
                failure: message,
                display_name: None,
                _in_flight: InFlight::new(&self.callbacks_pending),
            },
        };

        self.control_sender
            .send(pending)
            .map_err(|_| anyhow::anyhow!("failed to send supervision event to queue"))?;
        Ok(())
    }
}

#[async_trait]
impl Handler<SupervisionOutcome> for PythonActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        outcome: SupervisionOutcome,
    ) -> anyhow::Result<()> {
        let SupervisionOutcome {
            failure: message,
            display_name,
            verdict,
        } = outcome;
        let (status, description, cause) = match verdict {
            SupervisionVerdict::Handled => {
                // TODO: We also don't want to deliver multiple supervision
                // events from the same mesh if an earlier one is handled.
                tracing::info!(
                    name = "ActorMeshStatus",
                    status = "SupervisionError::Handled",
                    // only care about the event sender when the message is handled
                    actor_name = message.actor_mesh_name,
                    event = %message.event,
                    "__supervise__ on {} handled a supervision event, not reporting any further",
                    cx.self_addr(),
                );
                return Ok(());
            }
            // We propagate the event to the next owning actor by failing
            // with a new event that names this actor as its creator. This
            // does not set the causal chain for ActorSupervisionEvent, so
            // the original event is included in the error.
            SupervisionVerdict::Unhandled => (
                "SupervisionError::Unhandled",
                "did not handle a supervision event, reporting to the next owner",
                ActorErrorKind::UnhandledSupervisionEvent(Box::new(message.event.clone())),
            ),
            SupervisionVerdict::Raised(err) => (
                "SupervisionError::__supervise__::exception",
                "threw an exception",
                ActorErrorKind::ErrorDuringHandlingSupervision(
                    err,
                    Box::new(message.event.clone()),
                ),
            ),
        };
        for (actor_name, status) in [
            (
                message
                    .actor_mesh_name
                    .as_deref()
                    .unwrap_or_else(|| message.event.actor_id.log_name()),
                status,
            ),
            (cx.self_addr().log_name(), "UnhandledSupervisionEvent"),
        ] {
            tracing::info!(
                name = "ActorMeshStatus",
                status,
                actor_name,
                event = %message.event,
                "__supervise__ on {} {}",
                cx.self_addr(),
                description,
            );
        }
        Err(anyhow::Error::new(
            ActorErrorKind::UnhandledSupervisionEvent(Box::new(ActorSupervisionEvent::new(
                cx.self_addr().clone(),
                display_name,
                ActorStatus::Failed(cause),
                None,
            ))),
        ))
    }
}

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
struct LocalPort {
    instance: PyInstance,
    inner: Option<OncePortHandle<Result<Py<PyAny>, Py<PyAny>>>>,
}

impl Debug for LocalPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalPort")
            .field("inner", &self.inner)
            .finish()
    }
}

#[pymethods]
impl LocalPort {
    fn send(&mut self, obj: Py<PyAny>) -> PyResult<()> {
        let port = self.inner.take().expect("use local port once");
        port.post(self.instance.deref(), Ok(obj));
        Ok(())
    }
    /// Async-actor reply entry point. A local value needs no mesh-reference
    /// resolution, so it can be sent immediately.
    fn _resolve_and_send(&mut self, obj: Py<PyAny>) -> PyResult<()> {
        self.send(obj)
    }
    /// Sync-actor reply entry point. A local value needs no asynchronous work,
    /// so the blocking path is the same immediate send.
    fn _resolve_and_send_blocking(&mut self, obj: Py<PyAny>) -> PyResult<()> {
        self.send(obj)
    }
    fn exception(&mut self, e: Py<PyAny>) -> PyResult<()> {
        let port = self.inner.take().expect("use local port once");
        port.post(self.instance.deref(), Err(e));
        Ok(())
    }
}

/// A port that drops all messages sent to it.
/// Used when there is no response port for a message.
/// Any exceptions sent to it are re-raised in the current actor.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Debug)]
pub struct DroppingPort;

#[pymethods]
impl DroppingPort {
    #[new]
    fn new() -> Self {
        DroppingPort
    }

    fn send(&self, _obj: Py<PyAny>) -> PyResult<()> {
        Ok(())
    }

    /// Async-actor reply entry point. Dropping the value requires no reference
    /// resolution.
    fn _resolve_and_send(&self, obj: Py<PyAny>) -> PyResult<()> {
        self.send(obj)
    }

    /// Sync-actor reply entry point. Dropping the value requires no work to
    /// wait for, so the blocking path is also immediate.
    fn _resolve_and_send_blocking(&self, obj: Py<PyAny>) -> PyResult<()> {
        self.send(obj)
    }

    fn exception(&self, e: Bound<'_, PyAny>) -> PyResult<()> {
        // Unwrap ActorError to get the inner exception, matching Python behavior.
        let exc = if let Ok(inner) = e.getattr("exception") {
            inner
        } else {
            e
        };
        Err(PyErr::from_value(exc))
    }

    #[getter]
    fn get_return_undeliverable(&self) -> bool {
        true
    }

    #[setter]
    fn set_return_undeliverable(&self, _value: bool) {}
}

/// A port that sends messages to a remote receiver.
/// Wraps an EitherPortRef with the actor instance needed for sending.
#[pyclass(module = "monarch._src.actor.actor_mesh")]
pub struct Port {
    port_ref: EitherPortRef,
    instance: Instance<PythonActor>,
    rank: Option<usize>,
    /// Operation-context headers captured from the inbound request,
    /// re-emitted on every reply so failure surfaces can name the
    /// operation.
    reply_headers: hyperactor_config::Flattrs,
}

#[pymethods]
impl Port {
    #[new]
    fn new(
        port_ref: EitherPortRef,
        instance: &crate::context::PyInstance,
        rank: Option<usize>,
    ) -> Self {
        Self {
            port_ref,
            instance: instance.clone().into_instance(),
            rank,
            reply_headers: hyperactor_config::Flattrs::new(),
        }
    }

    #[getter("_port_ref")]
    fn port_ref_py(&self) -> EitherPortRef {
        self.port_ref.clone()
    }

    #[getter("_rank")]
    fn rank_py(&self) -> Option<usize> {
        self.rank
    }

    #[getter]
    fn get_return_undeliverable(&self) -> bool {
        self.port_ref.get_return_undeliverable()
    }

    #[setter]
    fn set_return_undeliverable(&mut self, value: bool) {
        self.port_ref.set_return_undeliverable(value);
    }

    // HOT PATH: Be mindful of performance when making changes here.
    // To test how performance is affected by a change, run the RPC benchmarks in
    // `monarch/python/benches/`.
    #[tracing::instrument(level = "debug", skip_all)]
    fn send(&mut self, py: Python<'_>, obj: Py<PyAny>) -> PyResult<()> {
        let message = PythonMessage::new_from_buf(
            PythonMessageKind::Result { rank: self.rank },
            pickle_to_part(py, &obj)?,
        );

        self.port_ref
            .post_with_headers(&self.instance, self.reply_headers.clone(), message)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn _send_message(&mut self, message: PythonMessage) -> PyResult<()> {
        self.port_ref
            .post_with_headers(&self.instance, self.reply_headers.clone(), message)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn exception(&mut self, py: Python<'_>, e: Py<PyAny>) -> PyResult<()> {
        let message = PythonMessage::new_from_buf(
            PythonMessageKind::Exception { rank: self.rank },
            pickle_to_part(py, &e)?,
        );

        self.port_ref
            .post_with_headers(&self.instance, self.reply_headers.clone(), message)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

impl Port {
    /// Constructor that attaches operation-context headers captured
    /// from the inbound request. The Python `#[new]` constructor
    /// defaults to empty headers.
    pub(crate) fn with_reply_headers(
        port_ref: EitherPortRef,
        instance: Instance<PythonActor>,
        rank: Option<usize>,
        reply_headers: hyperactor_config::Flattrs,
    ) -> Self {
        Self {
            port_ref,
            instance,
            rank,
            reply_headers,
        }
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PythonActorHandle>()?;
    hyperactor_mod.add_class::<PythonMessage>()?;
    hyperactor_mod.add_class::<PyMeshRef>()?;
    hyperactor_mod.add_class::<PythonMessageKind>()?;
    hyperactor_mod.add_class::<MethodSpecifier>()?;
    hyperactor_mod.add_class::<UnflattenArg>()?;
    hyperactor_mod.add_class::<QueuedMessage>()?;
    hyperactor_mod.add_class::<QueuedSupervision>()?;
    hyperactor_mod.add_class::<CallbacksPending>()?;
    hyperactor_mod.add_class::<QueuedUndeliverable>()?;
    hyperactor_mod.add_class::<DroppingPort>()?;
    hyperactor_mod.add_class::<Port>()?;
    hyperactor_mod.add_class::<ActorKind>()?;
    sync_actor::dispatch::register_python_bindings(hyperactor_mod)?;
    sync_actor::inbox::register_python_bindings(hyperactor_mod)?;
    Ok(())
}

/// Register actor-specific Python test seams in the shared testing module.
pub(crate) fn register_testing_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    sync_actor::inbox::register_testing_python_bindings(module)
}

#[cfg(test)]
mod tests {
    use futures::future::FutureExt;
    use hyperactor as reference;
    use hyperactor::accum::ReducerSpec;
    use hyperactor::accum::StreamingReducerOpts;
    use hyperactor::id::Label;
    use hyperactor::testing::ids::test_port_id;
    use hyperactor_mesh::Error as MeshError;
    use hyperactor_mesh::host_mesh::host_agent::ProcState;
    use hyperactor_mesh::mesh_id::ResourceId;
    use hyperactor_mesh::resource::Status;
    use hyperactor_mesh::resource::{self};
    use pyo3::PyTypeInfo;
    use pyo3::exceptions::PyValueError;
    use pyo3::ffi::c_str;
    use pyo3::panic::PanicException;

    use super::*;
    use crate::actor::to_py_error;

    #[test]
    fn test_call_method_indirect_correlation_id_round_trip() {
        let kind = PythonMessageKind::CallMethodIndirect {
            name: MethodSpecifier::ReturnsResponse {
                name: "forward".to_string(),
            },
            local_state_broker: ("broker".to_string(), 3),
            id: 7,
            unflatten_args: vec![UnflattenArg::Mailbox, UnflattenArg::PyObject],
            correlation_id: Some(42),
        };

        let serialized = wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&kind)
            .expect("indirect call message should serialize");
        let decoded = serialized
            .deserialized_unchecked::<PythonMessageKind>()
            .expect("serialized indirect call message should deserialize");

        assert_eq!(decoded, kind);
    }

    #[test]
    fn test_python_message_part_codec() {
        let reducer_spec = ReducerSpec {
            typehash: 123,
            builder_params: Some(wirevalue::Any::serialize(&"abcdefg12345".to_string()).unwrap()),
        };
        let port_ref = hyperactor::PortRef::<PythonMessage>::attest_reducible(
            test_port_id("world_0", "client", 123),
            Some(reducer_spec),
            StreamingReducerOpts::default(),
        );
        let message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ReturnsResponse {
                    name: "test".to_string(),
                },
                response_port: Some(EitherPortRef::Unbounded(port_ref.clone().into())),
                correlation_id: None,
            },
            message: Part::from(vec![1, 2, 3]),
            refs: Vec::new(),
        };
        {
            let mut multipart_message =
                wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&message).unwrap();
            let mut ports = vec![];
            multipart_message
                .visit_multipart_parts_mut::<reference::PortRefRepr, anyhow::Error>(|b| {
                    ports.push(b.clone());
                    Ok(())
                })
                .unwrap();
            assert_eq!(ports.len(), 1);
            assert_eq!(ports[0].port_addr(), port_ref.port_addr());
            assert_eq!(ports[0].reducer_spec(), port_ref.reducer_spec());
            assert_eq!(
                ports[0].get_return_undeliverable(),
                port_ref.get_return_undeliverable()
            );
            assert!(!ports[0].unsplit());
            assert_eq!(
                message,
                multipart_message
                    .deserialized_unchecked::<PythonMessage>()
                    .unwrap()
            );
        }

        let no_port_message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ReturnsResponse {
                    name: "test".to_string(),
                },
                response_port: None,
                correlation_id: None,
            },
            ..message
        };
        {
            let mut multipart_message =
                wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&no_port_message)
                    .unwrap();
            let mut ports = vec![];
            multipart_message
                .visit_multipart_parts_mut::<reference::PortRefRepr, anyhow::Error>(|b| {
                    ports.push(b.clone());
                    Ok(())
                })
                .unwrap();
            assert_eq!(ports.len(), 0);
            assert_eq!(
                no_port_message,
                multipart_message
                    .deserialized_unchecked::<PythonMessage>()
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_python_message_refs_travel_as_parts() {
        // A non-live proc mesh ref, built in-memory from ids (no spawn).
        fn proc_mesh_ref(seed: u64, label: &str) -> MeshRef {
            let proc_id = hyperactor::ProcId::new(
                hyperactor::id::Uid::Instance(seed, None),
                Some(Label::new("local").unwrap()),
            );
            let proc_addr = hyperactor::ProcAddr::new(
                proc_id,
                hyperactor::channel::ChannelAddr::Local(seed).into(),
            );
            let agent: hyperactor::ActorRef<hyperactor_mesh::proc_agent::ProcAgent> =
                hyperactor::ActorRef::attest(
                    proc_addr.actor_addr(hyperactor_mesh::proc_agent::PROC_AGENT_ACTOR_NAME),
                );
            let proc_ref = hyperactor_mesh::proc_mesh::ProcRef::new(proc_addr, 0, agent);
            MeshRef::Proc(Box::new(
                hyperactor_mesh::proc_mesh::ProcMeshRef::new_singleton(
                    hyperactor_mesh::mesh_id::ProcMeshId::singleton(Label::new(label).unwrap()),
                    proc_ref,
                )
                .unwrap(),
            ))
        }

        let message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ReturnsResponse {
                    name: "test".to_string(),
                },
                response_port: None,
                correlation_id: None,
            },
            message: Part::from(vec![1, 2, 3]),
            refs: vec![proc_mesh_ref(1, "a"), proc_mesh_ref(2, "b")],
        };

        let mut multipart_message =
            wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&message).unwrap();
        let mut parts = vec![];
        multipart_message
            .visit_multipart_parts_mut::<MeshRefRepr, anyhow::Error>(|b| {
                parts.push(b.clone());
                Ok(())
            })
            .unwrap();
        // Each MeshRef rides as its own typed part on the multipart wire.
        assert_eq!(parts.len(), 2);
        // And the message round-trips, reuniting the refs from those parts.
        assert_eq!(
            message,
            multipart_message
                .deserialized_unchecked::<PythonMessage>()
                .unwrap()
        );
    }

    #[test]
    fn to_py_error_preserves_proc_creation_message() {
        // State<ProcState> w/ `state.is_none()`
        let state: resource::State<ProcState> = resource::State {
            id: ResourceId::instance(Label::new("my-proc").unwrap()),
            status: Status::Failed("boom".into()),
            state: None,
            generation: 0,
            timestamp: std::time::SystemTime::now(),
        };

        // A ProcCreationError
        let mesh_agent: hyperactor::ActorRef<hyperactor_mesh::host_mesh::HostAgent> =
            hyperactor::ActorRef::attest(test_port_id("hello_0", "actor", 0).actor_addr());
        let expected_prefix = format!(
            "error creating proc (host rank 0) on host mesh agent {}",
            mesh_agent
        );
        let err = MeshError::ProcCreationError {
            host_rank: 0,
            mesh_agent,
            state: Box::new(state),
        };

        let rust_msg = err.to_string();
        let pyerr = to_py_error(err);

        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(pyerr.get_type(py).is(PyValueError::type_object(py)));
            let py_msg = pyerr.value(py).to_string();

            // 1) Bridge preserves the exact message
            assert_eq!(py_msg, rust_msg);
            // 2) Contains the structured state and failure status
            assert!(py_msg.contains(", state: "));
            assert!(py_msg.contains("\"status\":{\"Failed\":\"boom\"}"));
            // 3) Starts with the expected prefix
            assert!(py_msg.starts_with(&expected_prefix));
        });
    }

    // -- direct response ports ------------------------------------------
    //
    // Both implementations complete their effect before returning. Python sees
    // `None`; there is no task to drive or discard.

    /// A real `LocalPort` over a live once port, plus its receiver.
    ///
    /// The proc is deliberately not returned: the `Instance` owns a clone of it
    /// and the port owns the `Instance`, so the port keeps the proc alive by
    /// itself. It is an isolated proc because delivery never leaves the process
    /// -- posting to a once port hands the value to a oneshot sender -- so no
    /// served channel is needed, and a direct proc would serve one from a
    /// spawned task that outlives the `Proc`.
    ///
    /// `actor_instance` spawns detached introspect tasks, so callers must be
    /// `#[tokio::test]`: cleanup is the per-test runtime being dropped. Driving
    /// this fixture from the shared runtime would leak those tasks for the life
    /// of the test binary.
    fn local_port_fixture() -> (
        LocalPort,
        hyperactor::mailbox::OncePortReceiver<Result<Py<PyAny>, Py<PyAny>>>,
    ) {
        let proc = Proc::isolated();
        let instance = proc
            .actor_instance::<PythonActor>("resolve_and_send_client")
            .unwrap()
            .instance;
        let (handle, receiver) = instance.open_once_port::<Result<Py<PyAny>, Py<PyAny>>>();
        let port = LocalPort {
            instance: PyInstance::from(instance),
            inner: Some(handle),
        };
        (port, receiver)
    }

    // The single non-yielding poll is what makes this precise. Awaiting the
    // receiver would also accept a post made later by some other task, and
    // would hang rather than fail if the value never arrived at all. Requiring
    // the value to be there without ever yielding is the actual claim.
    #[tokio::test]
    async fn local_port_resolve_and_send_posts_before_return() {
        pyo3::Python::initialize();
        let (mut port, receiver) = local_port_fixture();

        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = 41i64.into_py_any(py).unwrap();
            port._resolve_and_send(value).unwrap()
        });

        let received = receiver
            .recv()
            .now_or_never()
            .expect("the value must be posted before _resolve_and_send returns")
            .unwrap();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_eq!(
                received.unwrap().extract::<i64>(py).unwrap(),
                41,
                "the synchronously posted value must be preserved"
            );
        });
    }

    // Check the Python-visible return rather than relying only on Rust's unit
    // return type: PyO3 must expose direct completion as `None`.
    #[tokio::test]
    async fn local_port_resolve_and_send_returns_none() {
        pyo3::Python::initialize();
        let (port, receiver) = local_port_fixture();

        monarch_with_gil_blocking(GilSite::Test, |py| {
            let port = Py::new(py, port).unwrap();
            let returned = port
                .bind(py)
                .call_method1("_resolve_and_send", (42,))
                .unwrap();
            assert!(returned.is_none(), "direct completion must return None");
        });

        let received = receiver
            .recv()
            .now_or_never()
            .expect("the value must be posted before _resolve_and_send returns")
            .unwrap();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_eq!(
                received.unwrap().extract::<i64>(py).unwrap(),
                42,
                "returning None must not change the posted value"
            );
        });
    }

    // KNOWN-BAD CURRENT BEHAVIOR: `LocalPort` is one-shot by panic, not by
    // error. `send` unwraps the taken handle with `expect("use local port once")`,
    // so a second call aborts the frame rather than returning `Err`. This test
    // characterizes that behavior; it does not make the panic a contract.
    //
    // The first call sits outside the `try` so that only the second call can
    // satisfy the assertions; a first-call failure surfaces as a test error
    // instead of masquerading as the expected one. The rest runs inside Python
    // because PyO3 maps the panic to `PanicException` at the boundary but
    // resumes it as a Rust panic if it escapes back out, so catching it in
    // Python is what makes the production-visible type observable. That type is
    // compared by identity against PyO3's own `PanicException`, which derives
    // from `BaseException` -- hence the `except BaseException`, and hence a
    // caller's ordinary error handling never sees this.
    #[tokio::test]
    async fn local_port_second_resolve_and_send_raises_panic_exception() {
        pyo3::Python::initialize();
        let (port, _receiver) = local_port_fixture();

        monarch_with_gil_blocking(GilSite::Test, |py| {
            let locals = PyDict::new(py);
            locals.set_item("port", Py::new(py, port).unwrap()).unwrap();
            py.run(
                c_str!(
                    r#"
assert port._resolve_and_send(1) is None
try:
    port._resolve_and_send(2)
except BaseException as err:
    raised = type(err)
    message = str(err)
else:
    raise AssertionError("a second _resolve_and_send must fail")
"#
                ),
                None,
                Some(&locals),
            )
            .unwrap();

            let raised = locals.get_item("raised").unwrap().unwrap();
            let message: String = locals
                .get_item("message")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!(
                raised.is(PanicException::type_object(py)),
                "a second send must surface as PanicException, got {raised}"
            );
            assert!(
                message.contains("use local port once"),
                "expected the one-shot panic message, got {message}"
            );
        });
    }

    // `DroppingPort` is stateless, so repeated direct completions return `None`.
    #[test]
    fn dropping_port_resolve_and_send_returns_none_and_is_idempotent() {
        pyo3::Python::initialize();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let port = Py::new(py, DroppingPort).unwrap();
            for value in [7, 8, 9] {
                let returned = port
                    .bind(py)
                    .call_method1("_resolve_and_send", (value,))
                    .unwrap();
                assert!(
                    returned.is_none(),
                    "every DroppingPort completion must return None"
                );
            }
        });
    }
}
