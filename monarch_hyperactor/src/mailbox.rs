/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::Arc;

use hyperactor::Endpoint as _;
use hyperactor::Mailbox;
use hyperactor::OncePortHandle;
use hyperactor::PortHandle;
use hyperactor::RemoteEndpoint as _;
use hyperactor::accum::Accumulator;
use hyperactor::accum::CommReducer;
use hyperactor::accum::ReducerFactory;
use hyperactor::accum::ReducerSpec;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::OncePortReceiver;
use hyperactor::mailbox::PortReceiver;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::monitored_return_handle;
use hyperactor_config::Flattrs;
use monarch_types::PickledPyObject;
use monarch_types::py_global;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyEOFError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::context::PyInstance;
use crate::handle::ObserverLoss;
use crate::handle::PyHandle;
use crate::handle::ReceiveOutcome;
use crate::pickle::PicklingState;
use crate::proc::PyActorAddr;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::traceback::current_traceback;

#[derive(Clone, Debug)]
#[pyclass(
    name = "Mailbox",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub struct PyMailbox {
    pub(super) inner: Mailbox,
}

impl PyMailbox {
    pub fn get_inner(&self) -> &Mailbox {
        &self.inner
    }
}

#[pymethods]
impl PyMailbox {
    fn open_port<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let (handle, receiver) = self.inner.open_port();
        let handle = Py::new(py, PythonPortHandle { inner: handle })?;
        let receiver = Py::new(
            py,
            PythonPortReceiver {
                inner: Arc::new(tokio::sync::Mutex::new(receiver)),
            },
        )?;
        PyTuple::new(py, vec![handle.into_any(), receiver.into_any()])
    }

    fn open_once_port<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let (handle, receiver) = self.inner.open_once_port();
        let handle = Py::new(
            py,
            PythonOncePortHandle {
                inner: Some(handle),
            },
        )?;
        let receiver = Py::new(
            py,
            PythonOncePortReceiver {
                inner: Arc::new(std::sync::Mutex::new(Some(receiver))),
            },
        )?;
        PyTuple::new(py, vec![handle.into_any(), receiver.into_any()])
    }

    fn open_accum_port<'py>(
        &self,
        py: Python<'py>,
        accumulator: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let py_accumulator = PythonAccumulator::new(py, accumulator)?;
        let (handle, receiver) = self.inner.open_accum_port(py_accumulator);
        let handle = Py::new(py, PythonPortHandle { inner: handle })?;
        let receiver = Py::new(
            py,
            PythonPortReceiver {
                inner: Arc::new(tokio::sync::Mutex::new(receiver)),
            },
        )?;
        PyTuple::new(py, vec![handle.into_any(), receiver.into_any()])
    }

    pub(super) fn post(&self, dest: &PyActorAddr, message: &PythonMessage) -> PyResult<()> {
        let port_id = dest
            .inner
            .port_addr(hyperactor::Port::handler::<PythonMessage>());
        let message = wirevalue::Any::serialize(message).map_err(|err| {
            PyRuntimeError::new_err(format!(
                "failed to serialize message ({:?}) to Any: {}",
                message, err
            ))
        })?;
        let envelope = MessageEnvelope::new(
            self.inner.actor_addr().clone(),
            port_id,
            message,
            Flattrs::new(),
        );
        let return_handle = self
            .inner
            .bound_return_handle()
            .unwrap_or(monitored_return_handle());
        self.inner.post(envelope, return_handle);
        Ok(())
    }

    #[getter]
    pub(super) fn actor_id(&self) -> PyActorAddr {
        PyActorAddr {
            inner: self.inner.actor_addr().clone(),
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

#[pyclass(
    frozen,
    name = "PortId",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
#[derive(Clone)]
pub struct PyPortId {
    inner: hyperactor::PortAddr,
}

impl From<hyperactor::PortAddr> for PyPortId {
    fn from(port_id: hyperactor::PortAddr) -> Self {
        Self { inner: port_id }
    }
}

impl From<PyPortId> for hyperactor::PortAddr {
    fn from(port_id: PyPortId) -> Self {
        port_id.inner
    }
}

impl From<Mailbox> for PyMailbox {
    fn from(inner: Mailbox) -> Self {
        PyMailbox { inner }
    }
}

#[pymethods]
impl PyPortId {
    #[new]
    #[pyo3(signature = (*, actor_id, port))]
    fn new(actor_id: &PyActorAddr, port: u64) -> Self {
        Self {
            inner: actor_id.inner.port_addr(port.into()),
        }
    }

    #[staticmethod]
    fn from_string(port_id: &str) -> PyResult<Self> {
        Ok(Self {
            inner: port_id.parse().map_err(|e| {
                PyValueError::new_err(format!("Failed to parse port id '{}': {}", port_id, e))
            })?,
        })
    }

    #[getter]
    fn actor_id(&self) -> PyActorAddr {
        PyActorAddr {
            inner: self.inner.actor_addr(),
        }
    }

    #[getter]
    fn index(&self) -> u64 {
        self.inner.index()
    }

    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.inner.to_string().hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(other) = other.extract::<PyPortId>() {
            Ok(self.inner == other.inner)
        } else {
            Ok(false)
        }
    }

    fn __reduce__<'py>(slf: &Bound<'py, Self>) -> PyResult<(Bound<'py, PyAny>, (String,))> {
        Ok((slf.getattr("from_string")?, (slf.borrow().__repr__(),)))
    }
}

impl std::fmt::Debug for PyPortId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[derive(Clone, Debug)]
#[pyclass(
    name = "PortHandle",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(crate) struct PythonPortHandle {
    inner: PortHandle<PythonMessage>,
}

#[pymethods]
impl PythonPortHandle {
    fn send(&self, instance: &PyInstance, message: PythonMessage) -> PyResult<()> {
        self.inner.post(instance.deref(), message);
        Ok(())
    }

    fn bind(&self) -> PythonPortRef {
        PythonPortRef {
            inner: self.inner.bind(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[pyclass(
    name = "PortRef",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub struct PythonPortRef {
    pub(crate) inner: hyperactor::PortRef<PythonMessage>,
}

#[pymethods]
impl PythonPortRef {
    #[new]
    fn new(port: PyPortId) -> Self {
        Self {
            inner: hyperactor::PortRef::attest(port.inner),
        }
    }
    fn __reduce__(slf: Bound<'_, PythonPortRef>) -> PyResult<(Bound<'_, PyType>, (PyPortId,))> {
        let id: PyPortId = (*slf.borrow()).inner.port_addr().clone().into();
        Ok((slf.get_type(), (id,)))
    }

    fn send(&self, instance: &PyInstance, message: PythonMessage) -> PyResult<()> {
        self.inner.post(instance.deref(), message);
        Ok(())
    }

    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    #[getter]
    fn port_id(&self) -> PyResult<PyPortId> {
        Ok(self.inner.port_addr().clone().into())
    }

    #[getter]
    fn get_return_undeliverable(&self) -> bool {
        self.inner.get_return_undeliverable()
    }

    #[setter]
    fn set_return_undeliverable(&mut self, return_undeliverable: bool) {
        self.inner.return_undeliverable(return_undeliverable);
    }
}

impl From<hyperactor::PortRef<PythonMessage>> for PythonPortRef {
    fn from(port_ref: hyperactor::PortRef<PythonMessage>) -> Self {
        Self { inner: port_ref }
    }
}

#[derive(Debug)]
#[pyclass(
    name = "PortReceiver",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(super) struct PythonPortReceiver {
    inner: Arc<tokio::sync::Mutex<PortReceiver<PythonMessage>>>,
}

// Called by recv() to capture the receiving actor's context. Only the
// Python function is cached here, not the context it returns.
py_global!(context, "monarch._src.actor.actor_mesh", "context");

// Provides _context, which we temporarily set to the captured context
// so unpickle hooks see the receiving actor on whichever thread decodes.
py_global!(context_var, "monarch._src.actor.actor_mesh", "_context");

/// Unpickle a channel's result: its value, or its remote exception as an
/// error. The outer error means decoding failed.
///
/// `Result` messages give the payload, or `(rank, payload)` when `ranked`
/// is true. `Exception` messages give the remote exception.
///
/// A ranked receive rejects a missing rank before unpickling. Unsupported
/// message kinds produce `ValueError`; unpickling errors propagate.
fn decode_channel_result(
    py: Python<'_>,
    message: PythonMessage,
    ranked: bool,
) -> PyResult<PyResult<Py<PyAny>>> {
    let rank = match &message.kind {
        PythonMessageKind::Result { rank } | PythonMessageKind::Exception { rank } => *rank,
        _ => None,
    };
    if ranked && rank.is_none() {
        let message = message.into_py_any(py)?;
        return Err(PyValueError::new_err(format!(
            "RankedPort receiver got a message without a rank {}",
            message.bind(py).str()?
        )));
    }
    let kind = message.kind;
    let refs = message.refs.into_iter().map(Some).collect();
    let payload = PicklingState::from_parts(message.message, VecDeque::new(), refs).unpickle(py)?;
    let outcome = match kind {
        PythonMessageKind::Result { .. } if ranked => (rank, payload).into_py_any(py),
        PythonMessageKind::Result { .. } => Ok(payload),
        PythonMessageKind::Exception { .. } => Err(PyErr::from_value(payload.into_bound(py))),
        kind => Err(PyValueError::new_err(format!(
            "Unexpected message kind: {}",
            kind.into_py_any(py)?.bind(py).str()?
        ))),
    };
    Ok(outcome)
}

/// Unpickle a channel result using the actor context captured when `recv()`
/// was called.
///
/// Unpickling can reconstruct Ports and other objects whose hooks call
/// `context()`. They must see the actor that called `recv()`, even though
/// decoding runs on a Tokio worker or on the thread of a `get()` that drives
/// the receive (HDL-24).
///
/// Temporarily install that context, then restore the previous context
/// whether decoding succeeds or returns an error.
fn decode_channel_result_in_context(
    py: Python<'_>,
    message: PythonMessage,
    ranked: bool,
    captured_context: Py<PyAny>,
) -> PyResult<PyResult<Py<PyAny>>> {
    let context_var = context_var(py);
    let previous = context_var.call_method1("get", (py.None(),))?;
    context_var.call_method1("set", (captured_context,))?;
    // Do not use ? here: a decoding error must not skip restoring the previous context.
    let result = decode_channel_result(py, message, ranked);
    context_var.call_method1("set", (previous,))?;
    result
}

/// Wait for one message, then decode it under the context captured at `recv()`.
async fn recv_async(
    receiver: Arc<tokio::sync::Mutex<PortReceiver<PythonMessage>>>,
    captured_context: Py<PyAny>,
    ranked: bool,
) -> ReceiveOutcome {
    // The receiver lock is released before waiting for the GIL or unpickling.
    let received = receiver.lock().await.recv().await;
    match received {
        Ok(message) => decode_received(message, ranked, captured_context).await,
        Err(err) => ReceiveOutcome::Outcome(Err(PyErr::new::<PyEOFError, _>(format!(
            "Port closed: {}",
            err
        )))),
    }
}

/// Decode a received message under the context captured at `recv()`. A
/// `get()` driving the receive on Python's main thread decodes it there,
/// where Ctrl-C can interrupt the decoding (HDL-26).
async fn decode_received(
    message: PythonMessage,
    ranked: bool,
    captured_context: Py<PyAny>,
) -> ReceiveOutcome {
    monarch_with_gil(GilSite::ReplyConvert, move |py| {
        ReceiveOutcome::decode(py, GilSite::ReplyConvert, message, move |py, message| {
            decode_channel_result_in_context(py, message, ranked, captured_context.clone_ref(py))
        })
    })
    .await
}

#[pymethods]
impl PythonPortReceiver {
    /// Return a Handle for one message. Its first observation receives and
    /// decodes it (HDL-24); a started receive stops once nobody can observe it
    /// (HDL-23).
    fn recv(&self, py: Python<'_>, ranked: bool) -> PyResult<PyHandle> {
        let captured_context = context(py).call0()?.unbind();
        let traceback = current_traceback()?;
        let receiver = self.inner.clone();
        Ok(PyHandle::deferred(
            py,
            recv_async(receiver, captured_context, ranked),
            traceback,
            ObserverLoss::Stop,
        ))
    }
}

impl PythonPortReceiver {
    #[allow(dead_code)]
    pub(super) fn inner(&self) -> Arc<tokio::sync::Mutex<PortReceiver<PythonMessage>>> {
        Arc::clone(&self.inner)
    }
}

#[derive(Debug)]
#[pyclass(
    name = "UndeliverableMessageEnvelope",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(crate) struct PythonUndeliverableMessageEnvelope {
    pub(crate) inner: Option<Undeliverable<MessageEnvelope>>,
}

impl PythonUndeliverableMessageEnvelope {
    fn inner(&self) -> PyResult<&Undeliverable<MessageEnvelope>> {
        self.inner.as_ref().ok_or_else(|| {
            PyErr::new::<PyRuntimeError, _>(
                "PythonUndeliverableMessageEnvelope was already consumed",
            )
        })
    }

    pub(crate) fn take(&mut self) -> anyhow::Result<Undeliverable<MessageEnvelope>> {
        self.inner.take().ok_or_else(|| {
            anyhow::anyhow!("PythonUndeliverableMessageEnvelope was already consumed")
        })
    }
}

#[pymethods]
impl PythonUndeliverableMessageEnvelope {
    fn __repr__(&self) -> PyResult<String> {
        let inner = self.inner()?;
        let Some(envelope) = inner.as_message() else {
            return Ok("UndeliverableMessageEnvelope(lost)".to_string());
        };
        Ok(format!(
            "UndeliverableMessageEnvelope(sender={}, dest={}, error={})",
            envelope.sender(),
            envelope.dest(),
            self.error_msg()?
        ))
    }

    fn sender(&self) -> PyResult<PyActorAddr> {
        let envelope = self.inner()?.as_message().ok_or_else(|| {
            PyErr::new::<PyRuntimeError, _>("undeliverable message reports do not have an envelope")
        })?;
        Ok(PyActorAddr {
            inner: envelope.sender().clone(),
        })
    }

    fn dest(&self) -> PyResult<PyPortId> {
        let envelope = self.inner()?.as_message().ok_or_else(|| {
            PyErr::new::<PyRuntimeError, _>("undeliverable message reports do not have an envelope")
        })?;
        let port_id: hyperactor::PortAddr = envelope.dest().clone();
        Ok(port_id.into())
    }

    fn error_msg(&self) -> PyResult<String> {
        match self.inner()? {
            Undeliverable::Returned(envelope) => {
                Ok(envelope.error_msg().unwrap_or_else(|| "None".to_string()))
            }
            Undeliverable::Report(report) => Ok(report.error_msg().unwrap_or_default()),
        }
    }
}

#[derive(Debug)]
#[pyclass(
    name = "OncePortHandle",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(super) struct PythonOncePortHandle {
    inner: Option<OncePortHandle<PythonMessage>>,
}

#[pymethods]
impl PythonOncePortHandle {
    fn send(&mut self, instance: &PyInstance, message: PythonMessage) -> PyResult<()> {
        let Some(port) = self.inner.take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePort is already used"));
        };
        port.post(instance.deref(), message);
        Ok(())
    }

    fn bind(&mut self) -> PyResult<PythonOncePortRef> {
        let Some(port) = self.inner.take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePort is already used"));
        };
        Ok(PythonOncePortRef {
            inner: Some(port.bind()),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[pyclass(
    name = "OncePortRef",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub struct PythonOncePortRef {
    pub(crate) inner: Option<hyperactor::OncePortRef<PythonMessage>>,
}

#[pymethods]
impl PythonOncePortRef {
    #[new]
    fn new(port: Option<PyPortId>) -> Self {
        Self {
            inner: port.map(|port| hyperactor::PortRef::attest(port.inner).into_once()),
        }
    }
    fn __reduce__(
        slf: Bound<'_, PythonOncePortRef>,
    ) -> PyResult<(Bound<'_, PyType>, (Option<PyPortId>,))> {
        let id: Option<PyPortId> = (*slf.borrow())
            .inner
            .as_ref()
            .map(|x: &hyperactor::OncePortRef<PythonMessage>| x.port_addr().clone().into());
        Ok((slf.get_type(), (id,)))
    }

    fn send(&mut self, instance: &PyInstance, message: PythonMessage) -> PyResult<()> {
        let Some(port_ref) = self.inner.take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePortRef is already used"));
        };
        let port_ref: hyperactor::OncePortRef<PythonMessage> = port_ref;
        port_ref.post(instance.deref(), message);
        Ok(())
    }

    fn __repr__(&self) -> String {
        self.inner.as_ref().map_or(
            "OncePortRef is already used".to_string(),
            |r: &hyperactor::OncePortRef<PythonMessage>| r.to_string(),
        )
    }

    #[getter]
    fn port_id(&self) -> PyResult<PyPortId> {
        Ok(self.inner.as_ref().unwrap().port_addr().clone().into())
    }

    #[getter]
    fn get_return_undeliverable(&self) -> bool {
        self.inner.as_ref().unwrap().get_return_undeliverable()
    }

    #[setter]
    fn set_return_undeliverable(&mut self, return_undeliverable: bool) {
        if let Some(ref mut inner) = self.inner {
            inner.return_undeliverable(return_undeliverable);
        }
    }
}

impl From<hyperactor::OncePortRef<PythonMessage>> for PythonOncePortRef {
    fn from(port_ref: hyperactor::OncePortRef<PythonMessage>) -> Self {
        Self {
            inner: Some(port_ref),
        }
    }
}

#[pyclass(
    name = "OncePortReceiver",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(super) struct PythonOncePortReceiver {
    inner: Arc<std::sync::Mutex<Option<OncePortReceiver<PythonMessage>>>>,
}

#[pymethods]
impl PythonOncePortReceiver {
    /// Return a Handle for the once port's sole message. Its first observation
    /// claims the port (HDL-24); a receive observed after that fails.
    fn recv(&self, py: Python<'_>, ranked: bool) -> PyResult<PyHandle> {
        let captured_context = context(py).call0()?.unbind();
        let traceback = current_traceback()?;
        let inner = self.inner.clone();
        let fut = async move {
            let Some(receiver) = inner.lock().unwrap().take() else {
                return ReceiveOutcome::Outcome(Err(PyErr::new::<PyValueError, _>(
                    "OncePort is already used",
                )));
            };
            match receiver.recv().await {
                Ok(message) => decode_received(message, ranked, captured_context).await,
                Err(err) => ReceiveOutcome::Outcome(Err(PyErr::new::<PyEOFError, _>(format!(
                    "Port closed: {}",
                    err
                )))),
            }
        };
        Ok(PyHandle::deferred(py, fut, traceback, ObserverLoss::Stop))
    }
}

impl PythonOncePortReceiver {
    #[allow(dead_code)]
    pub(super) fn inner(&self) -> Arc<std::sync::Mutex<Option<OncePortReceiver<PythonMessage>>>> {
        Arc::clone(&self.inner)
    }
}

#[derive(
    Clone,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    FromPyObject,
    IntoPyObject,
    Debug
)]
pub enum EitherPortRef {
    Unbounded(PythonPortRef),
    Once(PythonOncePortRef),
}

impl EitherPortRef {
    pub fn get_return_undeliverable(&self) -> bool {
        match self {
            EitherPortRef::Unbounded(port_ref) => port_ref.inner.get_return_undeliverable(),
            EitherPortRef::Once(once_port_ref) => once_port_ref.inner.as_ref().is_some_and(
                |r: &hyperactor::OncePortRef<PythonMessage>| r.get_return_undeliverable(),
            ),
        }
    }

    pub fn set_return_undeliverable(&mut self, return_undeliverable: bool) {
        match self {
            EitherPortRef::Unbounded(port_ref) => {
                port_ref.inner.return_undeliverable(return_undeliverable);
            }
            EitherPortRef::Once(once_port_ref) => {
                if let Some(ref mut inner) = once_port_ref.inner {
                    inner.return_undeliverable(return_undeliverable);
                }
            }
        }
    }

    /// Post a message through this port reference.
    /// The message is first resolved for any pending pickle state before sending.
    pub fn post(
        &mut self,
        cx: &impl hyperactor::context::Actor,
        message: crate::actor::PythonMessage,
    ) -> anyhow::Result<()> {
        match self {
            EitherPortRef::Unbounded(port_ref) => port_ref.inner.post(cx, message),
            EitherPortRef::Once(once_port_ref) => {
                let port = once_port_ref
                    .inner
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("OncePortRef already used"))?;
                port.post(cx, message);
            }
        }
        Ok(())
    }

    /// Post a message through this port reference with
    /// caller-supplied envelope headers. Delegates to the underlying
    /// `PortRef::post_with_headers` /
    /// `OncePortRef::post_with_headers`.
    pub fn post_with_headers(
        &mut self,
        cx: &impl hyperactor::context::Actor,
        headers: hyperactor_config::Flattrs,
        message: crate::actor::PythonMessage,
    ) -> anyhow::Result<()> {
        match self {
            EitherPortRef::Unbounded(port_ref) => {
                port_ref.inner.post_with_headers(cx, headers, message)
            }
            EitherPortRef::Once(once_port_ref) => {
                let port = once_port_ref
                    .inner
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("OncePortRef already used"))?;
                port.post_with_headers(cx, headers, message);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Named)]
struct PythonReducer(Py<PyAny>);

impl PythonReducer {
    fn new(params: Option<wirevalue::Any>) -> anyhow::Result<Self> {
        let p = params.ok_or_else(|| anyhow::anyhow!("params cannot be None"))?;
        let obj: PickledPyObject = p.deserialized()?;
        Ok(monarch_with_gil_blocking(
            GilSite::Reducer,
            |py: Python<'_>| -> PyResult<Self> {
                let unpickled = obj.unpickle(py)?;
                Ok(Self(unpickled.unbind()))
            },
        )?)
    }
}

impl CommReducer for PythonReducer {
    type Update = PythonMessage;

    fn reduce(&self, left: Self::Update, right: Self::Update) -> anyhow::Result<Self::Update> {
        monarch_with_gil_blocking(
            GilSite::Reducer,
            |py: Python<'_>| -> PyResult<PythonMessage> {
                let result = self.0.call(py, (left, right), None)?;
                result.extract::<PythonMessage>(py).map_err(Into::into)
            },
        )
        .map_err(Into::into)
    }
}

struct PythonAccumulator {
    accumulator: Py<PyAny>,
    reducer: Option<wirevalue::Any>,
}

impl PythonAccumulator {
    fn new(py: Python<'_>, accumulator: Py<PyAny>) -> PyResult<Self> {
        let py_reducer = accumulator.getattr(py, "reducer")?;
        let reducer: Option<wirevalue::Any> = if py_reducer.is_none(py) {
            None
        } else {
            let pickled = PickledPyObject::cloudpickle(py_reducer.bind(py))?;
            Some(
                wirevalue::Any::serialize(&pickled)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
            )
        };

        Ok(Self {
            accumulator,
            reducer,
        })
    }
}

impl Accumulator for PythonAccumulator {
    type State = PythonMessage;
    type Update = PythonMessage;

    fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()> {
        monarch_with_gil_blocking(GilSite::Accumulate, |py: Python<'_>| -> PyResult<()> {
            // Initialize state if it is empty.
            if matches!(state.kind, PythonMessageKind::Uninit {}) {
                *state = self
                    .accumulator
                    .getattr(py, "initial_state")?
                    .extract::<PythonMessage>(py)?;
            }

            // TODO(pzhang) Make accumulate consumes state and update, and returns
            // a new state. That will avoid this clone.
            let old_state = state.clone();
            let result = self.accumulator.call(py, (old_state, update), None)?;
            *state = result.extract::<PythonMessage>(py)?;
            Ok(())
        })
        .map_err(Into::into)
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        self.reducer.as_ref().map(|r| ReducerSpec {
            typehash: <PythonReducer as Named>::typehash(),
            builder_params: Some(r.clone()),
        })
    }
}

inventory::submit! {
    ReducerFactory {
        typehash_f: <PythonReducer as Named>::typehash,
        builder_f: |params| Ok(Box::new(PythonReducer::new(params)?)),
    }
}

py_global!(point, "monarch._src.actor.actor_mesh", "Point");

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyMailbox>()?;
    hyperactor_mod.add_class::<PyPortId>()?;
    hyperactor_mod.add_class::<PythonPortHandle>()?;
    hyperactor_mod.add_class::<PythonPortRef>()?;
    hyperactor_mod.add_class::<PythonPortReceiver>()?;
    hyperactor_mod.add_class::<PythonOncePortHandle>()?;
    hyperactor_mod.add_class::<PythonOncePortRef>()?;
    hyperactor_mod.add_class::<PythonOncePortReceiver>()?;
    hyperactor_mod.add_class::<PythonUndeliverableMessageEnvelope>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::Instance;
    use hyperactor::Proc;
    use pyo3::exceptions::PyModuleNotFoundError;
    use pyo3::types::PyDict;
    use serde_multipart::Part;

    use super::*;
    use crate::actor::MethodSpecifier;
    use crate::actor::PythonActor;
    use crate::context::PyContext;
    use crate::handle::PyHandle;
    use crate::runtime::ensure_python;
    use crate::traceback::ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK;

    const WAIT: Duration = Duration::from_secs(30);

    /// Supply missing Python package boundaries in the Rust-only test target.
    /// Existing modules and their attributes are left intact.
    fn test_module<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyModule>> {
        let modules = py
            .import("sys")?
            .getattr("modules")?
            .cast_into::<PyDict>()?;
        let mut current = String::new();
        let mut parent: Option<Bound<'py, PyModule>> = None;
        for part in path.split('.') {
            if !current.is_empty() {
                current.push('.');
            }
            current.push_str(part);
            let module = match modules.get_item(&current)? {
                Some(module) => module.cast_into::<PyModule>()?,
                None => {
                    let module = PyModule::new(py, &current)?;
                    modules.set_item(&current, &module)?;
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
        Ok(parent.expect("the test module path is nonempty"))
    }

    /// Provide only the context variable and plain-pickle hooks used by these
    /// integer/exception witnesses. Real channel payloads are covered in Python.
    fn install_decode_test_modules(py: Python<'_>) -> PyResult<()> {
        let helpers = PyModule::from_code(
            py,
            cr#"
from contextvars import ContextVar
_context = ContextVar("mailbox_test_context", default=None)
def context():
    return _context.get()
def maybe_torch():
    return False
"#,
            c"mailbox_test_helpers.py",
            c"mailbox_test_helpers",
        )?;
        let actor_mesh = test_module(py, "monarch._src.actor.actor_mesh")?;
        if actor_mesh.getattr("context").is_err() {
            actor_mesh.setattr("_context", helpers.getattr("_context")?)?;
            actor_mesh.setattr("context", helpers.getattr("context")?)?;
        }
        let pickle_hooks = test_module(py, "monarch._src.actor.pickle")?;
        if pickle_hooks.getattr("maybe_torch").is_err() {
            pickle_hooks.setattr("maybe_torch", helpers.getattr("maybe_torch")?)?;
        }
        match py.import("cloudpickle") {
            Ok(_) => (),
            Err(error) if error.is_instance_of::<PyModuleNotFoundError>(py) => {
                let cloudpickle = test_module(py, "cloudpickle")?;
                cloudpickle.setattr("cloudpickle", &cloudpickle)?;
                cloudpickle.setattr("loads", py.import("pickle")?.getattr("loads")?)?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    /// Python's default object str includes an address, not message content.
    fn without_object_address(text: &str) -> &str {
        text.split(" at 0x")
            .next()
            .expect("split always has a first part")
    }

    /// Give the binding a real actor context without bootstrapping a root client.
    fn in_actor_context<T>(
        instance: &Instance<PythonActor>,
        f: impl FnOnce(Python<'_>) -> PyResult<T>,
    ) -> T {
        monarch_with_gil_blocking(GilSite::Test, |py| -> PyResult<T> {
            install_decode_test_modules(py)?;
            let instance: PyInstance = instance.into();
            let captured = py
                .get_type::<PyContext>()
                .call_method1("_from_instance", (instance,))?;
            let context_var = context_var(py);
            let token = context_var.call_method1("set", (captured,))?;
            let result = f(py);
            context_var.call_method1("reset", (token,))?;
            result
        })
        .expect("the binding should run under the test actor's context")
    }

    fn test_instance(name: &str) -> Instance<PythonActor> {
        ensure_python();
        Proc::isolated()
            .actor_instance::<PythonActor>(name)
            .unwrap()
            .instance
    }

    /// Serialize a valid payload, rather than relying on empty default bytes.
    fn message(
        kind: PythonMessageKind,
        value: impl FnOnce(Python<'_>) -> Py<PyAny>,
    ) -> PythonMessage {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let bytes = py
                .import("pickle")
                .unwrap()
                .call_method1("dumps", (value(py),))
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap();
            PythonMessage {
                kind,
                message: Part::from(bytes),
                refs: Vec::new(),
            }
        })
    }

    fn result_message() -> PythonMessage {
        message(PythonMessageKind::Result { rank: Some(7) }, |py| {
            42.into_py_any(py).unwrap()
        })
    }

    /// Start a receive the way a Python observer does, without reading it
    /// (HDL-24).
    fn start(py: Python<'_>, handle: PyHandle) -> PyResult<PyHandle> {
        handle.poll(py)?;
        Ok(handle)
    }

    async fn value(handle: &PyHandle) -> Py<PyAny> {
        tokio::time::timeout(WAIT, handle.wait_future())
            .await
            .expect("the receive should complete before the deadline")
            .expect("the receive should publish a value")
    }

    fn assert_value(value: Py<PyAny>) {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert_eq!(value.extract::<i64>(py).unwrap(), 42);
        });
    }

    // A receive takes nothing until it is observed, even while it is alive: the
    // queued message goes to the receive that is observed.
    #[tokio::test]
    async fn port_recv_waits_for_observation() {
        let client = test_instance("port_recv_waits_for_observation");
        let (sender, receiver) = client.mailbox_for_py().open_port::<PythonMessage>();
        let receiver = PythonPortReceiver {
            inner: Arc::new(tokio::sync::Mutex::new(receiver)),
        };
        sender.try_post(&client, result_message()).unwrap();
        let unobserved = in_actor_context(&client, |py| receiver.recv(py, false));
        let observed = in_actor_context(&client, |py| start(py, receiver.recv(py, false)?));
        assert_value(value(&observed).await);
        drop(unobserved);
    }

    // The once port is claimed by the first receive observed, not the first
    // created; a receive observed after that fails.
    #[tokio::test]
    async fn once_recv_claims_on_first_observation() {
        let client = test_instance("once_recv_claims_on_first_observation");
        let (sender, receiver) = client.mailbox_for_py().open_once_port::<PythonMessage>();
        let receiver = PythonOncePortReceiver {
            inner: Arc::new(std::sync::Mutex::new(Some(receiver))),
        };
        drop(in_actor_context(&client, |py| receiver.recv(py, false)));
        let first = in_actor_context(&client, |py| receiver.recv(py, false));
        let second = in_actor_context(&client, |py| receiver.recv(py, false));
        sender.try_post(&client, result_message()).unwrap();
        assert_value(value(&first).await);
        let error = tokio::time::timeout(WAIT, second.wait_future())
            .await
            .expect("the second receive should finish")
            .expect_err("the once port is already claimed");
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(error.is_instance_of::<PyValueError>(py));
            assert_eq!(error.value(py).to_string(), "OncePort is already used");
        });
    }

    // Wait for destruction before sending. Drop-then-send alone would race the
    // cooperative stop and could legitimately have its message taken.
    #[tokio::test]
    async fn spawned_receive_stops_when_unobserved() {
        let client = test_instance("spawned_receive_stops_when_unobserved");
        let (sender, receiver) = client.mailbox_for_py().open_port::<PythonMessage>();
        let receiver = PythonPortReceiver {
            inner: Arc::new(tokio::sync::Mutex::new(receiver)),
        };
        drop(in_actor_context(&client, |py| {
            start(py, receiver.recv(py, false)?)
        }));
        tokio::time::timeout(WAIT, async {
            while Arc::strong_count(&receiver.inner) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the unobserved receive should release its receiver");
        sender.try_post(&client, result_message()).unwrap();
        let next = in_actor_context(&client, |py| start(py, receiver.recv(py, false)?));
        assert_value(value(&next).await);
    }

    #[tokio::test]
    async fn dropped_spawned_receive_releases_the_receiver() {
        let client = test_instance("dropped_spawned_receive_releases_the_receiver");
        let (_sender, receiver) = client.mailbox_for_py().open_port::<PythonMessage>();
        let receiver = PythonPortReceiver {
            inner: Arc::new(tokio::sync::Mutex::new(receiver)),
        };
        let weak = Arc::downgrade(&receiver.inner);
        drop(in_actor_context(&client, |py| {
            start(py, receiver.recv(py, false)?)
        }));
        drop(receiver);
        tokio::time::timeout(WAIT, async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the native port receiver must not be stranded in a task");
        assert!(weak.upgrade().is_none());
    }

    // Unbound ports disconnect when their sole sender drops. Mailbox::close()
    // records a status but does not disconnect these receive queues.
    #[tokio::test]
    async fn recv_reports_closed_ports() {
        let client = test_instance("recv_reports_closed_ports");
        let (sender, receiver) = client.mailbox_for_py().open_port::<PythonMessage>();
        let regular = PythonPortReceiver {
            inner: Arc::new(tokio::sync::Mutex::new(receiver)),
        };
        let regular = in_actor_context(&client, |py| start(py, regular.recv(py, false)?));
        drop(sender);
        let (sender, receiver) = client.mailbox_for_py().open_once_port::<PythonMessage>();
        let once = PythonOncePortReceiver {
            inner: Arc::new(std::sync::Mutex::new(Some(receiver))),
        };
        let once = in_actor_context(&client, |py| start(py, once.recv(py, false)?));
        drop(sender);
        for handle in [regular, once] {
            let error = tokio::time::timeout(WAIT, handle.wait_future())
                .await
                .expect("a disconnected receive must finish")
                .expect_err("it must fail");
            monarch_with_gil_blocking(GilSite::Test, |py| {
                assert!(error.is_instance_of::<PyEOFError>(py));
                assert!(error.value(py).to_string().starts_with("Port closed: "));
            });
        }
    }

    // A receive observed twice shares one result: the second observation
    // neither takes another message nor fails, so the next message is left for
    // the next receive.
    #[tokio::test]
    async fn second_observation_shares_the_receive() {
        let client = test_instance("second_observation_shares_the_receive");
        let (sender, receiver) = client.mailbox_for_py().open_port::<PythonMessage>();
        let receiver = PythonPortReceiver {
            inner: Arc::new(tokio::sync::Mutex::new(receiver)),
        };
        sender.try_post(&client, result_message()).unwrap();
        sender.try_post(&client, result_message()).unwrap();
        let item = in_actor_context(&client, |py| receiver.recv(py, false));
        let first = value(&item).await;
        let second = value(&item).await;
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(
                first.bind(py).is(second.bind(py)),
                "both observations should read the same object"
            );
        });
        let next = in_actor_context(&client, |py| receiver.recv(py, false));
        assert_value(value(&next).await);
    }

    #[tokio::test]
    async fn recv_maps_values_ranks_and_errors() {
        let client = test_instance("recv_maps_values_ranks_and_errors");
        let (sender, receiver) = client.mailbox_for_py().open_port::<PythonMessage>();
        let receiver = PythonPortReceiver {
            inner: Arc::new(tokio::sync::Mutex::new(receiver)),
        };
        for ranked in [false, true] {
            sender.try_post(&client, result_message()).unwrap();
            let handle = in_actor_context(&client, |py| start(py, receiver.recv(py, ranked)?));
            let received = value(&handle).await;
            monarch_with_gil_blocking(GilSite::Test, |py| {
                if ranked {
                    assert_eq!(received.extract::<(usize, i64)>(py).unwrap(), (7, 42));
                } else {
                    assert_eq!(received.extract::<i64>(py).unwrap(), 42);
                }
            });
        }
        for ranked in [false, true] {
            sender
                .try_post(
                    &client,
                    message(PythonMessageKind::Exception { rank: Some(7) }, |py| {
                        PyValueError::new_err("sent exception")
                            .into_value(py)
                            .into_any()
                    }),
                )
                .unwrap();
            let handle = in_actor_context(&client, |py| start(py, receiver.recv(py, ranked)?));
            let error = tokio::time::timeout(WAIT, handle.wait_future())
                .await
                .unwrap()
                .unwrap_err();
            monarch_with_gil_blocking(GilSite::Test, |py| {
                assert!(error.is_instance_of::<PyValueError>(py));
                assert_eq!(error.value(py).to_string(), "sent exception");
            });
        }
        // Invalid bytes prove rank validation happens before decoding, for
        // both Result and Exception messages with a missing rank.
        for kind in [
            PythonMessageKind::Result { rank: None },
            PythonMessageKind::Exception { rank: None },
        ] {
            let no_rank = PythonMessage {
                kind,
                ..Default::default()
            };
            let expected = monarch_with_gil_blocking(GilSite::Test, |py| {
                format!(
                    "RankedPort receiver got a message without a rank {}",
                    no_rank
                        .clone()
                        .into_py_any(py)
                        .unwrap()
                        .bind(py)
                        .str()
                        .unwrap()
                )
            });
            sender.try_post(&client, no_rank).unwrap();
            let handle = in_actor_context(&client, |py| start(py, receiver.recv(py, true)?));
            let error = tokio::time::timeout(WAIT, handle.wait_future())
                .await
                .unwrap()
                .unwrap_err();
            monarch_with_gil_blocking(GilSite::Test, |py| {
                assert!(error.is_instance_of::<PyValueError>(py));
                assert_eq!(
                    without_object_address(&error.value(py).to_string()),
                    without_object_address(&expected)
                );
            });
        }
        let kind = PythonMessageKind::CallMethod {
            name: MethodSpecifier::ReturnsResponse {
                name: "unexpected".to_string(),
            },
            response_port: None,
            correlation_id: None,
        };
        let expected = monarch_with_gil_blocking(GilSite::Test, |py| {
            format!(
                "Unexpected message kind: {}",
                kind.clone()
                    .into_py_any(py)
                    .unwrap()
                    .bind(py)
                    .str()
                    .unwrap()
            )
        });
        sender
            .try_post(&client, message(kind, |py| 42.into_py_any(py).unwrap()))
            .unwrap();
        let handle = in_actor_context(&client, |py| start(py, receiver.recv(py, false)?));
        let error = tokio::time::timeout(WAIT, handle.wait_future())
            .await
            .unwrap()
            .unwrap_err();
        monarch_with_gil_blocking(GilSite::Test, |py| {
            assert!(error.is_instance_of::<PyValueError>(py));
            assert_eq!(
                without_object_address(&error.value(py).to_string()),
                without_object_address(&expected)
            );
        });
    }

    #[tokio::test]
    async fn recv_captures_creation_traceback_only_when_enabled() {
        let client = test_instance("recv_captures_creation_traceback_only_when_enabled");
        let config = hyperactor_config::global::lock();
        for enabled in [false, true] {
            let _guard = config.override_key(ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK, enabled);
            let (sender, receiver) = client.mailbox_for_py().open_port::<PythonMessage>();
            let receiver = PythonPortReceiver {
                inner: Arc::new(tokio::sync::Mutex::new(receiver)),
            };
            sender.try_post(&client, result_message()).unwrap();
            let handle = in_actor_context(&client, |py| start(py, receiver.recv(py, false)?));
            assert_eq!(handle.has_traceback_for_test(), enabled);
            assert_value(value(&handle).await);

            let (sender, receiver) = client.mailbox_for_py().open_once_port::<PythonMessage>();
            let receiver = PythonOncePortReceiver {
                inner: Arc::new(std::sync::Mutex::new(Some(receiver))),
            };
            sender.try_post(&client, result_message()).unwrap();
            let handle = in_actor_context(&client, |py| start(py, receiver.recv(py, false)?));
            assert_eq!(handle.has_traceback_for_test(), enabled);
            assert_value(value(&handle).await);
        }
    }
}
