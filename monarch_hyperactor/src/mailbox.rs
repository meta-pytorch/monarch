/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

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
use crate::proc::PyActorAddr;
use crate::pytokio::PyPythonTask;
use crate::pytokio::PythonTask;
use crate::runtime::GilSite;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;

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

async fn recv_async(
    receiver: Arc<tokio::sync::Mutex<PortReceiver<PythonMessage>>>,
) -> PyResult<Py<PyAny>> {
    let message = receiver
        .lock()
        .await
        .recv()
        .await
        .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;

    monarch_with_gil(GilSite::ReplyConvert, |py| message.into_py_any(py)).await
}

#[pymethods]
impl PythonPortReceiver {
    fn recv_task(&mut self) -> PyResult<PyPythonTask> {
        let receiver = self.inner.clone();
        Ok(PythonTask::new(recv_async(receiver))?.into())
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
    fn recv_task(&mut self) -> PyResult<PyPythonTask> {
        let Some(receiver) = self.inner.lock().unwrap().take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePort is already used"));
        };
        let fut = async move {
            let message = receiver
                .recv()
                .await
                .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;

            monarch_with_gil(GilSite::ReplyConvert, |py| message.into_py_any(py)).await
        };
        Ok(PythonTask::new(fut)?.into())
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
                result.extract::<PythonMessage>(py)
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
