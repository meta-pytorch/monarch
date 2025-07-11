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
use std::sync::Arc;

use hyperactor::Mailbox;
use hyperactor::Named;
use hyperactor::OncePortHandle;
use hyperactor::OncePortRef;
use hyperactor::PortHandle;
use hyperactor::PortId;
use hyperactor::PortRef;
use hyperactor::accum::Accumulator;
use hyperactor::accum::CommReducer;
use hyperactor::accum::ReducerFactory;
use hyperactor::accum::ReducerSpec;
use hyperactor::attrs::Attrs;
use hyperactor::data::Serialized;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::OncePortReceiver;
use hyperactor::mailbox::PortReceiver;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::monitored_return_handle;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use hyperactor_mesh::comm::multicast::set_cast_info_on_headers;
use monarch_types::PickledPyObject;
use pyo3::exceptions::PyEOFError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use serde::Deserialize;
use serde::Serialize;

use crate::actor::PythonMessage;
use crate::proc::PyActorId;
use crate::runtime::signal_safe_block_on;
use crate::shape::PyShape;
#[derive(Clone, Debug)]
#[pyclass(
    name = "Mailbox",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub struct PyMailbox {
    pub inner: Mailbox,
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
                inner: std::sync::Mutex::new(Some(receiver)),
            },
        )?;
        PyTuple::new(py, vec![handle.into_any(), receiver.into_any()])
    }

    fn open_accum_port<'py>(
        &self,
        py: Python<'py>,
        accumulator: PyObject,
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

    pub(super) fn post(&self, dest: &PyActorId, message: &PythonMessage) -> PyResult<()> {
        let port_id = dest.inner.port_id(PythonMessage::port());
        let message = Serialized::serialize(message).map_err(|err| {
            PyRuntimeError::new_err(format!(
                "failed to serialize message ({:?}) to Serialized: {}",
                message, err
            ))
        })?;
        let envelope = MessageEnvelope::new(
            self.inner.actor_id().clone(),
            port_id,
            message,
            Attrs::new(),
        );
        let return_handle = self
            .inner
            .bound_return_handle()
            .unwrap_or(monitored_return_handle());
        self.inner.post(envelope, return_handle);
        Ok(())
    }

    pub(super) fn post_cast(
        &self,
        dest: &PyActorId,
        rank: usize,
        shape: &PyShape,
        message: &PythonMessage,
    ) -> PyResult<()> {
        let port_id = dest.inner.port_id(PythonMessage::port());
        let mut headers = Attrs::new();
        set_cast_info_on_headers(
            &mut headers,
            rank,
            shape.inner.clone(),
            self.inner.actor_id().clone(),
        );
        let message = Serialized::serialize(message).map_err(|err| {
            PyRuntimeError::new_err(format!(
                "failed to serialize message ({:?}) to Serialized: {}",
                message, err
            ))
        })?;
        let envelope =
            MessageEnvelope::new(self.inner.actor_id().clone(), port_id, message, headers);
        let return_handle = self
            .inner
            .bound_return_handle()
            .unwrap_or(monitored_return_handle());
        self.inner.post(envelope, return_handle);
        Ok(())
    }

    #[getter]
    fn actor_id(&self) -> PyActorId {
        PyActorId {
            inner: self.inner.actor_id().clone(),
        }
    }

    fn undeliverable_receiver<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Py<PythonUndeliverablePortReceiver>> {
        let (handle, receiver) = self.inner.open_port();
        handle.bind_to(Undeliverable::<MessageEnvelope>::port());
        let receiver = Py::new(
            py,
            PythonUndeliverablePortReceiver {
                inner: Arc::new(tokio::sync::Mutex::new(receiver)),
            },
        )?;

        Ok(receiver)
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
    inner: PortId,
}

impl From<PortId> for PyPortId {
    fn from(port_id: PortId) -> Self {
        Self { inner: port_id }
    }
}

impl From<PyPortId> for PortId {
    fn from(port_id: PyPortId) -> Self {
        port_id.inner
    }
}

#[pymethods]
impl PyPortId {
    #[new]
    #[pyo3(signature = (*, actor_id, port))]
    fn new(actor_id: &PyActorId, port: u64) -> Self {
        Self {
            inner: PortId(actor_id.inner.clone(), port),
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
    fn actor_id(&self) -> PyActorId {
        PyActorId {
            inner: self.inner.actor_id().clone(),
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
pub(super) struct PythonPortHandle {
    inner: PortHandle<PythonMessage>,
}

#[pymethods]
impl PythonPortHandle {
    fn send(&self, message: PythonMessage) -> PyResult<()> {
        self.inner
            .send(message)
            .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;
        Ok(())
    }

    fn bind(&self) -> PythonPortRef {
        PythonPortRef {
            inner: self.inner.bind(),
        }
    }
}

#[derive(Clone, Debug)]
#[pyclass(
    name = "UndeliverablePortHandle",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(super) struct PythonUndeliverablePortHandle {
    inner: PortHandle<Undeliverable<MessageEnvelope>>,
}

#[pymethods]
impl PythonUndeliverablePortHandle {
    fn bind_undeliverable(&self) {
        self.inner.bind_to(Undeliverable::<MessageEnvelope>::port());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[pyclass(
    name = "PortRef",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub struct PythonPortRef {
    pub(crate) inner: PortRef<PythonMessage>,
}

#[pymethods]
impl PythonPortRef {
    fn send(&self, mailbox: &PyMailbox, message: PythonMessage) -> PyResult<()> {
        self.inner
            .send(&mailbox.inner, message)
            .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;
        Ok(())
    }

    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    #[getter]
    fn port_id(&self) -> PyResult<PyPortId> {
        Ok(self.inner.port_id().clone().into())
    }
}

impl From<PortRef<PythonMessage>> for PythonPortRef {
    fn from(port_ref: PortRef<PythonMessage>) -> Self {
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

#[pymethods]
impl PythonPortReceiver {
    fn recv<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            receiver
                .lock()
                .await
                .recv()
                .await
                .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))
        })
    }
    fn blocking_recv<'py>(&mut self, py: Python<'py>) -> PyResult<PythonMessage> {
        let receiver = self.inner.clone();
        signal_safe_block_on(py, async move { receiver.lock().await.recv().await })?
            .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))
    }
}

#[derive(Debug)]
#[pyclass(
    name = "UndeliverableMessageEnvelope",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(crate) struct PythonUndeliverableMessageEnvelope {
    #[allow(dead_code)] // At this time, field `inner` isn't read.
    pub(crate) inner: Undeliverable<MessageEnvelope>,
}

#[derive(Debug)]
#[pyclass(
    name = "UndeliverablePortReceiver",
    module = "monarch._rust_bindings.monarch_hyperactor.mailbox"
)]
pub(super) struct PythonUndeliverablePortReceiver {
    inner: Arc<tokio::sync::Mutex<PortReceiver<Undeliverable<MessageEnvelope>>>>,
}

#[pymethods]
impl PythonUndeliverablePortReceiver {
    fn recv<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let message = receiver
                .lock()
                .await
                .recv()
                .await
                .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;
            Ok(PythonUndeliverableMessageEnvelope { inner: message })
        })
    }

    fn blocking_recv<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<PythonUndeliverableMessageEnvelope> {
        let receiver = self.inner.clone();
        let message = signal_safe_block_on(py, async move { receiver.lock().await.recv().await })?
            .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;

        Ok(PythonUndeliverableMessageEnvelope { inner: message })
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
    fn send(&mut self, message: PythonMessage) -> PyResult<()> {
        let Some(port) = self.inner.take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePort is already used"));
        };

        port.send(message)
            .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;
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
    pub(crate) inner: Option<OncePortRef<PythonMessage>>,
}

#[pymethods]
impl PythonOncePortRef {
    fn send(&mut self, mailbox: &PyMailbox, message: PythonMessage) -> PyResult<()> {
        let Some(port_ref) = self.inner.take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePortRef is already used"));
        };

        port_ref
            .send(&mailbox.inner, message)
            .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))?;
        Ok(())
    }

    fn __repr__(&self) -> String {
        self.inner
            .as_ref()
            .map_or("OncePortRef is already used".to_string(), |r| r.to_string())
    }

    #[getter]
    fn port_id(&self) -> PyResult<PyPortId> {
        Ok(self.inner.as_ref().unwrap().port_id().clone().into())
    }
}

impl From<OncePortRef<PythonMessage>> for PythonOncePortRef {
    fn from(port_ref: OncePortRef<PythonMessage>) -> Self {
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
    inner: std::sync::Mutex<Option<OncePortReceiver<PythonMessage>>>,
}

#[pymethods]
impl PythonOncePortReceiver {
    fn recv<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let Some(receiver) = self.inner.lock().unwrap().take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePort is already used"));
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            receiver
                .recv()
                .await
                .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))
        })
    }
    fn blocking_recv<'py>(&mut self, py: Python<'py>) -> PyResult<PythonMessage> {
        let Some(receiver) = self.inner.lock().unwrap().take() else {
            return Err(PyErr::new::<PyValueError, _>("OncePort is already used"));
        };
        signal_safe_block_on(py, async move { receiver.recv().await })?
            .map_err(|err| PyErr::new::<PyEOFError, _>(format!("Port closed: {}", err)))
    }
}

#[derive(
    Clone,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    FromPyObject,
    IntoPyObject
)]
pub enum EitherPortRef {
    Unbounded(PythonPortRef),
    Once(PythonOncePortRef),
}

impl Unbind for EitherPortRef {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        match self {
            EitherPortRef::Unbounded(port_ref) => port_ref.inner.unbind(bindings),
            EitherPortRef::Once(once_port_ref) => once_port_ref.inner.unbind(bindings),
        }
    }
}

impl Bind for EitherPortRef {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        match self {
            EitherPortRef::Unbounded(port_ref) => port_ref.inner.bind(bindings),
            EitherPortRef::Once(once_port_ref) => once_port_ref.inner.bind(bindings),
        }
    }
}

#[derive(Debug, Named)]
struct PythonReducer(PyObject);

impl PythonReducer {
    fn new(params: Option<Serialized>) -> anyhow::Result<Self> {
        let p = params.ok_or_else(|| anyhow::anyhow!("params cannot be None"))?;
        let obj: PickledPyObject = p.deserialized()?;
        Ok(Python::with_gil(|py: Python<'_>| -> PyResult<Self> {
            let unpickled = obj.unpickle(py)?;
            Ok(Self(unpickled.unbind()))
        })?)
    }
}

impl CommReducer for PythonReducer {
    type Update = PythonMessage;

    fn reduce(&self, left: Self::Update, right: Self::Update) -> anyhow::Result<Self::Update> {
        Python::with_gil(|py: Python<'_>| {
            let result = self.0.call(py, (left, right), None)?;
            Ok(result.extract::<PythonMessage>(py)?)
        })
    }
}

struct PythonAccumulator {
    accumulator: PyObject,
    reducer: Option<Serialized>,
}

impl PythonAccumulator {
    fn new<'py>(py: Python<'py>, accumulator: PyObject) -> PyResult<Self> {
        let py_reducer = accumulator.getattr(py, "reducer")?;
        let reducer: Option<Serialized> = if py_reducer.is_none(py) {
            None
        } else {
            let pickled = PickledPyObject::cloudpickle(py_reducer.bind(py))?;
            Some(
                Serialized::serialize(&pickled)
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
        Python::with_gil(|py: Python<'_>| {
            // Initialize state if it is empty.
            if state.message.is_empty() && state.method.is_empty() {
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

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyMailbox>()?;
    hyperactor_mod.add_class::<PyPortId>()?;
    hyperactor_mod.add_class::<PythonPortHandle>()?;
    hyperactor_mod.add_class::<PythonUndeliverablePortHandle>()?;
    hyperactor_mod.add_class::<PythonPortRef>()?;
    hyperactor_mod.add_class::<PythonPortReceiver>()?;
    hyperactor_mod.add_class::<PythonUndeliverablePortReceiver>()?;
    hyperactor_mod.add_class::<PythonOncePortHandle>()?;
    hyperactor_mod.add_class::<PythonOncePortRef>()?;
    hyperactor_mod.add_class::<PythonOncePortReceiver>()?;

    Ok(())
}
