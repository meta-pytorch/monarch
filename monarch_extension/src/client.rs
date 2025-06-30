/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::Arc;

use hyperactor::ActorRef;
use hyperactor::WorldId;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::data::Serialized;
use hyperactor_multiprocess::system_actor::SYSTEM_ACTOR_REF;
use hyperactor_multiprocess::system_actor::SystemMessageClient;
use hyperactor_multiprocess::system_actor::SystemSnapshotFilter;
use hyperactor_multiprocess::system_actor::WorldSnapshot;
use hyperactor_multiprocess::system_actor::WorldSnapshotProcInfo;
use monarch_hyperactor::proc::ControllerError;
use monarch_hyperactor::proc::InstanceWrapper;
use monarch_hyperactor::proc::PyActorId;
use monarch_hyperactor::proc::PyProc;
use monarch_hyperactor::proc::PySerialized;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_messages::client::ClientMessage;
use monarch_messages::client::Exception;
use monarch_messages::client::LogLevel;
use monarch_messages::controller::ControllerActor;
use monarch_messages::controller::ControllerMessage;
use monarch_messages::controller::ControllerMessageClient;
use monarch_messages::controller::DeviceFailure;
use monarch_messages::controller::Ranks;
use monarch_messages::controller::Seq;
use monarch_messages::controller::WorkerError;
use monarch_messages::debugger::DebuggerAction;
use monarch_messages::worker::Ref;
use monarch_types::PyTree;
use monarch_types::TryIntoPyObjectUnsafe;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::types::PyNone;
use tokio::sync::Mutex;
use torch_sys::RValue;

use crate::controller::PyRanks;
use crate::convert::convert;

#[pyclass(frozen, module = "monarch._rust_bindings.monarch_extension.client")]
pub struct WorkerResponse {
    seq: Seq,
    result: Option<Result<Serialized, Exception>>,
}

impl WorkerResponse {
    pub fn new(seq: Seq, result: Option<Result<Serialized, Exception>>) -> Self {
        Self { seq, result }
    }
}

#[pymethods]
impl WorkerResponse {
    #[staticmethod]
    fn new_for_unit_test(py: Python<'_>, seq: u64, response: PyObject) -> PyResult<Self> {
        if let Ok(exc) = response.downcast_bound::<PyException>(py) {
            Ok(Self {
                seq: seq.into(),
                result: Some(Err(exc.borrow().inner.clone())),
            })
        } else {
            Ok(Self {
                seq: seq.into(),
                result: Some(Ok(Serialized::serialize_anon(
                    &response.extract::<PyTree<RValue>>(py)?,
                )
                .map_err(|err| {
                    PyRuntimeError::new_err(format!("Failed to deserialize: {:?}", err))
                })?)),
            })
        }
    }

    // For now lets treat Seq as just an int with an opaque alias on python side.
    // We can expose the rust version later if desired.
    #[getter]
    fn seq(&self) -> u64 {
        self.seq.into()
    }

    // TODO: result() cannot yet be called within a device mesh.
    // Fake tensors, which are not on the intended devices, will cause the deserialization to fail.
    fn result(&self, py: Python<'_>) -> PyResult<PyObject> {
        if let Some(result) = &self.result {
            if result.is_err() {
                PyNone::get(py).into_py_any(py)
            } else {
                // TODO: Use better shared error class
                let rvalue = result
                    .clone()
                    .unwrap()
                    .deserialized::<PyTree<RValue>>()
                    .map_err(|err| {
                        PyRuntimeError::new_err(format!("Failed to deserialize: {:?}", err))
                    })?;
                // SAFETY: Safety requirements are propagated via the `unsafe` tag
                // on this method.
                Ok(unsafe { rvalue.try_to_object_unsafe(py)?.unbind() })
            }
        } else {
            PyNone::get(py).into_py_any(py)
        }
    }

    fn exception(&self, py: Python<'_>) -> PyResult<PyObject> {
        match self.result.as_ref() {
            Some(Ok(_)) => PyNone::get(py).into_py_any(py),
            Some(Err(exc)) => Ok(PyException::exception_to_py(py, exc)?),
            None => PyNone::get(py).into_py_any(py),
        }
    }

    fn is_exception(&self) -> bool {
        match self.result {
            Some(Err(_)) => true,
            _ => false,
        }
    }
}

#[pyclass(
    frozen,
    name = "WorldState",
    module = "monarch._rust_bindings.monarch_extension.client"
)]
pub struct PyWorldState {
    inner: WorldSnapshot,
}

#[pymethods]
impl PyWorldState {
    #[getter]
    fn labels(self_: PyRef<Self>, py: Python) -> PyResult<PyObject> {
        self_.inner.labels.clone().into_py_dict(py)?.into_py_any(py)
    }

    #[getter]
    fn procs(self_: PyRef<Self>, py: Python) -> PyResult<PyObject> {
        let proc_dict = PyDict::new(py);
        for (proc_id, proc_info) in self_.inner.procs.clone() {
            proc_dict.set_item(
                proc_id.to_string(),
                PyProcInfo::from(proc_info).into_pyobject(py)?,
            )?;
        }
        Ok(proc_dict.into())
    }
}

#[derive(Default, Clone)]
#[pyclass(
    frozen,
    name = "SystemSnapshotFilter",
    module = "monarch._rust_bindings.monarch_extension.client"
)]
pub struct PySystemSnapshotFilter {
    inner: SystemSnapshotFilter,
}

#[pymethods]
impl PySystemSnapshotFilter {
    #[new]
    #[pyo3(signature = (worlds = None, world_labels = None, proc_labels = None))]
    fn new(
        worlds: Option<Vec<String>>,
        world_labels: Option<HashMap<String, String>>,
        proc_labels: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            inner: SystemSnapshotFilter {
                worlds: worlds
                    .unwrap_or_default()
                    .iter()
                    .map(|name| WorldId(name.clone()))
                    .collect(),
                world_labels: world_labels.unwrap_or_default(),
                proc_labels: proc_labels.unwrap_or_default(),
            },
        }
    }

    #[getter]
    fn worlds(self_: PyRef<Self>, py: Python) -> PyResult<PyObject> {
        self_
            .inner
            .worlds
            .iter()
            .map(|world_id| world_id.name())
            .collect::<Vec<_>>()
            .into_py_any(py)
    }

    #[getter]
    fn world_labels(self_: PyRef<Self>, py: Python) -> PyResult<PyObject> {
        self_
            .inner
            .world_labels
            .clone()
            .into_py_dict(py)?
            .into_py_any(py)
    }

    #[getter]
    fn proc_labels(self_: PyRef<Self>, py: Python) -> PyResult<PyObject> {
        self_
            .inner
            .proc_labels
            .clone()
            .into_py_dict(py)?
            .into_py_any(py)
    }
}

impl From<&PySystemSnapshotFilter> for SystemSnapshotFilter {
    fn from(filter: &PySystemSnapshotFilter) -> Self {
        Self {
            worlds: filter.inner.worlds.clone(),
            world_labels: filter.inner.world_labels.clone(),
            proc_labels: filter.inner.proc_labels.clone(),
        }
    }
}

impl From<PySystemSnapshotFilter> for SystemSnapshotFilter {
    fn from(filter: PySystemSnapshotFilter) -> Self {
        Self {
            worlds: filter.inner.worlds.clone(),
            world_labels: filter.inner.world_labels.clone(),
            proc_labels: filter.inner.proc_labels.clone(),
        }
    }
}

#[pyclass(
    frozen,
    name = "ProcInfo",
    module = "monarch._rust_bindings.monarch_extension.client"
)]
pub struct PyProcInfo {
    inner: WorldSnapshotProcInfo,
}

impl From<WorldSnapshotProcInfo> for PyProcInfo {
    fn from(info: WorldSnapshotProcInfo) -> Self {
        Self { inner: info }
    }
}

#[pymethods]
impl PyProcInfo {
    #[getter]
    fn labels(self_: PyRef<Self>, py: Python) -> PyResult<PyObject> {
        Ok(self_.inner.labels.clone().into_py_dict(py)?.into())
    }
}

#[pyclass(
    frozen,
    subclass,
    name = "Exception",
    module = "monarch._rust_bindings.monarch_extension.client"
)]
pub struct PyException {
    inner: Exception,
}

impl PyException {
    pub(crate) fn exception_to_py(py: Python<'_>, exc: &Exception) -> PyResult<PyObject> {
        let initializer = PyClassInitializer::from(PyException { inner: exc.clone() });
        match exc {
            Exception::Error(_, _, _) => {
                Py::new(py, initializer.add_subclass(PyError))?.into_py_any(py)
            }
            Exception::Failure(_) => {
                Py::new(py, initializer.add_subclass(PyFailure))?.into_py_any(py)
            }
        }
    }
}

#[pyclass(frozen, extends = PyException, subclass, name = "Error", module = "monarch._rust_bindings.monarch_extension.client")]
pub struct PyError;

#[pymethods]
impl PyError {
    #[new]
    #[pyo3(signature = (*, seq, caused_by_seq, backtrace, actor_id))]
    fn new(
        seq: Seq,
        caused_by_seq: Seq,
        backtrace: String,
        actor_id: &PyActorId,
    ) -> PyResult<(Self, PyException)> {
        Ok((
            Self,
            PyException {
                inner: Exception::Error(
                    seq,
                    caused_by_seq,
                    WorkerError {
                        backtrace,
                        worker_actor_id: actor_id.into(),
                    },
                ),
            },
        ))
    }

    #[staticmethod]
    fn new_for_unit_test(
        py: Python<'_>,
        seq: Seq,
        caused_by_seq: Seq,
        actor_id: &PyActorId,
        backtrace: String,
    ) -> PyResult<PyObject> {
        let initializer = PyClassInitializer::from(PyException {
            inner: Exception::Error(
                seq,
                caused_by_seq,
                WorkerError {
                    worker_actor_id: actor_id.into(),
                    backtrace,
                },
            ),
        })
        .add_subclass(Self);
        Py::new(py, initializer)?.into_py_any(py)
    }

    #[getter]
    fn seq(self_: PyRef<Self>) -> u64 {
        self_.as_ref().inner.as_error().unwrap().0.into()
    }

    #[getter]
    fn caused_by_seq(self_: PyRef<Self>) -> u64 {
        self_.as_ref().inner.as_error().unwrap().1.into()
    }

    #[getter]
    fn actor_id(self_: PyRef<Self>) -> PyActorId {
        self_
            .as_ref()
            .inner
            .as_error()
            .unwrap()
            .2
            .worker_actor_id
            .clone()
            .into()
    }

    #[getter]
    fn backtrace(self_: PyRef<Self>) -> String {
        self_.as_ref().inner.as_error().unwrap().2.backtrace.clone()
    }
}

#[derive(Clone, PartialEq)]
#[pyclass(
    frozen,
    eq,
    eq_int,
    name = "LogLevel",
    module = "monarch._rust_bindings.monarch_extension.client"
)]
enum PyLogLevel {
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for PyLogLevel {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Info => Self::Info,
            LogLevel::Warn => Self::Warn,
            LogLevel::Error => Self::Error,
        }
    }
}

#[pymethods]
impl PyLogLevel {
    #[classattr]
    const WARNING: PyLogLevel = PyLogLevel::Warn;

    #[classattr]
    const ERROR: PyLogLevel = PyLogLevel::Error;

    #[classattr]
    const INFO: PyLogLevel = PyLogLevel::Info;
}

#[pyclass(
    frozen,
    name = "LogMessage",
    module = "monarch._rust_bindings.monarch_extension.client"
)]
pub struct LogMessage {
    level: PyLogLevel,
    message: String,
}

#[pymethods]
impl LogMessage {
    #[new]
    #[pyo3(signature = (*,  level, message))]
    fn new(level: PyLogLevel, message: String) -> PyResult<Self> {
        Ok(Self { level, message })
    }

    #[staticmethod]
    fn new_for_unit_test(_py: Python<'_>, level: PyLogLevel, message: String) -> PyResult<Self> {
        Ok(Self { level, message })
    }

    #[getter]
    fn message(self_: PyRef<Self>) -> String {
        self_.message.clone()
    }

    #[getter]
    fn level(self_: PyRef<Self>) -> PyLogLevel {
        self_.level.clone()
    }
}

#[pyclass(frozen, extends = PyException, subclass, name = "Failure", module = "monarch._rust_bindings.monarch_extension.client")]
pub struct PyFailure;

#[pymethods]
impl PyFailure {
    #[new]
    #[pyo3(signature = (*, backtrace, address, actor_id))]
    fn new(
        backtrace: String,
        address: String,
        actor_id: &PyActorId,
    ) -> PyResult<(Self, PyException)> {
        Ok((
            Self,
            PyException {
                inner: Exception::Failure(DeviceFailure {
                    actor_id: actor_id.into(),
                    address,
                    backtrace,
                }),
            },
        ))
    }

    #[staticmethod]
    fn new_for_unit_test(
        py: Python<'_>,
        actor_id: &PyActorId,
        backtrace: String,
    ) -> PyResult<PyObject> {
        let initializer = PyClassInitializer::from(PyException {
            inner: Exception::Failure(DeviceFailure {
                actor_id: actor_id.into(),
                address: "".to_string(),
                backtrace,
            }),
        })
        .add_subclass(Self);
        Py::new(py, initializer)?.into_py_any(py)
    }

    #[getter]
    fn actor_id(self_: PyRef<Self>) -> PyActorId {
        self_
            .as_ref()
            .inner
            .as_failure()
            .unwrap()
            .actor_id
            .clone()
            .into()
    }

    #[getter]
    fn address(self_: PyRef<Self>) -> String {
        self_.as_ref().inner.as_failure().unwrap().address.clone()
    }

    #[getter]
    fn backtrace(self_: PyRef<Self>) -> String {
        self_.as_ref().inner.as_failure().unwrap().backtrace.clone()
    }
}

#[pyclass(
    frozen,
    get_all,
    name = "DebuggerMessage",
    module = "monarch._rust_bindings.monarch_extension.client"
)]
pub struct DebuggerMessage {
    debugger_actor_id: PyActorId,
    action: DebuggerAction,
}

#[pymethods]
impl DebuggerMessage {
    #[new]
    #[pyo3(signature = (*, debugger_actor_id, action))]
    pub fn new(debugger_actor_id: PyActorId, action: DebuggerAction) -> PyResult<Self> {
        Ok(Self {
            debugger_actor_id,
            action,
        })
    }
}

#[pyclass(module = "monarch._rust_bindings.monarch_extension.client")]
pub struct ClientActor {
    instance: Arc<Mutex<InstanceWrapper<ClientMessage>>>,
}

impl ClientActor {
    // Send a message to stop the controller and workers in a mesh.
    fn stop_worlds_impl(&mut self, py: Python, world_names: Option<Vec<String>>) -> PyResult<()> {
        let system_actor_ref = &*SYSTEM_ACTOR_REF;
        let mailbox = self.instance.blocking_lock().mailbox().clone();
        let (tx, rx) = mailbox.open_once_port::<()>();
        let timeout = tokio::time::Duration::from_secs(4);
        let worlds_ids = world_names.map(|w| w.into_iter().map(WorldId).collect());
        signal_safe_block_on(py, async move {
            system_actor_ref
                .stop(&mailbox, worlds_ids, timeout, tx.bind())
                .await?;
            let timeout = tokio::time::Duration::from_secs(10);
            match RealClock.timeout(timeout, rx.recv()).await {
                Ok(result) => result.map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
                Err(_) => {
                    tracing::info!(
                        "timed out after {}ms waiting on the worlds to stop",
                        timeout.as_millis()
                    );
                }
            }
            Ok(())
        })?
    }
}

#[pymethods]
impl ClientActor {
    #[new]
    fn new(proc: &PyProc, actor_name: &str) -> PyResult<Self> {
        Ok(Self {
            instance: Arc::new(Mutex::new(InstanceWrapper::new(proc, actor_name)?)),
        })
    }

    #[staticmethod]
    fn new_with_parent(proc: &PyProc, parent: &PyActorId) -> PyResult<Self> {
        Ok(Self {
            instance: Arc::new(Mutex::new(InstanceWrapper::new_with_parent(
                proc,
                &parent.into(),
            )?)),
        })
    }

    /// Send a message to any actor that can receive the corresponding serialized
    /// message.
    fn send(&self, actor_id: &PyActorId, message: &PySerialized) -> PyResult<()> {
        self.instance.blocking_lock().send(actor_id, message)
    }

    fn send_obj(
        &self,
        controller: &PyActorId,
        ranks: PyRanks,
        message: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let ranks = match ranks {
            PyRanks::Slice(r) => Ranks::Slice(r.into()),
            PyRanks::SliceList(r) => {
                if r.is_empty() {
                    return Err(PyValueError::new_err("Send requires at least one rank"));
                }
                Ranks::SliceList(r.into_iter().map(|r| r.into()).collect())
            }
        };

        let message = convert(message)?;
        let message = Serialized::serialize(&message).map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to serialize message: {err}"))
        })?;
        let message = ControllerMessage::Send { ranks, message };
        let message = PySerialized::new(&message).map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to serialize message: {err}"))
        })?;
        self.instance.blocking_lock().send(controller, &message)
    }

    /// Attach the client to a controller actor. This will block until the controller responds.
    fn attach(&mut self, py: Python, controller_id: PyActorId) -> PyResult<()> {
        let mut instance = self.instance.blocking_lock();
        instance.set_controller((&controller_id).into());
        let mailbox = instance.mailbox().clone();
        let actor_id = instance.actor_id().clone();

        signal_safe_block_on(py, async move {
            ActorRef::<ControllerActor>::attest((&controller_id).into())
                .attach(&mailbox, ActorRef::attest(actor_id))
                .await
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })?
    }

    fn drop_refs(&self, py: Python, controller_id: PyActorId, refs: Vec<Ref>) -> PyResult<()> {
        let instance = self.instance.blocking_lock();
        let mailbox = instance.mailbox().clone();
        signal_safe_block_on(py, async move {
            ActorRef::<ControllerActor>::attest((&controller_id).into())
                .drop_refs(&mailbox, refs)
                .await
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })?
    }

    /// Get the next message from the queue. It will block until a message is received
    /// or the timeout is reached in which case it will return None
    /// If the actor has been stopped, this returns an error.
    #[pyo3(signature = (*, timeout_msec = None))]
    fn get_next_message<'py>(
        &mut self,
        py: Python<'py>,
        timeout_msec: Option<u64>,
    ) -> PyResult<PyObject> {
        let instance = self.instance.clone();
        let result = signal_safe_block_on(py, async move {
            instance.lock().await.next_message(timeout_msec).await
        })?;

        Python::with_gil(|py| {
            match result {
                Ok(Some(ClientMessage::Result { seq, result })) => {
                    WorkerResponse { seq, result }.into_py_any(py)
                }
                Ok(Some(ClientMessage::Log { level, message })) => LogMessage {
                    level: PyLogLevel::from(level),
                    message,
                }
                .into_py_any(py),
                Ok(Some(ClientMessage::DebuggerMessage {
                    debugger_actor_id,
                    action,
                })) => DebuggerMessage {
                    debugger_actor_id: debugger_actor_id.into(),
                    action,
                }
                .into_py_any(py),
                Ok(None) => PyNone::get(py).into_py_any(py),
                Err(err) => {
                    if let Some(ControllerError::Failed(controller_id, err_msg)) =
                        err.downcast_ref::<ControllerError>()
                    {
                        let failure = DeviceFailure {
                            actor_id: controller_id.clone(),
                            address: "".to_string(), // Controller is always task 0 for now.
                            backtrace: err_msg.clone(),
                        };
                        WorkerResponse {
                            seq: Seq::default(),
                            result: Some(Err(Exception::Failure(failure))),
                        }
                        .into_py_any(py)
                    } else {
                        Err(PyRuntimeError::new_err(err.to_string()))
                    }
                }
            }
        })
    }

    // Send a message to stop the controller and workers in a mesh.
    fn stop(&mut self, py: Python) -> PyResult<()> {
        self.stop_worlds_impl(py, None)
    }

    // Send a message to stop the controller and workers in a mesh.
    fn stop_worlds(&mut self, py: Python, world_names: Vec<String>) -> PyResult<()> {
        self.stop_worlds_impl(py, Some(world_names))
    }

    /// Put `self` into the `Stopped` state and maybe send the system
    /// actor a stop message. Return any outstanding received messages.
    fn drain_and_stop<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut instance = self.instance.blocking_lock();
        let messages = instance
            .drain_and_stop()
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
            .into_iter()
            .map(|message| match message {
                ClientMessage::Result { seq, result } => {
                    WorkerResponse { seq, result }.into_py_any(py)
                }
                ClientMessage::Log { level, message } => LogMessage {
                    level: PyLogLevel::from(level),
                    message,
                }
                .into_py_any(py),
                ClientMessage::DebuggerMessage {
                    debugger_actor_id,
                    action,
                } => DebuggerMessage {
                    debugger_actor_id: debugger_actor_id.into(),
                    action,
                }
                .into_py_any(py),
            })
            .collect::<PyResult<Vec<_>>>()?;
        PyList::new(py, messages)
    }

    /// Get the status of all the worlds from the system.
    #[pyo3(signature = (filter = None))]
    fn world_status<'py>(
        &mut self,
        py: Python<'py>,
        filter: Option<&PySystemSnapshotFilter>,
    ) -> PyResult<PyObject> {
        let instance = self.instance.clone();
        let filter = filter.cloned();
        let worlds = signal_safe_block_on(py, async move {
            instance
                .lock()
                .await
                .world_status(
                    filter.map_or(SystemSnapshotFilter::all(), SystemSnapshotFilter::from),
                )
                .await
        })??;
        Python::with_gil(|py| {
            let py_dict = PyDict::new(py);
            for (world, status) in worlds {
                py_dict.set_item(world.to_string(), status.to_string())?;
            }
            Ok(py_dict.into())
        })
    }

    /// Get a list of procs know to this system instance.
    /// world_filter contains a list of world names to filter on. Empty list means match all.
    /// label_filter contains list of actor labels to filter on. Empty list means match all.
    #[pyo3(signature = (filter = None))]
    fn world_state<'py>(
        &mut self,
        py: Python<'py>,
        filter: Option<&PySystemSnapshotFilter>,
    ) -> PyResult<PyObject> {
        let instance = self.instance.blocking_lock();
        let mailbox = instance.mailbox().clone();
        // TODO: we are cloning this so that we can pass it into the async
        // world. Figure out a better way without incurring a copy.
        let filter = filter.cloned();

        let snapshot = signal_safe_block_on(py, async move {
            SYSTEM_ACTOR_REF
                .snapshot(
                    &mailbox,
                    filter.map_or(SystemSnapshotFilter::all(), SystemSnapshotFilter::from),
                )
                .await
        })??;

        // Convert the snapshot to a Python dictionary
        let result: PyResult<PyObject> = Python::with_gil(|py| {
            let worlds_dict = PyDict::new(py);
            for (world, status) in snapshot.worlds {
                worlds_dict.set_item(
                    world.to_string(),
                    Py::new(py, PyWorldState { inner: status })?.into_pyobject(py)?,
                )?;
            }
            Ok(worlds_dict.into())
        });

        result
    }

    #[getter]
    fn actor_id(&self) -> PyResult<PyActorId> {
        let instance = self.instance.blocking_lock();
        Ok(PyActorId::from(instance.actor_id().clone()))
    }
}

pub(crate) fn register_python_bindings(client_msgs_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    client_msgs_mod.add_class::<WorkerResponse>()?;
    client_msgs_mod.add_class::<PyException>()?;
    client_msgs_mod.add_class::<PyError>()?;
    client_msgs_mod.add_class::<PyFailure>()?;
    client_msgs_mod.add_class::<ClientActor>()?;
    client_msgs_mod.add_class::<PyWorldState>()?;
    client_msgs_mod.add_class::<PySystemSnapshotFilter>()?;
    client_msgs_mod.add_class::<LogMessage>()?;
    client_msgs_mod.add_class::<PyLogLevel>()?;
    client_msgs_mod.add_class::<DebuggerMessage>()?;
    Ok(())
}
