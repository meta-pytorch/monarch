/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// This has been copied over from fbcode/monarch/hyperactor_python/src/actor.rs.
/// We are effectively vendoring he majority of that crate here by copying this over
/// and it can be updated and synced manually.
/// The main motivations for this are:
///     1. We want to avoid needed to figure out dependencies management between hyperactor_python
///        wheel and the rest of the codebase especially while things are in flux. Plus we are also
///        building everything in hyperactor_python into this wheel already (i.e. hyperactor deps).
///     2. In order to support autoreload in bento, potentially pickling in the future etc we need to
///        have a well defined module for these deps which needs to be monarch._rust_bindings.monarch_hyperactor.proc and
///        and making that the module of the classes in hyperactor python is weird.
use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Result;
use hyperactor::ActorRef;
use hyperactor::RemoteMessage;
use hyperactor::actor::Signal;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::ClockKind;
use hyperactor::data::Serialized;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::PortHandle;
use hyperactor::mailbox::PortReceiver;
use hyperactor::proc::Instance;
use hyperactor::proc::Proc;
use hyperactor::reference::ActorId;
use hyperactor::reference::Index;
use hyperactor::reference::ProcId;
use hyperactor::reference::WorldId;
use monarch_types::PickledPyObject;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::types::PyType;
use tokio::sync::OnceCell;
use tokio::sync::watch;

use crate::actor::PythonActor;
use crate::actor::PythonActorHandle;
use crate::mailbox::PyMailbox;
use crate::runtime::get_tokio_runtime;
use crate::runtime::signal_safe_block_on;

/// Wrapper around a proc that provides utilities to implement a python actor.
#[derive(Clone, Debug)]
#[pyclass(
    name = "Proc",
    module = "monarch._rust_bindings.monarch_hyperactor.proc"
)]
pub struct PyProc {
    pub(super) inner: Proc,
}

#[pymethods]
impl PyProc {
    #[new]
    #[pyo3(signature = ())]
    fn new() -> PyResult<Self> {
        Ok(Self {
            inner: Proc::local(),
        })
    }

    #[getter]
    fn world_name(&self) -> String {
        self.inner
            .proc_id()
            .world_name()
            .expect("proc must be ranked for world name")
            .to_string()
    }

    #[getter]
    fn rank(&self) -> usize {
        self.inner
            .proc_id()
            .rank()
            .expect("proc must be ranked for rank access")
    }

    #[getter]
    fn id(&self) -> String {
        self.inner.proc_id().to_string()
    }

    fn attach(&self, name: String) -> PyResult<PyMailbox> {
        let mailbox = self.inner.attach(&name)?;
        Ok(PyMailbox { inner: mailbox })
    }

    fn destroy<'py>(
        &mut self,
        timeout_in_secs: u64,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut inner = self.inner.clone();
        let (_stopped, aborted) = signal_safe_block_on(py, async move {
            inner
                .destroy_and_wait::<()>(Duration::from_secs(timeout_in_secs), None)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })??;
        let aborted_actors = aborted
            .into_iter()
            .map(|actor_id| format!("{}", actor_id))
            .collect::<Vec<_>>();
        // TODO: i don't think returning this list is of much use for
        // anything?
        PyList::new(py, aborted_actors)
    }

    #[pyo3(signature = (actor, name=None))]
    fn spawn<'py>(
        &self,
        py: Python<'py>,
        actor: &Bound<'py, PyType>,
        name: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let proc = self.inner.clone();
        let pickled_type = PickledPyObject::pickle(actor.as_any())?;
        crate::runtime::future_into_py(py, async move {
            Ok(PythonActorHandle {
                inner: proc.spawn(
                    name.as_deref().unwrap_or("anon"),
                    PythonActor::new(pickled_type)?,
                )?,
            })
        })
    }

    #[pyo3(signature = (actor, name=None))]
    fn spawn_blocking<'py>(
        &self,
        py: Python<'py>,
        actor: &Bound<'py, PyType>,
        name: Option<String>,
    ) -> PyResult<PythonActorHandle> {
        let proc = self.inner.clone();
        let pickled_type = PickledPyObject::pickle(actor.as_any())?;
        Ok(PythonActorHandle {
            inner: signal_safe_block_on(py, async move {
                proc.spawn(
                    name.as_deref().unwrap_or("anon"),
                    PythonActor::new(pickled_type)?,
                )
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))??,
        })
    }
}

impl PyProc {
    pub fn new_from_proc(proc: Proc) -> Self {
        Self { inner: proc }
    }
}

#[pyfunction]
#[pyo3(signature = (*, proc_id, bootstrap_addr, timeout = 5, supervision_update_interval = 0, listen_addr = None))]
pub fn init_proc(
    py: Python<'_>,
    proc_id: &str,
    bootstrap_addr: &str,
    #[allow(unused_variables)] // pyo3 will complain if we name this _timeout
    timeout: u64,
    supervision_update_interval: u64,
    listen_addr: Option<String>,
) -> PyResult<PyProc> {
    unimplemented!()
}

#[pyclass(
    frozen,
    name = "ActorId",
    module = "monarch._rust_bindings.monarch_hyperactor.proc"
)]
#[derive(Clone)]
pub struct PyActorId {
    pub(super) inner: ActorId,
}

impl From<ActorId> for PyActorId {
    fn from(actor_id: ActorId) -> Self {
        Self { inner: actor_id }
    }
}

impl From<PyActorId> for ActorId {
    fn from(val: PyActorId) -> Self {
        val.inner
    }
}

#[pymethods]
impl PyActorId {
    #[new]
    #[pyo3(signature = (*, world_name, rank, actor_name, pid = 0))]
    fn new(world_name: &str, rank: Index, actor_name: &str, pid: Index) -> Self {
        Self {
            inner: ActorId(
                ProcId::Ranked(WorldId(world_name.to_string()), rank),
                actor_name.to_string(),
                pid,
            ),
        }
    }

    #[staticmethod]
    fn from_string(actor_id: &str) -> PyResult<Self> {
        Ok(Self {
            inner: actor_id.parse().map_err(|e| {
                PyValueError::new_err(format!(
                    "Failed to extract actor id from {}: {}",
                    actor_id, e
                ))
            })?,
        })
    }

    #[getter]
    fn world_name(&self) -> String {
        self.inner.world_name().to_string()
    }

    #[getter]
    fn rank(&self) -> Index {
        self.inner.rank()
    }

    #[getter]
    fn actor_name(&self) -> String {
        self.inner.name().to_string()
    }

    #[getter]
    fn pid(&self) -> Index {
        self.inner.pid()
    }

    #[getter]
    fn proc_id(&self) -> String {
        self.inner.proc_id().to_string()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.inner.to_string().hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(other) = other.extract::<PyActorId>() {
            Ok(self.inner == other.inner)
        } else {
            Ok(false)
        }
    }

    fn __reduce__<'py>(slf: &Bound<'py, Self>) -> PyResult<(Bound<'py, PyAny>, (String,))> {
        Ok((slf.getattr("from_string")?, (slf.borrow().__str__(),)))
    }
}

impl From<&PyActorId> for ActorId {
    fn from(actor_id: &PyActorId) -> Self {
        actor_id.inner.clone()
    }
}

impl std::fmt::Debug for PyActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InstanceStatus {
    Running,
    Stopped,
}

/// Wrapper around a [`Serialized`] that allows returning it to python and
/// passed to python based detached actors to send to other actors.
#[pyclass(
    frozen,
    name = "Serialized",
    module = "monarch._rust_bindings.monarch_hyperactor.proc"
)]
#[derive(Debug)]
pub struct PySerialized {
    inner: Serialized,
    /// The message port (type) of the message.
    port: u64,
}

impl PySerialized {
    pub fn new<M: RemoteMessage>(message: &M) -> PyResult<Self> {
        Ok(Self {
            inner: Serialized::serialize(message).map_err(|err| {
                PyRuntimeError::new_err(format!(
                    "failed to serialize message ({:?}) to Serialized: {}",
                    message, err
                ))
            })?,
            port: M::port(),
        })
    }

    pub fn deserialized<M: RemoteMessage>(&self) -> PyResult<M> {
        self.inner.deserialized().map_err(|err| {
            PyRuntimeError::new_err(format!("failed to deserialize message: {}", err))
        })
    }

    /// The message port (type) of the message.
    pub fn port(&self) -> u64 {
        self.port
    }
}

/// Wrapper around an instance of an actor that provides utilities to implement
/// a python actor. This helps by allowing users to specialize the actor to the
/// message type they want to handle. [`PickledMessageClientActor``] is a specialization of this
/// for handling messages that are serialized to bytes using pickle.
pub struct InstanceWrapper<M: RemoteMessage> {
    instance: Instance<()>,
    message_receiver: PortReceiver<M>,
    signal_receiver: PortReceiver<Signal>,
    status: InstanceStatus,

    // TODO(T216450632): merge actor.rs and client.rs in monarch_extension
    signal_port: PortHandle<Signal>,
    last_controller_status_check: SystemTime,
    controller_id: OnceCell<ActorId>,
    controller_error_sender: watch::Sender<String>,
    controller_error_receiver: watch::Receiver<String>,
    clock: ClockKind,
    actor_id: ActorId,
}

/// Error that can occur when there is controller supervision error.
#[derive(thiserror::Error, Debug)]
pub enum ControllerError {
    #[error("controller actor {0} failed: {1}")]
    Failed(ActorId, String),
}

impl<M: RemoteMessage> InstanceWrapper<M> {
    pub fn new(proc: &PyProc, actor_name: &str) -> Result<Self> {
        InstanceWrapper::new_with_instance_and_clock(
            proc.inner.instance(actor_name)?.0,
            proc.inner.clock().clone(),
        )
    }

    fn new_with_instance_and_clock(instance: Instance<()>, clock: ClockKind) -> Result<Self> {
        // TEMPORARY: remove after using fixed message ports.
        let (_message_port, message_receiver) = instance.bind_actor_port::<M>();

        let (signal_port, signal_receiver) = instance.bind_actor_port::<Signal>();

        let (controller_error_sender, controller_error_receiver) = watch::channel("".to_string());
        let actor_id = instance.self_id().clone();

        Ok(Self {
            instance,
            message_receiver,
            signal_receiver,
            status: InstanceStatus::Running,
            signal_port,
            last_controller_status_check: clock.system_time_now(),
            controller_id: OnceCell::new(),
            controller_error_sender,
            controller_error_receiver,
            clock,
            actor_id,
        })
    }

    pub fn set_controller(&mut self, controller_id: ActorId) {
        self.controller_id.set(controller_id).unwrap();
    }

    /// Send a message to any actor. It is the responsibility of the caller to ensure the right
    /// payload accepted by the target actor has been serialized and provided to this function.
    pub fn send(&self, actor_id: &PyActorId, message: &PySerialized) -> PyResult<()> {
        hyperactor::tracing::debug!(
            name = "py_send_message",
            actor_id = hyperactor::tracing::field::display(self.actor_id()),
            receiver_actor_id = tracing::field::display(&actor_id.inner),
            ?message,
        );
        actor_id
            .inner
            .port_id(message.port())
            .send(&self.instance, message.inner.clone());
        Ok(())
    }

    /// Make sure the actor is running in detached mode and is alive.
    fn ensure_detached_and_alive(&mut self) -> Result<()> {
        anyhow::ensure!(
            self.status == InstanceStatus::Running,
            "actor is not running"
        );

        // This is a little weird as we are potentially stopping before responding to messages
        // but in reality if we receive stop signal and not stop and drain in most cases its
        // probably ok to stop early.
        // Also an implicit assumption here is that is the signal is stop and drain we allow things
        // to continue as there will hopefully not be new messages coming in. But need a proper draining
        // flow for this.
        // TODO: T208289078
        let signals = self.signal_receiver.drain();
        if signals.into_iter().any(|sig| matches!(sig, Signal::Stop)) {
            self.status = InstanceStatus::Stopped;
            anyhow::bail!("actor has been stopped");
        }

        if let Some(controller_id) = self.controller_id.get() {
            // Check if there is any pending controller error.
            match self.controller_error_receiver.has_changed() {
                Ok(true) => {
                    let controller_error = self.controller_error_receiver.borrow_and_update();
                    return Err(ControllerError::Failed(
                        controller_id.clone(),
                        controller_error.clone(),
                    )
                    .into());
                }
                _ => {}
            }

            // Schedule next check for controller
            let check_staleness = self
                .last_controller_status_check
                .elapsed()
                .unwrap_or_default();

            let signal_port = self.signal_port.clone();
            let controller_id = controller_id.clone();
            let controller_error_sender = self.controller_error_sender.clone();
            let clock = self.clock.clone();
            // Create a new child instance just to check hte
            let (checker_instance, checker_instance_handle) = self.instance().child()?;

            if check_staleness > Duration::from_secs(5) {
                get_tokio_runtime().spawn(async move {
                    let _ = checker_instance_handle.drain_and_stop();
                });
                self.last_controller_status_check = self.clock.system_time_now();
            }
        }

        Ok(())
    }

    /// Get the next message from the queue. It will wait until a message is received
    /// or the timeout is reached in which case it will return None.
    #[hyperactor::instrument(level = "trace", fields(actor_id = hyperactor::tracing::field::display(self.actor_id())))]
    pub async fn next_message(&mut self, timeout_msec: Option<u64>) -> Result<Option<M>> {
        hyperactor::declare_static_timer!(
            PY_NEXT_MESSAGE_TIMER,
            "py_next_message",
            hyperactor_telemetry::TimeUnit::Nanos
        );
        let _ = PY_NEXT_MESSAGE_TIMER
            .start(hyperactor::kv_pairs!("actor_id" => self.actor_id().to_string(), "mode" => match timeout_msec{
                None => "blocking",
                Some(0) => "polling",
                Some(_) => "blocking_with_timeout",
            }));
        self.ensure_detached_and_alive()?;
        match timeout_msec {
            // Blocking wait for next message.
            None => {
                self.message_receiver.recv().await.map(Some)},
            Some(0) => {
                // Non-blocking.
                // Try to get next message without waiting.
                self.message_receiver.try_recv()
            }
            Some(timeout_msec) => {
                // Blocking wait with a timeout.
                match self.clock.timeout(
                    Duration::from_millis(timeout_msec),
                    self.message_receiver.recv(),
                )
                .await
                {
                    Ok(output) => output.map(Some),
                    Err(_) => Ok(None), // Timeout reached
                }
            }
        }
        .map_err(|err| err.into())
        .inspect_err(|err| {
            hyperactor::metrics::ACTOR_MESSAGE_RECEIVE_ERRORS.add(1, hyperactor::kv_pairs!("actor_id" => self.actor_id().to_string()));
            tracing::error!(err=?err, actor_id=%self.actor_id(), "unable to receive next py message");
        })
        .inspect(|_|{
            hyperactor::metrics::ACTOR_MESSAGES_RECEIVED.add(1, hyperactor::kv_pairs!("actor_id" => self.actor_id().to_string()));
        })
    }

    /// Put the actor in stopped mode and return any messages that were received.
    #[hyperactor::instrument(fields(actor_id=hyperactor::tracing::field::display(self.actor_id())))]
    pub fn drain_and_stop(&mut self) -> Result<Vec<M>> {
        self.ensure_detached_and_alive()?;
        let messages: Vec<M> = self.message_receiver.drain().into_iter().collect();
        tracing::info!("stopping the client actor in Python client");
        self.status = InstanceStatus::Stopped;
        Ok(messages)
    }

    pub fn instance(&self) -> &Instance<()> {
        &self.instance
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(init_proc, hyperactor_mod)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.proc",
    )?;
    hyperactor_mod.add_function(f)?;

    hyperactor_mod.add_class::<PyProc>()?;
    hyperactor_mod.add_class::<PyActorId>()?;
    hyperactor_mod.add_class::<PySerialized>()?;
    Ok(())
}
