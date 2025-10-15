/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::error::Error;
use std::future::pending;
use std::sync::Arc;
use std::sync::OnceLock;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortHandle;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use hyperactor_mesh::comm::multicast::CastInfo;
use monarch_types::PickledPyObject;
use monarch_types::SerializablePyErr;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyBaseException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::types::PyType;
use serde::Deserialize;
use serde::Serialize;
use serde_bytes::ByteBuf;
use serde_multipart::Part;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::buffers::FrozenBuffer;
use crate::config::SHARED_ASYNCIO_RUNTIME;
use crate::context::PyInstance;
use crate::local_state_broker::BrokerId;
use crate::local_state_broker::LocalStateBrokerMessage;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PyMailbox;
use crate::mailbox::PythonUndeliverableMessageEnvelope;
use crate::proc::InstanceWrapper;
use crate::proc::PyActorId;
use crate::proc::PyProc;
use crate::proc::PySerialized;
use crate::pytokio::PythonTask;
use crate::runtime::signal_safe_block_on;
use crate::supervision::MeshFailure;
use crate::supervision::SupervisionFailureMessage;

#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Serialize, Deserialize, Named)]
pub struct PickledMessage {
    sender_actor_id: ActorId,
    message: ByteBuf,
}

impl std::fmt::Debug for PickledMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PickledMessage(sender_actor_id: {:?} message: {})",
            self.sender_actor_id,
            hyperactor::data::HexFmt(self.message.as_slice()),
        )
    }
}

#[pymethods]
impl PickledMessage {
    #[new]
    #[pyo3(signature = (*, sender_actor_id, message))]
    fn new(sender_actor_id: &PyActorId, message: Vec<u8>) -> Self {
        Self {
            sender_actor_id: sender_actor_id.into(),
            message: ByteBuf::from(message),
        }
    }

    #[getter]
    fn sender_actor_id(&self) -> PyActorId {
        self.sender_actor_id.clone().into()
    }

    #[getter]
    fn message<'a>(&self, py: Python<'a>) -> Bound<'a, PyBytes> {
        PyBytes::new(py, self.message.as_ref())
    }

    fn serialize(&self) -> PyResult<PySerialized> {
        PySerialized::new(self)
    }
}

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub struct PickledMessageClientActor {
    instance: Arc<Mutex<InstanceWrapper<PickledMessage>>>,
}

#[pymethods]
impl PickledMessageClientActor {
    #[new]
    fn new(proc: &PyProc, actor_name: &str) -> PyResult<Self> {
        Ok(Self {
            instance: Arc::new(Mutex::new(InstanceWrapper::new(proc, actor_name)?)),
        })
    }

    /// Send a message to any actor that can receive the corresponding serialized message.
    fn send(&self, actor_id: &PyActorId, message: &PySerialized) -> PyResult<()> {
        let instance = self.instance.blocking_lock();
        instance.send(actor_id, message)
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
            result
                .map(|res| res.into_py_any(py))?
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })
    }

    /// Stop the background task and return any messages that were received.
    /// TODO: This is currently just aborting the task, we should have a better way to stop it.
    fn drain_and_stop<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut instance = self.instance.blocking_lock();
        let messages = instance
            .drain_and_stop()
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
            .into_iter()
            .map(|message| message.into_py_any(py))
            .collect::<PyResult<Vec<_>>>()?;
        PyList::new(py, messages)
    }

    fn world_status<'py>(&mut self, py: Python<'py>) -> PyResult<PyObject> {
        let instance = Arc::clone(&self.instance);

        let worlds = signal_safe_block_on(py, async move {
            instance.lock().await.world_status(Default::default()).await
        })??;
        Python::with_gil(|py| {
            let py_dict = PyDict::new(py);
            for (world, status) in worlds {
                py_dict.set_item(world.to_string(), status.to_string())?;
            }
            Ok(py_dict.into())
        })
    }

    #[getter]
    fn actor_id(&self) -> PyResult<PyActorId> {
        let instance = self.instance.blocking_lock();
        Ok(PyActorId::from(instance.actor_id().clone()))
    }
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

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq)]
pub enum PythonMessageKind {
    CallMethod {
        name: MethodSpecifier,
        response_port: Option<EitherPortRef>,
    },
    Result {
        rank: Option<usize>,
    },
    Exception {
        rank: Option<usize>,
    },
    Uninit {},
    CallMethodIndirect {
        name: MethodSpecifier,
        local_state_broker: (String, usize),
        id: usize,
        // specify whether the argument to unflatten the local mailbox,
        // or the next argument of the local state.
        unflatten_args: Vec<UnflattenArg>,
    },
}

impl Default for PythonMessageKind {
    fn default() -> Self {
        PythonMessageKind::Uninit {}
    }
}

fn mailbox<'py, T: Actor>(py: Python<'py>, cx: &Context<'_, T>) -> Bound<'py, PyAny> {
    let mailbox: PyMailbox = cx.mailbox_for_py().clone().into();
    mailbox.into_bound_py_any(py).unwrap()
}

#[pyclass(frozen, module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Clone, Serialize, Deserialize, Named, PartialEq, Default)]
pub struct PythonMessage {
    pub kind: PythonMessageKind,
    pub message: Part,
}

struct ResolvedCallMethod {
    method: MethodSpecifier,
    bytes: FrozenBuffer,
    local_state: PyObject,
    /// Implements PortProtocol
    /// Concretely either a Port, DroppingPort, or LocalPort
    response_port: PyObject,
}

impl PythonMessage {
    pub fn new_from_buf(kind: PythonMessageKind, message: impl Into<Part>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn into_rank(self, rank: usize) -> Self {
        let rank = Some(rank);
        match self.kind {
            PythonMessageKind::Result { .. } => PythonMessage {
                kind: PythonMessageKind::Result { rank },
                message: self.message,
            },
            PythonMessageKind::Exception { .. } => PythonMessage {
                kind: PythonMessageKind::Exception { rank },
                message: self.message,
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
            } => {
                let broker = BrokerId::new(local_state_broker).resolve(cx).unwrap();
                let (send, recv) = cx.open_once_port();
                broker.send(LocalStateBrokerMessage::Get(id, send))?;
                let state = recv.recv().await?;
                let mut state_it = state.state.into_iter();
                Python::with_gil(|py| {
                    let mailbox = mailbox(py, cx);
                    let local_state = PyList::new(
                        py,
                        unflatten_args.into_iter().map(|x| -> Bound<'_, PyAny> {
                            match x {
                                UnflattenArg::Mailbox => mailbox.clone(),
                                UnflattenArg::PyObject => state_it.next().unwrap().into_bound(py),
                            }
                        }),
                    )
                    .unwrap()
                    .into();
                    let response_port = LocalPort {
                        inner: Some(state.response_port),
                    }
                    .into_py_any(py)
                    .unwrap();
                    Ok(ResolvedCallMethod {
                        method: name,
                        bytes: FrozenBuffer {
                            inner: self.message.into_inner(),
                        },
                        local_state,
                        response_port,
                    })
                })
            }
            PythonMessageKind::CallMethod {
                name,
                response_port,
            } => Python::with_gil(|py| {
                let mailbox = mailbox(py, cx);
                let local_state = py
                    .import("itertools")
                    .unwrap()
                    .call_method1("repeat", (mailbox.clone(),))
                    .unwrap()
                    .unbind();
                let instance: PyInstance = cx.into();
                let response_port = response_port
                    .map_or_else(
                        || {
                            py.import("monarch._src.actor.actor_mesh")
                                .unwrap()
                                .call_method0("DroppingPort")
                                .unwrap()
                        },
                        |x| {
                            let point = cx.cast_point();
                            py.import("monarch._src.actor.actor_mesh")
                                .unwrap()
                                .call_method1("Port", (x, instance, point.rank()))
                                .unwrap()
                        },
                    )
                    .unbind();
                Ok(ResolvedCallMethod {
                    method: name,
                    bytes: FrozenBuffer {
                        inner: self.message.into_inner(),
                    },
                    local_state,
                    response_port,
                })
            }),
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
                &hyperactor::data::HexFmt(&(*self.message)[..]).to_string(),
            )
            .finish()
    }
}

impl Unbind for PythonMessage {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        match &self.kind {
            PythonMessageKind::CallMethod { response_port, .. } => response_port.unbind(bindings),
            _ => Ok(()),
        }
    }
}

impl Bind for PythonMessage {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        match &mut self.kind {
            PythonMessageKind::CallMethod { response_port, .. } => response_port.bind(bindings),
            _ => Ok(()),
        }
    }
}

#[pymethods]
impl PythonMessage {
    #[new]
    #[pyo3(signature = (kind, message))]
    pub fn new<'py>(kind: PythonMessageKind, message: Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(buff) = message.extract::<Bound<'py, FrozenBuffer>>() {
            let frozen = buff.borrow_mut();
            return Ok(PythonMessage::new_from_buf(kind, frozen.inner.clone()));
        } else if let Ok(buff) = message.extract::<Bound<'py, PyBytes>>() {
            return Ok(PythonMessage::new_from_buf(
                kind,
                Vec::from(buff.as_bytes()),
            ));
        }

        Err(PyTypeError::new_err(
            "PythonMessage(buff) takes Buffer or bytes objects only",
        ))
    }

    #[getter]
    fn kind(&self) -> PythonMessageKind {
        self.kind.clone()
    }

    #[getter]
    fn message(&self) -> FrozenBuffer {
        FrozenBuffer {
            inner: self.message.clone().into_inner(),
        }
    }
}

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
pub(super) struct PythonActorHandle {
    pub(super) inner: ActorHandle<PythonActor>,
}

#[pymethods]
impl PythonActorHandle {
    // TODO: do the pickling in rust
    // TODO(pzhang) Use instance after its required by PortHandle.
    fn send(&self, _instance: &PyInstance, message: &PythonMessage) -> PyResult<()> {
        self.inner
            .send(message.clone())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        Ok(())
    }

    fn bind(&self) -> PyActorId {
        self.inner.bind::<PythonActor>().into_actor_id().into()
    }
}

#[derive(Debug)]
enum UnhandledErrorObserver {
    ForwardTo(UnboundedReceiver<SerializablePyErr>),
    HandlerActor(ActorHandle<EndpointPanic2SupervisionEvent>),
    None,
}

/// An actor for which message handlers are implemented in Python.
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        PythonMessage { cast = true },
        SupervisionFailureMessage { cast = true },
    ],
)]
pub struct PythonActor {
    /// The Python object that we delegate message handling to. An instance of
    /// `monarch.actor_mesh._Actor`.
    pub(super) actor: PyObject,

    /// Stores a reference to the Python event loop to run Python coroutines on.
    /// This is None when using single runtime mode, Some when using per-actor mode.
    task_locals: Option<pyo3_async_runtimes::TaskLocals>,
    panic_watcher: UnhandledErrorObserver,
    panic_sender: UnboundedSender<SerializablePyErr>,

    /// instance object that we keep across handle calls
    /// so that we can store information from the Init (spawn rank, controller controller)
    /// and provide it to other calls
    instance: Option<Py<crate::context::PyInstance>>,
}

impl PythonActor {
    /// Get the TaskLocals to use for this actor.
    /// Returns either the shared TaskLocals or this actor's own TaskLocals based on configuration.
    fn get_task_locals(&self, py: Python) -> &pyo3_async_runtimes::TaskLocals {
        self.task_locals.as_ref().unwrap_or_else(|| {
            // Use shared TaskLocals
            static SHARED_TASK_LOCALS: OnceLock<pyo3_async_runtimes::TaskLocals> = OnceLock::new();
            Python::allow_threads(py, || SHARED_TASK_LOCALS.get_or_init(create_task_locals))
        })
    }
}

#[async_trait]
impl Actor for PythonActor {
    type Params = PickledPyObject;

    async fn new(actor_type: PickledPyObject) -> Result<Self, anyhow::Error> {
        Ok(Python::with_gil(|py| -> Result<Self, SerializablePyErr> {
            let unpickled = actor_type.unpickle(py)?;
            let class_type: &Bound<'_, PyType> = unpickled.downcast()?;
            let actor: PyObject = class_type.call0()?.into_py_any(py)?;

            // Only create per-actor TaskLocals if not using shared runtime
            let task_locals = (!hyperactor::config::global::get(SHARED_ASYNCIO_RUNTIME))
                .then(|| Python::allow_threads(py, create_task_locals));
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            Ok(Self {
                actor,
                task_locals,
                panic_watcher: UnhandledErrorObserver::ForwardTo(rx),
                panic_sender: tx,
                instance: None,
            })
        })?)
    }

    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        self.panic_watcher = UnhandledErrorObserver::HandlerActor(
            match std::mem::replace(&mut self.panic_watcher, UnhandledErrorObserver::None) {
                UnhandledErrorObserver::ForwardTo(chan) => {
                    EndpointPanic2SupervisionEvent::spawn(this, chan).await?
                }
                UnhandledErrorObserver::HandlerActor(_actor) => {
                    panic!("init called twice");
                }
                UnhandledErrorObserver::None => {
                    unreachable!("init called while in an invalid state")
                }
            },
        );
        Ok(())
    }

    async fn handle_undeliverable_message(
        &mut self,
        ins: &Instance<Self>,
        envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        assert_eq!(envelope.0.sender(), ins.self_id());

        let cx = Context::new(ins, envelope.0.headers().clone());

        let (envelope, handled) = Python::with_gil(|py| {
            let py_cx = match self.instance {
                Some(ref instance) => crate::context::PyContext::new(&cx, instance.clone_ref(py)),
                None => {
                    let py_instance: crate::context::PyInstance = ins.into();
                    crate::context::PyContext::new(
                        &cx,
                        py_instance
                            .into_py_any(py)?
                            .downcast_bound(py)
                            .map_err(PyErr::from)?
                            .clone()
                            .unbind(),
                    )
                }
            }
            .into_bound_py_any(py)?;
            let py_envelope = PythonUndeliverableMessageEnvelope {
                inner: Some(envelope),
            }
            .into_bound_py_any(py)?;
            let handled = self
                .actor
                .call_method(
                    py,
                    "_handle_undeliverable_message",
                    (&py_cx, &py_envelope),
                    None,
                )
                .map_err(|err| anyhow::Error::from(SerializablePyErr::from(py, &err)))?
                .extract::<bool>(py)?;
            Ok::<_, anyhow::Error>((
                py_envelope
                    .downcast::<PythonUndeliverableMessageEnvelope>()
                    .map_err(PyErr::from)?
                    .try_borrow_mut()
                    .map_err(PyErr::from)?
                    .take()?,
                handled,
            ))
        })?;

        if !handled {
            hyperactor::actor::handle_undeliverable_message(ins, envelope)
        } else {
            Ok(())
        }
    }
}

/// Create a new TaskLocals with its own asyncio event loop in a dedicated thread.
fn create_task_locals() -> pyo3_async_runtimes::TaskLocals {
    Python::with_gil(|py| {
        let asyncio = Python::import(py, "asyncio").unwrap();
        let event_loop = asyncio.call_method0("new_event_loop").unwrap();
        let task_locals = pyo3_async_runtimes::TaskLocals::new(event_loop.clone())
            .copy_context(py)
            .unwrap();

        let kwargs = PyDict::new(py);
        let target = event_loop.getattr("run_forever").unwrap();
        kwargs.set_item("target", target).unwrap();
        let thread = py
            .import("threading")
            .unwrap()
            .call_method("Thread", (), Some(&kwargs))
            .unwrap();
        thread.call_method0("start").unwrap();
        task_locals
    })
}

// [Panics in async endpoints]
// This class exists to solve a deadlock when an async endpoint calls into some
// Rust code that panics.
//
// When an async endpoint is invoked and calls into Rust, the following sequence happens:
//
// hyperactor message -> PythonActor::handle() -> call _Actor.handle() in Python
//   -> convert the resulting coroutine into a Rust future, but scheduled on
//      the Python asyncio event loop (`into_future_with_locals`)
//   -> set a callback on Python asyncio loop to ping a channel that fulfills
//      the Rust future when the Python coroutine has finished. ('PyTaskCompleter`)
//
// This works fine for normal results and Python exceptions: we will take the
// result of the callback and send it through the channel, where it will be
// returned to the `await`er of the Rust future.
//
// This DOESN'T work for panics. The behavior of a panic in pyo3-bound code is
// that it will get caught by pyo3 and re-thrown to Python as a PanicException.
// And if that PanicException ever makes it back to Rust, it will get unwound
// instead of passed around as a normal PyErr type.
//
// So:
//   - Endpoint panics.
//   - This panic is captured as a PanicException in Python and
//     stored as the result of the Python asyncio task.
//   - When the callback in `PyTaskCompleter` queries the status of the task to
//     pass it back to the Rust awaiter, instead of getting a Result type, it
//     just starts resumes unwinding the PanicException
//   - This triggers a deadlock, because the whole task dies without ever
//     pinging the response channel, and the Rust awaiter will never complete.
//
// We work around this by passing a side-channel to our Python task so that it,
// in Python, can catch the PanicException and notify the Rust awaiter manually.
// In this way we can guarantee that the awaiter will complete even if the
// `PyTaskCompleter` callback explodes.
#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
struct PanicFlag {
    sender: Option<tokio::sync::oneshot::Sender<PyObject>>,
}

#[pymethods]
impl PanicFlag {
    fn signal_panic(&mut self, ex: PyObject) {
        self.sender.take().unwrap().send(ex).unwrap();
    }
}

/// The sole reason why this Actor exists as opposed to a background task is because returning Err in an Actor message
/// handler is how we can surface supervision events.
///
/// We call this actor EndpointPanic2SupervisionEvent because it's only responsibility is to turn endpoint panics into supervision events
#[derive(Debug)]
struct EndpointPanic2SupervisionEvent {
    endpoint_panic_rx: UnboundedReceiver<SerializablePyErr>,
}

#[async_trait]
impl Actor for EndpointPanic2SupervisionEvent {
    type Params = UnboundedReceiver<SerializablePyErr>;

    async fn new(
        endpoint_panic_rx: UnboundedReceiver<SerializablePyErr>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self { endpoint_panic_rx })
    }

    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.handle().send(WatchForEndpointPanics)?;
        Ok(())
    }
}

#[derive(Debug, Named, Serialize, Deserialize)]
struct WatchForEndpointPanics;

#[async_trait]
impl Handler<WatchForEndpointPanics> for EndpointPanic2SupervisionEvent {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        _message: WatchForEndpointPanics,
    ) -> anyhow::Result<()> {
        match self.endpoint_panic_rx.recv().await {
            Some(err) => {
                tracing::error!("caught error in async endpoint {}", err);
                return Err(err.into());
            }
            None => {
                tracing::warn!("panic forwarding channel was closed unexpectedly")
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<PythonMessage> for PythonActor {
    async fn handle(
        &mut self,
        cx: &Context<PythonActor>,
        message: PythonMessage,
    ) -> anyhow::Result<()> {
        let resolved = message.resolve_indirect_call(cx).await?;

        // Create a channel for signaling panics in async endpoints.
        // See [Panics in async endpoints].
        let (sender, receiver) = oneshot::channel();

        let future = Python::with_gil(|py| -> Result<_, SerializablePyErr> {
            let instance = self.instance.get_or_insert_with(|| {
                let instance: crate::context::PyInstance = cx.into();
                instance.into_pyobject(py).unwrap().into()
            });
            let awaitable = self.actor.call_method(
                py,
                "handle",
                (
                    crate::context::PyContext::new(cx, instance.clone_ref(py)),
                    resolved.method,
                    resolved.bytes,
                    PanicFlag {
                        sender: Some(sender),
                    },
                    resolved.local_state,
                    resolved.response_port,
                ),
                None,
            )?;

            pyo3_async_runtimes::into_future_with_locals(
                self.get_task_locals(py),
                awaitable.into_bound(py),
            )
            .map_err(|err| err.into())
        })?;

        // Spawn a child actor to await the Python handler method.
        tokio::spawn(
            handle_async_endpoint_panic(
                self.panic_sender.clone(),
                PythonTask::new(future),
                receiver,
            )
            .instrument(
                tracing::info_span!("py_panic_handler")
                    .follows_from(tracing::Span::current().id())
                    .clone(),
            ),
        );
        Ok(())
    }
}

#[async_trait]
impl Handler<SupervisionFailureMessage> for PythonActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: SupervisionFailureMessage,
    ) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            let instance = self.instance.get_or_insert_with(|| {
                let instance: crate::context::PyInstance = cx.into();
                instance.into_pyobject(py).unwrap().into()
            });
            let actor = self.actor.bind(py);
            // The _Actor class always has a __supervise__ method, so this should
            // never happen.
            if !actor.hasattr("__supervise__")? {
                return Err(anyhow::anyhow!("no __supervise__ method on {:?}", actor));
            }
            let result = actor.call_method(
                "__supervise__",
                (
                    crate::context::PyContext::new(cx, instance.clone_ref(py)),
                    MeshFailure::from(message),
                ),
                None,
            );
            match result {
                Ok(s) => {
                    if s.is_truthy()? {
                        // If the return value is truthy, then the exception was handled
                        // and doesn't need to be propagated.
                        // TODO: We also don't want to deliver multiple supervision
                        // events from the same mesh if an earlier one is handled.
                        Ok(())
                    } else {
                        // TODO: propagate.
                        Ok(())
                    }
                }
                Err(err) => {
                    // Any other exception will be returned, and will become its
                    // own supervision failure.
                    // TODO: we still need to propagate the original supervision
                    // error.
                    tracing::error!("caught error in __supervise__ {}", err);
                    Err(err.into())
                }
            }
        })
    }
}

async fn handle_async_endpoint_panic(
    panic_sender: UnboundedSender<SerializablePyErr>,
    task: PythonTask,
    side_channel: oneshot::Receiver<PyObject>,
) {
    let err_or_never = async {
        // The side channel will resolve with a value if a panic occured during
        // processing of the async endpoint, see [Panics in async endpoints].
        match side_channel.await {
            Ok(value) => Python::with_gil(|py| -> Option<SerializablePyErr> {
                let err: PyErr = value
                    .downcast_bound::<PyBaseException>(py)
                    .unwrap()
                    .clone()
                    .into();
                Some(err.into())
            }),
            // An Err means that the sender has been dropped without sending.
            // That's okay, it just means that the Python task has completed.
            // In that case, just never resolve this future. We expect the other
            // branch of the select to finish eventually.
            Err(_) => pending().await,
        }
    };
    let future = task.take();
    if let Some(panic) = tokio::select! {
        result = future => {
            match result {
                Ok(_) => None,
                Err(e) => Some(e.into()),
            }
        },
        result = err_or_never => {
            result
        }
    } {
        panic_sender
            .send(panic)
            .expect("Unable to send panic message");
    }
}

#[pyclass(module = "monarch._rust_bindings.monarch_hyperactor.actor")]
#[derive(Debug)]
struct LocalPort {
    inner: Option<OncePortHandle<Result<PyObject, PyObject>>>,
}

pub(crate) fn to_py_error<T>(e: T) -> PyErr
where
    T: Error,
{
    PyErr::new::<PyValueError, _>(e.to_string())
}

#[pymethods]
impl LocalPort {
    fn send(&mut self, obj: PyObject) -> PyResult<()> {
        let port = self.inner.take().expect("use local port once");
        port.send(Ok(obj)).map_err(to_py_error)
    }
    fn exception(&mut self, e: PyObject) -> PyResult<()> {
        let port = self.inner.take().expect("use local port once");
        port.send(Err(e)).map_err(to_py_error)
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PickledMessage>()?;
    hyperactor_mod.add_class::<PickledMessageClientActor>()?;
    hyperactor_mod.add_class::<PythonActorHandle>()?;
    hyperactor_mod.add_class::<PythonMessage>()?;
    hyperactor_mod.add_class::<PythonMessageKind>()?;
    hyperactor_mod.add_class::<MethodSpecifier>()?;
    hyperactor_mod.add_class::<UnflattenArg>()?;
    hyperactor_mod.add_class::<PanicFlag>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hyperactor::PortRef;
    use hyperactor::accum::ReducerSpec;
    use hyperactor::data::Serialized;
    use hyperactor::id;
    use hyperactor::message::ErasedUnbound;
    use hyperactor::message::Unbound;
    use hyperactor::reference::UnboundPort;

    use super::*;

    #[test]
    fn test_python_message_bind_unbind() {
        let reducer_spec = ReducerSpec {
            typehash: 123,
            builder_params: Some(Serialized::serialize(&"abcdefg12345".to_string()).unwrap()),
        };
        let port_ref = PortRef::<PythonMessage>::attest_reducible(
            id!(world[0].client[0][123]),
            Some(reducer_spec),
        );
        let message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ReturnsResponse {
                    name: "test".to_string(),
                },
                response_port: Some(EitherPortRef::Unbounded(port_ref.clone().into())),
            },
            message: Part::from(vec![1, 2, 3]),
        };
        {
            let mut erased = ErasedUnbound::try_from_message(message.clone()).unwrap();
            let mut bindings = vec![];
            erased
                .visit_mut::<UnboundPort>(|b| {
                    bindings.push(b.clone());
                    Ok(())
                })
                .unwrap();
            assert_eq!(bindings, vec![UnboundPort::from(&port_ref)]);
            let unbound = Unbound::try_from_message(message.clone()).unwrap();
            assert_eq!(message, unbound.bind().unwrap());
        }

        let no_port_message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ReturnsResponse {
                    name: "test".to_string(),
                },
                response_port: None,
            },
            ..message
        };
        {
            let mut erased = ErasedUnbound::try_from_message(no_port_message.clone()).unwrap();
            let mut bindings = vec![];
            erased
                .visit_mut::<UnboundPort>(|b| {
                    bindings.push(b.clone());
                    Ok(())
                })
                .unwrap();
            assert_eq!(bindings.len(), 0);
            let unbound = Unbound::try_from_message(no_port_message.clone()).unwrap();
            assert_eq!(no_port_message, unbound.bind().unwrap());
        }
    }
}
