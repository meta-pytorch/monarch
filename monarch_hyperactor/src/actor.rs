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
use hyperactor::PortHandle;
use hyperactor::Proc;
use hyperactor::ProcId;
use hyperactor::RemoteSpawn;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use hyperactor::reference::WorldId;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Attrs;
use hyperactor_mesh::actor_mesh::CAST_ACTOR_MESH_ID;
use hyperactor_mesh::comm::multicast::CAST_ORIGINATING_SENDER;
use hyperactor_mesh::comm::multicast::CastInfo;
use hyperactor_mesh::proc_mesh::default_bind_spec;
use hyperactor_mesh::reference::ActorMeshId;
use hyperactor_mesh::router;
use hyperactor_mesh::supervision::SupervisionFailureMessage;
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
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::buffers::Buffer;
use crate::buffers::FrozenBuffer;
use crate::config::SHARED_ASYNCIO_RUNTIME;
use crate::context::PyInstance;
use crate::local_state_broker::BrokerId;
use crate::local_state_broker::LocalStateBrokerMessage;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PyMailbox;
use crate::mailbox::PythonUndeliverableMessageEnvelope;
use crate::metrics::ENDPOINT_ACTOR_COUNT;
use crate::metrics::ENDPOINT_ACTOR_ERROR;
use crate::metrics::ENDPOINT_ACTOR_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_ACTOR_PANIC;
use crate::proc::InstanceWrapper;
use crate::proc::PyActorId;
use crate::proc::PyProc;
use crate::proc::PySerialized;
use crate::pytokio::PythonTask;
use crate::runtime::get_tokio_runtime;
use crate::runtime::signal_safe_block_on;
use crate::supervision::MeshFailure;

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

impl std::fmt::Display for MethodSpecifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MethodSpecifier::ReturnsResponse { name } => {
                write!(f, "{}", name)
            }
            MethodSpecifier::ExplicitPort { name } => {
                write!(f, "{}", name)
            }
            MethodSpecifier::Init {} => {
                write!(f, "__init__")
            }
        }
    }
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
                let broker = BrokerId::new(local_state_broker).resolve(cx).await;
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
                            inner: self.message.into_bytes(),
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
                        inner: self.message.into_bytes(),
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
                &hyperactor::data::HexFmt(&(*self.message.to_bytes())[..]).to_string(),
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
        if let Ok(mut buff) = message.extract::<PyRefMut<'py, Buffer>>() {
            return Ok(PythonMessage::new_from_buf(kind, buff.take_part()));
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
            inner: self.message.to_bytes(),
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

    /// instance object that we keep across handle calls
    /// so that we can store information from the Init (spawn rank, controller controller)
    /// and provide it to other calls
    instance: Option<Py<crate::context::PyInstance>>,
}

impl PythonActor {
    pub(crate) fn new(actor_type: PickledPyObject) -> Result<Self, anyhow::Error> {
        Ok(Python::with_gil(|py| -> Result<Self, SerializablePyErr> {
            let unpickled = actor_type.unpickle(py)?;
            let class_type: &Bound<'_, PyType> = unpickled.downcast()?;
            let actor: PyObject = class_type.call0()?.into_py_any(py)?;

            // Only create per-actor TaskLocals if not using shared runtime
            let task_locals = (!hyperactor_config::global::get(SHARED_ASYNCIO_RUNTIME))
                .then(|| Python::allow_threads(py, create_task_locals));
            Ok(Self {
                actor,
                task_locals,
                instance: None,
            })
        })?)
    }

    /// Get the TaskLocals to use for this actor.
    /// Returns either the shared TaskLocals or this actor's own TaskLocals based on configuration.
    fn get_task_locals(&self, py: Python) -> &pyo3_async_runtimes::TaskLocals {
        self.task_locals.as_ref().unwrap_or_else(|| {
            // Use shared TaskLocals
            static SHARED_TASK_LOCALS: OnceLock<pyo3_async_runtimes::TaskLocals> = OnceLock::new();
            Python::allow_threads(py, || SHARED_TASK_LOCALS.get_or_init(create_task_locals))
        })
    }

    /// Bootstrap the root client actor, creating a new proc for it.
    /// This is the legacy entry point that creates its own proc.
    pub(crate) fn bootstrap_client(py: Python<'_>) -> (&'static Instance<Self>, ActorHandle<Self>) {
        static ROOT_CLIENT_INSTANCE: OnceLock<Instance<PythonActor>> = OnceLock::new();

        let client_proc = Proc::direct_with_default(
            default_bind_spec().binding_addr(),
            "mesh_root_client_proc".into(),
            router::global().clone().boxed(),
        )
        .unwrap();

        Self::bootstrap_client_inner(py, client_proc, &ROOT_CLIENT_INSTANCE)
    }

    /// Bootstrap the client proc, storing the root client instance in given static.
    /// This is passed in because we require storage, as the instance is shared.
    /// This can be simplified when we remove v0.
    pub(crate) fn bootstrap_client_inner(
        py: Python<'_>,
        client_proc: Proc,
        root_client_instance: &'static OnceLock<Instance<PythonActor>>,
    ) -> (&'static Instance<Self>, ActorHandle<Self>) {
        // Make this proc reachable through the global router, so that we can use the
        // same client in both direct-addressed and ranked-addressed modes.
        //
        // DEPRECATE after v0 removal
        router::global().bind(client_proc.proc_id().clone().into(), client_proc.clone());

        let actor_mesh_mod = py
            .import("monarch._src.actor.actor_mesh")
            .expect("import actor_mesh");
        let root_client_class = actor_mesh_mod
            .getattr("RootClientActor")
            .expect("get RootClientActor");

        let mut actor = PythonActor::new(
            PickledPyObject::pickle(&actor_mesh_mod.getattr("_Actor").expect("get _Actor"))
                .expect("pickle _Actor"),
        )
        .expect("create client PythonActor");

        let (client, handle, supervision_rx, signal_rx, work_rx) = client_proc
            .actor_instance(
                root_client_class
                    .getattr("name")
                    .expect("get RootClientActor.name")
                    .extract()
                    .expect("extract RootClientActor.name"),
            )
            .expect("root instance create");

        root_client_instance
            .set(client)
            .map_err(|_| "already initialized root client instance")
            .unwrap();

        handle
            .send(
                PythonMessage::new(
                    PythonMessageKind::CallMethod {
                        name: MethodSpecifier::Init {},
                        response_port: None,
                    },
                    root_client_class
                        .call_method0("_pickled_init_args")
                        .expect("call RootClientActor._pickled_init_args"),
                )
                .expect("create RootClientActor init message"),
            )
            .expect("initialize root client");
        // Bind to ensure the Signal and Undeliverable<MessageEnvelope> ports
        // are bound.
        let _client_ref = handle.bind::<PythonActor>();

        let instance = root_client_instance.get().unwrap();

        get_tokio_runtime().spawn(async move {
            let mut signal_rx = signal_rx;
            let mut supervision_rx = supervision_rx;
            let mut work_rx = work_rx;
            let err = 'messages: loop {
                tokio::select! {
                    work = work_rx.recv() => {
                        let work = work.expect("inconsistent work queue state");
                        if let Err(err) = work.handle(&mut actor, instance).await {
                            let kind = ActorErrorKind::processing(err);
                            let err = ActorError {
                                actor_id: Box::new(instance.self_id().clone()),
                                kind: Box::new(kind),
                            };
                            // Give the actor a chance to handle the error produced
                            // in its own message handler. This is important because
                            // we want Undeliverable<MessageEnvelope>, which returns
                            // an Err typically, to create a supervision event and
                            // call __supervise__.
                            let supervision_event = actor_error_to_event(instance, &actor, err);
                            // If the immediate supervision event isn't handled, continue with
                            // exiting the loop.
                            // Else, continue handling messages.
                            if let Err(err) = instance.handle_supervision_event(&mut actor, supervision_event).await {
                                for supervision_event in supervision_rx.drain() {
                                    if let Err(err) = instance.handle_supervision_event(&mut actor, supervision_event).await {
                                        break 'messages err;
                                    }
                                }
                                break err;
                            }
                        }
                    }
                    _ = signal_rx.recv() => {
                        // TODO: do we need any signal handling for the root client?
                    }
                    Ok(supervision_event) = supervision_rx.recv() => {
                        if let Err(err) = instance.handle_supervision_event(&mut actor, supervision_event).await {
                            break err;
                        }
                    }
                };
            };
            let event = actor_error_to_event(instance, &actor, err);
            // The proc supervision handler will send to ProcMeshAgent, which
            // just records it in v1. We want to crash instead, as nothing will
            // monitor the client ProcMeshAgent for now.
            tracing::error!(
                "{}: could not propagate supervision event {} because it reached the global client: exiting the process with code 1",
                instance.self_id(),
                event,
            );

            std::process::exit(1);
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
                instance.self_id().clone(),
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
    py.allow_threads(|| {
        ROOT_CLIENT_ACTOR.get_or_init(|| {
            Python::with_gil(|py| {
                let (client, _handle) = PythonActor::bootstrap_client(py);
                client
            })
        })
    })
}

/// An undeliverable might have its sender address set as the comm actor instead
/// of the original sender. Update it based on the headers present in the message
/// so it matches the sender.
fn update_undeliverable_envelope_for_casting(
    mut envelope: Undeliverable<MessageEnvelope>,
) -> Undeliverable<MessageEnvelope> {
    let old_actor = envelope.0.sender().clone();
    // v1 casting
    if let Some(actor_id) = envelope.0.headers().get(CAST_ORIGINATING_SENDER).cloned() {
        tracing::debug!(
            actor_id = %old_actor,
            "PythonActor::handle_undeliverable_message: remapped comm-actor id to id from CAST_ORIGINATING_SENDER {}", actor_id
        );
        envelope.0.update_sender(actor_id);
    // v0 casting
    } else if let Some(actor_mesh_id) = envelope.0.headers().get(CAST_ACTOR_MESH_ID) {
        match actor_mesh_id {
            ActorMeshId::V0(proc_mesh_id, actor_name) => {
                let actor_id = ActorId(
                    ProcId::Ranked(WorldId(proc_mesh_id.0.clone()), 0),
                    actor_name.clone(),
                    0,
                );
                tracing::debug!(
                    actor_id = %old_actor,
                    "PythonActor::handle_undeliverable_message: remapped comm-actor id to mesh id from CAST_ACTOR_MESH_ID {}", actor_id
                );
                envelope.0.update_sender(actor_id);
            }
            ActorMeshId::V1(_) => {
                tracing::debug!(
                    "PythonActor::handle_undeliverable_message: headers present but V1 ActorMeshId; leaving actor_id unchanged"
                );
            }
        }
    } else {
        // Do nothing, it wasn't from a comm actor.
    }
    envelope
}

#[async_trait]
impl Actor for PythonActor {
    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        err: Option<&ActorError>,
    ) -> anyhow::Result<()> {
        // Calls the "__cleanup__" method on the python instance to allow the actor
        // to control its own cleanup.
        // No headers because this isn't in the context of a message.
        let cx = Context::new(this, Attrs::new());
        // Turn the ActorError into a representation of the error. We may not
        // have an original exception object or traceback, so we just pass in
        // the message.
        let err_as_str = err.map(|e| e.to_string());
        let future = Python::with_gil(|py| {
            let py_cx = match self.instance {
                Some(ref instance) => crate::context::PyContext::new(&cx, instance.clone_ref(py)),
                None => {
                    let py_instance: crate::context::PyInstance = this.into();
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
            let actor = self.actor.bind(py);
            // Some tests don't use the Actor base class, so add this check
            // to be defensive.
            match actor.hasattr("__cleanup__") {
                Ok(false) | Err(_) => {
                    // No cleanup found, default to returning None
                    return Ok(None);
                }
                _ => {}
            }
            let awaitable = actor
                .call_method("__cleanup__", (&py_cx, err_as_str), None)
                .map_err(|err| anyhow::Error::from(SerializablePyErr::from(py, &err)))?;
            if awaitable.is_none() {
                Ok(None)
            } else {
                pyo3_async_runtimes::into_future_with_locals(self.get_task_locals(py), awaitable)
                    .map(Some)
                    .map_err(anyhow::Error::from)
            }
        })?;
        if let Some(future) = future {
            future.await.map_err(anyhow::Error::from)?;
        }
        Ok(())
    }

    fn display_name(&self) -> Option<String> {
        self.instance.as_ref().and_then(|instance| {
            Python::with_gil(|py| instance.bind(py).str().ok().map(|s| s.to_string()))
        })
    }

    async fn handle_undeliverable_message(
        &mut self,
        ins: &Instance<Self>,
        mut envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        if envelope.0.sender() != ins.self_id() {
            // This can happen if the sender is comm. Update the envelope.
            envelope = update_undeliverable_envelope_for_casting(envelope);
        }
        assert_eq!(
            envelope.0.sender(),
            ins.self_id(),
            "undeliverable message was returned to the wrong actor. \
            Return address = {}, src actor = {}, dest actor port = {}",
            envelope.0.sender(),
            ins.self_id(),
            envelope.0.dest()
        );

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

    async fn handle_supervision_event(
        &mut self,
        this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        let cx = Context::new(this, Attrs::new());
        self.handle(
            &cx,
            SupervisionFailureMessage {
                actor_mesh_name: None,
                rank: None,
                event: event.clone(),
            },
        )
        .await
        .map(|_| true)
    }
}

#[async_trait]
impl RemoteSpawn for PythonActor {
    type Params = PickledPyObject;

    async fn new(actor_type: PickledPyObject) -> Result<Self, anyhow::Error> {
        Self::new(actor_type)
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
        // Need to make this a daemon thread, otherwise shutdown will hang.
        kwargs.set_item("daemon", true).unwrap();
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

#[async_trait]
impl Handler<PythonMessage> for PythonActor {
    #[hyperactor::instrument]
    async fn handle(
        &mut self,
        cx: &Context<PythonActor>,
        message: PythonMessage,
    ) -> anyhow::Result<()> {
        let resolved = message.resolve_indirect_call(cx).await?;
        let endpoint = resolved.method.to_string();

        // Create a channel for signaling panics in async endpoints.
        // See [Panics in async endpoints].
        let (sender, receiver) = oneshot::channel();

        let (future, rank) = Python::with_gil(|py| -> Result<_, SerializablePyErr> {
            let instance = self.instance.get_or_insert_with(|| {
                let instance: crate::context::PyInstance = cx.into();
                instance.into_pyobject(py).unwrap().into()
            });
            let rank = instance
                .getattr(py, "rank")?
                .getattr(py, "rank")?
                .extract::<usize>(py)?;
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
            .map(|a| (a, rank))
            .map_err(|err| err.into())
        })?;

        // Spawn a child actor to await the Python handler method.
        tokio::spawn(
            handle_async_endpoint_panic(
                cx.port(),
                PythonTask::new(future)?,
                receiver,
                cx.self_id().to_string(),
                endpoint.clone(),
            )
            .instrument(
                tracing::info_span!(
                    "PythonActor endpoint",
                    actor_id = %cx.self_id(),
                    %rank,
                    %endpoint
                )
                .or_current()
                .follows_from(tracing::Span::current().id())
                .clone(),
            ),
        );
        Ok(())
    }
}

#[derive(Debug)]
struct PanicFromPy(SerializablePyErr);

#[async_trait]
impl Handler<PanicFromPy> for PythonActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        PanicFromPy(err): PanicFromPy,
    ) -> anyhow::Result<()> {
        tracing::error!("caught error in async endpoint {}", err);
        Err(err.into())
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
                    MeshFailure::from(message.clone()),
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
                        tracing::info!(
                            name = "ActorMeshStatus",
                            status = "SupervisionError::Handled",
                            // only care about the event sender when the message is handled
                            actor_name = message.actor_mesh_name,
                            event = %message.event,
                            "__supervise__ on {} handled a supervision event, not reporting any further",
                            cx.self_id(),
                        );
                        Ok(())
                    } else {
                        // For a falsey return value, we propagate the supervision event
                        // to the next owning actor. We do this by returning a new
                        // error. This will not set the causal chain for ActorSupervisionEvent,
                        // so make sure to include the original event in the error message
                        // to provide context.

                        // False -- we propagate the event onward, but update it with the fact that
                        // this actor is now the event creator.
                        for (actor_name, status) in [
                            (
                                message
                                    .actor_mesh_name
                                    .as_deref()
                                    .unwrap_or_else(|| message.event.actor_id.name()),
                                "SupervisionError::Unhandled",
                            ),
                            (cx.self_id().name(), "UnhandledSupervisionEvent"),
                        ] {
                            tracing::info!(
                                name = "ActorMeshStatus",
                                status,
                                actor_name,
                                event = %message.event,
                                "__supervise__ on {} did not handle a supervision event, reporting to the next next owner",
                                cx.self_id(),
                            );
                        }
                        let err = ActorErrorKind::UnhandledSupervisionEvent(Box::new(
                            ActorSupervisionEvent::new(
                                cx.self_id().clone(),
                                self.display_name(),
                                ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(
                                    Box::new(message.event.clone()),
                                )),
                                None,
                            ),
                        ));
                        Err(anyhow::Error::new(err))
                    }
                }
                Err(err) => {
                    // Any other exception will supersede in the propagation chain,
                    // and will become its own supervision failure.
                    // Include the event it was handling in the error message.

                    // Add to caused_by chain.
                    for (actor_name, status) in [
                        (
                            message
                                .actor_mesh_name
                                .as_deref()
                                .unwrap_or_else(|| message.event.actor_id.name()),
                            "SupervisionError::__supervise__::exception",
                        ),
                        (cx.self_id().name(), "UnhandledSupervisionEvent"),
                    ] {
                        tracing::info!(
                            name = "ActorMeshStatus",
                            status,
                            actor_name,
                            event = %message.event,
                            "__supervise__ on {} threw an exception",
                            cx.self_id(),
                        );
                    }
                    let err = ActorErrorKind::UnhandledSupervisionEvent(Box::new(
                        ActorSupervisionEvent::new(
                            cx.self_id().clone(),
                            self.display_name(),
                            ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(
                                err.to_string(),
                                Box::new(message.event.clone()),
                            )),
                            None,
                        ),
                    ));
                    Err(anyhow::Error::new(err))
                }
            }
        })
    }
}

async fn handle_async_endpoint_panic(
    panic_sender: PortHandle<PanicFromPy>,
    task: PythonTask,
    side_channel: oneshot::Receiver<PyObject>,
    actor_id: String,
    endpoint: String,
) {
    // Create attributes for metrics with actor_id and endpoint
    let attributes =
        hyperactor_telemetry::kv_pairs!("actor_id" => actor_id, "endpoint" => endpoint);

    // Record the start time for latency measurement
    let start_time = std::time::Instant::now();

    // Increment throughput counter
    ENDPOINT_ACTOR_COUNT.add(1, attributes);

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
                ENDPOINT_ACTOR_PANIC.add(1, attributes);
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
        // Record error and panic metrics
        ENDPOINT_ACTOR_ERROR.add(1, attributes);

        panic_sender
            .send(PanicFromPy(panic))
            .expect("Unable to send panic message");
    }

    // Record latency in microseconds
    let elapsed_micros = start_time.elapsed().as_micros() as f64;
    ENDPOINT_ACTOR_LATENCY_US_HISTOGRAM.record(elapsed_micros, attributes);
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
    use hyperactor_mesh::resource::Status;
    use hyperactor_mesh::resource::{self};
    use hyperactor_mesh::v1::Error as MeshError;
    use hyperactor_mesh::v1::Name;
    use hyperactor_mesh::v1::host_mesh::mesh_agent::ProcState;
    use pyo3::PyTypeInfo;

    use super::*;
    use crate::actor::to_py_error;

    #[test]
    fn test_python_message_bind_unbind() {
        let reducer_spec = ReducerSpec {
            typehash: 123,
            builder_params: Some(Serialized::serialize(&"abcdefg12345".to_string()).unwrap()),
        };
        let port_ref = PortRef::<PythonMessage>::attest_reducible(
            id!(world[0].client[0][123]),
            Some(reducer_spec),
            None,
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

    #[test]
    fn to_py_error_preserves_proc_creation_message() {
        // State<ProcState> w/ `state.is_none()`
        let state: resource::State<ProcState> = resource::State {
            name: Name::new("my_proc").unwrap(),
            status: Status::Failed("boom".into()),
            state: None,
        };

        // A ProcCreationError
        let err = MeshError::ProcCreationError {
            host_rank: 0,
            mesh_agent: hyperactor::ActorRef::attest(id!(hello[0].actor[0])),
            state: Box::new(state),
        };

        let rust_msg = err.to_string();
        let pyerr = to_py_error(err);

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert!(pyerr.get_type(py).is(&PyValueError::type_object(py)));
            let py_msg = pyerr.value(py).to_string();

            // 1) Bridge preserves the exact message
            assert_eq!(py_msg, rust_msg);
            // 2) Contains the structured state and failure status
            assert!(py_msg.contains(", state: "));
            assert!(py_msg.contains("\"status\":{\"Failed\":\"boom\"}"));
            // 3) Starts with the expected prefix
            let expected_prefix = "error creating proc (host rank 0) on host mesh agent hello[0].actor[0]<hyperactor_mesh::v1::host_mesh::mesh_agent::HostMeshAgent>";
            assert!(py_msg.starts_with(expected_prefix));
        });
    }
}
