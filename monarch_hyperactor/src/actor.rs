/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::error::Error;
use std::fmt::Debug;
use std::future::pending;
use std::ops::Deref;
use std::sync::OnceLock;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::PortHandle;
use hyperactor::Proc;
use hyperactor::RemoteSpawn;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Signal;
use hyperactor::mailbox::BoxableMailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use hyperactor_mesh::casting::update_undeliverable_envelope_for_casting;
use hyperactor_mesh::comm::multicast::CAST_POINT;
use hyperactor_mesh::comm::multicast::CastInfo;
use hyperactor_mesh::router;
use hyperactor_mesh::supervision::MeshFailure;
use hyperactor_mesh::transport::default_bind_spec;
use monarch_types::PickledPyObject;
use monarch_types::SerializablePyErr;
use ndslice::Point;
use ndslice::extent;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyBaseException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::types::PyType;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use tokio::sync::oneshot;
use typeuri::Named;

use crate::buffers::FrozenBuffer;
use crate::config::ACTOR_QUEUE_DISPATCH;
use crate::config::SHARED_ASYNCIO_RUNTIME;
use crate::context::PyInstance;
use crate::local_state_broker::BrokerId;
use crate::local_state_broker::LocalStateBrokerMessage;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PyMailbox;
use crate::mailbox::PythonPortHandle;
use crate::mailbox::PythonUndeliverableMessageEnvelope;
use crate::metrics::ENDPOINT_ACTOR_COUNT;
use crate::metrics::ENDPOINT_ACTOR_ERROR;
use crate::metrics::ENDPOINT_ACTOR_LATENCY_US_HISTOGRAM;
use crate::metrics::ENDPOINT_ACTOR_PANIC;
use crate::proc::PyActorId;
use crate::pympsc;
use crate::pytokio::PythonTask;
use crate::runtime::get_proc_runtime;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;
use crate::supervision::PyMeshFailure;

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
wirevalue::register_type!(PythonMessageKind);

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
#[derive(Clone, Serialize, Deserialize, Named, Default, PartialEq)]
pub struct PythonMessage {
    pub kind: PythonMessageKind,
    pub message: Part,
}

wirevalue::register_type!(PythonMessage);

struct ResolvedCallMethod {
    method: MethodSpecifier,
    bytes: FrozenBuffer,
    local_state: Py<PyAny>,
    /// Implements PortProtocol
    /// Concretely either a Port, DroppingPort, or LocalPort
    response_port: Py<PyAny>,
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
    pub response_port: Py<PyAny>,
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
                broker.send(cx, LocalStateBrokerMessage::Get(id, send))?;
                let state = recv.recv().await?;
                let mut state_it = state.state.into_iter();
                monarch_with_gil(|py| {
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
                        instance: cx.into(),
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
                .await
            }
            PythonMessageKind::CallMethod {
                name,
                response_port,
            } => {
                monarch_with_gil(|py| {
                    let local_state: Py<PyAny> = PyList::empty(py).unbind().into();
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
                })
                .await
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
    pub fn new<'py>(kind: PythonMessageKind, message: PyRef<'py, FrozenBuffer>) -> PyResult<Self> {
        Ok(PythonMessage::new_from_buf(kind, message.inner.clone()))
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
    fn send(&self, instance: &PyInstance, message: &PythonMessage) -> PyResult<()> {
        self.inner
            .send(instance.deref(), message.clone())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        Ok(())
    }

    fn bind(&self) -> PyActorId {
        self.inner.bind::<PythonActor>().into_actor_id().into()
    }
}

/// Dispatch mode for Python actors.
#[derive(Debug)]
pub enum PythonActorDispatchMode {
    /// Direct dispatch: Rust acquires the GIL and calls Python handlers directly.
    Direct,
    /// Queue dispatch: Rust enqueues messages to a channel; Python dequeues and dispatches.
    Queue {
        /// Channel sender for enqueuing messages to Python.
        sender: pympsc::Sender,
        /// Channel receiver, taken during Actor::init to start the message loop.
        receiver: Option<pympsc::PyReceiver>,
    },
}

/// An actor for which message handlers are implemented in Python.
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        PythonMessage { cast = true },
        MeshFailure { cast = true },
    ],
)]
pub struct PythonActor {
    /// The Python object that we delegate message handling to.
    actor: Py<PyAny>,
    /// Stores a reference to the Python event loop to run Python coroutines on.
    /// This is None when using single runtime mode, Some when using per-actor mode.
    task_locals: Option<pyo3_async_runtimes::TaskLocals>,
    /// Instance object that we keep across handle calls so that we can store
    /// information from the Init (spawn rank, controller) and provide it to other calls.
    instance: Option<Py<crate::context::PyInstance>>,
    /// Dispatch mode for this actor.
    dispatch_mode: PythonActorDispatchMode,
    /// The location in the actor mesh at which this actor was spawned.
    spawn_point: OnceLock<Option<Point>>,
    /// Initial message to process during PythonActor::init.
    init_message: Option<PythonMessage>,
}

impl PythonActor {
    pub(crate) fn new(
        actor_type: PickledPyObject,
        init_message: Option<PythonMessage>,
        spawn_point: Option<Point>,
    ) -> Result<Self, anyhow::Error> {
        let use_queue_dispatch = hyperactor_config::global::get(ACTOR_QUEUE_DISPATCH);

        Ok(monarch_with_gil_blocking(
            |py| -> Result<Self, SerializablePyErr> {
                let unpickled = actor_type.unpickle(py)?;
                let class_type: &Bound<'_, PyType> = unpickled.downcast()?;
                let actor: Py<PyAny> = class_type.call0()?.into_py_any(py)?;

                // Only create per-actor TaskLocals if not using shared runtime
                let task_locals = (!hyperactor_config::global::get(SHARED_ASYNCIO_RUNTIME))
                    .then(|| Python::detach(py, create_task_locals));

                let dispatch_mode = if use_queue_dispatch {
                    let (sender, receiver) = pympsc::channel().map_err(|e| {
                        let py_err = PyRuntimeError::new_err(e.to_string());
                        SerializablePyErr::from(py, &py_err)
                    })?;
                    PythonActorDispatchMode::Queue {
                        sender,
                        receiver: Some(receiver),
                    }
                } else {
                    PythonActorDispatchMode::Direct
                };

                Ok(Self {
                    actor,
                    task_locals,
                    instance: None,
                    dispatch_mode,
                    spawn_point: OnceLock::from(spawn_point),
                    init_message,
                })
            },
        )?)
    }

    /// Get the TaskLocals to use for this actor.
    /// Returns either the shared TaskLocals or this actor's own TaskLocals based on configuration.
    fn get_task_locals(&self, py: Python) -> &pyo3_async_runtimes::TaskLocals {
        self.task_locals
            .as_ref()
            .unwrap_or_else(|| shared_task_locals(py))
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
            },
            init_frozen_buffer,
        );

        let mut actor = PythonActor::new(
            actor_type,
            Some(init_message),
            Some(extent!().point_of_rank(0).unwrap()),
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
        let instance = root_client_instance.get().unwrap();

        // Bind to ensure the Signal and Undeliverable<MessageEnvelope> ports
        // are bound.
        let _client_ref = handle.bind::<PythonActor>();

        get_tokio_runtime().spawn(async move {
            // This is gross. Sorry.
            actor.init(instance).await.unwrap();

            let mut signal_rx = signal_rx;
            let mut supervision_rx = supervision_rx;
            let mut work_rx = work_rx;
            let mut need_drain = false;
            let mut err = 'messages: loop {
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
                                        break 'messages Some(err);
                                    }
                                }
                                break Some(err);
                            }
                        }
                    }
                    signal = signal_rx.recv() => {
                        let signal = signal.map_err(ActorError::from);
                        tracing::info!(actor_id = %instance.self_id(), "client received signal {signal:?}");
                        match signal {
                            Ok(signal@(Signal::Stop(_) | Signal::DrainAndStop(_))) => {
                                need_drain = matches!(signal, Signal::DrainAndStop(_));
                                break None;
                            },
                            Ok(Signal::ChildStopped(_)) => {},
                            Ok(Signal::Abort(reason)) => {
                                break Some(ActorError { actor_id: Box::new(instance.self_id().clone()), kind: Box::new(ActorErrorKind::Aborted(reason)) })
                            },
                            Err(err) => break Some(err),
                        }
                    }
                    Ok(supervision_event) = supervision_rx.recv() => {
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
                            actor_id: Box::new(instance.self_id().clone()),
                            kind: Box::new(ActorErrorKind::processing(e)),
                        });
                        break;
                    }
                    n += 1;
                }
                tracing::debug!(actor_id = %instance.self_id(), "client drained {} messages before stopping", n);
            }
            if let Some(err) = err {
                let event = actor_error_to_event(instance, &actor, err);
                // The proc supervision handler will send to ProcMeshAgent, which
                // just records it in v1. We want to crash instead, as nothing will
                // monitor the client ProcMeshAgent for now.
                tracing::error!(
                    actor_id = %instance.self_id(),
                    "could not propagate supervision event {} because it reached the global client: exiting the process with code 1",
                    event,
                );

                std::process::exit(1);
            } else {
                tracing::info!(actor_id = %instance.self_id(), "client stopped");
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
    py.detach(|| {
        ROOT_CLIENT_ACTOR.get_or_init(|| {
            monarch_with_gil_blocking(|py| {
                let (client, _handle) = PythonActor::bootstrap_client(py);
                client
            })
        })
    })
}

#[async_trait]
impl Actor for PythonActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        if let PythonActorDispatchMode::Queue { receiver, .. } = &mut self.dispatch_mode {
            let receiver = receiver.take().unwrap();

            // Create an error port that converts PythonMessage to an abort signal.
            // This allows Python to send errors that trigger actor supervision.
            let error_port: hyperactor::PortHandle<PythonMessage> =
                this.port::<Signal>().contramap(|msg: PythonMessage| {
                    monarch_with_gil_blocking(|py| {
                        let err = match msg.kind {
                            PythonMessageKind::Exception { .. } => {
                                // Deserialize the error from the message
                                let cloudpickle = py.import("cloudpickle").unwrap();
                                let err_obj = cloudpickle
                                    .call_method1("loads", (msg.message.to_bytes().as_ref(),))
                                    .unwrap();
                                let py_err = pyo3::PyErr::from_value(err_obj);
                                SerializablePyErr::from(py, &py_err)
                            }
                            _ => {
                                let py_err = PyRuntimeError::new_err(format!(
                                    "expected Exception, got {:?}",
                                    msg.kind
                                ));
                                SerializablePyErr::from(py, &py_err)
                            }
                        };
                        Signal::Abort(err.to_string())
                    })
                });

            let error_port_handle = PythonPortHandle::new(error_port);

            monarch_with_gil(|py| {
                let tl = self
                    .task_locals
                    .as_ref()
                    .unwrap_or_else(|| shared_task_locals(py));
                let awaitable = self.actor.call_method(
                    py,
                    "_dispatch_loop",
                    (receiver, error_port_handle),
                    None,
                )?;
                let future =
                    pyo3_async_runtimes::into_future_with_locals(tl, awaitable.into_bound(py))?;
                tokio::spawn(async move {
                    if let Err(e) = future.await {
                        tracing::error!("message loop error: {}", e);
                    }
                });
                Ok::<_, anyhow::Error>(())
            })
            .await?;
        }

        if let Some(init_message) = self.init_message.take() {
            let spawn_point = self.spawn_point.get().unwrap().as_ref().expect("PythonActor should never be spawned with init_message unless spawn_point also specified").clone();
            let mut headers = Flattrs::new();
            headers.set(CAST_POINT, spawn_point);
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
        let err_as_str = err.map(|e| e.to_string());
        let future = monarch_with_gil(|py| {
            let py_cx = match &self.instance {
                Some(instance) => crate::context::PyContext::new(&cx, instance.clone_ref(py)),
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
        })
        .await?;
        if let Some(future) = future {
            future.await.map_err(anyhow::Error::from)?;
        }
        Ok(())
    }

    fn display_name(&self) -> Option<String> {
        self.instance.as_ref().and_then(|instance| {
            monarch_with_gil_blocking(|py| instance.bind(py).str().ok().map(|s| s.to_string()))
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
            Return address = {}, src actor = {}, dest actor port = {}, envelope headers = {}",
            envelope.0.sender(),
            ins.self_id(),
            envelope.0.dest(),
            envelope.0.headers()
        );

        let cx = Context::new(ins, envelope.0.headers().clone());

        let (envelope, handled) = monarch_with_gil(|py| {
            let py_cx = match &self.instance {
                Some(instance) => crate::context::PyContext::new(&cx, instance.clone_ref(py)),
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
        })
        .await?;

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
        let cx = Context::new(this, Flattrs::new());
        self.handle(
            &cx,
            MeshFailure {
                actor_mesh_name: None,
                rank: None,
                event: event.clone(),
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
}

impl PythonActorParams {
    pub(crate) fn new(actor_type: PickledPyObject, init_message: Option<PythonMessage>) -> Self {
        Self {
            actor_type,
            init_message,
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
        }: PythonActorParams,
        environment: Flattrs,
    ) -> Result<Self, anyhow::Error> {
        let spawn_point = environment.get(CAST_POINT);
        Self::new(actor_type, init_message, spawn_point)
    }
}

/// Create a new TaskLocals with its own asyncio event loop in a dedicated thread.
fn create_task_locals() -> pyo3_async_runtimes::TaskLocals {
    monarch_with_gil_blocking(|py| {
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

/// Get the shared TaskLocals, creating it if necessary.
fn shared_task_locals(py: Python) -> &'static pyo3_async_runtimes::TaskLocals {
    static SHARED_TASK_LOCALS: OnceLock<pyo3_async_runtimes::TaskLocals> = OnceLock::new();
    Python::detach(py, || SHARED_TASK_LOCALS.get_or_init(create_task_locals))
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
    sender: Option<tokio::sync::oneshot::Sender<Py<PyAny>>>,
}

#[pymethods]
impl PanicFlag {
    fn signal_panic(&mut self, ex: Py<PyAny>) {
        self.sender.take().unwrap().send(ex).unwrap();
    }
}

#[async_trait]
impl Handler<PythonMessage> for PythonActor {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn handle(
        &mut self,
        cx: &Context<PythonActor>,
        message: PythonMessage,
    ) -> anyhow::Result<()> {
        match &self.dispatch_mode {
            PythonActorDispatchMode::Direct => self.handle_direct(cx, message).await,
            PythonActorDispatchMode::Queue { sender, .. } => {
                let sender = sender.clone();
                self.handle_queue(cx, sender, message).await
            }
        }
    }
}

impl PythonActor {
    /// Handle a message using direct dispatch (current behavior).
    async fn handle_direct(
        &mut self,
        cx: &Context<'_, PythonActor>,
        message: PythonMessage,
    ) -> anyhow::Result<()> {
        let resolved = message.resolve_indirect_call(cx).await?;
        let endpoint = resolved.method.to_string();

        // Create a channel for signaling panics in async endpoints.
        // See [Panics in async endpoints].
        let (sender, receiver) = oneshot::channel();

        let future = monarch_with_gil(|py| -> Result<_, SerializablePyErr> {
            let inst = self.instance.get_or_insert_with(|| {
                let inst: crate::context::PyInstance = cx.into();
                inst.into_pyobject(py).unwrap().into()
            });

            let awaitable = self.actor.call_method(
                py,
                "handle",
                (
                    crate::context::PyContext::new(cx, inst.clone_ref(py)),
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

            let tl = self
                .task_locals
                .as_ref()
                .unwrap_or_else(|| shared_task_locals(py));

            pyo3_async_runtimes::into_future_with_locals(tl, awaitable.into_bound(py))
                .map_err(|err| err.into())
        })
        .await?;

        // Spawn a child actor to await the Python handler method.
        tokio::spawn(handle_async_endpoint_panic(
            cx.port(),
            PythonTask::new(future)?,
            receiver,
            cx.self_id().to_string(),
            endpoint,
        ));
        Ok(())
    }

    /// Handle a message using queue dispatch.
    /// Resolves the message on the Rust side and enqueues it for Python to process.
    async fn handle_queue(
        &mut self,
        cx: &Context<'_, PythonActor>,
        sender: pympsc::Sender,
        message: PythonMessage,
    ) -> anyhow::Result<()> {
        let resolved = message.resolve_indirect_call(cx).await?;

        let queued_msg = monarch_with_gil(|py| -> anyhow::Result<QueuedMessage> {
            let inst = self.instance.get_or_insert_with(|| {
                let inst: crate::context::PyInstance = cx.into();
                inst.into_pyobject(py).unwrap().into()
            });

            let py_context = crate::context::PyContext::new(cx, inst.clone_ref(py));
            let py_context_obj = Py::new(py, py_context)?;

            Ok(QueuedMessage {
                context: py_context_obj,
                method: resolved.method,
                bytes: resolved.bytes,
                local_state: resolved.local_state,
                response_port: resolved.response_port,
            })
        })
        .await?;

        sender
            .send(queued_msg)
            .map_err(|_| anyhow::anyhow!("failed to send message to queue"))?;

        Ok(())
    }
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
        // TODO: Consider routing supervision messages through the queue for Queue mode.
        // For now, supervision is always handled directly since it requires immediate response.

        monarch_with_gil(|py| {
            let inst = self.instance.get_or_insert_with(|| {
                let inst: crate::context::PyInstance = cx.into();
                inst.into_pyobject(py).unwrap().into()
            });
            // Compute display_name here since we can't call self.display_name() due to borrow.
            let display_name: Option<String> = inst.bind(py).str().ok().map(|s| s.to_string());
            let actor_bound = self.actor.bind(py);
            // The _Actor class always has a __supervise__ method, so this should
            // never happen.
            if !actor_bound.hasattr("__supervise__")? {
                return Err(anyhow::anyhow!(
                    "no __supervise__ method on {:?}",
                    actor_bound
                ));
            }
            let result = actor_bound.call_method(
                "__supervise__",
                (
                    crate::context::PyContext::new(cx, inst.clone_ref(py)),
                    PyMeshFailure::from(message.clone()),
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
                                display_name.clone(),
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
                            display_name,
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
        .await
    }
}

async fn handle_async_endpoint_panic(
    panic_sender: PortHandle<Signal>,
    task: PythonTask,
    side_channel: oneshot::Receiver<Py<PyAny>>,
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
            Ok(value) => {
                monarch_with_gil(|py| -> Option<SerializablePyErr> {
                    let err: PyErr = value
                        .downcast_bound::<PyBaseException>(py)
                        .unwrap()
                        .clone()
                        .into();
                    ENDPOINT_ACTOR_PANIC.add(1, attributes);
                    Some(err.into())
                })
                .await
            }
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
        static CLIENT: OnceLock<(Instance<()>, ActorHandle<()>)> = OnceLock::new();
        let client = &CLIENT
            .get_or_init(|| {
                get_proc_runtime()
                    .instance("async_endpoint_handler")
                    .unwrap()
            })
            .0;
        panic_sender
            .send(&client, Signal::Abort(panic.to_string()))
            .expect("Unable to send panic message");
    }

    // Record latency in microseconds
    let elapsed_micros = start_time.elapsed().as_micros() as f64;
    ENDPOINT_ACTOR_LATENCY_US_HISTOGRAM.record(elapsed_micros, attributes);
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

pub(crate) fn to_py_error<T>(e: T) -> PyErr
where
    T: Error,
{
    PyErr::new::<PyValueError, _>(e.to_string())
}

#[pymethods]
impl LocalPort {
    fn send(&mut self, obj: Py<PyAny>) -> PyResult<()> {
        let port = self.inner.take().expect("use local port once");
        port.send(self.instance.deref(), Ok(obj))
            .map_err(to_py_error)
    }
    fn exception(&mut self, e: Py<PyAny>) -> PyResult<()> {
        let port = self.inner.take().expect("use local port once");
        port.send(self.instance.deref(), Err(e))
            .map_err(to_py_error)
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PythonActorHandle>()?;
    hyperactor_mod.add_class::<PythonMessage>()?;
    hyperactor_mod.add_class::<PythonMessageKind>()?;
    hyperactor_mod.add_class::<MethodSpecifier>()?;
    hyperactor_mod.add_class::<UnflattenArg>()?;
    hyperactor_mod.add_class::<PanicFlag>()?;
    hyperactor_mod.add_class::<QueuedMessage>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hyperactor::PortRef;
    use hyperactor::accum::ReducerSpec;
    use hyperactor::accum::StreamingReducerOpts;
    use hyperactor::id;
    use hyperactor::message::ErasedUnbound;
    use hyperactor::message::Unbound;
    use hyperactor::reference::UnboundPort;
    use hyperactor_mesh::Error as MeshError;
    use hyperactor_mesh::Name;
    use hyperactor_mesh::host_mesh::mesh_agent::ProcState;
    use hyperactor_mesh::resource::Status;
    use hyperactor_mesh::resource::{self};
    use pyo3::PyTypeInfo;

    use super::*;
    use crate::actor::to_py_error;

    #[test]
    fn test_python_message_bind_unbind() {
        let reducer_spec = ReducerSpec {
            typehash: 123,
            builder_params: Some(wirevalue::Any::serialize(&"abcdefg12345".to_string()).unwrap()),
        };
        let port_ref = PortRef::<PythonMessage>::attest_reducible(
            id!(world[0].client[0][123]),
            Some(reducer_spec),
            StreamingReducerOpts::default(),
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

        pyo3::Python::initialize();
        monarch_with_gil_blocking(|py| {
            assert!(pyerr.get_type(py).is(PyValueError::type_object(py)));
            let py_msg = pyerr.value(py).to_string();

            // 1) Bridge preserves the exact message
            assert_eq!(py_msg, rust_msg);
            // 2) Contains the structured state and failure status
            assert!(py_msg.contains(", state: "));
            assert!(py_msg.contains("\"status\":{\"Failed\":\"boom\"}"));
            // 3) Starts with the expected prefix
            let expected_prefix = "error creating proc (host rank 0) on host mesh agent hello[0].actor[0]<hyperactor_mesh::host_mesh::mesh_agent::HostMeshAgent>";
            assert!(py_msg.starts_with(expected_prefix));
        });
    }
}
