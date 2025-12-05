/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::OnceCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::PortHandle;
use hyperactor::actor::ActorHandle;
use hyperactor::data::Serialized;
use hyperactor::forward;
use hyperactor::mailbox::OncePortHandle;
use hyperactor::mailbox::PortReceiver;
use hyperactor::proc::Proc;
use monarch_hyperactor::actor::PythonMessage;
use monarch_hyperactor::actor::PythonMessageKind;
use monarch_hyperactor::buffers::FrozenBuffer;
use monarch_hyperactor::local_state_broker::BrokerId;
use monarch_hyperactor::local_state_broker::LocalState;
use monarch_hyperactor::local_state_broker::LocalStateBrokerMessage;
use monarch_messages::controller::ControllerMessageClient;
use monarch_messages::controller::Seq;
use monarch_messages::controller::WorkerError;
use monarch_messages::worker::ActorCallParams;
use monarch_messages::worker::ActorMethodParams;
use monarch_messages::worker::ArgsKwargs;
use monarch_messages::worker::CallFunctionError;
use monarch_messages::worker::CallFunctionParams;
use monarch_messages::worker::SeqError;
use monarch_messages::worker::StreamRef;
use monarch_types::PyTree;
use monarch_types::SerializablePyErr;
use monarch_types::TryIntoPyObjectUnsafe;
use pyo3::prelude::*;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use torch_sys_cuda::cuda::Event;
use torch_sys_cuda::cuda::Stream;
use torch_sys2::CloneUnsafe;
use torch_sys2::CudaDevice;
use torch_sys2::TensorCell;
use torch_sys2::deep_clone;
use torch_sys2::factory_empty;
use torch_sys2::factory_zeros;
use tracing_subscriber::fmt::Subscriber;

use crate::ControllerActor;
use crate::DeviceMesh;
use crate::Factory;
use crate::Reduction;
use crate::Ref;
use crate::ResolvableFunction;
use crate::StreamCreationMode;
use crate::WireValue;
use crate::comm::CommMessage;
use crate::comm::CommMessageClient;
use crate::comm::NcclCommActor;

pub type TensorCellResult = Result<TensorCell, Arc<SeqError>>;

// These thread locals are accessed by the python runtime for debugging sessions.
thread_local! {
    pub static CONTROLLER_ACTOR_REF: OnceCell<ActorRef<ControllerActor>> = const { OnceCell::new() };
    pub static PROC: OnceCell<Proc> = const { OnceCell::new() };
    pub static ROOT_ACTOR_ID: OnceCell<ActorId> = const { OnceCell::new() };
}

fn pickle_python_result(
    py: Python<'_>,
    result: Bound<'_, PyAny>,
    worker_rank: usize,
) -> Result<PythonMessage, anyhow::Error> {
    let pickle = py
        .import("monarch._src.actor.actor_mesh")
        .unwrap()
        .getattr("_pickle")
        .unwrap();
    let data: FrozenBuffer = pickle
        .call1((result,))
        .map_err(|pyerr| anyhow::Error::from(SerializablePyErr::from(py, &pyerr)))?
        .extract()
        .unwrap();
    Ok(PythonMessage::new_from_buf(
        PythonMessageKind::Result {
            rank: Some(worker_rank),
        },
        data.inner,
    ))
}

#[derive(Debug)]
struct Recording {
    messages: Vec<StreamMessage>,
}

impl Recording {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum RecordingState {
    Defining {
        recording: Ref,
        // Set of borrow ids used to track proper borrow usage inside
        // a recording.
        defined_borrows: HashSet<u64>,
    },
    Running,
}

/// Messages handled by the stream. Generally these are stream-local versions of
/// [`crate::WorkerMessage`].
#[derive(Handler, HandleClient, Debug, Named)]
#[named(register = false)]
pub enum StreamMessage {
    CallFunction(
        CallFunctionParams,
        HashMap<Ref, DeviceMesh>,
        HashMap<Ref, (DeviceMesh, Vec<String>, Arc<ActorHandle<NcclCommActor>>)>,
    ),

    BorrowCreate {
        /// Id for the borrow.
        borrow: u64,
        /// Tensor to borrow.
        tensor: Ref,
        /// Port for sending the first use CUDA event + borrowed tensor to
        /// the borrower.
        first_use_sender: PortHandle<(Option<Event>, TensorCellResult)>,
    },

    BorrowFirstUse {
        /// Id for the borrow.
        borrow: u64,
        /// Ref for storing the borrowed tensor.
        result: Ref,
        /// Port for receiving the first use CUDA event + borrowed tensor from
        /// the provider stream.
        first_use_receiver: Arc<Mutex<PortReceiver<(Option<Event>, TensorCellResult)>>>,
    },

    BorrowLastUse {
        /// Id for the borrow.
        borrow: u64,
        /// Ref for the borrowed tensor.
        result: Ref,
        /// Port for sending the last use CUDA event and borrowed tensor.
        last_use_sender: PortHandle<(Option<Event>, TensorCellResult)>,
    },

    BorrowDrop {
        borrow: u64,
        /// Port for receiving the last use CUDA event and borrowed tensor.
        last_use_receiver: Arc<Mutex<PortReceiver<(Option<Event>, TensorCellResult)>>>,
    },

    DeleteRefs(Vec<Ref>),

    RequestStatus(#[reply] OncePortHandle<()>),

    InitComm(ActorHandle<NcclCommActor>),

    Reduce {
        comm: Arc<ActorHandle<NcclCommActor>>,
        dim_size: i64,
        result: Ref,
        local_tensor: Ref,
        factory: Factory,
        reduction: Reduction,
        scatter: bool,
        in_place: bool,
        out: Option<Ref>,
    },

    SendTensor {
        result: Ref,
        from_rank: Option<usize>,
        to_rank: Option<usize>,
        tensor: Ref,
        factory: Factory,
        comm: Arc<ActorHandle<NcclCommActor>>,
    },

    SendValue {
        seq: Seq,
        worker_actor_id: ActorId,
        mutates: Vec<Ref>,
        function: Option<ResolvableFunction>,
        args_kwargs: ArgsKwargs,
        device_meshes: HashMap<Ref, DeviceMesh>,
    },

    DefineRecording {
        recording: Ref,
    },

    FinalizeRecording {
        recording: Ref,
    },

    CallRecording {
        seq: Seq,
        recording: Ref,
        results: Vec<Ref>,
        actuals: Vec<Ref>,
    },

    RecordingFormal {
        result: Ref,
        argument_index: usize,
    },

    RecordingResult {
        result: Ref,
        output_index: usize,
    },

    SetRefUnitTestsOnly(Ref, WireValue),

    SetTensorRefUnitTestsOnly(Ref, TensorCellResult),

    GetRefUnitTestsOnly(
        Ref, // value
        #[reply] OncePortHandle<Option<Result<WireValue, String>>>,
    ),

    GetTensorRefUnitTestsOnly(Ref, #[reply] OncePortHandle<Option<TensorCellResult>>),

    SendResultOfActorCall(ActorCallParams),
    CallActorMethod(ActorMethodParams),
}

impl StreamMessage {
    fn clone_for_recording(&self) -> Self {
        match self {
            StreamMessage::RecordingFormal {
                result,
                argument_index,
            } => StreamMessage::RecordingFormal {
                result: *result,
                argument_index: *argument_index,
            },
            StreamMessage::RecordingResult {
                result,
                output_index,
            } => StreamMessage::RecordingResult {
                result: *result,
                output_index: *output_index,
            },
            StreamMessage::DeleteRefs(refs) => StreamMessage::DeleteRefs(refs.clone()),
            StreamMessage::CallFunction(params, device_meshes, remote_process_groups) => {
                StreamMessage::CallFunction(
                    params.clone(),
                    device_meshes.clone(),
                    remote_process_groups.clone(),
                )
            }
            StreamMessage::BorrowCreate {
                borrow,
                tensor,
                first_use_sender,
            } => StreamMessage::BorrowCreate {
                borrow: *borrow,
                tensor: *tensor,
                first_use_sender: first_use_sender.clone(),
            },
            StreamMessage::BorrowFirstUse {
                borrow,
                result,
                first_use_receiver,
            } => StreamMessage::BorrowFirstUse {
                borrow: *borrow,
                result: *result,
                first_use_receiver: first_use_receiver.clone(),
            },
            StreamMessage::BorrowLastUse {
                borrow,
                result,
                last_use_sender,
            } => StreamMessage::BorrowLastUse {
                borrow: *borrow,
                result: *result,
                last_use_sender: last_use_sender.clone(),
            },
            StreamMessage::BorrowDrop {
                borrow,
                last_use_receiver,
            } => StreamMessage::BorrowDrop {
                borrow: *borrow,
                last_use_receiver: last_use_receiver.clone(),
            },
            StreamMessage::Reduce {
                comm,
                dim_size,
                result,
                local_tensor,
                factory,
                reduction,
                scatter,
                in_place,
                out,
            } => StreamMessage::Reduce {
                comm: comm.clone(),
                dim_size: *dim_size,
                result: *result,
                local_tensor: *local_tensor,
                factory: factory.clone(),
                reduction: reduction.clone(),
                scatter: *scatter,
                in_place: *in_place,
                out: out.clone(),
            },
            StreamMessage::SendTensor {
                result,
                from_rank,
                to_rank,
                tensor,
                factory,
                comm,
            } => StreamMessage::SendTensor {
                result: *result,
                from_rank: *from_rank,
                to_rank: *to_rank,
                tensor: *tensor,
                factory: factory.clone(),
                comm: comm.clone(),
            },
            other => panic!(
                "StreamMessage variant not supported in recording: {:?}",
                other
            ),
        }
    }

    // Get the set of refs that this message defines.
    fn get_defined_refs(&self) -> HashSet<Ref> {
        match self {
            StreamMessage::RecordingFormal { result, .. } => HashSet::from([*result]),
            StreamMessage::CallFunction(params, ..) => {
                params.results.iter().filter_map(|&ref_| ref_).collect()
            }
            StreamMessage::BorrowFirstUse { result, .. } => HashSet::from([*result]),
            StreamMessage::Reduce { result, .. } => HashSet::from([*result]),
            StreamMessage::SendTensor {
                result, from_rank, ..
            } => {
                if from_rank.is_some() {
                    HashSet::from([*result])
                } else {
                    HashSet::new()
                }
            }
            // TODO(slurye): Add SendValue eventually.
            _ => HashSet::new(),
        }
    }

    // Get the set of refs that this message mutates.
    fn get_mutated_refs(&self) -> HashSet<Ref> {
        match self {
            StreamMessage::CallFunction(params, ..) => HashSet::from_iter(params.mutates.clone()),
            StreamMessage::Reduce {
                out,
                in_place,
                local_tensor,
                ..
            } => {
                if *in_place {
                    HashSet::from([*local_tensor])
                } else if let Some(out) = out {
                    HashSet::from([*out])
                } else {
                    HashSet::new()
                }
            }
            // TODO(slurye): Add SendValue eventually.
            _ => HashSet::new(),
        }
    }
}

/// A stream represents a linear sequence of execution. Operations on different
/// streams can execute concurrently.
///
/// For CUDA operators, streams will invoke the corresponding stream management
/// APIs to perform synchronization.
///
/// For CPU operators, streams will just execute synchronously on their own OS
/// thread.
#[derive(Debug)]
pub struct StreamActor {
    _world_size: usize,
    rank: usize,
    /// Mapping of refs in the controller environment to TensorIndex in this
    /// stream's local environment.
    // TODO(agallagher): Use `ValueError` as the error type.
    env: HashMap<Ref, Result<PyObject, Arc<SeqError>>>,
    /// How to create the stream.
    creation_mode: StreamCreationMode,
    /// CUDA stream that this actor will enqueue operations on. None if "device"
    /// is not a CUDA device.
    /// NOTE: We lazily create the stream, so that we do it from the dedicated
    /// Stream OS thread as, otherwise, we see deadlocks when done from
    /// unexpected threads.
    cuda_stream: OnceLock<Option<Stream>>,
    /// Device this stream should be scheduled on.
    device: Option<CudaDevice>,
    /// Communicator for this stream. Optional as we lazily initialize it.
    comm: Option<ActorHandle<NcclCommActor>>,
    /// Actor ref of the controller that created this stream.
    controller_actor: ActorRef<ControllerActor>,
    remote_process_groups: HashMap<Ref, PyObject>,
    recordings: HashMap<Ref, Recording>,
    active_recording: Option<RecordingState>,
    respond_with_python_message: bool,
    last_seq_error: Option<Arc<SeqError>>,
}

/// Parameters for creating a [`Stream`].
#[derive(Debug, Clone)]
pub struct StreamParams {
    pub world_size: usize,
    pub rank: usize,
    /// Controls how the underlying CUDA stream is created.
    pub creation_mode: StreamCreationMode,
    /// Id of this stream in the worker actor's stream table.
    pub id: StreamRef,
    /// Device this stream should be scheduled on. If none, don't do stream
    /// synchronization.
    pub device: Option<CudaDevice>,
    /// Actor ref of the controller that created this stream.
    pub controller_actor: ActorRef<ControllerActor>,
    pub respond_with_python_message: bool,
}

impl StreamActor {
    pub fn new(
        StreamParams {
            world_size,
            rank,
            id: _,
            device,
            controller_actor,
            creation_mode,
            respond_with_python_message,
        }: StreamParams,
    ) -> Self {
        Self {
            _world_size: world_size,
            rank,
            env: HashMap::new(),
            creation_mode,
            cuda_stream: OnceLock::new(),
            device,
            comm: None,
            controller_actor,
            remote_process_groups: HashMap::new(),
            recordings: HashMap::new(),
            active_recording: None,
            respond_with_python_message,
            last_seq_error: None,
        }
    }
}

#[async_trait]
impl Actor for StreamActor {
    async fn init(&mut self, cx: &Instance<Self>) -> Result<()> {
        // These thread locals are exposed via python functions, so we need to set them in the
        // same thread that python will run in. That means we need to initialize them here in
        // StreamActor::init instead of in StreamActor::new.
        CONTROLLER_ACTOR_REF.with(|controller_actor_ref| {
            controller_actor_ref.set(self.controller_actor.clone()).ok()
        });
        PROC.with(|proc| proc.set(cx.proc().clone()).ok());
        ROOT_ACTOR_ID.with(|root_actor_id| {
            root_actor_id
                .set(ActorId::root(
                    cx.self_id().proc_id().clone(),
                    cx.self_id().name().to_string(),
                ))
                .ok()
        });
        // Set the current stream for this actor thread.
        if let Some(stream) = self.cuda_stream() {
            Stream::set_current_stream(stream);
        }
        Ok(())
    }

    /// Specialize spawn_server_task for StreamActor, because we want to run the stream on a
    /// dedicated OS thread. This is because:
    ///   - Streams do expensive blocking CPU operations (like calling CPU kernels).
    ///   - Torch/CUDA make use of thread-local state, so moving tasks across
    ///     threads is problematic.
    fn spawn_server_task<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (join_tx, join_rx) = tokio::sync::oneshot::channel();
        // It is important that we spawn a standalone thread for the work here,
        // as opposed to using `spawn_blocking` to spawn a tokio-managed thread.
        // This is because the worker stream may call uninterruptible FFI code
        // that can deadlock (CUDA, NCCL).
        // If we use a tokio-managed blocking thread, then runtime teardown will
        // try to wait for tasks on that thread to reach an await point, and
        // hang forever.
        let builder = std::thread::Builder::new().name("worker-stream".to_string());
        let _thread_handle = builder.spawn(move || {
            // Spawn a new thread with a single-threaded tokio runtime to run the
            // actor loop.  We avoid the current-threaded runtime, so that we can
            // use `block_in_place` for nested async-to-sync-to-async flows.
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let result = rt.block_on(async {
                tokio::task::block_in_place(|| {
                    // Allow e.g. destructing py objects on this thread, which
                    // can happen at shutdown when the a stream actors env map
                    // for rvalues is dropped (e.g. P1673311499).
                    // https://github.com/PyO3/pyo3/discussions/3499
                    Python::with_gil(|py| {
                        py.allow_threads(|| {
                            let result = Handle::current().block_on(future);
                            if join_tx.send(result).is_err() {
                                panic!("could not send join result")
                            }
                        })
                    })
                })
            });
            rt.shutdown_timeout(Duration::from_weeks(1));
            result
        });

        // In order to bridge the synchronous join handle with the async world,
        // smuggle the result through a channel.
        tokio::spawn(async move { join_rx.await.unwrap() })
    }
}

/// The arguments we accept as inputs to Python function calls.
#[derive(Debug)]
enum PyArg {
    PyObject(PyObject),
}

/// Serialize into a `PyObject`.
impl<'py> TryIntoPyObjectUnsafe<'py, PyAny> for &PyArg {
    unsafe fn try_to_object_unsafe(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            PyArg::PyObject(obj) => Ok(obj.clone_ref(py).into_bound(py)),
        }
    }
}

impl StreamActor {
    fn tensor_to_pyobject(tensor_cell: TensorCell) -> PyObject {
        Python::with_gil(|py| {
            // SAFETY: Cloning a tensor was unsafe because we were tracking their references like
            // Rust objects (single mutable reference or many immutable references). We are
            // removing this functionality in upcoming patches, so we use the unsafe version here
            // until that happens.
            let tensor = unsafe {
                // Get the owned tensor by calling clone_unsafe on the reference
                tensor_cell.get_unchecked().clone_unsafe()
            };
            tensor.into_pyobject(py).unwrap().unbind()
        })
    }

    /// Extract a TensorCell from a PyObject.
    /// SAFETY: Uses new to create the TensorCell. Caller must ensure the PyObject
    /// contains a valid tensor.
    fn pyobject_to_tensor(py: Python<'_>, pyobj: &PyObject) -> PyResult<TensorCell> {
        use torch_sys2::Tensor;
        let tensor = pyobj.bind(py).extract::<Tensor>()?;
        // Create a new TensorCell from the extracted tensor
        Ok(TensorCell::new(tensor))
    }

    fn cuda_stream(&self) -> Option<&Stream> {
        self.cuda_stream
            .get_or_init(|| {
                self.device.map(|device| match self.creation_mode {
                    StreamCreationMode::UseDefaultStream => {
                        Stream::get_current_stream_on_device(device)
                    }
                    StreamCreationMode::CreateNewStream => Stream::new_with_device(device),
                })
            })
            .as_ref()
    }

    fn ref_to_pyobject(&self, ref_: &Ref) -> Result<PyObject, CallFunctionError> {
        let pyobject = self
            .env
            .get(ref_)
            .ok_or_else(|| CallFunctionError::RefNotFound(*ref_))?;
        match pyobject {
            Ok(val) => Ok(val.clone()),
            Err(err) => Err(CallFunctionError::DependentError(err.clone())),
        }
    }

    async fn report_seq_error(
        &mut self,
        cx: &Context<'_, Self>,
        seq: Seq,
        error: CallFunctionError,
    ) -> Result<Arc<SeqError>, anyhow::Error> {
        match error {
            CallFunctionError::DependentError(root) => Ok(root),
            CallFunctionError::Error(e) => {
                if self.active_recording.is_none() {
                    let worker_error = WorkerError {
                        backtrace: format!("{e}"),
                        worker_actor_id: cx.self_id().clone(),
                    };
                    tracing::info!("Propagating remote function error to client: {worker_error}");
                    self.controller_actor
                        .remote_function_failed(cx, seq, worker_error)
                        .await?
                }
                let err = Arc::new(SeqError { seq, error: e });
                self.last_seq_error = Some(err.clone());
                Ok(err)
            }
        }
    }

    async fn try_define<F>(
        &mut self,
        cx: &Context<'_, Self>,
        seq: Seq,
        result_refs: Vec<Option<Ref>>,
        mutates: &Vec<Ref>,
        f: F,
    ) -> Result<()>
    where
        F: AsyncFnOnce(&mut Self) -> Result<Vec<PyObject>, CallFunctionError>,
    {
        let actual_results = f(self).await;
        // Check if the expected number of returns is correct, otherwise convert
        // into an error.
        let op_results = actual_results.and_then(|actual_results| {
            if result_refs.len() == actual_results.len() {
                Ok(actual_results
                    .into_iter()
                    .zip(result_refs.iter())
                    .filter_map(|(result, ref_)| ref_.map(|ref_| (ref_, result)))
                    .collect::<Vec<(Ref, PyObject)>>())
            } else {
                Err(CallFunctionError::UnexpectedNumberOfReturns(
                    result_refs.len(),
                    actual_results.len(),
                ))
            }
        });

        // Propagate the results (either the actual values or an error) to the
        // right entries in the global env mapping.
        match op_results {
            Ok(op_results) => {
                for (ref_, pyobject) in op_results.into_iter() {
                    let prev = self.env.insert(ref_, Ok(pyobject));
                    assert!(prev.is_none(), "Duplicate write to reference: {:?}", ref_);
                }
            }
            Err(err) => {
                let err = self.report_seq_error(cx, seq, err).await?;
                for ref_ in result_refs {
                    match ref_ {
                        Some(ref_) => {
                            let prev = self.env.insert(ref_, Err(err.clone()));
                            assert!(prev.is_none(), "Duplicate write to reference: {:?}", ref_);
                        }
                        None => {}
                    }
                }
                for ref_ in mutates {
                    self.env.insert(*ref_, Err(err.clone()));
                }
            }
        }
        Ok(())
    }

    fn call_python_fn<'py>(
        &mut self,
        py: Python<'py>,
        _cx: &Context<Self>,
        function: Option<ResolvableFunction>,
        args_kwargs: ArgsKwargs,
        _mutates: &[Ref],
        device_meshes: HashMap<Ref, DeviceMesh>,
        remote_process_groups: HashMap<
            Ref,
            (DeviceMesh, Vec<String>, Arc<ActorHandle<NcclCommActor>>),
        >,
    ) -> Result<Bound<'py, PyAny>, CallFunctionError> {
        let (args_tuple, kwargs_dict) = args_kwargs
            .to_python(py)
            .map_err(|e| CallFunctionError::Error(e.into()))?;
        let function = function
            .map(|function| {
                function.resolve(py).map_err(|e| {
                    CallFunctionError::InvalidRemoteFunction(format!(
                        "failed to resolve function {}: {}",
                        function,
                        SerializablePyErr::from(py, &e)
                    ))
                })
            })
            .transpose()?;

        let remote_process_groups = remote_process_groups
            .into_iter()
            .map(|(gref, (_mesh, _dims, _comm))| {
                let group = match self.remote_process_groups.entry(gref) {
                    Entry::Occupied(ent) => ent.get().clone_ref(py),
                    Entry::Vacant(_ent) => {
                        panic!("no longer implemented");
                    }
                };
                PyResult::Ok((gref, group))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(SerializablePyErr::from_fn(py))?;

        let resolve = |val: Bound<'py, PyAny>| {
            val.extract::<PyTree<PyObject>>()
                .map_err(SerializablePyErr::from_fn(py))?
                .try_into_map(|obj| {
                    Ok(if let Ok(ref_) = Ref::from_py_object(obj.bind(py)) {
                        if let Some(mesh) = device_meshes.get(&ref_) {
                            PyArg::PyObject(
                                Py::new(py, mesh.clone())
                                    .map_err(SerializablePyErr::from_fn(py))?
                                    .into(),
                            )
                        } else if let Some(pg) = remote_process_groups.get(&ref_) {
                            PyArg::PyObject(pg.clone_ref(py))
                        } else {
                            let pyobj = self.ref_to_pyobject(&ref_)?;
                            PyArg::PyObject(pyobj)
                        }
                    } else {
                        PyArg::PyObject(obj)
                    })
                })
        };

        // Resolve args and kwargs
        let py_args: Vec<PyTree<PyArg>> = args_tuple
            .iter()
            .map(|item| resolve(item))
            .collect::<Result<_, CallFunctionError>>()?;

        let py_kwargs: HashMap<String, PyTree<PyArg>> = kwargs_dict
            .iter()
            .map(|(k, v)| {
                let key = k
                    .extract::<String>()
                    .map_err(SerializablePyErr::from_fn(py))?;
                let value = resolve(v)?;
                Ok((key, value))
            })
            .collect::<Result<_, CallFunctionError>>()?;

        // Call function.
        // Use custom subscriber to route Worker messages to stdout.
        let scoped_subscriber = Subscriber::builder().with_writer(std::io::stdout).finish();
        let result: Bound<'_, PyAny> =
            tracing::subscriber::with_default(scoped_subscriber, || {
                // TODO(agallagher): The args/kwargs conversion traits generate
                // the appropriate types here, but they get casted to `PyAny`.
                // It'd be nice to make `TryToPyObjectUnsafe` take a template
                // arg for the converted py object to avoid this downcast.
                // SAFETY: Tensor operations were unsafe because we were tracking their references
                // like Rust objects (single mutable reference or many immutable references). We are
                // removing this functionality in upcoming patches, so we use the unsafe version here
                // until that happens.
                let args = unsafe { py_args.try_to_object_unsafe(py) }
                    .map_err(SerializablePyErr::from_fn(py))?;
                // SAFETY: Same as above - reference tracking functionality is being removed.
                let kwargs = &unsafe { py_kwargs.try_to_object_unsafe(py) }
                    .map_err(SerializablePyErr::from_fn(py))?;

                if let Some(function) = function {
                    function
                        .call(args, Some(kwargs))
                        .map_err(SerializablePyErr::from_fn(py))
                } else {
                    Ok(args.get_item(0).unwrap())
                }
            })?;
        Ok(result)
    }

    fn call_python_fn_pytree(
        &mut self,
        cx: &hyperactor::Context<Self>,
        function: ResolvableFunction,
        args_kwargs: ArgsKwargs,
        mutates: &[Ref],
        device_meshes: HashMap<Ref, DeviceMesh>,
        remote_process_groups: HashMap<
            Ref,
            (DeviceMesh, Vec<String>, Arc<ActorHandle<NcclCommActor>>),
        >,
    ) -> Result<PyTree<PyObject>, CallFunctionError> {
        Python::with_gil(|py| {
            let result = self.call_python_fn(
                py,
                cx,
                Some(function),
                args_kwargs,
                mutates,
                device_meshes,
                remote_process_groups,
            )?;
            Ok(PyTree::<PyObject>::extract_bound(&result)
                .map_err(SerializablePyErr::from_fn(py))?)
        })
    }
    /// Retrieve `ref_` or create a fake value with the provided factory if it
    /// is an error. We use this for collective calls, where even if there was
    /// an upstream failure, we still have participate in the collective to
    /// avoid deadlocking the other ranks. It's okay to just put a nonsense
    /// value here of the correct shape; the controller will have been notified
    /// of the upstream failure and will know to ignore everything dependent on
    /// it.
    fn get_or_fake_on_err(&self, ref_: Ref, factory: &Factory) -> Result<TensorCell> {
        let pyobject = self
            .env
            .get(&ref_)
            .ok_or_else(|| anyhow!("tensor not found in stream: {ref_:#?}"))?;

        match pyobject {
            Ok(val) => Python::with_gil(|py| {
                Self::pyobject_to_tensor(py, val)
                    .map_err(|pyerr| anyhow::Error::from(SerializablePyErr::from(py, &pyerr)))
            }),
            Err(_) => {
                let t = factory_zeros(&factory.size, factory.dtype, factory.layout, factory.device);
                Ok(TensorCell::new(t))
            }
        }
    }

    fn get_defining_recording(&mut self) -> Option<(&mut Recording, &mut HashSet<u64>)> {
        self.active_recording
            .as_mut()
            .and_then(|state| match state {
                RecordingState::Defining {
                    recording,
                    defined_borrows,
                } => {
                    match self.recordings.get_mut(recording) {
                        Some(recording) => Some((recording, defined_borrows)),
                        // Panic, because this would be a logic error in the program.
                        None => panic!("recording not found: {:?}", recording),
                    }
                }
                RecordingState::Running => None,
            })
    }

    fn get_first_error(&self, refs: &[Ref]) -> Result<Option<Arc<SeqError>>> {
        for ref_ in refs {
            let rvalue_or_err = self
                .env
                .get(ref_)
                .ok_or_else(|| anyhow!("tensor not found in stream: {ref_:#?}"))?;
            if let Err(err) = rvalue_or_err {
                return Ok(Some(err.clone()));
            }
        }
        Ok(None)
    }
    async fn send_value_python_message(
        &mut self,
        cx: &hyperactor::Context<'_, Self>,
        seq: Seq,
        mutates: Vec<Ref>,
        function: Option<ResolvableFunction>,
        args_kwargs: ArgsKwargs,
        device_meshes: HashMap<Ref, DeviceMesh>,
    ) -> Result<()> {
        let rank = self.rank;
        self.try_define(cx, seq, vec![], &vec![], async |self_| {
            let python_message =
                Python::with_gil(|py| -> Result<PythonMessage, CallFunctionError> {
                    let python_result = tokio::task::block_in_place(|| {
                        self_.call_python_fn(
                            py,
                            cx,
                            function,
                            args_kwargs,
                            &mutates,
                            device_meshes,
                            HashMap::new(),
                        )
                    })?;
                    pickle_python_result(py, python_result, rank).map_err(CallFunctionError::Error)
                })?;
            let ser = Serialized::serialize(&python_message).unwrap();
            self_
                .controller_actor
                .fetch_result(cx, seq, Ok(ser))
                .await?;
            Ok(vec![])
        })
        .await
    }
    fn define_ref(&mut self, dest: Ref, src: Ref) -> Result<(), anyhow::Error> {
        let rvalue = self
            .env
            .get(&src)
            .ok_or_else(|| CallFunctionError::RefNotFound(src))?;
        self.env
            .insert(dest, Python::with_gil(|_py| rvalue.clone()));
        Ok(())
    }
    async fn call_actor(
        &mut self,
        cx: &Context<'_, Self>,
        params: ActorCallParams,
    ) -> Result<PyObject, CallFunctionError> {
        let local_state: Result<Vec<PyObject>> = Python::with_gil(|_py| {
            params
                .local_state
                .into_iter()
                .map(|elem| {
                    let pyobj = self.ref_to_pyobject(&elem)?;
                    Ok(pyobj.into_any())
                })
                .collect()
        });

        let (send, recv) = cx.open_once_port();
        let state = LocalState {
            response_port: send,
            state: local_state?,
        };
        let x: u64 = params.seq.into();
        let message = LocalStateBrokerMessage::Set(x as usize, state);

        let broker = BrokerId::new(params.broker_id).resolve(cx).unwrap();
        broker
            .send(message)
            .map_err(|e| CallFunctionError::Error(e.into()))?;
        let result = recv
            .recv()
            .await
            .map_err(|e| CallFunctionError::Error(e.into()))?;

        result.map_err(|pyerr| anyhow::Error::msg(pyerr.to_string()).into())
    }
}

#[async_trait]
#[forward(StreamMessage)]
impl StreamMessageHandler for StreamActor {
    async fn call_function(
        &mut self,
        cx: &Context<Self>,
        params: CallFunctionParams,
        device_meshes: HashMap<Ref, DeviceMesh>,
        remote_process_groups: HashMap<
            Ref,
            (DeviceMesh, Vec<String>, Arc<ActorHandle<NcclCommActor>>),
        >,
    ) -> Result<()> {
        if let Some((recording, _)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::CallFunction(
                params,
                device_meshes,
                remote_process_groups,
            ));
            return Ok(());
        }

        params.function.panic_if_requested();
        self.try_define(
            cx,
            params.seq,
            params.results,
            &params.mutates,
            async |self| {
                tokio::task::block_in_place(|| {
                    self.call_python_fn_pytree(
                        cx,
                        params.function,
                        params.args_kwargs,
                        &params.mutates,
                        device_meshes,
                        remote_process_groups,
                    )
                    .map(|results| results.into_leaves())
                })
            },
        )
        .await?;
        Ok(())
    }

    async fn borrow_create(
        &mut self,
        _cx: &Context<Self>,
        borrow: u64,
        tensor: Ref,
        first_use_sender: PortHandle<(Option<Event>, TensorCellResult)>,
    ) -> Result<()> {
        if let Some((recording, defined_borrows)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::BorrowCreate {
                borrow,
                tensor,
                first_use_sender,
            });
            ensure!(
                defined_borrows.insert(borrow),
                "duplicate borrow create in recording"
            );
            return Ok(());
        }

        let pyobj_result = self
            .env
            .get(&tensor)
            .ok_or_else(|| anyhow!("invalid reference for borrow_create: {:#?}", tensor))?;

        let result = match pyobj_result {
            Ok(pyobj) => Python::with_gil(|py| Ok(Self::pyobject_to_tensor(py, pyobj).unwrap())),
            Err(e) => Err(e.clone()),
        };

        let event = self.cuda_stream().map(|stream| stream.record_event(None));
        first_use_sender.send((event, result)).map_err(|err| {
            anyhow!(
                "failed sending first use event for borrow {:?}: {:?}",
                borrow,
                err
            )
        })
    }

    async fn borrow_first_use(
        &mut self,
        _cx: &Context<Self>,
        borrow: u64,
        result: Ref,
        first_use_receiver: Arc<Mutex<PortReceiver<(Option<Event>, TensorCellResult)>>>,
    ) -> Result<()> {
        if let Some((recording, _)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::BorrowFirstUse {
                borrow,
                result,
                first_use_receiver: first_use_receiver.clone(),
            });
            return Ok(());
        }

        let (first_use_event, cell) =
            first_use_receiver
                .lock()
                .await
                .recv()
                .await
                .map_err(|err| {
                    anyhow!(
                        "failed receiving first use event for borrow {:?}: {:?}",
                        borrow,
                        err
                    )
                })?;

        if let Some(stream) = self.cuda_stream() {
            stream.wait_event(
                &mut first_use_event.expect("sent borrow to CUDA stream, expected a CUDA event"),
            );
        }
        match cell {
            Ok(cell) => {
                let pyobj = Self::tensor_to_pyobject(cell);
                self.env.insert(result, Ok(pyobj));
            }
            Err(err) => {
                self.env.insert(result, Err(err.clone()));
            }
        }
        Ok(())
    }

    async fn borrow_last_use(
        &mut self,
        _cx: &Context<Self>,
        borrow: u64,
        result: Ref,
        last_use_sender: PortHandle<(Option<Event>, TensorCellResult)>,
    ) -> Result<()> {
        if let Some((recording, _)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::BorrowLastUse {
                borrow,
                result,
                last_use_sender,
            });
            return Ok(());
        }

        let event = self.cuda_stream().map(|stream| stream.record_event(None));
        let pyobj_or_err = self.env.remove(&result).ok_or(anyhow!(
            "Invalid reference for borrow_last_use: {result:#?}"
        ))?;
        let tensor = match pyobj_or_err {
            Ok(pyobj) => Ok(Python::with_gil(|py| {
                Self::pyobject_to_tensor(py, &pyobj).unwrap()
            })),
            Err(e) => Err(e),
        };

        last_use_sender.send((event, tensor)).map_err(|err| {
            anyhow!(
                "failed sending last use event for borrow {:?}: {:?}",
                borrow,
                err
            )
        })
    }

    async fn borrow_drop(
        &mut self,
        _cx: &Context<Self>,
        borrow: u64,
        last_use_receiver: Arc<Mutex<PortReceiver<(Option<Event>, TensorCellResult)>>>,
    ) -> Result<()> {
        if let Some((recording, defined_borrows)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::BorrowDrop {
                borrow,
                last_use_receiver: last_use_receiver.clone(),
            });
            ensure!(
                defined_borrows.remove(&borrow),
                "borrow drop for borrow not defined in recording"
            );
            return Ok(());
        }

        // The borrowed cell isn't used directly, but we still want to receive it here
        // so that the underlying tensor isn't dropped until after we synchronize the
        // CUDA streams.
        let (last_use_event, _cell) =
            last_use_receiver.lock().await.recv().await.map_err(|err| {
                anyhow!(
                    "failed receiving last use event for borrow {:?}: {:?}",
                    borrow,
                    err
                )
            })?;

        if let Some(stream) = self.cuda_stream() {
            stream.wait_event(
                &mut last_use_event.expect("sent borrow to CUDA stream, expected a CUDA event"),
            );
        }
        // let the cell drop.
        Ok(())
    }

    async fn delete_refs(&mut self, _cx: &Context<Self>, refs: Vec<Ref>) -> Result<()> {
        if let Some((recording, _)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::DeleteRefs(refs));
            return Ok(());
        }

        for ref_ in refs.iter() {
            self.env.remove(ref_);
        }
        Ok(())
    }

    async fn request_status(&mut self, _cx: &Context<Self>) -> Result<()> {
        if self.get_defining_recording().is_some() {
            bail!("request_status not allowed in recording");
        }

        Ok(())
    }

    async fn init_comm(
        &mut self,
        _cx: &Context<Self>,
        comm: ActorHandle<NcclCommActor>,
    ) -> Result<()> {
        if self.get_defining_recording().is_some() {
            bail!("init_comm not allowed in recording");
        }

        self.comm = Some(comm);
        Ok(())
    }

    async fn reduce(
        &mut self,
        cx: &Context<Self>,
        comm: Arc<ActorHandle<NcclCommActor>>,
        dim_size: i64,
        result: Ref,
        local_tensor: Ref,
        factory: Factory,
        reduction: Reduction,
        scatter: bool,
        in_place: bool,
        out: Option<Ref>,
    ) -> Result<()> {
        if let Some((recording, _)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::Reduce {
                comm,
                dim_size,
                result,
                local_tensor,
                factory,
                reduction,
                scatter,
                in_place,
                out,
            });
            return Ok(());
        }

        let stream = self
            .cuda_stream()
            .expect("reductions not yet supported for non-CUDA workers")
            .clone();
        let input_cell = self.get_or_fake_on_err(local_tensor, &factory)?;
        let out_cell = out
            .map(|out| self.get_or_fake_on_err(out, &factory))
            .transpose()?;
        let output_cell = match reduction {
            Reduction::Stack => {
                if scatter {
                    let output_cell = if in_place {
                        input_cell.clone()
                    } else {
                        out_cell.unwrap_or({
                            let borrow = input_cell.try_borrow().map_err(|e| anyhow!("{e:?}"))?;
                            let cloned = deep_clone(&borrow);
                            TensorCell::new(cloned)
                        })
                    };
                    comm.all_to_all_single(cx, output_cell.clone(), input_cell, stream)
                        .await?;
                    output_cell
                } else {
                    ensure!(
                        !in_place,
                        "in-place, non-scatter not supported for stack reduce"
                    );

                    let output_cell = out_cell.unwrap_or({
                        // In Python, this would be [dim_size, *factory.sizes]
                        let sizes = [&[dim_size][..], &factory.size[..]].concat();
                        let output =
                            factory_empty(&sizes, factory.dtype, factory.layout, factory.device);
                        TensorCell::new(output)
                    });

                    comm.all_gather_into_tensor(cx, output_cell.clone(), input_cell, stream)
                        .await?;
                    output_cell
                }
            }
            Reduction::ReduceOp(op) => {
                if scatter {
                    ensure!(!in_place, "in-place, scatter not supported for reduce");

                    let output_cell = out_cell.unwrap_or({
                        let output = factory_empty(
                            &factory.size[1..],
                            factory.dtype,
                            factory.layout,
                            factory.device,
                        );
                        TensorCell::new(output)
                    });
                    comm.reduce_scatter_tensor(cx, output_cell.clone(), input_cell, op, stream)
                        .await?;
                    output_cell
                } else {
                    let output_cell = if in_place {
                        input_cell.clone()
                    } else {
                        out_cell.map_or(
                            {
                                let borrow =
                                    input_cell.try_borrow().map_err(|e| anyhow!("{e:?}"))?;
                                let cloned = deep_clone(&borrow);
                                Ok(TensorCell::new(cloned))
                            },
                            |out_cell| -> Result<_, anyhow::Error> {
                                let mut out_borrow =
                                    out_cell.try_borrow_mut().map_err(|e| anyhow!("{e:?}"))?;
                                let in_borrow =
                                    input_cell.try_borrow().map_err(|e| anyhow!("{e:?}"))?;
                                out_borrow.copy_(&in_borrow);
                                drop(out_borrow);
                                Ok(out_cell)
                            },
                        )?
                    };

                    comm.all_reduce(cx, output_cell.clone(), op, stream).await?;
                    output_cell
                }
            }
        };

        let pyobj = Self::tensor_to_pyobject(output_cell);
        self.env.insert(result, Ok(pyobj));
        Ok(())
    }

    async fn send_tensor(
        &mut self,
        cx: &Context<Self>,
        result: Ref,
        from_rank: Option<usize>,
        to_rank: Option<usize>,
        tensor: Ref,
        factory: Factory,
        comm: Arc<ActorHandle<NcclCommActor>>,
    ) -> Result<()> {
        if let Some((recording, _)) = self.get_defining_recording() {
            recording.messages.push(StreamMessage::SendTensor {
                result,
                from_rank,
                to_rank,
                tensor,
                factory,
                comm,
            });
            return Ok(());
        }

        if to_rank.is_none() && from_rank.is_none() {
            bail!("tried to send tensor without a to/from rank");
        }

        // Value is local, so we do not have to actually send it.
        if from_rank == to_rank {
            let input_cell: &std::result::Result<PyObject, Arc<SeqError>> =
                self.env
                    .get(&tensor)
                    .ok_or_else(|| anyhow!("tensor not found in stream: {tensor:#?}"))?;
            let output_cell: Result<PyObject, Arc<SeqError>> = match input_cell {
                Ok(pyobj) => {
                    Python::with_gil(|py| -> Result<PyObject, Arc<SeqError>> {
                        let input_tensor = Self::pyobject_to_tensor(py, pyobj).unwrap();
                        // We create a defensive copy here to prevent mutations on
                        // the input tensor from affecting output tensor.
                        // Should we copy if input ref == output ref?
                        // Should we support copy-on-write to avoid unnecessary copy?
                        let borrow = input_tensor.try_borrow().unwrap();
                        let cloned = deep_clone(&borrow);
                        let cloned_cell = TensorCell::new(cloned);
                        Ok(Self::tensor_to_pyobject(cloned_cell))
                    })
                }
                Err(err) => Err(err.clone()),
            };
            self.env.insert(result, output_cell);
            return Ok(());
        }

        let mut messages = Vec::new();

        if let Some(to_rank) = to_rank {
            let input_cell = self.get_or_fake_on_err(tensor, &factory)?;
            messages.push(CommMessage::Send(
                input_cell,
                to_rank.try_into().unwrap(),
                self.cuda_stream()
                    .expect("tried to send_tensor on non-cuda stream")
                    .clone(),
                cx.open_once_port().0,
            ));
        }

        if let Some(from_rank) = from_rank {
            let output_cell = TensorCell::new(factory_empty(
                &factory.size,
                factory.dtype,
                factory.layout,
                factory.device,
            ));
            messages.push(CommMessage::Recv(
                output_cell.clone(),
                from_rank.try_into().unwrap(),
                self.cuda_stream()
                    .expect("tried to send_tensor on non-cuda stream")
                    .clone(),
                cx.open_once_port().0,
            ));
            let pyobj = Self::tensor_to_pyobject(output_cell);
            self.env.insert(result, Ok(pyobj));
        }

        comm.group(
            cx,
            messages,
            self.cuda_stream()
                .expect("tried to send_tensor on non-cuda stream")
                .clone(),
        )
        .await?;
        Ok(())
    }

    async fn send_value(
        &mut self,
        cx: &Context<Self>,
        seq: Seq,
        worker_actor_id: ActorId,
        mutates: Vec<Ref>,
        function: Option<ResolvableFunction>,
        args_kwargs: ArgsKwargs,
        device_meshes: HashMap<Ref, DeviceMesh>,
    ) -> Result<()> {
        if self.respond_with_python_message {
            return self
                .send_value_python_message(cx, seq, mutates, function, args_kwargs, device_meshes)
                .await;
        }

        let result = (|| -> Result<PyTree<PyObject>, CallFunctionError> {
            if let Some(function) = function {
                // If a function was provided, use that to resolve the value.
                tokio::task::block_in_place(|| {
                    self.call_python_fn_pytree(
                        cx,
                        function,
                        args_kwargs,
                        &mutates,
                        device_meshes,
                        HashMap::new(),
                    )
                })
            } else {
                // If there's no function provided, there should be exactly one arg
                // and no kwargs.
                Python::with_gil(|py| {
                    let (args, kwargs) = args_kwargs
                        .to_python(py)
                        .map_err(|e| CallFunctionError::Error(e.into()))?;
                    match (args.len(), kwargs.len()) {
                        (1, 0) => {
                            let arg = args.get_item(0).map_err(SerializablePyErr::from_fn(py))?;
                            arg.extract::<PyTree<PyObject>>()
                                .map_err(SerializablePyErr::from_fn(py))?
                                .try_into_map(|obj| {
                                    let bound_obj = obj.bind(py);
                                    if let Ok(ref_) = Ref::from_py_object(bound_obj) {
                                        self.ref_to_pyobject(&ref_)
                                    } else {
                                        Ok(obj)
                                    }
                                })
                        }
                        _ => Err(CallFunctionError::TooManyArgsForValue(
                            format!("args with {} elements", args.len()),
                            format!("kwargs with {} elements", kwargs.len()),
                        )),
                    }
                })
            }
        })();

        let value = match result {
            Ok(pyobject) => Ok(pyobject),
            Err(err) => {
                let err = self.report_seq_error(cx, seq, err).await?;
                for ref_ in mutates {
                    self.env.insert(ref_, Err(err.clone()));
                }
                Err(WorkerError {
                    backtrace: format!("{:?}", err),
                    worker_actor_id,
                })
            }
        };

        // Actually send the value.
        // NOTE: respond_with_python_message is always true, so serialization is not needed
        // The controller will receive the value through send_value_python_message instead
        let result = match value {
            Ok(_value) => {
                // This code path is never executed since respond_with_python_message is true
                unreachable!(
                    "send_value should return early when respond_with_python_message is true"
                )
            }
            Err(e) => Err(e),
        };
        self.controller_actor.fetch_result(cx, seq, result).await?;

        Ok(())
    }

    async fn send_result_of_actor_call(
        &mut self,
        cx: &Context<Self>,
        params: ActorCallParams,
    ) -> anyhow::Result<()> {
        let seq = params.seq;
        let mutates = params.mutates.clone();
        self.try_define(cx, seq, vec![], &mutates, async |self| {
            let value = self.call_actor(cx, params).await?;
            let result =
                Python::with_gil(|py| pickle_python_result(py, value.into_bound(py), self.rank))?;
            let result = Serialized::serialize(&result).unwrap();
            self.controller_actor
                .fetch_result(cx, seq, Ok(result))
                .await?;
            Ok(vec![])
        })
        .await
    }

    async fn call_actor_method(
        &mut self,
        cx: &Context<Self>,
        params: ActorMethodParams,
    ) -> anyhow::Result<()> {
        let seq = params.call.seq;
        let mutates = params.call.mutates.clone();
        self.try_define(cx, seq, params.results, &mutates, async |self| {
            let result = self.call_actor(cx, params.call).await?;
            let result = Python::with_gil(|py| {
                PyTree::<PyObject>::extract_bound(&result.into_bound(py))
                    .map_err(SerializablePyErr::from_fn(py))
            })?;
            Ok(result.into_leaves())
        })
        .await
    }

    async fn define_recording(&mut self, _cx: &Context<Self>, recording: Ref) -> Result<()> {
        if self.active_recording.is_some() {
            bail!("different recording already active");
        }
        match self.recordings.entry(recording) {
            Entry::Occupied(_) => bail!("recording {:?} already defined", recording),
            Entry::Vacant(entry) => entry.insert(Recording::new()),
        };
        self.active_recording = Some(RecordingState::Defining {
            recording,
            defined_borrows: HashSet::new(),
        });
        Ok(())
    }

    async fn finalize_recording(&mut self, _cx: &Context<Self>, recording: Ref) -> Result<()> {
        match self.active_recording {
            Some(RecordingState::Defining {
                recording: active_recording,
                ref defined_borrows,
            }) if active_recording == recording => {
                ensure!(
                    defined_borrows.is_empty(),
                    "all borrows created within recording must be dropped within recording"
                );
                self.active_recording = None;
            }
            _ => bail!("cannot finalize recording that isn't active"),
        }
        Ok(())
    }

    async fn recording_formal(
        &mut self,
        _cx: &Context<Self>,
        result: Ref,
        argument_index: usize,
    ) -> Result<()> {
        match self.get_defining_recording() {
            Some((recording, _)) => {
                recording.messages.push(StreamMessage::RecordingFormal {
                    result,
                    argument_index,
                });
            }
            None => bail!("recording_formal called outside of recording"),
        };
        Ok(())
    }

    async fn recording_result(
        &mut self,
        _cx: &Context<Self>,
        result: Ref,
        output_index: usize,
    ) -> Result<()> {
        match self.get_defining_recording() {
            Some((recording, _)) => {
                recording.messages.push(StreamMessage::RecordingResult {
                    result,
                    output_index,
                });
            }
            None => bail!("recording_result called outside of recording"),
        };
        Ok(())
    }

    async fn call_recording(
        &mut self,
        cx: &Context<Self>,
        seq: Seq,
        recording: Ref,
        results: Vec<Ref>,
        actuals: Vec<Ref>,
    ) -> Result<()> {
        if self.active_recording.is_some() {
            bail!("cannot call recording while another recording is active");
        }

        let messages = match self.recordings.get(&recording) {
            Some(recording) => recording
                .messages
                .iter()
                .map(|message| message.clone_for_recording())
                .collect::<Vec<_>>(),
            None => bail!("recording {:?} not found", recording),
        };

        self.active_recording = Some(RecordingState::Running);

        // Global error for all messages in the recording. The first time a message
        // fails in the recording, we set the error. We then need to propagate this
        // error to all of the refs mutated by the entire recording, as well as the
        // result refs.
        let mut error: Option<Arc<SeqError>> = None;
        // The set of all refs defined by this recording (excluding "results"),
        // which we need to ensure are deleted when the recording is done executing.
        let mut all_defined_refs = HashSet::new();
        // The set of all refs mutated by this recording. If there is an error with
        // any message, all of these refs need to have the correct error set.
        let mut all_mutated_refs = HashSet::new();
        // Map from the result ref of a RecordingFormal message to the associated
        // actual ref from "actuals". We need to track this in order to properly
        // handle recordings that mutate refs contained in "actuals" -- every
        // message in the recording that interacts with the recording inputs will
        // interact with the formal ref rather than the actual ref.
        let mut formal_to_actual_refs = HashMap::new();
        // clear any pre-existing error messages before recording started
        self.last_seq_error = None;
        for message in messages.into_iter() {
            let defined_refs = message.get_defined_refs();
            all_defined_refs.extend(defined_refs.clone());

            let mutated_refs_with_formals = message.get_mutated_refs();
            all_mutated_refs.extend(mutated_refs_with_formals.iter().filter_map(|ref_| {
                match formal_to_actual_refs.get(ref_) {
                    Some(actual_ref) => Some(*actual_ref),
                    None => {
                        if all_defined_refs.contains(ref_) {
                            None
                        } else {
                            Some(*ref_)
                        }
                    }
                }
            }));

            match message {
                StreamMessage::RecordingFormal {
                    result: formal_ref,
                    argument_index,
                } => match actuals.get(argument_index) {
                    None => bail!("recording_formal called with too few arguments"),
                    Some(actual_ref) => {
                        formal_to_actual_refs.insert(formal_ref, *actual_ref);
                        self.define_ref(formal_ref, *actual_ref)?;
                    }
                },
                StreamMessage::RecordingResult {
                    result: result_ref,
                    output_index,
                } => match results.get(output_index) {
                    None => bail!("recording_result called with too few results"),
                    Some(actual_result_ref) => {
                        self.define_ref(*actual_result_ref, result_ref)?;
                    }
                },
                StreamMessage::DeleteRefs(ref refs) => {
                    for ref_ in refs {
                        all_defined_refs.remove(ref_);
                    }
                    StreamMessageHandler::handle(self, cx, message).await?;
                }
                StreamMessage::CallFunction { .. } if error.is_some() => {
                    // CallFunction is expensive. If the recording already failed, then
                    // just update the necessary refs with the error. Most of the other
                    // message types need to run regardless because there are other actors
                    // that expect the call to happen (e.g., all of the borrow messages,
                    // pipe send/recv, send_tensor, reduce, etc.).
                    let error = error.clone().unwrap();
                    for ref_ in defined_refs.iter().chain(mutated_refs_with_formals.iter()) {
                        self.env.insert(*ref_, Err(error.clone()));
                    }
                }
                StreamMessage::BorrowLastUse { ref result, .. } => {
                    all_defined_refs.remove(result);
                    StreamMessageHandler::handle(self, cx, message).await?;
                }
                StreamMessage::Reduce {
                    local_tensor,
                    ref out,
                    ..
                } => {
                    // Reduce doesn't propagate errors to the result ref, so we need
                    // to check for existing errors on the input tensors and set the
                    // recording's error if necessary.
                    if error.is_none() {
                        let inputs_to_check = [Some(local_tensor), out.clone()]
                            .iter()
                            .filter_map(|r| *r)
                            .collect::<Vec<_>>();
                        error = self.get_first_error(inputs_to_check.as_slice())?;
                    }
                    StreamMessageHandler::handle(self, cx, message).await?;
                }
                StreamMessage::SendTensor {
                    ref tensor,
                    ref to_rank,
                    ..
                } => {
                    // If this rank is sending a tensor (e.g., to_rank has a value),
                    // we need to check for existing errors on the input tensor, because
                    // the error is only propagated to the result ref when this rank
                    // is also receiving a tensor.
                    if to_rank.is_some() && error.is_none() {
                        error = self.get_first_error(&[*tensor])?;
                    }
                    StreamMessageHandler::handle(self, cx, message).await?;
                }
                _ => {
                    StreamMessageHandler::handle(self, cx, message).await?;
                }
            };

            // It's not entirely trivial to determine whether a message "failed" or not.
            // For example, the CallFunction message can return Ok(..) if there is an error
            // in the underlying function call. But in that case, we would still want to
            // consider the recording call as "failed". Unlike in python, where we can just
            // wrap everything in try-except, in rust, we keep track of the last report SeqError, which
            // we clear before handling each recording message. If we see it is set, the
            // we know the recording has faild.
            match (&error, self.last_seq_error.take()) {
                (None, Some(seq_err)) => {
                    // Report failure to the controller.
                    self.controller_actor
                        .remote_function_failed(
                            cx,
                            seq,
                            WorkerError {
                                backtrace: format!("recording failed: {}", &seq_err),
                                worker_actor_id: cx.self_id().clone(),
                            },
                        )
                        .await?;
                    error = Some(seq_err)
                }
                _ => {}
            }
            // Continue processing the remaining stream messages regardless of error.
            // We need to do this partially for error propagation, but also because
            // certain messages (like borrows and reductions) need to run regardless
            // in order to prevent deadlocks.
        }

        // Delete the formal refs and some subset of the RecordingResult refs. The
        // controller should have generated DeleteRefs messages for all other refs
        // defined by the recording.
        StreamMessageHandler::handle(
            self,
            cx,
            StreamMessage::DeleteRefs(all_defined_refs.into_iter().collect()),
        )
        .await?;

        // Any refs mutated by the recording and all results should have the same error
        // (the original error that caused the recording to fail).
        if error.is_some() {
            for ref_ in results.iter().chain(all_mutated_refs.iter()) {
                self.env.insert(*ref_, Err(error.clone().unwrap()));
            }
        }

        self.active_recording = None;
        Ok(())
    }

    async fn set_ref_unit_tests_only(
        &mut self,
        _cx: &Context<Self>,
        reference: Ref,
        value: WireValue,
    ) -> Result<()> {
        let pyobj = Python::with_gil(|py| -> PyResult<PyObject> {
            Ok(value.into_pyobject(py)?.unbind().into())
        })?;
        self.env.insert(reference, Ok(pyobj));
        Ok(())
    }

    async fn set_tensor_ref_unit_tests_only(
        &mut self,
        _cx: &Context<Self>,
        reference: Ref,
        tensor_result: TensorCellResult,
    ) -> Result<()> {
        match tensor_result {
            Ok(tensor_cell) => {
                let pyobj = Self::tensor_to_pyobject(tensor_cell);
                self.env.insert(reference, Ok(pyobj));
            }
            Err(err) => {
                self.env.insert(reference, Err(err));
            }
        }
        Ok(())
    }

    async fn get_ref_unit_tests_only(
        &mut self,
        _cx: &Context<Self>,
        reference: Ref,
    ) -> Result<Option<Result<WireValue, String>>> {
        use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyNone, PyString};
        /// For testing only, doesn't support Tensor or TensorList.
        fn pyobject_to_wire(
            value: Result<PyObject, Arc<SeqError>>,
        ) -> Result<WireValue, Arc<SeqError>> {
            let pyobj = value?;
            Python::with_gil(|py| {
                let bound = pyobj.bind(py);
                // Check bool before int since Python's bool is a subclass of int
                if bound.is_instance_of::<PyBool>() {
                    Ok(WireValue::Bool(bound.extract::<bool>().unwrap()))
                } else if bound.is_instance_of::<PyInt>() {
                    Ok(WireValue::Int(bound.extract::<i64>().unwrap()))
                } else if bound.is_instance_of::<PyList>() {
                    if let Ok(val) = bound.extract::<Vec<i64>>() {
                        Ok(WireValue::IntList(val))
                    } else {
                        Ok(WireValue::String(format!(
                            "unsupported list type: {:?}",
                            bound
                        )))
                    }
                } else if bound.is_instance_of::<PyFloat>() {
                    Ok(WireValue::Double(bound.extract::<f64>().unwrap()))
                } else if bound.is_instance_of::<PyString>() {
                    Ok(WireValue::String(bound.extract::<String>().unwrap()))
                } else if bound.is_instance_of::<PyNone>() {
                    Ok(WireValue::None(()))
                } else {
                    Ok(WireValue::String(format!(
                        "unsupported pyobject type: {:?}",
                        bound
                    )))
                }
            })
        }
        Ok(self.env.get(&reference).map(|pyobj| {
            pyobject_to_wire(Python::with_gil(|_py| pyobj.clone())).map_err(|err| err.to_string())
        }))
    }

    async fn get_tensor_ref_unit_tests_only(
        &mut self,
        _cx: &Context<Self>,
        reference: Ref,
    ) -> Result<Option<TensorCellResult>> {
        match self.env.get(&reference) {
            Some(Ok(pyobj)) => Python::with_gil(|py| match Self::pyobject_to_tensor(py, pyobj) {
                Ok(tensor) => Ok(Some(Ok(tensor.try_cpu().unwrap()))),
                Err(e) => bail!("expected tensor, got extraction error: {:?}", e),
            }),
            Some(Err(err)) => Ok(Some(Err(err.clone()))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use hyperactor::actor::ActorStatus;
    use hyperactor::context;
    use hyperactor::supervision::ActorSupervisionEvent;
    use monarch_messages::controller::ControllerMessage;
    use monarch_messages::worker::StreamCreationMode;
    use monarch_types::PickledPyObject;
    use pyo3::IntoPyObjectExt;
    use timed_test::async_timed_test;
    use torch_sys_cuda::nccl::UniqueId;
    use torch_sys2::factory_float_tensor;
    use torch_sys2::testing::allclose;

    use super::*;
    use crate::comm::CommParams;
    use crate::test_util;

    fn fake_seq_error(err: anyhow::Error) -> Arc<SeqError> {
        Arc::new(SeqError {
            seq: 0.into(),
            error: err,
        })
    }

    struct TestSetup {
        proc: Proc,
        stream_actor: ActorHandle<StreamActor>,
        client: Instance<()>,
        // Unused, but necessary, because proc needs a supervision
        // port -- otherwise an actor failure will cause a crash.
        #[allow(dead_code)]
        supervision_rx: PortReceiver<ActorSupervisionEvent>,
        controller_rx: PortReceiver<ControllerMessage>,
        controller_actor: ActorRef<ControllerActor>,
        next_ref: Ref,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            Self::new_with_world_size(1).await
        }

        async fn new_with_world_size(world_size: usize) -> Result<Self> {
            test_util::test_setup()?;

            let proc = Proc::local();
            let (_, controller_actor, controller_rx) =
                proc.attach_actor::<ControllerActor, ControllerMessage>("controller")?;
            let (client, _handle) = proc.instance("client")?;
            let (supervision_tx, supervision_rx) = client.open_port();
            proc.set_supervision_coordinator(supervision_tx)?;
            let stream_actor = proc.spawn(
                "stream",
                StreamActor::new(StreamParams {
                    world_size,
                    rank: 0,
                    creation_mode: StreamCreationMode::UseDefaultStream,
                    id: 0.into(),
                    device: Some(CudaDevice::new(0.into())),
                    controller_actor: controller_actor.clone(),
                    respond_with_python_message: false,
                }),
            )?;

            Ok(Self {
                proc,
                stream_actor,
                client,
                supervision_rx,
                controller_rx,
                controller_actor,
                next_ref: 0.into(),
            })
        }

        fn next_ref(&mut self) -> Ref {
            let ref_ = self.next_ref;
            self.next_ref = Ref {
                id: self.next_ref.id + 1,
            };
            ref_
        }

        async fn set_tensor(&mut self, reference: Ref, data: &[f32]) -> Result<()> {
            let tensor = TensorCell::new(factory_float_tensor(data, "cuda".parse().unwrap()));
            self.stream_actor
                .set_tensor_ref_unit_tests_only(&self.client, reference, Ok(tensor))
                .await
        }

        async fn allclose(&mut self, reference: Ref, data: &[f32]) -> bool {
            let actual = self
                .stream_actor
                .get_tensor_ref_unit_tests_only(&self.client, reference)
                .await
                .unwrap()
                .unwrap()
                .unwrap();

            let result = allclose(
                &factory_float_tensor(data, "cpu".parse().unwrap()),
                &actual.borrow(),
            )
            .unwrap();
            // rustfmt-ignore
            result
        }

        async fn validate_dependent_error(&mut self, reference: Ref, error: Arc<SeqError>) {
            let result_error = self
                .stream_actor
                .get_tensor_ref_unit_tests_only(&self.client, reference)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();

            assert!(Arc::ptr_eq(&result_error, &error));
        }
    }

    async fn assert_actor_failed_with_msg(proc: &Proc, actor_id: &ActorId, expected_msg: String) {
        loop {
            let status = proc
                .ledger_snapshot()
                .roots
                .get(actor_id)
                .unwrap()
                .status
                .clone();
            if let ActorStatus::Failed(msg) = status {
                assert!(msg.to_string().contains(&expected_msg));
                break;
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn assert_refs_do_not_exist(test_setup: &TestSetup, refs: &[Ref]) {
        for ref_ in refs {
            assert!(
                test_setup
                    .stream_actor
                    .get_tensor_ref_unit_tests_only(&test_setup.client, *ref_)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    async fn fetch_result(
        cx: &impl context::Actor,
        stream_actor: ActorHandle<StreamActor>,
        seq: Seq,
        reference: Ref,
    ) {
        let ref_to_send = Python::with_gil(|py| {
            PickledPyObject::pickle(&reference.into_bound_py_any(py).unwrap()).unwrap()
        });

        stream_actor
            .send_value(
                cx,
                seq,
                stream_actor.actor_id().clone(),
                Vec::new(),
                None,
                ArgsKwargs::from_wire_values(
                    vec![WireValue::PyObject(ref_to_send)],
                    HashMap::new(),
                )
                .unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap()
    }

    async fn check_fetch_result_error(
        cx: &impl context::Actor,
        stream_actor: ActorHandle<StreamActor>,
        seq: Seq,
        reference: Ref,
        controller_rx: &mut PortReceiver<ControllerMessage>,
        expected_backtrace: &str,
    ) {
        fetch_result(cx, stream_actor, seq, reference).await;

        let controller_msg = controller_rx.recv().await.unwrap();
        match controller_msg {
            ControllerMessage::FetchResult {
                seq: actual_seq,
                value: Err(err),
            } => {
                assert_eq!(actual_seq, seq);
                assert!(
                    err.backtrace.contains(expected_backtrace),
                    "backtrace did not contain {:?}: {:?}",
                    expected_backtrace,
                    err.backtrace
                );
            }
            _ => panic!("Unexpected controller message: {:?}", controller_msg),
        };
    }

    async fn check_fetch_result_value(
        cx: &impl context::Actor,
        stream_actor: ActorHandle<StreamActor>,
        seq: Seq,
        reference: Ref,
        controller_rx: &mut PortReceiver<ControllerMessage>,
    ) {
        fetch_result(cx, stream_actor, seq, reference).await;

        let controller_msg = controller_rx.recv().await.unwrap();
        match controller_msg {
            ControllerMessage::FetchResult {
                value: Ok(_),
                seq: actual_seq,
            } => assert_eq!(seq, actual_seq),
            _ => panic!("Unexpected controller message: {:?}", controller_msg),
        };
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_define_recording_other_recording_active() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 1.into())
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "different recording already active".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_define_recording_already_defined() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;
        test_setup
            .stream_actor
            .finalize_recording(&test_setup.client, 0.into())
            .await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "already defined".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_finalize_recording_other_recording_active() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;
        test_setup
            .stream_actor
            .finalize_recording(&test_setup.client, 1.into())
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "cannot finalize recording that isn't active".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_recording_formal_outside_recording() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .recording_formal(&test_setup.client, 0.into(), 0)
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "recording_formal called outside of recording".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_recording_result_outside_recording() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .recording_result(&test_setup.client, 0.into(), 0)
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "recording_result called outside of recording".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_call_recording_other_recording_active() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;
        test_setup
            .stream_actor
            .call_recording(&test_setup.client, 0.into(), 0.into(), vec![], vec![])
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "cannot call recording while another recording is active".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_call_recording_not_found() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .call_recording(&test_setup.client, 0.into(), 0.into(), vec![], vec![])
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "not found".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_recording_formal_too_few_arguments() -> Result<()> {
        let test_setup = TestSetup::new().await?;

        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;

        test_setup
            .stream_actor
            .recording_formal(&test_setup.client, 1.into(), 0)
            .await?;

        test_setup
            .stream_actor
            .finalize_recording(&test_setup.client, 0.into())
            .await?;

        test_setup
            .stream_actor
            .call_recording(&test_setup.client, 0.into(), 0.into(), vec![], vec![])
            .await?;

        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "recording_formal called with too few arguments".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_recording_result_too_few_results() -> Result<()> {
        let test_setup = TestSetup::new().await?;

        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;

        test_setup
            .stream_actor
            .recording_result(&test_setup.client, 1.into(), 0)
            .await?;

        test_setup
            .stream_actor
            .finalize_recording(&test_setup.client, 0.into())
            .await?;

        test_setup
            .stream_actor
            .call_recording(&test_setup.client, 0.into(), 0.into(), vec![], vec![])
            .await?;

        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "recording_result called with too few results".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_basic_call_recording() -> Result<()> {
        let mut test_setup = TestSetup::new().await?;

        // Define a recording equivalent to:
        // def f(x, y):
        //   return y, x
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;

        let formal0_ref = 1.into();
        let formal0_index = 1;
        test_setup
            .stream_actor
            .recording_formal(&test_setup.client, formal0_ref, formal0_index)
            .await?;

        let formal1_ref = 2.into();
        let formal1_index = 0;
        test_setup
            .stream_actor
            .recording_formal(&test_setup.client, formal1_ref, formal1_index)
            .await?;

        let result0_ref = formal0_ref;
        let result0_index = 0;
        test_setup
            .stream_actor
            .recording_result(&test_setup.client, result0_ref, result0_index)
            .await?;

        let result1_ref = formal1_ref;
        let result1_index = 1;
        test_setup
            .stream_actor
            .recording_result(&test_setup.client, result1_ref, result1_index)
            .await?;

        test_setup
            .stream_actor
            .finalize_recording(&test_setup.client, 0.into())
            .await?;

        let actual0_ref = 3.into();
        test_setup.set_tensor(actual0_ref, &[1.0, 2.0, 3.0]).await?;

        let actual1_ref = 4.into();
        test_setup.set_tensor(actual1_ref, &[4.0, 5.0]).await?;

        // Call the recording with valid tensors for the actual inputs,
        // and store the results in refs 5 and 6.
        let actual_result0_ref = 5.into();
        let actual_result1_ref = 6.into();
        test_setup
            .stream_actor
            .call_recording(
                &test_setup.client,
                0.into(),
                0.into(),
                vec![actual_result0_ref, actual_result1_ref],
                vec![actual0_ref, actual1_ref],
            )
            .await?;

        // Ensure the results are correct.
        assert!(test_setup.allclose(actual_result0_ref, &[4.0, 5.0]).await);
        assert!(
            test_setup
                .allclose(actual_result1_ref, &[1.0, 2.0, 3.0])
                .await
        );

        // Ensure the temporary refs associated with the formals/results have
        // been deleted.
        assert_refs_do_not_exist(&test_setup, &[formal0_ref, formal1_ref]).await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_request_status_in_recording() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;
        test_setup
            .stream_actor
            .request_status(&test_setup.client)
            .await
            .expect_err("request_status should have failed");
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "request_status not allowed in recording".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_init_comm_in_recording() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;

        let dummy_comm = test_setup.proc.spawn(
            "comm",
            NcclCommActor::new(CommParams::New {
                device: CudaDevice::new(0.into()),
                unique_id: UniqueId::new()?,
                world_size: 1,
                rank: 0,
            })
            .await
            .unwrap(),
        )?;

        test_setup
            .stream_actor
            .init_comm(&test_setup.client, dummy_comm)
            .await?;
        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "init_comm not allowed in recording".into(),
        )
        .await;
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_borrow_create_duplicate_borrow() -> Result<()> {
        let mut test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;

        let borrow_id = 1;
        let tensor_ref = test_setup.next_ref();
        let (first_use_sender, _first_use_receiver) = test_setup.client.open_port();

        test_setup
            .stream_actor
            .borrow_create(
                &test_setup.client,
                borrow_id,
                tensor_ref,
                first_use_sender.clone(),
            )
            .await?;

        test_setup
            .stream_actor
            .borrow_create(&test_setup.client, borrow_id, tensor_ref, first_use_sender)
            .await?;

        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "duplicate borrow create in recording".into(),
        )
        .await;

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_borrow_drop_borrow_not_defined() -> Result<()> {
        let test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;

        let borrow_id = 1;
        let (_last_use_sender, last_use_receiver) = test_setup.client.open_port();

        test_setup
            .stream_actor
            .borrow_drop(
                &test_setup.client,
                borrow_id,
                Arc::new(Mutex::new(last_use_receiver)),
            )
            .await?;

        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "borrow drop for borrow not defined in recording".into(),
        )
        .await;

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_borrow_not_dropped_before_finalize() -> Result<()> {
        let mut test_setup = TestSetup::new().await?;
        test_setup
            .stream_actor
            .define_recording(&test_setup.client, 0.into())
            .await?;

        let borrow_id = 1;
        let tensor_ref = test_setup.next_ref();
        let (first_use_sender, _first_use_receiver) = test_setup.client.open_port();

        test_setup
            .stream_actor
            .borrow_create(
                &test_setup.client,
                borrow_id,
                tensor_ref,
                first_use_sender.clone(),
            )
            .await?;

        // Attempt to finalize the recording without dropping the borrow
        test_setup
            .stream_actor
            .finalize_recording(&test_setup.client, 0.into())
            .await?;

        assert_actor_failed_with_msg(
            &test_setup.proc,
            test_setup.stream_actor.actor_id(),
            "all borrows created within recording must be dropped within recording".into(),
        )
        .await;

        Ok(())
    }
}
