/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// NOTE: Until https://github.com/PyO3/pyo3/pull/4674, `pyo3::pymethods` trigger
// and unsafe-op-in-unsafe-fn warnings.
#![allow(unsafe_op_in_unsafe_fn)]

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Context;
use derive_more::Display;
use derive_more::From;
use derive_more::TryInto;
use enum_as_inner::EnumAsInner;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::RefClient;
use hyperactor::Unbind;
use hyperactor::reference::ActorId;
use monarch_types::SerializablePyErr;
use ndslice::Slice;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use torch_sys::BorrowError;
use torch_sys::Device;
use torch_sys::Layout;
use torch_sys::ScalarType;
use torch_sys::call_op::CallOpError;
use torch_sys_cuda::nccl::NcclConfig;
use torch_sys_cuda::nccl::ReduceOp;
use torch_sys_cuda::nccl::UniqueId;

use crate::controller::ControllerActor;
use crate::controller::Seq;
use crate::wire_value::WireValue;

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Hash,
    PartialEq,
    Eq,
    Copy,
    PartialOrd,
    Ord,
    From
)]
#[pyo3::pyclass(
    frozen,
    module = "monarch._rust_bindings.monarch_extension.tensor_worker"
)]
pub struct StreamRef {
    #[pyo3(get)]
    pub id: u64,
}

#[pyo3::pymethods]
impl StreamRef {
    #[new]
    #[pyo3(signature = (*, id))]
    fn new(id: u64) -> Self {
        Self { id }
    }

    fn __repr__(&self) -> String {
        format!("StreamRef({})", self.id)
    }

    // TODO: Upgrade pyo3 to use eq, ord on pyclass
    fn __richcmp__(&self, other: PyRef<Self>, op: pyo3::class::basic::CompareOp) -> PyResult<bool> {
        Ok(match op {
            pyo3::class::basic::CompareOp::Eq => self.id == other.id,
            pyo3::class::basic::CompareOp::Ne => self.id != other.id,
            pyo3::class::basic::CompareOp::Lt => self.id < other.id,
            pyo3::class::basic::CompareOp::Le => self.id <= other.id,
            pyo3::class::basic::CompareOp::Gt => self.id > other.id,
            pyo3::class::basic::CompareOp::Ge => self.id >= other.id,
        })
    }

    fn __hash__(&self) -> PyResult<u64> {
        Ok(self.id)
    }
}

// TODO: The Python implementation uses `Ref` to describe any worker value that
// can be referenced by the controller, including: tensors, streams, pipes,
// device meshes. We might be able to more explicitly type these, as they are
// not generally interchangeable.
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Hash,
    PartialEq,
    Eq,
    Copy,
    PartialOrd,
    Ord,
    From
)]
#[pyo3::pyclass(
    frozen,
    module = "monarch._rust_bindings.monarch_extension.tensor_worker"
)]
pub struct Ref {
    #[pyo3(get)]
    pub id: u64,
}

#[pyo3::pymethods]
impl Ref {
    #[new]
    fn new(id: u64) -> Self {
        Self { id }
    }

    #[getter]
    fn r#ref(&self) -> u64 {
        self.id
    }

    fn __repr__(&self) -> String {
        format!("Ref({})", self.id)
    }

    // TODO: Upgrade pyo3 to use eq, ord on pyclass
    fn __richcmp__(&self, other: PyRef<Self>, op: pyo3::class::basic::CompareOp) -> PyResult<bool> {
        Ok(match op {
            pyo3::class::basic::CompareOp::Eq => self.id == other.id,
            pyo3::class::basic::CompareOp::Ne => self.id != other.id,
            pyo3::class::basic::CompareOp::Lt => self.id < other.id,
            pyo3::class::basic::CompareOp::Le => self.id <= other.id,
            pyo3::class::basic::CompareOp::Gt => self.id > other.id,
            pyo3::class::basic::CompareOp::Ge => self.id >= other.id,
        })
    }

    fn __hash__(&self) -> PyResult<u64> {
        Ok(self.id)
    }

    fn __getnewargs_ex__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("id", self.id).unwrap();

        PyTuple::new(
            py,
            vec![
                PyTuple::empty(py).unbind().into_any(),
                kwargs.unbind().into_any(),
            ],
        )
    }
}

impl Ref {
    // This is a function on ref instead of impl FromPyObject due to a bug in pyo3
    // https://github.com/PyO3/pyo3/issues/4337
    pub fn from_py_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let attr_name = pyo3::intern!(obj.py(), "__monarch_ref__");
        if let Ok(ref_) = obj.extract::<Ref>() {
            return Ok(ref_);
        }
        if let Ok(func) = obj.getattr(attr_name) {
            if let Ok(Ok(val)) = func.call0().map(|val| val.extract::<u64>()) {
                return Ok(val.into());
            }
        }
        Err(PyValueError::new_err("Could not convert object to Ref"))
    }
}

impl Display for Ref {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "r{}", self.id)
    }
}

/// Identifies a CallFunction target. Can either be a torch op or a Python
/// global reference.
// TODO: do some validation on the namespace/opname/overload
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[pyo3::pyclass(
    frozen,
    module = "monarch._rust_bindings.monarch_extension.tensor_worker"
)]
pub struct FunctionPath {
    #[pyo3(get)]
    pub path: String,
}

impl fmt::Display for FunctionPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<function \"{}\">", self.path)
    }
}

impl<T: Into<String>> From<T> for FunctionPath {
    fn from(val: T) -> Self {
        Self { path: val.into() }
    }
}

#[pyo3::pymethods]
impl FunctionPath {
    #[new]
    #[pyo3(signature = (*, path))]
    pub fn new(path: String) -> Self {
        Self { path }
    }

    fn __repr__(&self) -> String {
        self.path.clone()
    }

    pub fn resolve<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (start, rest) = self.path.split_once(".").with_context(|| {
            format!(
                "invalid function path {}: paths must be fully qualified",
                self.path
            )
        })?;
        if start == "torch" {
            let mut cur = py.import("torch")?.into_any();
            for p in rest.split(".") {
                cur = cur.getattr(p)?;
            }
            Ok(cur)
        } else {
            let (module_fqn, function_name) = self.path.rsplit_once(".").with_context(|| {
                format!(
                    "invalid function path {}: paths must be fully qualified",
                    self.path
                )
            })?;
            let module = PyModule::import(py, module_fqn)?;
            let mut function = module.getattr(function_name)?;
            if function.hasattr("_remote_impl")? {
                function = function.getattr("_remote_impl")?;
            }
            Ok(function.downcast_into()?)
        }
    }
}

/// Identifies a CallFunction target. Can either be a torch op or a Python
/// global reference.
// TODO: do some validation on the namespace/opname/overload
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, From)]
#[pyo3::pyclass(
    frozen,
    module = "monarch._rust_bindings.monarch_extension.tensor_worker"
)]
pub struct Cloudpickle {
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

impl fmt::Display for Cloudpickle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<cloud-pickle>")
    }
}

#[pyo3::pymethods]
impl Cloudpickle {
    #[new]
    #[pyo3(signature = (*, bytes))]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    fn __repr__(&self) -> String {
        format!("Cloudpickle(bytes={:?})", self.bytes)
    }

    pub fn resolve<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let module = PyModule::import(py, "cloudpickle")?;
        let loads = module.getattr("loads")?;
        loads.call1((PyBytes::new(py, &self.bytes),))
    }
}

#[derive(
    PartialEq,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    TryInto,
    From,
    FromPyObject,
    Display
)]
pub enum ResolvableFunction {
    #[pyo3(transparent)]
    Cloudpickle(Cloudpickle),
    #[pyo3(transparent)]
    FunctionPath(FunctionPath),
}

impl<'py> IntoPyObject<'py> for ResolvableFunction {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            Self::Cloudpickle(func) => func.into_pyobject(py)?.into_any(),
            Self::FunctionPath(func) => func.into_pyobject(py)?.into_any(),
        })
    }
}

impl ResolvableFunction {
    pub fn resolve<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Cloudpickle(func) => Ok(func.resolve(py)?.into_any()),
            Self::FunctionPath(func) => func.resolve(py),
        }
    }

    /// For testing: this is a special remote function path that induces a panic
    /// when called.
    pub fn panic_if_requested(&self) {
        match self {
            Self::FunctionPath(func) => {
                if func.path == "__test_panic" {
                    panic!("__test_panic called");
                }
            }
            _ => (),
        }
    }
}

impl<T: Into<String>> From<T> for ResolvableFunction {
    fn from(val: T) -> Self {
        FunctionPath::from(val).into()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CallFunctionParams {
    /// Sequence ID of the invocation.
    pub seq: Seq,
    /// The references of the results to set.
    pub results: Vec<Option<Ref>>,
    /// The references of the mutates to set.
    pub mutates: Vec<Ref>,
    /// The function to call.
    pub function: ResolvableFunction,
    /// The arguments to the function.
    pub args: Vec<WireValue>,
    /// The keyword arguments to the function.
    pub kwargs: HashMap<String, WireValue>,
    /// The stream to call the function on.
    pub stream: StreamRef,
    /// The process groups to execute the function on.
    pub remote_process_groups: Vec<Ref>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActorCallParams {
    pub seq: Seq,
    // The BrokerId but we do not depend on hyperactor in messages.
    pub broker_id: (String, usize),
    /// Referenceable objects to pass to the actor as LocalState,
    /// these will be put into the PythonMessage
    /// during its unpickling.
    pub local_state: Vec<Ref>,
    /// Tensors that will be mutated by the call.
    pub mutates: Vec<Ref>,
    pub stream: StreamRef,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActorMethodParams {
    pub results: Vec<Option<Ref>>,
    pub call: ActorCallParams,
}

/// Type of reduction for [`WorkerMessage::Reduce`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reduction {
    /// A gather, concat'ing the values along the reduction dimension.
    Stack,
    /// A NCCL reduction type.
    ReduceOp(ReduceOp),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[pyo3::pyclass(
    frozen,
    name = "TensorFactory",
    module = "monarch._rust_bindings.monarch_extension.tensor_worker"
)]
pub struct Factory {
    pub size: Vec<i64>,
    #[serde(with = "torch_sys::ScalarTypeDef")]
    pub dtype: ScalarType,
    #[serde(with = "torch_sys::LayoutDef")]
    pub layout: Layout,
    pub device: Device,
}

#[pyo3::pymethods]
impl Factory {
    #[new]
    #[pyo3(signature = (*, size, dtype, layout, device))]
    pub fn new(
        py: Python<'_>,
        size: Vec<i64>,
        dtype: PyObject,
        layout: PyObject,
        device: PyObject,
    ) -> PyResult<Self> {
        // TODO: Add some validation around dtype / layout. We should have pyre types on
        // the python side to help in the short term.
        Ok(Self {
            size,
            dtype: dtype.extract::<ScalarType>(py)?,
            layout: layout.extract::<Layout>(py)?,
            device: device.extract::<Device>(py)?,
        })
    }

    #[staticmethod]
    pub fn from_py(obj: Bound<'_, PyAny>) -> PyResult<Self> {
        Self::new(
            obj.py(),
            obj.getattr("size")?.extract()?,
            obj.getattr("dtype")?.unbind(),
            obj.getattr("layout")?.unbind(),
            obj.getattr("device")?.unbind(),
        )
    }

    #[getter]
    fn size<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyTuple>> {
        PyTuple::new(py, self.size.iter())
    }

    #[getter]
    fn dtype<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        self.dtype.into_pyobject(py)
    }

    #[getter]
    fn layout<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        self.layout.into_pyobject(py)
    }

    #[getter]
    fn device(&self) -> String {
        self.device.to_string()
    }
}

/// Controls what CUDA stream an actor will use.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[pyo3::pyclass(
    module = "monarch._rust_bindings.monarch_extension.tensor_worker",
    eq,
    eq_int
)]
pub enum StreamCreationMode {
    /// Use the default stream for the current device.
    UseDefaultStream,
    /// Create a new stream for this actor.
    CreateNewStream,
}

/// An error associated with a seq number that failed to execute.
/// Any defined value that has an error value will have an assocated
/// SeqError that is the root cause of why that value has an error.
/// A value may have this error because it was directly defined by the
/// action associated with the sequence number, or if it was defined by
/// another action that dependend on the failing one.
#[derive(Debug, Named)]
#[named(register = false)]
pub struct SeqError {
    pub seq: Seq,
    pub error: anyhow::Error,
}

impl Display for SeqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

/// When a worker runs any function, it may not succeed either because the function itself
/// failed (Error) or because an input to the function already had an error value
/// DependentError.
#[derive(Error, Debug, Named)]
#[named(register = false)]
pub enum CallFunctionError {
    #[error("{0}")]
    Error(#[from] anyhow::Error),
    #[error("Computation depended on an input that failed with error: {0}")]
    DependentError(Arc<SeqError>),
}

impl CallFunctionError {
    // Static functions for backward compatibility with existing enum cases
    #[allow(non_snake_case)]
    pub fn RefNotFound(r: Ref) -> Self {
        Self::Error(anyhow::anyhow!("ref not found: {}", r))
    }

    #[allow(non_snake_case)]
    pub fn InvalidRemoteFunction(msg: String) -> Self {
        Self::Error(anyhow::anyhow!("invalid remote function: {}", msg))
    }

    #[allow(non_snake_case)]
    pub fn UnsupportedArgType(function_type: String, arg_type: String) -> Self {
        Self::Error(anyhow::anyhow!(
            "unsupported arg type for {} function: {}",
            function_type,
            arg_type
        ))
    }

    #[allow(non_snake_case)]
    pub fn RemoteFunctionFailed(err: SerializablePyErr) -> Self {
        Self::Error(anyhow::anyhow!("remote function failed: {}", err))
    }

    #[allow(non_snake_case)]
    pub fn BorrowError(err: BorrowError) -> Self {
        Self::Error(anyhow::anyhow!("borrow failed: {}", err))
    }

    #[allow(non_snake_case)]
    pub fn OperatorFailed(err: CallOpError) -> Self {
        Self::Error(anyhow::anyhow!("torch operator failed: {}", err))
    }

    #[allow(non_snake_case)]
    pub fn UnexpectedNumberOfReturns(expected: usize, actual: usize) -> Self {
        Self::Error(anyhow::anyhow!(
            "unexpected number of returns from op, expected {}, got {}",
            expected,
            actual
        ))
    }

    #[allow(non_snake_case)]
    pub fn TooManyArgsForValue(args: String, kwargs: String) -> Self {
        Self::Error(anyhow::anyhow!(
            "expected only a single arg (and no kwargs) when no function is given: {}, {}",
            args,
            kwargs
        ))
    }

    #[allow(non_snake_case)]
    pub fn Anyhow(err: anyhow::Error) -> Self {
        Self::Error(err)
    }
}

impl From<SerializablePyErr> for CallFunctionError {
    fn from(v: SerializablePyErr) -> CallFunctionError {
        CallFunctionError::Error(v.into())
    }
}

impl From<BorrowError> for CallFunctionError {
    fn from(v: BorrowError) -> CallFunctionError {
        CallFunctionError::Error(v.into())
    }
}

impl From<CallOpError> for CallFunctionError {
    fn from(v: CallOpError) -> CallFunctionError {
        CallFunctionError::Error(v.into())
    }
}

/// Worker messages. These define the observable behavior of the worker, so the
/// documentations here
#[derive(
    Handler,
    HandleClient,
    RefClient,
    Clone,
    Serialize,
    Deserialize,
    Debug,
    Named,
    EnumAsInner,
    Bind,
    Unbind
)]
pub enum WorkerMessage {
    /// Initialize backend network state.
    BackendNetworkInit(UniqueId),

    /// Initialize backend network state for point-to-point communication.
    BackendNetworkPointToPointInit {
        from_stream: StreamRef,
        to_stream: StreamRef,
    },

    /// Call a function, either a torch op or a Python `remote_function`.
    CallFunction(CallFunctionParams),

    /// Groups commands together; these commands will be executed in order by
    /// the worker.
    CommandGroup(Vec<WorkerMessage>),

    /// Create a [`Stream`] on the worker wih the provided id. Commands will be
    /// generally be scheduled onto streams to run; different streams can
    /// execute concurrently with one another.
    CreateStream {
        /// Id of the stream to create.
        id: StreamRef,
        /// Whether to use the default device stream or create a new one.
        stream_creation: StreamCreationMode,
    },

    /// Create a [`DeviceMesh`] on the worker, which can be used to schedule
    /// efficient inter-worker communication.
    CreateDeviceMesh {
        result: Ref,
        names: Vec<String>,
        ranks: Slice,
    },

    /// Create a PyTorch distributed process group on the worker, which can be
    /// used to schedule collectives in UDFs using monarch communicators.
    CreateRemoteProcessGroup {
        result: Ref,
        device_mesh: Ref,
        dims: Vec<String>,
    },

    /// Create a borrow of a tensor from one stream to another.
    ///
    /// Borrows allows streams to access tensors on another stream. The runtime
    /// will insert appropriate synchronization to ensure that cross-stream
    /// usage is safe.
    BorrowCreate {
        /// Ref of the resulting borrowed tensor
        result: Ref,
        /// Id for the borrow
        borrow: u64,
        /// Tensor to borrow
        tensor: Ref,
        /// Stream to borrow from
        from_stream: StreamRef,
        /// Stream to borrow to
        to_stream: StreamRef,
    },

    /// First use of the borrow on the receiving stream. This is a marker for
    /// synchronization.
    BorrowFirstUse {
        borrow: u64,
    },

    /// Last use of the borrow on the receiving stream. This is a marker for
    /// synchronization.
    BorrowLastUse {
        borrow: u64,
    },

    /// Drop the borrow and free the resources associated with it.
    BorrowDrop {
        borrow: u64,
    },

    /// Delete these refs from the worker state.
    DeleteRefs(Vec<Ref>),

    /// A [`ControllerMessage::Status`] will be send to the controller
    /// when all streams have processed all the message sent before this one.
    RequestStatus {
        seq: Seq,
        controller: bool,
    },

    /// Perform a reduction operation, using an efficient communication backend.
    /// Only NCCL is supported for now.
    Reduce {
        /// Where to store the result of the reduction.
        result: Ref,
        /// The tensor to reduce.
        tensor: Ref,
        /// Tensor metadata for `tensor` that can be used to construct a
        /// fresh tensor of appropriate size/shape. We use this if
        /// `tensor` isn't accessible for some reason (like a previous
        /// error on the worker).
        factory: Factory,
        /// The device mesh on which to perform the reduction.
        mesh: Ref,
        /// The stream to call the reduction on.
        stream: StreamRef,
        /// The dimensions of the device mesh to reduce over. The members of
        /// these dimension will form the members of the reduction collective.
        dims: Vec<String>,
        /// What kind of reduction to perform.
        reduction: Reduction,
        /// If `true`, the reduced result will be evenly split across the tensors
        /// of `dim`.
        scatter: bool,
        /// If `true`, the reduction will be performed in-place on `tensor`.
        in_place: bool,
        /// Pre-existing tensor that should be used as the output for the reduction.
        out: Option<Ref>,
    },

    /// Create a new communicator on each rank in `ranks`, capable of
    /// communicating with its peers along the specified dimensions.
    SplitComm {
        /// The device mesh dimensions along which the constructed communicator
        /// should be able to exchange data.
        dims: Vec<String>,
        /// The device mesh associated with the new communicator. One communicator
        /// will be created for every member of the mesh.
        device_mesh: Ref,
        /// The stream associated with the communicator. Communicator operations
        /// will be ordered with respect to other operations scheduled on this
        /// stream.
        stream: StreamRef,
        /// Configuration for the new communicator. If None, we will not pass a
        /// config object to nccl, which means that the created communicator
        /// will inherit its parent's config.
        config: Option<NcclConfig>,
    },

    /// Create a new communicator on each rank in `ranks`, capable of
    /// communicating with its peers along the specified dimensions.
    SplitCommForProcessGroup {
        /// The device mesh associated with the new communicator. One communicator
        /// will be created for every member of the mesh.
        remote_process_group: Ref,
        /// The stream associated with the communicator. Communicator operations
        /// will be ordered with respect to other operations scheduled on this
        /// stream.
        stream: StreamRef,
        /// Configuration for the new communicator. If None, we will not pass a
        /// config object to nccl, which means that the created communicator
        /// will inherit its parent's config.
        config: Option<NcclConfig>,
    },

    SendTensor {
        result: Ref,
        from_ranks: Slice,
        to_ranks: Slice,
        tensor: Ref,
        factory: Factory,
        from_stream: StreamRef,
        to_stream: StreamRef,
    },

    SendValue {
        seq: Seq,
        /// Pipe to send value to.  If `None`, value is sent to controller.
        destination: Option<Ref>,
        mutates: Vec<Ref>,
        /// Function to resolve the value to retrieve.  If `None`, then `args`
        /// must contain the value as its only element and `kwargs` must be
        /// empty.
        function: Option<ResolvableFunction>,
        args: Vec<WireValue>,
        kwargs: HashMap<String, WireValue>,
        /// The stream to retrieve from.
        stream: StreamRef,
    },

    SendResultOfActorCall(ActorCallParams),
    CallActorMethod(ActorMethodParams),
    PipeRecv {
        seq: Seq,
        /// Result refs.
        results: Vec<Option<Ref>>,
        /// Pipe to receive value from.
        pipe: Ref,
        /// The stream to retrieve from.
        stream: StreamRef,
    },

    /// Finish processing all messages previously sent to this worker and stop
    /// the actor loop. Any streams will also be drained.
    Exit {
        /// Optional error reason if the exit is the result of an error, including
        /// - optional actor id to indicate the source of the error
        /// - error message or stacktrace
        /// The worker process will be stopped if the error is provided.
        error: Option<(Option<ActorId>, String)>,
    },

    /// Defines (part of) a new recording on the worker. This is a list of commands
    /// representing the execution of a function that was defined using
    /// monarch.compile. If there are too many commands to send in a single
    /// DefineRecording message, the commands may be chunked into `ntotal_messages`,
    /// with the `index` field indicating how to order the DefineRecording messages
    /// for a single recording.
    DefineRecording {
        /// The ref associated with this recording that will be used to
        /// call it in the future.
        result: Ref,
        /// The number of output tensors.
        nresults: usize,
        /// The number of input tensors.
        nformals: usize,
        /// The list of commands to run.
        commands: Vec<WorkerMessage>,
        /// How many total DefineRecording messages make up this recording.
        ntotal_messages: usize,
        /// This DefineRecording message's index in the set of messages
        /// that make up this recording.
        index: usize,
    },

    /// Defines an input tensor for a recording.
    RecordingFormal {
        /// The ref that will be used to pass the input tensor to the
        /// recording.
        result: Ref,
        /// The index of the input tensor in the list of input tensors.
        argument_index: usize,
        /// The stream that this input tensor will be used on.
        stream: StreamRef,
    },

    /// Defines an output tensor for a recording.
    RecordingResult {
        /// The ref that will be used to store the output tensor.
        result: Ref,
        /// The index of the output tensor in the list of output tensors.
        output_index: usize,
        /// The stream that this output tensor will come from.
        stream: StreamRef,
    },

    /// Calls a recording that was previously defined using
    /// DefineRecording.
    CallRecording {
        /// The sequence number of the invocation.
        seq: Seq,
        /// The ref of the recording to call.
        recording: Ref,
        /// The list of refs where the result tensors from the recording
        /// will be stored.
        results: Vec<Ref>,
        /// The list of refs of input tensors to the recording.
        actuals: Vec<Ref>,
    },

    SetRefUnitTestsOnly {
        /// The reference to set.
        reference: Ref,
        /// The value to set it with.
        value: WireValue,
        /// The stream to set it on.
        stream: StreamRef,
    },

    GetRefUnitTestsOnly {
        /// The value to retrieve, expected to be a bool.
        value: Ref,
        /// The stream to retrieve from.
        stream: StreamRef,
        #[reply]
        response_port: hyperactor::OncePortRef<Option<Result<WireValue, String>>>,
    },
}

/// The parameters to spawn a worker actor.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct WorkerParams {
    // Global world size for this job
    pub world_size: usize,

    // Rank of the worker within the global world
    pub rank: usize,

    // Local cuda device that this worker represents. If None, we won't do CUDA
    // synchronization.
    pub device_index: Option<i8>,

    // Actor Ref for the controller that the worker is associated with.
    pub controller_actor: ActorRef<ControllerActor>,
}

hyperactor::behavior!(
    WorkerActor,
    WorkerMessage { cast = true },
);
