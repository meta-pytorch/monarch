/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// These are the worker messages exposed through pyo3 to python.
/// The actual documentation of the messages can be found in [`monarch_messages::worker::WorkerMessage`]
/// This split is currently needed to customize the constructors for the messages and due to the
/// fact that the current pyo3 version has weaker support for proc macro based pyclass customization.
/// A version bump + figuring out if we want to expose unittest structs to python along with portref etc
/// + support for constructor specialization will help avoid duplication here.
///   TODO: Potentially too many clones of slice objects, might need to refactor to avoid that.
use std::collections::HashMap;
use std::ops::DerefMut;

use anyhow::Result;
use hyperactor::data::Serialized;
use hyperactor::reference::ActorId;
use monarch_hyperactor::ndslice::PySlice;
use monarch_hyperactor::proc::PyActorId;
use monarch_hyperactor::runtime::get_tokio_runtime;
use monarch_messages::wire_value::WireValue;
use monarch_messages::wire_value::func_call_args_to_wire_values;
use monarch_messages::worker::*;
use monarch_types::TryIntoPyObjectUnsafe;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use torch_sys_cuda::nccl::ReduceOp;
use torch_sys_cuda::nccl::UniqueId;

#[pyclass(
    name = "WorkerMessage",
    subclass,
    module = "monarch._rust_bindings.monarch_extension.tensor_worker"
)]
pub(crate) struct PyWorkerMessage {
    pub message: WorkerMessage,
}

impl PyWorkerMessage {
    pub(crate) fn to_serialized(&self) -> PyResult<Serialized> {
        Serialized::serialize(&self.message)
            .map_err(|err| PyRuntimeError::new_err(format!("Failed to serialize message: {err}")))
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct BackendNetworkInit;

#[pymethods]
impl BackendNetworkInit {
    #[new]
    fn new() -> PyResult<(Self, PyWorkerMessage)> {
        Ok((
            Self,
            PyWorkerMessage {
                message: WorkerMessage::BackendNetworkInit(
                    UniqueId::new().map_err(|err| PyRuntimeError::new_err(err.to_string()))?,
                ),
            },
        ))
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct BackendNetworkPointToPointInit;

#[pymethods]
impl BackendNetworkPointToPointInit {
    #[new]
    #[pyo3(signature = (*, from_stream, to_stream))]
    fn new(from_stream: StreamRef, to_stream: StreamRef) -> PyResult<(Self, PyWorkerMessage)> {
        Ok((
            Self,
            PyWorkerMessage {
                message: WorkerMessage::BackendNetworkPointToPointInit {
                    from_stream,
                    to_stream,
                },
            },
        ))
    }

    #[getter]
    fn from_stream(self_: PyRef<Self>) -> StreamRef {
        self_
            .as_ref()
            .message
            .as_backend_network_point_to_point_init()
            .unwrap()
            .0
            .clone()
    }

    #[getter]
    fn to_stream(self_: PyRef<Self>) -> StreamRef {
        self_
            .as_ref()
            .message
            .as_backend_network_point_to_point_init()
            .unwrap()
            .1
            .clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct CallFunction;

#[pymethods]
impl CallFunction {
    #[new]
    #[pyo3(signature = (*, seq, results, mutates, function, args, kwargs, stream, remote_process_groups))]
    fn new(
        seq: u64,
        results: Vec<Option<Ref>>,
        mutates: Vec<Ref>,
        function: ResolvableFunction,
        args: &Bound<'_, PyTuple>,
        kwargs: &Bound<'_, PyDict>,
        stream: StreamRef,
        remote_process_groups: Vec<Ref>,
    ) -> PyResult<(Self, PyWorkerMessage)> {
        let (args, kwargs) = func_call_args_to_wire_values(Some(&function), args, kwargs)?;
        Ok((
            Self,
            PyWorkerMessage {
                message: WorkerMessage::CallFunction(CallFunctionParams {
                    seq: seq.into(),
                    results,
                    mutates,
                    function,
                    args,
                    kwargs,
                    stream,
                    remote_process_groups,
                }),
            },
        ))
    }

    #[getter]
    fn seq(self_: PyRef<Self>) -> u64 {
        self_
            .as_ref()
            .message
            .as_call_function()
            .unwrap()
            .seq
            .into()
    }

    #[getter]
    fn results(self_: PyRef<Self>) -> Vec<Option<Ref>> {
        self_
            .as_ref()
            .message
            .as_call_function()
            .unwrap()
            .results
            .clone()
    }

    #[getter]
    fn mutates(self_: PyRef<Self>) -> Vec<Ref> {
        self_
            .as_ref()
            .message
            .as_call_function()
            .unwrap()
            .mutates
            .clone()
    }

    #[getter]
    fn function(self_: PyRef<Self>) -> ResolvableFunction {
        self_
            .as_ref()
            .message
            .as_call_function()
            .unwrap()
            .function
            .clone()
    }

    #[getter]
    fn args(self_: PyRef<Self>) -> PyResult<PyObject> {
        wire_values_to_args(
            self_.py(),
            self_
                .as_ref()
                .message
                .as_call_function()
                .unwrap()
                .args
                .clone(),
        )
    }

    #[getter]
    fn kwargs(self_: PyRef<Self>) -> PyResult<PyObject> {
        wire_values_to_kwargs(
            self_.py(),
            self_
                .as_ref()
                .message
                .as_call_function()
                .unwrap()
                .kwargs
                .clone(),
        )
    }

    #[getter]
    fn stream(self_: PyRef<Self>) -> StreamRef {
        self_
            .as_ref()
            .message
            .as_call_function()
            .unwrap()
            .stream
            .clone()
    }

    #[getter]
    fn remote_process_groups(self_: PyRef<Self>) -> Vec<Ref> {
        self_
            .as_ref()
            .message
            .as_call_function()
            .unwrap()
            .remote_process_groups
            .clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct CreateStream;

#[pymethods]
impl CreateStream {
    #[new]
    #[pyo3(signature = (*, id, stream_creation))]
    fn new(
        id: StreamRef,
        stream_creation: StreamCreationMode,
    ) -> PyResult<(Self, PyWorkerMessage)> {
        Ok((
            Self,
            PyWorkerMessage {
                message: WorkerMessage::CreateStream {
                    id,
                    stream_creation,
                },
            },
        ))
    }

    #[getter]
    fn id(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_create_stream().unwrap().0.clone()
    }

    #[getter]
    fn stream_creation(self_: PyRef<Self>) -> StreamCreationMode {
        *self_.as_ref().message.as_create_stream().unwrap().1
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct CreateDeviceMesh;

#[pymethods]
impl CreateDeviceMesh {
    #[new]
    #[pyo3(signature = (*, result, names, ranks))]
    fn new(result: Ref, names: Vec<String>, ranks: &PySlice) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::CreateDeviceMesh {
                    result,
                    names,
                    ranks: ranks.clone().into(),
                },
            },
        )
    }

    #[getter]
    fn result(self_: PyRef<Self>) -> Ref {
        self_
            .as_ref()
            .message
            .as_create_device_mesh()
            .unwrap()
            .0
            .clone()
    }

    #[getter]
    fn names(self_: PyRef<Self>) -> Vec<String> {
        self_
            .as_ref()
            .message
            .as_create_device_mesh()
            .unwrap()
            .1
            .clone()
    }

    #[getter]
    fn ranks(self_: PyRef<Self>) -> PySlice {
        self_
            .as_ref()
            .message
            .as_create_device_mesh()
            .unwrap()
            .2
            .clone()
            .into()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct CreateRemoteProcessGroup;

#[pymethods]
impl CreateRemoteProcessGroup {
    #[new]
    #[pyo3(signature = (*, result, device_mesh, dims))]
    fn new(result: Ref, device_mesh: Ref, dims: Vec<String>) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::CreateRemoteProcessGroup {
                    result,
                    device_mesh,
                    dims,
                },
            },
        )
    }

    #[getter]
    fn result(self_: PyRef<Self>) -> Ref {
        self_
            .as_ref()
            .message
            .as_create_remote_process_group()
            .unwrap()
            .0
            .clone()
    }

    #[getter]
    fn device_mesh(self_: PyRef<Self>) -> Ref {
        self_
            .as_ref()
            .message
            .as_create_remote_process_group()
            .unwrap()
            .1
            .clone()
    }

    #[getter]
    fn dims(self_: PyRef<Self>) -> Vec<String> {
        self_
            .as_ref()
            .message
            .as_create_remote_process_group()
            .unwrap()
            .2
            .clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct BorrowCreate;

#[pymethods]
impl BorrowCreate {
    #[new]
    #[pyo3(signature = (*, result, borrow, tensor, from_stream, to_stream))]
    fn new(
        result: Ref,
        borrow: u64,
        tensor: Ref,
        from_stream: StreamRef,
        to_stream: StreamRef,
    ) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::BorrowCreate {
                    result,
                    borrow,
                    tensor,
                    from_stream,
                    to_stream,
                },
            },
        )
    }

    #[getter]
    fn result(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_borrow_create().unwrap().0.clone()
    }

    #[getter]
    fn borrow(self_: PyRef<Self>) -> u64 {
        *self_.as_ref().message.as_borrow_create().unwrap().1
    }

    #[getter]
    fn tensor(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_borrow_create().unwrap().2.clone()
    }

    #[getter]
    fn from_stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_borrow_create().unwrap().3.clone()
    }

    #[getter]
    fn to_stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_borrow_create().unwrap().4.clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct BorrowFirstUse;

#[pymethods]
impl BorrowFirstUse {
    #[new]
    #[pyo3(signature = (*, borrow))]
    fn new(borrow: u64) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::BorrowFirstUse { borrow },
            },
        )
    }

    #[getter]
    fn borrow(self_: PyRef<Self>) -> u64 {
        *self_.as_ref().message.as_borrow_first_use().unwrap()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct BorrowLastUse;

#[pymethods]
impl BorrowLastUse {
    #[new]
    #[pyo3(signature = (*, borrow))]
    fn new(borrow: u64) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::BorrowLastUse { borrow },
            },
        )
    }

    #[getter]
    fn borrow(self_: PyRef<Self>) -> u64 {
        *self_.as_ref().message.as_borrow_last_use().unwrap()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct BorrowDrop;

#[pymethods]
impl BorrowDrop {
    #[new]
    #[pyo3(signature = (*, borrow))]
    fn new(borrow: u64) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::BorrowDrop { borrow },
            },
        )
    }

    #[getter]
    fn borrow(self_: PyRef<Self>) -> u64 {
        *self_.as_ref().message.as_borrow_drop().unwrap()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct DeleteRefs;

#[pymethods]
impl DeleteRefs {
    #[new]
    #[pyo3(signature = (*, refs))]
    fn new(refs: Vec<Ref>) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::DeleteRefs(refs),
            },
        )
    }

    #[getter]
    fn refs(self_: PyRef<Self>) -> Vec<Ref> {
        self_.as_ref().message.as_delete_refs().unwrap().clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct RequestStatus;

#[pymethods]
impl RequestStatus {
    #[new]
    #[pyo3(signature = (*, seq, controller))]
    fn new(seq: u64, controller: bool) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::RequestStatus {
                    seq: seq.into(),
                    controller,
                },
            },
        )
    }

    #[getter]
    fn seq(self_: PyRef<Self>) -> u64 {
        self_.as_ref().message.as_request_status().unwrap().0.into()
    }

    #[getter]
    fn controller(self_: PyRef<Self>) -> bool {
        self_
            .as_ref()
            .message
            .as_request_status()
            .unwrap()
            .1
            .clone()
    }
}

#[pyclass(
    name = "ReductionType",
    module = "monarch._rust_bindings.monarch_extension.tensor_worker",
    eq,
    eq_int
)]
#[derive(Clone, PartialEq)]
enum PyReduction {
    Stack,
    Sum,
    Prod,
    Max,
    Min,
    Avg,
}

impl From<PyReduction> for Reduction {
    fn from(value: PyReduction) -> Self {
        match value {
            PyReduction::Stack => Reduction::Stack,
            PyReduction::Sum => Reduction::ReduceOp(ReduceOp::Sum),
            PyReduction::Prod => Reduction::ReduceOp(ReduceOp::Prod),
            PyReduction::Max => Reduction::ReduceOp(ReduceOp::Max),
            PyReduction::Min => Reduction::ReduceOp(ReduceOp::Min),
            PyReduction::Avg => Reduction::ReduceOp(ReduceOp::Avg),
        }
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct Reduce;

#[pymethods]
impl Reduce {
    #[new]
    #[pyo3(signature = (*, result, tensor, factory, mesh, dims, stream, scatter, in_place, reduction, out))]
    fn new(
        result: Ref,
        tensor: Ref,
        factory: Factory,
        mesh: Ref,
        dims: Vec<String>,
        stream: StreamRef,
        scatter: bool,
        in_place: bool,
        reduction: PyReduction,
        out: Option<Ref>,
    ) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::Reduce {
                    result,
                    tensor,
                    factory,
                    mesh,
                    stream,
                    dims,
                    reduction: reduction.into(),
                    scatter,
                    in_place,
                    out,
                },
            },
        )
    }

    #[getter]
    fn result(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_reduce().unwrap().0.clone()
    }

    #[getter]
    fn tensor(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_reduce().unwrap().1.clone()
    }

    #[getter]
    fn factory(self_: PyRef<Self>) -> Factory {
        self_.as_ref().message.as_reduce().unwrap().2.clone()
    }

    #[getter]
    fn mesh(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_reduce().unwrap().3.clone()
    }

    #[getter]
    fn stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_reduce().unwrap().4.clone()
    }

    #[getter]
    fn dims(self_: PyRef<Self>) -> Vec<String> {
        self_.as_ref().message.as_reduce().unwrap().5.clone()
    }

    #[getter]
    fn reduction(self_: PyRef<Self>) -> PyReduction {
        match self_.as_ref().message.as_reduce().unwrap().6 {
            Reduction::Stack => PyReduction::Stack,
            Reduction::ReduceOp(ReduceOp::Sum) => PyReduction::Sum,
            Reduction::ReduceOp(ReduceOp::Prod) => PyReduction::Prod,
            Reduction::ReduceOp(ReduceOp::Max) => PyReduction::Max,
            Reduction::ReduceOp(ReduceOp::Min) => PyReduction::Min,
            Reduction::ReduceOp(ReduceOp::Avg) => PyReduction::Avg,
        }
    }

    #[getter]
    fn scatter(self_: PyRef<Self>) -> bool {
        *self_.as_ref().message.as_reduce().unwrap().7
    }

    #[getter]
    fn in_place(self_: PyRef<Self>) -> bool {
        *self_.as_ref().message.as_reduce().unwrap().8
    }

    #[getter]
    fn out(self_: PyRef<Self>) -> Option<Ref> {
        self_.as_ref().message.as_reduce().unwrap().9.clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct SendTensor;

#[pymethods]
impl SendTensor {
    #[new]
    #[pyo3(signature = (*, tensor, from_stream, to_stream, from_ranks, to_ranks, result, factory))]
    fn new(
        tensor: Ref,
        from_stream: StreamRef,
        to_stream: StreamRef,
        from_ranks: &PySlice,
        to_ranks: &PySlice,
        result: Ref,
        factory: Factory,
    ) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::SendTensor {
                    result,
                    from_ranks: from_ranks.clone().into(),
                    to_ranks: to_ranks.clone().into(),
                    tensor,
                    factory,
                    from_stream,
                    to_stream,
                },
            },
        )
    }

    #[getter]
    fn result(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_send_tensor().unwrap().0.clone()
    }

    #[getter]
    fn from_ranks(self_: PyRef<Self>) -> PySlice {
        self_
            .as_ref()
            .message
            .as_send_tensor()
            .unwrap()
            .1
            .clone()
            .into()
    }

    #[getter]
    fn to_ranks(self_: PyRef<Self>) -> PySlice {
        self_
            .as_ref()
            .message
            .as_send_tensor()
            .unwrap()
            .2
            .clone()
            .into()
    }

    #[getter]
    fn tensor(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_send_tensor().unwrap().3.clone()
    }

    #[getter]
    fn factory(self_: PyRef<Self>) -> Factory {
        self_.as_ref().message.as_send_tensor().unwrap().4.clone()
    }

    #[getter]
    fn from_stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_send_tensor().unwrap().5.clone()
    }

    #[getter]
    fn to_stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_send_tensor().unwrap().6.clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct CreatePipe;

#[pymethods]
impl CreatePipe {
    #[new]
    #[pyo3(signature = (*, key, result, mesh, function, max_messages, args, kwargs))]
    fn new(
        key: String,
        result: Ref,
        mesh: Ref,
        function: ResolvableFunction,
        max_messages: i64,
        args: &Bound<'_, PyTuple>,
        kwargs: &Bound<'_, PyDict>,
    ) -> PyResult<(Self, PyWorkerMessage)> {
        let (args, kwargs) = func_call_args_to_wire_values(Some(&function), args, kwargs)?;
        Ok((
            Self,
            PyWorkerMessage {
                message: WorkerMessage::CreatePipe {
                    result,
                    key,
                    function,
                    max_messages,
                    mesh,
                    args,
                    kwargs,
                },
            },
        ))
    }

    #[getter]
    fn result(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_create_pipe().unwrap().0.clone()
    }

    #[getter]
    fn key(self_: PyRef<Self>) -> String {
        self_.as_ref().message.as_create_pipe().unwrap().1.clone()
    }

    #[getter]
    fn function(self_: PyRef<Self>) -> ResolvableFunction {
        self_.as_ref().message.as_create_pipe().unwrap().2.clone()
    }

    #[getter]
    fn max_messages(self_: PyRef<Self>) -> i64 {
        self_.as_ref().message.as_create_pipe().unwrap().3.clone()
    }

    #[getter]
    fn mesh(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_create_pipe().unwrap().4.clone()
    }

    #[getter]
    fn args(self_: PyRef<Self>) -> PyResult<PyObject> {
        wire_values_to_args(
            self_.py(),
            self_.as_ref().message.as_create_pipe().unwrap().5.clone(),
        )
    }

    #[getter]
    fn kwargs(self_: PyRef<Self>) -> PyResult<PyObject> {
        wire_values_to_kwargs(
            self_.py(),
            self_.as_ref().message.as_create_pipe().unwrap().6.clone(),
        )
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct SendValue;

#[pymethods]
impl SendValue {
    #[new]
    #[pyo3(signature = (*, seq, destination, mutates, function, args, kwargs, stream))]
    fn new(
        seq: u64,
        destination: Option<Ref>,
        mutates: Vec<Ref>,
        function: Option<ResolvableFunction>,
        args: &Bound<'_, PyTuple>,
        kwargs: &Bound<'_, PyDict>,
        stream: StreamRef,
    ) -> PyResult<(Self, PyWorkerMessage)> {
        if function.is_none() && (args.len() != 1 || !kwargs.is_empty()) {
            return Err(PyValueError::new_err(
                "SendValue with no function must have exactly one argument and no keyword arguments",
            ));
        }
        let (args, kwargs) = func_call_args_to_wire_values(function.as_ref(), args, kwargs)?;

        Ok((
            Self,
            PyWorkerMessage {
                message: WorkerMessage::SendValue {
                    seq: seq.into(),
                    destination,
                    mutates,
                    function,
                    args,
                    kwargs,
                    stream,
                },
            },
        ))
    }

    #[getter]
    fn seq(self_: PyRef<Self>) -> u64 {
        self_.as_ref().message.as_send_value().unwrap().0.into()
    }

    #[getter]
    fn destination(self_: PyRef<Self>) -> Option<Ref> {
        self_.as_ref().message.as_send_value().unwrap().1.clone()
    }

    #[getter]
    fn mutates(self_: PyRef<Self>) -> Vec<Ref> {
        self_.as_ref().message.as_send_value().unwrap().2.clone()
    }

    #[getter]
    fn function(self_: PyRef<Self>) -> Option<ResolvableFunction> {
        self_.as_ref().message.as_send_value().unwrap().3.clone()
    }

    #[getter]
    fn args(self_: PyRef<Self>) -> PyResult<PyObject> {
        wire_values_to_args(
            self_.py(),
            self_.as_ref().message.as_send_value().unwrap().4.clone(),
        )
    }

    #[getter]
    fn kwargs(self_: PyRef<Self>) -> PyResult<PyObject> {
        wire_values_to_kwargs(
            self_.py(),
            self_.as_ref().message.as_send_value().unwrap().5.clone(),
        )
    }

    #[getter]
    fn stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_send_value().unwrap().6.clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct SplitComm;

#[pymethods]
impl SplitComm {
    #[new]
    #[pyo3(signature = (*, dims, device_mesh, stream))]
    fn new(dims: Vec<String>, device_mesh: Ref, stream: StreamRef) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::SplitComm {
                    dims,
                    device_mesh,
                    stream,
                    config: None,
                },
            },
        )
    }

    #[getter]
    fn dims(self_: PyRef<Self>) -> Vec<String> {
        self_.as_ref().message.as_split_comm().unwrap().0.clone()
    }

    #[getter]
    fn device_mesh(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_split_comm().unwrap().1.clone()
    }
    #[getter]
    fn stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_split_comm().unwrap().2.clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct SplitCommForProcessGroup;

#[pymethods]
impl SplitCommForProcessGroup {
    #[new]
    #[pyo3(signature = (*, remote_process_group, stream))]
    fn new(remote_process_group: Ref, stream: StreamRef) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::SplitCommForProcessGroup {
                    remote_process_group,
                    stream,
                    config: None,
                },
            },
        )
    }

    #[getter]
    fn remote_process_group(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_split_comm().unwrap().1.clone()
    }

    #[getter]
    fn stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_split_comm().unwrap().2.clone()
    }
}

#[pyclass(extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct DefineRecording;

#[pymethods]
impl DefineRecording {
    #[new]
    #[pyo3(signature = (*, result, nresults, nformals, commands, ntotal_messages, index))]
    fn new(
        result: Ref,
        nresults: usize,
        nformals: usize,
        commands: Vec<PyRef<PyWorkerMessage>>,
        ntotal_messages: usize,
        index: usize,
    ) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::DefineRecording {
                    result,
                    nresults,
                    nformals,
                    commands: commands
                        .into_iter()
                        .map(|command| command.message.clone())
                        .collect(),
                    ntotal_messages,
                    index,
                },
            },
        )
    }

    fn append(mut self_: PyRefMut<'_, Self>, command: PyRef<PyWorkerMessage>) -> PyResult<()> {
        self_
            .as_super()
            .deref_mut()
            .message
            .as_define_recording_mut()
            .unwrap()
            .3
            .push(command.message.clone());
        Ok(())
    }

    fn append_call_function(
        mut self_: PyRefMut<'_, Self>,
        seq: u64,
        results: Vec<Option<Ref>>,
        mutates: Vec<Ref>,
        function: ResolvableFunction,
        args: &Bound<'_, PyTuple>,
        kwargs: &Bound<'_, PyDict>,
        stream: StreamRef,
        remote_process_groups: Vec<Ref>,
    ) -> PyResult<()> {
        let (args, kwargs) = func_call_args_to_wire_values(Some(&function), args, kwargs)?;
        self_
            .as_super()
            .deref_mut()
            .message
            .as_define_recording_mut()
            .unwrap()
            .3
            .push(WorkerMessage::CallFunction(CallFunctionParams {
                seq: seq.into(),
                results,
                mutates,
                function,
                args,
                kwargs,
                stream,
                remote_process_groups,
            }));
        Ok(())
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct RecordingFormal;

#[pymethods]
impl RecordingFormal {
    #[new]
    #[pyo3(signature = (*, result, argument_index, stream))]
    fn new(result: Ref, argument_index: usize, stream: StreamRef) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::RecordingFormal {
                    result,
                    argument_index,
                    stream,
                },
            },
        )
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct RecordingResult;

#[pymethods]
impl RecordingResult {
    #[new]
    #[pyo3(signature = (*, result, output_index, stream))]
    fn new(result: Ref, output_index: usize, stream: StreamRef) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::RecordingResult {
                    result,
                    output_index,
                    stream,
                },
            },
        )
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct CallRecording;

#[pymethods]
impl CallRecording {
    #[new]
    #[pyo3(signature = (*, seq, recording, results, actuals))]
    fn new(
        seq: u64,
        recording: Ref,
        results: Vec<Ref>,
        actuals: Vec<Ref>,
    ) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::CallRecording {
                    seq: seq.into(),
                    recording,
                    results,
                    actuals,
                },
            },
        )
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct PipeRecv;

#[pymethods]
impl PipeRecv {
    #[new]
    #[pyo3(signature = (*, seq, pipe, results, stream))]
    fn new(
        seq: u64,
        pipe: Ref,
        results: Vec<Option<Ref>>,
        stream: StreamRef,
    ) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::PipeRecv {
                    seq: seq.into(),
                    results,
                    pipe,
                    stream,
                },
            },
        )
    }

    #[getter]
    fn seq(self_: PyRef<Self>) -> u64 {
        self_.as_ref().message.as_pipe_recv().unwrap().0.into()
    }

    #[getter]
    fn pipe(self_: PyRef<Self>) -> Ref {
        self_.as_ref().message.as_pipe_recv().unwrap().2.clone()
    }

    #[getter]
    fn results(self_: PyRef<Self>) -> Vec<Option<Ref>> {
        self_.as_ref().message.as_pipe_recv().unwrap().1.clone()
    }

    #[getter]
    fn stream(self_: PyRef<Self>) -> StreamRef {
        self_.as_ref().message.as_pipe_recv().unwrap().3.clone()
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct Exit;

#[pymethods]
impl Exit {
    #[new]
    #[pyo3(signature = (*, error_reason))]
    fn new(error_reason: Option<(Option<PyActorId>, String)>) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::Exit {
                    error: error_reason
                        .map(|(actor_id, reason)| (actor_id.map(|id| ActorId::from(&id)), reason)),
                },
            },
        )
    }
}

#[pyclass(frozen, extends=PyWorkerMessage, module = "monarch._rust_bindings.monarch_extension.tensor_worker")]
struct CommandGroup;

#[pymethods]
impl CommandGroup {
    #[new]
    #[pyo3(signature = (*, commands))]
    fn new(commands: Vec<PyRef<PyWorkerMessage>>) -> (Self, PyWorkerMessage) {
        (
            Self,
            PyWorkerMessage {
                message: WorkerMessage::CommandGroup(
                    commands
                        .into_iter()
                        .map(|command| command.message.clone())
                        .collect(),
                ),
            },
        )
    }

    #[getter]
    fn commands(self_: PyRef<Self>) -> PyResult<Vec<PyObject>> {
        let py = self_.py();
        self_
            .as_ref()
            .message
            .as_command_group()
            .unwrap()
            .iter()
            .map(|message| worker_message_to_py(py, message))
            .collect::<Result<Vec<_>, PyErr>>()
    }
}

fn wire_values_to_args(py: Python<'_>, args: Vec<WireValue>) -> PyResult<PyObject> {
    let py_ags = args
        .into_iter()
        .map(|arg| {
            // SAFETY: This is ok as its used just to return the wire value back to the user and not
            // to mutate it.
            unsafe {
                arg.try_to_object_unsafe(py).map_err(|err| {
                    PyValueError::new_err(format!(
                        "Failed to convert a pytree of WireValues to a PyObject: {:?}",
                        err
                    ))
                })
            }
        })
        .collect::<Result<Vec<_>, PyErr>>()?;
    Ok(PyTuple::new(py, py_ags)?.into())
}

fn wire_values_to_kwargs(py: Python<'_>, kwargs: HashMap<String, WireValue>) -> PyResult<PyObject> {
    kwargs
        .into_iter()
        .map(|(k, v)| {
            // SAFETY: This is ok as its used just to return the wire value back to the user and not
            // to mutate it.
            Ok((k.clone(), unsafe {
                v.try_to_object_unsafe(py).map_err(|err| {
                    PyValueError::new_err(format!(
                        "Failed to convert a pytree of WireValues to a PyObject: {:?}",
                        err
                    ))
                })?
            }))
        })
        .collect::<Result<HashMap<_, _>, PyErr>>()?
        .into_py_any(py)
}

// TODO: This can become an impl on WorkerMessage once we adjust crate split with monarch_messages
pub(crate) fn worker_message_to_py(py: Python<'_>, message: &WorkerMessage) -> PyResult<PyObject> {
    let initializer = PyClassInitializer::from(PyWorkerMessage {
        message: message.clone(),
    });
    match message {
        WorkerMessage::BackendNetworkInit { .. } => {
            Py::new(py, initializer.add_subclass(BackendNetworkInit {}))?.into_py_any(py)
        }
        WorkerMessage::BackendNetworkPointToPointInit { .. } => Py::new(
            py,
            initializer.add_subclass(BackendNetworkPointToPointInit {}),
        )?
        .into_py_any(py),
        WorkerMessage::CallFunction { .. } => {
            Py::new(py, initializer.add_subclass(CallFunction {}))?.into_py_any(py)
        }
        WorkerMessage::CreateStream { .. } => {
            Py::new(py, initializer.add_subclass(CreateStream {}))?.into_py_any(py)
        }
        WorkerMessage::CreateRemoteProcessGroup { .. } => {
            Py::new(py, initializer.add_subclass(CreateRemoteProcessGroup {}))?.into_py_any(py)
        }
        WorkerMessage::CreateDeviceMesh { .. } => {
            Py::new(py, initializer.add_subclass(CreateDeviceMesh {}))?.into_py_any(py)
        }
        WorkerMessage::BorrowCreate { .. } => {
            Py::new(py, initializer.add_subclass(BorrowCreate {}))?.into_py_any(py)
        }
        WorkerMessage::BorrowFirstUse { .. } => {
            Py::new(py, initializer.add_subclass(BorrowFirstUse {}))?.into_py_any(py)
        }
        WorkerMessage::BorrowLastUse { .. } => {
            Py::new(py, initializer.add_subclass(BorrowLastUse {}))?.into_py_any(py)
        }
        WorkerMessage::BorrowDrop { .. } => {
            Py::new(py, initializer.add_subclass(BorrowDrop {}))?.into_py_any(py)
        }
        WorkerMessage::DeleteRefs { .. } => {
            Py::new(py, initializer.add_subclass(DeleteRefs {}))?.into_py_any(py)
        }
        WorkerMessage::RequestStatus { .. } => {
            Py::new(py, initializer.add_subclass(RequestStatus {}))?.into_py_any(py)
        }
        WorkerMessage::Reduce { .. } => {
            Py::new(py, initializer.add_subclass(Reduce {}))?.into_py_any(py)
        }
        WorkerMessage::SendTensor { .. } => {
            Py::new(py, initializer.add_subclass(SendTensor {}))?.into_py_any(py)
        }
        WorkerMessage::CreatePipe { .. } => {
            Py::new(py, initializer.add_subclass(CreatePipe {}))?.into_py_any(py)
        }
        WorkerMessage::SendValue { .. } => {
            Py::new(py, initializer.add_subclass(SendValue {}))?.into_py_any(py)
        }
        WorkerMessage::PipeRecv { .. } => {
            Py::new(py, initializer.add_subclass(PipeRecv {}))?.into_py_any(py)
        }
        WorkerMessage::SplitComm { .. } => {
            Py::new(py, initializer.add_subclass(SplitComm {}))?.into_py_any(py)
        }
        WorkerMessage::SplitCommForProcessGroup { .. } => {
            Py::new(py, initializer.add_subclass(SplitCommForProcessGroup {}))?.into_py_any(py)
        }
        WorkerMessage::Exit { .. } => {
            Py::new(py, initializer.add_subclass(Exit {}))?.into_py_any(py)
        }
        WorkerMessage::CommandGroup { .. } => {
            Py::new(py, initializer.add_subclass(CommandGroup {}))?.into_py_any(py)
        }
        WorkerMessage::DefineRecording { .. } => {
            Py::new(py, initializer.add_subclass(DefineRecording {}))?.into_py_any(py)
        }
        WorkerMessage::RecordingFormal { .. } => {
            Py::new(py, initializer.add_subclass(RecordingFormal {}))?.into_py_any(py)
        }
        WorkerMessage::RecordingResult { .. } => {
            Py::new(py, initializer.add_subclass(RecordingResult {}))?.into_py_any(py)
        }
        WorkerMessage::CallRecording { .. } => {
            Py::new(py, initializer.add_subclass(CallRecording {}))?.into_py_any(py)
        }
        WorkerMessage::SetRefUnitTestsOnly { .. } => unimplemented!(),
        WorkerMessage::GetRefUnitTestsOnly { .. } => unimplemented!(),
        WorkerMessage::SendResultOfActorCall { .. } => unimplemented!(),
        WorkerMessage::CallActorMethod { .. } => unimplemented!(),
    }
}

pub(crate) fn register_python_bindings(worker_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    worker_mod.add_class::<PyWorkerMessage>()?;
    worker_mod.add_class::<BackendNetworkInit>()?;
    worker_mod.add_class::<BackendNetworkPointToPointInit>()?;
    worker_mod.add_class::<CallFunction>()?;
    worker_mod.add_class::<CommandGroup>()?;
    worker_mod.add_class::<CreateStream>()?;
    worker_mod.add_class::<CreateDeviceMesh>()?;
    worker_mod.add_class::<CreateRemoteProcessGroup>()?;
    worker_mod.add_class::<BorrowCreate>()?;
    worker_mod.add_class::<BorrowFirstUse>()?;
    worker_mod.add_class::<BorrowLastUse>()?;
    worker_mod.add_class::<BorrowDrop>()?;
    worker_mod.add_class::<DeleteRefs>()?;
    worker_mod.add_class::<RequestStatus>()?;
    worker_mod.add_class::<Reduce>()?;
    worker_mod.add_class::<SendTensor>()?;
    worker_mod.add_class::<CreatePipe>()?;
    worker_mod.add_class::<SendValue>()?;
    worker_mod.add_class::<PipeRecv>()?;
    worker_mod.add_class::<Exit>()?;
    worker_mod.add_class::<Ref>()?;
    worker_mod.add_class::<StreamRef>()?;
    worker_mod.add_class::<Factory>()?;
    worker_mod.add_class::<FunctionPath>()?;
    worker_mod.add_class::<Cloudpickle>()?;
    worker_mod.add_class::<PyReduction>()?;
    worker_mod.add_class::<StreamCreationMode>()?;
    worker_mod.add_class::<SplitComm>()?;
    worker_mod.add_class::<SplitCommForProcessGroup>()?;
    worker_mod.add_class::<DefineRecording>()?;
    worker_mod.add_class::<RecordingFormal>()?;
    worker_mod.add_class::<RecordingResult>()?;
    worker_mod.add_class::<CallRecording>()?;

    Ok(())
}
