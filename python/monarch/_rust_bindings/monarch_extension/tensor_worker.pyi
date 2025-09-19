# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Callable, final, Optional, Sequence, Tuple

import torch
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.shape import Slice

@final
class Ref:
    """
    A reference to a value that exists on the worker and is used by other
    actors such as controller, client etc to reference the value.
    TODO: This is used for all types of values like tensors, streams, pipes etc.
    But should be split into separate types for each of them.

    Args:
    - `id`: The id of the value on the worker.
    """

    def __init__(self, id: int) -> None: ...
    @property
    def id(self) -> int:
        """The id of the value on the worker."""
        ...

    def __repr__(self) -> str: ...
    def __lt__(self, other: Ref) -> bool: ...
    def __le__(self, other: Ref) -> bool: ...
    def __eq__(self, value: Ref) -> bool: ...
    def __ne__(self, value: Ref) -> bool: ...
    def __gt__(self, other: Ref) -> bool: ...
    def __ge__(self, other: Ref) -> bool: ...
    def __hash__(self) -> int: ...
    def __getnewargs_ex__(self) -> tuple[tuple, dict]: ...

@final
class StreamRef:
    """
    A reference to a stream that exists on the worker and is used by other
    actors such as controller, client etc to reference it.

    Args:
    - `id`: The id of the stream on the worker.
    """

    def __init__(self, *, id: int) -> None: ...
    @property
    def id(self) -> int:
        """The id of the stream on the worker."""
        ...

    def __repr__(self) -> str: ...
    def __lt__(self, other: Ref) -> bool: ...
    def __le__(self, other: Ref) -> bool: ...
    def __eq__(self, value: Ref) -> bool: ...
    def __ne__(self, value: Ref) -> bool: ...
    def __gt__(self, other: Ref) -> bool: ...
    def __ge__(self, other: Ref) -> bool: ...
    def __hash__(self) -> int: ...

@final
class TensorFactory:
    """
    Factory class to hold necessary metadata to create tensors on the worker.

    Args:
    - `size`: The size of the tensor.
    - `dtype`: The data type of the tensor.
    - `layout`: The layout of the tensor.
    - `device`: The device of the tensor. (TODO: support torch.device)
    """

    def __init__(
        self,
        *,
        size: Sequence[int],
        # pyre-ignore
        dtype: torch.dtype,
        # pyre-ignore
        layout: torch.layout,
        # pyre-ignore
        device: torch.device,
    ) -> None: ...
    @property
    def size(self) -> tuple[int, ...]:
        """The size of the tensor."""
        ...

    @property
    def dtype(self) -> torch.dtype:
        """The data type of the tensor."""
        ...

    @property
    def layout(self) -> torch.layout:
        """The layout of the tensor."""
        ...

    @property
    def device(self) -> str:
        """The device of the tensor."""
        ...

@final
class FunctionPath:
    """
    The fully qualified path to a function on the worker.

    Args:
    - `path`: The path to the function eg. `builtins.range`
    """

    def __init__(self, *, path: str) -> None: ...
    @property
    def path(self) -> str:
        """The path to the function."""
        ...

    def __repr__(self) -> str: ...
    def resolve(self) -> Callable[..., object]:
        """Resolve the function path to a callable."""
        ...

@final
class Cloudpickle:
    """
    A serialized function to run remotely.

    Args:
    - `func`: The function wrap
    """

    def __init__(self, *, bytes: bytes) -> None: ...
    def __repr__(self) -> str: ...
    def resolve(self) -> Callable[..., object]:
        """Resolve the live function to a callable."""
        ...

ResolvableFunction = FunctionPath | Cloudpickle

@final
class StreamCreationMode:
    """
    Used to specify what CUDA stream to use for the worker stream creation.
    """

    UseDefaultStream: StreamCreationMode
    CreateNewStream: StreamCreationMode

    def __eq__(self, value: StreamCreationMode) -> bool: ...
    def __ne__(self, value: StreamCreationMode) -> bool: ...
    def __repr__(self) -> str: ...
    def __int__(self) -> int: ...

@final
class ReductionType:
    """Used to specify the reduction type for the Reduce command."""

    Stack: ReductionType
    Sum: ReductionType
    Prod: ReductionType
    Max: ReductionType
    Min: ReductionType
    Avg: ReductionType

    def __eq__(self, value: ReductionType) -> bool: ...
    def __ne__(self, value: ReductionType) -> bool: ...
    def __repr__(self) -> str: ...
    def __int__(self) -> int: ...

class WorkerMessage:
    """
    The base class for all messages that can be sent to the worker.
    This class is not meant to be instantiated or inherited directly.
    Instead, use the subclasses of this class to send messages to
    the worker.
    TODO: Expose all subclasses as attributes of this class.
    """

    ...

@final
class BackendNetworkInit(WorkerMessage):
    """Instruct the worker to initialize the backend network."""

    def __init__(self) -> None: ...

@final
class BackendNetworkPointToPointInit(WorkerMessage):
    """Instruct the worker to initialize the backend network for point-to-point communication."""

    def __init__(self, *, from_stream: StreamRef, to_stream: StreamRef) -> None: ...
    @property
    def from_stream(self) -> StreamRef:
        """Reference to the src stream to use for the point-to-point communication."""
        ...

    @property
    def to_stream(self) -> StreamRef:
        """Reference to the dst stream to use for the point-to-point communication."""
        ...

@final
class CallFunction(WorkerMessage):
    """
    Instruct the worker to call a function, either a torch op
    or a Python `remote_function`.

    Args:
    - `seq`: Sequence number of the message.
    - `results`: References to the values that the function returns.
    - `mutates`: References to the values that the function mutates.
    - `function`: Fully qualified path to the function.
    - `args`: Pytree-serializable arguments to the function.
    - `kwargs`: Pytree-serializable keyword arguments to the function.
    - `stream`: Reference to the stream the worker should use to execute the function.
    - `remote_process_groups`: References to the process groups the worker should use to execute the function.
    """

    def __init__(
        self,
        *,
        seq: int,
        results: Sequence[Ref | None],
        mutates: Sequence[Ref],
        function: ResolvableFunction,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        stream: StreamRef,
        remote_process_groups: Sequence[Ref],
    ) -> None: ...
    @property
    def seq(self) -> int:
        """Sequence number of the message."""
        ...

    @property
    def results(self) -> list[Ref | None]:
        """References to the values that the function returns."""
        ...

    @property
    def mutates(self) -> list[Ref]:
        """References to the values that the function mutates."""
        ...

    @property
    def function(self) -> ResolvableFunction:
        """Fully qualified path to the function."""
        ...

    @property
    def args(self) -> tuple[object, ...]:
        """
        Pytree-serializable arguments to the function.
        Accessing this property can be expensive as it clones.
        """
        ...

    @property
    def kwargs(self) -> dict[str, object]:
        """
        Pytree-serializable keyword arguments to the function.
        Accessing this property can be expensive as it clones.
        """
        ...

    @property
    def stream(self) -> StreamRef:
        """Reference to the stream the worker should use to execute the function."""
        ...

    @property
    def remote_process_groups(self) -> list[Ref]:
        """References to the process groups the worker should use to execute the function."""
        ...

@final
class CreateStream(WorkerMessage):
    """
    Instruct the worker to create a new stream. Worker will execute commands
    on streams concurrently.

    Args:
    - `id`: The id of the stream on the worker.
    - `stream_creation`: The CUDA stream to use for the created stream.
    """

    def __init__(
        self, *, id: StreamRef, stream_creation: StreamCreationMode
    ) -> None: ...
    @property
    def id(self) -> StreamRef:
        """The id of the stream on the worker."""
        ...

    @property
    def stream_creation(self) -> StreamCreationMode:
        """The CUDA stream to use for the created stream."""
        ...

@final
class CreateDeviceMesh(WorkerMessage):
    """
    Instruct the worker to create a new device mesh which can be used to schedule
    efficient inter-worker communication.

    Args:
    - `result`: Reference to the created device mesh.
    - `names`: Names of the dimensions in the device mesh.
    - `ranks`: Multi-dimensional slice of the ranks of the devices in the device mesh.
        The number of dimensions must match the number of names.
    """

    def __init__(self, *, result: Ref, names: Sequence[str], ranks: Slice) -> None: ...
    @property
    def result(self) -> Ref:
        """The reference to the created device mesh."""
        ...

    @property
    def names(self) -> list[str]:
        """The names of the dimensions in the device mesh."""
        ...

    @property
    def ranks(self) -> Slice:
        """The multi-dimensional slice of the ranks of the devices in the device mesh."""
        ...

@final
class CreateRemoteProcessGroup(WorkerMessage):
    """
    Instruct the worker to create a new PyTorch process group to allow UDFs to
    perform collectives.

    Args:
    - `result`: Reference to the created process group.
    - `device_mesh`: Device mesh to create group on.
    - `dims`: Device mesh dimensions group should use.
    """

    def __init__(
        self, *, result: Ref, device_mesh: Ref, dims: Sequence[str]
    ) -> None: ...
    @property
    def result(self) -> Ref:
        """The reference to the created process group."""
        ...

    @property
    def device_mesh(self) -> Ref:
        """The names of the dimensions in the device mesh."""
        ...

    @property
    def dims(self) -> list[str]:
        """The device mesh dimension to communicate over."""
        ...

@final
class BorrowCreate(WorkerMessage):
    """
    Instruct the worker to create a borrow of a tensor from one stream to another.

    Args:
    - `result`: Reference to the resulting borrowed tensor
    - `borrow`: The ID for the borrow.
    - `tensor`: Reference to the tensor to borrow.
    - `from_stream`: Reference to the stream to borrow from.
    - `to_stream`: Reference to the stream to borrow to.
    """

    def __init__(
        self,
        *,
        result: Ref,
        borrow: int,
        tensor: Ref,
        from_stream: StreamRef,
        to_stream: StreamRef,
    ) -> None: ...
    @property
    def result(self) -> Ref:
        """The reference to the resulting borrowed tensor."""
        ...

    @property
    def borrow(self) -> int:
        """The ID for the borrow."""
        ...

    @property
    def tensor(self) -> Ref:
        """The reference to the tensor to borrow."""
        ...

    @property
    def from_stream(self) -> StreamRef:
        """The reference to the stream to borrow from."""
        ...

    @property
    def to_stream(self) -> StreamRef:
        """The reference to the stream to borrow to."""
        ...

@final
class BorrowFirstUse(WorkerMessage):
    """
    A synchronization marker for the worker on first use of the borrowed tensor.

    Args:
    - borrow: The ID for the borrow.
    """

    def __init__(self, *, borrow: int) -> None: ...
    @property
    def borrow(self) -> int:
        """The ID for the borrow."""
        ...

@final
class BorrowLastUse(WorkerMessage):
    """
    A synchronization marker for the worker on last use of the borrowed tensor.

    Args:
    - borrow: The ID for the borrow.
    """

    def __init__(self, *, borrow: int) -> None: ...
    @property
    def borrow(self) -> int:
        """The ID for the borrow."""
        ...

@final
class BorrowDrop(WorkerMessage):
    """
    Instruct the worker to drop a borrow of a tensor.

    Args:
    - borrow: The ID for the borrow.
    """

    def __init__(self, *, borrow: int) -> None: ...
    @property
    def borrow(self) -> int:
        """The ID for the borrow."""
        ...

@final
class DeleteRefs(WorkerMessage):
    """
    Instruct the worker to delete the values referenced by the given refs
    from its state.

    Args:
    - refs: References to the values to delete.
    """

    def __init__(self, *, refs: Sequence[Ref]) -> None: ...
    @property
    def refs(self) -> list[Ref]:
        """References to the values to delete."""
        ...

@final
class RequestStatus(WorkerMessage):
    """
    Instruct the worker to respond back when all the messages before this
    message have been processed on all streams.

    Args:
    - seq: Sequence number of the message.
    - controller: Whether this message was sent by the controller.
    """

    def __init__(self, *, seq: int, controller: bool) -> None: ...
    @property
    def seq(self) -> int:
        """Sequence number of the message."""
        ...

    @property
    def controller(self) -> bool:
        """If this message was sent by the controller."""
        ...

@final
class Reduce(WorkerMessage):
    """
    Perform a reduction operation, using an efficient communication backend.

    Args:
    - `result`: Reference to the resulting tensor.
    - `tensor`: Reference to the tensor to reduce.
    - `factory`: Tensor metadata to create the resulting tensor if `tensor`
        is not available for some reason.
    - `mesh`: Reference to the device mesh to use for the reduction.
    - `stream`: Reference to the stream to use for the reduction.
    - `dims`: The dimensions of the device mesh to reduce over.
    - `reduction`: The reduction type to use for the reduction.
    - `scatter`: Whether to evenly split the resulting tensor across the ranks
        of the request `dim` in the device mesh.
    - `in_place`: Whether to perform the reduction in-place on `tensor`.
    """

    def __init__(
        self,
        *,
        result: Ref,
        tensor: Ref,
        factory: TensorFactory,
        mesh: Ref,
        stream: StreamRef,
        dims: Sequence[str],
        reduction: ReductionType,
        scatter: bool,
        in_place: bool,
        out: Ref | None,
    ) -> None: ...
    @property
    def result(self) -> Ref:
        """Reference to the resulting tensor."""
        ...

    @property
    def tensor(self) -> Ref:
        """Reference to the tensor to reduce."""
        ...

    @property
    def factory(self) -> TensorFactory:
        """
        Tensor metadata to create the resulting tensor if `tensor` is not
        available for some reason.
        """
        ...

    @property
    def mesh(self) -> Ref:
        """Reference to the device mesh to use for the reduction."""
        ...

    @property
    def stream(self) -> StreamRef:
        """Reference to the stream to use for the reduction."""
        ...

    @property
    def dims(self) -> list[str]:
        """The dimension of the device mesh to reduce over."""
        ...

    @property
    def reduction(self) -> ReductionType:
        """The reduction type to use for the reduction."""
        ...

    @property
    def scatter(self) -> bool:
        """
        Whether to evenly split the resulting tensor across the ranks of the
        request `dim` in the device mesh.
        """
        ...

    @property
    def in_place(self) -> bool:
        """Whether to perform the reduction in-place on `tensor`."""
        ...

    @property
    def out(self) -> Ref:
        """Reference to the out tensor."""
        ...

@final
class SendTensor(WorkerMessage):
    """
    Send a tenser from one slice of ranks to another slice of ranks.

    Args:
    - `result`: Reference to the resulting tensor.
    - `from_ranks`: Slice of ranks to send the tensor from.
    - `to_ranks`: Slice of ranks to send the tensor to.
    - `tensor`: Reference to the tensor to send.
    - `factory`: Tensor metadata to create the resulting tensor if `tensor`
        is not available for some reason.
    - `from_stream`: Reference to the src stream to use for this operation.
    - `to_stream`: Reference to the dst stream to use for this operation.
    """

    def __init__(
        self,
        *,
        result: Ref,
        from_ranks: Slice,
        to_ranks: Slice,
        tensor: Ref,
        factory: TensorFactory,
        from_stream: StreamRef,
        to_stream: StreamRef,
    ) -> None: ...
    @property
    def result(self) -> Ref:
        """Reference to the resulting tensor."""
        ...

    @property
    def from_ranks(self) -> Slice:
        """Slice of ranks to send the tensor from."""
        ...

    @property
    def to_ranks(self) -> Slice:
        """Slice of ranks to send the tensor to."""
        ...

    @property
    def tensor(self) -> Ref:
        """Reference to the tensor to send."""
        ...

    @property
    def factory(self) -> TensorFactory:
        """
        Tensor metadata to create the resulting tensor if `tensor` is not
        available for some reason.
        """
        ...

    @property
    def from_stream(self) -> StreamRef:
        """Reference to the src stream to use for this operation."""
        ...

    @property
    def to_stream(self) -> StreamRef:
        """Reference to the dst stream to use for this operation."""
        ...

@final
class CreatePipe(WorkerMessage):
    """
    Create a pipe on the worker.

    Args:
    - `result`: Reference to the resulting pipe.
    - `key`: The key of the pipe this mainly exists for backwards compatibility
        with the python impl.
    - `function`: Fully qualified path to the function to call to create the pipe.
    - `max_messages`: Maximum number of messages to buffer in the pipe.
    - `mesh`: Reference to the device mesh on which the pipes have been created.
    - `args`: Pytree-serializable arguments to the function.
    - `kwargs`: Pytree-serializable keyword arguments to the function.
    """

    def __init__(
        self,
        *,
        result: Ref,
        key: str,
        function: ResolvableFunction,
        max_messages: int,
        mesh: Ref,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None: ...
    @property
    def result(self) -> Ref:
        """Reference to the resulting pipe."""
        ...

    @property
    def key(self) -> str:
        """The key of the pipe this mainly exists for backwards compatibility with the python impl."""
        ...

    @property
    def function(self) -> ResolvableFunction:
        """Fully qualified path to the function to call to create the pipe."""
        ...

    @property
    def max_messages(self) -> int:
        """Maximum number of messages to buffer in the pipe."""
        ...

    @property
    def mesh(self) -> Ref:
        """Reference to the device mesh on which the pipes have been created."""
        ...

    @property
    def args(self) -> tuple[object, ...]:
        """
        Pytree-serializable arguments to the function.
        Accessing this property can be expensive as it clones.
        """
        ...

    @property
    def kwargs(self) -> dict[str, object]:
        """
        Pytree-serializable keyword arguments to the function.
        Accessing this property can be expensive as it clones.
        """
        ...

@final
class SendValue(WorkerMessage):
    """
    Send a value from one slice of ranks to another slice of ranks.

    Args:
    - `seq`: Sequence number of the message.
    - `destination`: Reference to the destination (Pipe) of the value. If `None`
        the value will be sent to the controller.
    - `function`: Fully qualified path to the function to call to transform the
        value before sending it.
    - `mutates`: References to the values that the function mutates.
    - `args`: Pytree-serializable arguments to the function. If `function` is
        `None` this must be a single value to send.
    - `kwargs`: Pytree-serializable keyword arguments to the function. If
        `function` is `None` this must be empty.
    - `stream`: Reference to the stream the worker should use to execute the
        operation.
    """

    def __init__(
        self,
        *,
        seq: int,
        destination: Ref | None,
        function: ResolvableFunction | None,
        mutates: Sequence[Ref],
        args: tuple[object, ...],
        kwargs: dict[str, object],
        stream: StreamRef,
    ) -> None: ...
    @property
    def seq(self) -> int:
        """Sequence number of the message."""
        ...

    @property
    def destination(self) -> Ref | None:
        """Reference to the destination (Pipe) of the value. If `None` the value will be sent to the controller."""
        ...

    @property
    def function(self) -> ResolvableFunction | None:
        """Fully qualified path to the function to call to transform the value before sending it."""
        ...

    @property
    def mutates(self) -> list[Ref]:
        """References to the values that the function mutates."""
        ...

    @property
    def args(self) -> list[object]:
        """
        Pytree-serializable arguments to the function.
        If `function` is `None` this must be a single value to send.
        Accessing this property can be expensive as it clones.
        """
        ...

    @property
    def kwargs(self) -> dict[str, object]:
        """
        Pytree-serializable keyword arguments to the function.
        If `function` is `None` this must be empty.
        Accessing this property can be expensive as it clones.
        """
        ...

    @property
    def stream(self) -> StreamRef:
        """Reference to the stream the worker should use to execute the operation."""
        ...

@final
class PipeRecv(WorkerMessage):
    """
    Receive a value from a pipe.

    Args:
    - `seq`: Sequence number of the message.
    - `pipe`: Reference to the pipe to receive from.
    - `results`: References to the values that the pipe returns.
    - `stream`: Reference to the stream the worker should use to execute the
        operation.
    """

    def __init__(
        self,
        *,
        seq: int,
        pipe: Ref,
        results: Sequence[Ref | None],
        stream: StreamRef,
    ) -> None: ...
    @property
    def seq(self) -> int:
        """Sequence number of the message."""
        ...

    @property
    def pipe(self) -> Ref:
        """Reference to the pipe to receive from."""
        ...

    @property
    def results(self) -> list[Ref | None]:
        """References to the values that the pipe returns."""
        ...

    @property
    def stream(self) -> StreamRef:
        """Reference to the stream the worker should use to execute the operation."""
        ...

@final
class CommandGroup(WorkerMessage):
    """
    A group of commands that should be executed on the worker.

    Args:
    - `commands`: The commands to execute.
    """

    def __init__(self, *, commands: Sequence[WorkerMessage]) -> None: ...
    @property
    def commands(self) -> list[WorkerMessage]:
        """The commands to execute."""
        ...

@final
class Exit(WorkerMessage):
    """Instruct the worker to exit."""

    def __init__(
        self, *, error_reason: Optional[tuple[Optional[ActorId], str]]
    ) -> None: ...

@final
class SplitComm(WorkerMessage):
    """
    Create a new communicator on each rank in `ranks`, capable of communicating
    with its peers along the specified dimensions.

    Args:
    - `dims`: The device mesh dimensions along which the constructed
        communicator should be able to exchange data.
    - `device_mesh`: The device mesh associated with the new communicator. One
        communicator will be created for every member of the mesh.
    - `stream`: The stream associated with the communicator.  Communicator
        operations will be ordered with respect to other operations scheduled on
        this stream.
    """

    def __init__(
        self,
        *,
        dims: Sequence[str],
        device_mesh: Ref,
        stream: StreamRef,
    ) -> None: ...
    @property
    def dims(self) -> Sequence[str]:
        """
        The device mesh dimensions along which the constructed communicator
        should be able to exchange data.
        """
        ...

    @property
    def device_mesh(self) -> Ref:
        """
        The device mesh associated with the new communicator. One
        communicator will be created for every member of the mesh.
        """
        ...

    @property
    def stream(self) -> StreamRef:
        """
        The stream associated with the communicator.  Communicator operations
        will be ordered with respect to other operations scheduled on this
        stream.
        """
        ...

@final
class SplitCommForProcessGroup(WorkerMessage):
    """
    Create a new communicator for the given `remote_process_group` for the given
    `stream`, capable of communicating with its peers along the specified
    dimensions.

    Args:
    - `remote_process_group`: The process group associated with the new
        communicator. One communicator will be created for every member of the
        mesh.
    - `stream`: The stream associated with the communicator.  Communicator
        operations will be ordered with respect to other operations scheduled on
        this stream.
    """

    def __init__(
        self,
        *,
        remote_process_group: Ref,
        stream: StreamRef,
    ) -> None: ...
    @property
    def remote_process_group(self) -> Ref:
        """
        The remote process group associated with the new communicator.  One
        communicator will be created for every member of the mesh.
        """
        ...

    @property
    def stream(self) -> StreamRef:
        """
        The stream associated with the communicator.  Communicator operations
        will be ordered with respect to other operations scheduled on this
        stream.
        """
        ...

@final
class DefineRecording(WorkerMessage):
    """
    Defines (part of) a new recording on the worker. This is a list of commands
    representing the execution of a function that was defined using
    monarch.compile. If there are too many commands to send in a single
    DefineRecording message, the commands may be chunked into `ntotal_messages`,
    with the `index` field indicating how to order the DefineRecording messages
    for a single recording.

    Args:
    - `result`: The ref associated with this recording that will be used to
        call it in the future.
    - `nresults`: The number of output tensors.
    - `nformals`: The number of input tensors.
    - `commands`: The list of commands to run.
    - `ntotal_messages`: How many total DefineRecording messages make up this
        recording.
    - `index`: This DefineRecording message's index in the set of messages
        that make up this recording.
    """

    def __init__(
        self,
        *,
        result: Ref,
        nresults: int,
        nformals: int,
        commands: Sequence[WorkerMessage],
        ntotal_messages: int,
        index: int,
    ) -> None: ...
    def append(self, command: WorkerMessage) -> None:
        """
        Append a command to the DefineRecording.

        Args:
        - `command`: The WorkerMessage to append.
        """
        ...

    def append_call_function(
        self,
        *,
        seq: int,
        results: Sequence[Ref | None],
        mutates: Sequence[Ref],
        function: ResolvableFunction,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        stream: StreamRef,
        remote_process_groups: Sequence[Ref],
    ) -> None:
        """
        Append a CallFunction command to the DefineRecording.

        Args:
        - `seq`: Sequence number of the message.
        - `results`: References to the values that the function returns.
        - `mutates`: References to the values that the function mutates.
        - `function`: Fully qualified path to the function.
        - `args`: Pytree-serializable arguments to the function.
        - `kwargs`: Pytree-serializable keyword arguments to the function.
        - `stream`: Reference to the stream the worker should use to execute the function.
        - `remote_process_groups`: References to the process groups the worker should use to execute the function.
        """
        ...

@final
class RecordingFormal(WorkerMessage):
    """
    Defines an input tensor for a recording.

    Args:
    - `result`: The ref that will be used to pass the input tensor to the
        recording.
    - `argument_index`: The index of the input tensor in the list of input tensors.
    - `stream`: The stream that this input tensor will be used on.
    """

    def __init__(
        self,
        *,
        result: Ref,
        argument_index: int,
        stream: StreamRef,
    ) -> None: ...

@final
class RecordingResult(WorkerMessage):
    """
    Defines an output tensor for a recording.

    Args:
    - `result`: The ref that will be used to store the output tensor.
    - `output_index`: The index of the output tensor in the list of output tensors.
    - `stream`: The stream that this output tensor will come from.
    """

    def __init__(
        self,
        *,
        result: Ref,
        output_index: int,
        stream: StreamRef,
    ) -> None: ...

@final
class CallRecording(WorkerMessage):
    """
    Calls a recording that was previously defined using DefineRecording.

    Args:
    - `seq`: The sequence number of the invocation.
    - `recording`: The ref of the recording to call.
    - `results`: The list of refs where the result tensors from the recording
        will be stored.
    - `actuals`: The list of refs of input tensors to the recording.
    """

    def __init__(
        self,
        *,
        seq: int,
        recording: Ref,
        results: Sequence[Ref],
        actuals: Sequence[Ref],
    ) -> None: ...
