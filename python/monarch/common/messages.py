# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

from traceback import FrameSummary
from typing import (
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
)

from monarch._rust_bindings.monarch_extension import tensor_worker
from monarch._rust_bindings.monarch_hyperactor.mailbox import Mailbox
from monarch._src.actor.shape import NDSlice
from monarch.common.function import ResolvableFromCloudpickle, ResolvableFunction
from monarch.common.invocation import DeviceException, RemoteException
from monarch.common.reference import Referenceable
from monarch.common.tree import flattener
from pyre_extensions import none_throws

from .tensor_factory import TensorFactory

if TYPE_CHECKING:
    from monarch.common.stream import StreamRef

    from .device_mesh import DeviceMesh, RemoteProcessGroup
    from .pipe import Pipe
    from .recording import Recording
    from .tensor import Tensor


Dims = Tuple[str, ...]


def _to_rust_function(
    x: ResolvableFunction,
) -> tensor_worker.ResolvableFunction:
    if isinstance(x, ResolvableFromCloudpickle):
        return tensor_worker.Cloudpickle(bytes=x.data)
    return tensor_worker.FunctionPath(path=str(x))


def _result_to_references(result: object) -> List[tensor_worker.Ref | None]:
    """
    Flatten the result pytree.
    Only keep the referenceables and leave the rest as None.
    The workers will generate the full result list so we know
    what referenceables to be assigned to.
    """
    leaves = flattener(result, lambda x: True)(result)
    return [
        _ref(leaf)
        if isinstance(leaf, Referenceable) or isinstance(leaf, tensor_worker.Ref)
        else None
        for leaf in leaves
    ]


def _ref(r: Referenceable | tensor_worker.Ref) -> tensor_worker.Ref:
    if isinstance(r, Referenceable):
        return tensor_worker.Ref(id=none_throws(r.ref))
    return r


class CreateDeviceMesh(NamedTuple):
    result: DeviceMesh
    names: Dims
    ranks: NDSlice


class CreateStream(NamedTuple):
    result: "StreamRef"
    default: bool


class CreateRemoteProcessGroup(NamedTuple):
    result: Referenceable
    device_mesh: DeviceMesh
    dims: Dims


class CallFunction(NamedTuple):
    ident: int
    result: object  # pytree with tensors in it
    mutates: Tuple[Tensor | tensor_worker.Ref, ...]
    function: ResolvableFunction
    args: Tuple[object, ...]
    kwargs: Dict[str, object]
    stream: "StreamRef"
    device_mesh: DeviceMesh
    remote_process_groups: List[RemoteProcessGroup]


class Exit(NamedTuple):
    destroy_pg: bool
    error: Optional[RemoteException | DeviceException | Exception]


class CommandGroup(NamedTuple):
    commands: List[NamedTuple]


class RecordingFormal(NamedTuple):
    result: Tensor | tensor_worker.Ref
    argument_index: int
    stream: "StreamRef"


class RecordingResult(NamedTuple):
    input: Tensor | tensor_worker.Ref
    output_index: int
    stream: "StreamRef"


class DefineRecording(NamedTuple):
    result: Recording
    nresults: int
    nformals: int
    commands: List[NamedTuple]
    ntotal_messages: int
    message_index: int


class CallRecording(NamedTuple):
    ident: int
    recording: Recording
    results: List[Tensor | tensor_worker.Ref]
    actuals: List[Tensor | tensor_worker.Ref]


class DeleteRefs(NamedTuple):
    refs: List[int]


# This is worker <> controller/backend comms only will be supported differently
class Restarted(NamedTuple):
    result: int


class SendValue(NamedTuple):
    ident: int
    destination: Pipe | None  # if present the pipe along which to send the result,
    # otherwise send FetchResult to controller
    mutates: Tuple[Tensor | tensor_worker.Ref, ...]
    function: ResolvableFunction | None  # None is equivalent to lambda x: x
    args: Tuple[object, ...]
    kwargs: Dict[str, object]
    stream: StreamRef


# Worker -> Controller comm only handled differently
class FetchResult(NamedTuple):
    ident: int
    value: object


# Worker -> Controller comm only handled differently
class RemoteFunctionFailed(NamedTuple):
    failing_ident: int
    stack_offset: int
    exception: Exception
    worker_frames: List[FrameSummary]


# Worker -> Controller comm only handled differently
class InternalException(NamedTuple):
    exception: Exception
    frames: List[FrameSummary]


# Worker -> Controller comm only handled differently
class RemoteGeneratorFailed(NamedTuple):
    exception: Exception
    frames: List[FrameSummary]


# Worker -> Controller comm only handled differently
class Status(NamedTuple):
    first_uncompleted_ident: int


# When the controller is waiting on a status update,
# it will request one even if it is before the
# periodic one.
class RequestStatus(NamedTuple):
    ident: int
    controller: bool


class BorrowCreate(NamedTuple):
    result: Tensor | tensor_worker.Ref
    borrow: int
    tensor: Tensor | tensor_worker.Ref
    from_stream: StreamRef
    to_stream: StreamRef


class BorrowDrop(NamedTuple):
    borrow: int  # id of borrowed tensor


class BorrowFirstUse(NamedTuple):
    borrow: int  # id of borrowed tensor


class BorrowLastUse(NamedTuple):
    borrow: int  # id of borrowed tensor


class SendTensor(NamedTuple):
    result: Tensor | tensor_worker.Ref
    from_ranks: NDSlice
    to_ranks: NDSlice
    tensor: Tensor | tensor_worker.Ref
    factory: TensorFactory
    from_stream: StreamRef
    to_stream: StreamRef


class SendResultOfActorCall(NamedTuple):
    seq: int
    broker_id: Tuple[str, int]
    local_state: Sequence[Tensor | tensor_worker.Ref]
    mutates: List[tensor_worker.Ref]
    stream: tensor_worker.StreamRef


class CallActorMethod(NamedTuple):
    seq: int
    result: object
    broker_id: Tuple[str, int]
    local_state: Sequence[Tensor | tensor_worker.Ref]
    mutates: List[tensor_worker.Ref]
    stream: tensor_worker.StreamRef


class SplitComm(NamedTuple):
    dims: Dims
    device_mesh: DeviceMesh
    stream: StreamRef


class SplitCommForProcessGroup(NamedTuple):
    remote_process_group: DeviceMesh
    stream: StreamRef


class Reduce(NamedTuple):
    result: Tensor | tensor_worker.Ref
    local_tensor: Tensor | tensor_worker.Ref
    factory: TensorFactory
    source_mesh: DeviceMesh
    stream: StreamRef
    dims: Dims
    reduction: str
    scatter: bool
    inplace: bool
    out: Tensor | tensor_worker.Ref | None


class CreatePipe(NamedTuple):
    result: Pipe
    key: str
    function: ResolvableFunction
    max_messages: int
    device_mesh: DeviceMesh
    args: Tuple[object, ...]
    kwargs: Dict[str, object]


class PipeRecv(NamedTuple):
    ident: int
    result: object  # pytree with tensors in it
    pipe: Pipe
    stream: StreamRef


class BackendNetworkInit(NamedTuple):
    hostname: str | None = None
    port: int | None = None


class BackendNetworkPointToPointInit(NamedTuple):
    from_stream: StreamRef
    to_stream: StreamRef


# TODO: This is not supported on the rust side and might be only needed for remote funcs
class DebuggerRead(NamedTuple):
    requested: int


# TODO: This is not supported on the rust side and might be only needed for remote funcs
class DebuggerWrite(NamedTuple):
    payload: bytes


# TODO: This is not supported on the rust side and might be only needed for remote funcs
class DebuggerMessage(NamedTuple):
    stream_id: int
    action: Literal["paused", "attach", "detach"] | DebuggerRead | DebuggerWrite


# TODO: Might need to be supported differently through typed worker exceptions
class DependentOnError(Exception):
    def __init__(self, ident: int) -> None:
        self.ident = ident
