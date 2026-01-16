# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import atexit
import bdb
import io
import logging
import os
import pdb  # noqa
import traceback
from collections import deque
from logging import Logger
from traceback import FrameSummary
from typing import (
    Any,
    cast,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import torch.utils._python_dispatch
from monarch._rust_bindings.monarch_extension import client
from monarch._rust_bindings.monarch_extension.mesh_controller import _Controller
from monarch._rust_bindings.monarch_extension.tensor_worker import Ref
from monarch._rust_bindings.monarch_hyperactor.actor import (
    MethodSpecifier,
    PythonMessage,
    PythonMessageKind,
    UnflattenArg,
)
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
from monarch._rust_bindings.monarch_hyperactor.mailbox import Mailbox, PortId
from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PendingPickleState
from monarch._src.actor.actor_mesh import Channel, Port
from monarch._src.actor.shape import NDSlice
from monarch.common import device_mesh, messages, stream
from monarch.common.controller_api import TController
from monarch.common.function import ResolvableFunction
from monarch.common.invocation import Seq
from monarch.common.messages import SendResultOfActorCall
from monarch.common.stream import Stream, StreamRef
from monarch.common.tensor import dtensor_check, InputChecker, Tensor
from monarch.common.tree import flatten

if TYPE_CHECKING:
    from monarch._src.actor.proc_mesh import HyProcMesh, ProcMesh

from monarch._rust_bindings.monarch_hyperactor.shape import Point
from monarch._src.actor.actor_mesh import context, Instance
from monarch._src.actor.device_utils import _local_device_count
from monarch.common.client import Client
from monarch.common.controller_api import LogMessage, MessageResult
from monarch.common.device_mesh import DeviceMesh
from monarch.common.future import Future as OldFuture
from monarch.common.invocation import DeviceException, RemoteException

logger: Logger = logging.getLogger(__name__)


def _set_trace(*, header=None):
    ds = PdbWrapper(header)
    ds.set_trace()


class PdbWrapper(pdb.Pdb):
    def __init__(self, header: Optional[str]):
        from monarch._rust_bindings.monarch_extension import debugger

        self._actor = debugger.PdbActor()
        self.header = header
        super().__init__(
            # pyre-ignore
            stdout=WriteWrapper(self._actor),
            stdin=ReadWrapper.create(self._actor),
        )
        self._first = True

    def setup(self, *args, **kwargs):
        r = super().setup(*args, **kwargs)
        if self._first:
            self._first = False
            # when we enter the debugger, we want to present the user's stack frame
            # not the nested one inside session.run. This means that the local
            # variables are what gets printed, etc. To do this
            # we first execute up 2 to get to that frame.
            self.do_up(2)
        return r

    def set_continue(self) -> None:
        from monarch._rust_bindings.monarch_messages.debugger import DebuggerAction

        r = super().set_continue()
        if not self.breaks:
            # no more breakpoints so this debugger will not
            # be used again, and we detach from the controller io.
            self._actor.send(DebuggerAction.Detach())
            self._actor.drain_and_stop()
            # break cycle with itself before we exit
            import sys

            self.stdin = sys.stdin
            self.stdout = sys.stdout
        return r

    def set_trace(self):
        from monarch._rust_bindings.monarch_messages.debugger import DebuggerAction

        self._actor.send(DebuggerAction.Paused())
        message = self._actor.receive()
        # we give the controller the option to ignore this request to debug
        # by issuing a "detach" message immediately.
        if isinstance(message, DebuggerAction.Detach):
            return
        elif isinstance(message, DebuggerAction.Attach):
            pass
        else:
            raise RuntimeError(f"unexpected debugger message {message}")
        if self.header:
            self.message(self.header)
        super().set_trace()

    def set_quit(self):
        from monarch._rust_bindings.monarch_messages.debugger import DebuggerAction

        self._actor.send(DebuggerAction.Detach())
        self._actor.drain_and_stop()
        super().set_quit()


class ReadWrapper(io.RawIOBase):
    def __init__(self, actor):
        self._actor = actor

    def readinto(self, b):
        from monarch._rust_bindings.monarch_extension import debugger
        from monarch._rust_bindings.monarch_messages.debugger import DebuggerAction

        self._actor.send(DebuggerAction.Read(len(b)))
        response = self._actor.receive()
        if isinstance(response, DebuggerAction.Detach):
            raise bdb.BdbQuit
        assert isinstance(response, DebuggerAction.Write)
        response = cast(DebuggerAction.Write, response)
        payload = debugger.get_bytes_from_write_action(response)
        assert len(payload) <= len(b)
        b[: len(payload)] = payload
        return len(payload)

    def readable(self) -> bool:
        return True

    @classmethod
    def create(cls, actor):
        return io.TextIOWrapper(io.BufferedReader(cls(actor)))


class WriteWrapper:
    def __init__(self, actor):
        self._actor = actor

    def writable(self) -> bool:
        return True

    def write(self, s: str):
        from monarch._rust_bindings.monarch_messages.debugger import DebuggerAction

        self._actor.send(DebuggerAction.Write(s.encode()))

    def flush(self):
        pass


class Controller(_Controller):
    def __init__(self, instance: Instance, workers: "HyProcMesh") -> None:
        super().__init__()
        self._mailbox: Mailbox = Instance._mailbox
        self._pending_debugger_sessions: deque[ActorId] = deque()

    def node(
        self,
        seq: int,
        defs: Sequence[object],
        uses: Sequence[object],
        port: Tuple[PortId, NDSlice] | None,
        tracebacks: List[List[FrameSummary]],
    ) -> None:
        actor_instance = context().actor_instance
        self._node(
            actor_instance._as_rust(),
            seq,
            defs,
            uses,
            port,
            tracebacks,
        )

    def drop_refs(self, refs: Sequence[object]) -> None:
        actor_instance = context().actor_instance
        self._drop_refs(actor_instance._as_rust(), refs)

    def next_message(
        self, timeout: Optional[float]
    ) -> Optional[LogMessage | MessageResult]:
        raise RuntimeError(
            "internal error: tensor engine does not produce futures that call next_message"
        )

    def send(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        msg: NamedTuple,
    ) -> None:
        with torch.utils._python_dispatch._disable_current_modes():
            actor_instance = context().actor_instance
            return super()._send(actor_instance._as_rust(), ranks, msg)

    def drain_and_stop(
        self,
    ) -> List[LogMessage | MessageResult | client.DebuggerMessage]:
        actor_instance = context().actor_instance
        self._drain_and_stop(actor_instance._as_rust())
        return []

    def sync_at_exit(self, port: PortId) -> None:
        actor_instance = context().actor_instance
        self._sync_at_exit(actor_instance._as_rust(), port)

    def stop_mesh(self):
        # I think this is a noop?

        pass


def _initialize_env(worker_point: Point, proc_id: str) -> None:
    worker_rank = worker_point.rank
    try:
        if "gpus" in worker_point:
            local_rank = worker_point["gpus"]
            gpus_per_host = worker_point.size("gpus")
        elif "gpu" in worker_point:
            local_rank = worker_point["gpu"]
            gpus_per_host = worker_point.size("gpu")
        else:
            gpus_per_host = _local_device_count()
            local_rank = worker_rank % gpus_per_host

        num_worker_procs = worker_point.extent.nelements
        process_env = {
            "CUDA_VISIBLE_DEVICES": str(local_rank),
            "NCCL_HOSTID": f"{proc_id}_host_{worker_rank // gpus_per_host}",
            # This is needed to avoid a hard failure in ncclx when we do not
            # have backend topology info (eg. on RE).
            "NCCL_IGNORE_TOPO_LOAD_FAILURE": "true",
            "LOCAL_RANK": str(local_rank),
            "RANK": str(worker_rank),
            "WORLD_SIZE": str(num_worker_procs),
            "LOCAL_WORLD_SIZE": str(gpus_per_host),
        }
        os.environ.update(process_env)
        pdb.set_trace = _set_trace
        # workaround for set_manual_seed somehow not working if cuda is not initialized\
        if torch.cuda.is_available():
            torch.cuda.init()

        def check_set_device(device):
            import os

            if str(device) not in os.environ.get("CUDA_VISIBLE_DEVICES", "").split(","):
                raise ValueError(
                    f"Only devices {os.environ.get('CUDA_VISIBLE_DEVICES', 'None')} are available to monarch worker, "
                    f"but torch.cuda.set_device({device}) was called"
                )

        torch.cuda.set_device = check_set_device
    except Exception:
        traceback.print_exc()
        raise


class MeshClient(Client):
    def fetch(
        self,
        mesh: "DeviceMesh",
        stream: "StreamRef",
        shard,
        preprocess_message,
        args,
        kwargs,
        defs: Tuple["Tensor", ...],
        uses: Tuple["Tensor", ...],
    ) -> "OldFuture":  # the OldFuture is a lie
        sender, receiver = Channel.open(once=True)

        ident = self.new_node(defs, uses, cast("OldFuture", sender))
        process = mesh._process(shard)
        self.send(
            process,
            messages.SendValue(
                ident,
                None,
                defs,
                preprocess_message,
                args,
                kwargs,
                stream,
            ),
        )
        # we have to ask for status updates
        # from workers to be sure they have finished
        # enough work to count this future as finished,
        # and all potential errors have been reported
        self._request_status()
        return cast("OldFuture", receiver.recv())

    def shutdown(
        self,
        destroy_pg: bool = True,
        error_reason: Optional[RemoteException | DeviceException | Exception] = None,
    ):
        # return
        if self.has_shutdown:
            return
        logger.info("shutting down the client gracefully")

        atexit.unregister(self._atexit)
        self._shutdown = True

        sender, receiver = Channel.open(once=True)
        assert sender._port_ref is not None
        self._mesh_controller.sync_at_exit(sender._port_ref.port_id)
        receiver.recv().get(timeout=60)
        # we are not expecting anything more now, because we already
        # waited for the responses
        self.inner.drain_and_stop()

    def _atexit(self) -> None:
        # Calling self.shutdown may cause a deadlock if something is wrong with
        # the networking. Or should we make shutdown() not wait indefinitely?
        self._shutdown = True

        # send shutdown message to stop other processes.
        self.inner.stop_mesh()

    @property
    def _mesh_controller(self) -> Controller:
        return cast(Controller, self.inner)

    def new_node_nocoalesce(
        self,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
        future: Optional["OldFuture"],
        tracebacks: List[List[traceback.FrameSummary]],
    ) -> Seq:
        seq = self._next_seq()
        # .ref can be None if the tensor is an intermediate result
        # inside a recording
        defs = cast(Sequence["Tensor"], [t for t in defs if t.ref is not None])
        uses = cast(Sequence["Tensor"], [t for t in uses if t.ref is not None])
        for d in defs:
            d._seq = seq
        response_port = None
        if future is not None:
            # method annotation is a lie to make Client happy
            port, slice = cast("Tuple[Port[Any], NDSlice]", future)
            assert port._port_ref is not None
            response_port = (port._port_ref.port_id, slice)
        self._mesh_controller.node(seq, defs, uses, response_port, tracebacks)
        return seq

    def handle_next_message(self, timeout: Optional[float]) -> bool:
        """
        Mesh controller message loop is handled by the tokio event loop.
        """
        return False


def spawn_tensor_engine(proc_mesh: "ProcMesh") -> DeviceMesh:
    # This argument to Controller
    # is currently only used for debug printing. It should be fixed to
    # report the proc ID instead of the rank it currently does.
    gpus = proc_mesh.sizes.get("gpus", 1)

    # we currently block on the creation of the proc mesh, but conceivably we could init concurrently here.
    backend_ctrl = Controller(context().actor_instance, proc_mesh._proc_mesh.block_on())
    client = MeshClient(cast("TController", backend_ctrl), proc_mesh.size(), gpus)
    dm = DeviceMesh(
        client,
        NDSlice.new_row_major(list(proc_mesh.sizes.values())),
        tuple(proc_mesh.sizes.keys()),
    )
    dm.exit = lambda: client.shutdown()
    return dm


class RemoteException(Exception):
    def __init__(
        self,
        worker_error_string: str,  # this should really be an exception + stacktrace but
        # worker code needs major refactor to make this possible
        controller_frames: List[traceback.FrameSummary],
        rank: int,
    ):
        self.worker_error_string = worker_error_string
        self.controller_frames = controller_frames
        self.rank = rank

    def __str__(self):
        try:
            controller_tb = "".join(traceback.format_list(self.controller_frames))
            return (
                f"A remote function has failed asynchronously on rank {self.rank}.\n"
                f"Traceback of where the remote function was issued on controller (most recent call last):\n{controller_tb}"
                f"Error as reported from worker:\n{self.worker_error_string}"
            )
        except Exception:
            traceback.print_exc()
            return "<exception formatting RemoteException>"


def _create_call_method_indirect_message(
    method_name: "MethodSpecifier",
    client: MeshClient,
    seq: Seq,
    args_kwargs_tuple: Buffer,
    refs: Sequence[Any],
    pending_pickle_state: Optional[PendingPickleState],
) -> Tuple[PythonMessage, Tuple[str, int]]:
    unflatten_args = [
        UnflattenArg.PyObject if isinstance(ref, Tensor) else UnflattenArg.Mailbox
        for ref in refs
    ]
    broker_id: Tuple[str, int] = client._mesh_controller.broker_id
    actor_msg = PythonMessage(
        PythonMessageKind.CallMethodIndirect(
            method_name, broker_id, seq, unflatten_args
        ),
        args_kwargs_tuple,
        pending_pickle_state,
    )
    return (actor_msg, broker_id)


def create_actor_message(
    method_name: MethodSpecifier,
    proc_mesh: Optional["ProcMesh"],
    args_kwargs_tuple: Buffer,
    refs: Sequence[Any],
    port: Optional[Port[Any]],
    pending_pickle_state: Optional[PendingPickleState],
) -> PythonMessage:
    tensors = [ref for ref in refs if isinstance(ref, Tensor)]
    # we have some monarch references, we need to ensure their
    # proc_mesh matches that of the tensors we sent to it
    chosen_stream = stream._active
    for t in tensors:
        if hasattr(t, "stream"):
            chosen_stream = t.stream
            break
    with InputChecker(tensors, lambda x: f"actor_call({x})") as checker:
        checker.check_mesh_stream_local(device_mesh._active, chosen_stream)
        # TODO: move propagators into Endpoint abstraction and run the propagator to get the
        # mutates
        checker.check_permission(())
    selected_device_mesh = proc_mesh and proc_mesh._device_mesh
    if selected_device_mesh is not checker.mesh:
        raise ValueError(
            f"monarch Tensors sent to an actor must be located on the same process as the actor. However {checker.mesh} is not {selected_device_mesh}."
            "NYI: better serialization of mesh names to make the mismatch more clear."
        )

    client = cast(MeshClient, checker.mesh.client)

    return _create_actor_message(
        method_name,
        args_kwargs_tuple,
        refs,
        port,
        client,
        checker.mesh,
        tensors,
        chosen_stream,
        pending_pickle_state,
    )


def _create_actor_message(
    method_name: MethodSpecifier,
    args_kwargs_tuple: Buffer,
    refs: Sequence[Any],
    port: Optional[Port[Any]],
    client: MeshClient,
    mesh: DeviceMesh,
    tensors: List[Tensor],
    chosen_stream: Stream,
    pending_pickle_state: Optional[PendingPickleState],
) -> PythonMessage:
    stream_ref = chosen_stream._to_ref(client)
    fut = (port, mesh._ndslice) if port is not None else None

    ident = client.new_node([], tensors, cast("OldFuture", fut))

    # To ensure that both the actor and the stream execute in order, we send a message
    # to each at this point. The message to the worker will be handled on the stream actor where
    # it will send the 'tensor's to the broker actor locally, along with a response port with the
    # computed value.

    # The message to the generic actor tells it to first wait on the broker to get the local arguments
    # from the stream, then it will run the actor method, and send the result to response port.

    actor_msg, broker_id = _create_call_method_indirect_message(
        method_name, client, ident, args_kwargs_tuple, refs, pending_pickle_state
    )
    worker_msg = SendResultOfActorCall(ident, broker_id, tensors, [], stream_ref)
    client.send(mesh._ndslice, worker_msg)
    # we have to ask for status updates
    # from workers to be sure they have finished
    # enough work to count this future as finished,
    # and all potential errors have been reported
    client._request_status()
    return actor_msg


def actor_rref(
    endpoint,
    args_kwargs_tuple: Buffer,
    refs: Sequence[Any],
    pending_pickle_state: Optional[PendingPickleState],
):
    chosen_stream = stream._active
    fake_result, dtensors, mutates, mesh = dtensor_check(
        endpoint._propagate,
        cast(ResolvableFunction, endpoint._name),
        refs,
        {},
        device_mesh._active,
        chosen_stream,
    )
    assert mesh is not None

    fake_result_dtensors, unflatten_result = flatten(
        fake_result, lambda x: isinstance(x, torch.Tensor)
    )
    result_dtensors = tuple(
        Tensor(fake, mesh, chosen_stream) for fake in fake_result_dtensors
    )
    seq = mesh.client.new_node(result_dtensors + mutates, dtensors)
    assert all(t.ref is not None for t in result_dtensors)
    assert all(t.ref is not None for t in mutates)
    result = result_msg = unflatten_result(result_dtensors)
    if len(result_dtensors) == 0:
        result_msg = None

    actor_msg, broker_id = _create_call_method_indirect_message(
        endpoint._name, mesh.client, seq, args_kwargs_tuple, refs, pending_pickle_state
    )
    endpoint._actor_mesh.cast(actor_msg, "all", context().actor_instance._as_rust())
    # note the device mesh has to be defined regardles so the remote functions
    # can invoke mesh.rank("...")

    mesh.define_remotely()

    mesh._send(
        messages.CallActorMethod(
            seq,
            result_msg,
            broker_id,
            refs,
            cast("List[Ref]", mutates),
            stream._active._to_ref(mesh.client),
        )
    )
    return result
