# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import asyncio
import bdb
import itertools
import logging
import os
import pdb  # noqa
import queue
import threading
from collections import deque
from contextlib import contextmanager
from traceback import extract_tb
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
)
from weakref import WeakKeyDictionary

import torch
import torch.distributed
import torch.fx
import zmq
import zmq.asyncio
from monarch._src.actor.shape import NDSlice
from monarch.common import messages
from monarch.common.function import ResolvableFunction
from monarch.common.messages import DependentOnError, Dims
from monarch.common.process_group import SingleControllerProcessGroupWrapper
from monarch.common.reference import Ref, Referenceable
from monarch.common.tensor_factory import TensorFactory
from monarch.common.tree import flatten, flattener
from monarch_supervisor import get_message_queue, Letter
from monarch_supervisor.logging import initialize_logging

from .compiled_block import CompiledBlock
from .debugger import _set_trace
from .monitor import Monitor

logger = logging.getLogger(__name__)
try:
    CONTROLLER_COMPILED_REPEAT = 0 != int(os.environ["CONTROLLER_COMPILED_REPEAT"])
except KeyError:
    CONTROLLER_COMPILED_REPEAT = True


def set_default_dtype(dtype: torch.dtype):
    torch.set_default_dtype(dtype)


class Dim(NamedTuple):
    name: str
    rank: int
    size: int
    members: List[int]


class RemoteProcessGroupShell:
    def __init__(self, device_mesh: "DeviceMesh", dims: Dims, ref: Ref):
        self.device_mesh = device_mesh
        self.dims = dims
        self.ref = ref

    # return the process group, sanity checking that the stream it was created on is the stream it is being used on.
    def get_process_group_for_stream(self, stream: "Stream"):
        return self.device_mesh.get_process_group(stream, self.dims, pg=self.ref)


def _new_process_group(
    controller_global_unique_name: str, ranks: Optional[List[int]], split: bool
):
    assert torch.distributed.is_initialized()
    from unittest.mock import patch

    # Pytorch versions from about the past month have an implementation of process group name with local names that
    # can cause TCPStore name collisions (https://www.internalfb.com/intern/diff/D67312715/).
    # This will get fixed soon in pytorch, but will take some time to rollout.
    # In the meantime, our workers have enough knowledge to simply generate a unique names based on the data they already have.
    # While not strictly needed once pytorch fixes the bug, this illustrates how our own initialization of nccl can just directly
    # provide a unique key for each process group it is creating.
    with patch(
        "torch.distributed.distributed_c10d._process_group_name",
        side_effect=lambda *args, **kwargs: controller_global_unique_name,
    ) as the_patch:
        if split:
            assert ranks is not None
            pg = torch.distributed.split_group(None, [ranks])
        else:
            pg = torch.distributed.new_group(ranks, use_local_synchronization=True)

        assert the_patch.called
    return pg


restart_count = 0


class DeviceMesh:
    def __init__(self, id: int, names: Dims, ranks: NDSlice, rank: int):
        self.id = id
        self.dims: Dict[str, Dim] = {}
        coordinates = ranks.coordinates(rank)
        for coordinate, name, size, stride in zip(
            coordinates, names, ranks.sizes, ranks.strides
        ):
            start = rank - stride * coordinate
            members = [*range(start, start + stride * size, stride)]
            assert members[coordinate] == rank
            self.dims[name] = Dim(name, coordinate, size, members)
        self.all_ranks: List[int] = list(ranks)
        self.process_group_for_stream: WeakKeyDictionary["Stream", Any] = (
            WeakKeyDictionary()
        )

    def get_ranks_for_dim_slice(self, names: Dims):
        if len(names) == 0:
            return []
        if len(names) == 1:
            return self.dims[names[0]].members
        if len(names) == len(self.dims):
            return self.all_ranks

        dims = [self.dims[n] for n in names]

        members = [dim.members for dim in dims]
        strides = [d[1] - d[0] if len(d) > 1 else 0 for d in members]
        start = members[0][dims[0].rank]
        for d, s in zip(dims, strides):
            start -= s * d.rank

        ranks = []
        for idxs in itertools.product(*[range(d.size) for d in dims]):
            offset = sum([i * s for i, s in zip(idxs, strides)])
            ranks.append(start + offset)
        return ranks

    def create_process_group(
        self, stream: "Stream", dims: Dims, pg: Optional[Ref] = None
    ):
        if stream not in self.process_group_for_stream:
            self.process_group_for_stream[stream] = {}
        dims = tuple(sorted(dims))
        key = (pg, dims)
        if key in self.process_group_for_stream[stream]:
            raise AssertionError(
                f"Tried to create a process group for {stream=}, {dims=} but it already exists!"
            )
        ranks = self.get_ranks_for_dim_slice(dims)
        indices = [
            str(d.rank) if d.name not in dims else "X" for d in self.dims.values()
        ]
        name = f"restart_{restart_count}_mesh_{self.id}_stream_{stream.id}_{'_'.join(indices)}"
        if pg is not None:
            name += f"_group_{pg}"
        self.process_group_for_stream[stream][key] = (
            SingleControllerProcessGroupWrapper(
                _new_process_group(name, ranks, split=True)
            )
        )
        return self.get_process_group(stream, dims, pg=pg)

    def get_process_group(self, stream: "Stream", dims: Dims, pg: Optional[Ref] = None):
        dims = tuple(sorted(dims))
        key = (pg, dims)
        return self.process_group_for_stream[stream][key]

    def create_process_group_shell(self, dims: Dims, ref: Ref):
        return RemoteProcessGroupShell(self, dims, ref)


def _rank(mesh: "DeviceMesh", dim: str):
    return torch.full((), mesh.dims[dim].rank, dtype=torch.long)


def _process_idx(mesh: "DeviceMesh"):
    """
    Return linear idx of the current process in the mesh.
    """
    # any dimension can be used to query our rank
    _, dim = next(iter(mesh.dims.items()))
    return torch.full((), dim.members[dim.rank], dtype=torch.long)


def _reduce(
    local_tensor: torch.Tensor,
    source_mesh: DeviceMesh,
    group,
    group_size: int,
    reduction: str,
    scatter: bool,
    inplace: bool,
    out: Optional[torch.Tensor],
):
    if reduction == "stack":
        if scatter:
            output = local_tensor
            if not inplace:
                output = local_tensor.clone() if out is None else out
            torch.distributed.all_to_all_single(output, local_tensor, group=group)
            return output

        assert not inplace
        output = (
            torch.empty(
                [group_size, *local_tensor.shape],
                dtype=local_tensor.dtype,
                device=local_tensor.device,
                layout=local_tensor.layout,
            )
            if out is None
            else out
        )
        torch.distributed.all_gather_into_tensor(output, local_tensor, group=group)
        return output

    op = getattr(torch.distributed.ReduceOp, reduction.upper())

    if scatter:
        assert not inplace
        output = (
            torch.empty(
                local_tensor.shape[1:],
                dtype=local_tensor.dtype,
                device=local_tensor.device,
                layout=local_tensor.layout,
            )
            if out is None
            else out
        )
        torch.distributed.reduce_scatter_tensor(
            output, local_tensor, op=op, group=group
        )
        return output

    output = local_tensor
    if not inplace:
        output = local_tensor.clone() if out is None else out
    torch.distributed.all_reduce(output, op=op, group=group)
    return output


class _TLS(threading.local):
    def __init__(self):
        self.tracing: Optional["CompiledBlock"] = None
        self.stream: Optional["Stream"] = None


_tls = _TLS()


def schedule_on_stream_thread(executes_on_error: bool):
    def wrapper(fn):
        return lambda self, *args, **kwargs: self.schedule(
            lambda: (
                logger.debug(
                    "executing: %s(args=%s, kwargs=%s)", fn.__name__, args, kwargs
                ),
                fn(self, *args, **kwargs),
            ),
            executes_on_error,
        )

    return wrapper


class Stream:
    def __init__(self, worker: "Worker", id: int, default: bool):
        self.id = id
        self.worker = worker
        self.thread: Optional[threading.Thread] = None
        self.q: queue.Queue[Callable[[], None]] = queue.Queue()
        # used to send messages pdb from controller see debugger.py
        self.debugger_queue: queue.Queue[Any] = queue.Queue()
        self.should_exit = threading.Event()
        self.current_recording: Optional[int] = None
        if default:
            self._cuda_stream = None
        else:
            self._cuda_stream = torch.cuda.Stream()

    @schedule_on_stream_thread(executes_on_error=False)
    def run_recording(
        self, ident: int, impl: Callable, results: List["Cell"], inputs: List["Cell"]
    ):
        self.current_recording = ident
        try:
            impl(results, inputs)
        finally:
            self.current_recording = None

    @property
    def cuda_stream(self):
        if self._cuda_stream is None:
            return torch.cuda.current_stream()
        else:
            return self._cuda_stream

    @contextmanager
    def enable(self):
        if self._cuda_stream is None:
            yield
            return
        with torch.cuda.stream(self._cuda_stream):
            yield

    def event(self):
        e = torch.cuda.Event()
        self.cuda_stream.record_event(e)
        return e

    def wait_event(self, event):
        self.cuda_stream.wait_event(event)

    def wait_stream(self, stream):
        self.cuda_stream.wait_stream(stream.cuda_stream)

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self.main)
        thread.start()
        return thread

    def main(self):
        _tls.stream = self
        with self.enable():
            try:
                while True:
                    self.q.get()()
            except StopIteration:
                pass
            except Exception as e:
                logger.exception("Stream thread exiting with exception.")
                msg = messages.InternalException(e, extract_tb(e.__traceback__))
                self.worker.schedule(lambda: self.worker.internal_error(msg))

    def exit(self):
        def stop():
            raise StopIteration

        self.schedule(stop)
        self.debugger_queue.put("detach")

    def join(self):
        if self.thread is None:
            return
        self.exit()
        self.thread.join()

    def schedule(self, fn: Callable[[], None], executes_on_error: bool = False):
        if _tls.tracing:
            tracing = _tls.tracing
            if executes_on_error:
                tracing.fallback[self].append(fn)
            with tracing.record_to(self):
                fn()
            return

        if self.thread is None:
            self.thread = threading.Thread(target=self.main, daemon=True)
            self.thread.start()
        self.q.put(fn)

    def call_or_trace(self, fn, *args, **kwargs):
        if _tls.tracing:
            return _tls.tracing.call_function(fn, args, kwargs)
        return fn(*args, **kwargs)

    def report_error(self, ident: int, index: int, e: Exception, extra: Any = None):
        logger.exception(f"Error generating {ident}, {extra=}", exc_info=e)
        self.worker.q.send(
            messages.RemoteFunctionFailed(ident, index, e, extract_tb(e.__traceback__))
        )
        return DependentOnError(ident)

    @contextmanager
    def try_define(
        self, ident: Optional[int], results: Sequence["Cell"], extra: Any = None
    ):
        tracing = _tls.tracing
        if tracing:
            ctx = tracing.current_context
            ctx.ident = ident
            tracing.mutates(results)

        try:
            yield
        except DependentOnError as e:
            for r in results:
                r.set(e)
            # note: there is no need to to send RemoteFunctionFailed
            # because the controller would have already gotten and propagated the
            # original created of DependentOnError.
        except bdb.BdbQuit:
            raise
        except Exception as e:
            # when try_define does not have an ident,
            # the only error we expected is DependendOnError
            # other errors should get treated as internal errors.
            if ident is None:
                raise
            if self.current_recording is not None:
                exc = self.report_error(self.current_recording, ident, e, extra)
            else:
                exc = self.report_error(ident, 0, e, extra)
            for r in results:
                r.set(exc)
        finally:
            if _tls.tracing:
                # pyre-fixme[8]: Attribute has type `ErrorContext`; used as `None`.
                _tls.tracing.current_context = None

    @schedule_on_stream_thread(executes_on_error=False)
    def call_function(
        self,
        ident: int,
        defines: Tuple["Cell", ...],
        flatten_result: Any,
        mutates: Tuple["Cell", ...],
        rfunction: ResolvableFunction,
        inputs: List["Cell"],
        unflatten_inputs: Any,
        device_mesh: Optional["DeviceMesh"] = None,
    ):
        with self.try_define(
            ident, [*defines, *mutates], extra=(rfunction, defines, mutates, inputs)
        ):
            function = rfunction.resolve()
            resolved_inputs = []
            for i in inputs:
                input_ = i.get()
                if isinstance(input_, RemoteProcessGroupShell):
                    # get the process group for the stream but dont' allow it to be created from
                    # this context since this isn't being run on the event loop.
                    resolved_inputs.append(input_.get_process_group_for_stream(self))
                else:
                    resolved_inputs.append(input_)

            args, kwargs = unflatten_inputs(resolved_inputs)
            if _tls.tracing:
                block = _tls.tracing
                fn_node: torch.fx.Node = block.call_function(function, args, kwargs)
                tensors = [
                    t.node if isinstance(t, torch.fx.Proxy) else t
                    for t in flatten_result(block.proxy(fn_node))
                ]
            else:
                result = function(*args, **kwargs)
                tensors = flatten_result(result)
            assert len(defines) == len(tensors)
            for d, t in zip(defines, tensors):
                d.set(t)

    @schedule_on_stream_thread(executes_on_error=False)
    def send_value(
        self,
        ident: int,
        rfunction: Optional[ResolvableFunction],
        mutates: Tuple["Cell", ...],
        inputs: List["Cell"],
        unflatten: Any,
        pipe: Optional["WorkerPipe"],
    ):
        with self.try_define(ident, mutates):
            args, kwargs = unflatten(c.get() for c in inputs)
            function = (lambda x: x) if rfunction is None else rfunction.resolve()
            result = function(*args, **kwargs)
            if pipe is None:
                self.worker.q.send(messages.FetchResult(ident, result))
            else:
                self.call_or_trace(pipe.send, result)

    @schedule_on_stream_thread(executes_on_error=False)
    def collective_call(
        self,
        function: Callable,
        factory: TensorFactory,
        input_: "Cell",
        result: "Cell",
        out: Optional["Cell"] = None,
    ):
        try:
            local_tensor = input_.get()
            out_tensor = None if out is None else out.get()
        except DependentOnError:
            # even if we were broken before, we have to participate in the collective
            # because we cannot signal to other ranks that we were broken
            # the controller will see the error message we sent before and know
            # the downstream values are broken.
            local_tensor = factory.zeros()
            out_tensor = None
        # XXX - we should be careful about starting the collective with a tensor that doesn't match the expected
        # factory size. It can error. however, before we can do something about it we need to assign a failure
        # identity to this reduce object.
        output = self.call_or_trace(function, local_tensor, out_tensor)
        result.set(output)

    @schedule_on_stream_thread(executes_on_error=True)
    def borrow_create(self, input_: "Cell", borrow: "Borrow"):
        self.call_or_trace(borrow.create, input_.get(), self)

    @schedule_on_stream_thread(executes_on_error=True)
    def borrow_first_use(self, result: "Cell", borrow: "Borrow"):
        with self.try_define(None, [result]):
            result.set(self.call_or_trace(borrow.first_use))

    @schedule_on_stream_thread(executes_on_error=True)
    def borrow_last_use(self, borrow: "Borrow"):
        self.call_or_trace(borrow.last_use)

    @schedule_on_stream_thread(executes_on_error=True)
    def borrow_drop(self, borrow: "Borrow"):
        self.call_or_trace(borrow.drop)


class Borrow:
    def __init__(self, from_stream: Stream, to_stream: Stream):
        self.from_stream = from_stream
        self.to_stream = to_stream
        self.first_use_queue = queue.Queue()
        self.last_use_queue = queue.Queue()
        # used to ensure the tensor memory stays alive in the
        # allocator until it is returned to its original stream
        self.tensor_storage = Cell(None)

    def create(self, input_: Any, stream: Stream):
        self.first_use_queue.put((stream.event(), input_))

    def first_use(self):
        event, t = self.first_use_queue.get()
        self.tensor_storage.set(t)
        self.to_stream.wait_event(event)
        # raise any potential error _after_ already processing
        # the events. We always do the synchronizations even
        # if the value being borrowed is an error.
        return self.tensor_storage.get()

    def last_use(self):
        t = self.tensor_storage.value
        self.tensor_storage.set(undefined_cell)
        self.last_use_queue.put((self.to_stream.event(), t))

    def drop(self):
        event, t = self.last_use_queue.get()
        self.from_stream.wait_event(event)
        del t


class WorkerMessageQueue(Protocol):
    def _socket(self, kind) -> zmq.Socket: ...

    def send(self, message: Any) -> None: ...

    async def recv_async(self) -> Letter: ...

    def recvready(self, timeout: Optional[float]) -> List[Letter]: ...


class WorkerPipe:
    """
    Worker (e.g Trainer) process pipe
    """

    def __init__(self, q: WorkerMessageQueue, pipe_name: str, max_messages: int = 50):
        # breaking abstraction layer here, but it is an easy way to get a way to send messages
        # to the process
        self._sock = q._socket(zmq.PAIR)
        self._sock.setsockopt(zmq.SNDHWM, max_messages)
        self._sock.setsockopt(zmq.RCVHWM, max_messages)
        self._sock.bind(pipe_name)

    def send(self, v: Any):
        self._sock.send_pyobj(v)

    def recv(self) -> Any:
        return self._sock.recv_pyobj()

    # Allows us to pass the pipe as a function that can be called to get the next value
    def resolve(self) -> Callable:
        return self.recv


undefined_cell = RuntimeError("undefined cell")


class Cell:
    __slots__ = ("value",)

    def __init__(self, initial_value=undefined_cell):
        self.value: Any = initial_value

    def __repr__(self):
        return "<C>"

    def set(self, value: Any):
        self.value = value

    def clear(self):
        self.value = undefined_cell

    def is_defined(self):
        return self.value is not undefined_cell

    def get(self) -> Any:
        tracing = _tls.tracing
        if (
            tracing is not None
            and self not in tracing.defined_cells
            and tracing.recording_stream is not None
        ):
            return tracing.input_cell(self)
        v = self.value
        if isinstance(v, Exception):
            raise v
        return v


class Worker:
    def __init__(self, q: WorkerMessageQueue, rank: int, world: int, local_rank: int):
        # remote ref id to local value
        self.env: Dict[int, Cell] = {}
        self.q = q
        self.rank = rank
        self.world = world
        self.local_rank = local_rank
        self.last_send_status = 0
        self.borrows: Dict[int, Tuple[Ref, Borrow]] = {}
        self.streams: List[Stream] = []
        self.send_recv_process_groups: Dict[Tuple[Stream, Stream], Any] = {}
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.stream_thread_error = False
        self.max_received_ident = 0

    def handle_message(self, event: NamedTuple):
        cmd = event.__class__.__name__
        if ident := getattr(event, "ident", None):
            self.max_received_ident = max(self.max_received_ident, ident)
        fn = getattr(self, cmd, None)
        if fn is not None:
            return fn(event)
        raise RuntimeError(f"unhandled event: {event}")

    def CreateDeviceMesh(
        self, m: messages.CreateDeviceMesh
    ):  # result: "Ref", names: Tuple[str, ...], ranks: NDSlice):
        # pyre-ignore
        self.define(m.result, DeviceMesh(m.result.id, m.names, m.ranks, self.rank))

    def resolve(self, r: Union[Referenceable, Ref]) -> Cell:
        assert isinstance(r, Ref)
        return self.env[r.id]

    def CallFunction(self, m: messages.CallFunction):
        flatten_result = flattener(m.result, lambda x: isinstance(x, Ref))
        results = flatten_result(m.result)
        defines = tuple(self.cell(r) for r in results)
        mutates = tuple(self.resolve(r) for r in m.mutates)
        stream: Stream = self.resolve(m.stream).get()
        device_mesh = (
            self.resolve(m.device_mesh).get() if m.device_mesh is not None else None
        )
        inputs, unflatten_inputs = self._inputs((m.args, m.kwargs))

        stream.call_function(
            m.ident,
            defines,
            flatten_result,
            mutates,
            m.function,
            inputs,
            unflatten_inputs,
            device_mesh,
        )

    def CreateRemoteProcessGroup(self, m: messages.CreateRemoteProcessGroup):
        device_mesh = self.resolve(m.device_mesh).get()
        result = self.cell(m.result)
        result.set(device_mesh.create_process_group_shell(m.dims, m.result))

    def CreateStream(self, m: messages.CreateStream):
        # pyre-ignore
        stream = Stream(self, m.result.id, m.default)
        self.streams.append(stream)
        self.define(m.result, stream)

    def _inputs(self, obj):
        refs, unflatten = flatten(obj, lambda x: isinstance(x, Ref))
        inputs = [self.env[r.id] for r in refs]
        return inputs, unflatten

    def SendValue(self, m: messages.SendValue):
        assert not _tls.tracing, (
            "controller should have prevented SendValue in repeat block."
        )
        stream: Stream = self.resolve(m.stream).get()
        pipe: Optional["WorkerPipe"] = (
            self.resolve(m.destination).get() if m.destination is not None else None
        )
        inputs, unflatten = self._inputs((m.args, m.kwargs))
        mutates = tuple(self.resolve(r) for r in m.mutates)
        stream.send_value(m.ident, m.function, mutates, inputs, unflatten, pipe)

    def PipeRecv(self, m: messages.PipeRecv):
        stream: Stream = self.resolve(m.stream).get()
        pipe: WorkerPipe = self.resolve(m.pipe).get()
        flatten = flattener(m.result, lambda x: isinstance(x, Ref))
        results = flatten(m.result)
        results = tuple(self.cell(r) for r in results)
        stream.call_function(
            m.ident,
            results,
            flatten,
            (),
            pipe,
            (),
            lambda x: ((), {}),
        )

    def RequestStatus(self, m: messages.RequestStatus):
        # wait until all streams have reach the point
        # we have scheduled, and then respond to the message
        ident = m.ident
        count = 0
        expected = 0

        # runs on asyncio event loop, but
        # is placed on the event loop by the
        # stream thread when it reaches this work item
        def increment_and_send():
            nonlocal count
            count += 1
            if count == expected:
                self._send_status(ident + 1)

        for stream in self.streams:
            if stream.thread is not None:
                expected += 1
                stream.schedule(lambda: self.schedule(increment_and_send))

        # if there were no active threads we still need to respond to status
        # messages to make sure controller knows we are alive
        if expected == 0:
            self._send_status(ident + 1)

    def Exit(self, m: messages.Exit):
        for stream in self.streams:
            stream.exit()
        for stream in self.streams:
            logger.info("joining stream")
            stream.join()
        if torch.distributed.is_initialized() and m.destroy_pg:
            for pg in self.send_recv_process_groups.values():
                torch.distributed.destroy_process_group(pg)
            if torch.cuda.is_available():
                torch.cuda.synchronize()
            torch.distributed.barrier()
            torch.distributed.destroy_process_group()
            logger.info("PG destroyed")
        raise StopIteration()

    def CommandGroup(self, m: messages.CommandGroup):
        for cmd in m.commands:
            self.handle_message(cmd)

    @contextmanager
    def trace(self, value: Optional["CompiledBlock"]) -> Generator[None, Any, Any]:
        old, _tls.tracing = _tls.tracing, value
        try:
            yield
        finally:
            _tls.tracing = old

    def DefineRecording(self, m: messages.DefineRecording):
        block = CompiledBlock()
        with self.trace(block):
            for cmd in m.commands:
                self.handle_message(cmd)
        block.emit()
        self.define(m.result, block)

    def RecordingFormal(self, m: messages.RecordingFormal):
        block = _tls.tracing
        assert block is not None
        self.cell(m.result).set(
            block.define_formal(self.resolve(m.stream).get(), m.argument_index)
        )

    def RecordingResult(self, m: messages.RecordingResult):
        block = _tls.tracing
        assert block is not None
        with block.record_to(self.resolve(m.stream).get()):
            node = self.resolve(m.input).get()
            assert isinstance(node, torch.fx.Node)
            block.define_result(node, m.output_index)

    def CallRecording(self, m: messages.CallRecording):
        recording: CompiledBlock = self.resolve(m.recording).get()
        actuals = [
            self.resolve(a) if i in recording.used_formals else None
            for i, a in enumerate(m.actuals)
        ]
        results = [
            self.cell(r) if i in recording.used_results else None
            for i, r in enumerate(m.results)
        ]
        for stream, impl in recording.impls.items():
            stream.run_recording(m.ident, impl, results, actuals)

    def DeleteRefs(self, m: messages.DeleteRefs):
        for id in m.refs:
            del self.env[id]

    def BorrowCreate(self, m: messages.BorrowCreate):
        from_stream: Stream = self.resolve(m.from_stream).get()
        to_stream: Stream = self.resolve(m.to_stream).get()
        tensor = self.resolve(m.tensor)
        borrow = Borrow(from_stream, to_stream)
        if _tls.tracing:
            _tls.tracing.defined_borrows[borrow] = True
        from_stream.borrow_create(tensor, borrow)
        # pyre-fixme[6]: For 2nd argument expected `Tuple[Ref, Borrow]` but got
        #  `Tuple[Tensor, Borrow]`.
        self.borrows[m.borrow] = (m.result, borrow)

    def BorrowFirstUse(self, m: messages.BorrowFirstUse):
        result_id, borrow = self.borrows[m.borrow]
        result = self.cell(result_id)
        borrow.to_stream.borrow_first_use(result, borrow)

    def BorrowLastUse(self, m: messages.BorrowLastUse):
        _, borrow = self.borrows[m.borrow]
        stream = borrow.to_stream
        stream.borrow_last_use(borrow)

    def BorrowDrop(self, m: messages.BorrowDrop):
        _, borrow = self.borrows.pop(m.borrow)
        assert not _tls.tracing or borrow in _tls.tracing.defined_borrows, (
            "controller should have stopped a drop of a borrow not created in a repeat loop"
        )
        stream = borrow.from_stream
        stream.borrow_drop(borrow)

    def CreatePipe(self, m: messages.CreatePipe):
        device_mesh: DeviceMesh = self.resolve(m.device_mesh).get()
        pipe_name = f"{m.key}-{self.rank}"
        ranks = {k: v.rank for k, v in device_mesh.dims.items()}
        sizes = {k: v.size for k, v in device_mesh.dims.items()}
        pipe = WorkerPipe(self.q, pipe_name, m.max_messages)
        self.define(m.result, pipe)

        pipe.send((m.function, ranks, sizes, m.args, m.kwargs))

    def SplitComm(self, m: messages.SplitComm):
        # Test whether this rank is in the mesh specified by the SplitComm
        # command. We do this by attempting to dereference the mesh ref; only
        # the ranks that are on the mesh will succeed.
        try:
            device_mesh = self.resolve(m.device_mesh).get()
            in_mesh = True
        except KeyError:
            in_mesh = False

        if in_mesh:
            # Create a split process group
            stream = self.resolve(m.stream).get()
            device_mesh.create_process_group(stream, m.dims)
        else:
            # this rank is not in the split group. We still need to participate
            # in the commSplit call, however.

            # This weird incantation is because the current default split_group
            # API requires all participants to know what the split ranks should
            # be. In our case, workers not part of the new group don't know. So
            # instead we manually contribute a NOCOLOR ncclCommSplit call.
            default_pg = torch.distributed.distributed_c10d._get_default_group()
            # pyre-ignore[16]
            default_pg._get_backend(torch.device("cuda")).perform_nocolor_split(
                default_pg.bound_device_id
            )

    def SplitCommForProcessGroup(self, m: messages.SplitCommForProcessGroup):
        # Test whether this rank is in the mesh specified by the
        # SplitCommForProcessGroup command. We do this by attempting to
        # dereference the mesh ref; only the ranks that are on the mesh will
        # succeed.
        try:
            pg = self.resolve(m.remote_process_group).get()
            in_mesh = True
        except KeyError:
            in_mesh = False

        if in_mesh:
            # Create a split process group
            stream = self.resolve(m.stream).get()
            pg.device_mesh.create_process_group(
                stream, pg.dims, pg=m.remote_process_group
            )
        else:
            # this rank is not in the split group. We still need to participate
            # in the commSplit call, however.

            # This weird incantation is because the current default split_group
            # API requires all participants to know what the split ranks should
            # be. In our case, workers not part of the new group don't know. So
            # instead we manually contribute a NOCOLOR ncclCommSplit call.
            default_pg = torch.distributed.distributed_c10d._get_default_group()
            # pyre-ignore[16]
            default_pg._get_backend(torch.device("cuda")).perform_nocolor_split(
                default_pg.bound_device_id
            )

    def Reduce(self, m: messages.Reduce):
        stream: Stream = self.resolve(m.stream).get()
        source_mesh: DeviceMesh = self.resolve(m.source_mesh).get()
        assert len(m.dims) <= len(source_mesh.dims)
        if len(m.dims) > 1:
            assert m.reduction != "stack" and not m.scatter
        pg = source_mesh.get_process_group(stream, m.dims)
        local_tensor = self.resolve(m.local_tensor)
        out = None if m.out is None else self.resolve(m.out)
        output = self.cell(m.result)

        # we need N only for "stack", and in this case we asserted that that len(m.dims) = 1
        N = len(source_mesh.dims[m.dims[0]].members) if m.reduction == "stack" else -1

        def reducer(local_tensor, out):
            return _reduce(
                local_tensor,
                source_mesh,
                pg,
                N,
                m.reduction,
                m.scatter,
                m.inplace,
                out,
            )

        stream.collective_call(reducer, m.factory, local_tensor, output, out)

    def SendTensor(self, m: messages.SendTensor):
        send_stream: Stream = self.resolve(m.from_stream).get()
        recv_stream: Stream = self.resolve(m.to_stream).get()
        pg = self.send_recv_process_groups[(send_stream, recv_stream)]

        try:
            index = m.from_ranks.index(self.rank)
            send_to_rank = m.to_ranks[index]
        except ValueError:
            send_to_rank = None

        try:
            index = m.to_ranks.index(self.rank)
            recv_from_rank = m.from_ranks[index]
        except ValueError:
            recv_from_rank = None

        if send_to_rank is None:
            the_stream = recv_stream
        elif recv_from_rank is None:
            the_stream = send_stream
        elif send_stream is recv_stream:
            the_stream = send_stream
        else:
            raise NotImplementedError(
                "We haven't implemented to_mesh between streams if a rank participates as both a sender and receiver."
                "It is possible, but would require the recv stream to send the output buffer tensor to the send stream and sync."
                "Then the send stream would do the nccl op, and then sync with sending stream again."
            )

        def send_recv(
            input_tensor: torch.Tensor, out: Optional[torch.Tensor]
        ) -> Optional[torch.Tensor]:
            # we consider to_mesh to always copy a tensor. But if the
            # from and to rank are the same, we really do not have
            # copy it. In this case we do a copy-on-write via _lazy_clone.
            # The tensor will only be copied for real if someone later
            # tries to mutate it.
            if send_to_rank == recv_from_rank:
                return input_tensor._lazy_clone()
            ops = []
            P2POp = torch.distributed.P2POp
            isend, irecv = torch.distributed.isend, torch.distributed.irecv
            if send_to_rank is not None:
                ops.append(P2POp(isend, input_tensor, send_to_rank, pg))

            if recv_from_rank is not None:
                output = m.factory.empty()
                ops.append(P2POp(irecv, output, recv_from_rank, pg))
            else:
                output = None
            # invoke batched p2p ops
            for op in torch.distributed.batch_isend_irecv(ops):
                op.wait()
            return output

        input = Cell(None) if send_to_rank is None else self.resolve(m.tensor)
        output = Cell(None) if recv_from_rank is None else self.cell(m.result)
        the_stream.collective_call(send_recv, m.factory, input, output, None)

    def BackendNetworkInit(self, m: messages.BackendNetworkInit):
        if torch.distributed.is_initialized():
            return  # for restarts in tests
        store = torch.distributed.TCPStore(
            m.hostname or os.environ["STORE_HOSTNAME"],
            m.port or int(os.environ["STORE_PORT"]),
        )
        torch.distributed.init_process_group(
            backend="nccl",
            world_size=self.world,
            rank=self.rank,
            store=store,
            device_id=torch.device("cuda:0"),
        )
        b = torch.zeros(1, device="cuda")
        torch.distributed.all_reduce(b)

    def BackendNetworkPointToPointInit(
        self, m: messages.BackendNetworkPointToPointInit
    ):
        from_stream: Stream = self.resolve(m.from_stream).get()
        to_stream: Stream = self.resolve(m.to_stream).get()
        self.send_recv_process_groups[(from_stream, to_stream)] = _new_process_group(
            f"restart_{restart_count}_send_{from_stream.id}_recv_{to_stream.id}",
            None,
            split=False,
        )

    def DebuggerMessage(self, m: messages.DebuggerMessage):
        stream: Stream = self.env[m.stream_id].get()
        stream.debugger_queue.put(m.action)

    def define(self, r: Union[Ref, Referenceable], value: Any):
        assert isinstance(r, Ref)
        self.env[r.id] = Cell(value)

    def cell(self, r: Union[Ref, Referenceable]):
        assert isinstance(r, Ref)
        c = self.env[r.id] = Cell()
        if _tls.tracing:
            _tls.tracing.defined_cells[c] = r.id
        return c

    def _send_status(self, first_uncompleted_ident):
        if first_uncompleted_ident > self.last_send_status:
            self.q.send(messages.Status(first_uncompleted_ident))
            self.last_send_status = first_uncompleted_ident

    async def worker_loop(self):
        monitor = Monitor()
        monitor.start()
        self.loop = asyncio.get_event_loop()
        debugq = deque()
        while True:
            try:
                # eventually this event loop should be handled as a separate
                # thread (maybe not even python) that just takes and
                # responds to messages, with a strong guarentee of never
                # getting stuck. For now we just run everything on this thread.
                monitor(
                    lambda: (
                        logger.error(
                            f"possible stall while waiting for message: recent messages: {debugq} "
                            f"{self.max_received_ident=} {self.last_send_status=}"
                        ),
                        logger.setLevel(logging.INFO),
                    ),
                    30.0,
                )
                _, msg = await self.q.recv_async()
                logger.debug(f"event: {msg}, env={list(self.env.keys())}")
                monitor(
                    (
                        lambda msg=msg: logger.error(
                            f"possible stall while handling {msg}"
                        )
                    ),
                    30.0,
                )
                self.handle_message(msg)

                debugq.append(msg)
                while len(debugq) > 10:
                    debugq.popleft()
            except StopIteration:
                self.q.recvready(0)
                self.q.recvready(0.01)
                return
            except Exception as e:
                logger.exception("Worker event loop exiting with internal exception")
                self.internal_error(
                    messages.InternalException(e, extract_tb(e.__traceback__))
                )

    def schedule(self, fn: Callable[[], None]):
        assert self.loop is not None
        self.loop.call_soon_threadsafe(fn)

    def internal_error(self, msg: messages.InternalException):
        self.q.send(msg)
        assert self.loop is not None
        self.loop.stop()

    def event_loop(self):
        pdb.set_trace = _set_trace
        try:
            asyncio.run(self.worker_loop())
        except RuntimeError as e:
            if "Event loop stopped" in str(e):
                logger.warning("Event loop exiting after reporting an internal error.")

            else:
                raise


def worker_main(_restartable):
    rank = int(os.environ["RANK"])
    world = int(os.environ["WORLD_SIZE"])
    local_rank = int(os.environ["LOCAL_RANK"])
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        devices = os.environ["CUDA_VISIBLE_DEVICES"].split(",")
        device = devices[local_rank]
    else:
        device = str(local_rank)
    os.environ["CUDA_VISIBLE_DEVICES"] = device
    initialize_logging(process_name=f"worker_{rank}")
    logger.info("starting, restartable=%s, local_rank=%d", _restartable, local_rank)
    # force CUDA to initialize before do any multithreading. This is a
    # workaround until https://github.com/pytorch/pytorch/pull/143238 is
    # available everywhere.
    if torch.cuda.is_available():
        torch.ones(1, device="cuda")
    q = get_message_queue()
    global restart_count
    for restart in itertools.count():
        restart_count = restart
        worker = Worker(q, rank, world, local_rank)
        worker.event_loop()
        if not _restartable:
            break
        q.send(messages.Restarted(0))
        logger.info("restarting")


class ProcessPipe:
    """Pipe Process Pipe"""

    def __init__(self, key: str, max_messages):
        import zmq

        q = get_message_queue()
        self._sock = q._socket(zmq.PAIR)
        self._sock.setsockopt(zmq.SNDHWM, max_messages)
        self._sock.setsockopt(zmq.RCVHWM, max_messages)
        self._sock.connect(key)
        self.ranks = {}
        self.sizes = {}

    def send(self, any: Any):
        self._sock.send_pyobj(any)

    def recv(self):
        return self._sock.recv_pyobj()


def pipe_main(key: str, max_messages):
    """Main function for pipe process"""
    initialize_logging(f"pipe_{key}")
    pipe_obj = ProcessPipe(key, max_messages)
    rfunction, pipe_obj.ranks, pipe_obj.sizes, args, kwargs = pipe_obj.recv()
    function = rfunction.resolve()
    try:
        function(pipe_obj, *args, **kwargs)
    except Exception as e:
        logger.exception("pipe_main exiting with exception")
        get_message_queue().send(
            messages.RemoteGeneratorFailed(e, extract_tb(e.__traceback__))
        )
