# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import copy
import cProfile
import enum
import heapq
import io
import itertools
import json
import logging
import os
import pickle
import pstats
import subprocess
import tempfile
import time
import traceback
import warnings
from collections import defaultdict
from enum import auto
from functools import cache
from pathlib import Path
from typing import (
    Any,
    cast,
    Generator,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import torch
from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
)
from monarch._src.actor.shape import iter_ranks, NDSlice
from monarch.common import messages
from monarch.common.controller_api import LogMessage, MessageResult
from monarch.common.device_mesh import DeviceMesh
from monarch.common.function import ResolvableFunction, ResolvableFunctionFromPath
from monarch.common.invocation import DeviceException
from monarch.simulator.command_history import CommandHistory, DTensorRef
from monarch.simulator.config import META_VAL
from monarch.simulator.ir import IRGraph
from monarch.simulator.mock_controller import MockController
from monarch.simulator.profiling import (
    FakeRuntimeProfiler,
    RuntimeEstimator,
    RuntimeProfiler,
)
from monarch.simulator.task import Borrow, EventTask, Task
from monarch.simulator.tensor import FakeTensorTracker
from monarch.simulator.trace import (
    dump_memory_trace,
    dump_process_name,
    dump_thread_event_trace,
    MemoryViewer,
    TraceEvent,
    upload_trace,
)
from monarch.simulator.utils import (
    clean_name,
    compress_workers_range,
    file_path_with_iter,
)
from monarch.simulator.worker import Worker, WorkerGroup
from torch.utils._pytree import tree_leaves

logger = logging.getLogger(__name__)


class SimulatorBackendMode(enum.Enum):
    """
    An enum to specify the mode of the simulator.
    """

    # Simulates the commands, dumps the trace, and reports the simulated
    # execution time and memory. It is the default mode.
    SIMULATE = auto()
    # Simulates the commands and reports the simulated execution time and memory
    # without generating a trace.
    SIMULATE_WITH_REPORT_ONLY = auto()
    # Only records the commands without actually simulating them.
    COMMAND_HISTORY = auto()
    # SIMULATE + COMMAND_HISTORY
    EVERYTHING = auto()

    @property
    def simulation_enabled(self) -> bool:
        return self in (self.SIMULATE, self.SIMULATE_WITH_REPORT_ONLY, self.EVERYTHING)

    @property
    def command_history_enabled(self) -> bool:
        return self in (self.COMMAND_HISTORY, self.EVERYTHING)


class SimulatorTraceMode(enum.Enum):
    """
    An enum to specify the mode of the simulated trace.
    """

    # Only traces the controller
    CONTROLLER_TRACE_ONLY = auto()
    # Only traces the streams of all the workers.
    STREAM_ONLY = auto()
    # Traces all the streams of all the workers.
    EVERYTHING = auto()

    @property
    def stream_enabled(self) -> bool:
        return self in (self.STREAM_ONLY, self.EVERYTHING)

    @property
    def controller_enabled(self) -> bool:
        return self in (self.CONTROLLER_TRACE_ONLY, self.EVERYTHING)


def get_fake_tensor(x):
    if isinstance(x, (torch.Tensor, DTensorRef)):
        return x._fake
    return x


def get_ids(tree):
    if isinstance(tree, (torch.Tensor, DTensorRef)):
        tree = [tree]
    ids = {}
    for arg in tree_leaves(tree):
        if isinstance(arg, (torch.Tensor, DTensorRef)):
            ids[arg.ref] = arg._fake
    return ids


class Simulator:
    """
    A class to simulate the execution of the commands from the controller.
    It can be used to simulate on the fly with SimulatorBackend() or replay an
    existing trace with Simulator.replay().
    """

    def __init__(
        self,
        *,
        world_size: int = 0,
        profile: bool = False,
        replay_file: Optional[str] = None,
        trace_mode: SimulatorTraceMode = SimulatorTraceMode.EVERYTHING,
        upload_trace: bool = False,
        trace_path: str = "trace.json",
        group_workers: bool = False,
    ):
        self.command_history: Optional[CommandHistory] = None
        if replay_file:
            self.command_history = CommandHistory.load(replay_file)
            world_size = self.command_history.world_size

        if world_size <= 0:
            raise ValueError(
                f"{world_size=} is not correct.  Please specify a valid "
                "world_size or ensure the replay file contains the world_size."
            )

        self.runtime = RuntimeEstimator()
        # Use FakeRuntimeProfiler when CUDA is not available or not functional
        use_real_profiler = False
        cuda_device_count = 0
        try:
            cuda_device_count = torch.cuda.device_count()
            if cuda_device_count > 0 and torch.cuda.is_available():
                # Try a small CUDA operation and sync to verify CUDA actually works
                test_tensor = torch.zeros(1, device="cuda")
                # Force synchronization to catch any deferred CUDA errors
                torch.cuda.synchronize()
                del test_tensor
                use_real_profiler = True
        except Exception:
            pass

        if use_real_profiler:
            self.runtime_profiler = RuntimeProfiler(world_size=cuda_device_count)
        else:
            self.runtime_profiler = FakeRuntimeProfiler(world_size=world_size)
        self.events: List[TraceEvent] = []
        self.command_id = 0
        self.fake_tensor_tracker = FakeTensorTracker()

        self._worker_groups: List[WorkerGroup] = []
        self._workers: List[Worker] = []
        self._worker_group_mapping = np.zeros(1, dtype=np.int32)
        if group_workers:
            self._worker_groups = [
                WorkerGroup(
                    np.arange(world_size), self.fake_tensor_tracker, self.runtime
                )
            ]
            self._worker_group_mapping = np.zeros(world_size, dtype=np.int32)
        else:
            self._workers = [
                Worker(self.fake_tensor_tracker, self.runtime)
                for _ in range(world_size)
            ]

        self.worker_commands = defaultdict(list)
        self.now = 0
        self.profiler = cProfile.Profile() if profile else None
        self.simulation_time = 0.0
        self.trace_mode = trace_mode
        self.upload_trace = upload_trace
        self._debug = False
        self.trace_path = os.path.abspath(trace_path)
        self.current_traceback = []

    @property
    def workers(self) -> List[Worker]:
        if self._worker_groups:
            # why can't pyre figure out the upcasting?
            return cast(List[Worker], self._worker_groups)
        else:
            return self._workers

    def _print_worker0(self) -> None:
        if not self._debug:
            return

        for idx, stream in self.workers[0].streams.items():
            if stream.task_queue:
                logger.info(
                    (
                        self.now,
                        idx,
                        stream.task_queue[0],
                        stream.task_queue[0].state,
                        stream.task_queue[0].dependencies,
                        stream.tensors,
                    )
                )

    def _run(self) -> None:
        """
        This method simulates the execution of tasks on workers. It iteratively checks
        the status of workers and executes tasks in three stages: maybe_set_ready,
        maybe_execute, and maybe_finish. These stages are performed in separate loops
        to simulate asynchronous execution. The method continues until no status change
        occurs.
        """

        task_changed_status = True
        while task_changed_status:
            self._print_worker0()
            task_changed_status = False
            for worker in self.workers:
                task_changed_status = worker.maybe_set_ready() or task_changed_status
            for worker in self.workers:
                task_changed_status = worker.maybe_execute() or task_changed_status
            for worker in self.workers:
                task_changed_status = worker.maybe_finish() or task_changed_status

    def _print_profiler(self):
        if self.profiler is None:
            return
        s = io.StringIO()
        ps = pstats.Stats(self.profiler, stream=s).sort_stats(pstats.SortKey.CUMULATIVE)
        ps.print_stats()
        print(s.getvalue())
        print(
            f"Simulation run time, excluding loading the file: {self.simulation_time}."
        )

    def _rank_to_worker(self, ranks: List[NDSlice]) -> Generator[Worker, None, None]:
        for rank in ranks:
            for worker in rank:
                yield self._workers[worker]

    def _ndslice_to_worker_group(
        self, ranks: List[NDSlice]
    ) -> Generator[WorkerGroup, None, None]:
        # TODO: While we already use numpy array, this can still be quite slow
        # because iterating ranks happens in Python. We should cache the results
        # since we don't have that many different ranks combinations.

        workers_list = [np.array(list(iter(ranks_))) for ranks_ in ranks]
        workers = np.sort(np.concatenate(workers_list))
        groups = np.bincount(self._worker_group_mapping[workers])
        groups_iter = cast(Iterable, groups.flat)
        all_matches = all(
            len(self._worker_groups[group_id].workers) == element_count
            for group_id, element_count in enumerate(groups_iter)
            if element_count > 0
        )
        if all_matches:
            for group_id in np.nonzero(groups)[0].flat:
                yield self._worker_groups[group_id]
        else:
            new_groups = []
            participate_groups = []
            groups_iter = cast(Iterable, groups.flat)
            for group_id, element_count in enumerate(groups_iter):
                group = self._worker_groups[group_id]
                new_groups.append(group)
                if element_count > 0:
                    participate_groups.append(group)

                    not_participate_set = np.setdiff1d(
                        group.workers, workers, assume_unique=True
                    )
                    not_participate_group = group.split(not_participate_set)
                    new_groups.append(not_participate_group)
                    self._worker_group_mapping[not_participate_set] = (
                        len(new_groups) - 1
                    )
            self._worker_groups = new_groups
            for group in participate_groups:
                yield group

    def iter_workers(self, ranks: List[NDSlice]) -> Generator[Worker, None, None]:
        if self._worker_groups:
            yield from self._ndslice_to_worker_group(ranks)
        else:
            yield from self._rank_to_worker(ranks)

    def _report(self, trace_path: str = "", memory_view_path: str = ""):
        trace = []

        exec_time = 0.0
        max_mem = 0.0

        # perfetto treads tid and pid as part of the same namespace
        # (unlike chrome://trace). If they colleide then names will
        # get clobbered, so we assign unique ids to each individual
        # concept.
        id_iter = iter(itertools.count(1))

        @cache
        def to_id(key):
            return next(id_iter)

        dump_process_name(trace, pid=0, name="Controller")
        exec_time = max(
            exec_time,
            dump_thread_event_trace(
                trace, self.events, pid=0, tid=0, name="Controller"
            ),
        )

        if isinstance(self.workers[0], WorkerGroup):
            workers = sorted(self.workers, key=lambda g: min(g.workers))
        else:
            workers = self.workers

        memory_viewer = MemoryViewer()
        for worker_id, worker in enumerate(workers):
            if not worker.events:
                continue
            pid = to_id(("worker", worker_id))
            name = f"Device {worker_id}"
            if isinstance(worker, WorkerGroup):
                name = f"{name} {compress_workers_range(worker.workers)}"
            dump_process_name(trace=trace, pid=pid, name=name)
            # TODO: find a better tid for worker trace
            exec_time = max(
                dump_thread_event_trace(
                    trace, self.events, pid=pid, tid=32000, name=name
                ),
                exec_time,
            )

            for stream_id, stream in worker.streams.items():
                tid = to_id(("stream", worker_id, stream_id))
                exec_time = max(
                    dump_thread_event_trace(
                        trace, stream.events, pid=pid, tid=tid, name=stream.name
                    ),
                    exec_time,
                )

            # Get the memory order
            curr_mem = 0
            memory_viewer.next_device()
            mem_events = {
                stream_id: copy.copy(stream.memory.events)
                for stream_id, stream in worker.streams.items()
            }
            while True:
                min_ts = float("inf")
                min_stream_events = None
                min_stream_id = 0
                for stream_id, events in mem_events.items():
                    if events and min_ts > events[0][0]:
                        min_ts = events[0][0]
                        min_stream_id, min_stream_events = stream_id, events

                if min_stream_events is None:
                    break

                mem_ts, mem_addr, mem_delta, traceback = heapq.heappop(
                    min_stream_events
                )
                curr_mem += mem_delta
                max_mem = max(curr_mem, max_mem)
                dump_memory_trace(
                    trace,
                    pid=pid,
                    memory=curr_mem,
                    ts=mem_ts,
                    name="memory",
                )
                memory_viewer.add_trace(mem_addr, mem_delta, min_stream_id, traceback)

        if trace_path:
            with open(trace_path, "w") as f:
                json.dump({"traceEvents": trace}, f, indent=4)

            memory_viewer.dump(memory_view_path)

            if self.upload_trace:
                upload_trace(os.path.abspath(f.name))

        return exec_time / 10**6, max_mem / 10**6

    def step(self, iter_count: int, dump_trace: bool = False) -> Tuple[float, float]:
        """
        Step to the next iteration simulation and return the execution time in second
        and peak memory usage in MB of this iteration.
        """
        path = file_path_with_iter(self.trace_path, iter_count) if dump_trace else ""
        directory = os.path.dirname(path)
        memory_view_path = os.path.join(directory, "memory_view.pt")
        memory_view_path = file_path_with_iter(memory_view_path, iter_count)
        return self._report(path, memory_view_path)

    def exit(self, iter_count: int, dump_trace: bool = False) -> Tuple[float, float]:
        return self.step(iter_count, dump_trace)

    @classmethod
    def replay(cls, replay_file: str, profile: bool = False) -> None:
        self = cls(replay_file=replay_file, profile=profile)
        for command in cast(CommandHistory, self.command_history).commands:
            if command.backend_command != "send":
                continue
            assert command.ranks is not None
            self.send(command.timestamp, command.ranks, command.msg)
        self._report()
        self._print_profiler()

    # Methods below simulate the methods of a real backend.
    def send(self, now: int, ranks: List[NDSlice], msg) -> None:
        logger.debug(f"Sending {msg}  at {now}.")
        self.current_traceback = traceback.extract_stack()[:-3]
        command_name = type(msg).__name__
        self.command_id += 1
        # These two commands typically take a long time to execute on the
        # controller side. Ignoring them will make the simulation trace easier
        # to read.
        if self.trace_mode.controller_enabled and command_name not in (
            "CreateDeviceMesh",
            "CreateStream",
        ):
            if command_name != "CallFunction":
                meta = [command_name] + META_VAL
            else:
                meta = [clean_name(msg.function.path)] + META_VAL
            self.events.append(
                TraceEvent(
                    self.now,
                    now - self.now,
                    meta,
                    self.command_id,
                    self.current_traceback,
                )
            )

        if self.trace_mode.controller_enabled:
            self.now = now

        if not self.trace_mode.stream_enabled and command_name != "CommandGroup":
            return

        begin = time.monotonic()
        if self.profiler:
            self.profiler.enable()

        attr = getattr(self, command_name, None)
        if attr is None:
            # Instead of silently ignoring the unimplemented method, a warning
            # gives us the signal to review any newly implemented messages.
            warnings.warn(
                f"Simulator doesn't implement {type(msg).__name__} {msg}."
                "This can cause incorrect simulation.",
                stacklevel=2,
            )
            return

        attr(ranks, msg)
        self._run()

        if self.profiler:
            self.profiler.disable()
            self.simulation_time += time.monotonic() - begin

    def recvready(self):
        raise NotImplementedError()

    def propagate(self, msg: messages.SendValue) -> Any:
        assert isinstance(msg.function, ResolvableFunction)
        call_msg = messages.CallFunction(
            ident=0,
            result=None,
            mutates=(),
            function=msg.function,
            args=msg.args,
            kwargs=msg.kwargs,
            stream=None,  # pyre-ignore[6]
            device_mesh=None,  # pyre-ignore[6]
            remote_process_groups=[],
        )
        ret = self.runtime_profiler.profile_cmd(call_msg, [0])
        return ret[0][0]

    def Exit(self, ranks: List[NDSlice], msg: messages.Exit):
        return

    def CallFunction(self, ranks: List[NDSlice], msg: messages.CallFunction):
        inputs = get_ids(msg.args)
        outputs = get_ids(msg.result)
        if msg.mutates:
            outputs.update(get_ids(msg.mutates))
        self.fake_tensor_tracker.add(inputs)
        self.fake_tensor_tracker.add(outputs)
        stream = msg.stream.ref
        for worker in self.iter_workers(ranks):
            name = clean_name(str(msg.function))
            worker.add_task(
                Task(
                    inputs=list(inputs.keys()),
                    outputs=list(outputs.keys()),
                    command_id=self.command_id,
                    start_time=self.now,
                    runtime=self.runtime.get_runtime(msg),
                    meta=[name],
                    traceback=self.current_traceback,
                ),
                self.now,
                stream=stream,
            )

    def SendTensor(self, ranks: List[NDSlice], msg: messages.SendTensor):
        # NOTE: The memory usage calculation for SendTensor may not be accurate when
        # the source and destination ranks are the same. In such cases, memory usage
        # should increase if the result tensor is modified. However, this depends on
        # the specific implementation by the worker.

        inputs = get_ids(msg.tensor)
        outputs = get_ids(msg.result)
        self.fake_tensor_tracker.add(inputs)
        self.fake_tensor_tracker.add(outputs)
        if msg.from_stream is not msg.to_stream:
            raise NotImplementedError(
                "simulator using to_mesh between different streams"
            )
        stream = msg.from_stream.ref

        if msg.from_ranks == msg.to_ranks:
            for worker in self.iter_workers([msg.from_ranks]):
                worker.add_task(
                    Task(
                        inputs=list(inputs.keys()),
                        outputs=list(outputs.keys()),
                        command_id=self.command_id,
                        start_time=self.now,
                        runtime=self.runtime.get_runtime(msg),
                        meta=["SendTensor"],
                        traceback=self.current_traceback,
                    ),
                    self.now,
                    stream=stream,
                )
        else:
            collectives_pair = []
            for worker in self.iter_workers([msg.from_ranks]):
                collectives_pair.append([])
                worker.add_task(
                    Task(
                        inputs=list(inputs.keys()),
                        outputs=[],
                        command_id=self.command_id,
                        start_time=self.now,
                        runtime=self.runtime.get_runtime(msg),
                        meta=["SendTensor"],
                        collectives=collectives_pair[-1],
                        traceback=self.current_traceback,
                    ),
                    self.now,
                    stream=stream,
                )

            for worker, collectives in zip(
                self.iter_workers([msg.to_ranks]), collectives_pair, strict=True
            ):
                worker.add_task(
                    Task(
                        inputs=[],
                        outputs=list(outputs.keys()),
                        command_id=self.command_id,
                        start_time=self.now,
                        runtime=self.runtime.get_runtime(msg),
                        meta=["RecvTensor"],
                        collectives=collectives,
                        traceback=self.current_traceback,
                    ),
                    self.now,
                    stream=stream,
                )

    def CommandGroup(self, ranks: List[NDSlice], msg: messages.CommandGroup):
        for command in msg.commands:
            self.send(self.now, ranks, command)

    def CreateStream(self, ranks: List[NDSlice], msg: messages.CreateStream):
        for worker in self.iter_workers(ranks):
            assert msg.result.ref is not None
            worker.create_stream(msg.result.ref, msg.result.name, default=msg.default)

    def Reduce(self, ranks: List[NDSlice], msg: messages.Reduce):
        inputs = get_ids(msg.local_tensor)
        outputs = get_ids(msg.result)
        self.fake_tensor_tracker.add(inputs)
        self.fake_tensor_tracker.add(outputs)

        # TODO: controller doesn't implement reduce and scatter yet so it is
        # not possible to get such a request.
        if msg.reduction == "stack":
            if msg.scatter:
                meta_str = "all_to_all"
            else:
                meta_str = "all_gather"
        else:
            if msg.scatter:
                meta_str = "all_reduce"
            else:
                meta_str = "reduce_scatter"

        meta = [meta_str]
        stream = msg.stream.ref
        collectives = []
        for worker in self.iter_workers(ranks):
            worker.add_task(
                Task(
                    inputs=list(inputs.keys()),
                    outputs=list(outputs.keys()),
                    start_time=self.now,
                    runtime=self.runtime.get_runtime(msg),
                    meta=meta,
                    command_id=self.command_id,
                    collectives=collectives,
                    traceback=self.current_traceback,
                ),
                self.now,
                stream=stream,
            )

    def BorrowCreate(self, ranks: List[NDSlice], msg: messages.BorrowCreate):
        inputs = get_ids(msg.tensor)
        outputs = get_ids(msg.result)
        self.fake_tensor_tracker.add(inputs)
        self.fake_tensor_tracker.add(outputs, is_borrowed=True)
        from_stream = msg.from_stream.ref
        to_stream = msg.to_stream.ref
        assert from_stream is not None
        assert to_stream is not None
        borrow = Borrow(
            ident=msg.borrow,
            tensor_src_id=cast(int, cast(DTensorRef, msg.tensor).ref),
            tensor_dst_id=cast(int, cast(DTensorRef, msg.result).ref),
            from_stream=from_stream,
            to_stream=to_stream,
        )
        for worker in self.iter_workers(ranks):
            recorded_task = worker.streams[from_stream].record_event()
            # Note: there is no perfect way to set the start_time when the
            # controller timing is disabled -- the wait event's start time
            # may be very early like 0. This is because only the GPU events
            # are tracked and there are no other GPU events except for
            # communications and wait events on the communication stream.
            # However, if we let the event's start_time to be based on the
            # main stream's timing, we may lose other information.
            start_time = self.now
            wait_event = EventTask(
                recorded_task=recorded_task,
                event_stream=from_stream,
                event_stream_name=worker.streams[from_stream].name,
                wait_stream=to_stream,
                wait_stream_name=worker.streams[to_stream].name,
                command_id=self.command_id,
                start_time=start_time,
                borrow=borrow,
                runtime=self.runtime.get_runtime("wait_event"),
                traceback=self.current_traceback,
            )
            worker.borrow(wait_event, borrow)

    def BorrowFirstUse(self, ranks: List[NDSlice], msg: messages.BorrowFirstUse):
        for worker in self.iter_workers(ranks):
            worker.borrow_first_use(msg.borrow, self.now)

    def BorrowLastUse(self, ranks: List[NDSlice], msg: messages.BorrowLastUse):
        for worker in self.iter_workers(ranks):
            borrow_wait_event = worker.wait_events[msg.borrow]
            recorded_task = worker.streams[borrow_wait_event.wait_stream].record_event()
            last_use_event = EventTask(
                recorded_task=recorded_task,
                event_stream=borrow_wait_event.wait_stream,
                event_stream_name=worker.streams[borrow_wait_event.wait_stream].name,
                wait_stream=borrow_wait_event.event_stream,
                wait_stream_name=worker.streams[borrow_wait_event.event_stream].name,
                command_id=self.command_id,
                start_time=self.now,
                runtime=self.runtime.get_runtime("wait_event"),
                traceback=self.current_traceback,
            )
            worker.borrow_last_use(last_use_event, msg.borrow)

    def BorrowDrop(self, ranks: List[NDSlice], msg: messages.BorrowDrop):
        for worker in self.iter_workers(ranks):
            worker.borrow_drop(msg.borrow, self.now)

    def DeleteRefs(self, ranks: List[NDSlice], msg: messages.DeleteRefs):
        for worker in self.iter_workers(ranks):
            worker.delete_refs(msg.refs, self.now)

    def BackendNetworkInit(
        self, ranks: List[NDSlice], msg: messages.BackendNetworkInit
    ):
        return

    def CreatePipe(self, ranks: List[NDSlice], msg: messages.CreatePipe):
        # We don't have to track Pipe creation (yet).
        return

    def PipeRecv(self, ranks: List[NDSlice], msg: messages.PipeRecv):
        outputs = get_ids(msg.result)
        cpu_device = torch.device("cpu")
        self.fake_tensor_tracker.add(outputs)
        for fake in outputs.values():
            if fake.device != cpu_device:
                raise NotImplementedError("PipeRecv only support CPU device now.")

        for worker in self.iter_workers(ranks):
            for tensor_id in outputs.keys():
                worker.add_cpu_tensor(tensor_id, self.now)

    # Not doing anything for the following messages (yet).
    def SendValue(self, ranks: List[NDSlice], msg: messages.SendValue):
        return

    def CreateDeviceMesh(self, ranks: List[NDSlice], msg: messages.CreateDeviceMesh):
        return

    def RequestStatus(self, ranks: List[NDSlice], msg: messages.RequestStatus):
        return

    def SplitComm(self, ranks: List[NDSlice], msg: messages.SplitComm):
        return

    def BackendNetworkPointToPointInit(
        self, ranks: List[NDSlice], msg: messages.BackendNetworkPointToPointInit
    ):
        return

    def CreateRemoteProcessGroup(
        self, ranks: List[NDSlice], msg: messages.CreateRemoteProcessGroup
    ):
        return


class SimulatorController(MockController):
    """
    A backend that simulates the behavior of the ProcessBackend. It can also be
    used to only record the commands sent to it, and then replay them later using
    the `Simulator` class.

    Args:
        world_size (int): The number of workers in the simulation.
        grph_per_host (int): The number of GPUs per machine.
    """

    def __init__(
        self,
        world_size: int,
        gpu_per_host: int,
        *,
        simulate_mode: SimulatorBackendMode = SimulatorBackendMode.SIMULATE,
        trace_mode: SimulatorTraceMode = SimulatorTraceMode.EVERYTHING,
        upload_trace: bool = False,
        trace_path: str = "trace.json",
        command_history_path: str = "command_history.pkl",
        group_workers: bool = False,
        ir: Optional[IRGraph] = None,
    ):
        if len(DTensorRef.created) != 0:
            DTensorRef.created.clear()
            warnings.warn(
                "clearing old DTensorRef information. TODO: support multiple simulator backends in the same process.",
                stacklevel=1,
            )
        super().__init__(world_size, verbose=False)

        self._gpu_per_host = gpu_per_host
        self.timestamp_base = time.monotonic_ns()
        self.worker_commands = defaultdict(list)
        self.simulator: Optional[Simulator] = None
        self.command_history: Optional[CommandHistory] = None
        self.iter = 0
        self.mode = simulate_mode
        self.exception = False
        self.ir = ir

        if self.mode.command_history_enabled:
            self.command_history = CommandHistory(
                world_size, file_path=os.path.abspath(command_history_path)
            )

        if self.mode.simulation_enabled:
            self.simulator = Simulator(
                world_size=world_size,
                trace_mode=trace_mode,
                upload_trace=upload_trace,
                trace_path=trace_path,
                group_workers=group_workers,
            )

    @property
    def gpu_per_host(self) -> int:
        return self._gpu_per_host

    def cleanup_simulation(self):
        DTensorRef.created.clear()

    def __del__(self):
        self.cleanup_simulation()

    def step(self) -> Tuple[float, float]:
        """
        Step to the next iteration simulation and return the execution time in second
        and peak memory usage in MB of this iteration. If the simulation mode is
        COMMAND_HISTORY, then the return time and memory will be 0.0 as the backend
        only records the commands.
        """
        if self.command_history:
            self.command_history.step(self.iter)

        if self.simulator:
            exec_time, max_mem = self.simulator.step(
                self.iter,
                dump_trace=(
                    self.mode != SimulatorBackendMode.SIMULATE_WITH_REPORT_ONLY
                ),
            )
        else:
            exec_time = max_mem = 0.0

        self.iter += 1

        return exec_time, max_mem

    def _send(self, ranks: Union[NDSlice, List[NDSlice]], msg: NamedTuple) -> None:
        now = time.monotonic_ns() - self.timestamp_base

        if isinstance(ranks, NDSlice):
            ranks = [ranks]

        if self.command_history:
            command = self.command_history.record(
                now,
                "send",
                self.simulator.command_id if self.simulator else 0,
                self.simulator.current_traceback if self.simulator else (),
                ranks,
                msg,
                None,
                self.ir,
            )
        else:
            command = CommandHistory.convert_command(
                now,
                "send",
                self.simulator.command_id if self.simulator else 0,
                self.simulator.current_traceback if self.simulator else (),
                ranks,
                msg,
                None,
                self.ir,
            )

        if self.simulator:
            self.simulator.send(now, cast(List[NDSlice], command.ranks), command.msg)

        if type(msg).__name__ == "SendValue":
            msg = cast(messages.SendValue, msg)
            if (
                isinstance(msg.function, ResolvableFunctionFromPath)
                and msg.function.path == "monarch.cached_remote_function._propagate"
            ):
                assert self.simulator is not None
                assert msg.destination is None
                ret = self.simulator.propagate(msg)
                for _ in iter_ranks(ranks):
                    self.history.future_completed(msg.ident, ret)
                return

        if type(msg).__name__ not in ("CommandGroup",):
            return super().send(ranks, msg)

    def send(self, ranks: Union[NDSlice, List[NDSlice]], msg: NamedTuple) -> None:
        if self.exception:
            return

        try:
            self._send(ranks, msg)
        except Exception as e:
            self.exception = True
            # TODO: Should we also call simulator.exit() and cleanup?
            self.responses.append(
                MessageResult(
                    seq=0,  # will not be used
                    result=None,
                    error=DeviceException(
                        e,
                        traceback.extract_tb(e.__traceback__),
                        ActorId.from_string("unknown[0].unknown[0]"),
                        message="Simulator has an internal error.",
                    ),
                )
            )

    def next_message(
        self, timeout: Optional[float]
    ) -> Optional[MessageResult | LogMessage]:
        now = time.monotonic_ns() - self.timestamp_base

        if self.command_history:
            self.command_history.record(
                now,
                "next_message",
                self.simulator.command_id if self.simulator else 0,
                self.simulator.current_traceback if self.simulator else (),
                None,
                None,
                timeout,
                self.ir,
            )

        return super().next_message(timeout)

    def Exit(self, ranks: Union[NDSlice, List[NDSlice]], msg: messages.Exit):
        if self.command_history:
            self.command_history.dump(self.command_history.file_path)
        if self.simulator:
            self.simulator.exit(
                self.iter,
                dump_trace=(
                    self.mode != SimulatorBackendMode.SIMULATE_WITH_REPORT_ONLY
                ),
            )
        self.cleanup_simulation()

        return super().Exit(ranks, msg)


class SimulatorInterface:
    """
    API for interactive with simulator.
        sim.mesh retrieves the simulator mesh.
    """

    def __init__(
        self, mesh: "DeviceMesh", ctrl: "SimulatorController", ir: Optional["IRGraph"]
    ):
        self.mesh = mesh
        self._ctrl = ctrl
        self._ir = ir

    def upload(self):
        sim = self._ctrl.simulator
        old, sim.upload_trace = sim.upload_trace, True
        try:
            self._ctrl.step()
        finally:
            sim.upload_trace = old

    def _display_html(self, html_code):
        import base64

        from IPython.display import display, Javascript

        # Encode the HTML code in base64 to be passed to JavaScript, then
        # decode from base64 inside JavaScript. This is a hack to get this to
        # work properly in Bento.
        b64_html = base64.b64encode(html_code.encode("utf-8")).decode("utf-8")

        # JavaScript to open a new window and write the HTML
        js_code = f"""
        var newWindow = window.open("", "_blank");
        newWindow.document.write(atob("{b64_html}"));
        newWindow.document.close();
        window.open("").close()
        """

        # Display the JavaScript
        display(Javascript(js_code))

    def _run_trace2html(self, json_filename, html_filename):
        # Call the trace2html script to convert JSON to HTML
        for trace2html in [
            "trace2html",
            Path.home() / "fbsource/third-party/catapult/tracing/bin/trace2html",
        ]:
            try:
                subprocess.run(
                    [trace2html, json_filename, "--output", html_filename], check=True
                )
                return
            except FileNotFoundError:
                pass
        raise RuntimeError(
            "trace2html not found. `git clone https://chromium.googlesource.com/catapult` and add catapult/tracing/bin to PATH"
        )

    def _display_trace(self, json_filename, pkl_filename):
        # Create temporary files for JSON and HTML
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as html_file:
            html_filename = html_file.name

        self._run_trace2html(json_filename, html_filename)

        with open(pkl_filename, "rb") as pfile:
            # @lint-ignore PYTHONPICKLEISBAD
            memory_data = pickle.load(pfile)
            import torch.cuda._memory_viz as viz

            self._display_html(viz.trace_plot(memory_data))

        # Read the HTML content from the temporary HTML file
        with open(html_filename, "r") as file:
            html_code = file.read()
            self._display_html(html_code)

    def display(self):
        """
        From a jupyter notebook, open the trace report as a new window in your browser.
        Watch for popup blockers.
        """
        sim = self._ctrl.simulator
        with (
            tempfile.NamedTemporaryFile(suffix=".json", delete=False) as json_file,
            tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as memory_pkl,
        ):
            sim._report(trace_path=json_file.name, memory_view_path=memory_pkl.name)
            self._display_trace(json_file.name, memory_pkl.name)

    def export_ir(self, ir_path: str) -> None:
        """
        Exports the simulator internal representation (IR) to a file.
        Args:
            ir_path (str): The path to the file where the IR will be exported.
        """
        assert self._ir is not None, "Simulator IR does not exist!"
        with open(ir_path, "wb") as f:
            torch.save(self._ir, f)
