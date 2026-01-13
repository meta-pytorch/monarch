# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import contextlib
import copy
import enum
import functools
import multiprocessing
import os
import socket
import time
import traceback
from contextlib import closing
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

import torch
import torch.distributed as dist
from monarch.common import messages
from monarch.common.function import resolvable_function
from monarch.common.function_caching import (
    hashable_tensor_flatten,
    HashableTreeSpec,
    key_filters,
    TensorGroup,
)
from monarch.common.tensor_factory import TensorFactory
from monarch.simulator.command_history import CommandHistory, DTensorRef
from torch.utils import _pytree as pytree
from torch.utils._mode_utils import no_dispatch


def get_free_port() -> int:
    configs = [(socket.AF_INET6, "::1"), (socket.AF_INET, "127.0.0.1")]
    errors = []

    for addr_family, address in configs:
        with socket.socket(addr_family, socket.SOCK_STREAM) as s:
            try:
                s.bind((address, 0))
                s.listen(0)
                with closing(s):
                    return s.getsockname()[1]
            except Exception as e:
                errors.append(
                    f"Binding failed with address {address} while getting free port: {e}"
                )

    # If this is reached, we failed to bind to any of the configs
    raise Exception(", ".join(errors))


# These functions below are from cached_remote_function.py but depending on
# cached_remote_function.py can cauce dependency issues.
def _to_factory(x):
    if isinstance(x, torch.Tensor):
        return (TensorFactory.from_tensor(x), x.requires_grad)
    return x


def _filter_key(v: Any):
    for filter in key_filters:
        v = filter(v)
    return v


def _make_key(args, kwargs):
    values, spec = pytree.tree_flatten((args, kwargs))
    return tuple(_filter_key(v) for v in values), HashableTreeSpec.from_treespec(spec)


class ProfilingWorker:
    _float_types: Set[torch.dtype] = {
        torch.float16,
        torch.bfloat16,
        torch.float32,
        torch.float64,
    }

    def __init__(self, world_size, rank) -> None:
        self.world_size = world_size
        self.rank = rank
        self.counter = 0

    @contextlib.contextmanager
    def _worker_env(self) -> Generator[dist.TCPStore, None, None]:
        try:
            store = dist.TCPStore(
                os.environ["STORE_HOSTNAME"],
                int(os.environ["STORE_PORT"]),
                timeout=timedelta(seconds=10),
            )
            torch.cuda.set_device(self.rank)
            yield store
        finally:
            if dist.is_initialized():
                dist.destroy_process_group()

    # Adapted from: https://fburl.com/3xpyoq93
    # NB: returns fake tensors
    def _run_function(
        self, func: Callable, args: Any, kwargs: Any
    ) -> Tuple[int, Any | None]:
        """
        Runs and benchmarks a fallback kernel for a given function.

        Args:
            func (Callable): The function to benchmark.
            args (Tuple): The arguments to pass to the function.
            kwargs (Dict[str, Any]): The keyword arguments to pass to the function.

        Returns:
            Tuple[int, Any | None]: A tuple containing the mean operation time in nano-seconds
                and the result of the function.
        """
        # these should all be supported, just to be safe
        # avoid fallback for operators which inplace modify metadata
        # because the input fake tensors would be umodified

        if torch.Tag.inplace_view in getattr(func, "tags", ()):
            raise NotImplementedError

        if args is None:
            args = ()

        if kwargs is None:
            kwargs = {}

        warmup_iters, actual_iters = 2, 3
        # We have to deecopy before entering `no_dispatch()` context so that
        # the copy won't materialize the fake tensor to a tensor automatically.
        args_copies = [
            copy.deepcopy(args) for _ in range(warmup_iters + actual_iters + 1)
        ]
        kwargs_copies = [
            copy.deepcopy(kwargs) for _ in range(warmup_iters + actual_iters + 1)
        ]

        with no_dispatch():
            materialized_tensors = {}

            def to_real_tensor(e):  # type: ignore[no-untyped-def]
                if isinstance(e, DTensorRef):
                    ref = e.ref

                    # TODO: Should we investigate this issue or not
                    # much we can do?
                    # Context: caching the materilized tensors won't work for
                    # TE's backward. It will crash without throwing any exception.
                    # out = materialized_tensors.get(ref, None)
                    out = None
                    if out is None:
                        e = e._fake
                        assert e is not None
                        if e.dtype in self._float_types:
                            out = torch.rand_like(e, device=e.fake_device)
                        else:
                            out = torch.ones_like(e, device=e.fake_device)
                        if e.is_sparse:
                            out._coalesced_(e.is_coalesced())
                        materialized_tensors[ref] = out
                    return out
                return e

            def materialize():
                args = args_copies.pop()
                kwargs = kwargs_copies.pop()
                flat_args, args_spec = pytree.tree_flatten((args, kwargs))
                flat_args = [to_real_tensor(a) for a in flat_args]
                args, kwargs = pytree.tree_unflatten(flat_args, args_spec)
                return args, kwargs

            args, kwargs = materialize()
            r = func(*args, **kwargs)

            warmup_iters, actual_iters = 2, 3
            for _ in range(warmup_iters):
                args, kwargs = materialize()
                func(*args, **kwargs)

            start_event = torch.cuda.Event(enable_timing=True)
            end_event = torch.cuda.Event(enable_timing=True)
            start_event.record(torch.cuda.current_stream())
            for _ in range(actual_iters):
                args, kwargs = materialize()
                func(*args, **kwargs)
            end_event.record(torch.cuda.current_stream())
            torch.cuda.synchronize()
            cuda_time = start_event.elapsed_time(end_event)
            mean_op_time = int(cuda_time / actual_iters * 1000)

        return r, mean_op_time

    def CallFunction(self, msg) -> None:
        func = msg.function.resolve()
        ret = self._run_function(func, msg.args, msg.kwargs)

        count = 2**31

        def tensor_to_dtensor_ref(t):
            nonlocal count
            count += 1
            t.ref = count
            return DTensorRef(t)

        return pytree.tree_map_only(torch.Tensor, tensor_to_dtensor_ref, ret)

    def run(self, conn) -> None:
        with self._worker_env() as store:
            try:
                while True:
                    msg = conn.recv()
                    if msg == "exit":
                        break
                    elif msg == "init_pg":
                        if not dist.is_initialized:
                            dist.init_process_group(
                                backend="nccl",
                                world_size=self.world_size,
                                rank=self.rank,
                                store=store,
                            )
                    else:
                        ret = self.CallFunction(msg)
                        conn.send(("result", ret))
                    self.counter += 1
            except Exception:
                conn.send(("exception", traceback.format_exc()))
            finally:
                conn.close()


class RuntimeProfiler:
    def __init__(self, world_size: int = 8, port: int = -1) -> None:
        # TODO: Add a cached mode to save the results into a pickle file so that
        # we can reuse the result without running anything.
        self.world_size = world_size
        self.port = port if port > 0 else get_free_port()
        self._initizlied = False
        self.parent_conns: List[multiprocessing.connection.Connection] = []
        self.cached: Dict[Tuple[Any, ...], Any] = {}

    def _lazy_init(self):
        if self._initizlied:
            return

        self.store = dist.TCPStore("localhost", self.port, is_master=True)
        self.processes = []
        self.world_size = self.world_size
        ctx = multiprocessing.get_context("spawn")
        os.environ["STORE_HOSTNAME"] = "localhost"
        os.environ["STORE_PORT"] = str(self.port)
        for i in range(self.world_size):
            parent_conn, child_conn = multiprocessing.Pipe()
            worker = ProfilingWorker(self.world_size, i)
            self.processes.append(
                ctx.Process(target=worker.run, args=(child_conn,), daemon=True),
            )
            self.parent_conns.append(parent_conn)
            self.processes[-1].start()

        self._initizlied = True

    def __exit__(self) -> None:
        if self._initizlied:
            for i in range(self.world_size):
                conn = self.parent_conns[i]
                conn.send("exit")
            time.sleep(0.1)

    def profile_cmd(self, cmd, ranks) -> List[Any | None]:
        self._lazy_init()

        ret = []
        assert type(cmd).__name__ == "CallFunction"
        cmd = CommandHistory.convert_msg(cmd)
        cmd = cmd._replace(function=resolvable_function(cmd.function))

        def dtensor_ref_filter(v: Any):
            if isinstance(v, DTensorRef):
                return v.factory
            return v

        key_filters.append(dtensor_ref_filter)
        tensors, shape_key = hashable_tensor_flatten((cmd, ranks), {})
        inputs_group = TensorGroup([t._fake for t in tensors])  # pyre-ignore[16]
        requires_grads = tuple(t.requires_grad for t in tensors)
        key = (shape_key, inputs_group.pattern, requires_grads)
        key_filters.pop()
        # key = _make_key((cmd, ranks), None)
        if key in self.cached:
            return self.cached[key]

        for i in ranks:
            conn = self.parent_conns[i]
            conn.send(cmd)

        # This cannot be merged to the previous for loop. A deadlock can happen.
        for _ in ranks:
            ret.append(conn.recv())

        clean_ret = []
        for r in ret:
            if r[0] == "exception":
                raise RuntimeError(r[1])
            clean_ret.append(r[1])

        self.cached[key] = clean_ret
        return clean_ret


class FakeRuntimeProfiler:
    """A fake runtime profiler that uses FakeTensorMode instead of CUDA.

    This profiler can be used when running simulations without actual GPUs.
    It executes operations using FakeTensorMode and returns dummy timing
    information, allowing the simulator to function without CUDA.
    """

    def __init__(self, world_size: int = 8) -> None:
        self.world_size = world_size
        self.cached: Dict[Tuple[Any, ...], Any] = {}

    def profile_cmd(self, cmd, ranks) -> List[Any | None]:
        from torch._subclasses.fake_tensor import FakeTensorMode

        assert type(cmd).__name__ == "CallFunction"
        cmd = CommandHistory.convert_msg(cmd)
        cmd = cmd._replace(function=resolvable_function(cmd.function))

        def dtensor_ref_filter(v: Any):
            if isinstance(v, DTensorRef):
                return v.factory
            return v

        key_filters.append(dtensor_ref_filter)
        tensors, shape_key = hashable_tensor_flatten((cmd, ranks), {})
        inputs_group = TensorGroup([t._fake for t in tensors])  # pyre-ignore[16]
        requires_grads = tuple(t.requires_grad for t in tensors)
        key = (shape_key, inputs_group.pattern, requires_grads)
        key_filters.pop()

        if key in self.cached:
            return self.cached[key]

        # Execute the function using FakeTensorMode instead of real CUDA
        func = cmd.function.resolve()
        args = cmd.args
        kwargs = cmd.kwargs if cmd.kwargs else {}

        def to_fake_tensor(e: Any) -> Any:
            if isinstance(e, DTensorRef):
                return e._fake
            return e

        flat_args, args_spec = pytree.tree_flatten((args, kwargs))
        flat_args = [to_fake_tensor(a) for a in flat_args]
        args, kwargs = pytree.tree_unflatten(flat_args, args_spec)

        with FakeTensorMode():
            with no_dispatch():
                # Execute the function with fake tensors
                try:
                    r = func(*args, **kwargs)
                except Exception:
                    # If the function fails, return zeros
                    r = torch.tensor(0.0)

        # Convert result to DTensorRef format expected by caller
        count = 2**31

        def tensor_to_dtensor_ref(t: Any) -> Any:
            nonlocal count
            if isinstance(t, torch.Tensor):
                count += 1
                # pyre-ignore[16]: Dynamically adding ref attribute for tracking
                t.ref = count
                return DTensorRef(t)
            return t

        ret = pytree.tree_map_only(torch.Tensor, tensor_to_dtensor_ref, r)
        # Return as list of (result, timing) tuples
        result = [((ret, 0),) for _ in ranks]

        self.cached[key] = result
        return result


def _return_if_exist(attr):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            user_fn = getattr(self, attr)
            if isinstance(user_fn, int):
                return user_fn
            elif callable(user_fn):
                return user_fn(*args, **kwargs)
            return func(self, *args, **kwargs)

        return wrapper

    return decorator


class TimingType(str, enum.Enum):
    SEND_TENSOR = "_send_tensor_time"
    REDUCE = "_reduce_time"
    CALL_FUNCTION = "_call_function_time"
    KERNEL_LAUNCH = "_kernel_launch_time"
    WAIT_EVENT = "_wait_event_time"


TimingFunction = Callable[[Optional[NamedTuple]], int]


class RuntimeEstimator:
    def __init__(self) -> None:
        self._call_function_time: TimingFunction | int | None = None
        self._reduce_time: TimingFunction | int | None = None
        self._send_tensor_time: TimingFunction | int | None = None
        self._wait_event_time: int | None = None
        self._kernel_launch_time: int | None = None

    @_return_if_exist("_send_tensor_time")
    def _get_send_tensor_time(self, msg: messages.SendTensor) -> int:
        if msg.from_ranks == msg.to_ranks:
            return 1_000
        return 100_000

    @_return_if_exist("_reduce_time")
    def _get_reduce_time(self, msg: messages.Reduce) -> int:
        return 100_000

    @_return_if_exist("_call_function_time")
    def _get_call_function_time(self, msg: messages.CallFunction) -> int:
        return 10_000

    @_return_if_exist("_kernel_launch_time")
    def _get_kernel_launch_time(self) -> int:
        return 500

    @_return_if_exist("_wait_event_time")
    def _get_wait_event_time(self) -> int:
        return 500

    def set_custom_timing(
        self, func_or_time: Dict[TimingType, TimingFunction | int]
    ) -> None:
        """
        Set custom timing values for specific message types or events.

        This method allows the user to define custom timing values for various
        operations in the simulator. The timing can be specified either as a fixed
        integer value or as a function that computes the timing dynamically.
        All the integer values are in nanoseconds.

        Args:
            func_or_time (Dict[TimingType, TimingFunction | int]): A dictionary
                mapping TimingType to either a function or an integer. If a function
                is provided, it should accept an optional NamedTuple as input and
                return an integer representing the timing in nanoseconds.

        Raises:
            AssertionError: If the values in the dictionary are neither integers
                nor callable functions.
        """
        for k, v in func_or_time.items():
            assert isinstance(v, int) or callable(v), (
                "The supported customized timing are an integer or a function."
            )
            setattr(self, k.value, v)

    def get_runtime(self, msg) -> int:
        match msg:
            case messages.CallFunction():
                return self._get_call_function_time(msg)
            case messages.Reduce():
                return self._get_reduce_time(msg)
            case messages.SendTensor():
                return self._get_send_tensor_time(msg)
            case "kernel_launch":
                return self._get_kernel_launch_time()
            case "wait_event":
                return self._get_wait_event_time()
            case _:
                raise ValueError(f"Get an unexpected message for profiling, {msg}.")
