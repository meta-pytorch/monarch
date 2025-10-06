# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import ctypes
import enum
import importlib.resources
import logging
import operator
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
from types import ModuleType
from typing import cast, Dict, Tuple

import pytest

import torch
from monarch._rust_bindings.monarch_hyperactor.actor import (
    PythonMessage,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.alloc import Alloc, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    PortId,
    PortRef,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent

from monarch._src.actor.actor_mesh import ActorMesh, Channel, context, Port
from monarch._src.actor.allocator import AllocHandle, ProcessAllocator
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import (
    create_local_host_mesh,
    fake_in_process_host,
    HostMesh,
)
from monarch._src.actor.proc_mesh import (
    _get_bootstrap_args,
    get_or_spawn_controller,
    ProcMesh,
)
from monarch._src.actor.v1.host_mesh import (
    _bootstrap_cmd,
    fake_in_process_host as fake_in_process_host_v1,
    HostMesh as HostMeshV1,
    this_host as this_host_v1,
    this_proc as this_proc_v1,
)
from monarch._src.actor.v1.proc_mesh import ProcMesh as ProcMeshV1

from monarch.actor import (
    Accumulator,
    Actor,
    current_actor_name,
    current_rank,
    current_size,
    endpoint,
    this_host,
    this_proc,
)
from monarch.tools.config import defaults
from typing_extensions import assert_type


class ApiVersion(enum.Enum):
    V0 = "v0"
    V1 = "v1"


needs_cuda = pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA not available",
)


class Counter(Actor):
    def __init__(self, v: int):
        self.v = v

    @endpoint
    async def incr(self):
        self.v += 1

    @endpoint
    async def value(self) -> int:
        return self.v

    @endpoint
    def value_sync_endpoint(self) -> int:
        return self.v


class Indirect(Actor):
    @endpoint
    async def call_value(self, c: Counter) -> int:
        return await c.value.choose()


def spawn_procs_on_host(
    host: HostMesh | HostMeshV1, per_host: Dict[str, int]
) -> ProcMesh | ProcMeshV1:
    if isinstance(host, HostMeshV1):
        return host.spawn_procs(name="proc", per_host=per_host)
    else:
        return host.spawn_procs(per_host)


def spawn_procs_on_fake_host(
    api_ver: ApiVersion, per_host: Dict[str, int]
) -> ProcMesh | ProcMeshV1:
    match api_ver:
        case ApiVersion.V1:
            return spawn_procs_on_host(fake_in_process_host_v1("fake_host"), per_host)
        case ApiVersion.V0:
            return spawn_procs_on_host(fake_in_process_host(), per_host)
        case _:
            raise ValueError(f"Unknown API version: {api_ver}")


def spawn_procs_on_this_host(
    api_ver: ApiVersion, per_host: Dict[str, int]
) -> ProcMesh | ProcMeshV1:
    match api_ver:
        case ApiVersion.V1:
            return spawn_procs_on_host(this_host_v1(), per_host)
        case ApiVersion.V0:
            return spawn_procs_on_host(this_host(), per_host)
        case _:
            raise ValueError(f"Unknown API version: {api_ver}")


def get_this_proc(api_ver: ApiVersion):
    match api_ver:
        case ApiVersion.V1:
            return this_proc_v1()
        case ApiVersion.V0:
            return this_proc()
        case _:
            raise ValueError(f"Unknown API version: {api_ver}")


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_choose(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    v = proc.spawn("counter", Counter, 3)
    i = proc.spawn("indirect", Indirect)
    v.incr.broadcast()
    result = await v.value.choose()

    # Test that Pyre derives the correct type for result (int, not Any)
    assert_type(result, int)
    result2 = await i.call_value.choose(v)

    assert result == result2

    result3 = await v.value_sync_endpoint.choose()
    assert_type(result, int)
    assert result2 == result3


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_stream(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    v = proc.spawn("counter2", Counter, 3)
    v.incr.broadcast()

    assert 8 == sum([await x for x in v.value.stream()])


class To(Actor):
    @endpoint
    async def whoami(self):
        return current_actor_name()


class From(Actor):
    @endpoint
    async def fetch(self, to: To):
        return [await x for x in to.whoami.stream()]


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_mesh_passed_to_mesh(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    f = proc.spawn("from", From)
    t = proc.spawn("to", To)
    all = [y for x in f.fetch.stream(t) for y in await x]
    assert len(all) == 4
    assert all[0] != all[1]


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_mesh_passed_to_mesh_on_different_proc_mesh(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    proc2 = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    f = proc.spawn("from", From)
    t = proc2.spawn("to", To)
    all = [y for x in f.fetch.stream(t) for y in await x]
    assert len(all) == 4
    assert all[0] != all[1]


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_actor_slicing(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    proc2 = spawn_procs_on_fake_host(api_ver, {"gpus": 2})

    f = proc.spawn("from", From)
    t = proc2.spawn("to", To)

    assert t.slice(gpus=0).whoami.call().get() != t.slice(gpus=1).whoami.call().get()

    result = [y for x in f.fetch.stream(t.slice(gpus=0)) for y in x.get()]
    assert len(result) == 2

    assert result[0] == result[1]


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_aggregate(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    counter = proc.spawn("counter", Counter, 1)
    counter.incr.broadcast()
    acc = Accumulator(counter.value, 0, operator.add)
    r = await acc.accumulate()
    assert r == 4


class RunIt(Actor):
    @endpoint
    async def run(self, fn):
        return fn()

    @endpoint
    async def return_current_rank_str(self):
        return str(current_rank())


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_rank_size(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    r = proc.spawn("runit", RunIt)

    acc = Accumulator(r.run, 0, operator.add)

    assert 1 == await acc.accumulate(lambda: current_rank()["gpus"])
    assert 4 == await acc.accumulate(lambda: current_size()["gpus"])


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_rank_string(api_ver: ApiVersion):
    match api_ver:
        case ApiVersion.V1:
            per_host = {"gpus": 2}
        case ApiVersion.V0:
            per_host = {"hosts": 1, "gpus": 2}
        case _:
            raise ValueError(f"Unknown API version: {api_ver}")
    proc = spawn_procs_on_fake_host(api_ver, per_host)
    r = proc.spawn("runit", RunIt)
    vm = r.return_current_rank_str.call().get()
    r0 = vm.flatten("r").slice(r=0).item()
    r1 = vm.flatten("r").slice(r=1).item()
    assert r0 == "{'hosts': 0/1, 'gpus': 0/2}"
    assert r1 == "{'hosts': 0/1, 'gpus': 1/2}"


class SyncActor(Actor):
    @endpoint
    def sync_endpoint(self, a_counter: Counter):
        return a_counter.value.choose().get()


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_sync_actor(api_ver: ApiVersion):
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    a = proc.spawn("actor", SyncActor)
    c = proc.spawn("counter", Counter, 5)
    r = await a.sync_endpoint.choose(c)
    assert r == 5


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_sync_actor_sync_client(api_ver: ApiVersion) -> None:
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    a = proc.spawn("actor", SyncActor)
    c = proc.spawn("counter", Counter, 5)
    r = a.sync_endpoint.choose(c).get()
    assert r == 5


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_proc_mesh_size(api_ver: ApiVersion) -> None:
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    assert 2 == proc.size("gpus")


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_rank_size_sync(api_ver: ApiVersion) -> None:
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    r = proc.spawn("runit", RunIt)

    acc = Accumulator(r.run, 0, operator.add)
    assert 1 == acc.accumulate(lambda: current_rank()["gpus"]).get()
    assert 4 == acc.accumulate(lambda: current_size()["gpus"]).get()


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_accumulate_sync(api_ver: ApiVersion) -> None:
    proc = spawn_procs_on_fake_host(api_ver, {"gpus": 2})
    counter = proc.spawn("counter", Counter, 1)
    counter.incr.broadcast()
    acc = Accumulator(counter.value, 0, operator.add)
    r = acc.accumulate().get()
    assert r == 4


class CastToCounter(Actor):
    @endpoint
    def doit(self, c: Counter):
        return list(c.value.call().get())


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_value_mesh(api_ver: ApiVersion) -> None:
    match api_ver:
        case ApiVersion.V1:
            per_host = {"gpus": 2}
        case ApiVersion.V0:
            per_host = {"hosts": 1, "gpus": 2}
        case _:
            raise ValueError(f"Unknown API version: {api_ver}")
    proc = spawn_procs_on_fake_host(api_ver, per_host)
    counter = proc.spawn("counter", Counter, 0)
    counter.slice(hosts=0, gpus=1).incr.broadcast()
    x = counter.value.call().get()
    assert 0 == x.item(hosts=0, gpus=0)
    assert 1 == x.item(hosts=0, gpus=1)
    assert 1 == x.slice(hosts=0, gpus=1).item()
    n = proc.spawn("ctc", CastToCounter)
    assert list(x) == n.slice(gpus=0).doit.call_one(counter).get()


@pytest.mark.timeout(60)
def test_rust_binding_modules_correct() -> None:
    """
    This tests that rust bindings will survive pickling correctly.

    To correctly define a rust binding, either

    (1) Set its module to "monarch._rust_bindings.rust_crate.rust_module",
        and make sure it is registered in monarch_extension/lib.rs
    (2) Set its module to some existing python file, and use @rust_struct to install
        the rust struct in that file and patch in any python extension methods.
    """
    import monarch._rust_bindings as bindings

    def check(module, path):
        for name, value in module.__dict__.items():
            if name.startswith("__"):
                continue
            if isinstance(value, ModuleType):
                check(value, f"{path}.{name}")
            elif hasattr(value, "__module__"):
                value_module = importlib.import_module(value.__module__)
                resolved_value = getattr(value_module, value.__name__)
                assert value is resolved_value

    check(bindings, "monarch._rust_bindings")


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_proc_mesh_liveness(api_ver: ApiVersion) -> None:
    mesh = spawn_procs_on_this_host(api_ver, {"gpus": 2})
    counter = mesh.spawn("counter", Counter, 1)
    del mesh
    # Give some time for the mesh to have been shut down.
    # (It only would if there were a bug.)
    time.sleep(0.5)
    counter.value.call().get()


class TLSActor(Actor):
    """An actor that manages thread-local state."""

    def __init__(self):
        self.local = threading.local()
        self.local.value = 0

    @endpoint
    def increment(self):
        self.local.value += 1

    @endpoint
    async def increment_async(self):
        self.local.value += 1

    @endpoint
    def get_value(self):
        return self.local.value

    @endpoint
    async def get_async(self):
        return self.local.value


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_actor_tls(api_ver: ApiVersion) -> None:
    """Test that thread-local state is respected."""
    pm = spawn_procs_on_this_host(api_ver, {"gpus": 1})
    am = pm.spawn("tls", TLSActor)
    await am.increment.call_one()
    await am.increment_async.call_one()
    await am.increment.call_one()
    await am.increment_async.call_one()

    assert 4 == await am.get_value.call_one()
    assert 4 == await am.get_async.call_one()


class TLSActorFullSync(Actor):
    """An actor that manages thread-local state."""

    def __init__(self):
        self.local = threading.local()
        self.local.value = 0

    @endpoint
    def increment(self):
        self.local.value += 1

    @endpoint
    def get_value(self):
        return self.local.value


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_actor_tls_full_sync(api_ver: ApiVersion) -> None:
    """Test that thread-local state is respected."""
    pm = spawn_procs_on_this_host(api_ver, {"gpus": 1})
    am = pm.spawn("tls", TLSActorFullSync)
    await am.increment.call_one()
    await am.increment.call_one()
    await am.increment.call_one()
    await am.increment.call_one()

    assert 4 == await am.get_value.call_one()


class AsyncActor(Actor):
    def __init__(self):
        self.should_exit = False

    @endpoint
    async def sleep(self) -> None:
        while True and not self.should_exit:
            await asyncio.sleep(1)

    @endpoint
    async def no_more(self) -> None:
        self.should_exit = True


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_async_concurrency(api_ver: ApiVersion):
    """Test that async endpoints will be processed concurrently."""
    pm = spawn_procs_on_this_host(api_ver, {})
    am = pm.spawn("async", AsyncActor)
    fut = am.sleep.call()
    # This call should go through and exit the sleep loop, as long as we are
    # actually concurrently processing messages.
    await am.no_more.call()
    await fut


async def awaitit(f):
    return await f


# def test_actor_future() -> None:
#     v = 0

#     async def incr():
#         nonlocal v
#         v += 1
#         return v

#     # can use async implementation from sync
#     # if no non-blocking is provided
#     f = Future(impl=incr, requires_loop=False)
#     assert f.get() == 1
#     assert v == 1
#     assert f.get() == 1
#     assert asyncio.run(awaitit(f)) == 1

#     f = Future(impl=incr, requires_loop=False)
#     assert asyncio.run(awaitit(f)) == 2
#     assert f.get() == 2

#     async def incr2():
#         nonlocal v
#         v += 2
#         return v

#     # Use non-blocking optimization if provided
#     f = Future(impl=incr2)
#     assert f.get() == 4

#     async def nope():
#         nonlocal v
#         v += 1
#         raise ValueError("nope")

#     f = Future(impl=nope, requires_loop=False)

#     with pytest.raises(ValueError):
#         f.get()

#     assert v == 5

#     with pytest.raises(ValueError):
#         f.get()

#     assert v == 5

#     with pytest.raises(ValueError):
#         asyncio.run(awaitit(f))

#     assert v == 5

#     async def nope2():
#         nonlocal v
#         v += 1
#         raise ValueError("nope")

#     f = Future(impl=nope2)

#     with pytest.raises(ValueError):
#         f.get()

#     assert v == 6

#     with pytest.raises(ValueError):
#         f.result()

#     assert f.exception() is not None

#     assert v == 6

#     with pytest.raises(ValueError):
#         asyncio.run(awaitit(f))

#     assert v == 6

#     async def seven():
#         return 7

#     f = Future(impl=seven, requires_loop=False)

#     assert 7 == f.get(timeout=0.001)

#     async def neverfinish():
#         f = asyncio.Future()
#         await f

#     f = Future(impl=neverfinish, requires_loop=True)

#     with pytest.raises(asyncio.exceptions.TimeoutError):
#         f.get(timeout=0.1)


class Printer(Actor):
    def __init__(self) -> None:
        self._logger: logging.Logger = logging.getLogger()

    @endpoint
    async def print(self, content: str) -> None:
        print(f"{content}", flush=True)
        sys.stdout.flush()
        sys.stderr.flush()

    @endpoint
    async def log(self, content: str) -> None:
        self._logger.error(f"{content}")
        for handler in self._logger.handlers:
            handler.flush()
        sys.stdout.flush()
        sys.stderr.flush()

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        # Don't throw an error on undeliverable messages. This actor is used in a test for
        # stopping actor meshes, and if we throw an error here then there is a race between
        # the asserted error that the mesh was stopped and the supervision error that a message
        # wasn't delivered.
        self._logger.error(f"Ignoring undeliverable message: {message}")
        return True


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_actor_log_streaming(api_ver: ApiVersion) -> None:
    # Save original file descriptors
    original_stdout_fd = os.dup(1)  # stdout
    original_stderr_fd = os.dup(2)  # stderr

    try:
        # Create temporary files to capture output
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False
        ) as stdout_file, tempfile.NamedTemporaryFile(
            mode="w+", delete=False
        ) as stderr_file:
            stdout_path = stdout_file.name
            stderr_path = stderr_file.name

            # Redirect file descriptors to our temp files
            # This will capture both Python and Rust output
            os.dup2(stdout_file.fileno(), 1)
            os.dup2(stderr_file.fileno(), 2)

            # Also redirect Python's sys.stdout/stderr for completeness
            original_sys_stdout = sys.stdout
            original_sys_stderr = sys.stderr
            sys.stdout = stdout_file
            sys.stderr = stderr_file

            try:
                pm = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
                am = pm.spawn("printer", Printer)

                # Disable streaming logs to client
                await pm.logging_option(
                    stream_to_client=False, aggregate_window_sec=None
                )
                await asyncio.sleep(1)

                # These should not be streamed to client initially
                for _ in range(5):
                    await am.print.call("no print streaming")
                    await am.log.call("no log streaming")
                await asyncio.sleep(1)

                # Enable streaming logs to client
                await pm.logging_option(
                    stream_to_client=True, aggregate_window_sec=1, level=logging.FATAL
                )
                # Give it some time to reflect
                await asyncio.sleep(1)

                # These should be streamed to client
                for _ in range(5):
                    await am.print.call("has print streaming")
                    await am.log.call("no log streaming due to level mismatch")
                await asyncio.sleep(1)

                # Enable streaming logs to client
                await pm.logging_option(
                    stream_to_client=True, aggregate_window_sec=1, level=logging.ERROR
                )
                # Give it some time to reflect
                await asyncio.sleep(1)

                # These should be streamed to client
                for _ in range(5):
                    await am.print.call("has print streaming too")
                    await am.log.call("has log streaming as level matched")

                match api_ver:
                    case ApiVersion.V1:
                        await asyncio.sleep(1)
                    case ApiVersion.V0:
                        await pm.stop()

                # Flush all outputs
                stdout_file.flush()
                stderr_file.flush()
                os.fsync(stdout_file.fileno())
                os.fsync(stderr_file.fileno())

            finally:
                # Restore Python's sys.stdout/stderr
                sys.stdout = original_sys_stdout
                sys.stderr = original_sys_stderr

        # Restore original file descriptors
        os.dup2(original_stdout_fd, 1)
        os.dup2(original_stderr_fd, 2)

        # Read the captured output
        with open(stdout_path, "r") as f:
            stdout_content = f.read()

        with open(stderr_path, "r") as f:
            stderr_content = f.read()

        # Clean up temp files
        os.unlink(stdout_path)
        os.unlink(stderr_path)

        # Assertions on the captured output
        # Has a leading context so we can distinguish between streamed log and
        # the log directly printed by the child processes as they share the same stdout/stderr
        assert not re.search(
            r"similar log lines.*no print streaming", stdout_content
        ), stdout_content
        assert not re.search(
            r"similar log lines.*no print streaming", stderr_content
        ), stderr_content
        assert not re.search(
            r"similar log lines.*no log streaming", stdout_content
        ), stdout_content
        assert not re.search(
            r"similar log lines.*no log streaming", stderr_content
        ), stderr_content
        assert not re.search(
            r"similar log lines.*no log streaming due to level mismatch", stdout_content
        ), stdout_content
        assert not re.search(
            r"similar log lines.*no log streaming due to level mismatch", stderr_content
        ), stderr_content

        assert re.search(
            r"similar log lines.*has print streaming", stdout_content
        ), stdout_content
        assert not re.search(
            r"similar log lines.*has print streaming", stderr_content
        ), stderr_content
        assert re.search(
            r"similar log lines.*has print streaming too", stdout_content
        ), stdout_content
        assert not re.search(
            r"similar log lines.*has print streaming too", stderr_content
        ), stderr_content
        assert not re.search(
            r"similar log lines.*log streaming as level matched", stdout_content
        ), stdout_content
        assert re.search(
            r"similar log lines.*log streaming as level matched",
            stderr_content,
        ), stderr_content

    finally:
        # Ensure file descriptors are restored even if something goes wrong
        try:
            os.dup2(original_stdout_fd, 1)
            os.dup2(original_stderr_fd, 2)
            os.close(original_stdout_fd)
            os.close(original_stderr_fd)
        except OSError:
            pass


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(120)
async def test_alloc_based_log_streaming(api_ver: ApiVersion) -> None:
    """Test both AllocHandle.stream_logs = False and True cases."""

    async def test_stream_logs_case(stream_logs: bool, test_name: str) -> None:
        # Save original file descriptors
        original_stdout_fd = os.dup(1)  # stdout

        try:
            # Create temporary files to capture output
            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as stdout_file:
                stdout_path = stdout_file.name
                os.dup2(stdout_file.fileno(), 1)
                original_sys_stdout = sys.stdout
                sys.stdout = stdout_file

                try:
                    # Create proc mesh with custom stream_logs setting
                    if api_ver == ApiVersion.V0:
                        host_mesh = create_local_host_mesh()
                        alloc_handle = host_mesh._alloc(hosts=1, gpus=2)

                        # Override the stream_logs setting
                        custom_alloc_handle = AllocHandle(
                            alloc_handle._hy_alloc, alloc_handle._extent, stream_logs
                        )

                        pm = ProcMesh.from_alloc(custom_alloc_handle)
                    else:

                        class ProcessAllocatorStreamLogs(ProcessAllocator):
                            def allocate_nonblocking(
                                self, spec: AllocSpec
                            ) -> PythonTask[Alloc]:
                                return super().allocate_nonblocking(spec)

                            def _stream_logs(self) -> bool:
                                return stream_logs

                        alloc = ProcessAllocatorStreamLogs(*_get_bootstrap_args())

                        host_mesh = HostMeshV1.allocate_nonblocking(
                            "host",
                            Extent(["hosts"], [1]),
                            alloc,
                            bootstrap_cmd=_bootstrap_cmd(),
                        )

                        pm = host_mesh.spawn_procs(name="proc", per_host={"gpus": 2})

                    am = pm.spawn("printer", Printer)

                    await pm.initialized

                    for _ in range(5):
                        await am.print.call(f"{test_name} print streaming")

                    if api_ver == ApiVersion.V0:
                        await pm.stop()
                    else:
                        # Wait for at least the aggregation window (3 seconds)
                        await asyncio.sleep(5)

                    # Flush all outputs
                    stdout_file.flush()
                    os.fsync(stdout_file.fileno())

                finally:
                    # Restore Python's sys.stdout
                    sys.stdout = original_sys_stdout

            # Restore original file descriptors
            os.dup2(original_stdout_fd, 1)

            # Read the captured output
            with open(stdout_path, "r") as f:
                stdout_content = f.read()

            # Clean up temp files
            os.unlink(stdout_path)

            if not stream_logs:
                # When stream_logs=False, logs should not be streamed to client
                assert not re.search(
                    rf"similar log lines.*{test_name} print streaming", stdout_content
                ), f"stream_logs=False case: {stdout_content}"
                assert re.search(
                    rf"{test_name} print streaming", stdout_content
                ), f"stream_logs=False case: {stdout_content}"
            else:
                # When stream_logs=True, logs should be streamed to client (no aggregation by default)
                assert re.search(
                    rf"similar log lines.*{test_name} print streaming", stdout_content
                ), f"stream_logs=True case: {stdout_content}"
                assert not re.search(
                    rf"\[[0-9]\]{test_name} print streaming", stdout_content
                ), f"stream_logs=True case: {stdout_content}"

        finally:
            # Ensure file descriptors are restored even if something goes wrong
            try:
                os.dup2(original_stdout_fd, 1)
                os.close(original_stdout_fd)
            except OSError:
                pass

    # Test both cases
    await test_stream_logs_case(False, "stream_logs_false")
    await test_stream_logs_case(True, "stream_logs_true")


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_logging_option_defaults(api_ver: ApiVersion) -> None:
    # Save original file descriptors
    original_stdout_fd = os.dup(1)  # stdout
    original_stderr_fd = os.dup(2)  # stderr

    try:
        # Create temporary files to capture output
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False
        ) as stdout_file, tempfile.NamedTemporaryFile(
            mode="w+", delete=False
        ) as stderr_file:
            stdout_path = stdout_file.name
            stderr_path = stderr_file.name

            # Redirect file descriptors to our temp files
            # This will capture both Python and Rust output
            os.dup2(stdout_file.fileno(), 1)
            os.dup2(stderr_file.fileno(), 2)

            # Also redirect Python's sys.stdout/stderr for completeness
            original_sys_stdout = sys.stdout
            original_sys_stderr = sys.stderr
            sys.stdout = stdout_file
            sys.stderr = stderr_file

            try:
                pm = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
                am = pm.spawn("printer", Printer)

                for _ in range(5):
                    await am.print.call("print streaming")
                    await am.log.call("log streaming")

                match api_ver:
                    case ApiVersion.V1:
                        # Wait for > default aggregation window (3 seconds)
                        await asyncio.sleep(5)
                    case ApiVersion.V0:
                        await pm.stop()

                # Flush all outputs
                stdout_file.flush()
                stderr_file.flush()
                os.fsync(stdout_file.fileno())
                os.fsync(stderr_file.fileno())

            finally:
                # Restore Python's sys.stdout/stderr
                sys.stdout = original_sys_stdout
                sys.stderr = original_sys_stderr

        # Restore original file descriptors
        os.dup2(original_stdout_fd, 1)
        os.dup2(original_stderr_fd, 2)

        # Read the captured output
        with open(stdout_path, "r") as f:
            stdout_content = f.read()

        with open(stderr_path, "r") as f:
            stderr_content = f.read()

        # Clean up temp files
        os.unlink(stdout_path)
        os.unlink(stderr_path)

        # Assertions on the captured output
        assert not re.search(
            r"similar log lines.*print streaming", stdout_content
        ), stdout_content
        assert re.search(r"print streaming", stdout_content), stdout_content
        assert not re.search(
            r"similar log lines.*print streaming", stderr_content
        ), stderr_content
        assert not re.search(
            r"similar log lines.*log streaming", stdout_content
        ), stdout_content
        assert not re.search(
            r"similar log lines.*log streaming", stderr_content
        ), stderr_content

    finally:
        # Ensure file descriptors are restored even if something goes wrong
        try:
            os.dup2(original_stdout_fd, 1)
            os.dup2(original_stderr_fd, 2)
            os.close(original_stdout_fd)
            os.close(original_stderr_fd)
        except OSError:
            pass


class MockEvents:
    def __init__(self):
        self.callbacks = {}
        self.registers = 0

    def register(self, event_name, callback):
        if event_name not in self.callbacks:
            self.callbacks[event_name] = []
        self.callbacks[event_name].append(callback)
        self.registers += 1

    def trigger(self, event_name, *args, **kwargs):
        if event_name in self.callbacks:
            for callback in self.callbacks[event_name]:
                callback(*args, **kwargs)


class MockIPython:
    def __init__(self):
        self.events = MockEvents()


# oss_skip: pytest keeps complaining about mocking get_ipython module
@pytest.mark.oss_skip
@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
async def test_flush_called_only_once(api_ver: ApiVersion) -> None:
    """Test that flush is called only once when ending an ipython cell"""
    mock_ipython = MockIPython()
    with unittest.mock.patch(
        "monarch._src.actor.logging.get_ipython",
        lambda: mock_ipython,
    ), unittest.mock.patch(
        "monarch._src.actor.logging.IN_IPYTHON", True
    ), unittest.mock.patch(
        "monarch._src.actor.logging.flush_all_proc_mesh_logs"
    ) as mock_flush:
        # Create 2 proc meshes with a large aggregation window
        pm1 = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
        _ = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
        # flush not yet called unless post_run_cell
        assert mock_flush.call_count == 0
        assert mock_ipython.events.registers == 0
        await pm1.logging_option(stream_to_client=True, aggregate_window_sec=600)
        assert mock_ipython.events.registers == 1

        # now, flush should be called only once
        mock_ipython.events.trigger("post_run_cell", unittest.mock.MagicMock())
        assert mock_flush.call_count == 1


# oss_skip: pytest keeps complaining about mocking get_ipython module
@pytest.mark.oss_skip
@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(180)
async def test_flush_logs_ipython(api_ver: ApiVersion) -> None:
    """Test that logs are flushed when get_ipython is available and post_run_cell event is triggered."""
    # Save original file descriptors
    original_stdout_fd = os.dup(1)  # stdout

    try:
        # Create temporary files to capture output
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as stdout_file:
            stdout_path = stdout_file.name

            # Redirect file descriptors to our temp files
            os.dup2(stdout_file.fileno(), 1)

            # Also redirect Python's sys.stdout
            original_sys_stdout = sys.stdout
            sys.stdout = stdout_file

            try:
                mock_ipython = MockIPython()

                with unittest.mock.patch(
                    "monarch._src.actor.logging.get_ipython",
                    lambda: mock_ipython,
                ), unittest.mock.patch("monarch._src.actor.logging.IN_IPYTHON", True):
                    # Make sure we can register and unregister callbacks
                    for _ in range(3):
                        pm1 = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
                        pm2 = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
                        am1 = pm1.spawn("printer", Printer)
                        am2 = pm2.spawn("printer", Printer)

                        # Set aggregation window to ensure logs are buffered
                        await pm1.logging_option(
                            stream_to_client=True, aggregate_window_sec=600
                        )
                        await pm2.logging_option(
                            stream_to_client=True, aggregate_window_sec=600
                        )

                        # Generate some logs that will be aggregated
                        for _ in range(5):
                            await am1.print.call("ipython1 test log")
                            await am2.print.call("ipython2 test log")

                        # Trigger the post_run_cell event which should flush logs
                        mock_ipython.events.trigger(
                            "post_run_cell", unittest.mock.MagicMock()
                        )

                    # Flush all outputs
                    stdout_file.flush()
                    os.fsync(stdout_file.fileno())

                # We expect to register post_run_cell hook only once per notebook/ipython session
                assert mock_ipython.events.registers == 1
                assert len(mock_ipython.events.callbacks["post_run_cell"]) == 1
            finally:
                # Restore Python's sys.stdout
                sys.stdout = original_sys_stdout

        # Restore original file descriptors
        os.dup2(original_stdout_fd, 1)

        # Read the captured output
        with open(stdout_path, "r") as f:
            stdout_content = f.read()

        # TODO: there are quite a lot of code dups and boilerplate; make them contextmanager utils

        # Clean up temp files
        os.unlink(stdout_path)

        # Verify that logs were flushed when the post_run_cell event was triggered
        # We should see the aggregated logs in the output
        assert (
            len(
                re.findall(
                    r"\[10 similar log lines\].*ipython1 test log", stdout_content
                )
            )
            == 3
        ), stdout_content

        assert (
            len(
                re.findall(
                    r"\[10 similar log lines\].*ipython2 test log", stdout_content
                )
            )
            == 3
        ), stdout_content

    finally:
        # Ensure file descriptors are restored even if something goes wrong
        try:
            os.dup2(original_stdout_fd, 1)
            os.close(original_stdout_fd)
        except OSError:
            pass


# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
@pytest.mark.oss_skip
async def test_flush_logs_fast_exit() -> None:
    # We use a subprocess to run the test so we can handle the flushed logs at the end.
    # Otherwise, it is hard to restore the original stdout/stderr.

    test_bin = importlib.resources.files(str(__package__)).joinpath("test_bin")

    # Run the binary in a separate process and capture stdout and stderr
    cmd = [str(test_bin), "flush-logs"]

    process = subprocess.run(cmd, capture_output=True, timeout=60, text=True)

    # Check if the process ended without error
    if process.returncode != 0:
        raise RuntimeError(f"{cmd} ended with error code {process.returncode}. ")

    # Assertions on the captured output, 160 = 32 procs * 5 logs per proc
    # 32 and 5 are specified in the test_bin flush-logs.
    assert (
        len(
            re.findall(
                r"160 similar log lines.*has print streaming",
                process.stdout,
            )
        )
        == 1
    ), process.stdout


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_flush_on_disable_aggregation(api_ver: ApiVersion) -> None:
    """Test that logs are flushed when disabling aggregation.

    This tests the corner case: "Make sure we flush whatever in the aggregators before disabling aggregation."
    """
    # Save original file descriptors
    original_stdout_fd = os.dup(1)  # stdout

    try:
        # Create temporary files to capture output
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as stdout_file:
            stdout_path = stdout_file.name

            # Redirect file descriptors to our temp files
            os.dup2(stdout_file.fileno(), 1)

            # Also redirect Python's sys.stdout
            original_sys_stdout = sys.stdout
            sys.stdout = stdout_file

            try:
                pm = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
                am = pm.spawn("printer", Printer)

                # Set a long aggregation window to ensure logs aren't flushed immediately
                await pm.logging_option(stream_to_client=True, aggregate_window_sec=60)

                # Generate some logs that will be aggregated but not flushed immediately
                for _ in range(5):
                    await am.print.call("aggregated log line")
                await asyncio.sleep(1)

                # Now disable aggregation - this should trigger an immediate flush
                await pm.logging_option(
                    stream_to_client=True, aggregate_window_sec=None
                )

                # Wait a bit to ensure logs are collected
                await asyncio.sleep(1)
                for _ in range(5):
                    await am.print.call("single log line")

                match api_ver:
                    case ApiVersion.V1:
                        # Wait for > default aggregation window (3 secs)
                        await asyncio.sleep(5)
                    case ApiVersion.V0:
                        await pm.stop()

                # Flush all outputs
                stdout_file.flush()
                os.fsync(stdout_file.fileno())

            finally:
                # Restore Python's sys.stdout
                sys.stdout = original_sys_stdout

        # Restore original file descriptors
        os.dup2(original_stdout_fd, 1)

        # Read the captured output
        with open(stdout_path, "r") as f:
            stdout_content = f.read()

        # Clean up temp files
        os.unlink(stdout_path)

        # Verify that logs were flushed when aggregation was disabled
        # We should see the aggregated logs in the output
        # 10 = 5 log lines * 2 procs
        assert re.search(
            r"\[10 similar log lines\].*aggregated log line", stdout_content
        ), stdout_content

        # No aggregated single log lines
        assert not re.search(
            r"similar log lines.*single log line", stdout_content
        ), stdout_content

        # 10 = 5 log lines * 2 procs
        assert (
            len(re.findall(r"\[.* [0-9]+\] single log line", stdout_content)) == 10
        ), stdout_content

    finally:
        # Ensure file descriptors are restored even if something goes wrong
        try:
            os.dup2(original_stdout_fd, 1)
            os.close(original_stdout_fd)
        except OSError:
            pass


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(120)
async def test_multiple_ongoing_flushes_no_deadlock(api_ver: ApiVersion) -> None:
    """
    The goal is to make sure when a user sends multiple sync flushes, we are not deadlocked.
    Because now a flush call is purely sync, it is very easy to get into a deadlock.
    So we assert the last flush call will not get into such a state.
    """
    pm = spawn_procs_on_this_host(api_ver, per_host={"gpus": 4})
    am = pm.spawn("printer", Printer)

    # Generate some logs that will be aggregated but not flushed immediately
    for _ in range(10):
        await am.print.call("aggregated log line")

    log_mesh = pm._logging_manager._logging_mesh_client
    assert log_mesh is not None
    futures = []
    for _ in range(5):
        # FIXME: the order of futures doesn't necessarily mean the order of flushes due to the async nature.
        await asyncio.sleep(0.1)
        futures.append(
            Future(
                coro=log_mesh.flush(context().actor_instance._as_rust()).spawn().task()
            )
        )

    # The last flush should not block
    futures[-1].get()


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_adjust_aggregation_window(api_ver: ApiVersion) -> None:
    """Test that the flush deadline is updated when the aggregation window is adjusted.

    This tests the corner case: "This can happen if the user has adjusted the aggregation window."
    """
    # Save original file descriptors
    original_stdout_fd = os.dup(1)  # stdout

    try:
        # Create temporary files to capture output
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as stdout_file:
            stdout_path = stdout_file.name

            # Redirect file descriptors to our temp files
            os.dup2(stdout_file.fileno(), 1)

            # Also redirect Python's sys.stdout
            original_sys_stdout = sys.stdout
            sys.stdout = stdout_file

            try:
                pm = spawn_procs_on_this_host(api_ver, per_host={"gpus": 2})
                am = pm.spawn("printer", Printer)

                # Set a long aggregation window initially
                await pm.logging_option(stream_to_client=True, aggregate_window_sec=100)

                # Generate some logs that will be aggregated
                for _ in range(3):
                    await am.print.call("first batch of logs")
                await asyncio.sleep(1)

                # Now adjust to a shorter window - this should update the flush deadline
                await pm.logging_option(stream_to_client=True, aggregate_window_sec=2)

                # Generate more logs
                for _ in range(3):
                    await am.print.call("second batch of logs")

                match api_ver:
                    case ApiVersion.V1:
                        # Wait for > aggregation window (2 secs)
                        await asyncio.sleep(4)
                    case ApiVersion.V0:
                        await pm.stop()

                # Flush all outputs
                stdout_file.flush()
                os.fsync(stdout_file.fileno())

            finally:
                # Restore Python's sys.stdout/stderr
                sys.stdout = original_sys_stdout

        # Restore original file descriptors
        os.dup2(original_stdout_fd, 1)

        # Read the captured output
        with open(stdout_path, "r") as f:
            stdout_content = f.read()

        # Clean up temp files
        os.unlink(stdout_path)

        # Verify that logs were flushed when the aggregation window was adjusted
        # We should see both batches of logs in the output
        assert re.search(
            r"\[6 similar log lines\].*first batch of logs", stdout_content
        ), stdout_content

        assert re.search(
            r"similar log lines.*second batch of logs", stdout_content
        ), stdout_content

    finally:
        # Ensure file descriptors are restored even if something goes wrong
        try:
            os.dup2(original_stdout_fd, 1)
            os.close(original_stdout_fd)
        except OSError:
            pass


class SendAlot(Actor):
    @endpoint
    async def send(self, port: Port[int]):
        for i in range(100):
            port.send(i)


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_port_as_argument(api_ver: ApiVersion) -> None:
    proc_mesh = spawn_procs_on_fake_host(api_ver, {"gpus": 1})
    s = proc_mesh.spawn("send_alot", SendAlot)
    send, recv = Channel[int].open()

    s.send.broadcast(send)

    for i in range(100):
        assert i == recv.recv().get()


@pytest.mark.timeout(30)
async def test_same_actor_twice() -> None:
    pm = spawn_procs_on_this_host(ApiVersion.V0, {"gpus": 1})
    await pm.spawn("dup", Counter, 0).initialized

    # The second spawn with the same name should fail with a specific error
    with pytest.raises(Exception) as exc_info:
        await pm.spawn("dup", Counter, 0).initialized

    # Assert that the error message contains the expected text about duplicate actor name
    error_msg = str(exc_info.value)
    assert (
        "gspawn failed: an actor with name 'dup' has already been spawned" in error_msg
    ), f"Expected error message about duplicate actor name, got: {error_msg}"


class LsActor(Actor):
    def __init__(self, workspace: str):
        self.workspace = workspace

    @endpoint
    async def ls(self) -> list[str]:
        return os.listdir(self.workspace)


async def test_sync_workspace() -> None:
    # create two workspaces: one for local and one for remote
    with tempfile.TemporaryDirectory() as workspace_src, tempfile.TemporaryDirectory() as workspace_dst:

        def bootstrap_WORKSPACE_DIR() -> None:
            import os

            os.environ["WORKSPACE_DIR"] = workspace_dst

        pm = this_host().spawn_procs(
            per_host={"gpus": 1}, bootstrap=bootstrap_WORKSPACE_DIR
        )

        config = defaults.config("slurm", workspace_src)
        await pm.sync_workspace(workspace=config.workspace, auto_reload=True)

        # no file in remote workspace initially
        am = pm.spawn("ls", LsActor, workspace_dst)
        for item in list(am.ls.call().get()):
            assert len(item[1]) == 0

        # write a file to local workspace
        file_path = os.path.join(workspace_src, "new_file")
        with open(file_path, "w") as f:
            f.write("hello world")
            f.flush()

        # force a sync and it should populate on the dst workspace
        await pm.sync_workspace(config.workspace, auto_reload=True)
        for item in list(am.ls.call().get()):
            assert len(item[1]) == 1
            assert item[1][0] == "new_file"
            file_path = os.path.join(workspace_dst, item[1][0])
            with open(file_path, "r") as f:
                assert f.readline() == "hello world"

    # sanity check
    assert "WORKSPACE_DIR" not in os.environ, "test leaves env var side-effects!"


class TestActorMeshStop(unittest.IsolatedAsyncioTestCase):
    async def test_actor_mesh_stop(self) -> None:
        pm = this_host().spawn_procs(per_host={"gpus": 2})
        am_1 = pm.spawn("printer", Printer)
        am_2 = pm.spawn("printer2", Printer)
        await am_1.print.call("hello 1")
        await am_1.log.call("hello 2")
        await cast(ActorMesh, am_1).stop()

        with self.assertRaisesRegex(
            RuntimeError,
            r"(?:`PythonActorMesh` has already been stopped|delivery error: broken link)",
        ):
            await am_1.print.call("hello 1")

        await am_2.print.call("hello 3")
        await am_2.log.call("hello 4")

        await pm.stop()

    async def test_proc_mesh_stop_after_actor_mesh_stop(self) -> None:
        pm = this_host().spawn_procs(per_host={"gpus": 2})
        am = pm.spawn("printer", Printer)

        await cast(ActorMesh, am).stop()
        await pm.stop()


class PortedActor(Actor):
    @endpoint(explicit_response_port=True)
    def add(self, port: "Port[int]", b: int) -> None:
        port.send(3 + b)


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
def test_ported_actor(api_ver: ApiVersion):
    proc_mesh = spawn_procs_on_fake_host(api_ver, {"gpus": 1})
    a = proc_mesh.spawn("port_actor", PortedActor)
    assert 5 == a.add.call_one(2).get()


async def _recv():
    return (7, 2, 3)


async def consume():
    r = await PythonTask.from_coroutine(_recv())
    assert r == (7, 2, 3)


@pytest.mark.timeout(60)
def test_python_task_tuple() -> None:
    PythonTask.from_coroutine(consume()).block_on()


def test_select_result() -> None:
    def s(t):
        time.sleep(t)
        return t

    a = PythonTask.spawn_blocking(lambda: s(4))
    b = PythonTask.spawn_blocking(lambda: s(0))
    r = PythonTask.select_one([a.task(), b.task()]).block_on()
    assert r == (0, 1)
    # FIXME: Sleep for 6 seconds to ensure that task `a` completes
    # before the test exits. Otherwise we get a SIGABRT.
    time.sleep(6)


class SleepActor(Actor):
    @endpoint
    async def sleep(self, t: float) -> None:
        await asyncio.sleep(t)


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
def test_mesh_len(api_ver: ApiVersion):
    proc_mesh = spawn_procs_on_fake_host(api_ver, {"gpus": 12})
    s = proc_mesh.spawn("sleep_actor", SleepActor)
    assert 12 == len(s)
    # FIXME: Actually figure out what's going on here.
    # Call an endpoint on the actor before the test
    # exits. Otherwise we might get a fatal PyGILState_Release
    # error.
    s.sleep.call(1).get()


class UndeliverableMessageReceiver(Actor):
    def __init__(self):
        self._messages = asyncio.Queue()

    @endpoint
    async def receive_undeliverable(
        self, sender: str, dest: str, error_msg: str
    ) -> None:
        await self._messages.put((sender, dest, error_msg))

    @endpoint
    async def get_messages(self) -> Tuple[str, str, str]:
        return await self._messages.get()


class UndeliverableMessageSender(Actor):
    @endpoint
    def send_undeliverable(self) -> None:
        actor_instance = context().actor_instance
        port_id = PortId(
            actor_id=ActorId(world_name="bogus", rank=0, actor_name="bogus"),
            port=1234,
        )
        port_ref = PortRef(port_id)
        port_ref.send(
            actor_instance._as_rust(),
            PythonMessage(PythonMessageKind.Result(None), b"123"),
        )


class UndeliverableMessageSenderWithOverride(UndeliverableMessageSender):
    def __init__(self, receiver: UndeliverableMessageReceiver):
        self._receiver = receiver

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        PythonTask.spawn_blocking(
            self._receiver.receive_undeliverable.call_one(
                str(message.sender()), str(message.dest()), message.error_msg()
            ).get
        )
        return True


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_undeliverable_message_with_override(api_ver: ApiVersion) -> None:
    pm = spawn_procs_on_this_host(api_ver, {"gpus": 1})
    receiver = pm.spawn("undeliverable_receiver", UndeliverableMessageReceiver)
    sender = pm.spawn(
        "undeliverable_sender", UndeliverableMessageSenderWithOverride, receiver
    )
    sender.send_undeliverable.call()
    sender, dest, error_msg = receiver.get_messages.call_one().get()
    assert "undeliverable_sender" in sender
    assert "bogus" in dest
    assert error_msg is not None
    pm.stop().get()


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
@pytest.mark.timeout(60)
async def test_undeliverable_message_without_override(api_ver: ApiVersion) -> None:
    pm = spawn_procs_on_this_host(api_ver, {"gpus": 1})
    sender = pm.spawn("undeliverable_sender", UndeliverableMessageSender)
    sender.send_undeliverable.call().get()
    # Wait a few seconds to ensure that the undeliverable message is processed
    # without crashing anything
    await asyncio.sleep(5)
    pm.stop().get()


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
def test_this_and_that(api_ver: ApiVersion):
    proc = get_this_proc(api_ver)
    counter = proc.spawn("counter", Counter, 7)
    assert 7 == counter.value.call_one().get()


class ReceptorActor(Actor):
    @endpoint
    def status(self):
        return 1


@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
async def test_things_survive_losing_python_reference(api_ver: ApiVersion) -> None:
    """Test the slice_receptor_mesh function in LOCAL mode, verifying that setup methods are called."""

    pm = spawn_procs_on_this_host(api_ver, {"gpus": 1})
    receptor = pm.spawn(
        "receptor",
        ReceptorActor,
    )
    receptor = receptor.slice(gpus=0)

    await receptor.status.call()


class IsInit(Actor):
    @endpoint
    def is_cuda_initialized(self) -> bool:
        cuda = ctypes.CDLL("libcuda.so.1")
        CUresult = ctypes.c_int
        cuDeviceGetCount = cuda.cuDeviceGetCount
        cuDeviceGetCount.argtypes = [ctypes.POINTER(ctypes.c_int)]
        cuDeviceGetCount.restype = CUresult
        count = ctypes.c_int()
        result = cuDeviceGetCount(ctypes.byref(count))
        CUDA_ERROR_NOT_INITIALIZED = 3
        return result == CUDA_ERROR_NOT_INITIALIZED


@pytest.mark.oss_skip
def test_cuda_is_not_initialized_in_a_new_proc():
    try:
        ctypes.CDLL("libcuda.so.1")
    except OSError:
        pytest.skip("cannot find cuda")
    proc = this_host().spawn_procs().spawn("is_init", IsInit)
    assert not proc.is_cuda_initialized.call_one().get()


class SpawningActorFromEndpointActor(Actor):
    def __init__(self, root="None"):
        self._root = root

    @endpoint
    def return_root(self):
        return self._root

    @endpoint
    async def spawning_from_endpoint(self, name, root) -> None:
        await get_or_spawn_controller(name, SpawningActorFromEndpointActor, root=root)


@pytest.mark.timeout(60)
def test_get_or_spawn_controller_inside_actor_endpoint():
    actor_1 = get_or_spawn_controller("actor_1", SpawningActorFromEndpointActor).get()
    actor_1.spawning_from_endpoint.call_one("actor_2", root="actor_1").get()
    actor_2 = get_or_spawn_controller("actor_2", SpawningActorFromEndpointActor).get()
    # verify that actor_2 was spawned from actor_1 with the correct root
    assert actor_2.return_root.call_one().get() == "actor_1"
