# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import contextlib
import ctypes
import importlib.resources
import io
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
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any, cast, Dict, Iterator, NamedTuple, Tuple

import cloudpickle
import monarch.actor
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
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.actor_mesh import ActorMesh, Channel, context, Port
from monarch._src.actor.allocator import ProcessAllocator
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import (
    _bootstrap_cmd,
    create_local_host_mesh,
    fake_in_process_host,
    HostMesh,
    this_host,
    this_proc,
)
from monarch._src.actor.proc_mesh import (
    _get_bootstrap_args,
    get_or_spawn_controller,
    HyProcMesh,
)
from monarch._src.job.job import LoginJob, ProcessState
from monarch.actor import (
    Accumulator,
    Actor,
    attach_to_workers,
    current_actor_name,
    current_rank,
    current_size,
    endpoint,
    ProcMesh,
)
from monarch.config import configured, parametrize_config
from monarch.tools.config import defaults
from typing_extensions import assert_type

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


class SyncCounter(Actor):
    def __init__(self, c):
        self.c = c

    @endpoint
    def value_sync_endpoint(self) -> int:
        return self.c.value.choose().get()


class Indirect(Actor):
    @endpoint
    async def call_value(self, c: Counter) -> int:
        return await c.value.choose()


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_choose():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    v = proc.spawn("counter", Counter, 3)
    i = proc.spawn("indirect", Indirect)
    v.incr.broadcast()
    result = await v.value.choose()

    # Test that Pyre derives the correct type for result (int, not Any)
    assert_type(result, int)
    result2 = await i.call_value.choose(v)

    assert result == result2

    v2 = proc.spawn("sync_counter", SyncCounter, v)
    result3 = v2.value_sync_endpoint.choose().get()
    assert_type(result, int)
    assert result2 == result3


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_stream():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
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


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_mesh_passed_to_mesh():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    f = proc.spawn("from", From)
    t = proc.spawn("to", To)
    # Make sure t is initialized before sending to f. Otherwise
    # f might call t.whoami before t.__init__.
    await t.whoami.call()
    all = [y for x in f.fetch.stream(t) for y in await x]
    assert len(all) == 4
    assert all[0] != all[1]


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_mesh_passed_to_mesh_on_different_proc_mesh():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    proc2 = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    f = proc.spawn("from", From)
    t = proc2.spawn("to", To)
    # Make sure t is initialized before sending to f. Otherwise
    # f might call t.whoami before t.__init__.
    await t.whoami.call()
    all = [y for x in f.fetch.stream(t) for y in await x]
    assert len(all) == 4
    assert all[0] != all[1]


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_actor_slicing():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    proc2 = fake_in_process_host().spawn_procs(per_host={"gpus": 2})

    f = proc.spawn("from", From)
    t = proc2.spawn("to", To)

    assert t.slice(gpus=0).whoami.call().get() != t.slice(gpus=1).whoami.call().get()

    result = [y for x in f.fetch.stream(t.slice(gpus=0)) for y in x.get()]
    assert len(result) == 2

    assert result[0] == result[1]


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_aggregate():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
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


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_rank_size():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    r = proc.spawn("runit", RunIt)

    acc = Accumulator(r.run, 0, operator.add)

    assert 1 == await acc.accumulate(lambda: current_rank()["gpus"])
    assert 4 == await acc.accumulate(lambda: current_size()["gpus"])


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_rank_string():
    per_host = {"hosts": 1, "gpus": 2}
    proc = fake_in_process_host().spawn_procs(per_host=per_host)
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


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_sync_actor():
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    a = proc.spawn("actor", SyncActor)
    c = proc.spawn("counter", Counter, 5)
    r = await a.sync_endpoint.choose(c)
    assert r == 5


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_sync_actor_sync_client() -> None:
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    a = proc.spawn("actor", SyncActor)
    c = proc.spawn("counter", Counter, 5)
    r = a.sync_endpoint.choose(c).get()
    assert r == 5


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_proc_mesh_size() -> None:
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    assert 2 == proc.size("gpus")


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_rank_size_sync() -> None:
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    r = proc.spawn("runit", RunIt)

    acc = Accumulator(r.run, 0, operator.add)
    assert 1 == acc.accumulate(lambda: current_rank()["gpus"]).get()
    assert 4 == acc.accumulate(lambda: current_size()["gpus"]).get()


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_accumulate_sync() -> None:
    proc = fake_in_process_host().spawn_procs(per_host={"gpus": 2})
    counter = proc.spawn("counter", Counter, 1)
    counter.incr.broadcast()
    acc = Accumulator(counter.value, 0, operator.add)
    r = acc.accumulate().get()
    assert r == 4


class CastToCounter(Actor):
    @endpoint
    def doit(self, c: Counter):
        return list(c.value.call().get())


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_value_mesh() -> None:
    per_host = {"hosts": 1, "gpus": 2}
    proc = fake_in_process_host().spawn_procs(per_host=per_host)
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


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_proc_mesh_liveness() -> None:
    mesh = this_host().spawn_procs(per_host={"gpus": 2})
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
    async def increment(self):
        self.local.value += 1

    @endpoint
    async def increment_async(self):
        self.local.value += 1

    @endpoint
    async def get_value(self):
        return self.local.value

    @endpoint
    async def get_async(self):
        return self.local.value


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_actor_tls() -> None:
    """Test that thread-local state is respected."""
    pm = this_host().spawn_procs(per_host={"gpus": 1})
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


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_actor_tls_full_sync() -> None:
    """Test that thread-local state is respected."""
    pm = this_host().spawn_procs(per_host={"gpus": 1})
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


async def awaitit(f):
    return await f


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


class RedirectedPaths(NamedTuple):
    """Filesystem paths for captured stdio from redirected_stdio().

    `stdout` is the path to the temporary file holding captured
    stdout.

    `stderr` is the path to the temporary file holding captured
    stderr, or None if stderr capture was disabled.

    """

    stdout: str
    stderr: str | None


@contextlib.contextmanager
def redirected_stdio(capture_stderr: bool = True) -> Iterator[RedirectedPaths]:
    """Temporarily redirect process stdout (and optionally stderr) to
    temp files.

    This is a context manager / generator that:
      * dup()s the original OS-level stdout/stderr FDs,
      * points fd 1 (and optionally fd 2) at temporary files,
      * swaps sys.stdout/sys.stderr to those files,
      * yields the on-disk paths of the capture files, and
      * on exit, flushes/fsyncs, restores the original FDs and
        sys.std*, and closes the temp files.

    It is primarily intended for tests that need to assert on what a
    subprocess or library printed to stdout/stderr at the OS FD level.

    """

    # Save original OS-level FDs
    original_stdout_fd = os.dup(1)
    original_stderr_fd = os.dup(2) if capture_stderr else None

    # Create temp files
    stdout_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
    stdout_path = stdout_file.name

    stderr_file = None
    stderr_path = None
    if capture_stderr:
        stderr_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        stderr_path = stderr_file.name

    # Save Python-level streams
    original_sys_stdout = sys.stdout
    original_sys_stderr = sys.stderr

    try:
        # Redirect OS-level stdout
        os.dup2(stdout_file.fileno(), 1)
        sys.stdout = stdout_file

        if capture_stderr and stderr_file is not None:
            os.dup2(stderr_file.fileno(), 2)
            sys.stderr = stderr_file

        # Let the caller run with redirected IO
        yield RedirectedPaths(stdout_path, stderr_path)

    finally:
        # Flush and fsync temp files so contents are visible on disk
        try:
            stdout_file.flush()
            os.fsync(stdout_file.fileno())
        except OSError:
            pass

        if capture_stderr and stderr_file is not None:
            try:
                stderr_file.flush()
                os.fsync(stderr_file.fileno())
            except OSError:
                pass

        # Restore Python-level streams
        sys.stdout = original_sys_stdout
        if capture_stderr:
            sys.stderr = original_sys_stderr

        # Restore OS-level FDs
        try:
            os.dup2(original_stdout_fd, 1)
            os.close(original_stdout_fd)
        except OSError:
            pass

        if capture_stderr and original_stderr_fd is not None:
            try:
                os.dup2(original_stderr_fd, 2)
                os.close(original_stderr_fd)
            except OSError:
                pass

        # Close the temp files
        stdout_file.close()
        if capture_stderr and stderr_file is not None:
            stderr_file.close()

        # Clean up temp files
        try:
            os.unlink(stdout_path)
            if stderr_path:
                os.unlink(stderr_path)
        except OSError:
            pass


@contextlib.contextmanager
def configured_with_redirected_stdio(
    capture_stderr: bool = True,
    **config_overrides: Any,
) -> Iterator[tuple[Dict[str, Any], RedirectedPaths]]:
    """Apply config overrides and capture stdio for the duration of
    the block."""
    with (
        configured(**config_overrides) as config,
        redirected_stdio(capture_stderr) as paths,
    ):
        yield (config, paths)


# oss_skip: pytest keeps complaining about mocking get_ipython module
@pytest.mark.oss_skip
async def test_actor_log_streaming() -> None:
    with configured_with_redirected_stdio(
        capture_stderr=True,
        enable_log_forwarding=True,
        enable_file_capture=True,
        tail_log_lines=100,
    ) as (_, paths):
        pm = this_host().spawn_procs(per_host={"gpus": 2})
        am = pm.spawn("printer", Printer)

        # Disable streaming logs to client
        await pm.logging_option(stream_to_client=False, aggregate_window_sec=None)
        await asyncio.sleep(1)

        # These should not be streamed to client initially
        for _ in range(5):
            await am.print.call("no print streaming")
            await am.log.call("no log streaming")
            await asyncio.sleep(1)

        # Enable streaming logs to client
        await pm.logging_option(
            stream_to_client=True,
            aggregate_window_sec=1,
            level=logging.FATAL,
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
            stream_to_client=True,
            aggregate_window_sec=1,
            level=logging.ERROR,
        )
        # Give it some time to reflect
        await asyncio.sleep(1)

        # These should be streamed to client
        for _ in range(5):
            await am.print.call("has print streaming too")
            await am.log.call("has log streaming as level matched")

        await asyncio.sleep(1)

        # Flush to ensure all output is written before reading
        sys.stdout.flush()
        sys.stderr.flush()

        # Read the captured output
        with open(paths.stdout, "r") as f:
            stdout_content = f.read()

        assert paths.stderr is not None
        with open(paths.stderr, "r") as f:
            stderr_content = f.read()

        # Assertions on the captured output
        # Has a leading context so we can distinguish between streamed log and
        # the log directly printed by the child processes as they share the same stdout/stderr
        assert not re.search(
            r"similar log lines.*no print streaming", stdout_content
        ), stdout_content
        assert not re.search(
            r"similar log lines.*no print streaming", stderr_content
        ), stderr_content
        assert not re.search(r"similar log lines.*no log streaming", stdout_content), (
            stdout_content
        )
        assert not re.search(r"similar log lines.*no log streaming", stderr_content), (
            stderr_content
        )
        assert not re.search(
            r"similar log lines.*no log streaming due to level mismatch",
            stdout_content,
        ), stdout_content
        assert not re.search(
            r"similar log lines.*no log streaming due to level mismatch",
            stderr_content,
        ), stderr_content

        assert re.search(r"similar log lines.*has print streaming", stdout_content), (
            stdout_content
        )
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


# oss_skip: pytest keeps complaining about mocking get_ipython module
# oss_skip: (SF) broken in GitHub by D86994420. Passes internally.
@pytest.mark.oss_skip
async def test_alloc_based_log_streaming() -> None:
    """Test both AllocHandle.stream_logs = False and True cases."""

    async def test_stream_logs_case(stream_logs: bool, test_name: str) -> None:
        with configured_with_redirected_stdio(
            capture_stderr=False,
            enable_log_forwarding=True,
            enable_file_capture=True,
            tail_log_lines=100,
        ) as (_, paths):
            # Create proc mesh with custom stream_logs setting
            class ProcessAllocatorStreamLogs(ProcessAllocator):
                def allocate_nonblocking(self, spec: AllocSpec) -> PythonTask[Alloc]:
                    return super().allocate_nonblocking(spec)

                def _stream_logs(self) -> bool:
                    return stream_logs

            alloc = ProcessAllocatorStreamLogs(*_get_bootstrap_args())

            host_mesh = HostMesh.allocate_nonblocking(
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

            # Wait for at least the aggregation window (3 seconds)
            await asyncio.sleep(5)

            # Flush to ensure all output is written before reading
            sys.stdout.flush()

            # Read the captured output
            with open(paths.stdout, "r") as f:
                stdout_content = f.read()

            if not stream_logs:
                # When stream_logs=False, logs should not be streamed to client
                assert not re.search(
                    rf"similar log lines.*{test_name} print streaming",
                    stdout_content,
                ), f"stream_logs=False case: {stdout_content}"
                assert re.search(rf"{test_name} print streaming", stdout_content), (
                    f"stream_logs=False case: {stdout_content}"
                )
            else:
                # When stream_logs=True, logs should be streamed to client (no aggregation by default)
                assert re.search(
                    rf"similar log lines.*{test_name} print streaming",
                    stdout_content,
                ), f"stream_logs=True case: {stdout_content}"
                assert not re.search(
                    rf"\[[0-9]\]{test_name} print streaming", stdout_content
                ), f"stream_logs=True case: {stdout_content}"

    # Test both cases
    await test_stream_logs_case(False, "stream_logs_false")
    await test_stream_logs_case(True, "stream_logs_true")


# oss_skip: (SF) broken in GitHub by D86994420. Passes internally.
@pytest.mark.oss_skip
async def test_logging_option_defaults() -> None:
    with configured_with_redirected_stdio(
        capture_stderr=True,
        enable_log_forwarding=True,
        enable_file_capture=True,
        tail_log_lines=100,
    ) as (_, paths):
        pm = this_host().spawn_procs(per_host={"gpus": 2})
        am = pm.spawn("printer", Printer)

        for _ in range(5):
            await am.print.call("print streaming")
            await am.log.call("log streaming")

        # Wait for > default aggregation window (3 seconds)
        await asyncio.sleep(5)

        # Flush to ensure all output is written before reading
        sys.stdout.flush()
        sys.stderr.flush()

        # Read the captured output
        with open(paths.stdout, "r") as f:
            stdout_content = f.read()

        assert paths.stderr is not None
        with open(paths.stderr, "r") as f:
            stderr_content = f.read()

        # Assertions on the captured output
        assert not re.search(r"similar log lines.*print streaming", stdout_content), (
            stdout_content
        )
        assert re.search(r"print streaming", stdout_content), stdout_content
        assert not re.search(r"similar log lines.*print streaming", stderr_content), (
            stderr_content
        )
        assert not re.search(r"similar log lines.*log streaming", stdout_content), (
            stdout_content
        )
        assert not re.search(r"similar log lines.*log streaming", stderr_content), (
            stderr_content
        )


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
async def test_flush_called_only_once() -> None:
    """Test that flush is called only once when ending an ipython cell"""
    with configured(
        enable_log_forwarding=True, enable_file_capture=True, tail_log_lines=100
    ):
        mock_ipython = MockIPython()
        with (
            unittest.mock.patch(
                "IPython.get_ipython",
                lambda: mock_ipython,
            ),
            unittest.mock.patch("monarch._src.actor.logging.IN_IPYTHON", True),
            unittest.mock.patch(
                "monarch._src.actor.logging.flush_all_proc_mesh_logs"
            ) as mock_flush,
            unittest.mock.patch(
                "monarch._src.actor.logging.LoggingManager.enable_fd_capture_if_in_ipython",
                return_value=None,
            ),
        ):
            # Create 2 proc meshes with a large aggregation window
            pm1 = this_host().spawn_procs(per_host={"gpus": 2})
            _ = this_host().spawn_procs(per_host={"gpus": 2})
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
@pytest.mark.timeout(180)
async def test_flush_logs_ipython() -> None:
    """Test that logs are flushed when get_ipython is available and post_run_cell event is triggered."""
    with configured_with_redirected_stdio(
        capture_stderr=False,
        enable_log_forwarding=True,
        enable_file_capture=True,
        tail_log_lines=100,
    ) as (_, paths):
        mock_ipython = MockIPython()

        with (
            unittest.mock.patch(
                "IPython.get_ipython",
                lambda: mock_ipython,
            ),
            unittest.mock.patch("monarch._src.actor.logging.IN_IPYTHON", True),
        ):
            # Make sure we can register and unregister callbacks
            for _ in range(3):
                pm1 = this_host().spawn_procs(per_host={"gpus": 2})
                pm2 = this_host().spawn_procs(per_host={"gpus": 2})
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
                mock_ipython.events.trigger("post_run_cell", unittest.mock.MagicMock())

        # We expect to register post_run_cell hook only once per notebook/ipython session
        assert mock_ipython.events.registers == 1
        assert len(mock_ipython.events.callbacks["post_run_cell"]) == 1

        # Flush to ensure all output is written before reading
        sys.stdout.flush()

        # Read the captured output
        with open(paths.stdout, "r") as f:
            stdout_content = f.read()

        # We triggered post_run_cell three times; in the current
        # implementation that yields three aggregated groups per
        # message type (though the counts may be 10, 10, 8 rather than
        # all 10).
        pattern1 = r"\[\d+ similar log lines\].*ipython1 test log"
        pattern2 = r"\[\d+ similar log lines\].*ipython2 test log"

        assert len(re.findall(pattern1, stdout_content)) >= 3, stdout_content
        assert len(re.findall(pattern2, stdout_content)) >= 3, stdout_content


# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
@pytest.mark.oss_skip
async def test_flush_logs_fast_exit() -> None:
    """Test that logs are flushed before the program exits.

    This tests that logs are aggregated correctly and flushed with
    fast process termination.

    """

    # We run a subprocess so we can handle the flushed logs at the
    # end. Otherwise, it is hard to restore the original
    # stdout/stderr.
    test_bin = importlib.resources.files(str(__package__)).joinpath("test_bin")
    cmd = [str(test_bin), "flush-logs"]
    # CalledProcessError includes returncode, cmd, stdout, stderr
    # automatically.
    process = subprocess.run(
        cmd, capture_output=True, timeout=60, text=True, check=True
    )

    # See python_actor_test_binary::_flush_logs().
    assert (
        len(
            re.findall(
                r"20 similar log lines.*has print streaming",
                process.stdout,
            )
        )
        == 1
    ), process.stdout


# oss_skip: (SF) broken in GitHub by D86994420. Passes internally.
@pytest.mark.oss_skip
async def test_flush_on_disable_aggregation() -> None:
    """Test that logs are flushed when disabling aggregation.

    This tests the corner case: "Make sure we flush whatever in the aggregators before disabling aggregation."
    """
    with configured_with_redirected_stdio(
        capture_stderr=False,
        enable_log_forwarding=True,
        enable_file_capture=True,
        tail_log_lines=100,
    ) as (_, paths):
        pm = this_host().spawn_procs(per_host={"gpus": 2})
        am = pm.spawn("printer", Printer)

        # Set a long aggregation window to ensure logs aren't flushed immediately
        await pm.logging_option(stream_to_client=True, aggregate_window_sec=60)

        # Generate some logs that will be aggregated but not flushed immediately
        for _ in range(5):
            await am.print.call("aggregated log line")
        await asyncio.sleep(1)

        # Now disable aggregation - this should trigger an immediate flush
        await pm.logging_option(stream_to_client=True, aggregate_window_sec=None)

        # Wait a bit to ensure logs are collected
        await asyncio.sleep(1)
        for _ in range(5):
            await am.print.call("single log line")

        # Wait for > default aggregation window (3 secs)
        await asyncio.sleep(5)

        # Flush to ensure all output is written before reading
        sys.stdout.flush()

        # Read the captured output
        with open(paths.stdout, "r") as f:
            stdout_content = f.read()

        # Verify that logs were flushed when aggregation was disabled
        # We should see the aggregated logs in the output
        # 10 = 5 log lines * 2 procs
        assert re.search(
            r"\[10 similar log lines\].*aggregated log line", stdout_content
        ), stdout_content

        # No aggregated single log lines
        assert not re.search(r"similar log lines.*single log line", stdout_content), (
            stdout_content
        )

        # 10 = 5 log lines * 2 procs
        total_single = len(
            re.findall(r"\[[^\]]+\] \[[0-9]+\] single log line", stdout_content)
        )
        assert total_single == 10, (
            f"Expected 10 single log lines, got {total_single} from {stdout_content}"
        )


@pytest.mark.timeout(120)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_multiple_ongoing_flushes_no_deadlock() -> None:
    """
    The goal is to make sure when a user sends multiple sync flushes, we are not deadlocked.
    Because now a flush call is purely sync, it is very easy to get into a deadlock.
    So we assert the last flush call will not get into such a state.
    """
    with configured(
        enable_log_forwarding=True, enable_file_capture=True, tail_log_lines=100
    ):
        pm = this_host().spawn_procs(per_host={"gpus": 4})
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
                    coro=log_mesh.flush(context().actor_instance._as_rust())
                    .spawn()
                    .task()
                )
            )

        # The last flush should not block
        futures[-1].get()


# oss_skip: (SF) broken in GitHub by D86994420. Passes internally.
@pytest.mark.oss_skip
async def test_adjust_aggregation_window() -> None:
    """Test that the flush deadline is updated when the aggregation window is adjusted.

    This tests the corner case: "This can happen if the user has adjusted the aggregation window."
    """
    with configured_with_redirected_stdio(
        capture_stderr=False,
        enable_log_forwarding=True,
        enable_file_capture=True,
        tail_log_lines=100,
    ) as (_, paths):
        pm = this_host().spawn_procs(per_host={"gpus": 2})
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

        # Wait for > aggregation window (2 secs)
        await asyncio.sleep(4)

        # Flush to ensure all output is written before reading
        sys.stdout.flush()

        # Read the captured output
        with open(paths.stdout, "r") as f:
            stdout_content = f.read()

        # Verify that logs were flushed when the aggregation window was adjusted
        # We should see both batches of logs in the output
        assert re.search(
            r"\[6 similar log lines\].*first batch of logs", stdout_content
        ), stdout_content

        assert re.search(r"similar log lines.*second batch of logs", stdout_content), (
            stdout_content
        )


class SendAlot(Actor):
    @endpoint
    async def send(self, port: Port[int]):
        for i in range(100):
            port.send(i)


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_port_as_argument() -> None:
    proc_mesh = fake_in_process_host().spawn_procs(per_host={"gpus": 1})
    s = proc_mesh.spawn("send_alot", SendAlot)
    send, recv = Channel[int].open()

    s.send.broadcast(send)

    for i in range(100):
        assert i == recv.recv().get()


class LsActor(Actor):
    def __init__(self, workspace: str):
        self.workspace = workspace

    @endpoint
    async def ls(self) -> list[str]:
        return os.listdir(self.workspace)


# Workspace sync test - requires rsync server infrastructure.
async def test_sync_workspace() -> None:
    # create two workspaces: one for local and one for remote
    with (
        tempfile.TemporaryDirectory() as workspace_src,
        tempfile.TemporaryDirectory() as workspace_dst,
    ):
        host = create_local_host_mesh(env={"WORKSPACE_DIR": workspace_dst})
        pm = host.spawn_procs(per_host={"gpus": 1})
        code_sync_mesh = host

        config = defaults.config("slurm", workspace_src)
        await code_sync_mesh.sync_workspace(
            workspace=config.workspace, auto_reload=True
        )

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
        await code_sync_mesh.sync_workspace(config.workspace, auto_reload=True)
        for item in list(am.ls.call().get()):
            assert len(item[1]) == 1
            assert item[1][0] == "new_file"
            file_path = os.path.join(workspace_dst, item[1][0])
            with open(file_path, "r") as f:
                assert f.readline() == "hello world"

    # sanity check
    assert "WORKSPACE_DIR" not in os.environ, "test leaves env var side-effects!"


@pytest.mark.timeout(120)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_proc_mesh_stop_after_actor_mesh_stop() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 2})
    am = pm.spawn("printer", Printer)
    # Make sure the actor is initialized first, else the stop and init can
    # race and the message becomes undeliverable.
    await am.print.call("hello world")

    await cast(ActorMesh, am).stop()
    await pm.stop()


class PortedActor(Actor):
    @endpoint(explicit_response_port=True)
    def add(self, port: "Port[int]", b: int) -> None:
        port.send(3 + b)


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_ported_actor():
    proc_mesh = fake_in_process_host().spawn_procs(per_host={"gpus": 1})
    a = proc_mesh.spawn("port_actor", PortedActor)
    assert 5 == a.add.call_one(2).get()


async def _recv():
    return (7, 2, 3)


async def consume():
    r = await PythonTask.from_coroutine(_recv())
    assert r == (7, 2, 3)


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_python_task_tuple() -> None:
    PythonTask.from_coroutine(consume()).block_on()


@parametrize_config(actor_queue_dispatch={True, False})
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


@parametrize_config(actor_queue_dispatch={True, False})
def test_mesh_len():
    proc_mesh = fake_in_process_host().spawn_procs(per_host={"gpus": 12})
    s = proc_mesh.spawn("sleep_actor", SleepActor)
    assert 12 == len(s)


@pytest.mark.oss_skip
def test_long_endpoint_completes() -> None:
    """Tests that a mesh with endpoints that take a long time to complete still
    complete and don't hit supervision errors"""
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    sleeper = pm.spawn("sleep", SleepActor)
    # Sleep for 60 seconds and then complete without an exception.
    sleeper.sleep.call(60).get()


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


@pytest.mark.timeout(10)
# Not compatible with queue dispatch, as it assumes concurrent dispatch
async def test_undeliverable_message_with_override() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 1})
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


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_undeliverable_message_without_override() -> None:
    # This test generates a fault that reaches the client. We don't want it to
    # crash.
    monarch.actor.unhandled_fault_hook = lambda failure: None
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    sender = pm.spawn("undeliverable_sender", UndeliverableMessageSender)
    sender.send_undeliverable.call().get()
    # Wait a few seconds to ensure that the undeliverable message is processed
    # without crashing anything
    await asyncio.sleep(5)
    pm.stop().get()


@parametrize_config(actor_queue_dispatch={True, False})
def test_this_and_that():
    proc = this_proc()
    counter = proc.spawn("counter", Counter, 7)
    assert 7 == counter.value.call_one().get()


class ReceptorActor(Actor):
    @endpoint
    def status(self):
        return 1


@parametrize_config(actor_queue_dispatch={True, False})
async def test_things_survive_losing_python_reference() -> None:
    """Test the slice_receptor_mesh function in LOCAL mode, verifying that setup methods are called."""

    pm = this_host().spawn_procs(per_host={"gpus": 1})
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
    async def return_root(self):
        return self._root

    @endpoint
    async def spawning_from_endpoint(self, name, root, get_or_spawn) -> None:
        await get_or_spawn(name, SpawningActorFromEndpointActor, root=root)


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_get_or_spawn_controller_inside_actor_endpoint():
    actor_1 = get_or_spawn_controller("actor_1", SpawningActorFromEndpointActor).get()
    actor_1.spawning_from_endpoint.call_one(
        "actor_2", "actor_1", get_or_spawn_controller
    ).get()
    actor_2 = get_or_spawn_controller("actor_2", SpawningActorFromEndpointActor).get()
    # verify that actor_2 was spawned from actor_1 with the correct root
    assert actor_2.return_root.call_one().get() == "actor_1"


class Hello(Actor):
    @endpoint
    def doit(self) -> str:
        return "hello!"


@parametrize_config(actor_queue_dispatch={True, False})
def test_simple_bootstrap():
    with TemporaryDirectory() as d:
        procs = []
        workers = []

        for i in range(4):
            addr = f"ipc://{d}/{i}"
            env = {**os.environ}
            if "FB_XAR_INVOKED_NAME" in os.environ:
                env["PYTHONPATH"] = ":".join(sys.path)
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    f'import sys; from monarch.actor import run_worker_loop_forever; run_worker_loop_forever(address={repr(addr)}, ca="trust_all_connections")',
                ],
                env=env,
            )
            procs.append(proc)
            workers.append(addr)

        hosts = attach_to_workers(ca="trust_all_connections", workers=workers)

        hello = hosts.spawn_procs().spawn("hello", Hello)

        r = hello.doit.call().get()
        for _, v in r.items():
            assert v == "hello!"
        for proc in procs:
            proc.kill()
            proc.wait()


class HostMeshActor(Actor):
    @endpoint
    async def this_host(self) -> HostMesh:
        return this_host()


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
def test_this_host() -> None:
    host = create_local_host_mesh(Extent(["hosts"], [6]))
    hosts_by_rank = [host.slice(hosts=i) for i in range(6)]
    for r, h in enumerate(hosts_by_rank):
        hy_host = h._hy_host_mesh.block_on()  # type: ignore
        assert hy_host.region.slice().offset == r
        assert len(hy_host.region.slice()) == 1

    proc_mesh_all = host.spawn_procs(per_host={"gpus": 2})
    # Make sure it works with a proc mesh spawned on a sliced host mesh
    proc_mesh_012 = host.slice(hosts=slice(0, 3)).spawn_procs(per_host={"gpus": 2})
    proc_mesh_345 = host.slice(hosts=slice(3, 6)).spawn_procs(per_host={"gpus": 2})

    am_all = proc_mesh_all.spawn("all", HostMeshActor)
    am_012 = proc_mesh_012.spawn("a012", HostMeshActor)
    am_345 = proc_mesh_345.spawn("a345", HostMeshActor)

    expected_hosts_by_rank = [h for h in hosts_by_rank for _ in range(2)]
    assert list(am_all.this_host.call().get().values()) == expected_hosts_by_rank
    assert list(am_012.this_host.call().get().values()) == expected_hosts_by_rank[:6]
    assert list(am_345.this_host.call().get().values()) == expected_hosts_by_rank[6:]

    # Procs 3 and 5 on hosts 1 and 2
    proc_mesh_012 = proc_mesh_012.slice(hosts=slice(1, 3), gpus=1)
    # Procs 6 and 10 on hosts 3 and 5
    proc_mesh_345 = proc_mesh_345.slice(hosts=slice(0, 3, 2), gpus=0)

    am_012 = proc_mesh_012.spawn("a012", HostMeshActor)
    am_345 = proc_mesh_345.spawn("a345", HostMeshActor)

    assert list(am_012.this_host.call().get().values()) == [
        expected_hosts_by_rank[3],
        expected_hosts_by_rank[5],
    ]
    assert list(am_345.this_host.call().get().values()) == [
        expected_hosts_by_rank[6],
        expected_hosts_by_rank[10],
    ]


class FakeLocalLoginJob(LoginJob):
    """

    Fake it that we are logging in by just making a local process that runs the bootstrap.
    """

    def __init__(self, dir: str):
        super().__init__()
        self._dir = dir

    def _start_host(self, host: str) -> ProcessState:
        env = {**os.environ}
        if "FB_XAR_INVOKED_NAME" in os.environ:
            env["PYTHONPATH"] = ":".join(sys.path)
        addr = f"ipc://{self._dir}/{host}"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                f'from monarch.actor import run_worker_loop_forever; run_worker_loop_forever(address={repr(addr)}, ca="trust_all_connections")',
            ],
            env=env,
            start_new_session=True,
        )
        return ProcessState(proc.pid, addr)


@parametrize_config(actor_queue_dispatch={True, False})
def test_login_job():
    with TemporaryDirectory() as temp_dir:
        j = FakeLocalLoginJob(temp_dir)
        j.add_mesh("hosts", ["fake", "hosts"])
        state = j.state(cached_path=None)

        hello = state.hosts.spawn_procs().spawn("hello", Hello)
        r = hello.doit.call().get()
        for _, v in r.items():
            assert v == "hello!"

        j.kill()


_global_foo = None


def setup_with_spawn() -> None:
    global _global_foo
    proc = this_proc()
    # Doesn't matter which actor is spawned, just make sure it persists.
    _global_foo = proc.spawn("foo", Counter, 0)
    _global_foo.incr.call().get()
    # Spawn one that dies to make sure it doesn't cause any issues.
    bar = proc.spawn("bar", Counter, 0)
    bar.incr.call().get()


async def async_setup_with_spawn() -> None:
    global _global_foo
    proc = this_proc()
    # Doesn't matter which actor is spawned, just make sure it persists.
    _global_foo = proc.spawn("foo", Counter, 0)
    await _global_foo.incr.call()
    # Spawn one that dies to make sure it doesn't cause any issues.
    bar = proc.spawn("bar", Counter, 0)
    await bar.incr.call()


# oss_skip: passes internally but fails on CI with "ValueError: error spawning proc mesh: statuses: Timeout(30.000905376s)=0..1"
@pytest.mark.oss_skip
def test_setup() -> None:
    procs = this_host().spawn_procs(bootstrap=setup_with_spawn)
    counter = procs.spawn("counter", Counter, 0)
    counter.incr.call().get()
    # Make sure no errors occur in the meantime
    time.sleep(10)


# oss_skip: passes internally but fails on CI with "ValueError: error spawning proc mesh: statuses: Timeout(30.000905376s)=0..1"
@pytest.mark.oss_skip
def test_setup_async() -> None:
    procs = this_host().spawn_procs(bootstrap=async_setup_with_spawn)
    counter = procs.spawn("counter", Counter, 0)
    counter.incr.call().get()
    # Make sure no errors occur in the meantime
    time.sleep(10)


class CaptureLogs:
    def __init__(self):
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(logging.Formatter("%(message)s"))

        logger = logging.getLogger("capture")
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        self.log_stream = log_stream
        self.logger = logger

    @property
    def contents(self) -> str:
        return self.log_stream.getvalue()


class Named(Actor):
    @endpoint
    def report(self) -> Any:
        logs = CaptureLogs()
        logs.logger.error("HUH")
        assert "test_python_actors.Named the_name{'f': 0/2}>" in logs.contents

        return context().actor_instance.creator, str(context().actor_instance)


@parametrize_config(actor_queue_dispatch={True, False})
def test_instance_name():
    cr, result = (
        this_host()
        .spawn_procs(per_host={"f": 2})
        .spawn("the_name", Named)
        .slice(f=0)
        .report.call_one()
        .get()
    )
    assert "test_python_actors.Named the_name{'f': 0/2}>" in result
    assert cr.name == "root"
    assert str(context().actor_instance) == "<root>"

    logs = CaptureLogs()
    logs.logger.error("HUH")
    assert "actor=<root>" in logs.contents
    try:
        monarch.actor.config.prefix_python_logs_with_actor = False
        logs = CaptureLogs()
        logs.logger.error("HUH")
        assert "actor=" not in logs.contents
    finally:
        monarch.actor.config.prefix_python_logs_with_actor = True


class TestPytokioActor(Actor):
    @endpoint
    def context_propagated_through_spawn(self) -> None:
        cx = context()

        async def task():
            assert cx is context()

        PythonTask.from_coroutine(coro=task()).spawn().block_on()

    @endpoint
    def context_propagated_through_spawn_blocking(self) -> None:
        cx = context()

        def task():
            assert cx is context()

        PythonTask.spawn_blocking(task).block_on()


@parametrize_config(actor_queue_dispatch={True, False})
def test_context_propagated_through_python_task_spawn():
    p = this_host().spawn_procs()
    a = p.spawn("test_pytokio_actor", TestPytokioActor)
    a.context_propagated_through_spawn.call().get()


@parametrize_config(actor_queue_dispatch={True, False})
def test_context_propagated_through_python_task_spawn_blocking():
    p = this_host().spawn_procs()
    a = p.spawn("test_pytokio_actor", TestPytokioActor)
    a.context_propagated_through_spawn_blocking.call().get()


class ActorWithCleanup(Actor):
    def __init__(self, counter: Counter) -> None:
        self.counter = counter

    @endpoint
    def check(self) -> None:
        pass

    def __cleanup__(self, exc: Exception | None):
        self.logger.info(f"Calling __cleanup__ on {self}, {exc=}")
        self.counter.incr.call_one().get()


class ActorWithAsyncCleanup(Actor):
    def __init__(self, counter: Counter) -> None:
        self.counter = counter

    @endpoint
    async def check(self) -> None:
        pass

    # Cleanup should match the async-ness of the other endpoints.
    async def __cleanup__(self, exc: Exception | None):
        self.logger.info(f"Calling __cleanup__ on {self}, {exc=}")
        await self.counter.incr.call_one()


@parametrize_config(actor_queue_dispatch={True, False})
def test_cleanup():
    procs = this_host().spawn_procs(per_host={"gpus": 1})
    counter = procs.spawn("counter", Counter, 0)
    cleanup = procs.spawn("cleanup", ActorWithCleanup, counter)
    # Call an endpoint to ensure it is constructed.
    cleanup.check.call_one().get()
    cast(ActorMesh[ActorWithCleanup], cleanup).stop().get()
    assert counter.value.call_one().get() == 1


@parametrize_config(actor_queue_dispatch={True, False})
def test_cleanup_async():
    procs = this_host().spawn_procs(per_host={"gpus": 1})
    counter = procs.spawn("counter", Counter, 0)
    cleanup = procs.spawn("cleanup", ActorWithCleanup, counter)
    # Call an endpoint to ensure it is constructed.
    cleanup.check.call_one().get()
    cast(ActorMesh[ActorWithCleanup], cleanup).stop().get()
    assert counter.value.call_one().get() == 1


class WrapperActor(Actor):
    """Just spawns an actor and does nothing with it."""

    def __init__(self, procs: ProcMesh, counter: Counter) -> None:
        # Ensure both the proc mesh and actor mesh owned by this actor are
        # cleaned up, and that there's no race between the two.
        self.procs = this_host().spawn_procs()
        # Also test that when spawning actors on a foreign proc mesh, the actors
        # are cleaned up even though the client is still alive.
        self.mesh_on_passed_procs = procs.spawn(
            "inner_passed", ActorWithCleanup, counter
        )
        self.mesh_on_owned_procs = self.procs.spawn(
            "inner_owned", ActorWithCleanup, counter
        )

    @endpoint
    def check(self) -> None:
        self.mesh_on_passed_procs.check.call().get()
        self.mesh_on_owned_procs.check.call().get()

    # Note that there is no __cleanup__ defined, the inner mesh should be
    # automatically stopped without needing to define one.


@parametrize_config(actor_queue_dispatch={True, False})
def test_recursive_stop():
    """Tests that if A owns B, and A is stopped, B is also stopped. Cleanup
    actors are used because we can observe a side effect of them stopping"""
    procs = this_host().spawn_procs(per_host={"gpus": 1})
    counter = procs.spawn("counter", Counter, 0)
    wrapper = procs.spawn("wrapper", WrapperActor, procs, counter)
    # Call an endpoint to ensure it is constructed.
    wrapper.check.call_one().get()
    # Calling stop on WrapperActor should also stop its owned ActorWithCleanup.
    cast(ActorMesh[WrapperActor], wrapper).stop().get()
    # The incr messages in the cleanups have no sequencing guarantee with when
    # stop is complete, nor with any further messages sent from this client.
    # So we need to make sure there is time for both messages to be processed.
    time.sleep(10)
    # Two increments to the counter: one for the actors on the owned proc mesh,
    # and one for the actors on the passed-in proc mesh.
    assert counter.value.call_one().get() == 2


@parametrize_config(actor_queue_dispatch={True, False})
def test_get_or_spawn_controller_on_unpickled_proc_mesh():
    class StoreActor(Actor):
        def __init__(self):
            self._store = {}

        @endpoint
        def put(self, key, value):
            self._store[key] = value

        @endpoint
        def get(self, key):
            return self._store[key]

    class MyActor(Actor):
        @endpoint
        def register_pm(self, proc_mesh):
            self.pm = proc_mesh

        @endpoint
        def spawn_and_get_from_store(self, actor_cls, key):
            child_actor = self.pm.spawn("child_actor", actor_cls)
            return child_actor.get_from_store.call_one(key).get()

    class ChildActor(Actor):
        @endpoint
        def get_from_store(self, key):
            return (
                get_or_spawn_controller("store", StoreActor)
                .get()
                .get.call_one(key)
                .get()
            )

    pm = this_host().spawn_procs(per_host={"procs": 1})
    my_actor = pm.spawn("my_actor", MyActor)

    store = get_or_spawn_controller("store", StoreActor).get()
    store.put.call_one("a", 1).get()

    child_pm = this_host().spawn_procs(per_host={"procs": 1})
    my_actor.register_pm.call(child_pm).get()
    assert my_actor.spawn_and_get_from_store.call_one(ChildActor, "a").get() == 1


@pytest.mark.timeout(60)
def test_raw_actor_mesh_pickle_blocks_on_proc_mesh_init() -> None:
    async def sleep_then_mesh(pm: Shared[HyProcMesh]) -> HyProcMesh:
        time.sleep(15)
        return await pm

    proc_mesh = this_host().spawn_procs(name="test_proc")
    proc_mesh._proc_mesh = PythonTask.from_coroutine(
        sleep_then_mesh(proc_mesh._proc_mesh)
    ).spawn()
    actor_mesh = proc_mesh.spawn("test", Hello)
    assert proc_mesh._proc_mesh.poll() is None
    cloudpickle.dumps(actor_mesh)
    assert proc_mesh._proc_mesh.poll() is not None
