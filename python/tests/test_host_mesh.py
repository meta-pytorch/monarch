# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import os
import pathlib
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import weakref
from contextlib import ExitStack, suppress
from typing import Any, cast, Dict, Generator, IO, List, Optional, Set
from unittest.mock import patch

import cloudpickle
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.context import Instance as HyInstance
from monarch._rust_bindings.monarch_hyperactor.host_mesh import (
    BootstrapCommand,
    HostMesh as HyHostMesh,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ProcId, Uid
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Point, Shape, Slice
from monarch._src.actor.actor_mesh import (
    _client_context,
    Actor,
    attach,
    context,
    shutdown_context,
)
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import (
    default_bootstrap_cmd,
    HostMesh,
    this_host,
    this_proc,
)
from monarch._src.actor.pickle import flatten, unflatten
from monarch._src.actor.proc_mesh import _get_bootstrap_args, get_or_spawn_controller
from monarch._src.job.job import ProcessState
from monarch._src.job.process import ProcessJob
from monarch._src.job.service_identity import new_service_proc_id, service_proc_addr
from monarch.config import configured
from scoped_state import scoped_state


@pytest.mark.timeout(60)
def test_process_job_host_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        assert host.extent.labels == ["hosts"]
        assert host.extent.sizes == [1]
        assert not host.stream_logs
        hy_host = host._hy_host_mesh.block_on()
        assert hy_host.region.labels == host.region.labels
        assert hy_host.region.slice() == host.region.slice()


@pytest.mark.timeout(60)
def test_multi_host_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 8}), cached_path=None) as state:
        host = state.hosts
        assert host.extent.labels == ["hosts"]
        assert host.extent.sizes == [8]
        assert not host.stream_logs
        assert host._ndslice == Slice(offset=0, sizes=[8], strides=[1])
        assert host._labels == ("hosts",)
        hy_host = host._hy_host_mesh.block_on()
        assert hy_host.region.labels == host.region.labels
        assert hy_host.region.slice() == host.region.slice()

        # Hosts 5 and 7
        slice_shape = Shape(
            labels=["hosts"], slice=Slice(offset=5, sizes=[2], strides=[2])
        )
        sliced = host._new_with_shape(slice_shape)
        assert sliced.extent.labels == ["hosts"]
        assert sliced.extent.sizes == [2]
        assert not sliced.stream_logs
        assert sliced._ndslice == Slice(offset=5, sizes=[2], strides=[2])
        assert sliced._labels == ("hosts",)
        hy_sliced = sliced._hy_host_mesh.block_on()
        assert hy_sliced.region.labels == sliced.region.labels
        assert hy_sliced.region.slice() == sliced.region.slice()

        release_host_mesh = threading.Event()

        async def resolve_host_mesh():
            await PythonTask.spawn_blocking(release_host_mesh.wait)
            return hy_host

        # Keep the native mesh unresolved so slicing must take the pending path.
        host._inner_host_mesh = PythonTask.from_coroutine(resolve_host_mesh()).spawn()
        try:
            assert host._hy_host_mesh.poll() is None
            pending_sliced = host._new_with_shape(slice_shape)
            assert pending_sliced._hy_host_mesh.poll() is None
        finally:
            release_host_mesh.set()

        hy_pending_sliced = pending_sliced._hy_host_mesh.block_on()
        assert hy_pending_sliced.region.labels == pending_sliced.region.labels
        assert hy_pending_sliced.region.slice() == pending_sliced.region.slice()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_spawn_proc_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 8}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(name="proc")
        assert proc_mesh._host_mesh is host
        assert proc_mesh._ndslice == host._ndslice
        assert tuple(proc_mesh._labels) == host._labels
        hy_proc_mesh = proc_mesh._proc_mesh.block_on()
        assert tuple(hy_proc_mesh.region.labels) == host._labels
        assert hy_proc_mesh.region.slice() == host.region.slice()

        # Hosts 5 and 7
        sliced_host = host._new_with_shape(
            Shape(labels=["hosts"], slice=Slice(offset=5, sizes=[2], strides=[2]))
        )
        sliced_proc = sliced_host.spawn_procs(
            name="proc_sliced", per_host={"gpus": 3, "just_for_fun": 4}
        )
        hy_sliced_proc = sliced_proc._proc_mesh.block_on()
        assert sliced_proc._host_mesh is sliced_host
        assert sliced_proc._ndslice == Slice(
            offset=0, sizes=[2, 3, 4], strides=[12, 4, 1]
        )
        assert sliced_proc._labels == ["hosts", "gpus", "just_for_fun"]
        assert hy_sliced_proc.region.labels == sliced_proc._labels
        assert hy_sliced_proc.region.slice() == sliced_proc._ndslice


@pytest.mark.timeout(60)
def test_pickle() -> None:
    with scoped_state(ProcessJob({"hosts": 8}), cached_path=None) as state:
        host = state.hosts
        host.initialized.get()
        _unused, pickled = flatten(host, lambda _: False)
        unpickled = unflatten(pickled.freeze(), _unused)
        assert isinstance(unpickled, HostMesh)
        assert host.extent.labels == ["hosts"]
        assert host.extent.sizes == [8]
        assert not host.stream_logs
        assert host._ndslice == Slice(offset=0, sizes=[8], strides=[1])
        assert host._labels == ("hosts",)
        hy_host = host._hy_host_mesh.block_on()
        assert hy_host.region.labels == host.region.labels
        assert hy_host.region.slice() == host.region.slice()


class RankActor(Actor):
    @endpoint
    async def get_rank(self) -> int:
        return context().actor_instance.rank.rank

    @endpoint
    async def get_pid(self) -> int:
        return os.getpid()


def is_process_running(pid: int) -> bool:
    """Check if a process with the given PID is still running."""
    try:
        os.kill(pid, 0)  # Signal 0 doesn't kill, just checks if process exists
        return True
    except OSError:
        return False


def _wait_for_marker(path: pathlib.Path) -> None:
    # Stress runs start several isolated ProcessJobs at once, so reaching the
    # bootstrap wrapper can take longer than a focused run.
    deadline = time.monotonic() + 30
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for marker {path}")
        time.sleep(0.01)


async def _wait_for_event(event: threading.Event, message: str) -> None:
    reached = await asyncio.wait_for(
        asyncio.to_thread(event.wait, 30),
        timeout=35,
    )
    assert reached, message


class _AwaitRecord:
    """Record when a synthetic pending spawn is actually awaited."""

    def __init__(self, record: list[str], value: str) -> None:
        self._record = record
        self._value = value

    def __await__(self) -> Generator[Any, None, None]:
        async def record() -> None:
            self._record.append(self._value)

        return record().__await__()


class _PendingActorRecord:
    """Expose an initializer that records when the real actor queue drives it."""

    def __init__(self, record: list[str]) -> None:
        self._record = record

    @property
    def initialized(self) -> Future[bool]:
        async def record() -> bool:
            self._record.append("pending_actor")
            return True

        return Future._from_coro(record())


class _HostTeardownCallThrough:
    """Observe binding calls while forwarding every call to the real mesh.

    The wrapper can count entry into ``PyHostMesh.stop`` and ``shutdown``. It
    cannot see which ``SharedCell::take`` branch Rust takes, so tests must not
    use it to claim that an inner domain operation ran.
    """

    def __init__(
        self,
        inner: HyHostMesh,
        record: list[str] | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self._inner = inner
        self._record = record
        self._release = release
        self.entered = threading.Event()
        self.calls: list[str] = []

    def _wrap(self, operation: str, task: PythonTask[None]) -> PythonTask[None]:
        self.calls.append(operation)
        if self._record is not None:
            self._record.append(f"native_{operation}")
        self.entered.set()
        release = self._release
        if release is None:
            return task
        release_event = cast(threading.Event, release)

        async def gated() -> None:
            released = await PythonTask.spawn_blocking(
                lambda: release_event.wait(timeout=30)
            )
            if not released:
                raise TimeoutError("host teardown release was not published")
            await task

        return PythonTask.from_coroutine(gated())

    def shutdown(self, instance: HyInstance) -> PythonTask[None]:
        return self._wrap("shutdown", self._inner.shutdown(instance))

    def stop(self, instance: HyInstance) -> PythonTask[None]:
        return self._wrap("stop", self._inner.stop(instance))


def _install_host_teardown_call_through(
    host: HostMesh,
    record: list[str] | None = None,
    release: threading.Event | None = None,
    inner: HyHostMesh | None = None,
) -> _HostTeardownCallThrough:
    if inner is None:
        inner = host._hy_host_mesh.block_on()
    call_through = _HostTeardownCallThrough(inner, record, release)
    # HostMesh reads this value through the Shared interface. The test wrapper
    # preserves that interface and forwards every teardown call to the exact
    # native object it replaced.
    host._inner_host_mesh = Shared.from_value(cast(HyHostMesh, call_through))
    return call_through


def _gated_bootstrap_command(
    directory: pathlib.Path,
) -> tuple[BootstrapCommand, pathlib.Path, pathlib.Path]:
    entered = directory / "bootstrap-entered"
    release = directory / "bootstrap-release"
    wrapper = directory / "gated-bootstrap.py"
    wrapper.write_text(
        textwrap.dedent(
            """\
            import os
            import pathlib
            import sys
            import time

            entered, release, program, arg0, *args = sys.argv[1:]
            pathlib.Path(entered).touch()
            deadline = time.monotonic() + 30
            while not pathlib.Path(release).exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("bootstrap release marker was not published")
                time.sleep(0.01)
            os.execvpe(program, [arg0, *args], os.environ)
            """
        )
    )

    base = default_bootstrap_cmd()
    # BootstrapCommand's Rust binding exposes every field, but its stub declares
    # only env. Read the argv from the helper used by default_bootstrap_cmd so
    # this fixture follows the real default without widening the production stub.
    original_program, original_args, _ = _get_bootstrap_args()
    command = BootstrapCommand(
        sys.executable,
        None,
        [
            str(wrapper),
            str(entered),
            str(release),
            original_program,
            original_program,
            *(original_args or []),
        ],
        dict(base.env),
    )
    return command, entered, release


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_preconstructed_shutdowns_drain_after_observer_cancellation() -> None:
    job = ProcessJob({"hosts": 1})
    release = threading.Event()
    first_shutdown: Future[None] | None = None
    try:
        host = job.state(cached_path=None).hosts
        proc_mesh = host.spawn_procs(name="drain_order")
        await asyncio.wait_for(proc_mesh.initialized, timeout=30)
        inner = host._hy_host_mesh.poll()
        assert inner is not None

        order: list[str] = []
        # These synthetic completions sit in the real pending-proc and
        # pending-actor queues. They prove traversal and ordering through those
        # queues, not the lifecycle of real proc or actor spawns.
        host._pending_spawns = [cast(Any, _AwaitRecord(order, "pending_proc"))]
        proc_mesh._pending_actor_spawns = [cast(Any, _PendingActorRecord(order))]
        flush_logging = proc_mesh._logging_manager._flush_from_tokio

        async def record_logging_flush() -> None:
            await flush_logging()
            order.append("logging_flush")

        call_through = _install_host_teardown_call_through(
            host,
            order,
            release,
            inner,
        )
        first_shutdown = host.shutdown()
        second_shutdown = host.shutdown()
        assert order == []
        assert call_through.calls == []

        with patch.object(
            proc_mesh._logging_manager,
            "_flush_from_tokio",
            record_logging_flush,
        ):
            observer = first_shutdown.as_asyncio()
            await _wait_for_event(
                call_through.entered,
                "host shutdown did not reach the native binding",
            )
            assert order == [
                "pending_proc",
                "pending_actor",
                "logging_flush",
                "native_shutdown",
            ]
            assert call_through.calls == ["shutdown"]

            assert observer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await observer

            release.set()
            assert (
                await asyncio.wait_for(
                    asyncio.to_thread(first_shutdown.get, 30),
                    timeout=35,
                )
                is None
            )
            assert await asyncio.wait_for(first_shutdown, timeout=30) is None
            assert host._inner_host_mesh is None

            with pytest.raises(RuntimeError) as second_error:
                await asyncio.wait_for(second_shutdown, timeout=30)
            assert type(second_error.value) is RuntimeError
            assert str(second_error.value) == "HostMesh has already been shut down"
            assert order == [
                "pending_proc",
                "pending_actor",
                "logging_flush",
                "native_shutdown",
                "logging_flush",
            ]
            assert call_through.calls == ["shutdown"]
    finally:
        release.set()
        try:
            if first_shutdown is not None:
                try:
                    await asyncio.wait_for(first_shutdown, timeout=30)
                except (Exception, asyncio.CancelledError):
                    pass
        finally:
            job.kill()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_shutdown_host_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        hm = state.hosts
        discarded_stop = hm.stop()
        discarded_shutdown = hm.shutdown()
        del discarded_stop, discarded_shutdown
        pm = hm.spawn_procs(per_host={"gpus": 2})
        am = pm.spawn("actor", RankActor)
        am.get_rank.choose().get(timeout=30)
        assert hm.shutdown().get(timeout=30) is None
        assert hm._inner_host_mesh is None


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_shutdown_immediately_after_proc_spawn_without_actor() -> None:
    with ExitStack() as cleanup:
        job = ProcessJob({"hosts": 1})
        cleanup.callback(job.kill)
        hm = job.state(cached_path=None).hosts
        hm.spawn_procs(name="pending_proc")

        shutdown_result = hm.shutdown().get(timeout=10.0)
        cleanup.pop_all()
        assert shutdown_result is None

        assert hm._inner_host_mesh is None
        with pytest.raises(
            RuntimeError,
            match="HostMesh has already been shut down",
        ):
            hm.spawn_procs()


@pytest.mark.timeout(90)
@isolate_in_subprocess
def test_shutdown_waits_for_a_pending_proc_spawn_after_caller_drops_mesh() -> None:
    with tempfile.TemporaryDirectory(prefix="monarch_spawn_gate_") as directory:
        command, entered, release = _gated_bootstrap_command(pathlib.Path(directory))
        with ExitStack() as cleanup:
            job = ProcessJob({"hosts": 1})
            cleanup.callback(job.kill)
            cleanup.callback(release.touch)
            hm = job.state(cached_path=None).hosts
            proc_mesh = hm.spawn_procs(
                name="pending_proc",
                bootstrap_command=command,
            )
            # HostMesh retains the raw native spawn in _pending_spawns and the
            # ProcMesh retains a second Shared that performs logging and setup.
            # Keep witnesses for both before discarding the caller's ProcMesh.
            spawn = hm._pending_spawns[-1]
            proc_initialization = proc_mesh._proc_mesh
            retained_proc_mesh = weakref.ref(proc_mesh)
            del proc_mesh
            assert retained_proc_mesh() is hm._proc_meshes[-1]
            _wait_for_marker(entered)

            flush_entered = threading.Event()
            flush_exited = threading.Event()
            release_shutdown = threading.Event()
            cleanup.callback(release_shutdown.set)
            flush_pending_spawns = hm._flush_pending_spawns

            async def record_flush_entry() -> None:
                flush_entered.set()
                await flush_pending_spawns()
                flush_exited.set()
                released = await PythonTask.spawn_blocking(
                    lambda: release_shutdown.wait(timeout=30)
                )
                if not released:
                    raise TimeoutError("native shutdown release was not published")

            with patch.object(hm, "_flush_pending_spawns", record_flush_entry):
                shutdown = hm.shutdown()
                shutdown_results: list[None] = []
                shutdown_errors: list[Exception] = []
                shutdown_done = threading.Event()

                def drive_shutdown() -> None:
                    try:
                        shutdown_results.append(shutdown.get(timeout=45))
                    except Exception as error:
                        shutdown_errors.append(error)
                    finally:
                        shutdown_done.set()

                shutdown_thread = threading.Thread(
                    target=drive_shutdown,
                    daemon=True,
                    name="drive-host-mesh-shutdown",
                )
                shutdown_thread.start()
                try:
                    # Start shutdown on another thread so entry into the flush,
                    # rather than thread scheduling, is the synchronization
                    # point for proving that the pending spawn blocks it.
                    assert flush_entered.wait(timeout=10)
                    assert spawn.poll() is None
                    assert proc_initialization.poll() is None
                    assert not flush_exited.is_set()
                    assert not shutdown_done.is_set()

                    release.touch()
                    completion_deadline = time.monotonic() + 30
                    while spawn.poll() is None:
                        if time.monotonic() >= completion_deadline:
                            raise AssertionError(
                                "proc spawn did not complete after bootstrap release"
                            )
                        time.sleep(0.01)
                    assert flush_exited.wait(
                        timeout=max(0.0, completion_deadline - time.monotonic())
                    )
                    proc_initialization_value = proc_initialization.poll()
                    while proc_initialization_value is None:
                        if time.monotonic() >= completion_deadline:
                            raise AssertionError(
                                "proc initialization did not complete after bootstrap release"
                            )
                        time.sleep(0.01)
                        proc_initialization_value = proc_initialization.poll()
                    release_shutdown.set()
                    assert shutdown_done.wait(timeout=15)
                finally:
                    release.touch()
                    release_shutdown.set()
                    try:
                        if shutdown_thread.is_alive():
                            job.kill()
                    finally:
                        shutdown_thread.join(timeout=10)

                assert not shutdown_thread.is_alive()
                assert flush_exited.is_set()
                assert shutdown_errors == []
                assert shutdown_results == [None]

            assert spawn.poll() is not None
            assert proc_initialization_value is not None
            assert hm._pending_spawns == []
            assert hm._inner_host_mesh is None
            with pytest.raises(
                RuntimeError,
                match="HostMesh has already been shut down",
            ):
                hm.spawn_procs()


def _spawn_embedded_worker(addr: str, child_output: IO[str]) -> "subprocess.Popen[str]":
    env = {**os.environ}
    if "FB_XAR_INVOKED_NAME" in os.environ:
        env["PYTHONPATH"] = ":".join(sys.path)
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from monarch.actor import run_worker_loop_until_shutdown; "
            f"run_worker_loop_until_shutdown(address={addr!r}, "
            'ca="trust_all_connections"); print("RETURNED", flush=True)',
        ],
        env=env,
        stdout=child_output,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def _reap_embedded_worker(child: "subprocess.Popen[str]", *, body_failed: bool) -> None:
    """Tear the worker down without masking a failure from the test body.

    Only raises on an otherwise-clean run; `body_failed` is passed in rather
    than inferred from `sys.exc_info()`, which reports the currently *handled*
    exception and can belong to an enclosing frame.
    """
    cleanup_failures: List[Exception] = []
    try:
        shutdown_context().get()
    except Exception as e:
        cleanup_failures.append(e)
    if child.poll() is None:
        with suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGKILL)
        try:
            child.wait(timeout=10.0)
        except subprocess.TimeoutExpired as e:
            cleanup_failures.append(e)
    if cleanup_failures and not body_failed:
        raise ExceptionGroup("embedded worker cleanup failed", cleanup_failures)


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_embedded_worker_loop_returns_after_host_shutdown() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    addr = f"{new_service_proc_id()}@tcp://127.0.0.1:{port}"
    with tempfile.TemporaryFile(mode="w+t") as child_output:
        child = _spawn_embedded_worker(addr, child_output)

        def read_child_output() -> str:
            child_output.seek(0)
            return child_output.read()

        body_failed = False
        try:
            hosts = attach_to_workers(ca="trust_all_connections", workers=[addr])
            hosts.initialized.get()
            hosts.shutdown().get()
            child.wait(timeout=20)
        except BaseException as e:
            body_failed = True
            e.add_note(f"embedded worker output:\n{read_child_output()}")
            raise
        finally:
            _reap_embedded_worker(child, body_failed=body_failed)

        output = read_child_output()
        assert child.returncode == 0, f"embedded worker failed:\n{output}"
        assert "RETURNED" in output


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_alias_bootstrap() -> None:
    """A worker started with an alias address (dial_to@bind_to) is reachable.

    The alias format `tcp://PUBLIC:PORT@tcp://0.0.0.0:PORT` lets a worker
    advertise a dialable address while binding to a wildcard interface. This
    is required where binding directly to the advertised address is not
    possible (e.g. behind NAT, where only `0.0.0.0` can be bound).
    """
    procs = []
    workers = []
    for _i in range(2):
        # Reserve an ephemeral port, then release it so the worker can bind
        # to it via the alias's bind_to address.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        env = {**os.environ}
        if "FB_XAR_INVOKED_NAME" in os.environ:
            env["PYTHONPATH"] = ":".join(sys.path)

        # dial_to advertises a reachable address; bind_to listens on the
        # wildcard interface.
        addr = f"tcp://127.0.0.1:{port}@tcp://0.0.0.0:{port}"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys; from monarch.actor import run_worker_loop_forever; "
                f'run_worker_loop_forever(address={addr!r}, ca="trust_all_connections")',
            ],
            env=env,
        )
        procs.append(proc)
        # Pass the full alias through attach_to_workers. The attach path must
        # canonicalize this to dial_to before using it as host identity.
        workers.append(addr)

    try:
        # pyrefly: ignore [bad-argument-type]
        hosts = attach_to_workers(ca="trust_all_connections", workers=workers)
        am = hosts.spawn_procs(per_host={"gpus": 1}).spawn("rank", RankActor)
        ranks = sorted(v for _, v in am.get_rank.call().get().items())
        assert ranks == [0, 1]
        hosts.shutdown().get()
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_attach_to_workers_accepts_future_addresses() -> None:
    """`attach_to_workers` accepts ``Future[str]`` worker addresses, exercising
    the ``_as_python_task`` -> ``Future._take_inner()`` path: the Future's
    underlying task is surrendered to Rust, driven there to yield the address,
    and the attach proceeds exactly as for plain ``str`` workers."""
    from monarch._src.actor.future import Future

    async def _resolve(addr: str) -> str:
        return addr

    procs = []
    workers = []
    for _i in range(2):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        env = {**os.environ}
        if "FB_XAR_INVOKED_NAME" in os.environ:
            env["PYTHONPATH"] = ":".join(sys.path)

        addr = f"tcp://127.0.0.1:{port}"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys; from monarch.actor import run_worker_loop_forever; "
                f'run_worker_loop_forever(address={addr!r}, ca="trust_all_connections")',
            ],
            env=env,
        )
        procs.append(proc)
        # Wrap the address in a Future[str] so attach goes through _take_inner.
        fut: Future[str] = Future._from_coro(_resolve(addr))
        workers.append(fut)

    try:
        # pyrefly: ignore [bad-argument-type]
        hosts = attach_to_workers(ca="trust_all_connections", workers=workers)
        am = hosts.spawn_procs(per_host={"gpus": 1}).spawn("rank", RankActor)
        ranks = sorted(v for _, v in am.get_rank.call().get().items())
        assert ranks == [0, 1]
        hosts.shutdown().get()
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_attach_starts_before_readiness_observation_and_survives_a_dropped_observer() -> (
    None
):
    address_started = threading.Event()
    release_address = threading.Event()

    async def resolve_address() -> str:
        address_started.set()
        released = await PythonTask.spawn_blocking(
            lambda: release_address.wait(timeout=30)
        )
        if not released:
            raise TimeoutError("worker address release was not published")
        return addr

    with ExitStack() as cleanup:
        job = ProcessJob({"hosts": 1})
        job.apply()
        cleanup.callback(job.kill)
        cleanup.callback(release_address.set)
        addr = job._host_to_pid["hosts_0"].channel
        address: Future[str] = Future._from_coro(resolve_address())

        hosts = attach_to_workers(
            ca="trust_all_connections",
            workers=[address],
        )

        with pytest.raises(ValueError) as consumed:
            address.get()
        assert str(consumed.value) == "Future was consumed."
        assert address_started.wait(timeout=10)

        discarded = hosts.initialized
        with pytest.raises(TimeoutError):
            discarded.get(timeout=0.25)
        del discarded
        release_address.set()

        assert hosts.initialized.get(timeout=30) is True
        assert hosts.initialized.get(timeout=30) is True
        assert hosts.shutdown().get(timeout=30) is None


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_host_mesh_context_manager() -> None:
    """Tests that the HostMesh can be used as a context manager and that it runs
    shutdown on exit"""
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        async with state.hosts as hm:
            pm = hm.spawn_procs(per_host={"gpus": 2})
            am = pm.spawn("actor", RankActor)
            await am.get_rank.choose()
        # Ensure that other operations fail after shutdown.
        with pytest.raises(RuntimeError, match="HostMesh has already been shut down"):
            hm.spawn_procs(per_host={"gpus": 2})
        with pytest.raises(RuntimeError, match="HostMesh has already been shut down"):
            await hm.shutdown()


@pytest.mark.timeout(60)
def test_shutdown_sliced_host_mesh_throws_exception() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        hm = state.hosts
        assert hm.initialized.get(timeout=30) is True
        hm_sliced = hm.slice(hosts=1)
        sliced_inner = hm_sliced._inner_host_mesh
        assert sliced_inner is not None
        reference = sliced_inner.poll()
        assert reference is not None
        instance = context().actor_instance._as_rust()

        with pytest.raises(RuntimeError) as raw_shutdown_error:
            reference.shutdown(instance)
        assert type(raw_shutdown_error.value) is RuntimeError
        assert str(raw_shutdown_error.value) == (
            "cannot shut down `HostMesh` that is a reference instead of owned"
        )

        with pytest.raises(RuntimeError) as raw_stop_error:
            reference.stop(instance)
        assert type(raw_stop_error.value) is RuntimeError
        assert str(raw_stop_error.value) == (
            "cannot stop `HostMesh` that is a reference instead of owned"
        )

        with pytest.raises(RuntimeError) as public_shutdown_error:
            hm_sliced.shutdown().get(timeout=30)
        assert type(public_shutdown_error.value) is RuntimeError
        assert str(public_shutdown_error.value) == str(raw_shutdown_error.value)

        assert hm_sliced._inner_host_mesh is sliced_inner


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_shutdown_unpickled_host_mesh_throws_exception() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        hm = state.hosts
        hm.initialized.get()
        hm_unpickled = cloudpickle.loads(cloudpickle.dumps(hm))
        with pytest.raises(RuntimeError):
            hm_unpickled.shutdown().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_stop_and_reconnect() -> None:
    # Use a short message delivery timeout so we don't have to sleep 35 s
    # waiting for undeliverable-message errors to surface.
    with configured(message_delivery_timeout="5s"):
        job = ProcessJob({"hosts": 2})
        try:
            # First connection: spawn actors, verify they work.
            with scoped_state(job, cached_path=None) as state:
                hm = state.hosts
                pm = hm.spawn_procs(per_host={"gpus": 1})
                am = pm.spawn("actor", RankActor)
                pids = am.get_pid.call().get(timeout=30)
                assert len(pids) == 2
                pids = [pid for _, pid in pids.items()]

                order: list[str] = []
                call_through = _install_host_teardown_call_through(hm, order)
                flush_pending_spawns = hm._flush_pending_spawns

                async def record_drain() -> None:
                    order.append("drain")
                    await flush_pending_spawns()

                with patch.object(hm, "_flush_pending_spawns", record_drain):
                    # Stop terminates user procs but leaves workers available.
                    assert hm.stop().get(timeout=30) is None
                    assert order == ["drain", "native_stop"]
                    assert call_through.calls == ["stop"]
                    assert all(not is_process_running(pid) for pid in pids)

                    # Sleep past the message delivery timeout to ensure that no
                    # errors surface from undeliverable messages to dead
                    # procs/actors.
                    time.sleep(7)

                    # Reconnect after stop and use an actor before exercising
                    # any repeated-teardown behavior. This remains the ordinary
                    # stop-and-reconnect regression witness.
                    hm2 = job.state(cached_path=None).hosts
                    pm2 = hm2.spawn_procs(per_host={"gpus": 1})
                    am2 = pm2.spawn("actor", RankActor)
                    ranks2 = list(am2.get_rank.call().get(timeout=30).items())
                    assert len(ranks2) == 2

                    # The second stop and following shutdown both report
                    # success after the first stop consumed the native cell.
                    assert hm.stop().get(timeout=30) is None
                    assert hm.shutdown().get(timeout=30) is None
                    assert hm._inner_host_mesh is None

                    assert order == [
                        "drain",
                        "native_stop",
                        "drain",
                        "native_stop",
                        "drain",
                        "native_shutdown",
                    ]
                    assert call_through.calls == ["stop", "stop", "shutdown"]
                    # The call count proves that both stop requests reached the
                    # binding. Source inspection, not this seam, shows that only
                    # the first took the cell and ran the domain stop operation.
                    # It also shows that an overlapping second stop can return
                    # success while the first domain stop is still running;
                    # this sequential test does not exercise that overlap.

                    # This second call characterizes current empty-cell false
                    # success: the reported shutdown left the same workers and
                    # actor live. Revise this assertion when repeated teardown
                    # is corrected; the first call above remains the independent
                    # reconnect-after-stop regression.
                    assert list(am2.get_rank.call().get(timeout=30).items()) == ranks2

                # Shutdown: fully tear down and exit workers.
                hm2.shutdown().get(timeout=30)
        finally:
            job.kill()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_stop_only_drains_own_mesh_procs() -> None:
    """Stopping one mesh should not kill procs belonging to another mesh
    on the same workers.

    Simulates the real scenario where the main process and the mount
    process each have their own HostMesh (with different names) connected
    to the same remote workers.
    """
    job = ProcessJob({"hosts": 2})

    # First mesh: simulate the main process.
    with scoped_state(job, cached_path=None) as state1:
        hm1 = state1.hosts
        pm1 = hm1.spawn_procs(per_host={"gpus": 1}, name="proc1")
        am1 = pm1.spawn("actor", RankActor)
        pids1 = list(am1.get_pid.call().get().values())
        assert len(pids1) == 2

        # Second mesh: simulate the mount process reconnecting. This
        # creates a new HostMesh with a different name via a second
        # state() call on the same job.
        state2 = job.state(cached_path=None)
        hm2 = state2.hosts
        pm2 = hm2.spawn_procs(per_host={"gpus": 1}, name="proc2")
        am2 = pm2.spawn("actor", RankActor)
        pids2 = list(am2.get_pid.call().get().values())
        assert len(pids2) == 2

        # Stop the first mesh — should only drain its own procs.
        hm1.stop().get()

        # Procs from mesh1 should be stopped.
        assert all(not is_process_running(pid) for pid in pids1)

        # Procs from mesh2 should still be alive.
        assert all(is_process_running(pid) for pid in pids2)

        # Clean up mesh2.
        hm2.shutdown().get()


class PidActor(Actor):
    @endpoint
    def get_pid(self) -> int:
        return os.getpid()


class CpuAffinityActor(Actor):
    @endpoint
    def get_affinity(self) -> List[int]:
        return sorted(os.sched_getaffinity(0))


def _parse_cpu_list(s: str) -> Set[int]:
    """Parse kernel CPU-list format (e.g. "0-3,8-11") into a set of ints."""
    cpus = set()
    for part in s.strip().split(","):
        if "-" in part:
            lo, hi = part.split("-", 1)
            cpus.update(range(int(lo), int(hi) + 1))
        else:
            cpus.add(int(part))
    return cpus


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_this_host_on_client_can_spawn_actual_os_processes() -> None:
    hm = this_host()
    assert not hm.is_fake_in_process
    am = hm.spawn_procs(per_host={"gpus": 4}).spawn("actor", PidActor)
    pids = am.get_pid.call().get()
    for pid in pids.values():
        assert pid != os.getpid()
    assert len(set(pids.values())) == 4


@pytest.mark.timeout(60)
def test_controllers_have_same_pid_as_client() -> None:
    pid_controller = get_or_spawn_controller(
        "pid_test_controllers_have_same_pid_as_client", PidActor
    ).get()
    assert pid_controller.get_pid.call_one().get() == os.getpid()


class PidActorController(Actor):
    @endpoint
    def spawn_pid_actor(self) -> PidActor:
        return this_host().spawn_procs(per_host={"gpus": 4}).spawn("pid", PidActor)


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_this_host_on_controllers_can_spawn_actual_os_processes() -> None:
    pid_controller_0 = get_or_spawn_controller(
        "pid_test_this_host_on_controllers_0", PidActorController
    ).get()
    pid_controller_1 = get_or_spawn_controller(
        "pid_test_this_host_on_controllers_1", PidActorController
    ).get()
    pid_0 = pid_controller_0.spawn_pid_actor.call_one().get()
    pid_1 = pid_controller_1.spawn_pid_actor.call_one().get()
    pid_0_values = list(pid_0.get_pid.call().get().values())
    pid_1_values = list(pid_1.get_pid.call().get().values())
    assert pid_0_values != pid_1_values
    assert len(set(pid_0_values)) == 4
    assert len(set(pid_1_values)) == 4


@pytest.mark.timeout(60)
def test_root_client_does_not_leak_host_meshes() -> None:
    orig_get_client_context = _client_context.get
    with patch.object(_client_context, "get") as mock_get_client_context:
        mock_get_client_context.side_effect = orig_get_client_context

        def sync_sleep_then_context():
            time.sleep(0.1)
            context()

        threads = []
        for _ in range(100):
            t = threading.Thread(target=sync_sleep_then_context)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        assert mock_get_client_context.call_count == 100


@pytest.mark.timeout(60)
def test_spawn_procs_proc_bind_length_mismatch() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        with pytest.raises(
            ValueError,
            match=r"proc_bind length \(1\) must equal procs_per_host \(4\)",
        ):
            host.spawn_procs(
                per_host={"gpus": 4},
                proc_bind=[{"cpunodebind": "0"}],
            )


@pytest.mark.timeout(120)
def test_spawn_procs_with_numactl_bind() -> None:
    numa_node0 = pathlib.Path("/sys/devices/system/node/node0")
    if not numa_node0.exists():
        pytest.skip("NUMA node0 not available")
    if shutil.which("numactl") is None:
        pytest.skip("numactl binary not found")

    node0_cpus = _parse_cpu_list((numa_node0 / "cpulist").read_text())
    assert node0_cpus, "node0 has no CPUs"

    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(
            name="numa_bound",
            per_host={"gpus": 2},
            proc_bind=[{"cpunodebind": "0"}, {"cpunodebind": "0"}],
        )
        am = proc_mesh.spawn("affinity", CpuAffinityActor)
        affinities = am.get_affinity.call().get()
        for rank, cpus in affinities.items():
            cpu_set = set(cpus)
            assert cpu_set, f"rank {rank} has empty affinity"
            assert cpu_set <= node0_cpus, (
                f"rank {rank} affinity {cpu_set} is not a subset of node0 CPUs {node0_cpus}"
            )


@pytest.mark.timeout(120)
def test_spawn_procs_with_taskset_bind() -> None:
    if not hasattr(os, "sched_getaffinity"):
        pytest.skip("os.sched_getaffinity not available on this platform")
    available = sorted(os.sched_getaffinity(0))
    if len(available) < 2:
        pytest.skip("fewer than 2 CPUs available")

    cpu_a = available[0]
    cpu_b = available[1]

    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(
            name="taskset_bound",
            per_host={"gpus": 2},
            proc_bind=[{"cpus": str(cpu_a)}, {"cpus": str(cpu_b)}],
        )
        am = proc_mesh.spawn("affinity", CpuAffinityActor)
        affinities = am.get_affinity.call().get()
        observed = {frozenset(cpus) for cpus in affinities.values()}
        assert observed == {frozenset({cpu_a}), frozenset({cpu_b})}, (
            f"expected affinities {{{cpu_a}}} and {{{cpu_b}}}, got {affinities}"
        )


class WrapperMarkerActor(Actor):
    """Returns the PID stamped by the wrapper executable."""

    @endpoint
    def get_wrapper_pid(self) -> str:
        return os.environ.get("PYTHON_WRAPPER_PID", "")

    @endpoint
    def spawn_inner(self) -> "WrapperMarkerActor":
        """Spawns a new proc via this_host() to test recursive propagation."""
        return (
            this_host()
            .spawn_procs(per_host={"procs": 1})
            .spawn("inner_marker", WrapperMarkerActor)
        )


def _make_python_wrapper() -> str:
    """
    Write a shell wrapper that stamps PYTHON_WRAPPER_PID=$$ (the wrapper
    shell's PID) and execs to the real Python interpreter.

    Using the PID rather than a plain boolean means each wrapper invocation
    produces a *different* value.  The recursive test can therefore verify
    that the wrapper ran a second time (producing a different PID) rather than
    just inheriting the first PID from the parent process environment.
    """
    real_python = sys.executable
    tmpdir = tempfile.mkdtemp(prefix="monarch_test_python_exe_")
    wrapper_path = os.path.join(tmpdir, "python_wrapper")
    with open(wrapper_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                #!/bin/sh
                export PYTHON_WRAPPER_PID=$$
                exec {real_python} "$@"
                """
            )
        )
    os.chmod(wrapper_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP)
    return wrapper_path


class CudaVisibleDevicesActor(Actor):
    @endpoint
    def get_cuda_visible_devices(self) -> str:
        return os.environ.get("CUDA_VISIBLE_DEVICES", "")


def test_get_bootstrap_args_includes_parent_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from monarch._src.actor.proc_mesh import _get_bootstrap_args

    monkeypatch.setenv("MONARCH_BOOTSTRAP_ENV_TEST", "present")

    _, _, env = _get_bootstrap_args()

    assert env["MONARCH_BOOTSTRAP_ENV_TEST"] == "present"


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_spawn_procs_with_bootstrap_command() -> None:
    from monarch._src.actor.host_mesh import default_bootstrap_cmd

    hm = this_host()

    # Use with_env to customize the bootstrap command
    base = default_bootstrap_cmd()
    cmd = base.with_env({"CUDA_VISIBLE_DEVICES": "3"})

    pm = hm.spawn_procs(per_host={"gpus": 1}, bootstrap_command=cmd)
    am = pm.spawn("cuda_marker", CudaVisibleDevicesActor)
    assert am.get_cuda_visible_devices.call_one().get() == "3"

    # A second spawn without bootstrap_command must not inherit the previous
    # spawn's env, confirming that with_env is non-mutating on the HostMesh.
    pm_no_env = hm.spawn_procs(per_host={"gpus": 1})
    am_no_env = pm_no_env.spawn("cuda_marker_default", CudaVisibleDevicesActor)
    assert am_no_env.get_cuda_visible_devices.call_one().get() != "3"


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_spawn_procs_with_bootstrap_command_callable() -> None:
    from monarch._rust_bindings.monarch_hyperactor.host_mesh import BootstrapCommand
    from monarch._src.actor.host_mesh import default_bootstrap_cmd

    hm = this_host()

    def per_rank_bootstrap(point: Point) -> BootstrapCommand:
        base = default_bootstrap_cmd()
        return base.with_env({"CUDA_VISIBLE_DEVICES": str(point["gpus"])})

    pm = hm.spawn_procs(per_host={"gpus": 2}, bootstrap_command=per_rank_bootstrap)
    am = pm.spawn("cuda_per_rank", CudaVisibleDevicesActor)
    observed = sorted(am.get_cuda_visible_devices.call().get().values())
    assert observed == ["0", "1"]


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_with_python_executable() -> None:
    wrapper_path = _make_python_wrapper()
    modified_host = this_host().with_python_executable(wrapper_path)

    # Direct spawn: proc should be launched via the wrapper; wrapper PID is set.
    am = modified_host.spawn_procs(per_host={"procs": 1}).spawn(
        "marker", WrapperMarkerActor
    )
    direct_pid = am.get_wrapper_pid.call_one().get()
    assert direct_pid != "", "wrapper was not executed for direct spawn"

    # Recursive spawn: actor calls this_host().spawn_procs() and the inner proc
    # should also be launched via the wrapper — proving the bootstrap command is
    # stored on the HostMeshRef and propagated.  The wrapper PID must differ
    # from the direct-spawn PID, ruling out simple env-var inheritance.
    inner_am = am.spawn_inner.call_one().get()
    inner_pid = inner_am.get_wrapper_pid.call_one().get()
    assert inner_pid != "", "wrapper was not executed for recursive spawn"
    assert inner_pid != direct_pid, (
        "recursive spawn inherited PID instead of re-running wrapper"
    )


class DuplexProcessJob(ProcessJob):
    """ProcessJob that also exposes a duplex socket on the first host.

    The duplex address is available via ``duplex_addr`` after calling
    ``state()``.  Pass this value to ``attach(...)`` before the
    first ``context()`` call so the client bootstraps by attaching to
    the worker.
    """

    def __init__(
        self,
        meshes: Optional[Dict[str, int]] = None,
        env: Optional[Dict[str, str]] = None,
        service_proc_id: Optional[ProcId] = None,
    ) -> None:
        super().__init__(meshes or {"hosts": 1}, env)
        self._duplex_addr: Optional[str] = None
        self._service_proc_id = service_proc_id

    def _create(self, client_script: Optional[str]) -> None:
        if client_script is not None:
            raise RuntimeError("DuplexProcessJob cannot run batch-mode scripts")

        self._tmpdir = tempfile.mkdtemp(prefix="monarch_duplex_job_")

        for mesh_name, count in self._meshes.items():
            for i in range(count):
                host_key = f"{mesh_name}_{i}"
                addr = f"ipc://{self._tmpdir}/{host_key}"
                service_proc_id = self._service_proc_id or new_service_proc_id()
                proc_addr = service_proc_addr(addr, service_proc_id)
                worker_env = {**os.environ, "HYPERACTOR_PROCESS_NAME": host_key}
                if self._env is not None:
                    worker_env.update(self._env)

                cmd = [
                    sys.executable,
                    "-c",
                    "from monarch.actor import run_worker_loop_forever; "
                    f"run_worker_loop_forever(address={proc_addr!r}, "
                    'ca="trust_all_connections")',
                ]
                proc = subprocess.Popen(cmd, env=worker_env, start_new_session=True)
                self._host_to_pid[host_key] = ProcessState(proc.pid, proc_addr)

        # Wait for the first worker's frontend socket to appear.
        # The duplex server is now on the same address as the frontend.
        first_key = f"{next(iter(self._meshes))}_0"
        assert self._tmpdir is not None
        sock_path = os.path.join(self._tmpdir, first_key)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if os.path.exists(sock_path):
                break
            time.sleep(0.05)
        else:
            self._kill()
            raise RuntimeError("frontend socket did not appear in time")
        # The duplex addr is the first worker's frontend (they share
        # the same address now). Use ipc:// format for zmq URL parsing.
        self._duplex_addr = f"ipc://{sock_path}"

    @property
    def duplex_addr(self) -> str:
        """Native-format ChannelAddr for the first worker's frontend/duplex address."""
        if self._duplex_addr is None:
            raise RuntimeError("call apply() first to start the worker")
        return self._duplex_addr

    @property
    def frontend_socket_path(self) -> str:
        """Filesystem path of the first worker's frontend Unix socket."""
        tmpdir = self._tmpdir
        if tmpdir is None:
            raise RuntimeError("call apply() first to start the worker")
        first_key = f"{next(iter(self._meshes))}_0"
        return os.path.join(tmpdir, first_key)


class EchoActor(Actor):
    """Replies with the message it receives."""

    @endpoint
    async def echo(self, msg: str) -> str:
        return msg

    @endpoint
    async def getenv(self, name: str) -> Optional[str]:
        return os.environ.get(name)


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_client_attach_addr() -> None:
    """Verify ``attach(...)`` causes the client to bootstrap by
    attaching to the worker, and that subsequent mesh operations route
    exclusively over the duplex channel.

    After bootstrapping and spawning procs we remove the worker's frontend
    socket from the filesystem.  Any subsequent client-side messaging that
    accidentally tries to dial the frontend will fail, proving that
    endpoint calls, replies, and undeliverable bounces travel exclusively
    over the duplex channel.
    """
    job = DuplexProcessJob()
    # Start the worker (so its duplex address exists) and attach the
    # client to it BEFORE anything bootstraps the client context —
    # `scoped_state(job)` connects via `attach_to_workers`, which would
    # otherwise bootstrap the client unattached.
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None) as state:
        context()

        # Remove the worker's frontend socket.  The client should
        # never need to dial the frontend — all communication
        # flows through the duplex channel.  If anything on the
        # client side accidentally tries to connect to the
        # frontend, it will fail.
        frontend = job.frontend_socket_path
        assert os.path.exists(frontend)
        os.remove(frontend)
        assert not os.path.exists(frontend)

        # (a) Spawn HostMesh, ProcMesh, and ActorMesh.
        hm = state.hosts
        assert hm is not None

        pm = hm.spawn_procs(per_host={"workers": 2})
        assert pm is not None

        am = pm.spawn("echo", EchoActor)

        # (b) Send messages to the actor mesh.
        results = am.echo.call("hello").get()

        # (c) Get replies back.
        for val in results.values():
            assert val == "hello"


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_client_attach_addr_this_host_and_this_proc() -> None:
    """After bootstrapping with ``attach(...)``, ``this_host()``
    returns the host-local mesh on this machine (not the attached host)
    and ``this_proc()`` returns the local client proc whose gateway is
    now connected to the remote gateway. The local client procs remain
    on this machine; the remote gateway holds routes back to them
    through the via duplex. ``this_host()`` keeps its ``"on this
    machine"`` meaning so that ``this_host().spawn_procs()`` always
    spawns local procs; to drive procs on the attached host, use the
    separate ``HostMesh`` returned by the job."""
    job = DuplexProcessJob()
    # Attach the client to the worker before any client bootstrap (see
    # test_client_attach_addr).
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None):
        context()

        proc = this_proc()
        assert proc is not None
        host = this_host()
        assert host is not None
        assert host.region == proc.host_mesh.region

        # Verify the meshes are usable by spawning an actor.
        am = proc.spawn("echo2", EchoActor)
        result = am.echo.call_one("ping").get()
        assert result == "ping"


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_client_attach_service_singleton_spawns_on_client_host() -> None:
    worker_marker = "MONARCH_TEST_ATTACHED_WORKER"
    assert worker_marker not in os.environ
    job = DuplexProcessJob(
        env={worker_marker: "1"},
        service_proc_id=ProcId.from_string("service"),
    )
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None):
        context()

        actor = (
            this_host()
            .spawn_procs(per_host={"local": 1})
            .spawn("local_origin", EchoActor)
        )

        assert actor.getenv.call_one(worker_marker).get() is None


class RelayActor(Actor):
    """Forwards a call to a target actor; used to test reverse reachability."""

    def __init__(self, target):
        self._target = target

    @endpoint
    async def relay(self, msg: str) -> str:
        return await self._target.echo.call_one(msg)


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_client_attach_service_instance_spawns_on_client_host() -> None:
    worker_marker = "MONARCH_TEST_ATTACHED_WORKER"
    assert worker_marker not in os.environ
    job = DuplexProcessJob(
        env={worker_marker: "1"},
        service_proc_id=ProcId(Uid.instance("service")),
    )
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None):
        context()

        local_echo = (
            this_host()
            .spawn_procs(per_host={"local": 1})
            .spawn("local_echo", EchoActor)
        )
        assert local_echo.getenv.call_one(worker_marker).get() is None


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_client_attach_addr_this_host_spawns_locally() -> None:
    """After attaching, ``this_host().spawn_procs()`` spawns procs on
    this machine.  Procs spawned this way are reachable from the
    client (forward) and from procs running on the attached host
    (reverse), demonstrating that addresses route both ways."""
    job = DuplexProcessJob()
    # Attach the client to the worker before any client bootstrap (see
    # test_client_attach_addr).
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None) as state:
        context()

        # this_host() spawns procs on this machine, not on the
        # attached host.
        local_pm = this_host().spawn_procs(per_host={"local": 1})
        local_echo = local_pm.spawn("local_echo", EchoActor)

        # client -> local procs reachable.
        assert local_echo.echo.call_one("from-client").get() == "from-client"

        # remote -> local procs reachable: a relay actor on the
        # attached job's host calls back into the local actor
        # mesh.
        remote_pm = state.hosts.spawn_procs(per_host={"remote": 1})
        relay = remote_pm.spawn("relay", RelayActor, local_echo)
        assert relay.relay.call_one("through-remote").get() == "through-remote"
