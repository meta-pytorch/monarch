# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import contextlib
import ctypes
import datetime
import importlib.resources
import os
import re
import subprocess
import sys
import time
from typing import Callable, cast, Optional

import monarch.actor
import pytest
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import hold_gil_for_test
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch._src.actor.actor_mesh import ActorMesh, context
from monarch._src.actor.host_mesh import fake_in_process_host, this_host
from monarch._src.actor.proc_mesh import ProcMesh
from monarch.actor import Actor, ActorError, endpoint, MeshFailure
from monarch.config import configured, parametrize_config


@contextlib.contextmanager
def override_fault_hook(callback=None):
    original_hook = monarch.actor.unhandled_fault_hook
    try:
        monarch.actor.unhandled_fault_hook = callback or (lambda failure: None)
        yield
    finally:
        monarch.actor.unhandled_fault_hook = original_hook


class ExceptionActor(Actor):
    def __init__(self, except_on_init=False) -> None:
        if except_on_init:
            raise Exception("This is an exception from __init__")
        pass

    @endpoint
    async def noop(self) -> None:
        pass

    @endpoint
    async def raise_exception(self) -> None:
        raise Exception("This is a test exception")

    @endpoint
    async def print_value(self, value) -> None:
        """Endpoint that takes a value and prints it."""
        print(f"Value received: {value}")
        return value


class ExceptionActorSync(Actor):
    def __init__(self, except_on_init=False) -> None:
        if except_on_init:
            raise Exception("This is an exception from __init__")
        pass

    @endpoint
    def raise_exception(self) -> None:
        raise Exception("This is a test exception")

    @endpoint
    def noop(self) -> None:
        pass


class NestedExceptionActor(Actor):
    @endpoint
    async def raise_exception_with_context(self) -> None:
        try:
            raise Exception("Inner exception")
        except Exception:
            # Don't use from here to set __context__ instead of __cause__
            raise Exception("Outer exception")

    @endpoint
    async def raise_exception_with_cause(self) -> None:
        try:
            raise Exception("Inner exception")
        except Exception as e:
            # Use from here to set __cause__ instead of __context__
            raise Exception("Outer exception") from e


class BrokenPickleClass:
    """A class that can be configured to raise exceptions during pickling/unpickling."""

    def __init__(
        self,
        raise_on_getstate=False,
        raise_on_setstate=False,
        exception_message="Pickle error",
    ):
        self.raise_on_getstate = raise_on_getstate
        self.raise_on_setstate = raise_on_setstate
        self.exception_message = exception_message
        self.value = "test_value"

    def __getstate__(self):
        """Called when pickling the object."""
        if self.raise_on_getstate:
            raise RuntimeError(f"__getstate__ error: {self.exception_message}")
        return {
            "raise_on_getstate": self.raise_on_getstate,
            "raise_on_setstate": self.raise_on_setstate,
            "exception_message": self.exception_message,
            "value": self.value,
        }

    def __setstate__(self, state):
        """Called when unpickling the object."""
        if state.get("raise_on_setstate", False):
            raise RuntimeError(
                f"__setstate__ error: {state.get('exception_message', 'Unpickle error')}"
            )
        self.__dict__.update(state)


def spawn_procs_on_fake_host(per_host: dict[str, int]) -> ProcMesh:
    return fake_in_process_host().spawn_procs(per_host)


def spawn_procs_on_this_host(per_host: dict[str, int]) -> ProcMesh:
    return this_host().spawn_procs(per_host)


@parametrize_config(actor_queue_dispatch={True, False})
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "distributed_proc_mesh"],
)
@pytest.mark.parametrize(
    "actor_class",
    [ExceptionActor, ExceptionActorSync],
)
@pytest.mark.parametrize("num_procs", [1, 2])
async def test_actor_exception(mesh, actor_class, num_procs) -> None:
    """
    Test that exceptions raised in actor endpoints are propagated to the client.
    """
    proc = mesh({"gpus": num_procs})
    exception_actor = proc.spawn("exception_actor", actor_class)

    with pytest.raises(ActorError, match="This is a test exception"):
        if num_procs == 1:
            await exception_actor.raise_exception.call_one()
        else:
            await exception_actor.raise_exception.call()


@parametrize_config(actor_queue_dispatch={True, False})
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "distributed_proc_mesh"],
)
@pytest.mark.parametrize(
    "actor_class",
    [ExceptionActor, ExceptionActorSync],
)
@pytest.mark.parametrize("num_procs", [1, 2])
def test_actor_exception_sync(mesh, actor_class, num_procs) -> None:
    """
    Test that exceptions raised in actor endpoints are propagated to the client.
    """
    proc = mesh({"gpus": num_procs})
    exception_actor = proc.spawn("exception_actor", actor_class)

    with pytest.raises(ActorError, match="This is a test exception"):
        if num_procs == 1:
            exception_actor.raise_exception.call_one().get()
        else:
            exception_actor.raise_exception.call().get()


@parametrize_config(
    # In queue dispatch mode, __init__ exceptions become supervision errors
    actor_queue_dispatch={
        False,
    }
)
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "distributed_proc_mesh"],
)
@pytest.mark.parametrize(
    "actor_class",
    [ExceptionActor, ExceptionActorSync],
)
@pytest.mark.parametrize("num_procs", [1, 2])
async def test_actor_init_exception_buggered(mesh, actor_class, num_procs) -> None:
    """
    Test that exceptions raised in actor initializers are propagated to the client.
    The correct behavior here should be to raise a supervision exception.
    """
    proc = mesh({"gpus": num_procs})

    # The exception in the constructor will be an unhandled fault by default,
    # override this to examine the normal behavior.
    with override_fault_hook():
        exception_actor = proc.spawn(
            "exception_actor", actor_class, except_on_init=True
        )
        with pytest.raises(ActorError, match="This is an exception from __init__"):
            if num_procs == 1:
                await exception_actor.noop.call_one()
            else:
                await exception_actor.noop.call()
        await asyncio.sleep(3)


@parametrize_config(
    # In queue dispatch mode, __init__ exceptions become supervision errors
    actor_queue_dispatch={
        False,
    }
)
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "distributed_proc_mesh"],
)
@pytest.mark.parametrize(
    "actor_class",
    [ExceptionActor, ExceptionActorSync],
)
@pytest.mark.parametrize("num_procs", [1, 2])
def test_actor_init_exception_sync_buggered(mesh, actor_class, num_procs) -> None:
    """
    Test that exceptions raised in actor initializers are propagated to the client.
    The correct behavior here should be to raise a supervision exception.
    """
    proc = mesh({"gpus": num_procs})

    # Same reason as the async test variant.
    with override_fault_hook():
        exception_actor = proc.spawn(
            "exception_actor", actor_class, except_on_init=True
        )
        with pytest.raises(ActorError, match="This is an exception from __init__"):
            if num_procs == 1:
                exception_actor.noop.call_one().get()
            else:
                exception_actor.noop.call().get()
        time.sleep(3)


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True})
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "distributed_proc_mesh"],
)
@pytest.mark.parametrize(
    "actor_class",
    [ExceptionActor, ExceptionActorSync],
)
@pytest.mark.parametrize("num_procs", [1, 2])
async def test_actor_init_exception(mesh, actor_class, num_procs) -> None:
    """
    Test that exceptions raised in actor initializers are propagated as supervision faults.
    In queue dispatch mode, __init__ exceptions become supervision errors delivered to the
    unhandled_fault_hook.
    """
    faults = []
    faulted = asyncio.Event()

    def fault_hook(failure):
        faults.append(failure)
        if len(faults) >= num_procs:
            faulted.set()

    with override_fault_hook(fault_hook):
        proc = mesh({"gpus": num_procs})
        proc.spawn("exception_actor", actor_class, except_on_init=True)

        # Wait for the faults to arrive at the hook
        await asyncio.wait_for(faulted.wait(), timeout=15.0)

    # Verify the fault was received
    assert len(faults) >= num_procs, f"Expected {num_procs} faults, got {len(faults)}"
    for fault in faults:
        fault_str = str(fault)
        assert "exception_actor" in fault_str
        assert "This is an exception from __init__" in fault_str


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True})
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "distributed_proc_mesh"],
)
@pytest.mark.parametrize(
    "actor_class",
    [ExceptionActor, ExceptionActorSync],
)
@pytest.mark.parametrize("num_procs", [1, 2])
def test_actor_init_exception_sync(mesh, actor_class, num_procs) -> None:
    """
    Test that exceptions raised in actor initializers are propagated as supervision faults.
    In queue dispatch mode, __init__ exceptions become supervision errors delivered to the
    unhandled_fault_hook.
    """
    import threading

    faults = []
    faulted = threading.Event()

    def fault_hook(failure):
        faults.append(failure)
        if len(faults) >= num_procs:
            faulted.set()

    with override_fault_hook(fault_hook):
        proc = mesh({"gpus": num_procs})
        proc.spawn("exception_actor", actor_class, except_on_init=True)

        # Wait for the faults to arrive at the hook
        assert faulted.wait(timeout=15.0), "Timed out waiting for faults"

    # Verify the fault was received
    assert len(faults) >= num_procs, f"Expected {num_procs} faults, got {len(faults)}"
    for fault in faults:
        fault_str = str(fault)
        assert "exception_actor" in fault_str
        assert "This is an exception from __init__" in fault_str


@parametrize_config(actor_queue_dispatch={True, False})
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "distributed_proc_mesh"],
)
async def test_actor_error_message(mesh) -> None:
    """
    Test that exceptions raised in actor endpoints capture nested exceptions.
    """
    proc = mesh({"gpus": 2})
    exception_actor = proc.spawn("exception_actor", NestedExceptionActor)

    with pytest.raises(ActorError) as exc_info:
        await exception_actor.raise_exception_with_cause.call()

    # Make sure both exception messages are present in the message.
    assert "Inner exception" in str(exc_info.value)
    assert "Outer exception" in str(exc_info.value)
    # Make sure the "cause" is set.
    assert "The above exception was the direct cause of the following exception" in str(
        exc_info.value
    )

    with pytest.raises(ActorError) as exc_info:
        await exception_actor.raise_exception_with_context.call()

    # Make sure both exception messages are present in the message.
    assert "Inner exception" in str(exc_info.value)
    assert "Outer exception" in str(exc_info.value)
    # Make sure the "cause" is set.
    assert "During handling of the above exception, another exception occurred" in str(
        exc_info.value
    )


'''
# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
@pytest.mark.oss_skip
@pytest.mark.parametrize("num_procs", [1, 2])
@pytest.mark.parametrize("sync_endpoint", [False, True])
@pytest.mark.parametrize("sync_test_impl", [False, True])
@pytest.mark.parametrize("endpoint_name", ["cause_segfault", "cause_panic"])
@pytest.mark.parametrize("api_ver", [ApiVersion.V0, ApiVersion.V1])
def test_actor_supervision(num_procs, sync_endpoint, sync_test_impl, endpoint_name, api_ver):
    """
    Test that an endpoint causing spontaenous process exit is handled by the supervisor.

    Today, these events are delivered to the client and cause the client process
    to exit with a non-zero code, so the only way we can test it is via a
    subprocess harness.
    """
    # Run the segfault test in a subprocess
    test_bin = importlib.resources.files("monarch.python.tests").joinpath("test_bin")
    cmd = [
        str(test_bin),
        "error-endpoint",
        f"--num-procs={num_procs}",
        f"--sync-endpoint={sync_endpoint}",
        f"--sync-test-impl={sync_test_impl}",
        f"--endpoint-name={endpoint_name}",
        f"--v1={api_ver == ApiVersion.V1}",
    ]
    try:
        print("running cmd", " ".join(cmd))
        process = subprocess.run(cmd, capture_output=True, timeout=180)
    except subprocess.TimeoutExpired as e:
        print("timeout expired")
        if e.stdout is not None:
            print(e.stdout.decode())
        if e.stderr is not None:
            print(e.stderr.decode())
        raise

    # Assert that the subprocess exited with a non-zero code
    assert "Started function error_test" in process.stdout.decode()
    assert (>
        process.returncode != 0
    ), f"Expected non-zero exit code, got {process.returncode}"
'''


# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
@pytest.mark.oss_skip
def test_proc_mesh_bootstrap_error():
    """
    Test that attempts to spawn a ProcMesh with a failure during bootstrap.
    """
    # Run the segfault test in a subprocess
    test_bin = importlib.resources.files("monarch.python.tests").joinpath("test_bin")
    cmd = [str(test_bin), "error-bootstrap"]
    try:
        print("running cmd", " ".join(cmd))
        process = subprocess.run(cmd, capture_output=True, timeout=180)
    except subprocess.TimeoutExpired as e:
        print("timeout expired")
        if e.stdout is not None:
            print(e.stdout.decode())
        if e.stderr is not None:
            print(e.stderr.decode())
        raise

    # Assert that the subprocess exited with a non-zero code
    assert "Started function error_bootstrap" in process.stdout.decode()
    assert process.returncode != 0, (
        f"Expected non-zero exit code, got {process.returncode}"
    )


@pytest.mark.oss_skip
@parametrize_config(actor_queue_dispatch={True, False})
@pytest.mark.parametrize("raise_on_getstate", [True, False])
@pytest.mark.parametrize("raise_on_setstate", [True, False])
@pytest.mark.parametrize("num_procs", [1, 2])
async def test_broken_pickle_class(
    raise_on_getstate, raise_on_setstate, num_procs
) -> None:
    """
    Test that exceptions during pickling/unpickling are properly handled.

    This test creates a BrokenPickleClass instance configured to raise exceptions
    during __getstate__ and/or __setstate__, then passes it to an ExceptionActor's
    print_value endpoint and verifies that an ActorError is raised.
    """
    if not raise_on_getstate and not raise_on_setstate:
        # Pass this test trivially
        return

    proc = spawn_procs_on_this_host({"gpus": num_procs})
    exception_actor = proc.spawn("exception_actor", ExceptionActor)

    # Create a BrokenPickleClass instance configured to raise exceptions
    broken_obj = BrokenPickleClass(
        raise_on_getstate=raise_on_getstate,
        raise_on_setstate=raise_on_setstate,
        exception_message="Test pickle error",
    )

    # On the getstate path, we expect a RuntimeError to be raised locally.
    # On the setstate path, we expect an ActorError to be raised remotely.
    error_type = RuntimeError if raise_on_getstate else ActorError
    error_pattern = "__getstate__ error" if raise_on_getstate else "__setstate__ error"

    with pytest.raises(error_type, match=error_pattern):
        if num_procs == 1:
            await exception_actor.print_value.call_one(broken_obj)
        else:
            await exception_actor.print_value.call(broken_obj)


"""
# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
@pytest.mark.oss_skip
async def test_exception_after_wait_unmonitored():
    # Run the test in a subprocess
    test_bin = importlib.resources.files("monarch.python.tests").joinpath("test_bin")
    cmd = [
        str(test_bin),
        "error-unmonitored",
        "--v1=False",
    ]
    try:
        print("running cmd", " ".join(cmd))
        process = subprocess.run(cmd, capture_output=True, timeout=180)
    except subprocess.TimeoutExpired as e:
        print("timeout expired")
        if e.stdout is not None:
            print(e.stdout.decode())
        if e.stderr is not None:
            print(e.stderr.decode())
        raise

    # Assert that the subprocess exited with a non-zero code
    assert "Started function _error_unmonitored" in process.stdout.decode()
    assert (
        process.returncode != 0
    ), f"Expected non-zero exit code, got {process.returncode}"
"""


# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
@pytest.mark.oss_skip
def test_python_actor_process_cleanup():
    """
    Test that PythonActor processes are cleaned up when the parent process dies.

    This test spawns an 8 process procmesh and calls an endpoint that returns a normal exception,
    then verifies that all spawned processes have been cleaned up after the spawned binary dies.
    """
    import os
    import signal
    import time

    # Run the error-cleanup test in a subprocess
    test_bin = importlib.resources.files("monarch.python.tests").joinpath("test_bin")
    cmd = [
        str(test_bin),
        "error-cleanup",
    ]

    try:
        print("running cmd", " ".join(cmd))
        process = subprocess.run(cmd, capture_output=True, timeout=180, text=True)
    except subprocess.TimeoutExpired as e:
        print("timeout expired")
        if e.stdout is not None:
            print(e.stdout.decode())
        if e.stderr is not None:
            print(e.stderr.decode())
        raise

    # Read stdout line by line to get child PIDs
    assert "Started function _error_cleanup() for parent process" in process.stdout

    child_pids = set()
    for line in process.stdout.splitlines():
        if line.startswith("CHILD_PIDS: "):
            pids_str = line[len("CHILD_PIDS: ") :]  # noqa
            child_pids = {
                int(pid.strip()) for pid in pids_str.split(",") if pid.strip()
            }
            print(f"Extracted child PIDs: {child_pids}")
            break

    if not child_pids:
        raise AssertionError("No child PIDs found in output")

    assert child_pids, "No child PIDs were collected from subprocess output"

    # Wait for child processes to be cleaned up
    print("Waiting for child processes to be cleaned up...")
    cleanup_timeout = 120
    start_time = time.time()

    def is_process_running(pid):
        """Check if a process with the given PID is still running."""
        try:
            os.kill(pid, 0)  # Signal 0 doesn't kill, just checks if process exists
            return True
        except OSError:
            return False

    still_running = set(child_pids)

    while time.time() - start_time < cleanup_timeout:
        if not still_running:
            print("All child processes have been cleaned up!")
            return

        still_running = {pid for pid in still_running if is_process_running(pid)}

        print(f"Still running child PIDs: {still_running}")
        time.sleep(2)

    # If we get here, some processes are still running
    # Try to clean up remaining processes
    for pid in still_running:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    raise AssertionError(
        f"Child processes not cleaned up after {cleanup_timeout}s: {still_running}"
    )


class ActorFailureError(BaseException):
    """Exception to simulate actor failure for supervision testing.

    Inherits from BaseException in order that supervision be
    triggered.

    """

    pass


class SyncErrorActor(Actor):
    @endpoint
    def fail_with_supervision_error(self) -> None:
        raise ActorFailureError("Simulated actor failure for supervision testing")

    @endpoint
    def check(self) -> str:
        return "this is a healthy check"

    @endpoint
    def check_with_exception(self) -> None:
        raise RuntimeError("failed the check with app error")

    @endpoint
    def get_pid(self) -> int:
        return os.getpid()

    @endpoint
    def exit_process(self) -> None:
        # Don't use sys.exit, as it raises a SystemExit exception, which would
        # be a separate error path that can be handled.
        os._exit(1)


class ErrorActor(Actor):
    @endpoint
    async def fail_with_supervision_error(self) -> None:
        raise ActorFailureError("Simulated actor failure for supervision testing")

    @endpoint
    async def check(self, i: int | None = None) -> str:
        if i is not None:
            print(f"---start checking: {i}")
            await asyncio.sleep(5)
            print(f"---end checking: {i}")
        return "this is a healthy check"

    @endpoint
    async def check_with_exception(self) -> None:
        raise RuntimeError("failed the check with app error")

    @endpoint
    async def get_pid(self) -> int:
        return os.getpid()

    @endpoint
    async def exit_process(self) -> None:
        # Same reason as SyncErrorActor.exit_process
        os._exit(1)


class Worker(Actor):
    @endpoint
    def work(self):
        raise ValueError("value error")


class Manager(Actor):
    @endpoint
    async def init(self):
        mesh = spawn_procs_on_this_host({"gpus": 1})
        self.workers = mesh.spawn("Worker", Worker)

    @endpoint
    async def route(self):
        return await self.workers.work.call_one()


@parametrize_config(actor_queue_dispatch={True, False})
async def test_errors_propagated() -> None:
    p_mesh = spawn_procs_on_this_host({"gpus": 1})
    mesh = p_mesh.spawn("manager", Manager)

    await mesh.init.call_one()

    with pytest.raises(ActorError) as err_info:
        await mesh.route.call_one()
    assert "value error" in str(err_info.value)


@pytest.mark.oss_skip
@pytest.mark.timeout(30)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_actor_mesh_supervision_handling() -> None:
    # This test doesn't want the client process to crash during testing.
    with override_fault_hook():
        proc = spawn_procs_on_this_host({"gpus": 1})

        e = proc.spawn("error", ErrorActor)

        # first check() call should succeed
        await e.check.call()

        # throw an application error
        with pytest.raises(ActorError, match="failed the check with app error"):
            await e.check_with_exception.call()

        # actor mesh should still be healthy
        await e.check.call()
        print("before failure")

        # existing call should fail with supervision error
        with pytest.raises(
            SupervisionError,
            match=".*Actor .* exited because of the following reason",
        ):
            await e.fail_with_supervision_error.call_one()
        print("after failure")

        # new call should fail with check of health state of actor mesh
        with pytest.raises(
            RuntimeError,
            match="failure on mesh.*error.*with event: "
            "The actor.*ErrorActor error.* and all its descendants have failed|"
            "Actor.*error.*exited because of the following",
        ):
            await e.check.call()
        print("after subsequent endpoint call")

        with pytest.raises(RuntimeError, match="error spawning actor mesh"):
            await proc.spawn("ex", ExceptionActorSync).initialized


class HealthyActor(Actor):
    @endpoint
    async def check(self):
        return "this is a healthy check"

    @endpoint
    async def check_with_payload(self, payload: str):
        pass


class Intermediate(Actor):
    @endpoint
    async def init_proc_mesh(self):
        mesh = spawn_procs_on_this_host({"gpus": 1})
        self._error_actor = mesh.spawn("error", ErrorActor)
        self._healthy_actor = mesh.spawn("healthy", HealthyActor)

    @endpoint
    async def forward_success(self):
        return await self._error_actor.check.call()

    @endpoint
    async def forward_error(self):
        return await self._error_actor.fail_with_supervision_error.call_one()

    @endpoint
    async def forward_healthy_check(self):
        return await self._healthy_actor.check.call()

    def __supervise__(self, failure) -> bool:
        # Suppress this error from propagating further, the purpose of this actor
        # is to test the v0 supervision handling by creating individual exceptions.
        return True

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        # Errors delivering messages to error_actor can be ignored, we don't
        # want Intermediate to stop.
        return True


@pytest.mark.timeout(30)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_actor_mesh_supervision_handling_chained_error() -> None:
    proc = spawn_procs_on_this_host({"gpus": 1})

    intermediate_actor = proc.spawn("intermediate", Intermediate)
    await intermediate_actor.init_proc_mesh.call()

    # first forward() call should succeed
    await intermediate_actor.forward_success.call()
    await intermediate_actor.forward_healthy_check.call()

    # in a chain of client -> Intermediate -> ErrorActor, a supervision error
    # happening in ErrorActor will be captured by Intermediate and re-raised
    # as an application error (ActorError).
    with pytest.raises(
        ActorError,
        match=".*Actor .* exited because of the following reason",
    ):
        await intermediate_actor.forward_error.call()

    # calling success endpoint should fail with ActorError, but with supervision msg.
    with pytest.raises(
        ActorError,
        match="The actor.*Intermediate intermediate.*ErrorActor error.*and all its descendants have failed",
    ):
        await intermediate_actor.forward_success.call()

    # healthy actor should still be working
    await intermediate_actor.forward_healthy_check.call()


@parametrize_config(actor_queue_dispatch={True, False})
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "proc_mesh"],
)
@pytest.mark.parametrize(
    "error_actor_cls",
    [ErrorActor, SyncErrorActor],
)
async def test_base_exception_handling(mesh, error_actor_cls) -> None:
    """Test that BaseException subclasses trigger supervision errors.

    This test verifies that both synchronous and asynchronous methods
    that raise ActorFailureError (a BaseException subclass) trigger
    supervision errors properly.

    """
    # This test doesn't want the client process to crash during testing.
    with override_fault_hook():
        proc = mesh({"gpus": 1})
        error_actor = proc.spawn("error", error_actor_cls)

        # The call should raise a SupervisionError
        with pytest.raises(
            SupervisionError,
            match=".*Actor .* exited because of the following reason",
        ):
            await error_actor.fail_with_supervision_error.call_one()

        # Subsequent calls should fail with a health state error, including
        # the previous error that had occurred.
        with pytest.raises(
            RuntimeError,
            match="failure on mesh .*error.* at rank 0 with event:.*"
            "The actor .*ErrorActor error.*and all its descendants have failed.*"
            "|"
            "Actor .*error.*exited because of the following reason:"
            ".*ErrorActor error.*and all its descendants have failed",
        ):
            await error_actor.check.call()
        # The above check call is undeliverable and might get returned to the client
        # later on. We need to wait for it to be processed.
        await asyncio.sleep(5)


@parametrize_config(actor_queue_dispatch={True, False})
@pytest.mark.parametrize(
    "error_actor_cls",
    [ErrorActor, SyncErrorActor],
)
async def test_process_exit_handling(error_actor_cls) -> None:
    """Test that process exit triggers supervision errors.

    This test verifies that both synchronous and asynchronous methods
    that exit the process trigger supervision errors properly.
    """
    # This test doesn't want the client process to crash during testing.
    with override_fault_hook():
        # This test doesn't work with the fake local proc mesh, because it exits
        # the process.
        proc = spawn_procs_on_this_host({"gpus": 1})
        error_actor = proc.spawn("error", error_actor_cls)

        base_match = "Endpoint call {}\\(\\) failed, (Actor .*error.* exited because of the following reason|actor mesh is stopped due to proc mesh shutdown)"
        # The call should raise a SupervisionError
        with pytest.raises(
            SupervisionError,
            match=base_match.format("error.exit_process"),
        ):
            await error_actor.exit_process.call_one()

        # Subsequent calls should fail with a health state error
        with pytest.raises(
            RuntimeError,
            # Message changes depending on actor_queue_dispatch.
            match="failure on mesh .*error.* at rank 0 with event: "
            "The actor.*ErrorActor error.*was running on a process which and all its descendants have failed"
            "|"
            "Actor.*error.*exited because of the following reason",
        ):
            await error_actor.check.call()


class FaultActor(Actor):
    # This will dereference a null pointer and crash the process
    # This should also kill the ProcMeshAgent, rendering it unresponsive.
    # In this case, actor mesh should still return a SupervisionError,
    # and proc_mesh.stop() should still work, albeit it will do nothing
    # because all the processes should be dead already.
    @endpoint
    def sigsegv(self) -> None:
        ctypes.string_at(0)

    # Simple endpoint that should succeed.
    @endpoint
    def check(self) -> None:
        return None


@pytest.mark.timeout(180)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_sigsegv_handling():
    # This test doesn't want the client process to crash during testing.
    with override_fault_hook():
        hosts = this_host()
        procs = hosts.spawn_procs({"gpus": 2})
        actor = procs.spawn("fault", FaultActor)

        # Make sure the actor is healthy first
        await actor.check.call()

        with pytest.raises(
            SupervisionError,
            match="Actor .* and all its descendants have failed",
        ):
            await actor.sigsegv.call()

        # Check that a second call still fails and doesn't hang.
        with pytest.raises(
            RuntimeError,
            match="failure on mesh.*fault.*with event.*|"
            "Actor.*fault.*exited because of the following reason",
        ):
            await actor.check.call()

        # Check that proc_mesh.stop() still works, even though the processes are dead.
        # It should check for the procs status without trying to kill them again.
        await procs.stop()

        # Re-spawn on the same host mesh should work.
        procs = hosts.spawn_procs({"gpus": 2})
        actor = procs.spawn("fault", FaultActor)

        # Results don't matter, just make sure there's no exception.
        await actor.check.call()


@parametrize_config(actor_queue_dispatch={True, False})
@pytest.mark.parametrize(
    "mesh",
    [spawn_procs_on_fake_host, spawn_procs_on_this_host],
    ids=["local_proc_mesh", "proc_mesh"],
)
@pytest.mark.timeout(30)
async def test_supervision_with_proc_mesh_stopped(mesh) -> None:
    with override_fault_hook():
        proc = mesh({"gpus": 1})
        actor_mesh = proc.spawn("healthy", HealthyActor)

        await actor_mesh.check.call()

        await proc.stop()

        # new call should fail with check of health state of actor mesh
        # after the proc mesh is stopped, the actor mesh is also stopped, and
        # the ProcMeshAgent is no longer reachable.
        with pytest.raises(
            SupervisionError,
            match="Endpoint call healthy.check\\(\\) failed, Actor.*healthy.*is "
            "unhealthy with reason:.*timeout waiting for response from host mesh agent for"
            "|actor mesh is stopped due to proc mesh shutdown"
            "|The actor .* and all its descendants have failed",
        ):
            await actor_mesh.check.call()

        # proc mesh cannot spawn new actors anymore
        with pytest.raises(RuntimeError, match="`ProcMesh` has already been stopped"):
            await proc.spawn("immediate", Intermediate).initialized


@pytest.mark.timeout(120)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_actor_mesh_stop() -> None:
    class Printer(Actor):
        @endpoint
        async def print(self, content: str) -> None:
            print(f"{content}", flush=True)

    pm = this_host().spawn_procs(per_host={"gpus": 2})
    am_1 = pm.spawn("printer", Printer)
    am_2 = pm.spawn("printer2", Printer)
    await am_1.print.call("hello 1")
    await cast(ActorMesh, am_1).stop()

    # This will generate an undeliverable message from the client to the printer
    # actor, and thus would cause the client to hit unhandled_fault_hook. We
    # ignore that so we can make sure a case with an actor mesh ref would get
    # the right error message.
    with override_fault_hook():
        with pytest.raises(
            SupervisionError,
            match=r"(?s)Actor .*printer-.* exited because of the following reason:.*stopped",
        ):
            await am_1.print.call("hello 2")

        # An independent actor mesh should be fine.
        await am_2.print.call("hello 3")

        await pm.stop()


# TODO - re-enable after resolving T232206970
@pytest.mark.oss_skip
@pytest.mark.timeout(120)
async def test_supervision_with_sending_error() -> None:
    # This test doesn't want the client process to crash during testing.
    errors = []

    def fault_hook(failure):
        print(f"Fault hook called with {failure}")
        errors.append(str(failure))

    with override_fault_hook(fault_hook):
        with configured(
            # Messages of length > this will cause a send error and a returned
            # undeliverable.
            codec_max_frame_length=50000000,
            # Limit retries for sending before giving up.
            message_delivery_timeout="5sec",
        ):
            proc = spawn_procs_on_this_host({"gpus": 1})
            actor_mesh = proc.spawn("healthy", HealthyActor)

            await actor_mesh.check.call()

            # send a small payload to trigger success
            await actor_mesh.check_with_payload.call(payload="a")

            # The host mesh agent sends or the proc mesh agent sends might break.
            # Either case is an error that tells us that the send failed.
            error_msg_regx = (
                "Actor .* (is unhealthy with reason|exited because of the following reason)|"
                "actor mesh is stopped due to proc mesh shutdown"
            )

            # send a large payload to trigger send timeout error
            error_msg = (
                r"Endpoint call healthy\.check_with_payload\(\) failed, "
                + error_msg_regx
            )
            # The returned exception here is about a timeout reaching the host,
            # but it is actually a send error, and the remote actor is healthy.
            # The unhandled_fault_hook is called first with a real send error,
            # which would normally crash the process, tested below.
            with pytest.raises(SupervisionError, match=error_msg):
                await actor_mesh.check_with_payload.call(payload="a" * 55000000)

    # The global python actor __supervise__ hook should be called with the
    # failure containing the send error.
    assert len(errors) >= 1
    error_msg = errors[0]
    # Make sure the error contains a few key things:
    # * Message from client was undeliverable
    #   * destination could be multiple actors like agent, healthy, etc. because all pending messages are sent back
    # * Reason was the message was too big.
    # In the future, it would be even better to deliver back to the receiver
    # of the endpoint. That way, the endpoint itself could raise this exception,
    # rather than hitting a general undeliverable endpoint.
    # This would require Undeliverable to know about a specific return channel.
    assert "MeshFailure" in error_msg
    assert "RootClientActor" in error_msg
    assert re.search(
        "a message from .*client.*was undeliverable and returned",
        error_msg,
        flags=re.MULTILINE,
    )
    assert re.search(
        "rejecting oversize frame: len=[0-9]+ > max=50000000.*CODEC_MAX_FRAME_LENGTH",
        error_msg,
        flags=re.MULTILINE,
    )


@pytest.mark.timeout(30)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_slice_supervision() -> None:
    # This test doesn't want the client process to crash during testing.
    with override_fault_hook():
        pm = spawn_procs_on_this_host({"gpus": 4})
        healthy_mesh = pm.spawn("healthy", HealthyActor)
        error_mesh = pm.spawn("error", ErrorActor)
        slice_1 = error_mesh.slice(gpus=slice(2, 4))
        slice_2 = error_mesh.slice(gpus=2)
        slice_3 = error_mesh.slice(gpus=3)

        match = "Actor .* (is unhealthy with reason:|exited because of the following reason:)"
        print("before slice_3 fail")
        # Trigger supervision error on gpus=3
        with pytest.raises(SupervisionError, match=match):
            await slice_3.fail_with_supervision_error.call()

        print("before error_mesh.check")
        # Mesh containing all gpus is unhealthy
        with pytest.raises(SupervisionError, match=match):
            await error_mesh.check.call()

        print("before slice_3.check")
        # Slice containing only gpus=3 is unhealthy
        with pytest.raises(
            RuntimeError,
            match="failure on mesh.*error.*at rank 3 with event: The actor.*ErrorActor error.* and all its descendants have failed|"
            "Actor.*error.*exited because of the following",
        ):
            await slice_3.check.call()

        print("before slice_1.check")
        # Slice containing gpus=3 is unhealthy
        with pytest.raises(RuntimeError, match=match):
            await slice_1.check.call()

        print("before slice_2.check")
        # Slice not containing gpus=3 is healthy
        check = await slice_2.check.call()
        for _, item in check.items():
            assert item == "this is a healthy check"

        print("before healthy_mesh.check")
        # Other actor mesh on the same proc mesh is healthy
        check = await healthy_mesh.check.call()
        for _, item in check.items():
            assert item == "this is a healthy check"


@pytest.mark.timeout(30)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_mesh_slices_inherit_parent_errors() -> None:
    # This test doesn't want the client process to crash during testing.
    monarch.actor.unhandled_fault_hook = lambda failure: None
    pm = spawn_procs_on_this_host({"gpus": 4})
    error_mesh = pm.spawn("error", ErrorActor)
    slice_1 = error_mesh.slice(gpus=slice(2, 4))

    # Trigger supervision error on gpus=2, 3, 4
    with pytest.raises(SupervisionError):
        await slice_1.fail_with_supervision_error.call()

    # Newly created slice containing gpu=3 is unhealthy
    slice_2 = error_mesh.slice(gpus=3)
    with pytest.raises(SupervisionError):
        await slice_2.check.call()

    # Newly created slice containing gpu=1 is healthy
    slice_3 = error_mesh.slice(gpus=1)
    check = await slice_3.check.call()
    for _, item in check.items():
        assert item == "this is a healthy check"

    await pm.stop()


class ErrorActorWithSupervise(ErrorActor):
    def __init__(self, proc_mesh: ProcMesh, should_handle: bool = True) -> None:
        super().__init__()
        self.mesh = proc_mesh.spawn("error_actor", ErrorActor)
        self.failures = []
        self.faulted = asyncio.Event()
        self.should_handle = should_handle

    @endpoint
    async def self_fail(self) -> None:
        """Fails this actor with a graceful error that causes the actor to stop"""
        raise ActorFailureError("Simulated actor failure for supervision testing")

    @endpoint
    async def self_ungraceful_fail(self) -> None:
        """Fails this actor with an ungraceful error that runs no cleanup.
        The controller actor on this process will be unreachable, and users
        of any sub-actors owned by this one should observe that"""
        os._exit(1)

    @endpoint
    async def stop_subworker(self) -> None:
        """Stop the inner actor this one owns"""
        await self.mesh.stop()

    @endpoint
    async def subworker_fail(self, sleep: float | None = None) -> None:
        """Fails the sub-actors this actor owns"""
        await self.mesh.check.call()
        try:
            # This should cause a SupervisionError to get raised.
            await self.mesh.fail_with_supervision_error.call()
        except SupervisionError:
            # We suppress this because __supervise__ will get called, we want
            # to make sure to test the v1 API.
            if sleep is not None:
                await asyncio.sleep(sleep)
            return
        raise AssertionError(
            "Should never get here, SupervisionError should be raised by the above call"
        )

    @endpoint
    async def subworker_fail_on_mesh_ref(self, mesh: ErrorActor) -> None:
        # Failures on a passed in ref should go to the owning actor, *not* the
        # current actor.
        await mesh.check.call()
        try:
            # This should cause a SupervisionError to get raised.
            await mesh.fail_with_supervision_error.call()
        except SupervisionError:
            # We suppress this because __supervise__ will get called, we want
            # to make sure to test the v1 API.
            return
        raise AssertionError(
            "Should never get here, SupervisionError should be raised by the above call"
        )

    @endpoint
    async def get_mesh(self) -> ErrorActor:
        return self.mesh

    @endpoint
    async def subworker_broadcast_fail(self) -> None:
        self.mesh.check.broadcast()
        # When not awaiting the result of an endpoint which experiences a
        # failure, it should still propagate back to __supervise__.
        self.mesh.fail_with_supervision_error.broadcast()
        # Give time for the failure to occur before returning, so get_failures
        # will have a non-empty result.
        await asyncio.wait_for(self.faulted.wait(), timeout=30)

    @endpoint
    async def get_failures(self) -> list[str]:
        # MeshFailure is not picklable, so we convert it to a string.
        return [str(f) for f in self.failures]

    @endpoint
    async def kill_nest(self) -> None:
        pids = await self.mesh.get_pid.call()
        # Kill the actors directly, make sure we get an error.
        for _, pid in pids:
            os.kill(pid, 9)

    def __supervise__(self, failure: MeshFailure) -> bool:
        self.failures.append(failure)
        self.faulted.set()
        # Returning true suppresses the error.
        return self.should_handle


@pytest.mark.timeout(30)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_supervise_callback_handled():
    pm = spawn_procs_on_this_host({"gpus": 4})
    # TODO: When using the same proc mesh for both, it occasionally fails with:
    # RuntimeError: error while spawning actor error_actor_1v4bMhugCu1q: failed ranks: [0, 2, 3]
    second_mesh = spawn_procs_on_this_host({"gpus": 4})
    supervisor = pm.spawn("supervisor", ErrorActorWithSupervise, second_mesh)

    await supervisor.subworker_fail.call()
    result = await supervisor.get_failures.call()
    result = [f for _, f in result]
    assert len(result) == 4
    # We only need to check one of the 4 supervisor actors.
    r = result[0]
    # The nested mesh of actors also has 4 dimensions.
    assert len(r) == 4

    def check_message(rank):
        # Ensure that the error message has the actor id and the rank.
        assert "MeshFailure" in r[rank]
        assert f"rank={rank}" in r[rank]
        assert "error_actor" in r[rank]

    for i in range(len(r)):
        check_message(i)

    await pm.stop()


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_supervise_callback_without_await_handled():
    pm = spawn_procs_on_this_host({"gpus": 4})
    # TODO: When using the same proc mesh for both, it occasionally fails with:
    # RuntimeError: error while spawning actor error_actor_1v4bMhugCu1q: failed ranks: [0, 2, 3]
    second_mesh = spawn_procs_on_this_host({"gpus": 4})
    supervisor = pm.spawn("supervisor", ErrorActorWithSupervise, second_mesh)

    await supervisor.subworker_broadcast_fail.call()
    result = await supervisor.get_failures.call()
    result = [f for _, f in result]
    assert len(result) == 4
    # We only need to check one of the 4 supervisor actors.
    r = result[0]
    # The nested mesh of actors also has 4 dimensions.
    assert len(r) == 4

    def check_message(rank):
        # Ensure that the error message has the actor id and the rank.
        assert "MeshFailure" in r[rank]
        assert f"rank={rank}" in r[rank]
        assert "error_actor" in r[rank]

    for i in range(len(r)):
        check_message(i)

    await pm.stop()


@pytest.mark.timeout(30)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_supervise_callback_with_mesh_ref():
    # Ensure that supervision events go to the
    pm = spawn_procs_on_this_host({"gpus": 1})
    # TODO: When using the same proc mesh for both, it occasionally fails with:
    # RuntimeError: error while spawning actor error_actor_1v4bMhugCu1q: failed ranks: [0, 2, 3]
    second_mesh = spawn_procs_on_this_host({"gpus": 4})
    supervisor = pm.spawn("supervisor", ErrorActorWithSupervise, second_mesh)
    supervisor2 = pm.spawn("supervisor2", ErrorActorWithSupervise, second_mesh)
    error_actor_mesh = await supervisor2.get_mesh.call_one()

    # Call on a mesh that is not owned by that supervisor actor.
    await supervisor.subworker_fail_on_mesh_ref.call(error_actor_mesh)
    # Failures should go to supervisor2 since it's the owner of the failing mesh
    # ref. None should go to "supervisor" because its owned actors weren't used
    # and didn't fail.
    results1 = await supervisor.get_failures.call()
    results1 = [f for _, f in results1]
    # One supervisor, 0 events.
    assert len(results1) == 1
    assert len(results1[0]) == 0

    results2 = await supervisor2.get_failures.call()
    results2 = [f for _, f in results2]
    assert len(results2) == 1
    # We only need to check one of the supervisor actors.
    r = results2[0]
    # The nested mesh of actors also has 4 dimensions.
    assert len(r) == 4

    def check_message(rank):
        # Ensure that the error message has the actor id and the rank.
        assert "MeshFailure" in r[rank]
        assert f"rank={rank}" in r[rank]
        assert "error_actor" in r[rank]

    for i in range(len(r)):
        check_message(i)

    await pm.stop()


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_supervise_callback_when_procs_killed():
    pm = spawn_procs_on_this_host({"gpus": 1})
    second_mesh = spawn_procs_on_this_host({"gpus": 4})
    supervisor = pm.spawn("supervisor", ErrorActorWithSupervise, second_mesh)

    await supervisor.subworker_broadcast_fail.call()
    result = await supervisor.get_failures.call()
    result = [f for _, f in result]
    assert len(result) == 1
    result = result[0]
    # The nested mesh of actors also has 4 dimensions.
    assert len(result) == 4

    def check_message(rank):
        # Ensure that the error message has the actor id and the rank.
        assert "MeshFailure" in result[rank]
        assert f"rank={rank}" in result[rank]
        assert "error_actor" in result[rank]

    for i in range(len(result)):
        check_message(i)

    await pm.stop()


@pytest.mark.timeout(30)
@parametrize_config(actor_queue_dispatch={True, False})
async def test_supervise_callback_unhandled():
    # This test doesn't want the client process to crash during testing.
    monarch.actor.unhandled_fault_hook = lambda failure: None
    # This test handles none of the supervision errors, and ensures they make their
    # way back to the client.
    pm = spawn_procs_on_this_host({"gpus": 1})
    # TODO: When using the same proc mesh for both, it occasionally fails with:
    # RuntimeError: error while spawning actor error_actor_1v4bMhugCu1q: failed ranks: [0, 2, 3]
    second_mesh = spawn_procs_on_this_host({"gpus": 1})
    supervisor = pm.spawn(
        "supervisor", ErrorActorWithSupervise, second_mesh, should_handle=False
    )

    message = re.compile(
        r"The actor .* and all its descendants have failed\..*error_actor",
        re.DOTALL,
    )
    # Note that __supervise__ will not get called until the next message
    # can be processed. If __supervise__ isn't called before the endpoint returns,
    # it won't be seen as a failure. Add the additional sleep so the actor can
    # handle the supervision message to propagate it.
    # Calling a second endpoint would also raise the same message.
    with pytest.raises(SupervisionError, match=message):
        await supervisor.subworker_fail.call(sleep=15)

    await pm.stop()


# This test takes up to 3 minutes to run because the timeout on controller
# unreachable is 120 seconds.
@pytest.mark.timeout(180)
async def test_actor_mesh_supervision_controller_dead() -> None:
    """Tests what happens when the owner of an actor crashes ungracefully, and
    another proc has a ref to that actor. That actor should stop and the ref
    should get a supervision error"""
    with override_fault_hook():
        pm = spawn_procs_on_this_host({"gpus": 1})
        second_mesh = spawn_procs_on_this_host({"gpus": 1})
        wrapper_mesh = pm.spawn("wrapper", ErrorActorWithSupervise, second_mesh)
        inner_mesh = await wrapper_mesh.get_mesh.call_one()
        # First, stop the inner actors. We do this so we can guarantee both
        # the wrapper and inner has exited.
        await wrapper_mesh.stop_subworker.call()

        # Cause a hard crash to make the controller unreachable.
        # This will deliver a supervision event to the client which is ignored
        # with the override_fault_hook.
        with pytest.raises(SupervisionError):
            await wrapper_mesh.self_ungraceful_fail.call_one()

        # The inner mesh should not be reachable, and have a good error message
        # explaining the problem. This will take as long as SUPERVISION_WATCHDOG_TIMEOUT
        # because that is how long we wait before assuming the controller is dead.
        with pytest.raises(
            SupervisionError,
            match="timed out reaching controller.*for mesh error_actor",
        ):
            await inner_mesh.check.call()

    await pm.stop()


@pytest.mark.timeout(60)
async def test_actor_abort() -> None:
    class AbortActor(Actor):
        @endpoint
        def abort(self, reason: Optional[str] = None) -> None:
            context().actor_instance.abort(reason)

    for reason in (None, "test abort reason"):
        fut: asyncio.Future[str] = asyncio.Future()

        def make_fault_hook(
            future: asyncio.Future[str],
        ) -> Callable[[MeshFailure], None]:
            loop = asyncio.get_running_loop()

            def fault_hook(failure: MeshFailure) -> None:
                report = failure.report()
                # Due to poor test isolation, we might observe MeshFailures from
                # other tests here, but we only care about the AbortActor.
                if "AbortActor" in report:
                    loop.call_soon_threadsafe(future.set_result, report)

            return fault_hook

        with override_fault_hook(make_fault_hook(fut)):
            pm = this_host().spawn_procs({"gpus": 1})
            actor = pm.spawn("abort", AbortActor)
            # This call will succeed, but the actor will abort.
            await actor.abort.call(reason)
            if reason is None:
                assert "no reason provided" in await fut
            else:
                assert reason in await fut


@pytest.mark.timeout(500)
async def test_gil_stall():
    """Test that many concurrent actor calls don't cause GIL stall issues.

    This test spawns actors and sends many concurrent requests while
    simultaneously holding the GIL in a background thread to simulate
    GIL contention. This verifies that the actor system doesn't deadlock
    when the GIL is held for extended periods.

    We use the Rust hold_gil_for_test function because Python code releases
    the GIL periodically (every sys.getswitchinterval() seconds). Rust's
    Python::with_gil with thread::sleep holds the GIL continuously.
    """
    # Set environment variables for actor/proc state polling intervals
    os.environ["HYPERACTOR_MESH_GET_ACTOR_STATE_MAX_IDLE"] = "1s"
    os.environ["HYPERACTOR_MESH_GET_PROC_STATE_MAX_IDLE"] = "2s"

    def timestamp():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    print(f"[{timestamp()}] spawning the procs", file=sys.stderr)
    pm = spawn_procs_on_this_host({"gpus": 1})
    print(f"[{timestamp()}] spawning the actors", file=sys.stderr)
    supervisor = pm.spawn("error", ErrorActor)
    rets = []

    # Start a background thread that will hold the GIL for 15 seconds
    # after a 2 second delay (to let requests start flowing).
    # This creates GIL contention - without the async GIL_LOCK in monarch_with_gil,
    # multiple tokio workers would block simultaneously on Python::with_gil,
    # starving the runtime. With GIL_LOCK, only one tokio task blocks at a time
    # while others await the async mutex.
    hold_gil_for_test(delay_secs=2.0, hold_secs=15.0)

    print(f"[{timestamp()}] start sending requests", file=sys.stderr)

    for i in range(0, 600):
        rets.append(supervisor.check.call(i))
    print(f"[{timestamp()}] all requests are sent", file=sys.stderr)
    gather_start = time.time()
    await asyncio.gather(*rets)
    gather_end = time.time()
    print(
        f"[{timestamp()}] all requests completed, gathering took {gather_end - gather_start:.3f} seconds",
        file=sys.stderr,
    )
