# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import os
import re
from threading import Event
from typing import Callable, Optional, TypeVar
from unittest.mock import patch

import monarch.actor
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure
from monarch.actor import Actor, Channel, endpoint, this_host


T = TypeVar("T")


class Lambda(Actor):
    @endpoint
    def run(self, func: Callable[[], T]) -> T:
        return func()


class Nest(Actor):
    def __init__(self):
        self.nest = (
            this_host().spawn_procs(per_host={"a_dim": 1}).spawn("nested", Lambda)
        )

    @endpoint
    def nested(self, func: Callable[[], T]) -> T:
        # pyrefly: ignore [bad-return]
        return self.nest.run.broadcast(func)

    @endpoint
    def nested_call_one(self, func: Callable[[], T]) -> T:
        return self.nest.run.call_one(func).get()

    @endpoint
    def direct(self, func: Callable[[], T]) -> T:
        return func()

    @endpoint
    def kill_nest(self) -> None:
        pid = self.nest.run.call_one(lambda: os.getpid()).get()
        os.kill(pid, 9)


def error():
    print("I AM ABOUT TO ERROR!!!!")
    raise ValueError("Error.")


class SuperviseNest(Nest):
    # pyrefly: ignore [bad-param-name-override]
    def __supervise__(self, x):
        print("SUPERVISE: ", x)


class FaultCapture:
    """Helper class to capture unhandled faults for testing."""

    def __init__(self):
        self.failure_happened = Event()
        self.captured_failure: Optional[MeshFailure] = None
        self.original_hook = None

    def __enter__(self):
        self.original_hook = monarch.actor.unhandled_fault_hook
        monarch.actor.unhandled_fault_hook = self.capture_fault
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.failure_happened.wait(timeout=30)
        # pyrefly: ignore [bad-assignment]
        monarch.actor.unhandled_fault_hook = self.original_hook

    def capture_fault(self, failure: MeshFailure) -> None:
        """Capture the fault instead of exiting the process."""
        print(f"Captured fault: {failure.report()}")
        self.captured_failure = failure
        self.failure_happened.set()

    def assert_fault_occurred(self, pattern: Optional[str] = None):
        """Assert that a fault was captured, optionally checking the message."""
        assert self.captured_failure is not None, (
            "Expected a fault to be captured, but none occurred"
        )
        if pattern:
            report = self.captured_failure.report()
            assert re.search(pattern, report), (
                f"Expected fault message to contain pattern '{pattern}', "
                f"but got: {report}"
            )


@isolate_in_subprocess
def test_actor_failure():
    """
    If an actor dies, the client should receive an unhandled fault.
    """
    with FaultCapture() as capture:
        actor = this_host().spawn_procs().spawn("actor", Lambda)
        actor.run.broadcast(error)

    capture.assert_fault_occurred("and all its descendants have failed:")


@isolate_in_subprocess
@pytest.mark.parametrize("once", [False, True])
@pytest.mark.parametrize("mode", ["get", "await"])
def test_root_failure_closes_unrelated_receives(once: bool, mode: str) -> None:
    actor = this_host().spawn_procs().spawn("failed_actor", Lambda)
    actor.run.call_one(lambda: None).get()
    _sender, receiver = Channel[int].open(once=once)
    pending = receiver.recv()
    with pytest.raises(TimeoutError):
        pending.get(timeout=0.05)

    interrupted = Event()
    with patch("_thread.interrupt_main", side_effect=interrupted.set):
        actor.run.broadcast(error)
        with pytest.raises(EOFError, match="Port closed: .*client stopped") as exc:
            if mode == "get":
                pending.get(timeout=30)
            else:

                async def receive() -> None:
                    await asyncio.wait_for(pending, timeout=30)

                asyncio.run(receive())
        assert "failed_actor" not in str(exc.value)
        assert interrupted.wait(timeout=30)


@isolate_in_subprocess
def test_proc_failure():
    """
    If a proc dies, the client should receive an unhandled fault.
    """
    with FaultCapture() as capture:
        actor = this_host().spawn_procs().spawn("top", Nest)
        actor.kill_nest.call_one().get()

    # Any actors on the proc mesh can report the proc failure, so it might be
    # "nested" or it might be other broken actors such as "log_level".
    capture.assert_fault_occurred("(nested|log_level)\\{'a_dim': 0/1\\}")
    capture.assert_fault_occurred(
        "the process this actor was running on failed: Killed\\(sig=9\\)"
    )


@isolate_in_subprocess
def test_nested_mesh_kills_actor_actor_error():
    """
    If a nested actor errors, the fault should propagate to the client.
    """
    with FaultCapture() as capture:
        actor = this_host().spawn_procs().spawn("actor", Nest)
        v = actor.nested_call_one.call_one(lambda: 4).get()
        assert v == 4
        actor.nested.call_one(error).get()
        print("ERRORED THE ACTOR")
    capture.assert_fault_occurred(
        "The actor.*test_supervision_hierarchy\\.Lambda nested\\{'a_dim': 0/1\\}.*and all its descendants have failed"
    )
