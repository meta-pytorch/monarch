import os
import time

from threading import Event
from typing import Callable, Optional, TypeVar

import monarch.actor
from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure

from monarch.actor import Actor, endpoint, this_host


T = TypeVar("T")


class Lambda(Actor):
    @endpoint
    def run(self, l: Callable[[], T]) -> T:
        return l()


class Nest(Actor):
    def __init__(self):
        self.nest = this_host().spawn_procs().spawn("nested", Lambda)

    @endpoint
    def nested(self, l: Callable[[], T]) -> T:
        return self.nest.run.broadcast(l)

    @endpoint
    def nested_call_one(self, l: Callable[[], T]) -> T:
        return self.nest.run.call_one(l).get()

    @endpoint
    def direct(self, l: Callable[[], T]) -> T:
        return l()

    @endpoint
    def kill_nest(self) -> None:
        pid = self.nest.run.call_one(lambda: os.getpid()).get()
        os.kill(pid, 9)


def error():
    print("I AM ABOUT TO ERROR!!!!")
    raise ValueError("Error.")


class SuperviseNest(Nest):
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
        self.failure_happened.wait(timeout=10)
        monarch.actor.unhandled_fault_hook = self.original_hook

    def capture_fault(self, failure: MeshFailure) -> None:
        """Capture the fault instead of exiting the process."""
        print(f"Captured fault: {failure.report()}")
        self.captured_failure = failure
        self.failure_happened.set()

    def assert_fault_occurred(self, expected_substring: Optional[str] = None):
        """Assert that a fault was captured, optionally checking the message."""
        assert (
            self.captured_failure is not None
        ), "Expected a fault to be captured, but none occurred"
        if expected_substring:
            report = self.captured_failure.report()
            assert expected_substring in report, (
                f"Expected fault message to contain '{expected_substring}', "
                f"but got: {report}"
            )


def test_actor_failure():
    """
    If an actor dies, the client should receive an unhandled fault.
    """
    with FaultCapture() as capture:
        l = this_host().spawn_procs().spawn("actor", Lambda)
        l.run.broadcast(error)

    capture.assert_fault_occurred("This occurred because the actor itself failed.")


def test_proc_failure():
    """
    If a proc dies, the client should receive an unhandled fault.
    """
    with FaultCapture() as capture:
        l = this_host().spawn_procs().spawn("actor", Nest)
        l.kill_nest.call_one().get()

    capture.assert_fault_occurred()


def test_nested_mesh_kills_actor_actor_error():
    """
    If a nested actor errors, the fault should propagate to the client.
    """
    with FaultCapture() as capture:
        l = this_host().spawn_procs().spawn("actor", Nest)
        v = l.nested_call_one.call_one(lambda: 4).get()
        assert v == 4
        l.nested.call_one(error).get()
        print("ERRORED THE ACTOR")
    capture.assert_fault_occurred(
        "actor <root>.<tests.test_supervision_hierarchy.Nest actor>.<tests.test_supervision_hierarchy.Lambda nested> failed"
    )
