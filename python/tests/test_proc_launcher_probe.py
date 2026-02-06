# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Wire format probe test for port communication.

This is a wire-format probe, not a lifecycle simulation. It validates
that when Python calls `port.send(value)` or `port.exception(exc)`,
Rust receives the expected `PythonMessage` envelope with `kind=Result`
or `kind=Exception`.

"""

import functools
from dataclasses import dataclass
from typing import Any, Callable, cast, Coroutine

import cloudpickle
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.pickle import pickle
from monarch._rust_bindings.monarch_hyperactor.proc_launcher_probe import (
    probe_exit_port_via_mesh,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._src.actor.actor_mesh import Actor, ActorMesh, context, Port
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import this_host


@dataclass
class ProbePayload:
    """Payload for wire format testing."""

    value: int
    items: list[str]


class WireFormatProbeActor(Actor):
    """Test actor for probing port wire format. Not a realistic
    launcher."""

    @endpoint(explicit_response_port=True)
    async def send_result_on_port(self, port: Port[ProbePayload], tag: str) -> None:
        """Send a Result payload to the port."""
        payload = ProbePayload(
            value=42,
            items=["line1", "line2"],
        )
        port.send(payload)

    @endpoint(explicit_response_port=True)
    async def send_exception_on_port(self, port: Port[ProbePayload], tag: str) -> None:
        """Catch an error and explicitly send it as an exception on
        the port.

        This simulates a caller that catches a failure and reports it
        via the port. It is NOT "endpoint threw" - the endpoint
        completes normally after sending the exception.

        """
        try:
            raise ValueError("simulated failure")
        except ValueError as e:
            port.exception(e)


def _python_task_test(
    fn: Callable[[], Coroutine[Any, Any, None]],
) -> Callable[[], None]:
    """
    Wrapper for tests that use the internal tokio event loop
    APIs and need to run on that event loop.
    """

    @functools.wraps(fn)
    def wrapper() -> None:
        return PythonTask.from_coroutine(fn()).block_on()

    return wrapper


@_python_task_test
async def test_port_receives_result() -> None:
    """Test that Rust receives PythonMessage(Result) with cloudpickled
    payload."""

    # Spawn the test actor.
    # Cast needed: spawn() is typed to return TActor for ergonomic
    # method access, but actually returns ActorMesh[TActor]. We need
    # ActorMesh to call slice().
    proc_mesh = this_host().spawn_procs(per_host={"gpus": 1})
    actor_mesh = cast(
        ActorMesh[WireFormatProbeActor],
        proc_mesh.spawn("probe_actor", WireFormatProbeActor),
    )

    # Get instance and mailbox from the test context
    ins = context().actor_instance
    instance = ins._as_rust()
    mailbox = ins._mailbox

    # Slice to a single actor and get its _inner (PythonActorMesh).
    # Cast needed: _inner is typed as ActorMeshProtocol but
    # probe_exit_port_via_mesh expects PythonActorMesh. At runtime
    # _inner is a PythonActorMesh.
    single_actor_mesh = actor_mesh.slice(gpus=0)
    actor_mesh_inner = cast(PythonActorMesh, single_actor_mesh._inner)

    # Pickle the args - port will be injected by runtime Args should
    # be (args_tuple, kwargs_dict) format
    args = ("test_tag",)
    kwargs = {}
    pickling_state = pickle((args, kwargs), allow_pending_pickles=False)

    # Call the Rust probe and await the result
    report = await probe_exit_port_via_mesh(
        actor_mesh_inner, instance, mailbox, "send_result_on_port", pickling_state
    )

    # Assert we received a PythonMessage
    assert report.received_type == "PythonMessage", f"Got {report.received_type}"
    assert report.error is None, f"Unexpected error: {report.error}"

    # Assert it's a Result, not an Exception
    assert report.kind == "Result", f"Expected Result, got {report.kind}"
    assert report.rank is not None, "rank should be present for Result"

    # Assert the payload can be decoded with cloudpickle
    payload = cloudpickle.loads(bytes(report.payload_bytes))

    # Verify it's the expected ProbePayload
    assert isinstance(payload, ProbePayload), (
        f"Expected ProbePayload, got {type(payload)}"
    )
    assert payload.value == 42, f"Expected value=42, got {payload.value}"
    assert payload.items == [
        "line1",
        "line2",
    ], f"Expected items=['line1', 'line2'], got {payload.items}"


@_python_task_test
async def test_port_receives_exception() -> None:
    """Test that Rust receives PythonMessage(Exception) when
    port.exception() is called."""

    # Spawn the test actor.
    # Cast needed: spawn() is typed to return TActor for ergonomic
    # method access, but actually returns ActorMesh[TActor]. We need
    # ActorMesh to call slice().
    proc_mesh = this_host().spawn_procs(per_host={"gpus": 1})
    actor_mesh = cast(
        ActorMesh[WireFormatProbeActor],
        proc_mesh.spawn("probe_actor", WireFormatProbeActor),
    )

    # Get instance and mailbox from the test context
    ins = context().actor_instance
    instance = ins._as_rust()
    mailbox = ins._mailbox

    # Slice to a single actor and get its _inner (PythonActorMesh).
    # Cast needed: _inner is typed as ActorMeshProtocol but
    # probe_exit_port_via_mesh expects PythonActorMesh. At runtime
    # _inner is a PythonActorMesh.
    single_actor_mesh = actor_mesh.slice(gpus=0)
    actor_mesh_inner = cast(PythonActorMesh, single_actor_mesh._inner)

    # Pickle the args - port will be injected by runtime Args should
    # be (args_tuple, kwargs_dict) format
    args = ("test_tag",)
    kwargs = {}
    pickling_state = pickle((args, kwargs), allow_pending_pickles=False)

    # Call the Rust probe and await the result
    report = await probe_exit_port_via_mesh(
        actor_mesh_inner, instance, mailbox, "send_exception_on_port", pickling_state
    )

    # Assert we received a PythonMessage
    assert report.received_type == "PythonMessage", f"Got {report.received_type}"
    assert report.error is None, f"Unexpected error: {report.error}"

    # Assert it's an Exception, not a Result
    assert report.kind == "Exception", f"Expected Exception, got {report.kind}"
    assert report.rank is not None, "rank should be present for Exception"

    # Assert the payload can be decoded as an exception
    exc = cloudpickle.loads(bytes(report.payload_bytes))

    # Verify it's a ValueError with our message
    assert isinstance(exc, ValueError), f"Expected ValueError, got {type(exc)}"
    assert str(exc) == "simulated failure", f"Expected 'simulated failure', got '{exc}'"
