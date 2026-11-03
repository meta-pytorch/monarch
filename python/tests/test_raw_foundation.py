# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Additional boundary and supervision witnesses for the raw foundation."""

import asyncio
import threading
from pathlib import Path
from typing import Any, cast

import monarch.actor
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.pickle import (
    pickle as monarch_pickle,
    push_tensor_engine_reference_if_active,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._src.actor.raw import _FRAME, _freeze, _MAGIC, _unframe
from monarch.actor import (
    Actor,
    Channel,
    concurrent_endpoint,
    endpoint,
    MessageEnvelope,
    RawMessageActor,
    send_message,
    spawn_actor_task,
    this_host,
)


async def _request(target, headers):
    reply, receiver = Channel[Any].open(once=True)
    send_message(target, MessageEnvelope(headers, reply_port=reply))
    return await asyncio.wait_for(receiver.recv(), 15)


class _ReferenceSink(Actor):
    async def handle(self, message):
        control = message.headers.get("control_reference")
        if control is not None:
            assert await control.ping.call_one() == "pong"
        return await super().handle(message)

    @endpoint
    async def ping(self):
        return "pong"

    @endpoint
    async def use(self, target, reply):
        reply.send(await target.ping.call_one())
        return "body and control references survived"


class _ReferenceRouter(Actor):
    def __init__(self, destination, control):
        self.destination = destination
        self.control = control

    def handle(self, message):
        send_message(
            self.destination,
            message.replace(
                headers={**message.headers, "control_reference": self.control}
            ),
        )

    @endpoint
    async def use(self, target, reply):
        raise AssertionError("endpoint router must not decode application arguments")


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_header_and_body_reference_tables_survive_same_forwarded_frame():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    sink = procs.slice(processes=0).spawn("sink", _ReferenceSink)
    body = procs.slice(processes=0).spawn("body", _ReferenceSink)
    control = procs.slice(processes=0).spawn("control", _ReferenceSink)
    router = procs.slice(processes=1).spawn("router", _ReferenceRouter, sink, control)
    try:
        await control.initialized
        reply, receiver = Channel[str].open(once=True)
        assert await router.use.call_one(body, reply) == (
            "body and control references survived"
        )
        assert await receiver.recv() == "pong"
    finally:
        await procs.stop()


class _TensorHeader:
    def __reduce__(self):
        push_tensor_engine_reference_if_active(self)
        return str, ("must not be sent",)


def test_decoded_control_header_shape_and_payload_input_are_validated():
    control = monarch_pickle((["not a dictionary"], None, None)).buffer().read()
    with pytest.raises(ValueError, match="dictionary"):
        _unframe(_freeze(_FRAME.pack(_MAGIC, len(control), 0) + control), [], [])
    with pytest.raises(TypeError):
        MessageEnvelope({}, cast(Any, 100))


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_pending_host_and_tensor_control_references_reject_without_waiting():
    """Gate a real native Shared producer and exercise HostMesh's own reducer."""
    procs = this_host().spawn_procs(per_host={"processes": 1})
    target = procs.spawn("target", _ReferenceSink)
    host = this_host()
    gate = threading.Event()
    try:
        await target.initialized
        value = host._hy_host_mesh.poll()
        assert value is not None

        def delayed():
            assert gate.wait(15), "raw header serialization blocked on a pending host"
            return value

        pending = object.__new__(type(host))
        pending.__dict__ = {
            **host.__dict__,
            "_inner_host_mesh": PythonTask.spawn_blocking(delayed),
        }
        assert pending._hy_host_mesh.poll() is None
        with pytest.raises(ValueError, match="pending mesh references"):
            send_message(target, MessageEnvelope({"nested": {"pending": pending}}))
        assert not gate.is_set()
        with pytest.raises(RuntimeError, match="Tensor engine references are not allowed"):
            send_message(target, MessageEnvelope({"bad": _TensorHeader()}))
        with pytest.raises(TypeError, match="reply_port"):
            send_message(target, MessageEnvelope({}, reply_port=object()))
        assert await target.ping.call_one() == "pong"
    finally:
        gate.set()
        await procs.stop()


class _TaskOwner(Actor):
    def __init__(self, path):
        self.path = path

    def handle(self, message):
        if message.headers.get("operation") == "background":
            spawn_actor_task(self, self.work(message.reply_port, "raw"))
            return None
        return super().handle(message)

    async def work(self, port, label):
        port.send("started")
        try:
            await asyncio.Event().wait()
        finally:
            with open(self.path, "a") as output:
                output.write(label + " cancelled\n")

    @concurrent_endpoint(explicit_response_port=True)
    async def background(self, port):
        await self.work(port, "endpoint")

    @endpoint
    async def registry(self):
        from monarch._src.actor.tasks import _states

        return sum(not task.done() for task in _states[self].records)

    async def __cleanup__(self, exc):
        with open(self.path, "a") as output:
            output.write("cleanup\n")


@pytest.mark.timeout(90)
@isolate_in_subprocess
async def test_raw_and_concurrent_tasks_share_registry_and_cleanup(tmp_path):
    path = str(tmp_path / "owned_tasks")
    procs = this_host().spawn_procs(per_host={"processes": 1})
    actor = procs.spawn("tasks", _TaskOwner, path)
    try:
        assert await _request(actor, {"operation": "background"}) == "started"
        assert await actor.background.call_one() == "started"
        assert await actor.registry.call_one() == 2
        await actor.stop()
        lines = Path(path).read_text().splitlines()
        assert sorted(lines[:-1]) == ["endpoint cancelled", "raw cancelled"]
        assert lines[-1] == "cleanup"
    finally:
        await procs.stop()


class _FailingRaw(RawMessageActor):
    async def handle(self, message):
        if message.headers["operation"] == "task":
            spawn_actor_task(self, self.fail())
        else:
            raise ValueError("raw handler failure")

    async def fail(self):
        await asyncio.sleep(0)
        raise ValueError("raw owned task failure")


@pytest.mark.timeout(90)
@isolate_in_subprocess
@pytest.mark.parametrize("operation", ["handler", "task"])
async def test_uncaught_raw_failures_reach_supervision(operation):
    happened = threading.Event()
    failures = []

    def capture(failure):
        failures.append(failure.report())
        happened.set()

    original = monarch.actor.unhandled_fault_hook
    monarch.actor.unhandled_fault_hook = capture
    procs = this_host().spawn_procs(per_host={"processes": 1})
    actor = procs.spawn("failing", _FailingRaw)
    try:
        await actor.initialized
        send_message(actor, MessageEnvelope({"operation": operation}))
        assert await asyncio.to_thread(happened.wait, 15)
        assert any(
            ("raw handler failure" if operation == "handler" else "raw owned task failure")
            in report
            for report in failures
        )
    finally:
        await procs.stop()
        monarch.actor.unhandled_fault_hook = original
