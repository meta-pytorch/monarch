# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import json
import os
import threading
from typing import Any, cast

import cloudpickle
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._src.actor.raw import _FRAME, _MAGIC, _unframe
from monarch.actor import (
    Actor,
    Channel,
    concurrent_endpoint,
    endpoint,
    MessageEnvelope,
    RawMessageActor,
    returns_future,
    send_message,
    spawn_actor_task,
    this_host,
)


@returns_future
async def ask(target, headers, payload=b""):
    reply, receiver = Channel[Any].open(once=True)
    send_message(target, MessageEnvelope(headers, payload, reply))
    return await receiver.recv()


class BytesActor(RawMessageActor):
    def handle(self, message: MessageEnvelope):
        assert message.payload.readonly
        operation = message.headers["operation"]
        if operation == "json":
            result = len(json.loads(bytes(message.payload)))
        elif operation == "pickle":
            result = cloudpickle.loads(bytes(message.payload))
        elif operation == "capability":
            result = message.headers["nested"]["destination"].ping.call_one().get()
        elif operation == "pid":
            result = os.getpid()
        else:
            result = bytes(message.payload)
        message.reply_port.send(result)
        return "this return is ignored"


class RawRouter(RawMessageActor):
    def __init__(self, destination):
        self.destination = destination

    def handle(self, message):
        send_message(
            self.destination,
            message.replace(headers={**message.headers, "hop": "router"}),
        )


def reconstruct_in_process(pid):
    assert os.getpid() == pid, "application payload decoded by a router"
    return "decoded only at sink"


class ReconstructionProbe:
    def __init__(self, pid):
        self.pid = pid

    def __reduce__(self):
        return reconstruct_in_process, (self.pid,)


class DelegatingActor(Actor):
    def handle(self, message):
        assert message.headers["protocol"] == "monarch.endpoint"
        self.seen = getattr(self, "seen", 0) + 1
        before = bytes(message.payload)
        result = super().handle(message)
        assert bytes(message.payload) == before
        return result

    @endpoint
    async def echo(self, value):
        return value

    @endpoint
    async def ping(self):
        return "pong"

    @endpoint(explicit_response_port=True)
    async def explicit(self, port, value):
        port.send(value)

    @endpoint
    async def fail(self):
        raise ValueError("endpoint error")

    @concurrent_endpoint
    async def concurrent(self, value):
        await asyncio.sleep(0)
        return value

    @endpoint
    async def count(self):
        return self.seen


class SyncDelegatingActor(Actor):
    def handle(self, message):
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()
        self.seen = getattr(self, "seen", 0) + 1
        return super().handle(message)

    @endpoint
    def echo(self, value):
        return value


class OpaqueEndpointRouter(Actor):
    def __init__(self, destination):
        self.destination = destination

    def handle(self, message):
        send_message(self.destination, message)

    @endpoint
    async def echo(self, value):
        raise AssertionError("the router must not decode or invoke endpoints")


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_raw_bytes_json_and_opaque_reconstruction_probe():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", BytesActor)
    router = procs.slice(processes=1).spawn("router", RawRouter, consumer)
    try:
        arbitrary = b"\x00\xffnot a pickle\x80"
        assert await ask(router, {"operation": "echo"}, arbitrary) == arbitrary
        assert (
            await ask(router, {"operation": "json"}, json.dumps([1, 2, 3]).encode())
            == 3
        )
        pid = await ask(consumer, {"operation": "pid"})
        payload = cloudpickle.dumps(ReconstructionProbe(pid))
        assert (
            await ask(router, {"operation": "pickle"}, payload)
            == "decoded only at sink"
        )
        # The sink's non-None return never produces an implicit second reply.
        reply, receiver = Channel[Any].open()
        send_message(consumer, MessageEnvelope({"operation": "echo"}, b"one", reply))
        send_message(consumer, MessageEnvelope({"operation": "echo"}, b"two", reply))
        assert await receiver.recv() == b"one"
        assert await receiver.recv() == b"two"
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_actor_delegation_and_opaque_endpoint_reference_forwarding():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", DelegatingActor)
    router = procs.slice(processes=1).spawn("router", OpaqueEndpointRouter, consumer)
    sync = procs.slice(processes=1).spawn("sync", SyncDelegatingActor)
    try:
        assert await sync.echo.call_one(3) == 3
        assert await consumer.echo.call_one("ordinary") == "ordinary"
        assert await consumer.explicit.call_one(7) == 7
        assert await consumer.concurrent.call_one(8) == 8
        with pytest.raises(Exception, match="endpoint error"):
            await consumer.fail.call_one()
        assert await consumer.ping.call_one() == "pong"
        reply, receiver = Channel[str].open()
        value = {"mesh": consumer, "port": reply, "proc": procs, "host": this_host()}
        returned = await router.echo.call_one(value)
        assert await returned["mesh"].ping.call_one() == "pong"
        assert returned["proc"].size() == 2
        assert returned["host"].size() == 1
        returned["port"].send("attached")
        assert await receiver.recv() == "attached"
        pid_actor = procs.slice(processes=0).spawn("pid", BytesActor)
        pid = await ask(pid_actor, {"operation": "pid"})
        assert (
            await router.echo.call_one(ReconstructionProbe(pid))
            == "decoded only at sink"
        )
        assert await consumer.count.call_one() >= 8
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_payload_snapshots_and_selection():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    actors = procs.spawn("bytes", BytesActor)
    reply, receiver = Channel[Any].open()
    payload = bytearray(b"before")
    message = MessageEnvelope(
        {"operation": "echo"}, memoryview(payload).toreadonly(), reply
    )
    payload[:] = b"after!"
    try:
        assert bytes(message.payload) == b"before"
        with pytest.raises(TypeError):
            message.payload[0] = 0
        with pytest.raises(TypeError):
            cast(Any, message.headers)["operation"] = "other"
        with pytest.raises(ValueError, match="exactly 1 actor"):
            send_message(actors, message)
        send_message(actors, message, selection="all")
        assert [await receiver.recv(), await receiver.recv()] == [b"before", b"before"]
        send_message(actors, message, selection="choose")
        assert await receiver.recv() == b"before"
        with pytest.raises(ValueError, match="selection"):
            send_message(actors, message, selection="invalid")
        message._local_state = [object()]
        with pytest.raises(ValueError, match="tensor-engine local state"):
            send_message(actors.slice(processes=0), message)
    finally:
        await procs.stop()


def frozen(data):
    buffer = Buffer()
    buffer.write(data)
    return buffer.freeze()


@pytest.mark.parametrize(
    "wire,refs",
    [
        (b"short", []),
        (_FRAME.pack(b"badmagic", 0, 0), []),
        (_FRAME.pack(_MAGIC, 100, 0), []),
        (_FRAME.pack(_MAGIC, 0, 1), []),
    ],
)
def test_invalid_raw_framing(wire, refs):
    with pytest.raises(ValueError, match="raw message"):
        _unframe(frozen(wire), [], refs)


def test_handle_endpoint_name_collision():
    from monarch._src.actor.actor_mesh import _actor_kind

    class Collision(Actor):
        @endpoint
        # pyrefly: ignore [bad-override]
        def handle(self):
            pass

    with pytest.raises(ValueError, match="collides"):
        _actor_kind(Collision)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_resolved_and_nested_pending_control_references():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    raw = procs.spawn("raw", BytesActor)
    destination = procs.spawn("destination", DelegatingActor)
    release = threading.Event()
    original = procs._proc_mesh
    try:
        assert await destination.ping.call_one() == "pong"
        assert (
            await ask(
                raw, {"operation": "capability", "nested": {"destination": destination}}
            )
            == "pong"
        )
        native = original.block_on()

        async def gated():
            await PythonTask.spawn_blocking(release.wait)
            return native

        procs._proc_mesh = PythonTask.from_coroutine(gated()).spawn()
        assert procs._proc_mesh.poll() is None
        # The gated reference is nested and never releases during serialization.
        # A blocking fallback reducer would deadlock this test before finally.
        with pytest.raises(ValueError, match="pending mesh references.*initialized"):
            send_message(raw, MessageEnvelope({"nested": [procs]}))
        assert not release.is_set()
    finally:
        release.set()
        procs._proc_mesh = original
        await procs.stop()


class OwnedTasks(RawMessageActor):
    def __init__(self, path):
        self.path = path

    async def handle(self, message):
        spawn_actor_task(self, self.run(message.reply_port), name="owned raw task")

    async def run(self, reply):
        reply.send("started")
        try:
            await asyncio.Event().wait()
        finally:
            with open(self.path, "a") as output:
                output.write("task\n")

    async def __cleanup__(self, exc):
        with open(self.path, "a") as output:
            output.write("cleanup\n")


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_generic_tasks_cleanup_before_user_hook(tmp_path):
    procs = this_host().spawn_procs(per_host={"processes": 1})
    path = tmp_path / "owned"
    actor = procs.spawn("owned", OwnedTasks, str(path))
    assert await ask(actor, {}) == "started"
    await procs.stop()
    assert path.read_text() == "task\ncleanup\n"
