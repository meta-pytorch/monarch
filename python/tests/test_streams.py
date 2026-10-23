# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import os
from typing import Any, cast

import cloudpickle
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import (
    Channel,
    MessageEnvelope,
    RawMessageActor,
    returns_future,
    send_message,
    spawn_actor_task,
    streams,
    this_host,
)
from monarch.actor.streams import ByteStream


async def serialized(value):
    yield cloudpickle.dumps(value)


def request(target, method, source, **kwargs):
    return streams.request(target, source, metadata={"method": method}, **kwargs)


@returns_future
async def call(target, operation):
    reply, receiver = Channel[Any].open(once=True)
    send_message(target, MessageEnvelope({"operation": operation}, reply_port=reply))
    return await receiver.recv()


class StreamActor(RawMessageActor):
    async def handle(self, message: MessageEnvelope):
        if message.headers.get("protocol") == streams.PROTOCOL:
            await streams.handle(self, message, self.dispatch)
        elif message.headers["operation"] == "ready":
            spawn_actor_task(self, self.control(message))
        else:
            await self.control(message)

    async def dispatch(self, chunks, metadata):
        return await getattr(self, metadata["method"])(chunks, metadata)

    async def control(self, message):
        result = await getattr(self, message.headers["operation"])()
        message.reply_port.send(result)


class Consumer(StreamActor):
    async def load(self, chunks: ByteStream, metadata) -> Any:
        return cloudpickle.loads(b"".join([chunk async for chunk in chunks]))

    async def collect(self, chunks: ByteStream, metadata) -> list[bytes]:
        result = [chunk async for chunk in chunks]
        for _ in range(2):
            with pytest.raises(StopAsyncIteration):
                await anext(chunks)
        return result

    async def fail(self, chunks: ByteStream, metadata) -> None:
        await anext(chunks)
        raise ValueError("destination boom")

    async def ignore(self, chunks: ByteStream, metadata) -> int:
        return 7

    async def early(self, chunks: ByteStream, metadata) -> bytes:
        return await anext(chunks)

    async def metadata(self, chunks, metadata):
        metadata["ready"].send(metadata["route_key"])
        total = 0
        async for chunk in chunks:
            total += len(chunk)
        return total

    async def ping(self) -> str:
        return "pong"


class Router(StreamActor):
    def __init__(self, destination):
        self.destination = destination
        self.observed = 0

    async def load(self, chunks: ByteStream, metadata) -> Any:
        return await streams.request(self.destination, chunks, metadata=metadata)

    async def collect(self, chunks: ByteStream, metadata) -> Any:
        async def observe():
            async for chunk in chunks:
                self.observed += len(chunk)
                yield chunk

        return await streams.request(
            self.destination, observe(), metadata=metadata, chunk_size=3
        )

    async def fail(self, chunks: ByteStream, metadata) -> None:
        return await streams.request(self.destination, chunks, metadata=metadata)

    async def early(self, chunks: ByteStream, metadata) -> Any:
        return await streams.request(self.destination, chunks, metadata=metadata)

    async def metadata(self, chunks, metadata):
        assert metadata["route_key"] == "training"
        return await streams.request(self.destination, chunks, metadata=metadata)

    async def ping(self) -> str:
        return "pong"


def decoded_at(pid):
    assert os.getpid() == pid, "application object decoded in relay"
    return "only consumer decoded"


class Probe:
    def __init__(self, pid):
        self.pid = pid

    def __reduce__(self):
        return decoded_at, (self.pid,)


class GatedConsumer(StreamActor):
    def __init__(self):
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.finished = 0

    async def collect(self, chunks: ByteStream, metadata) -> list[bytes]:
        try:
            first = await anext(chunks)
            self.started.set()
            await self.release.wait()
            return [first, *[chunk async for chunk in chunks]]
        finally:
            self.finished += 1

    async def overlap(self, chunks: ByteStream, metadata) -> str:
        read = asyncio.create_task(anext(chunks))
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="outstanding read"):
            await anext(chunks)
        await read
        return "rejected"

    async def ready(self):
        await self.started.wait()

    async def unblock(self):
        self.release.set()

    async def status(self):
        from monarch._src.actor.stream import _sessions
        from monarch._src.actor.tasks import _states

        return (
            self.finished,
            len(_sessions.get(self, {})),
            (
                sum(not task.done() for task in _states[self].records)
                if self in _states
                else 0
            ),
        )

    async def retained(self):
        from monarch._src.actor.stream import _sessions

        return self in _sessions

    async def pid(self):
        return os.getpid()


@pytest.mark.parametrize("size", [0, -1, True, 1.5, "3", None])
def test_invalid_chunk_size(size):
    with pytest.raises(ValueError, match="positive integer"):
        streams.request(None, cast(Any, None), chunk_size=size)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_stream_roundtrip_and_chunks():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", Consumer)
    router = procs.slice(processes=1).spawn("router", Router, consumer)
    try:
        obj = {"message": "abcd" * 1000, "nested": [1, None, b"abc"]}
        assert await request(consumer, "load", serialized(obj), chunk_size=17) == obj
        assert await request(router, "load", serialized(obj), chunk_size=100) == obj
        for elements in ([], [b""], [b"abc", b"de", b"", b"fghijk"]):

            async def source():
                for element in elements:
                    yield element

            expected = [
                part
                for element in elements
                for part in (
                    [b""]
                    if not element
                    else [element[i : i + 3] for i in range(0, len(element), 3)]
                )
            ]
            assert await request(router, "collect", source(), chunk_size=3) == expected
        with pytest.raises(ValueError, match="exactly 1 actor"):
            await request(procs.spawn("multiple", Consumer), "load", serialized(1))
        assert await call(consumer, "ping") == "pong"
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_stream_get():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", Consumer)
    try:
        assert request(consumer, "load", serialized([1, 2, 3]), chunk_size=2).get() == [
            1,
            2,
            3,
        ]
    finally:
        procs.stop().get()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_decode_only_at_consumer():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer_proc = procs.slice(processes=0)
    consumer = consumer_proc.spawn("consumer", Consumer)
    pid_actor = consumer_proc.spawn("pid", GatedConsumer)
    router = procs.slice(processes=1).spawn("router", Router, consumer)
    try:
        pid = await call(pid_actor, "pid")
        assert pid != os.getpid()
        assert (
            await request(router, "load", serialized(Probe(pid)), chunk_size=7)
            == "only consumer decoded"
        )
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_incremental_bounded_forwarding_and_isolation():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", GatedConsumer)
    router = procs.slice(processes=1).spawn("router", Router, consumer)
    first, first_rx = Channel[None].open()
    produced = []
    allow_finish = asyncio.Event()
    closed = asyncio.Event()

    async def source():
        try:
            produced.append(1)
            yield b"one"
            first.send(None)
            await allow_finish.wait()
            produced.append(2)
            yield b"two"
        finally:
            closed.set()

    try:
        result = request(router, "collect", source()).as_asyncio()
        await call(consumer, "ready")
        assert produced == [1]

        # A second isolated session can progress while the first waits.
        async def second():
            yield b"xyz"

        other = request(router, "collect", second()).as_asyncio()
        await call(consumer, "unblock")
        # The source's second pull proves the first arrived downstream before
        # the source is allowed to finish.
        await first_rx.recv()
        assert produced == [1]
        assert not result.done()
        assert await other == [b"xyz"]
        allow_finish.set()
        assert await result == [b"one", b"two"]
        assert closed.is_set()
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_errors_and_early_return_close_producers():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", Consumer)
    router = procs.slice(processes=1).spawn("router", Router, consumer)
    try:
        for mode in ("fail", "early", "source"):
            closed = asyncio.Event()
            produced = []

            async def source():
                try:
                    produced.append(1)
                    yield b"abc"
                    if mode == "source":
                        raise ValueError("source boom")
                    produced.append(2)
                    await asyncio.Event().wait()
                finally:
                    closed.set()

            if mode == "early":
                assert await request(router, "early", source()) == b"abc"
            else:
                method = "collect" if mode == "source" else "fail"
                with pytest.raises(
                    Exception,
                    match=f"{mode if mode == 'source' else 'destination'} boom",
                ):
                    await request(router, method, source())
            assert closed.is_set()
            assert produced == [1]
            assert await call(consumer, "ping") == "pong"
            assert await call(router, "ping") == "pong"

        async def bad():
            yield "not bytes"

        with pytest.raises(Exception, match="bytes elements"):
            await request(router, "collect", cast(Any, bad()))
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
@pytest.mark.parametrize("relay", [False, True])
@pytest.mark.parametrize("first_chunk", [False, True])
async def test_cancellation_and_session_cleanup(relay, first_chunk):
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", GatedConsumer)
    router = procs.slice(processes=1).spawn("router", Router, consumer)
    target = router if relay else consumer
    pulled = asyncio.Event()
    closed = asyncio.Event()

    async def source():
        try:
            pulled.set()
            if first_chunk:
                yield b"abc"
            await asyncio.Event().wait()
        finally:
            closed.set()

    try:
        result = request(target, "collect", source()).as_asyncio()
        await pulled.wait()
        result.cancel()
        with pytest.raises(asyncio.CancelledError):
            await result
        assert closed.is_set()
        assert await call(consumer, "status") == (1, 0, 0)
        assert await call(router, "ping") == "pong"
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_overlapping_reads_rejected():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", GatedConsumer)

    async def source():
        yield b"abc"

    try:
        assert await request(consumer, "overlap", source()) == "rejected"
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_immediate_cancel_and_late_control():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", GatedConsumer)
    closed = asyncio.Event()

    async def source():
        try:
            await asyncio.Event().wait()
            yield b"never"
        finally:
            closed.set()

    try:
        result = request(consumer, "collect", source()).as_asyncio()
        result.cancel()
        with pytest.raises(asyncio.CancelledError):
            await result
        # An already-completed/unknown session never leaves a tombstone.
        send_message(
            consumer,
            MessageEnvelope(
                {
                    "protocol": streams.PROTOCOL,
                    "operation": "cancel",
                    "session": "unknown",
                    "metadata": {"method": "collect"},
                }
            ),
        )
        assert (await call(consumer, "status"))[1:] == (0, 0)
        assert not await call(consumer, "retained")
    finally:
        await procs.stop()


class ShutdownConsumer(StreamActor):
    def __init__(self, path):
        self.path = path

    async def wait(self, chunks: ByteStream, metadata):
        from monarch.actor import Port

        port: Port[None] = cloudpickle.loads(
            b"".join([chunk async for chunk in chunks])
        )
        port.send(None)
        try:
            await asyncio.Event().wait()
        finally:
            with open(self.path, "w") as output:
                output.write("handler closed")


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_shutdown_active_stream(tmp_path):
    procs = this_host().spawn_procs(per_host={"processes": 1})
    path = tmp_path / "shutdown"
    consumer = procs.spawn("consumer", ShutdownConsumer, str(path))
    started, receiver = Channel[None].open()
    result = request(consumer, "wait", serialized(started)).as_asyncio()
    await receiver.recv()
    await procs.stop()
    assert path.read_text() == "handler closed"
    # Stopping the actor does not synthesize a successful stream response.
    result.cancel()
    with pytest.raises(BaseException):
        await result


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_serialization_is_lazy_on_observer_loop():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", Consumer)
    serializations = []
    loop = asyncio.get_running_loop()

    class Lazy:
        def __reduce__(self):
            assert asyncio.get_running_loop() is loop
            serializations.append(True)
            return str, ("lazy",)

    try:
        result = request(consumer, "load", serialized(Lazy()))
        assert serializations == []
        assert await result == "lazy"
        assert serializations == [True]
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_get_timeout_cancels_stream():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", GatedConsumer)
    closed = []

    async def source():
        try:
            yield b"abc"
            await asyncio.Event().wait()
        finally:
            closed.append(True)

    try:
        with pytest.raises(TimeoutError):
            request(consumer, "collect", source()).get(timeout=1)
        assert closed == [True]
        assert call(consumer, "status").get() == (1, 0, 0)
    finally:
        procs.stop().get()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_consumed_and_closed_streams_rejected():
    demand, _ = Channel[None].open()
    chunks = ByteStream(cast(Any, demand))
    await chunks.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        streams.request(None, chunks)
    consumed = ByteStream(cast(Any, demand))
    consumed._eof = True
    with pytest.raises(RuntimeError, match="consumed"):
        streams.request(None, consumed)
    with pytest.raises(TypeError, match="async byte iterable"):
        streams.request(None, cast(Any, [b"abc"]))
    assert not hasattr(ByteStream, "read")
    assert not hasattr(ByteStream, "load")


class ClosingProducer:
    def __init__(self):
        self.closes = 0
        self.pulls = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.pulls += 1
        if self.pulls == 1:
            return b"abc"
        raise StopAsyncIteration

    async def aclose(self):
        self.closes += 1
        assert self.closes == 1, "producer closed more than once"


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_owned_iterators_close_once_even_on_immediate_cancel():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", Consumer)
    try:
        for mode in ("success", "early", "ignore", "immediate"):
            source = ClosingProducer()
            method = mode if mode in ("early", "ignore") else "collect"
            result = request(consumer, method, source).as_asyncio()
            if mode == "immediate":
                result.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await result
                assert source.pulls == 0
            else:
                await result
            if mode == "ignore":
                assert source.pulls == 0
            assert source.closes == 1
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_wait_for_cancels_session():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", GatedConsumer)
    pulled = asyncio.Event()
    closed = asyncio.Event()

    async def source():
        try:
            pulled.set()
            await asyncio.Event().wait()
            yield b"never"
        finally:
            closed.set()

    try:
        result = request(consumer, "collect", source()).as_asyncio()
        await pulled.wait()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(result, timeout=0.01)
        assert closed.is_set()
        assert await call(consumer, "status") == (1, 0, 0)
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_local_actor_definition_serializes_by_value():
    from monarch.actor import RawMessageActor, streams

    class LocalActor(RawMessageActor):
        async def handle(self, message):
            await streams.handle(self, message, self.ingest)

        async def ingest(self, chunks, metadata):
            total = 0
            async for chunk in chunks:
                total += len(chunk)
            return metadata, total

    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", LocalActor)

    async def source():
        yield b"abc"

    try:
        assert await streams.request(consumer, source(), metadata="local") == (
            "local",
            3,
        )
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_metadata_before_first_chunk_through_router():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", Consumer)
    router = procs.slice(processes=1).spawn("router", Router, consumer)
    ready, receiver = Channel[str].open(once=True)
    release = asyncio.Event()
    produced = []

    async def source():
        await release.wait()
        produced.append(True)
        yield b"abc"

    try:
        result = streams.request(
            router,
            source(),
            metadata={"method": "metadata", "route_key": "training", "ready": ready},
        ).as_asyncio()
        assert await receiver.recv() == "training"
        assert produced == []
        release.set()
        assert await result == 3
    finally:
        await procs.stop()
