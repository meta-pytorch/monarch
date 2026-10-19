# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import inspect
import os
from typing import Any, cast

import cloudpickle
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import (
    Actor,
    ByteStream,
    Channel,
    concurrent_endpoint,
    endpoint,
    stream_endpoint,
    this_host,
)


class Consumer(Actor):
    @stream_endpoint
    async def load(self, chunks: ByteStream) -> Any:
        return cloudpickle.loads(b"".join([chunk async for chunk in chunks]))

    @stream_endpoint
    async def collect(self, chunks: ByteStream) -> list[bytes]:
        result = [chunk async for chunk in chunks]
        for _ in range(2):
            with pytest.raises(StopAsyncIteration):
                await anext(chunks)
        return result

    @stream_endpoint
    async def fail(self, chunks: ByteStream) -> None:
        await anext(chunks)
        raise ValueError("destination boom")

    @stream_endpoint
    async def ignore(self, chunks: ByteStream) -> int:
        return 7

    @stream_endpoint
    async def early(self, chunks: ByteStream) -> bytes:
        return await anext(chunks)

    @endpoint
    async def ping(self) -> str:
        return "pong"


class Router(Actor):
    def __init__(self, destination):
        self.destination = destination
        self.observed = 0

    @stream_endpoint
    async def load(self, chunks: ByteStream) -> Any:
        return await self.destination.load.stream(chunks)

    @stream_endpoint
    async def collect(self, chunks: ByteStream) -> Any:
        async def observe():
            async for chunk in chunks:
                self.observed += len(chunk)
                yield chunk

        return await self.destination.collect.stream_chunks(observe(), chunk_size=3)

    @stream_endpoint
    async def fail(self, chunks: ByteStream) -> None:
        return await self.destination.fail.stream(chunks)

    @stream_endpoint
    async def early(self, chunks: ByteStream) -> Any:
        return await self.destination.early.stream(chunks)

    @endpoint
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


class GatedConsumer(Actor):
    def __init__(self):
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.finished = 0

    @stream_endpoint
    async def collect(self, chunks: ByteStream) -> list[bytes]:
        try:
            first = await anext(chunks)
            self.started.set()
            await self.release.wait()
            return [first, *[chunk async for chunk in chunks]]
        finally:
            self.finished += 1

    @stream_endpoint
    async def overlap(self, chunks: ByteStream) -> str:
        read = asyncio.create_task(anext(chunks))
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="outstanding read"):
            await anext(chunks)
        await read
        return "rejected"

    @concurrent_endpoint
    async def ready(self):
        await self.started.wait()

    @endpoint
    async def unblock(self):
        self.release.set()

    @endpoint
    async def status(self):
        from monarch._src.actor.concurrent import _states
        from monarch._src.actor.stream import _sessions

        return self.finished, len(_sessions.get(self, {})), len(_states[self].records)

    @endpoint
    async def retained(self):
        from monarch._src.actor.stream import _sessions

        return self in _sessions

    @endpoint
    async def pid(self):
        return os.getpid()


@pytest.mark.parametrize("size", [0, -1, True, 1.5, "3", None])
def test_invalid_chunk_size(size):
    from monarch._src.actor.stream import StreamEndpoint

    client = StreamEndpoint(
        None,
        inspect.Signature(
            [inspect.Parameter("chunks", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        ),
        "chunks",
    )
    with pytest.raises(ValueError, match="positive integer"):
        client.stream({}, chunk_size=size)
    with pytest.raises(ValueError, match="positive integer"):
        client.stream_chunks(cast(Any, None), chunk_size=size)


def test_invalid_decorators():
    with pytest.raises(ValueError, match="async method"):
        stream_endpoint(cast(Any, lambda self, chunks: None))

    async def generator(self, chunks):
        yield b"x"

    with pytest.raises(ValueError, match="async method"):
        stream_endpoint(cast(Any, generator))

    async def extra(self, chunks, other):
        pass

    with pytest.raises(ValueError, match="exactly one"):
        stream_endpoint(cast(Any, extra))


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_stream_roundtrip_and_chunks():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", Consumer)
    router = procs.slice(processes=1).spawn("router", Router, consumer)
    try:
        obj = {"message": "abcd" * 1000, "nested": [1, None, b"abc"]}
        assert await consumer.load.stream(obj, chunk_size=17) == obj
        assert await router.load.stream(obj, chunk_size=100) == obj
        assert not hasattr(router.load, "broadcast")
        assert not hasattr(router.load, "call_one")
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
            assert (
                await router.collect.stream_chunks(source(), chunk_size=3) == expected
            )
        with pytest.raises(ValueError, match="exactly 1 actor"):
            await procs.spawn("multiple", Consumer).load.stream(1)
        assert await consumer.ping.call_one() == "pong"
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_stream_get():
    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", Consumer)
    try:
        assert consumer.load.stream([1, 2, 3], chunk_size=2).get() == [1, 2, 3]
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
        pid = await pid_actor.pid.call_one()
        assert pid != os.getpid()
        assert (
            await router.load.stream(Probe(pid), chunk_size=7)
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
        result = router.collect.stream_chunks(source()).as_asyncio()
        await consumer.ready.call_one()
        assert produced == [1]

        # A second isolated session can progress while the first waits.
        async def second():
            yield b"xyz"

        other = router.collect.stream_chunks(second()).as_asyncio()
        await consumer.unblock.call_one()
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
                assert await router.early.stream_chunks(source()) == b"abc"
            else:
                destination = router.collect if mode == "source" else router.fail
                with pytest.raises(
                    Exception,
                    match=f"{mode if mode == 'source' else 'destination'} boom",
                ):
                    await destination.stream_chunks(source())
            assert closed.is_set()
            assert produced == [1]
            assert await consumer.ping.call_one() == "pong"
            assert await router.ping.call_one() == "pong"

        async def bad():
            yield "not bytes"

        with pytest.raises(Exception, match="bytes elements"):
            await router.collect.stream_chunks(cast(Any, bad()))
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
        result = target.collect.stream_chunks(source()).as_asyncio()
        await pulled.wait()
        result.cancel()
        with pytest.raises(asyncio.CancelledError):
            await result
        assert closed.is_set()
        assert await consumer.status.call_one() == (1, 0, 0)
        assert await router.ping.call_one() == "pong"
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
        assert await consumer.overlap.stream_chunks(source()) == "rejected"
    finally:
        await procs.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_immediate_cancel_and_late_control():
    from monarch._src.actor.stream import _Cancel

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
        result = consumer.collect.stream_chunks(source()).as_asyncio()
        result.cancel()
        with pytest.raises(asyncio.CancelledError):
            await result
        # An already-completed/unknown session never leaves a tombstone.
        await consumer.collect._endpoint.call_one(_Cancel("unknown"))
        assert (await consumer.status.call_one())[1:] == (0, 0)
        assert not await consumer.retained.call_one()
    finally:
        await procs.stop()


class ShutdownConsumer(Actor):
    def __init__(self, path):
        self.path = path

    @stream_endpoint
    async def wait(self, chunks: ByteStream):
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
    result = consumer.wait.stream(started).as_asyncio()
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
    serialized = []
    loop = asyncio.get_running_loop()

    class Lazy:
        def __reduce__(self):
            assert asyncio.get_running_loop() is loop
            serialized.append(True)
            return str, ("lazy",)

    try:
        result = consumer.load.stream(Lazy())
        assert serialized == []
        assert await result == "lazy"
        assert serialized == [True]
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
            consumer.collect.stream_chunks(source()).get(timeout=1)
        assert closed == [True]
        assert consumer.status.call_one().get() == (1, 0, 0)
    finally:
        procs.stop().get()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_consumed_and_closed_streams_rejected():
    # Actor-local endpoints are descriptors, not client handles; use a real
    # destination handle to exercise forwarding validation.
    from monarch._src.actor.stream import StreamEndpoint

    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", Consumer)
    demand, _ = Channel[None].open()
    chunks = ByteStream(cast(Any, demand))
    try:
        await chunks.aclose()
        with pytest.raises(RuntimeError, match="closed"):
            consumer.collect.stream(chunks)
        consumed = ByteStream(cast(Any, demand))
        consumed._eof = True
        with pytest.raises(RuntimeError, match="consumed"):
            consumer.collect.stream_chunks(consumed)
        with pytest.raises(TypeError, match="async iterable"):
            StreamEndpoint(
                None,
                inspect.Signature(
                    [
                        inspect.Parameter(
                            "chunks", inspect.Parameter.POSITIONAL_OR_KEYWORD
                        )
                    ]
                ),
                "chunks",
            ).stream_chunks(cast(Any, [b"abc"]))
    finally:
        await procs.stop()


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
            endpoint = (
                getattr(consumer, mode)
                if mode in ("early", "ignore")
                else consumer.collect
            )
            result = endpoint.stream_chunks(source).as_asyncio()
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
        result = consumer.collect.stream_chunks(source()).as_asyncio()
        await pulled.wait()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(result, timeout=0.01)
        assert closed.is_set()
        assert await consumer.status.call_one() == (1, 0, 0)
    finally:
        await procs.stop()


class MetadataConsumer(Actor):
    @stream_endpoint
    async def mutable_default(self, payload: ByteStream, seen=[]):
        seen.append(True)
        return len(seen)

    @stream_endpoint
    async def consume(
        self, route_key, payload: ByteStream, count=2, *, marker="default", ready=None
    ):
        if ready is not None:
            ready.send((route_key, count, marker))
        return route_key, count, marker, [chunk async for chunk in payload]

    @stream_endpoint
    async def keyword(self, label="label", *, payload: ByteStream, flag=5):
        return label, flag, [chunk async for chunk in payload]

    @stream_endpoint
    async def positional(self, payload: ByteStream, /, count=3):
        return count, [chunk async for chunk in payload]

    @stream_endpoint
    async def variadic(self, payload: ByteStream, *extras, **metadata):
        return extras, metadata, [chunk async for chunk in payload]


class MetadataRouter(Actor):
    def __init__(self, destination):
        self.destination = destination

    @stream_endpoint
    async def route(
        self, payload: ByteStream, *, route_key, marker="router", ready=None
    ):
        return await self.destination.consume.stream(
            route_key, payload, marker=marker, ready=ready
        )


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_metadata_binding_and_forwarding_before_first_chunk():
    procs = this_host().spawn_procs(per_host={"processes": 2})
    consumer = procs.slice(processes=0).spawn("consumer", MetadataConsumer)
    router = procs.slice(processes=1).spawn("router", MetadataRouter, consumer)
    ready, receiver = Channel[tuple].open()
    allow_source = asyncio.Event()
    produced = []

    async def source():
        await allow_source.wait()
        produced.append(True)
        yield b"abc"

    try:
        result = router.route.stream_chunks(
            source(), route_key="training", ready=ready
        ).as_asyncio()
        assert await receiver.recv() == ("training", 2, "router")
        assert produced == []
        allow_source.set()
        assert await result == ("training", 2, "router", [b"abc"])

        async def chunks():
            yield b"abcdef"

        assert await consumer.consume.stream_chunks(
            "key", chunks(), 4, marker="explicit", chunk_size=3
        ) == ("key", 4, "explicit", [b"abc", b"def"])
        assert await consumer.keyword.stream_chunks(payload=chunks()) == (
            "label",
            5,
            [b"abcdef"],
        )
        assert await consumer.positional.stream_chunks(chunks()) == (3, [b"abcdef"])
        assert await consumer.variadic.stream_chunks(
            chunks(), "a", "b", metadata="value"
        ) == (("a", "b"), {"metadata": "value"}, [b"abcdef"])
        result = await consumer.keyword.stream("object", payload={"x": 1}, flag=8)
        assert result[:2] == ("object", 8)
        assert cloudpickle.loads(b"".join(result[2])) == {"x": 1}

        assert await consumer.mutable_default.stream_chunks(chunks()) == 1
        assert await consumer.mutable_default.stream_chunks(chunks()) == 2
        assert await consumer.mutable_default.stream_chunks(chunks(), seen=[]) == 1

        for call in (
            lambda: consumer.consume.stream("missing payload"),
            lambda: consumer.consume.stream("key", b"x", unknown=True),
            lambda: consumer.consume.stream("key", b"x", route_key="duplicate"),
            lambda: consumer.keyword.stream(b"x"),
            lambda: consumer.positional.stream(payload=b"x"),
        ):
            with pytest.raises(TypeError):
                call()
    finally:
        await procs.stop()


def test_stream_annotations_and_reserved_configuration():
    assert not hasattr(ByteStream, "read")
    assert not hasattr(ByteStream, "load")

    async def missing(self, payload, *, key):
        pass

    async def multiple(self, one: ByteStream, two: ByteStream):
        pass

    async def optional(self, payload: ByteStream = cast(Any, None)):
        pass

    async def variadic(self, *payload: ByteStream):
        pass

    async def reserved(self, payload: ByteStream, *, chunk_size):
        pass

    async def reserved_stream(self, chunk_size: ByteStream):
        pass

    for method in (missing, multiple):
        with pytest.raises(ValueError, match="exactly one ByteStream-annotated"):
            stream_endpoint(method)
    for method in (optional, variadic):
        with pytest.raises(ValueError, match="required, non-variadic"):
            stream_endpoint(method)
    for method in (reserved, reserved_stream):
        with pytest.raises(ValueError, match="reserves.*chunk_size"):
            stream_endpoint(method)

    async def forward(self, payload: "ByteStream", *, metadata):
        pass

    # Metadata annotations need not be present or resolvable.
    forward.__annotations__["metadata"] = "UnresolvedMetadata | 'ForwardRef'"
    stream_endpoint(forward)

    namespace: dict[str, Any] = {"ByteStream": ByteStream}
    exec(
        "from __future__ import annotations\n"
        'async def annotated(self, payload: "ByteStream", *, metadata: MissingType):\n'
        "    pass\n",
        namespace,
    )
    stream_endpoint(namespace["annotated"])


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_local_actor_definition_serializes_by_value():
    from monarch.actor import Actor, ByteStream, stream_endpoint

    class LocalActor(Actor):
        @stream_endpoint
        async def ingest(self, payload: ByteStream, *, key):
            return key, sum([len(chunk) async for chunk in payload])

    procs = this_host().spawn_procs(per_host={"processes": 1})
    consumer = procs.spawn("consumer", LocalActor)

    async def source():
        yield b"abc"

    try:
        assert await consumer.ingest.stream_chunks(source(), key="local") == (
            "local",
            3,
        )
    finally:
        await procs.stop()
