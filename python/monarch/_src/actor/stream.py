# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""One demand-driven byte stream per raw-message request."""

import asyncio
import contextlib
import uuid
import weakref
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    cast,
    TypeVar,
)

from monarch._src.actor.actor_mesh import Channel, Port, PortReceiver
from monarch._src.actor.future import Future
from monarch._src.actor.raw import MessageEnvelope, send_message
from monarch._src.actor.returns_future import returns_future
from monarch._src.actor.tasks import spawn_actor_task

R = TypeVar("R")
PROTOCOL = "monarch.byte-stream.v1"
_DEFAULT_CHUNK_SIZE = 64 * 1024


async def _recv(receiver: PortReceiver[R], wake: Port[Any]) -> R:
    pending = receiver.recv().as_asyncio()
    try:
        return await asyncio.shield(pending)
    except asyncio.CancelledError:
        # Port receives run on Tokio and outlive a cancelled asyncio observer.
        # Wake and settle that receive before releasing the private channel.
        wake.send(None)
        with contextlib.suppress(Exception):
            await asyncio.shield(pending)
        raise


class ByteStream(AsyncIterator[bytes]):
    """A single-consumer async byte iterator received by a stream endpoint.

    Only one read may be outstanding. Empty chunks are data; EOF is stable.
    A stream belongs to its invocation and cannot be reused after it closes.
    """

    def __init__(self, demand: Port[Port[bytes | None]]) -> None:
        self._demand = demand
        self._response, self._receiver = Channel[bytes | None].open()
        self._response.return_undeliverable = False
        self._reading = False
        self._eof = False
        self._closed = False

    def __aiter__(self) -> "ByteStream":
        return self

    async def __anext__(self) -> bytes:
        if self._eof:
            raise StopAsyncIteration
        if self._closed:
            raise RuntimeError("ByteStream is closed")
        if self._reading:
            raise RuntimeError("ByteStream already has an outstanding read")
        self._reading = True
        try:
            self._demand.send(self._response)
            chunk = await _recv(self._receiver, self._response)
            if chunk is None:
                self._eof = True
                raise StopAsyncIteration
            return chunk
        except BaseException:
            self._closed = True
            raise
        finally:
            self._reading = False

    async def aclose(self) -> None:
        """Close this invocation's input and reject further reads."""
        self._closed = True


class _StreamCancelled(Exception):
    pass


class _Invocation:
    def __init__(self) -> None:
        self.task: asyncio.Task[Any] | None = None
        self.cancelled = False

    def cancel(self) -> None:
        if self.cancelled:
            return
        self.cancelled = True
        if self.task is not None:
            self.task.cancel()


class _StreamObservation(asyncio.Task[R]):
    def __init__(self, observed: asyncio.Future[R], invocation: _Invocation) -> None:
        self._invocation = invocation

        async def observe() -> R:
            return await observed

        super().__init__(observe())

    def cancel(self, msg: Any = None) -> bool:
        # Cancelling the observer task before its first step would skip its
        # cleanup entirely. Cancel the owned invocation instead and let its
        # terminal settlement complete this observation, including that race.
        if self.done():
            return False
        self._invocation.cancel()
        return True


class _StreamFuture(Future[R]):
    _invocation: _Invocation

    def as_asyncio(self) -> asyncio.Future[R]:
        return _StreamObservation(super().as_asyncio(), self._invocation)


def _chunk_size(size: int) -> None:
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise ValueError("chunk_size must be a positive integer")


async def _chunks(
    source: AsyncIterable[bytes], size: int
) -> AsyncGenerator[bytes, None]:
    async for chunk in source:
        if not isinstance(chunk, bytes):
            raise TypeError("stream_chunks requires bytes elements")
        if not chunk:
            yield chunk
        else:
            for offset in range(0, len(chunk), size):
                yield chunk[offset : offset + size]


async def _pump(
    source: AsyncIterator[bytes],
    demand: PortReceiver[Port[bytes | None]],
    wake: Port[Port[bytes | None]],
) -> None:
    while True:
        response = await _recv(demand, wake)
        try:
            chunk = await anext(source)
        except StopAsyncIteration:
            response.send(None)
            return
        except Exception as error:
            response.exception(error)
            return
        response.send(chunk)


def request(
    target: Any,
    chunks: AsyncIterable[bytes],
    *,
    metadata: Any = None,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> Future[Any]:
    """Request one streamed operation on a singleton raw actor selection.

    The source and sending start on first observation, on the observing loop.
    Synchronous callers may use get(). Metadata is available before any chunks.
    Cancelling an asyncio observation cancels the session and waits for its
    cooperative terminal reply, including through forwarding calls.
    """
    _chunk_size(chunk_size)
    if not isinstance(chunks, AsyncIterable):
        raise TypeError("request requires an async byte iterable")
    if isinstance(chunks, ByteStream) and (chunks._closed or chunks._eof):
        raise RuntimeError("ByteStream is closed or consumed")
    invocation = _Invocation()
    future = _request(target, chunks, chunk_size, metadata, invocation)
    result = cast(_StreamFuture[Any], object.__new__(_StreamFuture))
    result._status = future._status
    result._invocation = invocation
    return result


@returns_future
async def _request(
    target: Any,
    chunks: AsyncIterable[bytes],
    size: int,
    metadata: Any,
    invocation: _Invocation,
) -> Any:
    invocation.task = asyncio.current_task()
    iterator = aiter(chunks)
    source = _chunks(iterator, size)
    session = uuid.uuid4().hex
    response: asyncio.Future[Any] | None = None
    pump: asyncio.Task[None] | None = None
    try:
        if invocation.cancelled:
            raise asyncio.CancelledError
        demand, receiver = Channel[Port[bytes | None]].open()
        demand.return_undeliverable = False
        reply, replies = Channel[Any].open(once=True)
        send_message(
            target,
            MessageEnvelope(
                {
                    "protocol": PROTOCOL,
                    "operation": "open",
                    "session": session,
                    "demand": demand,
                    "metadata": metadata,
                },
                reply_port=reply,
            ),
        )
        response = replies.recv().as_asyncio()
        pump = asyncio.create_task(_pump(source, receiver, demand))
        done, _ = await asyncio.wait(
            (response, pump), return_when=asyncio.FIRST_COMPLETED
        )
        if pump in done:
            pump.result()
        return await asyncio.shield(response)
    except BaseException:
        if response is not None and not response.done():
            send_message(
                target,
                MessageEnvelope(
                    {"protocol": PROTOCOL, "operation": "cancel", "session": session}
                ),
            )
            with contextlib.suppress(Exception):
                await asyncio.shield(response)
        raise
    finally:
        if pump is not None:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)
        await source.aclose()
        close = getattr(iterator, "aclose", None)
        if close is not None:
            await close()
        invocation.task = None


@dataclass
class _Session:
    task: asyncio.Task[Any] | None = None
    started: bool = False
    cancelled: bool = False


_sessions: weakref.WeakKeyDictionary[Any, dict[str, _Session]] = (
    weakref.WeakKeyDictionary()
)


async def _serve(
    actor: Any,
    message: MessageEnvelope,
    callback: Callable[[ByteStream, Any], Awaitable[Any]],
    record: _Session,
) -> None:
    session = message.headers["session"]
    record.started = True
    chunks = ByteStream(message.headers["demand"])
    reply = message.reply_port
    try:
        if record.cancelled:
            raise asyncio.CancelledError
        result = await callback(chunks, message.headers.get("metadata"))
        reply.send(result)
    except asyncio.CancelledError:
        reason = (
            "stream request cancelled" if record.cancelled else "stream actor stopped"
        )
        reply.exception(_StreamCancelled(reason))
    except Exception as error:
        reply.exception(error)
    finally:
        await chunks.aclose()
        sessions = _sessions[actor]
        del sessions[session]
        if not sessions:
            _sessions.pop(actor, None)


async def handle(
    actor: Any,
    message: MessageEnvelope,
    callback: Callable[[ByteStream, Any], Awaitable[Any]],
) -> None:
    """Dispatch this library's open/cancel controls and promptly return.

    The callback runs as an actor-owned task and returns one ordinary Port
    response. Sessions overlap; callbacks can interleave state at awaits. Raw
    delivery itself has no response or concurrency convention. Unknown session
    cancellation is harmless and creates no retained state.
    """
    if message.headers.get("protocol") != PROTOCOL:
        raise ValueError("unknown byte stream protocol")
    session = message.headers["session"]
    operation = message.headers["operation"]
    if operation == "cancel":
        sessions = _sessions.get(actor)
        record = None if sessions is None else sessions.get(session)
        if record is not None:
            record.cancelled = True
            if record.started and record.task is not None:
                record.task.cancel()
        return
    if operation != "open" or message.reply_port is None:
        raise ValueError("invalid byte stream operation or missing reply port")
    if not isinstance(message.headers.get("demand"), Port):
        raise ValueError("byte stream open requires a demand Port")
    sessions = _sessions.setdefault(actor, {})
    if session in sessions:
        raise ValueError("duplicate byte stream session")
    record = _Session()
    sessions[session] = record
    task = spawn_actor_task(
        actor, _serve(actor, message, callback, record), name="byte stream"
    )
    record.task = task

    def finish_unstarted(done: asyncio.Task[Any]) -> None:
        if not record.started:
            message.reply_port.exception(_StreamCancelled("stream actor stopped"))
            sessions.pop(session, None)
            if not sessions:
                _sessions.pop(actor, None)

    task.add_done_callback(finish_unstarted)
