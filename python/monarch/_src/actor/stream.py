# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Experimental, demand-driven byte requests with one endpoint response."""

import asyncio
import contextlib
import functools
import inspect
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
    Generic,
    TypeVar,
)

import cloudpickle
from monarch._src.actor.actor_mesh import Channel, Port, PortReceiver
from monarch._src.actor.concurrent import concurrent_endpoint
from monarch._src.actor.endpoint import EndpointProperty, Propagator
from monarch._src.actor.future import Future
from monarch._src.actor.returns_future import returns_future

R = TypeVar("R")
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


@dataclass
class _Open:
    session: str
    demand: Port[Port[bytes | None]]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass
class _Cancel:
    session: str


@dataclass
class _Handler:
    task: asyncio.Task[Any]
    cancelled: bool = False


_sessions: weakref.WeakKeyDictionary[Any, dict[str, _Handler]] = (
    weakref.WeakKeyDictionary()
)


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


async def _serialize(message: Any) -> AsyncGenerator[bytes, None]:
    yield cloudpickle.dumps(message)


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


class StreamEndpoint(Generic[R]):
    """Client for one streamed request and one normal response.

    Work, including cloudpickle serialization, starts on first observation of
    the returned Monarch Future, on that observer's loop. ``get()`` supports
    synchronous callers. Cancelling an asyncio observation cancels the session
    and waits for cooperative remote cleanup; other observers share its result.
    Only singleton actor selections are accepted. Invocations may overlap.
    """

    def __init__(
        self, endpoint: Any, signature: inspect.Signature, stream_parameter: str
    ) -> None:
        self._endpoint = endpoint
        self._signature = signature
        self._stream_parameter = stream_parameter

    def stream(
        self, *args: Any, chunk_size: int = _DEFAULT_CHUNK_SIZE, **kwargs: Any
    ) -> Future[R]:
        """Serialize the marked argument, or forward an incoming ByteStream.

        Other arguments are ordinary metadata in the invocation envelope.
        Serialization holds the full pickle at the source. Byte slices may copy.
        The receiver gets an iterator and chooses how to interpret its bytes.
        ``chunk_size`` is reserved by this client interface.
        """
        _chunk_size(chunk_size)
        bound = self._signature.bind(*args, **kwargs)
        message = bound.arguments[self._stream_parameter]
        source = message if isinstance(message, ByteStream) else _serialize(message)
        return self._start(bound, source, chunk_size)

    def stream_chunks(
        self, *args: Any, chunk_size: int = _DEFAULT_CHUNK_SIZE, **kwargs: Any
    ) -> Future[R]:
        """Send an async byte producer in the marked argument's position.

        Other arguments are ordinary metadata. Oversized producer elements are
        split. Demand bounds intermediary buffering to a few chunks per hop.
        Owned iterators close on success, error, or cancellation.
        """
        _chunk_size(chunk_size)
        bound = self._signature.bind(*args, **kwargs)
        source = bound.arguments[self._stream_parameter]
        if not isinstance(source, AsyncIterable):
            raise TypeError("stream_chunks requires an async iterable")
        return self._start(bound, source, chunk_size)

    def _start(
        self,
        bound: inspect.BoundArguments,
        source: AsyncIterable[bytes],
        size: int,
    ) -> Future[R]:
        if isinstance(source, ByteStream) and (source._closed or source._eof):
            raise RuntimeError("ByteStream is closed or consumed")
        bound.arguments[self._stream_parameter] = None
        invocation = _Invocation()
        future = self._run(source, size, invocation, bound.args, bound.kwargs)
        result = cast(_StreamFuture[R], object.__new__(_StreamFuture))
        result._status = future._status
        result._invocation = invocation
        return result

    @returns_future
    async def _run(
        self,
        chunks: AsyncIterable[bytes],
        size: int,
        invocation: _Invocation,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
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
            # A terminal response can race an already-sent pull or data reply.
            # These private ports may discard those late packets safely.
            demand.return_undeliverable = False
            # call_one performs singleton validation before dispatch.
            reply = self._endpoint.call_one(_Open(session, demand, args, kwargs))
            response = cast(asyncio.Future[R], asyncio.ensure_future(reply))
            pump = asyncio.create_task(_pump(source, receiver, demand))
            done, _ = await asyncio.wait(
                (response, pump), return_when=asyncio.FIRST_COMPLETED
            )
            if pump in done:
                pump.result()
            return await asyncio.shield(response)
        except BaseException:
            if response is not None and not response.done():
                await self._endpoint.call_one(_Cancel(session))
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


class _StreamEndpointProperty(EndpointProperty[Any, Any]):
    def __init__(
        self,
        concurrent: EndpointProperty[Any, Any],
        signature: inspect.Signature,
        stream_parameter: str,
    ) -> None:
        super().__init__(
            concurrent._method,
            cast(Propagator, concurrent._propagator),
            concurrent._explicit_response_port,
            concurrent._instrument,
        )
        self._signature = signature
        self._stream_parameter = stream_parameter

    def _bind(self, endpoint: Any) -> StreamEndpoint[Any]:
        return StreamEndpoint(endpoint, self._signature, self._stream_parameter)


async def _serve(
    instance: Any,
    request: _Open | _Cancel,
    method: Callable[..., Awaitable[Any]],
    signature: inspect.Signature,
    stream_parameter: str,
) -> Any:
    if isinstance(request, _Cancel):
        sessions = _sessions.get(instance)
        handler = None if sessions is None else sessions.get(request.session)
        if handler is not None:
            handler.cancelled = True
            handler.task.cancel()
            await asyncio.gather(handler.task, return_exceptions=True)
        return None
    sessions = _sessions.setdefault(instance, {})
    task = asyncio.current_task()
    assert task is not None
    handler = _Handler(task)
    sessions[request.session] = handler
    stream = ByteStream(request.demand)
    try:
        bound = signature.bind(*request.args, **request.kwargs)
        bound.arguments[stream_parameter] = stream
        return await method(instance, *bound.args, **bound.kwargs)
    except asyncio.CancelledError:
        if handler.cancelled:
            raise _StreamCancelled("stream request cancelled") from None
        raise
    finally:
        await stream.aclose()
        del sessions[request.session]
        if not sessions:
            _sessions.pop(instance, None)


def stream_endpoint(
    method: Callable[..., Awaitable[R]],
) -> StreamEndpoint[R]:
    """Decorate an async method with exactly one ByteStream-annotated input.

    Other parameters carry ordinary metadata. The marked parameter is an async
    byte iterator at the receiver; the caller supplies an object to ``stream``
    or an async byte producer to ``stream_chunks``. The handler returns one
    normal response. Sessions overlap and interleave actor state at awaits.
    ``chunk_size`` is reserved for client chunk configuration and cannot be a
    method parameter. Python typing preserves the result type but cannot
    transform the marked parameter's client type in an arbitrary signature.
    """
    if not inspect.iscoroutinefunction(method):
        raise ValueError(
            "stream_endpoint requires an async method, not an async generator"
        )
    parameters = list(inspect.signature(method).parameters.values())
    if not parameters or parameters[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise ValueError("stream_endpoint requires an instance method")
    signature = inspect.signature(method).replace(parameters=parameters[1:])
    if "chunk_size" in signature.parameters:
        raise ValueError("stream_endpoint reserves the parameter name chunk_size")
    # Resolve only simple marker references. Metadata annotations may contain
    # unresolved names or arbitrary expressions; they need no evaluation here.
    marked = []
    for parameter in signature.parameters.values():
        annotation = parameter.annotation
        if isinstance(annotation, str):
            names = annotation.strip("\"'").split(".")
            if all(name.isidentifier() for name in names):
                annotation = method.__globals__.get(names[0])
                for name in names[1:]:
                    annotation = getattr(annotation, name, None)
        if annotation is ByteStream:
            marked.append(parameter)
    if len(marked) != 1:
        raise ValueError(
            "stream_endpoint requires exactly one ByteStream-annotated input"
        )
    parameter = marked[0]
    if (
        parameter.kind
        in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        )
        or parameter.default is not inspect.Parameter.empty
    ):
        raise ValueError(
            "the ByteStream input must be a required, non-variadic parameter"
        )
    stream_parameter = parameter.name

    @functools.wraps(method)
    async def serve(instance: Any, request: _Open | _Cancel) -> Any:
        return await _serve(instance, request, method, signature, stream_parameter)

    # The wire signature describes protocol messages, not the public body.
    serve.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [
            inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter("request", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ]
    )
    concurrent = cast(EndpointProperty[Any, Any], concurrent_endpoint(serve))
    return cast(
        StreamEndpoint[R],
        _StreamEndpointProperty(concurrent, signature, stream_parameter),
    )
