# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""HTTP endpoints for actors.

The server itself is Rust (``monarch_hyperactor::http``): it accepts
connections, parses and routes requests, and writes responses on Monarch's
runtime without the GIL. This module runs the handlers, as tasks on the event
loop of the actor that started the server.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
from collections.abc import AsyncIterable, Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, NamedTuple
from urllib.parse import parse_qs, unquote

from monarch._rust_bindings.monarch_hyperactor.http import (
    HttpBodyWriter,
    HttpRequest,
    HttpServer as _HttpServer,
    routable_host,
    SIGNAL_CLOSED,
    SIGNAL_DISCONNECTED,
    SIGNAL_DRAINED,
)
from monarch._src.actor.mpsc import Receiver
from monarch._src.actor.telemetry import span

_logger: logging.Logger = logging.getLogger(__name__)

_INTERNAL_ERROR = b'{"error":"internal server error"}'
_UNAVAILABLE = b'{"error":"server is stopping"}'
_JSON = "application/json"


class _Headers(Mapping[str, str]):
    """Request headers, looked up without regard to case."""

    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        # The server lower-cases names. A repeated header is joined, as
        # RFC 9110 section 5.3 allows a recipient to do.
        self._values: dict[str, str] = {}
        for name, value in pairs:
            previous = self._values.get(name)
            self._values[name] = value if previous is None else f"{previous}, {value}"

    def __getitem__(self, name: str) -> str:
        return self._values[name.lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return repr(self._values)


class Request:
    """An HTTP request received by an :class:`HttpServer`."""

    def __init__(self, raw: HttpRequest) -> None:
        self._raw = raw
        self._headers: _Headers | None = None
        self._path_params: dict[str, str] | None = None
        self._body: bytes | None = None

    @property
    def method(self) -> str:
        """The request method, for example ``"POST"``."""
        return self._raw.method

    @property
    def path(self) -> str:
        """The request path as the client sent it, still percent-encoded."""
        return self._raw.path

    @property
    def query(self) -> str:
        """The query string without the leading ``?``; empty if there is none."""
        return self._raw.query

    @property
    def query_params(self) -> dict[str, list[str]]:
        """The query string parsed into names and their decoded values."""
        return parse_qs(self._raw.query, keep_blank_values=True)

    @property
    def headers(self) -> Mapping[str, str]:
        """The request headers. Lookup ignores case."""
        if self._headers is None:
            self._headers = _Headers(self._raw.headers)
        return self._headers

    @property
    def path_params(self) -> dict[str, str]:
        """The decoded values of the route's ``{name}`` segments."""
        if self._path_params is None:
            self._path_params = {
                name: unquote(value) for name, value in self._raw.path_params
            }
        return self._path_params

    @property
    def body(self) -> bytes:
        """The request body."""
        if self._body is None:
            self._body = self._raw.body
        return self._body

    def text(self) -> str:
        """The request body decoded as UTF-8."""
        return self.body.decode("utf-8")

    def json(self) -> Any:
        """The request body parsed as JSON.

        Raises ``ValueError`` if the body is not valid JSON.
        """
        return json.loads(self.body)


def _header_list(
    headers: Mapping[str, str] | None, content_type: str | None
) -> list[tuple[str, str]]:
    pairs = [] if headers is None else list(headers.items())
    if content_type is not None and not any(
        name.lower() == "content-type" for name, _ in pairs
    ):
        pairs.append(("content-type", content_type))
    return pairs


class Response:
    """An HTTP response with a complete body.

    Args:
        body: The response body. A ``str`` is sent as UTF-8.
        status: The HTTP status code.
        headers: Response headers.
        content_type: The ``Content-Type`` header, unless ``headers`` already
            has one. Defaults to ``text/plain; charset=utf-8`` for a ``str``
            body and to no header for ``bytes``.
    """

    def __init__(
        self,
        body: bytes | str = b"",
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str | None = None,
    ) -> None:
        if isinstance(body, str):
            body = body.encode("utf-8")
            if content_type is None:
                content_type = "text/plain; charset=utf-8"
        self.body: bytes = bytes(body)
        self.status: int = status
        self.headers: list[tuple[str, str]] = _header_list(headers, content_type)

    @classmethod
    def json(
        cls,
        value: Any,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        """A response whose body is ``value`` serialized as JSON."""
        body = json.dumps(value, separators=(",", ":")).encode("utf-8")
        return cls(body, status=status, headers=headers, content_type=_JSON)


class StreamingResponse:
    """An HTTP response whose body is sent as it is produced.

    The server sends each chunk as soon as ``chunks`` yields it, and waits
    for the client to keep up before asking for more. Server-sent events are
    the usual case: pass ``content_type="text/event-stream"`` and yield
    ``"data: ...\\n\\n"`` strings.

    If the client disconnects, the handler's task is cancelled at the
    ``yield`` it is suspended on, so an async generator's ``finally`` blocks
    run. If ``chunks`` raises, the connection is closed without a clean end,
    so the client can tell the body is incomplete.

    Args:
        chunks: Yields the body. A ``str`` chunk is sent as UTF-8.
        status: The HTTP status code.
        headers: Response headers.
        content_type: The ``Content-Type`` header, unless ``headers`` already
            has one.
    """

    def __init__(
        self,
        chunks: AsyncIterable[bytes | str],
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str | None = None,
    ) -> None:
        self.chunks: AsyncIterable[bytes | str] = chunks
        self.status: int = status
        self.headers: list[tuple[str, str]] = _header_list(headers, content_type)


Handler = Callable[[Request], Awaitable[Response | StreamingResponse]]


class _Route(NamedTuple):
    method: str
    path: str
    handler: Handler

    @property
    def name(self) -> str:
        return f"{self.method} {self.path}"


@dataclass
class _InFlight:
    task: asyncio.Task[None]
    # Set when the response stream has room again; created by a handler that
    # streams.
    drained: asyncio.Event | None = None


def _respond_error(raw: HttpRequest, status: int, body: bytes) -> None:
    try:
        raw.respond(status, [("content-type", _JSON)], body)
    except RuntimeError:
        # The request was already answered, or its response had started.
        pass


def _actor_name() -> str:
    from monarch._src.actor.actor_mesh import _context

    ctx = _context.get(None)
    if ctx is None:
        return "unknown"
    return getattr(ctx.actor_instance, "name", None) or "unknown"


class HttpServer:
    """Serves HTTP requests with coroutines of the actor that owns it.

    Create the server and register its routes in the actor's ``__init__``,
    start it from an endpoint, and stop it in ``__cleanup__``::

        class Generator(Actor):
            def __init__(self) -> None:
                self.http = HttpServer(host="0.0.0.0")
                self.http.post("/v1/generate", self.generate)

            @endpoint
            async def serve(self) -> str:
                await self.http.start()
                return self.http.url

            async def generate(self, request: Request) -> Response:
                return Response.json({"echo": request.json()})

            async def __cleanup__(self, exc: Exception | None) -> None:
                await self.http.stop()

    A handler is a coroutine function that takes a :class:`Request` and
    returns a :class:`Response` or a :class:`StreamingResponse`. Each request
    runs as its own task on the actor's event loop, so handlers run
    concurrently with each other and with the actor's endpoints, share the
    actor's state without locks, and can call other actors' endpoints. Like an
    endpoint, a handler must not block the event loop.

    If a handler raises, the client receives a 500 response, the exception is
    logged, and the actor keeps running. If the client disconnects before the
    response is complete, the handler's task is cancelled.

    The server needs an asyncio event loop, so the actor must have at least
    one ``async def`` endpoint, and :meth:`start` and :meth:`stop` must be
    awaited on the actor's event loop.

    The server speaks plain HTTP and does not authenticate clients. It listens
    only on the loopback interface unless ``host`` says otherwise.

    Args:
        host: The address to listen on. ``"0.0.0.0"`` or ``"::"`` listens on
            every interface, which is what makes the server reachable from
            other hosts.
        port: The port to listen on. Zero picks a free port; see
            :attr:`port`.
        advertised_host: The host to put in :attr:`url`. By default this is
            ``host``, or, when listening on every interface, the address
            Monarch's own ``tcp`` transport advertises for this host.
        max_body_bytes: Requests with a larger body are answered with 413.
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        advertised_host: str | None = None,
        max_body_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if not 0 <= port <= 65535:
            raise ValueError(f"port must be between 0 and 65535, got {port}")
        if max_body_bytes < 0:
            raise ValueError("max_body_bytes must not be negative")
        self._host = host
        self._port = port
        self._advertised_host = advertised_host
        self._max_body_bytes = max_body_bytes
        self._routes: list[_Route] = []
        self._server: _HttpServer | None = None
        self._pump: asyncio.Task[None] | None = None
        self._in_flight: dict[int, _InFlight] = {}

    def route(self, method: str, path: str, handler: Handler) -> None:
        """Handle ``method`` requests for ``path`` with ``handler``.

        A ``{name}`` path segment matches any one segment and appears in
        :attr:`Request.path_params`. When several routes match, the one with
        the most literal segments handles the request. ``HEAD`` requests are
        handled by the ``GET`` route unless ``HEAD`` has its own.

        Routes must be registered before :meth:`start`.
        """
        if self._server is not None:
            raise RuntimeError("routes must be registered before the server starts")
        self._routes.append(_Route(method.upper(), path, handler))

    def get(self, path: str, handler: Handler) -> None:
        """Handle ``GET`` requests for ``path``; see :meth:`route`."""
        self.route("GET", path, handler)

    def post(self, path: str, handler: Handler) -> None:
        """Handle ``POST`` requests for ``path``; see :meth:`route`."""
        self.route("POST", path, handler)

    def put(self, path: str, handler: Handler) -> None:
        """Handle ``PUT`` requests for ``path``; see :meth:`route`."""
        self.route("PUT", path, handler)

    def delete(self, path: str, handler: Handler) -> None:
        """Handle ``DELETE`` requests for ``path``; see :meth:`route`."""
        self.route("DELETE", path, handler)

    async def start(self) -> None:
        """Bind the listening socket and begin serving.

        Raises ``OSError`` if the address cannot be bound and ``ValueError``
        if a route is malformed or registered twice.
        """
        if self._server is not None:
            raise RuntimeError("the HTTP server is already started")
        loop = asyncio.get_running_loop()
        server, receiver = _HttpServer.start(
            self._host,
            self._port,
            [(route.method, route.path) for route in self._routes],
            max_body_bytes=self._max_body_bytes,
            actor=_actor_name(),
        )
        self._server = server
        self._pump = loop.create_task(self._run(server, receiver))

    async def stop(self, grace: float = 1.0) -> None:
        """Stop serving.

        The server stops accepting connections at once. Requests already
        being handled get ``grace`` seconds to finish; after that their
        clients receive 503 and their tasks are cancelled. Returns when no
        handler is running, at most about a second after the grace period.
        Does nothing if the server is not started.

        The default suits ``__cleanup__``, which Monarch cancels after
        ``HYPERACTOR_CLEANUP_TIMEOUT`` (3 seconds by default). To let long
        requests finish, call ``stop`` with a longer grace period from an
        endpoint before stopping the actor.
        """
        server, pump = self._server, self._pump
        if server is None or pump is None:
            return
        server.shutdown(grace)
        try:
            # The pump ends when the server reports that it has closed, and
            # cancels whatever handlers are left.
            await asyncio.shield(pump)
        finally:
            if pump.done():
                self._server = None
                self._pump = None

    @property
    def port(self) -> int:
        """The port the server is listening on."""
        return self._started().port

    @property
    def url(self) -> str:
        """The base URL clients should use, for example ``http://10.0.0.5:8000``.

        Raises ``RuntimeError`` if the server listens on every interface and
        no address that other hosts can reach is found; pass
        ``advertised_host`` in that case.
        """
        server = self._started()
        host = self._advertised_host
        if host is None:
            host = server.host
            if ipaddress.ip_address(host).is_unspecified:
                try:
                    host = routable_host()
                except (OSError, RuntimeError) as error:
                    raise RuntimeError(
                        f"cannot tell which address reaches this host ({error}); "
                        "pass advertised_host to HttpServer"
                    ) from error
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{server.port}"

    def _started(self) -> _HttpServer:
        if self._server is None:
            raise RuntimeError("the HTTP server is not started")
        return self._server

    async def _run(
        self,
        server: _HttpServer,
        receiver: Receiver[HttpRequest | tuple[int, int]],
    ) -> None:
        """Take requests and signals from the server until it closes."""
        loop = asyncio.get_running_loop()
        wake = receiver._event()
        try:
            while True:
                try:
                    item = receiver.try_recv()
                except EOFError:
                    # The server went away without closing, as it does when
                    # the runtime shuts down.
                    return
                if item is None:
                    await wake.wait()
                    wake.clear()
                elif isinstance(item, HttpRequest):
                    task = loop.create_task(self._handle(item))
                    self._in_flight[item.id] = _InFlight(task)
                else:
                    kind, request_id = item
                    if kind == SIGNAL_CLOSED:
                        return
                    in_flight = self._in_flight.get(request_id)
                    if in_flight is None:
                        continue
                    if kind == SIGNAL_DISCONNECTED:
                        in_flight.task.cancel()
                    elif kind == SIGNAL_DRAINED and in_flight.drained is not None:
                        in_flight.drained.set()
        finally:
            # If this task is ending for any other reason than the server
            # closing, nothing would answer further requests: refuse them
            # instead. This does nothing once the server is shutting down.
            server.shutdown(0)
            # Once the channel has closed its descriptor stays readable, so
            # stop watching it.
            loop.remove_reader(wake._read_fd)
            leftover = [in_flight.task for in_flight in self._in_flight.values()]
            for task in leftover:
                task.cancel()
            await asyncio.gather(*leftover, return_exceptions=True)

    async def _handle(self, raw: HttpRequest) -> None:
        route = self._routes[raw.route]
        try:
            with span(route.name):
                response = await route.handler(Request(raw))
                if isinstance(response, StreamingResponse):
                    await self._stream(raw, response)
                elif isinstance(response, Response):
                    raw.respond(response.status, response.headers, response.body)
                else:
                    raise TypeError(
                        f"the handler for {route.name} returned "
                        f"{type(response).__name__}, not a Response or "
                        "StreamingResponse"
                    )
        except Exception:
            _logger.exception("HTTP handler for %s failed", route.name)
            _respond_error(raw, 500, _INTERNAL_ERROR)
        except BaseException:
            # Cancelled. If the client is still waiting, answer it now rather
            # than when this task's frames are collected.
            _respond_error(raw, 503, _UNAVAILABLE)
            raise
        finally:
            self._in_flight.pop(raw.id, None)

    async def _stream(self, raw: HttpRequest, response: StreamingResponse) -> None:
        writer: HttpBodyWriter | None = raw.respond_stream(
            response.status, response.headers
        )
        if writer is None:
            # The client has gone; this task is about to be cancelled.
            return
        drained = asyncio.Event()
        self._in_flight[raw.id].drained = drained
        chunks = response.chunks.__aiter__()
        try:
            async for chunk in chunks:
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                if writer.write(bytes(chunk)):
                    await drained.wait()
                    drained.clear()
        except BrokenPipeError:
            # The connection closed under us; cancellation follows.
            return
        except BaseException:
            writer.abort()
            raise
        else:
            writer.close()
        finally:
            aclose = getattr(chunks, "aclose", None)
            if aclose is not None:
                await aclose()
