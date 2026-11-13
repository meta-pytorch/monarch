# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Tests for HTTP endpoints served by an actor."""

import asyncio
import contextlib
import http.client
import json
import socket
import time
from collections.abc import AsyncIterator
from typing import Any, NamedTuple
from urllib.parse import urlsplit

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, ActorError, current_rank, endpoint, this_host
from monarch.actor.http import HttpServer, Request, Response, StreamingResponse

_MIB = 1024 * 1024


class _Reply(NamedTuple):
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


def _connection(url: str) -> http.client.HTTPConnection:
    parts = urlsplit(url)
    assert parts.hostname is not None
    return http.client.HTTPConnection(parts.hostname, parts.port, timeout=30)


def _fetch_blocking(
    url: str,
    method: str,
    path: str,
    body: bytes | list[bytes] | None,
    headers: dict[str, str],
) -> _Reply:
    connection = _connection(url)
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        reply_headers = {name.lower(): value for name, value in response.getheaders()}
        return _Reply(response.status, reply_headers, response.read())
    finally:
        connection.close()


async def _fetch(
    url: str,
    method: str = "GET",
    path: str = "/",
    *,
    body: bytes | list[bytes] | None = None,
    headers: dict[str, str] | None = None,
) -> _Reply:
    # A standard client on its own thread, so the event loop stays free to
    # run the handlers under test.
    return await asyncio.to_thread(
        _fetch_blocking, url, method, path, body, headers or {}
    )


async def _connect(
    url: str, method: str, path: str
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Send a request on a raw connection, for tests that control when the
    client reads or disconnects."""
    parts = urlsplit(url)
    reader, writer = await asyncio.open_connection(parts.hostname, parts.port)
    writer.write(
        f"{method} {path} HTTP/1.1\r\nhost: {parts.netloc}\r\n\r\n".encode("ascii")
    )
    await writer.drain()
    return reader, writer


async def _read_head(reader: asyncio.StreamReader) -> tuple[int, dict[str, str]]:
    head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
    status_line, *header_lines = head.strip().split("\r\n")
    headers = {}
    for line in header_lines:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    return int(status_line.split()[1]), headers


async def _read_chunk(reader: asyncio.StreamReader) -> bytes:
    """Read one piece of a chunked body; empty at its end."""
    size = int((await reader.readline()).strip(), 16)
    chunk = await reader.readexactly(size)
    await reader.readexactly(2)
    return chunk


async def _close(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


@contextlib.asynccontextmanager
async def _serving(server: HttpServer) -> AsyncIterator[str]:
    await server.start()
    try:
        yield server.url
    finally:
        await server.stop(grace=1)


async def _echo(request: Request) -> Response:
    return Response(
        request.body,
        content_type=request.headers.get("content-type", "application/octet-stream"),
    )


@pytest.mark.timeout(60)
async def test_json_round_trip() -> None:
    async def handler(request: Request) -> Response:
        return Response.json({"received": request.json(), "method": request.method})

    server = HttpServer()
    server.post("/v1/generate", handler)
    async with _serving(server) as url:
        assert url == f"http://127.0.0.1:{server.port}"
        reply = await _fetch(
            url, "POST", "/v1/generate", body=json.dumps({"prompt": [1, 2]}).encode()
        )
    assert reply.status == 200
    assert reply.headers["content-type"] == "application/json"
    assert reply.json() == {"received": {"prompt": [1, 2]}, "method": "POST"}


@pytest.mark.timeout(60)
async def test_request_fields() -> None:
    seen: list[Request] = []

    async def handler(request: Request) -> Response:
        seen.append(request)
        return Response("ok")

    server = HttpServer()
    server.put("/models/{model}/versions/{version}", handler)
    async with _serving(server) as url:
        reply = await _fetch(
            url,
            "PUT",
            "/models/a%20b/versions/7?tag=x&tag=y&empty=",
            body="héllo".encode(),
            headers={"X-Session-ID": "abc"},
        )
    assert (reply.status, reply.body) == (200, b"ok")
    assert reply.headers["content-type"] == "text/plain; charset=utf-8"
    (request,) = seen
    assert request.method == "PUT"
    assert request.path == "/models/a%20b/versions/7"
    assert request.path_params == {"model": "a b", "version": "7"}
    assert request.query == "tag=x&tag=y&empty="
    assert request.query_params == {"tag": ["x", "y"], "empty": [""]}
    assert request.headers["x-session-id"] == "abc"
    assert request.headers["X-Session-ID"] == "abc"
    assert request.headers.get("missing") is None
    assert request.text() == "héllo"


@pytest.mark.timeout(60)
async def test_requests_that_match_no_route_never_reach_a_handler() -> None:
    calls: list[str] = []

    async def handler(request: Request) -> Response:
        calls.append(request.method)
        return Response("body")

    server = HttpServer()
    server.get("/a", handler)
    server.post("/a", handler)
    async with _serving(server) as url:
        missing = await _fetch(url, "GET", "/missing")
        wrong_method = await _fetch(url, "DELETE", "/a")
        head = await _fetch(url, "HEAD", "/a")
    assert missing.status == 404
    assert missing.json() == {"error": "not found"}
    assert wrong_method.status == 405
    assert wrong_method.headers["allow"] == "GET, POST"
    # HEAD is answered by the GET route, without a body.
    assert (head.status, head.body) == (200, b"")
    assert calls == ["HEAD"]


@pytest.mark.timeout(60)
async def test_most_specific_route_wins() -> None:
    async def by_name(request: Request) -> Response:
        return Response(f"name={request.path_params['name']}")

    async def default(request: Request) -> Response:
        return Response("default")

    server = HttpServer()
    server.get("/models/{name}", by_name)
    server.get("/models/default", default)
    async with _serving(server) as url:
        assert (await _fetch(url, "GET", "/models/default")).body == b"default"
        assert (await _fetch(url, "GET", "/models/other")).body == b"name=other"


@pytest.mark.timeout(60)
async def test_handler_failure_is_a_500_and_the_server_keeps_serving() -> None:
    async def raises(request: Request) -> Response:
        raise ValueError("secret detail")

    async def wrong_type(request: Request) -> Response:
        return "not a response"  # type: ignore[return-value]

    async def bad_header(request: Request) -> Response:
        return Response("x", headers={"x-bad": "line\r\nbreak"})

    server = HttpServer()
    server.get("/raises", raises)
    server.get("/wrong-type", wrong_type)
    server.get("/bad-header", bad_header)
    server.post("/echo", _echo)
    async with _serving(server) as url:
        for path in ("/raises", "/wrong-type", "/bad-header"):
            reply = await _fetch(url, "GET", path)
            assert reply.status == 500, path
            # The client is not told what went wrong inside the handler.
            assert reply.json() == {"error": "internal server error"}, path
        assert (
            await _fetch(url, "POST", "/echo", body=b"still up")
        ).body == b"still up"


@pytest.mark.timeout(60)
async def test_cancelled_handler_answers_the_client() -> None:
    tasks: list[asyncio.Task[None]] = []
    started = asyncio.Event()

    async def handler(request: Request) -> Response:
        task = asyncio.current_task()
        assert task is not None
        tasks.append(task)
        started.set()
        await asyncio.Event().wait()
        return Response("unreachable")

    server = HttpServer()
    server.get("/hang", handler)
    async with _serving(server) as url:
        request = asyncio.create_task(_fetch(url, "GET", "/hang"))
        await asyncio.wait_for(started.wait(), timeout=30)
        # Something other than the client going away cancels the handler:
        # the client must not be left waiting for a response.
        tasks[0].cancel()
        reply = await asyncio.wait_for(request, timeout=10)
    assert reply.status == 503


@pytest.mark.timeout(60)
async def test_request_body_limit() -> None:
    server = HttpServer(max_body_bytes=1024)
    server.post("/echo", _echo)
    async with _serving(server) as url:
        at_limit = await _fetch(url, "POST", "/echo", body=b"x" * 1024)
        over_limit = await _fetch(url, "POST", "/echo", body=b"x" * 1025)
        # A list is sent with chunked encoding, so the server learns the
        # length only by reading the body.
        chunked_at_limit = await _fetch(
            url, "POST", "/echo", body=[b"x" * 512, b"y" * 512]
        )
        chunked_over_limit = await _fetch(
            url, "POST", "/echo", body=[b"x" * 512, b"y" * 513]
        )
    assert at_limit.status == 200
    assert over_limit.status == 413
    assert (chunked_at_limit.status, chunked_at_limit.body) == (
        200,
        b"x" * 512 + b"y" * 512,
    )
    assert chunked_over_limit.status == 413


@pytest.mark.timeout(60)
async def test_large_bodies_round_trip() -> None:
    server = HttpServer()
    server.post("/echo", _echo)
    payload = bytes(range(256)) * (8 * _MIB // 256)
    async with _serving(server) as url:
        reply = await _fetch(url, "POST", "/echo", body=payload)
    assert reply.status == 200
    assert reply.headers["content-length"] == str(len(payload))
    assert reply.body == payload


@pytest.mark.timeout(60)
async def test_connection_is_reused_and_closed_on_stop() -> None:
    server = HttpServer()
    server.post("/echo", _echo)
    await server.start()
    connection = _connection(server.url)

    def exchange(body: bytes) -> tuple[bytes, int]:
        connection.request("POST", "/echo", body=body)
        reply = connection.getresponse().read()
        return reply, connection.sock.getsockname()[1]

    try:
        first, first_port = await asyncio.to_thread(exchange, b"one")
        second, second_port = await asyncio.to_thread(exchange, b"two")
        assert (first, second) == (b"one", b"two")
        assert first_port == second_port
        # An idle connection does not hold up stopping.
        started = time.monotonic()
        await server.stop(grace=30)
        assert time.monotonic() - started < 10
    finally:
        connection.close()
        await server.stop(grace=0)


@pytest.mark.timeout(60)
async def test_requests_are_handled_concurrently() -> None:
    both_arrived = asyncio.Event()
    arrived = 0

    async def handler(request: Request) -> Response:
        nonlocal arrived
        arrived += 1
        if arrived == 2:
            both_arrived.set()
        # Neither request can finish until the other has started.
        await both_arrived.wait()
        return Response("done")

    server = HttpServer()
    server.get("/wait", handler)
    async with _serving(server) as url:
        replies = await asyncio.wait_for(
            asyncio.gather(_fetch(url, "GET", "/wait"), _fetch(url, "GET", "/wait")),
            timeout=30,
        )
    assert [reply.status for reply in replies] == [200, 200]


@pytest.mark.timeout(60)
async def test_streaming_response_sends_chunks_as_produced() -> None:
    proceed = asyncio.Event()

    async def events() -> AsyncIterator[bytes | str]:
        yield "data: first\n\n"
        # The client must see the first event while the handler is still
        # running.
        await proceed.wait()
        yield "data: second\n\n"
        yield ""
        yield b"data: third\n\n"

    async def handler(request: Request) -> StreamingResponse:
        return StreamingResponse(events(), content_type="text/event-stream")

    server = HttpServer()
    server.get("/events", handler)
    async with _serving(server) as url:
        reader, writer = await _connect(url, "GET", "/events")
        try:
            status, headers = await _read_head(reader)
            assert status == 200
            assert headers["content-type"] == "text/event-stream"
            assert headers["transfer-encoding"] == "chunked"
            assert await _read_chunk(reader) == b"data: first\n\n"
            proceed.set()
            assert await _read_chunk(reader) == b"data: second\n\n"
            assert await _read_chunk(reader) == b"data: third\n\n"
            assert await _read_chunk(reader) == b""
        finally:
            await _close(writer)


@pytest.mark.timeout(120)
async def test_streaming_response_waits_for_a_slow_client() -> None:
    chunk = b"x" * _MIB
    total = 64
    produced = 0

    async def body() -> AsyncIterator[bytes]:
        nonlocal produced
        for _ in range(total):
            produced += 1
            yield chunk

    async def handler(request: Request) -> StreamingResponse:
        return StreamingResponse(body())

    server = HttpServer()
    server.get("/big", handler)
    async with _serving(server) as url:
        reader, writer = await _connect(url, "GET", "/big")
        try:
            assert (await _read_head(reader))[0] == 200
            # While the client reads nothing, the handler is held back rather
            # than queueing the whole body in memory.
            await asyncio.sleep(1)
            assert 0 < produced < total // 2
            received = 0
            while piece := await _read_chunk(reader):
                received += len(piece)
            assert received == total * _MIB
            assert produced == total
        finally:
            await _close(writer)


@pytest.mark.timeout(60)
async def test_failed_stream_does_not_end_cleanly() -> None:
    async def body() -> AsyncIterator[bytes]:
        yield b"partial"
        raise RuntimeError("generation failed")

    async def handler(request: Request) -> StreamingResponse:
        return StreamingResponse(body())

    server = HttpServer()
    server.get("/broken", handler)
    server.post("/echo", _echo)
    async with _serving(server) as url:
        reader, writer = await _connect(url, "GET", "/broken")
        try:
            assert (await _read_head(reader))[0] == 200
            assert await _read_chunk(reader) == b"partial"
            # The connection closes without the terminating chunk, so the
            # client can tell the body is incomplete.
            with contextlib.suppress(ConnectionError):
                assert await reader.read() == b""
        finally:
            await _close(writer)
        assert (await _fetch(url, "POST", "/echo", body=b"ok")).body == b"ok"


@pytest.mark.timeout(60)
async def test_client_disconnect_cancels_the_handler() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(request: Request) -> Response:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return Response("unreachable")

    server = HttpServer()
    server.get("/hang", handler)
    async with _serving(server) as url:
        _, writer = await _connect(url, "GET", "/hang")
        await asyncio.wait_for(started.wait(), timeout=30)
        await _close(writer)
        await asyncio.wait_for(cancelled.wait(), timeout=30)


@pytest.mark.timeout(60)
async def test_client_disconnect_stops_a_stream() -> None:
    closed = asyncio.Event()

    async def body() -> AsyncIterator[bytes]:
        try:
            while True:
                yield b"tick"
                await asyncio.sleep(0.01)
        finally:
            closed.set()

    async def handler(request: Request) -> StreamingResponse:
        return StreamingResponse(body())

    server = HttpServer()
    server.get("/ticks", handler)
    async with _serving(server) as url:
        reader, writer = await _connect(url, "GET", "/ticks")
        assert (await _read_head(reader))[0] == 200
        assert await _read_chunk(reader) == b"tick"
        await _close(writer)
        await asyncio.wait_for(closed.wait(), timeout=30)


@pytest.mark.timeout(60)
async def test_stop_lets_requests_in_flight_finish() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(request: Request) -> Response:
        started.set()
        await release.wait()
        return Response("finished")

    server = HttpServer()
    server.get("/slow", handler)
    await server.start()
    url = server.url
    request = asyncio.create_task(_fetch(url, "GET", "/slow"))
    await asyncio.wait_for(started.wait(), timeout=30)
    stopping = asyncio.create_task(server.stop(grace=30))
    await asyncio.sleep(0.2)
    assert not stopping.done()
    # No new connection is accepted while the old request drains.
    with pytest.raises(OSError):
        await _connect(url, "GET", "/slow")
    release.set()
    reply = await request
    assert (reply.status, reply.body) == (200, b"finished")
    await asyncio.wait_for(stopping, timeout=30)


@pytest.mark.timeout(60)
async def test_stop_fails_requests_still_running_after_the_grace_period() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(request: Request) -> Response:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return Response("unreachable")

    server = HttpServer()
    server.get("/hang", handler)
    await server.start()
    request = asyncio.create_task(_fetch(server.url, "GET", "/hang"))
    await asyncio.wait_for(started.wait(), timeout=30)
    began = time.monotonic()
    await server.stop(grace=0.2)
    assert time.monotonic() - began < 10
    assert (await request).status == 503
    assert cancelled.is_set()


@pytest.mark.timeout(60)
async def test_server_restarts_after_stop() -> None:
    server = HttpServer()
    server.post("/echo", _echo)
    for body in (b"first", b"second"):
        async with _serving(server) as url:
            assert (await _fetch(url, "POST", "/echo", body=body)).body == body
    with pytest.raises(RuntimeError):
        _ = server.url
    # Stopping a stopped server does nothing.
    await server.stop()


@pytest.mark.timeout(60)
async def test_misuse_is_reported() -> None:
    with pytest.raises(ValueError):
        HttpServer(port=70000)

    for method, path in (("GET", "no-slash"), ("GET", "/{}"), ("G T", "/ok")):
        server = HttpServer()
        server.route(method, path, _echo)
        with pytest.raises(ValueError):
            await server.start()

    duplicate = HttpServer()
    duplicate.get("/items/{a}", _echo)
    duplicate.get("/items/{b}", _echo)
    with pytest.raises(ValueError):
        await duplicate.start()

    server = HttpServer()
    with pytest.raises(RuntimeError):
        _ = server.port
    async with _serving(server):
        with pytest.raises(RuntimeError):
            await server.start()
        with pytest.raises(RuntimeError):
            server.get("/late", _echo)
        taken = HttpServer(port=server.port)
        with pytest.raises(OSError):
            await taken.start()


@pytest.mark.timeout(60)
async def test_url_names_a_host_other_machines_can_use() -> None:
    explicit = HttpServer(host="0.0.0.0", advertised_host="2001:db8::1")
    explicit.post("/echo", _echo)
    async with _serving(explicit) as url:
        assert url == f"http://[2001:db8::1]:{explicit.port}"
        # Listening on every interface includes loopback.
        local = f"http://127.0.0.1:{explicit.port}"
        assert (await _fetch(local, "POST", "/echo", body=b"hi")).body == b"hi"

    discovered = HttpServer(host="0.0.0.0")
    await discovered.start()
    try:
        # Whether this machine's host name resolves to a routable address
        # depends on where the test runs.
        try:
            host = urlsplit(discovered.url).hostname
        except RuntimeError as error:
            assert "advertised_host" in str(error)
        else:
            assert host not in ("0.0.0.0", "127.0.0.1", "localhost")
    finally:
        await discovered.stop()


class Backend(Actor):
    @endpoint
    async def double(self, value: int) -> int:
        return 2 * value


class Frontend(Actor):
    """Serves HTTP from its own state and from another actor."""

    def __init__(self, backend: Backend | None = None) -> None:
        self.backend = backend
        self.requests = 0
        self.http = HttpServer()
        self.http.get("/rank", self.rank)
        self.http.post("/double", self.double)

    @endpoint
    async def serve(self) -> str:
        await self.http.start()
        return self.http.url

    @endpoint
    async def request_count(self) -> int:
        return self.requests

    async def rank(self, request: Request) -> Response:
        self.requests += 1
        return Response.json(dict(current_rank()))

    async def double(self, request: Request) -> Response:
        self.requests += 1
        assert self.backend is not None
        doubled = await self.backend.double.call_one(request.json()["value"])
        return Response.json({"value": doubled})

    async def __cleanup__(self, exc: Exception | None) -> None:
        await self.http.stop(grace=1)


class Client(Actor):
    @endpoint
    async def post_json(self, url: str, path: str, value: object) -> object:
        reply = await _fetch(url, "POST", path, body=json.dumps(value).encode())
        return reply.status, reply.json()


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_actor_serves_http_to_another_process() -> None:
    procs = this_host().spawn_procs(per_host={"procs": 3})
    try:
        backend = procs.slice(procs=0).spawn("backend", Backend)
        frontend = procs.slice(procs=1).spawn("frontend", Frontend, backend)
        client = procs.slice(procs=2).spawn("client", Client)
        url = await frontend.serve.call_one()
        # The handler runs in the frontend's process, calls the backend actor,
        # and shares state with the frontend's endpoints.
        assert await client.post_json.call_one(url, "/double", {"value": 21}) == (
            200,
            {"value": 42},
        )
        assert await frontend.request_count.call_one() == 1

        # Stopping the actor runs __cleanup__, which closes the server.
        await frontend.stop()
        with pytest.raises(OSError):
            await _connect(url, "GET", "/rank")
    finally:
        await procs.stop()


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_each_rank_of_a_mesh_serves_its_own_port() -> None:
    procs = this_host().spawn_procs(per_host={"procs": 2})
    try:
        frontends = procs.spawn("frontends", Frontend)
        urls = await frontends.serve.call()
        ranks = {}
        for point, url in urls.items():
            reply = await _fetch(url, "GET", "/rank")
            ranks[point["procs"]] = reply.json()["procs"]
        assert ranks == {0: 0, 1: 1}
        assert len({url for _, url in urls.items()}) == 2

        # Starting a started server fails the call, not the actor.
        with pytest.raises(ActorError):
            await frontends.serve.call()
        counts = await frontends.request_count.call()
        assert sum(count for _, count in counts.items()) == 2
    finally:
        await procs.stop()
