# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final

from monarch._rust_bindings.monarch_hyperactor.pympsc import Receiver

SIGNAL_DISCONNECTED: int
"""Signal kind: the request's client went away before its response was complete."""

SIGNAL_DRAINED: int
"""Signal kind: the request's response stream has room again."""

SIGNAL_CLOSED: int
"""Signal kind: the server has stopped and sends nothing more."""

@final
class HttpBodyWriter:
    """Writes a streamed response body."""

    def write(self, chunk: bytes) -> bool:
        """Queue `chunk` for the client.

        Returns `True` if the caller must wait for this request's
        `SIGNAL_DRAINED` before writing more. Raises `BrokenPipeError` if the
        body can no longer be written.
        """
        ...

    def close(self) -> None:
        """End the body normally."""
        ...

    def abort(self) -> None:
        """Give up on the body: the connection is closed without a clean end."""
        ...

@final
class HttpRequest:
    """A request waiting for an answer. It can be answered once."""

    @property
    def id(self) -> int:
        """Identifies this request in the signals that follow it."""
        ...

    @property
    def route(self) -> int:
        """Index of the matched route in the list the server was started with."""
        ...

    @property
    def method(self) -> str: ...
    @property
    def path(self) -> str:
        """The path as sent, still percent-encoded."""
        ...

    @property
    def query(self) -> str:
        """The query string as sent, without the leading `?`; empty if absent."""
        ...

    @property
    def headers(self) -> list[tuple[str, str]]:
        """Header names (lower-case) and values, in the order received."""
        ...

    @property
    def path_params(self) -> list[tuple[str, str]]:
        """Names and values captured by the route's `{name}` segments, still
        percent-encoded."""
        ...

    @property
    def body(self) -> bytes: ...
    def respond(self, status: int, headers: list[tuple[str, str]], body: bytes) -> bool:
        """Answer with a complete body. Returns `False` if the client has gone."""
        ...

    def respond_stream(
        self, status: int, headers: list[tuple[str, str]]
    ) -> HttpBodyWriter | None:
        """Answer with a body written in pieces. Returns `None` if the client
        has gone."""
        ...

@final
class HttpServer:
    """A running HTTP server. Dropping it stops the server at once."""

    @staticmethod
    def start(
        host: str,
        port: int,
        routes: list[tuple[str, str]],
        *,
        max_body_bytes: int,
        actor: str,
    ) -> tuple[HttpServer, Receiver[HttpRequest | tuple[int, int]]]:
        """Bind `host:port` and start serving `routes`, a list of
        `(method, path pattern)`.

        Returns the server and the receiver of its events: an `HttpRequest`
        for each request that matches a route, and a `(kind, request_id)`
        tuple for each signal.
        """
        ...

    @property
    def host(self) -> str:
        """The address the server is bound to."""
        ...

    @property
    def port(self) -> int:
        """The port the server is bound to."""
        ...

    def routable_host(self) -> str:
        """An address of this host that other hosts can use to reach this
        server when bound to all interfaces."""
        ...

    def shutdown(self, grace_seconds: float) -> None:
        """Stop accepting connections and give requests in flight
        `grace_seconds` to finish. Later calls do nothing."""
        ...
