# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import functools
import http.client
import logging
import socket
import ssl
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterable
from urllib.parse import ParseResult, urlparse

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_DASHBOARD_PORT: int = 8265
# Hop-by-hop headers are connection-specific and must not be forwarded by a
# proxy (RFC 9110 sec. 7.6.1); the relay strips them in both directions.
_HOP_BY_HOP_HEADERS: frozenset[str] = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


@dataclass
class DashboardRelayServer:
    url: str
    server: ThreadingHTTPServer

    def serve_forever(self) -> None:
        self.server.serve_forever()

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class _ThreadingHTTPServerV6(ThreadingHTTPServer):
    # Bind over IPv6 so the Nest Dev Proxy edge (which reaches dev hosts over
    # IPv6) can connect; dual-stack still accepts IPv4 via v4-mapped addresses.
    address_family = socket.AF_INET6


def create_dashboard_relay_server(
    upstream_url: str,
    host: str = "::",
    port: int = DEFAULT_DASHBOARD_PORT,
    *,
    use_tls: bool = True,
) -> DashboardRelayServer:
    upstream = _parse_upstream_url(upstream_url)
    handler = _make_relay_handler(upstream)
    server_cls = _ThreadingHTTPServerV6 if ":" in host else ThreadingHTTPServer
    server = server_cls((host, port), handler)
    actual_port = server.server_port

    url = f"http://{_local_url_host(host)}:{actual_port}"
    meta_tls = _meta_tls() if use_tls else None
    if meta_tls is not None:
        tls_result = meta_tls.try_meta_tls_context()
        if tls_result is not None:
            ssl_ctx, tls_hostname = tls_result
            server.socket = ssl_ctx.wrap_socket(server.socket, server_side=True)
            url = meta_tls.nest_dev_proxy_url(tls_hostname, actual_port)

    return DashboardRelayServer(url=url, server=server)


def serve_dashboard_relay(
    upstream_url: str,
    host: str = "::",
    port: int = DEFAULT_DASHBOARD_PORT,
    *,
    use_tls: bool = True,
) -> None:
    relay = create_dashboard_relay_server(
        upstream_url=upstream_url,
        host=host,
        port=port,
        use_tls=use_tls,
    )
    _write_relay_banner(relay)
    try:
        relay.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopped Monarch dashboard relay.")
    finally:
        relay.shutdown()


def _write_relay_banner(relay: DashboardRelayServer) -> None:
    print(f"Monarch Dashboard: {relay.url}")
    print("Keep this process running while using the dashboard.")


def _meta_tls() -> Any | None:
    """Return the optional Meta TLS module, or None outside Meta-internal builds."""
    try:
        from monarch.monarch_dashboard.meta import tls
    except ImportError:
        return None
    return tls


def _local_url_host(host: str) -> str:
    if host in {"", "0.0.0.0", "::"}:
        return "localhost"
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def probe_dashboard_url(
    url: str,
    *,
    timeout: float = 3.0,
) -> None:
    parsed = _parse_upstream_url(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    conn = _upstream_connection(
        parsed,
        timeout=timeout,
    )
    try:
        conn.request("HEAD", path)
        response = conn.getresponse()
        response.read()
        # A 4xx still proves the dashboard is up and reachable; only a 5xx or a
        # transport error means it is not serving.
        if response.status >= 500:
            raise OSError(f"dashboard probe returned HTTP {response.status}")
    except http.client.HTTPException as error:
        raise OSError("dashboard probe failed") from error
    finally:
        conn.close()


def _parse_upstream_url(upstream_url: str) -> ParseResult:
    parsed = urlparse(upstream_url.rstrip("/"))
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"Unsupported dashboard upstream URL: {upstream_url!r}")
    return parsed


def _make_relay_handler(upstream: ParseResult) -> type[BaseHTTPRequestHandler]:
    class DashboardRelayHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            self._proxy()

        do_HEAD = do_GET
        do_POST = do_GET
        do_PUT = do_GET
        do_PATCH = do_GET
        do_DELETE = do_GET
        do_OPTIONS = do_GET

        def log_message(self, format: str, *args: object) -> None:
            logger.debug("dashboard relay: " + format, *args)

        def _proxy(self) -> None:
            body = self._read_request_body()
            conn = _upstream_connection(upstream)
            try:
                conn.request(
                    method=self.command,
                    url=self.path,
                    body=body,
                    headers=dict(_request_headers(self.headers.items(), upstream)),
                )
                response = conn.getresponse()
                response_body = response.read()
            except OSError as error:
                logger.warning("dashboard relay upstream request failed", exc_info=True)
                self.send_error(502, f"dashboard upstream unavailable: {error}")
                return
            finally:
                conn.close()

            self.send_response(response.status, response.reason)
            for key, value in _response_headers(response.getheaders()):
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response_body)

        def _read_request_body(self) -> bytes | None:
            content_length = self.headers.get("Content-Length")
            if content_length is None:
                return None
            return self.rfile.read(int(content_length))

    return DashboardRelayHandler


def _response_headers(headers: Iterable[tuple[str, str]]) -> Iterable[tuple[str, str]]:
    for key, value in headers:
        lowered = key.lower()
        if lowered in _HOP_BY_HOP_HEADERS or lowered == "content-length":
            continue
        yield key, value


def _request_headers(
    headers: Iterable[tuple[str, str]],
    upstream: ParseResult,
) -> Iterable[tuple[str, str]]:
    for key, value in headers:
        lowered = key.lower()
        if lowered in _HOP_BY_HOP_HEADERS or lowered == "host":
            continue
        yield key, value
    # Send the upstream's own Host so its virtual-host / TLS SNI routing
    # targets the dashboard rather than this relay.
    yield "Host", upstream.netloc


def _upstream_connection(
    upstream: ParseResult,
    *,
    timeout: float = 30.0,
) -> http.client.HTTPConnection:
    hostname = upstream.hostname
    if hostname is None:
        raise ValueError(f"Unsupported dashboard upstream URL: {upstream.geturl()!r}")
    port = upstream.port
    if upstream.scheme == "https":
        return http.client.HTTPSConnection(
            hostname,
            port=port,
            timeout=timeout,
            context=_upstream_ssl_context(),
        )
    return http.client.HTTPConnection(hostname, port=port, timeout=timeout)


@functools.cache
def _upstream_ssl_context() -> ssl.SSLContext:
    # Cached so the system trust store is loaded once, not per proxied request.
    return ssl.create_default_context()
