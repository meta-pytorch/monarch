# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Flask application factory for the Monarch Dashboard.

Creates and configures the Flask app, registers the API blueprint,
initialises the database layer pointing at the fake data SQLite file,
and serves the React frontend build as static files.
"""

import logging
import os
import socket
import ssl
import threading

from flask import Flask, send_from_directory
from monarch.monarch_dashboard import _PKG

from . import db
from .db import DBAdapter
from .routes import api

logger = logging.getLogger(__name__)


def create_app(adapter: DBAdapter) -> Flask:
    """Build a configured Flask application.

    Args:
        adapter: A DBAdapter instance for data access (e.g. SQLiteAdapter
            for local dev, QueryEngineAdapter for production).
    """
    build_dir = str(_PKG / "frontend" / "build")

    app = Flask(
        __name__,
        static_folder=os.path.join(build_dir, "static")
        if os.path.isdir(build_dir)
        else None,
    )

    db.set_adapter(adapter)
    app.register_blueprint(api)

    # Serve the React frontend at / (catch-all for client-side routing).
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def serve_frontend(path):
        if not os.path.isdir(build_dir):
            return {"error": "Frontend not built. API available at /api/"}, 404
        if path and os.path.isfile(os.path.join(build_dir, path)):
            return send_from_directory(build_dir, path)
        return send_from_directory(build_dir, "index.html")

    return app


def start_dashboard(
    adapter: DBAdapter,
    port: int = 8265,
    host: str = "::",
) -> dict:
    """Start the dashboard server in a daemon thread.

    The dashboard runs in-process because telemetry data lives entirely
    in-memory as DataFusion MemTables. There is no on-disk database, so
    the dashboard must share the process to access the QueryEngine.

    On devvm / DevGPU / OnDemand hosts with a local TLS cert, the
    server uses HTTPS and advertises a Nest Dev Proxy URL for direct
    browser access without port forwarding.

    Tupperware / MAST hosts are **not supported**: the Nest Dev Proxy
    edge does not route to ``.tw.fbinfra.net`` task pods. On those
    hosts the dashboard falls back to plain HTTP on localhost; use the
    mesh admin TUI (``monarch-tui``) from a devvm instead.

    Args:
        adapter: A DBAdapter instance for data access.
        port: HTTP port to listen on.
        host: Bind address. Defaults to ``"::"`` so the Nest Dev Proxy
            (which reaches the devvm over IPv6) can connect. Relies on
            the Linux kernel default ``IPV6_V6ONLY=0`` for dual-stack;
            IPv4 clients (including ``localhost``) still work via
            v4-mapped addresses.

    Returns:
        A dict with keys: ``url`` (external, browser-openable), ``local_url``
        (in-process URL matching the cert SAN), ``port``, ``pid`` (always
        None), ``handle`` (Thread).

    Raises:
        OSError: If the port is already in use.
    """
    # Check port availability before starting the thread so failures
    # are raised to the caller instead of silently killing the thread.
    # Resolve host to the correct address family so IPv4 ("127.0.0.1"),
    # IPv6 ("::"), and hostname forms all work.
    family, _, _, _, sockaddr = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)[
        0
    ]
    with socket.socket(family, socket.SOCK_STREAM) as s:
        try:
            s.bind(sockaddr)
        except OSError:
            logger.error("Dashboard failed to start: port %d is unavailable", port)
            raise

    ssl_ctx: ssl.SSLContext | None = None
    tls_hostname: str | None = None
    try:
        from monarch.monarch_dashboard.meta.tls import (
            nest_dev_proxy_url,
            try_meta_tls_context,
        )

        result = try_meta_tls_context()
        if result is not None:
            ssl_ctx, tls_hostname = result
    except ImportError:
        pass

    if ssl_ctx is not None and tls_hostname is not None:
        url = nest_dev_proxy_url(tls_hostname, port)
        local_url = f"https://{tls_hostname}:{port}"
    else:
        url = f"http://localhost:{port}"
        local_url = url

    app = create_app(adapter)
    # Suppress per-request werkzeug logs (they are noisy in production).
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    thread = threading.Thread(
        target=lambda: app.run(
            host=host, port=port, debug=False, use_reloader=False, ssl_context=ssl_ctx
        ),
        daemon=True,
        name="monarch-dashboard",
    )
    thread.start()
    logger.info("Monarch Dashboard running at %s", url)

    return {
        "url": url,
        "local_url": local_url,
        "port": port,
        "pid": None,
        "handle": thread,
    }
