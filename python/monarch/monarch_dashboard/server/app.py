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

import atexit
import logging
import os
import ssl
import threading
from contextlib import ExitStack
from importlib.resources import as_file

from flask import Flask, send_from_directory
from monarch.monarch_dashboard import _PKG
from werkzeug.serving import make_server

from . import db
from .db import DBAdapter
from .routes import api

logger = logging.getLogger(__name__)


def _frontend_build_dir() -> str:
    """Resolve the bundled React build directory to a real filesystem path.

    ``importlib.resources`` exposes packaged data as ``Traversable`` objects
    whose backing store may be a zip archive (e.g. FastZIP PARs) rather than a
    real directory; their ``str`` form is not a path that ``os.path`` or
    Flask's ``static_folder`` can open, and a module ``__file__`` likewise
    points inside the archive. ``as_file`` materialises the resource on the
    filesystem -- a no-op when it already lives on disk, an extraction to a
    temporary directory when it does not -- so the dashboard serves static
    files correctly under every Buck packaging mode. The materialised copy is
    retained for the process lifetime and removed at interpreter exit.

    Returns an empty string when the frontend has not been built, in which case
    the server degrades to API-only.
    """
    build = _PKG / "frontend" / "build"
    if not build.is_dir():
        return ""
    stack = ExitStack()
    atexit.register(stack.close)
    return str(stack.enter_context(as_file(build)))


def create_app(adapter: DBAdapter) -> Flask:
    """Build a configured Flask application.

    Args:
        adapter: A DBAdapter instance for data access (e.g. SQLiteAdapter
            for local dev, QueryEngineAdapter for production).
    """
    build_dir = _frontend_build_dir()

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
        (in-process URL matching the cert SAN), ``api_url`` (loopback
        plaintext URL for internal API clients), ``port``, ``pid`` (always
        None), ``handle`` (Thread), and ``api_handle`` (Thread).

    Raises:
        OSError: If the port is already in use.
    """
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

    app = create_app(adapter)
    # Suppress per-request werkzeug logs (they are noisy in production).
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    try:
        server = make_server(
            host,
            port,
            app,
            threaded=True,
            ssl_context=ssl_ctx,
        )
    except OSError:
        logger.exception("dashboard failed to start on %s:%s", host, port)
        raise

    actual_port = server.server_port
    if ssl_ctx is not None and tls_hostname is not None:
        # pyrefly: ignore [unbound-name]
        url = nest_dev_proxy_url(tls_hostname, actual_port)
        local_url = f"https://{tls_hostname}:{actual_port}"
    else:
        url = f"http://localhost:{actual_port}"
        local_url = url

    api_server = make_server(
        "127.0.0.1",
        0,
        app,
        threaded=True,
        ssl_context=None,
    )
    api_url = f"http://127.0.0.1:{api_server.server_port}"

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="monarch-dashboard",
    )
    thread.start()
    api_thread = threading.Thread(
        target=api_server.serve_forever,
        daemon=True,
        name="monarch-dashboard-api",
    )
    api_thread.start()
    logger.info("Monarch Dashboard running at %s", url)

    return {
        "url": url,
        "local_url": local_url,
        "api_url": api_url,
        "port": actual_port,
        "pid": None,
        "handle": thread,
        "api_handle": api_thread,
    }
