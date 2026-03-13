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
    host: str = "0.0.0.0",
) -> dict:
    """Start the dashboard server in a daemon thread.

    The dashboard runs in-process because telemetry data lives entirely
    in-memory as DataFusion MemTables. There is no on-disk database, so
    the dashboard must share the process to access the QueryEngine.

    Args:
        adapter: A DBAdapter instance for data access.
        port: HTTP port to listen on.
        host: Bind address.

    Returns:
        A dict with keys: ``url``, ``port``, ``pid`` (always None),
        ``handle`` (Thread).

    Raises:
        OSError: If the port is already in use.
    """
    # Check port availability before starting the thread so failures
    # are raised to the caller instead of silently killing the thread.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
        except OSError:
            logger.error("Dashboard failed to start: port %d is unavailable", port)
            raise

    display_host = "localhost" if host == "0.0.0.0" else host
    url = f"http://{display_host}:{port}"

    app = create_app(adapter)
    thread = threading.Thread(
        target=lambda: app.run(host=host, port=port, use_reloader=False),
        daemon=True,
        name="monarch-dashboard",
    )
    thread.start()
    logger.info("Monarch Dashboard running at %s", url)

    return {"url": url, "port": port, "pid": None, "handle": thread}
