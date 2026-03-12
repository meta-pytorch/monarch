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


def find_free_port() -> int:
    """Find an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


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
    port: int = 5000,
    host: str = "0.0.0.0",
) -> dict:
    """Start the dashboard server in a daemon thread.

    Args:
        adapter: A DBAdapter instance for data access.
        port: HTTP port to listen on.  Use 0 to auto-select a free port.
        host: Bind address.

    Returns:
        A dict with keys: ``url``, ``port``, ``pid`` (always None),
        ``handle`` (Thread).
    """
    if port == 0:
        port = find_free_port()

    url = f"http://{host}:{port}"

    app = create_app(adapter)
    thread = threading.Thread(
        target=lambda: app.run(host=host, port=port, use_reloader=False),
        daemon=True,
        name="monarch-dashboard",
    )
    thread.start()
    logger.info("Monarch Dashboard running at %s", url)

    return {"url": url, "port": port, "pid": None, "handle": thread}
