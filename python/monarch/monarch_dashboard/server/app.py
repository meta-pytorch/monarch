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

import os

from flask import Flask, send_from_directory
from monarch.monarch_dashboard import _PKG

from . import db
from .routes import api


def create_app(db_path: str | None = None) -> Flask:
    """Build a configured Flask application.

    Args:
        db_path: Path to the SQLite database.  Defaults to the
            ``MONARCH_DB_PATH`` environment variable, or
            ``fake_data/fake_data.db`` relative to the package root.
    """
    build_dir = str(_PKG / "frontend" / "build")

    app = Flask(
        __name__,
        static_folder=os.path.join(build_dir, "static")
        if os.path.isdir(build_dir)
        else None,
    )

    if db_path is None:
        db_path = os.environ.get(
            "MONARCH_DB_PATH",
            str(_PKG / "fake_data" / "fake_data.db"),
        )

    db.init(db_path)
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


# Allow ``flask --app server.app run`` to pick up the app.
app = create_app()
