# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Flask application factory for the Monarch Dashboard.

Creates and configures the Flask app, registers the API blueprint, and
initialises the database layer pointing at the fake data SQLite file.
"""

import os

from flask import Flask

from . import db
from .routes import api


def create_app(db_path: str | None = None) -> Flask:
    """Build a configured Flask application.

    Args:
        db_path: Path to the SQLite database.  Defaults to the
            ``MONARCH_DB_PATH`` environment variable, or
            ``fake_data/fake_data.db`` relative to the package root.
    """
    app = Flask(__name__)

    if db_path is None:
        db_path = os.environ.get(
            "MONARCH_DB_PATH",
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..",
                "fake_data",
                "fake_data.db",
            ),
        )

    db.init(db_path)
    app.register_blueprint(api)

    return app


# Allow ``flask --app server.app run`` to pick up the app.
app = create_app()
