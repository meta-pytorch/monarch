# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Production adapter: wraps the Monarch DataFusion QueryEngine.

Unlike the SQLite-based db.py (local dev/testing), this connects directly
to the live telemetry engine started by start_telemetry(). The QueryEngine
uses DataFusion as its SQL planner/executor and returns pyarrow Tables.
"""

from typing import Any

from monarch.distributed_telemetry.engine import QueryEngine
from monarch.monarch_dashboard.server.db import DBAdapter


class QueryEngineAdapter(DBAdapter):
    """Production adapter wrapping Monarch's DataFusion QueryEngine.

    Provides the same query interface as db.py's _query() but backed by
    the distributed telemetry system instead of a local SQLite file.

    Usage::

        from monarch.distributed_telemetry import start_telemetry
        engine = start_telemetry()
        adapter = QueryEngineAdapter(engine)
        rows = adapter.query("SELECT * FROM actors LIMIT 10")
    """

    def __init__(self, engine: QueryEngine) -> None:
        self._engine = engine

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL query and return rows as list of dicts."""
        table = self._engine.query(sql)
        return table.to_pylist()

    def table_names(self) -> list[str]:
        """Return available table names from the telemetry engine."""
        return self._engine._actor.table_names.call_one().get()
