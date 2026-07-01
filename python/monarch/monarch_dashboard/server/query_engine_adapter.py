# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Production adapter: wraps the Monarch DataFusion QueryEngine.

Unlike the SQLite-based db.py (local dev/testing), this connects directly
to the live telemetry engine attached to a job state. The QueryEngine
uses DataFusion as its SQL planner/executor and returns pyarrow Tables.
"""

import threading
from typing import Any

from monarch.distributed_telemetry.engine import QueryEngine
from monarch.monarch_dashboard.server.db import DBAdapter


class QueryEngineAdapter(DBAdapter):
    """Production adapter wrapping Monarch's DataFusion QueryEngine.

    Provides the same query interface as db.py's _query() but backed by
    the distributed telemetry system instead of a local SQLite file.

    Usage::

        from monarch.job import ProcessJob, TelemetryConfig
        state = ProcessJob({"hosts": 1}).enable_telemetry(TelemetryConfig()).state()
        engine = state.query_engine
        assert engine is not None
        adapter = QueryEngineAdapter(engine)
        rows = adapter.query("SELECT * FROM actors LIMIT 10")
    """

    def __init__(self, engine: QueryEngine) -> None:
        self._engine = engine
        self._query_lock = threading.Lock()

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL query and return rows as list of dicts."""
        # Flask serves dashboard requests concurrently, but the live query
        # engine path is not reentrant.
        with self._query_lock:
            return self._engine.query(sql).to_pylist()

    def table_names(self) -> list[str]:
        """Return available table names from the telemetry engine."""
        return self._engine._actor.table_names.call_one().get()

    def store_pyspy_dump(
        self, dump_id: str, proc_ref: str, pyspy_result_json: str
    ) -> None:
        """Store a py-spy dump result in the DataFusion pyspy tables."""
        self._engine._actor.store_pyspy_dump.call_one(
            dump_id, proc_ref, pyspy_result_json
        ).get()

    def ingest_snapshot_batch(self, table_name: str, arrow_ipc_bytes: bytes) -> None:
        """Store one snapshot Arrow IPC stream in the DataFusion snapshot tables."""
        self._engine._actor.ingest_snapshot_batch.call_one(
            table_name, arrow_ipc_bytes
        ).get()
