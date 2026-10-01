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

import contextlib
import io
import threading
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from monarch.distributed_telemetry.engine import QueryEngine
from monarch.monarch_dashboard.server.db import DBAdapter


class _ArrowChunkSink(io.RawIOBase):
    def __init__(self) -> None:
        """Initialize an empty writable sink for Arrow IPC bytes."""
        super().__init__()
        self._chunks: list[bytes] = []
        self._position = 0

    def writable(self) -> bool:
        """Report that PyArrow may write IPC bytes to this sink."""
        return True

    def write(self, data: Any) -> int:
        """Accept one IPC fragment and return its byte count."""
        chunk = bytes(data)
        self._chunks.append(chunk)
        self._position += len(chunk)
        return len(chunk)

    def tell(self) -> int:
        """Return the total number of IPC bytes written over the stream lifetime."""
        return self._position

    def take_chunks(self) -> list[bytes]:
        """Return all unread IPC fragments and mark them as consumed."""
        chunks = self._chunks
        self._chunks = []
        return chunks


def _encode_arrow_stream(reader: pa.RecordBatchReader) -> Iterator[bytes]:
    """Yield a standard Arrow IPC stream and close ``reader`` when finished."""
    with contextlib.closing(reader), _ArrowChunkSink() as sink:
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            yield from sink.take_chunks()
            for batch in reader:
                writer.write_batch(batch)
                yield from sink.take_chunks()
        yield from sink.take_chunks()


class QueryEngineAdapter(DBAdapter):
    """Production adapter wrapping Monarch's DataFusion QueryEngine.

    Provides the same query interface as db.py's _query() but backed by
    the distributed telemetry system instead of a local SQLite file.

    The job sidecar constructs this adapter with the internal query engine
    that backs the dashboard HTTP API.
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

    def query_stream(self, sql: str) -> Iterator[bytes]:
        """Yield a SQL result as a standard Arrow IPC stream.

        The shared query engine remains serialized until this iterator is
        exhausted or closed. Callers must do one of those to release it.
        """
        with self._query_lock:
            yield from _encode_arrow_stream(self._engine.query_stream(sql))

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
