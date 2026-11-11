# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Arrow fixtures queried by Monarch's native DataFusion engine."""

import threading
from collections.abc import Callable, Iterator
from typing import Any

import pyarrow as pa
from monarch._rust_bindings.monarch_distributed_telemetry.query_engine import (
    QueryEngine,
)
from monarch.monarch_dashboard.server.db import _format_sql, DBAdapter
from monarch.monarch_dashboard.server.query_engine_adapter import _encode_arrow_stream


class ArrowAdapter(DBAdapter):
    """Local dashboard data with the same SQL engine as live telemetry."""

    def __init__(self, tables: dict[str, pa.Table] | None = None) -> None:
        self.tables: dict[str, pa.Table] = {}
        self._engine = QueryEngine.local()
        self._lock = threading.RLock()
        for name, table in (tables or {}).items():
            self.set_table(name, table)

    def set_table(self, name: str, table: pa.Table) -> None:
        """Replace a fixture table, including its schema."""
        with self._lock:
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            self._engine.register_local_table(name, sink.getvalue().to_pybytes())
            self.tables[name] = table

    def append_rows(self, name: str, rows: list[dict[str, Any]]) -> None:
        """Append fixture rows using the table's existing Arrow schema."""
        if rows:
            with self._lock:
                table = self.tables[name]
                added = pa.Table.from_pylist(list(rows), schema=table.schema)
                self.set_table(name, pa.concat_tables([table, added]))

    def update_rows(
        self,
        name: str,
        predicate: Callable[[dict[str, Any]], bool],
        values: dict[str, Any],
    ) -> None:
        """Change selected fixture rows while retaining the Arrow schema."""
        with self._lock:
            table = self.tables[name]
            rows = [
                row | values if predicate(row) else row for row in table.to_pylist()
            ]
            self.set_table(name, pa.Table.from_pylist(list(rows), schema=table.schema))

    def delete_rows(
        self, name: str, predicate: Callable[[dict[str, Any]], bool]
    ) -> None:
        """Remove selected fixture rows while retaining an empty table's schema."""
        with self._lock:
            table = self.tables[name]
            rows = [row for row in table.to_pylist() if not predicate(row)]
            self.set_table(name, pa.Table.from_pylist(list(rows), schema=table.schema))

    def remove_table(self, name: str) -> None:
        """Remove a fixture table to exercise missing-telemetry handling."""
        with self._lock:
            with self._engine.query(f'DROP TABLE "{name}"') as reader:
                reader.read_all()
            del self.tables[name]

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Query fixtures, optionally substituting test SQL parameters."""
        with self._lock, self._engine.query(_format_sql(sql, params)) as reader:
            return reader.read_all().to_pylist()

    def query_stream(self, sql: str) -> Iterator[bytes]:
        """Stream a fixture query through the dashboard's Arrow encoder."""
        with self._lock:
            yield from _encode_arrow_stream(self._engine.query(sql))

    def query_one(self, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        """Return the first fixture result, or None when no rows match."""
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def table_names(self) -> list[str]:
        """Return the registered fixture table names."""
        with self._lock:
            return list(self.tables)
