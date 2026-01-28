# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
QueryEngine - Wrapper for the Rust QueryEngine.

Provides SQL query execution over distributed telemetry actors.
"""

from typing import Optional, TYPE_CHECKING

import pyarrow as pa
from monarch._rust_bindings.monarch_extension.distributed_telemetry import (
    QueryEngine as _QueryEngine,
)

if TYPE_CHECKING:
    from monarch.distributed_telemetry.actor import DistributedTelemetryActor


class QueryEngine:
    """
    SQL query engine for distributed telemetry data.

    Takes a singleton DistributedTelemetryActor (ActorMesh) and provides SQL
    query capabilities using DataFusion as the query planner and executor.

    The Rust QueryEngine is created lazily on first query to allow this object
    to be created from async contexts without blocking.
    """

    def __init__(self, actor: "DistributedTelemetryActor") -> None:
        """
        Create a QueryEngine.

        Args:
            actor: A singleton DistributedTelemetryActor (ActorMesh with one element)
                   that has all children added.
        """
        self._actor: "DistributedTelemetryActor" = actor
        self._engine: Optional[_QueryEngine] = None

    def _ensure_engine(self) -> _QueryEngine:
        """Lazily create the Rust QueryEngine on first use."""
        if self._engine is None:
            self._engine = _QueryEngine(self._actor)
        return self._engine

    def query(self, sql: str) -> pa.Table:
        """
        Execute a SQL query and return results as a PyArrow table.

        Args:
            sql: SQL query string

        Returns:
            PyArrow Table containing query results
        """
        data: bytes = self._ensure_engine().query(sql)
        reader = pa.ipc.open_stream(data)
        return reader.read_all()

    def query_raw(self, sql: str) -> bytes:
        """
        Execute a SQL query and return raw Arrow IPC stream.

        Args:
            sql: SQL query string

        Returns:
            Arrow IPC serialized stream containing all record batches
        """
        return self._ensure_engine().query(sql)
