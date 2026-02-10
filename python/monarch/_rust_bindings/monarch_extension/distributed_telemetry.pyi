# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final, List, Optional

from monarch._rust_bindings.monarch_hyperactor.mailbox import PortId

@final
class DatabaseScanner:
    """
    Rust-backed DataFusion database scanner.

    Each scanner holds local in-memory tables (DataFusion MemTables) and can
    scan them. Data flows directly Rust-to-Rust via PortRef for efficiency.
    """

    def __init__(self, rank: int) -> None:
        """
        Create a new DatabaseScanner.

        Args:
            rank: The rank of this scanner
        """
        ...

    def flush(self) -> None:
        """Flush any pending trace events to the tables."""
        ...

    def table_names(self) -> List[str]:
        """Get list of table names in the database."""
        ...

    def schema_for(self, table: str) -> bytes:
        """
        Get schema for a table in Arrow IPC format.

        Args:
            table: Name of the table

        Returns:
            Arrow IPC serialized schema
        """
        ...

    def scan(
        self,
        dest: PortId,
        table_name: str,
        projection: Optional[List[int]],
        limit: Optional[int],
        filter_expr: Optional[str],
    ) -> int:
        """
        Perform a scan, sending results directly to the dest port.

        Gets actor_instance from Python context() for sending.

        Sends local scan results to `dest` synchronously. The Python caller
        is responsible for calling children and waiting for them to complete.
        When this method and all child scans return, all data has been sent.

        Data flows directly Rust-to-Rust via PortRef for efficiency.

        Args:
            dest: The destination PortId to send results to
            table_name: Name of the table to scan
            projection: Optional list of column indices to project
            limit: Optional row limit
            filter_expr: Optional SQL WHERE clause

        Returns:
            Number of batches sent
        """
        ...

@final
class QueryEngine:
    """
    DataFusion-based SQL query engine for distributed telemetry.

    Takes a singleton Python actor (ActorMesh) and executes SQL queries,
    distributing scans to the actor hierarchy.
    """

    def __init__(self, actor: object) -> None:
        """
        Create a QueryEngine.

        Args:
            actor: A DistributedTelemetryActor (ActorMesh) to query
        """
        ...

    def query(self, sql: str) -> bytes:
        """
        Execute a SQL query.

        Args:
            sql: SQL query string

        Returns:
            Arrow IPC serialized stream containing all record batches
        """
        ...
