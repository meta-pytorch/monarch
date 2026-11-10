# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from typing import List, Optional, Tuple

from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr

class DatabaseScanner:
    """Local MemTable operations, scans with child stream merging."""

    def __new__(
        cls,
        rank: int,
        retention_secs: int = 3600,
        persist_dir: Optional[str] = None,
    ) -> "DatabaseScanner":
        """Create a scanner. When ``persist_dir`` is set, every table append is
        also written to a persist log there; if the log cannot be created, the
        scanner runs without it."""
        ...
    def close_persist(self) -> None:
        """Write pending telemetry and close the persist log, if enabled."""
        ...
    def apply_retention(self, table_name: str, where_clause: str) -> None:
        """Filter a table, keeping only rows that match the WHERE clause."""
        ...
    def table_names(self) -> List[str]:
        """Get list of table names."""
        ...
    def schema_for(self, table: str) -> bytes:
        """Get schema for a table in Arrow IPC format."""
        ...
    def store_pyspy_dump_py(
        self, dump_id: str, proc_ref: str, pyspy_result_json: str
    ) -> None:
        """Store a py-spy dump result into the pyspy_stacks table."""
        ...
    def ingest_snapshot_batch(self, table_name: str, arrow_ipc_bytes: bytes) -> None:
        """Decode one snapshot Arrow IPC stream and append it to a registered table."""
        ...
    def scan(
        self,
        dest: object,
        table_name: str,
        projection: Optional[List[int]] = None,
        limit: Optional[int] = None,
        filter_expr: Optional[str] = None,
    ) -> Tuple[ActorAddr, int]:
        """Perform a scan, sending results directly to the dest port.

        Returns this collector's actor address, which tags every batch it
        sent, and the number of batches sent.
        """
        ...
