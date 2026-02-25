# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from typing import List, Optional

class DatabaseScanner:
    """Local MemTable operations, scans with child stream merging."""

    def __new__(
        cls,
        rank: int,
        use_fake_data: bool = True,
        max_batches: int = 100,
        batch_size: int = 1000,
    ) -> "DatabaseScanner": ...
    def flush(self) -> None:
        """Flush any pending trace events to the tables."""
        ...
    def table_names(self) -> List[str]:
        """Get list of table names."""
        ...
    def schema_for(self, table: str) -> bytes:
        """Get schema for a table in Arrow IPC format."""
        ...
    def scan(
        self,
        dest: object,
        table_name: str,
        projection: Optional[List[int]] = None,
        limit: Optional[int] = None,
        filter_expr: Optional[str] = None,
    ) -> int:
        """Perform a scan, sending results directly to the dest port."""
        ...
