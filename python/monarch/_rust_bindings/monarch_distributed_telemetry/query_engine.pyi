# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import pyarrow as pa

class QueryEngine:
    """DataFusion query execution, creates ports, collects results."""

    def __new__(cls, actor: object) -> "QueryEngine": ...
    @staticmethod
    def local() -> "QueryEngine":
        """Create a DataFusion engine for local Arrow tables without actor RPCs."""
        ...
    def register_local_table(self, name: str, arrow_ipc_bytes: bytes) -> None:
        """Register or replace a local table from an Arrow IPC stream."""
        ...
    def __repr__(self) -> str: ...
    def query(self, sql: str) -> pa.RecordBatchReader:
        """Execute a SQL query and return an Arrow record batch reader."""
        ...
