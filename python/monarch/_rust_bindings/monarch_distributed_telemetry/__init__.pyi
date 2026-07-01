# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from monarch._rust_bindings.monarch_distributed_telemetry import (
    database_scanner as database_scanner,
    query_engine as query_engine,
)

def enable_record_batch_tracing(batch_size: int) -> None:
    """Register a RecordBatchSink with the telemetry system."""
    ...

def get_record_batch_flush_count() -> int:
    """Get the total number of RecordBatches flushed by the sink."""
    ...

def reset_record_batch_flush_count() -> None:
    """Reset the flush counter to zero."""
    ...

def _start_socket_ingest(
    scanner: database_scanner.DatabaseScanner, socket_path: str
) -> None:
    """Start Unix-socket ingest for a database scanner."""
    ...

def _register_trace_entity_schemas(
    scanner: database_scanner.DatabaseScanner,
) -> None:
    """Register trace and entity schemas for a database scanner."""
    ...

def _set_unix_socket_sink_path(socket_path: str) -> None:
    """Activate the process-global Unix socket sink."""
    ...
