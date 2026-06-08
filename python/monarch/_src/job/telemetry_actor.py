# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Host-local telemetry collector actor.

The actor is intentionally a narrow Python control/query shell over the Rust
`DatabaseScanner`. Hot telemetry frames do not pass through Python: producers
write framed Arrow IPC over `telemetry.sock`, Rust socket ingest validates and
decodes those frames, and the scanner owns the in-memory DataFusion tables.
Python coordinates activation and exposes actor endpoints for query fan-out.

More than one `TelemetryActor` may be started for what turns out to be the
same host-local socket namespace. That can happen in collocated/local jobs
(i.e. ProcessJob) where the client-side sidecar collector and a worker-host
collector resolve the same `/tmp/monarch_<apply_id>/telemetry.sock`, or during
a sidecar refresh while an existing collector is still alive. This is a deliberate
design choice. The non-destructive socket bind is the serialization point; the actor
that binds the socket owns the store, and an actor that observes a live socket
leaves its scanner unset and acts as an inert candidate.

TODO: consider dedup'ing the worker fan-out by hostname (or job type) so
ProcessJob / collocated jobs skip the redundant per-host candidate spawns
instead of relying on each one to lose the bind race and be torn down.
"""

from __future__ import annotations

import logging
import os
from typing import Any, List, Optional

from monarch._rust_bindings.monarch_distributed_telemetry import (
    _register_trace_entity_schemas,
    _start_socket_ingest,
)
from monarch._rust_bindings.monarch_distributed_telemetry.database_scanner import (
    DatabaseScanner,
)
from monarch._rust_bindings.monarch_extension.snapshot_integration import (
    _pre_register_snapshot_schemas,
)
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortId
from monarch.actor import Actor, current_rank, endpoint

logger: logging.Logger = logging.getLogger(__name__)

_SCAN_WORKER_TIMEOUT_SECS: float = 10.0


def telemetry_socket_dir(apply_id: str) -> str:
    """Return the host-local telemetry socket directory for an apply id."""
    return os.path.join("/tmp", f"monarch_{apply_id}")


def telemetry_socket_path(apply_id: str) -> str:
    """Return the host-local telemetry socket path for an apply id."""
    return os.path.join(telemetry_socket_dir(apply_id), "telemetry.sock")


class TelemetryActor(Actor):
    """Host-local telemetry collector actor."""

    def __init__(self, apply_id: str, retention_secs: int) -> None:
        # Job-instance identifier; namespaces the per-host socket path under
        # /tmp so concurrent jobs on the same host do not collide.
        self._apply_id: str = apply_id
        # Collector-side retention window in seconds for message tables; 0
        # disables retention. Passed to DataFusion's periodic retention task.
        self._retention_secs: int = retention_secs
        # The DataFusion store + socket-ingest pair this actor owns. `None`
        # means this actor did not win (or has not yet attempted) the
        # non-destructive bind, so it is not a live collector.
        self._scanner: DatabaseScanner | None = None
        # Other telemetry actors this collector fans queries out to. Empty
        # for leaf collectors; the query-root collector receives its list via
        # `set_worker_collectors` once the worker meshes exist.
        self._worker_collectors: list[Any] = []

    def _scanner_or_raise(self) -> DatabaseScanner:
        scanner = self._scanner
        if scanner is not None:
            return scanner
        raise RuntimeError("not an active telemetry collector")

    def _activate_impl(self) -> bool:
        """Lazily bind the local socket and stand up the scanner.

        No-op once active. On a previously skipped or failed actor the next
        call re-attempts activation: the bind is non-destructive, so a retry
        is safe and may succeed if the prior owner has gone away.
        """
        if self._scanner is not None:
            return True

        # 0o700 keeps the socket dir owner-only under shared /tmp: a co-tenant
        # without traversal permission on the parent cannot connect to the
        # socket and inject forged telemetry frames.
        socket_dir = telemetry_socket_dir(self._apply_id)
        os.makedirs(socket_dir, mode=0o700, exist_ok=True)
        os.chmod(socket_dir, 0o700)

        scanner = DatabaseScanner(
            current_rank().rank,
            retention_secs=self._retention_secs,
        )
        try:
            _register_trace_entity_schemas(scanner)
            _pre_register_snapshot_schemas(scanner)
            if _start_socket_ingest(scanner, telemetry_socket_path(self._apply_id)):
                self._scanner = scanner
                return True
        except Exception as error:
            logger.warning("telemetry collector activation failed: %s", error)
        return False

    @endpoint
    def activate(self) -> bool:
        """Lazily bind the local socket and stand up the scanner."""
        return self._activate_impl()

    @endpoint
    def set_worker_collectors(self, worker_collectors: List[Any]) -> None:
        """Replace the client collector's worker fan-out list."""
        self._scanner_or_raise()
        self._worker_collectors = list(worker_collectors)

    @endpoint
    def table_names(self) -> List[str]:
        """Get list of table names available in the local store."""
        return self._scanner_or_raise().table_names()

    @endpoint
    def schema_for(self, table: str) -> bytes:
        """Get schema for a table in Arrow IPC format."""
        return bytes(self._scanner_or_raise().schema_for(table))

    @endpoint
    def apply_retention(self, table_name: str, where_clause: str) -> None:
        """Apply a retention filter to a local table."""
        self._scanner_or_raise().apply_retention(table_name, where_clause)

    @endpoint
    def store_pyspy_dump(
        self, dump_id: str, proc_ref: str, pyspy_result_json: str
    ) -> bool:
        """Store py-spy dump data in local tables."""
        self._scanner_or_raise().store_pyspy_dump_py(
            dump_id, proc_ref, pyspy_result_json
        )
        return True

    @endpoint
    def scan(
        self,
        dest: PortId,
        table_name: str,
        projection: Optional[List[int]],
        limit: Optional[int],
        filter_expr: Optional[str],
    ) -> int:
        """Scan the local store and configured worker collectors."""
        # The client collector is the singleton query root: scan its local store
        # first, then fan out flat to active worker collectors. Worker collectors
        # have an empty `_worker_collectors` list, so the same endpoint is
        # leaf-only when invoked on a worker.
        local_count: int = self._scanner_or_raise().scan(
            dest, table_name, projection, limit, filter_expr
        )

        child_futures = []
        for collector in self._worker_collectors:
            try:
                # Constructing the call can fail if the sidecar holds a stale
                # actor ref. Treat that as reduced result coverage, matching
                # the legacy best-effort query behavior.
                child_futures.append(
                    collector.scan.call(
                        dest, table_name, projection, limit, filter_expr
                    )
                )
            except Exception:
                logger.info("worker telemetry scan call failed, skipping")

        total_count = local_count
        for future in child_futures:
            try:
                # Worker collectors are independent leaves. A slow or failed
                # worker should reduce query coverage, not fail the root query.
                child_results = future.get(timeout=_SCAN_WORKER_TIMEOUT_SECS)
                for _rank, count in child_results:
                    total_count += count
            except TimeoutError:
                logger.warning(
                    "worker telemetry scan timed out after %ss",
                    _SCAN_WORKER_TIMEOUT_SECS,
                )
            except Exception:
                logger.info("worker telemetry scan failed, skipping")

        return total_count
