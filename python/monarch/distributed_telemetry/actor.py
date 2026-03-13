# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
DistributedTelemetryActor - Python actor that orchestrates distributed SQL queries.

This actor wraps a DatabaseScanner (Rust) and manages child actor meshes.
It coordinates scans across the hierarchy. Data flows directly Rust-to-Rust
via ports for efficiency.

To avoid race conditions where events could be missed before the telemetry
actor initializes, the DatabaseScanner is created at process startup via
SetupActor's startup function mechanism. The scanner is stored in a module-level
variable and used by the DistributedTelemetryActor when it initializes.
"""

import functools
import logging
import os
from typing import Any, Callable, Dict, List, Optional

from monarch._rust_bindings.monarch_distributed_telemetry.database_scanner import (
    DatabaseScanner,
)
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    PortId,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure
from monarch._src.actor.proc_mesh import (
    ProcMesh,
    register_proc_mesh_spawn_callback,
    SetupActor,
)
from monarch.actor import Actor, current_rank, endpoint, this_proc
from monarch.distributed_telemetry.engine import QueryEngine
from monarch.monarch_dashboard.server.app import start_dashboard
from monarch.monarch_dashboard.server.query_engine_adapter import QueryEngineAdapter

logger: logging.Logger = logging.getLogger(__name__)

# Module-level scanner created at process startup to avoid race conditions.
_scanner: Optional[DatabaseScanner] = None
_scanner_startup_impl: Optional[Callable[[], None]] = None

# Module-level list of spawned ProcMeshes, recorded by the spawn callback.
_spawned_procs: List[ProcMesh] = []
_spawn_callback_registered: bool = False


def _on_proc_mesh_spawned(pm: ProcMesh) -> None:
    """Callback that records spawned ProcMeshes."""
    _spawned_procs.append(pm)


def _scanner_startup() -> Optional[Callable[[], None]]:
    return _scanner_startup_impl


SetupActor.register_startup_function(_scanner_startup)


def _register_scanner(
    batch_size: int,
    retention_secs: int = 600,
) -> None:
    global _scanner, _scanner_startup_impl, _spawn_callback_registered, _spawned_procs
    _scanner = DatabaseScanner(
        current_rank().rank,
        batch_size=batch_size,
        retention_secs=retention_secs,
    )
    _scanner_startup_impl = functools.partial(
        _register_scanner,
        batch_size=batch_size,
        retention_secs=retention_secs,
    )
    # Clear the spawned procs list when starting fresh
    _spawned_procs = []
    # Register the spawn callback once to record new ProcMeshes
    if not _spawn_callback_registered:
        register_proc_mesh_spawn_callback(_on_proc_mesh_spawned)
        _spawn_callback_registered = True


class DistributedTelemetryActor(Actor):
    """
    Distributed telemetry actor that wraps a local DatabaseScanner.

    The DatabaseScanner must already exist in the module-level _scanner variable
    before this actor is created.
    """

    def __init__(self) -> None:
        global _scanner
        assert _scanner is not None, "DatabaseScanner must be created before actor"
        self._scanner: DatabaseScanner = _scanner
        _scanner = None  # Transfer ownership

        self._children: Dict[str, Any] = {}
        self._num_procs_processed: int = 0

    def __supervise__(self, failure: MeshFailure) -> bool:
        """Handle child mesh failures gracefully.

        When a ProcMesh is stopped, the telemetry actors on it die. We remove
        the dead child so that subsequent scans skip it. Returning True
        prevents the failure from propagating up the supervision tree.

        Note: stopping a ProcMesh loses process-local telemetry data from
        those children.
        """
        self._children.pop(failure.mesh_name, None)
        logger.info("child mesh failed: %s", failure.mesh_name)
        return True

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        """Suppress undeliverable messages to dead children."""
        logger.info(
            "undeliverable message to %s: %s", message.dest(), message.error_msg()
        )
        return True

    def _spawn_missing_children(self) -> None:
        """Spawn telemetry actors for any new ProcMeshes we haven't processed yet."""
        for pm in _spawned_procs[self._num_procs_processed :]:
            actor_mesh = pm.spawn("telemetry", DistributedTelemetryActor)
            # pyre-ignore[16]: actor_mesh is an ActorMesh with _name
            mesh_name: str = actor_mesh._name.get()
            self._children[mesh_name] = actor_mesh
            self._num_procs_processed += 1

    @endpoint
    def ready(self) -> None:
        """No-op endpoint to confirm actor is initialized."""
        pass

    @endpoint
    def table_names(self) -> List[str]:
        """Get list of table names available in the database."""
        return self._scanner.table_names()

    @endpoint
    def schema_for(self, table: str) -> bytes:
        """Get schema for a table in Arrow IPC format."""
        return bytes(self._scanner.schema_for(table))

    @endpoint
    def add_children(self, children: "DistributedTelemetryActor") -> None:
        """Add a child actor mesh to scan when queries are executed."""
        # pyre-ignore[16]: children is an ActorMesh with _name
        mesh_name: str = children._name.get()
        self._children[mesh_name] = children

    @endpoint
    def apply_retention(self, table_name: str, where_clause: str) -> None:
        """Apply a retention filter to a table, then fan out to children."""
        self._scanner.apply_retention(table_name, where_clause)
        for child_mesh in self._children.values():
            try:
                # pyre-ignore[29]: child_mesh is an ActorMesh
                child_mesh.apply_retention.call(table_name, where_clause).get()
            except Exception:
                logger.info("child apply_retention failed, skipping")

    @endpoint
    def scan(
        self,
        dest: PortId,
        table_name: str,
        projection: Optional[List[int]],
        limit: Optional[int],
        filter_expr: Optional[str],
    ) -> int:
        """Perform a distributed scan, sending results to dest port."""
        # Spawn telemetry actors for any new ProcMeshes before scanning
        self._spawn_missing_children()

        local_count: int = self._scanner.scan(
            dest, table_name, projection, limit, filter_expr
        )

        # The __supervise__ callback removes dead children from the dict,
        # but it may not have been delivered yet when this scan runs
        # (message ordering is not guaranteed). The try/except handles
        # this timing gap by catching errors from dead children that
        # haven't been pruned yet.
        child_futures = []
        for child_mesh in self._children.values():
            try:
                # pyre-ignore[29]: child_mesh is an ActorMesh
                fut = child_mesh.scan.call(
                    dest, table_name, projection, limit, filter_expr
                )
                child_futures.append(fut)
            except Exception:
                logger.info("child scan call failed, skipping")

        total_count = local_count
        for fut in child_futures:
            try:
                child_results = fut.get()
                # pyre-ignore[16]: child_results is iterable of tuples
                for _rank, count in child_results:
                    total_count += count
            except Exception:
                logger.info("child scan failed, skipping")

        return total_count


def start_telemetry(
    batch_size: int = 1000,
    retention_secs: int = 600,
    include_dashboard: bool = True,
    dashboard_port: int = 8265,
) -> QueryEngine:
    """
    Start the distributed telemetry system and return a QueryEngine.

    Message tables (sent_messages, messages, message_status_events) retain
    only the last ``retention_secs`` seconds of data (default 10 minutes).
    All other tables have unlimited retention. Set to 0 to disable retention.

    Args:
        batch_size: Number of rows to buffer before flushing to a RecordBatch.
        retention_secs: Retention window in seconds for message tables.
            Defaults to 600 (10 minutes). 0 disables retention.
        include_dashboard: Whether to start the monarch dashboard web server.
        dashboard_port: Preferred port for the dashboard (default 8265).

    Returns:
        The QueryEngine for executing SQL queries.
    """
    _register_scanner(batch_size, retention_secs=retention_secs)
    coordinator = this_proc().spawn("telemetry_coordinator", DistributedTelemetryActor)
    query_engine = QueryEngine(coordinator)

    if include_dashboard:
        adapter = QueryEngineAdapter(query_engine)
        info = start_dashboard(
            adapter=adapter,
            port=dashboard_port,
        )
        dashboard_url = info["url"]
        os.environ["MONARCH_DASHBOARD_URL"] = dashboard_url
        logger.info("Monarch Dashboard: %s", dashboard_url)

    return query_engine
