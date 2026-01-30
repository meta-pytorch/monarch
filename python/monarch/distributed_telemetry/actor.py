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
from typing import Any, Callable, List, Optional

from monarch._rust_bindings.monarch_distributed_telemetry.database_scanner import (
    DatabaseScanner,
)
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortId
from monarch._src.actor.proc_mesh import (
    ProcMesh,
    register_proc_mesh_spawn_callback,
    SetupActor,
)
from monarch.actor import Actor, current_rank, endpoint, this_proc
from monarch.distributed_telemetry.engine import QueryEngine

# Module-level scanner created at process startup to avoid race conditions.
_scanner: Optional[DatabaseScanner] = None
_scanner_startup_impl = None


def _scanner_startup():
    return _scanner_startup_impl


SetupActor.register_startup_function(_scanner_startup)


def _register_scanner(use_fake_data: bool, batch_size: int) -> None:
    global _scanner, _scanner_startup_impl
    _scanner = DatabaseScanner(
        current_rank().rank, use_fake_data=use_fake_data, batch_size=batch_size
    )
    _scanner_startup_impl = functools.partial(
        _register_scanner, use_fake_data=use_fake_data, batch_size=batch_size
    )


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

        self._children: List[Any] = []
        self._spawn_callback: Callable[[ProcMesh], None] = self._on_proc_mesh_spawned
        register_proc_mesh_spawn_callback(self._spawn_callback)

    def _on_proc_mesh_spawned(self, pm: ProcMesh) -> None:
        """Callback invoked when a new ProcMesh is spawned."""
        actor_mesh = pm.spawn("telemetry", DistributedTelemetryActor)
        self._children.append(actor_mesh)

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
        self._children.append(children)

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
        local_count: int = self._scanner.scan(
            dest, table_name, projection, limit, filter_expr
        )

        child_futures = []
        for child_mesh in self._children:
            # pyre-ignore[29]: child_mesh is an ActorMesh
            fut = child_mesh.scan.call(dest, table_name, projection, limit, filter_expr)
            child_futures.append(fut)

        total_count = local_count
        for fut in child_futures:
            child_results = fut.get()
            # pyre-ignore[16]: child_results is iterable of tuples
            for _rank, count in child_results:
                total_count += count

        return total_count


def start_telemetry(use_fake_data: bool = False, batch_size: int = 1000) -> QueryEngine:
    """
    Start the distributed telemetry system and return a QueryEngine.

    Args:
        use_fake_data: If True, populate tables with fake demo data.
                       If False (default), tables are populated from real
                       tracing events via RecordBatchSink.
        batch_size: Number of rows to buffer before flushing to a RecordBatch.

    Returns:
        The QueryEngine for executing SQL queries.
    """
    # Reset if called again (e.g., in tests)
    _register_scanner(use_fake_data, batch_size)
    coordinator = this_proc().spawn("telemetry_coordinator", DistributedTelemetryActor)
    # Wait for actor to initialize so spawn callback is registered
    # pyre-ignore[29]: coordinator is an ActorMesh
    coordinator.ready.call().get()
    return QueryEngine(coordinator)
