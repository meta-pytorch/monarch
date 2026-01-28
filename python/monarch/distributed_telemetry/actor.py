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

When created, the actor registers a callback that automatically spawns
telemetry actors on any new ProcMesh and adds them as children. This creates
a hierarchical structure that mirrors the process tree.
"""

from typing import Any, Callable, List, Optional

from monarch._rust_bindings.monarch_extension.distributed_telemetry import (
    DatabaseScanner,
    QueryEngine as RustQueryEngine,
)
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortId
from monarch._src.actor.proc_mesh import ProcMesh, register_proc_mesh_spawn_callback
from monarch.actor import (
    Actor,
    current_rank,
    endpoint,
    Future,
    this_proc,
)
from monarch.distributed_telemetry.engine import QueryEngine


class DistributedTelemetryActor(Actor):
    """
    Distributed telemetry actor that wraps a local DatabaseScanner.

    When created, this actor registers a callback that automatically spawns
    telemetry actors on newly created ProcMeshes and adds them as children.
    This creates a hierarchical structure mirroring the process tree.

    When a scan is requested, it coordinates scanning across all children
    and merges the results. Data flows directly Rust-to-Rust via PortRef.
    """

    def __init__(self, use_fake_data: bool = True, batch_size: int = 1000) -> None:
        rank = current_rank().rank
        self._use_fake_data = use_fake_data
        self._batch_size = batch_size
        self._scanner: DatabaseScanner = DatabaseScanner(
            rank, use_fake_data=use_fake_data, batch_size=batch_size
        )
        # Children are ActorMesh instances
        self._children: List[Any] = []
        # Store callback reference for cleanup
        self._spawn_callback: Callable[[ProcMesh], None] = self._on_proc_mesh_spawned
        register_proc_mesh_spawn_callback(self._spawn_callback)

    def _on_proc_mesh_spawned(self, pm: ProcMesh) -> None:
        """Callback invoked when a new ProcMesh is spawned."""
        actor_mesh = pm.spawn(
            "telemetry",
            DistributedTelemetryActor,
            use_fake_data=self._use_fake_data,
            batch_size=self._batch_size,
        )
        # Wait for the child telemetry actors to initialize so their callbacks
        # are registered before this function returns. This ensures grandchildren
        # spawned immediately after will be captured.
        # pyre-ignore[29]: actor_mesh is an ActorMesh
        actor_mesh.ready.call().get()
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
        """
        Add a child actor mesh to scan when queries are executed.

        Args:
            children: An actor mesh of DistributedTelemetryActor instances
        """
        self._children.append(children)

    @endpoint
    def ensure_ready(self) -> None:
        """
        Ensure this actor and all children are ready.

        Blocks until all child telemetry actors have initialized.
        """
        for child_mesh in self._children:
            # Calling an endpoint on each child ensures its __init__ completed
            # pyre-ignore[29]: child_mesh is an ActorMesh
            child_mesh.ready.call().get()

    @endpoint
    def scan(
        self,
        dest: PortId,
        table_name: str,
        projection: Optional[List[int]],
        limit: Optional[int],
        filter_expr: Optional[str],
    ) -> int:
        """
        Perform a distributed scan, sending results to dest port.

        Called by QueryEngine from Rust. Coordinates the scan across this actor
        and all children. Data flows directly Rust-to-Rust via PortRef.
        Completion is signaled by this method returning.

        Args:
            dest: PortId to send QueryResponse messages to
            table_name: Name of the table to scan
            projection: Optional column indices to project
            limit: Optional row limit
            filter_expr: Optional SQL WHERE clause

        Returns:
            Total number of batches sent (local + all children)
        """
        # Send local data directly to dest, get batch count
        local_count: int = self._scanner.scan(
            dest,
            table_name,
            projection,
            limit,
            filter_expr,
        )

        # Call all children and collect futures
        # Children also send directly to dest
        child_futures = []
        for child_mesh in self._children:
            # Use .call() to get futures
            # pyre-ignore[29]: child_mesh is an ActorMesh
            fut = child_mesh.scan.call(
                dest,
                table_name,
                projection,
                limit,
                filter_expr,
            )
            child_futures.append(fut)

        # Wait for all child futures and sum their batch counts
        total_count = local_count
        for fut in child_futures:
            # Each child returns a list of (rank, count) tuples (one per actor in the mesh)
            child_results = fut.get()
            # pyre-ignore[16]: child_results is iterable of tuples
            for _rank, count in child_results:
                total_count += count

        return total_count


def start_telemetry(use_fake_data: bool = False) -> Future[QueryEngine]:
    """
    Start the distributed telemetry system and return a QueryEngine.

    Spawns a DistributedTelemetryActor on the current process and creates a
    QueryEngine for executing SQL queries. The returned future completes when
    the coordinator is initialized and its callback is registered. After
    .get() or await, any new ProcMesh spawned will automatically have telemetry
    actors created and registered.

    Args:
        use_fake_data: If True (default), populate tables with fake demo data.
                       If False, tables are populated from real tracing events
                       via RecordBatchSink (spans, span_events, events tables).

    Returns:
        A Future that resolves to the QueryEngine for executing SQL queries.

    Example:
        engine = start_telemetry().get()
        # ... spawn procs, they're automatically tracked ...
        result = engine.query("SELECT * FROM metrics")

    Example (real telemetry data):
        engine = start_telemetry(use_fake_data=False).get()
        # ... spawn procs, do work that generates tracing events ...
        result = engine.query("SELECT * FROM spans")
    """
    coordinator = this_proc().spawn(
        "telemetry_coordinator", DistributedTelemetryActor, use_fake_data=use_fake_data
    )

    async def task() -> QueryEngine:
        # pyre-ignore[29]: coordinator is an ActorMesh
        await coordinator.ready.call()
        # Create QueryEngine - this is safe from async context now because
        # table setup is deferred to first query() call
        return QueryEngine(coordinator)

    return Future(coro=task())
