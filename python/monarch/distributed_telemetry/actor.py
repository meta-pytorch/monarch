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
from typing import Any, Callable, Dict, List, Optional

from monarch._rust_bindings.monarch_distributed_telemetry.database_scanner import (
    DatabaseScanner,
)
from monarch._rust_bindings.monarch_hyperactor.actor import (
    MethodSpecifier,
    PythonMessage,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    PortId,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.pickle import pickle as monarch_pickle
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure
from monarch._src.actor.proc_mesh import (
    ProcMesh,
    register_proc_mesh_spawn_callback,
    SetupActor,
)
from monarch.actor import Actor, context, current_rank, endpoint, this_proc
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


def _build_telemetry_actor_id(proc_ref: str) -> Optional[ActorId]:
    """Parse a proc_ref string into an ActorId targeting the telemetry actor.

    Uses ``ActorId.from_string`` with a synthetic actor suffix to avoid
    fragile comma-splitting (addr can contain commas in complex formats
    like ``tcp![::1]:2345``).
    """
    try:
        parsed = ActorId.from_string(f"{proc_ref},_parse[0]")
        return ActorId(
            addr=parsed.addr,
            proc_name=parsed.proc_name,
            actor_name="telemetry",
            pid=0,
        )
    except Exception:
        logger.debug("failed to parse proc_ref %r into ActorId", proc_ref)
        return None


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
        self._proc_id: str = context().actor_instance.proc_id

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
    def get_proc_id(self) -> str:
        """Return the proc_id of this actor's proc."""
        return self._proc_id

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

    def _try_direct_store(
        self,
        actor_id: ActorId,
        dump_id: str,
        proc_ref: str,
        pyspy_result_json: str,
    ) -> bool:
        """Send a store message directly to an actor via its ActorId.

        Opens a once-port for the response so we can confirm the target
        actually stored the data. Times out after 5 seconds to avoid
        blocking indefinitely if the target actor does not exist.
        """
        try:
            self._spawn_missing_children()

            mailbox = context().actor_instance._mailbox
            handle, receiver = mailbox.open_once_port()
            port_ref = handle.bind()

            state = monarch_pickle(((dump_id, proc_ref, pyspy_result_json), {}))
            kind = PythonMessageKind.CallMethod(
                MethodSpecifier.ReturnsResponse("try_store_pyspy_dump"), port_ref
            )
            msg = PythonMessage(kind, state.buffer())
            mailbox.post(actor_id, msg)

            from monarch._src.actor.actor_mesh import PortReceiver

            return PortReceiver(mailbox, receiver).recv().get(timeout=5.0)
        except Exception:
            logger.debug("direct store to %s failed", actor_id, exc_info=True)
            return False

    @endpoint
    def store_pyspy_dump(
        self, dump_id: str, proc_ref: str, pyspy_result_json: str
    ) -> bool:
        """Store py-spy dump data in the pyspy DataFusion tables on the target proc.

        If ``proc_ref`` matches this actor's proc, stores locally.
        Otherwise sends directly to the target telemetry actor via ActorId.
        Falls back to root coordinator storage if the direct send fails.
        """
        if proc_ref == self._proc_id:
            self._scanner.store_pyspy_dump_py(dump_id, proc_ref, pyspy_result_json)
            return True

        # Try direct ActorId-based send, bypassing the mesh hierarchy.
        target = _build_telemetry_actor_id(proc_ref)
        if target is not None and self._try_direct_store(
            target, dump_id, proc_ref, pyspy_result_json
        ):
            return True

        # Direct send failed or could not parse; store on this (root)
        # coordinator so the data is not lost.
        logger.info("direct store failed for proc_ref %s, storing on root", proc_ref)
        self._scanner.store_pyspy_dump_py(dump_id, proc_ref, pyspy_result_json)
        return True

    @endpoint
    def try_store_pyspy_dump(
        self, dump_id: str, proc_ref: str, pyspy_result_json: str
    ) -> bool:
        """Try to store the dump if proc_ref matches this actor's proc.

        Unlike ``store_pyspy_dump``, this does not fall back to root storage.
        Called via direct ActorId-based messaging from the root coordinator.
        """
        if proc_ref == self._proc_id:
            self._scanner.store_pyspy_dump_py(dump_id, proc_ref, pyspy_result_json)
            return True
        return False

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
) -> "tuple[QueryEngine, str | None]":
    """
    Start the distributed telemetry system.

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
        A tuple of (QueryEngine, telemetry_url). ``telemetry_url`` is the
        base URL of the dashboard server (e.g. ``"http://localhost:8265"``)
        when ``include_dashboard`` is True, otherwise None. Pass it to
        ``host_mesh._spawn_admin(telemetry_url=...)`` to enable proxy routes.
    """
    _register_scanner(batch_size, retention_secs=retention_secs)
    coordinator = this_proc().spawn("telemetry_coordinator", DistributedTelemetryActor)
    query_engine = QueryEngine(coordinator)

    telemetry_url: str | None = None
    if include_dashboard:
        adapter = QueryEngineAdapter(query_engine)
        info = start_dashboard(
            adapter=adapter,
            port=dashboard_port,
        )
        telemetry_url = info["url"]
        logger.info("Monarch Dashboard: %s", telemetry_url)
        print(f"Monarch Dashboard: {telemetry_url}", flush=True)

    return query_engine, telemetry_url
