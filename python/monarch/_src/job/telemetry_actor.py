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

Only one live `TelemetryActor` may own a given host-local socket namespace. The
socket bind is the ownership point: the actor that binds
`/tmp/monarch_<apply_id>/telemetry.sock` owns the store, and any activation
candidate that cannot bind leaves its scanner unset and must not participate in
query fan-out.
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
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    PortId,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure
from monarch.actor import Actor, current_rank, endpoint, HostMesh, ProcMesh

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
        # means this actor has not activated as the single live collector for
        # the host-local socket namespace.
        self._scanner: DatabaseScanner | None = None
        # Worker actor meshes this collector fans queries out to, keyed by
        # stable mesh id (matching `MeshFailure.mesh_id`). Empty for leaf
        # collectors; the query-root collector owns and tracks the worker
        # meshes so their failures can be removed before later scans.
        self._worker_collectors: dict[str, Any] = {}
        self._worker_proc_meshes: list[ProcMesh] = []

    def _scanner_or_raise(self) -> DatabaseScanner:
        scanner = self._scanner
        if scanner is not None:
            return scanner
        raise RuntimeError("not an active telemetry collector")

    def _activate_impl(self) -> bool:
        """Lazily bind the local socket and stand up the scanner.

        No-op once active. On a previously failed actor the next call
        re-attempts activation and may succeed if the prior owner has gone away.
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
            _start_socket_ingest(scanner, telemetry_socket_path(self._apply_id))
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
    def start_worker_collector(
        self,
        host_mesh: HostMesh,
        spawn_worker_collector: bool = True,
    ) -> None:
        """Start and register the telemetry collector for a worker host mesh."""
        self._scanner_or_raise()
        self._start_worker_collector(
            host_mesh,
            TelemetryActor,
            self._apply_id,
            "telemetry_hosts",
            spawn_worker_collector,
        )

    def _start_worker_collector(
        self,
        host_mesh: HostMesh,
        actor_class: type[TelemetryActor],
        apply_id: str,
        proc_name: str,
        spawn_worker_collector: bool = True,
    ) -> Any | None:
        proc_mesh = host_mesh.spawn_procs(name=proc_name)
        if not spawn_worker_collector:
            self._worker_proc_meshes.append(proc_mesh)
            return None

        try:
            worker_collector_mesh: Any = proc_mesh.spawn(
                "TelemetryActor",
                actor_class,
                apply_id,
                self._retention_secs,
            )
            active = any(
                active for _rank, active in worker_collector_mesh.activate.call().get()
            )
        except Exception:
            self._stop_worker_mesh(
                proc_mesh,
                "telemetry collector startup failed",
            )
            raise

        self._worker_proc_meshes.append(proc_mesh)
        if active:
            self._worker_collectors[worker_collector_mesh.id] = worker_collector_mesh
            return worker_collector_mesh

        self._stop_worker_mesh(
            worker_collector_mesh,
            "telemetry collector inactive",
        )
        return None

    @endpoint
    def stop_worker_collectors(self) -> None:
        for proc_mesh in self._worker_proc_meshes:
            self._stop_worker_mesh(
                proc_mesh,
                "telemetry shutdown",
            )
        self._worker_proc_meshes.clear()
        self._worker_collectors.clear()

    def _stop_worker_mesh(self, mesh: Any, reason: str) -> None:
        try:
            mesh.stop(reason).get(timeout=5.0)
        except Exception:
            logger.info("failed to stop telemetry mesh: %s", reason, exc_info=True)

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        logger.info("worker telemetry message was undeliverable, skipping: %s", message)
        return True

    def __supervise__(self, failure: MeshFailure) -> bool:
        mesh_id = failure.mesh_id
        if mesh_id is not None:
            self._worker_collectors.pop(mesh_id, None)
        logger.warning("worker telemetry failure, skipping: %s", failure)
        return True

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
    def ingest_snapshot_batch(self, table_name: str, arrow_ipc_bytes: bytes) -> bool:
        """Store one snapshot Arrow IPC batch in a local registered table."""
        self._scanner_or_raise().ingest_snapshot_batch(table_name, arrow_ipc_bytes)
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
        """Scan the local store and configured worker collector meshes."""
        # The client collector is the singleton query root: scan its local store
        # first, then fan out flat to active worker collector meshes. Worker
        # collectors have an empty `_worker_collectors` registry, so the same
        # endpoint is leaf-only when invoked on a worker.
        local_count: int = self._scanner_or_raise().scan(
            dest, table_name, projection, limit, filter_expr
        )

        child_futures = []
        for mesh_id, collector_mesh in tuple(self._worker_collectors.items()):
            try:
                # Constructing the call can fail if the actor ref is stale.
                # Treat that as reduced result coverage, matching best-effort
                # query behavior.
                child_futures.append(
                    (
                        mesh_id,
                        collector_mesh.scan.call(
                            dest, table_name, projection, limit, filter_expr
                        ),
                    )
                )
            except Exception:
                logger.info("worker telemetry scan call failed, skipping")
                self._worker_collectors.pop(mesh_id, None)

        total_count = local_count
        for mesh_id, future in child_futures:
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
                self._worker_collectors.pop(mesh_id, None)

        return total_count
