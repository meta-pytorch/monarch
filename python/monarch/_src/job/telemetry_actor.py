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
candidate that cannot bind leaves its scanner unset and contributes no rows to
query fan-out.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from typing import Any, Callable, List, Optional, Tuple

from monarch._rust_bindings.monarch_distributed_telemetry import (
    _post_scan_counts,
    _register_telemetry_schemas,
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
from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr
from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure
from monarch._src.job.host_metrics_actor import GpuMetricsActor, HostMetricsActor
from monarch._src.job.telemetry_ingest import (
    telemetry_socket_dir,
    telemetry_socket_path,
)
from monarch.actor import (
    Actor,
    context,
    current_rank,
    endpoint,
    Future,
    HostMesh,
    Port,
    ProcMesh,
    this_proc,
)

logger: logging.Logger = logging.getLogger(__name__)

_SCAN_WORKER_TIMEOUT_SECS: float = 10.0

# `GpuMetricsActor.start` initializes NVML, which can hang.
_SAMPLER_START_TIMEOUT_SECS: float = 10.0


def telemetry_persist_dir(persist_dir: str, apply_id: str) -> str:
    """Return this collector's persist log directory under ``persist_dir``."""
    return os.path.join(persist_dir, apply_id, f"{socket.gethostname()}-{os.getpid()}")


class TelemetryActor(Actor):
    """Host-local telemetry collector actor."""

    def __init__(
        self,
        apply_id: str,
        retention_secs: int,
        host_metrics_interval_secs: float = 0.0,
        gpu_metrics_interval_secs: float = 0.0,
        persist_dir: str | None = None,
        client_persist_dir: str | None = None,
    ) -> None:
        # Job-instance identifier; namespaces the per-host socket path under
        # /tmp so concurrent jobs on the same host do not collide.
        self._apply_id: str = apply_id
        # Collector-side retention window in seconds for message and trace
        # tables; 0 disables retention. Passed to the periodic retention task.
        self._retention_secs: int = retention_secs
        # The DataFusion store + socket-ingest pair this actor owns. `None`
        # means this actor has not activated as the single live collector for
        # the host-local socket namespace.
        self._scanner: DatabaseScanner | None = None
        # Worker actor meshes this collector fans queries out to, keyed by
        # mesh name (matching `MeshFailure.mesh_name`). Empty for leaf
        # collectors; the query-root collector owns and tracks the worker
        # meshes so their failures can be removed before later scans.
        self._worker_collectors: dict[str, Any] = {}
        self._worker_proc_meshes: list[ProcMesh] = []
        # Seconds between host metrics samples; 0 disables host sampling.
        self._host_metrics_interval_secs: float = host_metrics_interval_secs
        # Seconds between GPU metrics samples; 0 disables GPU sampling.
        self._gpu_metrics_interval_secs: float = gpu_metrics_interval_secs
        # Sampler actor meshes spawned next to this collector's store. Held so
        # the samplers live as long as this collector.
        self._sampler_meshes: list[Any] = []
        # Root of the persist logs worker collectors write; `None` disables
        # persistence. Forwarded to every worker collector.
        self._persist_dir: str | None = persist_dir
        # Overrides `persist_dir` for this collector only. Set for the client
        # collector, whose host may lack the workers' shared mount.
        self._client_persist_dir: str | None = client_persist_dir

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

        persist_root = self._client_persist_dir or self._persist_dir
        scanner = DatabaseScanner(
            current_rank().rank,
            retention_secs=self._retention_secs,
            persist_dir=(
                telemetry_persist_dir(persist_root, self._apply_id)
                if persist_root
                else None
            ),
        )
        try:
            _register_telemetry_schemas(scanner)
            _pre_register_snapshot_schemas(scanner)
            _start_socket_ingest(scanner, telemetry_socket_path(self._apply_id))
        except Exception as error:
            logger.warning("telemetry collector activation failed: %s", error)
            return False
        self._scanner = scanner
        self._start_samplers()
        return True

    def _start_samplers(self) -> None:
        """Start host samplers in this proc, next to the store they feed.

        Only the collector that owns the host-local socket starts samplers, so
        each host runs at most one of each. Sampler failures never fail
        activation.
        """
        samplers = (
            ("host_metrics", HostMetricsActor, self._host_metrics_interval_secs),
            ("gpu_metrics", GpuMetricsActor, self._gpu_metrics_interval_secs),
        )
        for name, sampler_class, interval_secs in samplers:
            if interval_secs <= 0:
                continue
            try:
                mesh = this_proc().spawn(
                    name,
                    sampler_class,
                    telemetry_socket_path(self._apply_id),
                    interval_secs,
                )
                self._sampler_meshes.append(mesh)
                started = mesh.start.call_one().get(timeout=_SAMPLER_START_TIMEOUT_SECS)
                if not started:
                    logger.info("telemetry sampler unavailable on this host: %s", name)
            except TimeoutError:
                logger.warning(
                    "telemetry sampler %s did not start within %s s; it keeps "
                    "starting in the background",
                    name,
                    _SAMPLER_START_TIMEOUT_SECS,
                )
            except Exception:
                logger.warning(
                    "failed to start telemetry sampler: %s", name, exc_info=True
                )

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
                self._host_metrics_interval_secs,
                self._gpu_metrics_interval_secs,
                self._persist_dir,
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
            mesh_name = worker_collector_mesh._name.get()
            self._worker_collectors[mesh_name] = worker_collector_mesh
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

    def __cleanup__(self, exc: Exception | None) -> None:
        """Close this collector's persist log so it ends cleanly."""
        scanner = self._scanner
        if scanner is not None:
            scanner.close_persist()

    def __supervise__(self, failure: MeshFailure) -> bool:
        self._worker_collectors.pop(failure.mesh_name, None)
        logger.warning("telemetry failure, skipping: %s", failure)
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

    @endpoint(explicit_response_port=True)
    def scan(
        self,
        port: Port[List[Tuple[ActorAddr, int]]],
        dest: PortId,
        table_name: str,
        projection: Optional[List[int]],
        limit: Optional[int],
        filter_expr: Optional[str],
    ) -> None:
        """Scan the local store and configured worker collector meshes.

        Replies with the actor address and batch count of the local collector
        and of every collector in a worker mesh that replied before the scan
        deadline; a mesh that times out or fails is left out as a whole. Each
        collector tags the batches it sends to `dest` with its address, so the
        caller can tell when each one has fully arrived.
        """
        self._scan(
            port.send, port.exception, dest, table_name, projection, limit, filter_expr
        )

    @endpoint(explicit_response_port=True)
    def query_scan(
        self,
        port: Port[None],
        dest: PortId,
        table_name: str,
        projection: Optional[List[int]],
        limit: Optional[int],
        filter_expr: Optional[str],
    ) -> None:
        """Scan like `scan`, for the query engine that owns `dest`.

        Rather than replying with the batch counts, posts them to `dest` as
        the scan's final message, so the engine reads them without Python.
        Replies once they are posted.
        """
        instance = context().actor_instance._as_rust()

        def post(batch_counts: List[Tuple[ActorAddr, int]]) -> None:
            _post_scan_counts(instance, dest, batch_counts)
            port.send(None)

        self._scan(
            post, port.exception, dest, table_name, projection, limit, filter_expr
        )

    def _scan(
        self,
        reply: Callable[[List[Tuple[ActorAddr, int]]], None],
        fail: Callable[[Exception], None],
        dest: PortId,
        table_name: str,
        projection: Optional[List[int]],
        limit: Optional[int],
        filter_expr: Optional[str],
    ) -> None:
        # The client collector is the singleton query root: it fans out flat to
        # worker collector meshes, then scans its local store while they run.
        # Worker collectors have an empty `_worker_collectors` registry, so the
        # same endpoint is leaf-only when invoked on a worker.
        scanner = self._scanner
        if scanner is None:
            # An inactive worker shares a host with the collector that owns
            # its socket, such as the client's host, so it holds no rows.
            reply([])
            return

        deadline = time.monotonic() + _SCAN_WORKER_TIMEOUT_SECS
        child_futures: List[Tuple[str, Future[Any]]] = []
        for mesh_name, collector_mesh in tuple(self._worker_collectors.items()):
            try:
                # Constructing the call can fail if the actor ref is stale.
                # Treat that as reduced result coverage, matching best-effort
                # query behavior.
                child_futures.append(
                    (
                        mesh_name,
                        collector_mesh.scan.call(
                            dest, table_name, projection, limit, filter_expr
                        ),
                    )
                )
            except Exception as error:
                logger.warning(
                    "worker telemetry scan call to %s failed, dropping it: %s",
                    mesh_name,
                    error,
                )
                self._worker_collectors.pop(mesh_name, None)

        try:
            local_counts = scanner.scan(
                dest, table_name, projection, limit, filter_expr
            )
            if not child_futures:
                reply([local_counts])
                return
            # Waiting off the actor's thread lets the root start other scans
            # while these workers answer.
            threading.Thread(
                target=self._reply_with_worker_counts,
                args=(reply, fail, [local_counts], child_futures, deadline),
                name="telemetry-scan-reply",
                daemon=True,
            ).start()
        except Exception as error:
            # An exception escaping an explicit-port endpoint fails the actor,
            # so send it to the caller instead. Unlike a plain endpoint, the
            # caller gets the bare exception, without the remote traceback.
            fail(error)

    def _reply_with_worker_counts(
        self,
        reply: Callable[[List[Tuple[ActorAddr, int]]], None],
        fail: Callable[[Exception], None],
        batch_counts: List[Tuple[ActorAddr, int]],
        child_futures: List[Tuple[str, Future[Any]]],
        deadline: float,
    ) -> None:
        for mesh_name, future in child_futures:
            try:
                # Worker collectors are independent leaves. A slow or failed
                # worker should reduce query coverage, not fail the root query.
                child_results = future.get(
                    timeout=max(0.0, deadline - time.monotonic())
                )
                for _rank, counts in child_results:
                    batch_counts.extend(counts)
            except TimeoutError:
                logger.warning(
                    "worker telemetry scan of %s timed out after %ss",
                    mesh_name,
                    _SCAN_WORKER_TIMEOUT_SECS,
                )
            except Exception as error:
                logger.warning(
                    "worker telemetry scan of %s failed, dropping it: %s",
                    mesh_name,
                    error,
                )
                self._worker_collectors.pop(mesh_name, None)
        try:
            reply(batch_counts)
        except Exception as error:
            # Nothing else answers the caller once this thread gives up.
            fail(error)
