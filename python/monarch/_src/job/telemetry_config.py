# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Telemetry state hosted by the per-job sidecar.

The job sidecar is a separate Python process keyed on the job's `apply_id`.
Telemetry reuses that process to host the client-side `TelemetryActor`
(collector + scanner + dashboard), ingest telemetry frames over a per-host
Unix socket, and serve the dashboard's HTTP query API. Decoupling it from the
parent job process means:

- Telemetry state (in-memory DataFusion store) survives across `state()`
  calls and parent-process refreshes, so query results are stable across
  job restarts.
- The dashboard server keeps running without the parent needing an event
  loop.
- Producers on the same host write into one collector via a stable socket
  path, regardless of how many parent processes come and go.

The parent talks to telemetry via two channels:

- The job sidecar command socket: pickled `TelemetryRequest` control messages.
- The telemetry data socket (`/tmp/monarch_<apply_id>/telemetry.sock`):
  framed Arrow IPC from producers, decoded by Rust socket ingest into the
  collector's scanner.

Lifecycle: `Telemetry.ensure_open(apply_id, host_meshes)` is called twice per
`JobTrait._connect`. The first call (`host_meshes={}`) opens the job sidecar's
telemetry handle and activates the client process's `UnixSocketSink` *before*
raw host meshes are materialized, so host-mesh creation events are captured.
The second call (with materialised `host_meshes`) triggers worker fan-out: the
telemetry handle spawns per-host `TelemetryActor` candidates and hands the live
worker refs to the client collector for query fan-out.
"""

from __future__ import annotations

import functools
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, cast, Mapping, TypedDict

from monarch._rust_bindings.monarch_distributed_telemetry import (
    _set_unix_socket_sink_path,
)
from monarch._src.job.job_sidecar import create_job_sidecar, TelemetryRequest
from monarch._src.job.telemetry_actor import (
    telemetry_socket_dir,
    telemetry_socket_path,
    TelemetryActor,
)
from monarch.actor import context, HostMesh, ProcMesh, shutdown_context
from monarch.distributed_telemetry.engine import QueryEngine
from monarch.monarch_dashboard.server.app import start_dashboard
from monarch.monarch_dashboard.server.query_engine_adapter import QueryEngineAdapter

logger: logging.Logger = logging.getLogger(__name__)

_sidecar_socket_path: str | None = None
_startup_provider_registered: bool = False


@dataclass
class TelemetryConfig:
    """Configuration for automatic telemetry startup.

    When configured via ``JobTrait.enable_telemetry``, telemetry
    (and optionally a dashboard) is started when ``state()`` is called.

    Args:
        batch_size: Number of rows to buffer before flushing to a RecordBatch.
        retention_secs: Retention window in seconds for message tables.
            0 disables retention.
        include_dashboard: Whether to start the monarch dashboard web server.
        dashboard_port: Preferred port for the dashboard.
        snapshot_interval_secs: Interval in seconds between periodic mesh
            introspection snapshots. Snapshots capture the mesh topology
            into the telemetry query surface. 0 disables periodic capture
            (default). When ``include_dashboard`` is True and this is 0,
            it is automatically set to 30s because the dashboard requires
            snapshot data for system actor filtering.
        use_sidecar: Opt in to telemetry hosted by the job sidecar, which
            *replaces* the legacy in-process collector for this job (the two
            never run together). Transitional flag for the migration; defaults
            to the legacy path. When True, ``state.query_engine`` is None and
            ``state.query_engine_client`` serves queries instead.
    """

    batch_size: int = 1000
    retention_secs: int = 600
    include_dashboard: bool = False
    dashboard_port: int = 8265
    snapshot_interval_secs: float = 0  # 0 = disabled
    use_sidecar: bool = False

    def __post_init__(self) -> None:
        if self.include_dashboard and self.snapshot_interval_secs <= 0:
            self.snapshot_interval_secs = 30.0


# Successful response from the telemetry handle's `open_or_refresh` control message,
# pickled back over the command socket. The error case rides on the same
# wire as `{"error": traceback_str}` and is narrowed away by `ensure_open`
# before the cast.
class _TelemetryResponse(TypedDict):
    telemetry_url: str
    dashboard_url: str
    socket_path: str


def _config_from_wire(config: Mapping[str, object]) -> TelemetryConfig:
    """Rebuild a `TelemetryConfig` from the pickled wire dict. Only the three
    fields the telemetry handle acts on cross the wire; the rest take their
    defaults."""
    return TelemetryConfig(
        retention_secs=cast(int, config["retention_secs"]),
        include_dashboard=cast(bool, config["include_dashboard"]),
        dashboard_port=cast(int, config["dashboard_port"]),
    )


def install_sidecar_socket_sink(socket_path: str) -> None:
    """Install the sidecar telemetry socket sink locally and in future workers."""
    global _sidecar_socket_path
    if _sidecar_socket_path not in (None, socket_path):
        logger.warning(
            "replacing telemetry data socket path %s with %s",
            _sidecar_socket_path,
            socket_path,
        )
    _sidecar_socket_path = socket_path

    _ensure_setup_actor_telemetry_provider()
    _set_unix_socket_sink_path(socket_path)


def _unix_socket_sink_startup() -> Callable[[], None] | None:
    socket_path = _sidecar_socket_path
    if socket_path is None:
        return None
    return functools.partial(_set_unix_socket_sink_path, socket_path)


def _ensure_setup_actor_telemetry_provider() -> None:
    """Install the SetupActor provider that activates worker Unix sinks."""
    global _startup_provider_registered
    if _startup_provider_registered:
        return

    from monarch._src.actor.proc_mesh import SetupActor

    SetupActor.register_startup_function(_unix_socket_sink_startup)
    _startup_provider_registered = True


class Telemetry:
    """Parent-process client for telemetry hosted by the job sidecar.

    Uses `create_job_sidecar` so the job sidecar gets launched on first call
    and reused on subsequent calls (keyed on `apply_id`, not on config —
    config edits do not restart the sidecar). `ensure_open` is the single
    interaction point: it opens or refreshes the telemetry handle and forwards
    the host meshes for worker fan-out.
    """

    def __init__(self, config: TelemetryConfig) -> None:
        self._config: TelemetryConfig = config

    def ensure_open(
        self,
        apply_id: str,
        host_meshes: Mapping[str, HostMesh] | None = None,
    ) -> _TelemetryResponse:
        """Ensure the job sidecar's telemetry handle is open and refreshed."""
        if not isinstance(apply_id, str):
            raise RuntimeError("telemetry requires an active apply_id")

        socket_dir = telemetry_socket_dir(apply_id)
        os.makedirs(socket_dir, mode=0o700, exist_ok=True)
        os.chmod(socket_dir, 0o700)
        # `create_job_sidecar` is idempotent and keyed on `apply_id`: the
        # second call for the same job reuses the existing job sidecar rather
        # than relaunching. That is what makes the two-phase bootstrap to
        # fan-out flow safe to invoke from any of JobTrait's `_connect` branches.
        guard = create_job_sidecar(apply_id)
        response = guard.send(
            TelemetryRequest(
                apply_id=apply_id,
                config={
                    "retention_secs": self._config.retention_secs,
                    "include_dashboard": self._config.include_dashboard,
                    "dashboard_port": self._config.dashboard_port,
                },
                host_meshes=dict(host_meshes or {}),
            )
        ).get()
        if not isinstance(response, dict):
            raise RuntimeError(f"unexpected telemetry handle response: {response!r}")
        # The wire carries either a `_TelemetryResponse` shape or an
        # `{"error": traceback_str}` envelope from the sidecar's exception
        # handler; re-raise on the parent side so failures surface here.
        error = response.get("error")
        if isinstance(error, str):
            raise RuntimeError(error)
        return cast(_TelemetryResponse, response)


class _TelemetryHandle:
    """Live telemetry resources hosted inside the job sidecar worker.

    Owns the client `TelemetryActor` (which owns the scanner), the
    `QueryEngine` that backs the dashboard's `/api/query` route, and the
    dashboard server info. The job sidecar control loop drives
    `open_or_refresh` and `shutdown`.
    """

    def __init__(self, apply_id: str) -> None:
        self._apply_id: str = apply_id
        # `None` until the first `open_or_refresh` runs `_bootstrap`; serves as
        # the recorded config for drift detection. We intentionally do NOT restart
        # on drift — that would drop the in-memory store and re-bind the data socket,
        # defeating the point of the sidecar surviving across parent restarts.
        self._first_config: TelemetryConfig | None = None
        self._client_actor: Any | None = None
        self._query_engine: QueryEngine | None = None
        self._dashboard_info: dict[str, object] | None = None
        # None means worker fan-out has not run yet. An empty list means fan-out
        # ran, but no worker collectors became active.
        self._worker_proc_meshes: list[ProcMesh] | None = None

    @property
    def client_socket_path(self) -> str:
        return telemetry_socket_path(self._apply_id)

    def open_or_refresh(
        self,
        host_meshes: Mapping[str, HostMesh],
        config: Mapping[str, object],
    ) -> _TelemetryResponse:
        parsed = _config_from_wire(config)
        if self._first_config is None:
            self._bootstrap(parsed)
            self._first_config = parsed
        elif parsed != self._first_config:
            # Config drift is intentionally ignored. Restarting here would drop
            # the in-memory telemetry store and re-bind the data socket.
            logger.warning("telemetry handle config drift ignored")

        if host_meshes and self._worker_proc_meshes is None:
            self._worker_proc_meshes = self._spawn_telemetry_actors(host_meshes, parsed)

        dashboard_info = self._dashboard_info
        if dashboard_info is None:
            raise RuntimeError("telemetry handle is not open")
        api_url = dashboard_info["api_url"]
        url = dashboard_info["url"]
        if not isinstance(api_url, str) or not isinstance(url, str):
            raise RuntimeError(f"invalid dashboard info: {dashboard_info!r}")
        return {
            "telemetry_url": api_url,
            "dashboard_url": url,
            "socket_path": self.client_socket_path,
        }

    def _bootstrap(self, config: TelemetryConfig) -> None:
        # Reach the Python-wrapped `ProcMesh` via `monarch.actor.context()`.
        # The raw `PyProcMesh_Ref` returned by `bootstrap_host()` lacks
        # `.spawn`, so we cannot use it directly.
        actor_context = context()
        proc_mesh = actor_context.actor_instance.proc_mesh
        client_actor = proc_mesh.spawn(
            "telemetry",
            TelemetryActor,
            self._apply_id,
            config.retention_secs,
        )
        # `activate` is the actor's bind decision: triggers the
        # non-destructive Unix-socket bind and stands up the scanner. We
        # do not branch on the return — failures stay scannerless and the
        # scan endpoint surfaces them as best-effort empty results.
        client_actor.activate.call_one().get()

        query_engine = QueryEngine(client_actor)
        dashboard_info = start_dashboard(
            adapter=QueryEngineAdapter(query_engine),
            port=config.dashboard_port,
        )
        # Self-activate the sidecar process's own `UnixSocketSink` against
        # the client socket so telemetry emitted *by* the sidecar (dashboard
        # logs, query handler traces) flows into the same collector. Without
        # this the sidecar would be deaf to its own events.
        _set_unix_socket_sink_path(self.client_socket_path)

        self._client_actor = client_actor
        self._query_engine = query_engine
        self._dashboard_info = dashboard_info
        if config.include_dashboard:
            logger.info("Monarch Dashboard: %s", dashboard_info["url"])

    def _spawn_telemetry_actors(
        self,
        host_meshes: Mapping[str, HostMesh],
        config: TelemetryConfig,
    ) -> list[ProcMesh]:
        client_actor = self._client_actor
        if client_actor is None:
            raise RuntimeError("telemetry sidecar has no client actor")

        worker_proc_meshes: list[ProcMesh] = []
        worker_collector_meshes: list[Any] = []
        for host_mesh in host_meshes.values():
            try:
                proc_mesh, worker_collector_mesh = (
                    self._start_worker_telemetry_collector(
                        host_mesh,
                        config,
                    )
                )
            except Exception:
                logger.warning(
                    "failed to start worker telemetry collector",
                    exc_info=True,
                )
                continue
            worker_proc_meshes.append(proc_mesh)
            if worker_collector_mesh is not None:
                worker_collector_meshes.append(worker_collector_mesh)

        client_actor.set_worker_collector_meshes.call_one(worker_collector_meshes).get()
        return worker_proc_meshes

    def _start_worker_telemetry_collector(
        self,
        host_mesh: HostMesh,
        config: TelemetryConfig,
    ) -> tuple[ProcMesh, Any | None]:
        proc_mesh = host_mesh.spawn_procs(name="telemetry_hosts")
        try:
            worker_collector_mesh = proc_mesh.spawn(
                "TelemetryActor",
                TelemetryActor,
                self._apply_id,
                config.retention_secs,
            )
            active = any(
                active for _rank, active in worker_collector_mesh.activate.call().get()
            )
        except Exception:
            self._stop_worker_mesh(
                proc_mesh,
                "telemetry collector startup failed",
                "failed to stop failed telemetry worker proc mesh",
            )
            raise

        if active:
            return proc_mesh, worker_collector_mesh

        # TODO: avoid spawning the local worker collector when the root
        # collector already owns the socket. Until then, keep the proc alive so
        # mesh-admin snapshots can still resolve it, but stop the inactive
        # collector actor so queries do not fan out to it.
        self._stop_worker_mesh(
            worker_collector_mesh,
            "telemetry collector inactive",
            "failed to stop inactive telemetry worker collector",
        )
        return proc_mesh, None

    def _stop_worker_mesh(
        self,
        mesh: Any,
        reason: str,
        log_message: str,
    ) -> None:
        try:
            mesh.stop(reason).get()
        except Exception:
            logger.info(log_message, exc_info=True)

    def shutdown(self) -> None:
        for proc_mesh in self._worker_proc_meshes or []:
            try:
                proc_mesh.stop("telemetry shutdown").get()
            except Exception:
                logger.info(
                    "failed to stop telemetry worker proc mesh",
                    exc_info=True,
                )
        # Drain in-flight async work with a short cap. Beyond that, we
        # prefer a quick teardown over guaranteed completion — the sidecar
        # is being asked to die and the parent has already moved on.
        try:
            shutdown_context().get(timeout=5.0)
        except TimeoutError:
            logger.info("telemetry handle shutdown timed out")
        try:
            os.unlink(self.client_socket_path)
        except OSError:
            pass
