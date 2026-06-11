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
`_state()` runs `bootstrap_host`, so the host-mesh creation events are
captured. The second call (with materialised `host_meshes`) triggers worker
fan-out: the telemetry handle spawns per-host `TelemetryActor` candidates and
hands the live worker refs to the client collector for query fan-out.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, cast, Mapping, TypedDict

from monarch._rust_bindings.monarch_distributed_telemetry import (
    _set_unix_socket_sink_path,
)
from monarch._src.job.job_sidecar import create_job_sidecar, TelemetryRequest
from monarch._src.job.telemetry_actor import (
    telemetry_socket_dir,
    telemetry_socket_path,
    TelemetryActor,
)
from monarch.actor import context, HostMesh, shutdown_context
from monarch.distributed_telemetry.engine import QueryEngine
from monarch.monarch_dashboard.server.app import start_dashboard
from monarch.monarch_dashboard.server.query_engine_adapter import QueryEngineAdapter

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class TelemetryConfig:
    """Configuration for automatic telemetry startup.

    When passed to a job constructor, telemetry (and optionally a dashboard)
    is started automatically when ``state()`` is called.

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
    """

    batch_size: int = 1000
    retention_secs: int = 600
    include_dashboard: bool = False
    dashboard_port: int = 8265
    snapshot_interval_secs: float = 0  # 0 = disabled

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

        dashboard_info = self._dashboard_info
        if dashboard_info is None:
            raise RuntimeError("telemetry handle is not open")
        local_url = dashboard_info["local_url"]
        url = dashboard_info["url"]
        if not isinstance(local_url, str) or not isinstance(url, str):
            raise RuntimeError(f"invalid dashboard info: {dashboard_info!r}")
        return {
            "telemetry_url": local_url,
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

    def shutdown(self) -> None:
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
