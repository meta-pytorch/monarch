# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Lifecycle components for ``JobTrait``.

``JobTrait`` owns scheduler state and the current allocation. ``JobComponent``
owns the configuration and live handles for one job-scoped capability: mounts,
telemetry, admin, or snapshots.

The hooks mirror the job lifecycle after an allocation is selected:

- ``before_connect(job)``: before ``JobTrait._connect()`` materializes raw host
  meshes.
- ``connect(job, host_meshes)``: after raw host meshes exist; return the final
  user-facing meshes.
- ``state(job, job_state)``: after final meshes are wrapped in ``JobState``;
  start services and attach fields to the object returned to the caller.
- ``reset_runtime()``: drop local live handles.

``JobComponents`` is a stable typed container. It owns optional component
presence, dependency wiring, and phase ordering; ``JobTrait`` owns allocation
identity.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from monarch._rust_bindings.monarch_hyperactor.host_mesh import PyMeshAdminRef
from monarch._src.actor.host_mesh import _spawn_admin
from monarch._src.actor.sync_state import fake_sync_state
from monarch._src.job._telemetry_query_client import QueryEngineClient
from monarch._src.job.mount_config import Mounts
from monarch._src.job.telemetry_config import (
    install_sidecar_socket_sink,
    Telemetry,
    TelemetryConfig,
)
from monarch.actor import HostMesh

if TYPE_CHECKING:
    from monarch._src.job.job import JobState, JobTrait

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class MeshAdminConfig:
    """Configuration for automatic mesh admin agent startup.

    When configured via ``JobTrait.enable_admin``, a MeshAdminAgent HTTP
    server is spawned when ``state()`` is called. The server aggregates
    topology across all host meshes and exposes it via a REST API that the
    admin TUI can attach to.

    Args:
        admin_addr: Bind address for the admin HTTP server.  When
            ``None`` the server picks an available address automatically.
    """

    admin_addr: Optional[str] = None


class JobComponent:
    """A job-scoped component driven by ``JobTrait``."""

    def before_connect(self, job: "JobTrait") -> None:
        """Run at the start of ``JobTrait._connect``, before raw host meshes exist."""

    def connect(
        self, job: "JobTrait", host_meshes: Dict[str, HostMesh]
    ) -> Dict[str, HostMesh]:
        """Run during ``JobTrait._connect`` to produce final user-facing meshes."""
        return host_meshes

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        """Run during ``JobTrait.state`` to bring services up on final meshes."""

    def reset_runtime(self) -> None:
        """Drop local live handles owned by this component."""


class MountComponent(JobComponent):
    """Open configured mounts and apply per-mesh python executables.

    Runs in ``connect`` -- after ``JobTrait._connect`` materializes the raw
    host meshes and before any service comes up -- so it produces the final
    meshes that both the caller and the downstream ``state`` services see.
    Config drift is copied in place; the next ``connect`` lets
    ``Mounts.ensure_open`` refresh, replace, or clear sidecar mount state.
    """

    def __init__(self) -> None:
        self._mounts: Mounts = Mounts()
        self._python_executables: Dict[str, str] = {}
        self._default_python_exe: Optional[str] = None

    def connect(
        self, job: "JobTrait", host_meshes: Dict[str, HostMesh]
    ) -> Dict[str, HostMesh]:
        apply_id = job.apply_id
        if apply_id is not None:
            self._mounts.ensure_open(apply_id, host_meshes)
        result: Dict[str, HostMesh] = {}
        for mesh_name, mesh in host_meshes.items():
            exe = self.python_executable_for_mesh(mesh_name)
            if exe is not None:
                mesh = mesh.with_python_executable(exe)
            result[mesh_name] = mesh
        return result

    def python_executable_for_mesh(self, mesh_name: str) -> Optional[str]:
        return self._python_executables.get(mesh_name, self._default_python_exe)

    def remote_mount(
        self,
        source: str,
        mntpoint: Optional[str] = None,
        meshes: Optional[List[str]] = None,
        python_exe: Optional[str] = ".venv/bin/python",
        **kwargs: Any,
    ) -> None:
        default_exe_path = None
        mesh_exe_path = None
        if python_exe is not None:
            abs_source = os.path.abspath(source)
            local_exe = os.path.join(abs_source, python_exe)
            if not os.path.isfile(local_exe):
                raise ValueError(
                    f"python_exe '{python_exe}' not found locally at '{local_exe}'. "
                    f"Ensure the virtual environment exists in '{source}' before calling remote_mount."
                )
            abs_mntpoint = (
                os.path.abspath(mntpoint) if mntpoint is not None else abs_source
            )
            exe_path = os.path.join(abs_mntpoint, python_exe)
            if meshes is None:
                default_exe_path = exe_path
            else:
                mesh_exe_path = exe_path
        self._mounts.remote_mount(
            source=source, mntpoint=mntpoint, meshes=meshes, **kwargs
        )
        if default_exe_path is not None:
            self._default_python_exe = default_exe_path
        elif mesh_exe_path is not None and meshes is not None:
            for mesh_name in meshes:
                self._python_executables[mesh_name] = mesh_exe_path

    def gather_mount(
        self,
        remote_mount_point: str,
        local_mount_point: str,
        meshes: Optional[List[str]] = None,
    ) -> None:
        self._mounts.gather_mount(
            remote_mount_point=remote_mount_point,
            local_mount_point=local_mount_point,
            meshes=meshes,
        )


class TelemetryComponent(JobComponent):
    """Host telemetry for a job.

    ``before_connect`` opens the sidecar's telemetry handle and installs the
    client-process Unix socket sink *before* raw host meshes are materialized
    so host-mesh creation events are captured. ``connect`` asks the sidecar to
    fan worker collectors out across the materialized host meshes. Config drift
    clears parent-side handles, but the sidecar keeps its in-memory telemetry
    store for the same ``apply_id``.
    Telemetry is best-effort: any failure is logged and leaves telemetry
    disabled for this job rather than failing ``state()``.
    """

    def __init__(self, config: TelemetryConfig) -> None:
        self._config = config
        self._telemetry_url: Optional[str] = None
        self._dashboard_url: Optional[str] = None
        self._query_engine_client: Optional[QueryEngineClient] = None

    def reset_runtime(self) -> None:
        self._query_engine_client = None
        self._telemetry_url = None
        self._dashboard_url = None

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_query_engine_client"] = None
        state["_telemetry_url"] = None
        state["_dashboard_url"] = None
        return state

    def before_connect(self, job: "JobTrait") -> None:
        if self._query_engine_client is not None:
            return
        apply_id = job.apply_id
        if apply_id is None:
            return
        try:
            response = Telemetry(self._config).ensure_open(apply_id, host_meshes={})
            telemetry_url = response.get("telemetry_url")
            dashboard_url = response.get("dashboard_url")
            socket_path = response.get("socket_path")
            if not isinstance(telemetry_url, str) or not isinstance(socket_path, str):
                raise RuntimeError(
                    f"invalid job sidecar telemetry response: {response!r}"
                )
            self._telemetry_url = telemetry_url
            if isinstance(dashboard_url, str):
                self._dashboard_url = dashboard_url
                if self._config.include_dashboard:
                    print(f"Monarch Dashboard: {dashboard_url}", flush=True)
            self._query_engine_client = QueryEngineClient(telemetry_url)
            install_sidecar_socket_sink(socket_path)
        except Exception:
            # Reset so a partial bootstrap leaves a clean "disabled" state:
            # fan-out no-ops and `state` exposes no query client.
            self._query_engine_client = None
            self._telemetry_url = None
            self._dashboard_url = None
            logger.warning(
                "job sidecar telemetry bootstrap failed; telemetry disabled for this job",
                exc_info=True,
            )

    def connect(
        self, job: "JobTrait", host_meshes: Dict[str, HostMesh]
    ) -> Dict[str, HostMesh]:
        if self._query_engine_client is None:
            return host_meshes
        apply_id = job.apply_id
        if apply_id is None:
            return host_meshes
        try:
            # `_telemetry_url`/`_dashboard_url` are already set by
            # `before_connect`; this call only triggers worker fan-out, so its
            # response is intentionally ignored.
            Telemetry(self._config).ensure_open(
                apply_id,
                host_meshes=host_meshes,
                spawn_worker_collectors=job._should_spawn_telemetry_worker_collector_actors(),
            )
        except Exception:
            logger.warning(
                "job sidecar telemetry worker fan-out failed",
                exc_info=True,
            )
        return host_meshes

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        if self._query_engine_client is not None:
            job_state.query_engine_client = self._query_engine_client
            job_state.telemetry_url = self._telemetry_url
            job_state.dashboard_url = self._dashboard_url


class AdminComponent(JobComponent):
    """Start the mesh admin agent once per allocation.

    Owns the admin URL and ref. If telemetry is configured, the admin agent
    receives the telemetry URL from the active telemetry component.
    ``JobComponents`` resets this runtime when admin config drifts or telemetry
    config changes.
    """

    def __init__(
        self,
        config: MeshAdminConfig,
        telemetry: Optional[TelemetryComponent],
    ) -> None:
        self._config = config
        self._telemetry = telemetry
        self._admin_url: Optional[str] = None
        self._admin_ref: Optional[PyMeshAdminRef] = None

    def reset_runtime(self) -> None:
        self._admin_url = None
        self._admin_ref = None

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_admin_url"] = None
        state["_admin_ref"] = None
        return state

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        if self._admin_url is None:
            telemetry_url = (
                self._telemetry._telemetry_url if self._telemetry is not None else None
            )
            # state() is a sync API but is commonly called from inside
            # asyncio.run(...); mask the running loop so the inner .get() does not
            # trip the Future.get-in-loop check.
            with fake_sync_state():
                self._admin_url, self._admin_ref = _spawn_admin(
                    list(job_state._hosts.values()),
                    admin_addr=self._config.admin_addr,
                    telemetry_url=telemetry_url,
                ).get()
        job_state.admin_url = self._admin_url


class SnapshotComponent(JobComponent):
    """Start periodic mesh-introspection snapshots once per allocation.

    Requires both telemetry and admin to have come up, plus a positive
    ``snapshot_interval_secs``. The snapshot actor is owned by proc lifecycle,
    so this component has no fine-grained stop handle once started.

    Because the actor cannot be stopped, ``_started`` stays sticky for the life
    of the allocation: once running, the snapshot is not restarted even when its
    telemetry or admin dependencies are reset by config drift. Restarting would
    spawn a duplicate actor against the new runtime while the original keeps
    capturing from the old one. ``JobComponents`` warns on such drift; picking up
    new config requires killing and re-applying the job.
    """

    def __init__(
        self,
        telemetry: TelemetryComponent,
        admin: AdminComponent,
    ) -> None:
        self._telemetry = telemetry
        self._admin = admin
        self._started = False

    def reset_runtime(self) -> None:
        self._started = False

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_started"] = False
        return state

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        if self._started:
            return
        admin_ref = self._admin._admin_ref
        if admin_ref is None:
            return
        snapshot_interval_secs = self._telemetry._config.snapshot_interval_secs
        if snapshot_interval_secs <= 0:
            return

        from monarch.actor import context

        instance = context().actor_instance._as_rust()
        telemetry_url = self._telemetry._telemetry_url
        if telemetry_url is None:
            return
        from monarch._rust_bindings.monarch_extension.snapshot_integration import (
            _start_periodic_snapshots_http,
        )

        _start_periodic_snapshots_http(
            base_url=telemetry_url,
            admin_ref=admin_ref,
            instance=instance,
            interval_secs=snapshot_interval_secs,
        )
        self._started = True


@dataclass
class JobComponents:
    """Stable component set for one job.

    Field order is dependency order. Mounts always run first, telemetry
    feeds admin, and snapshots require both telemetry and admin.
    """

    mounts: MountComponent = field(default_factory=MountComponent)
    telemetry: Optional[TelemetryComponent] = None
    admin: Optional[AdminComponent] = None
    snapshot: Optional[SnapshotComponent] = None

    def _ordered(self) -> List[JobComponent]:
        components: List[JobComponent] = [self.mounts]
        if self.telemetry is not None:
            components.append(self.telemetry)
        if self.admin is not None:
            components.append(self.admin)
        if self.snapshot is not None:
            components.append(self.snapshot)
        return components

    def before_connect(self, job: "JobTrait") -> None:
        for component in self._ordered():
            component.before_connect(job)

    def connect(
        self, job: "JobTrait", host_meshes: Dict[str, HostMesh]
    ) -> Dict[str, HostMesh]:
        for component in self._ordered():
            host_meshes = component.connect(job, host_meshes)
        return host_meshes

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        for component in self._ordered():
            component.state(job, job_state)

    def reset_runtime(self) -> None:
        for component in reversed(self._ordered()):
            component.reset_runtime()

    def configure_telemetry(self, config: TelemetryConfig) -> None:
        if self.telemetry is None:
            self.telemetry = TelemetryComponent(config)
            telemetry_changed = True
        else:
            telemetry_changed = self.telemetry._config != config
            if telemetry_changed:
                self.telemetry.reset_runtime()
            self.telemetry._config = config
        if telemetry_changed:
            self._warn_if_snapshot_stale()
        if self.admin is not None:
            if telemetry_changed:
                self.admin.reset_runtime()
            # Admin config is unchanged here; only rewire its telemetry handle.
            self.admin._telemetry = self.telemetry
        self._configure_snapshot()

    def configure_admin(self, config: MeshAdminConfig) -> None:
        if self.admin is None:
            self.admin = AdminComponent(config, self.telemetry)
        else:
            self._set_admin_inputs(config)
        self._configure_snapshot()

    def _set_admin_inputs(self, config: MeshAdminConfig) -> None:
        admin = self.admin
        if admin is None:
            return
        if admin._config != config:
            admin.reset_runtime()
            self._warn_if_snapshot_stale()
        admin._config = config
        admin._telemetry = self.telemetry

    def _warn_if_snapshot_stale(self) -> None:
        """Warn when a dependency restarts under an already-running snapshot.

        The snapshot actor is fire-and-forget and cannot be stopped within an
        allocation, so a restarted telemetry or admin runtime leaves it bound to
        the old runtime. See :class:`SnapshotComponent`.

        TODO: give periodic snapshot capture an abort handle so
        ``SnapshotComponent.reset_runtime`` can stop the old actor; then reset
        the snapshot in this drift cascade alongside telemetry and admin.
        """
        if self.snapshot is not None and self.snapshot._started:
            logger.warning(
                "telemetry or admin config changed while periodic snapshots are running: "
                "snapshots stay bound to the original runtime until the job is killed and re-applied"
            )

    def _configure_snapshot(self) -> None:
        if self.telemetry is not None and self.admin is not None:
            if self.snapshot is None:
                self.snapshot = SnapshotComponent(self.telemetry, self.admin)
            else:
                self.snapshot._telemetry = self.telemetry
                self.snapshot._admin = self.admin
        elif self.snapshot is not None:
            self.snapshot = None
