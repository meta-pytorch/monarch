# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Lifecycle components for ``JobTrait``.

``JobTrait`` owns scheduler state and the current allocation. ``JobComponent``
owns the configuration and live handles for one job-scoped capability: mounts,
telemetry, or admin.

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
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from monarch._src.job._telemetry_query_client import QueryEngineClient
from monarch._src.job.job_sidecar import ensure_job_admin
from monarch._src.job.mount_config import Mounts
from monarch._src.job.telemetry_config import (
    install_sidecar_socket_sink,
    Telemetry,
    TelemetryConfig,
)
from monarch.actor import HostMesh, this_host
from monarch.config import get_global_config

if TYPE_CHECKING:
    from monarch._src.job.job import JobState, JobTrait

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class MeshAdminConfig:
    """Configuration for automatic mesh admin agent startup.

    The job sidecar starts a MeshAdminAgent HTTP server on the first
    ``state()`` call; later calls, from any process using the job, reuse it.
    The server aggregates topology across all host meshes and exposes it via a
    REST API that the admin TUI can attach to. If the address is taken,
    ``state()`` raises.

    Args:
        admin_addr: Bind address for the admin HTTP server. When ``None``, the
            server uses this process's ``mesh_admin_addr`` config.
    """

    admin_addr: Optional[str] = None


class JobComponent:
    """A job-scoped component driven by ``JobTrait``."""

    def before_connect(self, job: "JobTrait") -> None:
        """Run at the start of ``JobTrait._connect``, before raw host meshes exist."""

    def connect(
        self,
        job: "JobTrait",
        host_meshes: Dict[str, HostMesh],
        via_gateway: bool = False,
    ) -> Dict[str, HostMesh]:
        """Run during ``JobTrait._connect`` to produce final user-facing meshes.

        ``via_gateway`` says the job reaches its workers only through a scheduler gateway.
        Resolved by the caller so every component sees the same answer.
        """
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
        self,
        job: "JobTrait",
        host_meshes: Dict[str, HostMesh],
        via_gateway: bool = False,
    ) -> Dict[str, HostMesh]:
        apply_id = job.apply_id
        if apply_id is not None:
            self._mounts.ensure_open(apply_id, host_meshes, via_gateway)
        result: Dict[str, HostMesh] = {}
        for mesh_name, mesh in host_meshes.items():
            exe = self.python_executable_for_mesh(mesh_name)
            if exe is not None:
                mesh = mesh.with_python_executable(exe)
            result[mesh_name] = mesh
        return result

    def needs_sidecar(self) -> bool:
        """Whether configured mounts require the shared job sidecar."""
        return bool(self._mounts._remote_entries or self._mounts._gather_entries)

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

    @property
    def telemetry_url(self) -> Optional[str]:
        return self._telemetry_url

    def query_client(self, job: "JobTrait") -> QueryEngineClient:
        """Connect to the job's existing telemetry query endpoint."""
        apply_id = job.apply_id
        if apply_id is None:
            raise RuntimeError("the active job has no apply id")
        response = Telemetry(self._config).info(apply_id)
        telemetry_url = response["telemetry_url"]
        self._telemetry_url = telemetry_url
        self._query_engine_client = QueryEngineClient(telemetry_url)
        return self._query_engine_client

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
            self._query_engine_client = QueryEngineClient(telemetry_url)
            install_sidecar_socket_sink(socket_path)
            if self._dashboard_url is not None and self._config.include_dashboard:
                print(f"Monarch Dashboard: {self._dashboard_url}", flush=True)
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
        self,
        job: "JobTrait",
        host_meshes: Dict[str, HostMesh],
        via_gateway: bool = False,
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
    """Run the job's mesh admin agent in the job sidecar.

    The sidecar starts the admin once per allocation and keeps it for the life
    of the job, so later ``state`` calls reuse it. Each call adds this
    process's host, so actors spawned here stay in the topology. With live
    telemetry, the admin links to the dashboard and the sidecar captures
    periodic topology snapshots.
    """

    def __init__(
        self,
        config: MeshAdminConfig,
        telemetry: Optional[TelemetryComponent],
    ) -> None:
        self._config = config
        self._telemetry = telemetry
        self._admin_url: Optional[str] = None

    def reset_runtime(self) -> None:
        self._admin_url = None

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_admin_url"] = None
        return state

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        apply_id = job.apply_id
        assert apply_id is not None, "state() runs on an applied job"
        telemetry = self._telemetry
        telemetry_url = telemetry.telemetry_url if telemetry is not None else None
        # Resolve the default here: the sidecar does not see this process's
        # runtime config.
        admin_addr = self._config.admin_addr
        if admin_addr is None:
            admin_addr = str(get_global_config()["mesh_admin_addr"])
        admin_url = ensure_job_admin(
            apply_id,
            job_state._hosts,
            client_host=this_host(),
            admin_addr=admin_addr,
            telemetry_url=telemetry_url,
            snapshot_interval_secs=(
                telemetry._config.snapshot_interval_secs
                if telemetry is not None
                else None
            ),
        )
        # The admin prints its address in the sidecar log, not here.
        if admin_url != self._admin_url:
            print(f"Mesh admin: {admin_url}", file=sys.stderr, flush=True)
            self._admin_url = admin_url
        job_state.admin_url = admin_url


@dataclass
class JobComponents:
    """Stable component set for one job.

    Field order is dependency order. Mounts always run first, and telemetry
    feeds admin.
    """

    mounts: MountComponent = field(default_factory=MountComponent)
    telemetry: Optional[TelemetryComponent] = None
    admin: Optional[AdminComponent] = None

    def needs_sidecar(self) -> bool:
        """Whether mounts, telemetry, or admin require the shared job sidecar."""
        return (
            self.mounts.needs_sidecar()
            or self.telemetry is not None
            or self.admin is not None
        )

    def _ordered(self) -> List[JobComponent]:
        components: List[JobComponent] = [self.mounts]
        if self.telemetry is not None:
            components.append(self.telemetry)
        if self.admin is not None:
            components.append(self.admin)
        return components

    def before_connect(self, job: "JobTrait") -> None:
        for component in self._ordered():
            component.before_connect(job)

    def connect(
        self,
        job: "JobTrait",
        host_meshes: Dict[str, HostMesh],
        via_gateway: bool = False,
    ) -> Dict[str, HostMesh]:
        for component in self._ordered():
            host_meshes = component.connect(job, host_meshes, via_gateway)
        return host_meshes

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        for component in self._ordered():
            component.state(job, job_state)

    def reset_runtime(self) -> None:
        for component in reversed(self._ordered()):
            component.reset_runtime()

    def configure_telemetry(
        self,
        config: TelemetryConfig,
        mesh_admin_config: Optional[MeshAdminConfig] = None,
    ) -> None:
        """Configure telemetry and its mesh admin dependency."""
        telemetry = self.telemetry
        if telemetry is None:
            telemetry = TelemetryComponent(config)
            self.telemetry = telemetry
        elif telemetry._config != config:
            telemetry.reset_runtime()
            telemetry._config = config
        admin = self.admin
        if admin is None:
            self.admin = AdminComponent(
                mesh_admin_config
                if mesh_admin_config is not None
                else MeshAdminConfig(),
                telemetry,
            )
        else:
            admin._telemetry = telemetry
            if mesh_admin_config is not None:
                admin._config = mesh_admin_config

    def configure_admin(self, config: MeshAdminConfig) -> None:
        admin = self.admin
        if admin is None:
            self.admin = AdminComponent(config, self.telemetry)
        else:
            admin._config = config
