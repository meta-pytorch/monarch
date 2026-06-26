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
from monarch._src.job.mount_config import Mounts
from monarch._src.job.telemetry_config import TelemetryConfig
from monarch.actor import HostMesh
from monarch.distributed_telemetry.actor import start_telemetry
from monarch.distributed_telemetry.engine import QueryEngine

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
    """Start the legacy in-process telemetry collector once per allocation.

    Owns the query engine, collector URL, and scanner. Dependent components
    receive this component explicitly instead of reading telemetry state from
    ``JobTrait``. ``JobComponents`` resets this runtime when config drifts so
    the next ``state`` starts a collector with the new config.
    """

    def __init__(self, config: TelemetryConfig) -> None:
        self._config = config
        self._query_engine: Optional[QueryEngine] = None
        self._telemetry_url: Optional[str] = None
        self._scanner: Any = None

    def reset_runtime(self) -> None:
        self._query_engine = None
        self._telemetry_url = None
        self._scanner = None

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_query_engine"] = None
        state["_telemetry_url"] = None
        state["_scanner"] = None
        return state

    def state(self, job: "JobTrait", job_state: "JobState") -> None:
        if self._query_engine is None:
            cfg = self._config
            self._query_engine, self._telemetry_url, self._scanner = start_telemetry(
                batch_size=cfg.batch_size,
                retention_secs=cfg.retention_secs,
                include_dashboard=cfg.include_dashboard,
                dashboard_port=cfg.dashboard_port,
            )
        job_state.query_engine = self._query_engine
        job_state.telemetry_url = self._telemetry_url


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
        scanner = self._telemetry._scanner
        admin_ref = self._admin._admin_ref
        if scanner is None or admin_ref is None:
            return
        snapshot_interval_secs = self._telemetry._config.snapshot_interval_secs
        if snapshot_interval_secs <= 0:
            return

        from monarch._rust_bindings.monarch_extension.snapshot_integration import (
            _start_periodic_snapshots,
        )
        from monarch.actor import context

        _start_periodic_snapshots(
            scanner=scanner,
            admin_ref=admin_ref,
            instance=context().actor_instance._as_rust(),
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
                self._warn_if_snapshot_stale()
            self.telemetry._config = config
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

        TODO: give ``_start_periodic_snapshots`` an abort handle so
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
