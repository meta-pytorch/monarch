# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import os
import sys
import traceback
from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional

from monarch._src.job.job_sidecar import (
    ClearMountsRequest,
    create_job_sidecar,
    find_job_sidecar,
    MountsRequest,
)
from monarch.actor import HostMesh

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class RemoteMountEntry:
    """Declarative configuration for a single FUSE remote mount."""

    source: str
    mntpoint: Optional[str] = None
    meshes: Optional[List[str]] = None
    kwargs: dict = field(default_factory=dict)

    def apply(self, host_meshes: Mapping[str, HostMesh]) -> "list[Any]":
        """Open the remote mount for each targeted mesh. Returns handles.

        If ``mntpoint`` contains ``$SUBDIR`` it is replaced with the mesh name,
        so multiple local hosts do not collide on the same mount point.
        """
        from monarch.remotemount.remotemount import remotemount as _remotemount

        handles = []
        for mesh_name, raw_mesh in host_meshes.items():
            if self.meshes is not None and mesh_name not in self.meshes:
                continue
            handler = _remotemount(
                raw_mesh, self.source, mntpoint=self.mntpoint, **self.kwargs
            )
            handler.open()
            handles.append(handler)
        return handles


@dataclass
class GatherMountEntry:
    """Declarative configuration for a single gather mount."""

    remote_mount_point: str
    local_mount_point: str
    meshes: Optional[List[str]] = None

    def apply(self, host_meshes: Mapping[str, HostMesh]) -> "list[Any]":
        """Start the gather mount for each targeted mesh. Returns handles.

        Single targeted mesh: mounts directly at ``local_mount_point``.
        Multiple targeted meshes: mounts each at ``local_mount_point/<mesh_name>``.
        """
        from monarch._src.gather_mount.gather_mount import gather_mount as _gather_mount

        target_meshes = [
            (mesh_name, raw_mesh)
            for mesh_name, raw_mesh in host_meshes.items()
            if self.meshes is None or mesh_name in self.meshes
        ]
        multi = len(target_meshes) > 1

        handles = []
        for mesh_name, raw_mesh in target_meshes:
            local_path = (
                os.path.join(self.local_mount_point, mesh_name)
                if multi
                else self.local_mount_point
            )
            handles.append(_gather_mount(raw_mesh, self.remote_mount_point, local_path))
        return handles


class Mounts:
    """Declarative mount configuration for a job.

    Call :meth:`remote_mount` and :meth:`gather_mount` to register mounts.
    The live handles are managed by :class:`MountsHandle` in the background job
    sidecar process.
    """

    def __init__(self) -> None:
        self._remote_entries: list[RemoteMountEntry] = []
        self._gather_entries: list[GatherMountEntry] = []

    def remote_mount(
        self,
        source: str,
        mntpoint: Optional[str] = None,
        meshes: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        """Register a local directory to be mounted on workers via FUSE."""
        self._remote_entries.append(
            RemoteMountEntry(
                source=source, mntpoint=mntpoint, meshes=meshes, kwargs=kwargs
            )
        )

    def gather_mount(
        self,
        remote_mount_point: str,
        local_mount_point: str,
        meshes: Optional[List[str]] = None,
    ) -> None:
        """Register a remote directory to be mounted locally via gather mount."""
        self._gather_entries.append(
            GatherMountEntry(
                remote_mount_point=remote_mount_point,
                local_mount_point=local_mount_point,
                meshes=meshes,
            )
        )

    def open(self, host_meshes: Mapping[str, HostMesh]) -> "MountsHandle":
        """Open all mounts against *host_meshes* and return the live handle."""
        return MountsHandle(self, host_meshes)

    def ensure_open(self, apply_id: str, host_meshes: Mapping[str, HostMesh]) -> None:
        """Ensure a background job sidecar is running for this configuration.

        Keyed on ``apply_id``: reuses an existing process, then sends a refresh
        so the sidecar picks up any mount config changes. Clears existing mount
        state when no mounts are configured.
        """
        if not self._remote_entries and not self._gather_entries:
            guard = find_job_sidecar(apply_id)
            if guard is not None:
                guard.send(ClearMountsRequest()).get()
            return

        guard = create_job_sidecar(apply_id)
        guard.send(MountsRequest(self, dict(host_meshes))).get()


class MountsHandle:
    """Live mount handles for a running background job sidecar."""

    def __init__(self, mounts: Mounts, host_meshes: Mapping[str, HostMesh]) -> None:
        self._active_remote: list[Any] = []
        self._active_gather: list[Any] = []
        for entry in mounts._remote_entries:
            self._active_remote.extend(entry.apply(host_meshes))
        for entry in mounts._gather_entries:
            self._active_gather.extend(entry.apply(host_meshes))

    def refresh(self) -> None:
        """Refresh remote mounts in-place; gather mounts are unaffected."""
        for handler in self._active_remote:
            try:
                handler.refresh(handler.sourcepath)
            except Exception:
                _dbg(
                    f"refresh: ERROR refreshing remote mount {handler.sourcepath!r}:\n"
                    + traceback.format_exc()
                )

    def close(self) -> None:
        """Unmount all remote and gather mounts."""
        for handler in self._active_remote:
            try:
                handler.close()
            except Exception:
                _dbg(
                    f"close: ERROR unmounting {handler.mntpoint!r}:\n"
                    + traceback.format_exc()
                )
        for mount in self._active_gather:
            try:
                mount.close()
            except Exception:
                _dbg("close: ERROR closing gather mount:\n" + traceback.format_exc())


def _dbg(msg: str) -> None:
    print(f"[job_sidecar pid={os.getpid()}] {msg}", file=sys.stderr, flush=True)
