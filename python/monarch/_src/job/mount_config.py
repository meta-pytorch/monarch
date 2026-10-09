# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Iterator, List, Mapping, Optional

from monarch._src.gather_mount.gather_mount import _CLOSE_TIMEOUT_S
from monarch._src.job.job_sidecar import (
    ClearMountsRequest,
    find_job_sidecar,
    get_job_sidecar,
    MountsRequest,
    raise_for_sidecar_error,
)
from monarch.actor import HostMesh

logger: logging.Logger = logging.getLogger(__name__)

_MOUNT_REQUEST_TIMEOUT_S = 300.0


@dataclass
class RemoteMountEntry:
    """Declarative configuration for a single FUSE remote mount."""

    source: str
    mntpoint: Optional[str] = None
    meshes: Optional[List[str]] = None
    kwargs: dict = field(default_factory=dict)
    # Whether the process opening this mount reaches the workers only through a
    # scheduler gateway, and so cannot dial the broadcast chain's head. Set by
    # `Mounts.ensure_open` from the job's answer; it lives on the entry because the entry
    # is already what is pickled to the sidecar and what constructs the mount. False means
    # the client can dial directly.
    via_gateway: bool = False

    def create(self, host_meshes: Mapping[str, HostMesh]) -> Iterator[Any]:
        """Create a remote mount handle for each targeted mesh.

        If ``mntpoint`` contains ``$SUBDIR`` it is replaced with the mesh name,
        so multiple local hosts do not collide on the same mount point.
        """
        from monarch.remotemount.remotemount import remotemount as _remotemount

        for mesh_name, raw_mesh in host_meshes.items():
            if self.meshes is not None and mesh_name not in self.meshes:
                continue
            handler = _remotemount(
                raw_mesh, self.source, mntpoint=self.mntpoint, **self.kwargs
            )
            yield handler


@dataclass
class GatherMountEntry:
    """Declarative configuration for a single gather mount."""

    remote_mount_point: str
    local_mount_point: str
    meshes: Optional[List[str]] = None

    def apply(self, host_meshes: Mapping[str, HostMesh]) -> Iterator[Any]:
        """Start each targeted gather mount and yield its handle.

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

        for mesh_name, raw_mesh in target_meshes:
            local_path = (
                os.path.join(self.local_mount_point, mesh_name)
                if multi
                else self.local_mount_point
            )
            yield _gather_mount(raw_mesh, self.remote_mount_point, local_path)


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

    def ensure_open(
        self,
        apply_id: str,
        host_meshes: Mapping[str, HostMesh],
        via_gateway: bool = False,
    ) -> None:
        """Ensure a background job sidecar is running for this configuration.

        Keyed on ``apply_id``: reuses an existing process, then sends a refresh
        so the sidecar picks up any mount config changes. Clears existing mount
        state when no mounts are configured.
        """
        if not self._remote_entries and not self._gather_entries:
            guard = find_job_sidecar(apply_id)
            if guard is not None:
                response = guard.send(ClearMountsRequest()).get(
                    timeout=_MOUNT_REQUEST_TIMEOUT_S
                )
                raise_for_sidecar_error(response, "clear job mounts")
            return

        guard = get_job_sidecar(apply_id)
        # Stamped here, not at `remote_mount()` time: the answer depends on the running
        # job's scheduler, which is not known when the mount is declared.
        for entry in self._remote_entries:
            entry.via_gateway = via_gateway
        response = guard.send(MountsRequest(self, dict(host_meshes))).get(
            timeout=_MOUNT_REQUEST_TIMEOUT_S
        )
        raise_for_sidecar_error(response, "open or refresh job mounts")


class MountOpenError(RuntimeError):
    """An open failure whose remaining mounts still need cleanup."""

    def __init__(self, handle: "MountsHandle", error: Exception) -> None:
        super().__init__(f"{error}; partial mounts still require cleanup")
        self.handle = handle


class MountsHandle:
    """Live mount handles for a running background job sidecar."""

    def __init__(self, mounts: Mounts, host_meshes: Mapping[str, HostMesh]) -> None:
        self._active_remote: list[Any] = []
        self._active_gather: list[Any] = []
        try:
            for entry in mounts._remote_entries:
                for handler in entry.create(host_meshes):
                    # Own the handle before open starts so partial opens can be closed.
                    self._active_remote.append(handler)
                    handler.open(entry.via_gateway)
            for entry in mounts._gather_entries:
                for mount in entry.apply(host_meshes):
                    self._active_gather.append(mount)
        except Exception as error:
            try:
                self.close()
            except Exception:
                _dbg(
                    "initialization cleanup: ERROR closing partial mounts:\n"
                    + traceback.format_exc()
                )
                raise MountOpenError(self, error) from error
            raise

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
        deadline = time.monotonic() + _CLOSE_TIMEOUT_S
        failures: list[tuple[str, Exception]] = []
        active_remote = []
        for handler in self._active_remote:
            try:
                handler.close()
            except Exception as error:
                active_remote.append(handler)
                failures.append((f"remote mount {handler.mntpoint!r}", error))
                _dbg(
                    f"close: ERROR unmounting {handler.mntpoint!r}:\n"
                    + traceback.format_exc()
                )
        self._active_remote = active_remote
        active_gather = []
        for mount in self._active_gather:
            try:
                mount.close(deadline=deadline)
            except Exception as error:
                active_gather.append(mount)
                failures.append(("gather mount", error))
                _dbg("close: ERROR closing gather mount:\n" + traceback.format_exc())
        self._active_gather = active_gather
        if failures:
            labels = ", ".join(label for label, _ in failures)
            raise RuntimeError(
                f"failed to close {len(failures)} mount(s): {labels}"
            ) from failures[0][1]


def _dbg(msg: str) -> None:
    print(f"[job_sidecar pid={os.getpid()}] {msg}", file=sys.stderr, flush=True)
