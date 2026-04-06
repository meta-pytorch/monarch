# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import os
import pickle
import socket
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, List, Optional

from monarch._src.job.job_state import JobState

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class RemoteMountEntry:
    """Declarative configuration for a single FUSE remote mount."""

    source: str
    mntpoint: Optional[str] = None
    meshes: Optional[List[str]] = None
    kwargs: dict = field(default_factory=dict)

    def apply(self, raw_state: JobState) -> "list[Any]":
        """Open the remote mount for each targeted mesh. Returns handles.

        If ``mntpoint`` contains ``$SUBDIR`` it is replaced with the mesh name,
        so multiple local hosts do not collide on the same mount point.
        """
        from monarch.remotemount.remotemount import remotemount as _remotemount

        handles = []
        for mesh_name, raw_mesh in raw_state._hosts.items():
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

    def apply(self, raw_state: JobState) -> "list[Any]":
        """Start the gather mount for each targeted mesh. Returns handles.

        Single targeted mesh: mounts directly at ``local_mount_point``.
        Multiple targeted meshes: mounts each at ``local_mount_point/<mesh_name>``.
        """
        from monarch._src.gather_mount.gather_mount import gather_mount as _gather_mount

        target_meshes = [
            (mesh_name, raw_mesh)
            for mesh_name, raw_mesh in raw_state._hosts.items()
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
    The live handles are managed by :class:`MountsHandle` in the background
    mount process.
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

    def ensure_stopped(self, apply_id: str) -> None:
        """Shut down the background mount process for *apply_id*, if running.

        Does nothing if no mounts are configured or no process is found.
        """
        from monarch._src.job.process_guard import find_process

        lock_path = f"/tmp/monarch_mounts_{apply_id}.lock"
        guard = find_process(lock_path)
        if guard is not None:
            guard.shutdown()

    def open(self, job: "Any") -> "MountsHandle":
        """Open all mounts against *job* and return the live handle."""
        return MountsHandle(self, job._state())

    def ensure_open(self, job: "Any") -> None:
        """Ensure a background mount process is running for this configuration.

        Keyed on ``(apply_id, mounts)``: reuses an existing process with the
        same config, otherwise shuts down the old one and launches a new one.
        Always sends a refresh so the mount process picks up any changes.
        Does nothing if no mounts are configured.
        """
        apply_id = job.apply_id
        if not self._remote_entries and not self._gather_entries:
            self.ensure_stopped(apply_id)
            return

        from monarch._src.job.process_guard import ProcessGuard

        lock_path = f"/tmp/monarch_mounts_{apply_id}.lock"
        guard = ProcessGuard.create(
            lock_path,
            (apply_id, self),
            [sys.executable, "-m", "monarch._src.job._mount_worker"],
        )
        guard.send((self, job)).get()


class MountsHandle:
    """Live mount handles for a running background mount process."""

    def __init__(self, mounts: Mounts, raw_state: JobState) -> None:
        self._active_remote: list[Any] = []
        self._active_gather: list[Any] = []
        for entry in mounts._remote_entries:
            self._active_remote.extend(entry.apply(raw_state))
        for entry in mounts._gather_entries:
            self._active_gather.extend(entry.apply(raw_state))

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


# ── Background mount process ──────────────────────────────────────────────────


def _dbg(msg: str) -> None:
    print(f"[mount_process pid={os.getpid()}] {msg}", file=sys.stderr, flush=True)


def _run_mount_process(socket_path: str) -> None:
    """Run in the child process: bind socket then serve refresh/shutdown requests."""
    import signal as _signal

    _signal.signal(_signal.SIGINT, _signal.SIG_DFL)
    _signal.signal(_signal.SIGTERM, _signal.SIG_DFL)

    from monarch._src.job.process_guard import _Shutdown

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)
    server.listen(5)
    _dbg(f"listening on {socket_path}")

    handle: "MountsHandle | None" = None

    while True:
        try:
            conn, _ = server.accept()
        except OSError:
            break
        try:
            f = conn.makefile("rb")
            while True:
                try:
                    # @lint-ignore PYTHONPICKLEISBAD
                    msg = pickle.load(f)
                except EOFError:
                    break  # client disconnected (e.g. readiness probe)
                if isinstance(msg, _Shutdown):
                    _dbg("received shutdown")
                    if handle is not None:
                        handle.close()
                    conn.close()
                    server.close()
                    try:
                        os.unlink(socket_path)
                    except OSError:
                        pass
                    _dbg("exiting")
                    return
                mounts, job = msg
                try:
                    t0 = time.time()
                    if handle is None:
                        _dbg("initialising mounts")
                        handle = mounts.open(job)
                        _dbg(f"mounts opened in {time.time() - t0:.2f}s")
                    else:
                        _dbg("refreshing mounts")
                        handle.refresh()
                        _dbg(f"refresh complete in {time.time() - t0:.2f}s")
                except Exception:
                    _dbg("ERROR during mount operation:\n" + traceback.format_exc())
                # @lint-ignore PYTHONPICKLEISBAD
                conn.sendall(pickle.dumps("ok"))
        except Exception:
            _dbg("ERROR handling connection:\n" + traceback.format_exc())
        finally:
            try:
                conn.close()
            except Exception:
                pass

    server.close()
    _dbg("exiting")
