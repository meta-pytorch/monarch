# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Per-job sidecar command server.

The sidecar process is keyed by a job ``apply_id`` and kept alive by ``ProcessGuard``.
"""

import os
import pickle
import socket
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any

from monarch.actor import HostMesh


def job_sidecar_lock_path(apply_id: str) -> str:
    """Return the lock path for the per-job sidecar process."""
    return f"/tmp/monarch_job_sidecar_{apply_id}.lock"


def create_job_sidecar(apply_id: str) -> Any:
    """Ensure the per-job sidecar process is running and return its guard."""
    from monarch._src.job.process_guard import ProcessGuard

    return ProcessGuard.create(
        job_sidecar_lock_path(apply_id),
        apply_id,
        [sys.executable, "-m", "monarch._src.job._job_sidecar_worker"],
    )


def find_job_sidecar(apply_id: str) -> Any | None:
    """Return the per-job sidecar process guard if it exists."""
    from monarch._src.job.process_guard import find_process

    return find_process(job_sidecar_lock_path(apply_id))


def stop_job_sidecar(apply_id: str) -> None:
    """Shut down the per-job sidecar process for an apply id if it exists."""
    guard = find_job_sidecar(apply_id)
    if guard is not None:
        guard.shutdown()


@dataclass
class ClearMountsRequest:
    """Clear the job sidecar's mount state."""


@dataclass
class MountsRequest:
    """Refresh the job sidecar's mount state."""

    mounts: Any
    host_meshes: dict[str, HostMesh]


class _JobSidecarState:
    """State owned by the background job sidecar process."""

    def __init__(self) -> None:
        self._mounts_handle: Any | None = None
        self._mounts_key: bytes | None = None

    def handle_mounts(self, request: MountsRequest) -> str:
        # The request carries declarative mount config. Compare that serialized
        # config so edited mounts replace the live handles, while unchanged
        # config refreshes existing remote mounts in-place.
        # @lint-ignore PYTHONPICKLEISBAD
        mounts_key = pickle.dumps(request.mounts)
        t0 = time.time()
        if self._mounts_handle is None:
            _dbg("initialising mounts")
            self._mounts_handle = request.mounts.open(request.host_meshes)
            self._mounts_key = mounts_key
            _dbg(f"mounts opened in {time.time() - t0:.2f}s")
            return "ok"

        if mounts_key != self._mounts_key:
            _dbg("replacing mounts")
            self._mounts_handle.close()
            self._mounts_handle = request.mounts.open(request.host_meshes)
            self._mounts_key = mounts_key
            _dbg(f"mounts replaced in {time.time() - t0:.2f}s")
            return "ok"

        _dbg("refreshing mounts")
        self._mounts_handle.refresh()
        _dbg(f"refresh complete in {time.time() - t0:.2f}s")
        return "ok"

    def clear_mounts(self) -> str:
        """Close any live mounts and reset mount state."""
        if self._mounts_handle is not None:
            self._mounts_handle.close()
            self._mounts_handle = None
            self._mounts_key = None
        return "ok"

    def shutdown(self) -> None:
        self.clear_mounts()


def _dbg(msg: str) -> None:
    print(f"[job_sidecar pid={os.getpid()}] {msg}", file=sys.stderr, flush=True)


def _run_job_sidecar(socket_path: str) -> None:
    """Run in the child process: bind socket then serve refresh/shutdown requests."""
    import signal as _signal

    # Signal handlers are process-global and only legal from the main thread.
    if threading.current_thread() is threading.main_thread():
        _signal.signal(_signal.SIGINT, _signal.SIG_DFL)
        _signal.signal(_signal.SIGTERM, _signal.SIG_DFL)

    from monarch._src.job.process_guard import _Shutdown

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)
    server.listen(5)
    _dbg(f"listening on {socket_path}")

    state = _JobSidecarState()

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
                    state.shutdown()
                    conn.close()
                    server.close()
                    try:
                        os.unlink(socket_path)
                    except OSError:
                        pass
                    _dbg("exiting")
                    return
                try:
                    if isinstance(msg, MountsRequest):
                        response = state.handle_mounts(msg)
                    elif isinstance(msg, ClearMountsRequest):
                        response = state.clear_mounts()
                    else:
                        raise RuntimeError(f"unexpected job sidecar request: {msg!r}")
                except Exception:
                    _dbg(
                        "ERROR during job sidecar operation:\n" + traceback.format_exc()
                    )
                    response = "ok"
                # @lint-ignore PYTHONPICKLEISBAD
                conn.sendall(pickle.dumps(response))
        except Exception:
            _dbg("ERROR handling connection:\n" + traceback.format_exc())
        finally:
            try:
                conn.close()
            except Exception:
                pass

    server.close()
    _dbg("exiting")
