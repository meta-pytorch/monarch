# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import threading
import warnings
from typing import Optional, TextIO, Tuple

from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.handle import Handle
from monarch._rust_bindings.monarch_hyperactor.logging import LoggingMeshClient
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._src.actor.actor_mesh import context
from monarch._src.actor.ipython_check import is_ipython

IN_IPYTHON: bool = is_ipython()

logger: logging.Logger = logging.getLogger(__name__)

_global_flush_registered = False
_global_flush_lock = threading.Lock()
# Strong references to in-flight async flush tasks scheduled from the
# post_run_cell callback. The event loop only weak-refs tasks, so without
# this set a flush task could be garbage-collected mid-execution.
_pending_flush_tasks: set[asyncio.Task[None]] = set()

FD_READ_CHUNK_SIZE = 4096


def flush_all_proc_mesh_logs() -> None:
    """Flush logs from all active ProcMesh instances."""
    from monarch._src.actor.proc_mesh import get_active_proc_meshes

    for pm in get_active_proc_meshes():
        if pm._logging_manager._logging_mesh_client is not None:
            pm._logging_manager.flush()


async def _flush_all_proc_mesh_logs_async() -> None:
    """Async counterpart to ``flush_all_proc_mesh_logs``."""
    from monarch._src.actor.proc_mesh import get_active_proc_meshes

    for pm in get_active_proc_meshes():
        if pm._logging_manager._logging_mesh_client is not None:
            await pm._logging_manager.flush_async()


class LoggingManager:
    def __init__(self) -> None:
        self._logging_mesh_client_handle: Optional[Handle[LoggingMeshClient]] = None

    @property
    def _logging_mesh_client(self) -> Optional[LoggingMeshClient]:
        if self._logging_mesh_client_handle is None:
            return None
        try:
            return self._logging_mesh_client_handle.poll()
        except Exception:
            return None

    def start(
        self, instance: Instance, proc_mesh: Handle[HyProcMesh], stream_to_client: bool
    ) -> Handle[LoggingMeshClient]:
        if self._logging_mesh_client_handle is not None:
            return self._logging_mesh_client_handle
        self._logging_mesh_client_handle = LoggingMeshClient.spawn(
            instance,
            proc_mesh,
            stream_to_client=stream_to_client,
            aggregate_window_sec=3 if stream_to_client else None,
            level=logging.INFO,
        )
        return self._logging_mesh_client_handle

    def register_flusher_if_in_ipython(self) -> None:
        if IN_IPYTHON:
            # For ipython environment, a cell can end fast with threads running in background.
            # register a post_run_cell event ONCE to flush all logs from all proc meshes.
            with _global_flush_lock:
                global _global_flush_registered
                if not _global_flush_registered:
                    # pyre-ignore[21]: IPython is already loaded if IN_IPYTHON is True
                    from IPython import get_ipython

                    ipython = get_ipython()
                    assert ipython is not None

                    def _post_run_cell_flush(_: object) -> None:
                        # For async cells the loop is still running when the
                        # callback fires; `flush()` would then block that loop
                        # in `Handle.get(timeout=3)`.
                        # Schedule the async flush on the loop instead.
                        try:
                            loop = asyncio.get_running_loop()
                        except RuntimeError:
                            flush_all_proc_mesh_logs()
                        else:
                            task = loop.create_task(_flush_all_proc_mesh_logs_async())
                            _pending_flush_tasks.add(task)
                            task.add_done_callback(_pending_flush_tasks.discard)

                    ipython.events.register("post_run_cell", _post_run_cell_flush)
                    _global_flush_registered = True

    def enable_fd_capture_if_in_ipython(self) -> Optional[Tuple[int, int]]:
        """
        On notebooks, the UI shows logs from Python streams (sys.stdout/sys.stderr), but
        Monarch actors write directly to the OS file descriptors 1/2 (stdout/stderr). Those
        low-level writes bypass Python’s streams and therefore don’t appear in the
        notebook output.

        What this does:
        - Creates two OS pipes and uses dup2 to redirect the current process's
          stdout/stderr FDs (1/2) into those pipes.
        - Spawns tiny background threads that read bytes from the pipes and forward
          them into the notebook’s visible Python streams (sys.stdout/sys.stderr).

        If in IPython, returns backups of the original FDs so they can be restored.
        """
        if IN_IPYTHON:
            import os
            import sys

            r1, w1 = os.pipe()
            r2, w2 = os.pipe()
            b1 = os.dup(1)
            b2 = os.dup(2)
            os.dup2(w1, 1)
            os.dup2(w2, 2)
            os.close(w1)
            os.close(w2)

            def pump(fd: int, stream: TextIO) -> None:
                while True:
                    chunk = os.read(fd, FD_READ_CHUNK_SIZE)
                    if not chunk:
                        break
                    (
                        stream.buffer.write(chunk)
                        if hasattr(stream, "buffer")
                        else stream.write(chunk.decode("utf-8", "replace"))
                    )
                    stream.flush()

            threading.Thread(target=pump, args=(r1, sys.stdout), daemon=True).start()
            threading.Thread(target=pump, args=(r2, sys.stderr), daemon=True).start()

            return b1, b2

        return None

    async def logging_option(
        self,
        stream_to_client: bool = True,
        aggregate_window_sec: int | None = 3,
        level: int = logging.INFO,
    ) -> None:
        if level < 0 or level > 255:
            raise ValueError("Invalid logging level: {}".format(level))

        if stream_to_client:
            from monarch._rust_bindings.monarch_hyperactor.config import (
                get_global_config,
            )

            if not get_global_config().get("enable_log_forwarding", False):
                warnings.warn(
                    "logging_option(stream_to_client=True) has no effect: "
                    "log forwarding was disabled when this ProcMesh was "
                    "spawned, so no LogForwardActor mesh exists to stream "
                    "from. Call configure(enable_log_forwarding=True) before "
                    "creating the ProcMesh, or set "
                    "HYPERACTOR_MESH_ENABLE_LOG_FORWARDING=true.",
                )

        assert self._logging_mesh_client is not None
        self._logging_mesh_client.set_mode(
            context().actor_instance._as_rust(),
            stream_to_client=stream_to_client,
            aggregate_window_sec=aggregate_window_sec,
            level=level,
        )
        self.register_flusher_if_in_ipython()
        self.enable_fd_capture_if_in_ipython()

    def _new_flush_handle(self) -> Handle[None]:
        """Start a Rust logging flush and return its Handle."""
        assert self._logging_mesh_client is not None
        return self._logging_mesh_client.flush(context().actor_instance._as_rust())

    def flush(self) -> None:
        assert self._logging_mesh_client is not None
        try:
            # Wait for the running flush for up to 3 seconds.
            self._new_flush_handle().get(timeout=3)
        except Exception:
            # Best effort: this runs after each IPython cell, so a barrier that
            # fails or takes longer than three seconds must not reach the user.
            # A timed-out flush keeps running in the background.
            pass

    async def flush_async(self) -> None:
        """Flush by awaiting a Handle on the current asyncio event loop."""
        if self._logging_mesh_client is None:
            return
        try:
            # Start the flush and await its Handle on the asyncio loop.
            await self._new_flush_handle()
        except Exception:
            pass
