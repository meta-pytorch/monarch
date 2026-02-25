# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import threading
from typing import Optional, TextIO, Tuple

from monarch._rust_bindings.monarch_hyperactor.logging import LoggingMeshClient
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._src.actor.actor_mesh import context
from monarch._src.actor.future import Future
from monarch._src.actor.ipython_check import is_ipython

IN_IPYTHON: bool = is_ipython()

logger: logging.Logger = logging.getLogger(__name__)

_global_flush_registered = False
_global_flush_lock = threading.Lock()

FD_READ_CHUNK_SIZE = 4096


def flush_all_proc_mesh_logs() -> None:
    """Flush logs from all active ProcMesh instances."""
    from monarch._src.actor.proc_mesh import get_active_proc_meshes

    for pm in get_active_proc_meshes():
        if pm._logging_manager._logging_mesh_client is not None:
            pm._logging_manager.flush()


class LoggingManager:
    def __init__(self) -> None:
        self._logging_mesh_client: Optional[LoggingMeshClient] = None

    async def init(self, proc_mesh: HyProcMesh, stream_to_client: bool) -> None:
        if self._logging_mesh_client is not None:
            return

        instance = context().actor_instance._as_rust()
        self._logging_mesh_client = await LoggingMeshClient.spawn(
            instance, proc_mesh=proc_mesh
        )
        self._logging_mesh_client.set_mode(
            instance,
            stream_to_client=stream_to_client,
            aggregate_window_sec=3 if stream_to_client else None,
            level=logging.INFO,
        )

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
                    ipython.events.register(
                        "post_run_cell",
                        lambda _: flush_all_proc_mesh_logs(),
                    )
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

        assert self._logging_mesh_client is not None
        self._logging_mesh_client.set_mode(
            context().actor_instance._as_rust(),
            stream_to_client=stream_to_client,
            aggregate_window_sec=aggregate_window_sec,
            level=level,
        )
        self.register_flusher_if_in_ipython()
        self.enable_fd_capture_if_in_ipython()

    def flush(self) -> None:
        assert self._logging_mesh_client is not None
        try:
            # blocks for this proc mesh until 3 seconds timeout
            Future(
                coro=self._logging_mesh_client.flush(
                    context().actor_instance._as_rust()
                )
                .spawn()
                .task()
            ).get(timeout=3)
        except Exception:
            # TODO: A harmless exception happens to come through due to coroutine
            # accessing shared resources via logging_mesh_client. Flush works fine
            # but shared resource management under loggingMeshClient needs to be investigated
            pass

    async def flush_async(self) -> None:
        """Async version of flush for use in async contexts."""
        if self._logging_mesh_client is None:
            return
        try:
            await (
                self._logging_mesh_client.flush(context().actor_instance._as_rust())
                .spawn()
                .task()
            )
        except Exception:
            pass
