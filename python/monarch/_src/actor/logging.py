# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import threading

from monarch._rust_bindings.monarch_extension.logging import LoggingMeshClient

from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._src.actor.future import Future

IN_IPYTHON = False
try:
    # Check if we are in ipython environment
    # pyre-ignore[21]
    from IPython import get_ipython

    # pyre-ignore[21]
    from IPython.core.interactiveshell import ExecutionResult

    IN_IPYTHON = get_ipython() is not None
except ImportError:
    pass

logger: logging.Logger = logging.getLogger(__name__)

_global_flush_registered = False
_global_flush_lock = threading.Lock()


def flush_all_proc_mesh_logs() -> None:
    """Flush logs from all active ProcMesh instances."""
    # import `get_active_proc_meshes` here to avoid circular import dependency
    from monarch._src.actor.proc_mesh import get_active_proc_meshes

    for pm in get_active_proc_meshes():
        pm._logging_manager.flush()


class LoggingManager:
    def __init__(self) -> None:
        self._logging_mesh_client: LoggingMeshClient | None = None

    async def init(self, proc_mesh: HyProcMesh, stream_to_client: bool) -> None:
        if self._logging_mesh_client is not None:
            return

        self._logging_mesh_client = await LoggingMeshClient.spawn(proc_mesh=proc_mesh)
        self._logging_mesh_client.set_mode(
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
                    get_ipython().events.register(
                        "post_run_cell", lambda _: flush_all_proc_mesh_logs()
                    )
                    _global_flush_registered = True

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
            stream_to_client=stream_to_client,
            aggregate_window_sec=aggregate_window_sec,
            level=level,
        )
        self.register_flusher_if_in_ipython()

    def flush(self) -> None:
        assert self._logging_mesh_client is not None
        try:
            # blocks for this proc mesh until 3 seconds timeout
            Future(coro=self._logging_mesh_client.flush().spawn().task()).get(timeout=3)
        except Exception:
            # TODO: A harmless exception happens to come through due to coroutine
            # accessing shared resources via logging_mesh_client. Flush works fine
            # but shared resource management under loggingMeshClient needs to be investigated
            pass
