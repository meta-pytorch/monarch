# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import gc
import logging

from typing import Callable

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


class LoggingManager:
    def __init__(self) -> None:
        self._logging_mesh_client: LoggingMeshClient | None = None
        self._ipython_flush_logs_handler: Callable[..., None] | None = None

    async def init(self, proc_mesh: HyProcMesh, stream_to_client: bool) -> None:
        if self._logging_mesh_client is not None:
            return

        self._logging_mesh_client = await LoggingMeshClient.spawn(proc_mesh=proc_mesh)
        self._logging_mesh_client.set_mode(
            stream_to_client=stream_to_client,
            aggregate_window_sec=3 if stream_to_client else None,
            level=logging.INFO,
        )

        if IN_IPYTHON:
            # For ipython environment, a cell can end fast with threads running in background.
            # Flush all the ongoing logs proactively to avoid missing logs.
            assert self._logging_mesh_client is not None
            logging_client: LoggingMeshClient = self._logging_mesh_client
            ipython = get_ipython()

            # pyre-ignore[11]
            def flush_logs(_: ExecutionResult) -> None:
                try:
                    Future(coro=logging_client.flush().spawn().task()).get(3)
                except TimeoutError:
                    # We need to prevent failed proc meshes not coming back
                    pass

            # Force to recycle previous undropped proc_mesh.
            # Otherwise, we may end up with unregisterd dead callbacks.
            gc.collect()

            # Store the handler reference so we can unregister it later
            self._ipython_flush_logs_handler = flush_logs
            ipython.events.register("post_run_cell", flush_logs)

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

    def stop(self) -> None:
        if self._ipython_flush_logs_handler is not None:
            assert IN_IPYTHON
            ipython = get_ipython()
            assert ipython is not None
            ipython.events.unregister("post_run_cell", self._ipython_flush_logs_handler)
            self._ipython_flush_logs_handler = None
