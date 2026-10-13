# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Characterize host draining while proc-mesh logging is still initializing."""

import asyncio
from typing import Any
from unittest.mock import patch

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.handle import _new_handle_pair, Handle
from monarch._src.actor.logging import LoggingManager
from monarch._src.job.process import ProcessJob
from scoped_state import scoped_state


@pytest.mark.timeout(90)
@isolate_in_subprocess
def test_drain_pending_spawns_does_not_await_logging_init() -> None:
    pending, completer = _new_handle_pair()
    real_start = LoggingManager.start
    captured: list[tuple[Any, Any, bool]] = []

    def gated_start(
        manager: LoggingManager, instance: Any, raw: Any, stream: bool
    ) -> Any:
        captured.append((instance, raw, stream))
        manager._logging_mesh_client_handle = pending
        return pending

    proc_mesh = None
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        try:
            with patch.object(LoggingManager, "start", gated_start):
                proc_mesh = host.spawn_procs(name="logging_init_gate")
                native_spawn = host._pending_spawns[-1]
                # The raw procs exist; only logging readiness is held at the gate.
                assert native_spawn.get(timeout=30) is not None
                assert proc_mesh._logging_manager._logging_mesh_client is None
                assert proc_mesh._proc_mesh.poll() is None

                flush_without_client: list[bool] = []
                native_flush_constructions = 0
                flush_async = proc_mesh._logging_manager.flush_async
                new_flush_handle = proc_mesh._logging_manager._new_flush_handle

                async def record_flush_async() -> None:
                    flush_without_client.append(
                        proc_mesh._logging_manager._logging_mesh_client is None
                    )
                    await flush_async()

                def record_new_flush_handle() -> Handle[None]:
                    nonlocal native_flush_constructions
                    native_flush_constructions += 1
                    return new_flush_handle()

                with (
                    patch.object(
                        proc_mesh._logging_manager, "flush_async", record_flush_async
                    ),
                    patch.object(
                        proc_mesh._logging_manager,
                        "_new_flush_handle",
                        record_new_flush_handle,
                    ),
                ):
                    asyncio.run(
                        asyncio.wait_for(host._drain_pending_spawns(), timeout=10)
                    )

                assert flush_without_client == [True]
                assert native_flush_constructions == 0
                assert host._pending_spawns == []
                assert proc_mesh._proc_mesh.poll() is None
        finally:
            if proc_mesh is not None:
                manager = proc_mesh._logging_manager
                manager._logging_mesh_client_handle = None
                instance, raw, stream = captured[0]
                try:
                    client = real_start(manager, instance, raw, stream).get(timeout=30)
                except Exception as error:
                    completer.set_exception(error)
                    raise
                else:
                    completer.set_result(client)
                assert proc_mesh._proc_mesh.get(timeout=30) is not None
                assert manager._logging_mesh_client is not None
