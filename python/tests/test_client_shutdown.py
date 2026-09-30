# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import time
from typing import Any, Callable
from unittest.mock import patch

import pytest
from monarch._rust_bindings.monarch_hyperactor import host_mesh as host_mesh_binding
from monarch._src.actor import actor_mesh as actor_mesh_module
from monarch.actor import Actor, endpoint, shutdown_context, this_host


class Simple(Actor):
    @endpoint
    def get_pid(self) -> int:
        return os.getpid()


def pid_exists(pid: int) -> bool:
    """True if pid exists, else false"""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


class _ShutdownCallThrough:
    """Record shutdown state and forward to the real native binding."""

    def __init__(self, real_shutdown: Callable[[float | None], Any]) -> None:
        self._real_shutdown = real_shutdown
        self.calls = 0
        self.shutdown_done_at_call: list[bool] = []
        self.timeouts: list[float | None] = []

    def __call__(self, timeout_secs: float | None = None) -> Any:
        self.shutdown_done_at_call.append(actor_mesh_module._shutdown_done)
        self.timeouts.append(timeout_secs)
        self.calls += 1
        return self._real_shutdown(timeout_secs)


# This test has to be in its own file so it does not share any process state
# with other tests. The client cannot be restarted after it has been shutdown.
@pytest.mark.timeout(240)
def test_client_shutdown() -> None:
    procs = this_host().spawn_procs(per_host={"gpus": 2})
    actors = procs.spawn("simple", Simple)
    pid_items = list(actors.get_pid.call().get(timeout=30).items())
    pids = [pid for _, pid in pid_items]

    call_through = _ShutdownCallThrough(host_mesh_binding.shutdown_local_host_mesh)
    with patch.object(
        host_mesh_binding,
        "shutdown_local_host_mesh",
        call_through,
    ):
        discarded = shutdown_context()
        del discarded
        assert list(actors.get_pid.call().get(timeout=30).items()) == pid_items
        assert call_through.calls == 0
        assert call_through.shutdown_done_at_call == []

        # Both Futures capture the shutdown-sequence branch now. Driving the
        # first sets _shutdown_done; observing the second then proves that an
        # already-constructed sequence takes its own early return.
        first_shutdown = shutdown_context()
        second_shutdown = shutdown_context()
        assert first_shutdown.get(timeout=60) is None
        assert call_through.calls == 1
        assert call_through.shutdown_done_at_call == [True]
        assert call_through.timeouts == [None]

        # This Future captured the shutdown-sequence branch before the first
        # one ran, but observes _shutdown_done after the first one completes.
        assert second_shutdown.get(timeout=30) is None
        assert call_through.calls == 1
        assert call_through.shutdown_done_at_call == [True]

        # The client cannot be restarted after this one full shutdown: five
        # Rust registrations are OnceLock, and _client_context keeps the
        # stopped Context. Never reset _shutdown_done to rehearse it.
        del procs
        del actors

        # This Future is constructed after _shutdown_done became true, so it
        # takes the separate _noop branch and never calls the native binding.
        assert shutdown_context().get(timeout=30) is None
        assert call_through.calls == 1
        assert call_through.shutdown_done_at_call == [True]

    # After this, all the resources created by the client should be released,
    # including this_host and the procs. This observes worker-process exit after
    # explicit shutdown; it does not exercise client-process exit or atexit.
    still_alive = []
    for _ in range(4):
        time.sleep(5)
        still_alive = [pid_exists(pid) for pid in pids]
        if not any(still_alive):
            # successfully shut off all pids.
            return
    raise ValueError(
        "Some pids are still alive at the end of the waiting period: {}", still_alive
    )
