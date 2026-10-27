# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import threading
import time

import pytest
from monarch.actor import Actor, endpoint, shutdown_context, this_host


class Simple(Actor):
    @endpoint
    def ping(self) -> None:
        pass


def _actor_loop_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "monarch-actor-event-loop"]


# This test has to be in its own file so it does not share any process state
# with other tests. The client cannot be restarted after it has been shutdown.
@pytest.mark.timeout(120)
def test_shutdown_stops_root_client_loop() -> None:
    procs = this_host().spawn_procs(per_host={"gpus": 1})
    actors = procs.spawn("simple", Simple)
    actors.ping.call().get(timeout=30)
    assert _actor_loop_threads(), "the root client should own an event loop thread"

    assert shutdown_context().get(timeout=30) is None

    # Actor cleanup stops the loop without joining its thread, so allow the
    # thread a moment to exit. Without root client cleanup, it never does
    # until the interpreter-exit reaper runs.
    deadline = time.monotonic() + 10
    while _actor_loop_threads() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not _actor_loop_threads(), (
        f"actor event loop threads still running after shutdown: {_actor_loop_threads()}"
    )
