# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import time

import pytest
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


# This test has to be in its own file so it does not share any process state
# with other tests. The client cannot be restarted after it has been shutdown.
# pyre-fixme[56]: invalid decoration
@pytest.mark.timeout(30)
def test_client_shutdown() -> None:
    procs = this_host().spawn_procs(per_host={"gpus": 2})
    actors = procs.spawn("simple", Simple)
    pids = actors.get_pid.call().get()
    pids = [p for _, p in pids]
    # Now shutdown the client. Delete references to avoid accidental reuse.
    del procs
    del actors
    shutdown_context().get()
    # After this, all the resources created by the client should be released,
    # including this_host and the procs. We check this by seeing if the pids are
    # still alive after a short wait period (procs are cleaned up with an async
    # message).
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
