# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Test to verify the MONARCH_TEST_DIAG process leak detection in conftest.py.

Run with: uv run pytest python/tests/test_leak_detection.py -s
Then grep for MONARCH_TEST_DIAG in the output — the leaky test should show
processes_leaked > 0.
"""

import subprocess

import pytest
from monarch.actor import Actor, endpoint
from monarch._src.actor.host_mesh import this_host


class Dummy(Actor):
    @endpoint
    async def ping(self) -> str:
        return "pong"


@pytest.mark.timeout(60)
async def test_no_leak() -> None:
    """Properly cleaned up — should show processes_leaked=0."""
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    am = pm.spawn("dummy", Dummy)
    await am.ping.call()
    await pm.stop()


@pytest.mark.timeout(60)
async def test_leak() -> None:
    """Intentionally leaks — should show processes_leaked > 0."""
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    am = pm.spawn("dummy", Dummy)
    await am.ping.call()
    # Intentionally NOT calling await pm.stop()


def test_subprocess_no_leak() -> None:
    """Subprocess that exits — should show processes_leaked=0."""
    p = subprocess.run(["true"])


def test_subprocess_leak() -> None:
    """Subprocess left running — should show processes_leaked > 0."""
    subprocess.Popen(["sleep", "300"])
