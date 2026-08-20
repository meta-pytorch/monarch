# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Characterization oracle for the actor endpoint driver.

Pins the load-bearing runtime fact the pytokio-removal Stage 3 audit rests on:
an ``@endpoint`` body runs on a real asyncio event loop (driven by the Rust
dispatcher via ``into_future_with_locals``), NOT on a bare tokio worker thread.
Concretely, inside an endpoint:

  - ``asyncio.get_running_loop()`` succeeds (a loop is running); and
  - a bare ``await PythonTask.sleep(0)`` raises (pytokio refuses to await a raw
    ``PythonTask`` while an asyncio loop is active); and
  - awaiting a monarch ``Future`` succeeds through the asyncio path.

This is why the reverted "A1" reroute (replacing an await of a Monarch
``Future`` in the reply path with a bare ``await PythonTask``) was wrong: under
the loop the bare await raises. If a future change switches the dispatcher to
drive endpoints on the tokio runtime (``PythonTask.from_coroutine``), these
assertions flip -- which is the exact signal that such reroutes become valid.
Pinned for queue dispatch.
"""

import asyncio

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint


class _DriverProbe(Actor):
    @endpoint
    async def probe(self) -> tuple[bool, bool]:
        # (1) The endpoint body runs under a real asyncio loop.
        try:
            asyncio.get_running_loop()
            has_loop = True
        except RuntimeError:
            has_loop = False

        # (2) A bare PythonTask cannot be awaited while a loop is running.
        bare_await_raised = False
        try:
            await PythonTask.sleep(0)
        except RuntimeError:
            bare_await_raised = True

        # (3) Awaiting a monarch Future succeeds on the endpoint's asyncio loop.
        await Future._from_coro(PythonTask.sleep(0))

        return (has_loop, bare_await_raised)


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_actor_endpoint_is_asyncio_driven() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    probe = proc.spawn("driver_probe", _DriverProbe)
    has_loop, bare_await_raised = await probe.probe.call_one()
    assert has_loop, "an @endpoint body must run under a real asyncio loop"
    assert bare_await_raised, (
        "a bare `await PythonTask` inside an @endpoint must raise: pytokio "
        "refuses to await a raw PythonTask while an asyncio loop is active"
    )
    await proc.stop()
