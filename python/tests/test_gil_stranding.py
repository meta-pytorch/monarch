# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Reproduces https://github.com/meta-pytorch/monarch/issues/4938 on real procs,
channels, and GIL.

Supervision polls each proc agent with GetState and declares the proc crashed
when no reply arrives within ``GET_ACTOR_STATE_MAX_IDLE``. The agent and the
channels that carry GetState and its reply are pure Rust, so a Python thread
holding the GIL cannot delay them directly. It delays them through the way
Tokio's multi-threaded scheduler parks idle workers:

- One idle worker at a time owns the I/O and timer driver and sleeps in
  ``epoll_wait``. The other idle workers sleep on condvars with no timeout and
  wake only when another thread notifies them.
- When a message reaches an idle proc, the driver owner wakes, releases the
  driver, and carries the message through the channel reader to the actor task
  itself. Each step wakes a single task, so no other worker is notified: Tokio
  expects the owner to return to the driver shortly.
- For a Python endpoint, the actor task blocks in ``Python::attach`` until the
  holder lets go, so the owner does not return.
- Socket readiness and timer expiry are what would wake a sleeping worker, and
  only the driver observes them. With nobody polling it, the GetState that
  arrives next sits unread in the socket. The channel reader waits for a
  readiness event rather than sitting in a run queue, so the sleeping workers
  have nothing to run. One call is therefore enough; later messages wait in
  the socket too.

The stall ends when the GIL is released, or when a thread outside the runtime
wakes a worker, which then takes over the driver. In a busy proc another worker
soon parks on the driver, so the full stall needs an idle proc. Tokio's
unstable ``Builder::enable_eager_driver_handoff`` makes the driver owner wake a
sibling before it polls a task; with it GetState does not stall, and the
GIL-held test fails.

The GIL is held by a ``ctypes.PyDLL`` call to libc ``sleep``, which uses no CPU
and never returns to the eval loop. The controls make the same call through
``ctypes.CDLL``, which releases the GIL, or hold the GIL while no Python-bound
message reaches the proc.
"""

import asyncio
import ctypes
import os
import threading
import time

import monarch.actor
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, endpoint, MeshFailure, this_host

HOLD_SECS = 15
HOLD_DELAY_SECS = 0.5

_ENV = {
    "HYPERACTOR_MESH_SUPERVISION_POLL_FREQUENCY": "1s",
    "HYPERACTOR_MESH_GET_ACTOR_STATE_MAX_IDLE": "5s",
}


def _sleep_in_libc(keep_gil: bool) -> None:
    time.sleep(HOLD_DELAY_SECS)
    libc = ctypes.PyDLL(None) if keep_gil else ctypes.CDLL(None)
    libc.sleep(HOLD_SECS)


class Sink(Actor):
    @endpoint
    def pid(self) -> int:
        return os.getpid()

    @endpoint
    def sleep_in_libc(self, keep_gil: bool) -> None:
        """Starts a thread that sleeps in libc for ``HOLD_SECS``, holding the GIL
        if ``keep_gil``. The thread waits briefly so this reply leaves first."""
        threading.Thread(target=_sleep_in_libc, args=(keep_gil,), daemon=True).start()


async def _faults_while_sleeping(keep_gil: bool, send_message: bool) -> list[str]:
    """Returns the supervision faults raised while a thread in the sink's
    process sleeps in libc, each prefixed with whether that process was alive.
    With ``send_message``, the client calls a Python endpoint of the sink once
    during the sleep."""
    sink = this_host().spawn_procs().spawn("sink", Sink)
    pid = await sink.pid.call_one()
    faults = []

    def record(failure: MeshFailure) -> None:
        faults.append(
            f"sink alive: {os.path.exists(f'/proc/{pid}')}\n{failure.report()}"
        )

    monarch.actor.unhandled_fault_hook = record
    await sink.sleep_in_libc.call_one(keep_gil)
    start = time.monotonic()
    await asyncio.sleep(2 * HOLD_DELAY_SECS)
    call = sink.pid.call_one() if send_message else None
    while not faults and time.monotonic() - start < HOLD_SECS + 10:
        await asyncio.sleep(0.5)
    if not faults and call is not None:
        assert await call == pid
    return faults


@pytest.mark.timeout(120)
@isolate_in_subprocess(env=_ENV)
async def test_gil_hold_with_python_message_stalls_get_state() -> None:
    """Pins the bug in https://github.com/meta-pytorch/monarch/issues/4938: one
    call to a Python endpoint while the GIL is held stalls the pure-Rust
    GetState, and supervision declares the live proc crashed with the error
    seen in the MAST job. Once the stall is fixed, assert ``faults == []`` here
    as the controls do."""
    faults = await _faults_while_sleeping(keep_gil=True, send_message=True)
    assert faults, "GetState no longer stalls; assert faults == [] instead"
    assert faults[0].startswith("sink alive: True"), faults[0]
    assert "timeout waiting for message from proc mesh agent" in faults[0], faults[0]


@pytest.mark.timeout(120)
@isolate_in_subprocess(env=_ENV)
async def test_gil_released_with_python_message_keeps_get_state_alive() -> None:
    """The same sleep and call, but the sleep releases the GIL: the call alone
    does not stall GetState."""
    assert await _faults_while_sleeping(keep_gil=False, send_message=True) == []


@pytest.mark.timeout(120)
@isolate_in_subprocess(env=_ENV)
async def test_gil_hold_without_python_message_keeps_get_state_alive() -> None:
    """The GIL is held just as long, but no Tokio worker needs it: the proc
    agent answers GetState without the GIL."""
    assert await _faults_while_sleeping(keep_gil=True, send_message=False) == []
