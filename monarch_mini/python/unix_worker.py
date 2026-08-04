# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Helper run as a subprocess by test_smoke.py's cross-process unix tests.

Usage: ``python unix_worker.py <unix-url> <mode>``. Each mode connects to (or is
connected to by) actors in the parent test process over a real unix socket,
exchanges data, then closes its context — whose teardown flushes the socket — and
exits 0. A failed assertion exits non-zero, failing the test.
"""

import asyncio
import sys

import minimonarch
from minimonarch import Actor

ba = minimonarch.bytearray


async def _echo_once(actor: Actor, reply_to: bytes) -> None:
    """Wait for one message and send it straight back to `reply_to` (the same
    bytearrays are moved back, so the payload is never copied)."""
    parts = await actor.next()
    actor.send(reply_to, parts)


async def run_joiner(url: str) -> None:
    """This process is the CHILD and JOINS the parent the test serves over unix.

    `bridge` then serves its own inproc child `worker`, so a message the test
    sends to `worker` travels inproc -> unix -> inproc and is echoed straight
    back. `bridge` itself only ever relays (it is never addressed directly), so
    its queue holds just its two hellos.
    """
    bridge = Actor(b"bridge")
    bridge.join(url, "child")
    assert await bridge.next() == [b"bridge", b"root"]

    worker = Actor(b"worker")
    bridge.serve("inproc://worker-link", "parent")
    worker.join("inproc://worker-link", "child")
    assert await bridge.next() == [b"bridge", b"worker"]
    assert await worker.next() == [b"worker", b"bridge"]

    await _echo_once(worker, b"client")
    minimonarch.close()


async def run_server(url: str) -> None:
    """This process is the CHILD and SERVES (binds) over unix; the test is the
    PARENT and joins us.

    We sleep before serving so the test's join is posted first, exercising the
    connector's retry/backoff (join-before-serve).
    """
    await asyncio.sleep(0.2)
    server = Actor(b"server")
    server.serve(url, "child")
    assert await server.next() == [b"server", b"boss"]
    await _echo_once(server, b"boss")
    minimonarch.close()


async def run_parent(url: str) -> None:
    """This process hosts the PARENT and serves over unix; the test joins as the
    child. We then idle forever — the test hard-kills us to drop the socket and
    observe the failure cascade in its own actors.
    """
    up = Actor(b"up")
    up.serve(url, "parent")
    assert await up.next() == [b"up", b"mid"]
    while True:
        await asyncio.sleep(3600)


_MODES = {"joiner": run_joiner, "server": run_server, "parent": run_parent}


if __name__ == "__main__":
    url, mode = sys.argv[1], sys.argv[2]
    asyncio.run(_MODES[mode](url))
