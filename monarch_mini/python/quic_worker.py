# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Helper run as a subprocess by test_smoke.py's cross-process quic tests.

Usage: ``python quic_worker.py <quic-url> <mode>``. The QUIC TLS material is read
from the ``MM_QUIC_CERT`` / ``MM_QUIC_KEY`` / ``MM_QUIC_CA`` environment variables,
which the parent test process sets and the worker inherits.

Modes:
  * ``echo_child``  — join as a child, echo one message back, close cleanly, exit 0.
  * ``parent``      — serve as the parent, then idle forever (the test hard-kills /
                      freezes us so the survivor must detect the loss by heartbeat).
  * ``child``       — join as a child, then idle forever (same, from the child end).
"""

import asyncio
import sys

import minimonarch
from minimonarch import Actor

ba = minimonarch.bytearray


async def run_echo_child(url: str) -> None:
    """Join as child `q-echo`, echo one message back to `q-srv`, then close."""
    echo = Actor(b"q-echo")
    echo.join(url, "child")
    assert await echo.next() == [b"q-echo", b"q-srv"]
    parts = await echo.next()
    echo.send(b"q-srv", parts)
    minimonarch.close()


async def run_parent(url: str) -> None:
    """Serve as parent `q-up`; the test joins as child `q-mid`, then idles so the
    test can kill/freeze us and watch its child detect the loss via heartbeat."""
    up = Actor(b"q-up")
    up.serve(url, "parent")
    assert await up.next() == [b"q-up", b"q-mid"]
    while True:
        await asyncio.sleep(3600)


async def run_child(url: str) -> None:
    """Join as child `q-down`; the test serves as parent `q-boss`, then idles so the
    test can kill/freeze us and watch its parent detect the loss via heartbeat."""
    down = Actor(b"q-down")
    down.join(url, "child")
    assert await down.next() == [b"q-down", b"q-boss"]
    while True:
        await asyncio.sleep(3600)


_MODES = {"echo_child": run_echo_child, "parent": run_parent, "child": run_child}


if __name__ == "__main__":
    url, mode = sys.argv[1], sys.argv[2]
    asyncio.run(_MODES[mode](url))
