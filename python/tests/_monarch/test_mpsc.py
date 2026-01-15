# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from __future__ import annotations

import asyncio
from typing import Any

# Required to make rust_struct extensions work correctly.
import monarch._src.actor.mpsc  # noqa: F401
from monarch._rust_bindings.monarch_hyperactor.pympsc import (  # @manual=//monarch/monarch_extension:monarch_extension_no_torch
    channel_for_test,
    Receiver,
    TestSender,
)


async def test_async_recv() -> None:
    """Test async recv using pywaker."""
    tx, rx = channel_for_test()

    # Send messages
    tx.send(1)
    tx.send(2)
    tx.send(3)

    # Async recv
    assert await rx.recv() == 1
    assert await rx.recv() == 2
    assert await rx.recv() == 3


async def test_ping_pong() -> None:
    """Test ping-pong communication between two async tasks."""

    # Create two channels for bidirectional communication
    tx_to_task, rx_in_task = channel_for_test()
    tx_from_task, rx_from_task = channel_for_test()

    async def ping_pong_task(rx: Receiver[Any], tx: TestSender[Any]) -> None:
        tx.send(10)

        for i in range(9, 0, -2):
            n = int(await rx.recv())
            assert n == i
            tx.send(i - 1)

    task = asyncio.create_task(ping_pong_task(rx_in_task, tx_from_task))

    for i in range(10, 0, -2):
        received = await rx_from_task.recv()
        assert received == i
        tx_to_task.send(i - 1)

    # Receive final 0
    received = await rx_from_task.recv()
    assert received == 0

    # Wait for task to complete
    await task
