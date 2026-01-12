# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from __future__ import annotations

from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh
from monarch._rust_bindings.monarch_hyperactor.pychannel import (  # @manual=//monarch/monarch_extension:monarch_extension_no_torch
    channel_for_test,
    TestSender,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._src.actor.allocator import LocalAllocator
from monarch._src.actor.pickle import flatten, unflatten


class MyActor:
    async def handle(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError()


def test_import() -> None:
    try:
        import monarch._rust_bindings  # noqa
    except ImportError as e:
        raise ImportError(f"monarch._rust_bindings failed to import: {e}")


def test_actor_id() -> None:
    actor_id = ActorId(world_name="test", rank=0, actor_name="actor")
    assert actor_id.pid == 0
    assert str(actor_id) == "test[0].actor[0]"


def test_no_hang_on_shutdown() -> None:
    def test_fn() -> None:
        import monarch._rust_bindings  # noqa
        import torch  # noqa

        time.sleep(100)

    proc = multiprocessing.Process(target=test_fn)
    proc.start()
    pid = proc.pid
    assert pid is not None


def test_basic() -> None:
    tx, rx = channel_for_test()
    tx.send(1)
    tx.send(2)
    tx.send(3)
    assert rx.try_recv() == 1
    assert rx.try_recv() == 2
    assert rx.try_recv() == 3
    assert rx.try_recv() is None


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
    import asyncio

    # Create two channels for bidirectional communication
    tx_to_task, rx_in_task = channel_for_test()
    tx_from_task, rx_from_task = channel_for_test()

    async def ping_pong_task(rx: Receiver[int], tx: TestSender[int]) -> None:
        tx.send(10)

        for i in range(9, 0, -2):
            n: int = await rx.recv()
            assert n == i
            tx.send(i - 1)

    # pyre-ignore[6]: Receiver is generic but channel_for_test returns Any
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
