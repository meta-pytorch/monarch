# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Runnable sources for the recipes in ``docs/source/cookbook.md``.

Each recipe is a region delimited by ``# cookbook: <slug>`` and ``# cookbook: end``
markers. The cookbook embeds these regions verbatim with Sphinx ``literalinclude``,
so the documented snippets are exactly the code exercised here. Keep the region
bodies self-contained and free of test scaffolding.
"""

import asyncio
from typing import AsyncIterator

import pytest
from monarch.actor import Actor, Channel, context, endpoint, Port, this_host

pytestmark = pytest.mark.timeout(60)


class Counter(Actor):
    def __init__(self, value: int) -> None:
        self.value = value

    @endpoint
    async def increment(self) -> None:
        self.value += 1

    @endpoint
    async def get_value(self) -> int:
        return self.value


@pytest.fixture
async def counters() -> AsyncIterator[Counter]:
    procs = this_host().spawn_procs(per_host={"gpus": 4})
    yield procs.spawn("counters", Counter, 0)
    await procs.stop()


async def test_rank_0_slice(counters: Counter) -> None:
    # cookbook: rank-0-slice
    rank_0 = counters.flatten("rank").slice(rank=0)
    await rank_0.increment.call_one()
    # cookbook: end

    assert await rank_0.get_value.call_one() == 1


class Worker(Actor):
    mesh: "Worker"

    def __init__(self) -> None:
        self.pings = 0

    @endpoint
    async def set_mesh(self, mesh: "Worker") -> None:
        self.mesh = mesh

    @endpoint
    async def ping(self) -> str:
        self.pings += 1
        return "pong"

    @endpoint
    async def ping_count(self) -> int:
        return self.pings

    @endpoint
    async def call_coordinator(self) -> str:
        # cookbook: inter-rank-call
        coordinator = self.mesh.flatten("rank").slice(rank=0)
        return await coordinator.ping.call_one()
        # cookbook: end


async def test_inter_rank_call() -> None:
    procs = this_host().spawn_procs(per_host={"gpus": 4})
    workers = procs.spawn("workers", Worker)
    await workers.set_mesh.call(workers)

    others = workers.flatten("rank").slice(rank=slice(1, None))
    await others.call_coordinator.call()

    coordinator = workers.flatten("rank").slice(rank=0)
    assert await coordinator.ping_count.call_one() == 3

    await procs.stop()


STREAM_LEN = 3


class Aggregator(Actor):
    mesh: "Aggregator"

    @endpoint
    async def set_mesh(self, mesh: "Aggregator") -> None:
        self.mesh = mesh

    @endpoint
    async def collect(self) -> list[int]:
        # cookbook: port-open
        port, receiver = Channel[int].open()
        others = self.mesh.flatten("rank").slice(rank=slice(1, None))
        others.produce.broadcast(port)
        expected = others.size() * STREAM_LEN
        return sorted([await receiver.recv() for _ in range(expected)])
        # cookbook: end

    @endpoint
    async def produce(self, port: Port[int]) -> None:
        # cookbook: port-send
        rank = context().actor_instance.rank["gpus"]

        async def stream() -> None:
            for step in range(STREAM_LEN):
                port.send(rank * 10 + step)

        self.task = asyncio.create_task(stream())
        # cookbook: end


async def test_port_fan_in() -> None:
    procs = this_host().spawn_procs(per_host={"gpus": 4})
    aggregators = procs.spawn("aggregators", Aggregator)
    await aggregators.set_mesh.call(aggregators)

    coordinator = aggregators.flatten("rank").slice(rank=0)
    assert await coordinator.collect.call_one() == [10, 11, 12, 20, 21, 22, 30, 31, 32]

    await procs.stop()
