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


class Trainer(Actor):
    def __init__(self, parallel_dims: dict[str, int]) -> None:
        self.rank: int = context().actor_instance.rank.rank
        self.dp_rank: int = self.rank // parallel_dims["tp"] % parallel_dims["dp"]
        self.batch: list[int] = []

    @endpoint
    async def get_dp_rank(self) -> int:
        return self.dp_rank

    @endpoint
    async def train_step(self, batch: list[int]) -> None:
        self.batch = batch

    @endpoint
    async def received(self) -> tuple[int, list[int]]:
        return self.rank, self.batch


async def test_scatter() -> None:
    procs = this_host().spawn_procs(per_host={"gpus": 8})
    batches = [[0, 1], [2, 3]]

    # cookbook: scatter-split
    parallel_dims = {"pp": 2, "dp": 2, "tp": 2}
    trainer = procs.spawn("trainer", Trainer, parallel_dims)
    mesh = trainer.flatten("rank").split(rank=tuple(parallel_dims), **parallel_dims)
    by_dp_rank = [mesh.slice(dp=i) for i in range(mesh.size("dp"))]
    # cookbook: end

    # cookbook: scatter-check
    for dp_rank, trainer_at_dp_rank in enumerate(by_dp_rank):
        reported = set((await trainer_at_dp_rank.get_dp_rank.call()).values())
        if reported != {dp_rank}:
            raise RuntimeError(f"DP slice {dp_rank} holds DP ranks {reported}")
    # cookbook: end

    # cookbook: scatter-send
    await asyncio.gather(
        *[
            trainer_at_dp_rank.train_step.call(batches[dp_rank])
            for dp_rank, trainer_at_dp_rank in enumerate(by_dp_rank)
        ]
    )
    # cookbook: end

    received = dict((await trainer.received.call()).values())
    assert received == {
        0: [0, 1],
        1: [0, 1],
        4: [0, 1],
        5: [0, 1],
        2: [2, 3],
        3: [2, 3],
        6: [2, 3],
        7: [2, 3],
    }

    await procs.stop()


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
