#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Benchmark for measuring message throughput in Monarch actor mesh.
"""

import asyncio
import csv
import itertools
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import check_output

import humanfriendly
from monarch._rust_bindings.monarch_hyperactor.config import (  # @manual=//monarch/monarch_extension:monarch_extension_no_torch
    reload_config_from_env,
)
from monarch.actor import (  # @manual=//monarch/python/monarch/actor:actor_no_torch
    Actor,
    endpoint,
    proc_mesh,
    ProcMesh,
)
from windtunnel.benchmarks.python_benchmark_runner.benchmark import (
    main,
    register_benchmark,
    UserCounters,
    UserMetric,
)

FILE_PATH: str = "monarch/python/benches/actor_mesh_benchmark.py"


def get_rev() -> str:
    try:
        return (
            check_output(
                "hg id",
                shell=True,
            )
            .decode()
            .strip()
        )
    except Exception:
        return "unknown_rev"


def get_rev_name() -> str:
    try:
        name = (
            check_output(
                "hg log -r . --template '{desc|firstline}'",
                shell=True,
            )
            .decode()
            .strip()
        )

        if len(name) > 20:
            name = name[:20]
        return name
    except Exception:
        return "unknown_rev_name"


class Pong(Actor):
    @endpoint
    async def pong(self, data: bytes) -> int:
        return len(data)


class Ping(Actor):
    def __init__(self, other: Pong) -> None:
        self.pong = other

    @endpoint
    async def ping(self, data: bytes) -> int:
        await self.pong.pong.call(data)
        return len(data)


MILLION = 1_000_000


@dataclass
class Benchmark:
    counters: defaultdict[str, float] = field(
        default_factory=lambda: defaultdict(lambda: 0.0)
    )
    test_duration: float = 3.0
    min_iterations: int = 100
    _clock: float = 0.0
    _duration: float = 0.0
    _iterations: int = 0

    def _start(self) -> None:
        self._clock = time.monotonic()

    def _stop(self) -> float:
        assert self._clock != 0.0
        delta = time.monotonic() - self._clock
        self._duration += delta
        return delta

    def meta(self) -> dict[str, float | str | int]:
        return {
            "test_name": self.name(),
            "rev": get_rev(),
            "rev_name": get_rev_name(),
        }

    def name(self) -> str:
        return self.__class__.__name__

    def bump_counter(self, name: str, delta: float) -> None:
        name = name.replace(" ", "_")
        old = self.counters[name]
        self.counters[name] = old + delta

    def report(self) -> dict[str, float | str | int]:
        return self.meta() | self.counters

    def reached_min_iterations(self) -> bool:
        return self._iterations >= self.min_iterations

    def reached_min_duration(self) -> bool:
        return self._duration >= self.test_duration

    async def run(self) -> dict[str, float | str | int]:
        self.counters = defaultdict(lambda: 0.0)
        batch_size = 10
        batch_durations = []
        await self.setup()
        await self.run_once()
        while not (self.reached_min_iterations() and self.reached_min_duration()):
            remaining = self.test_duration - self._duration
            print(f"{self.name()}::{batch_size=}::time_{remaining=}")
            for _ in range(batch_size):
                self._start()
                await self.run_once()
                batch_durations.append(self._stop())
                self._iterations += 1

            remaining = self.test_duration - self._duration
            batch_size = max(int(remaining / min(batch_durations)), 10)

        await self.teardown()

        self.counters["total_test_duration_us"] = self._duration * MILLION
        self.counters["total_test_iterations"] = self._iterations
        avg = sum(batch_durations) / len(batch_durations)
        self.counters["avg_duration_us"] = avg * MILLION
        self.counters["min_duration_us"] = min(batch_durations) * MILLION
        self.counters["max_duration_us"] = max(batch_durations) * MILLION
        self.counters["stddev_duration_us"] = (
            sum([abs(x - avg) for x in batch_durations]) / len(batch_durations)
        ) * MILLION

        return self.report()

    async def setup(self) -> None:
        pass

    async def teardown(self) -> None:
        pass

    async def run_once(self) -> None:
        raise NotImplementedError()


@dataclass
class ActorLatency(Benchmark):
    host_count: int = 1
    gpu_count: int = 1
    message_size: int = 1024
    message: bytes = b""
    pong_mesh: ProcMesh | None = None
    pong_actors: Pong | None = None

    def meta(self) -> dict[str, float | str | int]:
        return super().meta() | {
            "hosts": self.host_count,
            "gpus": self.gpu_count,
            "message_size": self.message_size,
        }

    async def run_once(self) -> None:
        pong = self.pong_actors
        assert pong is not None
        await pong.pong.call(self.message)
        self.bump_counter("bytes", self.message_size)
        self.bump_counter("casts", 1)
        self.bump_counter("messages", pong.size())

    async def teardown(self) -> None:
        assert self.pong_mesh is not None
        await self.pong_mesh.stop()

    async def setup(self) -> None:
        reload_config_from_env()
        pong_mesh = proc_mesh(hosts=self.host_count, gpus=self.gpu_count)
        await pong_mesh.logging_option(stream_to_client=True, aggregate_window_sec=None)

        self.pong_actors = pong_mesh.spawn("pong", Pong)
        self.pong_mesh = pong_mesh
        self.message = bytes(self.message_size)


@dataclass
class ActorThroughput(ActorLatency):
    request_batch_size: int = 10
    min_iterations: int = 10

    def meta(self) -> dict[str, int | float | str]:
        return super().meta() | {
            "request_batch_size": self.request_batch_size,
        }

    async def run_once(self) -> None:
        pong = self.pong_actors
        assert pong is not None
        res = await asyncio.gather(
            *[pong.pong.call(self.message) for _ in range(self.request_batch_size)]
        )
        assert len(res) == self.request_batch_size, "did not receive all responses"
        self.bump_counter("bytes", self.message_size * self.request_batch_size)
        self.bump_counter("casts", self.request_batch_size)
        self.bump_counter("messages", pong.size() * self.request_batch_size)


message_sizes: list[int] = [10**n for n in range(3, 9, 2)]
host_counts = [1, 10]
gpu_counts = [1, 10]
host_counts = [1]
gpu_counts = [1]
runners = [
    ActorLatency,
    ActorThroughput,
]

for hosts, gpus, message_size, Runner in itertools.product(
    host_counts, gpu_counts, message_sizes, runners
):
    bench = Runner(
        host_count=hosts,
        gpu_count=gpus,
        message_size=message_size,
    )
    size_name = humanfriendly.format_size(message_size).replace(" ", "_")

    @register_benchmark(
        FILE_PATH,
        use_counters=True,
        name=f"{bench.__class__.__name__}_{hosts=}_{gpus=}_{size_name}",
        bench=bench,
    )
    async def bench_actor_scaling(counters: UserCounters, bench: Benchmark) -> None:
        filename = Path(f"/tmp/actor_mesh_benchmark/{get_rev()}.csv")
        filename.parent.mkdir(parents=True, exist_ok=True)
        w = None
        row = await bench.run()
        for k, v in row.items():
            if isinstance(v, (int, float)):
                counters[k] = UserMetric(value=int(v))

        with open(filename, "a") as f:
            w = csv.DictWriter(
                f,
                fieldnames=row.keys(),
            )
            if os.stat(filename).st_size == 0:
                w.writeheader()
                f.flush()
            w.writerow(row)
            f.flush()
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
