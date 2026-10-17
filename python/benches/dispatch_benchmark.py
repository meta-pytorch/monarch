# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Measure noop calls, stream observations and channel receives on the main thread
and inside sync actors, plus burst admission into an actor whose event loop is busy."""

from __future__ import annotations

import argparse
import asyncio
import collections
import concurrent.futures
import statistics
import threading
import time
from collections.abc import Callable

from monarch.actor import (
    Accumulator,
    Actor,
    Channel,
    concurrent_endpoint,
    endpoint,
    Port,
    this_host,
)
from monarch.benches.noop_rpc_benchmark import (
    benchmark_noop_rpc,
    NoopActor,
    SyncNoopActor,
)


class BusyLoopActor(Actor):
    """Blocks its event loop for 11 ms per step, then yields once, like an
    inference engine's step loop."""

    def __init__(self) -> None:
        self.running = False
        self.blocks = 0
        self.started: list[int] = []

    @endpoint
    async def start(self) -> None:
        self.running = True
        self.task: asyncio.Task[None] = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while self.running:
            time.sleep(0.011)
            self.blocks += 1
            await asyncio.sleep(0)

    @concurrent_endpoint
    async def req(self, i: int) -> int:
        self.started.append(self.blocks)
        return i

    @endpoint
    async def finish(self) -> list[int]:
        self.running = False
        await self.task
        return self.started


class SyncCaller(Actor):
    """Measure endpoint replies, stream items and channel receives on a sync driver."""

    @endpoint
    def measure(self, peer: SyncNoopActor, iterations: int) -> None:
        benchmark_noop_rpc(peer, iterations)

    @endpoint
    def measure_stream(self, peer: SyncNoopActor, iterations: int) -> None:
        benchmark_stream(peer, iterations)

    @endpoint
    def measure_receive(self, iterations: int) -> None:
        benchmark_receive_loopback(iterations)

    @endpoint
    def prepare_receive(self) -> Port[int]:
        sender, self._input_receiver = Channel[int].open()
        return sender

    @endpoint
    def receive_and_reply(self, reply: Port[int], count: int) -> None:
        """Receive each request through a channel and send it to the caller's channel."""
        for _ in range(count):
            reply.send(self._input_receiver.recv().get())


def report_receive_latencies(latencies_ms: list[float]) -> None:
    """Report channel receive timings in the same units as the RPC controls."""
    percentiles = statistics.quantiles(latencies_ms, n=100, method="inclusive")
    print("warmup iterations: 20")
    print(f"measured iterations: {len(latencies_ms)}")
    print(f"p50 receive latency: {percentiles[49]:.3f} ms")
    print(f"p99 receive latency: {percentiles[98]:.3f} ms")


def benchmark_receive(operation: Callable[[], int], iterations: int) -> None:
    """Time send-and-receive operations, checking every delivered value."""
    if iterations < 2:
        raise ValueError("iterations must be at least 2")
    for _ in range(20):
        assert operation() == 7
    latencies_ms = []
    for _ in range(iterations):
        start = time.perf_counter()
        assert operation() == 7
        latencies_ms.append((time.perf_counter() - start) * 1000)
    report_receive_latencies(latencies_ms)


def benchmark_receive_loopback(iterations: int, timeout: float | None = None) -> None:
    """Send to this actor's own channel, then wait for its receive Future."""
    sender, receiver = Channel[int].open()

    def receive() -> int:
        sender.send(7)
        return receiver.recv().get(timeout=timeout)

    benchmark_receive(receive, iterations)


async def benchmark_receive_asyncio(iterations: int) -> None:
    """Measure loopback receives observed by await on one asyncio loop."""
    if iterations < 2:
        raise ValueError("iterations must be at least 2")
    sender, receiver = Channel[int].open()
    latencies_ms = []
    for index in range(20 + iterations):
        start = time.perf_counter()
        sender.send(7)
        assert await receiver.recv() == 7
        if index >= 20:
            latencies_ms.append((time.perf_counter() - start) * 1000)
    report_receive_latencies(latencies_ms)


def benchmark_receive_plain_thread(iterations: int) -> None:
    """Run the whole loopback case on one plain Python thread, not a Tokio thread."""
    completion: concurrent.futures.Future[None] = concurrent.futures.Future()

    def measure() -> None:
        try:
            benchmark_receive_loopback(iterations)
        except Exception as error:
            completion.set_exception(error)
        else:
            completion.set_result(None)

    thread = threading.Thread(target=measure, daemon=True)
    thread.start()
    thread.join(timeout=120)
    if thread.is_alive():
        raise TimeoutError("plain-thread receive benchmark did not finish in 120s")
    completion.result(timeout=0)


def benchmark_receive_ping_pong(caller: SyncCaller, iterations: int) -> None:
    """Round-trip channel messages between the controller and a sync actor's proc."""
    requests = caller.prepare_receive.call_one().get()
    replies, receiver = Channel[int].open()
    completion = caller.receive_and_reply.call_one(replies, 20 + iterations)

    def receive() -> int:
        requests.send(7)
        return receiver.recv().get(timeout=10)

    benchmark_receive(receive, iterations)
    completion.get(timeout=10)


def benchmark_observation(operation: Callable[[], None], iterations: int) -> None:
    """Report p50 and p99 for completing a noop stream operation, after warmup."""
    if iterations < 2:
        raise ValueError("iterations must be at least 2")
    warmup_iterations = 20
    for _ in range(warmup_iterations):
        operation()
    latencies_ms: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        operation()
        latencies_ms.append((time.perf_counter() - start) * 1000)
    percentiles = statistics.quantiles(latencies_ms, n=100, method="inclusive")
    print(f"warmup iterations: {warmup_iterations}")
    print(f"measured iterations: {iterations}")
    print(f"p50 stream RPC latency: {percentiles[49]:.3f} ms")
    print(f"p99 stream RPC latency: {percentiles[98]:.3f} ms")


def benchmark_stream(peer: SyncNoopActor, iterations: int) -> None:
    """Measure untimed get() on each item, including the off-main inline path
    when called from SyncCaller."""

    def observe() -> None:
        for reply in peer.noop.stream():
            reply.get()

    benchmark_observation(observe, iterations)


def benchmark_accumulator(peer: SyncNoopActor, iterations: int) -> None:
    """Measure the stream's await path inside an Accumulator's private loop."""
    accumulator = Accumulator(peer.noop, None, lambda accumulated, _: accumulated)

    def observe() -> None:
        accumulator.accumulate().get()

    benchmark_observation(observe, iterations)


async def benchmark_burst(busy: BusyLoopActor, count: int) -> None:
    """Send `count` concurrent calls to `busy` at once and report how they got
    in."""
    await busy.start.call_one()
    begin = time.perf_counter()
    await asyncio.gather(*(busy.req.call_one(i) for i in range(count)))
    admission_ms = (time.perf_counter() - begin) * 1000
    started = await busy.finish.call_one()
    per_block = collections.Counter(started)
    print(f"burst size: {count}")
    print(f"burst admission: {admission_ms:.1f} ms")
    print(f"burst span: {max(per_block) - min(per_block) + 1} blocks")
    print(f"burst largest block: {max(per_block.values())} bodies")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument("--burst", type=int, default=256)
    args = parser.parse_args()
    procs = this_host().spawn_procs(per_host={"procs": 1})
    try:
        print("async endpoint:")
        benchmark_noop_rpc(procs.spawn("noop", NoopActor), args.iterations)
        print("sync endpoint:")
        sync_noop = procs.spawn("sync_noop", SyncNoopActor)
        benchmark_noop_rpc(sync_noop, args.iterations)
        print("nested sync-actor endpoint:")
        caller = procs.spawn("sync_caller", SyncCaller)
        caller.measure.call_one(sync_noop, args.iterations).get()
        print("main-thread stream get:")
        benchmark_stream(sync_noop, args.iterations)
        print("nested sync-actor stream get:")
        caller.measure_stream.call_one(sync_noop, args.iterations).get()
        print("main-thread Accumulator stream await:")
        benchmark_accumulator(sync_noop, args.iterations)
        print("main-thread channel receive get:")
        benchmark_receive_loopback(args.iterations)
        print("main-thread channel receive timed get:")
        benchmark_receive_loopback(args.iterations, timeout=5)
        print("asyncio channel receive await:")
        asyncio.run(benchmark_receive_asyncio(args.iterations))
        print("plain-thread channel receive get:")
        benchmark_receive_plain_thread(args.iterations)
        print("sync-actor channel receive get:")
        caller.measure_receive.call_one(args.iterations).get()
        print("cross-proc channel receive ping-pong:")
        benchmark_receive_ping_pong(caller, args.iterations)
        asyncio.run(benchmark_burst(procs.spawn("busy", BusyLoopActor), args.burst))
    finally:
        procs.stop().get()


if __name__ == "__main__":
    main()
