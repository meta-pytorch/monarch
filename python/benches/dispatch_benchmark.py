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
from typing import Any

from monarch.actor import (
    Accumulator,
    Actor,
    Channel,
    concurrent_endpoint,
    context,
    endpoint,
    Future,
    Port,
    ProcMesh,
    this_host,
)
from monarch.benches.noop_rpc_benchmark import (
    benchmark_noop_rpc,
    NoopActor,
    SyncNoopActor,
)

PENDING_PATH_MIN = 0.95


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
    def measure_call_one(self, peer: SyncNoopActor, iterations: int) -> None:
        benchmark_call_one(peer, iterations)

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


class BatchReplyActor(Actor):
    """Return a fixed small value, optionally with an out-of-band mesh reference."""

    @endpoint
    def value(self, with_refs: bool) -> Any:
        if with_refs:
            return 7, context().actor_instance.proc_mesh
        return 7


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


def benchmark_receive_once(iterations: int) -> None:
    """Open a once channel, send to it, then wait for its receive Future."""

    def receive() -> int:
        sender, receiver = Channel[int].open(once=True)
        sender.send(7)
        return receiver.recv().get()

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


def benchmark_observation(
    operation: Callable[[], None], iterations: int, label: str = "stream RPC"
) -> None:
    """Report p50 and p99 for completing a noop operation, after warmup."""
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
    print(f"p50 {label} latency: {percentiles[49]:.3f} ms")
    print(f"p99 {label} latency: {percentiles[98]:.3f} ms")


def benchmark_stream(peer: SyncNoopActor, iterations: int) -> None:
    """Measure untimed get() on each item, including the off-main inline path
    when called from SyncCaller."""

    def observe() -> None:
        for reply in peer.noop.stream():
            reply.get()

    benchmark_observation(observe, iterations)


def benchmark_call_one(peer: SyncNoopActor, iterations: int) -> None:
    """Measure untimed get() on a call_one() reply, including the off-main
    inline path when called from SyncCaller."""

    def observe() -> None:
        peer.noop.call_one().get()

    benchmark_observation(observe, iterations, label="call_one RPC")


async def benchmark_call_one_asyncio(peer: SyncNoopActor, iterations: int) -> None:
    """Measure ``await call_one()`` on one asyncio loop, timed per await."""
    if iterations < 2:
        raise ValueError("iterations must be at least 2")
    latencies_ms = []
    for index in range(20 + iterations):
        start = time.perf_counter()
        await peer.noop.call_one()
        if index >= 20:
            latencies_ms.append((time.perf_counter() - start) * 1000)
    percentiles = statistics.quantiles(latencies_ms, n=100, method="inclusive")
    print("warmup iterations: 20")
    print(f"measured iterations: {iterations}")
    print(f"p50 call_one RPC latency: {percentiles[49]:.3f} ms")
    print(f"p99 call_one RPC latency: {percentiles[98]:.3f} ms")


async def benchmark_call_asyncio(peer: SyncNoopActor, iterations: int) -> None:
    """Measure an actual asyncio observation of call(), timed per await."""
    if iterations < 2:
        raise ValueError("iterations must be at least 2")
    latencies_ms = []
    for index in range(20 + iterations):
        start = time.perf_counter()
        await peer.noop.call()
        if index >= 20:
            latencies_ms.append((time.perf_counter() - start) * 1000)
    percentiles = statistics.quantiles(latencies_ms, n=100, method="inclusive")
    print("warmup iterations: 20")
    print(f"measured iterations: {iterations}")
    print(f"p50 call RPC latency: {percentiles[49]:.3f} ms")
    print(f"p99 call RPC latency: {percentiles[98]:.3f} ms")


async def benchmark_call_batch_asyncio(peer: BatchReplyActor, iterations: int) -> None:
    """Measure ``await call(True)`` on four ranks, whose replies carry a mesh,
    timed per await."""
    if iterations < 2:
        raise ValueError("iterations must be at least 2")
    latencies_ms = []
    for index in range(20 + iterations):
        start = time.perf_counter()
        value = await peer.value.call(True)
        if index >= 20:
            latencies_ms.append((time.perf_counter() - start) * 1000)
        assert len(value) == 4
    percentiles = statistics.quantiles(latencies_ms, n=100, method="inclusive")
    print("warmup iterations: 20")
    print(f"measured iterations: {iterations}")
    print(f"p50 batch call latency: {percentiles[49]:.3f} ms")
    print(f"p99 batch call latency: {percentiles[98]:.3f} ms")


def benchmark_call_batch(
    peer: BatchReplyActor, iterations: int, with_refs: bool
) -> None:
    """Measure four-rank Future observation, then element access separately.

    Each access sample gets a fresh ValueMesh outside its measured interval.
    This keeps the lazy-cell cost distinct from building the Python ValueMesh.
    """

    def observe() -> None:
        value = peer.value.call(with_refs).get()
        assert len(value) == 4

    benchmark_observation(observe, iterations, label="batch call")
    latencies_ms = []
    for index in range(20 + iterations):
        value = peer.value.call(with_refs).get()
        start = time.perf_counter()
        values = list(value.values())
        assert (
            [item[0] for item in values] == [7] * 4 if with_refs else values == [7] * 4
        )
        if index >= 20:
            latencies_ms.append((time.perf_counter() - start) * 1000)
    percentiles = statistics.quantiles(latencies_ms, n=100, method="inclusive")
    print("warmup iterations: 20")
    print(f"measured iterations: {iterations}")
    print(f"p50 batch elements latency: {percentiles[49]:.3f} ms")
    print(f"p99 batch elements latency: {percentiles[98]:.3f} ms")


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


async def count_pending(call: Callable[[], Future[Any]], iterations: int) -> int:
    """Await ``call()`` ``iterations`` times; return how many of its
    ``as_asyncio()`` futures were not yet done when returned.

    ``as_asyncio()`` completes the future before returning when the reply has
    already arrived. On the pending branch only the loop completes it, which
    cannot happen before this coroutine yields.
    """
    pending = 0
    for _ in range(iterations):
        future = call().as_asyncio()
        pending += not future.done()
        await future
    return pending


def check_pending(procs: ProcMesh, iterations: int) -> None:
    """Require each endpoint-await shape to use the pending path often enough."""
    if iterations < 1:
        raise ValueError("iterations must be at least 1")
    sync_noop = procs.spawn("sync_noop", SyncNoopActor)
    batch_procs = this_host().spawn_procs(per_host={"batch_rank": 4})
    try:
        batch_peer = batch_procs.spawn("batch_reply", BatchReplyActor)
        shapes: dict[str, Callable[[], Future[Any]]] = {
            "asyncio call_one await": lambda: sync_noop.noop.call_one(),
            "asyncio call await": lambda: sync_noop.noop.call(),
            "asyncio four-rank ref-bearing call await": lambda: batch_peer.value.call(
                True
            ),
        }
        failures = []
        for label, call in shapes.items():
            pending = asyncio.run(count_pending(call, iterations))
            fraction = pending / iterations
            print(f"{label}: {pending} of {iterations} pending ({fraction:.1%})")
            if fraction < PENDING_PATH_MIN:
                failures.append((label, fraction))
        if failures:
            label, fraction = failures[0]
            raise RuntimeError(
                f"{label}: {fraction:.1%} pending is below the required "
                f"{PENDING_PATH_MIN:.1%}"
            )
    finally:
        batch_procs.stop().get()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument("--burst", type=int, default=256)
    parser.add_argument(
        "--check-pending",
        action="store_true",
        help="instead of timing, require every endpoint-await shape to take the "
        "pending path often enough",
    )
    args = parser.parse_args()
    procs = this_host().spawn_procs(per_host={"procs": 1})
    try:
        if args.check_pending:
            check_pending(procs, args.iterations)
            return
        print("async endpoint:")
        benchmark_noop_rpc(procs.spawn("noop", NoopActor), args.iterations)
        print("sync endpoint:")
        sync_noop = procs.spawn("sync_noop", SyncNoopActor)
        benchmark_noop_rpc(sync_noop, args.iterations)
        print("main-thread call_one get:")
        benchmark_call_one(sync_noop, args.iterations)
        print("asyncio call_one await:")
        asyncio.run(benchmark_call_one_asyncio(sync_noop, args.iterations))
        print("asyncio call await:")
        asyncio.run(benchmark_call_asyncio(sync_noop, args.iterations))
        print("nested sync-actor endpoint:")
        caller = procs.spawn("sync_caller", SyncCaller)
        caller.measure.call_one(sync_noop, args.iterations).get()
        print("nested sync-actor call_one get:")
        caller.measure_call_one.call_one(sync_noop, args.iterations).get()
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
        print("main-thread once-port receive get:")
        benchmark_receive_once(args.iterations)
        print("asyncio channel receive await:")
        asyncio.run(benchmark_receive_asyncio(args.iterations))
        print("plain-thread channel receive get:")
        benchmark_receive_plain_thread(args.iterations)
        print("sync-actor channel receive get:")
        caller.measure_receive.call_one(args.iterations).get()
        print("cross-proc channel receive ping-pong:")
        benchmark_receive_ping_pong(caller, args.iterations)
        batch_procs = this_host().spawn_procs(per_host={"batch_rank": 4})
        try:
            batch_peer = batch_procs.spawn("batch_reply", BatchReplyActor)
            print("main-thread four-rank no-ref call get:")
            benchmark_call_batch(batch_peer, args.iterations, with_refs=False)
            print("main-thread four-rank ref-bearing call get:")
            benchmark_call_batch(batch_peer, args.iterations, with_refs=True)
            print("asyncio four-rank ref-bearing call await:")
            asyncio.run(benchmark_call_batch_asyncio(batch_peer, args.iterations))
        finally:
            batch_procs.stop().get()
        asyncio.run(benchmark_burst(procs.spawn("busy", BusyLoopActor), args.burst))
    finally:
        procs.stop().get()


if __name__ == "__main__":
    main()
