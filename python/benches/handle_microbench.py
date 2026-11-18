# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Time the native Handle's observation paths, and three end-to-end calls.

Ready observations are too short to time one at a time, so they run in fixed
batches and report the median batch time per operation. Every other path is
timed per operation and reports p50 and p99. Before an operation counts, the
benchmark checks, through the existing Python surface, that it took the
intended path; an operation that fails its check is excluded and counted.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import json
import math
import os
import queue
import statistics
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any, NamedTuple

from monarch._rust_bindings.monarch_hyperactor.handle import (
    _HandleCompleter,
    _new_handle_pair,
    Handle,
)
from monarch._rust_bindings.monarch_hyperactor.testing import _handle_observer_count
from monarch.actor import Actor, Channel, context, endpoint, this_host

# Operations discarded at the start of each per-operation path.
WARMUP_OPERATIONS = 200
# Batches discarded at the start of each batched path.
WARMUP_BATCHES = 2
# Operations per batch, fixed per path so that a batch takes at least 1 ms.
READY_GET_BATCH = 10_000
READY_POLL_BATCH = 20_000
READY_AWAIT_BATCH = 2_000
# Not None, so that poll() can tell a value from a pending Handle.
VALUE = 7


class ValueActor(Actor):
    @endpoint
    def value(self) -> int:
        return VALUE


class Latencies(NamedTuple):
    """Per-operation durations in nanoseconds. An excluded operation failed its
    path check; its warmup entry is None."""

    warmup: list[int | None]
    samples: list[int]
    excluded: int


class Batches(NamedTuple):
    """Per-operation time of each batch, in nanoseconds."""

    size: int
    warmup: list[float]
    per_operation: list[float]


class PendingAwaits(NamedTuple):
    latencies: Latencies
    # How often the loop was asked to watch a descriptor.
    add_reader_calls: int


def time_operations(operation: Callable[[], int | None], count: int) -> Latencies:
    """Run warmup and then ``count`` operations. Each returns its duration, or
    None if it failed its path check."""
    durations = [operation() for _ in range(WARMUP_OPERATIONS + count)]
    return _latencies(durations)


async def time_operations_async(
    operation: Callable[[], Awaitable[int | None]], count: int
) -> Latencies:
    durations = [await operation() for _ in range(WARMUP_OPERATIONS + count)]
    return _latencies(durations)


def _latencies(durations: list[int | None]) -> Latencies:
    measured = durations[WARMUP_OPERATIONS:]
    samples = [d for d in measured if d is not None]
    return Latencies(
        durations[:WARMUP_OPERATIONS], samples, len(measured) - len(samples)
    )


def time_batches(batch: Callable[[], None], size: int, count: int) -> Batches:
    per_operation = []
    for _ in range(WARMUP_BATCHES + count):
        start = time.perf_counter_ns()
        batch()
        per_operation.append((time.perf_counter_ns() - start) / size)
    return Batches(size, per_operation[:WARMUP_BATCHES], per_operation[WARMUP_BATCHES:])


def _ready_handle() -> Handle[int]:
    handle, completer = _new_handle_pair()
    completer.set_result(VALUE)
    return handle


def ready_get(count: int) -> Batches:
    handle = _ready_handle()

    def batch() -> None:
        for _ in range(READY_GET_BATCH):
            handle.get()

    return time_batches(batch, READY_GET_BATCH, count)


def ready_poll(count: int) -> Batches:
    handle = _ready_handle()

    def batch() -> None:
        for _ in range(READY_POLL_BATCH):
            handle.poll()

    return time_batches(batch, READY_POLL_BATCH, count)


def ready_await(count: int) -> Batches:
    """Await a completed Handle, which returns without giving the loop a turn."""
    handle = _ready_handle()

    async def batches() -> Batches:
        per_operation = []
        for _ in range(WARMUP_BATCHES + count):
            start = time.perf_counter_ns()
            for _ in range(READY_AWAIT_BATCH):
                await handle
            per_operation.append((time.perf_counter_ns() - start) / READY_AWAIT_BATCH)
        warmup, measured = (
            per_operation[:WARMUP_BATCHES],
            per_operation[WARMUP_BATCHES:],
        )
        return Batches(READY_AWAIT_BATCH, warmup, measured)

    return asyncio.run(batches())


def _publish_after_registration(
    completers: queue.SimpleQueue[_HandleCompleter[int] | None], cpu: int
) -> None:
    """Complete each Handle only once its get() is registered: the pair counts
    the Handle as one observer and a waiting get() as a second."""
    os.sched_setaffinity(0, {cpu})
    while (completer := completers.get()) is not None:
        while _handle_observer_count(completer) < 2:
            time.sleep(0)
        completer.set_result(VALUE)


def pending_get(count: int) -> Latencies:
    """Time get() on a pending Handle that another thread completes.

    The publishing and observing threads share one CPU. Otherwise the
    scheduler's placement of the two, which varies from run to run, decides
    whether each wake-up crosses CPUs, and that dominates the time.
    """
    affinity = os.sched_getaffinity(0)
    cpu = min(affinity)
    os.sched_setaffinity(0, {cpu})
    completers: queue.SimpleQueue[_HandleCompleter[int] | None] = queue.SimpleQueue()
    publisher = threading.Thread(
        target=_publish_after_registration, args=(completers, cpu), daemon=True
    )
    publisher.start()

    def operation() -> int | None:
        handle, completer = _new_handle_pair()
        completers.put(completer)
        start = time.perf_counter_ns()
        value = handle.get()
        elapsed = time.perf_counter_ns() - start
        return elapsed if value == VALUE else None

    try:
        return time_operations(operation, count)
    finally:
        completers.put(None)
        publisher.join()
        os.sched_setaffinity(0, affinity)


def port_receive_get(count: int) -> Latencies:
    """Time get() driving a port receive inline. The message is sent first, and
    nothing observes the receive before get(), so get() starts it (HDL-24)."""
    sender, receiver = Channel[int].open()

    def operation() -> int | None:
        sender.send(VALUE)
        handle = receiver.recv()._as_handle()
        start = time.perf_counter_ns()
        value = handle.get()
        elapsed = time.perf_counter_ns() - start
        return elapsed if value == VALUE else None

    return time_operations(operation, count)


def raw_reply_poll(actor: ValueActor, count: int) -> Latencies:
    """Time the poll() that unpickles a call_one() reply on the caller.

    No Python-visible probe tells a pickled reply from a pending one without
    claiming it, so poll until a value returns and keep that poll alone. This
    is the only observer, and only an observer moves a pickled reply to its
    value, so that poll found the reply pickled and unpickled it.
    """

    def operation() -> int | None:
        handle = actor.value.call_one()._as_handle()
        while True:
            start = time.perf_counter_ns()
            value = handle.poll()
            elapsed = time.perf_counter_ns() - start
            if value is not None:
                return elapsed if value == VALUE else None

    return time_operations(operation, count)


class LoopWitness:
    """Record which path completes each pending await on one event loop.

    Monarch decides once per loop, on its first pending await, whether to
    complete awaits through a pipe the loop watches or through
    ``call_soon_threadsafe``, and caches that decision. Install the witness on
    a fresh loop, before its first Handle await. On the fallback loop,
    ``add_reader`` raises ``NotImplementedError``. On the pipe loop, it counts
    each run of the reader it registers. On both, ``call_soon_threadsafe``
    records the first argument it schedules: the fallback schedules its
    completer with the awaited future as that argument.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, fallback: bool) -> None:
        self.fallback = fallback
        self.add_reader_calls = 0
        self.reader_runs = 0
        self.scheduled: list[object] = []
        self._call_soon_threadsafe = loop.call_soon_threadsafe
        self._add_reader = loop.add_reader
        loop.call_soon_threadsafe = self.call_soon_threadsafe
        loop.add_reader = self.add_reader

    def call_soon_threadsafe(
        self, callback: Callable[..., object], *args: Any, **kwargs: Any
    ) -> asyncio.Handle:
        if args:
            self.scheduled.append(args[0])
        return self._call_soon_threadsafe(callback, *args, **kwargs)

    def add_reader(self, fd: Any, callback: Callable[..., object], *args: Any) -> None:
        self.add_reader_calls += 1
        if self.fallback:
            raise NotImplementedError

        def counted(*callback_args: Any) -> object:
            self.reader_runs += 1
            return callback(*callback_args)

        self._add_reader(fd, counted, *args)

    def took_path(self, future: asyncio.Future[Any], reader_runs: int) -> bool:
        """Whether this loop's path completed ``future``. ``reader_runs`` is the
        reader count before the await. Clears the record of scheduled calls."""
        scheduled = any(item is future for item in self.scheduled)
        self.scheduled.clear()
        if self.fallback:
            return scheduled
        return not scheduled and self.reader_runs > reader_runs


async def _await_pending(
    witness: LoopWitness, make_handle: Callable[[], Handle[Any]], expected: object
) -> int | None:
    handle = make_handle()
    reader_runs = witness.reader_runs
    start = time.perf_counter_ns()
    future = handle.as_asyncio()
    pending = not future.done()
    value = await future
    elapsed = time.perf_counter_ns() - start
    on_path = witness.took_path(future, reader_runs)
    return elapsed if pending and on_path and value == expected else None


def run_pending_awaits(
    loop: asyncio.AbstractEventLoop,
    make_handle: Callable[[], Handle[Any]],
    expected: object,
    count: int,
    fallback: bool,
) -> PendingAwaits:
    """Time ``count`` awaits of fresh pending Handles from ``make_handle``,
    after warmup, on ``loop``."""
    witness = LoopWitness(loop, fallback)
    latencies = loop.run_until_complete(
        time_operations_async(
            lambda: _await_pending(witness, make_handle, expected), count
        )
    )
    return PendingAwaits(latencies, witness.add_reader_calls)


def pending_awaits(
    make_handle: Callable[[], Handle[Any]],
    expected: object,
    count: int,
    fallback: bool,
) -> Latencies:
    """Time pending awaits on a fresh loop that takes the pipe, or the fallback."""
    loop = asyncio.new_event_loop()
    try:
        result = run_pending_awaits(loop, make_handle, expected, count, fallback)
    finally:
        loop.close()
    if result.add_reader_calls == 0:
        raise RuntimeError("the fresh loop was never asked to watch a descriptor")
    return result.latencies


def call_one_get(actor: ValueActor, count: int) -> Latencies:
    def operation() -> int | None:
        start = time.perf_counter_ns()
        value = actor.value.call_one().get()
        elapsed = time.perf_counter_ns() - start
        return elapsed if value == VALUE else None

    return time_operations(operation, count)


def call_get(actor: ValueActor, count: int) -> Latencies:
    def operation() -> int | None:
        start = time.perf_counter_ns()
        values = actor.value.call().get()
        elapsed = time.perf_counter_ns() - start
        return elapsed if list(values.values()) == [VALUE] else None

    return time_operations(operation, count)


def stream_item_get(actor: ValueActor, count: int) -> Latencies:
    def operation() -> int | None:
        start = time.perf_counter_ns()
        values = [item.get() for item in actor.value.stream()]
        elapsed = time.perf_counter_ns() - start
        return elapsed if values == [VALUE] else None

    return time_operations(operation, count)


def paths(
    actor: ValueActor, operations: int, batches: int
) -> dict[str, Callable[[], Latencies | Batches]]:
    """Every path, in run order."""

    def call_one_handle() -> Handle[int]:
        return actor.value.call_one()._as_handle()

    return {
        "ready get": lambda: ready_get(batches),
        "ready poll": lambda: ready_poll(batches),
        "ready await": lambda: ready_await(batches),
        "pending get": lambda: pending_get(operations),
        "port receive get": lambda: port_receive_get(operations),
        "raw reply poll": lambda: raw_reply_poll(actor, operations),
        "pipe await": lambda: pending_awaits(
            call_one_handle, VALUE, operations, fallback=False
        ),
        "fallback await": lambda: pending_awaits(
            call_one_handle, VALUE, operations, fallback=True
        ),
        "call_one get": lambda: call_one_get(actor, operations),
        "call get": lambda: call_get(actor, operations),
        "stream item get": lambda: stream_item_get(actor, operations),
    }


def timer() -> tuple[int, float]:
    """Return the clock's resolution and the cost of an empty interval.

    Consecutive ``perf_counter_ns`` readings are never closer than one call
    apart, so the smallest step measures the call, not the clock. The clock's
    resolution is the greatest common divisor of the steps; the empty interval
    is their median.
    """
    steps = []
    for _ in range(100_000):
        start = time.perf_counter_ns()
        steps.append(time.perf_counter_ns() - start)
    return functools.reduce(math.gcd, steps), statistics.median(steps)


def report(name: str, result: Latencies | Batches) -> None:
    if isinstance(result, Batches):
        median = statistics.median(result.per_operation)
        print(
            f"{name}: {median:.1f} ns per operation, median of "
            f"{len(result.per_operation)} batches of {result.size}"
        )
        return
    if len(result.samples) < 2:
        raise RuntimeError(f"{name}: {result.excluded} operations failed their check")
    percentiles = statistics.quantiles(result.samples, n=100, method="inclusive")
    print(
        f"{name}: p50 {percentiles[49]:.0f} ns, p99 {percentiles[98]:.0f} ns, "
        f"{len(result.samples)} operations, {result.excluded} excluded"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--operations", type=int, default=2000)
    parser.add_argument("--batches", type=int, default=50)
    parser.add_argument("--paths", help="comma-separated path names; default all")
    parser.add_argument("--samples", help="write every raw sample to this JSON file")
    args = parser.parse_args()
    if args.operations < 2 or args.batches < 1:
        parser.error("--operations must be at least 2 and --batches at least 1")

    context()
    resolution, empty = timer()
    print(f"python {sys.version.split()[0]}")
    print(f"timer resolution: {resolution} ns; empty interval: {empty:.0f} ns")
    procs = this_host().spawn_procs(per_host={"procs": 1})
    try:
        actor = procs.spawn("value", ValueActor)
        selected = paths(actor, args.operations, args.batches)
        if args.paths:
            selected = {name: selected[name] for name in args.paths.split(",")}
        results = {name: run() for name, run in selected.items()}
    finally:
        procs.stop().get()
    for name, result in results.items():
        report(name, result)
    if args.samples:
        with open(args.samples, "w") as samples:
            json.dump({name: r._asdict() for name, r in results.items()}, samples)


if __name__ == "__main__":
    main()
