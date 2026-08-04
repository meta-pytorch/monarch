# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Benchmark: in-process self send -> receive round trips at various sizes.

An actor sends a message to itself and awaits it back, for message sizes from
64 B up to 1 GiB. The payload is a single minimonarch.bytearray, allocated once:
send() *moves* its storage into the message, next() hands that same storage back
as a bytearray, and the next iteration moves it again. The one buffer ping-pongs
with zero copies and zero per-iteration allocation, so the round-trip time
should be flat regardless of size.

Run as a pytest test (use -s to see the report):

    uv run pytest -s test_bench.py

or standalone:

    uv run python test_bench.py
"""

import asyncio
import time

import minimonarch
from minimonarch import Actor

ba = minimonarch.bytearray

ITERS = 20

# (label, size in bytes), 64 B .. 1 GiB.
SIZES = [
    ("64 B", 64),
    ("1 KiB", 1 << 10),
    ("64 KiB", 1 << 16),
    ("1 MiB", 1 << 20),
    ("16 MiB", 1 << 24),
    ("256 MiB", 1 << 28),
    ("1 GiB", 1 << 30),
]


async def roundtrip_per_iter(size: int, iters: int) -> float:
    """Mean seconds for one send->receive round trip at `size`. A single buffer
    is allocated once and reused: send moves it out, next() hands it back."""
    a = Actor(b"bench")
    buf = ba(size)  # allocated ONCE; reused every iteration

    # Warm up (prime the poller/loop machinery).
    a.send(b"bench", [buf])
    buf = (await a.next())[0]

    total = 0.0
    for _ in range(iters):
        start = time.perf_counter()
        a.send(b"bench", [buf])  # moves buf's storage into the message
        msg = await a.next()  # hands that same storage back as a bytearray
        total += time.perf_counter() - start
        buf = msg[0]  # reuse it next iteration — no allocation, no copy
        assert len(buf) == size
    return total / iters


async def run_benchmark() -> list[tuple[str, int, float]]:
    results = []
    print(f"\nin-process self round-trip, {ITERS} iters/size (move, zero-copy)\n")
    print(f"{'size':>10} {'per-iter':>14} {'throughput':>16}")
    print("-" * 42)
    for label, size in SIZES:
        per_iter = await roundtrip_per_iter(size, ITERS)
        gbps = (size / per_iter) / 1e9 if per_iter else float("inf")
        results.append((label, size, per_iter))
        print(f"{label:>10} {per_iter * 1e6:>11.2f} us {gbps:>12.2f} GB/s")
    return results


async def test_roundtrip_does_not_scale_with_size() -> None:
    results = await run_benchmark()
    per_iters = [p for _, _, p in results]
    smallest = min(per_iters)
    largest_size_per_iter = results[-1][2]  # 1 GiB

    # If delivery copied the payload, 1 GiB would cost ~0.1 s/iter — orders of
    # magnitude above the ~tens-of-microseconds round trip. The move keeps it
    # flat; allow generous slack for scheduling noise but still catch O(size).
    budget = smallest * 30 + 2e-3
    print(
        f"\n1 GiB per-iter {largest_size_per_iter * 1e6:.2f} us vs "
        f"min {smallest * 1e6:.2f} us (budget {budget * 1e6:.2f} us)"
    )
    assert largest_size_per_iter <= budget, (
        "in-process round-trip time scales with message size — delivery is "
        "not zero-copy"
    )
    print("OK: in-process delivery does not scale with message size")


if __name__ == "__main__":
    asyncio.run(test_roundtrip_does_not_scale_with_size())
