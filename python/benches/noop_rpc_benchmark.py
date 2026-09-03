#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import statistics
import time

from monarch.actor import Actor, endpoint

_WARMUP_ITERATIONS = 10


class NoopActor(Actor):
    @endpoint
    async def noop(self) -> None:
        return None


def benchmark_noop_rpc(
    actor: NoopActor,
    num_iterations: int,
) -> None:
    """Measure the latency of the requested number of noop calls."""
    if num_iterations <= 0:
        raise ValueError("num_iterations must be positive")

    for _ in range(_WARMUP_ITERATIONS):
        actor.noop.call().get()

    latencies_ms = []
    for _ in range(num_iterations):
        start = time.perf_counter()
        actor.noop.call().get()
        latencies_ms.append((time.perf_counter() - start) * 1000)

    print(f"warmup iterations: {_WARMUP_ITERATIONS}")
    print(f"measured iterations: {num_iterations}")
    print(f"mean noop RPC latency: {statistics.fmean(latencies_ms):.3f} ms")
    print(f"p50 noop RPC latency: {statistics.median(latencies_ms):.3f} ms")
