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


class NoopActor(Actor):
    @endpoint
    async def noop(self) -> None:
        return None


def benchmark_noop_rpc(
    actor: NoopActor,
    num_iterations: int,
    *,
    warmup_iterations: int = 20,
) -> None:
    """Measure the latency of the requested number of noop calls."""
    if num_iterations < 2:
        raise ValueError("num_iterations must be at least 2")
    if warmup_iterations < 0:
        raise ValueError("warmup_iterations must be nonnegative")

    for _ in range(warmup_iterations):
        actor.noop.call().get()

    latencies_ms: list[float] = []
    for _ in range(num_iterations):
        start = time.perf_counter()
        actor.noop.call().get()
        latencies_ms.append((time.perf_counter() - start) * 1000)

    print(f"warmup iterations: {warmup_iterations}")
    print(f"measured iterations: {num_iterations}")
    percentiles = statistics.quantiles(
        latencies_ms,
        n=100,
        method="inclusive",
    )
    print(f"p50 noop RPC latency: {percentiles[49]:.3f} ms")
    print(f"p99 noop RPC latency: {percentiles[98]:.3f} ms")
