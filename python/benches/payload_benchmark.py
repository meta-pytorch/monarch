#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import math
import time
from collections.abc import Sequence
from dataclasses import dataclass

from monarch.actor import Actor, endpoint
from monarch.benches.benchmark_utils import BenchmarkRecord, summarize_samples


MIB = 1024 * 1024
DEFAULT_PAYLOAD_SIZES = (0, 1, 16, 256, 4096, 65536, MIB, 4 * MIB, 16 * MIB)


class CastLatencyActor(Actor):
    """Receive casts and report how many have arrived."""

    def __init__(self) -> None:
        self._received = 0

    @endpoint
    async def sink(self, payload: bytes) -> None:
        self._received += 1

    @endpoint
    async def barrier(self) -> int:
        return self._received


class E2ELatencyActor(Actor):
    """Acknowledge payload receipt for end-to-end latency measurements."""

    @endpoint
    async def accept(self, payload: bytes) -> int:
        return len(payload)


class BandwidthActor(Actor):
    """Measure elapsed time across a stream of received payloads."""

    def __init__(self) -> None:
        self._received = 0
        self._bandwidth_remaining = 0
        self._bandwidth_started_ns: int | None = None
        self._bandwidth_elapsed_ns: int | None = None

    @endpoint
    async def sink(self, payload: bytes) -> None:
        if self._bandwidth_remaining > 0 and self._bandwidth_started_ns is None:
            self._bandwidth_started_ns = time.perf_counter_ns()
        self._received += 1
        if self._bandwidth_remaining > 0:
            self._bandwidth_remaining -= 1
            if self._bandwidth_remaining == 0:
                started_ns = self._bandwidth_started_ns
                if started_ns is None:
                    raise RuntimeError("bandwidth trial did not start")
                self._bandwidth_elapsed_ns = time.perf_counter_ns() - started_ns

    @endpoint
    async def begin_bandwidth_trial(self, iterations: int) -> int:
        if iterations < 2:
            raise ValueError("bandwidth trial requires at least two messages")
        if self._bandwidth_remaining != 0:
            raise RuntimeError("bandwidth trial is already running")
        self._bandwidth_remaining = iterations
        self._bandwidth_started_ns = None
        self._bandwidth_elapsed_ns = None
        return self._received

    @endpoint
    async def finish_bandwidth_trial(self) -> tuple[int, int]:
        if self._bandwidth_remaining != 0 or self._bandwidth_elapsed_ns is None:
            raise RuntimeError("bandwidth trial did not receive every payload")
        elapsed_ns = self._bandwidth_elapsed_ns
        self._bandwidth_started_ns = None
        self._bandwidth_elapsed_ns = None
        return self._received, elapsed_ns


@dataclass(frozen=True)
class PayloadBenchmarkConfig:
    """Iteration and byte-volume settings for payload benchmarks."""

    max_latency_iterations: int = 200
    warmup_iterations: int = 20
    latency_target_bytes: int = 256 * MIB
    bandwidth_target_bytes: int = 64 * MIB
    bandwidth_trials: int = 5
    max_bandwidth_iterations: int = 10_000

    def validate(self) -> None:
        if self.max_latency_iterations < 2:
            raise ValueError("max_latency_iterations must be at least 2")
        if self.warmup_iterations < 0:
            raise ValueError("warmup_iterations must be nonnegative")
        if self.latency_target_bytes <= 0 or self.bandwidth_target_bytes <= 0:
            raise ValueError("target byte counts must be positive")
        if self.bandwidth_trials < 2:
            raise ValueError("bandwidth_trials must be at least 2")
        if self.max_bandwidth_iterations <= 0:
            raise ValueError("max_bandwidth_iterations must be positive")


def _iteration_count(limit: int, target_bytes: int, payload_size: int) -> int:
    return max(2, min(limit, math.ceil(target_bytes / max(payload_size, 1))))


def _validate_inputs(
    payload_sizes: Sequence[int],
    config: PayloadBenchmarkConfig | None,
) -> PayloadBenchmarkConfig:
    config = config or PayloadBenchmarkConfig()
    config.validate()
    if not payload_sizes or any(size < 0 for size in payload_sizes):
        raise ValueError("payload_sizes must contain nonnegative sizes")
    return config


def _latency_record(
    name: str, payload_size: int, samples_us: Sequence[float]
) -> BenchmarkRecord:
    return {
        "benchmark": name,
        "payload_bytes": payload_size,
        "samples": len(samples_us),
        "unit": "us",
        **{
            f"{name}_{key}_us": value
            for key, value in summarize_samples(samples_us).items()
        },
    }


def _measure_cast_submit(
    actor: CastLatencyActor, payload: bytes, iterations: int
) -> list[float]:
    previous = actor.barrier.call_one().get()
    samples = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        actor.sink.broadcast(payload)
        samples.append((time.perf_counter_ns() - start) / 1000)
    observed = actor.barrier.call_one().get()
    if observed != previous + iterations:
        raise RuntimeError(f"expected {previous + iterations} messages, got {observed}")
    return samples


def _measure_e2e(
    actor: E2ELatencyActor, payload: bytes, iterations: int
) -> list[float]:
    samples = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        received = actor.accept.call_one(payload).get()
        samples.append((time.perf_counter_ns() - start) / 1000)
        if received != len(payload):
            raise RuntimeError(f"expected {len(payload)} bytes, received {received}")
    return samples


def _measure_bandwidth(
    actor: BandwidthActor,
    payload: bytes,
    iterations: int,
    trials: int,
) -> BenchmarkRecord:
    bytes_per_second = []
    messages_per_second = []
    for _ in range(trials):
        previous = actor.begin_bandwidth_trial.call_one(iterations).get()
        for _ in range(iterations):
            actor.sink.broadcast(payload)
        observed, elapsed_ns = actor.finish_bandwidth_trial.call_one().get()
        if observed != previous + iterations:
            raise RuntimeError(
                f"expected {previous + iterations} messages, got {observed}"
            )
        if elapsed_ns <= 0:
            raise RuntimeError(f"invalid bandwidth duration: {elapsed_ns} ns")
        elapsed = elapsed_ns / 1_000_000_000
        measured_messages = iterations - 1
        bytes_per_second.append(len(payload) * measured_messages / elapsed)
        messages_per_second.append(measured_messages / elapsed)
    return {
        "benchmark": "point_to_point_bandwidth",
        "payload_bytes": len(payload),
        "iterations_per_trial": iterations,
        "samples": trials,
        "unit": "bytes_per_second",
        **{
            f"bandwidth_{key}_bytes_per_second": value
            for key, value in summarize_samples(bytes_per_second).items()
        },
        "messages_p50_per_second": summarize_samples(messages_per_second)["p50"],
    }


def benchmark_cast_submit_latency(
    actor: CastLatencyActor,
    payload_sizes: Sequence[int] = DEFAULT_PAYLOAD_SIZES,
    config: PayloadBenchmarkConfig | None = None,
) -> list[BenchmarkRecord]:
    """Benchmark point-to-point cast submission latency."""
    config = _validate_inputs(payload_sizes, config)
    records = []
    for payload_size in payload_sizes:
        payload = bytes(payload_size)
        iterations = _iteration_count(
            config.max_latency_iterations,
            config.latency_target_bytes,
            payload_size,
        )
        records.append(
            _latency_record(
                "cast_submit_latency",
                payload_size,
                _measure_cast_submit(actor, payload, iterations),
            )
        )
    return records


def benchmark_e2e_latency(
    actor: E2ELatencyActor,
    payload_sizes: Sequence[int] = DEFAULT_PAYLOAD_SIZES,
    config: PayloadBenchmarkConfig | None = None,
) -> list[BenchmarkRecord]:
    """Benchmark point-to-point request and response latency."""
    config = _validate_inputs(payload_sizes, config)
    records = []
    for payload_size in payload_sizes:
        payload = bytes(payload_size)
        iterations = _iteration_count(
            config.max_latency_iterations,
            config.latency_target_bytes,
            payload_size,
        )
        warmups = min(
            config.warmup_iterations,
            math.ceil(16 * MIB / max(payload_size, 1)),
        )
        for _ in range(warmups):
            actor.accept.call_one(payload).get()

        records.append(
            _latency_record(
                "e2e_latency",
                payload_size,
                _measure_e2e(actor, payload, iterations),
            )
        )
    return records


def benchmark_bandwidth(
    actor: BandwidthActor,
    payload_sizes: Sequence[int] = DEFAULT_PAYLOAD_SIZES,
    config: PayloadBenchmarkConfig | None = None,
) -> list[BenchmarkRecord]:
    """Benchmark point-to-point cast bandwidth."""
    config = _validate_inputs(payload_sizes, config)
    records = []
    for payload_size in payload_sizes:
        payload = bytes(payload_size)
        bandwidth_iterations = _iteration_count(
            config.max_bandwidth_iterations,
            config.bandwidth_target_bytes,
            payload_size,
        )
        records.append(
            _measure_bandwidth(
                actor,
                payload,
                bandwidth_iterations,
                config.bandwidth_trials,
            )
        )
    return records
