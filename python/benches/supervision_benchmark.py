#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import monarch.actor
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch.actor import Actor, endpoint, HostMesh, MeshFailure, ProcMesh, this_proc
from monarch.benches.benchmark_utils import (
    BenchmarkRecord,
    data_actor_mesh_config,
    stop_proc_mesh,
    summarize_samples,
)


FailureKind = Literal["base_exception", "process_death"]
DEFAULT_FAILURE_KINDS: tuple[FailureKind, ...] = ("base_exception", "process_death")


class SupervisionActor(Actor):
    @endpoint
    async def ready(self) -> None:
        return None

    @endpoint
    async def raise_base_exception(self) -> None:
        raise BaseException("intentional benchmark failure")

    @endpoint
    async def exit_process(self) -> None:
        os._exit(1)


def _first_rank(actor: SupervisionActor) -> SupervisionActor:
    return actor.flatten("benchmark_rank").slice(benchmark_rank=0)


class SupervisionOwner(Actor):
    """Own a failing actor mesh and measure delivery to ``__supervise__``."""

    def __init__(
        self,
        proc_mesh: ProcMesh,
        actor_name: str,
        data_actor_mesh: bool,
    ) -> None:
        self._faulted = asyncio.Event()
        self._failure_started_ns: int | None = None
        self._supervision_latency_ms: float | None = None
        with data_actor_mesh_config(data_actor_mesh):
            self._actor: SupervisionActor = proc_mesh.spawn(
                actor_name, SupervisionActor
            )

    @endpoint
    async def ready(self) -> None:
        await self._actor.ready.call()

    @endpoint
    async def fail_one_and_wait(self, kind: FailureKind) -> float:
        target = _first_rank(self._actor)
        self._failure_started_ns = time.perf_counter_ns()
        if kind == "base_exception":
            target.raise_base_exception.broadcast()
        else:
            target.exit_process.broadcast()
        await asyncio.wait_for(self._faulted.wait(), timeout=30.0)
        latency_ms = self._supervision_latency_ms
        if latency_ms is None:
            raise RuntimeError("supervision callback did not record latency")
        return latency_ms

    def __supervise__(self, failure: MeshFailure) -> bool:
        started_ns = self._failure_started_ns
        if started_ns is None:
            return False
        if self._supervision_latency_ms is None:
            self._supervision_latency_ms = (
                time.perf_counter_ns() - started_ns
            ) / 1_000_000
            self._faulted.set()
        return True


@dataclass(frozen=True)
class SupervisionBenchmarkConfig:
    iterations: int = 10
    warmup_iterations: int = 2
    data_actor_mesh: bool = False
    failure_kinds: tuple[FailureKind, ...] = DEFAULT_FAILURE_KINDS

    def validate(self) -> None:
        if self.iterations < 2:
            raise ValueError("iterations must be at least 2")
        if self.warmup_iterations < 0:
            raise ValueError("warmup_iterations must be nonnegative")
        if not self.failure_kinds:
            raise ValueError("failure_kinds must not be empty")


def _record(
    benchmark: str,
    kind: FailureKind,
    samples_ms: Sequence[float],
) -> BenchmarkRecord:
    return {
        "benchmark": benchmark,
        "failure_kind": kind,
        "samples": len(samples_ms),
        "unit": "ms",
        **{
            f"latency_{key}_ms": value
            for key, value in summarize_samples(samples_ms).items()
        },
    }


def _run_error_trial(
    host_mesh: HostMesh,
    kind: FailureKind,
    trial: int,
    data_actor_mesh: bool,
) -> float:
    proc_mesh = host_mesh.spawn_procs(
        per_host={"procs": 1}, name=f"supervision_{kind}_{trial}"
    )
    try:
        proc_mesh.initialized.get()
        with data_actor_mesh_config(data_actor_mesh):
            actor: SupervisionActor = proc_mesh.spawn(
                f"supervision_actor_{kind}_{trial}", SupervisionActor
            )
            actor.initialized.get()
        target = _first_rank(actor)
        target.ready.call_one().get()

        start = time.perf_counter_ns()
        try:
            if kind == "base_exception":
                target.raise_base_exception.call_one().get()
            else:
                target.exit_process.call_one().get()
        except SupervisionError:
            return (time.perf_counter_ns() - start) / 1_000_000
        raise RuntimeError(f"{kind} did not produce a SupervisionError")
    finally:
        stop_proc_mesh(proc_mesh)


def _run_owner_trial(
    host_mesh: HostMesh,
    kind: FailureKind,
    trial: int,
    data_actor_mesh: bool,
) -> float:
    actor_proc_mesh = host_mesh.spawn_procs(
        per_host={"procs": 1}, name=f"supervise_actor_{kind}_{trial}"
    )
    owner: SupervisionOwner | None = None
    try:
        actor_proc_mesh.initialized.get()
        owner = this_proc().spawn(
            f"supervision_owner_{kind}_{trial}",
            SupervisionOwner,
            actor_proc_mesh,
            f"owned_supervision_actor_{kind}_{trial}",
            data_actor_mesh,
        )
        owner.initialized.get()
        owner.ready.call_one().get()
        return owner.fail_one_and_wait.call_one(kind).get()
    finally:
        if owner is not None:
            owner.stop().get()
        stop_proc_mesh(actor_proc_mesh)


def benchmark_supervision(
    host_mesh: HostMesh,
    config: SupervisionBenchmarkConfig | None = None,
) -> list[BenchmarkRecord]:
    """Measure error delivery to a caller and to an owning actor."""
    config = config or SupervisionBenchmarkConfig()
    config.validate()
    original_hook = monarch.actor.unhandled_fault_hook
    monarch.actor.unhandled_fault_hook = lambda failure: None
    try:
        records = []
        for kind in config.failure_kinds:
            error_samples = [
                _run_error_trial(
                    host_mesh,
                    kind,
                    trial,
                    config.data_actor_mesh,
                )
                for trial in range(config.warmup_iterations + config.iterations)
            ]
            owner_samples = [
                _run_owner_trial(
                    host_mesh,
                    kind,
                    trial,
                    config.data_actor_mesh,
                )
                for trial in range(config.warmup_iterations + config.iterations)
            ]
            records.extend(
                (
                    _record(
                        "supervision_error_latency",
                        kind,
                        error_samples[config.warmup_iterations :],
                    ),
                    _record(
                        "owner_supervise_latency",
                        kind,
                        owner_samples[config.warmup_iterations :],
                    ),
                )
            )
        return records
    finally:
        monarch.actor.unhandled_fault_hook = original_hook
