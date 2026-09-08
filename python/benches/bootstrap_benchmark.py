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
from monarch.job import JobTrait


class BootstrapNoopActor(Actor):
    @endpoint
    async def noop(self) -> None:
        return None


def benchmark_bootstrap(
    job: JobTrait,
    num_iterations: int,
    procs_per_host: int = 8,
    *,
    host_mesh_name: str = "hosts",
    warmup_iterations: int = 20,
) -> None:
    """Measure bootstrap on a job's available host mesh through the first RPC."""
    if num_iterations < 2:
        raise ValueError("num_iterations must be at least 2")
    if procs_per_host <= 0:
        raise ValueError("procs_per_host must be positive")
    if warmup_iterations < 0:
        raise ValueError("warmup_iterations must be nonnegative")

    host_mesh = getattr(job.state(cached_path=None), host_mesh_name)
    host_mesh.initialized.get()

    proc_mesh_spawn_times: list[float] = []
    actor_mesh_spawn_times: list[float] = []
    first_rpc_times: list[float] = []
    total_bootstrap_times: list[float] = []

    for iteration in range(warmup_iterations + num_iterations):
        start = time.perf_counter()
        proc_mesh = host_mesh.spawn_procs(per_host={"procs": procs_per_host})
        try:
            proc_mesh.initialized.get()
            proc_mesh_ready = time.perf_counter()

            actor: BootstrapNoopActor = proc_mesh.spawn(
                "bootstrap_benchmark", BootstrapNoopActor
            )
            actor.initialized.get()
            actor_mesh_ready = time.perf_counter()

            actor.noop.call().get()
            first_rpc_complete = time.perf_counter()

            if iteration >= warmup_iterations:
                proc_mesh_spawn_times.append(proc_mesh_ready - start)
                actor_mesh_spawn_times.append(actor_mesh_ready - proc_mesh_ready)
                first_rpc_times.append(first_rpc_complete - actor_mesh_ready)
                total_bootstrap_times.append(first_rpc_complete - start)
        finally:
            proc_mesh.stop().get()

    print(f"warmup iterations: {warmup_iterations}")
    print(f"measured iterations: {num_iterations}")
    for name, timings in (
        ("proc mesh spawn", proc_mesh_spawn_times),
        ("actor mesh spawn", actor_mesh_spawn_times),
        ("first noop RPC", first_rpc_times),
        ("total bootstrap", total_bootstrap_times),
    ):
        percentiles = statistics.quantiles(
            timings,
            n=100,
            method="inclusive",
        )
        print(f"{name}: p50 {percentiles[49]:.3f} s, p99 {percentiles[98]:.3f} s")
