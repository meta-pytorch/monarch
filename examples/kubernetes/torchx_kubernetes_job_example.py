#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Test script for TorchXKubernetesJob

This script tests the Monarch Kubernetes JobTrait implementation by:
1. Launching workers on a Kubernetes cluster using TorchX/Volcano
2. Spawning a simple actor across the workers
3. Running a basic distributed computation
4. Verifying the results

Prerequisites:
- Minikube/Kubernetes cluster with Volcano scheduler installed.
- kubectl configured to access the cluster.
- A pod running a Monarch container to act as the client.
  - You can use the same container referenced in the image argument of the job below.
"""

import asyncio
import sys

from monarch._src.job.torchx_kubernetes import TorchXKubernetesJob
from monarch.actor import Actor, current_rank, current_size, endpoint


class TestActor(Actor):
    """Simple actor for testing distributed execution."""

    def __init__(self):
        self.rank = dict(current_rank())
        self.size = dict(current_size())

    @endpoint
    async def hello(self) -> str:
        """Return a greeting with rank information."""
        return f"Hello from rank {self.rank}, world size: {self.size}"

    @endpoint
    async def compute(self, x: int) -> int:
        """Simple computation: multiply input by rank."""
        result = x * (self.rank.get("hosts", 0) + 1)
        print(f"Rank {self.rank}: {x} * {self.rank.get('hosts', 0) + 1} = {result}")
        return result


async def main():
    print("=" * 80)
    print("Monarch Kubernetes JobTrait Test")
    print("=" * 80)

    # Create a Kubernetes job with 2 workers (pods)
    # Each worker gets 1 CPU and 4GB memory
    print("\n[1/4] Creating TorchXKubernetesJob...")
    job = TorchXKubernetesJob(
        meshes={"workers": 2},
        namespace="monarch-tests",
        queue="monarch",
        image="ghcr.io/meta-pytorch/monarch:2026-01-11-alpha",
        job_name="monarch-test",
        cpu=1,
        memMB=4096,
        gpu=0,
        job_start_timeout=300,
    )

    try:
        # Get the job state (this will submit the job and wait for workers to start)
        print("\n[2/4] Submitting job and waiting for workers to start...")
        print("This may take a few minutes while Kubernetes schedules pods...")
        state = job.state()

        # Access the workers mesh (HostMesh)
        workers_host_mesh = state.workers
        print(f"\n[3/4] Workers ready! HostMesh extent: {workers_host_mesh.extent}")

        # Wait for HostMesh initialization
        print("    Waiting for HostMesh initialization...")
        await workers_host_mesh.initialized
        print("    HostMesh initialized!")

        # First spawn a ProcMesh from the HostMesh, then spawn actors on it
        print("    Spawning ProcMesh from HostMesh...")
        workers_proc_mesh = workers_host_mesh.spawn_procs()
        print(f"    ProcMesh extent: {workers_proc_mesh.extent}")

        # Wait for ProcMesh initialization
        print("    Waiting for ProcMesh initialization...")
        await workers_proc_mesh.initialized
        print("    ProcMesh initialized!")

        # Spawn actors on the ProcMesh
        print("\n[4/4] Spawning test actors and running computations...")
        test_actors = workers_proc_mesh.spawn("test_actor", TestActor)

        # Test 1: Call hello on all actors
        print("\n--- Test 1: Hello from all actors ---")
        greetings = await test_actors.hello.call()
        for rank, greeting in greetings.items():
            print(f"  Rank {rank}: {greeting}")

        # Test 2: Run a simple computation on all actors
        print("\n--- Test 2: Distributed computation ---")
        input_value = 10
        print(f"Input value: {input_value}")
        results = await test_actors.compute.call(input_value)
        print(f"Results from all workers: {results}")
        total = sum(results.values())
        print(f"Sum of all results: {total}")

        # Verify results
        expected_total = sum(input_value * (i + 1) for i in range(2))
        if total == expected_total:
            print("\nSUCCESS! All tests passed.")
            print(f"   Expected total: {expected_total}, Got: {total}")
        else:
            print("\nFAILURE! Results don't match.")
            print(f"   Expected total: {expected_total}, Got: {total}")
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up the job
        print("\n[Cleanup] Killing job...")
        job.kill()
        print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
