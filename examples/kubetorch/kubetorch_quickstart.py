#!/usr/bin/env python3
"""
Kubetorch Monarch Quickstart Example

This script demonstrates running Monarch actors on Kubernetes using Kubetorch.
Run from your local machine (outside the cluster) with Kubetorch installed.

Usage:
    python kubetorch_quickstart.py [--num-hosts 2] [--gpus-per-host 1] [--cpu-only]

Requirements:
    - Kubernetes cluster with Kubetorch installed
    - kubectl configured to access the cluster
    - pip install kubetorch
"""

import argparse
import os


def main():
    parser = argparse.ArgumentParser(description="Kubetorch Monarch Quickstart")
    parser.add_argument("--num-hosts", type=int, default=2, help="Number of worker hosts")
    parser.add_argument("--gpus-per-host", type=int, default=1, help="GPUs per host (0 for CPU-only)")
    parser.add_argument("--cpu-only", action="store_true", help="Run in CPU-only mode")
    parser.add_argument("--namespace", type=str, default="default", help="Kubernetes namespace")
    args = parser.parse_args()

    gpus_per_host = 0 if args.cpu_only else args.gpus_per_host

    print("=" * 60)
    print("Monarch Getting Started with Kubetorch")
    print("=" * 60)
    print()
    print("Configuration:")
    print(f"  Hosts: {args.num_hosts}")
    print(f"  GPUs per host: {gpus_per_host}")
    print(f"  Namespace: {args.namespace}")
    print()

    # Import after parsing args to fail fast on missing args
    import kubetorch as kt
    from kubetorch.monarch import KubernetesJob
    from monarch.actor import Actor, endpoint

    # Define actor classes
    class Counter(Actor):
        def __init__(self):
            self.count = 0

        @endpoint
        def increment(self, amount: int = 1) -> int:
            self.count += amount
            return self.count

        @endpoint
        def get_value(self) -> int:
            return self.count

    class Trainer(Actor):
        @endpoint
        def step(self) -> str:
            return f"Trainer {self.rank} taking a step."

        @endpoint
        def info(self) -> str:
            return f"Trainer at rank {self.rank}"

    # Create compute specification
    print("[1] Creating compute specification...")
    if gpus_per_host > 0:
        compute = kt.Compute(
            gpu=gpus_per_host,
            replicas=args.num_hosts,
            namespace=args.namespace,
        )
    else:
        compute = kt.Compute(
            cpu="1",
            replicas=args.num_hosts,
            namespace=args.namespace,
        )

    # Create job
    print("[2] Creating KubernetesJob and launching workers...")
    job = KubernetesJob(compute=compute)

    try:
        # Get job state
        state = job.state()
        host_mesh = state.workers
        print(f"    Got host mesh with extent: {host_mesh.sizes}")
        print()

        # Spawn processes
        print("[3] Spawning processes on hosts...")
        per_host = {"gpus": gpus_per_host} if gpus_per_host > 0 else {"procs": 1}
        proc_mesh = host_mesh.spawn_procs(per_host=per_host)
        print(f"    Process mesh extent: {proc_mesh.sizes}")
        print()

        # Spawn Counter actors
        print("[4] Spawning Counter actors...")
        counters = proc_mesh.spawn("counters", Counter)
        print()

        # Increment counters
        print("[5] Broadcasting increment to all counters...")
        counters.increment.broadcast(amount=3)
        print()

        # Get counter values
        print("[6] Getting counter values...")
        values = counters.get_value.call().get()
        print(f"    Counter values: {values}")
        print()

        # Spawn Trainer actors
        print("[7] Spawning Trainer actors...")
        trainers = proc_mesh.spawn("trainers", Trainer)
        print()

        # Perform training step
        print("[8] Performing distributed training step...")
        step_results = trainers.step.call().get()
        for item in step_results.items():
            print(f"    {item}")
        print()

        # Get trainer info
        print("[9] Getting trainer info...")
        info_results = trainers.info.call().get()
        for item in info_results.items():
            print(f"    {item}")
        print()

        print("=" * 60)
        print("Success! Monarch actors ran on Kubetorch!")
        print("=" * 60)
        print()

    finally:
        print("[10] Cleaning up...")
        job.kill()
        print("    Job terminated.")


if __name__ == "__main__":
    main()
