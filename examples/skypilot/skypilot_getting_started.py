#!/usr/bin/env python3
"""
Running Monarch on Kubernetes with SkyPilot
===========================================

This script demonstrates running Monarch actors on cloud infrastructure
provisioned by SkyPilot (Kubernetes or cloud VMs).

Prerequisites:
    pip install torchmonarch-nightly
    pip install skypilot[kubernetes]  # or skypilot[aws], skypilot[gcp], etc.
    sky check  # Verify SkyPilot configuration

Usage:
    # Run on Kubernetes:
    python getting_started.py --cloud kubernetes --num-hosts 2

    # Run on AWS:
    python getting_started.py --cloud aws --num-hosts 2

    # Run on GCP:
    python getting_started.py --cloud gcp --num-hosts 2
"""

import argparse
import os
import sys

# Set timeouts before importing monarch
os.environ["HYPERACTOR_HOST_SPAWN_READY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE"] = "300s"

# If running inside a SkyPilot cluster, unset the in-cluster context
# to allow launching new clusters on the same Kubernetes cluster
if "SKYPILOT_IN_CLUSTER_CONTEXT_NAME" in os.environ:
    del os.environ["SKYPILOT_IN_CLUSTER_CONTEXT_NAME"]

# Check dependencies before importing
try:
    import sky
except ImportError:
    print("ERROR: SkyPilot is not installed. Run: pip install skypilot[kubernetes]")
    sys.exit(1)

try:
    from monarch.actor import Actor, endpoint, ProcMesh, context
except ImportError as e:
    print(f"ERROR: Monarch is not properly installed: {e}")
    print("Run: pip install torchmonarch-nightly")
    sys.exit(1)

# Import SkyPilotJob from the local package
from skypilot_job import SkyPilotJob

# ============================================================================
# Step 1: Define actors (same as getting started guide)
# ============================================================================


class Counter(Actor):
    """A simple counter actor that demonstrates basic messaging."""

    def __init__(self, initial_value: int = 0):
        self.value = initial_value

    @endpoint
    def increment(self) -> None:
        self.value += 1

    @endpoint
    def get_value(self) -> int:
        return self.value


class Trainer(Actor):
    """A trainer actor that demonstrates distributed training patterns."""

    @endpoint
    def step(self) -> str:
        my_point = context().message_rank
        return f"Trainer {my_point} taking a step."

    @endpoint
    def get_info(self) -> str:
        rank = context().actor_instance.rank
        return f"Trainer at rank {rank}"


# ============================================================================
# Step 2: Create a SkyPilot Job to provision cloud infrastructure
# ============================================================================


def get_cloud(cloud_name: str):
    """Get SkyPilot cloud object from name."""
    clouds = {
        "kubernetes": sky.Kubernetes,
        "aws": sky.AWS,
        "gcp": sky.GCP,
        "azure": sky.Azure,
        "lambda": sky.Lambda,
    }
    if cloud_name.lower() not in clouds:
        raise ValueError(f"Unknown cloud: {cloud_name}. Available: {list(clouds.keys())}")
    return clouds[cloud_name.lower()]()


def main():
    parser = argparse.ArgumentParser(description="Monarch Getting Started with SkyPilot")
    parser.add_argument(
        "--cloud",
        default="kubernetes",
        help="Cloud provider to use (kubernetes, aws, gcp, azure, lambda)",
    )
    parser.add_argument(
        "--num-hosts",
        type=int,
        default=2,
        help="Number of host nodes to provision",
    )
    parser.add_argument(
        "--gpus-per-host",
        type=int,
        default=1,
        help="Number of GPU processes per host",
    )
    parser.add_argument(
        "--cluster-name",
        default="monarch-getting-started",
        help="Name for the SkyPilot cluster",
    )
    parser.add_argument(
        "--accelerator",
        default="H200:1",
        help="GPU accelerator to request (e.g., H100:1, A100:1, V100:1)",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Cloud region/Kubernetes context to use",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Monarch Getting Started with SkyPilot")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Cloud: {args.cloud}")
    print(f"  Hosts: {args.num_hosts}")
    print(f"  GPUs per host: {args.gpus_per_host}")
    print(f"  Accelerator: {args.accelerator}")
    print(f"  Cluster name: {args.cluster_name}")
    if args.region:
        print(f"  Region: {args.region}")

    # Create a SkyPilotJob to provision nodes
    print("\n[1] Creating SkyPilot job...")

    # Build resources specification
    resources_kwargs = {
        "cloud": get_cloud(args.cloud),
        "accelerators": args.accelerator,
    }
    if args.region:
        resources_kwargs["region"] = args.region
    
    job = SkyPilotJob(
        # Define the mesh of hosts we need
        meshes={"trainers": args.num_hosts},
        resources=sky.Resources(**resources_kwargs),
        cluster_name=args.cluster_name,
        # Auto-cleanup after 10 minutes of idle time
        idle_minutes_to_autostop=10,
        down_on_autostop=True,
    )

    try:
        # Get the job state - this launches the cluster and returns HostMeshes
        print("\n[2] Launching cluster and starting Monarch workers...")
        state = job.state()

        # Get our host mesh
        hosts = state.trainers
        print(f"    Got host mesh with extent: {hosts.extent}")

        # ====================================================================
        # Step 3: Spawn processes and actors on the cloud hosts
        # ====================================================================

        print("\n[3] Spawning processes on cloud hosts...")
        # Create a process mesh - GPU processes per host
        procs: ProcMesh = hosts.spawn_procs(per_host={"gpus": args.gpus_per_host})
        print(f"    Process mesh extent: {procs.extent}")

        # Spawn counter actors
        print("\n[4] Spawning Counter actors...")
        counters: Counter = procs.spawn("counters", Counter, initial_value=0)

        # ====================================================================
        # Step 4: Interact with the actors
        # ====================================================================

        # Broadcast increment to all counters
        print("\n[5] Broadcasting increment to all counters...")
        counters.increment.broadcast()
        counters.increment.broadcast()
        counters.increment.broadcast()

        # Get all counter values
        print("\n[6] Getting counter values...")
        values = counters.get_value.call().get()
        print(f"    Counter values: {values}")

        # Spawn trainer actors
        print("\n[7] Spawning Trainer actors...")
        trainers: Trainer = procs.spawn("trainers", Trainer)

        # Do a training step
        print("\n[8] Performing distributed training step...")
        results = trainers.step.call().get()
        for r in results:
            print(f"    {r}")

        # Get trainer info
        print("\n[9] Getting trainer info...")
        info = trainers.get_info.call().get()
        for i in info:
            print(f"    {i}")

        print("\n" + "=" * 60)
        print("Success! Monarch actors ran on SkyPilot cluster!")
        print("=" * 60)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        print(f"\n[10] ERROR - not cleaning up cluster for debugging...")
        print(f"    You can debug with: sky ssh {args.cluster_name}")
        print(f"    To clean up later: sky down {args.cluster_name}")
        raise
    else:
        # Clean up - tear down the SkyPilot cluster
        print("\n[10] Cleaning up SkyPilot cluster...")
        job.kill()
        print("    Cluster terminated.")


if __name__ == "__main__":
    main()

