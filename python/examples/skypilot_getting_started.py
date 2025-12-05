#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Monarch Getting Started with SkyPilot
=====================================

This script demonstrates running Monarch actors on cloud infrastructure
provisioned by SkyPilot. It follows the Monarch getting started guide
but uses SkyPilot to launch the worker nodes.

Prerequisites:
- Monarch installed with its Rust bindings (build with `pip install -e .` in monarch/)
- SkyPilot installed and configured (run `sky check`)
- torchmonarch available on PyPI (requires CUDA on remote nodes)

Usage:
    python skypilot_getting_started.py

    # With explicit options:
    python skypilot_getting_started.py --cloud kubernetes --num-hosts 2

See SKY_README.md for full documentation.
"""

import argparse
import os
import sys

# Set timeouts before importing monarch - worker setup takes time
os.environ["HYPERACTOR_HOST_SPAWN_READY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE"] = "300s"

# Check dependencies before importing
try:
    import sky
except ImportError:
    print("ERROR: SkyPilot is not installed. Run: pip install skypilot")
    sys.exit(1)

try:
    from monarch.job import SkyPilotJob
    from monarch.actor import Actor, endpoint, ProcMesh, context
except ImportError as e:
    print(f"ERROR: Monarch is not properly installed: {e}")
    print("\nTo install Monarch, you need to build it from source:")
    print("  cd monarch/")
    print("  pip install -e .")
    print("\nThis requires the Rust toolchain and other dependencies.")
    print("See monarch/README.md for full installation instructions.")
    sys.exit(1)

# ============================================================================
# Step 1: Define our Actors (same as getting started guide)
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
    }
    if cloud_name.lower() not in clouds:
        raise ValueError(f"Unknown cloud: {cloud_name}. Available: {list(clouds.keys())}")
    return clouds[cloud_name.lower()]()


def main():
    parser = argparse.ArgumentParser(description="Monarch Getting Started with SkyPilot")
    parser.add_argument(
        "--cloud",
        default="kubernetes",
        help="Cloud provider to use (kubernetes, aws, gcp, azure)",
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
        default=2,
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
    # This will launch cloud instances and start Monarch workers on them
    print("\n[1] Creating SkyPilot job...")

    # Setup commands to install Monarch on the remote nodes
    # Build from source to ensure client/worker version compatibility
    # NOTE: Currently builds WITHOUT tensor engine due to old rdma-core on Ubuntu 20.04
    setup_commands = """
set -ex

# Add PPA for newer toolchains
sudo apt-get update
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get update

# Install system dependencies
sudo apt-get install -y \
  build-essential \
  ninja-build \
  g++-11 \
  rdma-core \
  libibverbs1 \
  libmlx5-1 \
  libibverbs-dev \
  curl \
  pkg-config \
  libssl-dev

# Install CUDA toolkit and NCCL
wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt-get update
sudo apt-get install -y cuda-toolkit-12-1
sudo apt-get install -y --allow-change-held-packages libnccl2=2.28.9-1+cuda12.9 libnccl-dev=2.28.9-1+cuda12.9

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
rustup default nightly

# Install Python dependencies
cd ~/sky_workdir
pip install setuptools-rust maturin
pip install -r torch-requirements.txt -r build-requirements.txt

# Build Monarch (without tensor engine due to old rdma-core)
CC=gcc-11 CXX=g++-11 USE_TENSOR_ENGINE=0 pip install --no-build-isolation .

echo "DONE INSTALLING MONARCH"
"""

    # Build resources specification
    resources_kwargs = {
        "cloud": get_cloud(args.cloud),
        "cpus": "2+",
        "accelerators": args.accelerator,  # GPU required - torchmonarch needs CUDA
    }
    if args.region:
        resources_kwargs["region"] = args.region

    job = SkyPilotJob(
        # Define the mesh of hosts we need
        meshes={"trainers": args.num_hosts},
        # Specify cloud resources - GPU required for torchmonarch (needs CUDA)
        resources=sky.Resources(**resources_kwargs),
        cluster_name=args.cluster_name,
        # Auto-cleanup after 10 minutes of idle time
        idle_minutes_to_autostop=10,
        down_on_autostop=True,
        # Setup commands to install dependencies
        setup_commands=setup_commands,
        # Sync Monarch source to workers for building
        workdir="/home/sky/dev/monarch",
        # Use default python (same as used by pip in setup)
        python_exe="python",
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
        print("SUCCESS! Monarch actors ran on SkyPilot cluster!")
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

