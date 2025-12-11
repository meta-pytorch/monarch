#!/usr/bin/env python3
"""
Monarch DDP Example with SkyPilot
=================================

This script demonstrates running PyTorch DDP (DistributedDataParallel) training
on cloud infrastructure provisioned by SkyPilot.

Adapted from the SLURM DDP example (slurm_ddp.ipynb).

Usage:
    python skypilot_ddp.py --num-hosts 2 --gpus-per-host 1
"""

import argparse
import asyncio
import logging
import os
import sys

# Set timeouts before importing monarch
os.environ["HYPERACTOR_HOST_SPAWN_READY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE"] = "300s"

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim

from monarch.actor import Actor, current_rank, endpoint
from monarch.utils import setup_env_for_distributed
from torch.nn.parallel import DistributedDataParallel as DDP

# Import SkyPilotJob from local module
from skypilot_job import SkyPilotJob

try:
    import sky
except ImportError:
    print("ERROR: SkyPilot is not installed. Run: pip install skypilot[kubernetes]")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)


class ToyModel(nn.Module):
    """A simple toy model for demonstration purposes."""

    def __init__(self):
        super(ToyModel, self).__init__()
        self.net1 = nn.Linear(10, 10)
        self.relu = nn.ReLU()
        self.net2 = nn.Linear(10, 5)

    def forward(self, x):
        return self.net2(self.relu(self.net1(x)))


class DDPActor(Actor):
    """This Actor wraps the basic functionality from Torch's DDP example.

    Adapted from: https://docs.pytorch.org/tutorials/intermediate/ddp_tutorial.html#basic-use-case
    """

    def __init__(self):
        self.rank = current_rank().rank

    def _rprint(self, msg):
        """Helper method to print with rank information."""
        print(f"{self.rank=} {msg}")

    @endpoint
    async def setup(self):
        """Initialize the PyTorch distributed process group."""
        self._rprint("Initializing torch distributed")

        WORLD_SIZE = int(os.environ["WORLD_SIZE"])
        # initialize the process group
        dist.init_process_group("gloo", rank=self.rank, world_size=WORLD_SIZE)
        self._rprint("Finished initializing torch distributed")

    @endpoint
    async def cleanup(self):
        """Clean up the PyTorch distributed process group."""
        self._rprint("Cleaning up torch distributed")
        dist.destroy_process_group()

    @endpoint
    async def demo_basic(self):
        """Run a basic DDP training example."""
        self._rprint("Running basic DDP example")

        # create model and move it to GPU with id rank
        local_rank = int(os.environ["LOCAL_RANK"])
        self._rprint(f"{local_rank=}")
        model = ToyModel().to(local_rank)
        ddp_model = DDP(model, device_ids=[local_rank])

        loss_fn = nn.MSELoss()
        optimizer = optim.SGD(ddp_model.parameters(), lr=0.001)

        optimizer.zero_grad()
        outputs = ddp_model(torch.randn(20, 10))
        labels = torch.randn(20, 5).to(local_rank)
        loss_fn(outputs, labels).backward()
        optimizer.step()

        print(f"{self.rank=} Finished running basic DDP example")


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


async def main():
    parser = argparse.ArgumentParser(description="Monarch DDP with SkyPilot")
    parser.add_argument("--cloud", default="kubernetes", help="Cloud provider")
    parser.add_argument("--num-hosts", type=int, default=2, help="Number of hosts")
    parser.add_argument("--gpus-per-host", type=int, default=1, help="GPUs per host")
    parser.add_argument("--cluster-name", default="monarch-ddp", help="Cluster name")
    parser.add_argument("--accelerator", default="H200:1", help="GPU accelerator")
    args = parser.parse_args()

    print("=" * 60)
    print("Monarch DDP Example with SkyPilot")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Cloud: {args.cloud}")
    print(f"  Hosts: {args.num_hosts}")
    print(f"  GPUs per host: {args.gpus_per_host}")
    print(f"  Accelerator: {args.accelerator}")

    # Create SkyPilot job
    job = SkyPilotJob(
        meshes={"mesh0": args.num_hosts},
        resources=sky.Resources(
            cloud=get_cloud(args.cloud),
            accelerators=args.accelerator,
        ),
        cluster_name=args.cluster_name,
        idle_minutes_to_autostop=10,
        down_on_autostop=True,
    )

    try:
        print("\n[1] Launching SkyPilot cluster...")
        job_state = job.state()
        
        print("\n[2] Creating process mesh...")
        proc_mesh = job_state.mesh0.spawn_procs({"gpus": args.gpus_per_host})
        print(f"    Process mesh extent: {proc_mesh.extent}")

        print("\n[3] Spawning DDP actors...")
        ddp_actor = proc_mesh.spawn("ddp_actor", DDPActor)

        print("\n[4] Setting up distributed environment...")
        await setup_env_for_distributed(proc_mesh)

        print("\n[5] Running DDP example...")
        await ddp_actor.setup.call()
        await ddp_actor.demo_basic.call()
        await ddp_actor.cleanup.call()

        print("\n" + "=" * 60)
        print("DDP example completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        print(f"\nNot cleaning up cluster for debugging...")
        print(f"    Debug with: sky ssh {args.cluster_name}")
        print(f"    Clean up: sky down {args.cluster_name}")
        raise
    else:
        print("\n[6] Cleaning up SkyPilot cluster...")
        job.kill()
        print("    Done!")


if __name__ == "__main__":
    asyncio.run(main())

