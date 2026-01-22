# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Kubernetes DDP Example

Demonstrates running PyTorch Distributed Data Parallel (DDP) training
on Kubernetes using Monarch's KubernetesJob and MonarchMesh CRD and operator.

Usage:
    python kubernetes_ddp.py --num_hosts 2 --gpus_per_host 4
"""

# pyre-ignore-all-errors

import argparse
import asyncio
import logging
import os

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim
from monarch.actor import Actor, current_rank, endpoint
from monarch.job.kubernetes import KubernetesJob
from monarch.spmd import setup_torch_elastic_env_async
from monarch.tools.network import AddrType
from torch.nn.parallel import DistributedDataParallel as DDP


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)

logger: logging.Logger = logging.getLogger(__name__)


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
    """Actor that wraps PyTorch DDP functionality.

    Adapted from: https://docs.pytorch.org/tutorials/intermediate/ddp_tutorial.html
    """

    def __init__(self):
        self.rank = current_rank().rank

    def _rprint(self, msg):
        """Helper method to print with rank information."""
        print(f"{self.rank=} {msg}", flush=True)

    @endpoint
    async def setup(self):
        """Initialize the PyTorch distributed process group."""
        self._rprint("Initializing torch distributed")

        world_size = int(os.environ["WORLD_SIZE"])
        rank = int(os.environ["RANK"])
        dist.init_process_group("nccl", rank=rank, world_size=world_size)
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

        local_rank = int(os.environ["LOCAL_RANK"])
        self._rprint(f"{local_rank=}")

        # Create model and move to GPU
        model = ToyModel().to(local_rank)
        ddp_model = DDP(model, device_ids=[local_rank])

        loss_fn = nn.MSELoss()
        optimizer = optim.SGD(ddp_model.parameters(), lr=0.001)

        # Forward pass
        optimizer.zero_grad()
        outputs = ddp_model(torch.randn(20, 10))
        labels = torch.randn(20, 5).to(local_rank)

        # Backward pass
        loss_fn(outputs, labels).backward()
        optimizer.step()

        print(f"{self.rank=} Finished running basic DDP example", flush=True)


async def main(num_hosts: int, gpus_per_host: int, mesh_name: str) -> None:
    logger.info("=" * 60)
    logger.info("Kubernetes DDP Example")
    logger.info(f"Configuration: {num_hosts} hosts, {gpus_per_host} GPUs/host")
    logger.info("=" * 60)

    # Create Kubernetes job connecting to pre-provisioned pods
    k8s_job = KubernetesJob(namespace="monarch-tests")

    # Add mesh configuration - uses MonarchMesh CRD labels by default
    k8s_job.add_mesh(mesh_name, num_replicas=num_hosts)

    try:
        # Get job state and create process mesh
        job_state = k8s_job.state()
        host_mesh = getattr(job_state, mesh_name)
        proc_mesh = host_mesh.spawn_procs({"gpus": gpus_per_host})

        # Setup distributed environment (RANK, LOCAL_RANK, WORLD_SIZE, MASTER_ADDR, etc.)
        # Use IPv4 addresses since short hostnames may not resolve across pods
        await setup_torch_elastic_env_async(proc_mesh, use_ipaddr=AddrType.IPv4)

        # Spawn DDP actor
        ddp_actor = proc_mesh.spawn("ddp_actor", DDPActor)

        # Run DDP example
        await ddp_actor.setup.call()
        await ddp_actor.demo_basic.call()
        await ddp_actor.cleanup.call()

        logger.info("=" * 60)
        logger.info("DDP example completed successfully!")
        logger.info("=" * 60)

        # Stop the proc mesh
        proc_mesh.stop().get()

    except Exception as e:
        logger.error(f"DDP example failed: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kubernetes DDP Example")
    parser.add_argument(
        "--num_hosts",
        type=int,
        default=2,
        help="Number of hosts (must match MonarchMesh replicas)",
    )
    parser.add_argument(
        "--gpus_per_host",
        type=int,
        default=4,
        help="Number of GPUs per host (must match GPU resources in MonarchMesh Pod Template)",
    )
    parser.add_argument(
        "--mesh_name",
        type=str,
        default="ddpmesh",
        help="Name of the MonarchMesh (must match MonarchMesh name)",
    )
    args = parser.parse_args()

    asyncio.run(main(args.num_hosts, args.gpus_per_host, args.mesh_name))
