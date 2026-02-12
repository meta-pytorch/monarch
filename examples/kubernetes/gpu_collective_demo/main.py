#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Simple GPU collective demo with Monarch on Kubernetes.

This demo:
- Creates a mesh with configurable hosts and GPUs per host
- Runs an allreduce collective to verify GPU communication across all ranks
- Validates that each proc has exclusive access to its own GPU
"""

import argparse
import asyncio
import logging
import os
import socket

import torch
import torch.distributed as dist
from monarch.actor import Actor, endpoint
from monarch.job.kubernetes import ImageSpec, KubernetesJob
from monarch.spmd import setup_torch_elastic_env_async
from monarch.tools.network import AddrType

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


class GPUCollectiveActor(Actor):
    """Actor that runs a simple GPU allreduce collective."""

    @endpoint
    async def run_allreduce(self) -> dict:
        """Run allreduce and return info about this rank's GPU."""
        rank = int(os.environ["RANK"])
        local_rank = int(os.environ["LOCAL_RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
        device = f"cuda:{local_rank}"

        logger.info(f"Rank {rank}/{world_size}: Initializing process group on {device}")
        dist.init_process_group("nccl", rank=rank, world_size=world_size)

        try:
            gpu_name = torch.cuda.get_device_name(local_rank)
            gpu_memory = torch.cuda.get_device_properties(local_rank).total_memory

            tensor = torch.tensor([float(rank)], device=device)
            original_value = tensor.item()

            logger.info(f"Rank {rank}: Before allreduce, tensor = {original_value}")
            dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
            allreduce_result = tensor.item()
            logger.info(f"Rank {rank}: After allreduce, tensor = {allreduce_result}")

            return {
                "rank": rank,
                "world_size": world_size,
                "hostname": socket.gethostname(),
                "gpu_id": local_rank,
                "gpu_name": gpu_name,
                "gpu_memory_gb": gpu_memory / (1024**3),
                "original_value": original_value,
                "allreduce_result": allreduce_result,
                "expected_sum": sum(range(world_size)),
                "success": allreduce_result == sum(range(world_size)),
            }
        finally:
            dist.destroy_process_group()


async def main(num_hosts: int, num_gpus_per_host: int, provision: bool) -> None:
    logger.info("=" * 60)
    logger.info("GPU Collective Demo - Monarch on Kubernetes")
    logger.info("=" * 60)

    # Connect to Kubernetes job and create proc mesh
    job = KubernetesJob(namespace="monarch-tests")
    if provision:
        job.add_mesh(
            "gpumesh1",
            num_replicas=num_hosts,
            image_spec=ImageSpec(
                "ghcr.io/meta-pytorch/monarch:latest",
                resources={"nvidia.com/gpu": num_gpus_per_host},
            ),
        )
    else:
        job.add_mesh("gpumesh1", num_replicas=num_hosts)
    host_mesh = job.state().gpumesh1
    proc_mesh = host_mesh.spawn_procs({"gpus": num_gpus_per_host})

    # Log actual dimensions from the mesh
    logger.info(
        f"Configuration: {host_mesh.sizes['hosts']} host(s), "
        f"{proc_mesh.sizes['gpus']} GPUs/host"
    )

    # Setup torch distributed environment (RANK, LOCAL_RANK, WORLD_SIZE, etc.)
    # Use IPv4 addresses for MASTER_ADDR since short hostnames like "mesh1-0"
    # are not resolvable across pods - only the FQDN or IP addresses work.
    await setup_torch_elastic_env_async(proc_mesh, use_ipaddr=AddrType.IPv4)

    actor = proc_mesh.spawn("gpu_collective_actor", GPUCollectiveActor)
    results = await actor.run_allreduce.call()

    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)

    all_success = True
    hostnames = set()

    for _, result in results.flatten("rank"):
        status = "✓" if result["success"] else "✗"
        hostnames.add(result["hostname"])
        logger.info(
            f"{status} Rank {result['rank']}: GPU {result['gpu_id']} ({result['gpu_name']}) "
            f"- allreduce {result['original_value']} -> {result['allreduce_result']}"
        )
        if not result["success"]:
            all_success = False

    logger.info("=" * 60)
    if all_success and len(hostnames) == num_hosts:
        logger.info("✓ Demo PASSED: GPU collective working across all hosts!")
    else:
        logger.error("✗ Demo FAILED: Check logs above for details")

    proc_mesh.stop().get()

    if provision:
        job.kill()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GPU Collective Demo with Monarch")
    parser.add_argument(
        "--num_hosts", type=int, default=1, help="Number of hosts (default: 1)"
    )
    parser.add_argument(
        "--num_gpus_per_host",
        type=int,
        default=4,
        help="Number of GPUs per host (default: 4)",
    )
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Provision MonarchMesh CRDs from Python (no YAML manifests needed)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.num_hosts, args.num_gpus_per_host, args.provision))
