# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Utility functions for running Monarch examples on SLURM with v1 API.

Provides helper functions for creating and managing SLURM jobs:
- create_slurm_job(): Create a new SLURM job with mesh configuration
- cleanup_job(): Terminate and clean up SLURM jobs
"""

import logging

from monarch.job import JobTrait, SlurmJob


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger: logging.Logger = logging.getLogger(__name__)


def create_slurm_job(
    mesh_name: str,
    num_nodes: int,
    gpus_per_node: int,
    time_limit: str = "06:00:00",
    python_exe: str = None,
) -> SlurmJob:
    """
    Create a SLURM job for Monarch v1 API.

    Args:
        mesh_name: Name assigned to the primary mesh for this example.
                   A JobTrait can consist of multiple meshes, and
                   Monarch allows for re-attaching to ongoing jobs.
        num_nodes: Number of nodes allocated per mesh
        gpus_per_node: Number of GPUs per node in the mesh
        time_limit: Time limit for the SLURM job (default: "06:00:00")
        python_exe: Optional path to python executable

    Returns:
        SlurmJob: A configured SLURM job instance

    Note:
        SlurmJob is just one instance of a Monarch scheduler interface.
        Consult the JobTrait documentation to find one that's right for your usecase.
    """
    default_job_name = "monarch_example"

    slurm_job_args = {
        "meshes": {mesh_name: num_nodes},
        "job_name": default_job_name,
        "gpus_per_node": gpus_per_node,
        "time_limit": time_limit,
    }
    if python_exe:
        slurm_job_args["python_exe"] = python_exe

    return SlurmJob(
        **slurm_job_args,
        # ... additional args can be passed here
    )


async def cleanup_job(job: JobTrait) -> None:
    """
    Cancel the SLURM job, releasing all reserved nodes back to the cluster.

    Args:
        job: A JobTrait, like the one returned from create_slurm_job()

    Note:
        The job will also terminate automatically when the configured TTL
        is exceeded, but explicit cleanup is recommended for long-running
        notebooks or scripts.
    """
    job.kill()
    logger.info("Job terminated successfully")

