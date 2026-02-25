# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
SPMD Job Example
================

This example demonstrates how to use ``monarch.job.spmd`` (``serve()`` and
``run_spmd()``) to launch PyTorch DDP training. It shows single-node training
with a local scheduler and multi-node training with slurm.

The ``serve()`` function accepts either a torchx ``AppDef`` or a simple
command list. The ``run_spmd()`` method executes the training script across
all workers.

Why Use SPMD Jobs?
------------------

The ``serve()`` + ``run_spmd()`` pattern enables an interactive development
workflow to quickly re-run and debug SPMD training scripts:

- **Reserve once, iterate many times**: The scheduler allocates hosts once via
  ``serve()``, then you can call ``run_spmd()`` repeatedly without reprovisioning.
  Edit your training script, sync code, and re-run—all on the same reserved hosts.

- **Remote debugging**: Add ``breakpoint()`` in your training script, then attach
  the Monarch debugger from a separate terminal::

      $ monarch debug

  This opens an interactive pdb session where you can inspect variables, step
  through code, and debug across all ranks. See :doc:`debugging <../debugging>`
  for details.

Note:
    When passing a command list, only single-node torchrun is supported
    (``--standalone`` or ``--nnodes=1``). For multi-node training, use an
    ``AppDef`` with a scheduler that manages node allocation.

This example shows:

- How to use ``serve()`` with a command list for single-node training
- How to use ``serve()`` with an ``AppDef`` for multi-node training on slurm
- How to attach the Monarch debugger for interactive debugging
- How to reload a cached job and re-run ``run_spmd()`` on provisioned hosts

Training Script
---------------

This example reuses the ``train.py`` script from the same directory::

    import os
    import torch
    import torch.distributed as dist
    import torch.nn as nn
    import torch.optim as optim
    from torch.nn.parallel import DistributedDataParallel as DDP

    def main():
        dist.init_process_group("nccl")
        rank = dist.get_rank()
        local_rank = int(os.environ["LOCAL_RANK"])
        torch.cuda.set_device(local_rank)

        model = nn.Linear(10, 1).cuda()
        ddp_model = DDP(model)

        optimizer = optim.SGD(ddp_model.parameters(), lr=0.01)

        for step in range(5):
            inputs = torch.randn(4, 10).cuda()
            outputs = ddp_model(inputs)
            loss = outputs.sum()
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            print(f"[Rank {rank}] Step {step} loss={loss.item()}")

        dist.destroy_process_group()

    if __name__ == "__main__":
        main()
"""

# %%
# Imports
# -------

import os

from monarch.job.spmd import serve

# %%
# Configuration
# -------------
# Configure the number of GPUs and paths.

GPUS_PER_HOST = 4

# Get absolute path to train.py (in the same directory as this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRAIN_SCRIPT = os.path.join(SCRIPT_DIR, "train.py")


# %%
# Main Function
# -------------
# The main function uses ``serve()`` with a command list to launch workers
# and ``run_spmd()`` to execute the training.


def main():
    """Launch and run SPMD DDP training."""
    print("=" * 60)
    print("SPMD Job Example")
    print(f"Training script: {TRAIN_SCRIPT}")
    print(f"GPUs per host: {GPUS_PER_HOST}")
    print("=" * 60)

    # Launch workers using a torchrun command
    job = serve(
        [
            "torchrun",
            f"--nproc-per-node={GPUS_PER_HOST}",
            "--standalone",
            TRAIN_SCRIPT,
        ],
        scheduler="local_cwd",
    )

    # Execute training across all workers
    job.run_spmd()

    print("=" * 60)
    print("SPMD job completed successfully!")
    print("=" * 60)


# %%
# Multi-Node Training with AppDef
# --------------------------------
# For multi-node training, use an ``AppDef`` with a scheduler that manages
# node allocation (e.g., slurm). This example shows how to
# construct such an AppDef (not executed in this demo).


def multi_node_example():
    """Example of multi-node training setup (not executed)."""
    from torchx import specs

    # Create an AppDef for 2-node training with 8 GPUs per node
    app = specs.AppDef(
        name="multi-node-training",
        roles=[
            specs.Role(
                name="trainer",
                image="",  # Docker image or workspace
                entrypoint="torchrun",
                args=[
                    "--nnodes=2",  # Multi-node is supported with AppDef
                    "--nproc-per-node=8",
                    "--rdzv-backend=c10d",
                    "--rdzv-endpoint=$MASTER_ADDR:$MASTER_PORT",
                    TRAIN_SCRIPT,
                ],
                num_replicas=2,  # Number of nodes
                resource=specs.Resource(cpu=32, gpu=8, memMB=256000),
            ),
        ],
    )

    # Launch with a scheduler that manages multi-node allocation.
    # See https://pytorch.org/torchx/latest/schedulers/slurm.html for slurm
    job = serve(
        app,
        scheduler="slurm",
        scheduler_cfg={
            "partition": "gpu",
        },
    )

    # Execute training across all nodes
    job.run_spmd()


# %%
# Iterating on Your Training Script
# ----------------------------------
# After the initial run, you can edit ``train.py`` and re-run without
# reprovisioning. The job state is cached, so reload and run again::
#
#     from monarch.job.spmd import job_load
#
#     job = job_load(".monarch/job_state.pkl")
#     job.run_spmd()  # runs on same reserved hosts


# %%
# Running the Example
# -------------------
# Run this example with::
#
#     python docs/source/examples/ddp/spmd_job.py
#
# This will:
#
# 1. Launch Monarch workers with torchrun using the ``local_cwd`` scheduler
# 2. Execute the DDP training script on all GPUs

if __name__ == "__main__":
    main()
