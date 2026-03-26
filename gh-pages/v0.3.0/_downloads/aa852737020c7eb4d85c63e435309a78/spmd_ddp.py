# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
DDP Examples Using SPMDActor
============================

This example demonstrates how to run PyTorch's Distributed Data Parallel (DDP)
using Monarch's ``SPMDActor``. The actor configures torch elastic environment
variables and executes the training script, replicating torchrun behavior.

This example shows:

- How to spawn a process mesh on the local host
- How to use ``SPMDActor`` to run a DDP training script
- How ``SPMDActor`` configures RANK, LOCAL_RANK, WORLD_SIZE, etc.

Training Script
---------------

The training script (``train.py``) is a standard PyTorch DDP script::

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

This script:

- Initializes NCCL process group (environment variables set by ``SPMDActor``)
- Creates a simple linear model wrapped in DDP
- Runs 5 training steps
- Cleans up the process group
"""

# %%
# Imports
# -------
# We import Monarch's actor API and SPMDActor.

import os

from monarch.actor import this_host
from monarch.spmd import SPMDActor


GPUS_PER_HOST = 4

# Get absolute path to train.py (in the same directory as this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRAIN_SCRIPT = os.path.join(SCRIPT_DIR, "train.py")


# %%
# Create Process Mesh
# -------------------
# Spawn a process mesh on the local host with 4 GPU processes.

local_proc_mesh = this_host().spawn_procs(per_host={"gpus": GPUS_PER_HOST})


# %%
# Run DDP Training with SPMDActor
# -------------------------------
# Spawn ``SPMDActor`` on the process mesh. The actor configures torch elastic
# environment variables (RANK, LOCAL_RANK, WORLD_SIZE, MASTER_ADDR, MASTER_PORT)
# and executes the training script.

spmd_actors = local_proc_mesh.spawn("_SPMDActor", SPMDActor)

# Get master address/port from first actor (all coordinates = 0)
first_values = dict.fromkeys(local_proc_mesh._labels, 0)
master_addr, master_port = (
    spmd_actors.slice(**first_values).get_host_port.call_one(None).get()
)

# Execute training script across the mesh
spmd_actors.main.call(master_addr, master_port, [TRAIN_SCRIPT]).get()

print("DDP example completed successfully!")
