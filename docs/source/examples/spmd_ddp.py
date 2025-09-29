# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
DDP Examples Using Classic SPMD / torch.distributed
==================================================

This example demonstrates how to run PyTorch's Distributed Data Parallel (DDP)
within Monarch actors. We'll adapt the basic DDP example from PyTorch's
documentation and wrap it in Monarch's actor framework.

This example shows:
- How to initialize torch.distributed within Monarch actors
- How to create and use DDP models in a distributed setting
- How to properly clean up distributed resources
"""

# %%
# First, we'll import the necessary libraries and define our model and actor classes

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim

from monarch.actor import Actor, current_rank, endpoint, proc_mesh

from torch.nn.parallel import DistributedDataParallel as DDP


WORLD_SIZE = 4


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

    Conveniently, all of the methods we need are already laid out for us,
    so we can just wrap them in the usual Actor endpoint semantic with some
    light modifications.

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
        model = ToyModel().to(self.rank)
        ddp_model = DDP(model, device_ids=[self.rank])

        loss_fn = nn.MSELoss()
        optimizer = optim.SGD(ddp_model.parameters(), lr=0.001)

        optimizer.zero_grad()
        outputs = ddp_model(torch.randn(20, 10))
        labels = torch.randn(20, 5).to(self.rank)
        loss_fn(outputs, labels).backward()
        optimizer.step()

        print(f"{self.rank=} Finished running basic DDP example")


# %%
# Now we'll define functions to create and run our DDP example


async def create_ddp_actors():
    """Create the process mesh and spawn DDP actors."""
    # Spawn a process mesh
    local_proc_mesh = proc_mesh(
        gpus=WORLD_SIZE,
        env={
            "MASTER_ADDR": "localhost",
            "MASTER_PORT": "12355",
        },
    )
    # Spawn our actor mesh on top of the process mesh
    ddp_actor = local_proc_mesh.spawn("ddp_actor", DDPActor)
    return ddp_actor, local_proc_mesh


async def setup_distributed(ddp_actor):
    """Initialize the distributed environment."""
    # Setup torch Distributed
    await ddp_actor.setup.call()


async def run_ddp_example(ddp_actor):
    """Run the DDP training example."""
    # Run the demo
    await ddp_actor.demo_basic.call()


async def cleanup_distributed(ddp_actor):
    """Clean up distributed resources."""
    # Clean up
    await ddp_actor.cleanup.call()


# %%
# Main function to run the complete example
async def main():
    """Main function to run the DDP example."""
    # Create actors
    ddp_actor, proc_mesh = await create_ddp_actors()

    # Setup distributed environment
    await setup_distributed(ddp_actor)

    # Run DDP example
    await run_ddp_example(ddp_actor)

    # Clean up
    await cleanup_distributed(ddp_actor)

    print("DDP example completed successfully!")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
