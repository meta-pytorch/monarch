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
import getpass
import argparse
import asyncio
import getpass
import json
import logging
import socket
import os
import pathlib
import sys

import cloudpickle

from compute_world_size_actor import TestActor

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
# from monarch._src.actor.meta.allocator import MastAllocator, MastAllocatorConfig

from monarch.actor import ProcMesh
from monarch.tools import commands
from monarch.tools.components import hyperactor
from monarch.tools.config import Config, UnnamedAppDef
from monarch._src.actor.allocator import RemoteAllocator, StaticRemoteAllocInitializer, TorchXRemoteAllocInitializer


import math
import os

import torch
import torch.distributed as dist
import torch.nn.functional as F
from monarch.actor import Actor, current_rank, current_size, endpoint

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim

from monarch.actor import Actor, current_rank, endpoint, proc_mesh

from torch.nn.parallel import DistributedDataParallel as DDP


USER = getpass.getuser()
HOME = pathlib.Path().home()
CWD = os.getcwd()
DEACTIVATE = None

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)


logger: logging.Logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--force-restart",
    action="store_true",
    default=False,
    help="Force restart the job (default: False)",
)

args = parser.parse_args()


def write_env_file(prefix):
    hostname = socket.gethostname()
    pid = os.getpid()
    filename = f"/home/ubuntu/ahmads/env.{prefix}.{hostname}.{pid}"
    with open(filename, "w") as f:
        f.write(str(os.environ))



def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        addr = s.getsockname()
        port = addr[1]
        return port


class _TorchDistributedInitActor(Actor):
    def __init__(self) -> None:
        self.rank: int = current_rank().rank

    @endpoint
    def get_host_port(self) -> tuple[str, int]:
        return (socket.gethostname(), _find_free_port())

    @endpoint
    def setup_env(self, master_addr: str, master_port: int) -> None:
        cr = current_rank()
        # Assume last dimension is the local rank.
        last_label = cr.shape.labels[-1]
        local_world_size = cr.size(last_label)
        world_size = len(cr)
        global_rank = cr.rank
        local_rank = min(world_size, global_rank % local_world_size)
        group_rank = global_rank // local_world_size
        group_world_size = (world_size + local_world_size - 1) // local_world_size
        env = {
            "MASTER_ADDR": master_addr,
            "MASTER_PORT": str(master_port),
            "RANK": str(global_rank),
            "LOCAL_RANK": str(local_rank),
            "LOCAL_WORLD_SIZE": str(local_world_size),
            "GROUP_RANK": str(group_rank),
            "GROUP_WORLD_SIZE": str(group_world_size),
            "ROLE_RANK": str(global_rank),
            "ROLE_WORLD_SIZE": str(world_size),
            "ROLE_NAME": "rank",
            "WORLD_SIZE": str(world_size),
        }
        os.environ.update(env)
        write_env_file("env")


async def setup_env_for_distributed(
    proc_mesh: ProcMesh,
    master_addr: str | None = None,
    master_port: int | None = None,
) -> None:
    """
    Sets up environment variables for pytorch distributed.
    It selects a random proc in the proc_mesh to be the master node.
    It sets enviornment variables like RANK, LOCAL_RANK, WORLD_SIZE, etc.
    If master_addr and master_port are None, it will automatically select a master node and port.
    """
    assert (
        (master_addr is None) == (master_port is None)
    ), "Either both master_addr and master_port must be specified or neither must be specified."
    am = await proc_mesh.spawn("_TorchDistributedInitActor", _TorchDistributedInitActor)
    if master_addr is None:
        # We use call instead of call_one because call_one can't handle tuple return types.
        vm = await am.flatten("rank").slice(rank=0).get_host_port.call()
        master_addr, master_port = vm.item()
    assert master_port is not None, "master_port should not be None here."
    await am.setup_env.call(master_addr, master_port)





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

        write_env_file("setup")

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


# %%
# Main function to run the complete example
async def main():
    """Main function to run the DDP example."""
    jobname = f"monarch-{USER}"

    # similar to Docker image; should contain a conda env in the $img_root/conda/ directory
    # when config.workspace is not None, an ephemeral fbpkg version is created
    # that conda-packs the currently active local conda env AND the directory specified by workspace
    image = "monarch_default_workspace:latest"

    num_hosts = 2
    

    appdef = hyperactor.host_mesh(
            image=image,
            # TODO: For some reason gpu.medium doens't work here
            meshes=[f"mesh0:{num_hosts}:aws_g5.12xlarge"],  # mesh_name:num_hosts:host_type
        )
    
    # TODO: Register this so we don't have to do this every time
    for role in appdef.roles:
        role.resource.memMB = 186777

    config = Config(
        scheduler="slurm",
        scheduler_args={
            # NOTE: replace with your own values
            "hpcIdentity": "pytorch_distributed",
            "hpcJobOncall": "monarch",
            "hpcClusterUuid": "MastProdCluster",
            "rmAttribution": "pytorch4all_clients_approved",
        },
        appdef=appdef,
        workspace=str(CWD),  # or None to disable building ephemeral,
    )

    # config.dryrun = True
    # o = commands.create(config)
    # print(o)
    # sys.exit(0)

    server_info = await commands.get_or_create(
        jobname,
        config,
        force_restart=args.force_restart,
    )
    # TODO: why is gpus equal to -1 in server_info?

    num_gpus_per_host = appdef.roles[0].resource.gpu
    print(f"ahmad: {server_info} {role.resource.gpu}")

    logger.info(
        "\n===== Server Info =====\n%s",
        json.dumps(server_info.to_json(), indent=2),
    )

    mesh_dimensions = {
        "host": server_info.get_mesh_spec("mesh0").num_hosts,
        "gpu": server_info.get_mesh_spec("mesh0").gpus,
    }
    # this is redundant but is here for example sake
    mesh_name = server_info.get_mesh_spec("mesh0").name

    allocator = RemoteAllocator(world_id="foo", initializer=TorchXRemoteAllocInitializer(server_info.server_handle))
    alloc = await allocator.allocate(AllocSpec(AllocConstraints(), hosts=num_hosts, gpus=num_gpus_per_host))

    proc_mesh = await ProcMesh.from_alloc(alloc)


    ddp_actor = await proc_mesh.spawn("ddp_actor", DDPActor)

    await setup_env_for_distributed(proc_mesh)

    await ddp_actor.setup.call()
    await ddp_actor.demo_basic.call()
    await ddp_actor.cleanup.call()

    print("DDP example completed successfully!")


#     actor = await proc_mesh.spawn("compute_world_size_actor", TestActor)

#     logger.info("computing world size...")
#     values = await actor.compute_world_size.call(
#         master_addr=server_info.host0(mesh_name),
#         master_port=29500,
#     )

#     values_by_rank = {f"rank_{p.rank}": v for p, v in list(values.flatten("rank"))}

#     logger.info(f"""computed world_sizes:
# {'-'*40}
# {json.dumps(values_by_rank, indent=2)}
# {'-'*40}""")

    commands.kill(f"slurm:///{server_info.name}")



if __name__ == "__main__":
    import asyncio

    asyncio.run(main())