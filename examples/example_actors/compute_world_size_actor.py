# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import math
import os

import torch
import torch.distributed as dist
import torch.nn.functional as F
from monarch.actor import Actor, current_rank, current_size, endpoint


class ComputeWorldSizeActor(Actor):
    """Silly actor that computes the world size by all-reducing rank-hot tensors"""

    def __init__(self) -> None:
        pass

    @endpoint
    async def compute_world_size(self, master_addr: str, master_port: int) -> int:
        rank: int = current_rank().rank
        world_size: int = math.prod(current_size().values())

        backend = "nccl"
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(master_port)

        print(f"""Initializing process group `{backend}`:
  MASTER_ADDR = {master_addr}
  MASTER_PORT = {master_port}
  RANK        = {rank}
  WORLD_SIZE  = {world_size}""")

        dist.init_process_group(backend, rank=rank, world_size=world_size)

        try:
            # TODO: generalize this.
            local_rank = rank % 4  # current_rank()["gpus"]
            t = F.one_hot(
                torch.tensor(rank, device=f"cuda:{local_rank}"),
                num_classes=dist.get_world_size(),
            )
            dist.all_reduce(t)
            return int(torch.sum(t).item())
        finally:
            dist.destroy_process_group()
