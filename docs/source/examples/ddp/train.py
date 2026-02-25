# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

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
