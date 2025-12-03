# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import math
import os
import runpy
import sys

from monarch._src.actor.actor_mesh import Actor, current_rank, current_size
from monarch._src.actor.endpoint import endpoint


class SPMDActor(Actor):
    """
    Actor that sets up PyTorch distributed training environment variables and
    executes SPMD training scripts.

    This actor replicates torchrun's behavior by configuring RANK, WORLD_SIZE,
    LOCAL_RANK, MASTER_ADDR, and MASTER_PORT environment variables before
    launching the training script or module.
    All rank and mesh information is automatically derived from current_rank().

    Args:
        master_addr: Address of the master node for rendezvous.
        master_port: Port on the master node for rendezvous.
    """

    def __init__(
        self,
        master_addr: str,
        master_port: int,
    ) -> None:
        super().__init__()

        point = current_rank()
        sizes = current_size()  # Returns dict: {"hosts": N, "gpus": M, ...}

        self.local_rank: int = point["gpus"]  # LOCAL_RANK
        self.rank: int = point.rank  # RANK (global)
        self.nproc_per_node: int = sizes["gpus"]  # Number of GPUs per host
        self.world_size: int = math.prod(sizes.values())

        self.master_addr = master_addr
        self.master_port = master_port

    @endpoint
    def main(self, script_args: list[str]) -> bool:
        """
        Set up distributed training environment and execute the training script.

        Args:
            script_args: Arguments for the training script. First element is either
                "-m" (for module execution) or the script path, followed by script arguments.

        Returns:
            True on successful execution.

        Raises:
            ValueError: If no script or module is specified.
        """
        os.environ.update(
            {
                "RANK": str(self.rank),
                "WORLD_SIZE": str(self.world_size),
                "LOCAL_RANK": str(self.local_rank),
                "MASTER_ADDR": self.master_addr,
                "MASTER_PORT": str(self.master_port),
            }
        )

        if script_args and script_args[0] == "-m":
            module_name = script_args[1]
            sys.argv = [module_name] + list(script_args[2:])
            runpy.run_module(module_name, run_name="__main__", alter_sys=True)
        elif script_args:
            script_path = script_args[0]
            sys.argv = list(script_args)
            runpy.run_path(script_path, run_name="__main__")
        else:
            raise ValueError("No script or module specified")

        return True
