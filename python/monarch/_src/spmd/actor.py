# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import math
import os
import runpy
import socket
import sys
from typing import Optional

from monarch._src.actor.actor_mesh import Actor, current_rank, current_size
from monarch._src.actor.endpoint import endpoint
from monarch.tools.network import AddrType, get_ipaddr


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        addr = s.getsockname()
        port = addr[1]
        return port


class SPMDActor(Actor):
    """
    Actor that sets up PyTorch distibuted.run training environment variables and
    executes SPMD training scripts.

    This actor replicates torchrun's behavior by configuring environment variables (see https://docs.pytorch.org/docs/stable/elastic/run.html#environment-variables)
    for PyTorch distributed training including RANK, WORLD_SIZE, LOCAL_RANK,
    LOCAL_WORLD_SIZE, GROUP_RANK, GROUP_WORLD_SIZE, ROLE_RANK, ROLE_WORLD_SIZE,
    ROLE_NAME, MASTER_ADDR, and MASTER_PORT before launching the training script.
    All rank and mesh information is automatically derived from current_rank().

    Example::

        from monarch.spmd import SPMDActor

        # Spawn SPMDActor on the process mesh
        spmd_actors = proc_mesh.spawn("_SPMDActor", SPMDActor)

        # Get master address/port from first actor
        first_values = dict.fromkeys(proc_mesh._labels, 0)
        master_addr, master_port = spmd_actors.slice(**first_values).get_host_port.call_one(None).get()

        # Execute training script across the mesh
        spmd_actors.main.call(master_addr, master_port, ["-m", "train", "--lr", "0.001"]).get()

    For custom client code that uses torch.distributed directly::

        from monarch.actor import this_host
        from monarch.spmd import setup_torch_elastic_env

        # Create a process mesh with 2 GPUs per host
        proc_mesh = this_host().spawn_procs(per_host={"gpus": 2})

        # Set up torch elastic environment (spawns SPMDActor internally)
        setup_torch_elastic_env(proc_mesh)

        # ... rest of custom client code
    """

    def __init__(
        self,
    ) -> None:
        super().__init__()

        point = current_rank()
        gpu_dim = point.extent.labels[-1]  # Typically "gpus"
        sizes = current_size()  # Returns dict: {"hosts": N, "gpus": M, ...}

        self.local_rank: int = point[gpu_dim]  # LOCAL_RANK
        self.rank: int = point.rank  # RANK (global)
        self.nproc_per_node: int = sizes[gpu_dim]  # Number of GPUs per host
        self.world_size: int = math.prod(sizes.values())
        self.local_world_size: int = sizes[gpu_dim]
        self.group_rank: int = self.rank // self.local_world_size
        self.group_world_size: int = (
            self.world_size + self.local_world_size - 1
        ) // self.local_world_size

    def _setup_env(self, master_addr: str, master_port: int) -> None:
        os.environ.update(
            {
                "MASTER_ADDR": master_addr,
                "MASTER_PORT": str(master_port),
                "RANK": str(self.rank),
                "LOCAL_RANK": str(self.local_rank),
                "LOCAL_WORLD_SIZE": str(self.local_world_size),
                "GROUP_RANK": str(self.group_rank),
                "GROUP_WORLD_SIZE": str(self.group_world_size),
                "ROLE_RANK": str(self.rank),
                "ROLE_WORLD_SIZE": str(self.world_size),
                "ROLE_NAME": "rank",
                "WORLD_SIZE": str(self.world_size),
            }
        )

    @endpoint
    def get_host_port(self, use_ipaddr: Optional[AddrType]) -> tuple[str, int]:
        hostname = socket.gethostname()
        port = _find_free_port()
        if use_ipaddr is None:
            return (hostname, port)

        ipaddr = get_ipaddr(hostname, port, use_ipaddr)
        return (ipaddr, port)

    @endpoint
    def setup_env(self, master_addr: str, master_port: int) -> None:
        """
        Set up distributed training environment variables.
        """
        self._setup_env(master_addr, master_port)

    @endpoint
    def main(self, master_addr: str, master_port: int, script_args: list[str]) -> bool:
        """
        Set up distributed training environment and execute the training script.

        Args:
            master_addr: Master node address for distributed training
            master_port: Master node port for distributed training
            script_args: Arguments for the training script. First element is either
                "-m" (for module execution) or the script path, followed by script arguments.

        Returns:
            True on successful execution.

        Raises:
            ValueError: If no script or module specified.
        """
        self._setup_env(master_addr, master_port)

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
