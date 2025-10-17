# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


# pyre-strict
import os
import socket

from typing import Optional

from monarch.actor import Actor, current_rank, endpoint, ProcMesh
from monarch.tools.network import AddrType, get_ipaddr


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
    def get_host_port(self, use_ipaddr: Optional[AddrType]) -> tuple[str, int]:
        hostname = socket.gethostname()
        port = _find_free_port()
        if use_ipaddr is None:
            return (hostname, port)

        ipaddr = get_ipaddr(hostname, port, use_ipaddr)
        return (ipaddr, port)

    @endpoint
    def setup_env(self, master_addr: str, master_port: int) -> None:
        cr = current_rank()
        # Assume last dimension is the local rank.
        last_label = cr.extent.labels[-1]
        local_world_size = cr.size(last_label)
        world_size = cr.extent.nelements
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


async def setup_env_for_distributed(
    proc_mesh: ProcMesh,
    master_addr: str | None = None,
    master_port: int | None = None,
    use_ipaddr: Optional[AddrType] = None,
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
    am = proc_mesh.spawn("_TorchDistributedInitActor", _TorchDistributedInitActor)
    if master_addr is None:
        # We use call instead of call_one because call_one can't handle tuple return types.
        vm = await am.flatten("rank").slice(rank=0).get_host_port.call(use_ipaddr)
        master_addr, master_port = vm.item()
    assert master_port is not None, "master_port should not be None here."
    await am.setup_env.call(master_addr, master_port)
