# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Internal implementation of SPMD primitives.

Provides functions to configure torch elastic environment variables across a Monarch
ProcMesh, enabling torchrun-style SPMD scripts to run on Monarch's actor system.
"""

from typing import Optional

from monarch._src.actor.proc_mesh import ProcMesh
from monarch._src.spmd.actor import SPMDActor
from monarch.tools.network import AddrType


def setup_torch_elastic_env(
    proc_mesh: ProcMesh,
    master_addr: str | None = None,
    master_port: int | None = None,
    use_ipaddr: Optional[AddrType] = None,
) -> None:
    """
    Sets up environment variables for pytorch torchelastic.
    It sets enviornment variables like RANK, LOCAL_RANK, WORLD_SIZE, etc.
    If master_addr and master_port are None, it will automatically select a master node and port.
    It selects the first proc in the proc_mesh to be the master node.
    """
    assert (master_addr is None) == (master_port is None), (
        "Either both master_addr and master_port must be specified or neither must be specified."
    )
    am = proc_mesh.spawn("_SPMDActor", SPMDActor)
    if master_addr is None:
        # Select the first actor (all coordinates = 0) to get the master host/port
        first_values = dict.fromkeys(proc_mesh._labels, 0)
        master_addr, master_port = (
            am.slice(**first_values).get_host_port.call_one(use_ipaddr).get()
        )
    assert master_port is not None, "master_port should not be None here."
    am.setup_env.call(master_addr, master_port).get()


async def setup_torch_elastic_env_async(
    proc_mesh: ProcMesh,
    master_addr: str | None = None,
    master_port: int | None = None,
    use_ipaddr: Optional[AddrType] = None,
) -> None:
    """
    Sets up environment variables for pytorch torchelastic.
    It sets enviornment variables like RANK, LOCAL_RANK, WORLD_SIZE, etc.
    If master_addr and master_port are None, it will automatically select a master node and port.
    It selects the first proc in the proc_mesh to be the master node.
    """
    assert (master_addr is None) == (master_port is None), (
        "Either both master_addr and master_port must be specified or neither must be specified."
    )
    am = proc_mesh.spawn("_SPMDActor", SPMDActor)
    if master_addr is None:
        # Select the first actor (all coordinates = 0) to get the master host/port
        first_values = dict.fromkeys(proc_mesh._labels, 0)
        master_addr, master_port = await am.slice(
            **first_values
        ).get_host_port.call_one(use_ipaddr)
    assert master_port is not None, "master_port should not be None here."
    await am.setup_env.call(master_addr, master_port)
