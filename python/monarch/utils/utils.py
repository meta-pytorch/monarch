# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


# pyre-strict
import warnings
from typing import Optional

from monarch.actor import ProcMesh
from monarch.spmd import setup_torch_elastic_env_async
from monarch.tools.network import AddrType


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

    .. deprecated:: 0.2.0
        This function is deprecated and will be removed in monarch 0.3.0.
        Use :func:`monarch.spmd.setup_torch_elastic_env` instead.
    """
    warnings.warn(
        "setup_env_for_distributed is deprecated and will be removed in monarch 0.3.0. "
        "Use monarch.spmd.setup_torch_elastic_env instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    await setup_torch_elastic_env_async(proc_mesh, master_addr, master_port, use_ipaddr)
