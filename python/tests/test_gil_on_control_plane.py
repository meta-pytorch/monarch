# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Fitness test proving no unsanctioned Python runs on the control-plane tokio runtime.

The control plane is the tokio runtime that drives actor messaging. Grabbing
the GIL there stalls the runtime for every actor it serves, so any Python
execution on it must go through a sanctioned, accounted path. The Rust side
increments a counter whenever the GIL is taken on the control plane outside a
sanctioned scope; these tests exercise the real actor and RDMA paths and assert
the counter stays at zero.
"""

import os

# Required to enable RDMA support; matches test_rdma.py.
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import pytest
import torch
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.runtime import (
    _force_unsanctioned_gil_on_control_plane,
    _get_gil_on_control_plane,
    _reset_gil_on_control_plane,
)
from monarch.actor import Actor, endpoint, this_host
from monarch.rdma import RDMABuffer
from rdma_test_utils import rdma_backends


needs_cuda = pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA not available",
)


class GilProbeServer(Actor):
    def __init__(self):
        self.data = torch.ones(10, 10, dtype=torch.float32)

    @endpoint
    async def ping(self, value: int) -> int:
        return value + 1

    @endpoint
    async def buffer(self) -> RDMABuffer:
        byte_tensor = self.data.view(torch.uint8).flatten()
        return RDMABuffer(byte_tensor)


class GilProbeClient(Actor):
    def __init__(self):
        self.data = torch.zeros(10, 10, dtype=torch.float32)

    @endpoint
    async def read(self, buffer: RDMABuffer) -> None:
        byte_tensor = self.data.view(torch.uint8).flatten()
        await buffer.read_into(byte_tensor)

    @endpoint
    async def write(self, buffer: RDMABuffer) -> None:
        byte_tensor = self.data.view(torch.uint8).flatten()
        await buffer.write_from(byte_tensor)


@pytest.mark.timeout(60)
@needs_cuda
@rdma_backends
@isolate_in_subprocess
async def test_no_unsanctioned_gil_on_control_plane() -> None:
    """Real actor calls and RDMA transfers must not grab the control-plane GIL.

    Spawns actors, awaits an endpoint call, then registers an ``RDMABuffer`` and
    rounds a ``read_into``/``write_from`` through it. The control-plane GIL
    counter must remain zero across all of it.
    """
    _reset_gil_on_control_plane()

    server_proc = this_host().spawn_procs(per_host={"gpus": 1})
    client_proc = this_host().spawn_procs(per_host={"gpus": 1})
    server = server_proc.spawn("server", GilProbeServer)
    client = client_proc.spawn("client", GilProbeClient)

    assert await server.ping.call_one(41) == 42

    buffer = await server.buffer.call_one()
    await client.read.call_one(buffer)
    await client.write.call_one(buffer)

    assert _get_gil_on_control_plane() == 0


@isolate_in_subprocess
def test_unsanctioned_gil_on_control_plane_is_counted() -> None:
    """The counter must observe a forced unsanctioned control-plane GIL grab.

    Guards against the positive test passing because the counter is wedged at
    zero: forcing one grab must move it above zero.
    """
    _reset_gil_on_control_plane()
    _force_unsanctioned_gil_on_control_plane()
    assert _get_gil_on_control_plane() > 0
