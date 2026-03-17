# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import sys

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, endpoint, this_host
from monarch.config import configured
from monarch.rdma import is_ibverbs_available, RDMABuffer

# Verify torch is NOT imported as a side-effect of loading monarch.rdma.
assert "torch" not in sys.modules, (
    "torch was unexpectedly imported when loading monarch.rdma"
)

# TODO(slurye): Enable these tests in OSS once the shutdown hang issue is fixed.
pytestmark = pytest.mark.oss_skip

RDMA_BACKENDS = []
if is_ibverbs_available():
    RDMA_BACKENDS.append("ibverbs")
RDMA_BACKENDS.append("tcp")


class CpuActor(Actor):
    def __init__(self) -> None:
        self.data = bytearray(range(256))
        self.buf = None

    @endpoint
    async def create_buffer(self) -> RDMABuffer:
        self.buf = RDMABuffer(memoryview(self.data))
        return self.buf

    @endpoint
    async def read_buffer(self, buf: RDMABuffer) -> bytes:
        dst = bytearray(len(self.data))
        await buf.read_into(memoryview(dst))
        return bytes(dst)


@pytest.mark.parametrize("rdma_backend", RDMA_BACKENDS)
@isolate_in_subprocess
async def test_rdma_buffer_cpu_memoryview(rdma_backend):
    """RDMABuffer works with bytearray/memoryview CPU buffers without importing torch."""
    if rdma_backend == "tcp":
        cm = configured(rdma_disable_ibverbs=True)
    else:
        cm = configured(rdma_allow_tcp_fallback=False)

    with cm:
        proc1 = this_host().spawn_procs(per_host={"processes": 1})
        proc2 = this_host().spawn_procs(per_host={"processes": 1})
        sender = proc1.spawn("sender", CpuActor)
        receiver = proc2.spawn("receiver", CpuActor)

        buf = await sender.create_buffer.call_one()
        received = await receiver.read_buffer.call_one(buf)
        assert received == bytes(range(256))

        assert "torch" not in sys.modules

        await proc1.stop()
        await proc2.stop()
