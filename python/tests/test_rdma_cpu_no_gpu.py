# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
RDMA tests for no_gpu builds.

Runs only under the `:with_gpu[no_gpu]` Buck configuration, where
`monarch_rdma`'s `cuda` feature is off and the ibverbs/EFA backends are
not compiled in. Verifies that `RDMABuffer` still works end-to-end via
the TCP transport.
"""

import sys

import pytest

if sys.platform != "linux":
    pytest.skip("linux-only", allow_module_level=True)

from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, endpoint, this_host
from monarch.rdma import get_rdma_backend, is_ibverbs_available, RDMABuffer


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


def test_no_ibverbs_at_compile_time():
    """In a no_gpu build the ibverbs backend is gated out at compile time,
    so `is_ibverbs_available()` is the CPU stub returning false and
    `get_rdma_backend()` reports `"tcp"` via the `RDMA_ALLOW_TCP_FALLBACK`
    default."""
    assert not is_ibverbs_available()
    assert get_rdma_backend() == "tcp"


@isolate_in_subprocess
async def test_rdma_buffer_via_tcp_fallback():
    """RDMABuffer round-trip across two CPU actors works via the TCP backend."""
    proc1 = this_host().spawn_procs(per_host={"processes": 1})
    proc2 = this_host().spawn_procs(per_host={"processes": 1})

    sender = proc1.spawn("sender", CpuActor)
    receiver = proc2.spawn("receiver", CpuActor)

    buf = await sender.create_buffer.call_one()
    received = await receiver.read_buffer.call_one(buf)

    assert received == bytes(range(256))

    await proc1.stop()
    await proc2.stop()
