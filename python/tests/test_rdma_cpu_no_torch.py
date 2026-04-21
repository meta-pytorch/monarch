# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import sys
import time

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, endpoint, this_host
from monarch.config import configured
from monarch.rdma import is_ibverbs_available, RDMABuffer

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
    async def ping(self) -> str:
        return "pong"

    @endpoint
    async def create_buffer(self) -> RDMABuffer:
        import time as _time

        t0 = _time.perf_counter()
        self.buf = RDMABuffer(memoryview(self.data))
        t1 = _time.perf_counter()
        print(
            f"[ACTOR TIMING] RDMABuffer() total: {(t1 - t0) * 1000:.1f}ms", flush=True
        )
        return self.buf

    @endpoint
    async def read_buffer(self, buf: RDMABuffer) -> bytes:
        import time as _time

        t0 = _time.perf_counter()
        dst = bytearray(len(self.data))
        await buf.read_into(memoryview(dst))
        t1 = _time.perf_counter()
        print(
            f"[ACTOR TIMING] read_buffer: buf.read_into(): {(t1 - t0) * 1000:.1f}ms",
            flush=True,
        )
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
        t0 = time.perf_counter()
        proc1 = this_host().spawn_procs(per_host={"processes": 1})
        proc2 = this_host().spawn_procs(per_host={"processes": 1})
        t1 = time.perf_counter()
        print(f"\n[TIMING] spawn_procs: {(t1 - t0) * 1000:.1f}ms")

        sender = proc1.spawn("sender", CpuActor)
        receiver = proc2.spawn("receiver", CpuActor)
        t2 = time.perf_counter()
        print(f"[TIMING] spawn actors: {(t2 - t1) * 1000:.1f}ms")

        pong = await sender.ping.call_one()
        t_ping = time.perf_counter()
        print(f"[TIMING] first ping (process startup): {(t_ping - t2) * 1000:.1f}ms")

        buf = await sender.create_buffer.call_one()
        t3 = time.perf_counter()
        print(
            f"[TIMING] create_buffer (sender, after ping): {(t3 - t_ping) * 1000:.1f}ms"
        )

        received = await receiver.read_buffer.call_one(buf)
        t4 = time.perf_counter()
        print(
            f"[TIMING] read_buffer (receiver, includes QP setup + transfer): {(t4 - t3) * 1000:.1f}ms"
        )

        assert received == bytes(range(256))

        assert "torch" not in sys.modules

        await proc1.stop()
        await proc2.stop()
        t5 = time.perf_counter()
        print(f"[TIMING] stop: {(t5 - t4) * 1000:.1f}ms")
        print(f"[TIMING] total: {(t5 - t0) * 1000:.1f}ms")
