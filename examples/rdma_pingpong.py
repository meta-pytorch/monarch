# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import mmap
import os
import socket
import sys
import time
from typing import Optional

import fire
import torch
import xxhash
from monarch.actor import Actor, endpoint, shutdown_context
from monarch.rdma import RDMABuffer


def _checksum(buf: bytes) -> str:
    """Compute an xxhash-based checksum hex digest."""
    return xxhash.xxh64(buf).hexdigest()


class PingPongActor(Actor):
    """Actor that participates in RDMA pingpong."""

    def __init__(self, size_bytes: int, buffer_type: str = "tensor"):
        self.hostname = socket.gethostname()
        self.size_bytes = size_bytes
        self.buffer_type = buffer_type

        if buffer_type == "tensor":
            n = size_bytes // 4
            self.data = torch.rand(n, dtype=torch.float32)
            self.recv_buf = torch.zeros(n, dtype=torch.float32)
        elif buffer_type == "bytearray":
            self.data = bytearray(os.urandom(size_bytes))
            self.recv_buf = bytearray(size_bytes)
        elif buffer_type == "memoryview":
            self._data_mmap = mmap.mmap(-1, size_bytes)
            self._data_mmap.write(os.urandom(size_bytes))
            self._data_mmap.seek(0)
            self.data = self._data_mmap
            self._recv_mmap = mmap.mmap(-1, size_bytes)
            self.recv_buf = self._recv_mmap
        else:
            raise ValueError(f"Unknown buffer_type: {buffer_type!r}")

    @endpoint
    async def init_rdma(self):
        """Pre-initialize the RDMA manager (avoids block_on deadlock)."""
        from monarch._src.actor.future import Future
        from monarch._src.rdma.rdma import _ensure_init_rdma_manager

        await Future(coro=_ensure_init_rdma_manager())

    @endpoint
    async def get_buffer(self) -> RDMABuffer:
        if self.buffer_type == "tensor":
            return RDMABuffer(self.data.view(torch.uint8).flatten())
        else:
            return RDMABuffer(memoryview(self.data))

    @endpoint
    async def read_from(self, peer_buf: RDMABuffer) -> float:
        """Read peer's data into recv_buf, return elapsed seconds."""
        if self.buffer_type == "tensor":
            self.recv_buf.zero_()
            local = self.recv_buf.view(torch.uint8).flatten()
        elif self.buffer_type == "bytearray":
            for i in range(len(self.recv_buf)):
                self.recv_buf[i] = 0
            local = memoryview(self.recv_buf)
        else:
            self._recv_mmap.seek(0)
            self._recv_mmap.write(b"\x00" * self.size_bytes)
            self._recv_mmap.seek(0)
            local = memoryview(self.recv_buf)

        t0 = time.perf_counter()
        await peer_buf.read_into(local, timeout=60)
        return time.perf_counter() - t0

    @endpoint
    async def checksum(self, which: str = "data") -> str:
        buf = self.data if which == "data" else self.recv_buf
        if self.buffer_type == "tensor":
            raw = buf.numpy().tobytes()
        else:
            raw = bytes(buf)
        return _checksum(raw)


def main(
    data_size_mb: int = 100,
    num_iterations: int = 5,
    backend: str = "slurm",
    partition: Optional[str] = None,
    hpc_identity: str = "hyper_monarch",
    hpc_job_oncall: str = "monarch",
    hpc_cluster_uuid: str = "MastGenAICluster",
    rm_attribution: str = "msl_infra_pytorch_dev",
    buffer_type: str = "tensor",
):
    """RDMA Pingpong: transfer data between two nodes via RDMABuffer."""
    sys.stdout.reconfigure(line_buffering=True)
    size = data_size_mb * 1024 * 1024

    if buffer_type not in ("tensor", "bytearray", "memoryview"):
        raise ValueError(
            f"Unknown buffer_type: {buffer_type!r}; "
            "choose from 'tensor', 'bytearray', 'memoryview'"
        )

    if backend == "mast":
        from monarch.actor import enable_transport
        from monarch.job.meta import MASTJob

        enable_transport("metatls-hostname")
        job = MASTJob(
            hpcIdentity=hpc_identity,
            hpcJobOncall=hpc_job_oncall,
            hpcClusterUuid=hpc_cluster_uuid,
            rmAttribution=rm_attribution,
            useStrictName=True,
            localityConstraints=["region", "gtn"],
        )
        job.add_mesh("workers", 2)
    else:
        from monarch.job import SlurmJob

        job = SlurmJob(
            meshes={"workers": 2},
            gpus_per_node=1,
            partition=partition,
            exclusive=False,
            log_dir=os.path.expanduser("~/monarch_slurm_logs"),
        )

    workers = job.state().workers
    procs = workers.spawn_procs()
    a0 = procs.spawn("a0", PingPongActor, size, buffer_type).slice(hosts=0)
    a1 = procs.spawn("a1", PingPongActor, size, buffer_type).slice(hosts=1)

    a0.init_rdma.call_one().get()
    a1.init_rdma.call_one().get()
    buf0 = a0.get_buffer.call_one().get()
    buf1 = a1.get_buffer.call_one().get()
    cksum0 = a0.checksum.call_one("data").get()
    cksum1 = a1.checksum.call_one("data").get()

    print(
        f"RDMA Pingpong: {data_size_mb} MB x {num_iterations} iters (buffer_type={buffer_type})"
    )
    for i in range(num_iterations):
        # Ping: a0 reads from a1
        dt = a0.read_from.call_one(buf1).get()
        got = a0.checksum.call_one("recv").get()
        ok = got == cksum1
        print(
            f"  [{i + 1}] ping {dt:.3f}s {size / dt / 1e9:.2f} GB/s {'PASS' if ok else 'FAIL'}"
        )

        # Pong: a1 reads from a0
        dt = a1.read_from.call_one(buf0).get()
        got = a1.checksum.call_one("recv").get()
        ok = got == cksum0
        print(
            f"  [{i + 1}] pong {dt:.3f}s {size / dt / 1e9:.2f} GB/s {'PASS' if ok else 'FAIL'}"
        )

    workers.shutdown().get()
    shutdown_context().get()
    print("Done!")


if __name__ == "__main__":
    fire.Fire(main)
