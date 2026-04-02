# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Host-to-host channel throughput benchmark.

Runs sender and receiver on MAST hosts (not devserver) to measure
intra-cluster throughput, matching D97817298's setup.

Usage:
    CONDA_PREFIX=$HOME/monarch_conda_envs/worker/conda \
    ~/monarch_conda_envs/client/conda/bin/python3.12 \
    python/benches/remotemount/bench_host_to_host.py \
    --host_type gb200 --data_size_mb 16384
"""

from __future__ import annotations

import logging
import mmap
import sys
import time

import cloudpickle
import fire
from monarch.actor import Actor, enable_transport, endpoint
from monarch.job.meta import MASTJob


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class BenchActor(Actor):
    """Actor that can act as sender or receiver for channel throughput."""

    def __init__(self) -> None:
        self._receiver = None
        self._buffer = None
        self._buffer_mv = None

    @endpoint
    def setup_receiver(self, num_streams: int, total_size: int) -> list[str]:
        """Create channel receivers and return addresses."""
        from monarch._rust_bindings.monarch_extension.tls_receiver import TlsReceiver

        self._buffer = mmap.mmap(-1, total_size, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
        self._buffer_mv = memoryview(self._buffer)
        self._receiver = TlsReceiver(num_streams)
        return self._receiver.data_addrs

    @endpoint
    def wait_receive(self) -> float:
        """Wait for all data to arrive. Returns bytes received."""
        assert self._receiver is not None
        self._receiver.wait(self._buffer_mv)
        self._receiver = None
        return len(self._buffer_mv)

    @endpoint
    def send_data(
        self, data_addrs: list[str], total_size: int, num_blocks: int, block_size: int
    ) -> tuple[float, float]:
        """Allocate buffer, fill with data, send via channels. Returns (elapsed, gb_s)."""
        from monarch._rust_bindings.monarch_extension.tls_sender import (
            send_blocks_from_buffer,
        )

        # Allocate and fill buffer
        buf = mmap.mmap(-1, total_size, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
        mv = memoryview(buf)
        # Fill with pattern (not random - faster to allocate)
        mv[:8] = b"\xab" * 8  # Just touch the pages

        dirty_blocks = list(range(num_blocks))

        t0 = time.monotonic()
        send_blocks_from_buffer(mv, total_size, dirty_blocks, data_addrs)
        elapsed = time.monotonic() - t0
        gb_s = total_size / elapsed / 1e9
        return (elapsed, gb_s)


def main(
    host_type: str = "gb200",
    data_size_mb: int = 16384,
    num_hosts: int = 2,
) -> None:
    from monarch.config import configure

    configure(
        enable_log_forwarding=True,
        host_spawn_ready_timeout="120s",
        mesh_proc_spawn_max_idle="120s",
        message_delivery_timeout="600s",
    )

    enable_transport("metatls-hostname")

    total_bytes = data_size_mb * 1024 * 1024
    block_size = 64 * 1024 * 1024
    num_blocks = (total_bytes + block_size - 1) // block_size

    print("=" * 60)
    print("Host-to-host channel throughput benchmark")
    print(
        f"Data: {data_size_mb}MB, Blocks: {num_blocks} x {block_size // (1024 * 1024)}MB"
    )
    print(f"Host type: {host_type}, Hosts: {num_hosts}")
    print("=" * 60)

    job = MASTJob(
        hpcIdentity="hyper_monarch",
        hpcJobOncall="monarch",
        rmAttribution="msl_infra_pytorch_dev",
        hpcClusterUuid="MastGenAICluster",
        useStrictName=True,
        env={"PYTHONDONTWRITEBYTECODE": "1"},
    )
    job.add_mesh("workers", num_hosts, host_type=host_type)
    job.add_directory(".", ".")
    state = job.state()
    proc_mesh = state.workers.spawn_procs(per_host={"procs": 1})
    mesh = proc_mesh.spawn("bench", BenchActor)

    receiver = mesh.slice(hosts=0, procs=0)
    sender = mesh.slice(hosts=1, procs=0)

    print(f"\n{'Streams':>8}  {'Transfer':>10}  {'Throughput':>12}")
    print("-" * 36)

    for num_streams in [1, 4, 8, 16, 32, 64]:
        # Setup receiver
        result = receiver.setup_receiver.call(num_streams, total_bytes).get()
        data_addrs = [v for _, v in result][0]

        # Start receiver waiting (non-blocking)
        recv_future = receiver.wait_receive.call()

        # Run sender
        result = sender.send_data.call(
            data_addrs, total_bytes, num_blocks, block_size
        ).get()
        elapsed, gb_s = [v for _, v in result][0]

        # Wait for receiver
        recv_future.get()

        print(f"{num_streams:>8}  {elapsed:>9.2f}s  {gb_s:>10.2f} GB/s")

    print()
    job.kill()
    print("Done.")


cloudpickle.register_pickle_by_value(sys.modules[__name__])

if __name__ == "__main__":
    fire.Fire(main)
