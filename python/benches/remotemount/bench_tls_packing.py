# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Benchmark remotemount transfer throughput.

Measures cold transfer, no-change skip, and incremental re-transfer
using RDMA-based flat fan-out.

Usage:
    buck run fbcode//monarch/python/benches/remotemount:bench_tls_packing
    buck run fbcode//monarch/python/benches/remotemount:bench_tls_packing -- --backend mast --num_hosts 2
    buck run fbcode//monarch/python/benches/remotemount:bench_tls_packing -- --data_size_mb 200
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import tempfile
import time

import fire
from monarch.actor import Actor, endpoint, this_host


class TestActor(Actor):
    @endpoint
    def check_file(self, path):
        """Read a file from the mount and return its contents."""
        try:
            with open(path) as f:
                return f.read()
        except Exception as e:
            return f"ERROR: {e}"

    @endpoint
    def list_dir(self, path):
        """List files in a directory."""
        try:
            return sorted(os.listdir(path))
        except Exception as e:
            return f"ERROR: {e}"


def _format_throughput(nbytes, seconds):
    if seconds <= 0:
        return "inf"
    mbps = nbytes / seconds / (1024 * 1024)
    if mbps >= 1024:
        return f"{mbps / 1024:.2f} GB/s"
    return f"{mbps:.0f} MB/s"


def bench_cold_transfer(host_mesh, test_dir, backend):
    """Run a single cold transfer and return open_time."""
    from monarch.remotemount import remotemount

    mount_point = (
        test_dir if backend == "mast" else tempfile.mkdtemp(prefix="remotemount_mnt_")
    )

    handler = remotemount(host_mesh, test_dir, mount_point, backend=backend)

    t0 = time.time()
    handler.open()
    open_time = time.time() - t0

    handler.close()

    if mount_point != test_dir:
        shutil.rmtree(mount_point, ignore_errors=True)

    return open_time


def bench_incremental_cycle(host_mesh, test_dir, test_actors, backend):
    """Run full open/skip/modify/re-transfer cycle. Returns dict of timings."""
    from monarch.remotemount import remotemount

    mount_point = (
        test_dir if backend == "mast" else tempfile.mkdtemp(prefix="remotemount_mnt_")
    )

    handler = remotemount(host_mesh, test_dir, mount_point, backend=backend)

    # Cold transfer.
    t0 = time.time()
    handler.open()
    cold_time = time.time() - t0

    # Verify content.
    results = test_actors.check_file.call(
        os.path.join(mount_point, "config.json")
    ).get()
    for point, content in results:
        assert "ERROR" not in str(content), f"rank{point.rank}: {content}"

    # No-change skip.
    handler.close()
    t0 = time.time()
    handler.open()
    skip_time = time.time() - t0

    # Modify and re-transfer.
    handler.close()
    with open(os.path.join(test_dir, "config.json"), "w") as f:
        f.write('{"lr": 0.01, "epochs": 20}\n')

    t0 = time.time()
    handler.open()
    retransfer_time = time.time() - t0

    # Verify updated content.
    results = test_actors.check_file.call(
        os.path.join(mount_point, "config.json")
    ).get()
    for point, content in results:
        assert "0.01" in str(content), f"rank{point.rank}: {content}"

    # Restore original content for next run.
    handler.close()
    with open(os.path.join(test_dir, "config.json"), "w") as f:
        f.write('{"lr": 0.001, "epochs": 10}\n')

    if mount_point != test_dir:
        shutil.rmtree(mount_point, ignore_errors=True)

    return {
        "cold": cold_time,
        "skip": skip_time,
        "retransfer": retransfer_time,
    }


def main(
    backend="local",
    num_hosts=1,
    gpus_per_host=1,
    host_type="gb300",
    locality_constraints="",
    data_size_mb=50,
    runs=3,
    verbose=True,
) -> None:
    if verbose:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )

    from monarch.config import configure

    configure(
        enable_log_forwarding=True,
        tail_log_lines=100,
        host_spawn_ready_timeout="300s",
        mesh_proc_spawn_max_idle="300s",
        message_delivery_timeout="600s",
    )

    # Create test directory.
    test_dir = tempfile.mkdtemp(prefix="remotemount_bench_")
    os.makedirs(os.path.join(test_dir, "src"), exist_ok=True)

    for i in range(5):
        with open(os.path.join(test_dir, "src", f"mod_{i}.py"), "w") as f:
            f.write(f"# Module {i}\ndef func(): return {i}\n")

    with open(os.path.join(test_dir, "config.json"), "w") as f:
        f.write('{"lr": 0.001, "epochs": 10}\n')

    with open(os.path.join(test_dir, "data.bin"), "wb") as f:
        f.write(os.urandom(data_size_mb * 1024 * 1024))

    total = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, fs in os.walk(test_dir)
        for f in fs
    )

    print("=" * 78)
    print(f"Remotemount benchmark — {total / (1024 * 1024):.0f} MB payload")
    print("=" * 78)
    print(f"Backend: {backend}\n")

    # Set up host mesh.
    job = None
    if backend == "mast":
        from monarch.actor import enable_transport
        from monarch.job.meta import MASTJob

        enable_transport("metatls-hostname")
        lc = None
        if locality_constraints and locality_constraints != "":
            lc = locality_constraints.split(";")
        job = MASTJob(
            hpcIdentity="hyper_monarch",
            hpcJobOncall="monarch",
            rmAttribution="msl_infra_pytorch_dev",
            hpcClusterUuid="MastGenAICluster",
            useStrictName=True,
            localityConstraints=lc,
            env={
                "PYTHONDONTWRITEBYTECODE": "1",
                "MAST_PRECHECK_SKIP_TIME_CONSUMING_CHECKS": "1",
                # Force lazy symbol binding on aarch64 to avoid
                # ImportError on undefined symbols never called at runtime.
                "LD_BIND_LAZY": "1",
                "MONARCH_RDMA_TCP_FALLBACK_PARALLELISM": os.environ.get(
                    "MONARCH_RDMA_TCP_FALLBACK_PARALLELISM", "1"
                ),
            },
        )
        job.add_mesh("workers", num_hosts, host_type=host_type)
        job.add_directory(tempfile.mkdtemp())
        host_meshes = job.state()
        host_mesh = host_meshes.workers
    else:
        host_mesh = this_host()

    procs = host_mesh.spawn_procs(per_host={"gpus": gpus_per_host})
    test_actors = procs.spawn("TestActor", TestActor)

    # ---- Cold transfer runs ----
    data_bin = os.path.join(test_dir, "data.bin")
    data_size = os.path.getsize(data_bin)

    print(f"--- Cold transfer ({runs} runs) ---")
    print(f"{'Run':>5}  {'Cold':>8}  {'Throughput':>12}")
    print("-" * 30)

    for run in range(runs):
        # Rewrite data.bin to force cold transfer.
        with open(data_bin, "wb") as f:
            remaining = data_size
            while remaining > 0:
                chunk = min(remaining, 64 * 1024 * 1024)
                f.write(os.urandom(chunk))
                remaining -= chunk
        t = bench_cold_transfer(host_mesh, test_dir, backend)
        print(f"{run + 1:>5}  {t:>7.2f}s  {_format_throughput(total, t):>12}")

    # ---- Incremental cycle ----
    print("\n--- Incremental cycle (cold / skip / re-transfer) ---")
    timings = bench_incremental_cycle(host_mesh, test_dir, test_actors, backend)
    print(
        f"  Cold:       {timings['cold']:>7.2f}s  {_format_throughput(total, timings['cold']):>12}"
    )
    print(f"  Skip:       {timings['skip']:>7.2f}s")
    print(f"  Retransfer: {timings['retransfer']:>7.2f}s")

    print()

    # Cleanup.
    if job is not None:
        job.kill()
        print("MAST job killed.")
    shutil.rmtree(test_dir, ignore_errors=True)
    print("Done.")


# Register for pickle-by-value so TestActor can be sent to remote workers.
import cloudpickle  # noqa: E402

cloudpickle.register_pickle_by_value(sys.modules[__name__])

if __name__ == "__main__":
    fire.Fire(main)
