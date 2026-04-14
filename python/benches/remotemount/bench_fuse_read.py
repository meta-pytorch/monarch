#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Benchmark FUSE read latency and throughput.

Measures:
  1. Small-read latency (many 4KB reads)
  2. Large-read throughput (sequential 1MB reads)
  3. stat() latency (metadata lookups)
  4. Baseline: same operations on a regular tmpfs file for comparison
"""

import os
import tempfile
import time

from monarch._rust_bindings.monarch_extension.chunked_fuse import mount_chunked_fuse


def make_attr(mode=0o100644, size=0, nlink=1):
    now = time.time()
    return {
        "st_atime": now,
        "st_ctime": now,
        "st_gid": os.getgid(),
        "st_mode": mode,
        "st_mtime": now,
        "st_nlink": nlink,
        "st_size": size,
        "st_uid": os.getuid(),
    }


def bench_reads(path, read_size, num_reads, label):
    """Read read_size bytes num_reads times, report latency and throughput."""
    # Warm up
    with open(path, "rb") as f:
        f.read(read_size)

    t0 = time.time()
    for _ in range(num_reads):
        with open(path, "rb") as f:
            f.read(read_size)
    elapsed = time.time() - t0

    total_bytes = read_size * num_reads
    lat_us = elapsed / num_reads * 1e6
    tp_mbs = total_bytes / elapsed / 1e6
    print(
        f"  {label}: {num_reads} x {read_size // 1024}KB "
        f"in {elapsed:.3f}s — {lat_us:.1f} µs/read, {tp_mbs:.1f} MB/s"
    )
    return elapsed


def bench_stats(path, num_stats, label):
    """stat() a file num_stats times."""
    # Warm up
    os.stat(path)

    t0 = time.time()
    for _ in range(num_stats):
        os.stat(path)
    elapsed = time.time() - t0

    lat_us = elapsed / num_stats * 1e6
    print(f"  {label}: {num_stats} x stat() in {elapsed:.3f}s — {lat_us:.1f} µs/stat")
    return elapsed


def bench_open_read_close(path, read_size, num_iters, label):
    """Measure open+read+close cycle."""
    fd = os.open(path, os.O_RDONLY)
    os.read(fd, read_size)
    os.close(fd)

    t0 = time.time()
    for _ in range(num_iters):
        fd = os.open(path, os.O_RDONLY)
        os.read(fd, read_size)
        os.close(fd)
    elapsed = time.time() - t0

    lat_us = elapsed / num_iters * 1e6
    print(
        f"  {label}: {num_iters} x open+read+close "
        f"in {elapsed:.3f}s — {lat_us:.1f} µs/iter"
    )
    return elapsed


def main():
    file_size = 10 * 1024 * 1024  # 10 MB
    data = os.urandom(file_size)
    num_small = 5000
    num_large = 500
    num_stats = 10000

    # --- Baseline: regular tmpfs file ---
    print("=== Baseline (tmpfs) ===")
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "data.bin")
        with open(path, "wb") as f:
            f.write(data)
        bench_reads(path, 4096, num_small, "4KB reads")
        bench_reads(path, 1024 * 1024, num_large, "1MB reads")
        bench_stats(path, num_stats, "stat()")
        bench_open_read_close(path, 4096, num_small, "open+4KB+close")

    metadata = {
        "/": {
            "attr": make_attr(mode=0o40755, size=4096, nlink=2),
            "children": ["data.bin"],
        },
        "/data.bin": {
            "attr": make_attr(size=file_size),
            "global_offset": 0,
            "file_len": file_size,
        },
    }

    # --- FUSE (TTL=200ms hardcoded) ---
    print("\n=== FUSE (TTL=200ms) ===")
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(metadata, [memoryview(data)], file_size, mnt)
        path = os.path.join(mnt, "data.bin")
        bench_reads(path, 4096, num_small, "4KB reads")
        bench_reads(path, 1024 * 1024, num_large, "1MB reads")
        bench_stats(path, num_stats, "stat()")
        bench_open_read_close(path, 4096, num_small, "open+4KB+close")
        handle.unmount()


if __name__ == "__main__":
    main()
