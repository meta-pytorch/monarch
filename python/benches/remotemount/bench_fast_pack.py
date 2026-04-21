# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Benchmark for fast_pack: measures packing throughput (GB/s) across
varying directory sizes with randomly sized files.

Usage:
    buck run @fbcode//mode/dev-nosan fbcode//monarch/python/benches:bench_fast_pack
    buck run @fbcode//mode/dev-nosan fbcode//monarch/python/benches:bench_fast_pack -- --sizes 1,4,16,64
"""

from __future__ import annotations

import argparse
import gc
import math
import os
import random
import tempfile
import time


def create_random_directory(
    base_dir: str,
    total_bytes: int,
    min_file: int = 1024,
    max_file: int = 256 * 1024 * 1024,
) -> tuple[int, list[int]]:
    """Create a directory tree with randomly sized files summing to ~total_bytes.

    Returns (actual_bytes_written, list_of_file_sizes).
    """
    written = 0
    file_idx = 0
    file_sizes: list[int] = []
    subdirs = ["", "models", "data/train", "data/val", "config"]
    for d in subdirs:
        os.makedirs(os.path.join(base_dir, d), exist_ok=True)

    while written < total_bytes:
        remaining = total_bytes - written
        size = min(random.randint(min_file, max_file), remaining)
        subdir = random.choice(subdirs)
        path = os.path.join(base_dir, subdir, f"file_{file_idx:06d}.bin")
        with open(path, "wb") as f:
            left = size
            while left > 0:
                chunk = min(left, 1024 * 1024)
                f.write(os.urandom(chunk))
                left -= chunk
        written += size
        file_sizes.append(size)
        file_idx += 1

    return written, file_sizes


def format_bytes(n: int) -> str:
    """Human-readable byte size."""
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024**3):.1f} GB"
    if n >= 1024 * 1024:
        return f"{n / (1024**2):.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def file_size_stats(sizes: list[int]) -> str:
    """Return mean/stddev/min/max summary."""
    n = len(sizes)
    if n == 0:
        return "no files"
    mean = sum(sizes) / n
    variance = sum((s - mean) ** 2 for s in sizes) / n
    stddev = math.sqrt(variance)
    return (
        f"{n} files, "
        f"mean={format_bytes(int(mean))}, "
        f"stddev={format_bytes(int(stddev))}, "
        f"min={format_bytes(min(sizes))}, "
        f"max={format_bytes(max(sizes))}"
    )


def bench_pack(total_gb: float, iterations: int = 3) -> None:
    """Benchmark pack_directory_chunked at a given total size."""
    from monarch.remotemount.fast_pack import pack_directory_chunked

    total_bytes = int(total_gb * 1024 * 1024 * 1024)

    with tempfile.TemporaryDirectory() as base_dir:
        print(f"\n--- {total_gb:.1f} GB ---")
        print("Creating test files...", end=" ", flush=True)
        t0 = time.monotonic()
        actual, file_sizes = create_random_directory(base_dir, total_bytes)
        create_time = time.monotonic() - t0
        actual_gb = actual / (1024**3)
        print(f"{actual_gb:.2f} GB in {create_time:.1f}s")
        print(f"  {file_size_stats(file_sizes)}")

        # Warmup (first run populates page cache).
        print("  Warmup...", end=" ", flush=True)
        t0 = time.monotonic()
        meta, staging_mv, chunks, hashes, _pi = pack_directory_chunked(base_dir)
        warmup_time = time.monotonic() - t0
        warmup_gbs = actual_gb / warmup_time
        print(f"{warmup_time:.2f}s ({warmup_gbs:.2f} GB/s)")

        # Verify data integrity.
        total_packed = sum(len(c) for c in chunks)
        assert total_packed == actual, f"packed {total_packed} != actual {actual}"
        assert len(hashes) > 0, "expected at least one hash"

        del meta, staging_mv, chunks, hashes, _pi
        gc.collect()

        # Timed runs.
        times = []
        nhashes = 0
        for _i in range(iterations):
            t0 = time.monotonic()
            meta, staging_mv, chunks, hashes, _pi = pack_directory_chunked(base_dir)
            elapsed = time.monotonic() - t0
            times.append(elapsed)
            nhashes = len(hashes)
            del meta, staging_mv, chunks, hashes, _pi
            gc.collect()

        avg = sum(times) / len(times)
        best = min(times)
        gbs_avg = actual_gb / avg
        gbs_best = actual_gb / best
        print(f"  Avg:  {avg:.2f}s ({gbs_avg:.2f} GB/s)")
        print(f"  Best: {best:.2f}s ({gbs_best:.2f} GB/s)")
        print(f"  Hashes/run: {nhashes}")


def bench_scaling(size_gb: float = 4.0) -> None:
    """Benchmark with CPU affinity restricted to 1, 2, 4, 8, 16 cores."""
    from monarch.remotemount.fast_pack import pack_directory_chunked

    total_bytes = int(size_gb * 1024 * 1024 * 1024)
    available = os.cpu_count() or 1

    print(f"\n=== Scaling test ({size_gb:.0f} GB, {available} CPUs available) ===")

    with tempfile.TemporaryDirectory() as base_dir:
        print("Creating test files...", end=" ", flush=True)
        t0 = time.monotonic()
        actual, file_sizes = create_random_directory(base_dir, total_bytes)
        actual_gb = actual / (1024**3)
        print(f"{actual_gb:.2f} GB in {time.monotonic() - t0:.1f}s")
        print(f"  {file_size_stats(file_sizes)}")

        # Warmup.
        meta, staging_mv, chunks, hashes, _pi = pack_directory_chunked(base_dir)
        del meta, staging_mv, chunks, hashes, _pi
        gc.collect()

        core_counts = [c for c in [1, 2, 4, 8, 16] if c <= available]
        all_cpus = list(range(available))

        print(f"\n  {'Cores':>5}  {'Time':>7}  {'GB/s':>7}")
        print(f"  {'-----':>5}  {'-----':>7}  {'-----':>7}")

        for ncores in core_counts:
            # Restrict this process to ncores CPUs.
            target_cpus = all_cpus[:ncores]
            os.sched_setaffinity(0, target_cpus)

            times = []
            for _i in range(3):
                t0 = time.monotonic()
                meta, staging_mv, chunks, hashes, _pi = pack_directory_chunked(base_dir)
                elapsed = time.monotonic() - t0
                times.append(elapsed)
                del meta, staging_mv, chunks, hashes, _pi
                gc.collect()

            best = min(times)
            gbs = actual_gb / best
            print(f"  {ncores:>5}  {best:>6.2f}s  {gbs:>6.2f}")

        # Restore full affinity.
        os.sched_setaffinity(0, all_cpus)


def main() -> None:
    parser = argparse.ArgumentParser(description="fast_pack benchmark")
    parser.add_argument(
        "--sizes",
        type=str,
        default="0.5,1,4,16,64",
        help="Comma-separated sizes in GB (default: 0.5,1,4,16,64)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Timed iterations per size (default: 3)",
    )
    parser.add_argument(
        "--scaling",
        action="store_true",
        help="Run CPU scaling test (restricts affinity to 1,2,4,8,16 cores)",
    )
    parser.add_argument(
        "--scaling-size",
        type=float,
        default=4.0,
        help="Size in GB for scaling test (default: 4)",
    )
    args = parser.parse_args()

    cpus = os.cpu_count() or 1
    print(f"fast_pack benchmark ({cpus} CPUs available)")

    if args.scaling:
        bench_scaling(size_gb=args.scaling_size)
    else:
        sizes = [float(s) for s in args.sizes.split(",")]
        print(f"Sizes: {sizes} GB, iterations: {args.iterations}")
        for size in sizes:
            bench_pack(size, iterations=args.iterations)

    print("\nDone.")


if __name__ == "__main__":
    main()
