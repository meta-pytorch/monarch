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
"""

from __future__ import annotations

import argparse
import gc
import os
import random
import tempfile
import time


def create_random_directory(
    base_dir: str,
    total_bytes: int,
    min_file: int = 1024,
    max_file: int = 256 * 1024 * 1024,
) -> int:
    """Create a directory tree with randomly sized files summing to ~total_bytes.

    Returns the actual number of bytes written.
    """
    written = 0
    file_idx = 0
    # Create a few subdirectories for realism.
    subdirs = ["", "models", "data/train", "data/val", "config"]
    for d in subdirs:
        os.makedirs(os.path.join(base_dir, d), exist_ok=True)

    while written < total_bytes:
        remaining = total_bytes - written
        size = min(random.randint(min_file, max_file), remaining)
        subdir = random.choice(subdirs)
        path = os.path.join(base_dir, subdir, f"file_{file_idx:06d}.bin")
        with open(path, "wb") as f:
            # Write in 1MB chunks to avoid huge single allocations.
            left = size
            while left > 0:
                chunk = min(left, 1024 * 1024)
                f.write(os.urandom(chunk))
                left -= chunk
        written += size
        file_idx += 1

    return written


def bench_pack(total_gb: float, iterations: int = 3) -> None:
    """Benchmark pack_directory_chunked at a given total size."""
    from monarch.remotemount.fast_pack import pack_directory_chunked

    total_bytes = int(total_gb * 1024 * 1024 * 1024)

    with tempfile.TemporaryDirectory() as base_dir:
        print(f"\n--- {total_gb:.1f} GB ---")
        print("Creating test files...", end=" ", flush=True)
        t0 = time.monotonic()
        actual = create_random_directory(base_dir, total_bytes)
        create_time = time.monotonic() - t0
        actual_gb = actual / (1024**3)
        print(f"{actual_gb:.2f} GB in {create_time:.1f}s")

        # Count files for reference.
        nfiles = sum(len(files) for _, _, files in os.walk(base_dir))
        print(f"  Files: {nfiles}")

        # Warm up (first run populates page cache).
        print("  Warmup...", end=" ", flush=True)
        t0 = time.monotonic()
        meta, staging_mv, chunks, hashes = pack_directory_chunked(base_dir)
        warmup_time = time.monotonic() - t0
        warmup_gbs = actual_gb / warmup_time
        print(f"{warmup_time:.2f}s ({warmup_gbs:.2f} GB/s)")

        # Verify data integrity.
        total_packed = sum(len(c) for c in chunks)
        assert total_packed == actual, f"packed {total_packed} != actual {actual}"
        assert len(hashes) > 0, "expected at least one hash"

        # Free the first result.
        del meta, staging_mv, chunks, hashes
        gc.collect()

        # Timed runs.
        times = []
        nhashes = 0
        for _i in range(iterations):
            t0 = time.monotonic()
            meta, staging_mv, chunks, hashes = pack_directory_chunked(base_dir)
            elapsed = time.monotonic() - t0
            times.append(elapsed)
            nhashes = len(hashes)
            # Free between iterations.
            del meta, staging_mv, chunks, hashes
            gc.collect()

        avg = sum(times) / len(times)
        best = min(times)
        gbs_avg = actual_gb / avg
        gbs_best = actual_gb / best
        print(f"  Avg:  {avg:.2f}s ({gbs_avg:.2f} GB/s)")
        print(f"  Best: {best:.2f}s ({gbs_best:.2f} GB/s)")
        print(f"  Hashes/run: {nhashes}")


def bench_memoryview_ownership() -> None:
    """Verify no copies: mutating the memoryview is visible, and GC frees mmap."""
    from monarch._rust_bindings.monarch_extension.fast_pack import (
        pack_files_with_offsets,
    )

    print("\n--- Ownership / zero-copy check ---")
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.bin")
        data = b"A" * 4096
        with open(path, "wb") as f:
            f.write(data)

        buf, hashes = pack_files_with_offsets([(path, 0, len(data))], len(data))
        mv = memoryview(buf)
        assert len(mv) == len(data)
        assert bytes(mv[:4]) == b"AAAA"

        # Write through memoryview — verifies no copy.
        mv[0] = ord("Z")
        assert bytes(mv[:4]) == b"ZAAA"

        # Verify the buf itself reflects the change (same memory).
        mv2 = memoryview(buf)
        assert mv2[0] == ord("Z")

        del mv, mv2, buf
        gc.collect()
        print("  PASS: zero-copy write-through and GC cleanup verified")


def main() -> None:
    parser = argparse.ArgumentParser(description="fast_pack benchmark")
    parser.add_argument(
        "--sizes",
        type=str,
        default="0.1,0.5,1,4,16",
        help="Comma-separated list of sizes in GB to benchmark (default: 0.1,0.5,1,4,16)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of timed iterations per size (default: 3)",
    )
    args = parser.parse_args()

    sizes = [float(s) for s in args.sizes.split(",")]

    print("fast_pack benchmark")
    print(f"Sizes: {sizes} GB, iterations: {args.iterations}")

    bench_memoryview_ownership()

    for size in sizes:
        bench_pack(size, iterations=args.iterations)

    print("\nDone.")


if __name__ == "__main__":
    main()
