# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Benchmark metadata encoding for chunked_fuse mount.

Measures the cost of json.dumps() for the metadata dict passed from
Python to Rust at mount time. Helps decide whether JSON is the right
serialization format or if we should pass the dict directly via PyO3.

Usage:
    buck run fbcode//monarch/python/benches:bench_metadata_encoding
"""

from __future__ import annotations

import json
import time
from typing import Any


def make_synthetic_metadata(num_files: int, num_dirs: int = 0) -> dict[str, Any]:
    """Build a metadata dict similar to what the packer produces for a mount."""
    meta = {}
    now = time.time()
    base_attr = {
        "st_atime": now,
        "st_ctime": now,
        "st_gid": 1000,
        "st_mode": 0o40755,
        "st_mtime": now,
        "st_nlink": 2,
        "st_size": 4096,
        "st_uid": 1000,
    }

    # Root directory
    children = []
    for i in range(num_dirs):
        children.append(f"dir_{i}")
    for i in range(num_files):
        children.append(f"file_{i}.bin")
    meta["/"] = {"attr": base_attr.copy(), "children": children}

    # Subdirectories
    for i in range(num_dirs):
        dir_children = [f"sub_file_{j}.txt" for j in range(5)]
        meta[f"/dir_{i}"] = {"attr": base_attr.copy(), "children": dir_children}
        offset = num_files * 1024 + i * 5 * 256
        for j in range(5):
            file_attr = base_attr.copy()
            file_attr["st_mode"] = 0o100644
            file_attr["st_size"] = 256
            file_attr["st_nlink"] = 1
            # pyrefly: ignore [unsupported-operation]
            meta[f"/dir_{i}/sub_file_{j}.txt"] = {
                "attr": file_attr,
                "global_offset": offset + j * 256,
                "file_len": 256,
            }

    # Top-level files
    offset = 0
    for i in range(num_files):
        file_size = 1024  # 1KB each
        file_attr = base_attr.copy()
        file_attr["st_mode"] = 0o100644
        file_attr["st_size"] = file_size
        file_attr["st_nlink"] = 1
        # pyrefly: ignore [unsupported-operation]
        meta[f"/file_{i}.bin"] = {
            "attr": file_attr,
            "global_offset": offset,
            "file_len": file_size,
        }
        offset += file_size

    return meta


def bench_json_encode(meta: dict[str, Any], iterations: int = 100) -> tuple[float, int]:
    """Return (avg_seconds, json_size_bytes)."""
    # Warm up
    json_str = json.dumps(meta)
    json_size = len(json_str.encode("utf-8"))

    start = time.perf_counter()
    for _ in range(iterations):
        json.dumps(meta)
    elapsed = time.perf_counter() - start
    return elapsed / iterations, json_size


def bench_json_decode(json_str: str, iterations: int = 100) -> float:
    """Return avg_seconds for json.loads."""
    # Warm up
    json.loads(json_str)

    start = time.perf_counter()
    for _ in range(iterations):
        json.loads(json_str)
    elapsed = time.perf_counter() - start
    return elapsed / iterations


def format_size(nbytes: int) -> str:
    if nbytes < 1024:
        return f"{nbytes} B"
    elif nbytes < 1024 * 1024:
        return f"{nbytes / 1024:.1f} KB"
    else:
        return f"{nbytes / (1024 * 1024):.1f} MB"


def format_time(seconds: float) -> str:
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.0f} us"
    elif seconds < 1.0:
        return f"{seconds * 1000:.1f} ms"
    else:
        return f"{seconds:.2f} s"


def main() -> None:
    print("=" * 72)
    print("Metadata JSON encoding benchmark")
    print("=" * 72)

    # --- Synthetic metadata ---
    print("\n--- Synthetic metadata (json.dumps + json.loads) ---")
    print(
        f"{'Files':>10}  {'Entries':>8}  {'JSON size':>10}  {'dumps':>10}  {'loads':>10}"
    )
    print("-" * 60)

    for num_files in [100, 1_000, 10_000, 100_000]:
        num_dirs = num_files // 20
        meta = make_synthetic_metadata(num_files, num_dirs)
        num_entries = len(meta)

        iters = max(10, 1000 // (num_files // 100))
        dumps_time, json_size = bench_json_encode(meta, iterations=iters)
        json_str = json.dumps(meta)
        loads_time = bench_json_decode(json_str, iterations=iters)

        print(
            f"{num_files:>10,}  {num_entries:>8,}  {format_size(json_size):>10}  "
            f"{format_time(dumps_time):>10}  {format_time(loads_time):>10}"
        )

    print()


if __name__ == "__main__":
    main()
