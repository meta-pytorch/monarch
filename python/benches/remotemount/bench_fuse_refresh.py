#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Benchmark FUSE refresh scenarios.

Based on TestFuseRefresh test cases. Measures the cost of each refresh
pattern: unchanged data, changed data, added/deleted files, offset
shifts (defrag), sequential reads across refresh, and many-files-one-changed.
"""

import os
import tempfile
import time

from monarch._rust_bindings.monarch_extension.chunked_fuse import mount_chunked_fuse
from monarch.remotemount.fast_pack import pack_directory_chunked


def _make_attr(mode=0o100644, size=0, nlink=1):
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


def _dir_attr(size=4096, nlink=2):
    return _make_attr(mode=0o40755, size=size, nlink=nlink)


def _file_attr(size):
    return _make_attr(mode=0o100644, size=size)


def _file_meta(name, data, offset=0):
    return {
        f"/{name}": {
            "attr": _file_attr(len(data)),
            "global_offset": offset,
            "file_len": len(data),
        }
    }


def _timed(label, fn):
    t0 = time.time()
    result = fn()
    elapsed = time.time() - t0
    print(f"  {label}: {elapsed * 1000:.1f} ms")
    return result, elapsed


def bench_unchanged_file():
    """Refresh with identical content."""
    content = os.urandom(1024 * 1024)
    metadata = {
        "/": {"attr": _dir_attr(), "children": ["f.bin"]},
        "/f.bin": {
            "attr": _file_attr(len(content)),
            "global_offset": 0,
            "file_len": len(content),
        },
    }
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(metadata, [memoryview(content)], len(content), mnt)
        _timed(
            "refresh (unchanged 1MB)",
            lambda: handle.refresh(metadata, [memoryview(content)], len(content)),
        )
        _timed(
            "read after refresh", lambda: open(os.path.join(mnt, "f.bin"), "rb").read()
        )
        handle.unmount()


def bench_changed_file():
    """Refresh with different content."""
    v1 = os.urandom(1024 * 1024)
    v2 = os.urandom(1024 * 1024)

    def meta_fn(d):
        return {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(len(d)),
                "global_offset": 0,
                "file_len": len(d),
            },
        }

    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(meta_fn(v1), [memoryview(v1)], len(v1), mnt)
        _timed(
            "refresh (changed 1MB)",
            lambda: handle.refresh(meta_fn(v2), [memoryview(v2)], len(v2)),
        )
        _, elapsed = _timed(
            "read after refresh", lambda: open(os.path.join(mnt, "f.bin"), "rb").read()
        )
        handle.unmount()


def bench_added_file():
    """Refresh that adds a new file."""
    v1 = b"aaa"
    meta1 = {
        "/": {"attr": _dir_attr(), "children": ["a.txt"]},
        "/a.txt": {"attr": _file_attr(3), "global_offset": 0, "file_len": 3},
    }
    v2 = b"aaabbb"
    meta2 = {
        "/": {"attr": _dir_attr(), "children": ["a.txt", "b.txt"]},
        "/a.txt": {"attr": _file_attr(3), "global_offset": 0, "file_len": 3},
        "/b.txt": {"attr": _file_attr(3), "global_offset": 3, "file_len": 3},
    }
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(meta1, [memoryview(v1)], 3, mnt)
        _timed(
            "refresh (add file)",
            lambda: handle.refresh(meta2, [memoryview(v2)], len(v2)),
        )
        _timed("listdir after add", lambda: os.listdir(mnt))
        _timed("read new file", lambda: open(os.path.join(mnt, "b.txt"), "rb").read())
        handle.unmount()


def bench_deleted_file():
    """Refresh that removes a file."""
    v1 = b"aaabbb"
    meta1 = {
        "/": {"attr": _dir_attr(), "children": ["a.txt", "b.txt"]},
        "/a.txt": {"attr": _file_attr(3), "global_offset": 0, "file_len": 3},
        "/b.txt": {"attr": _file_attr(3), "global_offset": 3, "file_len": 3},
    }
    v2 = b"aaa"
    meta2 = {
        "/": {"attr": _dir_attr(), "children": ["a.txt"]},
        "/a.txt": {"attr": _file_attr(3), "global_offset": 0, "file_len": 3},
    }
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(meta1, [memoryview(v1)], len(v1), mnt)
        _timed(
            "refresh (delete file)", lambda: handle.refresh(meta2, [memoryview(v2)], 3)
        )
        _timed("listdir after delete", lambda: os.listdir(mnt))
        handle.unmount()


def bench_defrag_offset_shift():
    """Refresh where global_offset changes but content is same."""
    file_data = os.urandom(1024 * 1024)
    buf1 = b"\x00" * (1024 * 1024) + file_data
    meta1 = {
        "/": {"attr": _dir_attr(), "children": ["f.bin"]},
        "/f.bin": {
            "attr": _file_attr(len(file_data)),
            "global_offset": 1024 * 1024,
            "file_len": len(file_data),
        },
    }
    meta2 = {
        "/": {"attr": _dir_attr(), "children": ["f.bin"]},
        "/f.bin": {
            "attr": _file_attr(len(file_data)),
            "global_offset": 0,
            "file_len": len(file_data),
        },
    }
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(meta1, [memoryview(buf1)], len(buf1), mnt)
        _timed(
            "refresh (defrag 1MB, offset shift)",
            lambda: handle.refresh(meta2, [memoryview(file_data)], len(file_data)),
        )
        _timed(
            "read after defrag", lambda: open(os.path.join(mnt, "f.bin"), "rb").read()
        )
        handle.unmount()


def bench_sequential_read_across_refresh():
    """Read half, refresh, read other half."""
    half = 512 * 1024
    content = os.urandom(half * 2)
    metadata = {
        "/": {"attr": _dir_attr(), "children": ["f.bin"]},
        "/f.bin": {
            "attr": _file_attr(len(content)),
            "global_offset": 0,
            "file_len": len(content),
        },
    }
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(metadata, [memoryview(content)], len(content), mnt)
        fh = open(os.path.join(mnt, "f.bin"), "rb")
        _timed("read first 512KB", lambda: fh.read(half))
        _timed(
            "refresh (unchanged)",
            lambda: handle.refresh(metadata, [memoryview(content)], len(content)),
        )
        _timed("read second 512KB", lambda: fh.read(half))
        fh.close()
        handle.unmount()


def bench_many_files_one_changed():
    """50 files, change one, measure refresh + read-all."""
    num_files = 50
    file_size = 64 * 1024
    data = os.urandom(num_files * file_size)
    children = [f"f_{i:03d}.bin" for i in range(num_files)]
    metadata = {"/": {"attr": _dir_attr(), "children": children}}
    for i in range(num_files):
        metadata[f"/f_{i:03d}.bin"] = {
            "attr": _file_attr(file_size),
            "global_offset": i * file_size,
            "file_len": file_size,
        }

    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(metadata, [memoryview(data)], len(data), mnt)
        # Change file 25.
        new_data = bytearray(data)
        new_data[25 * file_size : 26 * file_size] = os.urandom(file_size)
        new_data = bytes(new_data)

        _timed(
            f"refresh (1/{num_files} files changed, {num_files * file_size // 1024}KB total)",
            lambda: handle.refresh(metadata, [memoryview(new_data)], len(new_data)),
        )

        def read_all():
            for i in range(num_files):
                with open(os.path.join(mnt, f"f_{i:03d}.bin"), "rb") as f:
                    f.read()

        _timed(f"read all {num_files} files after refresh", read_all)
        handle.unmount()


def bench_multiple_rapid_refreshes():
    """20 rapid refreshes, measure total and per-refresh time."""

    def meta_fn(sz):
        return {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(sz),
                "global_offset": 0,
                "file_len": sz,
            },
        }

    content = os.urandom(1024 * 1024)
    n = 20
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(
            meta_fn(len(content)), [memoryview(content)], len(content), mnt
        )
        t0 = time.time()
        for _i in range(n):
            new_content = os.urandom(1024 * 1024)
            handle.refresh(
                meta_fn(len(new_content)), [memoryview(new_content)], len(new_content)
            )
        elapsed = time.time() - t0
        print(
            f"  {n} refreshes (1MB each): {elapsed * 1000:.1f} ms total, "
            f"{elapsed / n * 1000:.1f} ms/refresh"
        )
        handle.unmount()


def bench_end_to_end_pack_refresh():
    """Full round-trip: pack, mount, modify, re-pack, refresh, read."""
    with tempfile.TemporaryDirectory() as src:
        data = os.urandom(10 * 1024 * 1024)
        with open(os.path.join(src, "data.bin"), "wb") as f:
            f.write(data)

        _timed("initial pack (10MB)", lambda: pack_directory_chunked(src))
        meta, _, chunks, _, _ = pack_directory_chunked(src)

        with tempfile.TemporaryDirectory() as mnt:
            chunk_size = len(bytes(chunks[0])) if chunks else 1
            handle = mount_chunked_fuse(meta, chunks, chunk_size, mnt)

            _timed(
                "initial read (10MB)",
                lambda: open(os.path.join(mnt, "data.bin"), "rb").read(),
            )

            # Modify and re-pack.
            with open(os.path.join(src, "data.bin"), "wb") as f:
                f.write(os.urandom(10 * 1024 * 1024))

            _timed("re-pack (10MB)", lambda: pack_directory_chunked(src))
            meta2, _, chunks2, _, _ = pack_directory_chunked(src)
            chunk_size2 = len(bytes(chunks2[0])) if chunks2 else 1

            _timed(
                "refresh (10MB)", lambda: handle.refresh(meta2, chunks2, chunk_size2)
            )

            _timed(
                "read after refresh (10MB)",
                lambda: open(os.path.join(mnt, "data.bin"), "rb").read(),
            )

            handle.unmount()


def main():
    print("=== FUSE Refresh Benchmarks (TTL=200ms, settle=600ms) ===\n")

    print("--- Unchanged file ---")
    bench_unchanged_file()

    print("\n--- Changed file ---")
    bench_changed_file()

    print("\n--- Added file ---")
    bench_added_file()

    print("\n--- Deleted file ---")
    bench_deleted_file()

    print("\n--- Defrag (offset shift) ---")
    bench_defrag_offset_shift()

    print("\n--- Sequential read across refresh ---")
    bench_sequential_read_across_refresh()

    print("\n--- Many files, one changed ---")
    bench_many_files_one_changed()

    print("\n--- Multiple rapid refreshes ---")
    bench_multiple_rapid_refreshes()

    print("\n--- End-to-end pack + refresh ---")
    bench_end_to_end_pack_refresh()


if __name__ == "__main__":
    main()
