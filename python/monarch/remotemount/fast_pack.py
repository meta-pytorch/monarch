# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

from monarch._rust_bindings.monarch_extension.fast_pack import (
    block_hashes_py,
    pack_files_with_offsets,
)

logger: logging.Logger = logging.getLogger(__name__)

CHUNK_SIZE: int = (1024 * 1024 * 1024) * 8
HASH_BLOCK_SIZE: int = 64 * 1024 * 1024  # 64MB blocks for incremental diffing
FRAG_THRESHOLD: float = 0.2  # max dead-space ratio before sequential repack


# pyre-fixme[24]: Generic type `memoryview` expects 1 type parameter.
def block_hashes(data_mv: memoryview, block_size: int = HASH_BLOCK_SIZE) -> list[str]:
    """Compute xxh64 per block of a packed memoryview."""
    return list(block_hashes_py(data_mv, block_size))


def load_pack_index(path: str) -> dict[str, Any] | None:
    """Load JSON pack index from disk. Returns None if the file doesn't exist."""
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)  # pyre-ignore[7]


def save_pack_index(path: str, index_data: dict[str, Any]) -> None:
    """Write pack index as JSON."""
    with open(path, "w") as f:
        json.dump(index_data, f)


def _compute_file_hashes(
    # pyre-fixme[24]: Generic type `memoryview` expects 1 type parameter.
    staging_mv: memoryview,
    file_entries: list[tuple[str, str, int, int]],
    offset_map: dict[str, int],
) -> dict[str, str]:
    """Compute xxh64 per file from packed buffer. Returns {vpath: hash_hex}."""
    import xxhash

    hashes: dict[str, str] = {}
    for vpath, _full_path, file_len, _mtime_ns in file_entries:
        offset = offset_map[vpath]
        hashes[vpath] = xxhash.xxh64(staging_mv[offset : offset + file_len]).hexdigest()
    return hashes


def _assign_offsets(
    file_entries: list[tuple[str, str, int, int]],
    previous_index: dict[str, Any],
) -> tuple[dict[str, int], int, int]:
    """Append-only offset assignment.

    Unchanged files keep their original offsets. Changed/new files are
    appended after the previous total size.

    Returns:
        (offset_map, new_total_size, dead_space)
    """
    prev_files: dict[str, Any] = previous_index.get("files", {})
    prev_total: int = previous_index.get("total_size", 0)

    offset_map: dict[str, int] = {}
    dead_space = 0
    append_offset = prev_total

    current_vpaths: set[str] = set()
    for vpath, _full_path, file_len, mtime_ns in file_entries:
        current_vpaths.add(vpath)
        prev = prev_files.get(vpath)
        if prev and prev["size"] == file_len and prev["mtime_ns"] == mtime_ns:
            # File unchanged — keep old offset.
            offset_map[vpath] = prev["offset"]
        else:
            # File changed or new — append at the end.
            if prev:
                dead_space += prev["size"]
            offset_map[vpath] = append_offset
            append_offset += file_len

    # Deleted files contribute dead space.
    for vpath, info in prev_files.items():
        if vpath not in current_vpaths:
            dead_space += info["size"]

    return offset_map, append_offset, dead_space


def pack_directory_chunked(
    source_path: str,
    chunk_size: int | None = None,
    previous_index: dict[str, Any] | None = None,
    # pyre-fixme[24]: Generic type `memoryview` expects 1 type parameter.
) -> tuple[
    dict[str, Any],
    memoryview | None,
    list[memoryview],
    list[str],
    dict[str, Any] | None,
]:
    """Walk a directory, pack all files into contiguous mmap chunks.

    When *previous_index* is provided (from a prior run's pack index), files
    whose ``(mtime_ns, size)`` match the index keep their original offsets and
    changed/new files are appended at the end of the buffer.  If the resulting
    dead-space ratio exceeds ``FRAG_THRESHOLD``, the layout falls back to
    sequential packing.

    Returns (fs_metadata, staging_mv, chunks, block_hashes_list, pack_index)
    where:
    - fs_metadata: dict mapping virtual paths to stat/offset metadata
    - staging_mv: memoryview over the packed data
    - chunks: list of chunk-sized memoryview slices
    - block_hashes_list: list of xxh64 hex digest strings per block
    - pack_index: dict with per-file offset/size/mtime/hash for incremental packing
    """
    if chunk_size is None:
        chunk_size = CHUNK_SIZE

    fs_metadata: dict[str, Any] = {}
    file_entries: list[tuple[str, str, int, int]] = []

    source_path = os.path.abspath(source_path)

    t_walk_start = time.time()
    for root, dirs, files in os.walk(source_path):
        rel_path = root[len(source_path) :]
        if rel_path == "":
            rel_path = "/"

        # Directory Metadata
        st = os.stat(root)
        fs_metadata[rel_path] = {
            "attr": {
                key: getattr(st, key)
                for key in (
                    "st_atime",
                    "st_ctime",
                    "st_gid",
                    "st_mode",
                    "st_mtime",
                    "st_nlink",
                    "st_size",
                    "st_uid",
                )
            },
            "children": dirs + files,
        }

        for f in files:
            full_path = os.path.join(root, f)
            virtual_path = (rel_path + "/" + f) if rel_path != "/" else ("/" + f)

            lst = os.lstat(full_path)
            is_symlink = (lst.st_mode & 0o170000) == 0o120000

            if is_symlink:
                fs_metadata[virtual_path] = {
                    "attr": {
                        key: getattr(lst, key)
                        for key in (
                            "st_atime",
                            "st_ctime",
                            "st_gid",
                            "st_mode",
                            "st_mtime",
                            "st_nlink",
                            "st_size",
                            "st_uid",
                        )
                    },
                    "link_target": os.readlink(full_path),
                }
            else:
                file_len = lst.st_size
                mtime_ns = lst.st_mtime_ns
                attr = {
                    key: getattr(lst, key)
                    for key in (
                        "st_atime",
                        "st_ctime",
                        "st_gid",
                        "st_mode",
                        "st_mtime",
                        "st_nlink",
                        "st_size",
                        "st_uid",
                    )
                }
                attr["st_size"] = file_len

                # Defer global_offset — assigned after offset-assignment phase.
                fs_metadata[virtual_path] = {
                    "attr": attr,
                    "file_len": file_len,
                }

                file_entries.append((virtual_path, full_path, file_len, mtime_ns))

    t_walk_done = time.time()
    print(
        f"Directory walk: {len(file_entries)} files in {t_walk_done - t_walk_start:.2f}s",
        file=sys.stderr,
        flush=True,
    )

    # --- Offset assignment phase ---
    offset_map: dict[str, int] = {}
    total_size: int = 0
    use_append = False
    if previous_index and previous_index.get("files"):
        offset_map, total_size, dead_space = _assign_offsets(
            file_entries, previous_index
        )
        if total_size == 0:
            pass  # No files to pack.
        elif dead_space / total_size > FRAG_THRESHOLD:
            print(
                f"Fragmentation {dead_space / total_size:.1%} exceeds threshold "
                f"{FRAG_THRESHOLD:.0%}, repacking sequentially",
                file=sys.stderr,
                flush=True,
            )
        else:
            n_reused = sum(
                1
                for vpath, _, flen, mns in file_entries
                if previous_index["files"].get(vpath, {}).get("size") == flen
                and previous_index["files"].get(vpath, {}).get("mtime_ns") == mns
            )
            print(
                f"Append-only layout: {n_reused}/{len(file_entries)} files reused, "
                f"dead_space={dead_space // 1024}KiB "
                f"({dead_space / total_size:.1%} of {total_size // (1024**2)}MiB)"
            )
            use_append = True

    if not use_append:
        # Sequential offsets (default behavior).
        offset_map = {}
        current_offset = 0
        for vpath, _full_path, file_len, _mtime_ns in file_entries:
            offset_map[vpath] = current_offset
            current_offset += file_len
        total_size = current_offset

    # Set global_offset in fs_metadata and build file_list for Rust packer.
    file_list: list[tuple[str, int, int]] = []
    for vpath, full_path, file_len, _mtime_ns in file_entries:
        fs_metadata[vpath]["global_offset"] = offset_map[vpath]
        file_list.append((full_path, offset_map[vpath], file_len))

    print(
        f"Packing {total_size // (1024**2)}MiB, {len(file_list)} files",
        file=sys.stderr,
        flush=True,
    )

    if total_size == 0:
        return fs_metadata, None, [], [], None

    t_pack_start = time.time()
    buf, hashes = pack_files_with_offsets(file_list, total_size)
    t_pack_done = time.time()
    pack_gbs = (total_size / 1e9) / max(t_pack_done - t_pack_start, 1e-9)
    print(
        f"pack_files_with_offsets: {total_size // (1024**2)}MiB in "
        f"{t_pack_done - t_pack_start:.2f}s ({pack_gbs:.1f} GB/s)",
        file=sys.stderr,
        flush=True,
    )
    staging_mv = memoryview(buf)
    chunks = [
        staging_mv[i : i + chunk_size] for i in range(0, len(staging_mv), chunk_size)
    ]

    # Compute per-file content hashes and build pack index.
    file_hashes = _compute_file_hashes(staging_mv, file_entries, offset_map)
    new_pack_index: dict[str, Any] = {
        "total_size": total_size,
        "files": {
            vpath: {
                "offset": offset_map[vpath],
                "size": file_len,
                "mtime_ns": mtime_ns,
                "content_hash": file_hashes[vpath],
            }
            for vpath, _full_path, file_len, mtime_ns in file_entries
        },
    }

    return fs_metadata, staging_mv, chunks, list(hashes), new_pack_index
