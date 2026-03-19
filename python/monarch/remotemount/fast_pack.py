# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import logging
import os
from typing import Any

from monarch._rust_bindings.monarch_extension.fast_pack import (
    pack_files_with_offsets as _c_pack_files,
)

logger: logging.Logger = logging.getLogger(__name__)

CHUNK_SIZE: int = (1024 * 1024 * 1024) * 8
HASH_BLOCK_SIZE: int = 100 * 1024 * 1024  # 100MB blocks for incremental diffing


# pyre-fixme[24]: Generic type `memoryview` expects 1 type parameter.
def block_hashes(data_mv: memoryview, block_size: int = HASH_BLOCK_SIZE) -> list[str]:
    """Compute xxhash per block of a packed memoryview."""
    import xxhash

    hashes = []
    for i in range(0, len(data_mv), block_size):
        hashes.append(xxhash.xxh64(bytes(data_mv[i : i + block_size])).hexdigest())
    return hashes


def pack_directory_chunked(
    source_path: str,
    chunk_size: int | None = None,
    # pyre-fixme[24]: Generic type `memoryview` expects 1 type parameter.
) -> tuple[dict[str, Any], memoryview | None, list[memoryview], list[str]]:
    """Walk a directory, pack all files into contiguous mmap chunks.

    Returns (fs_metadata, staging_mv, chunks, block_hashes_list)
    where:
    - fs_metadata: dict mapping virtual paths to stat/offset metadata
    - staging_mv: memoryview over the packed data
    - chunks: list of chunk-sized memoryview slices
    - block_hashes_list: list of xxh64 hex digest strings per block
    """
    if chunk_size is None:
        chunk_size = CHUNK_SIZE

    fs_metadata: dict[str, Any] = {}
    file_list: list[tuple[str, int, int]] = []

    # Tracks the virtual address of the filesystem
    current_global_offset = 0

    source_path = os.path.abspath(source_path)

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

                fs_metadata[virtual_path] = {
                    "attr": attr,
                    "global_offset": current_global_offset,
                    "file_len": file_len,
                }

                file_list.append((full_path, current_global_offset, file_len))
                # Tracks the virtual address of the filesystem
                current_global_offset += file_len

    total_size = current_global_offset
    logger.info(f"Packing {total_size // (1024**2)}MiB, {len(file_list)} files")

    if total_size == 0:
        return fs_metadata, None, [], []

    staging_mv, hashes = _c_pack_files(file_list, total_size)
    chunks = [
        staging_mv[i : i + chunk_size] for i in range(0, len(staging_mv), chunk_size)
    ]

    return fs_metadata, staging_mv, chunks, list(hashes)
