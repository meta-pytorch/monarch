# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Any

def pack_files_with_offsets(
    file_list: list[tuple[str, int, int]],
    total_size: int,
    hash_block_size: int | None = None,
    max_threads: int | None = None,
) -> tuple[Any, list[str]]: ...
def load_file_and_hash(
    path: str,
    hash_block_size: int | None = None,
    padded_size: int | None = None,
    max_threads: int | None = None,
) -> tuple[Any, list[str]]: ...
def load_file_into_buffer(
    path: str,
    buffer: Any,
    hash_block_size: int | None = None,
    max_threads: int | None = None,
) -> list[str]: ...
