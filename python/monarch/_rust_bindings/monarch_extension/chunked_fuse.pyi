# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Sequence

class FuseMountHandle:
    def unmount(self) -> None: ...

def mount_chunked_fuse(
    metadata_json: str,
    chunks: Sequence[memoryview],
    chunk_size: int,
    mount_point: str,
) -> FuseMountHandle: ...
