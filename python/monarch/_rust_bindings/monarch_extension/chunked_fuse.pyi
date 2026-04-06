# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from collections.abc import Sequence
from typing import Any

class FuseMountHandle:
    def unmount(self) -> None: ...
    def refresh(
        self,
        metadata: dict[str, Any],
        chunks: Sequence[Any],
        chunk_size: int,
    ) -> None: ...

def mount_chunked_fuse(
    metadata: dict[str, Any],
    chunks: Sequence[Any],
    chunk_size: int,
    mount_point: str,
) -> FuseMountHandle: ...
