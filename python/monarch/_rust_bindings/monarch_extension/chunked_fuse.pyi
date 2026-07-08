# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Any

class FuseMountHandle:
    def unmount(self) -> None: ...
    def refresh(
        self,
        metadata: dict[str, Any],
        new_total_size: int,
    ) -> None: ...
    def block_ptr(self, block_id: int) -> int: ...
    def receive_block(self, block_id: int, stale: list[str]) -> None: ...

def mount_chunked_fuse(
    metadata: dict[str, Any],
    total_size: int,
    mount_point: str,
    fault_callback: Any,
) -> FuseMountHandle: ...
