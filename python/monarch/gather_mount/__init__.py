# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""gather_mount – read-only FUSE mount of remote shard file systems."""

from __future__ import annotations

from monarch._src.gather_mount.gather_mount import gather_mount

__all__ = [
    "gather_mount",
]
