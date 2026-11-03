# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Shared execution timing and exit-code normalization."""

HEARTBEAT_SECS = 10.0
HEARTBEAT_TIMEOUT_US = 45_000_000


def normalize_exit_code(code: int) -> int:
    """Represent signal termination as the shell's conventional nonzero code."""
    return code if code >= 0 else min(128 - code, 255)
