# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

def get_or_create_trace_id() -> str:
    """
    Get the trace id or create a new one if it doesn't exist.
    """
    ...

def export_profile(
    telemetry_url: str,
    start_us: int,
    end_us: int,
    output: str | None = None,
    upload: bool = False,
) -> str:
    """Export a distributed telemetry interval and return its path or URL."""
    ...
