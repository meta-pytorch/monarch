# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Simple TTL cache shared by dashboard route handlers.

Telemetry data is inherently slightly stale (scanners batch at intervals),
so a short cache avoids redundant distributed scans when the dashboard
polls multiple endpoints concurrently.
"""

import time
from typing import Any, Callable

_cache: dict[str, tuple[float, Any]] = {}

# Default TTL in seconds.
CACHE_TTL = 2.0


def cached(key: str, fn: Callable[[], Any], ttl: float = CACHE_TTL) -> Any:
    """Return a cached result for *key*, or compute via *fn* and cache it."""
    now = time.monotonic()
    entry = _cache.get(key)
    if entry is not None and now - entry[0] < ttl:
        return entry[1]
    result = fn()
    _cache[key] = (now, result)
    return result
