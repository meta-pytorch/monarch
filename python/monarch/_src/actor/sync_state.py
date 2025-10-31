# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
from contextlib import contextmanager
from typing import Any, Generator


@contextmanager
def fake_sync_state() -> Generator[None, None, None]:
    prev_loop = asyncio.events._get_running_loop()
    asyncio._set_running_loop(None)
    try:
        yield
    finally:
        asyncio._set_running_loop(prev_loop)
