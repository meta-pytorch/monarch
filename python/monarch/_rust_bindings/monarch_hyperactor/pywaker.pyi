# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
from typing import Protocol

from typing_extensions import Literal

class Event(Protocol):
    @property
    def _read_fd(self) -> int: ...
    _event_loop: asyncio.AbstractEventLoop | None
    _event: asyncio.Event | None
    async def wait(self) -> Literal[True]: ...
    def clear(self) -> None: ...

class PyTestWaker:
    def wake(self) -> bool: ...
    @staticmethod
    def create() -> tuple["PyTestWaker", Event]: ...
