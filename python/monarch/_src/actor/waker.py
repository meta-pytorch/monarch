# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import abc
import asyncio
import os

from monarch._src.actor.python_extension_methods import rust_struct
from typing_extensions import Literal


@rust_struct("monarch_hyperactor::pywaker::Event")
class Event(abc.ABC):
    """An event that that is set by a waker."""

    _read_fd: int
    _event_loop: asyncio.AbstractEventLoop | None
    _event: asyncio.Event | None

    async def wait(self) -> Literal[True]:
        """Wait for the event to be set. Once set, remains set until cleared.

        After first await, the event is bound to that event loop and cannot be used from other loops."""

        if self._event is None:
            self._event = asyncio.Event()
        assert self._event is not None

        event_loop = asyncio.get_event_loop()
        if self._event_loop is None:
            self._event_loop = event_loop

            assert self._event is not None
            event: asyncio.Event = self._event
            assert event is not None

            def notified() -> None:
                os.read(self._read_fd, 1)
                event.set()

            self._event_loop.add_reader(self._read_fd, notified)
        elif self._event_loop is not event_loop:
            raise RuntimeError(
                f"Event already associated with event loop {self._event_loop}"
            )

        assert self._event is not None
        return await self._event.wait()

    def clear(self) -> None:
        """Clear the event."""
        if self._event is not None:
            self._event.clear()

    def __del__(self) -> None:
        if self._event_loop is not None:
            self._event_loop.remove_reader(self._read_fd)


@rust_struct("monarch_hyperactor::pywaker::TestWaker")
class TestWaker:
    def wake(self) -> bool: ...

    @staticmethod
    def create() -> tuple["TestWaker", Event]: ...
