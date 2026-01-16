# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import abc
from typing import Generic, TypeVar

from monarch._src.actor.python_extension_methods import rust_struct
from monarch._src.actor.waker import Event

T = TypeVar("T", covariant=True)


@rust_struct("monarch_hyperactor::pympsc::Receiver")
class Receiver(Generic[T], abc.ABC):
    """Channel receiver with both sync and async recv methods."""

    def try_recv(self) -> T | None:
        """Non-blocking receive. Returns None if channel is empty."""
        ...

    async def recv(self) -> T:
        """
        Async receive. Waits for a message to arrive without blocking the event loop.

        This method uses pywaker to efficiently wait for messages without
        blocking the event loop.

        Returns:
            The next message from the channel.

        Raises:
            EOFError: If the channel is closed.
        """
        event = self._event()

        while True:
            item = self.try_recv()
            if item is not None:
                return item

            await event.wait()
            event.clear()

    def _event(self) -> Event:
        """Event indicating newly enqueued message."""
        ...
