# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
from typing import Any, Generator, Generic, Optional, Sequence, TypeVar

T = TypeVar("T")

class _HandleCompleter(Generic[T]):
    """The private one-shot producer returned with a pending ``Handle``."""

    def set_result(self, value: T) -> None: ...
    def set_exception(self, error: BaseException) -> None: ...

class Handle(Generic[T]):
    """
    An observe-only handle to a background operation. It resolves once and stays
    observable by any number of later observers. Unlike Shared, a Handle never
    drives a Python coroutine. If its producer ends before publishing a result,
    every observer raises RuntimeError.
    """

    def get(self, timeout: Optional[float] = None) -> T:
        """
        Block the calling thread until the handle resolves and return its value.
        Behavior is keyed to the calling context, not to whether the value is
        ready: in a tokio runtime context it always raises WouldBlockRuntime, even
        for a ready value (blocking there would panic the runtime) -- use poll() or
        as_asyncio(); on a running asyncio loop it warns, since get() can freeze
        the loop; on a sync thread it blocks until resolved. On timeout it raises
        TimeoutError without cancelling the handle, so a later get()/poll()/await
        still observes completion.
        """
        ...

    def poll(self) -> Optional[T]:
        """
        If the handle has resolved, return the value; otherwise return None. If
        its producer has ended without publishing a result, raise RuntimeError.
        Non-consuming: the value stays observable by later observers.
        """
        ...

    def as_asyncio(self) -> "asyncio.Future[T]":
        """
        Return a standard asyncio.Future that resolves when the handle does.
        Requires a running event loop; off a loop it raises RuntimeError. If the
        producer ends without publishing a result, the Future resolves with
        RuntimeError. A handle that already holds its outcome returns a Future
        that is already done, so awaiting it does not yield to the loop.
        """
        ...

    def __await__(self) -> Generator[Any, Any, T]:
        """
        Await the handle on a running asyncio loop, delegating to as_asyncio().
        """
        ...

def _new_handle_pair() -> tuple[Handle[T], _HandleCompleter[T]]:
    """Create a pending ``Handle`` and its sole completion endpoint."""
    ...

def _after(ready: Sequence[Handle[Any]], result: Handle[T]) -> Handle[T]:
    """Observe result after the prerequisites succeed, in their given order."""
    ...

class WouldBlockRuntime(RuntimeError):
    """
    Raised when a synchronous API refuses to enter or block on Tokio from an
    existing Tokio runtime context.

    Raised by ``Handle.get()``, a fresh root-client bootstrap (``context()``
    with no actor context and no initialized client, or ``attach()``), and the
    blocking worker-loop wrappers. A ``@returns_future`` Future's ``.get()``
    also raises it where blocking would stall a running event loop. Reusing an already-initialized client does
    not block and so does not raise.
    """

    ...
