# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

T = TypeVar("T")

class PythonTask(Generic[T], Awaitable[T]):
    """
    A tokio::Future whose result returns a python object.
    """

    def block_on(self) -> T:
        """
        Synchronously wait on the result of this task, returning the result.
        Consumes the PythonTask object.
        """
        ...

    def spawn(self) -> Shared[T]:
        """
        Schedule this task to run on concurrently on the tokio runtime.
        Returns a handle that can be awaited on multiple times so the
        result can be shared among users.
        """
        ...

    def spawn_handle(self) -> "Handle[T]":
        """
        Spawn this task on the tokio runtime and return an observe-only Handle
        (get/poll/as_asyncio/await). Consumes the PythonTask.
        """
        ...

    @staticmethod
    def from_coroutine(coro: Coroutine[Any, Any, T]) -> PythonTask[T]:
        """
        Create a PythonTask from a python coroutine. The coroutine should only await on other PythonTasks created
        using the pytokio APIs.

        This allows async python code to particiipate in the tokio runtime. There is no asyncio runtime event loop involved.
        """
        ...

    @staticmethod
    def spawn_blocking(fn: Callable[[], T]) -> Shared[T]:
        """
        Concurrently run a python function in a way where it is acceptable for it to make synchronous calls back into
        Tokio. See tokio::spawn_blocking for more information.
        """
        ...

    def __await__(self) -> Generator[Any, Any, T]:
        """
        PythonTasks created with from_coroutine can use await when they are being run in the tokio runtime.
        PythonTasks cannot be awaited from the asyncio runtime.
        """
        ...

    def with_timeout(self, seconds: float) -> PythonTask[T]:
        """
        Perform the task but throw a TimeoutException if not finished in 'seconds' seconds.
        """
        ...

    @staticmethod
    def select_one(
        tasks: "Sequence[PythonTask[T]]",
    ) -> "PythonTask[Tuple[T, int]]":
        """
        Run the tasks concurrently and return the first one to finish along with the index of which task it was.
        """
        ...

    @staticmethod
    def sleep(seconds: float) -> "PythonTask[None]": ...

class Shared(Generic[T]):
    """
    The result of a spawned PythonTask, which can be awaited on multiple times like Python Futures.
    """
    def __await__(self) -> Generator[Any, Any, T]: ...
    def block_on(self) -> T: ...
    def poll(self) -> Optional[T]:
        """
        If the task has completed, return the result. Otherwise, return None.
        This is useful because it allows us to get the result of the task
        without blocking the tokio runtime.
        """
        ...
    def task(self) -> PythonTask[T]:
        """
        Create a one-use Task that awaits on this if you want to use other PythonTask apis like with_timeout.
        """
        ...
    @classmethod
    def from_value(cls, value: T) -> "Shared[T]":
        """
        Create a Shared that has already completed with the given value. It will return that
        value the first time poll is called.
        """
        ...

class Handle(Generic[T]):
    """
    An observe-only handle to a background task. It resolves once and stays
    observable by any number of later observers. Unlike Shared, a Handle never
    drives a Python coroutine.
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
        If the handle has resolved, return the value; otherwise return None.
        Non-consuming: the value stays observable by later observers.
        """
        ...

    def as_asyncio(self) -> "asyncio.Future[T]":
        """
        Return a standard asyncio.Future that resolves when the handle does.
        Requires a running event loop; off a loop it raises RuntimeError.
        """
        ...

    def __await__(self) -> Generator[Any, Any, T]:
        """
        Await the handle on a running asyncio loop, delegating to as_asyncio().
        """
        ...

class WouldBlockRuntime(RuntimeError):
    """
    Raised when a synchronous API refuses to enter or block on Tokio from an
    existing Tokio runtime context.

    Two raisers today: ``Handle.get()``, and a fresh root-client bootstrap
    (``context()`` with no actor context and no initialized client, or
    ``attach()``). Reusing an already-initialized client does not block and so
    does not raise.
    """

    ...

def is_tokio_thread() -> bool:
    """
    Returns true if the current thread is a tokio worker thread (and block_on will fail).
    """
    ...
