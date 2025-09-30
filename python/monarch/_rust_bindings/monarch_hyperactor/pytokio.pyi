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
    NoReturn,
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
    def task(self) -> PythonTask[T]:
        """
        Create a one-use Task that awaits on this if you want to use other PythonTask apis like with_timeout.
        """
        ...

def is_tokio_thread() -> bool:
    """
    Returns true if the current thread is a tokio worker thread (and block_on will fail).
    """
    ...
