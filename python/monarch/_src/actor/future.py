# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
from typing import (
    Any,
    cast,
    Coroutine,
    Generator,
    Generic,
    NamedTuple,
    Optional,
    TypeVar,
)

from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    is_tokio_thread,
    PythonTask,
    Shared,
)

R = TypeVar("R")


async def _aincomplete(impl: Any, self: Any) -> Any:
    try:
        return self._set_result(await impl())
    except Exception as e:
        self._set_exception(e)
        raise


# Future is our generic mechanism for providing both a synchronous and asynchronous API for
# Monarch Future objects.

# We treat all code as running in one of two contexts: synchronous (asyncio._get_running_loop() is None)
# or asynchronous.

# Inside of asynchronous code, clients of our API must use `await` to wait for monarch Futures to prevent
# blocking the surrounding event loop.

# In synchronous code users must call get() because the call is comming from an non-async function so
# await is not allowed.

# [avoiding async code duplication]
# Because we allow for two modes, it is tempting as developers of Monarch to start to write two copies of
# of code for each mode. However, this results in a lot of confusing code duplication.
# To avoid this, we utilize the fact that synchronous code is allowed to start/complete an asyncio event loop
# via asyncio.run in order to complete the `get()` operation. So we can just write the async version and use
# it to implement the synchronoous version.

# However, starting and running an event loop is somewhat expensive. For simple messages, using an event loop
# is about 4x slower than just directly waiting on the tokio result. To avoid this slow down we perform an
# optimization. For any case where the `impl` coroutine of a future calls `await` only on PythonFuture
# (a Tokio future returning a Python value) objects, we pass requires_loop=False to the Future. In this mode,
# the future will just run the coroutine manually, and the PythonFuture object will recognize it is being awaited
# without an event loop (search [avoiding code duplication]) and simply do a blocking wait. By avoiding the event
# loop machinery, this gives it the same throughput as if we ran it synchronously.


class _Unawaited(NamedTuple):
    coro: PythonTask[Any]


class _Complete(NamedTuple):
    value: Any


class _Exception(NamedTuple):
    exe: Exception


class _Asyncio(NamedTuple):
    fut: asyncio.Future


class _Tokio(NamedTuple):
    shared: Shared[Any]


_Status = _Unawaited | _Complete | _Exception | _Asyncio | _Tokio


class Future(Generic[R]):
    """
    The Future class wraps a PythonTask, which is a handle to a asyncio coroutine running on the Tokio event loop.
    These coroutines do not use asyncio or asyncio.Future; instead, they are executed directly on the Tokio runtime.
    The Future class provides both synchronous (.get()) and asynchronous APIs (await) for interacting with these tasks.

    Args:
        coro (Coroutine[Any, Any, R] | PythonTask[R]): The coroutine or PythonTask representing
            the asynchronous computation.

    """

    def __init__(self, *, coro: "Coroutine[Any, Any, R] | PythonTask[R]") -> None:
        self._status: _Status = _Unawaited(
            coro if isinstance(coro, PythonTask) else PythonTask.from_coroutine(coro)
        )

    def get(self, timeout: Optional[float] = None) -> R:
        match self._status:
            case _Unawaited(coro=coro):
                try:
                    if timeout is not None:
                        coro = coro.with_timeout(timeout)
                    v = coro.block_on()
                    self._status = _Complete(v)
                    return cast("R", v)
                except Exception as e:
                    self._status = _Exception(e)
                    raise e from None
            case _Asyncio(_):
                raise ValueError(
                    "already converted into an asyncio.Future, use 'await' to get the value."
                )
            case _Complete(value=value):
                return cast("R", value)
            case _Exception(exe=exe):
                raise exe
            case _Tokio(_):
                raise ValueError(
                    "already converted into a pytokio.Shared object, use 'await' from a PythonTask coroutine to get the value."
                )
            case _:
                raise RuntimeError("unknown status")

    def __await__(self) -> Generator[Any, Any, R]:
        if asyncio._get_running_loop() is not None:
            match self._status:
                case _Unawaited(coro=coro):
                    loop = asyncio.get_running_loop()
                    fut = loop.create_future()
                    self._status = _Asyncio(fut)

                    def set_result(fut: asyncio.Future[R], value: R) -> None:
                        if not fut.cancelled():
                            fut.set_result(value)

                    def set_exception(fut: asyncio.Future[R], e: Exception) -> None:
                        if not fut.cancelled():
                            fut.set_exception(e)

                    async def mark_complete() -> None:
                        try:
                            func, value = set_result, await coro
                        except Exception as e:
                            func, value = set_exception, e
                        loop.call_soon_threadsafe(func, fut, value)

                    PythonTask.from_coroutine(mark_complete()).spawn()
                    return fut.__await__()
                case _Asyncio(fut=fut):
                    return fut.__await__()
                case _Tokio(_):
                    raise ValueError(
                        "already converted into a tokio future, but being awaited from the asyncio loop."
                    )
                case _:
                    raise ValueError(
                        "already converted into a synchronous future, use 'get' to get the value."
                    )
        elif is_tokio_thread():
            match self._status:
                case _Unawaited(coro=coro):
                    shared = coro.spawn()
                    self._status = _Tokio(shared)
                    return shared.__await__()
                case _Tokio(shared=shared):
                    return shared.__await__()
                case _Asyncio(_):
                    raise ValueError(
                        "already converted into asyncio future, but being awaited from the tokio loop."
                    )
                case _:
                    raise ValueError(
                        "already converted into a synchronous future, use 'get' to get the value."
                    )
        else:
            raise ValueError(
                "__await__ with no active event loop (either asyncio or tokio)"
            )

    # compatibility with old tensor engine Future objects
    # hopefully we do not need done(), add_callback because
    # they are harder to implement right.
    def result(self, timeout: Optional[float] = None) -> R:
        return self.get(timeout)

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        try:
            self.get(timeout)
            return None
        except Exception as e:
            return e
