# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
import math
import warnings
from typing import (
    Any,
    cast,
    Coroutine,
    Generator,
    Generic,
    List,
    NamedTuple,
    Optional,
    TypeVar,
)

from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    Handle,
    is_tokio_thread,
    PythonTask,
    WouldBlockRuntime,
)
from monarch._src.actor.telemetry import log_with_tracing

R = TypeVar("R")


async def _aincomplete(impl: Any, self: Any) -> Any:
    try:
        return self._set_result(await impl())
    except Exception as e:
        self._set_exception(e)
        raise


class _Unawaited(NamedTuple):
    coro: PythonTask[Any]


class _Complete(NamedTuple):
    value: Any


class _Exception(NamedTuple):
    exe: Exception


class _Handle(NamedTuple):
    handle: Handle[Any]


class _Taken(NamedTuple):
    pass


_Status = _Unawaited | _Complete | _Exception | _Handle | _Taken


class Future(Generic[R]):
    """A result returned by Monarch asynchronous operations.

    Use ``get()`` from synchronous code. On an asyncio loop, use ``await`` or
    ``as_asyncio()``. Future objects cannot be constructed directly.
    """

    _status: _Status

    def __init__(self) -> None:
        raise TypeError(
            "Future objects are returned by Monarch operations and cannot be "
            "constructed directly."
        )

    @classmethod
    def _from_coro(cls, coro: "Coroutine[Any, Any, R] | PythonTask[R]") -> "Future[R]":
        future = cast("Future[R]", object.__new__(cls))
        future._status = _Unawaited(
            coro if isinstance(coro, PythonTask) else PythonTask.from_coroutine(coro)
        )
        return future

    def _take_inner(self) -> "PythonTask[R]":
        """Take the underlying one-shot ``PythonTask`` from this Future.

        Awaiting the task drives its Rust future inline as part of the caller.
        Calling ``spawn()`` and awaiting its ``Shared`` observes a separately
        running producer that is not aborted when the observer is dropped. Only
        valid on a Future that has not yet been awaited or resolved; the Future is
        spent afterward (a second take, or any get()/await, raises).
        """
        match self._status:
            case _Unawaited(coro=coro):
                self._status = _Taken()
                return cast("PythonTask[R]", coro)
            case _:
                raise ValueError("Future has already been awaited or resolved.")

    def get(self, timeout: Optional[float] = None) -> R:
        """Get the result of the Future.

        Caveats:

        This method is designed to be used in places where event loops are not available. Besides that, you should
        avoid using this method if possible. Instead, use `as_asyncio()` (or `await`). This is because when Future.get() is called from
        within an active event loop, it blocks synchronously and does not yield control. That may degrade performance
        by preventing other tasks from running, and can potentially cause deadlocks if this future depends on them.

        A `timeout` never consumes the Future: on `TimeoutError` the underlying task keeps running, so a later `get()`/`await` still observes its result.

        examples:

        This is not recommended because `fut.get()` blocks the event loop and might lead to issues explained above.
        ```
        def inner_func(fut):
            result = fut.get()
            # ...

        async def out_func(fut):
            inner_func(fut)
        ```

        This is okay because everything is running synchronously.
        ```
        def inner_func(fut):
            result = fut.get()
            # ...

        def main():
            # ...
            inner_func(fut)
        ```
        """
        in_asyncio = asyncio._get_running_loop() is not None
        in_tokio = is_tokio_thread()
        if in_asyncio or in_tokio:
            # Forward the event to Rust tracing for every in-loop/tokio caller,
            # including non-actor driver processes where no `TracingForwarder`
            # handler is on the Python logging chain. A UserWarning (separate
            # from this trace) fires only on a running asyncio loop, never on a
            # Tokio thread: the blocking `_Unawaited`/`_Handle` paths raise
            # `WouldBlockRuntime` before warning, and cached `_Complete`/
            # `_Exception` reads don't block or warn. On asyncio it fires once,
            # inline below for the no-timeout `_Unawaited` path, else via
            # `Handle.get()` for the `get(timeout=...)`/`_Handle` paths.
            log_with_tracing(
                logging.WARNING,
                "Future.get() called from within an active event loop",
                extra={"context": "asyncio" if in_asyncio else "tokio"},
                stacklevel=2,
            )
        match self._status:
            case _Unawaited(coro=coro):
                if in_tokio:
                    # Cannot block inside a Tokio runtime. Refuse cleanly BEFORE
                    # spawning or consuming the task (mirroring Handle.get()), for
                    # BOTH the timeout and no-timeout paths, so a rejected call
                    # never starts work or flips state and a later get()/await
                    # from a valid context still drives the Future. (Otherwise the
                    # no-timeout path's block_on() takes the task then panics, and
                    # the timeout path spawns a Handle then raises -- both mutate.)
                    raise WouldBlockRuntime(
                        "Future.get() cannot block from within a Tokio runtime; "
                        "observe the Future from a synchronous or asyncio context."
                    )
                if timeout is not None:
                    # Validate the timeout BEFORE spawning: an invalid value must
                    # not start work or flip state to _Handle. Handle.get()
                    # re-validates authoritatively; this only avoids the spawn on
                    # a bad argument.
                    if not math.isfinite(timeout) or timeout < 0:
                        raise ValueError(
                            f"invalid timeout {timeout}: expected a non-negative, finite number of seconds"
                        )
                    # A timeout must not destroy the Future. Observe the task
                    # through a Handle, which is non-cancelling on timeout, so a
                    # later get()/poll()/await still resolves. Handle.get() applies
                    # the context policy and emits the warning itself.
                    handle = coro.spawn_handle()
                    self._status = _Handle(handle)
                    return cast("R", handle.get(timeout))
                if in_asyncio:
                    warnings.warn(
                        "Future.get() was called from within an active event loop. Because this method blocks "
                        "synchronously and does not yield control, it may degrade performance by preventing "
                        "other tasks from running, and can potentially cause deadlocks if this future depends "
                        "on them. It is encouraged to use as_asyncio() (or await) instead.",
                        UserWarning,
                        stacklevel=2,
                    )
                try:
                    v = coro.block_on()
                    self._status = _Complete(v)
                    return cast("R", v)
                except Exception as e:
                    self._status = _Exception(e)
                    raise e from None
            case _Handle(handle=handle):
                # Observe the shared Handle. `Handle.get()` applies its own
                # context policy (warn on a live loop, `WouldBlockRuntime` in a
                # Tokio runtime context, block on a sync thread) and emits the
                # Python warning itself, so we do not warn again here.
                return cast("R", handle.get(timeout))
            case _Complete(value=value):
                return cast("R", value)
            case _Exception(exe=exe):
                raise exe
            case _Taken():
                raise ValueError("Future was consumed.")
            case _:
                raise RuntimeError("unknown status")

    def __await__(self) -> Generator[Any, Any, R]:
        if asyncio._get_running_loop() is not None:
            # Asyncio callers observe through the Handle; `__await__` delegates
            # to `as_asyncio()`.
            return self.as_asyncio().__await__()
        elif is_tokio_thread():
            match self._status:
                case _Unawaited():
                    raise RuntimeError(
                        "Future cannot be awaited on a Tokio thread; observe it "
                        "from an asyncio loop or synchronous context."
                    )
                case _Handle(_):
                    raise ValueError(
                        "Future is backed by a Handle and is not awaitable on a tokio thread; "
                        "use get() or as_asyncio() from a sync/asyncio context."
                    )
                case _Taken():
                    raise ValueError("Future was consumed.")
                case _:
                    raise ValueError(
                        "already converted into a synchronous future, use 'get' to get the value."
                    )
        else:
            raise ValueError(
                "__await__ with no active event loop (either asyncio or tokio)"
            )

    def as_asyncio(self) -> "asyncio.Future[R]":
        """Return a standard ``asyncio.Future`` that resolves when this Future
        does.

        Requires a running event loop; off a loop it raises ``RuntimeError``
        **without** consuming the underlying task (the Future stays unawaited, so
        a later ``get()`` still drives it). Observation is non-consuming:
        repeated ``as_asyncio()``/``await`` each return a fresh loop-local future
        observing the same result.
        """
        loop = asyncio._get_running_loop()
        if loop is None:
            raise RuntimeError("as_asyncio() requires a running asyncio event loop.")
        match self._status:
            case _Unawaited(coro=coro):
                # The loop is confirmed above, so spawning is safe: an off-loop
                # call raised already, leaving the Future in `_Unawaited`.
                handle = coro.spawn_handle()
                self._status = _Handle(handle)
                return handle.as_asyncio()
            case _Handle(handle=handle):
                return handle.as_asyncio()
            case _Complete(value=value):
                done = loop.create_future()
                done.set_result(value)
                return cast("asyncio.Future[R]", done)
            case _Exception(exe=exe):
                failed = loop.create_future()
                # asyncio.Future.set_exception rejects StopIteration (PEP 479);
                # surface it as `raise StopIteration` already does out of a
                # coroutine, matching the Handle observer path.
                failed.set_exception(
                    RuntimeError("coroutine raised StopIteration")
                    if isinstance(exe, StopIteration)
                    else exe
                )
                return cast("asyncio.Future[R]", failed)
            case _Taken():
                raise ValueError("Future was consumed.")
            case _:
                raise RuntimeError("unknown status")

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

    @staticmethod
    def gather(
        *futures: "Future[Any]",
        return_exceptions: bool = False,
    ) -> "Future[List[Any]]":
        """Drive multiple Futures concurrently, returning a Future of their results.

        Results are returned in the same order as the input futures.
        Input futures are consumed and should not be reused.

        Args:
            return_exceptions: If False (default), the first exception raised by
                any future propagates immediately. If True, exceptions are treated
                as successful results and returned in the result list.
        """
        if not futures:

            async def _empty() -> List[Any]:
                return []

            return Future(coro=_empty())

        tasks: List[PythonTask[Any]] = []
        for f in futures:
            match f._status:
                case _Unawaited(coro=coro):
                    tasks.append(coro)
                    f._status = _Exception(
                        ValueError("this Future was consumed by gather()")
                    )
                case _:
                    raise ValueError(
                        "Future passed to gather() has already been consumed "
                        "(via .get(), await, or another gather call)"
                    )

        async def _gather() -> List[Any]:
            shared = [t.spawn() for t in tasks]
            results: List[Any] = []
            for s in shared:
                if return_exceptions:
                    try:
                        results.append(await s)
                    except Exception as e:
                        results.append(e)
                else:
                    results.append(await s)
            return results

        return Future(coro=_gather())
