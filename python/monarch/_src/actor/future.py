# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import math
from typing import (
    Any,
    cast,
    Generator,
    Generic,
    NamedTuple,
    Optional,
    TYPE_CHECKING,
    TypeVar,
)

from monarch._rust_bindings.monarch_hyperactor.handle import (
    _new_handle_pair,
    Handle,
    WouldBlockRuntime,
)
from monarch._rust_bindings.monarch_hyperactor.runtime import _is_in_tokio_runtime
from monarch._src.actor.telemetry import log_with_tracing

# `returns_future` imports `Future` from this module. `_BodyCell` is needed only
# for annotations, so importing it only during type checking avoids a circular
# runtime import.
if TYPE_CHECKING:
    from monarch._src.actor.returns_future import _BodyCell

R = TypeVar("R")

# Raised by every blocking `Future.get()` path on a Tokio thread. Handle-backed
# Futures raise it too, rather than `Handle.get()`'s message, which suggests a
# `poll()` that `Future` does not have.
_GET_ON_TOKIO_REFUSAL = (
    "Future.get() cannot block from within a Tokio runtime; "
    "observe the Future from a synchronous or asyncio context."
)


def _ready_handle(value: R) -> Handle[R]:
    """Put an existing value in a Handle without starting any work."""
    handle, completer = _new_handle_pair()
    completer.set_result(value)
    return handle


# Shared by the Python-implemented `Future.get()` paths when a running asyncio
# loop is detected. Handle-backed Futures enforce the same policy in native
# `Handle.get()`.
_GET_ON_LOOP_WARNING = (
    "Future.get() was called from within an active event loop. Because this method blocks "
    "synchronously and does not yield control, it may degrade performance by preventing "
    "other tasks from running, and can potentially cause deadlocks if this future depends "
    "on them. It is encouraged to use as_asyncio() (or await) instead."
)


def _validate_timeout(timeout: float | None) -> None:
    if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
        raise ValueError(
            f"invalid timeout {timeout}: expected a non-negative, finite number of seconds"
        )


class _Handle(NamedTuple):
    """An operation observed through a native Handle, which manages its shared
    value or error. The work itself may be native or Python-owned."""

    handle: Handle[Any]


class _Body(NamedTuple):
    """The body of a ``@returns_future`` function, run on the loop of its first
    observer (see ``monarch._src.actor.returns_future``)."""

    body: "_BodyCell"


# A Future either observes an operation through a native Handle or runs a
# Python async function body.
_Status = _Handle | _Body


class Future(Generic[R]):
    """A result returned by Monarch asynchronous operations.

    Use ``get()`` from synchronous code. On an asyncio loop, use ``await`` or
    ``as_asyncio()``. Future objects cannot be constructed directly, and like
    ``asyncio.Future`` they cannot be pickled.
    """

    _status: _Status

    def __init__(self) -> None:
        raise TypeError(
            "Future objects are returned by Monarch operations and cannot be "
            "constructed directly."
        )

    @classmethod
    def _resolved(cls, value: R) -> "Future[R]":
        """Return a Future over a Handle already resolved to ``value``, without
        starting work or bootstrapping the root client."""
        return cls._from_handle(_ready_handle(value))

    @classmethod
    def _from_handle(cls, handle: Handle[R]) -> "Future[R]":
        if not isinstance(handle, Handle):
            raise TypeError(f"expected Handle, got {type(handle).__name__}")
        future = cast("Future[R]", object.__new__(cls))
        future._status = _Handle(handle)
        return future

    @classmethod
    def _from_body(cls, body: "_BodyCell") -> "Future[R]":
        future = cast("Future[R]", object.__new__(cls))
        future._status = _Body(body)
        return future

    def _as_handle(self) -> "Handle[R]":
        match self._status:
            case _Handle(handle=handle):
                return cast("Handle[R]", handle)
            case _Body():
                # RF-6: waiting on the Handle of a body nobody has started
                # would never finish.
                raise ValueError("a returns_future Future has no Handle")
            case _:
                raise RuntimeError("unknown status")

    def get(self, timeout: Optional[float] = None) -> R:
        """Get the result of the Future.

        Caveats:

        This method is designed to be used in places where event loops are not available. Besides that, you should
        avoid using this method if possible. Instead, use `as_asyncio()` (or `await`). This is because when Future.get() is called from
        within an active event loop, it blocks synchronously and does not yield control. That may degrade performance
        by preventing other tasks from running, and can potentially cause deadlocks if this future depends on them.

        Except for a Future returned by a `@returns_future` function, a `timeout` never consumes the Future: on `TimeoutError` the underlying task keeps running, so a later `get()`/`await` still observes its result.

        For a Future returned by a `@returns_future` function, the first `get()` runs the body on the calling thread's event loop, as `asyncio.run` does.
        If it times out or is interrupted, the body is cancelled and the Future fails with `TimeoutError` or `CancelledError`; nothing resumes it.
        Inside running asyncio code, `get()` runs the body the same way on a short-lived helper thread and waits for it;
        inside a body that a `get()` is driving, it raises `WouldBlockRuntime`.

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
        in_tokio = _is_in_tokio_runtime()
        if in_asyncio or in_tokio:
            # Forward the event to Rust tracing for every in-loop/tokio caller,
            # including non-actor driver processes where no `TracingForwarder`
            # handler is on the Python logging chain. A UserWarning (separate
            # from this trace) fires only on a running asyncio loop, never on a
            # Tokio thread: the blocking `_Handle` path raises
            # `WouldBlockRuntime` before warning. On asyncio it fires once, via
            # `Handle.get()`.
            log_with_tracing(
                logging.WARNING,
                "Future.get() called from within an active event loop",
                extra={"context": "asyncio" if in_asyncio else "tokio"},
                stacklevel=2,
            )
        match self._status:
            case _Handle(handle=handle):
                if in_tokio:
                    raise WouldBlockRuntime(_GET_ON_TOKIO_REFUSAL)
                # Observe the shared Handle. `Handle.get()` applies the rest of
                # its context policy (warn on a live loop, block on a sync
                # thread) and emits the Python warning itself, so we do not warn
                # again here.
                return cast("R", handle.get(timeout))
            case _Body(body=body):
                # Refuse before binding, leaving the body unstarted.
                if in_tokio:
                    raise WouldBlockRuntime(_GET_ON_TOKIO_REFUSAL)
                _validate_timeout(timeout)
                return cast("R", body.get(timeout))
            case _:
                raise RuntimeError("unknown status")

    def __await__(self) -> Generator[Any, Any, R]:
        match self._status:
            case _Handle(handle=handle):
                return handle.__await__()
        # Only a `@returns_future` body gets past the match.
        if asyncio._get_running_loop() is not None:
            # Asyncio callers observe through the Handle; `__await__` delegates
            # to `as_asyncio()`.
            return self.as_asyncio().__await__()
        elif _is_in_tokio_runtime():
            raise RuntimeError(
                "Future cannot be awaited on a Tokio thread; observe it "
                "from an asyncio loop or synchronous context."
            )
        else:
            raise ValueError(
                "__await__ with no active event loop (either asyncio or tokio)"
            )

    def as_asyncio(self) -> "asyncio.Future[R]":
        """Return a standard ``asyncio.Future`` that resolves when this Future
        does.

        Requires a running event loop; off a loop it raises ``RuntimeError``
        **without** starting the underlying operation, so a later ``get()``
        still observes it. Observation is non-consuming:
        repeated ``as_asyncio()``/``await`` each return a fresh loop-local future
        observing the same result.
        """
        loop = asyncio._get_running_loop()
        if loop is None:
            raise RuntimeError("as_asyncio() requires a running asyncio event loop.")
        match self._status:
            case _Handle(handle=handle):
                return cast("asyncio.Future[R]", handle.as_asyncio())
            case _Body(body=body):
                return cast("asyncio.Future[R]", body.as_asyncio(loop))
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
