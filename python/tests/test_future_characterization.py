# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Characterization oracle for the public ``monarch`` ``Future`` state machine.

Pins the *current* behavior of ``monarch._src.actor.future.Future`` -- its
internal states and every transition between them, its conversion-error
strings, the ``get()``-inside-a-loop warning, and the ``_take_inner()``
accessor with its ``_Taken`` terminal state -- so that any change to that
behavior is caught here and made explicit.
"""

import asyncio

import pytest
from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    is_tokio_thread,
    PythonTask,
)
from monarch._src.actor import future as future_mod
from monarch._src.actor.future import Future


async def _value(v):
    return v


async def _raise(exc: BaseException):
    raise exc


async def _await_once(fut: Future):
    return await fut


def _run_in_tokio(coro):
    # Drive ``coro`` to completion on the Tokio runtime. Code inside ``coro``
    # runs on a Tokio worker thread, where ``is_tokio_thread()`` is True and a
    # ``Future`` takes its ``__await__`` tokio branch. ``block_on`` itself runs
    # on the (non-worker) calling thread, so it is allowed to block.
    return PythonTask.from_coroutine(coro).block_on()


# ---------------------------------------------------------------------------
# States and transitions
#
# A fresh Future is _Unawaited; the first access drives the underlying task
# exactly once and moves it to a terminal/converted state (_Complete,
# _Exception, _Asyncio, or _Tokio). These pin that lifecycle and its
# idempotency. The two-state redesign must preserve resolve-and-cache for
# results and errors; the two *awaitable* branches (_Asyncio/_Tokio) are the
# dual-world machinery it removes -- after D1 a Future is not awaitable and
# callers use as_asyncio().
# ---------------------------------------------------------------------------


def test_get_success_transitions_to_complete_and_is_idempotent():
    """First sync get() drives the task, transitions _Unawaited -> _Complete,
    and returns the value; a second get() returns the cached value without
    re-running the coroutine. The coroutine must run exactly once, so
    resolve-and-cache is the contract being pinned."""
    fut: Future[int] = Future(coro=_value(42))
    assert fut.get() == 42
    assert fut.get() == 42


def test_get_exception_transitions_to_exception_and_reraises_stored():
    """Failure mirror of the success terminal: the first get() catches the
    raised exception, transitions _Unawaited -> _Exception (caching the
    object), and re-raises; a second get() re-raises the *same stored* object
    without re-running. Identity is asserted between the two gets (not against
    the constructed ValueError) because the exception crosses the Rust/pyo3
    boundary -- the contract is that _Exception caches one object and re-raises
    it."""
    fut: Future[int] = Future(coro=_raise(ValueError("boom")))
    with pytest.raises(ValueError, match="boom") as first:
        fut.get()
    with pytest.raises(ValueError, match="boom") as second:
        fut.get()
    assert second.value is first.value


def test_get_timeout_raises_timeout_error():
    """get(timeout=...) wraps the task in with_timeout, so an unfinished task
    surfaces a TimeoutError (caught, stored as _Exception, raised) rather than
    hanging."""
    fut: Future[None] = Future(coro=PythonTask.sleep(3600))
    with pytest.raises(TimeoutError):
        fut.get(timeout=0.1)


async def test_await_asyncio_transitions_to_asyncio_and_is_idempotent():
    """Awaiting under an asyncio loop transitions _Unawaited -> _Asyncio
    (bridging the task to an asyncio.Future) and yields the value; re-awaiting
    reuses the _Asyncio future. This is one of the two awaitable branches the
    redesign removes."""
    fut: Future[int] = Future(coro=_value(7))
    assert await fut == 7  # _Unawaited -> _Asyncio
    assert await fut == 7  # idempotent re-await of the _Asyncio state


def test_await_tokio_transitions_to_tokio_and_is_idempotent():
    """Awaiting on a tokio thread (inside from_coroutine, where
    is_tokio_thread() is True) transitions _Unawaited -> _Tokio (spawning a
    Shared) and yields the value; re-awaiting reuses it. The _Asyncio-vs-_Tokio
    split is exactly the "which async world am I in" duality P1/D1 collapse."""
    fut: Future[int] = Future(coro=_value(9))

    async def driver():
        assert is_tokio_thread()
        first = await fut  # _Unawaited -> _Tokio
        second = await fut  # idempotent re-await of the _Tokio state
        return first, second

    assert _run_in_tokio(driver()) == (9, 9)


# ---------------------------------------------------------------------------
# Conversion errors -- the seven state-tied raises, pinned verbatim. (The
# numbers follow the source's raises; #3, the unreachable RuntimeError
# "unknown status" fallthrough, is intentionally not pinned.)
#
# These are the five-state machine's "which async world / already-converted"
# mixing guards -- the errors the RFC's Motivation quotes ("already converted
# into an asyncio.Future, ..."). Each is keyed off the *state*, not the data: a
# Future can hold a fully-resolved value and still refuse the wrong accessor.
# The two-state, non-awaitable redesign deletes all of them (no _Asyncio/_Tokio
# states and no await => none of these errors); this oracle is what flags any
# change to them before stage 6 intends it.
# ---------------------------------------------------------------------------


def test_error1_get_after_asyncio_conversion():
    """get() after an asyncio await (_Asyncio). The value is fully available in
    the bridged asyncio.Future, yet get() refuses it and says to use 'await' --
    the guard is on state, not data."""
    fut: Future[int] = Future(coro=_value(1))
    asyncio.run(_await_once(fut))  # -> _Asyncio
    with pytest.raises(ValueError, match=r"already converted into an asyncio\.Future"):
        fut.get()


def test_error2_get_after_tokio_conversion():
    """get() after a tokio await (_Tokio): symmetric to error1 -- once converted
    to a pytokio Shared, get() refuses and points at awaiting inside a
    from_coroutine coroutine."""
    fut: Future[int] = Future(coro=_value(1))
    _run_in_tokio(_await_once(fut))  # -> _Tokio
    with pytest.raises(
        ValueError, match=r"already converted into a pytokio\.Shared object"
    ):
        fut.get()


def test_error4_await_asyncio_on_tokio_converted():
    """Cross-runtime: convert on tokio (_Tokio), then await under asyncio -- the
    asyncio branch refuses a tokio-converted Future."""
    fut: Future[int] = Future(coro=_value(1))
    _run_in_tokio(_await_once(fut))  # -> _Tokio

    async def attempt():
        with pytest.raises(ValueError, match="already converted into a tokio future"):
            await fut

    asyncio.run(attempt())


def test_error5_await_asyncio_after_resolved():
    """await under asyncio after a sync get() resolved it (_Complete): the
    synchronous world was already chosen, so the asyncio await is refused."""
    fut: Future[int] = Future(coro=_value(1))
    assert fut.get() == 1  # -> _Complete

    async def attempt():
        with pytest.raises(
            ValueError, match="already converted into a synchronous future"
        ):
            await fut

    asyncio.run(attempt())


def test_error6_await_tokio_on_asyncio_converted():
    """Cross-runtime mirror of error4: convert on asyncio (_Asyncio), then await
    on a tokio thread -- the tokio branch refuses an asyncio-converted Future."""
    fut: Future[int] = Future(coro=_value(1))
    asyncio.run(_await_once(fut))  # -> _Asyncio

    async def attempt():
        await fut

    with pytest.raises(ValueError, match="already converted into asyncio future"):
        _run_in_tokio(attempt())


def test_error7_await_tokio_after_resolved():
    """await on a tokio thread after a sync get() resolved it (_Complete): the
    same refusal as error5 (same message), reached from the tokio branch."""
    fut: Future[int] = Future(coro=_value(1))
    assert fut.get() == 1  # -> _Complete

    async def attempt():
        await fut

    with pytest.raises(ValueError, match="already converted into a synchronous future"):
        _run_in_tokio(attempt())


def test_error8_await_with_no_event_loop():
    """await with neither an asyncio loop nor a tokio runtime active: there is no
    driver, so __await__ refuses outright."""
    fut: Future[int] = Future(coro=_value(1))
    with pytest.raises(ValueError, match="no active event loop"):
        fut.__await__()


# ---------------------------------------------------------------------------
# get()-inside-a-loop warning -- both branches
#
# The warning fires for asyncio OR tokio callers and forwards a tracing event
# with extra["context"] = "asyncio"/"tokio". We assert the observable seam by
# monkeypatching the module-level log_with_tracing rather than reading Rust
# tracing output.
# ---------------------------------------------------------------------------


def test_get_in_asyncio_loop_warns_and_still_returns(monkeypatch):
    """get() inside an asyncio loop (main thread) warns and forwards a tracing
    event with context='asyncio', but -- because the block is legal on the main
    thread -- still returns the value (backward-compatible)."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))

    async def runner():
        fut: Future[int] = Future(coro=_value(5))
        with pytest.warns(UserWarning, match="active event loop"):
            return fut.get()

    assert asyncio.run(runner()) == 5
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "asyncio"


def test_get_in_tokio_thread_warns_then_cannot_block(monkeypatch):
    """On a real tokio thread get() warns (context='tokio') but then cannot
    block within the runtime, so it raises rather than returning -- the
    asymmetry with the asyncio branch, and precisely the case WouldBlockRuntime
    is meant to replace."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))
    fut: Future[int] = Future(coro=_value(5))

    async def attempt():
        assert is_tokio_thread()
        return fut.get()

    with pytest.raises(BaseException, match="from within a runtime"):
        _run_in_tokio(attempt())
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "tokio"


# ---------------------------------------------------------------------------
# _take_inner() and the _Taken terminal state
#
# _take_inner() requires an unawaited Future -- it fails if the Future was
# already resolved or converted by get()/await. On success it surrenders the
# underlying PythonTask to a caller that drives it directly and transitions the
# Future to the terminal _Taken state, so the Future is spent: a second take,
# or any later get()/await, fails rather than silently re-driving the one-shot
# task.
# ---------------------------------------------------------------------------


def test_take_inner_returns_task_and_marks_future_taken():
    """_take_inner() on an unawaited Future returns the underlying (still
    drivable) PythonTask and transitions the Future to the terminal _Taken
    state."""
    fut: Future[int] = Future(coro=_value(1))
    task = fut._take_inner()
    assert isinstance(task, PythonTask)
    assert isinstance(fut._status, future_mod._Taken)
    assert task.block_on() == 1


def test_take_inner_twice_raises():
    """The Future is spent after the first _take_inner(); a second raises."""
    fut: Future[int] = Future(coro=_value(1))
    fut._take_inner()
    with pytest.raises(ValueError, match="already been awaited"):
        fut._take_inner()


def test_get_after_take_inner_raises():
    """get() after _take_inner() fails at the Future instead of re-driving the
    surrendered task."""
    fut: Future[int] = Future(coro=_value(1))
    fut._take_inner()
    with pytest.raises(ValueError, match="consumed"):
        fut.get()


async def test_await_asyncio_after_take_inner_raises():
    """await under asyncio after _take_inner() fails instead of re-driving."""
    fut: Future[int] = Future(coro=_value(1))
    fut._take_inner()
    with pytest.raises(ValueError, match="consumed"):
        await fut


def test_await_tokio_after_take_inner_raises():
    """await on a tokio thread after _take_inner() fails instead of re-driving."""
    fut: Future[int] = Future(coro=_value(1))
    fut._take_inner()

    async def attempt():
        await fut

    with pytest.raises(ValueError, match="consumed"):
        _run_in_tokio(attempt())


def test_take_inner_after_get_raises():
    """_take_inner() requires an unawaited Future: once get() has resolved it
    (_Complete), taking the inner task is refused."""
    fut: Future[int] = Future(coro=_value(1))
    assert fut.get() == 1  # -> _Complete
    with pytest.raises(ValueError, match="already been awaited"):
        fut._take_inner()


def test_take_inner_after_asyncio_await_raises():
    """Once an asyncio await has converted the Future (_Asyncio), _take_inner()
    is refused."""
    fut: Future[int] = Future(coro=_value(1))
    asyncio.run(_await_once(fut))  # -> _Asyncio
    with pytest.raises(ValueError, match="already been awaited"):
        fut._take_inner()


def test_take_inner_after_tokio_await_raises():
    """Once a tokio await has converted the Future (_Tokio), _take_inner() is
    refused."""
    fut: Future[int] = Future(coro=_value(1))
    _run_in_tokio(_await_once(fut))  # -> _Tokio
    with pytest.raises(ValueError, match="already been awaited"):
        fut._take_inner()
