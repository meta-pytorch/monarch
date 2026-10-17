# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Characterization oracle for ``Future`` and its permanent ``Handle`` carrier.

Pins the behavior of ``monarch._src.actor.future.Future`` -- its internal
states and the transitions between them, ``as_asyncio()`` and the ``__await__``
shim over it, rejection of Tokio-thread awaits, the ``get()``-inside-a-loop
warning -- plus the construction and completion invariants of its permanent ``Handle``
carrier, so that any change to that behavior is caught here and made explicit.
"""

import _thread
import asyncio
import importlib
import sys
import threading
import time
import warnings
from typing import Any, Callable, cast, NamedTuple

import pytest
from monarch._rust_bindings.monarch_hyperactor.handle import (
    _HandleCompleter,
    _new_handle_pair,
    Handle,
    WouldBlockRuntime,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.runtime import _is_in_tokio_runtime
from monarch._rust_bindings.monarch_hyperactor.testing import (
    _handle_receiver_count,
    _make_handle_probe,
    _PROBE_SUCCESS_VALUE,
)
from monarch._src.actor import future as future_mod
from monarch._src.actor.future import Future
from monarch._src.actor.returns_future import returns_future


async def _value(v):
    return v


async def _raise(exc: BaseException):
    raise exc


async def _sleep_then(seconds: float, v):
    await PythonTask.sleep(seconds)
    return v


async def _await_once(fut: Future):
    return await fut


def _future_pending() -> "tuple[Any, Callable[[], None]]":
    """A facade observable that stays pending until it is drained.

    Deliberately not a long timed sleep. A ``PythonTask.sleep(3600)`` cannot
    race a short-timeout assertion, but it outlives the test: the runtime still
    owns it at interpreter exit, and outstanding tasks are a documented source
    of SIGABRT there. A gate the test controls has neither problem -- the task
    cannot finish early, so the timeout assertion is race-free, and ``drain``
    ends it before the test returns.
    """
    gate = {"released": False}

    async def hold():
        while not gate["released"]:
            await PythonTask.sleep(0.005)
        return _PROBE_SUCCESS_VALUE

    fut: Future[int] = Future._from_coro(hold())

    def drain() -> None:
        gate["released"] = True
        fut.get()  # block until the task has actually finished

    return fut, drain


def _run_in_tokio(coro):
    # Drive ``coro`` to completion on the Tokio runtime. Code inside ``coro``
    # runs on a Tokio worker thread, where ``_is_in_tokio_runtime()`` is True and a
    # ``Future`` takes its ``__await__`` tokio branch. ``block_on`` itself runs
    # on the (non-worker) calling thread, so it is allowed to block.
    #
    # ``PythonTask.spawn_blocking`` is NOT an equivalent driver, even though
    # ``_is_in_tokio_runtime()`` is also True there: a blocking-pool thread tolerates
    # nested blocking, while a runtime worker panics on it. Any Tokio case that
    # turns on refusing to block must use this worker path, or it silently
    # proves nothing.
    return PythonTask.from_coroutine(coro).block_on()


# ---------------------------------------------------------------------------
# States and transitions
#
# A fresh Future is _Unawaited; the first access drives the underlying task
# exactly once. Sync get() caches into _Complete/_Exception; as_asyncio() and
# the asyncio __await__ shim spawn a Handle and move to the observe-only
# _Handle. A Tokio-thread await is rejected without starting the task or
# changing its state. These pin that lifecycle and its idempotency.
# ---------------------------------------------------------------------------


def test_handle_binding_has_permanent_module_identity():
    """Attests HDL-16."""
    pytokio = importlib.import_module(
        "monarch._rust_bindings.monarch_hyperactor.pytokio"
    )

    # HDL-16: Handle has one permanent module, and its surfaces do not leak
    # from pytokio.
    assert Handle.__module__ == "monarch._rust_bindings.monarch_hyperactor.handle"
    assert (
        WouldBlockRuntime.__module__
        == "monarch._rust_bindings.monarch_hyperactor.handle"
    )
    assert not hasattr(pytokio, "Handle")
    assert not hasattr(pytokio, "WouldBlockRuntime")
    assert not hasattr(pytokio, "is_tokio_thread")


def test_direct_construction_is_rejected():
    with pytest.raises(
        TypeError,
        match="Future objects are returned by Monarch operations",
    ):
        Future()
    with pytest.raises(TypeError, match="unexpected keyword argument 'coro'"):
        Future(coro=None)


def test_from_handle_stores_the_supplied_handle_without_starting_work(monkeypatch):
    probe = _probe("success")
    handle = cast("Any", probe._handle)
    observations = []
    original_get = Handle.get

    def unexpected_observation(*args, **kwargs):
        observations.append((args, kwargs))
        raise AssertionError("_from_handle() must not observe the Handle")

    for method in ("poll", "get", "as_asyncio", "__await__"):
        monkeypatch.setattr(Handle, method, unexpected_observation)
    try:
        fut = Future._from_handle(handle)
        assert isinstance(fut._status, future_mod._Handle)
        assert fut._status.handle is handle
        assert not probe._completed
        assert observations == []
    finally:
        original_error = sys.exc_info()[1]
        probe._release()
        try:
            original_get(handle, timeout=2)
        except Exception as cleanup_error:
            if original_error is None:
                raise
            # BaseException.add_note() is unavailable on the Python 3.10 OSS
            # test lane. The original failure still takes precedence there.
            if hasattr(original_error, "add_note"):
                original_error.add_note(f"cleanup also failed: {cleanup_error!r}")


def test_from_handle_rejects_non_handle_values():
    with pytest.raises(TypeError, match="expected Handle"):
        Future._from_handle(cast("Any", object()))


def test_as_handle_spawns_an_unawaited_task_once(monkeypatch):
    spawn_calls = 0
    original_spawn_handle = PythonTask.spawn_handle

    def counted_spawn_handle(task):
        nonlocal spawn_calls
        spawn_calls += 1
        return original_spawn_handle(task)

    monkeypatch.setattr(PythonTask, "spawn_handle", counted_spawn_handle)

    gate = {"released": False}
    runs = {"started": 0, "completed": 0}

    async def controlled():
        runs["started"] += 1
        while not gate["released"]:
            await PythonTask.sleep(0.005)
        runs["completed"] += 1
        return 7

    fut: Future[int] = Future._from_coro(controlled())
    first = None
    try:
        first = fut._as_handle()
        second = fut._as_handle()
        assert spawn_calls == 1
        assert first is second

        gate["released"] = True
        assert first.get() == 7
        assert fut.get() == 7
        assert runs == {"started": 1, "completed": 1}
    finally:
        original_error = sys.exc_info()[1]
        gate["released"] = True
        try:
            observer = first if first is not None else fut
            observer.get(timeout=2)
        except Exception as cleanup_error:
            if original_error is None:
                raise
            # BaseException.add_note() is unavailable on the Python 3.10 OSS
            # test lane. The original failure still takes precedence there.
            if hasattr(original_error, "add_note"):
                original_error.add_note(f"cleanup also failed: {cleanup_error!r}")


def test_as_handle_rejects_terminal_states_without_mutation():
    complete: Future[int] = Future._from_coro(_value(1))
    assert complete.get() == 1

    failed: Future[int] = Future._from_coro(_raise(ValueError("boom")))
    with pytest.raises(ValueError, match="boom"):
        failed.get()

    for fut in (complete, failed):
        status = fut._status
        with pytest.raises(ValueError, match="does not have a Handle"):
            fut._as_handle()
        assert fut._status is status


def test_get_success_transitions_to_complete_and_is_idempotent():
    """First sync get() drives the task, transitions _Unawaited -> _Complete,
    and returns the value; a second get() returns the cached value without
    re-running the coroutine. The coroutine must run exactly once, so
    resolve-and-cache is the contract being pinned."""
    fut: Future[int] = Future._from_coro(_value(42))
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
    fut: Future[int] = Future._from_coro(_raise(ValueError("boom")))
    with pytest.raises(ValueError, match="boom") as first:
        fut.get()
    with pytest.raises(ValueError, match="boom") as second:
        fut.get()
    assert second.value is first.value


def test_get_timeout_raises_timeout_error():
    """get(timeout=...) observes the task through a Handle with the given
    deadline, so an unfinished task surfaces a TimeoutError rather than hanging.
    The timeout is non-cancelling (state becomes _Handle); the sibling
    test_get_timeout_is_non_poisoning_and_reobservable pins re-observation."""
    fut, drain = _future_pending()
    try:
        with pytest.raises(TimeoutError):
            fut.get(timeout=0.1)
    finally:
        drain()


def test_get_timeout_is_non_poisoning_and_reobservable():
    """get(timeout=...) that times out routes through a Handle and does NOT
    poison the Future: it transitions to _Handle, and a later get() (no timeout)
    still resolves the same task (the timeout is non-cancelling)."""
    fut: Future[int] = Future._from_coro(_sleep_then(0.2, 11))
    with pytest.raises(TimeoutError):
        fut.get(timeout=0.01)  # too short: task still running
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 11  # later get() still observes completion


def test_get_timeout_success_returns_and_transitions_to_handle():
    """get(timeout=...) whose task finishes within the timeout returns the value
    and transitions to the observe-only _Handle (not _Complete): the timed get
    routes through a Handle, so a later no-timeout get() re-observes the result."""
    fut: Future[int] = Future._from_coro(_sleep_then(0.01, 7))
    assert fut.get(timeout=5) == 7
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 7


async def test_await_asyncio_transitions_to_handle_and_is_reobservable():
    """Awaiting under an asyncio loop bridges through as_asyncio(), transitions
    _Unawaited -> the observe-only _Handle, and yields the value; re-awaiting
    observes the same Handle (a fresh loop-local future each time)."""
    fut: Future[int] = Future._from_coro(_value(7))
    assert await fut == 7  # _Unawaited -> _Handle
    assert isinstance(fut._status, future_mod._Handle)
    assert await fut == 7  # re-observe the same Handle


def test_nested_future_await_raises_without_consuming_inner_future():
    """An outer Future drives its coroutine on Tokio, where awaiting an inner
    Future is rejected without starting or consuming the inner task. A later
    valid observer can still drive the inner task exactly once."""
    started = []

    async def value():
        started.append("started")
        return 9

    fut: Future[int] = Future._from_coro(value())

    async def driver():
        assert _is_in_tokio_runtime()
        with pytest.raises(RuntimeError) as caught:
            await fut
        return str(caught.value)

    outer: Future[str] = Future._from_coro(driver())
    assert outer.get() == (
        "Future cannot be awaited on a Tokio thread; observe it from an "
        "asyncio loop or synchronous context."
    )
    assert started == []
    assert isinstance(fut._status, future_mod._Unawaited)
    assert fut.get() == 9
    assert started == ["started"]


# ---------------------------------------------------------------------------
# Observation is non-consuming now: get()/as_asyncio()/await mix freely on the
# asyncio+sync side (get after await, await after get, repeated as_asyncio all
# resolve). A _Handle awaited on a Tokio thread is still rejected. These pin
# both the supported observations and the remaining cross-driver guard.
# ---------------------------------------------------------------------------


def test_get_after_asyncio_await_returns():
    """get() after an asyncio await (_Handle) now returns the value instead of
    raising -- observation is non-consuming, so the sync get() observes the same
    Handle."""
    fut: Future[int] = Future._from_coro(_value(1))
    asyncio.run(_await_once(fut))  # -> _Handle
    assert fut.get() == 1


def test_get_after_asyncio_await_error_reraises():
    """Failure mirror of the previous: get() on a _Handle whose live producer
    FAILED re-raises the stored exception through handle.get(). Drives the live
    spawn_handle producer (not a pre-resolved get())."""
    fut: Future[int] = Future._from_coro(_raise(ValueError("boom")))

    async def bridge():
        # Drive _Unawaited -> _Handle via the live producer; swallow the failure
        # so we can re-observe it via get() below.
        with pytest.raises(ValueError, match="boom"):
            await fut

    asyncio.run(bridge())
    assert isinstance(fut._status, future_mod._Handle)
    with pytest.raises(ValueError, match="boom"):
        fut.get()


def test_await_asyncio_after_get_returns():
    """await under asyncio after a sync get() resolved it (_Complete) now returns
    the value -- as_asyncio() hands back a settled loop future instead of
    raising."""
    fut: Future[int] = Future._from_coro(_value(1))
    assert fut.get() == 1  # -> _Complete

    async def attempt():
        return await fut

    assert asyncio.run(attempt()) == 1


def test_await_asyncio_after_get_exception_reraises_stored():
    """Failure mirror of the previous: await under asyncio after a failed sync
    get() (_Exception) hands back a settled failed loop future that re-raises the
    *same* stored exception object."""
    boom = ValueError("boom")
    fut: Future[int] = Future._from_coro(_raise(boom))
    with pytest.raises(ValueError, match="boom"):
        fut.get()  # -> _Exception

    async def attempt():
        with pytest.raises(ValueError, match="boom") as caught:
            await fut
        return caught.value

    assert asyncio.run(attempt()) is boom


def test_await_asyncio_live_handle_error_propagates():
    """A failing coroutine driven LIVE through the Handle producer (spawn_handle
    via as_asyncio, from _Unawaited -- not pre-resolved) surfaces the exception
    through the bridged asyncio future: exercises send_result(Err) -> observer ->
    set_exception, which the pre-resolved settled-future path does not."""
    fut: Future[int] = Future._from_coro(_raise(ValueError("boom")))

    async def attempt():
        with pytest.raises(ValueError, match="boom"):
            await fut  # _Unawaited -> live _Handle, error via the observer

    asyncio.run(attempt())


def test_await_tokio_on_handle_bridged_raises():
    """Convert on asyncio (_Handle), then await on a tokio thread: Handle's
    await path reports asyncio's native no-running-loop error."""
    fut: Future[int] = Future._from_coro(_value(1))
    asyncio.run(_await_once(fut))  # -> _Handle

    async def attempt():
        await fut

    with pytest.raises(RuntimeError, match="no running event loop") as caught:
        _run_in_tokio(attempt())
    assert not isinstance(caught.value, WouldBlockRuntime)


def test_await_tokio_after_get_raises_synchronous_future():
    """await on a tokio thread after a sync get() resolved it (_Complete): the
    tokio branch refuses with the 'already converted into a synchronous future'
    guard."""
    fut: Future[int] = Future._from_coro(_value(1))
    assert fut.get() == 1  # -> _Complete

    async def attempt():
        await fut

    with pytest.raises(ValueError, match="already converted into a synchronous future"):
        _run_in_tokio(attempt())


def test_await_tokio_after_get_exception_raises_synchronous_future():
    """Failure mirror of test_await_tokio_after_get_raises_synchronous_future:
    await on a tokio thread after a failed sync get() (_Exception) hits the same
    'synchronous future' guard as _Complete."""
    fut: Future[int] = Future._from_coro(_raise(ValueError("boom")))
    with pytest.raises(ValueError, match="boom"):
        fut.get()  # -> _Exception

    async def attempt():
        await fut

    with pytest.raises(ValueError, match="already converted into a synchronous future"):
        _run_in_tokio(attempt())


def test_await_with_no_event_loop_raises_and_is_non_consuming():
    """await with neither an asyncio loop nor a tokio runtime active: there is no
    driver, so __await__ refuses outright -- and refusing consumes nothing, so
    the Future stays _Unawaited and a later get() still drives it."""
    fut: Future[int] = Future._from_coro(_value(1))
    with pytest.raises(ValueError, match="no active event loop"):
        fut.__await__()
    assert isinstance(fut._status, future_mod._Unawaited)
    assert fut.get() == 1


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
        fut: Future[int] = Future._from_coro(_value(5))
        with pytest.warns(UserWarning, match="active event loop") as record:
            value = fut.get()
        # the asyncio-context advice is as_asyncio()/await (both valid on a loop)
        assert any("as_asyncio" in str(w.message) for w in record)
        return value

    assert asyncio.run(runner()) == 5
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "asyncio"


def test_get_in_tokio_thread_raises_would_block_and_is_non_consuming(monkeypatch):
    """On a real tokio thread no-timeout get() raises WouldBlockRuntime up front
    (aligned with Handle.get()) WITHOUT consuming the task: no UserWarning fires,
    the tracing event still forwards (context='tokio'), the Future stays
    _Unawaited, and a later get() from a sync context still drives it to a
    value. (Previously it warned, then block_on() consumed the task and panicked
    'from within a runtime', losing the work and bricking the Future.)"""
    calls = []
    warned = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))
    monkeypatch.setattr(
        future_mod.warnings, "warn", lambda msg, *a, **k: warned.append(str(msg))
    )
    fut: Future[int] = Future._from_coro(_value(5))

    async def attempt():
        assert _is_in_tokio_runtime()
        return fut.get()

    with pytest.raises(
        WouldBlockRuntime,
        match="observe the Future from a synchronous or asyncio context",
    ):
        _run_in_tokio(attempt())
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "tokio"
    assert warned == []  # tokio get() refuses instead of warning
    assert isinstance(fut._status, future_mod._Unawaited)  # task not consumed
    assert fut.get() == 5  # still drivable from a sync context


def test_get_timeout_in_tokio_thread_raises_would_block_and_is_non_consuming(
    monkeypatch,
):
    """get(timeout=...) on a tokio thread is refused by the in_tokio check up
    front -- BEFORE spawning a Handle -- so it raises WouldBlockRuntime without
    starting work or flipping state: no UserWarning, the trace still forwards
    (context='tokio'), the Future stays _Unawaited, and a later sync get() still
    drives it."""
    calls = []
    warned = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))
    monkeypatch.setattr(
        future_mod.warnings, "warn", lambda msg, *a, **k: warned.append(str(msg))
    )
    fut: Future[int] = Future._from_coro(_value(5))

    async def attempt():
        assert _is_in_tokio_runtime()
        return fut.get(timeout=0.1)

    with pytest.raises(
        WouldBlockRuntime,
        match="observe the Future from a synchronous or asyncio context",
    ):
        _run_in_tokio(attempt())
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "tokio"
    assert warned == []
    assert isinstance(fut._status, future_mod._Unawaited)  # not spawned/consumed
    assert fut.get() == 5  # still drivable from a sync context


def test_get_invalid_timeout_does_not_spawn_or_mutate():
    """An invalid timeout (NaN/negative/non-finite) is rejected with ValueError
    BEFORE spawning a Handle, so a bad argument never starts work or flips state:
    the Future stays _Unawaited and a later valid get() still drives it."""
    for bad in (float("nan"), -1.0, float("inf")):
        fut: Future[int] = Future._from_coro(_value(8))
        with pytest.raises(ValueError, match="invalid timeout"):
            fut.get(timeout=bad)
        assert isinstance(fut._status, future_mod._Unawaited)
        assert fut.get() == 8  # still drivable after a rejected bad-timeout call


def test_get_on_handle_in_asyncio_loop_warns_and_forwards_once(monkeypatch):
    """get() on an already-bridged Future (_Handle) from inside an asyncio loop
    forwards exactly one tracing event (context='asyncio') and warns exactly
    once: Future.get() forwards the trace, Handle.get() emits the UserWarning,
    and the _Handle branch does not re-warn."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))

    async def runner():
        fut: Future[int] = Future._from_coro(_value(5))
        fut.as_asyncio()  # -> _Handle
        with pytest.warns(UserWarning) as caught:
            value = fut.get()
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        return value, len(user_warnings)

    value, n_user_warnings = asyncio.run(runner())
    assert value == 5
    assert n_user_warnings == 1
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "asyncio"


def test_get_in_loop_on_cached_state_traces_without_warning(monkeypatch):
    """get() inside an asyncio loop on an already-cached Future (_Complete or
    _Exception) forwards exactly one tracing event per call and emits NO
    UserWarning: the warning is reserved for paths that actually block the loop,
    and a cached read does not."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))

    done: Future[int] = Future._from_coro(_value(5))
    assert done.get() == 5  # off loop -> _Complete
    failed: Future[int] = Future._from_coro(_raise(ValueError("boom")))
    with pytest.raises(ValueError, match="boom"):
        failed.get()  # off loop -> _Exception
    calls.clear()

    async def runner():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert done.get() == 5  # cached _Complete, inside the loop
            with pytest.raises(ValueError, match="boom"):
                failed.get()  # cached _Exception, inside the loop
        return [w for w in caught if issubclass(w.category, UserWarning)]

    assert asyncio.run(runner()) == []
    assert len(calls) == 2  # one tracing forward per in-loop get()
    assert all(c["extra"]["context"] == "asyncio" for c in calls)


def test_handle_and_would_block_runtime_are_importable():
    """The permanent Handle bindings expose both types, with the right base."""
    assert issubclass(WouldBlockRuntime, RuntimeError)
    assert isinstance(Handle, type)


def test_handle_completion_pair_resolves_once_and_replays_value():
    """The private producer publishes once; Handle observation is repeatable."""
    handle, completer = _new_handle_pair()

    completer.set_result(42)

    assert handle.get() == 42
    assert handle.get() == 42
    with pytest.raises(RuntimeError, match="already been completed"):
        completer.set_result(7)
    assert handle.get() == 42


def test_pending_handle_get_interrupts_without_consuming_completion() -> None:
    """Ctrl-C interrupts a main-thread native wait, not the paired producer."""
    assert threading.current_thread() is threading.main_thread()
    handle, completer = _new_handle_pair()
    assert _handle_receiver_count(completer) == 1
    entered = threading.Event()
    stopped = threading.Event()
    fallback_used = threading.Event()
    interrupt_lock = threading.Lock()

    def interrupt_pending_get() -> None:
        try:
            deadline = time.monotonic() + 5
            while _handle_receiver_count(completer) < 2:
                if stopped.wait(0.001) or time.monotonic() >= deadline:
                    return
            # Only get() can have cloned this pair's receiver. This thread
            # holds the GIL now, so get() has also released it: the native
            # pending call is active, not just about to start on a Python line.
            with interrupt_lock:
                if stopped.is_set():
                    return
                entered.set()
                _thread.interrupt_main()
            stopped.wait(5)
        finally:
            if not stopped.is_set():
                # Bound a broken signal wait. Mark this BEFORE completion:
                # a queued interrupt delivered on return to Python must not
                # make pytest.raises falsely pass after the fallback unblocks.
                fallback_used.set()
                completer.set_result(42)

    helper = threading.Thread(target=interrupt_pending_get, daemon=True)
    helper.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            handle.get()
    finally:
        # Cancel under the same lock as the request, so the helper cannot send
        # a delayed interrupt after cleanup begins, even if get() failed early.
        with interrupt_lock:
            stopped.set()
        helper.join(timeout=6)
    assert not helper.is_alive(), "the interrupt helper must finish inside the test"
    assert entered.is_set(), "the interrupt must follow entry into the native wait"
    assert not fallback_used.is_set(), "get() must respond before fallback completion"
    assert _handle_receiver_count(completer) == 1, "the interrupted wait must be gone"

    completer.set_result(42)
    assert handle.get() == 42
    assert handle.get() == 42


def test_handle_completion_pair_preserves_exception():
    """Failure publication preserves the exception object for every observer."""

    class CompletionFailure(BaseException):
        pass

    error = CompletionFailure("completion failed")
    handle, completer = _new_handle_pair()
    completer.set_exception(error)

    for _ in range(2):
        with pytest.raises(CompletionFailure) as raised:
            handle.get()
        assert raised.value is error


def test_handle_completion_pair_accepts_exception_as_value():
    """The caller, not the value's type, chooses success versus failure."""
    value = RuntimeError("successful value")
    handle, completer = _new_handle_pair()

    completer.set_result(value)

    assert handle.get() is value


def test_handle_completion_pair_rejects_non_exception_without_consuming():
    """set_exception() rejects non-exceptions without consuming the completer."""
    handle, completer = _new_handle_pair()

    with pytest.raises(TypeError, match="BaseException"):
        cast(Any, completer).set_exception("not an exception")
    completer.set_result(42)

    assert handle.get() == 42


def test_dropped_handle_completer_surfaces_error():
    """Dropping the sole producer closes the Handle instead of stranding it."""
    handle, completer = _new_handle_pair()

    del completer

    with pytest.raises(
        RuntimeError, match="Handle producer ended without publishing a result"
    ):
        handle.get(timeout=2)


def test_handle_completion_survives_observer_drop():
    """Dropping the Handle does not consume or invalidate its producer."""
    handle, completer = _new_handle_pair()

    del handle

    completer.set_result(42)
    with pytest.raises(RuntimeError, match="already been completed"):
        completer.set_result(7)


def test_handle_completion_pair_types_are_not_directly_constructible():
    """HDL-11 permits only atomic creation through the private pair factory."""
    with pytest.raises(TypeError):
        Handle()
    with pytest.raises(TypeError):
        _HandleCompleter()


# ---------------------------------------------------------------------------
# as_asyncio() observation semantics
#
# as_asyncio() spawns a Handle once and observes it: observation is
# non-consuming, each call returns a fresh loop-local future, it requires a
# running loop, and it must not consume the task when there is none.
# ---------------------------------------------------------------------------


def test_as_asyncio_off_loop_raises_and_is_non_consuming():
    """Off a running loop, as_asyncio() raises RuntimeError WITHOUT spawning: the
    Future stays _Unawaited, so a later get() still drives the task."""
    fut: Future[int] = Future._from_coro(_value(3))
    with pytest.raises(RuntimeError):
        fut.as_asyncio()
    assert isinstance(fut._status, future_mod._Unawaited)
    assert fut.get() == 3


async def test_as_asyncio_twice_same_loop_both_resolve():
    """Two as_asyncio() futures from one Future on the same loop both resolve to
    the value (ordinary multi-observer case); the Future is _Handle after the
    first."""
    fut: Future[int] = Future._from_coro(_value(4))
    f1 = fut.as_asyncio()
    assert isinstance(fut._status, future_mod._Handle)
    f2 = fut.as_asyncio()
    assert await f1 == 4
    assert await f2 == 4


async def test_as_asyncio_cancel_one_then_await_again_resolves():
    """Cancelling one as_asyncio() observer does not poison the Future: a later
    await still resolves (each observer is a fresh loop-local future)."""
    fut: Future[int] = Future._from_coro(_value(5))
    f1 = fut.as_asyncio()
    f1.cancel()
    assert await fut == 5


def test_as_asyncio_across_two_loops_both_resolve():
    """The same Future observed from two different event loops resolves in each
    (each as_asyncio() binds a fresh future to the current loop)."""
    fut: Future[int] = Future._from_coro(_value(6))

    async def observe():
        return await fut

    assert asyncio.run(observe()) == 6
    assert asyncio.run(observe()) == 6


async def test_as_asyncio_on_cached_stop_iteration_wraps_in_runtime_error():
    """as_asyncio() on a cached _Exception must wrap a StopIteration in
    RuntimeError before set_exception: asyncio.Future.set_exception rejects
    StopIteration (PEP 479) with TypeError, which would otherwise brick the
    returned future. Mirrors the Handle observer path (HDL-7). A StopIteration
    can't reach _Exception via from_coroutine (it becomes a normal return), so
    the cached state is set directly."""
    fut: Future[int] = Future._from_coro(_value(1))
    fut._status = future_mod._Exception(StopIteration("done"))
    settled = fut.as_asyncio()
    with pytest.raises(RuntimeError, match="StopIteration"):
        await settled


# ---------------------------------------------------------------------------
# Future/Handle contract matrix
#
# Three proof layers, kept structurally apart on purpose:
#
#   * incumbent Future oracle -- what the public facade does today;
#   * direct Handle oracle    -- what the underlying Handle does, observed
#                                through a real Rust-produced Handle rather
#                                than through the facade;
#   * Handle-backed facade    -- how a Future built directly on a Handle
#                                behaves through Future._from_handle().
#
# Every target assertion lives in an ``_assert_*`` helper that takes a
# *factory*, never a hard-coded constructor, and each row is driven by one of
# four registries below. The Handle-backed Future entries run those assertions
# against the facade without changing the assertion bodies.
#
# Assertions that pin an incumbent divergence are deliberately NOT
# factory-driven: they describe behavior that is going away, and there is
# nothing to activate.
#
# Registry per row (every one of the 15 has a seam):
#
#   PRESERVED_BY_BOTH  -- target already satisfied by both representations
#     fm.repeated_observation, fm.exception_identity, fm.off_loop_as_asyncio
#   TARGET_ONLY        -- ready observables using the target Handle semantics
#     fm.ready_get_on_loop_warns, fm.ready_get_on_tokio_refuses,
#     fm.invalid_timeout_on_ready, fm.ready_as_asyncio_settlement,
#     fm.terminal_baseexception_reobservable, fm.off_loop_await, fm.tokio_await
#   GATED_TARGETS      -- (control, observable) pairs, producer held mid-flight
#     fm.timeout_then_success, fm.cancelled_observer
#   FACADE_FACTORIES   -- facade-only surfaces, no Handle primitive
#     fm.get_tracing_event, fm.result_alias, fm.exception_accessor
#
# Incumbent-divergence companions (not factory-driven, no activation):
#   fm.ready_get_on_tokio_refuses, fm.invalid_timeout_on_ready,
#   fm.terminal_baseexception_reobservable
#
# fm.cancelled_observer and fm.ready_as_asyncio_settlement also have an
# unparametrized current-side test, but both are *preserve* rows: both
# representations behave identically, so each such test is a second oracle for
# the same behavior, not a divergence.
#
# The permanent Handle-side oracle for fm.tokio_await lives in handle.rs, so
# that contract does not depend on a PythonTask-driven helper.
# ---------------------------------------------------------------------------


def _probe(outcome: str):
    """A gated probe: the producer has spawned but parks until released."""
    return _make_handle_probe(outcome)


def _ready_handle(outcome: str):
    """A genuinely ready Rust-produced Handle in ``outcome``'s terminal state.

    Released and waited synchronously rather than slept on, so readiness is
    deterministic. Returns the probe too, so a caller can assert its witnesses.
    """
    probe = _make_handle_probe(outcome)
    # Released before returning, so a caller that fails mid-assertion cannot
    # strand a parked producer; there is nothing left to reap.
    probe._release()
    probe._wait_completed()
    return probe, cast("Any", probe._handle)


# -- observable factories ----------------------------------------------------


def _future_observable(outcome: str):
    """An incumbent-facade observable matching the probe's outcomes."""
    if outcome == "success":
        return Future._from_coro(_value(_PROBE_SUCCESS_VALUE))
    if outcome == "exception":
        return Future._from_coro(_raise(ValueError("probe failure")))
    if outcome == "base_exception":
        return Future._from_coro(_raise(KeyboardInterrupt("probe base failure")))
    raise AssertionError(f"no incumbent observable for {outcome!r}")


class _Facade(NamedTuple):
    """A facade representation: terminal-state observables plus a pending one.

    Two callables rather than one because a *pending* observable needs a
    control to end it, and a terminal one does not. A Handle-backed entry
    supplies its own pair; no assertion body changes.
    """

    observable: "Callable[[str], Any]"
    pending: "Callable[[], tuple[Any, Callable[[], None]]]"


def _handle_observable(outcome: str):
    """A ready Rust-produced Handle in ``outcome``'s terminal state."""
    return _ready_handle(outcome)[1]


def _handle_gated():
    """A (control, observable) pair whose producer is held mid-flight."""
    probe = _probe("success")
    return probe, cast("Any", probe._handle)


def _handle_future_observable(outcome: str):
    """A Future facade over a ready Rust-produced Handle."""
    return Future._from_handle(_handle_observable(outcome))


def _handle_future_gated():
    """A gated producer observed through a Handle-backed Future."""
    control, handle = _handle_gated()
    return control, Future._from_handle(handle)


def _handle_future_pending():
    """A pending Handle-backed Future and a bounded drain callback."""
    control, future = _handle_future_gated()

    def drain() -> None:
        original_error = sys.exc_info()[1]
        control._release()
        try:
            future.get(timeout=2)
        except Exception as cleanup_error:
            if original_error is None:
                raise
            # BaseException.add_note() is unavailable on the Python 3.10 OSS
            # test lane. The original failure still takes precedence there.
            if hasattr(original_error, "add_note"):
                original_error.add_note(f"cleanup also failed: {cleanup_error!r}")

    return future, drain


@returns_future
async def _body(outcome: str):
    if outcome == "success":
        return _PROBE_SUCCESS_VALUE
    if outcome == "exception":
        raise ValueError("probe failure")
    if outcome == "base_exception":
        raise KeyboardInterrupt("probe base failure")
    raise AssertionError(f"no body observable for {outcome!r}")


# Target already satisfied by both representations.
PRESERVED_BY_BOTH = [
    pytest.param(_future_observable, id="current_future"),
    pytest.param(_handle_observable, id="raw_handle"),
    pytest.param(_handle_future_observable, id="handle_backed_future"),
    pytest.param(_body, id="returns_future_body"),
]

# Ready-observable rows that exercise the target Handle semantics. A body
# Future keeps the facade's Tokio and await guards instead.
TARGET_ONLY = [
    pytest.param(_handle_observable, id="raw_handle"),
    pytest.param(_handle_future_observable, id="handle_backed_future"),
]

# Rows needing a producer held mid-flight, plus the control that releases it.
GATED_TARGETS = [
    pytest.param(_handle_gated, id="raw_handle"),
    pytest.param(_handle_future_gated, id="handle_backed_future"),
]

# Facade-only surfaces: result()/exception()/tracing have no Handle primitive,
# so the seam starts with the incumbent Future and gains the Handle-backed one.
FACADE_FACTORIES = [
    pytest.param(_Facade(_future_observable, _future_pending), id="current_future"),
    pytest.param(
        _Facade(_handle_future_observable, _handle_future_pending),
        id="handle_backed_future",
    ),
]


# -- target-behavior assertions, shared across representations ---------------


def _assert_repeated_observation(make_observable):
    ok = make_observable("success")
    assert ok.get() == _PROBE_SUCCESS_VALUE
    assert ok.get() == _PROBE_SUCCESS_VALUE

    bad = make_observable("exception")
    with pytest.raises(ValueError, match="probe failure"):
        bad.get()
    with pytest.raises(ValueError, match="probe failure"):
        bad.get()


def _assert_exception_identity(make_observable):
    bad = make_observable("exception")
    with pytest.raises(ValueError) as first:
        bad.get()
    with pytest.raises(ValueError) as second:
        bad.get()
    assert second.value is first.value


def _assert_off_loop_as_asyncio_refuses(make_observable):
    ok = make_observable("success")
    with pytest.raises(RuntimeError) as caught:
        ok.as_asyncio()
    assert not isinstance(caught.value, WouldBlockRuntime)
    assert ok.get() == _PROBE_SUCCESS_VALUE  # refusing consumed nothing


def _assert_invalid_timeout_validated(make_observable):
    ok = make_observable("success")
    for bad in (float("nan"), -1.0, float("inf")):
        with pytest.raises(ValueError):
            ok.get(timeout=bad)
    assert ok.get() == _PROBE_SUCCESS_VALUE  # rejection was non-destructive


def _assert_terminal_baseexception_reobservable(make_observable):
    base = make_observable("base_exception")
    with pytest.raises(KeyboardInterrupt) as first:
        base.get()
    with pytest.raises(KeyboardInterrupt) as second:
        base.get()
    assert second.value is first.value


def _assert_off_loop_await_raises_native(make_observable):
    ok = make_observable("success")
    with pytest.raises(RuntimeError) as caught:
        ok.__await__()
    assert not isinstance(caught.value, WouldBlockRuntime)
    assert "no running event loop" in str(caught.value)
    assert ok.get() == _PROBE_SUCCESS_VALUE


def _assert_tokio_await_raises_native(make_observable):
    from monarch._src.actor.actor_mesh import _client_context

    ok = make_observable("success")

    async def attempt():
        assert _is_in_tokio_runtime()
        # WouldBlockRuntime subclasses RuntimeError, so a bare match would also
        # accept get()'s refusal or the root-client bootstrap guard. Identify
        # the raiser, and pin the client as initialized so the guard is
        # structurally unreachable.
        assert _client_context._val is not None, "bootstrap guard could fire"
        with pytest.raises(RuntimeError) as caught:
            ok.__await__()
        assert not isinstance(caught.value, WouldBlockRuntime)
        return str(caught.value)

    assert "no running event loop" in _run_in_tokio(attempt())
    assert ok.get() == _PROBE_SUCCESS_VALUE  # refusal consumed nothing


def _assert_ready_get_on_tokio_refuses(make_observable, message):
    from monarch._src.actor.actor_mesh import _client_context

    ok = make_observable("success")
    assert ok.get() == _PROBE_SUCCESS_VALUE  # ready before crossing over

    async def attempt():
        assert _is_in_tokio_runtime()
        assert _client_context._val is not None, "bootstrap guard could fire"
        with pytest.raises(WouldBlockRuntime) as caught:
            ok.get()
        return str(caught.value)

    # Each API refuses with its own message, which also tells it apart from
    # the bootstrap guard. A Handle-backed Future must not suggest poll(),
    # which Future does not have.
    assert _run_in_tokio(attempt()) == message


async def _assert_ready_get_on_loop_warns(make_observable):
    ok = make_observable("success")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert ok.get() == _PROBE_SUCCESS_VALUE
    matching = [
        w
        for w in caught
        if issubclass(w.category, UserWarning) and "event loop" in str(w.message)
    ]
    assert len(matching) == 1
    # Attributed to the caller's line, not to Monarch's own get() wrapper.
    assert matching[0].filename == __file__


def _assert_timeout_then_success(make_gated):
    control, observable = make_gated()
    # A gated producer parks until released, so a failing assertion would leave
    # it running past the test. Release and reap unconditionally.
    try:
        with pytest.raises(TimeoutError):
            observable.get(timeout=0.01)  # still gated
        control._release()
        assert observable.get() == _PROBE_SUCCESS_VALUE
    finally:
        control._release()
        control._wait_completed()


async def _assert_cancelled_observer(make_gated):
    control, observable = make_gated()
    try:
        first = observable.as_asyncio()
        assert first.cancel() is True
        assert first.cancelled()
        # The producer is provably still running while the observer dies; a
        # completed producer could otherwise mask a cancelled one.
        assert not control._completed

        control._release()
        # Awaiting the second observer is itself proof of publication.
        assert await observable.as_asyncio() == _PROBE_SUCCESS_VALUE
        assert control._completed
    finally:
        # Teardown only: on the happy path the await above already proved
        # publication. This blocks the loop, which is acceptable when the test
        # is already unwinding and the producer must not outlive it.
        control._release()
        control._wait_completed()


def _assert_result_aliases_get(facade):
    done = facade.observable("success")
    assert done.result() == _PROBE_SUCCESS_VALUE
    assert done.result() == done.get()

    failed = facade.observable("exception")
    with pytest.raises(ValueError, match="probe failure") as first:
        failed.result()
    with pytest.raises(ValueError, match="probe failure") as second:
        failed.get()
    assert second.value is first.value

    pending, drain = facade.pending()
    try:
        with pytest.raises(TimeoutError):
            pending.result(timeout=0.01)
    finally:
        drain()


def _assert_exception_accessor(facade):
    assert facade.observable("success").exception() is None

    returned = facade.observable("exception").exception()
    assert isinstance(returned, ValueError)
    assert str(returned) == "probe failure"

    pending, drain = facade.pending()
    try:
        assert isinstance(pending.exception(timeout=0.01), TimeoutError)
    finally:
        drain()

    with pytest.raises(KeyboardInterrupt):
        facade.observable("base_exception").exception()


def _assert_get_emits_one_tracing_event(facade, monkeypatch):
    """Exactly one tracing event per in-loop get(), in both driver contexts.

    Deliberately does not pin the refusal *message*: the incumbent facade and a
    Handle-backed one word it differently, and the row is about the trace, not
    the text. It does pin the exception type and rule out the bootstrap guard,
    so the tokio half cannot pass without the observer actually refusing.
    """
    from monarch._src.actor.actor_mesh import _client_context, context

    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))

    # asyncio: get() blocks the loop but still returns, tracing once.
    async def runner():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert facade.observable("success").get() == _PROBE_SUCCESS_VALUE

    asyncio.run(runner())
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "asyncio"

    # tokio: a fresh observable, refused by the observer rather than by the
    # root-client bootstrap guard, traced exactly once, and non-consuming.
    context()  # establish the client off-Tokio so the guard cannot fire
    calls.clear()
    fresh = facade.observable("success")

    async def attempt():
        assert _is_in_tokio_runtime()
        assert _client_context._val is not None, "bootstrap guard could fire"
        with pytest.raises(WouldBlockRuntime):
            fresh.get()

    _run_in_tokio(attempt())
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "tokio"
    assert fresh.get() == _PROBE_SUCCESS_VALUE  # refusal consumed nothing


# -- fm.repeated_observation -------------------------------------------------


@pytest.mark.parametrize("make_observable", PRESERVED_BY_BOTH)
def test_fm_repeated_observation_is_non_consuming(make_observable):
    """Repeated observation returns the same terminal state, for success and
    for an ordinary error, in every representation."""
    _assert_repeated_observation(make_observable)


# -- fm.exception_identity ---------------------------------------------------


@pytest.mark.parametrize("make_observable", PRESERVED_BY_BOTH)
def test_fm_exception_identity_preserves_object(make_observable):
    """Repeated observation yields the *same* Python exception object, not an
    equal copy. Asserted at object identity because comparing messages would
    pass even if each observation minted a fresh exception."""
    _assert_exception_identity(make_observable)


# -- fm.timeout_then_success -------------------------------------------------


@pytest.mark.parametrize("make_gated", GATED_TARGETS)
def test_fm_timeout_then_success_is_non_cancelling(
    make_gated: "Callable[[], tuple[Any, Any]]",
) -> None:
    """A timed-out get() leaves the producer running, so a later observer still
    sees the value."""
    _assert_timeout_then_success(make_gated)


# -- fm.cancelled_observer ---------------------------------------------------


@pytest.mark.parametrize("make_gated", GATED_TARGETS)
async def test_fm_cancelled_observer_survives_cancellation(make_gated):
    """Cancelling one observer cancels only that observer; the producer runs on
    and a later observer still receives the value."""
    await _assert_cancelled_observer(make_gated)


async def test_fm_cancelled_observer_current_survives_cancellation():
    """Incumbent facade: a cancellation, when it takes, is real, and a later
    await still resolves. The body can finish before ``as_asyncio()`` checks
    its Handle, which then returns an already-done observer that cannot be
    cancelled; the deterministic witness is ``_assert_cancelled_observer``."""
    fut: Future[int] = Future._from_coro(_value(5))
    first = fut.as_asyncio()
    if first.cancel():
        assert first.cancelled()
    assert await fut == 5


# -- fm.ready_get_on_loop_warns ----------------------------------------------


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
async def test_fm_ready_get_on_loop_warns_when_ready(make_observable):
    """Target behavior: get() warns on a running loop *even when already
    ready*, where a cached current Future reads silently (see
    test_get_in_loop_on_cached_state_traces_without_warning)."""
    await _assert_ready_get_on_loop_warns(make_observable)


# -- fm.ready_get_on_tokio_refuses -------------------------------------------


def test_fm_ready_get_on_tokio_refuses_current_cached_returns_and_reraises():
    """Incumbent divergence: a *cached* Future read on a Tokio thread is not
    refused -- the in_tokio guard only covers _Unawaited, so _Complete returns
    and _Exception re-raises."""
    done: Future[int] = Future._from_coro(_value(5))
    assert done.get() == 5  # -> _Complete, off loop
    failed: Future[int] = Future._from_coro(_raise(ValueError("boom")))
    with pytest.raises(ValueError, match="boom"):
        failed.get()  # -> _Exception, off loop

    async def attempt():
        assert _is_in_tokio_runtime()
        value = done.get()
        try:
            failed.get()
        except ValueError as err:
            return value, str(err)
        raise AssertionError("cached _Exception should have re-raised")

    assert _run_in_tokio(attempt()) == (5, "boom")


@pytest.mark.parametrize(
    ("make_observable", "message"),
    [
        pytest.param(
            _handle_observable,
            "get() cannot be called from a Tokio runtime context; "
            "use poll() or as_asyncio()",
            id="raw_handle",
        ),
        pytest.param(
            _handle_future_observable,
            "Future.get() cannot block from within a Tokio runtime; "
            "observe the Future from a synchronous or asyncio context.",
            id="handle_backed_future",
        ),
    ],
)
def test_fm_ready_get_on_tokio_refuses_when_ready(make_observable, message):
    """Target behavior: get() refuses on Tokio unconditionally, even ready. The
    observable is driven to ready *before* entering the worker."""
    _assert_ready_get_on_tokio_refuses(make_observable, message)


# -- fm.invalid_timeout_on_ready ---------------------------------------------


def test_fm_invalid_timeout_on_ready_current_bypasses_validation():
    """Incumbent divergence: a cached Future ignores the timeout argument
    entirely, so an invalid value is silently accepted. Contrast
    test_get_invalid_timeout_does_not_spawn_or_mutate, which validates only
    because the Future is still _Unawaited."""
    fut: Future[int] = Future._from_coro(_value(8))
    assert fut.get() == 8  # -> _Complete
    for bad in (float("nan"), -1.0, float("inf")):
        assert fut.get(timeout=bad) == 8  # no ValueError


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
def test_fm_invalid_timeout_on_ready_validates_first(make_observable):
    """Target behavior: the timeout is validated before readiness is observed,
    so a ready observable still rejects a bad value (HDL-12)."""
    _assert_invalid_timeout_validated(make_observable)


# -- fm.ready_as_asyncio_settlement ------------------------------------------


async def test_fm_ready_as_asyncio_settlement_current_is_done_synchronously():
    """Incumbent facade: a cached Future hands back an already-done loop
    future -- ``.done()`` is True before the loop runs."""
    fut: Future[int] = Future._from_coro(_value(9))
    await asyncio.to_thread(fut.get)  # resolve off the loop -> _Complete
    settled = fut.as_asyncio()
    assert settled.done()
    assert await settled == 9


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
async def test_fm_ready_as_asyncio_settlement_is_done_synchronously(make_observable):
    """Target behavior: a ready observable's loop future is already done, as a
    cached Future's is (HDL-22)."""
    ok = make_observable("success")
    observer = ok.as_asyncio()
    assert observer.done()
    assert await observer == _PROBE_SUCCESS_VALUE


# -- fm.terminal_baseexception_reobservable ----------------------------------


def test_fm_terminal_baseexception_current_poisons_the_future():
    """Incumbent divergence: a terminal BaseException escapes get() without
    being cached (only ``except Exception`` stores into _Exception), so the
    one-shot task is consumed while the Future still looks _Unawaited and a
    second observation no longer yields the original error."""
    fut: Future[int] = Future._from_coro(_raise(KeyboardInterrupt("stop")))
    with pytest.raises(KeyboardInterrupt):
        fut.get()
    assert isinstance(fut._status, future_mod._Unawaited)  # never cached
    with pytest.raises(BaseException) as second:
        fut.get()
    assert not isinstance(second.value, KeyboardInterrupt)
    assert isinstance(second.value, ValueError)
    assert "PythonTask already consumed" in str(second.value)


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
def test_fm_terminal_baseexception_remains_reobservable(make_observable):
    """Target behavior: a terminal BaseException stays observable, and the same
    object comes back each time."""
    _assert_terminal_baseexception_reobservable(make_observable)


# -- fm.off_loop_as_asyncio --------------------------------------------------


@pytest.mark.parametrize("make_observable", PRESERVED_BY_BOTH)
def test_fm_off_loop_as_asyncio_refuses_and_is_non_consuming(make_observable):
    """Off a running loop, as_asyncio() raises RuntimeError and consumes
    nothing. The two representations differ only in the message, which this row
    deliberately does not pin."""
    _assert_off_loop_as_asyncio_refuses(make_observable)


# -- fm.off_loop_await -------------------------------------------------------


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
def test_fm_off_loop_await_raises_native(make_observable):
    """Target behavior: __await__() off a loop raises asyncio's native
    RuntimeError without consuming the observable, where the incumbent Future
    raises ValueError (test_await_with_no_event_loop_raises_and_is_non_consuming)."""
    _assert_off_loop_await_raises_native(make_observable)


# -- fm.tokio_await ----------------------------------------------------------


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
def test_fm_tokio_await_raises_native_not_would_block(make_observable):
    """Target behavior on a runtime worker: no asyncio loop exists, so
    __await__() raises the native no-running-asyncio-loop RuntimeError, and the
    observable survives the refusal.

    The permanent Handle-side oracle lives in handle.rs; this case is the
    reusable facade coverage."""
    _assert_tokio_await_raises_native(make_observable)


# -- fm.get_tracing_event ----------------------------------------------------


@pytest.mark.parametrize("facade", FACADE_FACTORIES)
def test_fm_get_tracing_event_emits_exactly_one(facade, monkeypatch):
    """Facade-only: an in-loop get() forwards exactly one tracing event with
    context='asyncio'. The trace belongs to the public facade, not to Handle."""
    _assert_get_emits_one_tracing_event(facade, monkeypatch)


# -- fm.result_alias ---------------------------------------------------------


@pytest.mark.parametrize("facade", FACADE_FACTORIES)
def test_fm_result_alias_matches_get(facade):
    """Facade-only: result(timeout) is exactly get(timeout) -- same value, same
    cached exception object, same timeout behavior."""
    _assert_result_aliases_get(facade)


# -- fm.exception_accessor ---------------------------------------------------


@pytest.mark.parametrize("facade", FACADE_FACTORIES)
def test_fm_exception_accessor_contract(facade):
    """Facade-only: exception() returns None on success, returns a caught
    Exception (including TimeoutError) rather than raising, and lets a
    BaseException escape."""
    _assert_exception_accessor(facade)


# -- probe self-checks -------------------------------------------------------


def test_probe_is_closed_and_deterministic():
    """The probe gates completion, exposes its witnesses, and rejects anything
    outside the closed outcome set -- in particular it takes no producer
    function."""
    probe = _probe("success")
    try:
        # Only completion is gated. The producer is eager, so it may already
        # have entered its body and set the started witness before the gate
        # opens; asserting the negative there would be a race.
        assert not probe._completed
        probe._release()
        probe._wait_completed()
        assert probe._started
        assert probe._completed
        assert cast("Any", probe._handle).get() == _PROBE_SUCCESS_VALUE

        with pytest.raises(ValueError, match="unknown probe outcome"):
            _make_handle_probe("arbitrary_callable")
        # The real closure is the signature: the binding takes a str, so a
        # producer function cannot be smuggled in at all.
        with pytest.raises(TypeError):
            _make_handle_probe(lambda: None)
    finally:
        probe._release()
        probe._wait_completed()


def test_probe_handle_lookup_is_non_consuming():
    """Repeated lookup hands back the same live handle rather than consuming
    it, so one payload of assertions can observe it many times."""
    probe, first = _ready_handle("success")
    second = cast("Any", probe._handle)
    assert first.get() == _PROBE_SUCCESS_VALUE
    assert second.get() == _PROBE_SUCCESS_VALUE
