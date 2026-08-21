# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Characterization oracle for the public ``monarch`` ``Future`` state machine.

Pins the behavior of ``monarch._src.actor.future.Future`` -- its internal
states and the transitions between them, ``as_asyncio()`` and the ``__await__``
shim over it, rejection of Tokio-thread awaits, the ``get()``-inside-a-loop
warning, and the ``_take_inner()`` accessor with its ``_Taken`` terminal state
-- so that any change to that behavior is caught here and made explicit.
"""

import asyncio
import warnings
from typing import Any, Callable, cast, NamedTuple

import pytest
from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    Handle,
    is_tokio_thread,
    PythonTask,
    WouldBlockRuntime,
)
from monarch._rust_bindings.monarch_hyperactor.testing import (
    _make_handle_probe,
    _PROBE_SUCCESS_VALUE,
)
from monarch._src.actor import future as future_mod
from monarch._src.actor.future import Future


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
    # runs on a Tokio worker thread, where ``is_tokio_thread()`` is True and a
    # ``Future`` takes its ``__await__`` tokio branch. ``block_on`` itself runs
    # on the (non-worker) calling thread, so it is allowed to block.
    #
    # ``PythonTask.spawn_blocking`` is NOT an equivalent driver, even though
    # ``is_tokio_thread()`` is also True there: a blocking-pool thread tolerates
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


def test_direct_construction_is_rejected():
    with pytest.raises(
        TypeError,
        match="Future objects are returned by Monarch operations",
    ):
        Future()
    with pytest.raises(TypeError, match="unexpected keyword argument 'coro'"):
        Future(coro=None)


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
        assert is_tokio_thread()
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
    """Convert on asyncio (_Handle), then await on a tokio thread: the tokio
    branch intentionally refuses a Future already bridged to asyncio."""
    fut: Future[int] = Future._from_coro(_value(1))
    asyncio.run(_await_once(fut))  # -> _Handle

    async def attempt():
        await fut

    with pytest.raises(ValueError, match="not awaitable on a tokio thread"):
        _run_in_tokio(attempt())


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
        assert is_tokio_thread()
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
        assert is_tokio_thread()
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
    """``Handle`` and ``WouldBlockRuntime`` import from the pytokio bindings, and
    ``WouldBlockRuntime`` subclasses ``RuntimeError``."""
    assert issubclass(WouldBlockRuntime, RuntimeError)
    assert isinstance(Handle, type)


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
    fut: Future[int] = Future._from_coro(_value(1))
    task = fut._take_inner()
    assert isinstance(task, PythonTask)
    assert isinstance(fut._status, future_mod._Taken)
    assert task.block_on() == 1


def test_take_inner_twice_raises():
    """The Future is spent after the first _take_inner(); a second raises."""
    fut: Future[int] = Future._from_coro(_value(1))
    fut._take_inner()
    with pytest.raises(ValueError, match="already been awaited"):
        fut._take_inner()


def test_get_after_take_inner_raises():
    """get() after _take_inner() fails at the Future instead of re-driving the
    surrendered task."""
    fut: Future[int] = Future._from_coro(_value(1))
    fut._take_inner()
    with pytest.raises(ValueError, match="consumed"):
        fut.get()


async def test_await_asyncio_after_take_inner_raises():
    """await under asyncio after _take_inner() fails instead of re-driving."""
    fut: Future[int] = Future._from_coro(_value(1))
    fut._take_inner()
    with pytest.raises(ValueError, match="consumed"):
        await fut


def test_await_tokio_after_take_inner_raises():
    """await on a tokio thread after _take_inner() fails instead of re-driving."""
    fut: Future[int] = Future._from_coro(_value(1))
    fut._take_inner()

    async def attempt():
        await fut

    with pytest.raises(ValueError, match="consumed"):
        _run_in_tokio(attempt())


def test_take_inner_after_get_raises():
    """_take_inner() requires an unawaited Future: once get() has resolved it
    (_Complete), taking the inner task is refused."""
    fut: Future[int] = Future._from_coro(_value(1))
    assert fut.get() == 1  # -> _Complete
    with pytest.raises(ValueError, match="already been awaited"):
        fut._take_inner()


def test_take_inner_after_asyncio_await_raises():
    """Once an asyncio await has bridged the Future (_Handle), _take_inner() is
    refused."""
    fut: Future[int] = Future._from_coro(_value(1))
    asyncio.run(_await_once(fut))  # -> _Handle
    with pytest.raises(ValueError, match="already been awaited"):
        fut._take_inner()


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
#                                behaves. Not executable yet: Future has no
#                                Handle-backed constructor.
#
# The third layer is a seam, not a promise. Every target assertion lives in an
# ``_assert_*`` helper that takes a *factory*, never a hard-coded constructor,
# and each row is driven by one of four registries below. Adding a
# Handle-backed Future factory to a registry runs that row's assertions against
# the facade with no edit to the assertion body.
#
# Assertions that pin an incumbent divergence are deliberately NOT
# factory-driven: they describe behavior that is going away, and there is
# nothing to activate.
#
# Registry per row (every one of the 15 has a seam):
#
#   PRESERVED_BY_BOTH  -- target already satisfied by both representations
#     fm.repeated_observation, fm.exception_identity, fm.off_loop_as_asyncio
#   TARGET_ONLY        -- ready observable; only the Handle satisfies it today
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
#   fm.ready_as_asyncio_settlement, fm.terminal_baseexception_reobservable
#
# fm.cancelled_observer also has an unparametrized current-side test, but it is
# a *preserve* row: both representations behave identically, so that test is a
# second oracle for the same behavior, not a divergence.
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


# Target already satisfied by both representations.
PRESERVED_BY_BOTH = [
    pytest.param(_future_observable, id="current_future"),
    pytest.param(_handle_observable, id="raw_handle"),
]

# Ready-observable rows where only the Handle satisfies the target today.
TARGET_ONLY = [pytest.param(_handle_observable, id="raw_handle")]

# Rows needing a producer held mid-flight, plus the control that releases it.
GATED_TARGETS = [pytest.param(_handle_gated, id="raw_handle")]

# Facade-only surfaces: result()/exception()/tracing have no Handle primitive,
# so the seam starts with the incumbent Future and gains the Handle-backed one.
FACADE_FACTORIES = [
    pytest.param(_Facade(_future_observable, _future_pending), id="current_future")
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
        assert is_tokio_thread()
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


def _assert_ready_get_on_tokio_refuses(make_observable):
    from monarch._src.actor.actor_mesh import _client_context

    ok = make_observable("success")
    assert ok.get() == _PROBE_SUCCESS_VALUE  # ready before crossing over

    async def attempt():
        assert is_tokio_thread()
        assert _client_context._val is not None, "bootstrap guard could fire"
        with pytest.raises(WouldBlockRuntime) as caught:
            ok.get()
        return str(caught.value)

    # The Handle-specific message identifies the observer. A Handle-backed
    # facade delegates to the same observer, so it carries the same message.
    assert _run_in_tokio(attempt()) == (
        "get() cannot be called from a Tokio runtime context; "
        "use poll() or as_asyncio()"
    )


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
        assert is_tokio_thread()
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
def test_fm_timeout_then_success_is_non_cancelling(make_gated):
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
    """Incumbent facade: the cancellation is real, and a later await still
    resolves."""
    fut: Future[int] = Future._from_coro(_value(5))
    first = fut.as_asyncio()
    assert first.cancel() is True
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
        assert is_tokio_thread()
        value = done.get()
        try:
            failed.get()
        except ValueError as err:
            return value, str(err)
        raise AssertionError("cached _Exception should have re-raised")

    assert _run_in_tokio(attempt()) == (5, "boom")


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
def test_fm_ready_get_on_tokio_refuses_when_ready(make_observable):
    """Target behavior: get() refuses on Tokio unconditionally, even ready. The
    observable is driven to ready *before* entering the worker."""
    _assert_ready_get_on_tokio_refuses(make_observable)


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
    """Incumbent divergence: a cached Future hands back an already-done loop
    future -- ``.done()`` is True before the loop runs."""
    fut: Future[int] = Future._from_coro(_value(9))
    await asyncio.to_thread(fut.get)  # resolve off the loop -> _Complete
    settled = fut.as_asyncio()
    assert settled.done()
    assert await settled == 9


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
async def test_fm_ready_as_asyncio_settlement_settles_on_loop(make_observable):
    """Target behavior: a ready observable's loop future is *not* done
    synchronously -- it settles through a scheduled callback."""
    ok = make_observable("success")
    observer = ok.as_asyncio()
    assert not observer.done()
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
