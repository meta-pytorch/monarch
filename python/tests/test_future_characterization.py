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
import pickle
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
from monarch._rust_bindings.monarch_hyperactor.runtime import _is_in_tokio_runtime
from monarch._rust_bindings.monarch_hyperactor.testing import (
    _call_in_tokio_context,
    _handle_observer_count,
    _make_handle_probe,
    _PROBE_SUCCESS_VALUE,
)
from monarch._src.actor import future as future_mod
from monarch._src.actor.future import Future
from monarch._src.actor.returns_future import returns_future
from monarch.actor import Channel


def _received_value(value):
    """A lazy channel receive of one supplied value."""
    sender, receiver = Channel[Any].open()
    sender.send(value)
    return receiver.recv()


def _received_error(error: BaseException):
    """A lazy receive that raises a sent exception when observed."""
    sender, receiver = Channel[Any].open()
    sender.exception(cast(Any, error))
    return receiver.recv()


async def _await_once(fut: Future):
    return await fut


def _future_pending() -> "tuple[Any, Callable[[], None]]":
    """A receive held pending until the test sends its value in drain()."""
    sender, receiver = Channel[int].open()
    future = receiver.recv()

    def drain() -> None:
        sender.send(_PROBE_SUCCESS_VALUE)
        future.get(timeout=5)

    return future, drain


@pytest.mark.parametrize("raises", [False, True])
def test_tokio_context_probe_restores_context_and_stays_on_calling_thread(raises):
    assert not _is_in_tokio_runtime()
    caller = threading.get_ident()
    error = ValueError("probe failed")

    def probe():
        assert _is_in_tokio_runtime()
        assert threading.get_ident() == caller
        if raises:
            raise error
        return 7

    if raises:
        with pytest.raises(ValueError, match="probe failed") as caught:
            _call_in_tokio_context(probe)
        assert caught.value is error
    else:
        assert _call_in_tokio_context(probe) == 7
    assert not _is_in_tokio_runtime()


# ---------------------------------------------------------------------------
# States and transitions
#
# A receive Future holds a Handle (_Handle) from creation. The Handle's first
# observation takes its message and unpickles it, and every later observation
# reads that one result. A get(), timed or not, on any thread drives the
# receive inline and hands it to Tokio if a timeout or Ctrl-C ends the wait;
# any other observation starts it on Tokio. A rejected observation (a
# Tokio-thread get() or await, an invalid timeout, as_asyncio() off a loop)
# starts nothing. These pin that lifecycle and its idempotency.
# ---------------------------------------------------------------------------


def test_handle_binding_has_permanent_module_identity():
    """Attests HDL-16."""
    # HDL-16: Handle and its blocking error have one permanent module.
    assert Handle.__module__ == "monarch._rust_bindings.monarch_hyperactor.handle"
    assert (
        WouldBlockRuntime.__module__
        == "monarch._rust_bindings.monarch_hyperactor.handle"
    )


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


def test_as_handle_returns_the_one_receive_handle():
    """Repeated _as_handle() calls share the Future's one Handle rather than
    taking another reply."""
    sender, receiver = Channel[int].open()
    fut = receiver.recv()
    first = fut._as_handle()
    second = fut._as_handle()
    assert first is second

    sender.send(7)
    assert first.get(timeout=5) == 7
    assert fut.get(timeout=5) == 7
    sender.send(8)
    assert receiver.recv().get(timeout=5) == 8


def test_as_handle_returns_the_handle_after_completion():
    """A completed receive Future keeps its Handle, so _as_handle() returns it
    after success or failure. A Future._resolved value is a ready Handle, and
    _as_handle() returns that Handle too."""
    complete: Future[int] = _received_value(1)
    assert complete.get() == 1

    failed: Future[int] = _received_error(ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        failed.get()

    for fut in (complete, failed):
        status = fut._status
        assert isinstance(status, future_mod._Handle)
        assert fut._as_handle() is status.handle
        assert fut._status is status

    resolved: Future[int] = Future._resolved(1)
    status = resolved._status
    assert isinstance(status, future_mod._Handle)
    assert resolved._as_handle() is status.handle
    assert resolved._status is status
    assert resolved.get() == 1


def test_futures_cannot_be_pickled():
    """Like asyncio.Future, no Monarch Future can be pickled: a resolved value,
    a Handle-backed operation and a returns_future body each refuse."""
    for fut in (
        Future._resolved(1),
        _handle_future_observable("success"),
        _body("success"),
    ):
        with pytest.raises(TypeError, match="cannot pickle"):
            pickle.dumps(fut)


def test_get_success_stays_a_handle_and_is_idempotent():
    """An untimed get() takes one message and keeps its value in the Future's
    Handle; repeated get() returns that value without taking another message."""
    fut: Future[int] = _received_value(42)
    assert fut.get() == 42
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 42


def test_get_exception_reraises_the_stored_object():
    """Failure mirror of the success case: the first get() raises the received
    exception, which stays in the Future's Handle; a second get() re-raises the
    *same stored* object without receiving again. Identity is asserted between
    the two gets (not against the constructed ValueError) because the exception
    crosses the Rust/pyo3 boundary -- the contract is that the Handle stores one
    object and re-raises it."""
    fut: Future[int] = _received_error(ValueError("boom"))
    with pytest.raises(ValueError, match="boom") as first:
        fut.get()
    assert isinstance(fut._status, future_mod._Handle)
    with pytest.raises(ValueError, match="boom") as second:
        fut.get()
    assert second.value is first.value


def test_get_timeout_raises_timeout_error():
    """get(timeout=...) observes the receive through its Handle with the given
    deadline, so an unfinished receive surfaces a TimeoutError rather than
    hanging. The timeout is non-cancelling; the sibling
    test_get_timeout_is_non_poisoning_and_reobservable pins re-observation."""
    fut, drain = _future_pending()
    try:
        with pytest.raises(TimeoutError):
            fut.get(timeout=0.1)
    finally:
        drain()


def test_get_timeout_is_non_poisoning_and_reobservable():
    """get(timeout=...) that times out routes through the Handle and does NOT
    poison the Future: it stays _Handle, and a later get() (no timeout) still
    resolves the same receive (the timeout is non-cancelling)."""
    sender, receiver = Channel[int].open()
    fut = receiver.recv()
    with pytest.raises(TimeoutError):
        fut.get(timeout=0.01)
    assert isinstance(fut._status, future_mod._Handle)
    sender.send(11)
    assert fut.get(timeout=5) == 11


def test_get_timeout_success_returns_and_stays_a_handle():
    """get(timeout=...) whose receive finishes within the timeout returns the
    value; the Future keeps its Handle, so a later no-timeout get() re-observes
    the result."""
    fut: Future[int] = _received_value(7)
    assert fut.get(timeout=5) == 7
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 7


async def test_await_asyncio_observes_the_handle_and_is_reobservable():
    """Awaiting under an asyncio loop observes the Future's Handle through
    as_asyncio() and yields the value; re-awaiting observes the same Handle (a
    fresh loop-local future each time)."""
    fut: Future[int] = _received_value(7)
    assert await fut == 7  # starts the receive
    assert isinstance(fut._status, future_mod._Handle)
    assert await fut == 7  # re-observe the same Handle


def test_tokio_await_raises_without_consuming_receive():
    """Tokio refuses a receive before it can take its queued message: the
    Handle's await path reports asyncio's native no-running-loop error and
    starts nothing."""
    fut = _received_value(9)

    def attempt():
        assert _is_in_tokio_runtime()
        with pytest.raises(RuntimeError) as caught:
            fut.__await__()
        assert not isinstance(caught.value, WouldBlockRuntime)
        return str(caught.value)

    assert "no running event loop" in _call_in_tokio_context(attempt)
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 9


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
    fut: Future[int] = _received_value(1)
    asyncio.run(_await_once(fut))  # -> _Handle
    assert fut.get() == 1


def test_get_after_asyncio_await_error_reraises():
    """Failure mirror of the previous: get() on a _Handle whose live producer
    FAILED re-raises the stored exception through handle.get(). Drives the live
    Handle producer (not a pre-resolved get())."""
    fut: Future[int] = _received_error(ValueError("boom"))

    async def bridge():
        # Start the receive through the live producer; swallow the failure so
        # we can re-observe it via get() below.
        with pytest.raises(ValueError, match="boom"):
            await fut

    asyncio.run(bridge())
    assert isinstance(fut._status, future_mod._Handle)
    with pytest.raises(ValueError, match="boom"):
        fut.get()


def test_await_asyncio_after_get_returns():
    """await under asyncio after a sync get() resolved it returns the value --
    the Handle's as_asyncio() hands back an already-settled loop future
    (HDL-22)."""
    fut: Future[int] = _received_value(1)
    assert fut.get() == 1  # resolves the Handle

    async def attempt():
        return await fut

    assert asyncio.run(attempt()) == 1


def test_await_asyncio_after_get_exception_reraises_stored():
    """Failure mirror of the previous: await under asyncio after a failed sync
    get() hands back a settled failed loop future that re-raises the *same*
    stored exception object."""
    boom = ValueError("boom")
    fut: Future[int] = _received_error(boom)
    with pytest.raises(ValueError, match="boom") as first:
        fut.get()  # stores the error in the Handle

    async def attempt():
        with pytest.raises(ValueError, match="boom") as caught:
            await fut
        return caught.value

    assert asyncio.run(attempt()) is first.value


def test_await_asyncio_live_handle_error_propagates():
    """A failing receive started by as_asyncio (not pre-resolved) surfaces the
    exception through the bridged asyncio future: exercises the producer's error
    publication -> observer -> set_exception, which the pre-resolved
    settled-future path does not."""
    fut: Future[int] = _received_error(ValueError("boom"))

    async def attempt():
        with pytest.raises(ValueError, match="boom"):
            await fut  # starts the receive; error via the observer

    asyncio.run(attempt())


def test_await_tokio_on_handle_bridged_raises():
    """Convert on asyncio (_Handle), then await on a tokio thread: Handle's
    await path reports asyncio's native no-running-loop error."""
    fut: Future[int] = _received_value(1)
    asyncio.run(_await_once(fut))  # -> _Handle

    def attempt():
        fut.__await__()

    with pytest.raises(RuntimeError, match="no running event loop") as caught:
        _call_in_tokio_context(attempt)
    assert not isinstance(caught.value, WouldBlockRuntime)


def test_await_tokio_after_get_raises_native():
    """await on a tokio thread after a sync get() resolved it: the Future keeps
    its Handle, whose await path reports asyncio's native no-running-loop
    error."""
    fut: Future[int] = _received_value(1)
    assert fut.get() == 1  # resolves the Handle

    def attempt():
        fut.__await__()

    with pytest.raises(RuntimeError, match="no running event loop") as caught:
        _call_in_tokio_context(attempt)
    assert not isinstance(caught.value, WouldBlockRuntime)


def test_await_tokio_after_get_exception_raises_native():
    """Failure mirror of test_await_tokio_after_get_raises_native: await on a
    tokio thread after a failed sync get() reports the same native
    no-running-loop error."""
    fut: Future[int] = _received_error(ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        fut.get()  # stores the error in the Handle

    def attempt():
        fut.__await__()

    with pytest.raises(RuntimeError, match="no running event loop") as caught:
        _call_in_tokio_context(attempt)
    assert not isinstance(caught.value, WouldBlockRuntime)


def test_await_with_no_event_loop_raises_and_is_non_consuming():
    """await with neither an asyncio loop nor a tokio runtime active: the
    Handle's await path raises asyncio's native no-running-loop error, and
    refusing starts nothing, so a later get() still takes the message."""
    fut: Future[int] = _received_value(1)
    with pytest.raises(RuntimeError, match="no running event loop") as caught:
        fut.__await__()
    assert not isinstance(caught.value, WouldBlockRuntime)
    assert isinstance(fut._status, future_mod._Handle)
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
        fut: Future[int] = _received_value(5)
        with pytest.warns(UserWarning, match="running asyncio event loop") as record:
            value = fut.get()
        # the asyncio-context advice is as_asyncio()/await (both valid on a loop)
        assert any("as_asyncio" in str(w.message) for w in record)
        return value

    assert asyncio.run(runner()) == 5
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "asyncio"


def test_get_in_tokio_thread_raises_would_block_and_is_non_consuming(monkeypatch):
    """In a Tokio runtime context, no-timeout get() raises WouldBlockRuntime up
    front WITHOUT starting the receive: no UserWarning fires, the tracing event
    still forwards (context='tokio'), the Future stays _Handle, and a later get()
    from a sync context still takes the message."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))
    fut: Future[int] = _received_value(5)

    def attempt():
        assert _is_in_tokio_runtime()
        return fut.get()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(
            WouldBlockRuntime,
            match="observe the Future from a synchronous or asyncio context",
        ):
            _call_in_tokio_context(attempt)
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "tokio"
    # tokio get() refuses instead of warning
    assert [w for w in caught if issubclass(w.category, UserWarning)] == []
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 5  # the refusal left the message for a sync get()


def test_get_timeout_in_tokio_thread_raises_would_block_and_is_non_consuming(
    monkeypatch,
):
    """get(timeout=...) on a tokio thread is refused by the in_tokio check up
    front -- BEFORE the Handle starts the receive -- so it raises
    WouldBlockRuntime without starting work: no UserWarning, the trace still
    forwards (context='tokio'), the Future stays _Handle, and a later sync get()
    still takes the message."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))
    fut: Future[int] = _received_value(5)

    def attempt():
        assert _is_in_tokio_runtime()
        return fut.get(timeout=0.1)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(
            WouldBlockRuntime,
            match="observe the Future from a synchronous or asyncio context",
        ):
            _call_in_tokio_context(attempt)
    assert len(calls) == 1
    assert calls[0]["extra"]["context"] == "tokio"
    assert [w for w in caught if issubclass(w.category, UserWarning)] == []
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 5  # the refusal left the message for a sync get()


def test_get_invalid_timeout_does_not_spawn_or_mutate():
    """An invalid timeout (NaN/negative/non-finite) is rejected with ValueError
    before the Handle starts the receive, so a bad argument never starts work:
    the Future stays _Handle and a later valid get() still takes the message."""
    for bad in (float("nan"), -1.0, float("inf")):
        fut: Future[int] = _received_value(8)
        with pytest.raises(ValueError, match="invalid timeout"):
            fut.get(timeout=bad)
        assert isinstance(fut._status, future_mod._Handle)
        assert fut.get() == 8  # still receivable after a rejected bad-timeout call


def test_get_on_handle_in_asyncio_loop_warns_and_forwards_once(monkeypatch):
    """get() on an already-bridged Future (_Handle) from inside an asyncio loop
    forwards exactly one tracing event (context='asyncio') and warns exactly
    once: Future.get() forwards the trace, Handle.get() emits the UserWarning,
    and the _Handle branch does not re-warn."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))

    async def runner():
        fut: Future[int] = _received_value(5)
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


def test_get_in_loop_on_completed_receive_warns_and_traces(monkeypatch):
    """get() inside an asyncio loop on an already-completed receive Future
    forwards exactly one tracing event per call and warns once per call, even
    though the value or error is ready: a completed receive follows the ordinary
    Handle read policy (HDL-8)."""
    calls = []
    monkeypatch.setattr(future_mod, "log_with_tracing", lambda *a, **k: calls.append(k))

    done: Future[int] = _received_value(5)
    assert done.get() == 5  # completes off the loop
    failed: Future[int] = _received_error(ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        failed.get()  # completes off the loop
    calls.clear()

    async def runner():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert done.get() == 5  # ready value, inside the loop
            with pytest.raises(ValueError, match="boom"):
                failed.get()  # ready error, inside the loop
        return [w for w in caught if issubclass(w.category, UserWarning)]

    assert len(asyncio.run(runner())) == 2  # one warning per in-loop get()
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
    assert _handle_observer_count(completer) == 1
    entered = threading.Event()
    stopped = threading.Event()
    fallback_used = threading.Event()
    interrupt_lock = threading.Lock()

    def interrupt_pending_get() -> None:
        try:
            deadline = time.monotonic() + 5
            while _handle_observer_count(completer) < 2:
                if stopped.wait(0.001) or time.monotonic() >= deadline:
                    return
            # Only get() can have added an observer of this pair's core. This
            # thread holds the GIL now, so get() has also released it: the native
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
    assert _handle_observer_count(completer) == 1, "the interrupted wait must be gone"

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
# as_asyncio() observes the Future's Handle: observation is non-consuming, each
# call returns a fresh loop-local future, it requires a running loop, and it
# must not start the receive when there is none.
# ---------------------------------------------------------------------------


def test_as_asyncio_off_loop_raises_and_is_non_consuming():
    """Off a running loop, as_asyncio() raises RuntimeError WITHOUT starting the
    receive: the Future stays _Handle, so a later get() still takes the
    message."""
    fut: Future[int] = _received_value(3)
    with pytest.raises(RuntimeError):
        fut.as_asyncio()
    assert isinstance(fut._status, future_mod._Handle)
    assert fut.get() == 3


async def test_as_asyncio_twice_same_loop_both_resolve():
    """Two as_asyncio() futures from one Future on the same loop both resolve to
    the value (ordinary multi-observer case); the Future is _Handle after the
    first."""
    fut: Future[int] = _received_value(4)
    f1 = fut.as_asyncio()
    assert isinstance(fut._status, future_mod._Handle)
    f2 = fut.as_asyncio()
    assert await f1 == 4
    assert await f2 == 4


async def test_as_asyncio_cancel_one_then_await_again_resolves():
    """Cancelling one as_asyncio() observer does not poison the Future: a later
    await still resolves (each observer is a fresh loop-local future)."""
    fut: Future[int] = _received_value(5)
    f1 = fut.as_asyncio()
    f1.cancel()
    assert await fut == 5


def test_as_asyncio_across_two_loops_both_resolve():
    """The same Future observed from two different event loops resolves in each
    (each as_asyncio() binds a fresh future to the current loop)."""
    fut: Future[int] = _received_value(6)

    async def observe():
        return await fut

    assert asyncio.run(observe()) == 6
    assert asyncio.run(observe()) == 6


def test_as_asyncio_on_completed_stop_iteration_wraps_in_runtime_error():
    """as_asyncio() on a receive whose stored error is a StopIteration wraps it
    in RuntimeError before set_exception: asyncio.Future.set_exception rejects
    StopIteration (PEP 479) with TypeError, which would otherwise brick the
    returned future (HDL-7). The receive completes off the loop first, so this
    exercises the already-settled path (HDL-22)."""
    fut: Future[int] = _received_error(StopIteration("done"))
    with pytest.raises(StopIteration):
        fut.get()

    async def observe():
        settled = fut.as_asyncio()
        assert settled.done()
        with pytest.raises(RuntimeError, match="StopIteration"):
            await settled

    asyncio.run(observe())


# ---------------------------------------------------------------------------
# Future/Handle contract matrix
#
# The matrix covers raw Handles, Handle-backed Futures and returns_future
# bodies. Receive Futures are Handle-backed too, and the receive tests above
# follow the same Handle rules.
#
# Every target assertion lives in an ``_assert_*`` helper that takes a
# *factory*, never a hard-coded constructor, and each row is driven by one of
# four registries below. The Handle-backed Future entries run those assertions
# against the facade without changing the assertion bodies.
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
# Completed-receive cases, alongside the Handle factory rows:
#   fm.ready_get_on_tokio_refuses, fm.invalid_timeout_on_ready,
#   fm.ready_as_asyncio_settlement, fm.terminal_baseexception_reobservable
#
# The permanent Handle-side oracle for fm.tokio_await lives in handle.rs, so
# that contract is checked directly through the Rust binding.
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


class _Facade(NamedTuple):
    """Future factories for ready outcomes and a controlled pending result."""

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


def _resolved_observable(outcome: str):
    """A Future._resolved value. It can only be resolved to a value, so only
    the success outcome exists."""
    assert outcome == "success", f"no resolved observable for {outcome!r}"
    return Future._resolved(_PROBE_SUCCESS_VALUE)


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

# The same rows plus a Future._resolved value, for assertions that observe
# only success: a resolved Future follows the same Handle semantics.
SUCCESS_TARGETS = [
    *TARGET_ONLY,
    pytest.param(_resolved_observable, id="resolved_future"),
]

# Rows needing a producer held mid-flight, plus the control that releases it.
GATED_TARGETS = [
    pytest.param(_handle_gated, id="raw_handle"),
    pytest.param(_handle_future_gated, id="handle_backed_future"),
]

# Facade-only surfaces: result()/exception()/tracing have no Handle primitive.
FACADE_FACTORIES = [
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
    from monarch._src.actor.actor_mesh import _client_context, context

    context()  # establish the client off-Tokio so the guard cannot fire
    ok = make_observable("success")

    def attempt():
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

    assert "no running event loop" in _call_in_tokio_context(attempt)
    assert ok.get() == _PROBE_SUCCESS_VALUE  # refusal consumed nothing


def _assert_ready_get_on_tokio_refuses(make_observable, message):
    from monarch._src.actor.actor_mesh import _client_context, context

    context()  # establish the client off-Tokio so the guard cannot fire
    ok = make_observable("success")
    assert ok.get() == _PROBE_SUCCESS_VALUE  # ready before crossing over

    def attempt():
        assert _is_in_tokio_runtime()
        assert _client_context._val is not None, "bootstrap guard could fire"
        with pytest.raises(WouldBlockRuntime) as caught:
            ok.get()
        return str(caught.value)

    # Each API refuses with its own message, which also tells it apart from
    # the bootstrap guard. A Handle-backed Future must not suggest poll(),
    # which Future does not have.
    assert _call_in_tokio_context(attempt) == message


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

    The refusal message is checked elsewhere. This helper checks the trace,
    the exception type and that the observer refuses, rather than the
    root-client bootstrap guard.
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

    def attempt():
        assert _is_in_tokio_runtime()
        assert _client_context._val is not None, "bootstrap guard could fire"
        with pytest.raises(WouldBlockRuntime):
            fresh.get()

    _call_in_tokio_context(attempt)
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


# -- fm.ready_get_on_loop_warns ----------------------------------------------


@pytest.mark.parametrize("make_observable", SUCCESS_TARGETS)
async def test_fm_ready_get_on_loop_warns_when_ready(make_observable):
    """Target behavior: get() warns on a running loop *even when already
    ready*, as a completed receive Future does (see
    test_get_in_loop_on_completed_receive_warns_and_traces)."""
    await _assert_ready_get_on_loop_warns(make_observable)


# -- fm.ready_get_on_tokio_refuses -------------------------------------------


def test_fm_ready_get_on_tokio_refuses_completed_receive():
    """A completed receive Future follows the Handle read policy: get() on
    Tokio refuses even though its value or error is ready, and the refusal
    consumes nothing."""
    done: Future[int] = _received_value(5)
    assert done.get() == 5  # completes off the loop
    failed: Future[int] = _received_error(ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        failed.get()  # completes off the loop

    def attempt():
        assert _is_in_tokio_runtime()
        messages = []
        for fut in (done, failed):
            with pytest.raises(WouldBlockRuntime) as caught:
                fut.get()
            messages.append(str(caught.value))
        return messages

    # The Future's own refusal, not the root-client bootstrap guard.
    assert _call_in_tokio_context(attempt) == [future_mod._GET_ON_TOKIO_REFUSAL] * 2
    assert done.get() == 5
    with pytest.raises(ValueError, match="boom"):
        failed.get()


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
        pytest.param(
            _resolved_observable,
            "Future.get() cannot block from within a Tokio runtime; "
            "observe the Future from a synchronous or asyncio context.",
            id="resolved_future",
        ),
    ],
)
def test_fm_ready_get_on_tokio_refuses_when_ready(make_observable, message):
    """Target behavior: get() refuses on Tokio unconditionally, even ready. The
    observable is driven to ready *before* entering the runtime context."""
    _assert_ready_get_on_tokio_refuses(make_observable, message)


# -- fm.invalid_timeout_on_ready ---------------------------------------------


def test_fm_invalid_timeout_on_completed_receive_validates_first():
    """A completed receive Future validates the timeout before reading, as any
    Handle does (HDL-12): an invalid value raises ValueError even though the
    value is ready, and a later valid get() still returns it."""
    fut: Future[int] = _received_value(8)
    assert fut.get() == 8  # completes the receive
    for bad in (float("nan"), -1.0, float("inf")):
        with pytest.raises(ValueError, match="invalid timeout"):
            fut.get(timeout=bad)
    assert fut.get() == 8


@pytest.mark.parametrize("make_observable", SUCCESS_TARGETS)
def test_fm_invalid_timeout_on_ready_validates_first(make_observable):
    """Target behavior: the timeout is validated before readiness is observed,
    so a ready observable still rejects a bad value (HDL-12)."""
    _assert_invalid_timeout_validated(make_observable)


# -- fm.ready_as_asyncio_settlement ------------------------------------------


async def test_fm_ready_as_asyncio_settlement_completed_receive_is_done():
    """A receive completed off the loop hands back an already-done loop future
    -- ``.done()`` is True before the loop runs (HDL-22)."""
    fut: Future[int] = _received_value(9)
    await asyncio.to_thread(fut.get)  # resolve off the loop, inline on a worker
    settled = fut.as_asyncio()
    assert settled.done()
    assert await settled == 9


@pytest.mark.parametrize("make_observable", SUCCESS_TARGETS)
async def test_fm_ready_as_asyncio_settlement_is_done_synchronously(make_observable):
    """Target behavior: a ready observable's loop future is already done, and
    awaiting the observable itself returns its value (HDL-22)."""
    ok = make_observable("success")
    observer = ok.as_asyncio()
    assert observer.done()
    assert await observer == _PROBE_SUCCESS_VALUE
    assert await ok == _PROBE_SUCCESS_VALUE


# -- fm.terminal_baseexception_reobservable ----------------------------------


def test_fm_terminal_baseexception_from_receive_remains_reobservable():
    """A receive whose sent exception is a terminal BaseException stores it like
    any other outcome: a second get() re-raises the same object rather than
    finding the receive taken."""
    fut: Future[int] = _received_error(KeyboardInterrupt("stop"))
    with pytest.raises(KeyboardInterrupt) as first:
        fut.get()
    assert isinstance(fut._status, future_mod._Handle)
    with pytest.raises(KeyboardInterrupt) as second:
        fut.get()
    assert second.value is first.value


@pytest.mark.parametrize("make_observable", TARGET_ONLY)
def test_fm_terminal_baseexception_remains_reobservable(make_observable):
    """Target behavior: a terminal BaseException stays observable, and the same
    object comes back each time."""
    _assert_terminal_baseexception_reobservable(make_observable)


# -- fm.off_loop_as_asyncio --------------------------------------------------


@pytest.mark.parametrize("make_observable", PRESERVED_BY_BOTH)
def test_fm_off_loop_as_asyncio_refuses_and_is_non_consuming(make_observable):
    """Off a running loop, each observable refuses as_asyncio() without
    consuming its result. This test checks the error type, not its message."""
    _assert_off_loop_as_asyncio_refuses(make_observable)


# -- fm.off_loop_await -------------------------------------------------------


@pytest.mark.parametrize("make_observable", SUCCESS_TARGETS)
def test_fm_off_loop_await_raises_native(make_observable):
    """Target behavior: __await__() off a loop raises asyncio's native
    RuntimeError without consuming the observable. A receive Future does the
    same (test_await_with_no_event_loop_raises_and_is_non_consuming)."""
    _assert_off_loop_await_raises_native(make_observable)


# -- fm.tokio_await ----------------------------------------------------------


@pytest.mark.parametrize("make_observable", SUCCESS_TARGETS)
def test_fm_tokio_await_raises_native_not_would_block(make_observable):
    """Target behavior in a Tokio context with no asyncio loop:
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
