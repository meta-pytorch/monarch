# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Constructed reproduction of the interpreter-exit actor-loop abort.

Run by ``test_actor_loop_finalization``. Two arms:

``unmitigated``
    suppress the loop marker so the production reaper cannot see the loops,
    and the loops' ``stop`` so root-client cleanup cannot stop them, then hold
    one of them inside a PyO3 call across interpreter finalization.
    CPython <= 3.12 force-exits that daemon thread with ``pthread_exit``; the
    forced unwind crosses PyO3's ``catch_unwind`` and glibc aborts.

Both arms spawn a user actor on the client proc whose ``__cleanup__``
never returns, so shutdown's cleanup bound (``CLEANUP_TIMEOUT``) gives
up on it and leaves its loop running: the survival the reaper exists
for. ``fixed`` lets the reaper stop it; ``unmitigated`` hides it.

``fixed``
    leave the marker in place and let the reaper run. Version-independent.

Neither arm uses ``Thread.join()`` or ``Thread.is_alive()``: on CPython 3.14
``is_alive()`` can enter an unbounded OS-thread join once ``thread_is_exiting``
is set, and this module runs on 3.14. Thread liveness is observed by membership
in ``threading.enumerate()`` and waited for by bounded polling.
"""

from __future__ import annotations

import asyncio
import atexit
import importlib
import os
import resource
import sys
import threading
import time
from typing import Any, Callable, TypeVar

from monarch.python.tests import actor_loop_finalization_timeouts as timeouts


_ACTOR_EVENT_LOOP_ATTRIBUTE = "_monarch_actor_event_loop"
_ACTOR_DRIVER_ATTRIBUTE = "_monarch_actor_driver"

# `context()` bootstraps the root client actor, an async actor with one asyncio
# loop on its own thread, and the `controller_controller` actor, a sync actor
# with one driver thread. The wedged-cleanup actor spawned on the client proc
# adds a second async loop.
_EXPECTED_ACTOR_LOOPS = 2
_EXPECTED_ACTOR_DRIVERS = 1

# Deadlines live in a side-effect-free module so the test can assert they nest
# inside its own subprocess timeout without importing this one.
_LOOP_DISCOVERY_TIMEOUT_S = timeouts.LOOP_DISCOVERY_TIMEOUT_S
_LOOP_DISCOVERY_SETTLE_S = timeouts.LOOP_DISCOVERY_SETTLE_S
_LOOP_DISCOVERY_POLL_S = timeouts.LOOP_DISCOVERY_POLL_S
_MODEL_ARM_TIMEOUT_S = timeouts.MODEL_ARM_TIMEOUT_S
_THREAD_EXIT_POLL_S = timeouts.THREAD_EXIT_POLL_S
_FINALIZATION_SWITCH_INTERVAL_S = timeouts.FINALIZATION_SWITCH_INTERVAL_S

T = TypeVar("T")

_actor_loops: list[tuple[asyncio.AbstractEventLoop, threading.Thread]] = []
_actor_drivers: list[threading.Thread] = []
_model_mode = "fixed"


def _note(message: str) -> None:
    # Unbuffered: the unmitigated arm ends in an abort that discards anything
    # still sitting in a Python-level stderr buffer.
    os.write(2, f"{message}\n".encode())


def _fail_model(message: str, exit_code: int) -> None:
    _note(f"finalization model failed: {message}")
    os._exit(exit_code)


def _ignore_loop_stop() -> None:
    pass


def _gil_enabled() -> bool:
    # `sys._is_gil_enabled` exists on 3.13+; earlier versions always have it.
    probe = getattr(sys, "_is_gil_enabled", None)
    return True if probe is None else bool(probe())


def _actor_event_loops() -> list[tuple[asyncio.AbstractEventLoop, threading.Thread]]:
    loops = []
    for thread in threading.enumerate():
        target = getattr(thread, "_target", None)
        loop = getattr(target, "__self__", None)
        if getattr(target, "__name__", None) == "run_forever" and isinstance(
            loop, asyncio.AbstractEventLoop
        ):
            loops.append((loop, thread))
    return loops


def _actor_driver_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if hasattr(t, _ACTOR_DRIVER_ATTRIBUTE)]


def _live_drivers(drivers: list[threading.Thread]) -> list[threading.Thread]:
    """Those still enumerable; not `is_alive()`, for the reason `_live` gives."""
    running = set(map(id, threading.enumerate()))
    return [thread for thread in drivers if id(thread) in running]


def _live(
    actor_loops: list[tuple[asyncio.AbstractEventLoop, threading.Thread]],
) -> list[tuple[asyncio.AbstractEventLoop, threading.Thread]]:
    """Those whose thread is still enumerable.

    Deliberately not `thread.is_alive()`: that can enter an unbounded OS-thread
    join on CPython 3.14, and every wait at this boundary must be bounded.
    """
    running = set(map(id, threading.enumerate()))
    return [(loop, thread) for loop, thread in actor_loops if id(thread) in running]


def _wait_for_actor_threads(
    find: Callable[[], list[T]], expected: int, what: str
) -> list[T]:
    """Block until exactly `expected` of `find()` have been steady for a settle window.

    `context()` returns before `controller_controller` finishes spawning, so an
    immediate snapshot can miss its thread and the model would then arm the
    finalization boundary while a thread it does not control is still arriving.
    """
    deadline = time.monotonic() + _LOOP_DISCOVERY_TIMEOUT_S
    steady_since: float | None = None
    while True:
        loops = find()
        if len(loops) > expected:
            raise RuntimeError(
                f"expected {expected} {what} threads, observed {len(loops)}; "
                "the model's topology assumption is stale"
            )
        if len(loops) == expected:
            now = time.monotonic()
            if steady_since is None:
                steady_since = now
            elif now - steady_since >= _LOOP_DISCOVERY_SETTLE_S:
                return loops
        else:
            steady_since = None
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"expected {expected} {what} threads, observed {len(loops)} "
                f"after {_LOOP_DISCOVERY_TIMEOUT_S}s"
            )
        time.sleep(_LOOP_DISCOVERY_POLL_S)


def _stop_actor_event_loops(
    actor_loops: list[tuple[asyncio.AbstractEventLoop, threading.Thread]],
) -> None:
    """Stop each loop and poll until its thread leaves `threading.enumerate()`."""
    for loop, _thread in actor_loops:
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError as error:
            _fail_model(f"actor loop rejected stop: {error}", 72)

    deadline = time.monotonic() + _MODEL_ARM_TIMEOUT_S
    while True:
        survivors = _live(actor_loops)
        if not survivors:
            return
        if time.monotonic() >= deadline:
            _fail_model(
                f"actor loops survived stop: {[t.name for _l, t in survivors]}", 73
            )
        time.sleep(_THREAD_EXIT_POLL_S)


def _exercise_finalization_boundary() -> None:
    alive = _live(_actor_loops)
    # The survivor count is what separates the arms: the reaper ran and emptied
    # the set, or it was suppressed and did not. Report it in every arm so both
    # assert on the same observable, and write it before anything can redirect
    # fd 2 (`closeStdPipesAtExit` points it at /dev/null during `exit()`).
    _note(f"{_model_mode} observed {len(alive)} actor loops after runtime shutdown")
    drivers = _live_drivers(_actor_drivers)
    _note(f"{_model_mode} observed {len(drivers)} actor drivers after runtime shutdown")

    if _model_mode == "fixed":
        if alive:
            _fail_model(f"reaper left {len(alive)} actor loops alive", 75)
        if drivers:
            _fail_model(f"reaper left {len(drivers)} actor drivers alive", 76)
        return

    if not alive:
        _fail_model(f"no actor loop survived in {_model_mode} mode", 74)

    entered = threading.Event()
    release = threading.Event()
    loop, _thread = alive[0]
    _stop_actor_event_loops(alive[1:])
    try:
        loop.call_soon_threadsafe(_wait_on_event_for_exit_test, entered, release)
    except RuntimeError as error:
        _fail_model(f"surviving loop rejected callback: {error}", 70)

    if not entered.wait(timeout=_MODEL_ARM_TIMEOUT_S):
        _fail_model("actor loop did not enter the PyO3 callback", 71)

    # The actor thread is now waiting inside the PyO3 call with the GIL
    # released. Releasing it must let that thread reach its next GIL attach
    # while `Py_FinalizeEx` is still running.
    #
    # Do NOT starve the switch interval here. glibc writes the fatal message to
    # fd 2, and `facebook::contextprop::cli::closeStdPipesAtExit` -- a libc
    # `atexit` handler, so it runs after `Py_FinalizeEx` returns -- points fd 2
    # at /dev/null. A long switch interval keeps the GIL on the main thread
    # until the very end of finalization, so the woken thread aborts after that
    # redirect and its diagnostic is discarded. Measured: 9/30 runs kept the
    # message at 60s, 30/30 at the default.
    sys.setswitchinterval(_FINALIZATION_SWITCH_INTERVAL_S)
    release.set()


# Register before importing Monarch. atexit is LIFO, so this runs after
# shutdown_context and shutdown_tokio_runtime at the final pre-finalization
# boundary that the production failure races.
atexit.register(_exercise_finalization_boundary)

_runtime = importlib.import_module("monarch._rust_bindings.monarch_hyperactor.runtime")
_wait_on_event_for_exit_test = _runtime._wait_on_event_for_exit_test
_actor = importlib.import_module("monarch.actor")
context = _actor.context


class _WedgedCleanupActor(_actor.Actor):
    """User actor whose ``__cleanup__`` never returns.

    Shutdown gives up on it at ``CLEANUP_TIMEOUT`` and leaves its loop
    running. Keeping its mesh referenced keeps the actor alive until then.
    """

    @_actor.endpoint
    async def ping(self) -> None:
        pass

    async def __cleanup__(self, _exc: Exception | None) -> None:
        await asyncio.Event().wait()


_wedged_meshes: list[Any] = []


def main() -> None:
    global _model_mode

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    mode = sys.argv[1] if len(sys.argv) == 2 else "fixed"
    if mode not in {"fixed", "unmitigated"}:
        raise ValueError(f"unknown model mode: {mode}")
    _model_mode = mode

    # The parent checks these: a 3.14 test target whose model resource was
    # built at the default version would otherwise report false coverage.
    _note(
        f"{mode} python {sys.version_info.major}.{sys.version_info.minor} "
        f"gil={'enabled' if _gil_enabled() else 'disabled'}"
    )

    context()
    _wedged_meshes.append(
        _actor.this_proc().spawn("wedged_cleanup", _WedgedCleanupActor)
    )
    _wedged_meshes[0].ping.call().get(timeout=30)
    _actor_loops.extend(
        _wait_for_actor_threads(
            _actor_event_loops, _EXPECTED_ACTOR_LOOPS, "actor event-loop"
        )
    )
    _note(f"{mode} captured {len(_actor_loops)} actor loops")
    _actor_drivers.extend(
        _wait_for_actor_threads(
            _actor_driver_threads, _EXPECTED_ACTOR_DRIVERS, "actor driver"
        )
    )
    _note(f"{mode} captured {len(_actor_drivers)} actor drivers")

    if mode == "unmitigated":
        for loop, thread in _actor_loops:
            # Strict: if production stopped marking actor-loop threads, this
            # arm would silently become a second copy of `fixed`.
            if not hasattr(thread, _ACTOR_EVENT_LOOP_ATTRIBUTE):
                raise RuntimeError(
                    f"actor loop thread {thread.name!r} carries no "
                    f"{_ACTOR_EVENT_LOOP_ATTRIBUTE} marker to suppress"
                )
            delattr(thread, _ACTOR_EVENT_LOOP_ATTRIBUTE)
            loop.stop = _ignore_loop_stop


if __name__ == "__main__":
    main()
