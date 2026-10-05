# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import collections
import contextlib
import os
import time
import unittest.mock
from tempfile import TemporaryDirectory
from typing import Any, cast, Iterator

import monarch.actor
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.actor import SupervisionInFlight
from monarch._rust_bindings.monarch_hyperactor.pympsc import (  # @manual=//monarch/monarch_extension:monarch_extension
    channel_for_test,
)
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch._src.actor import actor_mesh
from monarch.actor import Actor, concurrent_endpoint, endpoint, Port, this_host

# ── Queue-dispatch invariant coverage ──
# QD-1: test_dispatch_loop_yields_after_a_bounded_streak,
#       test_dispatch_streak_survives_handler_suspension_and_empty_waits,
#       test_dispatch_yield_leaves_the_next_message_queued,
#       test_busy_loop_admits_queued_concurrent_calls_in_bounded_batches
# QD-2: test_dispatch_loop_yields_after_a_bounded_streak,
#       test_busy_loop_admits_queued_concurrent_calls_in_bounded_batches


class AsyncGate(Actor):
    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.unblock = asyncio.Event()

    @concurrent_endpoint
    async def wait(self) -> str:
        self.ready.set()
        await self.unblock.wait()
        return "done"

    @endpoint
    async def release_when_ready(self) -> str:
        await self.ready.wait()
        self.unblock.set()
        return "released"


class ExplicitPortAsyncGate(Actor):
    def __init__(self) -> None:
        self.unblock = asyncio.Event()

    @concurrent_endpoint(explicit_response_port=True)
    async def wait(self, port: Port[str]) -> None:
        port.send("started")
        await self.unblock.wait()

    @endpoint
    async def ping(self) -> str:
        return "pong"

    @endpoint
    async def release(self) -> None:
        self.unblock.set()


class SequentialExplicitPortGate(Actor):
    @endpoint(explicit_response_port=True)
    async def wait(self, port: Port[str]) -> None:
        port.send("started")
        await asyncio.sleep(0.3)

    @endpoint
    async def ping(self) -> str:
        return "pong"


class LoopShutdownCancelsConcurrentEndpoint(Actor):
    def __init__(self, path: str) -> None:
        self.path = path
        self.unblock = asyncio.Event()

    @concurrent_endpoint(explicit_response_port=True)
    async def run(self, port: Port[str]) -> None:
        port.send("started")
        try:
            await self.unblock.wait()
        except asyncio.CancelledError:
            with open(self.path, "w") as f:
                f.write("cancelled")
            raise


class ConcurrentEndpointCleanupOrder(Actor):
    def __init__(self, path: str) -> None:
        self.path = path
        self.file = open(path, "w")
        self.unblock = asyncio.Event()

    @concurrent_endpoint(explicit_response_port=True)
    async def run(self, port: Port[str]) -> None:
        port.send("started")
        try:
            await self.unblock.wait()
        finally:
            self.file.write("task_cancelled\n")
            self.file.flush()

    async def __cleanup__(self, exc: Exception | None) -> None:  # type: ignore[override]
        self.file.write("cleanup\n")
        self.file.close()


class FailingConcurrentEndpointActor(Actor):
    @concurrent_endpoint
    async def fail(self) -> None:
        raise ValueError("boom")

    @endpoint
    async def ping(self) -> str:
        return "pong"


class ConcurrentExplicitPortFailingActor(Actor):
    @concurrent_endpoint(explicit_response_port=True)
    async def fail(self, port: Port[None]) -> None:
        raise ValueError("explicit boom")

    @endpoint
    async def ping(self) -> str:
        return "pong"


class InheritedConcurrentBase(Actor):
    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.unblock = asyncio.Event()

    @concurrent_endpoint
    async def base_wait(self) -> str:
        self.ready.set()
        await self.unblock.wait()
        return "base"


class InheritedConcurrentChild(InheritedConcurrentBase):
    @concurrent_endpoint
    async def child_wait(self) -> str:
        await self.ready.wait()
        return "child"

    @endpoint
    async def release(self) -> None:
        self.unblock.set()


class BusyLoopActor(Actor):
    """Blocks its event loop for 11 ms per step, then yields once, like an
    inference engine's step loop."""

    def __init__(self) -> None:
        self.running = False
        self.blocks = 0
        self.started: list[tuple[int, int]] = []

    @endpoint
    async def start(self) -> None:
        self.running = True
        self.task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while self.running:
            time.sleep(0.011)
            self.blocks += 1
            await asyncio.sleep(0)

    @concurrent_endpoint
    async def req(self, i: int) -> int:
        self.started.append((self.blocks, i))
        return i

    @endpoint
    async def finish(self) -> list[tuple[int, int]]:
        self.running = False
        await self.task
        return self.started


class _QueuedMessage:
    """The fields and reports of a queued message that `_dispatch_loop` uses;
    `bytes` carries the message's index."""

    def __init__(self, index: int) -> None:
        self.context = None
        self.method = None
        self.bytes = index
        self.local_state = []
        self.refs = []
        self.response_port = None
        self.correlation_id = None

    def _report_complete(self) -> None:
        pass

    def _report_failed(self) -> None:
        pass


class _RecordingActor:
    """Records the index of each message dispatched to it. Every
    `suspend_every`th handler suspends for one loop turn, without going
    through `asyncio.sleep`."""

    def __init__(self, suspend_every: int = 0) -> None:
        self.dispatched: list[int] = []
        self.suspend_every = suspend_every

    async def handle(self, ctx, method, message, panic_flag, *_rest) -> None:
        self.dispatched.append(message)
        if self.suspend_every and len(self.dispatched) % self.suspend_every == 0:
            loop = asyncio.get_running_loop()
            turn = loop.create_future()
            loop.call_soon(turn.set_result, None)
            await turn


class _Instance:
    def kill(self, reason: str) -> None:
        raise AssertionError(reason)


@contextlib.contextmanager
def _dispatcher_yields(actor: _RecordingActor) -> Iterator[list[int]]:
    """Record how many messages had been dispatched at each of the
    dispatcher's own yields."""
    yields: list[int] = []
    real = actor_mesh._yield_to_loop

    async def recording() -> None:
        yields.append(len(actor.dispatched))
        await real()

    with unittest.mock.patch.object(actor_mesh, "_yield_to_loop", recording):
        yield yields


async def _until_dispatched(actor: _RecordingActor, count: int) -> list[int]:
    """Wait until `count` messages have been dispatched; return the number
    dispatched at each loop turn."""
    turns = [len(actor.dispatched)]
    while turns[-1] < count:
        await asyncio.sleep(0)
        turns.append(len(actor.dispatched))
    return turns


def _start_dispatch(actor: _RecordingActor, rx: Any) -> asyncio.Task[None]:
    """Run `_dispatch_loop` over a test channel's receiver."""
    return asyncio.create_task(
        actor_mesh._dispatch_loop(
            actor, cast(Any, rx), cast(Any, _Instance()), SupervisionInFlight()
        )
    )


async def _stop(dispatcher: asyncio.Task[None]) -> None:
    dispatcher.cancel()
    await dispatcher


def test_concurrent_endpoint_wraps_endpoints() -> None:
    assert cast(Any, AsyncGate.wait)._explicit_response_port
    assert cast(Any, ExplicitPortAsyncGate.wait)._explicit_response_port


def test_concurrent_endpoint_rejects_endpoint_chaining() -> None:
    with pytest.raises(ValueError, match="does not wrap @endpoint"):

        class StackedDecoratorActor(Actor):
            @concurrent_endpoint
            @endpoint
            async def ping(self) -> str:
                return "pong"


def test_concurrent_endpoint_allows_mixed_hierarchy() -> None:
    assert cast(Any, InheritedConcurrentChild.base_wait)._explicit_response_port
    assert cast(Any, InheritedConcurrentChild.child_wait)._explicit_response_port
    assert not cast(Any, InheritedConcurrentChild.release)._explicit_response_port


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_concurrent_async_endpoint_runs_in_parallel() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    gate = proc.spawn("async_gate", AsyncGate)

    try:
        wait = gate.wait.call_one()
        assert (
            await asyncio.wait_for(gate.release_when_ready.call_one(), timeout=10)
            == "released"
        )
        assert await asyncio.wait_for(wait, timeout=10) == "done"
    finally:
        await proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_concurrent_explicit_port_runs_in_parallel() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    gate = proc.spawn("explicit_port_async_gate", ExplicitPortAsyncGate)

    try:
        assert await asyncio.wait_for(gate.wait.call_one(), timeout=10) == "started"
        assert await asyncio.wait_for(gate.ping.call_one(), timeout=10) == "pong"
        await gate.release.call_one()
    finally:
        await proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_inherited_concurrent_endpoints_run_in_parallel() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    gate = proc.spawn("inherited_concurrent_child", InheritedConcurrentChild)

    try:
        base_wait = gate.base_wait.call_one()
        assert await asyncio.wait_for(gate.child_wait.call_one(), timeout=10) == "child"
        await gate.release.call_one()
        assert await asyncio.wait_for(base_wait, timeout=10) == "base"
    finally:
        await proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_concurrent_endpoint_exception_uses_actor_error_context() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    actor = proc.spawn("failing_concurrent_endpoint", FailingConcurrentEndpointActor)

    try:
        with pytest.raises(
            Exception, match="Actor call failing_concurrent_endpoint.fail failed"
        ):
            await asyncio.wait_for(actor.fail.call_one(), timeout=10)
    finally:
        await proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_concurrent_explicit_port_exception_kills_actor() -> None:
    """An ``@concurrent_endpoint(explicit_response_port=True)`` body that raises
    instead of sending through its port kills the actor with a supervision
    error, just as a plain ``@endpoint(explicit_response_port=True)`` does (see
    ``test_explicit_response_port_exception_kills_actor`` in
    ``test_actor_error.py``). The escaped exception is not silently swallowed."""
    monarch.actor.unhandled_fault_hook = lambda failure: None
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    actor = proc.spawn(
        "concurrent_explicit_port_failing_actor", ConcurrentExplicitPortFailingActor
    )

    try:
        with pytest.raises(SupervisionError):
            await asyncio.wait_for(actor.fail.call_one(), timeout=15)
    finally:
        await proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_queue_dispatch_keeps_async_actor_non_concurrent_by_default() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    gate = proc.spawn("sequential_explicit_port_gate", SequentialExplicitPortGate)

    try:
        assert await asyncio.wait_for(gate.wait.call_one(), timeout=10) == "started"
        ping = asyncio.ensure_future(gate.ping.call_one())
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(ping), timeout=0.1)
        assert await asyncio.wait_for(ping, timeout=10) == "pong"
    finally:
        await proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_actor_loop_shutdown_cancels_concurrent_endpoint_tasks() -> None:
    with TemporaryDirectory() as tmpdir:
        done_path = os.path.join(tmpdir, "done")
        proc = this_host().spawn_procs(per_host={"gpus": 1})
        gate = proc.spawn(
            "loop_shutdown_cancels_concurrent_endpoint",
            LoopShutdownCancelsConcurrentEndpoint,
            done_path,
        )

        assert await asyncio.wait_for(gate.run.call_one(), timeout=10) == "started"
        await asyncio.wait_for(proc.stop(), timeout=10)
        with open(done_path) as f:
            assert f.read() == "cancelled"


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_actor_loop_shutdown_cancels_concurrent_endpoint_before_cleanup() -> None:
    with TemporaryDirectory() as tmpdir:
        done_path = os.path.join(tmpdir, "done")
        proc = this_host().spawn_procs(per_host={"gpus": 1})
        gate = proc.spawn(
            "loop_shutdown_cancels_before_cleanup",
            ConcurrentEndpointCleanupOrder,
            done_path,
        )

        assert await asyncio.wait_for(gate.run.call_one(), timeout=10) == "started"
        await asyncio.wait_for(proc.stop(), timeout=10)
        with open(done_path) as f:
            assert f.read() == "task_cancelled\ncleanup\n"


def test_concurrent_endpoint_rejects_sync_endpoint() -> None:
    with pytest.raises(ValueError, match="can only wrap async endpoints"):

        class ConcurrentSyncEndpoint(Actor):
            @concurrent_endpoint
            def ping(self) -> str:
                return "pong"


# QD-1, QD-2: a backlog is dispatched in queue order, in batches of at most
# `_DISPATCH_BATCH` separated by the dispatcher's own yields.
async def test_dispatch_loop_yields_after_a_bounded_streak() -> None:
    batch = actor_mesh._DISPATCH_BATCH
    count = 3 * batch + 5
    tx, rx = channel_for_test()
    for i in range(count):
        tx.send(_QueuedMessage(i))
    actor = _RecordingActor()
    with _dispatcher_yields(actor) as yields:
        dispatcher = _start_dispatch(actor, rx)
        try:
            turns = await asyncio.wait_for(_until_dispatched(actor, count), 10)
        finally:
            await _stop(dispatcher)
    assert actor.dispatched == list(range(count))
    assert yields == [batch, 2 * batch, 3 * batch]
    assert max(b - a for a, b in zip(turns, turns[1:])) <= batch


# QD-1: neither a handler's own suspension nor a wait on an empty receiver
# resets the dispatcher's count.
async def test_dispatch_streak_survives_handler_suspension_and_empty_waits() -> None:
    batch = actor_mesh._DISPATCH_BATCH

    # Every tenth handler suspends for a loop turn.
    count = 2 * batch + 5
    tx, rx = channel_for_test()
    for i in range(count):
        tx.send(_QueuedMessage(i))
    actor = _RecordingActor(suspend_every=10)
    with _dispatcher_yields(actor) as yields:
        dispatcher = _start_dispatch(actor, rx)
        try:
            await asyncio.wait_for(_until_dispatched(actor, count), 10)
        finally:
            await _stop(dispatcher)
    assert yields == [batch, 2 * batch]

    # The receiver runs empty 10 messages short of a batch, then 20 more
    # arrive.
    tx, rx = channel_for_test()
    for i in range(batch - 10):
        tx.send(_QueuedMessage(i))
    actor = _RecordingActor()
    with _dispatcher_yields(actor) as yields:
        dispatcher = _start_dispatch(actor, rx)
        try:
            await asyncio.wait_for(_until_dispatched(actor, batch - 10), 10)
            for i in range(batch - 10, batch + 10):
                tx.send(_QueuedMessage(i))
            await asyncio.wait_for(_until_dispatched(actor, batch + 10), 10)
        finally:
            await _stop(dispatcher)
    assert actor.dispatched == list(range(batch + 10))
    assert yields == [batch]


# QD-1: the dispatcher yields before it takes the next message, so cancelling
# it during that yield leaves the message queued rather than dropping it.
async def test_dispatch_yield_leaves_the_next_message_queued() -> None:
    batch = actor_mesh._DISPATCH_BATCH
    tx, rx = channel_for_test()
    for i in range(batch + 1):
        tx.send(_QueuedMessage(i))
    actor = _RecordingActor()

    async def cancel_during_yield() -> None:
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        await asyncio.sleep(0)

    with unittest.mock.patch.object(actor_mesh, "_yield_to_loop", cancel_during_yield):
        await asyncio.wait_for(_start_dispatch(actor, rx), 10)
    assert actor.dispatched == list(range(batch))
    left = rx.try_recv()
    assert left is not None and left.bytes == batch


# QD-1, QD-2: queued concurrent calls to an actor whose loop is busy start in
# send order, at most `_DISPATCH_BATCH` of them between two steps of its loop.
# How the arrivals split across steps depends on timing, so this does not
# assert how many steps the burst spans.
@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_busy_loop_admits_queued_concurrent_calls_in_bounded_batches() -> None:
    count = 256
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    busy = proc.spawn("busy_loop", BusyLoopActor)
    try:
        await busy.start.call_one()
        assert await asyncio.gather(
            *(busy.req.call_one(i) for i in range(count))
        ) == list(range(count))
        started = await busy.finish.call_one()
    finally:
        await proc.stop()
    per_block = collections.Counter(block for block, _ in started)
    assert max(per_block.values()) <= actor_mesh._DISPATCH_BATCH
    assert [i for _, i in started] == list(range(count))
