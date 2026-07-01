# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import os
from tempfile import TemporaryDirectory
from typing import Any, cast

import monarch.actor
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch.actor import Actor, concurrent_endpoint, endpoint, Port, this_host
from monarch.config import parametrize_config


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
@parametrize_config(actor_queue_dispatch={True, False})
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
@parametrize_config(actor_queue_dispatch={True, False})
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
@parametrize_config(actor_queue_dispatch={True, False})
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
@parametrize_config(actor_queue_dispatch={True, False})
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
@parametrize_config(actor_queue_dispatch={True, False})
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
@parametrize_config(actor_queue_dispatch={True})
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
@parametrize_config(actor_queue_dispatch={True, False})
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
@parametrize_config(actor_queue_dispatch={True, False})
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
