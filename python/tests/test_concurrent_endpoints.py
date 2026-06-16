# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import logging
import os
from tempfile import TemporaryDirectory
from typing import Any, cast

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, concurrent_endpoint, endpoint, Port, this_host
from monarch.actor.concurrent import _run_endpoint
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


class FakePort:
    def __init__(self) -> None:
        self.exception_value: Exception | None = None

    def exception(self, exception: Exception) -> None:
        self.exception_value = exception


class FakeActorInstance:
    name = "fake_actor"
    actor_id = "fake_actor_id"

    def __init__(self) -> None:
        self.finished_tokens: list[int] = []

    def _execution_start(self, method_name: str) -> int:
        return 7

    def _execution_finish(self, token: int) -> None:
        self.finished_tokens.append(token)


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


async def test_explicit_port_exception_logs_warning_without_response(
    caplog: pytest.LogCaptureFixture,
) -> None:
    port = FakePort()
    actor_instance = FakeActorInstance()

    async def call() -> None:
        raise ValueError("explicit boom")

    with caplog.at_level(logging.WARNING, logger="monarch.actor.concurrent"):
        await _run_endpoint(
            actor_instance,
            port,
            call,
            method_name="fail",
            should_instrument=False,
            forwards_exception=False,
        )

    assert port.exception_value is None
    assert "concurrent explicit response-port endpoint raised" in caplog.text
    assert actor_instance.finished_tokens == [7]


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
async def test_concurrent_explicit_port_exception_is_not_auto_forwarded() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    actor = proc.spawn(
        "concurrent_explicit_port_failing_actor", ConcurrentExplicitPortFailingActor
    )

    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(actor.fail.call_one(), timeout=0.2)
        assert await asyncio.wait_for(actor.ping.call_one(), timeout=10) == "pong"
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
@parametrize_config(actor_queue_dispatch={True, False}, shared_asyncio_runtime={False})
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
@parametrize_config(actor_queue_dispatch={True, False}, shared_asyncio_runtime={False})
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
