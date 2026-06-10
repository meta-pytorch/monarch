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

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, concurrent_actor, endpoint, Port, this_host
from monarch.config import parametrize_config


@concurrent_actor
class AsyncGate(Actor):
    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.unblock = asyncio.Event()

    @endpoint
    async def wait(self) -> str:
        self.ready.set()
        await self.unblock.wait()
        return "done"

    @endpoint
    async def release_when_ready(self) -> str:
        await self.ready.wait()
        self.unblock.set()
        return "released"


@concurrent_actor
class ExplicitPortAsyncGate(Actor):
    def __init__(self) -> None:
        self.unblock = asyncio.Event()

    @endpoint(explicit_response_port=True)
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


@concurrent_actor
class CleanupWaitsForConcurrentEndpoint(Actor):
    def __init__(self) -> None:
        self.done = False

    @endpoint(explicit_response_port=True)
    async def run(self, port: Port[str]) -> None:
        port.send("started")
        await asyncio.sleep(0.2)
        self.done = True

    async def __cleanup__(self, exc: Exception | None) -> None:  # type: ignore[override]
        assert self.done


@concurrent_actor
class NoCleanupWaitsForConcurrentEndpoint(Actor):
    def __init__(self, path: str) -> None:
        self.path = path

    @endpoint(explicit_response_port=True)
    async def run(self, port: Port[str]) -> None:
        port.send("started")
        await asyncio.sleep(0.2)
        with open(self.path, "w") as f:
            f.write("done")


def test_concurrent_actor_wraps_endpoints() -> None:
    assert cast(Any, AsyncGate.wait)._explicit_response_port
    assert cast(Any, ExplicitPortAsyncGate.wait)._explicit_response_port


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True})
@isolate_in_subprocess
async def test_queue_dispatch_runs_concurrent_async_endpoint_in_parallel() -> None:
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
@parametrize_config(actor_queue_dispatch={True})
@isolate_in_subprocess
async def test_queue_dispatch_runs_concurrent_explicit_port_in_parallel() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    gate = proc.spawn("explicit_port_async_gate", ExplicitPortAsyncGate)

    try:
        assert await asyncio.wait_for(gate.wait.call_one(), timeout=10) == "started"
        assert await asyncio.wait_for(gate.ping.call_one(), timeout=10) == "pong"
        await gate.release.call_one()
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
@parametrize_config(actor_queue_dispatch={True})
@isolate_in_subprocess
async def test_queue_dispatch_waits_for_concurrent_messages_before_cleanup() -> None:
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    gate = proc.spawn(
        "cleanup_waits_for_concurrent_endpoint", CleanupWaitsForConcurrentEndpoint
    )

    assert await asyncio.wait_for(gate.run.call_one(), timeout=10) == "started"
    await asyncio.wait_for(proc.stop(), timeout=10)


@pytest.mark.timeout(60)
@parametrize_config(actor_queue_dispatch={True})
@isolate_in_subprocess
async def test_queue_dispatch_waits_for_concurrent_messages_without_cleanup() -> None:
    with TemporaryDirectory() as tmpdir:
        done_path = os.path.join(tmpdir, "done")
        proc = this_host().spawn_procs(per_host={"gpus": 1})
        gate = proc.spawn(
            "no_cleanup_waits_for_concurrent_endpoint",
            NoCleanupWaitsForConcurrentEndpoint,
            done_path,
        )

        assert await asyncio.wait_for(gate.run.call_one(), timeout=10) == "started"
        await asyncio.wait_for(proc.stop(), timeout=10)
        with open(done_path) as f:
            assert f.read() == "done"


def test_concurrent_actor_rejects_sync_endpoint() -> None:
    with pytest.raises(ValueError, match="can only wrap async endpoints"):

        @concurrent_actor
        class ConcurrentSyncEndpoint(Actor):
            @endpoint
            def ping(self) -> str:
                return "pong"


def test_concurrent_actor_rejects_sync_cleanup() -> None:
    with pytest.raises(ValueError, match="requires async __cleanup__"):

        @concurrent_actor
        class ConcurrentSyncCleanup(Actor):
            @endpoint
            async def ping(self) -> str:
                return "pong"

            def __cleanup__(self, exc: Exception | None) -> None:
                pass
