# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs the Rust sealed-postcard with Alice and Bob in Firecracker microVMs.

The orchestration lives in `sealed_postcard_microvms`, which the standalone
demo also runs. `test_sealed_postcard_across_microvms` runs the real exchange
across two microVMs; the other tests check, without KVM, that it releases the
identity generator. `test_microvm` checks the release of the VMs themselves.

Each cleanup test registers every stand-in process as soon as it starts and
recovers them independently of the code under test, so a regression can't
leave a process behind. A test's own assertion fails first; its directory is
removed only once recovery has confirmed every stand-in is gone.
"""

import asyncio
import contextlib
import shutil
import tempfile
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import microvm
import pytest
import sealed_postcard_microvms as microvms

_RECOVERY_TIMEOUT = 5


@pytest.mark.timeout(900)
# oss_skip: needs KVM and Buck-built Firecracker inputs.
@pytest.mark.oss_skip
def test_sealed_postcard_across_microvms() -> None:
    if (reason := microvm.kvm_unavailable()) is not None:
        pytest.skip(reason)
    inputs = microvms.Inputs.from_env()
    run_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-fc-"))
    asyncio.run(microvms.run_scenario(run_dir, inputs))
    assert not run_dir.exists(), "a fully released run should remove its directory"


async def _recover_children(processes: Iterable[asyncio.subprocess.Process]) -> bool:
    """Kill and reap every process in `processes` still running; True if all
    are reaped."""
    recovered = True
    for process in processes:
        if process.returncode is not None:
            continue
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        try:
            await asyncio.wait_for(process.wait(), _RECOVERY_TIMEOUT)
        except TimeoutError:
            recovered = False
    return recovered


def test_identity_generation_is_released_when_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Interrupting the run while it generates identities kills and reaps the
    generator, then removes the run directory."""
    tools = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-ids-"))
    run_dir = tools / "run"
    run_dir.mkdir()
    # Stands in for `sealed-postcard identities`, which then never finishes.
    generator = tools / "sealed-postcard"
    generator.write_text("#!/bin/sh\nexec sleep 600\n")
    generator.chmod(0o755)
    inputs = microvms.Inputs(
        firecracker=generator,
        kernel=generator,
        rootfs=generator,
        sealed_postcard=generator,
    )
    # The test keeps its own handle on every process the run starts, so its
    # recovery doesn't depend on the code under test.
    spawned: list[asyncio.subprocess.Process] = []
    create_subprocess_exec = asyncio.create_subprocess_exec

    async def recording_exec(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        process = await create_subprocess_exec(*args, **kwargs)
        spawned.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", recording_exec)
    recovered = False

    async def scenario() -> None:
        nonlocal recovered
        task = asyncio.ensure_future(microvms.run_scenario(run_dir, inputs))
        try:
            deadline = time.monotonic() + 10
            while not spawned:
                assert not task.done(), "the run ended before generating identities"
                assert time.monotonic() < deadline, "the generator never started"
                await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 30)
            assert spawned[0].returncode is not None, "the generator should be reaped"
        finally:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await asyncio.wait_for(task, 30)
            recovered = await _recover_children(spawned)

    try:
        asyncio.run(scenario())
        assert not run_dir.exists(), "a fully released run should remove its directory"
    finally:
        if recovered:
            shutil.rmtree(tools, ignore_errors=True)
    assert recovered, f"the generator survived recovery; preserved {tools}"


class _UnreapableProcess:
    """A process stand-in that never finishes exiting."""

    pid = 0
    returncode: int | None = None

    def kill(self) -> None:
        pass

    async def wait(self) -> int:
        await asyncio.Event().wait()
        return 0


def test_unreaped_generator_preserves_the_run_directory() -> None:
    """If the generator can't be reaped within its bound, cleanup reports it
    and keeps the run directory, as for any other process."""
    run_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-unr-"))
    process = cast(asyncio.subprocess.Process, _UnreapableProcess())

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match=str(run_dir)) as raised:
            await asyncio.wait_for(
                microvm.cleanup([microvms._Child(process)], run_dir), 30
            )
        assert isinstance(raised.value.__cause__, TimeoutError)

    try:
        asyncio.run(scenario())
        assert run_dir.exists(), "an unconfirmed reap should preserve the run directory"
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
