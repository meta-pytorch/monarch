# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs the Rust sealed-postcard with Alice and Bob in Firecracker microVMs.

The orchestration lives in `sealed_postcard_microvms`, which the standalone
demo also runs. `test_sealed_postcard_across_microvms` runs the real exchange
across two microVMs; the other tests check its cleanup without KVM.

Each cleanup test registers every stand-in process as soon as it starts and
recovers them independently of the code under test, so a regression can't
leave a process behind. A test's own assertion fails first; its directory is
removed only once recovery has confirmed every stand-in is gone.
"""

import asyncio
import contextlib
import multiprocessing
import os
import shutil
import signal
import tempfile
import time
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path
from typing import Any, cast

import pytest
import sealed_postcard_microvms as microvms

_RECOVERY_TIMEOUT = 5


async def _no_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.close()


async def _stand_in_guest(workdir: Path, guests: list[microvms._Vm]) -> microvms._Vm:
    """A guest with a real control listener and `sleep 60` for its VMM,
    registered in `guests` before anything is acquired."""
    guest = microvms._Vm(workdir)
    guests.append(guest)
    guest.control_server = await asyncio.start_unix_server(
        _no_agent, str(guest.control_path)
    )
    guest.fc = await asyncio.create_subprocess_exec("sleep", "60")
    return guest


async def _recover_guests(guests: Iterable[microvms._Vm]) -> bool:
    """Kill and reap any stand-in VMM still running; True if all are reaped."""
    recovered = True
    for guest in guests:
        if guest.control_server is not None:
            guest.control_server.close()
        with contextlib.suppress(FileNotFoundError):
            guest.control_path.unlink()
        if guest.fc is None or guest.fc.returncode is not None:
            continue
        with contextlib.suppress(ProcessLookupError):
            guest.fc.kill()
        try:
            await asyncio.wait_for(guest.fc.wait(), _RECOVERY_TIMEOUT)
        except TimeoutError:
            recovered = False
    return recovered


def _run_recovered(
    directory: Path,
    scenario: Callable[[list[microvms._Vm]], Awaitable[None]],
) -> None:
    """Run `scenario`, then recover its stand-ins. The directory is removed
    only if every stand-in was reaped."""
    guests: list[microvms._Vm] = []
    recovered = False

    async def run() -> None:
        nonlocal recovered
        try:
            await scenario(guests)
        finally:
            recovered = await _recover_guests(guests)

    try:
        asyncio.run(run())
        assert recovered, f"a stand-in survived recovery; preserved {directory}"
    finally:
        if recovered:
            shutil.rmtree(directory, ignore_errors=True)


def _released(guest: microvms._Vm) -> bool:
    assert guest.fc is not None
    return guest.fc.returncode is not None and not guest.control_path.exists()


def test_guest_release_frees_partial_boot() -> None:
    async def scenario(guests: list[microvms._Vm]) -> None:
        guest = await _stand_in_guest(workdir, guests)
        await asyncio.wait_for(guest.release(), 30)
        assert _released(guest), "release should reap the VMM and unlink its listener"
        with pytest.raises(OSError):
            await asyncio.open_unix_connection(str(guest.control_path))
        await asyncio.wait_for(guest.release(), 1)

    workdir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-rel-"))
    _run_recovered(workdir, scenario)


class _FailingOwner:
    async def release(self) -> None:
        raise TimeoutError("a stand-in process was not reaped")


def test_cleanup_releases_every_owner_when_one_fails() -> None:
    async def scenario(guests: list[microvms._Vm]) -> None:
        (run_dir / "a").mkdir()
        (run_dir / "b").mkdir()
        first = await _stand_in_guest(run_dir / "a", guests)
        second = await _stand_in_guest(run_dir / "b", guests)
        owners = [first, _FailingOwner(), second]
        with pytest.raises(RuntimeError, match=str(run_dir)):
            await asyncio.wait_for(microvms._cleanup(owners, run_dir), 30)
        assert _released(first) and _released(second), "both guests should be released"
        assert run_dir.exists(), "an unconfirmed reap should preserve the run directory"

    run_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-cln-"))
    _run_recovered(run_dir, scenario)


@pytest.mark.timeout(900)
# oss_skip: needs KVM and Buck-built Firecracker inputs.
@pytest.mark.oss_skip
def test_sealed_postcard_across_microvms() -> None:
    if (reason := microvms._kvm_unavailable()) is not None:
        pytest.skip(reason)
    inputs = microvms.Inputs.from_env()
    run_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-fc-"))
    asyncio.run(microvms.run_scenario(run_dir, inputs))
    assert not run_dir.exists(), "a fully released run should remove its directory"


def _gone(pid: int, *, reap: bool) -> bool:
    """Whether `pid` no longer exists. With `reap`, first reap it if it is an
    exited child of this process, as an orphan reparented here would be."""
    if reap:
        with contextlib.suppress(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def _wait_gone(pid: int, timeout: float, *, reap: bool) -> bool:
    deadline = time.monotonic() + timeout
    while not _gone(pid, reap=reap):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
    return True


def _recover_pids(pids: Iterable[int]) -> bool:
    """Kill every process in `pids` that still exists and wait for it to go;
    True if all are gone. A process already gone is not signalled."""
    recovered = True
    for pid in pids:
        if _gone(pid, reap=True):
            continue
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
        recovered = _wait_gone(pid, _RECOVERY_TIMEOUT, reap=True) and recovered
    return recovered


async def _interruptible(run_dir: Path) -> None:
    """Acquire two stand-in guests, recording each stand-in's PID as soon as it
    starts, report readiness, then wait to be stopped."""
    owners: list[microvms._Vm] = []
    try:
        for name in ("a", "b"):
            (run_dir / name).mkdir()
            guest = await _stand_in_guest(run_dir / name, owners)
            assert guest.fc is not None
            with open(run_dir / "pids", "a") as pids:
                pids.write(f"{guest.fc.pid}\n")
        (run_dir / "kept.log").write_text("kept\n")
        (run_dir / "ready").touch()
        await asyncio.Event().wait()
    finally:
        await microvms._cleanup(owners, run_dir, keep_run_dir=True)


def _interrupt_child(run_dir: Path) -> None:
    os._exit(microvms.run_cli(lambda: _interruptible(run_dir)))


def _check_interrupt(sig: signal.Signals) -> None:
    run_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-int-"))
    # A forked child already has this module imported, so it runs the real
    # signal wrapper without starting a new interpreter.
    child = multiprocessing.get_context("fork").Process(
        target=_interrupt_child, args=(run_dir,)
    )
    child.start()
    recovered = False
    try:
        deadline = time.monotonic() + 10
        while not (run_dir / "ready").exists():
            assert time.monotonic() < deadline, "the controller never became ready"
            time.sleep(0.01)
        pids = [int(pid) for pid in (run_dir / "pids").read_text().split()]
        assert child.pid is not None
        os.kill(child.pid, sig)
        child.join(30)
        assert child.exitcode not in (None, 0), f"{sig.name} should fail the run"
        assert all(_wait_gone(pid, 5, reap=False) for pid in pids), (
            "the controller should reap every stand-in"
        )
        for name in ("a", "b"):
            assert not Path(f"{run_dir / name / 'v.sock'}_9000").exists()
        assert (run_dir / "kept.log").read_text() == "kept\n"
    finally:
        if child.is_alive():
            child.kill()
            child.join(_RECOVERY_TIMEOUT)
        written = run_dir / "pids"
        pids = (
            [int(pid) for pid in written.read_text().split()]
            if written.exists()
            else []
        )
        stand_ins_gone = _recover_pids(pids)
        recovered = not child.is_alive() and stand_ins_gone
        if recovered:
            shutil.rmtree(run_dir, ignore_errors=True)
    assert recovered, f"a stand-in survived recovery; preserved {run_dir}"


def test_demo_interrupt_releases_guests() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        _check_interrupt(sig)


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
                microvms._cleanup([microvms._Child(process)], run_dir), 30
            )
        assert isinstance(raised.value.__cause__, TimeoutError)

    try:
        asyncio.run(scenario())
        assert run_dir.exists(), "an unconfirmed reap should preserve the run directory"
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
