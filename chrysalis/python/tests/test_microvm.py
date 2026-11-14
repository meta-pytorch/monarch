# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Checks the release of microVMs by `firecracker/microvm.py`, without KVM.

Each stand-in guest has a real control listener and `sleep 60` for its VMM.
Each test registers every stand-in as soon as it starts and recovers them
independently of the code under test, so a regression can't leave a process
behind. A test's own assertion fails first; its directory is removed only once
recovery has confirmed every stand-in is gone.
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

import microvm
import pytest

_RECOVERY_TIMEOUT = 5


async def _no_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.close()


async def _stand_in_guest(
    workdir: Path, guests: list[microvm.MicroVm]
) -> microvm.MicroVm:
    """A guest with a real control listener and `sleep 60` for its VMM,
    registered in `guests` before anything is acquired."""
    guest = microvm.MicroVm(workdir)
    guests.append(guest)
    guest.control_server = await asyncio.start_unix_server(
        _no_agent, str(guest.control_path)
    )
    guest.fc = await asyncio.create_subprocess_exec("sleep", "60")
    return guest


async def _recover_guests(guests: Iterable[microvm.MicroVm]) -> bool:
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
    scenario: Callable[[list[microvm.MicroVm]], Awaitable[None]],
) -> None:
    """Run `scenario`, then recover its stand-ins. The directory is removed
    only if every stand-in was reaped."""
    guests: list[microvm.MicroVm] = []
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


def _released(guest: microvm.MicroVm) -> bool:
    assert guest.fc is not None
    return guest.fc.returncode is not None and not guest.control_path.exists()


def test_guest_release_frees_partial_boot() -> None:
    async def scenario(guests: list[microvm.MicroVm]) -> None:
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
    async def scenario(guests: list[microvm.MicroVm]) -> None:
        (run_dir / "a").mkdir()
        (run_dir / "b").mkdir()
        first = await _stand_in_guest(run_dir / "a", guests)
        second = await _stand_in_guest(run_dir / "b", guests)
        owners = [first, _FailingOwner(), second]
        with pytest.raises(RuntimeError, match=str(run_dir)):
            await asyncio.wait_for(microvm.cleanup(owners, run_dir), 30)
        assert _released(first) and _released(second), "both guests should be released"
        assert run_dir.exists(), "an unconfirmed reap should preserve the run directory"

    run_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-cln-"))
    _run_recovered(run_dir, scenario)


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
    owners: list[microvm.MicroVm] = []
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
        await microvm.cleanup(owners, run_dir, keep_run_dir=True)


def _interrupt_child(run_dir: Path) -> None:
    os._exit(microvm.run_cli(lambda: _interruptible(run_dir), print))


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


def test_run_cli_interrupt_releases_guests() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        _check_interrupt(sig)
