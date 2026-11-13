# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs a Python program in a Firecracker guest built by `python_guest_rootfs`.

`test_python_program_runs_in_guest` boots the probe's image in a microVM and
checks what the probe reports from inside it. The other tests check, without
KVM, that the driver releases everything it started.
"""

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import cast

import pytest

# Where `guest_rootfs.bzl` installs the program in every image it builds.
GUEST_PROGRAM = "/usr/local/bin/guest-program"
# The host port fc_agent.py connects to for its command.
CONTROL_PORT = 9000
# As in firecracker/microvm.py: fc_agent.py is the guest's first process.
BOOT_ARGS = (
    "console=ttyS0 reboot=k panic=1 pci=off quiet root=/dev/vda ro init=/fc_agent.py"
)

# All timeouts below are in seconds.

# Wait for Firecracker to exit after the agent reboots the guest.
VMM_EXIT_TIMEOUT = 30
# Wait for a killed Firecracker to be reaped.
REAP_TIMEOUT = 10


def _built(var: str) -> Path:
    """The Buck output whose path the test target passes in `var`."""
    path = Path(os.environ[var]).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{var}={path} is not a built file")
    return path


def _kvm_unavailable() -> str | None:
    """Explain why this user cannot use `/dev/kvm`, or return `None`."""
    if not os.access("/dev/kvm", os.R_OK | os.W_OK):
        return "/dev/kvm is not usable by this user"
    return None


def _run_in_guest(
    firecracker: Path,
    kernel: Path,
    rootfs: Path,
    argv: list[str],
    boot_timeout: float,
    exit_timeout: float,
) -> tuple[dict[str, object], str]:
    """Boot one microVM from `rootfs`, run its program with `argv`, and return
    the agent's exit report and the guest's console text.

    Every wait is bounded. Whatever happens, `_release` stops Firecracker and
    releases the sockets and the run directory.
    """
    run = Path(tempfile.mkdtemp(prefix="pyguest-", dir="/tmp"))
    console_path = run / "console.log"
    fc: subprocess.Popen[bytes] | None = None
    listener: socket.socket | None = None
    connection: socket.socket | None = None
    exit_event: dict[str, object] = {}
    console = ""
    failed = True
    try:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(run / f"v.sock_{CONTROL_PORT}"))
        listener.listen(1)
        spec = {
            "boot-source": {
                "kernel_image_path": str(kernel),
                "boot_args": BOOT_ARGS,
            },
            "drives": [
                {
                    "drive_id": "root",
                    "path_on_host": str(rootfs),
                    "is_root_device": True,
                    "is_read_only": True,
                }
            ],
            "machine-config": {"vcpu_count": 1, "mem_size_mib": 512},
            # Guest connections to host port P arrive at `<uds_path>_P`.
            "vsock": {"guest_cid": 3, "uds_path": str(run / "v.sock")},
        }
        (run / "vm.json").write_text(json.dumps(spec))
        with open(console_path, "wb") as out:
            fc = subprocess.Popen(
                [str(firecracker), "--no-api", "--config-file", str(run / "vm.json")],
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        listener.settimeout(boot_timeout)
        connection, _ = listener.accept()
        command = {"executable": GUEST_PROGRAM, "argv": argv, "env": {}, "cwd": "/"}
        connection.sendall((json.dumps(command) + "\n").encode())
        exit_event = _exit_report(connection, exit_timeout)
        fc.wait(timeout=VMM_EXIT_TIMEOUT)
        failed = False
    finally:
        try:
            if console_path.exists():
                console = console_path.read_text(errors="replace")
            if failed:
                print(f"guest console:\n{console}", flush=True)
        finally:
            _release(fc, [listener, connection], run)
    return exit_event, console


def _exit_report(connection: socket.socket, timeout: float) -> dict[str, object]:
    """Read the agent's reports until its exit report, within `timeout` for the
    whole wait."""
    deadline = time.monotonic() + timeout
    with connection.makefile("rb") as reports:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no exit report within {timeout} s")
            connection.settimeout(remaining)
            line = reports.readline()
            if not line:
                raise RuntimeError("the guest agent closed without an exit report")
            event = json.loads(line)
            if event["event"] == "exit":
                return event


def _release(
    fc: subprocess.Popen[bytes] | None,
    sockets: list[socket.socket | None],
    run: Path,
) -> None:
    """Stop Firecracker, close the sockets, then remove the run directory.

    Every step runs. Unless Firecracker was never started or its reap is
    confirmed, keep the run directory and its console. If it is not reaped
    within `REAP_TIMEOUT`, raise `RuntimeError`.
    """
    reaped = True
    unreaped: tuple[int, subprocess.TimeoutExpired] | None = None
    try:
        if fc is not None and fc.poll() is None:
            reaped = False
            # The group is signalled only while its leader is unreaped, so its
            # ID cannot have been reused.
            try:
                os.killpg(fc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                fc.wait(timeout=REAP_TIMEOUT)
                reaped = True
            except subprocess.TimeoutExpired as e:
                unreaped = (fc.pid, e)
    finally:
        for sock in sockets:
            if sock is not None:
                sock.close()
        if reaped:
            shutil.rmtree(run)
    if unreaped is None:
        return
    pid, error = unreaped
    # Raised outside the `except` block and without `from`, so a failure
    # already propagating from the run stays in the traceback as its context.
    raise RuntimeError(
        f"Firecracker {pid} was not reaped within {REAP_TIMEOUT} s ({error}); "
        f"kept {run}"
    )


def _running(pid: int) -> bool:
    """Whether `pid` is a process that still runs: present and not a zombie."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return False
    # The state follows the parenthesized command name, which may contain spaces.
    return stat.rsplit(")", 1)[1].split()[0] != "Z"


@pytest.mark.timeout(600)
# oss_skip: needs KVM and Buck-built Firecracker inputs.
@pytest.mark.oss_skip
def test_python_program_runs_in_guest() -> None:
    """The probe runs in a guest built by `python_guest_rootfs`.

    Its Python version is this test's. Only loopback is up, and the only other
    interface is the down `sit0` the guest kernel creates. The root filesystem
    is read-only. Two Chrysalis nodes exchange a message over loopback.
    """
    if (reason := _kvm_unavailable()) is not None:
        pytest.skip(reason)
    exit_event, console = _run_in_guest(
        _built("CHR_FC_FIRECRACKER"),
        _built("CHR_FC_KERNEL"),
        _built("CHR_FC_ROOTFS"),
        ["python_guest_probe"],
        boot_timeout=60,
        exit_timeout=120,
    )
    assert exit_event["exit_code"] == 0 and exit_event["signal"] is None, console
    lines = console.splitlines()
    version = f"python {sys.version_info.major}.{sys.version_info.minor}"
    for expected in (
        version,
        "interfaces: lo,sit0",
        "up: lo",
        "root write: EROFS",
        "probe exchange: ok",
    ):
        assert expected in lines, f"{expected!r} is not a console line:\n{console}"


def test_driver_reclaims_a_guest_that_never_connects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A guest that never connects is a boot timeout, and the driver kills
    Firecracker's whole process group and removes the run directory."""
    leader_pid, child_pid = tmp_path / "leader.pid", tmp_path / "child.pid"
    fake = tmp_path / "firecracker"
    fake.write_text(
        f"#!/bin/sh\necho $$ > {leader_pid}\nsleep 600 &\necho $! > {child_pid}\nwait\n"
    )
    fake.chmod(0o755)
    runs: list[Path] = []
    mkdtemp = tempfile.mkdtemp

    def recording_mkdtemp(prefix: str, dir: str) -> str:
        path = mkdtemp(prefix=prefix, dir=dir)
        runs.append(Path(path))
        return path

    monkeypatch.setattr(tempfile, "mkdtemp", recording_mkdtemp)
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        _run_in_guest(
            fake,
            tmp_path / "vmlinux",
            tmp_path / "rootfs",
            ["probe"],
            boot_timeout=2,
            exit_timeout=2,
        )
    assert time.monotonic() - start < 2 + REAP_TIMEOUT
    pids = [int(leader_pid.read_text()), int(child_pid.read_text())]
    deadline = time.monotonic() + 5
    while (alive := [pid for pid in pids if _running(pid)]) and (
        time.monotonic() < deadline
    ):
        time.sleep(0.1)
    assert not alive, f"processes outlived the driver: {alive}"
    assert len(runs) == 1 and not runs[0].exists()


class _UnreapableProcess:
    """Stands in for a Firecracker process that is never reaped.

    Its `pid` is a process the test owns, so the group signal reaches nothing
    else. `wait` records its timeout and raises `wait_error`, or
    `TimeoutExpired` by default.
    """

    def __init__(
        self, owned: subprocess.Popen[bytes], wait_error: Exception | None = None
    ) -> None:
        self.pid: int = owned.pid
        self.wait_error = wait_error
        self.wait_timeouts: list[float | None] = []

    def poll(self) -> None:
        return None

    def wait(self, timeout: float | None = None) -> int:
        self.wait_timeouts.append(timeout)
        raise self.wait_error or subprocess.TimeoutExpired("firecracker", timeout or 0)


def test_unreaped_guest_preserves_the_run_directory() -> None:
    """If Firecracker cannot be reaped within its bound, cleanup still closes
    the sockets, keeps the run directory and console, and raises an error that
    keeps the run's own failure as its context."""
    owned = subprocess.Popen(["sleep", "600"], start_new_session=True)
    run = Path(tempfile.mkdtemp(prefix="pyguest-", dir="/tmp"))
    (run / "console.log").write_text("guest console\n")
    left, right = socket.socketpair()
    stand_in = _UnreapableProcess(owned)
    try:
        with pytest.raises(RuntimeError, match=re.escape(str(run))) as raised:
            try:
                raise TimeoutError("run failed")
            finally:
                _release(cast(subprocess.Popen[bytes], stand_in), [left, right], run)
        context = raised.value.__context__
        assert isinstance(context, TimeoutError) and str(context) == "run failed"
        assert not raised.value.__suppress_context__
        assert stand_in.wait_timeouts == [REAP_TIMEOUT]
        assert left.fileno() == -1 and right.fileno() == -1
        assert (run / "console.log").read_text() == "guest console\n"
    finally:
        owned.kill()
        owned.wait(timeout=REAP_TIMEOUT)
        shutil.rmtree(run, ignore_errors=True)


def test_reap_error_still_closes_sockets_and_keeps_the_run_directory() -> None:
    """An unexpected error while reaping Firecracker propagates, but cleanup
    still closes the sockets and, the reap unconfirmed, keeps the run
    directory and console."""
    owned = subprocess.Popen(["sleep", "600"], start_new_session=True)
    run = Path(tempfile.mkdtemp(prefix="pyguest-", dir="/tmp"))
    (run / "console.log").write_text("guest console\n")
    left, right = socket.socketpair()
    stand_in = _UnreapableProcess(owned, wait_error=OSError("injected"))
    try:
        with pytest.raises(OSError, match="injected"):
            _release(cast(subprocess.Popen[bytes], stand_in), [left, right], run)
        assert left.fileno() == -1 and right.fileno() == -1
        assert (run / "console.log").read_text() == "guest console\n"
    finally:
        owned.kill()
        owned.wait(timeout=REAP_TIMEOUT)
        shutil.rmtree(run, ignore_errors=True)
