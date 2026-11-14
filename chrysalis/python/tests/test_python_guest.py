# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs a Python program in a Firecracker guest built by `python_guest_rootfs`.

`test_python_program_runs_in_guest` boots the probe's image in a microVM
through `python_guest.main` and checks what the probe reports from inside it.
`test_a_guest_that_never_connects_is_reclaimed` checks, without KVM, that a
run whose guest never connects ends unsuccessfully and releases everything it
started.
"""

import os
import shutil
import signal
import sys
import time
from pathlib import Path

import microvm
import pytest
import python_guest


def _reaped(pid: int) -> bool:
    """Whether this process's child `pid` has been reaped: waiting for it fails.
    An exited child is reaped here, but still reported as not reaped before."""
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return True
    return False


def _recover(pid: int) -> bool:
    """Kill the child `pid` if it hasn't been reaped, and wait up to 5 seconds
    to reap it; True once it is reaped. A reaped PID is never signalled, so
    nothing that reuses it is touched."""
    deadline = time.monotonic() + 5
    killed = False
    while True:
        try:
            if os.waitpid(pid, os.WNOHANG)[0]:
                return True
        except ChildProcessError:
            return True
        if not killed:
            os.kill(pid, signal.SIGKILL)
            killed = True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)


@pytest.mark.timeout(600)
# oss_skip: needs KVM and Buck-built Firecracker inputs.
@pytest.mark.oss_skip
def test_python_program_runs_in_guest(capsys: pytest.CaptureFixture[str]) -> None:
    """The probe runs in a guest built by `python_guest_rootfs`.

    Its Python version is this test's. Only loopback is up, and the only other
    interface is the down `sit0` the guest kernel creates. The root filesystem
    is read-only. Two Chrysalis nodes exchange a message over loopback.
    """
    if (reason := microvm.kvm_unavailable()) is not None:
        pytest.skip(reason)
    status = python_guest.main([os.environ["CHR_FC_ROOTFS"], "python_guest_probe"])
    console = capsys.readouterr().out
    assert status == 0, console
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


def test_a_guest_that_never_connects_is_reclaimed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A stand-in Firecracker that never connects makes `main` fail at
    `RUN_TIMEOUT`, and the stand-in is reaped and the run directory removed."""
    # Records its PID and its run directory (the directory of its
    # `--config-file` argument), then becomes `sleep`: one process.
    stand_in = tmp_path / "firecracker"
    stand_in.write_text(
        "#!/bin/sh\n"
        f"echo $$ > {tmp_path}/pid\n"
        f'dirname "$3" > {tmp_path}/run.path\n'
        "exec sleep 600\n"
    )
    stand_in.chmod(0o755)
    (tmp_path / "vmlinux").touch()
    (tmp_path / "rootfs").touch()
    monkeypatch.setenv("CHR_FC_FIRECRACKER", str(stand_in))
    monkeypatch.setenv("CHR_FC_KERNEL", str(tmp_path / "vmlinux"))
    monkeypatch.setattr(python_guest, "kvm_unavailable", lambda: None)
    monkeypatch.setattr(python_guest, "RUN_TIMEOUT", 1)
    recovered = False
    try:
        assert python_guest.main([str(tmp_path / "rootfs"), "probe"]) == 1
        assert (
            "python_guest: failed: the guest did not finish within 1 s\n"
            in capsys.readouterr().err
        )
        pid = int((tmp_path / "pid").read_text())
        assert _reaped(pid), "main should reap the stand-in"
        run_dir = Path((tmp_path / "run.path").read_text().strip())
        assert not run_dir.exists(), "the run directory should be removed"
    finally:
        pid_file, run_path = tmp_path / "pid", tmp_path / "run.path"
        recovered = not pid_file.exists() or _recover(int(pid_file.read_text()))
        if recovered and run_path.exists():
            shutil.rmtree(run_path.read_text().strip(), ignore_errors=True)
    assert recovered, f"the stand-in survived recovery; preserved {tmp_path}"
