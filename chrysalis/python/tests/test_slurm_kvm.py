# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs the KVM probe `slurm/kvm-run-check.sh` as a job would, under bash.

Where this user can open `/dev/kvm`, the probe must run its one-instruction
guest to the halt and print PASS. Without KVM, it must still refuse root before
it touches `/dev/kvm`. A probe that hangs, as a guest can on a broken kernel,
must not outlive its test.
"""

import contextlib
import fcntl
import os
import signal
import subprocess
import time
from collections.abc import Mapping
from pathlib import Path

import pytest


def _probe(
    script: str, env: Mapping[str, str] | None = None, timeout: float = 60
) -> tuple[int, list[str]]:
    """Runs `script` under bash; returns its status and its output lines.

    The probe's guest runs in a `python3` child of bash, so killing bash alone
    would leave the guest running. The probe gets its own process group. A
    timeout or any other exit while bash is unreaped kills the whole group
    before re-raising. Once bash has been reaped, its pid no longer names a
    group this test owns, so the original exception propagates unchanged.
    """
    proc = subprocess.Popen(
        ["bash", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        start_new_session=True,
    )
    try:
        output, _ = proc.communicate(timeout=timeout)
    except BaseException:
        if proc.returncode is None:
            # bash is not yet reaped, so its pid still names this group.
            os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate(timeout=5)
        raise
    return proc.returncode, output.splitlines()


# oss_skip: needs KVM and the Buck-provided probe path.
@pytest.mark.oss_skip
def test_probe_runs_its_guest_to_the_halt() -> None:
    if not os.access("/dev/kvm", os.R_OK | os.W_OK):
        pytest.skip("/dev/kvm is not usable by this user")
    if os.getuid() == 0:
        pytest.skip("the probe refuses root")
    status, lines = _probe(os.environ["KVM_RUN_CHECK"])
    assert status == 0, lines
    assert "KVM_GET_API_VERSION: 12" in lines
    assert "KVM_RUN: returned, exit_reason 5" in lines
    assert "PASS" in lines


def test_probe_refuses_root_before_opening_kvm(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_id = bin_dir / "id"
    fake_id.write_text("#!/bin/sh\necho 0\n")
    fake_id.chmod(0o755)
    env = os.environ | {"PATH": f"{bin_dir}:{os.environ['PATH']}"}
    status, lines = _probe(os.environ["KVM_RUN_CHECK"], env)
    assert status == 1, lines
    assert "FAIL: running as root" in lines
    assert not any(line.startswith("KVM_GET_API_VERSION") for line in lines)
    assert "PASS" not in lines


# Shaped like the probe: bash runs a `python3` child that inherits its output.
# The child takes a lock, records its pid, then hangs as a stuck guest would.
_HANGING_PROBE = """python3 - "$GUEST_LOCK" "$GUEST_PID" <<'PY'
import fcntl, os, sys, time
lock = open(sys.argv[1], "w")
fcntl.flock(lock, fcntl.LOCK_EX)
with open(sys.argv[2], "w") as f:
    f.write(str(os.getpid()))
while True:
    time.sleep(1)
PY
"""


def _kill_leftover_guest(pid_file: Path, lock: Path) -> None:
    """Kills the hanging fixture's guest if a broken `_probe` left it running.

    The pid is signalled only while its command line still names this test's
    lock, so a reused pid is never touched.
    """
    if not pid_file.exists():
        return
    guest = int(pid_file.read_text())
    with contextlib.suppress(FileNotFoundError, ProcessLookupError):
        if str(lock).encode() in Path(f"/proc/{guest}/cmdline").read_bytes():
            os.kill(guest, signal.SIGKILL)


def test_probe_timeout_stops_the_guest_process(tmp_path: Path) -> None:
    probe = tmp_path / "probe.sh"
    probe.write_text(_HANGING_PROBE)
    lock, pid_file = tmp_path / "guest-lock", tmp_path / "guest-pid"
    env = os.environ | {"GUEST_LOCK": str(lock), "GUEST_PID": str(pid_file)}
    try:
        start = time.monotonic()
        with pytest.raises(subprocess.TimeoutExpired):
            _probe(str(probe), env, timeout=2)
        assert time.monotonic() - start < 10
        guest = int(pid_file.read_text())
        # The orphaned guest is reaped by the namespace's init, not the helper.
        deadline = time.monotonic() + 5
        while Path(f"/proc/{guest}").exists():
            assert time.monotonic() < deadline, f"guest {guest} remains"
            time.sleep(0.02)
        with lock.open("w") as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        _kill_leftover_guest(pid_file, lock)


def test_failure_after_bash_is_reaped_signals_no_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `communicate` decodes the output only after it has reaped bash, so an
    # undecodable byte raises once the group id may belong to someone else.
    probe = tmp_path / "probe.sh"
    probe.write_text("printf '\\377\\n'\n")
    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: signalled.append((pgid, sig)))
    with pytest.raises(UnicodeDecodeError):
        _probe(str(probe))
    assert signalled == []
