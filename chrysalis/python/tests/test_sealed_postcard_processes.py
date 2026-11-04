# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs Alice, Relay, and Bob as three processes over Unix-domain streams.

This step has no VM. Each process uses `VsockDatagramSocket` with a
`unix:<path>` address, so the byte streams are ordinary Unix-domain stream
sockets. The carrier still frames and reconstructs datagrams, exchanges its
HELLO with each `node:` name, reuses connections, manages its queues, and shuts
down its tasks.

The later VM test uses the same carrier. Only the way it opens the byte stream
changes: a guest uses `native:<cid>:<port>`, while its host uses Firecracker's
`hybrid:<port>:<path>` connection.
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


_STOP_TIMEOUT = 10
_KILL_TIMEOUT = 5


def _binary() -> str:
    path = Path(os.environ["SEALED_POSTCARD"]).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"SEALED_POSTCARD={path} is not a built file")
    return str(path)


class _Role:
    def __init__(
        self,
        argv: list[str],
        *,
        env: dict[str, str],
        log: Path,
    ) -> None:
        self.log_path = log
        self._log = open(log, "w", encoding="utf-8")
        self._status: int | None = None
        self.process = subprocess.Popen(
            argv,
            env=env,
            start_new_session=True,
            stdout=self._log,
            stderr=self._log,
            text=True,
        )

    @property
    def pgid(self) -> int:
        return self.process.pid

    def wait(self, timeout: float) -> int:
        if self._status is not None:
            return self._status
        deadline = time.monotonic() + timeout
        flags = os.WEXITED | os.WNOHANG | os.WNOWAIT
        while os.waitid(os.P_PID, self.process.pid, flags) is None:
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(self.process.args, timeout)
            time.sleep(0.01)
        # Keep the exited leader unreaped until `stop` has dealt with the whole
        # group. Its PID then cannot be reused for an unrelated process group.
        return self.stop()

    def _signal_group(self, sent: signal.Signals) -> None:
        try:
            os.killpg(self.pgid, sent)
        except ProcessLookupError:
            pass

    def _group_exists(self) -> bool:
        try:
            os.killpg(self.pgid, 0)
        except ProcessLookupError:
            return False
        return True

    def stop(self, first: signal.Signals = signal.SIGTERM) -> int:
        if self._status is not None:
            return self._status
        # The group may outlive its leader, so signal it even when the process
        # has already exited.
        self._signal_group(first)
        status = self.process.poll()
        if status is None:
            try:
                status = self.process.wait(timeout=_STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass

        if self._group_exists():
            self._signal_group(signal.SIGKILL)

        deadline = time.monotonic() + _KILL_TIMEOUT
        if status is None:
            remaining = max(0, deadline - time.monotonic())
            status = self.process.wait(timeout=remaining)
        while self._group_exists():
            if time.monotonic() >= deadline:
                raise TimeoutError(f"process group {self.pgid} survived SIGKILL")
            time.sleep(0.01)
        assert status is not None
        self._status = status
        return status

    def output(self) -> str:
        self._log.flush()
        return self.log_path.read_text()

    def close(self) -> None:
        self._log.close()


def _identity_env(path: Path) -> dict[str, str]:
    return {**os.environ, "SEALED_POSTCARD_IDENTITY": path.read_text()}


def _parse_pids(output: str) -> dict[str, str]:
    pids = dict(line.split() for line in output.splitlines())
    assert set(pids) == {"alice", "relay", "bob"}
    assert all(re.fullmatch(r"[0-9a-f]{32}", pid) for pid in pids.values())
    return pids


def _cleanup(roles: list[_Role], directory: Path) -> None:
    error: Exception | None = None
    for role in reversed(roles):
        try:
            role.stop()
        except Exception as exc:
            if error is None:
                error = exc
        finally:
            role.close()
    if error is not None:
        raise RuntimeError(
            f"failed to stop every role; preserved run directory {directory}"
        ) from error
    shutil.rmtree(directory)


def test_sealed_postcard_as_three_processes() -> None:
    directory = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-procs-"))
    roles: list[_Role] = []
    try:
        identities = subprocess.run(
            [_binary(), "identities", "--out", str(directory / "ids")],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        pids = _parse_pids(identities.stdout)
        alice_path = directory / "a"
        relay_path = directory / "r"

        alice = _Role(
            [
                _binary(),
                "alice",
                "--listen",
                f"unix:{alice_path}",
                "--bob-pid",
                pids["bob"],
            ],
            env=_identity_env(directory / "ids" / "alice.pem"),
            log=directory / "alice.log",
        )
        roles.append(alice)
        relay = _Role(
            [
                _binary(),
                "relay",
                "--parent-pid",
                pids["alice"],
                "--parent",
                f"unix:{alice_path}",
                "--listen",
                f"unix:{relay_path}",
            ],
            env=_identity_env(directory / "ids" / "relay.pem"),
            log=directory / "relay.log",
        )
        roles.append(relay)
        bob = _Role(
            [
                _binary(),
                "bob",
                "--parent-pid",
                pids["relay"],
                "--parent",
                f"unix:{relay_path}",
                "--alice-pid",
                pids["alice"],
            ],
            env=_identity_env(directory / "ids" / "bob.pem"),
            log=directory / "bob.log",
        )
        roles.append(bob)

        assert alice.wait(timeout=120) == 0, alice.output()
        assert bob.stop(first=signal.SIGINT) == 0, bob.output()
        assert "bob served 33 streams, all from alice: true" in bob.output()
        assert relay.stop(first=signal.SIGTERM) == 0, relay.output()
        assert re.search(
            r"relay observed [1-9][0-9]* datagrams, "
            r"postcard plaintext seen: false, log complete: true",
            relay.output(),
        )
    finally:
        _cleanup(roles, directory)


def _check_cleanup(script: str) -> None:
    directory = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-cleanup-"))
    role = _Role(
        [sys.executable, "-c", script],
        env=os.environ.copy(),
        log=directory / "stubborn.log",
    )
    try:
        deadline = time.monotonic() + 5
        while "ready" not in role.output():
            if time.monotonic() >= deadline:
                raise TimeoutError("stubborn process did not become ready")
            time.sleep(0.01)

        started = time.monotonic()
        role.stop()
        role.close()
        try:
            os.killpg(role.pgid, 0)
        except ProcessLookupError:
            pass
        else:
            raise AssertionError("process group survived cleanup")
        shutil.rmtree(directory)
        assert time.monotonic() - started < 20
        assert not directory.exists()
    finally:
        if role._status is None:
            role._signal_group(signal.SIGKILL)
            try:
                role.process.wait(timeout=_KILL_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass
        role.close()
        shutil.rmtree(directory, ignore_errors=True)


def test_cleanup_reaps_every_process_in_the_group() -> None:
    _check_cleanup(
        "import signal, subprocess, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "subprocess.Popen(['/bin/sleep', '600']); "
        "print('ready', flush=True); time.sleep(600)"
    )
    _check_cleanup(
        "import subprocess; subprocess.Popen(['/bin/sleep', '600']); "
        "print('ready', flush=True)"
    )
    _check_cleanup(
        "import signal, subprocess, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "subprocess.Popen(['/bin/sleep', '600']); "
        "signal.signal(signal.SIGTERM, signal.SIG_DFL); "
        "print('ready', flush=True); time.sleep(600)"
    )
