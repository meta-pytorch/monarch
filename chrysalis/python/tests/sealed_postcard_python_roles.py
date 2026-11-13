# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs the Python sealed-postcard roles as processes and cleans them up.

The process tests share this: each role is a `Role` in its own session, with
its output in a log in the test's run directory. `cleanup` stops every role,
then removes the directory, or keeps it and raises if a role's process group
survived.
"""

import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path


_STOP_TIMEOUT = 10
_KILL_TIMEOUT = 5


def binary() -> str:
    path = Path(os.environ["SEALED_POSTCARD_PYTHON"]).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"SEALED_POSTCARD_PYTHON={path} is not a built file")
    return str(path)


class Role:
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

    def stop(self) -> int:
        if self._status is not None:
            return self._status
        # The group may outlive its leader, so signal it even when the process
        # has already exited.
        self._signal_group(signal.SIGTERM)
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
        self._status = status
        return status

    def output(self) -> str:
        self._log.flush()
        return self.log_path.read_text()

    def close(self) -> None:
        self._log.close()


def identity_env(path: Path) -> dict[str, str]:
    return {**os.environ, "SEALED_POSTCARD_IDENTITY": path.read_text()}


def parse_pids(output: str, roles: set[str]) -> dict[str, str]:
    """Parse `identities`' `<role> <pid>` lines, which must name exactly `roles`."""
    pids = dict(line.split() for line in output.splitlines())
    assert set(pids) == roles, pids
    assert all(re.fullmatch(r"[0-9a-f]{32}", pid) for pid in pids.values())
    return pids


def cleanup(roles: list[Role], directory: Path) -> None:
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
