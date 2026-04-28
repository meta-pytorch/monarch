# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import shutil
import signal
import subprocess
import sys
import time
from typing import Callable

import pytest
from monarch._src.spmd.host_mesh import (
    _IN_PAR,
    _spawn_worker_process,
    _worker_addr_key,
    host_mesh_from_store,
)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _wait_for(
    predicate: Callable[[], bool], timeout: float = 5.0, interval: float = 0.05
) -> bool:
    """Poll ``predicate`` until it returns truthy or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class _InMemoryStore:
    """Minimal store with the subset of ``torch.distributed.Store`` that
    :func:`host_mesh_from_store` consumes."""

    def __init__(self) -> None:
        self._values: dict[str, bytes] = {}

    def set(self, key: str, value: str | bytes) -> None:
        self._values[key] = value.encode() if isinstance(value, str) else value

    def get(self, key: str) -> bytes:
        return self._values[key]


def test_spawn_ipc_returns_addr_and_live_pid() -> None:
    addr, pid = _spawn_worker_process(transport="ipc", name="test_worker")
    try:
        assert addr.startswith("ipc://")
        socket_path = addr.removeprefix("ipc://")
        assert "/monarch_worker_" in socket_path
        assert os.path.basename(socket_path) == "test_worker"
        assert pid > 0
        assert _pid_alive(pid)
    finally:
        # Don't rely on parent-exit cleanup for tests: SIGKILL the worker
        # directly and drop its tmpdir so the test host stays tidy.
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        ipc_dir = os.path.dirname(addr.removeprefix("ipc://"))
        shutil.rmtree(ipc_dir, ignore_errors=True)


def test_invalid_transport_raises_value_error() -> None:
    with pytest.raises(ValueError, match="unsupported transport"):
        _spawn_worker_process(transport="bogus")


def test_net_transports_require_explicit_port() -> None:
    # Port 0 means "kernel assigns at bind time," which the parent can't
    # learn without racing — so we reject it up front rather than silently
    # pre-reserving a free port. Applies to every port-using transport.
    for transport in ("tcp", "metatls", "metatls-hostname"):
        with pytest.raises(ValueError, match="monarch_port must be an explicit"):
            _spawn_worker_process(transport=transport, monarch_port=0)


@pytest.mark.parametrize(
    "missing", ["RANK", "LOCAL_RANK", "WORLD_SIZE", "LOCAL_WORLD_SIZE"]
)
def test_host_mesh_from_store_requires_torchelastic_env(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    # Every torchelastic env var must be present when no keyword override
    # is supplied. Baseline-set all four, then unset the one under test
    # and verify we get a targeted error.
    monkeypatch.setenv("RANK", "0")
    monkeypatch.setenv("LOCAL_RANK", "0")
    monkeypatch.setenv("WORLD_SIZE", "1")
    monkeypatch.setenv("LOCAL_WORLD_SIZE", "1")
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(RuntimeError, match=missing):
        host_mesh_from_store(_InMemoryStore())


def test_host_mesh_from_store_kwargs_override_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Unset every env var, then pass all four topology values via
    # keyword arguments. Use a rank 3 / local_rank 1 layout so we hit the
    # non-primary passive branch without spawning anything.
    for var in ("RANK", "LOCAL_RANK", "WORLD_SIZE", "LOCAL_WORLD_SIZE"):
        monkeypatch.delenv(var, raising=False)

    store = _InMemoryStore()
    assert (
        host_mesh_from_store(
            store,
            transport="ipc",
            rank=3,
            local_rank=1,
            world_size=4,
            local_world_size=2,
        )
        is None
    )


def test_host_mesh_from_store_validates_world_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RANK", "0")
    monkeypatch.setenv("LOCAL_RANK", "0")
    monkeypatch.setenv("WORLD_SIZE", "3")
    monkeypatch.setenv("LOCAL_WORLD_SIZE", "2")
    with pytest.raises(ValueError, match="divisible"):
        host_mesh_from_store(_InMemoryStore())


def test_host_mesh_from_store_non_local_rank_zero_is_passive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Rank with LOCAL_RANK != 0 and RANK != 0 spawns nothing and returns
    # None. Use rank 3 so group_rank = 3 // 2 = 1, letting us assert the
    # key this rank's group would have published if local rank 0 had
    # incorrectly invoked us is absent.
    monkeypatch.setenv("RANK", "3")
    monkeypatch.setenv("LOCAL_RANK", "1")
    monkeypatch.setenv("WORLD_SIZE", "4")
    monkeypatch.setenv("LOCAL_WORLD_SIZE", "2")

    store = _InMemoryStore()
    assert host_mesh_from_store(store, transport="ipc") is None
    # This rank did not spawn; nobody published for group 1.
    assert _worker_addr_key(1) not in store._values


def test_parent_death_kills_worker_via_pipe_eof() -> None:
    # End-to-end check for the parent-death safety net:
    #   test runner -> intermediate parent -> worker
    # Under Buck/PAR we re-invoke the test's own PAR binary with
    # PAR_MAIN_OVERRIDE pointing at tests.spmd_host_mesh_parent_helper.
    # Under plain pytest sys.argv[0] is the pytest CLI (which doesn't
    # honor PAR_MAIN_OVERRIDE), so we run the helper script directly.
    # Either way the helper calls _spawn_worker_process(...) and exits;
    # the worker's only lifeline is the parent-watch pipe, so when the
    # intermediate parent exits the kernel closes its write end and the
    # worker's blocking os.read returns EOF, triggering os._exit(0).
    if _IN_PAR:
        env = {
            **os.environ,
            "PAR_MAIN_OVERRIDE": "tests.spmd_host_mesh_parent_helper",
        }
        cmd = [sys.argv[0]]
    else:
        helper_path = os.path.join(
            os.path.dirname(__file__), "spmd_host_mesh_parent_helper.py"
        )
        env = dict(os.environ)
        cmd = [sys.executable, helper_path]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    worker_pid = int(result.stdout.strip())

    exited = _wait_for(lambda: not _pid_alive(worker_pid), timeout=10.0)
    if not exited:
        # Safety net failed to fire — kill the worker ourselves so the test
        # harness doesn't leak it, then report the failure.
        try:
            os.kill(worker_pid, signal.SIGKILL)
        except OSError:
            pass
    assert exited, f"worker pid={worker_pid} still alive after parent exit"
