# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Integration tests for the monarch CLI.

All job configuration lives in separate Python module files (job_a.py,
job_b.py) written into the test working directory — the same pattern a user
would follow.  Each test applies one (or both) configs via
``monarch apply`` and then exercises the CLI features under test.

Having two configs lets us test that switching configs correctly invalidates
the old mount process and starts a fresh one for the new configuration.

Run with:
    uv run pytest python/tests/test_cli_integration.py -v
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from textwrap import dedent

import pytest


pytestmark = pytest.mark.skipif(
    not (shutil.which("fusermount3") or shutil.which("fusermount")),
    reason="fusermount3 not found — FUSE unavailable",
)

# ── Job config template ────────────────────────────────────────────────────

_JOB_TEMPLATE = dedent("""\
    import os
    from monarch._src.job.process import ProcessJob

    _base = os.path.abspath("{label}")

    job = ProcessJob({{"hosts": 2}})
    # $SUBDIR in mntpoint gives each worker a unique mount path so they
    # don't conflict when sharing the local filesystem (ProcessJob).
    job.remote_mount(os.path.join(_base, "src"), mntpoint=os.path.join(_base, "worker", "$SUBDIR"), python_exe=None)
    job.gather_mount(
        os.path.join(_base, "$SUBDIR"),
        os.path.join(_base, "gathered"),
    )
""")

# ── Fixture ────────────────────────────────────────────────────────────────


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Set up a clean working directory with two job configs (a and b)."""
    monkeypatch.chdir(tmp_path)

    for label in ("a", "b"):
        base = tmp_path / label
        src = base / "src"
        src.mkdir(parents=True)
        (src / "hello.txt").write_text(f"config {label} v1\n")
        for i in range(2):
            wd = base / f"hosts_{i}"
            wd.mkdir()
            (wd / "output.txt").write_text(f"config {label} worker {i}\n")
        (tmp_path / f"job_{label}.py").write_text(_JOB_TEMPLATE.format(label=label))

    (tmp_path / ".monarch").mkdir()

    yield tmp_path

    try:
        _cli(tmp_path, "kill", check=False)
    except Exception:
        pass


# ── Helpers ────────────────────────────────────────────────────────────────


def _cli(workdir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    # Use a real file for stderr rather than a pipe.  The mount-worker daemon
    # inherits the apply subprocess's stderr fd and keeps it open after apply
    # exits; with a PIPE that would block communicate() waiting for EOF, but
    # with a regular file we just seek-and-read after the child exits.
    import tempfile

    with tempfile.TemporaryFile() as stderr_f:
        proc = subprocess.run(
            [sys.executable, "-m", "monarch.tools.cli", *args],
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=stderr_f,
        )
        stderr_f.seek(0)
        stderr = stderr_f.read().decode(errors="replace")
    stdout = (proc.stdout or b"").decode(errors="replace")
    result = subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)
    if check and result.returncode != 0:
        pytest.fail(
            f"CLI {args!r} exited {result.returncode}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return result


def _apply(workdir: Path, label: str) -> None:
    _cli(workdir, "apply", f"job_{label}.job")


def _src(workdir: Path, label: str) -> str:
    return str(workdir / label / "src")


def _mnt(workdir: Path, label: str) -> str:
    """Mount point on rank-0 worker (hosts_0) where the remote-mounted src appears."""
    return str(workdir / label / "worker" / "hosts_0")


def _gathered(workdir: Path, label: str) -> str:
    return str(workdir / label / "gathered")


def _worker_dir(workdir: Path, label: str, host: int) -> str:
    return str(workdir / label / f"hosts_{host}")


# ── Context management ─────────────────────────────────────────────────────


def test_context_ls_no_contexts_initially(env):
    result = _cli(env, "context", "ls", check=False)
    assert "No contexts" in result.stdout


def test_context_create_and_ls(env):
    print(env)
    _cli(env, "context", "create", "work")
    result = _cli(env, "context", "ls")
    assert "work" in result.stdout


def test_context_use_migrates_plain_file(env):
    """context use on a plain job_state.pkl migrates it to default/ first."""
    (env / ".monarch" / "job_state.pkl").write_bytes(b"placeholder")
    assert not (env / ".monarch" / "job_state.pkl").is_symlink()

    _cli(env, "context", "use", "work")

    assert (env / ".monarch" / "job_state.pkl").is_symlink()
    assert (env / ".monarch" / "default" / "state.pkl").exists()
    result = _cli(env, "context", "ls")
    assert "* work" in result.stdout
    assert "default" in result.stdout


def test_context_rm_restores_default_link(env):
    (env / ".monarch" / "job_state.pkl").write_bytes(b"placeholder")
    _cli(env, "context", "use", "work")
    _cli(env, "context", "rm", "work")

    assert (env / ".monarch" / "job_state.pkl").is_symlink()
    assert (
        Path(os.readlink(str(env / ".monarch" / "job_state.pkl")))
        == Path("default") / "state.pkl"
    )


def test_context_rm_removes_directory(env):
    _cli(env, "context", "create", "scratch")
    assert (env / ".monarch" / "scratch").is_dir()
    _cli(env, "context", "rm", "scratch")
    assert not (env / ".monarch" / "scratch").exists()


# ── exec ───────────────────────────────────────────────────────────────────


def test_exec_basic(env):
    _apply(env, "a")
    result = _cli(env, "exec", "echo", "hi")
    assert "hi" in result.stdout


def test_exec_all_ranks(env):
    """--all redirects output to files; both ranks run the command."""
    _apply(env, "a")
    result = _cli(env, "exec", "--all", "echo", "from_rank")
    # --all → output redirected, so stdout has the report line
    assert "Output" in result.stdout


def test_exec_python_script(env):
    script = env / "greet.py"
    script.write_text('print("hello_from_script")\n')
    _apply(env, "a")
    result = _cli(env, "exec", str(script))
    assert "hello_from_script" in result.stdout


def test_exec_python_module(env, tmp_path):
    (tmp_path / "mymod.py").write_text('print("hello_from_module")\n')
    _apply(env, "a")
    result = _cli(env, "exec", "--workdir", str(tmp_path), "-m", "mymod")
    assert "hello_from_module" in result.stdout


# ── Remote mount ───────────────────────────────────────────────────────────


def test_remote_mount_files_visible_on_workers(env):
    """Files pushed via remote_mount are visible at the mount point on workers."""
    _apply(env, "a")
    result = _cli(env, "exec", "cat", f"{_mnt(env, 'a')}/hello.txt")
    assert "config a v1" in result.stdout


def test_remote_mount_refresh_workers_see_updated_content(env):
    """Second exec sends refresh; workers see the new file content."""
    _apply(env, "a")
    src = _src(env, "a")
    mnt = _mnt(env, "a")

    _cli(env, "exec", "true")

    (Path(src) / "hello.txt").write_text("config a v2\n")

    result = _cli(env, "exec", "cat", f"{mnt}/hello.txt")
    assert "config a v2" in result.stdout


def test_remote_mount_new_file_visible_after_refresh(env):
    _apply(env, "a")
    src = _src(env, "a")
    mnt = _mnt(env, "a")

    _cli(env, "exec", "true")

    (Path(src) / "new.txt").write_text("brand new\n")

    result = _cli(env, "exec", "cat", f"{mnt}/new.txt")
    assert "brand new" in result.stdout


# ── Gather mount ───────────────────────────────────────────────────────────


def test_gather_mount_exposes_per_host_output(env):
    _apply(env, "a")
    gathered = _gathered(env, "a")
    assert os.path.ismount(gathered)
    for i in range(2):
        content = (Path(gathered) / f"hosts_{i}" / "output.txt").read_text()
        assert f"config a worker {i}" in content


def test_gather_mount_persists_across_apply_calls(env):
    """Re-applying the same config reuses the mount process; FUSE stays up."""
    _apply(env, "a")
    gathered = _gathered(env, "a")
    assert os.path.ismount(gathered)

    _apply(env, "a")
    assert os.path.ismount(gathered)
    assert (
        "config a worker 0" in (Path(gathered) / "hosts_0" / "output.txt").read_text()
    )


def test_gather_mount_reflects_worker_writes_via_inotify(env):
    from monarch._src.gather_mount.gather_mount import _NOTIFY_BATCH_S

    _apply(env, "a")
    gathered = _gathered(env, "a")
    path = Path(gathered) / "hosts_0" / "output.txt"

    assert "config a worker 0" in path.read_text()

    (Path(_worker_dir(env, "a", 0)) / "output.txt").write_text("updated!\n")
    time.sleep(_NOTIFY_BATCH_S * 3)

    assert "updated!" in path.read_text()


# ── Mount process cache invalidation ───────────────────────────────────────


def test_switching_configs_uses_new_mounts(env):
    """Applying a different config starts a new mount process with new files."""
    _apply(env, "a")
    result = _cli(env, "exec", "cat", f"{_mnt(env, 'a')}/hello.txt")
    assert "config a v1" in result.stdout

    _apply(env, "b")
    result = _cli(env, "exec", "cat", f"{_mnt(env, 'b')}/hello.txt")
    assert "config b v1" in result.stdout
    assert "config a" not in result.stdout


def test_switching_configs_new_gather_mount(env):
    """After switching to config b, the gather mount exposes b's workers."""
    _apply(env, "a")
    _apply(env, "b")

    gathered_b = _gathered(env, "b")
    assert os.path.ismount(gathered_b)
    content = (Path(gathered_b) / "hosts_0" / "output.txt").read_text()
    assert "config b worker 0" in content


# ── Kill ───────────────────────────────────────────────────────────────────


def test_kill_via_cli_stops_workers(env):
    _apply(env, "a")

    from monarch._src.job.job import job_load

    job = job_load(str(env / ".monarch" / "job_state.pkl"))
    pids = [ps.pid for ps in job._host_to_pid.values()]
    assert len(pids) == 2

    _cli(env, "kill")
    time.sleep(0.5)

    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_exec_recreates_job_after_kill(env):
    """After kill, exec auto-applies (recreates workers) and succeeds."""
    _apply(env, "a")
    _cli(env, "kill")
    time.sleep(0.5)

    result = _cli(env, "exec", "echo", "nope")
    assert result.returncode == 0
    assert "nope" in result.stdout
