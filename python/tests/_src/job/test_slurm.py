# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import shlex
import subprocess
from unittest.mock import patch

import pytest
from monarch._src.job import _slurm_batch
from monarch._src.job.job import BatchJob, job_load
from monarch._src.job.slurm import SlurmJob


def _fake_sbatch(*args, **kwargs):
    return subprocess.CompletedProcess(
        args=["sbatch"], returncode=0, stdout="Submitted batch job 12345\n", stderr=""
    )


def _make_job(**overrides) -> SlurmJob:
    params = {
        "meshes": {"trainer": 1, "generator": 1},
        "gpus_per_node": 8,
        "partition": "gpu",
        "time_limit": "01:00:00",
        "slurm_args": ["--qos=dev", "--account=acct"],
        "python_exe": "/venv/bin/python",
        "exclusive": True,
    }
    params.update(overrides)
    return SlurmJob(**params)


def _submitted_script(mock) -> str:
    """The script piped to the single sbatch call, asserting the call shape."""
    calls = [c for c in mock.call_args_list if c.args and c.args[0] == ["sbatch"]]
    assert len(calls) == 1, f"expected one sbatch call, got {len(calls)}"
    call = calls[0]
    assert call.kwargs.get("check") is True
    assert call.kwargs.get("text") is True
    return call.kwargs["input"]


# ---- sbatch script generation ------------------------------------------------


def test_batch_mode_invokes_in_allocation_runner(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = "/venv/bin/python -m my.train --config foo"
    with patch(
        "monarch._src.job.slurm.subprocess.run", side_effect=_fake_sbatch
    ) as mock:
        job = _make_job()
        job.apply(client_script=client)

    script = _submitted_script(mock)
    # sbatch body is a single call to the runner with the (quoted) client command
    assert "-m monarch._src.job._slurm_batch" in script
    assert "--port 22222" in script
    assert shlex.quote(client) in script
    assert "--nodes=2" in script
    # the shell stays dumb: worker srun + teardown now live in the runner
    assert "srun" not in script
    assert "trap" not in script
    assert "scancel" not in script
    assert "sleep" not in script
    # a BatchJob wrapper is cached so the in-allocation client reconnects
    cached = job_load(str(tmp_path / ".monarch" / "job_state.pkl"))
    assert isinstance(cached, BatchJob)
    # The BatchJob wrapper is the running authority (its _running is always
    # self); the wrapped job is deliberately NOT pre-marked running -- batch
    # mode determines liveness from $SLURM_JOB_ID + squeue. (regression guard
    # for the JobTrait "subclasses must not set _status directly" contract)
    assert cached.active
    assert not cached._job.active


def test_external_controller_mode_has_no_client(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch(
        "monarch._src.job.slurm.subprocess.run", side_effect=_fake_sbatch
    ) as mock:
        job = _make_job()
        job.apply()  # no client_script -> workers only

    script = _submitted_script(mock)
    assert "srun" in script
    assert "run_worker_loop_forever" in script
    assert "_slurm_batch" not in script
    assert "MONARCH_BATCH_JOB" not in script
    assert not (tmp_path / ".monarch" / "job_state.pkl").exists()


def test_submit_raises_when_job_id_unparseable(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    no_id = subprocess.CompletedProcess(["sbatch"], 0, stdout="(nothing)\n", stderr="")
    with patch("monarch._src.job.slurm.subprocess.run", return_value=no_id):
        with pytest.raises(RuntimeError, match="parse job ID"):
            _make_job()._submit_slurm_job(2)


def test_submit_wraps_sbatch_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    err = subprocess.CalledProcessError(1, ["sbatch"], stderr="boom")
    with patch("monarch._src.job.slurm.subprocess.run", side_effect=err):
        with pytest.raises(RuntimeError, match="Failed to submit SLURM job"):
            _make_job()._submit_slurm_job(2)


# ---- $SLURM_JOB_ID fallback + _kill lifecycle --------------------------------


def test_resolved_job_id_fallback_order(monkeypatch):
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    monkeypatch.delenv("MONARCH_BATCH_JOB", raising=False)
    job = _make_job()
    assert job._resolved_job_id() is None

    # Outside batch mode the $SLURM_JOB_ID fallback must NOT fire: an external
    # controller could itself be running inside an unrelated SLURM allocation,
    # and adopting that id would query/scancel the wrong job.
    monkeypatch.setenv("SLURM_JOB_ID", "99999")
    assert job._resolved_job_id() is None

    # Reloaded inside its own batch allocation, it adopts $SLURM_JOB_ID.
    monkeypatch.setenv("MONARCH_BATCH_JOB", "1")
    assert job._resolved_job_id() == "99999"

    # A submitted id always takes precedence over the env.
    job._slurm_job_id = "12345"
    assert job._resolved_job_id() == "12345"


def test_kill_is_noop_inside_batch_allocation(monkeypatch):
    # The in-allocation client must not scancel its own allocation; the runner
    # owns teardown. BatchJob registers _kill as the client's atexit hook.
    monkeypatch.setenv("MONARCH_BATCH_JOB", "1")
    monkeypatch.setenv("SLURM_JOB_ID", "99999")
    job = _make_job()
    with patch("monarch._src.job.slurm.subprocess.run") as run:
        job._kill()
    run.assert_not_called()


def test_kill_scancels_for_external_controller(monkeypatch):
    monkeypatch.delenv("MONARCH_BATCH_JOB", raising=False)
    job = _make_job()
    job._slurm_job_id = "777"
    seen = []

    def _record(*args, **kwargs):
        seen.append(args[0])
        return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

    with patch("monarch._src.job.slurm.subprocess.run", side_effect=_record):
        job._kill()
    assert ["scancel", "777"] in seen


def test_jobs_active_for_reloaded_batch_job(monkeypatch):
    # A job reloaded inside its own batch allocation pickles as not_running (the
    # launcher never marked it), but batch mode treats it as active and confirms
    # liveness via squeue on $SLURM_JOB_ID rather than the local flag.
    monkeypatch.setenv("MONARCH_BATCH_JOB", "1")
    monkeypatch.setenv("SLURM_JOB_ID", "424242")
    job = _make_job()
    assert not job.active  # freshly unpickled: no local launch state
    assert job._slurm_job_id is None
    with patch.object(
        SlurmJob, "_get_job_info_json", return_value={"job_state": ["RUNNING"]}
    ) as info:
        assert job._jobs_active() is True
    info.assert_called_once_with("424242")


# ---- the in-allocation runner ------------------------------------------------


class _FakeWorkers:
    """Stands in for the backgrounded worker srun Popen."""

    def __init__(self, *, wait_times_out: bool = False):
        self._wait_times_out = wait_times_out
        self.terminated = False
        self.killed = False

    def poll(self):
        return None  # still running when the client exits

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        if self._wait_times_out:
            raise subprocess.TimeoutExpired(cmd="srun", timeout=timeout)
        return 0

    def kill(self):
        self.killed = True


def _run_runner(
    monkeypatch, workers, client="/venv/bin/python -m my.train --config foo"
):
    captured = {}

    def _fake_run(cmd, *a, **k):
        captured["client_cmd"] = cmd
        captured["env"] = k.get("env")
        return subprocess.CompletedProcess(cmd, 7)

    monkeypatch.setattr(_slurm_batch.subprocess, "Popen", lambda cmd, *a, **k: workers)
    monkeypatch.setattr(_slurm_batch.subprocess, "run", _fake_run)
    with pytest.raises(SystemExit) as exc:
        _slurm_batch.main(["--port", "22222", client])
    return exc.value.code, captured


def test_runner_marks_client_and_tears_down_workers(monkeypatch):
    workers = _FakeWorkers()
    code, captured = _run_runner(monkeypatch, workers)

    assert code == 7  # client's exit status propagates
    # client runs as argv (no shell) with the batch marker set
    assert captured["client_cmd"] == [
        "/venv/bin/python",
        "-m",
        "my.train",
        "--config",
        "foo",
    ]
    assert captured["env"]["MONARCH_BATCH_JOB"] == "1"
    assert workers.terminated is True
    assert workers.killed is False


def test_runner_kills_workers_if_terminate_times_out(monkeypatch):
    workers = _FakeWorkers(wait_times_out=True)
    _run_runner(monkeypatch, workers)
    assert workers.terminated is True
    assert workers.killed is True  # falls back to SIGKILL when terminate hangs
