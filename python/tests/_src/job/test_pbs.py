# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import json
import shlex
import subprocess
from unittest.mock import patch

import pytest
from monarch._src.job import _pbs_batch
from monarch._src.job.job import BatchJob, job_load
from monarch._src.job.pbs import parse_exec_host, PBSJob
from monarch._src.job.service_identity import (
    allocate_service_proc_ids,
    deserialize_service_proc_ids,
    serialize_service_proc_ids,
    SERVICE_PROC_IDS_ENV,
)


def _fake_qsub(*args, **kwargs):
    return subprocess.CompletedProcess(
        args=["qsub"], returncode=0, stdout="12345.pbs-server\n", stderr=""
    )


def _qstat(job_state: str, exec_host: str = ""):
    job = {"job_state": job_state}
    if exec_host:
        job["exec_host"] = exec_host
    return subprocess.CompletedProcess(
        args=["qstat"],
        returncode=0,
        stdout=json.dumps({"Jobs": {"12345.pbs-server": job}}),
        stderr="",
    )


def _make_job(**overrides) -> PBSJob:
    params = {
        "meshes": {"trainer": 1, "generator": 1},
        "queue": "debug",
        "account": "proj",
        "time_limit": "01:00:00",
        "pbs_args": ["-l filesystems=home", "-l place=scatter"],
        "python_exe": "/venv/bin/python",
    }
    params.update(overrides)
    return PBSJob(**params)


def _submitted_script(mock) -> str:
    """The script piped to the single qsub call, asserting the call shape."""
    calls = [c for c in mock.call_args_list if c.args and c.args[0] == ["qsub"]]
    assert len(calls) == 1, f"expected one qsub call, got {len(calls)}"
    call = calls[0]
    assert call.kwargs.get("check") is True
    assert call.kwargs.get("text") is True
    return call.kwargs["input"]


@pytest.fixture
def nodefile(tmp_path, monkeypatch):
    path = tmp_path / "nodefile"
    path.write_text("host-a.cluster.org\nhost-b.cluster.org\nhost-a.cluster.org\n")
    monkeypatch.setenv("PBS_NODEFILE", str(path))
    return path


def test_batch_mode_invokes_in_allocation_runner(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = "python -m my.train --config foo"
    with patch("monarch._src.job.pbs.subprocess.run", side_effect=_fake_qsub) as mock:
        job = _make_job()
        job.apply(client_script=client)

    script = _submitted_script(mock)
    lines = script.splitlines()
    assert "#PBS -l select=2" in lines
    assert "#PBS -l walltime=01:00:00" in lines
    assert "#PBS -q debug" in lines and "#PBS -A proj" in lines
    assert "#PBS -l filesystems=home" in lines and "#PBS -l place=scatter" in lines
    assert 'cd "$PBS_O_WORKDIR"' in lines
    assert any(line.startswith(f"export {SERVICE_PROC_IDS_ENV}=") for line in lines)
    runner = lines[-1]
    assert "-m monarch._src.job._pbs_batch" in runner
    assert "--port 22222" in runner and "--bind-to" not in runner
    assert runner.endswith(shlex.quote(client))
    # a BatchJob wrapper is cached so the in-allocation client reconnects
    cached = job_load(str(tmp_path / ".monarch" / "job_state.pkl"))
    assert isinstance(cached, BatchJob)
    assert cached.active
    assert not cached._job.active
    assert job._pbs_job_id == "12345.pbs-server"


def test_external_controller_mode_has_no_client(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch("monarch._src.job.pbs.subprocess.run", side_effect=_fake_qsub) as mock:
        _make_job().apply()

    runner = _submitted_script(mock).splitlines()[-1]
    assert runner.endswith("--rank-env PMI_RANK")
    assert not (tmp_path / ".monarch" / "job_state.pkl").exists()


def test_launcher_options_reach_the_runner(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch("monarch._src.job.pbs.subprocess.run", side_effect=_fake_qsub) as mock:
        _make_job(
            mpiexec="/opt/pals/mpiexec",
            mpiexec_args=["--cpu-bind=none"],
            rank_env="PALS_RANKID",
            bind_to="0.0.0.0",
        ).apply()

    runner = _submitted_script(mock).splitlines()[-1]
    assert "--mpiexec /opt/pals/mpiexec" in runner
    assert "--mpiexec-arg=--cpu-bind=none" in runner
    assert "--rank-env PALS_RANKID" in runner
    assert "--bind-to 0.0.0.0" in runner


def test_optional_directives_omitted_when_unset(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch("monarch._src.job.pbs.subprocess.run", side_effect=_fake_qsub) as mock:
        PBSJob(meshes={"workers": 1}).apply()

    script = _submitted_script(mock)
    assert (
        "walltime" not in script and "#PBS -q" not in script and "#PBS -A" not in script
    )


def test_submit_raises_when_job_id_unparseable(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    empty = subprocess.CompletedProcess(
        args=["qsub"], returncode=0, stdout="", stderr=""
    )
    with patch("monarch._src.job.pbs.subprocess.run", return_value=empty):
        with pytest.raises(RuntimeError, match="Failed to parse job ID"):
            _make_job().apply()


def test_submit_wraps_qsub_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    error = subprocess.CalledProcessError(1, ["qsub"], stderr="queue is full")
    with patch("monarch._src.job.pbs.subprocess.run", side_effect=error):
        with pytest.raises(RuntimeError, match="queue is full"):
            _make_job().apply()


@pytest.mark.parametrize("bind_to", ["0.0.0.0:22222", "tcp://0.0.0.0", "worker"])
def test_bind_to_rejects_values_that_are_not_ip_addresses(bind_to):
    with pytest.raises(ValueError, match="IPv4 or IPv6 address"):
        _make_job(bind_to=bind_to)


def test_parse_exec_host():
    assert parse_exec_host("x1.dom/0*64+x2.dom/0*64+x1.dom/1*4") == ["x1", "x2"]


def test_wait_for_job_start_returns_exec_hosts():
    job = _make_job()
    states = iter([_qstat("Q"), _qstat("R", "host-a/0*8+host-b/0*8")])
    with (
        patch(
            "monarch._src.job.pbs.subprocess.run",
            side_effect=lambda *a, **k: next(states),
        ),
        patch("monarch._src.job.pbs.time.sleep"),
    ):
        assert job._wait_for_job_start("12345.pbs-server", 2) == ["host-a", "host-b"]


def test_wait_for_job_start_raises_on_finished_job():
    job = _make_job()
    with (
        patch("monarch._src.job.pbs.subprocess.run", return_value=_qstat("F")),
        patch.object(job, "_kill") as kill,
    ):
        with pytest.raises(RuntimeError, match="failed with state: F"):
            job._wait_for_job_start("12345.pbs-server", 2)
    kill.assert_called_once()


def test_state_pairs_controller_addresses_with_worker_service_proc_ids():
    job = _make_job()
    job._pbs_job_id = "12345.pbs-server"
    job._all_hostnames = ["worker-a", "worker-b"]
    job._ensure_service_proc_ids(2)
    service_proc_ids = list(job._service_proc_ids)

    with (
        patch.object(job, "_jobs_active", return_value=True),
        patch(
            "monarch._src.job.pbs.attach_to_workers", return_value=object()
        ) as attach,
    ):
        job._state()

    assert attach.call_args_list[0].kwargs["workers"] == [
        f"{service_proc_ids[0]}@tcp://worker-a:22222"
    ]
    assert attach.call_args_list[1].kwargs["workers"] == [
        f"{service_proc_ids[1]}@tcp://worker-b:22222"
    ]


def test_batch_state_adopts_the_allocation_of_a_hand_written_script(
    nodefile, monkeypatch
):
    """Inside an allocation: no qsub, hosts from $PBS_NODEFILE, ids from the runner's env."""
    ids = allocate_service_proc_ids(2)
    monkeypatch.setenv("MONARCH_BATCH_JOB", "1")
    monkeypatch.setenv(SERVICE_PROC_IDS_ENV, serialize_service_proc_ids(ids))
    job = PBSJob(meshes={"hosts": 2})

    with (
        patch(
            "monarch._src.job.pbs.subprocess.run", side_effect=AssertionError("no qsub")
        ),
        patch(
            "monarch._src.job.pbs.attach_to_workers", return_value=object()
        ) as attach,
    ):
        job.state(cached_path=None)

    assert attach.call_args.kwargs["workers"] == [
        f"{ids[0]}@tcp://host-a:22222",
        f"{ids[1]}@tcp://host-b:22222",
    ]


def test_can_run_compares_launcher_options():
    job = _make_job()
    with patch.object(job, "_jobs_active", return_value=True):
        assert job.can_run(_make_job())
        assert not job.can_run(_make_job(rank_env="PALS_RANKID"))
        assert not job.can_run(_make_job(bind_to="0.0.0.0"))


def test_read_nodefile_short_names_in_order(nodefile):
    assert _pbs_batch.read_nodefile() == ["host-a", "host-b"]


def test_read_nodefile_rejects_a_missing_or_empty_file(tmp_path):
    with pytest.raises(RuntimeError, match="not a readable file"):
        _pbs_batch.read_nodefile(str(tmp_path / "missing"))
    empty = tmp_path / "empty"
    empty.write_text("\n")
    with pytest.raises(RuntimeError, match="lists no hosts"):
        _pbs_batch.read_nodefile(str(empty))


def test_service_proc_ids_must_cover_every_node():
    job = _make_job()
    with pytest.raises(ValueError, match="need 2 nodes, not 3"):
        job._ensure_service_proc_ids(3)


def test_worker_bootstrap_uses_rank_env_and_drops_launcher_env(monkeypatch):
    ids = allocate_service_proc_ids(4)
    monkeypatch.setenv(SERVICE_PROC_IDS_ENV, serialize_service_proc_ids(ids))
    monkeypatch.setenv("PALS_RANKID", "3")
    monkeypatch.setenv("PMI_RANK", "3")

    with (
        patch("socket.gethostname", return_value="worker-a.cluster.org"),
        patch("monarch.actor.run_worker_loop_forever") as run_worker,
    ):
        exec(_pbs_batch._worker_bootstrap(22222, "0.0.0.0", "PALS_RANKID"), {})
        import os

        leaked = [k for k in os.environ if k.startswith(("PMI_", "PALS_"))]

    run_worker.assert_called_once_with(
        address=f"{ids[3]}@tcp://worker-a:22222@tcp://0.0.0.0:22222",
        ca="trust_all_connections",
    )
    assert leaked == []


def test_runner_seeds_workers_with_mpiexec_then_runs_the_client(nodefile, monkeypatch):
    monkeypatch.delenv(SERVICE_PROC_IDS_ENV, raising=False)
    calls = {}

    class FakeWorkers:
        def __init__(self, cmd, env):
            calls["workers"] = (cmd, env)

        def poll(self):
            return None

        def terminate(self):
            calls["terminated"] = True

        def wait(self, timeout=None):
            return 0

    def fake_run(cmd, env):
        calls["client"] = (cmd, env)
        return subprocess.CompletedProcess(args=cmd, returncode=3)

    with (
        patch("monarch._src.job._pbs_batch.subprocess.Popen", FakeWorkers),
        patch("monarch._src.job._pbs_batch.subprocess.run", side_effect=fake_run),
        pytest.raises(SystemExit) as exit_info,
    ):
        _pbs_batch.main(
            ["--mpiexec", "mpx", "--mpiexec-arg=--cpu-bind=none", "python -m my.train"]
        )

    assert exit_info.value.code == 3
    cmd, worker_env = calls["workers"]
    assert cmd[:8] == [
        "mpx",
        "-n",
        "2",
        "--ppn",
        "1",
        "--hosts",
        "host-a,host-b",
        "--cpu-bind=none",
    ]
    # a hand-written script has no ids yet: the runner allocates one per host
    assert len(deserialize_service_proc_ids(worker_env[SERVICE_PROC_IDS_ENV])) == 2
    client_cmd, client_env = calls["client"]
    assert client_cmd == ["python", "-m", "my.train"]
    assert client_env["MONARCH_BATCH_JOB"] == "1"
    assert client_env[SERVICE_PROC_IDS_ENV] == worker_env[SERVICE_PROC_IDS_ENV]
    assert calls["terminated"]


def test_worker_bootstrap_stops_when_the_rank_variable_is_missing(monkeypatch):
    monkeypatch.delenv("PALS_RANKID", raising=False)
    source = _pbs_batch._worker_bootstrap(22222, None, "PALS_RANKID")
    with pytest.raises(SystemExit, match="PALS_RANKID is not set"):
        exec(source, {})
