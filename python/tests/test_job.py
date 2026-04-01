# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import os
import subprocess
import sys
import tempfile
from typing import cast, Dict, Optional, Sequence
from unittest.mock import MagicMock, patch

import pytest

# Import directly from _src since job module isn't properly exposed
from monarch._src.job.job import (
    job_load,
    job_loads,
    JobState,
    JobTrait,
    LocalJob,
    MeshAdminConfig,
    TelemetryConfig,
)
from monarch.actor import HostMesh


class MockJobTrait(JobTrait):
    """
    Mock implementation of JobTrait for testing purposes.
    """

    def __init__(
        self,
        host_names: Sequence[str] = ("default",),
        compatible_specs=None,
    ):
        """
        Initialize a mock job trait.

        Args:
            host_names: Names of host meshes to create in the state
            compatible_specs: List of specs this job is compatible with, or None if compatible with all
        """
        super().__init__()
        self._host_names = host_names
        self._compatible_specs = compatible_specs
        # Track mock state for testing
        self.create_called = False
        self.create_args = None
        self.kill_called = False

    def _state(self) -> JobState:
        """Return a mock job state with fake host meshes."""
        # Create a mock host mesh for each host name
        mock_hosts: Dict[str, HostMesh] = {}
        for name in self._host_names:
            # Using a simple object that mimics HostMesh for testing
            class MockHostMesh:
                def __init__(self, name):
                    self.name = name

                def __repr__(self):
                    return f"MockHostMesh({self.name})"

            mock_hosts[name] = cast("HostMesh", MockHostMesh(name))

        return JobState(mock_hosts)

    def _create(self, client_script: Optional[str] = None):
        """Mock implementation that tracks the creation call."""
        self.create_called = True
        self.create_args = client_script

    def can_run(self, spec: "JobTrait") -> bool:
        """
        Check if this mock job can run the given spec.

        If compatible_specs was provided, check if the spec is in the list.
        Otherwise, just return True.
        """
        if self._compatible_specs is None:
            return True
        return spec in self._compatible_specs

    def _kill(self):
        """Mock implementation that tracks the kill call."""
        self.kill_called = True


def test_apply():
    """Test applying a job."""
    job = MockJobTrait()

    # Initial state - create_called should be False
    assert not job.create_called

    # Apply the job
    job.apply()

    # After apply(), create_called should be True
    assert job.create_called

    # Applying again shouldn't call _create again
    job.create_called = False
    job.apply()
    assert not job.create_called


def test_state_after_apply():
    """Test getting state from an already applied job."""
    job = MockJobTrait(host_names=["trainers", "dataloaders"])

    # Apply the job first
    job.apply()

    # Get the state
    state = job.state()

    # Check that the state has the expected host meshes
    assert hasattr(state, "trainers")
    assert hasattr(state, "dataloaders")


def test_state_from_cache():
    """Test loading job state from a compatible cached job."""
    # Create a job that will be saved to the cache
    original_job = MockJobTrait(host_names=["trainers", "evaluators"])
    original_job.apply()

    # Create a temp file for caching
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        cache_path = tmp.name

    try:
        # Save the original job to the cache
        original_job.dump(cache_path)

        # Create a new job that should use the cached job
        new_job = MockJobTrait()

        # Get state using the cache
        state = new_job.state(cached_path=cache_path)

        # Check that state has the host meshes from the cached job
        assert hasattr(state, "trainers")
        assert hasattr(state, "evaluators")

        # Since we used the cached job, _create should not have been called
        assert not new_job.create_called

    finally:
        # Clean up the temp file
        if os.path.exists(cache_path):
            os.unlink(cache_path)


def test_incompatible_cache():
    """Test behavior when cache contains an incompatible job."""
    # Create a job that will be saved to the cache
    # This job is compatible with nothing (empty compatible_specs list)
    cached_job = MockJobTrait(compatible_specs=[])
    cached_job.apply()

    # Create a temp file for caching
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        cache_path = tmp.name

    try:
        # Save the cached job
        cached_job.dump(cache_path)

        # Create a new job that should NOT use the cached job
        new_job = MockJobTrait(host_names=["workers"])

        # Get state using the cache - this should not use the cached job
        state = new_job.state(cached_path=cache_path)

        # The new job should have been applied
        assert new_job.create_called

        # State should have the host meshes from the new job, not the cached one
        assert hasattr(state, "workers")

        # Try accessing the attributes - they shouldn't exist
        try:
            state.trainers
            raise AssertionError("state should not have trainers attribute")
        except (KeyError, AttributeError):
            pass

        try:
            state.evaluators
            raise AssertionError("state should not have evaluators attribute")
        except (KeyError, AttributeError):
            pass

    finally:
        # Clean up the temp file
        if os.path.exists(cache_path):
            os.unlink(cache_path)


def test_state_no_cache():
    """Test getting state when no cache path is provided."""
    job = MockJobTrait()

    # Get state without providing a cache path
    state = job.state(cached_path=None)

    # The job should have been applied
    assert job.create_called

    # State should have the default host mesh
    assert hasattr(state, "default")


def test_dump_load():
    """Test saving a job to a file and loading it back."""
    # Create and apply a job
    original_job = MockJobTrait(host_names=["gpu_nodes"])
    original_job.apply()

    # Create a temp file for saving the job
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        job_path = tmp.name

    try:
        # Save the job to a file
        original_job.dump(job_path)

        # Load the job directly from the file
        loaded_job = job_load(job_path)

        # Get state from the loaded job
        state = loaded_job.state()

        # Check that the state has the expected host mesh
        assert hasattr(state, "gpu_nodes")

    finally:
        # Clean up the temp file
        if os.path.exists(job_path):
            os.unlink(job_path)


def test_dumps_loads():
    """Test serializing and deserializing a job using dumps/loads."""
    original_job = MockJobTrait(host_names=["trainers", "parameter_servers"])
    original_job.apply()

    # Serialize the job to bytes
    serialized = original_job.dumps()

    # Deserialize the job
    loaded_job = job_loads(serialized)

    # Get state from the loaded job
    state = loaded_job.state()

    # Check that the state has the expected host meshes
    assert hasattr(state, "trainers")
    assert hasattr(state, "parameter_servers")


def test_cache_write():
    """Test that job is written to cache when state is called with a cache path."""
    job = MockJobTrait()

    # Create a temp file for caching
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        cache_path = tmp.name
        # Delete it so we can check if it gets created
        os.unlink(cache_path)

    try:
        # Get state with a cache path - this should apply the job and write to cache
        job.state(cached_path=cache_path)

        # Check that the cache file was created
        assert os.path.exists(cache_path)

        # Load the job from the cache
        cached_job = job_load(cache_path)

        # Get state from the loaded job
        cached_state = cached_job.state()

        # Check that it has the expected host mesh
        assert hasattr(cached_state, "default")

    finally:
        # Clean up the temp file
        if os.path.exists(cache_path):
            os.unlink(cache_path)


def test_kill():
    """Test the kill method."""
    job = MockJobTrait()
    job.apply()
    # Kill shouldn't have been called yet
    assert not job.kill_called

    # Call kill
    job.kill()

    # kill_called should now be True
    assert job.kill_called


def test_state_query_engine_none_without_telemetry():
    """Test that query_engine is None when no telemetry is configured."""
    job = MockJobTrait()
    state = job.state(cached_path=None)
    assert state.query_engine is None
    assert state.telemetry_url is None


@patch("monarch._src.job.job.start_telemetry")
def test_state_query_engine_set_with_telemetry(mock_start):
    """Test that query_engine is set when telemetry is configured."""
    mock_engine = MagicMock()
    mock_url = "http://localhost:8265"
    mock_start.return_value = (mock_engine, mock_url)

    job = MockJobTrait().enable_telemetry(TelemetryConfig())
    state = job.state(cached_path=None)

    assert state.query_engine is not None
    assert state.query_engine is mock_engine
    assert state.telemetry_url == mock_url


@patch("monarch._src.job.job.start_telemetry")
def test_telemetry_started_only_once(mock_start):
    """Test that telemetry is not restarted on subsequent state() calls."""
    mock_start.return_value = (MagicMock(), "http://localhost:8265")

    job = MockJobTrait().enable_telemetry(TelemetryConfig())
    job.state(cached_path=None)
    job.state(cached_path=None)

    mock_start.assert_called_once()


@patch("monarch._src.job.job.start_telemetry")
def test_telemetry_dropped_on_pickle(mock_start):
    """Test that query_engine is dropped during pickling and restored after."""
    mock_start.return_value = (MagicMock(), "http://localhost:8265")

    job = MockJobTrait().enable_telemetry(TelemetryConfig())
    job.state(cached_path=None)
    assert mock_start.call_count == 1

    # Serialize and deserialize — query_engine should be dropped
    loaded_job = job_loads(job.dumps())
    assert loaded_job._query_engine is None
    assert loaded_job._telemetry_url is None

    # Getting state again should re-initialize telemetry
    state = loaded_job.state(cached_path=None)
    assert mock_start.call_count == 2
    assert state.query_engine is not None


def test_state_admin_url_none_without_mesh_admin():
    """Test that admin_url is None when no mesh admin is configured."""
    job = MockJobTrait()
    state = job.state(cached_path=None)
    assert state.admin_url is None


@patch("monarch._src.job.job._spawn_admin")
def test_state_admin_url_set_with_mesh_admin(mock_spawn):
    """Test that admin_url is available on the first state() call."""
    mock_future = MagicMock()
    mock_future.get.return_value = "http://localhost:1729"
    mock_spawn.return_value = mock_future

    job = MockJobTrait().enable_admin(MeshAdminConfig())
    state = job.state(cached_path=None)

    mock_spawn.assert_called_once()
    assert state.admin_url == "http://localhost:1729"


@patch("monarch._src.job.job._spawn_admin")
def test_mesh_admin_started_only_once(mock_spawn):
    """Test that mesh admin is not restarted on subsequent state() calls."""
    mock_future = MagicMock()
    mock_future.get.return_value = "http://localhost:1729"
    mock_spawn.return_value = mock_future

    job = MockJobTrait().enable_admin(MeshAdminConfig())
    job.state(cached_path=None)
    job.state(cached_path=None)

    mock_spawn.assert_called_once()


@patch("monarch._src.job.job._spawn_admin")
def test_mesh_admin_dropped_on_pickle(mock_spawn):
    """Test that admin_url is dropped during pickling and restored after."""
    mock_future = MagicMock()
    mock_future.get.return_value = "http://localhost:1729"
    mock_spawn.return_value = mock_future

    job = MockJobTrait().enable_admin(MeshAdminConfig())
    job.state(cached_path=None)
    assert mock_spawn.call_count == 1

    # Serialize and deserialize — admin_url should be dropped
    loaded_job = job_loads(job.dumps())
    assert loaded_job._admin_url is None

    # Getting state again should re-spawn admin
    state = loaded_job.state(cached_path=None)
    assert mock_spawn.call_count == 2
    assert state.admin_url is not None


@patch("monarch._src.job.job._spawn_admin")
def test_mesh_admin_receives_custom_addr(mock_spawn):
    """Test that MeshAdminConfig.admin_addr is forwarded to _spawn_admin."""
    mock_future = MagicMock()
    mock_future.get.return_value = "http://myhost:9999"
    mock_spawn.return_value = mock_future

    job = MockJobTrait().enable_admin(MeshAdminConfig(admin_addr="myhost:9999"))
    job.state(cached_path=None)

    _, kwargs = mock_spawn.call_args
    assert kwargs.get("admin_addr") == "myhost:9999"


# Tests for LocalJob implementation


def test_local_job():
    """Test LocalJob initialization and state access."""
    # Create a job
    job = LocalJob(hosts=["trainers", "workers", "parameter_servers"])

    # Apply the job
    job.apply()

    # Get the state
    state = job.state()

    # Check that the state has all the host meshes
    assert hasattr(state, "trainers")
    assert hasattr(state, "workers")
    assert hasattr(state, "parameter_servers")


def test_local_job_default_hosts():
    """Test that LocalJob works with default host names."""
    # Create a job with default hosts
    job = LocalJob()

    # Apply the job
    job.apply()

    # Get the state
    state = job.state()

    # Check that the state has the default "hosts" mesh
    assert hasattr(state, "hosts")


def test_local_job_compatibility():
    """Test the can_run method of LocalJob."""
    # LocalJob.can_run always returns False as per implementation
    job = LocalJob()
    mock_job = MockJobTrait()

    # LocalJob should not be able to run any spec
    assert job.can_run(mock_job) is False


train_script = os.path.join(os.path.dirname(__file__), "job_train.py")


# TODO(https://github.com/meta-pytorch/monarch/issues/2213): Occasional GIL release failure.
@pytest.mark.oss_skip
def test_train_script_job_state_regular():
    """
    Test that the train.py script picks up the default 'hosts' in regular mode.
    """
    # Path to train.py
    if "FB_XAR_INVOKED_NAME" in os.environ:
        pytest.skip("buck")

    with tempfile.TemporaryDirectory():
        # Run train.py directly (no batch mode) in the temp directory
        env = os.environ.copy()
        if "MONARCH_BATCH_JOB" in env:
            del env["MONARCH_BATCH_JOB"]  # Ensure batch mode is off

        # Set the working directory to the temp directory
        # Execute train.py
        result = subprocess.run(
            [sys.executable, train_script],
            env=env,
            capture_output=True,
            text=True,
        )

        # Print any error output for debugging
        if result.returncode != 0:
            print(f"Script failed with stderr: {result.stderr}")
            print(f"Script stdout: {result.stdout}")

        # Check that the script executed successfully
        assert result.returncode == 0

        # Check output for hosts mesh existence
        assert "hosts True" in result.stdout
        # In non-batch mode, batch_launched_hosts shouldn't be available
        assert "batch_launched_hosts False" in result.stdout


# TODO(https://github.com/meta-pytorch/monarch/issues/2213): Occasional GIL release failure.
@pytest.mark.oss_skip
def test_train_script_job_state_batch():
    """
    Test that the train.py script picks up the 'batch_launched_hosts' in batch mode.
    """
    if "FB_XAR_INVOKED_NAME" in os.environ:
        pytest.skip("buck")

    try:
        job = LocalJob(("batch_launched_hosts",))
        job.apply(client_script=train_script)
        status = job.process.wait()
        stdout = open(os.path.join(job._log_dir, "stdout.log"), "r").read()
        stderr = open(os.path.join(job._log_dir, "stderr.log"), "r").read()
        assert status == 0, f"Job failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        assert "batch_launched_hosts True" in stdout
        # look in job._log_dir for the stdout file which will have the batch_lauched_hosts True
    finally:
        # Clean up
        if os.path.exists(".monarch/job_state.pkl"):
            os.unlink(".monarch/job_state.pkl")


# ── BashActor tests ────────────────────────────────────────────────────────


def _bash(script: str) -> dict:
    """Run a script using the same logic as BashActor.run, bypassing the actor framework."""
    import selectors
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=True) as f:
        f.write(script)
        f.flush()
        proc = subprocess.Popen(
            ["bash", f.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout = proc.stdout
        stderr = proc.stderr
        assert stdout is not None
        assert stderr is not None
        stdout_lines: list = []
        stderr_lines: list = []
        sel = selectors.DefaultSelector()
        sel.register(stdout, selectors.EVENT_READ, "stdout")
        sel.register(stderr, selectors.EVENT_READ, "stderr")
        while sel.get_map():
            for key, _ in sel.select():
                # pyre-ignore[16]: fileobj is IO[str] here, not int
                line = key.fileobj.readline()
                if not line:
                    sel.unregister(key.fileobj)
                    continue
                if key.data == "stdout":
                    stdout_lines.append(line)
                else:
                    stderr_lines.append(line)
        sel.close()
        proc.wait()
    return {
        "returncode": proc.returncode,
        "stdout": "".join(stdout_lines),
        "stderr": "".join(stderr_lines),
    }


def test_bash_actor_stdout():
    result = _bash("#!/bin/bash\necho hello\necho world\n")
    assert result["returncode"] == 0
    assert "hello" in result["stdout"]
    assert "world" in result["stdout"]


def test_bash_actor_stderr():
    result = _bash("#!/bin/bash\necho oops >&2\n")
    assert result["returncode"] == 0
    assert "oops" in result["stderr"]


def test_bash_actor_exit_code():
    result = _bash("#!/bin/bash\nexit 42\n")
    assert result["returncode"] == 42


def test_bash_actor_interleaved():
    result = _bash("#!/bin/bash\necho out1\necho err1 >&2\necho out2\necho err2 >&2\n")
    assert result["returncode"] == 0
    assert "out1" in result["stdout"]
    assert "out2" in result["stdout"]
    assert "err1" in result["stderr"]
    assert "err2" in result["stderr"]


def test_bash_actor_large_output():
    """Large output must not deadlock (pipe buffer overflow)."""
    result = _bash("#!/bin/bash\nfor i in $(seq 1 10000); do echo line$i; done\n")
    assert result["returncode"] == 0
    lines = result["stdout"].strip().split("\n")
    assert len(lines) == 10000
    assert lines[0] == "line1"
    assert lines[-1] == "line10000"


# ── BashActor target_ranks tests ──────────────────────────────────────────


def _bash_with_rank(script: str, my_rank: int, target_ranks: list | None) -> dict:
    """Simulate BashActor.run with target_ranks, bypassing the actor framework."""
    if target_ranks is not None and my_rank not in target_ranks:
        return {"returncode": 0, "stdout": "", "stderr": "", "skipped": True}
    return _bash(script)


def test_bash_actor_target_ranks_skipped():
    """Non-targeted ranks should skip execution."""
    result = _bash_with_rank(
        "#!/bin/bash\necho should-not-run\n", my_rank=1, target_ranks=[0]
    )
    assert result.get("skipped") is True
    assert result["returncode"] == 0
    assert result["stdout"] == ""


def test_bash_actor_target_ranks_executed():
    """Targeted ranks should execute normally."""
    result = _bash_with_rank("#!/bin/bash\necho hello\n", my_rank=0, target_ranks=[0])
    assert result.get("skipped") is None
    assert result["returncode"] == 0
    assert "hello" in result["stdout"]


def test_bash_actor_target_ranks_none_runs_all():
    """When target_ranks is None, all ranks execute."""
    result = _bash_with_rank("#!/bin/bash\necho hello\n", my_rank=5, target_ranks=None)
    assert result.get("skipped") is None
    assert result["returncode"] == 0
    assert "hello" in result["stdout"]


# ── Streaming BashActor tests ─────────────────────────────────────────────


def _bash_streaming(script: str) -> dict:
    """Run a script using the same streaming logic as BashActor.run_streaming,
    bypassing the actor framework.  Simulates Port.send() by appending
    tagged lines to a list.
    """
    import selectors

    messages: list[str] = []

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=True) as f:
        f.write(script)
        f.flush()
        proc = subprocess.Popen(
            ["bash", f.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout = proc.stdout
        stderr = proc.stderr
        assert stdout is not None
        assert stderr is not None
        sel = selectors.DefaultSelector()
        sel.register(stdout, selectors.EVENT_READ, "stdout")
        sel.register(stderr, selectors.EVENT_READ, "stderr")
        while sel.get_map():
            for key, _ in sel.select():
                # pyre-ignore[16]: fileobj is IO[str] here, not int
                line = key.fileobj.readline()
                if not line:
                    sel.unregister(key.fileobj)
                    continue
                tag = "out" if key.data == "stdout" else "err"
                messages.append(f"0:{tag}:{line}")
        sel.close()
        proc.wait()
    messages.append(f"0:rc:{proc.returncode}")

    # Parse messages back into stdout/stderr/returncode
    # Format: "rank:tag:content"
    stdout_parts = []
    stderr_parts = []
    returncode = -1
    for msg in messages:
        if msg.startswith("skip:"):
            continue
        rank_str, tag, content = msg.split(":", 2)
        if tag == "out":
            stdout_parts.append(content)
        elif tag == "err":
            stderr_parts.append(content)
        elif tag == "rc":
            returncode = int(content)

    return {
        "returncode": returncode,
        "stdout": "".join(stdout_parts),
        "stderr": "".join(stderr_parts),
        "messages": messages,
    }


def test_streaming_stdout():
    result = _bash_streaming("#!/bin/bash\necho hello\necho world\n")
    assert result["returncode"] == 0
    assert "hello" in result["stdout"]
    assert "world" in result["stdout"]
    # Verify messages are tagged with rank:tag:content format
    out_msgs = [m for m in result["messages"] if ":out:" in m]
    assert len(out_msgs) == 2
    assert out_msgs[0] == "0:out:hello\n"
    assert out_msgs[1] == "0:out:world\n"
    # Last message is always rank:rc:code
    assert result["messages"][-1] == "0:rc:0"


def test_streaming_stderr():
    result = _bash_streaming("#!/bin/bash\necho oops >&2\n")
    assert result["returncode"] == 0
    assert "oops" in result["stderr"]
    err_msgs = [m for m in result["messages"] if ":err:" in m]
    assert len(err_msgs) == 1
    assert err_msgs[0] == "0:err:oops\n"


def test_streaming_exit_code():
    result = _bash_streaming("#!/bin/bash\nexit 42\n")
    assert result["returncode"] == 42
    assert result["messages"][-1] == "0:rc:42"


def test_streaming_interleaved():
    result = _bash_streaming(
        "#!/bin/bash\necho out1\necho err1 >&2\necho out2\necho err2 >&2\n"
    )
    assert result["returncode"] == 0
    assert "out1" in result["stdout"]
    assert "out2" in result["stdout"]
    assert "err1" in result["stderr"]
    assert "err2" in result["stderr"]
    # Verify we got tagged messages for each line
    out_msgs = [m for m in result["messages"] if ":out:" in m]
    err_msgs = [m for m in result["messages"] if ":err:" in m]
    assert len(out_msgs) == 2
    assert len(err_msgs) == 2


def test_streaming_large_output():
    """Streaming large output must not deadlock."""
    result = _bash_streaming(
        "#!/bin/bash\nfor i in $(seq 1 10000); do echo line$i; done\n"
    )
    assert result["returncode"] == 0
    lines = result["stdout"].strip().split("\n")
    assert len(lines) == 10000
    assert lines[0] == "line1"
    assert lines[-1] == "line10000"
    # Each line should be a separate message
    out_msgs = [m for m in result["messages"] if ":out:" in m]
    assert len(out_msgs) == 10000


def test_streaming_matches_blocking():
    """Streaming mode produces the same stdout/stderr as blocking mode."""
    script = "#!/bin/bash\necho hello\necho oops >&2\necho world\nexit 7\n"
    blocking = _bash(script)
    streaming = _bash_streaming(script)
    assert blocking["returncode"] == streaming["returncode"]
    assert blocking["stdout"] == streaming["stdout"]
    assert blocking["stderr"] == streaming["stderr"]


# ── MASTJob __getstate__ test ──────────────────────────────────────────────


def test_mast_job_get_runner_is_lazy():
    """MASTJob._get_runner uses lazy import to avoid slow top-level loads."""
    import inspect

    try:
        from monarch._src.job.meta.mast import MASTJob
    except ModuleNotFoundError:
        pytest.skip("monarch._src.job.meta not available (OSS build)")

    source = inspect.getsource(MASTJob._get_runner)
    # The import must be inside the method, not at module level.
    assert "from monarch._src.tools.commands import torchx_runner" in source
