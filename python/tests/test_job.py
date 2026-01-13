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

import pytest

# Import directly from _src since job module isn't properly exposed
from monarch._src.job.job import job_load, job_loads, JobState, JobTrait, LocalJob
from monarch.actor import HostMesh


class MockJobTrait(JobTrait):
    """
    Mock implementation of JobTrait for testing purposes.
    """

    def __init__(self, host_names: Sequence[str] = ("default",), compatible_specs=None):
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
