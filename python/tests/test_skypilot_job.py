# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Tests for the SkyPilot job integration."""

import os
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest


# Check if SkyPilot is available
try:
    import sky

    HAS_SKYPILOT = True
except ImportError:
    HAS_SKYPILOT = False

# Check if Monarch bindings are available
try:
    from monarch._rust_bindings.monarch_hyperactor.config import configure

    HAS_MONARCH_BINDINGS = True
except ImportError:
    HAS_MONARCH_BINDINGS = False

# Skip all tests in this module if SkyPilot or Monarch bindings are not installed
pytestmark = [
    pytest.mark.skipif(not HAS_SKYPILOT, reason="SkyPilot not installed"),
    pytest.mark.skipif(not HAS_MONARCH_BINDINGS, reason="Monarch bindings not available"),
]


class MockClusterHandle:
    """Mock CloudVmRayResourceHandle for testing."""

    def __init__(
        self,
        cluster_name: str,
        node_ips: List[Tuple[str, str]],
    ):
        self.cluster_name = cluster_name
        self.cluster_name_on_cloud = cluster_name
        self.stable_internal_external_ips = node_ips
        self.launched_nodes = len(node_ips)


class MockStatusResponse:
    """Mock status response from sky.status()."""

    def __init__(
        self,
        name: str,
        status: "sky.ClusterStatus",
        handle: Optional[MockClusterHandle] = None,
    ):
        self.name = name
        self.status = status
        self.handle = handle


@pytest.fixture
def mock_sky():
    """Fixture to mock SkyPilot SDK functions."""
    with mock.patch("monarch._src.job.skypilot.sky") as mock_sky_module:
        # Mock ClusterStatus enum
        mock_sky_module.ClusterStatus = sky.ClusterStatus

        # Mock sky.launch to return a mock request_id
        mock_sky_module.launch.return_value = "mock-request-id"

        # Mock sky.get to return appropriate results
        def mock_get(request_id):
            if request_id == "mock-request-id":
                # Return (job_id, handle) for launch
                return (
                    1,
                    MockClusterHandle(
                        "test-cluster",
                        [("10.0.0.1", "1.2.3.4"), ("10.0.0.2", "1.2.3.5")],
                    ),
                )
            elif request_id == "mock-status-request-id":
                # Return list of status responses
                return [
                    MockStatusResponse(
                        "test-cluster",
                        sky.ClusterStatus.UP,
                        MockClusterHandle(
                            "test-cluster",
                            [("10.0.0.1", "1.2.3.4"), ("10.0.0.2", "1.2.3.5")],
                        ),
                    )
                ]
            elif request_id == "mock-down-request-id":
                return None
            return None

        mock_sky_module.get.side_effect = mock_get

        # Mock sky.status
        mock_sky_module.status.return_value = "mock-status-request-id"

        # Mock sky.down
        mock_sky_module.down.return_value = "mock-down-request-id"

        # Mock sky.Task
        mock_sky_module.Task = mock.MagicMock()

        # Mock sky.Resources
        mock_sky_module.Resources = sky.Resources

        yield mock_sky_module


@pytest.fixture
def mock_attach_to_workers():
    """Fixture to mock attach_to_workers wrapper."""
    with mock.patch(
        "monarch._src.job.skypilot._attach_to_workers_wrapper"
    ) as mock_attach:
        # Create a simple mock HostMesh
        class MockHostMesh:
            def __init__(self, name):
                self.name = name

        def create_mock_host_mesh(name, ca, workers):
            return MockHostMesh(name)

        mock_attach.side_effect = create_mock_host_mesh
        yield mock_attach


@pytest.fixture
def mock_configure_transport():
    """Fixture to mock _configure_transport."""
    with mock.patch(
        "monarch._src.job.skypilot._configure_transport"
    ) as mock_config:
        yield mock_config


@pytest.mark.skipif(not HAS_SKYPILOT, reason="SkyPilot not installed")
def test_skypilot_job_import():
    """Test that SkyPilotJob can be imported from monarch.job."""
    from monarch.job import SkyPilotJob

    # SkyPilotJob should be available (or None if import failed)
    # This test verifies the export is working
    if HAS_MONARCH_BINDINGS:
        assert SkyPilotJob is not None
    # If bindings are not available, SkyPilotJob will be None (graceful degradation)


def test_skypilot_job_init(mock_configure_transport):
    """Test SkyPilotJob initialization."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(
        meshes={"trainers": 2, "workers": 1},
        cluster_name="test-cluster",
        monarch_port=12345,
    )

    assert job._meshes == {"trainers": 2, "workers": 1}
    assert job._cluster_name == "test-cluster"
    assert job._port == 12345
    assert job._launched_cluster_name is None
    assert job._node_ips == []


def test_skypilot_job_init_with_resources(mock_configure_transport):
    """Test SkyPilotJob initialization with SkyPilot resources."""
    from monarch._src.job.skypilot import SkyPilotJob

    resources = sky.Resources(accelerators="A100:1")

    job = SkyPilotJob(
        meshes={"trainers": 4},
        resources=resources,
        cluster_name="gpu-cluster",
    )

    assert job._resources == resources
    assert job._meshes == {"trainers": 4}


def test_skypilot_job_build_worker_command(mock_configure_transport):
    """Test the worker command generation."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(
        meshes={"trainers": 1},
        monarch_port=22222,
    )

    command = job._build_worker_command()

    # Check that the command contains expected elements
    assert "socket.gethostname()" in command
    assert "tcp://" in command
    assert "22222" in command
    assert "run_worker_loop_forever" in command
    assert 'ca="trust_all_connections"' in command


def test_skypilot_job_create(mock_sky, mock_attach_to_workers, mock_configure_transport):
    """Test the _create method."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(
        meshes={"trainers": 2},
        cluster_name="test-cluster",
    )

    # Call _create
    job._create(None)

    # Verify sky.launch was called
    mock_sky.launch.assert_called_once()

    # Check that cluster name was stored
    assert job._launched_cluster_name == "test-cluster"


def test_skypilot_job_create_batch_mode_raises(mock_sky, mock_configure_transport):
    """Test that _create raises an error for batch mode."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(meshes={"trainers": 1})

    with pytest.raises(RuntimeError, match="batch-mode scripts"):
        job._create("some_script.py")


def test_skypilot_job_state(mock_sky, mock_attach_to_workers, mock_configure_transport):
    """Test the _state method."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(
        meshes={"trainers": 2},
        cluster_name="test-cluster",
    )

    # Apply the job first
    job.apply()

    # Now get state
    state = job._state()

    # Verify attach_to_workers was called with correct addresses
    mock_attach_to_workers.assert_called()
    call_args = mock_attach_to_workers.call_args

    # Check the call arguments
    assert call_args.kwargs["name"] == "trainers"
    assert call_args.kwargs["ca"] == "trust_all_connections"
    # Workers should use external IPs
    workers = call_args.kwargs["workers"]
    assert len(workers) == 2
    assert all("tcp://" in w for w in workers)

    # Check that state has the trainers mesh
    assert hasattr(state, "trainers")


def test_skypilot_job_state_multiple_meshes(mock_sky, mock_attach_to_workers, mock_configure_transport):
    """Test _state with multiple meshes."""
    from monarch._src.job.skypilot import SkyPilotJob

    # Create mock status with 3 nodes
    def mock_get_multi(request_id):
        if request_id == "mock-request-id":
            return (
                1,
                MockClusterHandle(
                    "test-cluster",
                    [
                        ("10.0.0.1", "1.2.3.4"),
                        ("10.0.0.2", "1.2.3.5"),
                        ("10.0.0.3", "1.2.3.6"),
                    ],
                ),
            )
        elif request_id == "mock-status-request-id":
            return [
                MockStatusResponse(
                    "test-cluster",
                    sky.ClusterStatus.UP,
                    MockClusterHandle(
                        "test-cluster",
                        [
                            ("10.0.0.1", "1.2.3.4"),
                            ("10.0.0.2", "1.2.3.5"),
                            ("10.0.0.3", "1.2.3.6"),
                        ],
                    ),
                )
            ]
        return None

    mock_sky.get.side_effect = mock_get_multi

    job = SkyPilotJob(
        meshes={"trainers": 2, "evaluator": 1},
        cluster_name="test-cluster",
    )

    job.apply()
    state = job._state()

    # Verify attach_to_workers was called twice (once for each mesh)
    assert mock_attach_to_workers.call_count == 2

    # Check that state has both meshes
    assert hasattr(state, "trainers")
    assert hasattr(state, "evaluator")


def test_skypilot_job_kill(mock_sky, mock_attach_to_workers, mock_configure_transport):
    """Test the _kill method."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(
        meshes={"trainers": 1},
        cluster_name="test-cluster",
    )

    # Apply the job first
    job.apply()
    assert job._launched_cluster_name == "test-cluster"

    # Kill the job
    job._kill()

    # Verify sky.down was called
    mock_sky.down.assert_called_once_with("test-cluster")

    # Check that state was cleared
    assert job._launched_cluster_name is None
    assert job._node_ips == []


def test_skypilot_job_can_run(mock_sky, mock_attach_to_workers, mock_configure_transport):
    """Test the can_run method."""
    from monarch._src.job.skypilot import SkyPilotJob

    job1 = SkyPilotJob(
        meshes={"trainers": 2},
        cluster_name="test-cluster",
        monarch_port=22222,
    )

    job2 = SkyPilotJob(
        meshes={"trainers": 2},
        cluster_name="test-cluster",
        monarch_port=22222,
    )

    job3 = SkyPilotJob(
        meshes={"trainers": 4},  # Different mesh config
        cluster_name="test-cluster",
        monarch_port=22222,
    )

    # Apply job1
    job1.apply()

    # job1 should be able to run job2 (same config)
    assert job1.can_run(job2) is True

    # job1 should NOT be able to run job3 (different mesh config)
    assert job1.can_run(job3) is False


def test_skypilot_job_jobs_active(mock_sky, mock_attach_to_workers, mock_configure_transport):
    """Test the _jobs_active method."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(
        meshes={"trainers": 1},
        cluster_name="test-cluster",
    )

    # Before apply, should not be active
    assert job._jobs_active() is False

    # Apply the job
    job.apply()

    # After apply, should be active (mocked status returns UP)
    assert job._jobs_active() is True


def test_skypilot_job_serialization(mock_sky, mock_attach_to_workers, mock_configure_transport):
    """Test that SkyPilotJob can be serialized and deserialized."""
    from monarch._src.job.skypilot import SkyPilotJob
    from monarch._src.job.job import job_loads

    job = SkyPilotJob(
        meshes={"trainers": 2, "workers": 1},
        cluster_name="test-cluster",
        monarch_port=33333,
    )

    # Serialize
    serialized = job.dumps()

    # Deserialize
    loaded_job = job_loads(serialized)

    # Check attributes
    assert isinstance(loaded_job, SkyPilotJob)
    assert loaded_job._meshes == {"trainers": 2, "workers": 1}
    assert loaded_job._cluster_name == "test-cluster"
    assert loaded_job._port == 33333


def test_skypilot_job_with_setup_commands(mock_configure_transport):
    """Test SkyPilotJob with custom setup commands."""
    from monarch._src.job.skypilot import SkyPilotJob

    setup = "pip install torch\npip install monarch"

    job = SkyPilotJob(
        meshes={"trainers": 1},
        setup_commands=setup,
    )

    assert job._setup_commands == setup


def test_skypilot_job_with_autostop(mock_configure_transport):
    """Test SkyPilotJob with autostop configuration."""
    from monarch._src.job.skypilot import SkyPilotJob

    job = SkyPilotJob(
        meshes={"trainers": 1},
        idle_minutes_to_autostop=30,
        down_on_autostop=True,
    )

    assert job._idle_minutes_to_autostop == 30
    assert job._down_on_autostop is True


# Integration test - only run if explicitly requested
@pytest.mark.skip(reason="Integration test - run manually with --run-integration")
def test_skypilot_job_integration():
    """
    Integration test that actually launches a SkyPilot cluster.

    To run this test:
        pytest tests/test_skypilot_job.py::test_skypilot_job_integration --run-integration

    Make sure you have SkyPilot credentials configured.
    """
    from monarch._src.job.skypilot import SkyPilotJob

    # Create a minimal job - just 1 node with cheap resources
    job = SkyPilotJob(
        meshes={"workers": 1},
        resources=sky.Resources(
            cloud=sky.AWS(),  # Change to your preferred cloud
            cpus="2+",
        ),
        cluster_name="monarch-test-integration",
        idle_minutes_to_autostop=5,
        down_on_autostop=True,
    )

    try:
        # Apply the job
        job.apply()

        # Check that we can get state
        state = job.state()
        assert hasattr(state, "workers")

        print("Integration test passed!")
    finally:
        # Always clean up
        job.kill()

