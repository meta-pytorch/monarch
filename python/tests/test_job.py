# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import contextlib
import os
import pickle
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
from dataclasses import dataclass
from typing import cast, Dict, Optional, Sequence
from unittest.mock import MagicMock, patch

import monarch._src.job._job_sidecar_worker as js_worker
import monarch._src.job.job_sidecar as js
import pytest
from monarch._src.actor.future import tokio_oracle

# Import directly from _src since job module isn't properly exposed
from monarch._src.job.job import (
    BatchJob,
    exec_command,
    job_load,
    job_loads,
    JobState,
    JobTrait,
    LocalJob,
    MeshAdminConfig,
    ProcessState,
    TelemetryConfig,
)
from monarch._src.job.job_components import JobComponent, JobComponents, MountComponent
from monarch._src.job.mount_config import Mounts
from monarch._src.job.process import ProcessJob
from monarch._src.job.process_guard import _Shutdown, _wait_for_socket
from monarch.actor import Future, HostMesh


def _append_line(path: str, line: str) -> None:
    with open(path, "a") as f:
        f.write(line + "\n")


@dataclass
class _RecordingMountHandle:
    name: str
    log_path: str

    def refresh(self) -> None:
        _append_line(self.log_path, f"refresh:{self.name}")

    def close(self) -> None:
        _append_line(self.log_path, f"close:{self.name}")


@dataclass
class _RecordingMounts:
    name: str
    log_path: str

    def open(self, host_meshes: dict[str, object]) -> _RecordingMountHandle:
        _append_line(self.log_path, f"open:{self.name}:{','.join(host_meshes)}")
        return _RecordingMountHandle(self.name, self.log_path)


def _send_sidecar_request(socket_path: str, message: object) -> object:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(socket_path)
        # @lint-ignore PYTHONPICKLEISBAD
        client.sendall(pickle.dumps(message))
        # @lint-ignore PYTHONPICKLEISBAD
        return pickle.load(client.makefile("rb"))
    finally:
        client.close()


def _send_sidecar_shutdown(socket_path: str) -> None:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(socket_path)
        # @lint-ignore PYTHONPICKLEISBAD
        client.sendall(pickle.dumps(_Shutdown()))
    finally:
        client.close()


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
        mock_hosts: Dict[str, HostMesh] = {}

        class MockHostMesh:
            def __init__(self, name, python_executable=None):
                self.name = name
                self.python_executable = python_executable

            def __repr__(self):
                return f"MockHostMesh({self.name})"

            def with_python_executable(self, python_executable):
                return MockHostMesh(self.name, python_executable)

        for name in self._host_names:
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


def test_spawn_module_uses_module_command_outside_par():
    with (
        patch.object(js, "_IN_PAR", False),
        patch("monarch._src.job.process_guard.ProcessGuard.create") as create,
    ):
        js.spawn_module("lock", "key", "monarch.fake_module")

    lock_path, config_key, command = create.call_args.args
    assert lock_path == "lock"
    assert config_key == "key"
    assert command == [sys.executable, "-m", "monarch.fake_module"]
    assert create.call_args.kwargs == {"env": None}


def test_spawn_module_passes_transport_arg_outside_par():
    with (
        patch.object(js, "_IN_PAR", False),
        patch("monarch._src.job.process_guard.ProcessGuard.create") as create,
    ):
        js.spawn_module(
            "lock",
            "key",
            "monarch.fake_module",
            runtime_transport="metatls",
        )

    lock_path, config_key, command = create.call_args.args
    assert lock_path == "lock"
    assert config_key == "key"
    assert command == [
        sys.executable,
        "-m",
        "monarch.fake_module",
        "--runtime-transport",
        "metatls",
    ]
    assert create.call_args.kwargs == {"env": None}


def test_spawn_module_reenters_parent_binary_inside_par():
    with (
        patch.object(js, "_IN_PAR", True),
        patch.object(sys, "argv", ["/tmp/parent.par"]),
        patch("monarch._src.job.process_guard.ProcessGuard.create") as create,
    ):
        js.spawn_module("lock", "key", "monarch.fake_module")

    lock_path, config_key, command = create.call_args.args
    assert lock_path == "lock"
    assert config_key == "key"
    assert command == ["/tmp/parent.par"]
    assert create.call_args.kwargs == {
        "env": {"PAR_MAIN_OVERRIDE": "monarch.fake_module"}
    }


def test_create_job_sidecar_spawns_job_sidecar_worker_module():
    with (
        patch.object(js, "sidecar_transport_from_runtime", return_value="metatls"),
        patch.object(js, "spawn_module") as spawn_module,
    ):
        js.create_job_sidecar("apply_id")

    spawn_module.assert_called_once_with(
        js.job_sidecar_lock_path("apply_id"),
        "apply_id",
        "monarch._src.job._job_sidecar_worker",
        runtime_transport="metatls",
    )


def test_mounts_ensure_open_clears_existing_sidecar_when_empty():
    """Empty mount config should clear stale mount state on an existing sidecar."""
    guard = MagicMock()
    with patch(
        "monarch._src.job.mount_config.find_job_sidecar", return_value=guard
    ) as find_sidecar:
        Mounts().ensure_open("apply_id", {})

    find_sidecar.assert_called_once_with("apply_id")
    request = guard.send.call_args.args[0]
    assert isinstance(request, js.ClearMountsRequest)
    guard.send.return_value.get.assert_called_once_with()


def test_mounts_ensure_open_does_not_create_sidecar_when_empty():
    """Empty mount config should not start a sidecar just to clear no state."""
    with (
        patch(
            "monarch._src.job.mount_config.find_job_sidecar", return_value=None
        ) as find_sidecar,
        patch("monarch._src.job.mount_config.create_job_sidecar") as create_sidecar,
    ):
        Mounts().ensure_open("apply_id", {})

    find_sidecar.assert_called_once_with("apply_id")
    create_sidecar.assert_not_called()


def test_mounts_ensure_open_sends_mounts_request():
    mounts = Mounts()
    mounts.remote_mount("/tmp/source")
    guard = MagicMock()

    with patch(
        "monarch._src.job.mount_config.create_job_sidecar",
        return_value=guard,
    ) as create_sidecar:
        mounts.ensure_open("apply_id", {})

    create_sidecar.assert_called_once_with("apply_id")
    request = guard.send.call_args.args[0]
    assert isinstance(request, js.MountsRequest)
    guard.send.return_value.get.assert_called_once_with()


def test_job_sidecar_worker_passes_transport_arg_to_server():
    with (
        patch.object(
            sys,
            "argv",
            [
                "worker",
                "--runtime-transport",
                "metatls",
                "/tmp/socket",
                "123",
            ],
        ),
        patch("monarch._src.job.job_sidecar._run_job_sidecar") as run_sidecar,
    ):
        js_worker.main()

    run_sidecar.assert_called_once_with("/tmp/socket", runtime_transport="metatls")


def test_run_job_sidecar_manages_mount_lifecycle():
    """Mount requests open once, refresh unchanged config, replace drift, and
    clear stale state."""
    with tempfile.TemporaryDirectory(prefix="monarch_sidecar_", dir="/tmp") as tempdir:
        socket_path = os.path.join(tempdir, "cmd.sock")
        log_path = os.path.join(tempdir, "events.log")
        thread = threading.Thread(
            target=js._run_job_sidecar,
            args=(socket_path,),
            daemon=True,
        )

        with patch("signal.signal"):
            thread.start()
            _wait_for_socket(socket_path, timeout=10.0)

        try:
            host_meshes = {"job": "mesh"}
            assert (
                _send_sidecar_request(
                    socket_path,
                    js.MountsRequest(_RecordingMounts("one", log_path), host_meshes),
                )
                == "ok"
            )
            assert (
                _send_sidecar_request(
                    socket_path,
                    js.MountsRequest(_RecordingMounts("one", log_path), host_meshes),
                )
                == "ok"
            )
            assert (
                _send_sidecar_request(
                    socket_path,
                    js.MountsRequest(_RecordingMounts("two", log_path), host_meshes),
                )
                == "ok"
            )
            assert _send_sidecar_request(socket_path, js.ClearMountsRequest()) == "ok"
            assert _send_sidecar_request(socket_path, js.ClearMountsRequest()) == "ok"
        finally:
            try:
                _send_sidecar_shutdown(socket_path)
            except OSError:
                pass
            thread.join(timeout=10.0)

        assert not thread.is_alive(), "job sidecar did not exit on shutdown"
        with open(log_path) as f:
            assert f.read().splitlines() == [
                "open:one:job",
                "refresh:one",
                "close:one",
                "open:two:job",
                "close:two",
            ]


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
    apply_id = job.apply_id
    assert apply_id is not None
    # Kill shouldn't have been called yet
    assert not job.kill_called

    with patch("monarch._src.job.job.stop_job_sidecar") as stop_sidecar:
        job.kill()

    # kill_called should now be True
    assert job.kill_called
    stop_sidecar.assert_called_once_with(apply_id)


def test_process_job_kill_reaps_worker_session():
    """_kill reaps the worker's whole session, including procs the worker spawns
    into their own process groups -- which killpg of the worker alone orphans."""
    if not os.path.isdir("/proc"):
        pytest.skip("session reap requires /proc")

    def session_members(sid):
        alive = []
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            pid = int(entry)
            try:
                if os.getsid(pid) != sid:
                    continue
                with open(f"/proc/{pid}/stat") as stat:
                    state = stat.read().rsplit(")", 1)[1].split()[0]
            except OSError:
                continue
            if state not in ("Z", "X", "x"):
                alive.append(pid)
        return alive

    # A worker mimicking ProcessJob's: its own session (start_new_session=True)
    # with children in their own process groups, all ignoring SIGTERM so _kill
    # must escalate to SIGKILL to reap them.
    worker_src = (
        "import os, signal, time\n"
        "for _ in range(3):\n"
        "    if os.fork() == 0:\n"
        "        os.setpgid(0, 0)\n"
        "        signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "        while True: time.sleep(0.5)\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "while True: time.sleep(0.5)\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", worker_src], start_new_session=True)
    worker = proc.pid
    tmpdir = None

    try:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and len(session_members(worker)) < 4:
            time.sleep(0.05)
        assert len(session_members(worker)) == 4  # worker + 3 children

        tmpdir = tempfile.mkdtemp(prefix="test_process_job_kill_")
        job = ProcessJob({"hosts": 1})
        job._host_to_pid = {"h_0": ProcessState(worker, f"ipc://{tmpdir}/h_0")}
        job._tmpdir = tmpdir

        job._kill()

        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and session_members(worker):
            time.sleep(0.05)
        assert session_members(worker) == []
        assert not os.path.isdir(tmpdir)
    finally:
        for pid in session_members(worker):
            with contextlib.suppress(OSError):
                os.kill(pid, signal.SIGKILL)
        with contextlib.suppress(OSError, subprocess.TimeoutExpired):
            proc.wait(timeout=2)
        if tmpdir is not None:
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_state_query_engine_none_without_telemetry():
    """Test that query_engine is None when no telemetry is configured."""
    job = MockJobTrait()
    state = job.state(cached_path=None)
    assert state.query_engine is None
    assert state.telemetry_url is None


def test_state_query_client_set_with_telemetry():
    """Test that telemetry configures the sidecar and mesh admin."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    assert state.query_engine is None
    assert state.query_engine_client is m.query_engine_client_cls.return_value
    assert state.telemetry_url == "http://sidecar"
    assert state.admin_url == "http://localhost:1729"
    assert job._components.snapshot is not None
    _, kwargs = m.spawn_admin.call_args
    assert kwargs["admin_addr"] is None
    m.start_snapshots.assert_called_once_with(
        base_url="http://sidecar",
        admin_ref=m.admin_ref,
        instance=m.snapshot_instance,
        interval_secs=30.0,
    )


def test_component_configuration_after_apply_adds_telemetry():
    with _patched_sidecar() as m:
        job = MockJobTrait()
        job.apply()
        components = job._components
        assert components is not None
        assert components.telemetry is None

        job.enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    assert job._components is components
    assert components.telemetry is not None
    assert state.query_engine is None
    assert state.query_engine_client is m.query_engine_client_cls.return_value
    assert state.telemetry_url == "http://sidecar"


def test_mount_configuration_after_apply_reuses_mount_component_on_connect():
    job = MockJobTrait()
    job.apply()
    components = job._components
    assert components is not None
    mount_component = components.mounts

    job.remote_mount("/source", python_exe=None)
    job.gather_mount("/worker/path", "/local/path")
    with patch("monarch._src.job.mount_config.Mounts.ensure_open") as ensure_open:
        job.state(cached_path=None)

    assert job._components is components
    assert components.mounts is mount_component
    ensure_open.assert_called_once()


def test_telemetry_config_change_restarts_telemetry_runtime():
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig(retention_secs=1))
        job.state(cached_path=None)
        components = job._components
        assert components is not None
        mount_component = components.mounts
        telemetry_component = components.telemetry
        assert telemetry_component is not None

        job.enable_telemetry(TelemetryConfig(retention_secs=2))
        job.state(cached_path=None)

    assert job._components is components
    assert components.mounts is mount_component
    assert components.telemetry is telemetry_component
    assert m.query_engine_client_cls.call_count == 2


def test_cached_running_job_uses_current_component_configuration():
    cached_job = MockJobTrait(host_names=["cached"]).enable_telemetry(TelemetryConfig())
    cached_job.apply()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        cache_path = tmp.name

    try:
        cached_job.dump(cache_path)

        with _patched_sidecar() as m:
            new_job = MockJobTrait(host_names=["new"]).enable_telemetry(
                TelemetryConfig(retention_secs=2)
            )
            state = new_job.state(cached_path=cache_path)

        assert not new_job.create_called
        assert state.cached.name == "cached"
        assert state.query_engine is None
        assert state.query_engine_client is m.query_engine_client_cls.return_value
        m.telemetry_cls.assert_any_call(TelemetryConfig(retention_secs=2))
    finally:
        if os.path.exists(cache_path):
            os.unlink(cache_path)


def test_unpickled_running_job_accepts_component_configuration():
    original_job = MockJobTrait()
    original_job.apply()
    loaded_job = job_loads(original_job.dumps())

    with _patched_sidecar() as m:
        loaded_job.enable_telemetry(TelemetryConfig())
        state = loaded_job.state(cached_path=None)

    assert state.query_engine is None
    assert state.query_engine_client is m.query_engine_client_cls.return_value
    assert state.telemetry_url == "http://sidecar"


def test_lifecycle_component_hooks_use_job_context():
    class FinalHost:
        name = "final"

    class ProbeComponent(JobComponent):
        def __init__(self):
            self.jobs = []
            self.events = []

        def before_connect(self, job: JobTrait) -> None:
            self.jobs.append(job)
            self.events.append(
                (
                    "before_connect",
                    job.apply_id is not None,
                    job._running is job,
                )
            )

        def connect(
            self, job: JobTrait, host_meshes: Dict[str, HostMesh]
        ) -> Dict[str, HostMesh]:
            self.jobs.append(job)
            self.events.append(
                (
                    "connect",
                    job.apply_id is not None,
                    job._running is job,
                    tuple(sorted(host_meshes)),
                )
            )
            return {"final": cast(HostMesh, FinalHost())}

        def state(self, job: JobTrait, job_state: JobState) -> None:
            self.jobs.append(job)
            self.events.append(
                (
                    "state",
                    job.apply_id is not None,
                    job._running is job,
                    tuple(sorted(job_state._hosts)),
                    job_state.final is job_state._hosts["final"],
                )
            )

        def reset_runtime(self) -> None:
            self.events.append(("reset_runtime",))

    probe = ProbeComponent()
    job = MockJobTrait(host_names=("raw",))
    job._components = JobComponents(cast(MountComponent, probe))
    state = job.state(cached_path=None)

    assert state.final.name == "final"
    assert all(seen_job is job for seen_job in probe.jobs)
    assert probe.events == [
        ("before_connect", True, True),
        ("connect", True, True, ("raw",)),
        ("state", True, True, ("final",), True),
    ]

    state = job.state(cached_path=None)

    assert state.final.name == "final"
    assert probe.events == [
        ("before_connect", True, True),
        ("connect", True, True, ("raw",)),
        ("state", True, True, ("final",), True),
        ("before_connect", True, True),
        ("connect", True, True, ("raw",)),
        ("state", True, True, ("final",), True),
    ]

    with patch("monarch._src.job.job.stop_job_sidecar"):
        job.kill()

    assert probe.events[-1] == ("reset_runtime",)


def test_batch_job_runs_component_lifecycle_on_wrapped_job():
    class ProbeComponent(JobComponent):
        def __init__(self):
            self.jobs = []
            self.events = []

        def before_connect(self, job: JobTrait) -> None:
            self.jobs.append(job)
            self.events.append("before_connect")

        def connect(
            self, job: JobTrait, host_meshes: Dict[str, HostMesh]
        ) -> Dict[str, HostMesh]:
            self.jobs.append(job)
            self.events.append("connect")
            return host_meshes

        def state(self, job: JobTrait, job_state: JobState) -> None:
            self.jobs.append(job)
            self.events.append("state")

    probe = ProbeComponent()
    job = MockJobTrait(host_names=("raw",))
    job._components = JobComponents(cast(MountComponent, probe))
    batch = BatchJob(job)

    state = batch.state(cached_path=None)

    assert state.raw.name == "raw"
    assert all(seen_job is batch for seen_job in probe.jobs)
    assert batch.apply_id is not None
    assert probe.events == ["before_connect", "connect", "state"]


def test_lifecycle_component_runtime_reset_on_job_teardown():
    """Job teardown resets component runtime while preserving config."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig())
        job.state(cached_path=None)
        components = job._components
        apply_id = job.apply_id

        job.state(cached_path=None)
        assert job._components is components
        assert m.query_engine_client_cls.call_count == 1

        with patch("monarch._src.job.job.stop_job_sidecar") as stop_sidecar:
            job.kill()

    stop_sidecar.assert_called_once_with(apply_id)
    assert job._components is components

    with _patched_sidecar() as m:
        job.state(cached_path=None)
    assert job._components is components
    assert m.query_engine_client_cls.call_count == 1


def test_telemetry_dropped_on_pickle():
    """Test that query client is dropped during pickling and restored after."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig())
        job.state(cached_path=None)
        assert m.query_engine_client_cls.call_count == 1

    # Serialize and deserialize — live handles should be dropped
    loaded_job = job_loads(job.dumps())
    assert loaded_job._components.telemetry is not None

    # Getting state again should re-initialize telemetry
    with _patched_sidecar() as m:
        state = loaded_job.state(cached_path=None)
    assert m.query_engine_client_cls.call_count == 1
    assert state.query_engine_client is not None


def test_state_admin_url_none_without_mesh_admin():
    """Test that admin_url is None when no mesh admin is configured."""
    job = MockJobTrait()
    state = job.state(cached_path=None)
    assert state.admin_url is None


def test_state_admin_url_set_with_telemetry():
    """Test that admin_url is available on the first state() call."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    m.spawn_admin.assert_called_once()
    assert state.admin_url == "http://localhost:1729"


def test_mesh_admin_started_only_once():
    """Test that mesh admin is not restarted on subsequent state() calls."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig())
        job.state(cached_path=None)
        job.state(cached_path=None)

    m.spawn_admin.assert_called_once()


def test_mesh_admin_dropped_on_pickle():
    """Test that admin_url is dropped during pickling and restored after."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig())
        job.state(cached_path=None)
        assert m.spawn_admin.call_count == 1

        # Serialize and deserialize — live handles should be dropped
        loaded_job = job_loads(job.dumps())
        assert loaded_job._components.admin is not None

        # Getting state again should re-spawn admin
        state = loaded_job.state(cached_path=None)

    assert m.spawn_admin.call_count == 2
    assert state.admin_url is not None


def test_mesh_admin_receives_custom_addr():
    """Test that MeshAdminConfig.admin_addr is forwarded to _spawn_admin."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(
            TelemetryConfig(),
            mesh_admin_config=MeshAdminConfig(admin_addr="myhost:9999"),
        )
        job.state(cached_path=None)

    _, kwargs = m.spawn_admin.call_args
    assert kwargs.get("admin_addr") == "myhost:9999"


def test_enable_admin_remains_independent_of_telemetry():
    config = MeshAdminConfig(admin_addr="myhost:9999")
    job = MockJobTrait()

    result = job.enable_admin(config)

    assert result is job
    assert job._components.telemetry is None
    assert job._components.snapshot is None
    assert job._components.admin is not None
    assert job._components.admin._config is config


def test_enable_telemetry_reuses_existing_mesh_admin():
    config = MeshAdminConfig(admin_addr="myhost:9999")
    job = MockJobTrait().enable_admin(config)
    admin = job._components.admin

    job.enable_telemetry(TelemetryConfig())

    assert admin is not None
    assert job._components.admin is admin
    assert admin._config is config
    assert admin._telemetry is job._components.telemetry
    assert job._components.snapshot is not None
    assert job._components.snapshot._admin is admin
    assert job._components.snapshot._telemetry is job._components.telemetry


def test_mesh_admin_receives_telemetry_url():
    """Test that admin links to telemetry when both are configured."""
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    _, kwargs = m.spawn_admin.call_args
    assert kwargs.get("telemetry_url") == "http://sidecar"
    assert state.telemetry_url == "http://sidecar"
    assert state.admin_url == "http://localhost:1729"


def test_mesh_admin_restarts_when_telemetry_config_changes():
    mock_future = MagicMock()
    mock_admin_ref = MagicMock()
    mock_future.get.side_effect = [
        ("http://admin-one", mock_admin_ref),
        ("http://admin-two", mock_admin_ref),
    ]
    telemetry_one = {
        "telemetry_url": "http://telemetry-one",
        "dashboard_url": "http://dashboard-one",
        "socket_path": "/tmp/telemetry-one.sock",
    }
    telemetry_two = {
        "telemetry_url": "http://telemetry-two",
        "dashboard_url": "http://dashboard-two",
        "socket_path": "/tmp/telemetry-two.sock",
    }

    with _patched_sidecar(
        ensure_open_side_effect=[
            telemetry_one,
            telemetry_one,
            telemetry_two,
            telemetry_two,
        ]
    ) as m:
        m.spawn_admin.return_value = mock_future
        job = MockJobTrait().enable_telemetry(TelemetryConfig(retention_secs=1))
        state = job.state(cached_path=None)
        assert state.telemetry_url == "http://telemetry-one"
        assert state.admin_url == "http://admin-one"

        job.enable_telemetry(TelemetryConfig(retention_secs=2))
        state = job.state(cached_path=None)

    assert state.telemetry_url == "http://telemetry-two"
    assert state.admin_url == "http://admin-two"
    assert m.spawn_admin.call_count == 2
    assert [call.kwargs["telemetry_url"] for call in m.spawn_admin.call_args_list] == [
        "http://telemetry-one",
        "http://telemetry-two",
    ]


def test_snapshot_component_starts_once_after_telemetry_and_admin():
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(
            TelemetryConfig(snapshot_interval_secs=5.0)
        )
        job.state(cached_path=None)
        job.state(cached_path=None)

    m.start_snapshots.assert_called_once_with(
        base_url="http://sidecar",
        admin_ref=m.admin_ref,
        instance=m.snapshot_instance,
        interval_secs=5.0,
    )


def test_snapshot_component_can_be_disabled():
    with _patched_sidecar() as m:
        job = MockJobTrait().enable_telemetry(
            TelemetryConfig(include_dashboard=True, snapshot_interval_secs=0)
        )
        job.state(cached_path=None)

    m.start_snapshots.assert_not_called()


def test_batch_job_shares_component_runtime_with_wrapped_job():
    with _patched_sidecar() as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        batch = BatchJob(job)
        batch_state = batch.state(cached_path=None)
        job_state = job.state(cached_path=None)

    assert batch_state.query_engine is None
    assert batch_state.query_engine_client is m.query_engine_client_cls.return_value
    assert batch_state.telemetry_url == "http://sidecar"
    assert batch_state.admin_url == "http://localhost:1729"
    assert job_state.query_engine is None
    assert job_state.query_engine_client is m.query_engine_client_cls.return_value
    assert job_state.telemetry_url == "http://sidecar"
    assert job_state.admin_url == "http://localhost:1729"
    m.spawn_admin.assert_called_once()


@contextlib.contextmanager
def _patched_sidecar(ensure_open_side_effect=None, ensure_open_return=None):
    with (
        patch("monarch._src.job.job_components.Telemetry") as telemetry_cls,
        patch(
            "monarch._src.job.job_components.install_sidecar_socket_sink"
        ) as install_sink,
        patch("monarch._src.job.job_components.QueryEngineClient") as qec_cls,
        patch("monarch._src.job.job_components._spawn_admin") as spawn_admin,
        patch("monarch.actor.context") as actor_context,
        patch(
            "monarch._rust_bindings.monarch_extension.snapshot_integration._start_periodic_snapshots_http"
        ) as start_snapshots,
    ):
        ensure_open = telemetry_cls.return_value.ensure_open
        if ensure_open_side_effect is not None:
            ensure_open.side_effect = ensure_open_side_effect
        else:
            ensure_open.return_value = ensure_open_return or {
                "telemetry_url": "http://sidecar",
                "dashboard_url": "http://dashboard",
                "socket_path": "/tmp/telemetry.sock",
            }
        admin_ref = MagicMock()
        admin_future = MagicMock()
        admin_future.get.return_value = ("http://localhost:1729", admin_ref)
        spawn_admin.return_value = admin_future
        snapshot_instance = MagicMock()
        actor_context.return_value.actor_instance._as_rust.return_value = (
            snapshot_instance
        )
        yield types.SimpleNamespace(
            ensure_open=ensure_open,
            install_sink=install_sink,
            query_engine_client_cls=qec_cls,
            telemetry_cls=telemetry_cls,
            spawn_admin=spawn_admin,
            admin_ref=admin_ref,
            start_snapshots=start_snapshots,
            snapshot_instance=snapshot_instance,
        )


def test_mesh_admin_receives_sidecar_telemetry_url():
    """Admin links to sidecar telemetry when the sidecar path is configured."""
    with _patched_sidecar() as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    _, kwargs = m.spawn_admin.call_args
    assert kwargs.get("telemetry_url") == "http://sidecar"
    assert state.telemetry_url == "http://sidecar"
    assert state.admin_url == "http://localhost:1729"


def test_telemetry_uses_sidecar():
    """Telemetry exposes the sidecar query client."""
    with _patched_sidecar() as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    assert state.query_engine is None
    assert state.query_engine_client is m.query_engine_client_cls.return_value
    assert state.telemetry_url == "http://sidecar"
    assert state.dashboard_url == "http://dashboard"
    m.query_engine_client_cls.assert_called_once_with("http://sidecar")
    m.install_sink.assert_called_once_with("/tmp/telemetry.sock")


def test_sidecar_bootstrap_then_fanout_carry_host_meshes():
    """state() bootstraps the sidecar with empty host_meshes, then fans out
    workers with the materialized host meshes."""
    with _patched_sidecar() as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        job.state(cached_path=None)

    assert m.ensure_open.call_count == 2
    bootstrap_kwargs = m.ensure_open.call_args_list[0].kwargs
    fanout_kwargs = m.ensure_open.call_args_list[1].kwargs
    assert bootstrap_kwargs["host_meshes"] == {}
    assert "spawn_worker_collectors" not in bootstrap_kwargs
    assert set(fanout_kwargs["host_meshes"].keys()) == {"hosts"}
    assert fanout_kwargs["spawn_worker_collectors"] is True


def test_local_job_sidecar_skips_worker_collector_actors():
    """Local sidecar telemetry keeps fan-out procs without collector actors."""
    with _patched_sidecar() as m:
        job = LocalJob(hosts=["hosts"]).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    assert m.ensure_open.call_count == 2
    bootstrap_kwargs = m.ensure_open.call_args_list[0].kwargs
    fanout_kwargs = m.ensure_open.call_args_list[1].kwargs
    assert bootstrap_kwargs["host_meshes"] == {}
    assert "spawn_worker_collectors" not in bootstrap_kwargs
    assert set(fanout_kwargs["host_meshes"].keys()) == {"hosts"}
    assert fanout_kwargs["spawn_worker_collectors"] is False
    assert state.query_engine_client is m.query_engine_client_cls.return_value


def test_process_job_sidecar_skips_worker_collector_actors():
    """ProcessJob sidecar telemetry keeps fan-out procs without collector actors."""
    mock_host_mesh = cast("HostMesh", MagicMock())
    with (
        _patched_sidecar() as m,
        patch.object(ProcessJob, "_create"),
        patch.object(
            ProcessJob,
            "_state",
            return_value=JobState({"hosts": mock_host_mesh}),
        ),
    ):
        job = ProcessJob({"hosts": 1}).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    assert m.ensure_open.call_count == 2
    bootstrap_kwargs = m.ensure_open.call_args_list[0].kwargs
    fanout_kwargs = m.ensure_open.call_args_list[1].kwargs
    assert bootstrap_kwargs["host_meshes"] == {}
    assert "spawn_worker_collectors" not in bootstrap_kwargs
    assert set(fanout_kwargs["host_meshes"].keys()) == {"hosts"}
    assert fanout_kwargs["spawn_worker_collectors"] is False
    assert state.query_engine_client is m.query_engine_client_cls.return_value


def test_sidecar_worker_fanout_uses_configured_host_meshes():
    """Worker fan-out should spawn telemetry collectors from the same host
    mesh configuration that state() returns to user code."""
    python_exe = "/mnt/app/.venv/bin/python"
    with _patched_sidecar() as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        job._components.mounts._default_python_exe = python_exe
        state = job.state(cached_path=None)

    fanout_hosts = m.ensure_open.call_args_list[1].kwargs["host_meshes"]
    assert fanout_hosts["hosts"].python_executable == python_exe
    assert state.hosts.python_executable == python_exe


def test_sidecar_bootstrap_failure_is_isolated():
    """A sidecar bootstrap failure disables telemetry and never fails state()."""
    with _patched_sidecar(ensure_open_side_effect=RuntimeError("boom")) as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)  # must not raise

    assert state.query_engine_client is None
    assert state.query_engine is None
    m.query_engine_client_cls.assert_not_called()


def test_sidecar_worker_fanout_failure_is_isolated():
    """A fan-out failure after a successful bootstrap is swallowed; the client
    from bootstrap stays exposed."""
    good = {
        "telemetry_url": "http://sidecar",
        "dashboard_url": "http://dashboard",
        "socket_path": "/tmp/telemetry.sock",
    }
    with _patched_sidecar(
        ensure_open_side_effect=[good, RuntimeError("fanout boom")]
    ) as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)  # must not raise

    assert m.ensure_open.call_count == 2
    assert state.query_engine_client is m.query_engine_client_cls.return_value


def test_telemetry_bootstraps_sidecar_by_default():
    """Telemetry always uses the sidecar."""
    with _patched_sidecar() as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)

    assert m.ensure_open.call_count == 2
    assert state.query_engine is None
    assert state.query_engine_client is m.query_engine_client_cls.return_value


def test_sidecar_query_client_dropped_on_pickle():
    """The sidecar query client is dropped on pickle and re-bootstrapped on the
    next state() (mirrors the legacy query_engine behavior)."""
    with _patched_sidecar() as m:
        job = MockJobTrait(host_names=["hosts"]).enable_telemetry(TelemetryConfig())
        state = job.state(cached_path=None)
        assert state.query_engine_client is m.query_engine_client_cls.return_value

        # __getstate__ drops live handles; the deserialized job is clean.
        loaded = job_loads(job.dumps())
        assert loaded._components.telemetry is not None
        assert loaded._components.telemetry._telemetry_url is None

        # Next state() re-bootstraps the sidecar.
        state = loaded.state(cached_path=None)
        assert state.query_engine_client is m.query_engine_client_cls.return_value


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
        # pyrefly: ignore [no-matching-overload]
        stdout = open(os.path.join(job._log_dir, "stdout.log"), "r").read()
        # pyrefly: ignore [no-matching-overload]
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


# ── exec_command tests ─────────────────────────────────────────────────────


def _exec_env():
    """Mock host_mesh -> procs -> bash_actors for driving exec_command.

    procs.stop() returns a real Future whose coroutine flips
    markers["stop_driven"] only when it is actually awaited/driven, so a test can
    prove the finally ran to completion rather than merely that stop() was called.
    Each test configures the endpoint calls (bash_actors.run / run_python).
    """
    markers = {"stop_driven": False}

    async def _stop_impl() -> None:
        markers["stop_driven"] = True

    bash_actors = MagicMock()
    procs = MagicMock()
    procs.spawn.return_value = bash_actors
    procs.stop.return_value = Future(coro=_stop_impl())
    host_mesh = MagicMock()
    host_mesh.spawn_procs.return_value = procs
    return host_mesh, procs, bash_actors, markers


def _results_future(pairs):
    """A real Future resolving to pairs, an iterable of (rank_key, result)."""

    async def _impl():
        return pairs

    return Future(coro=_impl())


def test_exec_command_returns_max_returncode():
    """exec_command returns the maximum return code across ranks."""
    host_mesh, _procs, bash_actors, _markers = _exec_env()
    bash_actors.run.call.return_value = _results_future(
        [
            ("rank0", {"returncode": 0, "stdout": "", "stderr": ""}),
            ("rank1", {"returncode": 7, "stdout": "", "stderr": ""}),
        ]
    )
    assert exec_command(host_mesh, ["echo", "hi"]).get() == 7


def test_exec_command_drives_cleanup_on_success():
    """procs.stop() is driven to completion on the happy path."""
    host_mesh, procs, bash_actors, markers = _exec_env()
    bash_actors.run.call.return_value = _results_future(
        [("rank0", {"returncode": 0, "stdout": "", "stderr": ""})]
    )
    exec_command(host_mesh, ["echo", "hi"]).get()
    assert markers["stop_driven"]
    procs.stop.assert_called_once()


def test_exec_command_drives_cleanup_on_error():
    """The finally must drive procs.stop() to completion even when the run raises."""

    class _Boom(Exception):
        pass

    host_mesh, procs, bash_actors, markers = _exec_env()
    bash_actors.run.call.side_effect = _Boom("run failed")
    with pytest.raises(_Boom):
        exec_command(host_mesh, ["echo", "hi"]).get()
    # Proves the finally did real async work, not merely that stop() was called.
    assert markers["stop_driven"]
    procs.stop.assert_called_once()


def test_exec_command_dispatches_to_run_python_for_py_scripts():
    """A .py command routes to run_python.call, not run.call."""
    host_mesh, _procs, bash_actors, _markers = _exec_env()
    bash_actors.run_python.call.return_value = _results_future(
        [("rank0", {"returncode": 0, "stdout": "", "stderr": ""})]
    )
    exec_command(host_mesh, ["train.py", "--flag"]).get()
    bash_actors.run_python.call.assert_called_once()
    bash_actors.run.call.assert_not_called()


def test_exec_command_dispatches_to_run_for_shell_commands():
    """A non-.py command builds a bash script and routes to run.call."""
    host_mesh, _procs, bash_actors, _markers = _exec_env()
    bash_actors.run.call.return_value = _results_future(
        [("rank0", {"returncode": 0, "stdout": "", "stderr": ""})]
    )
    exec_command(host_mesh, ["echo", "hi"]).get()
    bash_actors.run.call.assert_called_once()
    bash_actors.run_python.call.assert_not_called()
    script = bash_actors.run.call.call_args.args[0]
    assert script.startswith("#!/bin/bash")
    assert "echo hi" in script


def test_exec_command_slices_by_point():
    """point= selects a sub-mesh via host_mesh.slice(**point)."""
    host_mesh, procs, bash_actors, _markers = _exec_env()
    sliced = MagicMock()
    sliced.spawn_procs.return_value = procs
    host_mesh.slice.return_value = sliced
    bash_actors.run.call.return_value = _results_future(
        [("rank0", {"returncode": 0, "stdout": "", "stderr": ""})]
    )
    exec_command(host_mesh, ["echo", "hi"], point={"gpu": 2}).get()
    host_mesh.slice.assert_called_once_with(gpu=2)
    sliced.spawn_procs.assert_called_once()


def test_exec_command_slices_by_rank():
    """rank= selects a single rank via flatten('rank').slice(rank=...)."""
    host_mesh, procs, bash_actors, _markers = _exec_env()
    flattened = MagicMock()
    sliced = MagicMock()
    host_mesh.flatten.return_value = flattened
    flattened.slice.return_value = sliced
    sliced.spawn_procs.return_value = procs
    bash_actors.run.call.return_value = _results_future(
        [("rank0", {"returncode": 0, "stdout": "", "stderr": ""})]
    )
    exec_command(host_mesh, ["echo", "hi"], rank=3).get()
    host_mesh.flatten.assert_called_once_with("rank")
    flattened.slice.assert_called_once_with(rank=3)


def test_exec_command_prints_output_when_no_output_dir(capsys):
    """With no output_dir, each rank's stdout is echoed to the caller."""
    host_mesh, _procs, bash_actors, _markers = _exec_env()
    bash_actors.run.call.return_value = _results_future(
        [("rank0", {"returncode": 0, "stdout": "HELLO", "stderr": ""})]
    )
    exec_command(host_mesh, ["echo", "hi"]).get()
    assert "HELLO" in capsys.readouterr().out


def test_exec_command_output_dir_suppresses_printing(capsys):
    """With output_dir set, stdout/stderr are not echoed to the caller."""
    host_mesh, _procs, bash_actors, _markers = _exec_env()
    bash_actors.run.call.return_value = _results_future(
        [("rank0", {"returncode": 0, "stdout": "SHOULD_NOT_PRINT", "stderr": ""})]
    )
    exec_command(host_mesh, ["echo", "hi"], output_dir="/tmp/out").get()
    assert "SHOULD_NOT_PRINT" not in capsys.readouterr().out


def test_exec_command_produces_no_tokio():
    """Gate A (job cluster): exec_command produces no `_Tokio` state.

    Drives exec_command with the record-only `_Tokio` oracle enabled and asserts
    job.py produced no `_Tokio` (no `await <Future>` on the tokio thread). Before
    the `_take_inner()` migration this recorded the three job producers
    (run_python.call, run.call, procs.stop); after it, zero.
    """
    with tokio_oracle() as records:
        # shell branch: exercises run.call + the procs.stop finally
        host_mesh, _procs, bash_actors, _markers = _exec_env()
        bash_actors.run.call.return_value = _results_future(
            [("rank0", {"returncode": 0, "stdout": "", "stderr": ""})]
        )
        exec_command(host_mesh, ["echo", "hi"]).get()

        # python branch: exercises run_python.call + the procs.stop finally
        host_mesh, _procs, bash_actors, _markers = _exec_env()
        bash_actors.run_python.call.return_value = _results_future(
            [("rank0", {"returncode": 0, "stdout": "", "stderr": ""})]
        )
        exec_command(host_mesh, ["train.py"]).get()

        job_records = [r for r in records if r.filename.endswith("job.py")]
        assert job_records == [], f"exec_command still produces _Tokio: {job_records}"
