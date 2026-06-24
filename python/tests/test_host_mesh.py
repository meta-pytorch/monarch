# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import os
import pathlib
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from typing import Dict, List, Optional, Set
from unittest.mock import patch

import cloudpickle
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.shape import Point, Shape, Slice
from monarch._src.actor.actor_mesh import _client_context, Actor, attach, context
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import HostMesh, this_host, this_proc
from monarch._src.actor.pickle import flatten, unflatten
from monarch._src.actor.proc_mesh import get_or_spawn_controller
from monarch._src.job.job import ProcessState
from monarch._src.job.process import ProcessJob
from monarch.config import configured
from scoped_state import scoped_state


@pytest.mark.timeout(60)
def test_process_job_host_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        assert host.extent.labels == ["hosts"]
        assert host.extent.sizes == [1]
        assert not host.stream_logs
        hy_host = host._hy_host_mesh.block_on()
        assert hy_host.region.labels == host.region.labels
        assert hy_host.region.slice() == host.region.slice()


@pytest.mark.timeout(60)
def test_multi_host_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 8}), cached_path=None) as state:
        host = state.hosts
        assert host.extent.labels == ["hosts"]
        assert host.extent.sizes == [8]
        assert not host.stream_logs
        assert host._ndslice == Slice(offset=0, sizes=[8], strides=[1])
        assert host._labels == ("hosts",)
        hy_host = host._hy_host_mesh.block_on()
        assert hy_host.region.labels == host.region.labels
        assert hy_host.region.slice() == host.region.slice()

        # Hosts 5 and 7
        sliced = host._new_with_shape(
            Shape(labels=["hosts"], slice=Slice(offset=5, sizes=[2], strides=[2]))
        )
        assert sliced.extent.labels == ["hosts"]
        assert sliced.extent.sizes == [2]
        assert not sliced.stream_logs
        assert sliced._ndslice == Slice(offset=5, sizes=[2], strides=[2])
        assert sliced._labels == ("hosts",)
        hy_sliced = sliced._hy_host_mesh.block_on()
        assert hy_sliced.region.labels == sliced.region.labels
        assert hy_sliced.region.slice() == sliced.region.slice()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_spawn_proc_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 8}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(name="proc")
        assert proc_mesh._host_mesh is host
        assert proc_mesh._ndslice == host._ndslice
        assert tuple(proc_mesh._labels) == host._labels
        hy_proc_mesh = proc_mesh._proc_mesh.block_on()
        assert tuple(hy_proc_mesh.region.labels) == host._labels
        assert hy_proc_mesh.region.slice() == host.region.slice()

        # Hosts 5 and 7
        sliced_host = host._new_with_shape(
            Shape(labels=["hosts"], slice=Slice(offset=5, sizes=[2], strides=[2]))
        )
        sliced_proc = sliced_host.spawn_procs(
            name="proc_sliced", per_host={"gpus": 3, "just_for_fun": 4}
        )
        hy_sliced_proc = sliced_proc._proc_mesh.block_on()
        assert sliced_proc._host_mesh is sliced_host
        assert sliced_proc._ndslice == Slice(
            offset=0, sizes=[2, 3, 4], strides=[12, 4, 1]
        )
        assert sliced_proc._labels == ["hosts", "gpus", "just_for_fun"]
        assert hy_sliced_proc.region.labels == sliced_proc._labels
        assert hy_sliced_proc.region.slice() == sliced_proc._ndslice


@pytest.mark.timeout(60)
def test_pickle() -> None:
    with scoped_state(ProcessJob({"hosts": 8}), cached_path=None) as state:
        host = state.hosts
        host.initialized.get()
        _unused, pickled = flatten(host, lambda _: False)
        unpickled = unflatten(pickled.freeze(), _unused)
        assert isinstance(unpickled, HostMesh)
        assert host.extent.labels == ["hosts"]
        assert host.extent.sizes == [8]
        assert not host.stream_logs
        assert host._ndslice == Slice(offset=0, sizes=[8], strides=[1])
        assert host._labels == ("hosts",)
        hy_host = host._hy_host_mesh.block_on()
        assert hy_host.region.labels == host.region.labels
        assert hy_host.region.slice() == host.region.slice()


class RankActor(Actor):
    @endpoint
    async def get_rank(self) -> int:
        return context().actor_instance.rank.rank

    @endpoint
    async def get_pid(self) -> int:
        return os.getpid()


def is_process_running(pid: int) -> bool:
    """Check if a process with the given PID is still running."""
    try:
        os.kill(pid, 0)  # Signal 0 doesn't kill, just checks if process exists
        return True
    except OSError:
        return False


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_shutdown_host_mesh() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        hm = state.hosts
        pm = hm.spawn_procs(per_host={"gpus": 2})
        am = pm.spawn("actor", RankActor)
        am.get_rank.choose().get()
        hm.shutdown().get()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_alias_bootstrap() -> None:
    """A worker started with an alias address (dial_to@bind_to) is reachable.

    The alias format `tcp://PUBLIC:PORT@tcp://0.0.0.0:PORT` lets a worker
    advertise a dialable address while binding to a wildcard interface. This
    is required where binding directly to the advertised address is not
    possible (e.g. behind NAT, where only `0.0.0.0` can be bound).
    """
    procs = []
    workers = []
    for _i in range(2):
        # Reserve an ephemeral port, then release it so the worker can bind
        # to it via the alias's bind_to address.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        env = {**os.environ}
        if "FB_XAR_INVOKED_NAME" in os.environ:
            env["PYTHONPATH"] = ":".join(sys.path)

        # dial_to advertises a reachable address; bind_to listens on the
        # wildcard interface.
        addr = f"tcp://127.0.0.1:{port}@tcp://0.0.0.0:{port}"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys; from monarch.actor import run_worker_loop_forever; "
                f'run_worker_loop_forever(address={addr!r}, ca="trust_all_connections")',
            ],
            env=env,
        )
        procs.append(proc)
        # Pass the full alias through attach_to_workers. The attach path must
        # canonicalize this to dial_to before using it as host identity.
        workers.append(addr)

    try:
        # pyrefly: ignore [bad-argument-type]
        hosts = attach_to_workers(ca="trust_all_connections", workers=workers)
        am = hosts.spawn_procs(per_host={"gpus": 1}).spawn("rank", RankActor)
        ranks = sorted(v for _, v in am.get_rank.call().get().items())
        assert ranks == [0, 1]
        hosts.shutdown().get()
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_host_mesh_context_manager() -> None:
    """Tests that the HostMesh can be used as a context manager and that it runs
    shutdown on exit"""
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        async with state.hosts as hm:
            pm = hm.spawn_procs(per_host={"gpus": 2})
            am = pm.spawn("actor", RankActor)
            await am.get_rank.choose()
        # Ensure that other operations fail after shutdown.
        with pytest.raises(RuntimeError, match="HostMesh has already been shut down"):
            hm.spawn_procs(per_host={"gpus": 2})
        with pytest.raises(RuntimeError, match="HostMesh has already been shut down"):
            await hm.shutdown()


@pytest.mark.timeout(60)
def test_shutdown_sliced_host_mesh_throws_exception() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        hm = state.hosts
        hm_sliced = hm.slice(hosts=1)
        with pytest.raises(RuntimeError):
            hm_sliced.shutdown().get()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_shutdown_unpickled_host_mesh_throws_exception() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        hm = state.hosts
        hm.initialized.get()
        hm_unpickled = cloudpickle.loads(cloudpickle.dumps(hm))
        with pytest.raises(RuntimeError):
            hm_unpickled.shutdown().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_stop_and_reconnect() -> None:
    # Use a short message delivery timeout so we don't have to sleep 35 s
    # waiting for undeliverable-message errors to surface.
    with configured(message_delivery_timeout="5s"):
        job = ProcessJob({"hosts": 2})

        # First connection: spawn actors, verify they work.
        with scoped_state(job, cached_path=None) as state:
            hm = state.hosts
            pm = hm.spawn_procs(per_host={"gpus": 1})
            am = pm.spawn("actor", RankActor)
            pids = am.get_pid.call().get()
            assert len(pids) == 2
            pids = [pid for _, pid in pids.items()]

            # Stop: terminate user procs but keep workers alive.
            hm.stop().get()
            # Ensure that the procs are actually dead.
            assert all(not is_process_running(pid) for pid in pids)

            # Sleep past the message delivery timeout to ensure that no errors
            # surface from undeliverable messages to dead procs/actors.
            time.sleep(7)

            # Second connection: reconnect to the same workers via state().
            hm2 = job.state(cached_path=None).hosts
            pm2 = hm2.spawn_procs(per_host={"gpus": 1})
            am2 = pm2.spawn("actor", RankActor)
            ranks2 = am2.get_rank.call().get()
            assert len(ranks2) == 2

            # Shutdown: fully tear down and exit workers.
            hm2.shutdown().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_stop_only_drains_own_mesh_procs() -> None:
    """Stopping one mesh should not kill procs belonging to another mesh
    on the same workers.

    Simulates the real scenario where the main process and the mount
    process each have their own HostMesh (with different names) connected
    to the same remote workers.
    """
    job = ProcessJob({"hosts": 2})

    # First mesh: simulate the main process.
    with scoped_state(job, cached_path=None) as state1:
        hm1 = state1.hosts
        pm1 = hm1.spawn_procs(per_host={"gpus": 1}, name="proc1")
        am1 = pm1.spawn("actor", RankActor)
        pids1 = list(am1.get_pid.call().get().values())
        assert len(pids1) == 2

        # Second mesh: simulate the mount process reconnecting. This
        # creates a new HostMesh with a different name via a second
        # state() call on the same job.
        state2 = job.state(cached_path=None)
        hm2 = state2.hosts
        pm2 = hm2.spawn_procs(per_host={"gpus": 1}, name="proc2")
        am2 = pm2.spawn("actor", RankActor)
        pids2 = list(am2.get_pid.call().get().values())
        assert len(pids2) == 2

        # Stop the first mesh — should only drain its own procs.
        hm1.stop().get()

        # Procs from mesh1 should be stopped.
        assert all(not is_process_running(pid) for pid in pids1)

        # Procs from mesh2 should still be alive.
        assert all(is_process_running(pid) for pid in pids2)

        # Clean up mesh2.
        hm2.shutdown().get()


class PidActor(Actor):
    @endpoint
    def get_pid(self) -> int:
        return os.getpid()


class CpuAffinityActor(Actor):
    @endpoint
    def get_affinity(self) -> List[int]:
        return sorted(os.sched_getaffinity(0))


def _parse_cpu_list(s: str) -> Set[int]:
    """Parse kernel CPU-list format (e.g. "0-3,8-11") into a set of ints."""
    cpus = set()
    for part in s.strip().split(","):
        if "-" in part:
            lo, hi = part.split("-", 1)
            cpus.update(range(int(lo), int(hi) + 1))
        else:
            cpus.add(int(part))
    return cpus


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_this_host_on_client_can_spawn_actual_os_processes() -> None:
    hm = this_host()
    assert not hm.is_fake_in_process
    am = hm.spawn_procs(per_host={"gpus": 4}).spawn("actor", PidActor)
    pids = am.get_pid.call().get()
    for pid in pids.values():
        assert pid != os.getpid()
    assert len(set(pids.values())) == 4


@pytest.mark.timeout(60)
def test_controllers_have_same_pid_as_client() -> None:
    pid_controller = get_or_spawn_controller(
        "pid_test_controllers_have_same_pid_as_client", PidActor
    ).get()
    assert pid_controller.get_pid.call_one().get() == os.getpid()


class PidActorController(Actor):
    @endpoint
    def spawn_pid_actor(self) -> PidActor:
        return this_host().spawn_procs(per_host={"gpus": 4}).spawn("pid", PidActor)


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_this_host_on_controllers_can_spawn_actual_os_processes() -> None:
    pid_controller_0 = get_or_spawn_controller(
        "pid_test_this_host_on_controllers_0", PidActorController
    ).get()
    pid_controller_1 = get_or_spawn_controller(
        "pid_test_this_host_on_controllers_1", PidActorController
    ).get()
    pid_0 = pid_controller_0.spawn_pid_actor.call_one().get()
    pid_1 = pid_controller_1.spawn_pid_actor.call_one().get()
    pid_0_values = list(pid_0.get_pid.call().get().values())
    pid_1_values = list(pid_1.get_pid.call().get().values())
    assert pid_0_values != pid_1_values
    assert len(set(pid_0_values)) == 4
    assert len(set(pid_1_values)) == 4


@pytest.mark.timeout(60)
def test_root_client_does_not_leak_host_meshes() -> None:
    orig_get_client_context = _client_context.get
    with patch.object(_client_context, "get") as mock_get_client_context:
        mock_get_client_context.side_effect = orig_get_client_context

        def sync_sleep_then_context():
            time.sleep(0.1)
            context()

        threads = []
        for _ in range(100):
            t = threading.Thread(target=sync_sleep_then_context)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        assert mock_get_client_context.call_count == 100


@pytest.mark.timeout(60)
def test_spawn_procs_proc_bind_length_mismatch() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        with pytest.raises(
            ValueError,
            match=r"proc_bind length \(1\) must equal procs_per_host \(4\)",
        ):
            host.spawn_procs(
                per_host={"gpus": 4},
                proc_bind=[{"cpunodebind": "0"}],
            )


@pytest.mark.timeout(120)
def test_spawn_procs_with_numactl_bind() -> None:
    numa_node0 = pathlib.Path("/sys/devices/system/node/node0")
    if not numa_node0.exists():
        pytest.skip("NUMA node0 not available")
    if shutil.which("numactl") is None:
        pytest.skip("numactl binary not found")

    node0_cpus = _parse_cpu_list((numa_node0 / "cpulist").read_text())
    assert node0_cpus, "node0 has no CPUs"

    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(
            name="numa_bound",
            per_host={"gpus": 2},
            proc_bind=[{"cpunodebind": "0"}, {"cpunodebind": "0"}],
        )
        am = proc_mesh.spawn("affinity", CpuAffinityActor)
        affinities = am.get_affinity.call().get()
        for rank, cpus in affinities.items():
            cpu_set = set(cpus)
            assert cpu_set, f"rank {rank} has empty affinity"
            assert cpu_set <= node0_cpus, (
                f"rank {rank} affinity {cpu_set} is not a subset of node0 CPUs {node0_cpus}"
            )


@pytest.mark.timeout(120)
def test_spawn_procs_with_taskset_bind() -> None:
    if not hasattr(os, "sched_getaffinity"):
        pytest.skip("os.sched_getaffinity not available on this platform")
    available = sorted(os.sched_getaffinity(0))
    if len(available) < 2:
        pytest.skip("fewer than 2 CPUs available")

    cpu_a = available[0]
    cpu_b = available[1]

    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(
            name="taskset_bound",
            per_host={"gpus": 2},
            proc_bind=[{"cpus": str(cpu_a)}, {"cpus": str(cpu_b)}],
        )
        am = proc_mesh.spawn("affinity", CpuAffinityActor)
        affinities = am.get_affinity.call().get()
        observed = {frozenset(cpus) for cpus in affinities.values()}
        assert observed == {frozenset({cpu_a}), frozenset({cpu_b})}, (
            f"expected affinities {{{cpu_a}}} and {{{cpu_b}}}, got {affinities}"
        )


class WrapperMarkerActor(Actor):
    """Returns the PID stamped by the wrapper executable."""

    @endpoint
    def get_wrapper_pid(self) -> str:
        return os.environ.get("PYTHON_WRAPPER_PID", "")

    @endpoint
    def spawn_inner(self) -> "WrapperMarkerActor":
        """Spawns a new proc via this_host() to test recursive propagation."""
        return (
            this_host()
            .spawn_procs(per_host={"procs": 1})
            .spawn("inner_marker", WrapperMarkerActor)
        )


def _make_python_wrapper() -> str:
    """
    Write a shell wrapper that stamps PYTHON_WRAPPER_PID=$$ (the wrapper
    shell's PID) and execs to the real Python interpreter.

    Using the PID rather than a plain boolean means each wrapper invocation
    produces a *different* value.  The recursive test can therefore verify
    that the wrapper ran a second time (producing a different PID) rather than
    just inheriting the first PID from the parent process environment.
    """
    real_python = sys.executable
    tmpdir = tempfile.mkdtemp(prefix="monarch_test_python_exe_")
    wrapper_path = os.path.join(tmpdir, "python_wrapper")
    with open(wrapper_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                #!/bin/sh
                export PYTHON_WRAPPER_PID=$$
                exec {real_python} "$@"
                """
            )
        )
    os.chmod(wrapper_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP)
    return wrapper_path


class CudaVisibleDevicesActor(Actor):
    @endpoint
    def get_cuda_visible_devices(self) -> str:
        return os.environ.get("CUDA_VISIBLE_DEVICES", "")


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_spawn_procs_with_bootstrap_command() -> None:
    from monarch._src.actor.host_mesh import default_bootstrap_cmd

    hm = this_host()

    # Use with_env to customize the bootstrap command
    base = default_bootstrap_cmd()
    cmd = base.with_env({"CUDA_VISIBLE_DEVICES": "3"})

    pm = hm.spawn_procs(per_host={"gpus": 1}, bootstrap_command=cmd)
    am = pm.spawn("cuda_marker", CudaVisibleDevicesActor)
    assert am.get_cuda_visible_devices.call_one().get() == "3"

    # A second spawn without bootstrap_command must not inherit the previous
    # spawn's env, confirming that with_env is non-mutating on the HostMesh.
    pm_no_env = hm.spawn_procs(per_host={"gpus": 1})
    am_no_env = pm_no_env.spawn("cuda_marker_default", CudaVisibleDevicesActor)
    assert am_no_env.get_cuda_visible_devices.call_one().get() != "3"


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_spawn_procs_with_bootstrap_command_callable() -> None:
    from monarch._rust_bindings.monarch_hyperactor.host_mesh import BootstrapCommand
    from monarch._src.actor.host_mesh import default_bootstrap_cmd

    hm = this_host()

    def per_rank_bootstrap(point: Point) -> BootstrapCommand:
        base = default_bootstrap_cmd()
        return base.with_env({"CUDA_VISIBLE_DEVICES": str(point["gpus"])})

    pm = hm.spawn_procs(per_host={"gpus": 2}, bootstrap_command=per_rank_bootstrap)
    am = pm.spawn("cuda_per_rank", CudaVisibleDevicesActor)
    observed = sorted(am.get_cuda_visible_devices.call().get().values())
    assert observed == ["0", "1"]


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_with_python_executable() -> None:
    wrapper_path = _make_python_wrapper()
    modified_host = this_host().with_python_executable(wrapper_path)

    # Direct spawn: proc should be launched via the wrapper; wrapper PID is set.
    am = modified_host.spawn_procs(per_host={"procs": 1}).spawn(
        "marker", WrapperMarkerActor
    )
    direct_pid = am.get_wrapper_pid.call_one().get()
    assert direct_pid != "", "wrapper was not executed for direct spawn"

    # Recursive spawn: actor calls this_host().spawn_procs() and the inner proc
    # should also be launched via the wrapper — proving the bootstrap command is
    # stored on the HostMeshRef and propagated.  The wrapper PID must differ
    # from the direct-spawn PID, ruling out simple env-var inheritance.
    inner_am = am.spawn_inner.call_one().get()
    inner_pid = inner_am.get_wrapper_pid.call_one().get()
    assert inner_pid != "", "wrapper was not executed for recursive spawn"
    assert inner_pid != direct_pid, (
        "recursive spawn inherited PID instead of re-running wrapper"
    )


class DuplexProcessJob(ProcessJob):
    """ProcessJob that also exposes a duplex socket on the first host.

    The duplex address is available via ``duplex_addr`` after calling
    ``state()``.  Pass this value to ``attach(...)`` before the
    first ``context()`` call so the client bootstraps by attaching to
    the worker.
    """

    def __init__(
        self,
        meshes: Optional[Dict[str, int]] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(meshes or {"hosts": 1}, env)
        self._duplex_addr: Optional[str] = None

    def _create(self, client_script: Optional[str]) -> None:
        if client_script is not None:
            raise RuntimeError("DuplexProcessJob cannot run batch-mode scripts")

        self._tmpdir = tempfile.mkdtemp(prefix="monarch_duplex_job_")

        for mesh_name, count in self._meshes.items():
            for i in range(count):
                host_key = f"{mesh_name}_{i}"
                addr = f"ipc://{self._tmpdir}/{host_key}"
                worker_env = {**os.environ, "HYPERACTOR_PROCESS_NAME": host_key}
                if self._env is not None:
                    worker_env.update(self._env)

                cmd = [
                    sys.executable,
                    "-c",
                    "from monarch.actor import run_worker_loop_forever; "
                    f'run_worker_loop_forever(address="{addr}", '
                    'ca="trust_all_connections")',
                ]
                proc = subprocess.Popen(cmd, env=worker_env, start_new_session=True)
                self._host_to_pid[host_key] = ProcessState(proc.pid, addr)

        # Wait for the first worker's frontend socket to appear.
        # The duplex server is now on the same address as the frontend.
        first_key = f"{next(iter(self._meshes))}_0"
        assert self._tmpdir is not None
        sock_path = os.path.join(self._tmpdir, first_key)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if os.path.exists(sock_path):
                break
            time.sleep(0.05)
        else:
            self._kill()
            raise RuntimeError("frontend socket did not appear in time")
        # The duplex addr is the first worker's frontend (they share
        # the same address now). Use ipc:// format for zmq URL parsing.
        self._duplex_addr = f"ipc://{sock_path}"

    @property
    def duplex_addr(self) -> str:
        """Native-format ChannelAddr for the first worker's frontend/duplex address."""
        if self._duplex_addr is None:
            raise RuntimeError("call apply() first to start the worker")
        return self._duplex_addr

    @property
    def frontend_socket_path(self) -> str:
        """Filesystem path of the first worker's frontend Unix socket."""
        tmpdir = self._tmpdir
        if tmpdir is None:
            raise RuntimeError("call apply() first to start the worker")
        first_key = f"{next(iter(self._meshes))}_0"
        return os.path.join(tmpdir, first_key)


class EchoActor(Actor):
    """Replies with the message it receives."""

    @endpoint
    async def echo(self, msg: str) -> str:
        return msg


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_client_attach_addr() -> None:
    """Verify ``attach(...)`` causes the client to bootstrap by
    attaching to the worker, and that subsequent mesh operations route
    exclusively over the duplex channel.

    After bootstrapping and spawning procs we remove the worker's frontend
    socket from the filesystem.  Any subsequent client-side messaging that
    accidentally tries to dial the frontend will fail, proving that
    endpoint calls, replies, and undeliverable bounces travel exclusively
    over the duplex channel.
    """
    job = DuplexProcessJob()
    # Start the worker (so its duplex address exists) and attach the
    # client to it BEFORE anything bootstraps the client context —
    # `scoped_state(job)` connects via `attach_to_workers`, which would
    # otherwise bootstrap the client unattached.
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None) as state:
        context()

        # Remove the worker's frontend socket.  The client should
        # never need to dial the frontend — all communication
        # flows through the duplex channel.  If anything on the
        # client side accidentally tries to connect to the
        # frontend, it will fail.
        frontend = job.frontend_socket_path
        assert os.path.exists(frontend)
        os.remove(frontend)
        assert not os.path.exists(frontend)

        # (a) Spawn HostMesh, ProcMesh, and ActorMesh.
        hm = state.hosts
        assert hm is not None

        pm = hm.spawn_procs(per_host={"workers": 2})
        assert pm is not None

        am = pm.spawn("echo", EchoActor)

        # (b) Send messages to the actor mesh.
        results = am.echo.call("hello").get()

        # (c) Get replies back.
        for val in results.values():
            assert val == "hello"


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_client_attach_addr_this_host_and_this_proc() -> None:
    """After bootstrapping with ``attach(...)``, ``this_host()``
    returns the host-local mesh on this machine (not the attached host)
    and ``this_proc()`` returns the local client proc whose gateway is
    now connected to the remote gateway. The local client procs remain
    on this machine; the remote gateway holds routes back to them
    through the via duplex. ``this_host()`` keeps its ``"on this
    machine"`` meaning so that ``this_host().spawn_procs()`` always
    spawns local procs; to drive procs on the attached host, use the
    separate ``HostMesh`` returned by the job."""
    job = DuplexProcessJob()
    # Attach the client to the worker before any client bootstrap (see
    # test_client_attach_addr).
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None):
        context()

        proc = this_proc()
        assert proc is not None
        host = this_host()
        assert host is not None
        assert host is proc.host_mesh

        # Verify the meshes are usable by spawning an actor.
        am = proc.spawn("echo2", EchoActor)
        result = am.echo.call_one("ping").get()
        assert result == "ping"


class RelayActor(Actor):
    """Forwards a call to a target actor; used to test reverse reachability."""

    def __init__(self, target):
        self._target = target

    @endpoint
    async def relay(self, msg: str) -> str:
        return await self._target.echo.call_one(msg)


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_client_attach_addr_this_host_spawns_locally() -> None:
    """After attaching, ``this_host().spawn_procs()`` spawns procs on
    this machine.  Procs spawned this way are reachable from the
    client (forward) and from procs running on the attached host
    (reverse), demonstrating that addresses route both ways."""
    job = DuplexProcessJob()
    # Attach the client to the worker before any client bootstrap (see
    # test_client_attach_addr).
    job.apply()
    attach(job.duplex_addr)
    with scoped_state(job, cached_path=None) as state:
        context()

        # this_host() spawns procs on this machine, not on the
        # attached host.
        local_pm = this_host().spawn_procs(per_host={"local": 1})
        local_echo = local_pm.spawn("local_echo", EchoActor)

        # client -> local procs reachable.
        assert local_echo.echo.call_one("from-client").get() == "from-client"

        # remote -> local procs reachable: a relay actor on the
        # attached job's host calls back into the local actor
        # mesh.
        remote_pm = state.hosts.spawn_procs(per_host={"remote": 1})
        relay = remote_pm.spawn("relay", RelayActor, local_echo)
        assert relay.relay.call_one("through-remote").get() == "through-remote"
