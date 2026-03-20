# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import os
import pathlib
import shutil
import threading
import time
from typing import List, Set
from unittest.mock import patch

import cloudpickle
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.shape import Shape, Slice
from monarch._src.actor.actor_mesh import _client_context, Actor, context
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import HostMesh, this_host
from monarch._src.actor.pickle import flatten, unflatten
from monarch._src.actor.proc_mesh import get_or_spawn_controller
from monarch._src.job.process import ProcessJob
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
async def test_host_mesh_context_manager() -> None:
    """Tests that the HostMesh can be used as a context manager and that it runs
    shutdown on exit"""
    async with ProcessJob({"hosts": 2}).state(cached_path=None).hosts as hm:
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
        hm.shutdown().get()


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
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    with pytest.raises(
        ValueError, match=r"proc_bind length \(1\) must equal procs_per_host \(4\)"
    ):
        host.spawn_procs(
            per_host={"gpus": 4},
            proc_bind=[{"cpunodebind": "0"}],
        )


@pytest.mark.timeout(120)
def test_spawn_procs_with_numactl_bind() -> None:
    numa_node0 = pathlib.Path("/sys/devices/system/node/node0")
    if not numa_node0.exists():
        # pyre-fixme[29]: skip is a function
        pytest.skip("NUMA node0 not available")
    if shutil.which("numactl") is None:
        # pyre-fixme[29]: skip is a function
        pytest.skip("numactl binary not found")

    node0_cpus = _parse_cpu_list((numa_node0 / "cpulist").read_text())
    assert node0_cpus, "node0 has no CPUs"

    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
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
    available = sorted(os.sched_getaffinity(0))
    if len(available) < 2:
        # pyre-fixme[29]: skip is a function
        pytest.skip("fewer than 2 CPUs available")

    cpu_a = available[0]
    cpu_b = available[1]

    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
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
