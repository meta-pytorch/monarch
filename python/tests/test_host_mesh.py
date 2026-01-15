# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import os
import threading
import time
from unittest.mock import patch

import cloudpickle
import monarch._src.actor.host_mesh
import pytest
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape, Slice
from monarch._src.actor.actor_mesh import _client_context, Actor, context
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import (
    create_local_host_mesh,
    fake_in_process_host,
    HostMesh,
    this_host,
)
from monarch._src.actor.pickle import flatten, unflatten
from monarch._src.actor.proc_mesh import get_or_spawn_controller


@pytest.mark.timeout(60)
def test_fake_in_process_host() -> None:
    host = fake_in_process_host()
    assert host.extent.labels == []
    assert host.extent.sizes == []
    assert not host.stream_logs
    hy_host = host._hy_host_mesh.block_on()
    assert hy_host.region.labels == host.region.labels
    assert hy_host.region.slice() == host.region.slice()


@pytest.mark.timeout(60)
def test_create_local_host_mesh() -> None:
    host = create_local_host_mesh()
    assert host.extent.labels == []
    assert host.extent.sizes == []
    assert not host.stream_logs
    hy_host = host._hy_host_mesh.block_on()
    assert hy_host.region.labels == host.region.labels
    assert hy_host.region.slice() == host.region.slice()


@pytest.mark.timeout(60)
def test_multi_dim_host_mesh() -> None:
    host = create_local_host_mesh(
        Extent(["replicas", "hosts"], [2, 4]),
    )
    assert host.extent.labels == ["replicas", "hosts"]
    assert host.extent.sizes == [2, 4]
    assert not host.stream_logs
    assert host._ndslice == Slice(offset=0, sizes=[2, 4], strides=[4, 1])
    assert host._labels == ("replicas", "hosts")
    hy_host = host._hy_host_mesh.block_on()
    assert hy_host.region.labels == host.region.labels
    assert hy_host.region.slice() == host.region.slice()

    # Hosts 1 and 3 on replica 1
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
def test_spawn_proc_mesh() -> None:
    host = create_local_host_mesh(
        Extent(["replicas", "hosts"], [2, 4]),
    )
    proc_mesh = host.spawn_procs(name="proc")
    assert proc_mesh._host_mesh is host
    assert proc_mesh._ndslice == host._ndslice
    assert tuple(proc_mesh._labels) == host._labels
    hy_proc_mesh = proc_mesh._proc_mesh.block_on()
    assert tuple(hy_proc_mesh.region.labels) == host._labels
    assert hy_proc_mesh.region.slice() == host.region.slice()

    # Hosts 1 and 3 on replica 1
    sliced_host = host._new_with_shape(
        Shape(labels=["hosts"], slice=Slice(offset=5, sizes=[2], strides=[2]))
    )
    sliced_proc = sliced_host.spawn_procs(
        name="proc_sliced", per_host={"gpus": 3, "just_for_fun": 4}
    )
    hy_sliced_proc = sliced_proc._proc_mesh.block_on()
    assert sliced_proc._host_mesh is sliced_host
    assert sliced_proc._ndslice == Slice(offset=0, sizes=[2, 3, 4], strides=[12, 4, 1])
    assert sliced_proc._labels == ["hosts", "gpus", "just_for_fun"]
    assert hy_sliced_proc.region.labels == sliced_proc._labels
    assert hy_sliced_proc.region.slice() == sliced_proc._ndslice


@pytest.mark.timeout(60)
def test_pickle() -> None:
    host = create_local_host_mesh(
        Extent(["replicas", "hosts"], [2, 4]),
    )
    host.initialized.get()
    _unused, pickled = flatten(host, lambda _: False)
    unpickled = unflatten(pickled.freeze(), _unused)
    assert isinstance(unpickled, HostMesh)
    assert host.extent.labels == ["replicas", "hosts"]
    assert host.extent.sizes == [2, 4]
    assert not host.stream_logs
    assert host._ndslice == Slice(offset=0, sizes=[2, 4], strides=[4, 1])
    assert host._labels == ("replicas", "hosts")
    hy_host = host._hy_host_mesh.block_on()
    assert hy_host.region.labels == host.region.labels
    assert hy_host.region.slice() == host.region.slice()


class RankActor(Actor):
    @endpoint
    async def get_rank(self) -> int:
        return context().actor_instance.rank.rank


@pytest.mark.timeout(60)
def test_shutdown_host_mesh() -> None:
    hm = create_local_host_mesh(Extent(["hosts"], [2]))
    pm = hm.spawn_procs(per_host={"gpus": 2})
    am = pm.spawn("actor", RankActor)
    am.get_rank.choose().get()
    hm.shutdown().get()


@pytest.mark.timeout(60)
def test_shutdown_sliced_host_mesh_throws_exception() -> None:
    hm = create_local_host_mesh(Extent(["hosts"], [2]))
    hm_sliced = hm.slice(hosts=1)
    with pytest.raises(RuntimeError):
        hm_sliced.shutdown().get()


@pytest.mark.timeout(60)
def test_shutdown_unpickled_host_mesh_throws_exception() -> None:
    hm = create_local_host_mesh(Extent(["hosts"], [2]))
    hm.initialized.get()
    hm_unpickled = cloudpickle.loads(cloudpickle.dumps(hm))
    with pytest.raises(RuntimeError):
        hm_unpickled.shutdown().get()
    hm.shutdown().get()


class PidActor(Actor):
    @endpoint
    def get_pid(self) -> int:
        return os.getpid()


@pytest.mark.timeout(60)
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
    with (
        patch.object(_client_context, "get") as mock_get_client_context,
        patch.object(
            monarch._src.actor.host_mesh, "create_local_host_mesh"
        ) as mock_create_local,
    ):
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
        # If this test is run in isolation, the local host mesh will
        # be created once. But if it runs with other tests, the host mesh
        # will have already been initialized and the function never gets
        # called.
        assert mock_create_local.call_count in (0, 1)
