# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import cloudpickle
import pytest
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape, Slice
from monarch._src.actor.actor_mesh import Actor, context
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import (
    create_local_host_mesh,
    fake_in_process_host,
    HostMesh,
)
from monarch._src.actor.pickle import flatten, unflatten
from monarch._src.actor.v1 import enabled as v1_enabled


pytestmark = pytest.mark.skipif(not v1_enabled, reason="v1 not enabled")


@pytest.mark.timeout(60)
def test_fake_in_process_host() -> None:
    host = fake_in_process_host()
    assert host.extent.labels == ["hosts"]
    assert host.extent.sizes == [1]
    assert not host.stream_logs
    hy_host = host._hy_host_mesh.block_on()
    assert hy_host.region.labels == host.region.labels
    assert hy_host.region.slice() == host.region.slice()


@pytest.mark.timeout(60)
def test_create_local_host_mesh() -> None:
    host = create_local_host_mesh()
    assert host.extent.labels == ["hosts"]
    assert host.extent.sizes == [1]
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
    _unused, pickled = flatten(host, lambda _: False)
    unpickled = unflatten(pickled, _unused)
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
    hm_unpickled = cloudpickle.loads(cloudpickle.dumps(hm))
    with pytest.raises(RuntimeError):
        hm_unpickled.shutdown().get()
    hm.shutdown().get()
