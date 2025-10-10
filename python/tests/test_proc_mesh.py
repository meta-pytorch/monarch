# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import cast

import cloudpickle

import pytest
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape, Slice
from monarch._src.actor.actor_mesh import Actor, ActorMesh, context, ValueMesh
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import create_local_host_mesh, this_host
from monarch._src.actor.proc_mesh import ProcMesh
from monarch._src.actor.v1 import enabled as v1_enabled


pytestmark = pytest.mark.skipif(not v1_enabled, reason="v1 not enabled")


_proc_rank = -1


class TestActor(Actor):
    def __init__(self, initial_value: int = 0):
        self.value = initial_value
        global _proc_rank
        if _proc_rank == -1:
            _proc_rank = context().actor_instance.rank.rank

    @endpoint
    async def get_value(self) -> int:
        return self.value

    @endpoint
    async def set_value(self, value: int) -> None:
        self.value = value

    @endpoint
    async def get_proc_rank(self) -> int:
        return _proc_rank

    @endpoint
    async def spawn_on_this_host(self) -> "TestActor":
        rank = context().actor_instance.rank.rank
        return (
            this_host()
            ._new_with_shape(
                Shape(
                    labels=["hosts"], slice=Slice(offset=rank, sizes=[1], strides=[1])
                )
            )
            .spawn_procs(name=f"test_proc_{rank}", per_host={"gpus": 4})
            .spawn(f"test_{rank}", TestActor, rank)
        )

    @endpoint
    async def get_rank_plus_init_value(self) -> int:
        return context().actor_instance.rank.rank + self.value

    @endpoint
    async def call_on_other_mesh(self, actor: "TestActor") -> ValueMesh[int]:
        return await actor.get_rank_plus_init_value.call()


@pytest.mark.timeout(60)
async def test_proc_mesh_initialization() -> None:
    host = create_local_host_mesh()
    proc_mesh = host.spawn_procs(name="test_proc")
    # Test that initialization completes successfully
    assert await proc_mesh.initialized


@pytest.mark.timeout(60)
def test_proc_mesh_spawn_single_actor() -> None:
    host = create_local_host_mesh()
    proc_mesh = host.spawn_procs(name="test_proc")
    actor = proc_mesh.spawn("test_actor", TestActor, 42)
    assert actor.get_value.call_one().get() == 42
    actor.set_value.call_one(43).get()
    assert actor.get_value.call_one().get() == 43


@pytest.mark.timeout(60)
def test_proc_mesh_multi_actor() -> None:
    host = create_local_host_mesh(Extent(["replicas", "hosts"], [2, 2]))
    proc_mesh = host.spawn_procs(name="test_proc", per_host={"gpus": 3})
    actor = proc_mesh.spawn("test_actor", TestActor, 42)

    proc_ranks = actor.get_proc_rank.call().get()
    assert proc_ranks.extent.labels == ["replicas", "hosts", "gpus"]
    assert proc_ranks.extent.sizes == [2, 2, 3]
    for i, (point, rank) in enumerate(proc_ranks.items()):
        assert rank == i
        assert point.rank == i


@pytest.mark.timeout(60)
def test_proc_mesh_sliced() -> None:
    host = create_local_host_mesh(Extent(["replicas", "hosts"], [2, 2]))
    proc_mesh = host.spawn_procs(name="test_proc", per_host={"gpus": 3})
    # Initialize _proc_rank on each actor process
    actor = proc_mesh.spawn("test_actor", TestActor, 42)
    actor.get_proc_rank.call().get()
    # Replicas 0 and 1, host 0, gpus 1 and 2
    sliced = proc_mesh._new_with_shape(
        Shape(
            labels=["replicas", "gpus"],
            slice=Slice(offset=1, sizes=[2, 2], strides=[6, 1]),
        )
    )
    actor = sliced.spawn("test_actor_sliced", TestActor, 42)
    proc_ranks = actor.get_proc_rank.call().get()
    assert proc_ranks.extent.labels == ["replicas", "gpus"]
    assert proc_ranks.extent.sizes == [2, 2]
    for (i, (point, rank)), expected_rank in zip(
        enumerate(proc_ranks.items()), [1, 2, 7, 8]
    ):
        assert rank == expected_rank
        assert point.rank == i


@pytest.mark.timeout(120)
def test_nested_meshes() -> None:
    host = create_local_host_mesh(Extent(["hosts"], [2]))
    proc = host.spawn_procs(name="proc")
    actor = proc.spawn("actor", TestActor)
    nested = actor.spawn_on_this_host.call().get()
    nested_0 = nested.item(hosts=0)
    nested_1 = nested.item(hosts=1)
    for i, nested in enumerate([nested_0, nested_1]):
        region = cast(
            ProcMesh, cast(ActorMesh[TestActor], nested)._proc_mesh
        )._host_mesh.region
        assert region.labels == ["hosts"]
        assert region.slice() == Slice(offset=i, sizes=[1], strides=[1])
    res_0 = nested_0.slice(gpus=0).call_on_other_mesh.call_one(nested_1).get()
    res_1 = nested_1.slice(gpus=0).call_on_other_mesh.call_one(nested_0).get()
    for point, value in res_0:
        assert value == point.rank + 1
    for point, value in res_1:
        assert value == point.rank


@pytest.mark.timeout(60)
async def test_pickle_initialized_proc_mesh_in_tokio_thread() -> None:
    host = create_local_host_mesh(Extent(["hosts"], [2]))
    proc = host.spawn_procs(per_host={"gpus": 2})

    async def task():
        cloudpickle.dumps(proc)

    await proc.initialized
    PythonTask.from_coroutine(task()).block_on()

    async def task():
        cloudpickle.dumps(proc.slice(gpus=0, hosts=0))

    PythonTask.from_coroutine(task()).block_on()
