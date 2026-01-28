# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import os
import threading
import time
from typing import cast
from unittest.mock import MagicMock, patch

import cloudpickle
import monarch._src.actor.host_mesh
import monarch.actor
import pytest
from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape, Slice
from monarch._src.actor.actor_mesh import (
    _client_context,
    Actor,
    ActorMesh,
    context,
    ValueMesh,
)
from monarch._src.actor.allocator import LocalAllocator, ProcessAllocator
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import (
    create_local_host_mesh,
    HostMesh,
    this_host,
    this_proc,
)
from monarch._src.actor.proc_mesh import (
    _get_bootstrap_args,
    get_or_spawn_controller,
    ProcMesh,
    register_proc_mesh_spawn_callback,
    unregister_proc_mesh_spawn_callback,
)


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
    monarch.actor.unhandled_fault_hook = lambda failure: None
    host = create_local_host_mesh(Extent(["hosts"], [2]))
    proc = host.spawn_procs(per_host={"gpus": 2})

    async def task():
        cloudpickle.dumps(proc)

    await proc.initialized
    PythonTask.from_coroutine(task()).block_on()

    async def task():
        cloudpickle.dumps(proc.slice(gpus=0, hosts=0))

    PythonTask.from_coroutine(task()).block_on()


@pytest.mark.timeout(60)
async def test_deprecated_proc_mesh_from_alloc_mock() -> None:
    num_hosts = 2
    num_gpus = 8

    def test_setup() -> None:
        import os

        os.environ["TEST_VAR"] = "test_value"

    constraints = AllocConstraints(match_labels={"test_label": "test_value"})
    allocator = LocalAllocator()
    spec = AllocSpec(
        constraints,
        hosts=num_hosts,
        gpus=num_gpus,
    )

    with patch.object(HostMesh, "allocate_nonblocking") as mock_host_alloc:
        mock_host_mesh = MagicMock()
        mock_host_mesh.spawn_procs = MagicMock()
        mock_host_alloc.return_value = mock_host_mesh

        alloc_handle = allocator.allocate(spec)
        ProcMesh.from_alloc(alloc_handle, test_setup)

        mock_host_alloc.assert_called_once()
        (name, extent, allocator, constraints) = mock_host_alloc.call_args.args

        assert name == "host_mesh_from_alloc"
        assert extent == Extent(["hosts", "gpus"], [num_hosts, num_gpus])
        assert isinstance(allocator, LocalAllocator)
        assert constraints.match_labels == {"test_label": "test_value"}

        mock_host_mesh.spawn_procs.assert_called_once_with(bootstrap=test_setup)


@pytest.mark.timeout(60)
def test_deprecated_proc_mesh_from_alloc_multi_actor() -> None:
    allocator = ProcessAllocator(*_get_bootstrap_args())
    spec = AllocSpec(AllocConstraints(), replicas=2, hosts=2, gpus=3)
    alloc_handle = allocator.allocate(spec)
    proc_mesh = ProcMesh.from_alloc(alloc_handle)

    actor = proc_mesh.spawn("test_actor", TestActor, 42)

    proc_ranks = actor.get_proc_rank.call().get()
    assert proc_ranks.extent.labels == ["replicas", "hosts", "gpus"]
    assert proc_ranks.extent.sizes == [2, 2, 3]
    for i, (point, rank) in enumerate(proc_ranks.items()):
        assert rank == i
        assert point.rank == i


class PidActor(Actor):
    @endpoint
    def get_pid(self) -> int:
        return os.getpid()


@pytest.mark.timeout(60)
def test_this_proc_on_root_client_spawns_actor_in_client_os_process() -> None:
    proc = this_proc()
    actor = proc.spawn("pid_actor", PidActor)
    assert actor.get_pid.call_one().get() == os.getpid()


@pytest.mark.timeout(60)
def test_proc_mesh_on_root_client_spawns_actor_in_client_os_process() -> None:
    proc = this_proc()
    actor = proc.spawn("pid_actor", PidActor)
    assert actor.get_pid.call_one().get() == os.getpid()


class PidActorController(Actor):
    @endpoint
    def spawn_pid_actor_with_this_proc(self) -> PidActor:
        return this_proc().spawn("pid", PidActor)

    @endpoint
    def spawn_pid_actor_with_proc_mesh(self) -> PidActor:
        return context().actor_instance.proc_mesh.spawn("pid", PidActor)


@pytest.mark.timeout(60)
def test_this_proc_in_controller_spawns_actor_in_client_os_process() -> None:
    pid_controller = get_or_spawn_controller(
        "pid_test_this_proc_in_controller", PidActorController
    ).get()
    assert (
        pid_controller.spawn_pid_actor_with_this_proc.call_one()
        .get()
        .get_pid.call_one()
        .get()
        == os.getpid()
    )


@pytest.mark.timeout(60)
def test_context_proc_mesh_in_controller_spawns_actor_in_client_os_process() -> None:
    pid_controller = get_or_spawn_controller(
        "pid_test_context_proc_mesh_in_controller", PidActorController
    ).get()
    assert (
        pid_controller.spawn_pid_actor_with_proc_mesh.call_one()
        .get()
        .get_pid.call_one()
        .get()
        == os.getpid()
    )


@pytest.mark.timeout(60)
def test_root_client_does_not_leak_proc_meshes() -> None:
    orig_get_client_context = _client_context.get
    with (
        patch.object(_client_context, "get") as mock_get_client_context,
        patch.object(
            monarch._src.actor.host_mesh, "fake_in_process_host"
        ) as mock_fake_in_process_host,
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
        assert mock_fake_in_process_host.call_count in (0, 1)


@pytest.mark.timeout(60)
def test_actor_spawn_does_not_block_on_proc_mesh_init() -> None:
    async def sleep_then_mesh(pm: Shared[HyProcMesh]) -> HyProcMesh:
        time.sleep(15)
        return await pm

    host = create_local_host_mesh()
    proc_mesh = host.spawn_procs(name="test_proc")
    proc_mesh._proc_mesh = PythonTask.from_coroutine(
        sleep_then_mesh(proc_mesh._proc_mesh)
    ).spawn()
    assert proc_mesh._proc_mesh.poll() is None
    proc_mesh.spawn("pid", PidActor)
    assert proc_mesh._proc_mesh.poll() is None


@pytest.mark.timeout(60)
def test_raw_proc_mesh_pickle_blocks_on_proc_mesh_init() -> None:
    async def sleep_then_mesh(pm: Shared[HyProcMesh]) -> HyProcMesh:
        time.sleep(15)
        return await pm

    proc_mesh = this_host().spawn_procs(name="test_proc")
    proc_mesh._proc_mesh = PythonTask.from_coroutine(
        sleep_then_mesh(proc_mesh._proc_mesh)
    ).spawn()
    assert proc_mesh._proc_mesh.poll() is None
    cloudpickle.dumps(proc_mesh)
    assert proc_mesh._proc_mesh.poll() is not None


@pytest.mark.timeout(60)
def test_proc_mesh_spawn_callback() -> None:
    """Test that registered callbacks are invoked when a ProcMesh is spawned."""
    spawned_meshes: list[ProcMesh] = []

    def callback(pm: ProcMesh) -> None:
        spawned_meshes.append(pm)

    register_proc_mesh_spawn_callback(callback)
    try:
        host = create_local_host_mesh()
        proc_mesh = host.spawn_procs(name="test_proc")

        assert len(spawned_meshes) == 1
        assert spawned_meshes[0] is proc_mesh
    finally:
        unregister_proc_mesh_spawn_callback(callback)


@pytest.mark.timeout(60)
def test_proc_mesh_spawn_callback_multiple() -> None:
    """Test that multiple callbacks are all invoked."""
    callback1_meshes: list[ProcMesh] = []
    callback2_meshes: list[ProcMesh] = []

    def callback1(pm: ProcMesh) -> None:
        callback1_meshes.append(pm)

    def callback2(pm: ProcMesh) -> None:
        callback2_meshes.append(pm)

    register_proc_mesh_spawn_callback(callback1)
    register_proc_mesh_spawn_callback(callback2)
    try:
        host = create_local_host_mesh()
        proc_mesh = host.spawn_procs(name="test_proc")

        assert len(callback1_meshes) == 1
        assert len(callback2_meshes) == 1
        assert callback1_meshes[0] is proc_mesh
        assert callback2_meshes[0] is proc_mesh
    finally:
        unregister_proc_mesh_spawn_callback(callback1)
        unregister_proc_mesh_spawn_callback(callback2)


@pytest.mark.timeout(60)
def test_proc_mesh_spawn_callback_unregister() -> None:
    """Test that unregistered callbacks are not invoked."""
    spawned_meshes: list[ProcMesh] = []

    def callback(pm: ProcMesh) -> None:
        spawned_meshes.append(pm)

    register_proc_mesh_spawn_callback(callback)
    unregister_proc_mesh_spawn_callback(callback)

    host = create_local_host_mesh()
    host.spawn_procs(name="test_proc")

    assert len(spawned_meshes) == 0
