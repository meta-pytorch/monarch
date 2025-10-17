# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import random
import time
from typing import Optional

import pytest
import torch

try:
    from later.unittest import TestCase
except ModuleNotFoundError:
    from unittest import TestCase
from monarch import fetch_shard, remote
from monarch.common.device_mesh import DeviceMesh, DeviceMeshStatus, no_mesh
from monarch.common.invocation import DeviceException, RemoteException
from monarch.rust_backend_mesh import MeshWorld, PoolDeviceMeshProvider
from monarch.rust_local_mesh import (
    Bootstrap,
    local_mesh_provider,
    local_meshes_and_bootstraps,
    LoggingLocation,
    SocketType,
    SupervisionParams,
)


def _do_bogus_tensor_work(
    x: torch.Tensor, y: torch.Tensor, fail_rank: Optional[int] = None
) -> torch.Tensor:
    return x + y  # real function actually does x @ y


do_bogus_tensor_work = remote(
    "monarch.worker._testing_function.do_bogus_tensor_work",
    propagate=_do_bogus_tensor_work,
)


def mesh_provider(
    meshes: int = 2,
    hosts_per_mesh: int = 1,
    gpus_per_host: int = 1,
    # pyre-fixme[11]: Annotation `DeviceMeshProvider` is not defined as a type.
) -> tuple[PoolDeviceMeshProvider, Bootstrap]:
    return local_mesh_provider(
        meshes=meshes,
        hosts_per_mesh=hosts_per_mesh,
        gpus_per_host=gpus_per_host,
        socket_type=SocketType.UNIX,
        logging_location=LoggingLocation.DEFAULT,
        supervision_params=SupervisionParams(
            update_timeout_in_sec=10,  # Fail fast
            query_interval_in_sec=1,
            update_interval_in_sec=1,
        ),
        auto_epoch=True,
    )


def local_meshes(
    meshes: int = 2,
    hosts_per_mesh: int = 1,
    gpus_per_host: int = 1,
) -> tuple[list[DeviceMesh], Bootstrap]:
    return local_meshes_and_bootstraps(
        meshes=meshes,
        hosts_per_mesh=hosts_per_mesh,
        gpus_per_host=gpus_per_host,
        socket_type=SocketType.UNIX,
        logging_location=LoggingLocation.DEFAULT,
        supervision_params=SupervisionParams(
            update_timeout_in_sec=10,  # Fail fast
            query_interval_in_sec=1,
            update_interval_in_sec=1,
        ),
        auto_epoch=True,
    )


# Set global timeout--sandcastle's timeout is 600s. A test that sandcastle times
# out is not counted as a failure, so we set a more restrictive timeout to
# ensure we see a hard failure in CI.
# The timeout is set to 250s as the failover takes longer than other tests.
@pytest.mark.timeout(250)
class TestFaultTolerance(TestCase):
    def test_mesh_provider(self) -> None:
        # Create multiple meshes using mesh provider
        replicas = 4
        provider, bootstrap = mesh_provider(meshes=replicas)
        meshes: list[DeviceMesh] = []
        while len(meshes) < replicas:
            dm = provider.new_mesh()
            meshes.append(dm)

        statuses = provider._root_client.world_status()
        for _, status in statuses.items():
            assert (
                DeviceMeshStatus(status) != DeviceMeshStatus.UNHEALTHY
            ), f"unexpected unhealthy mesh; world status: {statuses}"

        # Check that all meshes are initially live
        for mesh in meshes:
            with mesh.activate():
                t = torch.ones(1)
                local_t = fetch_shard(t).result()
            assert torch.equal(local_t, torch.ones(1))

        # Simulate a failure by killing one of the processes
        bootstrap.processes[-1].kill()

        # Find unhealthy mesh
        # Mix user and device errors
        unhealthy_meshes = []
        for mesh in meshes:
            with mesh.activate():
                # Send a call to trigger a failure
                x = torch.rand(3, 4)
                y = torch.rand(3, 4)
                z = do_bogus_tensor_work(x, y)
                try:
                    _ = fetch_shard(z).result()
                except RemoteException:
                    pass
                except DeviceException as e:
                    # Device error
                    unhealthy_meshes.append(mesh)
                    mesh.exit(e)

        self.assertEqual(len(unhealthy_meshes), 1)

        for _ in range(20):
            statuses = provider._root_client.world_status()
            # The 4th worker and controller worlds should have been evicted.
            if len(statuses) != 6:
                time.sleep(1)
                continue
            else:
                for status in statuses.values():
                    assert DeviceMeshStatus(status) == DeviceMeshStatus.LIVE
                return
        raise RuntimeError(f"Unexpected world statuses: {statuses}")

    def test_worker_supervision_failure(self) -> None:
        meshes, bootstrap = local_meshes(meshes=1)
        assert len(meshes) == 1
        mesh = meshes[0]

        # Check the mesh initially functional
        with mesh.activate():
            t = torch.ones(1)
            local_t = fetch_shard(t).result()
        assert torch.equal(local_t, torch.ones(1))

        # Simulate a failure by killing one of the processes
        bootstrap.processes[-1].kill()

        # A device error will be raised
        with mesh.activate():
            t = torch.ones(1)
            with self.assertRaisesRegex(DeviceException, r"crashed"):
                local_t = fetch_shard(t).result()

    def test_multi_mesh_failure_isolation(self) -> None:
        replicas = 4
        provider, bootstrap = mesh_provider(meshes=replicas)
        meshes: list[DeviceMesh] = []
        while len(meshes) < replicas:
            dm = provider.new_mesh()
            meshes.append(dm)

        # Check the meshes initially functional
        for mesh in meshes:
            with mesh.activate():
                t = torch.ones(1)
                local_t = fetch_shard(t).result()
            assert torch.equal(local_t, torch.ones(1))

        initial_size = len(provider._root_client.world_status())

        # Simulate a failure by killing one of the processes
        bootstrap.processes[-1].kill()

        # Mix user and device errors
        healthy_meshes = []
        unhealthy_meshes = []
        for mesh in meshes:
            with mesh.activate():
                # Send a call to trigger a failure
                x = torch.rand(3, 4)
                y = torch.rand(3, 4)
                z = do_bogus_tensor_work(x, y)
                try:
                    _ = fetch_shard(z).result()
                except RemoteException:
                    # User error
                    fetch_shard(x).result()
                    healthy_meshes.append(mesh)
                except DeviceException as e:
                    # Device error
                    unhealthy_meshes.append(mesh)
                    mesh.exit(e)

        self.assertEqual(len(healthy_meshes), replicas - 1)
        self.assertEqual(len(unhealthy_meshes), 1)

        while True:
            size = len(provider._root_client.world_status())
            if size == initial_size - 2:
                break

        # The healthy meshes should still be functional
        for mesh in healthy_meshes:
            with mesh.activate():
                t = torch.ones(1)
                local_t = fetch_shard(t).result()
            assert torch.equal(local_t, torch.ones(1))

    def test_out_of_order_receive(self) -> None:
        meshes, _ = local_meshes(meshes=8)

        # Check the meshes initially functional
        ts = []
        for i, mesh in enumerate(meshes):
            with mesh.activate():
                t = torch.ones(i + 1)
                ts.append(t)

        # Shuffle the meshes to makes sure the client is able to dispatch results in order
        indices = list(range(len(meshes)))
        shuffled_meshes = list(zip(indices, meshes, ts))
        random.shuffle(shuffled_meshes)
        for i, mesh, t in shuffled_meshes:
            with mesh.activate():
                local_t = fetch_shard(t).result()
            assert torch.equal(local_t, torch.ones(i + 1))

    def test_mesh_shrink_and_grow(self) -> None:
        # Create multiple meshes using mesh provider
        replicas = 4
        provider, bootstrap = mesh_provider(meshes=replicas)
        meshes: list[DeviceMesh] = []
        while len(meshes) < replicas:
            dm = provider.new_mesh()
            meshes.append(dm)

        worlds = len(provider._root_client.world_status())
        assigned_meshes = provider._mesh_map
        assert len(assigned_meshes) == replicas

        # Happy path
        for i, mesh in enumerate(meshes):
            with mesh.activate():
                t = torch.ones(i + 1)
                local_t = fetch_shard(t).result()
            assert torch.equal(local_t, torch.ones(i + 1))

        # Kill a worker
        mesh_to_kill: MeshWorld = list(bootstrap.mesh_worlds.keys())[1]
        procs = bootstrap.mesh_worlds[mesh_to_kill]
        assert len(procs) == 2
        procs[-1].kill()

        # The killed mesh will become unhealthy
        healthy_meshes = []
        unhealthy_meshes = []
        for i, mesh in enumerate(meshes):
            with mesh.activate():
                try:
                    t = torch.ones(i + 1)
                    local_t = fetch_shard(t).result()
                    with no_mesh.activate():
                        assert torch.equal(local_t, torch.ones(i + 1))
                    healthy_meshes.append(mesh)
                except DeviceException as e:
                    unhealthy_meshes.append(mesh)
                    mesh.exit(e)
        assert len(healthy_meshes) == replicas - 1
        assert len(unhealthy_meshes) == 1

        # Restart the mesh
        for proc in procs:
            proc.kill()

        # We should be able to acquire a new mesh without waiting for the old mesh to be evicted
        (worker_world, controller_id) = mesh_to_kill
        bootstrap.launch_mesh(controller_id=controller_id, worker_world=worker_world)

        dm = provider.new_mesh()
        healthy_meshes.append(dm)

        # We could have 4 or 5 meshes depending on if the unhealthy mesh is evicted
        assigned_meshes = provider._mesh_map
        assert len(assigned_meshes) >= replicas

        # We are happy again
        assert len(healthy_meshes) == replicas
        for i, mesh in enumerate(healthy_meshes):
            with mesh.activate():
                t = torch.ones(i + 1)
                local_t = fetch_shard(t).result()
            assert torch.equal(local_t, torch.ones(i + 1))

        # Old world should be evicted and new world should be spawned. So we ended up with the same number of worlds.
        while len((provider._root_client.world_status())) != worlds:
            # We expect to evict both controller and worker worlds from the same mesh.
            time.sleep(1)

        # Eventually, we only have 4 healthy meshes
        assigned_meshes = provider._mesh_map
        while len(assigned_meshes) != replicas:
            with self.assertRaisesRegex(
                TimeoutError, r"Could not find a healthy world"
            ):
                _ = provider.new_mesh(timeout_in_sec=1)
            assigned_meshes = provider._mesh_map
            time.sleep(1)

    def test_kill_controller(self) -> None:
        # Create multiple meshes using mesh provider
        replicas = 2
        provider, bootstrap = mesh_provider(meshes=replicas)
        meshes: list[DeviceMesh] = []
        while len(meshes) < replicas:
            dm = provider.new_mesh()
            meshes.append(dm)

        # Happy path
        for i, mesh in enumerate(meshes):
            with mesh.activate():
                t = torch.ones(i + 1)
                local_t = fetch_shard(t).result()
            assert torch.equal(local_t, torch.ones(i + 1))

        # Kill a controller
        mesh_to_kill: MeshWorld = list(bootstrap.mesh_worlds.keys())[1]
        procs = bootstrap.mesh_worlds[mesh_to_kill]
        assert len(procs) == 2
        procs[0].kill()

        # We should be able to detect the failure
        healthy_meshes = []
        detected_failure = False
        for i, mesh in enumerate(meshes):
            with mesh.activate():
                try:
                    t = torch.ones(i + 1)
                    local_t = fetch_shard(t).result()
                    with no_mesh.activate():
                        assert torch.equal(local_t, torch.ones(i + 1))
                    healthy_meshes.append(mesh)
                except DeviceException:
                    detected_failure = True
        assert len(healthy_meshes) == replicas - 1
        assert detected_failure

    def test_late_client_attaching(self) -> None:
        provider, _ = mesh_provider(meshes=1)

        # Wait for the meshes to be healthy
        healthy_meshes = 0
        while healthy_meshes < 2:
            healthy_meshes = 0
            statuses = provider._root_client.world_status()
            for _, status in statuses.items():
                if DeviceMeshStatus(status) == DeviceMeshStatus.LIVE:
                    healthy_meshes += 1
            time.sleep(1)

        # Sleep long enough to allow those "hidden messages" to be sent
        time.sleep(15)

        # Those "hidden messages" should not cause a trouble before a client is ready
        mesh = provider.new_mesh()
        with mesh.activate():
            t = torch.ones(1)
            fetch_shard(t).result()
