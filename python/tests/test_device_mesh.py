# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import pytest
from monarch import NDSlice
from monarch.common.client import Client
from monarch.common.device_mesh import DeviceMesh
from monarch.simulator.mock_controller import MockController


class TestDeviceMesh:
    def test_mesh_index(self) -> None:
        fake_processes = NDSlice(offset=0, sizes=[2, 3, 4], strides=[12, 4, 1])
        ctrl = MockController(1, False)
        client = Client(ctrl, ctrl.world_size, ctrl.gpu_per_host)
        dm = DeviceMesh(client, fake_processes, ("a", "b", "c"))
        assert 0 == dm(a=0, b=0, c=0).processes[0]
        x = dm(a=0, b=0)
        assert x.processes[:] == fake_processes[0:4]
        assert x.names == ("c",)
        assert x.processes.sizes[0] == 4
        x = dm(c=slice(None, None, 2))
        assert x.processes[:] == fake_processes[::2]
        x = dm(b=2, c=3)
        assert x.processes[:] == (11, 23)
        client.shutdown()

    def test_mesh_reshape(self) -> None:
        fake_processes = NDSlice(offset=0, sizes=[60, 24], strides=[24, 1])
        ctrl = MockController(1, False)
        client = Client(ctrl, ctrl.world_size, ctrl.gpu_per_host)
        dm = DeviceMesh(client, fake_processes, ("host", "gpu"))
        dm2 = dm.split(host=("dp", "pp"), gpu=("tp",), pp=4)
        assert dm2.names == ("dp", "pp", "tp")
        assert dm2.processes.sizes == [15, 4, 24]
        assert dm2.processes.strides == [4 * 24, 24, 1]

        dm3 = dm.rename(host="dp", gpu="tp")
        assert dm3.names == ("dp", "tp")
        assert dm.processes.strides == dm3.processes.strides
        dm4 = dm.split(host=("dp", "pp"), gpu=("tp",), dp=4)
        assert dm4.processes.sizes == [4, 15, 24]
        dm5 = dm.split(host=("dp", "pp"), dp=60)
        assert dm5.processes.sizes == [60, 1, 24]
        dm6 = dm.split(host=("dp", "pp"), pp=60)
        assert dm6.processes.sizes == [1, 60, 24]

        with pytest.raises(ValueError, match="Cannot infer size"):
            dm2 = dm.split(host=("dp", "pp"))

        with pytest.raises(ValueError, match="unused size constraints"):
            dm2 = dm.split(host=("dp", "pp"), pp=4, ddp=3)

        dm2 = dm.rename(host="dp")
        assert dm2.names == ("dp", "gpu")

        with pytest.raises(ValueError, match="Duplicate dimension name"):
            dm2 = dm.split(host=("dp", "pp"), gpu=("pp",), dp=3)

        with pytest.raises(ValueError, match="evenly divided"):
            dm2 = dm.split(host=("dp", "pp"), dp=7)

        client.shutdown()

    def test_flatten(self) -> None:
        fake_processes = NDSlice(offset=0, sizes=[60, 24], strides=[24, 1])
        ctrl = MockController(1, False)
        client = Client(ctrl, ctrl.world_size, ctrl.gpu_per_host)
        dm = DeviceMesh(client, fake_processes, ("host", "gpu"))
        dm2 = dm.flatten("gpu")
        assert dm2.names == ("gpu",)
        assert dm2.processes.sizes == [60 * 24]
        assert dm2.processes.strides == [1]
        client.shutdown()

        good_cases = [
            NDSlice(offset=0, sizes=[100], strides=[1]),
            NDSlice(offset=100, sizes=[8, 4, 2], strides=[8, 2, 1]),
            NDSlice(offset=1, sizes=[4, 2], strides=[2, 1]),
        ]
        for slice in good_cases:
            ctrl = MockController(1, False)
            client = Client(ctrl, ctrl.world_size, ctrl.gpu_per_host)
            dm = DeviceMesh(
                client, slice, tuple(f"dim{d}" for d in range(len(slice.sizes)))
            )
            dm2 = dm.flatten("outer")
            assert dm2.names == ("outer",)
            assert dm2.processes.strides == [1]
            assert list(slice) == list(dm2.processes)
            client.shutdown()

        # Test some bad ones (sparse slices).
        bad_cases = [
            NDSlice(offset=0, sizes=[100], strides=[2]),
            NDSlice(offset=0, sizes=[64, 32], strides=[64, 1]),
        ]
        for slice in bad_cases:
            with pytest.raises(ValueError, match="cannot flatten sparse mesh"):
                ctrl = MockController(1, False)
                client = Client(ctrl, ctrl.world_size, ctrl.gpu_per_host)
                dm = DeviceMesh(
                    client, slice, tuple(f"dim{d}" for d in range(len(slice.sizes)))
                )
                dm.flatten("bad_dim")
                client.shutdown()

    def test_worker_mesh_init(self) -> None:
        from monarch.worker.worker import DeviceMesh as WorkerDeviceMesh

        processes = NDSlice(offset=0, sizes=[3, 4], strides=[4, 1])
        wdm = WorkerDeviceMesh(0, ("a", "b"), processes, rank=1)
        a, b = wdm.dims["a"], wdm.dims["b"]
        assert b.members == [0, 1, 2, 3]
        assert b.rank == 1

        assert a.members == [1, 5, 9]
        assert a.rank == 0

        wdm = WorkerDeviceMesh(0, ("a", "b"), processes, rank=6)
        a, b = wdm.dims["a"], wdm.dims["b"]
        assert b.members == [4, 5, 6, 7]
        assert b.rank == 2
        assert a.members == [2, 6, 10]
        assert a.rank == 1

        processes = NDSlice(offset=0, sizes=[3, 4, 2], strides=[8, 2, 1])
        wdm = WorkerDeviceMesh(0, ("a", "b", "c"), processes, rank=10)
