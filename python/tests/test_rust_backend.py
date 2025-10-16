# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from contextlib import contextmanager
from typing import Generator
from unittest import TestCase

import monarch

import pytest
import torch
import torch.utils._python_dispatch
from monarch import fetch_shard, no_mesh, remote, Stream
from monarch.common.device_mesh import DeviceMesh
from monarch.common.remote import call_on_shard_and_fetch
from monarch.rust_local_mesh import local_meshes, LoggingLocation, SocketType
from torch.nn.attention import sdpa_kernel, SDPBackend
from torch.nn.functional import scaled_dot_product_attention


def simple_all_reduce(*args, **kwargs):
    return torch.ones(args[0].shape)


simple_all_reduce = remote(
    "monarch.worker._testing_function.simple_all_reduce_local",
    propagate=simple_all_reduce,
)


@contextmanager
def local_mesh(
    hosts: int = 1, gpu_per_host: int = 2, activate: bool = True
) -> Generator[DeviceMesh, None, None]:
    with monarch.rust_local_mesh.local_mesh(
        hosts=hosts,
        gpus_per_host=gpu_per_host,
        socket_type=SocketType.UNIX,
        logging_location=LoggingLocation.DEFAULT,
    ) as dm:
        try:
            if activate:
                with dm.activate():
                    yield dm
            else:
                yield dm
            dm.exit()
        except Exception:
            dm.client._shutdown = True
            raise


# Set global timeout--sandcastle's timeout is 600s. A test that sandcastle times
# out is not counted as a failure, so we set a more restrictive timeout to
# ensure we see a hard failure in CI.
@pytest.mark.timeout(120)
@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
class TestRustBackend(TestCase):
    def test_local_mesh_setup(self):
        with local_mesh():
            t = torch.zeros(3, 4)
            t.add_(1)
            fut = fetch_shard(t)

            with no_mesh.activate():
                local_t = fut.result()
        assert torch.equal(local_t, torch.ones(3, 4))

    def test_result_in_mesh(self):
        with local_mesh():
            t = torch.ones(3, 4)
            t.add_(-1)
            # Assert calling result() is fine within an active mesh.
            local_t = fetch_shard(t).result()
        assert torch.equal(local_t, torch.zeros(3, 4))

    def test_errors(self):
        t = torch.rand(3, 4)
        with local_mesh(2, 2) as dm:
            y = torch.rand(3, 4)
            with pytest.raises(TypeError, match="LOCAL_TENSOR"):
                t.add(y)
            with pytest.raises(TypeError, match="WRONG_MESH"):
                sub_mesh = dm(host=0)
                with sub_mesh.activate():
                    x = torch.rand(3, 4)
                    x.add(y)
            other = Stream("other")
            t = torch.rand(10).cuda()
            with pytest.raises(TypeError, match="WRONG_STREAM"):
                with other.activate():
                    t = t.reduce("host", "sum")

    def test_multi_hosts(self):
        with local_mesh(hosts=2, gpu_per_host=2):
            t = torch.rand(3, 4).cuda()
            local_t1 = fetch_shard(t, {"host": 1, "gpu": 0}).result()
            local_t2 = fetch_shard(t, {"host": 1, "gpu": 0}).result()
            local_t3 = fetch_shard(t, {"host": 0, "gpu": 1}).result()
        assert torch.equal(local_t1, local_t2)
        assert not torch.equal(local_t1, local_t3)

    def test_fetch_preprocess(self):
        with local_mesh():
            assert (
                "an argument processed"
                == call_on_shard_and_fetch(
                    remote("monarch.worker._testing_function.do_some_processing"),
                    "an argument",
                ).result()
            )

    def test_brutal_shutdown(self):
        with monarch.rust_local_mesh.local_mesh(
            hosts=1, gpus_per_host=1, socket_type=SocketType.UNIX
        ) as dm:
            dm.exit()
            dm.deactivate()

    def test_results_filtering(self):
        with local_mesh(gpu_per_host=1):
            query = torch.rand(1, 1, 1, 1, dtype=torch.float16, device="cuda")
            key = torch.rand(1, 1, 1, 1, dtype=torch.float16, device="cuda")
            value = torch.rand(1, 1, 1, 1, dtype=torch.float16, device="cuda")
            with sdpa_kernel(backends=[SDPBackend.FLASH_ATTENTION]):
                # This function will send 9 results. Only 5 of them will be set.
                t = scaled_dot_product_attention(query, key, value)
                fut = fetch_shard(t)
                local_tensor = fut.result()
            assert len(local_tensor) == 1

    def test_live_function(self):
        with local_mesh():

            @remote
            def has_nan(t):
                return torch.isnan(t).any().item()

            t = torch.rand(3, 4)
            res = call_on_shard_and_fetch(
                has_nan, t, shard={"host": 0, "gpu": 0}
            ).result()

        self.assertFalse(res)

    def test_multiple_global_meshes(self):
        """
        This test is to validate we can have a single client process
        connecting to multiple global meshes. The global meshes are distinct
        from each other to provide native failure domain isolation.
        """
        replicas = 4
        with local_meshes(
            meshes=replicas,
            hosts_per_mesh=1,
            gpus_per_host=1,
            socket_type=SocketType.UNIX,
            logging_location=LoggingLocation.DEFAULT,
        ) as groups:
            results = []
            for i, group in enumerate(groups):
                with group.activate():
                    t = torch.ones(i + 1)
                    results.append(fetch_shard(t).result())
            for i in range(replicas):
                assert torch.equal(results[i], torch.ones(i + 1))

            for group in groups:
                group.exit()
                group.deactivate()

    def test_get_world_status(self) -> None:
        with local_mesh(gpu_per_host=2) as mesh:
            mesh_info = mesh.get_info()

            self.assertIsNotNone(mesh_info.mesh_labels)
            self.assertEqual(len(mesh_info.devices_labels), 2)

    def test_ivalue_problems(self) -> None:
        with local_mesh(hosts=1, gpu_per_host=1):
            from typing import cast

            from monarch.common.messages import CallFunction, CommandGroup

            a = cast(monarch.Tensor, torch.rand(3, 4))
            result = monarch.Tensor(a._fake, a.mesh, a.stream)
            msg = CallFunction(
                0,
                result,
                (),
                monarch.common.function.ResolvableFunctionFromPath(
                    "torch.ops.aten.mul.Tensor"
                ),
                (2, a),
                {},
                a.stream._to_ref(a.mesh.client),
                a.mesh,
                [],
            )
            # Internally, this will call CallFunction(...).to_rust_message().
            # The 2 arg will be converted to an IValue tensor via rust + C++.
            # Then when the CommandGroup message gets converted to rust, it
            # will attempt to clone the rust CallFunction message, which will
            # attempt to clone the IValue tensor, which will cause a crash.
            # Upon attempting to clone the IValue tensor, our custom __torch_dispatch__
            # intercepts the following two calls:
            #   aten._to_copy.default () (2,) {'dtype': torch.float64, 'device': device(type='cpu')}
            #   aten.clone.default () (2,) {}

            with torch.utils._python_dispatch._disable_current_modes():
                CommandGroup([msg]).to_rust_message()
