# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import itertools
import re
import sys
import traceback

import monarch
import monarch.random
import pytest
import torch
from monarch import fetch_shard, grad_function, grad_generator, Stream, Tensor
from monarch._testing import TestingContext
from monarch.common.device_mesh import DeviceMesh, no_mesh
from monarch.common.remote import remote
from monarch.common.tree import flattener
from monarch_supervisor.logging import fix_exception_lines


def custom_excepthook(exc_type, exc_value, exc_traceback):
    tb_lines = fix_exception_lines(
        traceback.format_exception(exc_type, exc_value, exc_traceback)
    )
    print("\n".join(tb_lines), file=sys.stderr)


sys.excepthook = custom_excepthook


@pytest.fixture(scope="module", autouse=True)
def testing_context():
    global local
    with TestingContext() as local:
        yield


panic = remote("__test_panic", propagate="inspect")

remote_sleep = remote("time.sleep", propagate="inspect")


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
# Set global timeout--sandcastle's timeout is 600s. A test that sandcastle times
# out is not counted as a failure, so we set a more restrictive timeout to
# ensure we see a hard failure in CI.
@pytest.mark.timeout(120)
class TestController:
    @classmethod
    def local_device_mesh(
        cls,
        N,
        gpu_per_host,
        activate=True,
    ):
        return local.local_device_mesh(
            N,
            gpu_per_host,
            activate,
        )

    def test_errors(self):
        t = torch.rand(3, 4)
        with self.local_device_mesh(2, 2) as device_mesh:
            y = torch.rand(3, 4)
            with pytest.raises(TypeError, match="LOCAL_TENSOR"):
                t.add(y)
            with pytest.raises(TypeError, match="WRONG_MESH"):
                sm = device_mesh.slice(host=0)
                with sm.activate():
                    x = torch.rand(3, 4)
                    x.add(y)

            other = Stream("other")
            t = torch.rand(10).cuda()
            with pytest.raises(TypeError, match="WRONG_STREAM"):
                with other.activate():
                    t = t.reduce("host", "sum")

    def test_sub_mesh(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            h0 = device_mesh.slice(host=0)
            h1 = device_mesh.slice(host=1)
            with h0.activate():
                _ = torch.rand(3, 4)
            with h1.activate():
                _ = torch.rand(3, 4)
                # Runs on a different mesh but should still work

    def test_fetch_result_device(self):
        with self.local_device_mesh(2, 2):
            on_gpu = torch.ones(2, 3, device="cuda")
            on_cpu = torch.ones(2, 3, device="cpu")

            on_gpu_local = fetch_shard(on_gpu).result()
            on_cpu_local = fetch_shard(on_cpu).result()

        assert on_gpu_local.device == torch.device("cpu")
        assert on_cpu_local.device == torch.device("cpu")

    def test_dim1_mesh(self):
        with self.local_device_mesh(2, 2, activate=False) as device_mesh:
            mesh3d = device_mesh.split(host=("oh", "ih"), ih=1)
            with mesh3d.activate():
                x = torch.ones(3, 4)
                local_x = fetch_shard(x).result()

        assert torch.equal(local_x, torch.ones(3, 4))

    def test_sub_mesh_use_only_one(self):
        with self.local_device_mesh(2, 2, activate=False) as device_mesh:
            h0 = device_mesh.slice(host=0)

            with h0.activate():
                x = torch.ones(3, 4)
                local_x = fetch_shard(x)

            local_x = local_x.result(timeout=20)
            assert torch.equal(local_x, torch.ones(3, 4))

    def test_sub_mesh_process_grop(self):
        with self.local_device_mesh(2, 2, activate=False) as device_mesh:
            h0 = device_mesh.slice(host=0)
            pg0 = h0.process_group(("gpu",))
            pg1 = h0.process_group(("gpu",))
            # Is there a way to functionally test that these two PG's aren't
            # the same in the backend?
            assert pg0 != pg1

    def test_reduce(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            x = (
                12 * 2 * device_mesh.rank("host")
                + 12 * device_mesh.rank("gpu")
                + torch.arange(12, device="cuda").reshape(3, 4)
            )
            y = x.reduce("gpu", "sum")
            g = x.reduce("gpu", "stack")
            with pytest.raises(TypeError, match="When scattering"):
                x = x.reduce("gpu", "sum", scatter=True)
            x = x.reshape(2, 6)
            atoa = x.reduce("gpu", "stack", scatter=True)
            rs = x.reduce("gpu", "sum", scatter=True)
            rad = x.reduce((), "sum")
            rade = x.reduce(("gpu", "host"), "sum")
            with pytest.raises(
                ValueError, match="is not valid for multiple dimensions"
            ):
                x.reduce((), "sum", scatter=True)
            with pytest.raises(
                ValueError, match="is not valid for multiple dimensions"
            ):
                x.reduce((), "stack")
            with pytest.raises(
                ValueError, match="is not valid for multiple dimensions"
            ):
                x.reduce((), "stack", scatter=True)
            y_local = fetch_shard(y).result()
            g_local = fetch_shard(g).result()
            # TODO compute the expected values to compare agains in the below section
            _ = fetch_shard(atoa).result()
            _ = fetch_shard(rs).result()
            rad_local = fetch_shard(rad).result()
            rade_local = fetch_shard(rade).result()

        xs = {
            (h, g): 12 * 2 * h + 12 * g + torch.arange(12, device="cpu").reshape(3, 4)
            for h, g in itertools.product(range(2), range(2))
        }

        y_expected = xs[(0, 0)] + xs[(0, 1)]
        g_expected = torch.stack([xs[(0, 0)], xs[(0, 1)]])
        assert torch.equal(y_local, y_expected)
        assert torch.equal(g_local, g_expected)
        rad_expected = (xs[(0, 0)] + xs[(0, 1)] + xs[(1, 0)] + xs[(1, 1)]).reshape(
            rad_local.shape
        )
        assert torch.equal(rad_local, rad_expected)
        assert torch.equal(rade_local, rad_expected)

        # test is run on 4 GPUs, can't have mesh with 3 non-trivial dimensions
        with self.local_device_mesh(2, 2, activate=False) as mesh2d:
            device_mesh = mesh2d.split(host=("oh", "ih"), ih=1)
            with device_mesh.activate():
                x = (
                    12 * 2 * device_mesh.rank("oh")
                    + 12 * device_mesh.rank("gpu")
                    + torch.arange(12, device="cuda").reshape(3, 4)
                )
                y = x.reduce(("ih", "gpu"), "sum")
                y_local = fetch_shard(y).result()
                z = x.reduce(("oh", "gpu"), "sum")
                z_local = fetch_shard(z).result()

        assert torch.equal(y_local, y_expected)
        assert torch.equal(z_local, rad_expected.reshape(z_local.shape))

    def test_reduce_out(self):
        with self.local_device_mesh(2, 2):
            inp = torch.rand(2, 4, device="cuda")
            out_incorrect = torch.rand(2, 4, device="cuda")
            out = torch.rand(4, device="cuda")

            with pytest.raises(
                ValueError, match="Reduce expects the shape to be torch.Size."
            ):
                _ = inp.reduce("host", reduction="sum", scatter=True, out=out_incorrect)

            reduce_out = inp.reduce("host", reduction="sum", scatter=True)
            local_out = fetch_shard(out).result()
            local_reduce_out = fetch_shard(reduce_out).result()
            assert out._fake is not reduce_out._fake
            with no_mesh.activate():
                assert not torch.equal(local_out, local_reduce_out)

            reduce_out = inp.reduce("host", reduction="sum", scatter=True, out=out)
            local_out = fetch_shard(out).result()
            local_reduce_out = fetch_shard(reduce_out).result()
            assert out._fake is reduce_out._fake
            with no_mesh.activate():
                assert torch.equal(local_out, local_reduce_out)

    def test_fetch(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            h = device_mesh.rank("host")
            g = device_mesh.rank("gpu")
            for hi in range(2):
                for gi in range(2):
                    x, y = fetch_shard((h, g), {"host": hi, "gpu": gi}).result()
                    with no_mesh.activate():
                        assert (hi, gi) == (x.item(), y.item())

    def test_mutate(self):
        with self.local_device_mesh(2, 2):
            x = torch.rand(3, 4).cuda()
            x.abs_()
            s = Stream("other")
            b, drop = s.borrow(x)
            with pytest.raises(TypeError, match="would be mutated"):
                x.abs_()
            with s.activate():
                _ = b.add(b)
            drop.drop()
            x.abs_()
            b, drop = s.borrow(x, mutable=True)
            with s.activate():
                b.abs_()
            drop.drop()
            # del b
            x.abs_()

    def test_movement(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            sm0 = device_mesh.slice(host=0)
            sm1 = device_mesh.slice(host=1)

            with sm0.activate():
                x = torch.rand(3, 4, device="cuda")
                _ = x.to_mesh(sm1)

            a = torch.rand(3, 4, device="cuda")

            b = a.slice_mesh(host=0)
            _ = b.to_mesh(sm0)
            _ = b.to_mesh(sm1)

    def test_broadcast_one(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            for dim in ("host", "gpu"):
                subset = device_mesh.slice(**{dim: 1})
                with subset.activate():
                    x = torch.rand(3, device="cuda")
                    y = x.to_mesh(device_mesh)

                with subset.activate():
                    a = monarch.inspect(x)
                with device_mesh.activate():
                    b = monarch.inspect(y.reduce(dim, reduction="stack"))
                with no_mesh.activate():
                    assert torch.allclose(a.expand(2, -1), b, rtol=0, atol=0)

    def test_broadcast_two(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            subset = device_mesh.slice(host=1, gpu=1)
            with subset.activate():
                x = torch.rand(3, device="cuda")
                y = x.to_mesh(device_mesh)

            with subset.activate():
                a = monarch.inspect(x)
            with device_mesh.activate():
                b = monarch.inspect(
                    y.reduce("host", reduction="stack").reduce("gpu", reduction="stack")
                )
            with no_mesh.activate():
                assert torch.allclose(a.expand(2, 2, -1), b, rtol=0, atol=0)

    def test_autograd(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            x = torch.rand(3, 4, requires_grad=True)
            y = torch.rand(4, 3, requires_grad=True)
            z = torch.rand(3, requires_grad=True)

            foo = (x @ y + z).sum()
            with no_mesh.activate():
                # check backward restores forward mesh
                for t in grad_generator(foo, [z, y, x]):
                    with device_mesh.activate():
                        fetch_shard(t).result()

    def test_mesh_semantics(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            host0 = device_mesh.slice(host=0)
            host1 = device_mesh.slice(host=1)
            with host0.activate():
                x = torch.randn(5)
            y = x * 5
            with host1.activate():
                a = torch.randn(5)
                b = a * 5
                x.cos()
            y.cos()
            b.cos()

    def test_autograd_multi_mesh(self):
        @grad_function
        def to_mesh(x: Tensor, mesh: DeviceMesh):
            omesh = x.mesh

            def backward(grad_x: Tensor):
                print(grad_x.mesh, omesh)
                return grad_x.to_mesh(omesh), None

            return x.to_mesh(mesh), backward

        with self.local_device_mesh(2, 2) as device_mesh:
            host0 = device_mesh.slice(host=0)
            host1 = device_mesh.slice(host=1)
            with host0.activate():
                x = torch.rand(3, 4, requires_grad=True, device="cuda")
                y = torch.rand(4, 3, requires_grad=True, device="cuda")
                t = x @ y
                t = to_mesh(t, host1)
            with host1.activate():
                z = torch.rand(3, requires_grad=True, device="cuda")
                foo = (t + z).sum()

            for r in grad_generator(foo, [z, y, x]):
                with r.mesh.activate():
                    print(fetch_shard(r).result())

    def test_many(self):
        with self.local_device_mesh(2, 2):
            x = torch.rand(3, 4)
            for _ in range(2048):
                x = x + torch.rand(3, 4)
            fetch_shard(x).result()

    def test_flattener(self):
        e = (8, 9, {"a": 10, "b": 11})
        flatten = flattener(e)
        e2 = (0, 1, {"a": 2, "b": 3})
        assert [0, 1, 2, 3] == flatten(e2)

    def test_torch_tensor(self):
        with self.local_device_mesh(2, 2):
            t = torch.tensor([1, 2, 4])
            tc = torch.tensor([1, 2, 4], device="cuda")
            t2 = fetch_shard(t).result()
            tc2 = fetch_shard(tc).result()
        assert torch.allclose(t2, torch.tensor([1, 2, 4]))
        assert torch.allclose(tc2, torch.tensor([1, 2, 4], device="cpu"))

    def test_to_mesh_aliasing(self):
        with self.local_device_mesh(2, 2) as mesh:
            p2p_stream = Stream("p2p_stream")

            ppmesh = mesh.flatten("all").split(
                all=(
                    "dp",
                    "pp",
                ),
                pp=2,
            )
            pp_meshes = [ppmesh.slice(pp=i) for i in range(2)]

            with ppmesh.activate():
                with pp_meshes[0].activate():
                    x = torch.randn((3, 3), device="cuda")
                    x_borrowed_tensor, x_borrow = p2p_stream.borrow(x)
                    with p2p_stream.activate():
                        y_on_mesh_1_p2p_stream = x_borrowed_tensor.to_mesh(pp_meshes[1])

                with pp_meshes[1].activate():
                    x_borrow.drop()
                    y_on_mesh_1_default_stream, y_borrow = (
                        monarch.get_active_stream().borrow(y_on_mesh_1_p2p_stream)
                    )

                    monarch.inspect(y_on_mesh_1_default_stream)
                    y_borrow.drop()

    def test_to_mesh_cow(self):
        with self.local_device_mesh(2, 2) as mesh:
            t = torch.zeros((), device="cuda")
            t2 = t.to_mesh(mesh)
            t.add_(1)
            assert monarch.inspect(t2).item() == 0
            assert monarch.inspect(t).item() == 1

    def test_to_mesh_stream(self):
        other = monarch.Stream("other")
        with self.local_device_mesh(2, 2) as mesh:
            m0 = mesh.slice(host=0)
            m1 = mesh.slice(host=1)
            with m0.activate():
                t2 = torch.rand(3, 4, device="cuda").to_mesh(m1, stream=other)
            with m1.activate(), other.activate():
                # assert doesn't fail
                monarch.inspect(t2 + t2)

    def test_dropped_trace(self):
        with self.local_device_mesh(2, 2) as _:
            x = torch.rand(4, 4).cuda()
            s = Stream("other")
            b, drop = s.borrow(x)
            drop.drop()
            with s.activate():
                pattern = re.compile(
                    ".*tensor.*is dropped at.*.*drop.drop().*", flags=re.DOTALL
                )
                with pytest.raises(TypeError, match=pattern):
                    _ = b.abs()

    def test_sub_mesh_reduce(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            host1 = device_mesh.slice(host=1)
            with host1.activate():
                myrank = (
                    (device_mesh.rank("host") + 1) * 2 + device_mesh.rank("gpu") + 1
                )
                x = torch.ones((3, 4), device="cuda") * myrank
                reduce = x.reduce("gpu", "sum")
                local_reduce = fetch_shard(reduce).result()

        assert torch.equal(local_reduce, torch.ones(3, 4) * 11)

    def test_size(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            assert device_mesh.size(["host", "gpu"]) == 4

    def test_random_state(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            monarch.random.make_deterministic()
            for device in ("cpu", "cuda"):
                a = monarch.random.get_state()
                monarch.inspect(a)
                first = torch.rand(1, device=device)
                monarch.random.set_state(a)
                second = torch.rand(1, device=device)
                f, s = monarch.inspect((first, second))
                with no_mesh.activate():
                    assert torch.allclose(f, s, atol=0, rtol=1)
                seed = device_mesh.rank(["host", "gpu"]) + 4
                s2 = monarch.random.new_state(seed)
                s3 = monarch.random.new_state(seed)
                monarch.random.set_state(s2)
                r0 = torch.rand(1, device=device)
                if device == "cuda":
                    for d in ("host", "gpu"):
                        r0 = r0.reduce(d, reduction="stack")
                monarch.random.set_state(s3)
                r1 = torch.rand(1, device=device)
                if device == "cuda":
                    for d in ("host", "gpu"):
                        r1 = r1.reduce(d, reduction="stack")
                r2, r3 = monarch.inspect((r0, r1))
                monarch.random.set_state(a)
                with no_mesh.activate():
                    assert torch.allclose(r2, r3, atol=0, rtol=0)
                    assert not torch.allclose(r2, f, atol=0, rtol=0)

    def test_torch_op_with_optional_tensors(self):
        """
        This test ensures that for torch ops like LayerNorm, which allow for
        optional tensor arguments, the controller serializes monarch tensors
        correctly as Refs instead of as IValues.
        """
        with self.local_device_mesh(2, 2):
            x = torch.rand(3, 4, device="cuda")
            # When bias and elementwise_affine are true, extra tensors are passed through optional
            # fields inside LayerNorm. When they are false, None is passed to the same optional fields.
            # If we are handling serialization correctly, there shouldn't be a crash in either case.
            layer_norm_with_vals = torch.nn.LayerNorm(
                4, device="cuda", bias=True, elementwise_affine=True
            )
            layer_norm_with_none = torch.nn.LayerNorm(
                4, device="cuda", bias=False, elementwise_affine=False
            )
            monarch.inspect(layer_norm_with_vals(x))
            monarch.inspect(layer_norm_with_none(x))

    def test_reduce_pytree(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            a = device_mesh.rank(("gpu", "host")) + torch.zeros((1,), device="cuda")
            b = device_mesh.rank(("gpu", "host")) + torch.ones((1,), device="cuda")

            tensor_dict = {"a": a, "b": b}
            _ = monarch.reduce_(tensor_dict, dims=("gpu", "host"), reduction="sum")
            reduced_tensor_dict = monarch.reduce(
                tensor_dict, dims=("gpu", "host"), reduction="sum"
            )
            reduced_a = fetch_shard(reduced_tensor_dict["a"]).result()
            reduced_b = fetch_shard(reduced_tensor_dict["b"]).result()
            reduced_a_inplace = fetch_shard(tensor_dict["a"]).result()
            reduced_b_inplace = fetch_shard(tensor_dict["b"]).result()

        assert torch.equal(reduced_a_inplace, torch.tensor([6.0]))
        assert torch.equal(reduced_b_inplace, torch.tensor([10.0]))
        assert torch.equal(reduced_a, torch.tensor([24.0]))
        assert torch.equal(reduced_b, torch.tensor([40.0]))

    def test_to_mesh_pytree(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            host0 = device_mesh.slice(host=0)
            host1 = device_mesh.slice(host=1)

            with host0.activate():
                a = torch.zeros((1,), device="cuda")
                b = torch.ones((1,), device="cuda")
                tensor_dict = {"a": a, "b": b}
                moved_tensor_dict = monarch.to_mesh(tensor_dict, host1)

            with host1.activate():
                moved_tensor_dict["a"].add_(1)
                moved_tensor_dict["b"].add_(1)

            moved_tensor_a = monarch.inspect(moved_tensor_dict["a"])
            moved_tensor_b = monarch.inspect(moved_tensor_dict["b"])

            host0.exit()
            host1.exit()

        assert torch.equal(moved_tensor_a, torch.tensor([1.0]))
        assert torch.equal(moved_tensor_b, torch.tensor([2.0]))

    def test_hanging_error(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            remote(lambda: torch.rand(3) + torch.rand(4), propagate=lambda: None)()

            with pytest.raises(Exception, match="The size of tensor"):
                device_mesh.client.shutdown()

    def test_slice_mesh_pytree(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            a = device_mesh.rank(("host")) + torch.zeros((1,), device="cuda")
            b = device_mesh.rank(("host")) + torch.ones((1,), device="cuda")

            tensor_dict = {"a": a, "b": b}
            host0_slices = monarch.slice_mesh(tensor_dict, host=0)
            host1_slices = monarch.slice_mesh(tensor_dict, host=1)

            host0 = device_mesh.slice(host=0)
            host1 = device_mesh.slice(host=1)

            host0_tensors = monarch.to_mesh(host0_slices, host0)
            host1_tensors = monarch.to_mesh(host1_slices, host1)

            with host0.activate():
                _ = monarch.reduce_(host0_tensors, dims=("gpu"), reduction="sum")
                host0_a = fetch_shard(host0_tensors["a"]).result()
                host0_b = fetch_shard(host0_tensors["b"]).result()

            with host1.activate():
                _ = monarch.reduce_(host1_tensors, dims=("gpu"), reduction="sum")
                host1_a = fetch_shard(host1_tensors["a"]).result()
                host1_b = fetch_shard(host1_tensors["b"]).result()

            host0.exit()
            host1.exit()

        assert torch.equal(host0_a, torch.tensor([0.0]))
        assert torch.equal(host0_b, torch.tensor([2.0]))
        assert torch.equal(host1_a, torch.tensor([2.0]))
        assert torch.equal(host1_b, torch.tensor([4.0]))


def a_function_called_by_a_live_function(x):
    return 2 * x


def a_live_function_call_by_a_live_function(x):
    return 3 * x
