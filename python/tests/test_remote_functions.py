# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import itertools
import math
import sys
import traceback
from typing import ContextManager, Tuple
from unittest.mock import patch

import monarch
import pytest
import torch
from monarch import fetch_shard, inspect, OpaqueRef, remote, Stream
from monarch._testing import TestingContext
from monarch.builtins.log import log_remote
from monarch.builtins.random import set_manual_seed_remote
from monarch.cached_remote_function import remote_autograd_function
from monarch.common import remote as remote_module
from monarch.common.device_mesh import DeviceMesh, no_mesh
from monarch.common.remote import call_on_shard_and_fetch
from monarch.mesh_controller import RemoteException
from monarch.opaque_module import OpaqueModule
from monarch.opaque_object import opaque_method, OpaqueObject
from monarch_supervisor.logging import fix_exception_lines


def custom_excepthook(exc_type, exc_value, exc_traceback):
    tb_lines = fix_exception_lines(
        traceback.format_exception(exc_type, exc_value, exc_traceback)
    )
    print("\n".join(tb_lines), file=sys.stderr)


sys.excepthook = custom_excepthook


def _set_device_udf(*args):
    return torch.zeros(1)


set_device_udf = remote(
    "monarch.worker._testing_function.set_device_udf_worker", propagate=_set_device_udf
)

rlist = remote("builtins.list", propagate=lambda elem: elem)


def _do_bogus_tensor_work(x, y, fail_rank=None):
    return x + y  # real function actually does x @ y


do_bogus_tensor_work = remote(
    "monarch.worker._testing_function.do_bogus_tensor_work",
    propagate=_do_bogus_tensor_work,
)


sleep = remote("monarch.worker._testing_function.remote_sleep", propagate="inspect")

new_barrier_hackery = remote(
    "monarch.worker._testing_function.new_barrier_hackery",
    propagate=lambda threads: torch.zeros(1),
)

wait_barrier_hackery = remote(
    "monarch.worker._testing_function.wait_barrier_hackery",
    propagate=lambda t: None,
)

setup_state = remote(
    "monarch.worker._testing_function.setup_state_worker",
    propagate=lambda: [OpaqueRef(None) for _ in range(4)],
)

iteration = remote(
    "monarch.worker._testing_function.iteration_worker",
    propagate=lambda model, dataloader, criterion, optimizer, pg: torch.zeros(1),
)

opaque_ref_key_table_length = remote(
    "monarch.worker._testing_function.opaque_ref_key_table_length_worker",
    propagate=lambda: torch.zeros(1),
)

create_opaque_ref = remote(
    "monarch.worker._testing_function.create_opaque_ref_worker",
    propagate=lambda: OpaqueRef(None),
)

outer_remote_function_that_calls_inner = remote(
    "monarch.worker._testing_function.outer_remote_function_that_calls_inner",
    propagate=lambda: torch.zeros(1),
)


@pytest.fixture(scope="module", autouse=True)
def testing_context():
    global local
    with TestingContext() as local:
        yield


class RemoteFunctionsTestBase:
    @classmethod
    def local_device_mesh(
        cls,
        num_hosts: int,
        gpu_per_host: int,
        activate: bool = True,
    ) -> ContextManager[DeviceMesh]:
        # pyre-fixme[10]: pytest defines this fixture.
        return local.local_device_mesh(
            num_hosts,
            gpu_per_host,
            activate,
        )


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
# Set global timeout--sandcastle's timeout is 600s. A test that sandcastle times
# out is not counted as a failure, so we set a more restrictive timeout to
# ensure we see a hard failure in CI.
@pytest.mark.timeout(120)
class TestRemoteFunctions(RemoteFunctionsTestBase):
    def test_hello(self):
        with self.local_device_mesh(2, 2):
            log_remote("hello, world")

    def test_eager_remote_function_failed(self):
        with self.local_device_mesh(1, 2) as _:
            x = torch.rand(3, 4)
            y = torch.rand(3, 4)
            z = do_bogus_tensor_work(x, y, fail_rank=1)
            a = z + x
            with pytest.raises(RemoteException, match="do_bogus_tensor_work"):
                # NCCL init is slow, and fails on internal RE!
                _ = fetch_shard(a).result(timeout=40)

    def test_set_device_inside_udf_fails_with_explanation(self):
        with self.local_device_mesh(2, 2):
            t = set_device_udf(2)
            try:
                inspect(t)
            except RemoteException as e:
                if isinstance(e, RemoteException):
                    backtrace = e.worker_error_string
                else:
                    backtrace = "\n".join([frame.name for frame in e.worker_frames])
                assert "are available to monarch worker" in backtrace

    def test_simple_tensors(self):
        with self.local_device_mesh(2, 2):
            x = torch.rand(3, 4)
            y = x + x
            log_remote("%s %s", x, y)
            z = torch.std_mean(x)
            log_remote("%s", z)

    def test_user_call(self):
        with self.local_device_mesh(2, 2) as _:
            x = torch.rand(3, 4)
            y = rlist((x + 1, x))
            log_remote("%s", y)

            # resume monday:
            # 1. tensor ctor resource guard (done)
            # 2. __torch_dispatch__ forward of normal ops (done)
            # 3. collectives created for device mesh
            # 4. implement comms APIs
            # 5. transfer tensor back, and simple future to wait for result.

    def test_remote_exception(self):
        with self.local_device_mesh(2, 2) as _:
            x = torch.rand(3, 4)
            y = torch.rand(3, 4)
            z = do_bogus_tensor_work(x, y)
            a = z + x
            b = x + y
            with pytest.raises(RemoteException, match="do_bogus_tensor_work"):
                # NCCL init is slow, and fails on internal RE!
                _ = fetch_shard(a).result(timeout=20)
            # but values not dependent on z are fine
            fetch_shard(b).result(timeout=10)

    def test_distributed_error(self):
        with self.local_device_mesh(2, 2) as _:
            x = torch.rand(3, 4).cuda()
            y = torch.rand(3, 4).cuda()
            # z is broken on rank 1 but not others
            z = do_bogus_tensor_work(x, y, fail_rank=1)
            # test that rank 1 is still doing work despite z failing
            a = (x + y).reduce("gpu")
            fetch_shard(a).result()
            # but z itself should fail, even if we do not fetch it from rank 1
            # (since fetch shard says we first want to assert the whole tensor is correct)
            with pytest.raises(RemoteException, match="do_bogus_tensor_work"):
                fetch_shard(z).result()
            # try to reduce z, which should fail, but ranks that are not 1 do not
            # know about the failure. Rank 1 should still participate in the reduce
            # to unblock work.
            rz = z.reduce("gpu")
            # but we should see the error message still retrieving it because it is
            # dependent on an error.
            with pytest.raises(RemoteException, match="do_bogus_tensor_work"):
                fetch_shard(rz).result()
            # however, we should still be able to compute and get a result back
            # from host 1, signaling that the reduction didn't get cuda compute stuck.
            fetch_shard(2 * x, gpu=1, host=0).result()

    def test_streams_run_parallel(self):
        with self.local_device_mesh(2, 2):
            # test that these two streams do in fact run in parallel
            # on the worker by having each stream wait on a barrier.
            # The Tensor t is just used as a data-dependency so that
            # we can make sure new_barrier_hackery is called before
            # the wait on 'other'.
            other = Stream("other")
            t = new_barrier_hackery(2)
            t_other, borrow = other.borrow(t)
            with borrow:
                with other.activate():
                    wait_barrier_hackery(t_other)
                wait_barrier_hackery(t)
            fetch_shard(t).result()

    def test_debug(self):
        gonna_pdb = remote(
            "monarch.worker._testing_function.gonna_pdb", propagate="inspect"
        )

        with self.local_device_mesh(2, 2):
            writes = []

            def dw(s):
                writes.append(s)

            def dr(n):
                buffer = "".join(["print(x)\n", "c\n"]).encode()
                assert len(buffer) <= n
                return buffer

            patch_read = patch("monarch.controller.debugger.read", new=dr)
            patch_write = patch("monarch.controller.debugger.write", new=dw)
            with patch_read, patch_write:
                gonna_pdb()
                # xxx: we do not process messages from workers
                # unless fetching a result
                fetch_shard(None).result()
                assert "".join(writes).count("7\n") == 4

    def test_fetch_preprocess(self):
        with self.local_device_mesh(2, 2):
            assert (
                "an argument processed"
                == call_on_shard_and_fetch(
                    remote("monarch.worker._testing_function.do_some_processing"),
                    "an argument",
                ).result()
            )

    def test_cached_remote_function(self):
        fn = remote("monarch.worker._testing_function.how_many_of_these_do_you_want")
        start_hits = remote_module._hit
        with self.local_device_mesh(2, 2):
            x = torch.ones(3, 4)
            y = torch.rand(3, 4)

            a, _, _ = fn(3, x)
            b, _, _ = fn(3, x)
            assert len(a._aliases.aliases) == 1
            assert len(b._aliases.aliases) == 1
            _, _, _ = fn(3, y)
            t0, t1 = fn(2, x)
            t0.add(t1)
            local_a = fetch_shard(a).result()
            with no_mesh.activate():
                assert torch.all(local_a == 1.0)

            end_hits = remote_module._hit
            assert end_hits - start_hits == 2

    def test_remote_autograd_function(self):
        from monarch.worker import _testing_function

        remote_fn = remote_autograd_function(
            _testing_function.TestRemoteAutogradFunction
        )

        with self.local_device_mesh(1, 1):
            x = torch.ones(1, requires_grad=True)
            y = torch.ones_like(x).requires_grad_(True)
            outs = remote_fn.apply(x, y)
            assert outs[3] == 4
            local_0 = fetch_shard(outs[0]).result()
            local_1 = fetch_shard(outs[1]).result()
            (outs[0] + outs[1]).sum().backward()
            # unfortunately, grad_fn of local tensor is always None
            # regardless of whether we set `no_grad` on the worker
            # so we can test only requires_grad
            for ll in (local_0, local_1):
                assert not ll.requires_grad
            grad_local_0 = fetch_shard(x.grad).result()
            grad_local_1 = fetch_shard(x.grad).result()
            x = x.detach()
            x.grad = None
            y.grad = None
            outs = remote_fn.apply(x, y)
            local_0_f = fetch_shard(outs[0]).result()
            (outs[0] + outs[1]).sum().backward()
            assert x.grad is None
            grad_local_1_f = fetch_shard(y.grad).result()

        assert torch.equal(local_0_f, torch.full_like(local_0_f, 2))
        assert torch.equal(local_0, torch.ones_like(local_0))
        assert torch.equal(grad_local_0, torch.ones_like(local_0))
        assert torch.equal(grad_local_1, torch.ones_like(local_0))
        assert torch.equal(grad_local_1_f, torch.ones_like(local_0))

    def test_cached_remote_aliases(self):
        fn = remote("monarch.worker._testing_function.remote_chunk")
        with self.local_device_mesh(1, 1):
            x = torch.randn(16, 5, device="cuda")
            outs = fn(x)
            aliases = outs[0]._aliases.aliases
            # x and 4 results of x.chunk(4)
            assert len(aliases) == 5
            assert outs[2]._fake.storage_offset() == 40

    def test_live_function(self):
        def bar(x, y):
            return (
                a_function_called_by_a_live_function(x)
                + a_live_function_call_by_a_live_function(y)
                + math.pi
            )

        @remote
        def check(x):
            return torch.allclose(x, torch.zeros(()) + math.pi + 5)

        y = 7

        @monarch.remote
        def close():
            return y

        @monarch.remote
        def cuda_works(x):
            return x.cuda()

        with self.local_device_mesh(2, 2):
            a = torch.ones(())
            assert call_on_shard_and_fetch(check, bar(a, a)).result()
            # ensure we do not attempt to pickle closures
            close()

            b = cuda_works(a)
            fetch_shard(b).result()

            @monarch.remote
            def something_else():
                raise Exception("No")  # this line appears

            # check that the stack trace has correct line numbers
            with pytest.raises(Exception, match=r"this line appears"):
                something_else()

    def test_setting_random_seed(self):
        with self.local_device_mesh(2, 2):
            set_manual_seed_remote(12345)
            t = torch.randn(3, 4)
            t_d = torch.randn(3, 4, device="cuda")
            ref = fetch_shard(t).result()
            ref_d = fetch_shard(t_d).result()
            vals = {
                (h, d): fetch_shard(t, {"host": h, "gpu": d}).result()
                for h, d in itertools.product(range(2), repeat=2)
            }

            vals_d = {
                (h, d): fetch_shard(t_d, {"host": h, "gpu": d}).result()
                for h, d in itertools.product(range(2), repeat=2)
            }

        for v, v_d in zip(vals.values(), vals_d.values()):
            assert torch.equal(v, ref)
            assert torch.equal(v_d, ref_d)

    def test_return_exception(self):
        @monarch.remote
        def simple():
            return Exception("is a valid value to return")

        with self.local_device_mesh(1, 1):
            # This should be a valid return than an exception to raise
            call_on_shard_and_fetch(simple).result()

    def test_opaque_object(self):
        with self.local_device_mesh(2, 2):

            class Foo(OpaqueObject):
                @opaque_method
                def add(self, x: torch.Tensor):
                    return x + x

            f = Foo("monarch.worker._testing_function.WorkerFoo", 4.0)

            result = monarch.inspect(f.add(torch.ones(3, 4)))
            with monarch.no_mesh.activate():
                assert torch.allclose(torch.full((3, 4), 5.0), result)

            f.hi = 4
            assert f.hi == 4

    def test_opaqueRef_key_deleted(self):
        with self.local_device_mesh(1, 1):
            ref = create_opaque_ref()
            assert inspect(opaque_ref_key_table_length()).item() == 1
            del ref
            assert inspect(opaque_ref_key_table_length()).item() == 0

    def test_opaque_module(self):
        with self.local_device_mesh(2, 2):
            linear = OpaqueModule("torch.nn.Linear", 3, 3, device="cuda")
            with torch.no_grad():
                for p in linear.parameters():
                    p.zero_()
            input_ = torch.rand(4, 3, device="cuda")
            # we should have been able to clear the parameters and have that result
            # affect how the linear works.
            output = linear.call_method("forward", lambda self, x: x.clone(), input_)
            assert monarch.inspect(output.sum()).item() == 0

    def test_opaque_module_autograd(self):
        with self.local_device_mesh(2, 2):
            input_ = torch.rand(3, 3, device="cuda", requires_grad=True)

            linear = OpaqueModule("torch.nn.Linear", 3, 3, device="cuda")
            output = linear(input_, propagator=lambda self, x: x.clone())
            r = output.sum()
            with torch.no_grad():
                r.backward()

            weight, bias = linear.parameters()
            ig0, wg0, bg0 = monarch.inspect((input_.grad, weight.grad, bias.grad))

            input_.grad = None
            weight.grad = None
            bias.grad = None

            (input_ @ weight.T + bias).sum().backward()

            ig1, wg1, bg1 = monarch.inspect((input_.grad, weight.grad, bias.grad))

            with monarch.no_mesh.activate():
                assert torch.allclose(ig0, ig1)
                assert torch.allclose(wg0, wg1)
                assert torch.allclose(bg0, bg1)

    def test_remote_function_failure_message_contains_traceback(self):
        with self.local_device_mesh(2, 2):
            x = outer_remote_function_that_calls_inner()
            try:
                inspect(x)
            except RemoteException as e:
                assert "outer_remote_function" in e.worker_error_string
                assert "inner_remote_function" in e.worker_error_string


def a_function_called_by_a_live_function(x):
    return 2 * x


def a_live_function_call_by_a_live_function(x):
    return 3 * x


@remote
def return_them(x: torch.Tensor, y: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
    return (x, y)


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
class TestMeshSpecific(RemoteFunctionsTestBase):
    def test_value_mesh(self):
        with self.local_device_mesh(2, 2, "mesh") as device_mesh:
            x = device_mesh.rank("host")
            y = device_mesh.rank("gpu")
            r = return_them.call(x, y).get()

            for p, (h, g) in r:
                assert p["host"] == h.item()
                assert p["gpu"] == g.item()
