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
from typing import Callable, ContextManager, Tuple
from unittest.mock import patch

import monarch
import pytest

import torch
from monarch import (
    fetch_shard,
    inspect,
    no_mesh,
    OpaqueRef,
    Pipe,
    remote,
    remote_generator,
    RemoteException as OldRemoteException,
    Stream,
)

from monarch._testing import TestingContext
from monarch.builtins.log import log_remote
from monarch.builtins.random import set_manual_seed_remote
from monarch.cached_remote_function import remote_autograd_function
from monarch.common import remote as remote_module
from monarch.common.device_mesh import DeviceMesh
from monarch.common.remote import call_on_shard_and_fetch, Remote
from monarch.mesh_controller import RemoteException as NewRemoteException

from monarch.opaque_module import OpaqueModule
from monarch.opaque_object import opaque_method, OpaqueObject
from monarch.worker._testing_function import (
    all_gather,
    all_gather_into_tensor,
    all_reduce,
    all_to_all,
    all_to_all_single,
    barrier,
    broadcast,
    gather,
    irecv,
    isend,
    reduce,
    reduce_scatter,
    reduce_scatter_tensor,
    scatter,
)
from monarch_supervisor.logging import fix_exception_lines
from torch.distributed import ReduceOp

RemoteException = (NewRemoteException, OldRemoteException)


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


@remote_generator("monarch.worker._testing_function.example_echo_add")
def example_echo_add(p: "Pipe"):
    while True:
        yield p.recv() + 1


@remote_generator("monarch.worker._testing_function.example_data_loader")
def example_data_loader(p: "Pipe", x, y):
    for _i in range(x, y):
        yield torch.zeros(())


@remote_generator(
    "monarch.worker._testing_function.example_data_loader_small_pipe",
    max_messages=1,
)
def example_data_loader_small_pipe(p: "Pipe", iters: int, shape: Tuple[int, int]):
    for _i in range(iters):
        yield torch.zeros(shape)


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
    @classmethod
    def do_test_reduce_scatter_tensor(cls, reduce_op, expected_tensor):
        n_gpus = 2
        with cls.local_device_mesh(2, n_gpus) as device_mesh:
            rank = device_mesh.rank("host") * n_gpus + device_mesh.rank("gpu")
            tensor_in = rank * torch.arange(0, 8, device="cuda", dtype=float).reshape(
                4, 2
            )
            tensor_out = torch.arange(2, device="cuda", dtype=float)
            pg = device_mesh.process_group(("host", "gpu"))

            reduce_scatter_tensor(tensor_out, tensor_in, op=reduce_op, group=pg)

            for host in range(2):
                for gpu in range(n_gpus):
                    rank = 2 * host + gpu
                    local_tensor_out = inspect(tensor_out, {"host": host, "gpu": gpu})
                    with no_mesh.activate():
                        assert torch.equal(
                            local_tensor_out,
                            expected_tensor[rank],
                        )

    @classmethod
    def do_test_reduce_scatter_tensor_subgroup(
        cls,
        reduce_op,
        expected_tensor_host_group: torch.Tensor,
        expected_tensor_gpu_group: torch.Tensor,
    ) -> None:
        n_gpus = 2
        with cls.local_device_mesh(2, n_gpus) as device_mesh:
            # Use a group smaller than the world size.
            host_pg = device_mesh.process_group("host")
            gpu_pg = device_mesh.process_group("gpu")
            # host_rank = device_mesh.rank("host")
            # gpu_rank = device_mesh.rank("gpu")
            rank = device_mesh.rank(("host", "gpu"))

            tensor_in = rank * torch.arange(
                0, 8, device="cuda", dtype=torch.float32
            ).reshape(4, 2)

            gpu_tensor_out = torch.zeros(4, device="cuda", dtype=torch.float32)
            reduce_scatter_tensor(gpu_tensor_out, tensor_in, op=reduce_op, group=gpu_pg)

            tensor_in = rank * torch.arange(
                0, 8, device="cuda", dtype=torch.float32
            ).reshape(4, 2)
            host_tensor_out = torch.zeros(4, device="cuda", dtype=torch.float32)
            reduce_scatter_tensor(
                host_tensor_out, tensor_in, op=reduce_op, group=host_pg
            )

            for host in range(2):
                for gpu in range(n_gpus):
                    rank = host * 2 + gpu
                    local_gpu_tensor_out = inspect(
                        gpu_tensor_out, {"host": host, "gpu": gpu}
                    )
                    local_host_tensor_out = inspect(
                        host_tensor_out, {"host": host, "gpu": gpu}
                    )
                    with no_mesh.activate():
                        assert torch.equal(
                            local_host_tensor_out,
                            expected_tensor_host_group[rank],
                        ), f"{rank=}, {host=}, {gpu=}"
                        assert torch.equal(
                            local_gpu_tensor_out,
                            expected_tensor_gpu_group[rank],
                        ), f"{rank=}, {host=}, {gpu=}"

    @classmethod
    def do_test_reduce_scatter(
        cls,
        reduce_op: ReduceOp,
        expected_tensor: torch.Tensor,
    ) -> None:
        n_gpus = 2
        with cls.local_device_mesh(2, n_gpus) as device_mesh:
            rank = device_mesh.rank("host") * n_gpus + device_mesh.rank("gpu")
            tensor_in = rank * torch.arange(0, 8, device="cuda", dtype=torch.float32)
            tensor_out = torch.arange(2, device="cuda", dtype=torch.float32)
            pg = device_mesh.process_group(("host", "gpu"))

            tensor_out = reduce_scatter(
                tensor_out,
                list(torch.chunk(tensor_in, 2 * n_gpus)),
                op=reduce_op,
                group=pg,
            )

            for host in range(2):
                for gpu in range(n_gpus):
                    rank = 2 * host + gpu
                    local_tensor_out = inspect(tensor_out, {"host": host, "gpu": gpu})
                    with no_mesh.activate():
                        assert torch.equal(
                            local_tensor_out,
                            expected_tensor[rank],
                        )

    @classmethod
    def do_test_all_reduce(cls, reduce_op, expected_tensor):
        n_gpus = 2
        with cls.local_device_mesh(2, n_gpus) as device_mesh:
            rank = device_mesh.rank(("host", "gpu"))
            tensor_in = rank * torch.arange(0, 8, device="cuda", dtype=float).reshape(
                4, 2
            )
            pg = device_mesh.process_group(("host", "gpu"))

            tensor_out = all_reduce(tensor_in, op=reduce_op, group=pg)

            for host in range(2):
                for gpu in range(n_gpus):
                    local_tensor_out = inspect(tensor_out, {"host": host, "gpu": gpu})
                    with no_mesh.activate():
                        assert torch.equal(
                            local_tensor_out,
                            expected_tensor,
                        )

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

    def test_remote_function_with_comms_full_mesh(self):
        nGPUs = 2
        with self.local_device_mesh(2, nGPUs) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            myrank = (
                (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank("gpu") + 1
            )
            x = torch.ones((3, 4), device="cuda") * myrank

            reduce = all_reduce(x, group=pg)
            local_reduce = fetch_shard(reduce).result()
        assert torch.equal(local_reduce, torch.ones(3, 4) * 18)

    def test_remote_function_with_comms_by_dimension(self):
        nGPUs = 2
        with self.local_device_mesh(2, nGPUs) as device_mesh:
            pg = device_mesh.process_group(("gpu",))
            myrank = (
                (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank("gpu") + 1
            )
            x = torch.ones((3, 4), device="cuda") * myrank
            reduce = all_reduce(x, group=pg)
            local_reduce_host_0 = fetch_shard(reduce).result()
            local_reduce_host_1 = fetch_shard(reduce, {"gpu": 1, "host": 1}).result()
        assert torch.equal(local_reduce_host_0, torch.ones(3, 4) * 7)
        assert torch.equal(local_reduce_host_1, torch.ones(3, 4) * 11)

        with self.local_device_mesh(2, nGPUs) as device_mesh:
            pg = device_mesh.process_group(("host",))
            myrank = (
                (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank("gpu") + 1
            )
            x = torch.ones((3, 4), device="cuda") * myrank
            reduce = all_reduce(x, group=pg)
            local_reduce_gpu_0 = fetch_shard(reduce).result()
            local_reduce_gpu_2 = fetch_shard(reduce, {"gpu": 1, "host": 0}).result()
        assert torch.equal(local_reduce_gpu_0, torch.ones(3, 4) * 8)

        assert torch.equal(local_reduce_gpu_2, torch.ones(3, 4) * 10)

    def test_remote_function_with_comms_sub_mesh(self):
        nGPUs = 2
        with self.local_device_mesh(2, nGPUs, activate=False) as device_mesh:
            host1 = device_mesh(host=1)
            with host1.activate():
                pg = device_mesh.process_group(("gpu",))
                myrank = (
                    (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank("gpu") + 1
                )
                x = torch.ones((3, 4), device="cuda") * myrank
                reduce = all_reduce(x, group=pg)
                local_reduce = fetch_shard(reduce).result()

            assert torch.equal(local_reduce, torch.ones(3, 4) * 11)

            host0 = device_mesh(host=0)
            with host0.activate():
                pg = device_mesh.process_group(("gpu",))
                myrank = (
                    (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank("gpu") + 1
                )
                x = torch.ones((3, 4), device="cuda") * myrank
                reduce = all_reduce(x, group=pg)
                local_reduce = fetch_shard(reduce).result()

            assert torch.equal(local_reduce, torch.ones(3, 4) * 7)

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

    def test_remote_function_barrier(self):
        nGPUs = 2
        with self.local_device_mesh(2, nGPUs) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            finished = barrier(group=pg)
            local = fetch_shard(finished).result()
        assert local.item() == 1.0

    def test_remote_function_all_gather(self) -> None:
        nGPUs = 2
        with self.local_device_mesh(2, nGPUs) as device_mesh:
            myrank = (
                (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank("gpu") + 1
            )
            # Don't start at zero to ensure there are no leftover zeros.
            tensor_in = torch.arange(1, 3, device="cuda") * myrank
            world_size = 2 * nGPUs
            tensor_out = list(
                torch.zeros(2 * world_size, dtype=torch.int64, device="cuda").chunk(
                    world_size
                )
            )
            pg = device_mesh.process_group(("host", "gpu"))

            tensor_out = all_gather(tensor_out, tensor_in, group=pg)
            local_tensor_out = inspect(tensor_out)

        t0, t1, t2, t3 = local_tensor_out
        assert torch.equal(t0, torch.tensor([3, 6]))
        assert torch.equal(t1, torch.tensor([4, 8]))
        assert torch.equal(t2, torch.tensor([5, 10]))
        assert torch.equal(t3, torch.tensor([6, 12]))

    def test_remote_function_all_gather_into_tensor(self):
        nGPUs = 2
        with self.local_device_mesh(2, nGPUs) as device_mesh:
            myrank = (
                (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank("gpu") + 1
            )
            # Don't start at zero to ensure there are no leftover zeros.
            tensor_in = torch.arange(1, 3, device="cuda") * myrank
            tensor_out = torch.zeros(2 * nGPUs * 2, dtype=torch.int64, device="cuda")
            pg = device_mesh.process_group(("host", "gpu"))

            finished = all_gather_into_tensor(tensor_out, tensor_in, group=pg)
            local_finished = inspect(finished)
            local_tensor_out = inspect(tensor_out)

        assert local_finished.item() == 1.0
        assert torch.equal(local_tensor_out, torch.tensor([3, 6, 4, 8, 5, 10, 6, 12]))

    def test_remote_function_isend(self):
        nGPUs = 2
        with self.local_device_mesh(2, nGPUs) as device_mesh:
            pg = device_mesh.process_group(("host",))
            host_0_mesh = device_mesh(host=0)
            host_1_mesh = device_mesh(host=1)
            with host_0_mesh.activate():
                to_rank = (device_mesh.rank("host") + 1) * nGPUs + device_mesh.rank(
                    "gpu"
                )
                t0 = torch.ones(1, device="cuda")
                finished0 = isend(t0, to_rank, group=pg)
            with host_1_mesh.activate():
                from_rank = (device_mesh.rank("host") - 1) * nGPUs + device_mesh.rank(
                    "gpu"
                )
                t1 = torch.zeros(1, device="cuda")
                finished1 = irecv(t1, from_rank, group=pg)

            with host_0_mesh.activate():
                local_finished_0 = inspect(finished0)
            with host_1_mesh.activate():
                local_finished_1 = inspect(finished1)
        assert local_finished_0.item() == 1.0
        assert local_finished_1.item() == 1.0

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

    def test_pipe(self):
        with self.local_device_mesh(2, 2):
            p = example_echo_add()
            for _i in range(10):
                x = torch.rand(3, 4)
                p.send(x)
                y = p.recv()
                x, y = fetch_shard((x, y)).result()
                with no_mesh.activate():
                    assert torch.allclose(x + 1, y)

    def test_loader(self):
        with self.local_device_mesh(2, 2):
            p = example_data_loader(3, 7)
            for i in range(3, 7):
                x = fetch_shard(p.recv()).result()
                with no_mesh.activate():
                    assert x.item() == i

    def test_loader_blocks_with_small_pipe(self):
        with self.local_device_mesh(2, 2):
            iters = 10
            p = example_data_loader_small_pipe(iters, (1000, 1000))
            # timeout should proc on pipe process
            sleep(0.6)
            # it takes a few iters of reasonably sized tensors to fill up OS buffer
            # max_messages (SNDHWM) only affects the zmq buffer
            for _ in range(iters - 1):
                p.recv()
            t = fetch_shard(p.recv()).result()
        assert t[0][0].item() == -1.0

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

    def test_opaqueRef_setup_state_and_iteration(self):
        with self.local_device_mesh(1, 2) as mesh:
            pg = mesh.process_group(("gpu",))
            model, dataloader, criterion, optimizer = setup_state()
            num_epochs = 5
            for _ in range(num_epochs):
                loss = iteration(model, dataloader, criterion, optimizer, pg)
                assert inspect(loss).item() > 0

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

    def test_remote_function_reduce_scatter_tensor_sum(self):
        self.do_test_reduce_scatter_tensor(
            "mesh",
            torch.distributed.ReduceOp.SUM,
            (
                torch.arange(0, 8, dtype=float).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=float).unsqueeze(-1).unsqueeze(-1)
            ).sum(0),
        )

    def test_remote_function_reduce_scatter_tensor_subgroup_sum(self) -> None:
        self.do_test_reduce_scatter_tensor_subgroup(
            torch.distributed.ReduceOp.SUM,
            expected_tensor_host_group=torch.tensor(
                [[0, 2, 4, 6], [0, 4, 8, 12], [8, 10, 12, 14], [16, 20, 24, 28]],
                dtype=torch.float32,
            ),
            expected_tensor_gpu_group=torch.tensor(
                [[0, 1, 2, 3], [4, 5, 6, 7], [0, 5, 10, 15], [20, 25, 30, 35]],
                dtype=torch.float32,
            ),
        )

    def test_remote_function_reduce_scatter_tensor_avg(self):
        self.do_test_reduce_scatter_tensor(
            "mesh",
            torch.distributed.ReduceOp.AVG,
            (
                torch.arange(0, 8, dtype=float).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=float).unsqueeze(-1).unsqueeze(-1)
            ).mean(0),
        )

    def test_remote_function_reduce_scatter_sum(self) -> None:
        self.do_test_reduce_scatter(
            torch.distributed.ReduceOp.SUM,
            (
                torch.arange(0, 8, dtype=torch.float32).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=torch.float32).unsqueeze(-1).unsqueeze(-1)
            ).sum(0),
        )

    def test_remote_function_reduce_scatter_avg(self) -> None:
        self.do_test_reduce_scatter(
            torch.distributed.ReduceOp.AVG,
            (
                torch.arange(0, 8, dtype=torch.float32).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=torch.float32).unsqueeze(-1).unsqueeze(-1)
            ).mean(0),
        )

    def test_remote_function_all_reduce_sum(self):
        self.do_test_all_reduce(
            "mesh",
            torch.distributed.ReduceOp.SUM,
            (
                torch.arange(0, 8, dtype=float).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=float).unsqueeze(-1).unsqueeze(-1)
            ).sum(0),
        )

    def test_remote_function_all_reduce_avg(self):
        self.do_test_all_reduce(
            torch.distributed.ReduceOp.AVG,
            (
                torch.arange(0, 8, dtype=float).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=float).unsqueeze(-1).unsqueeze(-1)
            ).mean(0),
        )

    def test_remote_function_all_reduce_max(self):
        self.do_test_all_reduce(
            torch.distributed.ReduceOp.MAX,
            (
                torch.arange(0, 8, dtype=float).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=float).unsqueeze(-1).unsqueeze(-1)
            ).max(0)[0],
        )

    def test_remote_function_all_reduce_min(self):
        self.do_test_all_reduce(
            torch.distributed.ReduceOp.MIN,
            (
                torch.arange(0, 8, dtype=float).reshape(1, 4, 2).repeat(4, 1, 1)
                * torch.arange(4, dtype=float).unsqueeze(-1).unsqueeze(-1)
            ).min(0)[0],
        )

    def test_remote_function_failure_message_contains_traceback(self):
        with self.local_device_mesh(2, 2):
            x = outer_remote_function_that_calls_inner()
            try:
                inspect(x)
            except OldRemoteException as e:
                backtrace = "\n".join([frame.name for frame in e.worker_frames])
                assert "outer_remote_function" in backtrace
                assert "inner_remote_function" in backtrace
            except NewRemoteException as e:
                assert "outer_remote_function" in e.worker_error_string
                assert "inner_remote_function" in e.worker_error_string

    def test_remote_function_broadcast(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            for i in range(4):
                rank = 2 * device_mesh.rank("host") + device_mesh.rank("gpu")
                rank = rank.cuda()
                broadcast(rank, src=i, group=pg)
                for host in range(2):
                    for gpu in range(2):
                        with no_mesh.activate():
                            assert inspect(rank, {"host": host, "gpu": gpu}).item() == i

    def test_remote_function_all_to_all_single(self):
        with self.local_device_mesh(2, 2) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            tensor_in = torch.arange(4, device="cuda", dtype=float)
            tensor_out = torch.empty(4, device="cuda", dtype=float)
            all_to_all_single(tensor_out, tensor_in, group=pg)
            for host in range(2):
                for gpu in range(2):
                    rank = 2 * host + gpu
                    with no_mesh.activate():
                        assert torch.equal(
                            inspect(tensor_out, {"host": host, "gpu": gpu}),
                            rank * torch.ones(4),
                        )

    def test_remote_function_all_to_all(self) -> None:
        world_size = 2
        n_gpus = 2
        size = world_size * n_gpus
        expected_tensors = [
            torch.tensor([0, 4, 8, 12], dtype=torch.float32),
            torch.tensor([1, 5, 9, 13], dtype=torch.float32),
            torch.tensor([2, 6, 10, 14], dtype=torch.float32),
            torch.tensor([3, 7, 11, 15], dtype=torch.float32),
        ]

        with self.local_device_mesh(world_size, n_gpus) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            rank = n_gpus * device_mesh.rank("host") + device_mesh.rank("gpu")
            in_tensors = list(
                torch.chunk(
                    torch.arange(size, device="cuda", dtype=torch.float32)
                    + (rank * size),
                    size,
                )
            )
            # These values will be replaced, just used for shape.
            out_tensors = list(torch.zeros(size, device="cuda").chunk(size))
            out_tensors = all_to_all(out_tensors, in_tensors, group=pg)
            for host in range(world_size):
                for gpu in range(n_gpus):
                    local_tensor_out = inspect(out_tensors, {"host": host, "gpu": gpu})
                    rank = host * n_gpus + gpu
                    with no_mesh.activate():
                        # Combine the tensor list together for a better comparison
                        # message.
                        local_tensor_out = torch.cat(local_tensor_out)
                        assert torch.equal(
                            local_tensor_out, expected_tensors[rank]
                        ), f"For {rank=}, {host=}, {gpu=}"


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
# Set global timeout--sandcastle's timeout is 600s. A test that sandcastle times
# out is not counted as a failure, so we set a more restrictive timeout to
# ensure we see a hard failure in CI.
@pytest.mark.timeout(120)
class TestComm(RemoteFunctionsTestBase):
    N_GPUS: int = 2
    N_HOSTS: int = 2

    @property
    def world_size(self) -> int:
        return self.N_GPUS * self.N_HOSTS

    @property
    def device(self):
        self.fail("test subclass didn't override device")

    def _test_tensor_dtype_complex(self) -> None:
        with self.local_device_mesh(self.N_HOSTS, self.N_GPUS) as device_mesh:
            group = device_mesh.process_group(("host", "gpu"))
            tensor = torch.rand(2, device="cuda")
            tensor_c = torch.view_as_complex(tensor)
            tensor_list = [
                torch.rand(2, device="cuda") for _ in range(self.N_HOSTS * self.N_GPUS)
            ]
            tensor_list_c = list(tensor_list)
            tensor_list_c[1] = torch.view_as_complex(tensor_list_c[1])

            inspect(all_gather(tensor_list, tensor, group=group))
            inspect(all_gather(tensor_list, tensor_c, group=group))
            inspect(all_gather(tensor_list_c, tensor, group=group))
            inspect(all_gather(tensor_list_c, tensor_c, group=group))

    def test_nccl_barrier(self) -> None:
        with self.local_device_mesh(self.N_HOSTS, self.N_GPUS) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            rank = device_mesh.rank(("host", "gpu"))
            t = torch.tensor([1] * 10, device="cuda") + rank
            all_reduce(t, group=pg)

            for host in range(self.N_HOSTS):
                for gpu in range(self.N_GPUS):
                    rank = 2 * host + gpu
                    with no_mesh.activate():
                        # all reduce will sum rank + 1 across all ranks.
                        expected_tensor = torch.tensor(
                            [sum(range(1, self.world_size + 1))] * 10
                        )
                        assert torch.equal(
                            expected_tensor,
                            inspect(t, {"host": host, "gpu": gpu}),
                        )

    def test_tensor_dtype_complex(self) -> None:
        self._test_tensor_dtype_complex()

    def test_reduce_scatter_base_k(self) -> None:
        expected_tensor = (
            torch.arange(self.N_HOSTS * self.N_GPUS * 2, dtype=torch.float32)
            .reshape(1, self.N_HOSTS * self.N_GPUS, 2)
            .repeat(self.N_HOSTS * self.N_GPUS, 1, 1)
        ).sum(0)
        with self.local_device_mesh(self.N_HOSTS, self.N_GPUS) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            output_tensor = torch.zeros(2, dtype=torch.int64, device="cuda")
            input_tensors = torch.arange(
                self.N_HOSTS * self.N_GPUS * 2, dtype=torch.int64, device="cuda"
            )
            input_tensors = torch.reshape(
                input_tensors, (self.N_HOSTS * self.N_GPUS, 2)
            )
            # Input is [[0, 1], [2, 3], [4, 5], [6, 7]] across 4 ranks.
            # After reduce + scatter, output_tensor should be [0 * 4, 1 * 4] on the 0th rank
            # and [2 * 4, 3 * 4] on the 1st rank, and so on
            reduce_scatter_tensor(output_tensor, input_tensors, group=pg)

            for host in range(self.N_HOSTS):
                for gpu in range(self.N_GPUS):
                    rank = 2 * host + gpu
                    output_tensor_local = inspect(
                        output_tensor, {"host": host, "gpu": gpu}
                    )
                    with no_mesh.activate():
                        assert torch.equal(output_tensor_local, expected_tensor[rank])


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
# Set global timeout--sandcastle's timeout is 600s. A test that sandcastle times
# out is not counted as a failure, so we set a more restrictive timeout to
# ensure we see a hard failure in CI.
@pytest.mark.timeout(120)
class TestNcclProcessGroupWithDispatchedCollectives(RemoteFunctionsTestBase):
    """This test is copied from test_c10d_nccl.py::NcclProcessGroupWithDispatchedCollectivesTests
    in torch, but modified to setup a Monarch device mesh and use remote functions"""

    N_GPUS: int = 2
    N_HOSTS: int = 2

    def _call_collective_with_varying_tensors(
        self,
        world_size: int,
        collective: Remote[..., torch.Tensor],
        *args,
        **kwargs,
    ) -> None:
        # call collective with varying tensors to ensure that the tensors are
        # correctly dispatched

        # ensure supported devices (cpu, cuda) succeeds during dispatch call
        tensor = torch.zeros(2, 2, device=torch.device("cuda"))
        # multi tensor collectives
        if collective == barrier:
            fetch_shard(collective(*args, **kwargs)).result()
        elif collective == all_gather:
            output_list = list(
                torch.zeros(world_size * 2, 2, device=torch.device("cuda")).chunk(
                    world_size
                )
            )
            fetch_shard(collective(output_list, tensor, *args, **kwargs)).result()
        elif collective == reduce_scatter:
            fetch_shard(
                collective(tensor, [tensor] * world_size, *args, **kwargs)
            ).result()
        elif collective == gather:
            gather_list = list(
                torch.zeros(world_size * 2, 2, device=torch.device("cuda")).chunk(
                    world_size
                )
            )
            fetch_shard(collective(tensor, gather_list, *args, **kwargs)).result()
        elif collective == scatter:
            fetch_shard(
                collective(tensor, [tensor] * world_size, *args, **kwargs)
            ).result()
        elif collective == all_to_all:
            fetch_shard(
                collective(
                    [tensor] * world_size, [tensor] * world_size, *args, **kwargs
                )
            ).result()
        else:
            fetch_shard(collective(tensor, *args, **kwargs)).result()

    @pytest.mark.parametrize(
        "collective",
        [
            reduce,
            broadcast,
            all_reduce,
            all_gather,
            reduce_scatter,
            barrier,
            all_to_all,
            gather,
            scatter,
        ],
        ids=[
            "reduce",
            "broadcast",
            "all_reduce",
            "all_gather",
            "reduce_scatter",
            "barrier",
            "all_to_all",
            "gather",
            "scatter",
        ],
    )
    def test_collectives(self, collective: Callable[..., torch.Tensor]) -> None:
        world_size = self.N_HOSTS * self.N_GPUS
        with self.local_device_mesh(self.N_HOSTS, self.N_GPUS) as device_mesh:
            rank = device_mesh.rank(("host", "gpu"))
            pg = device_mesh.process_group(("host", "gpu"))

            kwargs: dict[str, object] = {"group": pg}
            if collective == reduce:
                kwargs["group_dst"] = 0
            elif collective == broadcast:
                kwargs["group_src"] = rank
            elif collective == gather:
                kwargs["group_dst"] = 0
            elif collective == scatter:
                kwargs["group_src"] = 0
            self._call_collective_with_varying_tensors(world_size, collective, **kwargs)

    def test_all_to_all_single(self) -> None:
        with self.local_device_mesh(self.N_HOSTS, self.N_GPUS) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            # test alltoall_base
            tensor_in = torch.arange(4, device="cuda", dtype=torch.float32)
            tensor_out = torch.empty(4, device="cuda", dtype=torch.float32)
            all_to_all_single(tensor_out, tensor_in, group=pg)

            for host in range(self.N_HOSTS):
                for gpu in range(self.N_GPUS):
                    rank = 2 * host + gpu
                    with no_mesh.activate():
                        assert torch.equal(
                            inspect(tensor_out, {"host": host, "gpu": gpu}),
                            rank * torch.ones(4),
                        )

    def test_allgather_base(self) -> None:
        with self.local_device_mesh(self.N_HOSTS, self.N_GPUS) as device_mesh:
            pg = device_mesh.process_group(("host", "gpu"))
            rank = (
                (device_mesh.rank("host") + 1) * self.N_GPUS
                + device_mesh.rank("gpu")
                + 1
            )
            tensor_in = torch.arange(2, device="cuda") * rank
            tensor_out = torch.zeros(
                self.N_HOSTS * self.N_GPUS * 2, dtype=torch.int64, device="cuda"
            )
            all_gather_into_tensor(tensor_out, tensor_in, group=pg)
            local_tensor_out = inspect(tensor_out)
            with no_mesh.activate():
                assert torch.equal(
                    local_tensor_out, torch.tensor([0, 3, 0, 4, 0, 5, 0, 6])
                )


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
