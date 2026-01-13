# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import itertools
from contextlib import contextmanager
from enum import Enum
from typing import ContextManager, List
from unittest.mock import patch

import monarch
import pytest
import torch
from monarch import coalescing, fetch_shard, get_active_stream, remote, Stream
from monarch._testing import TestingContext
from monarch.common._coalescing import _record_and_define, compile
from monarch.common.device_mesh import DeviceMesh, get_active_mesh, no_mesh
from monarch.common.function_caching import AliasOf, Storage, TensorGroup
from monarch.common.tensor import Tensor


def _do_bogus_tensor_work(x, y, fail_rank=None):
    return x + y  # real function actually does x @ y


do_bogus_tensor_work = remote(
    "monarch.worker._testing_function.do_bogus_tensor_work",
    propagate=_do_bogus_tensor_work,
)


def inspect(x):
    return fetch_shard(x).result().item()


@pytest.fixture(scope="module", autouse=True)
def testing_context():
    global local
    with TestingContext() as local:
        yield


class BackendType(Enum):
    PY = "py"
    RS = "rs"
    MESH = "mesh"


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
@pytest.mark.parametrize(
    "backend_type", [BackendType.PY, BackendType.RS, BackendType.MESH]
)
class TestCoalescing:
    @classmethod
    def local_device_mesh(
        cls,
        num_hosts: int,
        gpu_per_host: int,
        backend_type: BackendType,
        activate: bool = True,
    ) -> ContextManager[DeviceMesh]:
        # pyre-fixme[10]: pytest defines this fixture.
        return local.local_device_mesh(
            num_hosts,
            gpu_per_host,
            activate,
            backend=backend_type.value,
        )

    @property
    def num_outstanding_messages(self) -> int:
        return sum(
            len(msgs)
            for msgs in get_active_mesh().client.recorder.flat_messages.values()
        )

    def test_basic_coalescing(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            with coalescing():
                a = torch.zeros(3, 4)
                for _ in range(1, 10):
                    a = a + torch.ones(3, 4)
                # no messages should have been sient since coalescing is enabled
                assert self.num_outstanding_messages >= 10
            # now that the coalesce is done we should have flushed the messages
            assert self.num_outstanding_messages == 0

    def test_repeat_simple(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.zeros(())

            @compile(verify=False)
            def fn():
                nonlocal a
                z = torch.ones(())
                a += z
                return z

            z = None
            for _ in range(3):
                z = fn()

            assert inspect(a) == 3
            assert inspect(z) == 1

    def test_repeat_formals(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.rand(3, 4)

            @compile(verify=False)
            def fn(a, b):
                return 2 * a + b

            for _ in range(3):
                b = torch.rand(3, 4)
                z = fn(a, b)
                lz, la, lb = monarch.inspect((z, a, b))
                assert isinstance(la, torch.Tensor)
                assert isinstance(lb, torch.Tensor)
                with no_mesh.activate():
                    assert torch.allclose(lz, 2 * la + lb)

            @compile(verify=False)
            def fn(b):
                return 2 * a + b

            for _ in range(3):
                b = torch.rand(3, 4)
                z = fn(b)
                lz, la, lb = monarch.inspect((z, a, b))
                assert isinstance(la, torch.Tensor)
                assert isinstance(lb, torch.Tensor)
                with no_mesh.activate():
                    assert torch.allclose(lz, 2 * la + lb)

    def test_repeat_error_inside(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.zeros(())

            @compile(verify=False)
            def fn():
                nonlocal a
                z = torch.ones(())
                a += z
                do_bogus_tensor_work(z, z)
                return z

            z = fn()
            # recorded coalescing will lump errors together so check that
            with pytest.raises(Exception, match="both arguments to matmul"):
                inspect(z)

    def test_repeat_inner_borrow(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.zeros(())
            other = Stream("other")
            with other.activate():
                b = torch.ones(())

            @compile(verify=False)
            def fn():
                nonlocal a, b
                c, borrow = get_active_stream().borrow(b)
                with borrow:
                    a += c

            for _ in range(3):
                fn()

            assert inspect(a) == 3

    def test_repeat_outer_borrow(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.zeros(())
            other = Stream("other")
            with other.activate():
                b = torch.ones(())
            c, borrow = get_active_stream().borrow(b)

            @compile(verify=False)
            def fn():
                nonlocal a, c
                a += c
                z = torch.rand(3, 4)
                del c
                return z

            with borrow:
                z = None
                for _ in range(3):
                    z = fn()

            result = fetch_shard(a).result()
            fetch_shard(z).result()
            with no_mesh.activate():
                assert result.item() == 3

    def test_nested_coalescing(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            with coalescing():
                a = torch.zeros(3, 4)
                with coalescing():
                    for _ in range(1, 10):
                        a = a + torch.ones(3, 4)
                    # confirm that there are messages awaiting to be send
                    assert self.num_outstanding_messages >= 10
                # since we are in the nested block we shouldn't have flushed the messages yet
                assert self.num_outstanding_messages >= 10
            # now that the outer coalesce is done we should have flushed the messages
            assert self.num_outstanding_messages == 0

    def test_no_coalescing(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.zeros(3, 4)
            for _ in range(1, 10):
                a = a + torch.ones(3, 4)
            # without coalescing the messages should be sent with nothing outstanding
            assert self.num_outstanding_messages == 0

    @contextmanager
    def assertRecorded(self, times: int):
        with patch(
            "monarch.common._coalescing._record_and_define",
            side_effect=_record_and_define,
        ) as m:
            yield
            assert m.call_count == times

    def assertAliases(self, tensors: List[Tensor], aliasing: List[int]):
        group = TensorGroup([t._fake for t in tensors])
        c = iter(itertools.count())
        actual = []
        assert len(group.pattern.entries) == len(tensors)
        assert len(aliasing) == len(tensors)
        for e in group.pattern.entries:
            match e.storage:
                case AliasOf(offset=offset):
                    actual.append(offset)
                case Storage():
                    actual.append(next(c))
        assert aliasing == actual

    def test_compile_aliasing(self, backend_type) -> None:
        with self.local_device_mesh(1, 1, backend_type):

            @compile(verify=False)
            def add(a, b):
                return a + b

            @compile(verify=False)
            def return_cond(a, b, c):
                if c:
                    return a
                else:
                    return b

            a = torch.rand(3, 4)
            b = torch.rand(3, 4)
            with self.assertRecorded(1):
                r = add(a, b)
                assert r.size() == (3, 4)
                r2 = add(b, a)
                self.assertAliases([a, b, r2, r], [0, 1, 2, 3])

            c = torch.rand(4)
            d = torch.rand(4, 4)
            with self.assertRecorded(1):
                e = add(c, d)
                assert e.size() == (4, 4)
                e = add(c, torch.rand(4, 4))
                assert e.size() == (4, 4)

            with self.assertRecorded(1):
                r = add(a, 4)
                self.assertAliases([r, a], [0, 1])

            with self.assertRecorded(1):
                r0 = return_cond(a, b, True)
                self.assertAliases([a, b, r0], [0, 1, 0])
                r1 = return_cond(b, a, True)
                self.assertAliases([a, b, r1], [0, 1, 1])

            with self.assertRecorded(1):
                r0 = return_cond(a, b, False)
                self.assertAliases([a, b, r0], [0, 1, 1])
                r1 = return_cond(a, b, False)
                self.assertAliases([b, a, r1], [0, 1, 0])

            @compile(verify=False)
            def captured(b):
                return a + b

            with self.assertRecorded(1):
                r = captured(b)
                self.assertAliases([a, b, r], [0, 1, 2])
                r = captured(torch.rand(3, 4))
                assert r.size() == (3, 4)

            with self.assertRecorded(1):
                # input aliased with capture
                captured(a)
                captured(a)

            @compile(verify=False)
            def weird(f, g):
                o = f + g
                return o, o[0], f[0], g[0], a[0]

            with self.assertRecorded(1):
                r0, r1, r2, r3, r4 = weird(c, d)
                self.assertAliases(
                    [c, d, a, r0, r1, r2, r3, r4], [0, 1, 2, 3, 3, 0, 1, 2]
                )

    def test_compile_input_permissions(self, backend_type):
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.rand(3, 4)

            @compile(verify=False)
            def add(b):
                return a + b

            with self.assertRecorded(1):
                c = add(torch.rand(3, 4))

            other = Stream("other")
            ab, borrow = other.borrow(a, mutable=True)

            with borrow:
                with pytest.raises(TypeError, match="BORROWED"):
                    add(torch.rand(3, 4))

            # test we can read it again
            add(torch.rand(3, 4))

            ab, borrow = other.borrow(a)
            with borrow:
                add(torch.rand(3, 4))

            with self.assertRecorded(0):
                with other.activate():
                    c = torch.rand(3, 4)
                c, borrow = monarch.get_active_stream().borrow(c)
                with borrow:
                    add(c)

            a.drop()

            with pytest.raises(TypeError, match="DROPPED"):
                add(torch.rand(3, 4))

    def test_compile_verify(self, backend_type):
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.rand(3, 4)

            @compile(verify=True)
            def add(b):
                return a + b

            c = False

            @compile(verify=True)
            def add_broken(b):
                nonlocal c
                if c:
                    a = torch.zeros(3, 4)
                else:
                    a = torch.rand(3, 4)
                return a.add(b)

            with self.assertRecorded(2):
                add(torch.rand(3, 4))
                add(torch.rand(3, 4))
                add(torch.rand(3, 4))

            add_broken(torch.rand(3, 4))
            with pytest.raises(RuntimeError, match="diverges"):
                c = True
                add_broken(torch.rand(3, 4))

    def test_dropped(self, backend_type):
        with self.local_device_mesh(1, 1, backend_type):
            a = torch.rand(3, 4)
            b = None

            @compile(verify=False)
            def foo():
                nonlocal b
                b = a + a

            foo()
            with pytest.raises(TypeError, match="DROPPED"):
                b.add(4)

    def test_across_mesh(self, backend_type):
        with self.local_device_mesh(2, 1, backend_type) as m:
            m0 = m(host=0)
            m1 = m(host=1)

            @compile
            def foo(a, b):
                with m0.activate():
                    r0 = a + a
                with m1.activate():
                    r1 = b + b
                return r0, r1

            with m0.activate():
                a = torch.rand(3, 4)
            with m1.activate():
                b = torch.rand(3, 4)

            r0, r1 = foo(a, b)
            with m0.activate():
                monarch.inspect(r0)
            with m1.activate():
                monarch.inspect(r0)

    def test_grad_not_supported(self, backend_type):
        with self.local_device_mesh(1, 1, backend_type):

            @compile
            def foo(x):
                return x

            y = torch.rand(3, requires_grad=True)

            @compile
            def returnit():
                return y

            with pytest.raises(TypeError, match="REQUIRES_GRAD"):
                foo(torch.rand(3, requires_grad=True))

            with pytest.raises(TypeError, match="REQUIRES_GRAD"):
                returnit()

    def test_mutate_inputs(self, backend_type):
        with self.local_device_mesh(1, 1, backend_type) as mesh:

            @compile(verify=False)
            def foo(x_not_mutated, w_not_mutated, y, y_alias, z, z_alias):
                u = (
                    x_not_mutated.mul(2.0)
                    + w_not_mutated
                    + z_alias.unsqueeze(0).repeat(3, 1)
                )
                v = y.add(5.0)
                stream = monarch.Stream("borrow")
                borrowed_y_alias, y_alias_borrow = stream.borrow(y_alias, mutable=True)
                with stream.activate():
                    borrowed_y_alias.add_(1.0)
                y_alias_borrow.drop()
                z.add_(1.0)
                return u, v

            x_not_mutated = torch.rand(3, 3)
            w_not_mutated = torch.rand(3, 3)
            y = torch.rand(3, 3)
            y_alias = y.reshape(-1)
            z = torch.rand(3, 3)
            z_alias = z[0, :]

            mutated_inputs = (y, y_alias, z, z_alias)
            mutated_aliases = set().union(*[t._aliases.aliases for t in mutated_inputs])
            all_inputs = (x_not_mutated, w_not_mutated) + mutated_inputs
            with patch.object(
                mesh.client,
                "new_node_nocoalesce",
                side_effect=mesh.client.new_node_nocoalesce,
            ) as new_node:
                for _ in range(2):
                    u, v = foo(*all_inputs)
                    (mutated, used, _, _), _ = new_node.call_args
                    assert mutated_aliases.union(
                        u._aliases.aliases, v._aliases.aliases
                    ) == set(mutated)
                    assert set(all_inputs) == set(used)
