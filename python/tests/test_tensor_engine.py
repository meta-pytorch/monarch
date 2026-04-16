# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import monarch
import pytest
import torch
from monarch import remote
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, as_endpoint, endpoint
from monarch.mesh_controller import spawn_tensor_engine


two_gpu = pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)


@two_gpu
def test_tensor_engine() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 2})

    dm = spawn_tensor_engine(pm)
    with dm.activate():
        r = monarch.inspect(2 * torch.zeros(3, 4))

    fm = dm.flatten("all")
    with fm.activate():
        f = monarch.inspect(2 * torch.zeros(3, 4), all=1)

    assert torch.allclose(torch.zeros(3, 4), r)
    assert torch.allclose(torch.zeros(3, 4), f)

    @remote(propagate=lambda x: x)
    def nope(x):
        raise ValueError("nope")

    with pytest.raises(monarch.mesh_controller.RemoteException):
        with dm.activate():
            monarch.inspect(nope(torch.zeros(3, 4)))

    dm.exit()


@two_gpu
def test_proc_mesh_tensor_engine() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 2})
    with pm.activate():
        f = 10 * pm.rank_tensor("gpus").cuda()
        a = monarch.inspect(f, gpus=0)
        b = monarch.inspect(f, gpus=1)

    one = pm.slice(gpus=1)
    with one.activate():
        sliced_b = monarch.slice_mesh(f, gpus=1).to_mesh(one)
        c = monarch.inspect(sliced_b * 10)
    assert a == 0
    assert b == 10
    assert c == 100


class AddWithState(Actor):
    def __init__(self, state: torch.Tensor):
        super().__init__()
        self.state = state

    @endpoint
    def forward(self, x) -> torch.Tensor:
        return x + self.state


@two_gpu
def test_actor_with_tensors() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    with pm.activate():
        x = pm.spawn("adder", AddWithState, torch.ones(()))
        y = torch.ones(())
        assert x.forward.call(y).get(timeout=5).item(gpus=0).item() == 2


class Counter(Actor):
    def __init__(self):
        super().__init__()
        self.c = 0

    @endpoint
    def incr(self, x) -> int:
        self.c += 1
        return self.c - 1


@two_gpu
def test_actor_tensor_ordering() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    with pm.activate():
        counter = pm.spawn("a", Counter)
        results = []
        for _ in range(0, 10, 2):
            # tensor engine call
            results.append(counter.incr.call(torch.ones(())))
            # non-tensor engine call
            results.append(counter.incr.call(1))

        assert list(range(10)) == [r.get().item(gpus=0) for r in results]


class Linear(Actor):
    def __init__(self, N: int, M: int):
        self.weight = torch.zeros((N, M))

    def forward(self, x) -> torch.Tensor:
        return x @ self.weight

    @endpoint(propagate="inspect")
    def update(self, w: torch.Tensor) -> None:
        self.weight += w


@two_gpu
def test_rref_actor() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    with pm.activate():
        x = pm.spawn("linear", Linear, 3, 4)

        y = torch.ones((4, 3))
        t = as_endpoint(x.forward, propagate=lambda x: torch.rand(3, 4)).rref(y)
        assert monarch.inspect(t.sum()).item() == 0
        x.update.rref(torch.ones((3, 4)))
        t = as_endpoint(x.forward, propagate=lambda x: torch.rand(3, 4)).rref(y)
        assert monarch.inspect(t.sum()).item() == 3 * 4 * 4
