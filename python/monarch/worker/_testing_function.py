# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
import os
import threading
from time import sleep, time
from typing import Tuple

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim
from monarch._rust_bindings.monarch_extension.debugger import (  # @manual=//monarch/monarch_extension:monarch_extension
    get_bytes_from_write_action,
    PdbActor,
)
from monarch._rust_bindings.monarch_messages.debugger import DebuggerAction
from monarch.common import opaque_ref
from monarch.common.pipe import Pipe
from monarch.common.process_group import SingleControllerProcessGroupWrapper
from monarch.common.remote import remote
from torch.utils.data import DataLoader, TensorDataset


logger = logging.getLogger(__name__)

"""
Collection of worker-side remote functions that are used in unit tests
"""


# code used for testing but useful to have importable (e.g. can refer to remote functions)
def do_bogus_tensor_work(x, y, fail_rank=None):
    if fail_rank is not None and int(os.environ["RANK"]) != fail_rank:
        return x
    return x @ y


def set_device_udf_worker(device: int):
    torch.cuda.set_device(device)
    return torch.ones(1)


def example_data_loader(p: "Pipe", x: int, y: int):
    for i in range(x, y):
        p.send(torch.full((), i))


def example_data_loader_small_pipe(p: "Pipe", iters: int, shape: Tuple[int, int]):
    t0 = time()
    for i in range(iters):
        if time() - t0 > 0.5:
            p.send(torch.full(shape, -1.0))
        else:
            p.send(torch.full(shape, i))


def example_echo_add(p: "Pipe"):
    while True:
        p.send(p.recv() + 1 + p.ranks["gpu"])


def log(*args, **kwargs):
    logger.info(*args, **kwargs)


def remote_sleep(t: float):
    sleep(t)


def has_nan(t):
    return torch.isnan(t).any().item()


def new_barrier_hackery(threads):
    global _barrier
    _barrier = threading.Barrier(threads)
    return torch.zeros(1)


def wait_barrier_hackery(t: torch.Tensor):
    # pyre-fixme[10]: Name `_barrier` is used but not defined.
    _barrier.wait()


def all_reduce_prop(tensor, *args, **kwargs):
    tensor.add_(1)
    return tensor


@remote(propagate=all_reduce_prop)
def all_reduce(tensor, group=None, op=dist.ReduceOp.SUM):
    dist.all_reduce(tensor, op=op, group=group)
    return tensor


@remote(propagate=lambda *args, **kwargs: torch.ones(1))
def barrier(group=None, device_ids=None):
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    dist.barrier(group=group, async_op=False, device_ids=device_ids)
    return torch.ones(1)


@remote(
    propagate=lambda tensor_list, *args, **kwargs: [
        torch.zeros_like(t) for t in tensor_list
    ]
)
def all_gather(
    tensor_list: list[torch.Tensor],
    tensor: torch.Tensor,
    group=None,
) -> list[torch.Tensor]:
    dist.all_gather(tensor_list, tensor, group=group, async_op=False)
    return tensor_list


@remote(propagate=lambda output_tensor, input_tensor, group=None: torch.zeros(1))
def all_gather_into_tensor(output_tensor, input_tensor, group=None):
    dist.all_gather_into_tensor(output_tensor, input_tensor, group=group)
    return torch.ones(1)


@remote(propagate=lambda t, *args, **kwargs: torch.ones(1))
def isend(t, destination, group=None):
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    req = dist.isend(t, destination.item(), group=group)
    assert isinstance(req.is_completed(), bool)
    req.wait()
    return torch.ones(1)


def irecv_prop(t, src, group=None):
    # irecv mutates its input.
    t.add_(1)
    return torch.ones(1)


@remote(propagate=irecv_prop)
def irecv(t, src, group=None):
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    req = dist.irecv(tensor=t, src=src.item(), group=group)
    assert isinstance(req.is_completed(), bool)
    req.wait()
    return torch.ones(1)


def gonna_pdb():
    x = 3 + 4
    import pdb  # noqa

    pdb.set_trace()
    print(x)


def do_some_processing(a_string):
    return a_string + " processed"


def how_many_of_these_do_you_want(n: int, t: torch.Tensor):
    return [t + i for i in range(n)]


def remote_chunk(t: torch.Tensor):
    return t.chunk(4, dim=0)


class TestRemoteAutogradFunction(torch.autograd.Function):
    @staticmethod
    def forward(ctx, x, y):
        ctx.save_for_backward(x)
        if x.requires_grad:
            out0 = x * y
        else:
            out0 = x + y

        return out0, y, torch.ones(4), 4

    @staticmethod
    def backward(ctx, dx1, dx2, dx3, dx4):
        return dx1, dx2


class _TestMultiplyAllReduce(torch.autograd.Function):
    "Existing user autograd.Function"

    @staticmethod
    def forward(ctx, x, y, pg):
        wa = torch.rand(x.shape, device=x.device)
        ctx.save_for_backward(x, y, wa)
        ctx.my_property = True
        ctx.pg = pg
        z = x * y
        dist.all_reduce(z, op=dist.ReduceOp.SUM, group=pg)
        return z

    @staticmethod
    def backward(ctx, grad_output):
        x, y, a = ctx.saved_tensors
        assert ctx.my_property
        grad_x = grad_output * y
        grad_y = grad_output * x * a
        dist.all_reduce(grad_x, op=dist.ReduceOp.SUM, group=ctx.pg)
        dist.all_reduce(grad_y, op=dist.ReduceOp.SUM, group=ctx.pg)
        return grad_x, grad_y, None


class SimpleModel(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(SimpleModel, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        return x


def setup_state_worker():
    input_size = 10
    hidden_size = 20
    output_size = 1
    batch_size = 16
    learning_rate = 0.01

    x = torch.rand(100, input_size).cuda()
    y = torch.rand(100, output_size).cuda()
    dataset = TensorDataset(x, y)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    model = SimpleModel(input_size, hidden_size, output_size).cuda()
    criterion = nn.MSELoss().cuda()
    optimizer = optim.SGD(model.parameters(), lr=learning_rate)

    return [
        opaque_ref.OpaqueRef(obj) for obj in [model, dataloader, criterion, optimizer]
    ]


def iteration_worker(model_ref, dataloader_ref, criterion_ref, optimizer_ref, pg):
    model = model_ref.value
    dataloader = dataloader_ref.value
    criterion = criterion_ref.value
    optimizer = optimizer_ref.value

    epoch_loss = 0.0
    for inputs, targets in dataloader:
        outputs = model(inputs)
        loss = criterion(outputs, targets)

        optimizer.zero_grad()
        loss.backward()
        for param in model.parameters():
            dist.all_reduce(param.grad, op=dist.ReduceOp.SUM, group=pg)
        optimizer.step()

        epoch_loss += loss.item()
    return torch.tensor(epoch_loss)


def create_opaque_ref_worker():
    return opaque_ref.OpaqueRef(nn.Linear(10, 10))


def opaque_ref_key_table_length_worker() -> torch.Tensor:
    return torch.tensor(len(list(opaque_ref._key_table.keys())))


class WorkerFoo:
    def __init__(self, v):
        self.t = torch.full((), v)

    def add(self, b):
        return self.t + b


def reduce_prop(tensor, *args, **kwargs):
    return tensor.add_(1)


@remote(propagate=reduce_prop)
def reduce(
    tensor: torch.Tensor,
    dst: int | None = None,
    op: dist.ReduceOp = dist.ReduceOp.SUM,
    group=None,
    group_dst: int | None = None,
) -> torch.Tensor:
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    dist.reduce(tensor, dst, op=op, group=group, async_op=False, group_dst=group_dst)
    return tensor


def reduce_scatter_prop(output, *args, **kwargs):
    # reduce_scatter mutates its input argument.
    output.add_(1)
    return output


@remote(propagate=reduce_scatter_prop)
def reduce_scatter(output, input_list, op=dist.ReduceOp.SUM, group=None):
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    dist.reduce_scatter(output, input_list, op=op, group=group, async_op=False)
    return output


def reduce_scatter_tensor_prop(tensor, *args, **kwargs):
    # reduce_scatter_tensor mutates its input argument.
    tensor.add_(1)
    return tensor


@remote(propagate=reduce_scatter_tensor_prop)
def reduce_scatter_tensor(
    output_tensor, input_tensor, group=None, op=dist.ReduceOp.SUM
):
    dist.reduce_scatter_tensor(output_tensor, input_tensor, group=group, op=op)
    return output_tensor


def gather_prop(tensor, gather_list=None, *args, **kwargs) -> torch.Tensor:
    # Gather mutates its gather_list and does not modify the input tensor.
    if gather_list is not None:
        for t in gather_list:
            t.add_(1)
    return torch.zeros_like(tensor)


@remote(propagate=gather_prop)
def gather(
    tensor: torch.Tensor,
    gather_list: list[torch.Tensor] | None = None,
    dst: int | None = None,
    group=None,
    group_dst: int | None = None,
) -> torch.Tensor:
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    if group_dst is not None:
        if group_dst != dist.get_rank(group):
            # Don't set the gather_list on any rank other than the source.
            gather_list = None
    elif dst is not None:
        if dst != dist.get_rank(group):
            # Don't set the gather_list on any rank other than the source.
            gather_list = None
    dist.gather(
        tensor,
        gather_list=gather_list,
        dst=dst,
        group=group,
        async_op=False,
        group_dst=group_dst,
    )
    return tensor


# Scatter mutates its input tensor.
@remote(propagate=lambda tensor, *args, **kwargs: tensor.add_(1))
def scatter(
    tensor: torch.Tensor,
    scatter_list: list[torch.Tensor] | None = None,
    src: int | None = None,
    group=None,
    group_src: int | None = None,
) -> torch.Tensor:
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    if group_src is not None:
        if group_src != dist.get_rank(group):
            # Don't set the scatter_list on any rank other than the source.
            scatter_list = None
    elif src is not None:
        if src != dist.get_rank(group):
            # Don't set the scatter_list on any rank other than the source.
            scatter_list = None
    dist.scatter(
        tensor,
        scatter_list=scatter_list,
        src=src,
        group=group,
        async_op=False,
        group_src=group_src,
    )
    return tensor


def inner_remote_function_that_fails():
    raise Exception("Failed to execute inner_remote_function_that_fails")


def outer_remote_function_that_calls_inner():
    inner_remote_function_that_fails()
    return torch.zeros(1)


def broadcast_prop(tensor, *args, **kwargs) -> torch.Tensor:
    # Broadcast mutates its input tensor.
    return tensor.add_(1)


@remote(propagate=broadcast_prop)
def broadcast(
    tensor: torch.Tensor,
    src: int | None = None,
    group=None,
    group_src: int | None = None,
) -> torch.Tensor:
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    dist.broadcast(tensor, src=src, group=group, async_op=False, group_src=group_src)
    return tensor


def all_to_all_prop(
    output_tensor_list: list[torch.Tensor],
    input_tensor_list: list[torch.Tensor],
    *args,
    **kwargs,
) -> list[torch.Tensor]:
    for t in output_tensor_list:
        # Mutate the output tensors to ensure that fetches on the output tensor
        # list are propagated.
        t.add_(1)
    return output_tensor_list


@remote(propagate=all_to_all_prop)
def all_to_all(
    output_tensor_list: list[torch.Tensor],
    input_tensor_list: list[torch.Tensor],
    group=None,
) -> list[torch.Tensor]:
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    dist.all_to_all(output_tensor_list, input_tensor_list, group=group, async_op=False)
    return output_tensor_list


def all_to_all_single_prop(output_tensor, *args, **kwargs) -> torch.Tensor:
    # Mutate the output tensor to ensure that fetches on the output tensor
    # are propagated.
    output_tensor.add_(1)
    return output_tensor


@remote(propagate=all_to_all_single_prop)
def all_to_all_single(
    output_tensor: torch.Tensor, input_tensor: torch.Tensor, group=None
) -> torch.Tensor:
    if isinstance(group, SingleControllerProcessGroupWrapper):
        group = group.process_group
    dist.all_to_all_single(output_tensor, input_tensor, group=group)
    return output_tensor


def test_pdb_actor():
    pdb_actor = PdbActor()
    pdb_actor.send(DebuggerAction.Paused())
    assert isinstance(pdb_actor.receive(), DebuggerAction.Attach)
    pdb_actor.send(DebuggerAction.Read(4))
    msg = pdb_actor.receive()
    assert isinstance(msg, DebuggerAction.Write)
    assert get_bytes_from_write_action(msg) == b"1234"
    pdb_actor.send(DebuggerAction.Write(b"5678"))
    assert isinstance(pdb_actor.receive(), DebuggerAction.Detach)
    return torch.zeros(1)
