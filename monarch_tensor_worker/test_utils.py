# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

from typing import TYPE_CHECKING

import torch

if TYPE_CHECKING:
    from monarch.worker.worker import (  # @manual //monarch/python/monarch/worker:worker
        DeviceMesh,
    )


def resolve_value(val: object) -> object:
    return val


def mesh_rank(mesh: DeviceMesh, dim: str) -> int:
    return mesh.dims[dim].rank


def test_scalar_type(val: torch.dtype) -> torch.dtype:
    assert val == torch.float32
    return val


def test_layout(val: torch.layout) -> torch.layout:
    assert val is torch.strided
    return val


def test_none(val: object) -> None:
    assert val is None
    return None


def test_device(val: torch.device) -> torch.device:
    assert val == torch.device(1)
    return val


def test_memory_format(val: torch.memory_format) -> torch.memory_format:
    assert val == torch.contiguous_format
    return val


def none() -> None:
    return None


def test_remote_process_group(group: torch.distributed.ProcessGroup) -> None:
    assert group.rank() in [0, 1]
    assert group.size() == 2

    # Test an all-reduce.
    tensor = torch.tensor([1, 2, 3], device="cuda")
    torch.distributed.all_reduce(tensor, op=torch.distributed.ReduceOp.SUM, group=group)
    assert tensor.cpu().equal(torch.tensor([2, 4, 6]))

    # Test an all-gather-into-tensor.
    input_tensor = torch.tensor([2], dtype=torch.int32, device="cuda")
    output_tensor = torch.empty(group.size(), dtype=torch.int32, device="cuda")
    torch.distributed.all_gather_into_tensor(output_tensor, input_tensor, group=group)
    assert output_tensor.cpu().equal(torch.tensor([2, 2]))

    # Test an barrier.
    torch.distributed.barrier(group=group)

    # Test a reduce-scatter-tensor.
    input_tensor = torch.tensor([[0, 1], [2, 3]], dtype=torch.int32, device="cuda")
    output_tensor = torch.empty(group.size(), dtype=torch.int32, device="cuda")
    torch.distributed.reduce_scatter_tensor(
        output_tensor, input_tensor, op=torch.distributed.ReduceOp.SUM, group=group
    )
    # This function is used inside a rust test that does torch.distributed ops with multiple
    # workers in the same process, which behaves weirdly with rank assignment. So we can't use
    # worker rank to assert a specific output.
    assert output_tensor.cpu().equal(torch.tensor([0, 2])) or output_tensor.cpu().equal(
        torch.tensor([4, 6])
    ), (
        f"Expected {output_tensor.cpu()} to equal {torch.tensor([0, 2])} or {torch.tensor([4, 6])}"
    )
