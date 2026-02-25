# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import torch
from monarch.common.remote import remote


def run_loop_local(n_iters, tensor_shape=(2, 2)):
    local = torch.zeros(*tensor_shape)
    ones = torch.ones(*tensor_shape)
    for _ in range(n_iters):
        local = ones + local
    return local


def _run_loop(*args, **kwargs):
    return torch.ones(args[1])


run_loop = remote("tests.dispatch_bench_helper.run_loop_local", propagate=_run_loop)
