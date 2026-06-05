# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
from typing import NamedTuple, Tuple

import torch
from monarch.common.remote import remote
from monarch.common.tensor import Tensor


class State(NamedTuple):
    cpu: Tensor
    cuda: Tensor


@remote(
    propagate=lambda: (
        torch.empty(5056, dtype=torch.uint8),
        torch.empty(16, dtype=torch.uint8),
    )
)
def _get_state() -> Tuple[torch.Tensor, torch.Tensor]:
    return (torch.get_rng_state(), torch.cuda.get_rng_state())


@remote(propagate=lambda state: None)
def set_state(state: Tuple[Tensor, Tensor]):
    cpu, device = state
    torch.set_rng_state(cpu)
    torch.cuda.set_rng_state(device)


@remote(propagate=lambda _: None)
def _manual_seed(seed: torch.Tensor):
    torch.manual_seed(seed.item())


@remote(propagate=lambda: None)
def make_deterministic():
    torch.use_deterministic_algorithms(True)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    # env var for deterministic CuBLAS
    # https://pytorch.org/docs/stable/generated/torch.use_deterministic_algorithms.html
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"


def get_state() -> State:
    # pyrefly: ignore [bad-argument-type]
    return State(*_get_state())


def new_state(seed: Tensor) -> State:
    orig = get_state()
    _manual_seed(seed)
    mine = get_state()
    set_state(orig)
    return mine
