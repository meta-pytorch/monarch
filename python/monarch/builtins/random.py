# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

# pyre strict
from typing import Callable

import torch
from monarch.common.remote import remote


@remote(propagate="inspect")
def set_manual_seed_remote(seed: int, process_idx: int = 0) -> None:
    torch.manual_seed(seed ^ process_idx)


@remote(propagate=lambda: torch.zeros(1))
def get_rng_state_remote() -> torch.Tensor:
    return torch.get_rng_state()


@remote(propagate="inspect")
def set_rng_state_remote(new_state: torch.Tensor) -> None:
    torch.set_rng_state(new_state)


def _run_no_return(f: Callable) -> None:
    f()
    return None


# TODO: return result when uint64 is supported from remote function
@remote(propagate=lambda: _run_no_return(torch.seed))
def seed_remote() -> None:
    torch.seed()


# same underlying implementation as seed_remote (torch.seed)
# TODO: return result when uint64 is supported from remote function
@remote(propagate=lambda: _run_no_return(torch.random.seed))
def random_seed_remote() -> None:
    torch.random.seed()


@remote(propagate="inspect")
def manual_seed_cuda_remote(seed: int) -> None:
    torch.cuda.manual_seed(seed)


@remote(propagate="inspect")
def manual_seed_all_cuda_remote(seed: int) -> None:
    torch.cuda.manual_seed_all(seed)


@remote(propagate=lambda: [torch.zeros(1)])
def get_rng_state_all_cuda_remote() -> list[torch.Tensor]:
    return torch.cuda.get_rng_state_all()


@remote(propagate="inspect")
def set_rng_state_all_cuda_remote(states: list[torch.Tensor]) -> None:
    torch.cuda.set_rng_state_all(states)


# initial_seed may sometimes return a uint64 which currenly can't be unwrapped by the framework
# def initial_seed_remote() -> int: ...
