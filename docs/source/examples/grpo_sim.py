# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Simulation of GRPO without Tensor Engine
=====================================
Demonstrates working with grpo_actor.py to show that GRPO can be simulated
without real GPUs.

Run with::

    buck2 run //monarch/docs/source/examples:grpo_sim
"""

import asyncio
from typing import Any, Dict, Optional

import torch
from monarch._src.actor.mock import patch_actor

from monarch.actor import Actor, endpoint

from monarch.docs.source.examples.grpo_actor import (
    Generator,
    Learner,
    main as grpo_main,
    ReplayBuffer,
)
from monarch.rdma import RDMABuffer


class MockLearner(Actor):
    def __init__(self, replay_buffer: ReplayBuffer) -> None:
        self.replay_buffer = replay_buffer
        self.generators: Optional[Any] = None

    @endpoint
    async def weights_handle(self) -> Dict[str, RDMABuffer]:
        return {}

    @endpoint
    async def step(self) -> torch.Tensor:
        return torch.tensor(1.0)

    @endpoint
    async def init_generators(self, generators: Any) -> None:
        self.generators = generators


class MockLearner2(MockLearner):
    @endpoint
    async def step(self) -> torch.Tensor:
        return torch.tensor(2.0)


class MockGenerator(Generator):
    @endpoint
    async def generate(self, state: torch.Tensor) -> None:
        pass


@patch_actor(Learner, MockLearner2)
@patch_actor(Generator, MockGenerator)
async def simulate_with_decorators() -> None:
    await grpo_main()


async def main() -> None:
    await simulate_with_decorators()

    with patch_actor(Learner, MockLearner), patch_actor(Generator, MockGenerator):
        await grpo_main()


if __name__ == "__main__":
    asyncio.run(main())
