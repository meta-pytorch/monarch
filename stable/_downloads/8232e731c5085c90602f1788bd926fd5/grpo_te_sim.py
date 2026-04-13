# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Simulation of GRPO with Tensor Engine
=====================================
Demonstrates patch_tensor_engine working with grpo_actor_te.py to show
that tensor engine can be simulated without real GPUs.

Run with::

    buck2 run //monarch/docs/source/examples:grpo_te_sim
"""

import asyncio

from monarch._src.actor.mock import patch_tensor_engine
from monarch.docs.source.examples.grpo_actor_te import GRPOTrainer, main as grpo_te_main


@patch_tensor_engine(GRPOTrainer)
async def simulate_with_tensor_engine() -> None:
    """Run GRPO training with simulated tensor engine."""
    await grpo_te_main()


async def main() -> None:
    # Decorator syntax
    await simulate_with_tensor_engine()

    # Context manager syntax
    with patch_tensor_engine(GRPOTrainer):
        await grpo_te_main()


if __name__ == "__main__":
    asyncio.run(main())
