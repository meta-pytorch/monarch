# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Demo: Actor Mocking for TorchForge GRPO Training

This example demonstrates how Monarch's patch_actor mechanism works with
TorchForge's ForgeActor-based actors. The key insight is that patch_actor
intercepts at proc_mesh.spawn() - any actor spawned through this path will
be subject to mocking.

TorchForge's ForgeActor.as_actor() and as_service() ultimately call
proc_mesh.spawn(), which means patch_actor works seamlessly with ForgeActor.

Requirements:
    This demo requires a conda environment with TorchForge + Monarch installed:

    cd /path/to/fbcode/pytorch/torchforge
    conda create -n forge python=3.12
    conda activate forge
    ./scripts/install.sh

    # Install Monarch from source (for latest patch_actor support)
    cd /path/to/fbcode/monarch
    pip uninstall setuptools_scm -y
    SETUPTOOLS_SCM_PRETEND_VERSION=0.0.1 pip install --no-build-isolation -e .

Usage:
    python grpo_forge_sim.py
"""

import asyncio

from grpo_forge_lib import (
    ForgeComputeAdvantages,
    ForgeGenerator,
    ForgeReferenceModel,
    ForgeReplayBuffer,
    ForgeRewardActor,
    ForgeTitanTrainer,
    main as grpo_main,
    MockComputeAdvantages,
    MockGenerator,
    MockReferenceModel,
    MockReplayBuffer,
    MockRewardActor,
    MockTitanTrainer,
)

from monarch._src.actor.mock import patch_actor


@patch_actor(ForgeGenerator, MockGenerator)
@patch_actor(ForgeTitanTrainer, MockTitanTrainer)
@patch_actor(ForgeReferenceModel, MockReferenceModel)
@patch_actor(ForgeRewardActor, MockRewardActor)
@patch_actor(ForgeReplayBuffer, MockReplayBuffer)
@patch_actor(ForgeComputeAdvantages, MockComputeAdvantages)
async def simulate_with_decorators() -> None:
    """Run GRPO with mocked actors using @patch_actor decorators."""
    await grpo_main()


async def main() -> None:
    """Main entry point."""
    print("""
================================================================================
TorchForge + Monarch Actor Mocking Demo
================================================================================

This demo shows how Monarch's patch_actor mechanism works with TorchForge's
ForgeActor-based actors.

This enables testing GRPO training without GPUs, vLLM, or heavy dependencies.
""")

    # Run with decorators
    await simulate_with_decorators()

    # Can also run with context managers
    # with patch_actor(ForgeGenerator, MockGenerator), \
    #      patch_actor(ForgeTitanTrainer, MockTitanTrainer):
    #     await grpo_main()

    print("""
================================================================================
Demo completed successfully!
================================================================================

- Used @patch_actor to replace heavy TorchForge actors with lightweight mocks
- Ran a full GRPO training loop (generate -> reward -> train -> update weights)
- No GPUs, vLLM, or TorchTitan required!
""")


if __name__ == "__main__":
    asyncio.run(main())
