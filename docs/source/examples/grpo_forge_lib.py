# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Mock actors and training loop for TorchForge GRPO simulation.

This module provides lightweight mock implementations of TorchForge actors
that can be used with Monarch's patch_actor mechanism to test GRPO training
without requiring GPUs, vLLM, or TorchTitan.

See grpo_forge_sim.py for usage examples.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import torch

# TorchForge actor imports (requires conda environment with Forge installed)
from forge.actors.generator import Generator as ForgeGenerator
from forge.actors.reference_model import ReferenceModel as ForgeReferenceModel
from forge.actors.replay_buffer import ReplayBuffer as ForgeReplayBuffer
from forge.actors.trainer import TitanTrainer as ForgeTitanTrainer
from forge.rl.advantage import ComputeAdvantages as ForgeComputeAdvantages
from forge.rl.grading import RewardActor as ForgeRewardActor
from monarch.actor import Actor, endpoint, this_host


# ==============================================================================
# Mock Actor Implementations
# ==============================================================================


class MockGenerator(Actor):
    """Mock generator that returns dummy completions without vLLM."""

    def __init__(self, **kwargs: Any) -> None:
        self._step = 0

    @endpoint
    async def setup(self) -> None:
        print("[MockGenerator] setup (mocked)")

    @endpoint
    async def generate(self, prompt: str) -> List[Any]:
        """Return mock completions."""

        @dataclass
        class MockCompletion:
            text: str
            prompt_ids: torch.Tensor
            token_ids: torch.Tensor
            stop_reason: str = "eos"
            generator_version: int = 0

        return [
            MockCompletion(
                text=f"Mock answer {i}: 42",
                prompt_ids=torch.zeros(10, dtype=torch.long),
                token_ids=torch.zeros(20, dtype=torch.long),
                generator_version=self._step,
            )
            for i in range(4)
        ]

    @endpoint
    async def update_weights(self, step: int) -> None:
        print(f"[MockGenerator] update_weights -> step {step}")
        self._step = step


class MockTitanTrainer(Actor):
    """Mock trainer that simulates training steps."""

    def __init__(self, **kwargs: Any) -> None:
        self._step = 0

    @endpoint
    async def setup(self) -> None:
        print("[MockTitanTrainer] setup (mocked)")

    @endpoint
    async def train_step(self, inputs: torch.Tensor, targets: Dict[str, Any]) -> None:
        self._step += 1
        print(f"[MockTitanTrainer] train_step #{self._step}")

    @endpoint
    async def push_weights(self, step: int) -> None:
        print(f"[MockTitanTrainer] push_weights step={step}")


class MockReferenceModel(Actor):
    """Mock reference model that returns dummy logprobs."""

    def __init__(self, **kwargs: Any) -> None:
        pass

    @endpoint
    async def setup(self) -> None:
        print("[MockReferenceModel] setup (mocked)")

    @endpoint
    async def forward(
        self,
        input_ids: torch.Tensor,
        max_req_tokens: int,
        return_logprobs: bool = True,
    ) -> torch.Tensor:
        batch_size = input_ids.shape[0]
        response_len = max(input_ids.shape[1] - max_req_tokens, 1)
        return torch.zeros(batch_size, response_len)


class MockRewardActor(Actor):
    """Mock reward actor that returns varied rewards."""

    def __init__(self, **kwargs: Any) -> None:
        self._call_count = 0

    @endpoint
    async def setup(self) -> None:
        print("[MockRewardActor] setup (mocked)")

    @endpoint
    async def evaluate_response(
        self, prompt: str, response: str, target: str
    ) -> tuple[Dict[str, float], float]:
        self._call_count += 1
        reward = random.uniform(0.5, 1.0)
        return ({"mock_reward": reward}, reward)


class MockReplayBuffer(Actor):
    """Mock replay buffer."""

    def __init__(self, **kwargs: Any) -> None:
        self._buffer: List[Any] = []

    @endpoint
    async def setup(self) -> None:
        print("[MockReplayBuffer] setup (mocked)")

    @endpoint
    async def add(self, episode: Any) -> None:
        self._buffer.append(episode)

    @endpoint
    async def sample(self, curr_policy_version: int) -> Optional[Any]:
        if len(self._buffer) < 4:
            return None
        return (torch.zeros(4, 32), {"advantages": torch.ones(4)})


class MockComputeAdvantages(Actor):
    """Mock advantage computation."""

    def __init__(self, **kwargs: Any) -> None:
        pass

    @endpoint
    async def setup(self) -> None:
        print("[MockComputeAdvantages] setup (mocked)")

    @endpoint
    async def compute(self, episodes: List[Any]) -> List[float]:
        return [1.0] * len(episodes)


# ==============================================================================
# GRPO Training Loop
# ==============================================================================


async def grpo_training_loop(
    generator: Any,
    trainer: Any,
    ref_model: Any,
    reward_actor: Any,
    replay_buffer: Any,
    compute_advantages: Any,
    num_steps: int = 3,
) -> None:
    """Simplified GRPO training loop mirroring TorchForge's apps/grpo/main.py."""
    print("\n" + "-" * 60)
    print("GRPO Training Loop")
    print("-" * 60 + "\n")

    for step in range(num_steps):
        prompt = f"What is {step + 1} + {step + 1}?"
        responses = await generator.generate.call_one(prompt)
        print(f"[Step {step}] Generated {len(responses)} responses")

        for i, response in enumerate(responses):
            reward_breakdown, reward = await reward_actor.evaluate_response.call_one(
                prompt=prompt, response=response.text, target=str((step + 1) * 2)
            )
            print(f"  Response {i}: reward={reward:.3f}")

        input_ids = torch.zeros(len(responses), 32, dtype=torch.long)
        await ref_model.forward.call_one(
            input_ids, max_req_tokens=10, return_logprobs=True
        )

        episodes = [{"response": r, "reward": 1.0} for r in responses]
        await compute_advantages.compute.call_one(episodes)

        for episode in episodes:
            await replay_buffer.add.call_one(episode)

        batch = await replay_buffer.sample.call_one(curr_policy_version=step)
        if batch is not None:
            inputs, targets = batch
            await trainer.train_step.call(inputs, targets)
            await trainer.push_weights.call(step)
            await generator.update_weights.call_one(step)

        print(f"[Step {step}] Complete\n")

    print("-" * 60)
    print("Training Complete!")
    print("-" * 60)


async def main() -> None:
    """
    Main entry point that spawns actors and runs the training loop.

    This function spawns real TorchForge actors. To run with mocks,
    use patch_actor decorators as shown in grpo_forge_sim.py.
    """
    host = this_host()
    proc_mesh = host.spawn_procs(per_host={"procs": 1})

    print("Spawning actors...")
    generator = proc_mesh.spawn("generator", ForgeGenerator)
    trainer = proc_mesh.spawn("trainer", ForgeTitanTrainer)
    ref_model = proc_mesh.spawn("ref_model", ForgeReferenceModel)
    reward_actor = proc_mesh.spawn("reward_actor", ForgeRewardActor)
    replay_buffer = proc_mesh.spawn("replay_buffer", ForgeReplayBuffer)
    compute_advantages = proc_mesh.spawn("compute_advantages", ForgeComputeAdvantages)

    print("Setting up actors...")
    await generator.setup.call()
    await trainer.setup.call()
    await ref_model.setup.call()
    await reward_actor.setup.call()
    await replay_buffer.setup.call()
    await compute_advantages.setup.call()

    await grpo_training_loop(
        generator=generator,
        trainer=trainer,
        ref_model=ref_model,
        reward_actor=reward_actor,
        replay_buffer=replay_buffer,
        compute_advantages=compute_advantages,
        num_steps=3,
    )
