# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
GRPO with Tensor Engine and Actors
==================================
This example demonstrates using the Monarch tensor engine for distributed
training within actors. The implementation shows how to use mesh.activate()
inside actor endpoints for distributed tensor operations.

Run with::

    buck2 run -c fbcode.enable_gpu_sections=true \\
        //monarch/docs/source/examples:grpo_actor_te

Simplifications from grpo_actor.py
----------------------------------
The full grpo_actor.py example uses multiple actors (Generator, Scorer, Learner,
TrajectoryQueue, ReplayBuffer) with RDMA-based weight synchronization and async
queues. This simplified version consolidates everything into a single GRPOTrainer
actor to focus on demonstrating tensor engine integration.

Tensor engine requires all operations inside activate() context**: The full
example passes tensors between actors via queues/RDMA, but tensor engine's
distributed tensors cannot be serialized across actor boundaries. By keeping
all operations in one actor, we can run the entire GRPO algorithm (generation,
scoring, training) inside a single activate() context.

Some features in the full example don't work
with tensor engine's distributed tensors:
- ``torch.distributions.Categorical`` (uses unsupported ops)
- RDMA buffer serialization of distributed tensors
- Passing distributed tensors through async queues
"""

import asyncio

import torch
import torch.nn as nn
import torch.optim as optim
from monarch.actor import Actor, context, endpoint, this_host
from monarch.fetch import fetch_shard

# %%
# Configuration
G = 8  # group size for GRPO
STATE_DIM = 4
ACTION_DIM = 4
BATCH_SIZE = 2


# %%
class GRPOTrainer(Actor):
    """GRPO trainer using tensor engine for distributed training.

    This actor demonstrates tensor engine integration within an actor.
    The training loop runs inside proc_mesh.activate() context for
    distributed tensor operations.
    """

    def __init__(self) -> None:
        """Initialize the trainer with tensor engine mesh."""
        # Get the proc_mesh from actor context
        self.proc_mesh = context().actor_instance.proc_mesh

        # Create models inside tensor engine context
        with self.proc_mesh.activate():
            torch.set_default_device("cuda")

            # Policy network
            self.model = nn.Sequential(
                nn.Linear(STATE_DIM, 16), nn.Tanh(), nn.Linear(16, ACTION_DIM)
            )

            # Reference network for KL penalty
            self.ref_model = nn.Sequential(
                nn.Linear(STATE_DIM, 16), nn.Tanh(), nn.Linear(16, ACTION_DIM)
            )
            for p in self.ref_model.parameters():
                p.requires_grad = False

            # Optimizer (fused=True required for tensor engine compatibility)
            self.optim = optim.Adam(
                self.model.parameters(), lr=1e-3, eps=1e-5, fused=True
            )

        # PPO hyperparameters
        self.eps = 0.2
        self.kl_coeff = 0.1
        self.step_count = 0

    @endpoint
    async def train_step(self) -> float:
        """Perform one training step using tensor engine.

        All training operations (forward, backward, optimizer step) happen
        inside the proc_mesh.activate() context for distributed operations.

        Returns:
            Loss value from the update
        """
        with self.proc_mesh.activate():
            # ========================================
            # GENERATION PHASE
            # ========================================
            # Generate random states (inside activate context)
            states = torch.randn(BATCH_SIZE, STATE_DIM, device="cuda")

            # Expand states for group size
            states_expanded = states.repeat_interleave(G, 0)

            # Generate actions from policy
            with torch.no_grad():
                logits = self.model(states_expanded)
                probs = torch.softmax(logits, dim=-1)
                actions = torch.multinomial(probs, num_samples=1).squeeze(-1)
                old_logps = (
                    torch.log_softmax(logits, dim=-1)
                    .gather(1, actions.unsqueeze(-1))
                    .squeeze(-1)
                )

            # ========================================
            # SCORING PHASE
            # ========================================
            # Simple reward: negative distance from mean action
            rewards = -torch.abs(actions.float() - actions.float().mean())

            # ========================================
            # TRAINING PHASE
            # ========================================
            # Compute advantages (GRPO uses group-based advantage)
            rewards_reshaped = rewards.view(BATCH_SIZE, G)
            baselines = rewards_reshaped.mean(dim=1, keepdim=True)
            advantages = (rewards_reshaped - baselines).reshape(-1)
            if advantages.numel() > 1:
                adv_mean = advantages.mean()
                adv_std = advantages.std()
                advantages = (advantages - adv_mean) / (adv_std + 1e-8)

            # Forward pass - compute new log probabilities
            logits = self.model(states_expanded)
            log_probs = torch.log_softmax(logits, dim=-1)
            new_logps = log_probs.gather(1, actions.unsqueeze(-1)).squeeze(-1)

            # PPO clipped objective
            ratio = (new_logps - old_logps).exp()
            unclipped = ratio * advantages
            clipped = torch.clamp(ratio, 1 - self.eps, 1 + self.eps) * advantages
            ppo_loss = -torch.min(unclipped, clipped).mean()

            # KL penalty
            with torch.no_grad():
                ref_logits = self.ref_model(states_expanded)
            ref_log_probs = torch.log_softmax(ref_logits, dim=-1)
            probs = torch.softmax(logits, dim=-1)
            kl = (probs * (log_probs - ref_log_probs)).sum(dim=-1).mean()

            # Total loss
            loss = ppo_loss + self.kl_coeff * kl

            # Backward pass
            self.optim.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
            self.optim.step()

            # Materialize loss using fetch_shard for async context
            try:
                loss_value = await fetch_shard(loss.detach())
                result = loss_value.item()
            except Exception:
                # Simulator mode: fetch_shard may return incompatible Future
                result = 0.0

        self.step_count += 1
        return result


# %%
async def main():
    """Run GRPO training with tensor engine inside actors.

    This demonstrates:
    1. Tensor engine integration within actors via proc_mesh.activate()
    2. Distributed tensor operations for training
    3. Using fetch_shard() for async-safe materialization
    """
    # Create process mesh for actors
    actor_mesh = this_host().spawn_procs(per_host={"gpus": 1})

    # Spawn trainer actor
    trainer = actor_mesh.spawn("trainer", GRPOTrainer)

    print("Starting GRPO training with tensor engine...")

    # Training loop
    for step in range(5):
        loss = await trainer.train_step.call_one()
        print(f"[Step {step:02d}] loss={loss:.4f}")

    print("Training complete!")


# %%
if __name__ == "__main__":
    asyncio.run(main())
