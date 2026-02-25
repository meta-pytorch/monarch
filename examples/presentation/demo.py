# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Monarch Framework Library: RLHF Components and Utilities
=======================================================

Core classes and components for Monarch RLHF demonstration:

- Core data structures (TrajectorySlice, TrainingBatch)
- GRPO algorithm components (Generator, Scorer, Learner)
- Infrastructure components (TrajectoryQueue, ReplayBuffer)
- Mock components for testing (MockScorer, InstrumentedLearner, MockGenerator)
- Simulation and analysis utilities

Separation allows main demo script to focus on orchestration.
"""

import asyncio
import copy
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import monarch
import torch
import torch.nn as nn
import torch.optim as optim
from monarch.actor import Actor, endpoint
from monarch.rdma import RDMABuffer
from torch.distributions import Categorical, kl_divergence

# Simplified hyperparameters for the demo
G = 8  # group size (number of completions per prompt)
STATE_DIM = 4  # simplified state representation
ACTION_DIM = 4  # vocabulary size (simplified)


@dataclass
class TrajectorySlice:
    """Batch of text completions from one generator call.

    In real RLHF:
    - prompts and completions
    - log probabilities from generation policy
    - rewards from human feedback or reward model

    Demo uses simplified tensor representations.
    """

    policy_version: int
    state: torch.Tensor  # Input prompt representation [STATE_DIM]
    actions: torch.Tensor  # Generated completion tokens [G]
    old_logps: torch.Tensor  # Log probabilities [G]
    rewards: torch.Tensor  # Human feedback scores [G]

    @staticmethod
    def fake(policy_version: int = 0) -> "TrajectorySlice":
        """Generate a fake trajectory slice for testing code correctness.

        Args:
            policy_version: The policy version to use (default: 0)

        Returns:
            A TrajectorySlice with randomly generated but valid test data
        """
        return TrajectorySlice(
            policy_version=policy_version,
            state=torch.randn(STATE_DIM),  # Random state representation
            actions=torch.randint(0, ACTION_DIM, (G,)),  # Random action tokens
            old_logps=torch.randn(G) * 0.5,  # Random log probabilities
            rewards=torch.randn(G),  # Random reward scores
        )


@dataclass
class TrainingBatch:
    """Batch of trajectory slices for policy updates."""

    states: torch.Tensor
    actions: torch.Tensor
    old_logps: torch.Tensor
    rewards: torch.Tensor
    policy_versions: List[int]


class TrajectoryQueue(Actor):
    """Async queue for passing completions from Generator to Scorer."""

    def __init__(self):
        self.queue: asyncio.Queue[TrajectorySlice] = asyncio.Queue()

    @endpoint
    async def put(self, slice: TrajectorySlice) -> None:
        await self.queue.put(slice)

    @endpoint
    async def get(self) -> TrajectorySlice:
        return await self.queue.get()


class ReplayBuffer(Actor):
    """Storage for scored trajectory slices with prioritized sampling.

    Newer policy versions typically produce higher quality data,
    so we weight sampling towards more recent experiences.
    """

    def __init__(self):
        self.storage: List[Tuple[int, TrajectorySlice]] = []
        self.storage_event = asyncio.Event()

    @endpoint
    async def put(self, slice: TrajectorySlice) -> None:
        self.storage.append((slice.policy_version, slice))
        self.storage_event.set()

    async def _wait_for_storage(self):
        if not self.storage:
            await self.storage_event.wait()

    @endpoint
    async def sample_from(self, k: int) -> List[TrajectorySlice]:
        """Sample k slices preferring newer policy versions."""
        try:
            await asyncio.wait_for(self._wait_for_storage(), timeout=10.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timeout waiting for ReplayBuffer to be populated")

        # Weight sampling by policy version (newer = higher weight)
        policy_versions = [version + 1 for version, _ in self.storage]
        total = sum(policy_versions)
        probs = [v / total for v in policy_versions]

        indices = list(range(len(self.storage)))
        chosen_indices = random.choices(indices, weights=probs, k=k)
        return [self.storage[i][1] for i in chosen_indices]


class Scorer(Actor):
    """Simulates human feedback by scoring text completions.

    Real RLHF would:
    - Run completions through reward model trained on human preferences
    - Interface with human annotators for online feedback
    - Handle safety filtering and content moderation

    Demo uses simple neural network to assign scores.
    """

    def __init__(self, trajectory_queue: Any, replay_buffer: Any):
        self.trajectory_queue = trajectory_queue
        self.replay_buffer = replay_buffer
        # Simple reward model (in practice, this would be much more sophisticated)
        self.net = nn.Sequential(
            nn.Linear(STATE_DIM + 1, 8),
            nn.Tanh(),
            nn.Linear(8, 1),
        ).to("cuda")
        self.running = False

    async def _score_slice(self, slice: TrajectorySlice) -> None:
        """Assign rewards to completions in trajectory slice."""
        # Combine prompt and completion for reward model
        s = slice.state.to("cuda").unsqueeze(0).repeat(G, 1)
        a = slice.actions.to("cuda").float().unsqueeze(-1)
        rewards = self.net(torch.cat([s, a], dim=-1)).squeeze(-1)

        scored = TrajectorySlice(
            policy_version=slice.policy_version,
            state=slice.state,
            actions=slice.actions,
            old_logps=slice.old_logps,
            rewards=rewards,
        )
        await self.replay_buffer.put.call(scored)

    @endpoint
    async def run(self) -> None:
        """Main scoring loop - continuously process completions."""
        if self.running:
            return

        self.running = True
        try:
            while self.running:
                try:
                    slice_ = await asyncio.wait_for(
                        self.trajectory_queue.get.call_one(),
                        timeout=10.0,
                    )
                    await self._score_slice(slice_)
                except asyncio.TimeoutError:
                    continue
        except Exception as e:
            print(f"Scorer error: {e}")
        finally:
            self.running = False

    @endpoint
    async def stop(self) -> None:
        self.running = False


class Learner(Actor):
    """Implements GRPO policy updates for RLHF.

    Core RL algorithm:
    - Computes advantages from human feedback rewards
    - Updates policy using GRPO clipped objective
    - Maintains KL divergence constraint vs reference model
    - Synchronizes weights with generators via RDMA
    """

    def __init__(self, replay_buffer: Any):
        print(f"Learner {monarch.actor.context().message_rank}")
        # Policy network (language model in practice)
        self.model = nn.Sequential(
            nn.Linear(STATE_DIM, 16), nn.Tanh(), nn.Linear(16, ACTION_DIM)
        ).to("cuda")

        # Reference model for KL penalty (frozen copy of original policy)
        self.ref_model = copy.deepcopy(self.model)
        for p in self.ref_model.parameters():
            p.requires_grad = False
        self.ref_model.eval()

        # GRPO hyperparameters
        self.optim = optim.Adam(self.model.parameters(), lr=1e-3, eps=1e-5)
        self.eps = 0.2  # GRPO clipping parameter
        self.kl_coeff = 0.1  # KL divergence coefficient

        self.policy_version = 0
        self.replay_buffer = replay_buffer
        self.batch_size = 2
        self.generators: Optional[Any] = None
        self._weights_handle: Dict[str, Tuple[RDMABuffer]] = {}

    @endpoint
    async def step(self) -> torch.Tensor:
        """Perform one GRPO training step."""
        # Notify generators of new policy version
        if self.generators:
            await self.generators.update.call(self.policy_version)

        # Sample batch from replay buffer
        slices = await self.replay_buffer(self.batch_size)
        raw_states = torch.stack([s.state for s in slices])
        actions = torch.cat([s.actions for s in slices])
        old_logps = torch.cat([s.old_logps for s in slices])
        rewards = torch.cat([s.rewards for s in slices])

        # Prepare for batch processing
        states = raw_states.repeat_interleave(G, 0).to("cuda")
        actions, old_logps, rewards = [
            x.to("cuda") for x in (actions, old_logps, rewards)
        ]

        # Update policy
        advantages = self._compute_advantages(rewards)
        return self._apply_policy_update(states, actions, old_logps, advantages)

    @endpoint
    async def init_generators(self, generators: Any) -> None:
        """Link generators for weight synchronization."""
        self.generators = generators

    @endpoint
    async def weights_handle(self) -> Dict[str, Tuple[torch.Tensor, RDMABuffer]]:
        """Create RDMA buffers for efficient weight sharing."""
        self._weights_handle = {
            k: RDMABuffer(v) for k, v in self.model.state_dict().items()
        }
        return self._weights_handle

    def _compute_advantages(self, rewards: torch.Tensor) -> torch.Tensor:
        """Compute advantages for GRPO updates.

        Advantages measure how much better each completion is
        compared to average, helping focus learning on high-quality responses.
        """
        batch_size = rewards.shape[0] // G
        rewards_reshaped = rewards.view(batch_size, G)

        # Use mean reward as baseline
        baselines = rewards_reshaped.mean(dim=1, keepdim=True)
        advantages = rewards_reshaped - baselines
        advantages = advantages.reshape(-1)

        # Normalize for stability
        if advantages.numel() > 1:
            advantages = (advantages - advantages.mean()) / (advantages.std() + 1e-8)

        return advantages

    def _apply_policy_update(
        self,
        states: torch.Tensor,
        actions: torch.Tensor,
        old_logps: torch.Tensor,
        advantages: torch.Tensor,
    ) -> torch.Tensor:
        """Apply GRPO update with KL penalty."""
        # Current policy probabilities
        dist_new = Categorical(logits=self.model(states))
        new_logps = dist_new.log_prob(actions)

        # GRPO clipped objective
        ratio = (new_logps - old_logps).exp()
        unclipped = ratio * advantages
        clipped = torch.clamp(ratio, 1 - self.eps, 1 + self.eps) * advantages
        grpo_loss = -torch.min(unclipped, clipped).mean()

        # KL penalty to prevent catastrophic forgetting
        with torch.no_grad():
            ref_logits = self.ref_model(states)
        kl = kl_divergence(Categorical(logits=ref_logits), dist_new).mean()

        # Combined loss
        loss = grpo_loss + self.kl_coeff * kl

        # Update
        self.optim.zero_grad()
        loss.backward()
        nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
        self.optim.step()
        self.policy_version += 1

        return loss.detach()


class GeneratorState:
    """State machine states for coordinating weight updates."""

    READY_TO_GENERATE = "READY_TO_GENERATE"
    READY_TO_UPDATE = "READY_TO_UPDATE"


class Generator(Actor):
    """Generates text completions using current policy.

    Real RLHF would:
    - Sample completions from language model given prompts
    - Handle batching and efficient inference
    - Coordinate with multiple GPU workers

    Key features:
    - Maintains local copy of policy weights synchronized via RDMA
    - Uses state machine to coordinate generation and weight updates
    - Produces multiple completions per prompt for better sample efficiency
    """

    def __init__(self, weight_buffers, trajectory_queue):
        self.model = nn.Sequential(
            nn.Linear(STATE_DIM, 16), nn.Tanh(), nn.Linear(16, ACTION_DIM)
        ).to("cuda")
        self.weight_buffers = weight_buffers
        self.trajectory_queue = trajectory_queue
        self.state = GeneratorState.READY_TO_GENERATE
        self.cond = asyncio.Condition()
        self.policy_version = 0

    @endpoint
    async def generate(self, state: torch.Tensor) -> None:
        """Generate completions for given prompt."""
        async with self.cond:
            # Wait for ready state
            await self.cond.wait_for(
                lambda: self.state == GeneratorState.READY_TO_GENERATE
            )

            # Generate multiple completions
            x = state.to("cuda").unsqueeze(0).repeat(G, 1)
            dist = Categorical(logits=self.model(x))
            acts = dist.sample()
            logps = dist.log_prob(acts)

            # Create trajectory slice
            slice_ = TrajectorySlice(
                self.policy_version, state, acts, logps, torch.zeros(G)
            )

        # Send for scoring
        await self.trajectory_queue.put.call(slice_)

        async with self.cond:
            # Ready for weight update
            self.state = GeneratorState.READY_TO_UPDATE
            self.cond.notify_all()

    @endpoint
    async def update(self, version: int) -> None:
        """Update policy weights from learner via RDMA."""
        async with self.cond:
            # Copy weights from RDMA buffers
            sd = self.model.state_dict()
            asyncio.gather(
                await b.read_into(sd[n]) for n, b in self.weight_buffers.items()
            )
            self.model.load_state_dict(sd)

            # Update version and state
            self.policy_version = version
            self.state = GeneratorState.READY_TO_GENERATE
            self.cond.notify_all()


# Mock Components


class MockScorer(Actor):
    """Mock scorer for testing different reward strategies."""

    def __init__(
        self,
        trajectory_queue: Any,
        replay_buffer: Any,
        reward_strategy: str = "optimistic",
    ):
        self.trajectory_queue = trajectory_queue
        self.replay_buffer = replay_buffer
        self.reward_strategy = reward_strategy
        self.running = False
        print(f"MockScorer initialized with '{reward_strategy}' reward strategy")

    async def _score_slice(self, slice: TrajectorySlice) -> None:
        """Apply different reward strategies."""
        if self.reward_strategy == "optimistic":
            # High rewards for testing policy improvement
            rewards = torch.ones(G) * 2.0
        elif self.reward_strategy == "pessimistic":
            # Low rewards for testing robustness
            rewards = torch.ones(G) * -1.0
        elif self.reward_strategy == "sparse":
            # Sparse rewards for testing exploration
            rewards = torch.zeros(G)
            rewards[0] = 5.0  # Only reward first completion
        else:
            # Random rewards
            rewards = torch.randn(G)

        scored = TrajectorySlice(
            policy_version=slice.policy_version,
            state=slice.state,
            actions=slice.actions,
            old_logps=slice.old_logps,
            rewards=rewards,
        )
        await self.replay_buffer.put.call(scored)

    @endpoint
    async def run(self) -> None:
        """Mock scoring loop with logging."""
        if self.running:
            return

        self.running = True
        processed_count = 0
        try:
            while self.running:
                try:
                    slice_ = await asyncio.wait_for(
                        self.trajectory_queue.get.call_one(),
                        timeout=10.0,
                    )
                    await self._score_slice(slice_)
                    processed_count += 1
                    if processed_count % 2 == 0:  # Log every 2 slices
                        print(
                            f"  MockScorer processed {processed_count} trajectory slices"
                        )
                except asyncio.TimeoutError:
                    continue
        except Exception as e:
            print(f"MockScorer error: {e}")
        finally:
            self.running = False

    @endpoint
    async def stop(self) -> None:
        self.running = False


class InstrumentedLearner(Actor):
    """Learner with additional instrumentation."""

    def __init__(self, replay_buffer: Any):
        self.replay_buffer = replay_buffer
        self.policy_version = 0
        self.generators: Optional[Any] = None
        self.step_count = 0
        print("InstrumentedLearner: Tracking detailed metrics")

    @endpoint
    async def init_generators(self, generators: Any) -> None:
        self.generators = generators

    @endpoint
    async def weights_handle(self) -> Dict[str, RDMABuffer]:
        # Return empty dict for simplified mock
        return {}

    @endpoint
    async def step(self) -> torch.Tensor:
        """Instrumented training step with detailed logging."""
        self.step_count += 1

        # Mock policy update
        if self.generators:
            await self.generators.update.call(self.policy_version)

        # Simulate sampling from replay buffer
        try:
            slices = await self.replay_buffer.sample_from.call_one(2)

            # Log detailed metrics
            rewards = torch.cat([s.rewards for s in slices])
            avg_reward = rewards.mean().item()
            reward_std = rewards.std().item()

            print(f"  Learner Step {self.step_count}:")
            print(f"     • Avg Reward: {avg_reward:.3f} (±{reward_std:.3f})")
            print(f"     • Batch Size: {len(slices)} trajectories")
            print(f"     • Policy Version: {self.policy_version}")

            # Mock loss (would normally be computed from GRPO)
            mock_loss = torch.tensor(abs(avg_reward) * 0.5)

        except Exception as e:
            print(f"     Replay buffer not ready: {e}")
            mock_loss = torch.tensor(1.0)

        self.policy_version += 1
        return mock_loss


class MockGenerator(Actor):
    """Simplified generator."""

    def __init__(self, weight_buffers, trajectory_queue):
        self.trajectory_queue = trajectory_queue
        self.policy_version = 0
        print("MockGenerator: Fast completion generation")

    @endpoint
    async def generate(self, state: torch.Tensor) -> None:
        """Generate mock completions instantly."""
        # Create mock trajectory slice
        slice_ = TrajectorySlice(
            self.policy_version,
            state,
            torch.randint(0, ACTION_DIM, (G,)),  # Random tokens
            torch.randn(G) * 0.5,  # Random log probs
            torch.zeros(G),  # Empty rewards (filled by scorer)
        )
        await self.trajectory_queue.put.call(slice_)

    @endpoint
    async def update(self, version: int) -> None:
        """Mock weight update."""
        self.policy_version = version


# Performance Analysis


def create_simplified_learner_model():
    """Create simplified model representing language model learner."""
    return nn.Sequential(
        nn.Linear(512, 1024),  # Larger dimensions for realistic analysis
        nn.ReLU(),
        nn.Linear(1024, 1024),
        nn.ReLU(),
        nn.Linear(1024, 512),
        nn.LayerNorm(512),
    ).to("cuda")


def simulate_learner_training_step(
    model, optimizer, loss_fn, batch_size=8, seq_len=128
):
    """Simulate training step with realistic tensor operations."""

    print("Simulating Learner training step:")
    print(f"   • Batch size: {batch_size}")
    print(f"   • Sequence length: {seq_len}")
    print(f"   • Model parameters: {sum(p.numel() for p in model.parameters()):,}")

    # Create realistic training data
    input_ids = torch.randint(0, 1000, (batch_size, seq_len, 512), device="cuda")
    target = torch.randn(batch_size, seq_len, 512, device="cuda")

    # Forward pass with gradient computation
    optimizer.zero_grad()
    output = model(input_ids)
    loss = loss_fn(output, target)

    # Backward pass
    loss.backward()

    # Simulate gradient communication using Stream API
    comms = monarch.Stream("gradient_communication")

    # Simple communication pattern
    # Create sample gradient for communication simulation
    sample_grad = torch.randn(1024, device="cuda")
    grad_comm, borrow = comms.borrow(sample_grad, mutable=True)
    with comms.activate():
        grad_comm.reduce_("gpu", "sum")
    borrow.drop()

    # Apply optimizer step
    optimizer.step()

    # Return simple mock loss value
    return 0.42
