# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Distributed PPO-like Reinforcement Learning with Monarch Actors
==============================================================
This example demonstrates implementing a distributed PPO-like reinforcement learning
algorithm using the Monarch actor framework. The implementation features:
- Distributed actor architecture with Generator, Scorer, and Learner components
- Asynchronous communication via queues
- RDMA-based weight synchronization
- Event-driven architecture for efficient processing
The example shows how to:
- Set up distributed actors on separate GPU meshes
- Implement policy gradient methods in a distributed setting
- Use RDMA buffers for efficient parameter sharing
- Create an asynchronous training loop with multiple components
"""

import os

os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

# %%
import asyncio
import copy
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import torch
import torch.nn as nn
import torch.optim as optim
from monarch.actor import Actor, endpoint, this_host
from monarch.rdma import RDMABuffer
from torch.distributions import Categorical, kl_divergence

# %%
"""
Online reinforcement learning (RL) training loop using the Monarch actor framework.
This example implements a distributed PPO-like algorithm with three main components:
1. Generator: Produces actions using the current policy and sends them for scoring
2. Scorer: Evaluates actions and assigns rewards
3. Learner: Updates policy based on collected experiences

Key features demonstrated:
- Distributed actors on separate GPU meshes
- Asynchronous communication via queues
- RDMA-based weight synchronization
- Event-driven architecture
"""
G = 8  # group size
STATE_DIM = 4
ACTION_DIM = 4  # vocab size


# %%
@dataclass
class TrajectorySlice:
    """Single trajectory from one generator call.

    Attributes:
        policy_version: Version of policy that produced this slice
        state: Input state tensor [STATE_DIM]
        actions: Generated actions [G]
        old_logps: Log probabilities of actions under generation policy [G]
        rewards: Rewards for each action (initially zeros, filled by Scorer) [G]
    """

    policy_version: int
    state: torch.Tensor
    actions: torch.Tensor
    old_logps: torch.Tensor
    rewards: torch.Tensor


# %%
@dataclass
class TrainingBatch:
    """Batch of trajectories for training.

    Attributes:
        states: Batched states [batch_size, STATE_DIM]
        actions: Batched actions [batch_size * G]
        old_logps: Batched log probabilities [batch_size * G]
        rewards: Batched rewards [batch_size * G]
        policy_versions: List of policy versions for each slice
    """

    states: torch.Tensor
    actions: torch.Tensor
    old_logps: torch.Tensor
    rewards: torch.Tensor
    policy_versions: List[int]


# %%
class TrajectoryQueue(Actor):
    """Queue for trajectory slices between Generator and Scorer."""

    def __init__(self):
        """Initialize an empty queue."""
        self.queue: asyncio.Queue[TrajectorySlice] = asyncio.Queue()

    @endpoint
    async def put(self, slice: TrajectorySlice) -> None:
        """Add a trajectory slice to the queue.

        Args:
            slice: The trajectory slice to add
        """
        await self.queue.put(slice)

    @endpoint
    async def get(self) -> TrajectorySlice:
        """Remove and return a trajectory slice from the queue.
        Returns:
            The next trajectory slice in the queue
        """
        return await self.queue.get()


# %%
class ReplayBuffer(Actor):
    """Storage for scored trajectory slices with weighted sampling."""

    def __init__(self):
        """Initialize an empty buffer."""
        self.storage: List[Tuple[int, TrajectorySlice]] = []  # (version, slice)
        self.storage_event = asyncio.Event()

    @endpoint
    async def put(self, slice: TrajectorySlice) -> None:
        """Add a trajectory slice to the buffer.

        Args:
            slice: The trajectory slice to add
        """
        self.storage.append((slice.policy_version, slice))
        self.storage_event.set()

    async def _wait_for_storage(self):
        if not self.storage:
            await self.storage_event.wait()

    @endpoint
    async def sample_from(self, k: int) -> List[TrajectorySlice]:
        """Sample k trajectory slices using weighted sampling.

        Items from newer policy versions have higher probability of being selected.
        If the buffer is empty, waits for it to be populated with a timeout.

        Args:
            k: Number of slices to sample

        Returns:
            List of sampled trajectory slices

        Raises:
            RuntimeError: If buffer is empty after timeout
        """
        try:
            await asyncio.wait_for(self._wait_for_storage(), timeout=10.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timeout waiting for ReplayBuffer to be populated")

        # Extract policy versions and add 1 to ensure all weights are positive
        policy_versions = [version + 1 for version, _ in self.storage]

        # Use policy versions as weights for sampling
        total = sum(policy_versions)
        probs = [v / total for v in policy_versions]

        # Sample indices based on policy version weights
        indices = list(range(len(self.storage)))
        chosen_indices = random.choices(indices, weights=probs, k=k)
        return [self.storage[i][1] for i in chosen_indices]


# %%
class Scorer(Actor):
    """Evaluates actions and assigns rewards to trajectory slices."""

    def __init__(self, trajectory_queue: Any, replay_buffer: Any):
        """Initialize the scorer.

        Args:
            trajectory_queue: Queue to pull trajectory slices from
            replay_buffer: Buffer to store scored slices in
        """
        self.trajectory_queue = trajectory_queue
        self.replay_buffer = replay_buffer
        self.net = nn.Sequential(
            nn.Linear(STATE_DIM + 1, 8),
            nn.Tanh(),
            nn.Linear(8, 1),
        ).to("cuda")
        self.running = False

    async def _score_slice(self, slice: TrajectorySlice) -> None:
        """Score a trajectory slice and store it in the replay buffer.

        Args:
            slice: The trajectory slice to score
        """
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
        """Start the scoring event loop.

        Continuously pulls slices from the queue, scores them,
        and puts them in the replay buffer until stopped.
        """
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
            print(f"Scorer event loop error: {e}")
        finally:
            self.running = False

    @endpoint
    async def stop(self) -> None:
        """Stop the scoring event loop."""
        self.running = False


# %%
class Learner(Actor):
    """Updates policy based on collected experiences using PPO algorithm."""

    def __init__(self, replay_buffer: Any):
        """Initialize the learner.

        Args:
            replay_buffer: Buffer to sample experiences from
        """
        # Policy network and reference network for KL divergence
        self.model = nn.Sequential(
            nn.Linear(STATE_DIM, 16), nn.Tanh(), nn.Linear(16, ACTION_DIM)
        ).to("cuda")
        self.ref_model = copy.deepcopy(self.model)
        for p in self.ref_model.parameters():
            p.requires_grad = False
        self.ref_model.eval()

        # Optimization parameters
        self.optim = optim.Adam(self.model.parameters(), lr=1e-3, eps=1e-5)
        self.eps = 0.2  # PPO clipping parameter
        self.kl_coeff = 0.1  # KL divergence coefficient
        self.policy_version = 0
        self.replay_buffer = replay_buffer
        self.batch_size = 2
        self.generators: Optional[Any] = None
        self._weights_handle: Dict[str, Tuple[torch.Tensor, RDMABuffer]] = {}

    @endpoint
    async def init_generators(self, generators: Any) -> None:
        """Set the generators service for weight updates.

        Args:
            generators: Service to notify of policy updates
        """
        self.generators = generators

    @endpoint
    async def weights_handle(self) -> Dict[str, Tuple[torch.Tensor, RDMABuffer]]:
        """Create RDMA buffers for model weights.

        Returns:
            Dictionary mapping parameter names to RDMA buffers
        """
        self._weights_handle = {
            k: (v, RDMABuffer(v.view(torch.uint8).flatten()))
            for k, v in self.model.state_dict().items()
        }
        return self._weights_handle

    def _compute_advantages(self, rewards: torch.Tensor) -> torch.Tensor:
        """Compute advantages from rewards.

        In PPO, advantages represent how much better an action is compared to the average.
        Here we compute advantages by subtracting a baseline (mean reward) from the rewards
        and then normalizing to stabilize training.

        Args:
            rewards: Raw rewards tensor [batch_size * G]

        Returns:
            Advantages tensor [batch_size * G]
        """
        # First, reshape rewards to [batch_size, G] to compute per-state baseline
        batch_size = rewards.shape[0] // G
        rewards_reshaped = rewards.view(batch_size, G)

        # Compute baseline (mean reward) for each state
        baselines = rewards_reshaped.mean(dim=1, keepdim=True)  # [batch_size, 1]

        # Subtract baseline from rewards to get advantages
        advantages = rewards_reshaped - baselines  # [batch_size, G]

        # Reshape back to original shape
        advantages = advantages.reshape(-1)  # [batch_size * G]

        # Normalize advantages for training stability
        if advantages.numel() > 1:  # Check if we have more than one element
            advantages = (advantages - advantages.mean()) / (advantages.std() + 1e-8)

        return advantages

    def _apply_policy_update(
        self,
        states: torch.Tensor,
        actions: torch.Tensor,
        old_logps: torch.Tensor,
        advantages: torch.Tensor,
    ) -> torch.Tensor:
        """Apply PPO update to policy network.

        Args:
            states: Batch of states
            actions: Batch of actions
            old_logps: Log probabilities from old policy
            advantages: Normalized advantages

        Returns:
            Loss value
        """
        # Compute new policy distribution and log probabilities
        dist_new = Categorical(logits=self.model(states))
        new_logps = dist_new.log_prob(actions)

        # PPO clipped objective
        ratio = (new_logps - old_logps).exp()
        unclipped = ratio * advantages
        clipped = torch.clamp(ratio, 1 - self.eps, 1 + self.eps) * advantages
        ppo_loss = -torch.min(unclipped, clipped).mean()

        # KL penalty to prevent large policy updates
        with torch.no_grad():
            ref_logits = self.ref_model(states)
        kl = kl_divergence(Categorical(logits=ref_logits), dist_new).mean()

        # Update policy
        loss = ppo_loss + self.kl_coeff * kl
        self.optim.zero_grad()
        loss.backward()
        nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
        self.optim.step()
        self.policy_version += 1

        # Return loss value
        return loss.detach()

    @endpoint
    async def step(self) -> torch.Tensor:
        """Perform one training step.

        Returns:
            Loss value from the update
        """
        # Notify generators of current policy version
        if self.generators:
            await self.generators.update.call(self.policy_version)

        # Sample and process trajectory slices
        slices = await self.replay_buffer.sample_from.call_one(self.batch_size)
        raw_states = torch.stack([s.state for s in slices])
        actions = torch.cat([s.actions for s in slices])
        old_logps = torch.cat([s.old_logps for s in slices])
        rewards = torch.cat([s.rewards for s in slices])

        # Prepare tensors for update
        states = raw_states.repeat_interleave(G, 0).to("cuda")
        actions, old_logps, rewards = [
            x.to("cuda") for x in (actions, old_logps, rewards)
        ]
        # Compute advantages and update policy
        advs = self._compute_advantages(rewards)
        return self._apply_policy_update(states, actions, old_logps, advs)


# %%
class GeneratorState:
    """States for the Generator's state machine."""

    READY_TO_GENERATE = "READY_TO_GENERATE"
    READY_TO_UPDATE = "READY_TO_UPDATE"


# %%
class Generator(Actor):
    """Generates actions using the current policy.
    Maintains a copy of the policy network that is synchronized with the Learner
    via RDMA buffers. Generates actions for given states and sends them to the
    trajectory queue for scoring.
    """

    def __init__(self, weight_buffers, trajectory_queue):
        """Initialize the generator.

        Args:
            weight_buffers: RDMA buffers for policy weights
            trajectory_queue: Queue to put generated trajectories in
        """
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
        """Generate actions for a given state.

        Args:
            state: Input state tensor [STATE_DIM]
        """
        async with self.cond:
            # Wait until ready to generate
            await self.cond.wait_for(
                lambda: self.state == GeneratorState.READY_TO_GENERATE
            )

            # Generate actions using current policy
            x = state.to("cuda").unsqueeze(0).repeat(G, 1)
            dist = Categorical(logits=self.model(x))
            acts = dist.sample()
            logps = dist.log_prob(acts)

            # Create trajectory slice
            slice_ = TrajectorySlice(
                self.policy_version,
                state,
                acts,
                logps,
                torch.zeros(G),
            )

        # Send to trajectory queue for scoring
        await self.trajectory_queue.put.call(slice_)

        async with self.cond:
            # Signal ready for update
            self.state = GeneratorState.READY_TO_UPDATE
            self.cond.notify_all()

    @endpoint
    async def update(self, version: int) -> None:
        """Update policy weights from RDMA buffers.

        Args:
            version: New policy version number
        """
        async with self.cond:
            # Copy weights from RDMA buffers
            sd = self.model.state_dict()
            for n, (_, b) in self.weight_buffers.items():
                await b.read_into(sd[n].view(torch.uint8).flatten())
            self.model.load_state_dict(sd)
            # Update version and state
            self.policy_version = version
            self.state = GeneratorState.READY_TO_GENERATE
            self.cond.notify_all()


# %%
async def main():
    """Run the distributed reinforcement learning training loop."""
    # Create process meshes for different components
    learner_mesh = this_host().spawn_procs(per_host={"gpus": 1})
    gen_mesh = this_host().spawn_procs(per_host={"gpus": 2})

    # Spawn actors on the learner mesh
    traj_q = learner_mesh.spawn("traj", TrajectoryQueue)
    replay_buf = learner_mesh.spawn("rb", ReplayBuffer)
    learner = learner_mesh.spawn("learner", Learner, replay_buf)
    scorer = learner_mesh.spawn("scorer", Scorer, traj_q, replay_buf)

    # Get weight buffers and spawn generators on the generator mesh
    wb = await learner.weights_handle.call_one()
    generators = gen_mesh.spawn(
        "generator",
        Generator,
        wb,
        traj_q,
    )
    await learner.init_generators.call(generators)

    # initial generator entry
    await generators.generate.call(torch.randn(STATE_DIM))
    # Start the scorer event loop in the background
    scorer_run_future = scorer.run.call_one()

    for step in range(5):
        state = torch.randn(STATE_DIM)
        # Generate actions and update policy in parallel
        _, loss = await asyncio.gather(
            generators.generate.call(state),
            learner.step.call_one(),
        )
        print(f"[Step {step:02d}] loss={loss:.3f}")
    # Clean up - stop the scorer and wait for background task to complete
    print("🛑 Stopping scorer...")
    await scorer.stop.call_one()
    await scorer_run_future

    print("✅ Training complete")


# %%
if __name__ == "__main__":
    asyncio.run(main())
