# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
GRPO with Direct Port Messaging
===============================

This example runs the same GRPO-style task as ``grpo_actor.py`` — group-
relative advantages, PPO-clip objective, KL penalty against a reference
policy — but wires the actors together with plain Monarch ports instead
of queue-style intermediary actors and ``RDMABuffer`` weight sync.

A *pool* of ``NUM_GENERATORS`` ``Generator`` actors produces rollouts in
parallel, all writing into the same rollouts port. A single ``Scorer``,
``RefModel``, and ``Trainer`` pipeline processes them::

    Generator x4 ─┐
                  │ Rollout    +--------+   ScoredRollout   +----------+
                  ├──────────► | Scorer | ────────────────► | RefModel |
                  │            +--------+                   +----+-----+
                  │                                              │
                  │                                              │ RefScoredRollout
                  │                                              ▼
                  │                                        +-----------+
                  │    send(update_weights,                |           |
                  │         selection="choose")            |  Trainer  |
                  └◄───────────────────────────────────────|           |
                                                           +-----+-----+
                                                                 │
                                                                 │ once: MAX_STEPS
                                                                 ▼
                                                             controller

The only hop that is *not* a port send is the weight update. The
``Trainer`` uses ``send(gen_mesh.update_weights, ..., selection="choose")``
to dispatch each fresh ``state_dict`` to exactly one of the four
generators, fire-and-forget. This pattern load-balances weight transfer
across the pool and mirrors how real asynchronous RL systems drift
generator and trainer replicas in and out of lock-step.

Coroutine analogy
-----------------

Each ``run`` endpoint reads like a Python generator function that both
sends values and receives values back through ``yield``::

    def producer():
        while True:
            x = work()
            y = yield x          # send x, receive y

    def transformer():
        while True:
            x = yield            # receive
            yield transform(x)   # send

In the actor version we replace ``yield value`` with
``port.send(value)`` and a bare ``yield`` (for receiving) with
``await recv.recv()``. The control flow is the same, but the
"coroutines" live in separate processes and communicate through the
Monarch mailbox system.

Concurrent endpoints on one actor
---------------------------------

The ``Generator`` needs to do two things at once: keep producing
rollouts, and accept weight updates whenever the trainer fires them.
Monarch runs async endpoints on the same actor concurrently on one
event loop, so we simply define two endpoints — ``run`` (the rollout
loop) and ``update_weights`` — and let them communicate through an
internal ``asyncio.Queue``. ``update_weights`` enqueues a new
``state_dict`` and returns immediately; ``run`` drains the queue at the
top of each iteration. The queue cleanly separates the "enqueue" side
from the "consume" side without a second actor.

Shutdown has two phases. The rollout loop runs as an ``asyncio``
background task (launched by a one-shot ``start`` endpoint rather than
as a long-lived endpoint invocation, so that ``ActorMesh.stop()`` can
always make progress). When the trainer finishes ``MAX_STEPS`` it
calls ``gen_mesh.drain.call()`` — a dedicated endpoint that cancels
the rollout loop on each generator and fires a shutdown-sentinel
``None`` down ``rollouts_out``. ``Scorer`` and ``RefModel`` forward
the first ``None`` they see and return. The trainer then calls
``gen_mesh.stop()`` to release the pool; each generator's
``__cleanup__`` runs as a safety net, cancelling the task if ``drain``
was not already called. ``__cleanup__`` is used only for resources the
actor owns (here, its asyncio task) — messaging goes through regular
endpoints.

Contrast with ``grpo_actor.py``
-------------------------------

``grpo_actor.py`` funnels data through dedicated queue actors
(``TrajectoryQueue``, ``ReplayBuffer``) and copies weights via
``RDMABuffer``. Here we use neither — only ``Channel.open()``,
``Port.send(...)``, and one ``send(..., selection="choose")`` call for
load-balanced weight dispatch. The code is shorter, and the data-flow
graph is visible at a glance in ``main``.

Ports and RDMA are compatible
-----------------------------

Direct port messaging and ``RDMABuffer`` are not mutually exclusive.
``RDMABuffer`` handles are themselves picklable, so the trainer could
equally well call
``send(gen_mesh.update_weights, args=(version, rdma_buffers), ...,
selection="choose")`` and have each generator ``read_into`` its local
tensors. That retains the zero-copy GPU-to-GPU path of
``grpo_actor.py`` while keeping the port-driven control flow shown
here.
"""

# %%
import asyncio
import copy
from dataclasses import dataclass

import torch
import torch.nn as nn
import torch.optim as optim
from monarch.actor import Actor, Channel, endpoint, Port, send, this_host
from torch.distributions import Categorical

# %%
# Problem setup
# -------------
# A synthetic GRPO task. On each rollout the generator draws a random
# state and samples ``G`` actions from the current policy, forming a
# GRPO group. The scorer — a fixed small MLP — assigns a reward to each
# (state, action) pair. The reference model is a frozen snapshot of the
# policy at step 0 and provides the log-probs used in the KL penalty.
# The trainer applies a PPO-clip update with a KL-to-reference term and
# ships the fresh weights to one of the generators.

STATE_DIM = 4
ACTION_DIM = 4
HIDDEN = 8
G = 8  # group size
NUM_GENERATORS = 4
MAX_STEPS = 20
PPO_CLIP = 0.2
KL_COEFF = 0.05


def build_policy() -> nn.Module:
    return nn.Sequential(
        nn.Linear(STATE_DIM, HIDDEN),
        nn.Tanh(),
        nn.Linear(HIDDEN, ACTION_DIM),
    )


def build_reward() -> nn.Module:
    """Non-trainable reward model used by the scorer."""
    net = nn.Sequential(
        nn.Linear(STATE_DIM + 1, HIDDEN),
        nn.Tanh(),
        nn.Linear(HIDDEN, 1),
    )
    for p in net.parameters():
        p.requires_grad = False
    return net


# %%
# Messages around the ring
# ------------------------


@dataclass
class Rollout:
    state: torch.Tensor  # [STATE_DIM]
    actions: torch.Tensor  # [G] long
    logps: torch.Tensor  # [G] float, log-prob under the sampling policy


@dataclass
class ScoredRollout:
    state: torch.Tensor
    actions: torch.Tensor
    logps: torch.Tensor
    rewards: torch.Tensor  # [G]


@dataclass
class RefScoredRollout:
    state: torch.Tensor
    actions: torch.Tensor
    logps: torch.Tensor
    rewards: torch.Tensor
    ref_logps: torch.Tensor  # [G]


# %%
# Generator pool
# --------------
# Four ``Generator`` actors share one ``rollouts_out`` port — the
# Scorer's input. All four concurrently push rollouts into that port;
# the Scorer processes them in arrival order.
#
# Each generator exposes two concurrent async endpoints:
#
# - ``run``: a tight rollout loop, kicked off once at startup.
# - ``update_weights``: enqueues a fresh ``state_dict`` for the loop to
#   apply on its next iteration.
#
# The endpoints coordinate through an ``asyncio.Queue`` internal to the
# actor. Because Monarch schedules async endpoints cooperatively on one
# event loop, ``update_weights`` can land while ``run`` is between
# iterations, and vice versa.


class Generator(Actor):
    def __init__(self, rollouts_out: Port):
        self.model = build_policy()
        self.rollouts_out = rollouts_out
        # "Enqueue side" of the weight-update protocol. ``update_weights``
        # writes here; the rollout loop reads from here at the top of
        # each iteration. The queue decouples the two endpoints, so an
        # incoming weight update never blocks rollout production.
        self._weights_q: asyncio.Queue = asyncio.Queue()
        self._loop_task: asyncio.Task | None = None

    @endpoint
    async def start(self) -> None:
        """Launch the rollout loop as a background asyncio task. Returns
        immediately. Running the loop as a task (rather than as a
        long-lived endpoint invocation) lets ``ActorMesh.stop()`` and
        other endpoints always make progress."""
        self._loop_task = asyncio.create_task(self._rollout_loop())

    async def _rollout_loop(self) -> None:
        """Rollout coroutine.

        Equivalent of::

            while True:
                if weights_pending:
                    model.load(latest_weights)
                yield rollout
        """
        while True:
            # Drain any pending weight updates; keep only the latest.
            latest_sd = None
            while not self._weights_q.empty():
                latest_sd = self._weights_q.get_nowait()
            if latest_sd is not None:
                self.model.load_state_dict(latest_sd)

            state = torch.randn(STATE_DIM)
            dist = Categorical(logits=self.model(state))
            actions = dist.sample((G,))
            logps = dist.log_prob(actions)

            # "yield rollout" — fire-and-forget into the shared port.
            self.rollouts_out.send(Rollout(state, actions, logps))

            # Yield to the event loop so ``update_weights`` and ``drain``
            # messages land between rollouts, and so the task can be
            # cancelled here cleanly.
            await asyncio.sleep(0)

    @endpoint
    async def update_weights(self, new_sd: dict) -> None:
        """Enqueue a fresh ``state_dict``. Invoked by the trainer via
        ``send(..., selection="choose")``, so only one generator in the
        pool receives each update."""
        await self._weights_q.put(new_sd)

    @endpoint
    async def drain(self) -> None:
        """Stop producing rollouts and send a shutdown sentinel on the
        shared rollouts port so ``Scorer`` and ``RefModel`` can exit
        their loops cleanly. Called by the trainer after the final
        training step, before ``ActorMesh.stop()``."""
        await self._cancel_loop()
        self.rollouts_out.send(None)

    async def _cancel_loop(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

    async def __cleanup__(self, exc: Exception | None) -> None:
        """Safety-net cleanup of the one resource this actor owns — its
        rollout task. Monarch invokes this automatically on
        ``ActorMesh.stop()``. Messaging (the shutdown sentinel) is
        handled by the regular ``drain`` endpoint, not here."""
        await self._cancel_loop()


# %%
# Scorer actor
# ------------
# Stateless reward model. Receives rollouts from the shared port
# (populated by every generator in the pool), computes a per-action
# reward, forwards a ``ScoredRollout``.


class Scorer(Actor):
    def __init__(self):
        self.rollouts_port, self.rollouts_recv = Channel.open()
        self.reward_net = build_reward()

    @endpoint
    async def rollouts_port_(self) -> Port:
        return self.rollouts_port

    @endpoint
    async def run(self, scored_out: Port) -> None:
        """Scorer coroutine.

        Equivalent of::

            while True:
                rollout = yield
                if rollout is None:
                    yield None
                    return
                yield score(rollout)
        """
        while True:
            r = await self.rollouts_recv.recv()
            if r is None:
                scored_out.send(None)
                return

            sa = torch.cat(
                [
                    r.state.unsqueeze(0).repeat(G, 1),
                    r.actions.float().unsqueeze(-1),
                ],
                dim=-1,
            )
            rewards = self.reward_net(sa).squeeze(-1)

            scored_out.send(ScoredRollout(r.state, r.actions, r.logps, rewards))


# %%
# Reference-model actor
# ---------------------
# Frozen copy of the initial policy. Adds reference log-probs of the
# sampled actions so the trainer can compute a KL-to-reference penalty.


class RefModel(Actor):
    def __init__(self, init_state_dict: dict):
        self.model = build_policy()
        self.model.load_state_dict(init_state_dict)
        for p in self.model.parameters():
            p.requires_grad = False
        self.model.eval()
        self.scored_port, self.scored_recv = Channel.open()

    @endpoint
    async def scored_port_(self) -> Port:
        return self.scored_port

    @endpoint
    async def run(self, ref_scored_out: Port) -> None:
        """Reference-model coroutine.

        Equivalent of::

            while True:
                scored = yield
                if scored is None:
                    yield None
                    return
                yield with_ref_logps(scored)
        """
        while True:
            s = await self.scored_recv.recv()
            if s is None:
                ref_scored_out.send(None)
                return

            with torch.no_grad():
                ref_logps = Categorical(logits=self.model(s.state)).log_prob(s.actions)

            ref_scored_out.send(
                RefScoredRollout(s.state, s.actions, s.logps, s.rewards, ref_logps)
            )


# %%
# Trainer actor
# -------------
# Runs GRPO: PPO-clip on group-normalised advantages plus a KL penalty
# against the reference model. On each step it dispatches a fresh
# ``state_dict`` to one of the generators via
# ``send(..., selection="choose")`` — fire-and-forget. After
# ``MAX_STEPS`` updates it broadcasts ``stop`` to the generator pool and
# fires the one-shot done port.


class Trainer(Actor):
    def __init__(self, max_steps: int, done_port: Port):
        self.model = build_policy()
        self.optim = optim.Adam(self.model.parameters(), lr=0.05)
        self.ref_scored_port, self.ref_scored_recv = Channel.open()
        self.done_port = done_port
        self.max_steps = max_steps

    @endpoint
    async def ref_scored_port_(self) -> Port:
        return self.ref_scored_port

    @endpoint
    async def initial_state_dict(self) -> dict:
        """Snapshot of the policy weights at step 0, used to seed the
        reference model."""
        return copy.deepcopy(self.model.state_dict())

    @endpoint
    async def run(self, gen_mesh) -> None:
        """Trainer coroutine.

        Equivalent of::

            for step in range(max_steps):
                batch = yield
                weights = grpo_step(batch)
                fire_and_forget(gen_mesh.update_weights, weights, choose=1)
            gen_mesh.drain.call()        # stop loops, propagate sentinel
            done_port.send(max_steps)
        """
        for step in range(self.max_steps):
            # "batch = (yield)"
            b: RefScoredRollout = await self.ref_scored_recv.recv()

            # GRPO group-relative advantage.
            adv = (b.rewards - b.rewards.mean()) / (b.rewards.std() + 1e-8)

            # PPO-clip objective on the sampled actions.
            new_logps = Categorical(logits=self.model(b.state)).log_prob(b.actions)
            ratio = (new_logps - b.logps).exp()
            unclipped = ratio * adv
            clipped = torch.clamp(ratio, 1 - PPO_CLIP, 1 + PPO_CLIP) * adv
            ppo_loss = -torch.min(unclipped, clipped).mean()

            # Schulman's k1 KL estimator against the reference policy.
            kl = (new_logps - b.ref_logps).mean()

            loss = ppo_loss + KL_COEFF * kl

            self.optim.zero_grad()
            loss.backward()
            self.optim.step()

            print(
                f"[trainer] step {step:02d}  "
                f"reward={b.rewards.mean().item():+.3f}  "
                f"ppo_loss={ppo_loss.item():+.3f}  "
                f"kl={kl.item():+.3f}"
            )

            # Fire-and-forget weight dispatch. ``selection="choose"``
            # load-balances across the generator pool; ``port=None``
            # means no response is collected.
            new_sd = copy.deepcopy(self.model.state_dict())
            send(
                gen_mesh.update_weights,
                args=(new_sd,),
                kwargs={},
                selection="choose",
            )

        # ``drain`` stops the rollout loop on every generator and fires
        # a ``None`` down ``rollouts_out``; the scorer and ref forward
        # the first ``None`` they see and return. The controller calls
        # ``gen_mesh.stop()`` after everything has wound down, which
        # triggers ``__cleanup__`` on each generator as a safety net.
        await gen_mesh.drain.call()
        self.done_port.send(self.max_steps)


# %%
# Controller
# ----------
# All the controller does is:
#
# 1. Open the one-shot done channel.
# 2. Spawn each actor; the generator mesh holds ``NUM_GENERATORS``
#    replicas, everything else is a single actor.
# 3. Snapshot the trainer's initial weights and seed the reference
#    model with them.
# 4. Fetch each actor's input port and pass it to the previous hop's
#    ``run`` endpoint.
# 5. Fire every ``run`` endpoint without awaiting.
# 6. ``await done_recv.recv()``. That single await is the whole main
#    loop.


async def main() -> None:
    gen_mesh = this_host().spawn_procs(per_host={"gpus": NUM_GENERATORS})
    scorer_mesh = this_host().spawn_procs(per_host={"gpus": 1})
    ref_mesh = this_host().spawn_procs(per_host={"gpus": 1})
    trn_mesh = this_host().spawn_procs(per_host={"gpus": 1})

    done_port, done_recv = Channel.open(once=True)

    # Spawn the trainer first so we can seed the reference model with
    # its step-0 snapshot.
    trn = trn_mesh.spawn("trn", Trainer, MAX_STEPS, done_port)
    init_sd = await trn.initial_state_dict.call_one()

    # Spawn the scorer next — we need its rollouts port before
    # generators can be constructed (they all write into it).
    scorer = scorer_mesh.spawn("scorer", Scorer)
    rollouts_in = await scorer.rollouts_port_.call_one()

    # Spawn the generator pool. All four actors receive the *same* port
    # object in their constructor; on unpickle, each port re-binds to
    # the receiving actor's mailbox so the sends all land at the scorer.
    gen = gen_mesh.spawn("gen", Generator, rollouts_in)

    ref = ref_mesh.spawn("ref", RefModel, init_sd)

    scored_in = await ref.scored_port_.call_one()
    ref_scored_in = await trn.ref_scored_port_.call_one()

    # Kick off every coroutine. ``gen.start.call()`` launches the
    # rollout loop as a background task on each of the four generators
    # and returns immediately. The rest are single-actor meshes whose
    # ``run`` endpoints loop for the lifetime of training.
    await gen.start.call()
    scorer_fut = scorer.run.call_one(scored_in)
    ref_fut = ref.run.call_one(ref_scored_in)
    trn_fut = trn.run.call_one(gen)

    final_step = await done_recv.recv()
    print(f"[controller] training complete at step {final_step}")

    await scorer_fut
    await ref_fut
    await trn_fut

    # Everyone has finished their work. Stop the generator pool; each
    # generator's ``__cleanup__`` runs as a safety net.
    await gen_mesh.stop()


# %%
if __name__ == "__main__":
    asyncio.run(main())
