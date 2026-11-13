# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

r"""
SGLang Rollout Engines
=================================

RL post-training spends much of its time generating rollouts. This example runs
a fleet of SGLang engine replicas across Kubernetes pods. Each replica is a
Monarch actor that hosts ``sglang.Engine`` in-process. Rollout workers send
requests to a router actor, which spreads them over the fleet and updates the
engines' weights.

.. code-block:: text

    controller pod                         engine pods (KubernetesJob)
    +---------------------+                +-------------------------------+
    | RolloutWorker x 2   |                | pod 0: SGLangEngine (GPU 0)   |
    |   |                 |   Monarch      |        SGLangEngine (GPU 1)   |
    |   v                 | -------------> +-------------------------------+
    | Router actor        |                | pod 1: SGLangEngine (GPU 0)   |
    |  sessions, versions |                |        SGLangEngine (GPU 1)   |
    +---------------------+                +-------------------------------+

The example covers what an RL run needs from its inference engines:

Token-in, token-out with logprobs
    Requests carry prompt token IDs, and responses return the sampled token IDs
    with their logprobs, so a multi-turn rollout extends its context without
    retokenizing it.

Sticky routing
    Each turn of a rollout resends the whole conversation. The router pins
    each session to one replica, whose radix cache already holds the
    conversation, so only the new tokens need prefill.

Versioned weight updates
    The router stages new weights on every replica while the fleet keeps
    serving, then applies them one replica at a time, after that replica's
    in-flight requests finish. Every completion reports the weight version
    that produced it.

How the Actors Interact
-----------------------

.. code-block:: text

    demo (controller process)
      |                             |
      | rollout                     | start, reset, update_weights
      v                             v
    +---------------+  generate,   +-------------------------------+
    | RolloutWorker | -----------> | Router                        |
    | x 2           | end_session  |  policy, replica load         |
    +---------------+              +-------------------------------+
                                                   |
                                                   | spawn, generate,
                                                   | stage_weights,
                                                   | apply_weights
                                                   v
                                   +-------------------------------+
                                   | SGLangEngine x 4              |
                                   |  one process per GPU group    |
                                   +-------------------------------+

Each arrow is an endpoint call, and its reply travels back the same way. The
demo hands each rollout worker the router. The workers send every turn of
their sessions to the router's ``generate`` and finish each session with
``end_session``; the router picks a replica, calls that replica's
``generate``, and returns the completion. The demo drives the rest through the
router: ``start`` spawns the engines, ``reset`` switches the routing policy,
and ``update_weights`` stages and applies new weights.

Why Run SGLang Inside Monarch?
------------------------------

The usual deployment is ``sglang.launch_server`` behind a router, with clients
speaking HTTP, and it is the right choice for serving users. In an RL run, the
engines are part of the training program, and as Monarch actors they are under
its control: one program knows which weights each replica holds, a crashed
replica is reported to the actor that spawned it rather than showing up as
timeouts, weights
move as tensors in actor messages rather than through shared storage, and the
same script runs on one machine (``--local``) or across many pods.

The cost is coupling. SGLang and Monarch share one Python environment, so
SGLang's pinned PyTorch version applies to both, and the Python APIs used here
(``begin_weight_update``, ``update_weights_from_tensor``) change between
releases more often than the HTTP API. If the engines must outlive the
training job or serve other clients, run SGLang as a server.

Running SGLang Inside an Actor
------------------------------

``sglang.Engine`` runs its model in child processes and keeps an asyncio front
end in the process that creates it:

- Its synchronous methods run the engine's own event loop, which fails on the
  actor's event loop, and creating it on the main thread installs a
  ``SIGTERM`` handler that would take over Monarch's. So the ``SGLangEngine``
  actor creates the engine on a dedicated thread and makes every engine call
  there; endpoints await those calls and keep handling other messages.
- Each replica runs in its own process, and the router sets that process's
  ``CUDA_VISIBLE_DEVICES`` when it launches it. SGLang uses every visible GPU
  for tensor parallelism.

Prerequisites
-------------

- A Kubernetes cluster with the `MonarchMesh CRD and operator
  <https://github.com/meta-pytorch/monarch-kubernetes/>`_ installed, and GPU
  nodes that expose ``nvidia.com/gpu`` and can reach the Hugging Face Hub. The
  default configuration uses two pods with two GPUs each.
- One Python environment with both SGLang and Monarch, at the same path on the
  controller and on every engine pod. Install SGLang first, then
  ``torchmonarch``, which does not depend on PyTorch and so keeps SGLang's
  pinned version. For demonstration, the engine pods install it when they
  start (``WORKER_SETUP`` in the full script), and
  ``sglang_actors/controller.yaml`` creates a controller pod with the same
  environment.

Run the Example
---------------

This page leaves out the imports, the engine's shutdown details, the prompt
and weight helpers, and the pod template; the `full script
<https://github.com/meta-pytorch/monarch/blob/main/docs/source/examples/sglang_actors.py>`_
has everything. Create the controller pod, copy the script into it, and run it
with that environment's Python::

    kubectl create namespace monarch-tests
    kubectl apply -f sglang_actors/controller.yaml
    kubectl wait --for=condition=Ready pod/sglang-controller \
        -n monarch-tests --timeout=1200s
    kubectl cp sglang_actors.py monarch-tests/sglang-controller:/tmp/
    kubectl exec -it -n monarch-tests sglang-controller -- \
        /opt/sglang/bin/python -u /tmp/sglang_actors.py

The script deletes its engine pods when it finishes, and ``kubectl delete -f
sglang_actors/controller.yaml`` removes the controller. Pass ``--local`` to run
on one machine's GPUs instead.

On two pods with two GPUs each, the run prints:

.. code-block:: text

    replica 0: host engines-0, GPUs 0
    replica 1: host engines-0, GPUs 1
    replica 2: host engines-1, GPUs 0
    replica 3: host engines-1, GPUs 1
    round-robin: cache hit 48.1%  21.6s  requests per replica [16, 16, 16, 16]
         sticky: cache hit 92.2%  2.1s  requests per replica [16, 16, 16, 16]
    weights v1 on 4 replicas in 1.7s, logprobs changed on all; sessions spanning v0..v1: 8

Round-robin sends each turn of a session to a different replica, so much of
the conversation needs prefill again. Sticky routing reuses the cache for
nearly every turn after the first, and the workload finishes about ten times
faster. The gap grows with the fleet: with ``--num-hosts 4 --sessions 32`` (8
replicas), round-robin drops to an 18.6% cache hit while sticky stays at
92.6%.
"""

# sphinx_gallery_start_ignore
import argparse
import asyncio
import functools
import itertools
import os
import socket
import textwrap
import time
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version as package_version
from typing import Any, TYPE_CHECKING, TypedDict, TypeVar

import torch
from huggingface_hub import snapshot_download
from kubernetes.client import (
    V1Container,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Probe,
    V1ResourceRequirements,
    V1TCPSocketAction,
    V1Volume,
    V1VolumeMount,
)
from monarch._src.job.kubernetes import _WORKER_BOOTSTRAP_SCRIPT

# sphinx_gallery_end_ignore
from monarch.actor import (
    Actor,
    concurrent_endpoint,
    current_rank,
    default_bootstrap_cmd,
    endpoint,
    HostMesh,
    this_host,
)
from monarch.job.kubernetes import KubernetesJob

# sphinx_gallery_start_ignore
from safetensors.torch import load_file
from transformers import AutoTokenizer, PreTrainedTokenizerBase

if TYPE_CHECKING:
    import sglang

R = TypeVar("R")
# sphinx_gallery_end_ignore


# %%
# SGLang Engine Actors
# --------------------
#
# Each ``SGLangEngine`` actor owns one ``sglang.Engine``. Every call to the
# engine runs on the actor's dedicated thread, so engine calls never overlap.
# ``generate`` queues requests, and a short batching window turns concurrent
# requests into one batched engine call, which SGLang schedules together.
#
# .. code-block:: text
#
#     SGLangEngine actor process (CUDA_VISIBLE_DEVICES set at launch)
#     +-----------------------------------------------------------------+
#     | actor event loop                        engine thread           |
#     |   generate() ---+                       +--------------------+  |
#     |   generate() ---+--> queue --5 ms-->    | sglang.Engine      |  |
#     |   generate() ---+    (one batch)        |   generate(batch)  |  |
#     |   stage_weights --> host memory         |   load weights,    |  |
#     |   apply_weights -------------------->   |   between batches  |  |
#     |                                         +---------+----------+  |
#     +---------------------------------------------------|-------------+
#                                                         | ZMQ
#                                          SGLang scheduler and detokenizer
#                                          processes (on the GPU)
#
# A weight update takes two calls. ``stage_weights`` receives the tensors and
# holds them in host memory while the replica keeps serving, so the network
# transfer does not pause generation. ``apply_weights`` loads them into SGLang
# on the engine thread, between batches, so every batch comes from one weight
# version. SGLang's tensor update API does not take a version, so the actor
# stamps outputs with the version of its latest update.


# sphinx_gallery_start_ignore
async def cancel_other_tasks() -> None:
    """Cancel every other task on the running event loop and wait for them."""
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# sphinx_gallery_end_ignore
BATCH_WINDOW = 0.005  # seconds to collect concurrent requests into one batch

# SGLang sampling parameters, such as ``temperature`` and ``max_new_tokens``.
SamplingParams = dict[str, float]
# One output of ``sglang.Engine.generate``, whose ``meta_info`` holds logprobs
# and cache statistics. SGLang does not type it.
EngineOutput = dict[str, Any]
# The per-request arguments of one engine call.
Request = tuple[list[int], SamplingParams]


class Completion(TypedDict):
    """One sampled completion, with the weight version that produced it."""

    replica: int
    version: int
    output_ids: list[int]
    logprobs: list[float]
    text: str
    prompt_tokens: int
    cached_tokens: int


class Placement(TypedDict):
    """Where a replica runs."""

    replica: int
    host: str
    gpus: str


class SGLangEngine(Actor):
    """One SGLang engine replica, running in-process on a dedicated thread."""

    def __init__(self, replica: int, model_path: str) -> None:
        self.replica = replica
        self.model_path = model_path
        self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sglang")
        self.engine: sglang.Engine  # created by start()
        self.version = 0
        self.staged: tuple[list[tuple[str, torch.Tensor]], int] | None = None
        # Requests waiting for the next batch, each paired with the future its
        # caller awaits.
        self.queue: list[tuple[Request, asyncio.Future[tuple[EngineOutput, int]]]] = []

    async def _on_thread(self, fn: Callable[[], R]) -> R:
        return await asyncio.get_running_loop().run_in_executor(self.thread, fn)

    @concurrent_endpoint
    async def start(self) -> Placement:
        """Start SGLang on this process's GPUs and report the placement."""
        # Importing SGLang changes process-wide settings (event loop policy,
        # multiprocessing start method), so only engine processes import it.
        import sglang

        self.engine = await self._on_thread(
            lambda: sglang.Engine(
                model_path=self.model_path,
                tp_size=len(os.environ["CUDA_VISIBLE_DEVICES"].split(",")),
                base_gpu_id=0,
                log_level="error",
            )
        )
        return {
            "replica": self.replica,
            "host": socket.gethostname(),
            "gpus": os.environ["CUDA_VISIBLE_DEVICES"],
        }

    @concurrent_endpoint
    async def generate(
        self, input_ids: list[int], sampling_params: SamplingParams
    ) -> Completion:
        """Sample one completion of ``input_ids``."""
        output, version = await self._submit((input_ids, sampling_params))
        meta = output["meta_info"]
        return {
            "replica": self.replica,
            "version": version,
            "output_ids": [t for _, t, _ in meta["output_token_logprobs"]],
            "logprobs": [lp for lp, _, _ in meta["output_token_logprobs"]],
            "text": output["text"],
            "prompt_tokens": meta["prompt_tokens"],
            "cached_tokens": meta["cached_tokens"],
        }

    async def _submit(self, request: Request) -> tuple[EngineOutput, int]:
        """Queue ``request`` for the next batch and wait for its output.

        The first request into an empty queue schedules a flush; requests that
        arrive before it runs join the same batch. The output comes back with
        the weight version that produced it.
        """
        future: asyncio.Future[tuple[EngineOutput, int]] = (
            asyncio.get_running_loop().create_future()
        )
        if not self.queue:
            asyncio.create_task(self._flush())
        self.queue.append((request, future))
        return await future

    async def _flush(self) -> None:
        """Run everything queued during the batching window as one engine call.

        Taking the queue empties it, so requests that arrive while this batch
        runs schedule the next flush. That batch waits on the engine thread
        until this one finishes, and weight updates run on the same thread, so
        weights change only between batches. If the engine call fails, every
        request in the batch fails with its error.
        """
        await asyncio.sleep(BATCH_WINDOW)
        batch, self.queue = self.queue, []
        try:
            outputs = await self._on_thread(
                lambda: self.engine.generate(
                    input_ids=[ids for (ids, _), _ in batch],
                    sampling_params=[params for (_, params), _ in batch],
                    return_logprob=True,
                )
            )
        except Exception as e:
            for _, future in batch:
                if not future.done():
                    future.set_exception(e)
            return
        # A caller cancelled while its request ran has already finished its
        # future.
        for (_, future), output in zip(batch, outputs):
            if not future.done():
                future.set_result((output, self.version))

    def _load(self, named_tensors: list[tuple[str, torch.Tensor]]) -> None:
        for step in (
            self.engine.begin_weight_update,
            functools.partial(self.engine.update_weights_from_tensor, named_tensors),
            self.engine.end_weight_update,
        ):
            ok, message = step()
            if not ok:
                raise RuntimeError(f"replica {self.replica}: {message}")

    @concurrent_endpoint
    async def stage_weights(
        self, named_tensors: list[tuple[str, torch.Tensor]], version: int
    ) -> None:
        """Hold new weights in host memory without touching the engine."""
        self.staged = (named_tensors, version)

    @concurrent_endpoint
    async def apply_weights(self) -> None:
        """Load the staged weights and tag later outputs with their version."""
        assert self.staged is not None, "stage_weights must come first"
        named_tensors, version = self.staged
        await self._on_thread(lambda: self._load(named_tensors))
        self.version = version
        self.staged = None

    @concurrent_endpoint
    async def flush_cache(self) -> None:
        """Drop the radix cache so that measurements start cold."""
        await self._on_thread(self.engine.flush_cache)

    # sphinx_gallery_start_ignore
    def _shutdown(self) -> None:
        self.engine.shutdown()
        # The engine's own event loop runs only inside its synchronous calls,
        # so its background tasks are still pending; cancel them.
        self.engine.loop.run_until_complete(cancel_other_tasks())

    # sphinx_gallery_end_ignore
    async def __cleanup__(self, exc: Exception | None) -> None:
        """Shut SGLang down when the actor stops."""
        if hasattr(self, "engine"):
            await self._on_thread(self._shutdown)


# %%
# Routing Policies
# ----------------
#
# The router keeps one ``Replica`` record per engine with its in-flight load,
# and asks a routing policy to choose among the replicas that are serving. A
# policy sees only those records, so it is easy to swap: the demo compares
# ``RoundRobin`` with ``Sticky``.
#
# ``LeastLoaded`` breaks ties by the replica it chose least recently. When many
# sessions start at once, every replica is idle; always taking the first one
# would pin all of those sessions to it. ``Sticky`` pins each new session to
# the replica ``LeastLoaded`` picks, keeps it there, and forgets the session
# when its rollout ends.
#
# .. code-block:: text
#
#                   turn 1   turn 2   turn 3   turn 4
#     round-robin:  r0       r1       r2       r3      every turn prefills again
#     sticky:       r2       r2       r2       r2      turns 2-4 reuse the cache


class Replica:
    """The router's record of one engine replica."""

    def __init__(self, index: int) -> None:
        self.index = index
        self.engine: SGLangEngine  # set when the replica is spawned
        self.load = 0
        self.serving = asyncio.Event()
        self.serving.set()
        self.idle = asyncio.Event()
        self.idle.set()


class RoutingPolicy:
    """Choose a replica for each request."""

    def choose(self, session: str, candidates: list[Replica]) -> Replica:
        raise NotImplementedError

    def pinned(self, session: str) -> Replica | None:
        """The replica that holds this session's cache, if any."""
        return None

    def release(self, session: str) -> None:
        """Forget a session that sends no more requests."""


class RoundRobin(RoutingPolicy):
    def __init__(self) -> None:
        self.turn = itertools.count()

    def choose(self, session: str, candidates: list[Replica]) -> Replica:
        return candidates[next(self.turn) % len(candidates)]


class LeastLoaded(RoutingPolicy):
    def __init__(self) -> None:
        self.picks = itertools.count()
        self.last_pick: dict[int, int] = {}

    def choose(self, session: str, candidates: list[Replica]) -> Replica:
        lowest = min(r.load for r in candidates)
        replica = min(
            (r for r in candidates if r.load == lowest),
            key=lambda r: self.last_pick.get(r.index, -1),
        )
        self.last_pick[replica.index] = next(self.picks)
        return replica


class Sticky(RoutingPolicy):
    def __init__(self) -> None:
        self.fallback = LeastLoaded()
        self.sessions: dict[str, Replica] = {}

    def choose(self, session: str, candidates: list[Replica]) -> Replica:
        replica = self.sessions.get(session)
        if replica not in candidates:
            replica = self.fallback.choose(session, candidates)
            self.sessions[session] = replica
        return replica

    def pinned(self, session: str) -> Replica | None:
        return self.sessions.get(session)

    def release(self, session: str) -> None:
        self.sessions.pop(session, None)


# %%
# Router Actor
# ------------
#
# The ``Router`` spawns one process per replica on the host mesh and sets that
# process's GPUs through its launch environment. For each request, it asks the
# policy for a replica and counts the request in that replica's load until it
# returns.
#
# A weight update first stages the weights on every replica while they all keep
# serving. Then it updates one replica at a time: it stops routing to the
# replica, waits for the replica's in-flight requests, applies the weights,
# and routes to it again. A session pinned to that replica waits for it rather
# than moving, so it keeps its cache. The rest of the fleet keeps serving, so
# for a moment replicas run different versions, which the version on each
# completion records.
#
# .. code-block:: text
#
#                stage   update r0    update r1    update r2    update r3
#     replica 0  v0 .... drain load  v1 .................................
#     replica 1  v0 ................ drain load  v1 .....................
#     replica 2  v0 ............................ drain load  v1 .........
#     replica 3  v0 ........................................ drain load  v1


class Router(Actor):
    """Route rollout requests over a fleet of engine replicas.

    ``gpu_groups`` lists each replica's ``CUDA_VISIBLE_DEVICES`` on a host, so
    every host runs ``len(gpu_groups)`` replicas.
    """

    def __init__(
        self,
        hosts: HostMesh,
        gpu_groups: list[str],
        model_path: str,
        policy: RoutingPolicy,
    ) -> None:
        self.hosts = hosts.flatten("host")
        self.gpu_groups = gpu_groups
        self.model_path = model_path
        self.policy = policy
        count = len(self.hosts) * len(gpu_groups)
        self.replicas = [Replica(i) for i in range(count)]

    async def _spawn(self, replica: Replica) -> Placement:
        host, slot = divmod(replica.index, len(self.gpu_groups))
        name = f"replica{replica.index}"
        procs = self.hosts.slice(host=host).spawn_procs(
            name=name,
            bootstrap_command=default_bootstrap_cmd().with_env(
                {"CUDA_VISIBLE_DEVICES": self.gpu_groups[slot]}
            ),
        )
        replica.engine = procs.spawn(name, SGLangEngine, replica.index, self.model_path)
        return await replica.engine.start.call_one()

    @concurrent_endpoint
    async def start(self) -> list[Placement]:
        """Start every replica and return where each one runs."""
        return list(await asyncio.gather(*map(self._spawn, self.replicas)))

    @concurrent_endpoint
    async def generate(
        self, session: str, input_ids: list[int], sampling_params: SamplingParams
    ) -> Completion:
        """Sample a completion on the replica the policy chooses."""
        pinned = self.policy.pinned(session)
        if pinned is not None:
            await pinned.serving.wait()
        serving = [r for r in self.replicas if r.serving.is_set()]
        replica = self.policy.choose(session, serving or self.replicas)
        await replica.serving.wait()
        replica.load += 1
        replica.idle.clear()
        try:
            return await replica.engine.generate.call_one(input_ids, sampling_params)
        finally:
            replica.load -= 1
            if replica.load == 0:
                replica.idle.set()

    @concurrent_endpoint
    async def end_session(self, session: str) -> None:
        """Forget a session whose rollout has finished."""
        self.policy.release(session)

    @concurrent_endpoint
    async def generate_everywhere(
        self, input_ids: list[int], sampling_params: SamplingParams
    ) -> list[Completion]:
        """Run one request on every replica, for checking weights."""
        return [
            await r.engine.generate.call_one(input_ids, sampling_params)
            for r in self.replicas
        ]

    @concurrent_endpoint
    async def update_weights(
        self, named_tensors: list[tuple[str, torch.Tensor]], version: int
    ) -> None:
        """Stage weights everywhere, then apply them one replica at a time."""
        await asyncio.gather(
            *(
                r.engine.stage_weights.call_one(named_tensors, version)
                for r in self.replicas
            )
        )
        for replica in self.replicas:
            replica.serving.clear()
            try:
                await replica.idle.wait()
                await replica.engine.apply_weights.call_one()
            finally:
                replica.serving.set()

    @concurrent_endpoint
    async def reset(self, policy: RoutingPolicy) -> None:
        """Switch routing policy and empty every replica's cache."""
        self.policy = policy
        await asyncio.gather(*(r.engine.flush_cache.call_one() for r in self.replicas))


# %%
# Multi-Turn Rollouts
# -------------------
#
# Each session asks a question, then asks for more detail several times. The
# rollout appends the sampled token IDs and the next user turn to the context
# instead of re-rendering the conversation as text, so every turn's prompt
# starts with the previous turn's exact tokens:
#
# .. code-block:: text
#
#     turn 1 prompt: [question]
#     turn 2 prompt: [question][output 1][Go deeper.]
#     turn 3 prompt: [question][output 1][Go deeper.][output 2][Go deeper.]
#                    \________ already in the replica's cache ________/ \__new___/
#
# ``user_turn`` uses Qwen3's chat format; adapt it to your model's chat
# template.

# sphinx_gallery_start_ignore
QUESTIONS = [
    "Why is the sky blue?",
    "How do vaccines train the immune system?",
    "What causes ocean tides?",
    "How does a transistor switch current?",
    "Why do leaves change color in autumn?",
    "How do noise-cancelling headphones work?",
    "What makes a bridge stable?",
    "How does GPS determine position?",
]


def prompt_ids(tokenizer: PreTrainedTokenizerBase, question: str) -> list[int]:
    """Token IDs of a one-question chat prompt."""
    text = tokenizer.apply_chat_template(
        [{"role": "user", "content": question}],
        tokenize=False,
        add_generation_prompt=True,
        enable_thinking=False,
    )
    return tokenizer(text, add_special_tokens=False)["input_ids"]


# sphinx_gallery_end_ignore
def user_turn(tokenizer: PreTrainedTokenizerBase, text: str) -> list[int]:
    """Token IDs that close the assistant turn and open the next one."""
    template = (
        f"<|im_end|>\n<|im_start|>user\n{text}<|im_end|>\n"
        "<|im_start|>assistant\n<think>\n\n</think>\n\n"
    )
    return tokenizer(template, add_special_tokens=False)["input_ids"]


class RolloutWorker(Actor):
    """Run multi-turn rollouts through the router."""

    def __init__(self, model_path: str, turns: int, max_new_tokens: int) -> None:
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.turns = turns
        self.followup = user_turn(self.tokenizer, "Go deeper.")
        self.params: SamplingParams = {
            "temperature": 1.0,
            "max_new_tokens": max_new_tokens,
        }

    @endpoint
    async def rollout(
        self, router: Router, tag: str, sessions: int
    ) -> list[list[Completion]]:
        """Run this worker's share of ``sessions`` concurrent sessions."""
        point = current_rank()
        mine = range(point.rank, sessions, point.extent.nelements)
        return list(
            await asyncio.gather(
                *(
                    self._session(router, f"{tag}{i}", QUESTIONS[i % len(QUESTIONS)])
                    for i in mine
                )
            )
        )

    async def _session(
        self, router: Router, session: str, question: str
    ) -> list[Completion]:
        """Run one conversation and return its turns."""
        ids = prompt_ids(self.tokenizer, question)
        turns: list[Completion] = []
        try:
            for _ in range(self.turns):
                completion = await router.generate.call_one(session, ids, self.params)
                turns.append(completion)
                ids = ids + completion["output_ids"] + self.followup
        finally:
            await router.end_session.call_one(session)
        return turns


async def run_workload(
    workers: RolloutWorker, router: Router, args: argparse.Namespace, tag: str
) -> list[list[Completion]]:
    """Run ``args.sessions`` concurrent sessions spread over the rollout workers."""
    results = await workers.rollout.call(router, tag, args.sessions)
    return [session for sessions in results.values() for session in sessions]


# sphinx_gallery_start_ignore
def cache_hit_rate(results: Sequence[Completion]) -> float:
    """Fraction of prompt tokens that SGLang served from its radix cache."""
    prompt = sum(r["prompt_tokens"] for r in results)
    return sum(r["cached_tokens"] for r in results) / prompt


def perturbed_weights(model_path: str, scale: float) -> list[tuple[str, torch.Tensor]]:
    """Return a few scaled MLP weights, standing in for a trainer update.

    A trainer would send every parameter; four layers keep the demo's messages
    small while still changing the model's outputs.
    """
    directory = snapshot_download(model_path, allow_patterns=["*.safetensors"])
    named = []
    for file in sorted(os.listdir(directory)):
        if not file.endswith(".safetensors"):
            continue
        for name, tensor in load_file(os.path.join(directory, file)).items():
            if name.endswith("mlp.down_proj.weight") and ".layers." in name:
                layer = int(name.split(".layers.")[1].split(".")[0])
                if layer < 4:
                    named.append((name, tensor * scale))
    return named


# sphinx_gallery_end_ignore


# %%
# Demo
# ----
#
# The demo starts the fleet and checks each capability in turn. The rollout
# workers receive the router in each call and send it their requests.


# %%
# Round-Robin Versus Sticky Routing
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# The same workload, 16 sessions of 4 turns from the two rollout workers, runs
# once per policy, each time starting from empty caches. Round-robin moves
# every turn to another replica; sticky keeps a session on one replica, so
# later turns find the conversation in its cache.
#
# .. code-block:: text
#
#     pass 1: RoundRobin            pass 2: Sticky
#     (caches emptied first)        (caches emptied first)
#
#     session A, turn 1 -> r0       session A, turn 1 -> r2
#                turn 2 -> r1                  turn 2 -> r2  cache hit
#                turn 3 -> r2                  turn 3 -> r2  cache hit
#                turn 4 -> r3                  turn 4 -> r2  cache hit
#
#     each pass reports its cache hit rate, time, and requests per replica


async def compare_routing(
    router: Router, workers: RolloutWorker, args: argparse.Namespace, replicas: int
) -> None:
    """Run the same workload with round-robin and sticky routing."""
    rates = {}
    for name, policy in (("round-robin", RoundRobin()), ("sticky", Sticky())):
        await router.reset.call_one(policy)
        start = time.monotonic()
        sessions = await run_workload(workers, router, args, f"{name}-")
        turns = [turn for s in sessions for turn in s]
        rates[name] = cache_hit_rate(turns)
        served = Counter(turn["replica"] for turn in turns)
        print(
            f"{name:>11}: cache hit {rates[name]:.1%}  "
            f"{time.monotonic() - start:.1f}s  "
            f"requests per replica {[served[r] for r in range(replicas)]}"
        )
    assert rates["sticky"] > rates["round-robin"]


# %%
# Weight Update Under Load
# ~~~~~~~~~~~~~~~~~~~~~~~~
#
# A probe prompt records every replica's logprobs. While a workload runs, the
# router stages new weights on all replicas, then drains and loads them one
# replica at a time. Afterward every replica reports version 1 and the probe's
# logprobs have changed. A session that straddles the update has turns from
# both versions.
#
# .. code-block:: text
#
#     probe (v0) --> workload running ----------------------> probe (v1):
#                       |                                    all replicas on
#                       +-- update_weights(v1)               v1, logprobs
#                             stage on r0, r1, r2, r3        changed
#                             r0: drain, load
#                             r1: drain, load
#                             r2: drain, load
#                             r3: drain, load
#
#     session X: turn 1 (v0), turn 2 (v0), turn 3 (v1), turn 4 (v1)


async def update_under_load(
    router: Router,
    workers: RolloutWorker,
    tokenizer: PreTrainedTokenizerBase,
    args: argparse.Namespace,
) -> None:
    """Update weights while sessions run, then check versions and outputs."""
    probe = prompt_ids(tokenizer, "Name the largest planet.")
    greedy = {"temperature": 0.0, "max_new_tokens": 8}
    before = await router.generate_everywhere.call_one(probe, greedy)
    weights = perturbed_weights(args.model_path, 1.1)
    workload = asyncio.create_task(run_workload(workers, router, args, "v-"))
    await asyncio.sleep(0.5)
    start = time.monotonic()
    await router.update_weights.call_one(weights, 1)
    took = time.monotonic() - start
    sessions = await workload
    after = await router.generate_everywhere.call_one(probe, greedy)
    assert all(a["version"] == 1 for a in after)
    assert all(a["logprobs"] != b["logprobs"] for a, b in zip(after, before))
    mixed = sum(1 for s in sessions if len({t["version"] for t in s}) > 1)
    print(
        f"weights v1 on {len(after)} replicas in {took:.1f}s, logprobs changed "
        f"on all; sessions spanning v0..v1: {mixed}"
    )


async def demo(
    router: Router, workers: RolloutWorker, args: argparse.Namespace
) -> None:
    """Start the fleet and exercise each capability."""
    tokenizer = AutoTokenizer.from_pretrained(args.model_path)
    placements = await router.start.call_one()
    for p in placements:
        print(f"replica {p['replica']}: host {p['host']}, GPUs {p['gpus']}")
    await compare_routing(router, workers, args, len(placements))
    await update_under_load(router, workers, tokenizer, args)


# %%
# Provision the Fleet
# -------------------
#
# ``KubernetesJob`` creates a ``MonarchMesh`` resource, and the operator
# provisions the engine pods. The pod template requests GPUs and a
# memory-backed ``/dev/shm``, and marks the pod ready only once the Monarch
# worker listens on port 26600. The image is a plain Python image, not a CUDA
# one, so ``NVIDIA_DRIVER_CAPABILITIES`` asks the NVIDIA runtime to mount the
# CUDA driver library.
#
# Installing the environment at pod startup is for DEMONSTRATION ONLY; bake it
# into the worker image in production. The pods install the same SGLang and
# Monarch versions as the controller. FlashInfer compiles some kernels on
# first use for GPUs without prebuilt ones, which needs a CUDA compiler, a C++
# compiler, and ``ninja`` on ``PATH``, so the setup provides those as well.
# The pod template and the command-line flags are in the full script.

# sphinx_gallery_start_ignore
WORKER_SETUP = textwrap.dedent("""\
    set -e
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq && apt-get install -y -qq g++ > /dev/null
    uv venv -q /opt/sglang --python 3.12
    VIRTUAL_ENV=/opt/sglang uv pip install -q sglang=={sglang} torchmonarch=={monarch} \\
        "nvidia-cuda-nvcc==13.0.*" "nvidia-cuda-runtime==13.0.*" \\
        "nvidia-cuda-cccl==13.0.*" "nvidia-cuda-crt==13.0.*" "nvidia-nvvm==13.0.*"
    cuda=/opt/sglang/lib/python3.12/site-packages/nvidia/cu13
    ln -sfn lib "$cuda/lib64" && ln -sf libcudart.so.13 "$cuda/lib/libcudart.so"
    export CUDA_HOME="$cuda" PATH="/opt/sglang/bin:$cuda/bin:$PATH"
    exec /opt/sglang/bin/python -u -c "$MONARCH_BOOTSTRAP"
""")


def build_pod_template(gpus: int) -> V1PodTemplateSpec:
    """Return an engine pod template that installs the environment first."""
    gpu_resources = {"nvidia.com/gpu": str(gpus)}
    setup = WORKER_SETUP.format(
        sglang=package_version("sglang"), monarch=package_version("torchmonarch")
    )
    return V1PodTemplateSpec(
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="worker",
                    image="ghcr.io/astral-sh/uv:python3.12-trixie",
                    command=["/bin/bash", "-c", setup],
                    env=[
                        V1EnvVar(name="MONARCH_PORT", value="26600"),
                        V1EnvVar(
                            name="MONARCH_BOOTSTRAP", value=_WORKER_BOOTSTRAP_SCRIPT
                        ),
                        V1EnvVar(name="HF_HOME", value="/tmp/hf_cache"),
                        V1EnvVar(
                            name="NVIDIA_DRIVER_CAPABILITIES", value="compute,utility"
                        ),
                    ],
                    resources=V1ResourceRequirements(
                        limits=gpu_resources, requests=gpu_resources
                    ),
                    readiness_probe=V1Probe(
                        tcp_socket=V1TCPSocketAction(port=26600),
                        initial_delay_seconds=30,
                        period_seconds=5,
                        failure_threshold=240,
                    ),
                    volume_mounts=[V1VolumeMount(name="dshm", mount_path="/dev/shm")],
                )
            ],
            volumes=[
                V1Volume(
                    name="dshm",
                    empty_dir=V1EmptyDirVolumeSource(
                        medium="Memory", size_limit="16Gi"
                    ),
                )
            ],
        ),
    )


def parse_args() -> argparse.Namespace:
    """Parse fleet and workload options."""
    parser = argparse.ArgumentParser(description="A fleet of SGLang engines")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--num-hosts", type=int, default=2)
    parser.add_argument("--gpus-per-host", type=int, default=2)
    parser.add_argument("--tp-size", type=int, default=1)
    parser.add_argument("--model-path", default="Qwen/Qwen3-0.6B")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--sessions", type=int, default=16)
    parser.add_argument("--turns", type=int, default=4)
    parser.add_argument("--max-new-tokens", type=int, default=128)
    args = parser.parse_args()
    if args.gpus_per_host % args.tp_size:
        raise ValueError("GPUs per host must be a multiple of the TP size")
    return args


# sphinx_gallery_end_ignore
def main() -> None:
    """Provision the fleet, run the demo, and tear everything down."""
    args = parse_args()
    job = None
    if args.local:
        hosts = this_host()
    else:
        job = KubernetesJob(namespace="monarch-tests", timeout=1800)
        job.add_mesh(
            "engines",
            num_replicas=args.num_hosts,
            pod_template=build_pod_template(args.gpus_per_host),
        )
        hosts = job.state(cached_path=None).engines
    gpu_groups = [
        ",".join(map(str, range(first, first + args.tp_size)))
        for first in range(0, args.gpus_per_host, args.tp_size)
    ]
    router_procs = this_host().spawn_procs()
    worker_procs = this_host().spawn_procs(per_host={"workers": args.workers})
    try:
        router = router_procs.spawn(
            "router",
            Router,
            hosts,
            gpu_groups,
            args.model_path,
            Sticky(),
        )
        workers = worker_procs.spawn(
            "rollout",
            RolloutWorker,
            args.model_path,
            args.turns,
            args.max_new_tokens,
        )
        asyncio.run(demo(router, workers, args))
    finally:
        worker_procs.stop().get()
        router_procs.stop().get()
        if job is not None:
            job.kill()


if __name__ == "__main__":
    main()
