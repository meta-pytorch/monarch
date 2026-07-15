# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
GRPO on Kubernetes (Qwen3.5-0.8B-Base on GSM8K)
===============================================

This tutorial runs a GRPO-style RL fine-tuning loop on Kubernetes using
Monarch's ``KubernetesJob`` with two heterogeneous ``MonarchMesh`` CRDs:

- a ``learner`` mesh on one pod with 2 GPUs; the trainable policy is
  split across both via ``device_map="auto"`` so the model + AdamW state
  + activations fit comfortably;
- a multi-pod ``generator`` mesh that rolls out G=6 completions per prompt
  and syncs updated policy weights back from the learner over ``RDMABuffer``.

Prompts come from ``openai/gsm8k``. The model is asked via a system prompt
to emit its final integer inside ``<answer>...</answer>`` tags. The reward
is ``1.0`` for exact-string match of the extracted answer with the gold,
``0.0`` otherwise.

The algorithm is intentionally minimal -- token-level REINFORCE on
group-relative advantages, no KL anchor, no PPO clipping, no format
shaping. The point is to show how to wire a Monarch actor mesh for
distributed RL on Kubernetes (two heterogeneous meshes, replay-buffer
actor, scorer actor, RDMA weight sync, async rollout / train). For
KL-anchored production GRPO see the :doc:`grpo_actor` sibling example.

Prerequisites
-------------

- A Kubernetes cluster with GPU worker nodes; the learner pod requests
  ``nvidia.com/gpu: 2`` and each generator pod requests as many as
  ``--gpus_per_generator`` (default 4).
- ``nvidia.com/gpu`` device plugin installed.
- Monarch operator and ``MonarchMesh`` CRD installed
  (see https://github.com/meta-pytorch/monarch-kubernetes).
- ``kubectl`` configured for the cluster.
- Internet access from worker pods (for downloading Qwen3.5-0.8B-Base and pip
  installing ``transformers``). No HF token is required -- Qwen3.5 is Apache
  2.0 and ungated.
- The controller pod installs ``datasets`` at startup (see
  ``manifests/grpo_provision.yaml``); the worker pods do not need it
  because prompts are loaded in ``main()`` on the controller.

RDMA transport caveat
---------------------

This example calls ``monarch.configure(rdma_allow_tcp_fallback=True)`` so
cross-pod ``RDMABuffer`` reads fall back to TCP on clusters without an RDMA
CNI / device plugin. This is demonstrative, not performance-tuned. On a
cluster with ibverbs plumbing, drop the ``configure()`` call and add the
appropriate device resource requests to ``build_pod_template``.

Pod startup time
----------------

Cold worker pods need a few minutes before they are ready:
- ``pip install transformers tokenizers accelerate``
- Downloading the model weights from Hugging Face

``KubernetesJob(timeout=600)`` gives 10 minutes for the meshes to come up.

Running the example
-------------------

::

    kubectl apply -f manifests/grpo_provision.yaml
    kubectl wait --for=condition=Ready pod/grpo-controller -n monarch-tests
    kubectl cp kubernetes_grpo.py monarch-tests/grpo-controller:/tmp/kubernetes_grpo.py
    kubectl exec -it grpo-controller -n monarch-tests -- python /tmp/kubernetes_grpo.py
    kubectl delete -f manifests/grpo_provision.yaml

Each step prints one ``[Step NNN] loss=... reward=... gn=...`` line.
``reward`` is the mean correctness of the sampled replay-buffer batch;
``gn`` is the pre-clip grad norm. Steps with ``reward=0.000`` or
``reward=1.000`` show ``gn=0.000``: every completion in the group got
the same score, so the group-relative advantage is zero and the gradient
vanishes. Mixing in non-degenerate groups is what drives learning. The
headline "did training work?" signal is the held-out GSM8K test
accuracy: a baseline pass before step 0 and a final pass after the
training loop, both sharded greedy-decode across the generator mesh.

A representative 100-step run on the reference config
(1 learner pod x 2 GPUs, 2 generator pods x 4 GPUs, and 512 eval prompts)
ends up looking like this:

::

    ============================================================
    Training summary
    ------------------------------------------------------------
    Baseline acc: 0.375 (192/512)
    Final acc:    0.562 (288/512)
    Delta:        +0.188
    ============================================================

Larger Qwen3.5-Base policies show the same end-to-end story with bigger
deltas. Bumping ``MODEL_NAME`` to ``Qwen/Qwen3.5-2B-Base`` (no other
changes) on the same config produces:

::

    ============================================================
    Training summary
    ------------------------------------------------------------
    Baseline acc: 0.494 (253/512)
    Final acc:    0.742 (380/512)
    Delta:        +0.248
    ============================================================

The hyperparameters are reasonable defaults but this script is not tuned
for SOTA GSM8K accuracy -- the point is to show how to wire a Monarch
actor mesh for distributed RL on Kubernetes (two heterogeneous meshes,
replay-buffer actor, scorer actor, RDMA weight sync, async rollout /
train).
"""

# %%
# Imports
# -------

import argparse
import asyncio
import math
import random
import re
import textwrap
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import monarch
import torch
import torch.nn as nn
import torch.optim as optim
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
from monarch.actor import Actor, current_rank, current_size, endpoint
from monarch.job.kubernetes import KubernetesJob
from monarch.rdma import RDMABuffer

# %%
# Cross-pod RDMABuffer reads use TCP fallback by default on clusters without
# an RDMA CNI. Also extend supervision / spawn-idle timeouts so large model
# loads inside actor __init__ (which block the event loop for tens of
# seconds) don't get killed by the default 60s liveness watchdog.
monarch.configure(
    rdma_allow_tcp_fallback=True,
    actor_spawn_max_idle="10m",
    get_proc_state_max_idle="10m",
    supervision_watchdog_timeout="10m",
    enable_log_forwarding=True,
)

# %%
# Constants
# ---------

MODEL_NAME = "Qwen/Qwen3.5-0.8B-Base"
G = 6  # GRPO group size (completions sampled per prompt)
MAX_NEW_TOKENS = 256
MONARCH_NAMESPACE = "monarch-tests"
MONARCH_IMAGE = "ghcr.io/meta-pytorch/monarch:latest"


# %%
# Task-specific: GSM8K reward, prompts, and dataset
# -------------------------------------------------
#
# Everything below in this section is GSM8K-specific. To adapt this example
# to another task, replace this block: define a new prompt template, parsing
# helpers, dataset loader, and a class with a ``score`` method matching the
# ``GSM8KScorePolicy.score`` signature. Nothing in the actor topology, RL
# loss, or RDMA weight sync below knows about GSM8K.

SYSTEM_PROMPT = (
    "Solve the problem and put your final integer answer inside "
    "<answer>...</answer> tags."
)

# Matches the GSM8K dataset's native '#### <int>' ground-truth marker, used
# only to pull the gold answer out of ``ex["answer"]`` at dataset load time.
GSM8K_GT_PATTERN = re.compile(r"####\s*(-?\d+)")


def load_gsm8k_prompts(split: str, num_prompts: int) -> List[Tuple[str, str]]:
    """Load the first ``num_prompts`` problems from GSM8K ``split``.

    Returns a list of (prompt_text, ground_truth_string) tuples. Pass
    ``num_prompts=0`` to use the entire split.

    .. warning::
        Downloading the dataset from the Hugging Face Hub at job launch is
        for DEMONSTRATION ONLY. In production, stage the dataset onto a
        persistent volume (e.g. a PVC or an object-store-backed mount) and
        point ``datasets.load_dataset`` at the local path. This avoids
        repeated downloads, removes a dependency on outbound network
        access, and gives reproducible, auditable inputs across runs.
    """
    from datasets import load_dataset

    ds = load_dataset("openai/gsm8k", "main", split=split)
    if num_prompts > 0:
        ds = ds.select(range(min(num_prompts, len(ds))))
    out: List[Tuple[str, str]] = []
    for ex in ds:
        m = GSM8K_GT_PATTERN.search(ex["answer"])
        if m is None:
            continue
        out.append((ex["question"], m.group(1)))
    return out


def extract_xml_answer(text: str) -> Optional[str]:
    """Return the content of the last ``<answer>...</answer>`` block, stripped.

    Returns ``None`` if no well-formed block exists or the content is empty.
    """
    parts = text.split("<answer>")
    if len(parts) < 2:
        return None
    last = parts[-1]
    if "</answer>" not in last:
        return None
    answer = last.split("</answer>")[0].strip()
    return answer or None


def is_correct(generated_text: str, ground_truth: str) -> bool:
    """True when the answer extracted from ``generated_text`` matches the gold."""
    return extract_xml_answer(generated_text) == ground_truth


def format_prompt(tokenizer: Any, prompt_text: str) -> str:
    """Wrap ``prompt_text`` in the GSM8K system prompt + chat template."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt_text},
    ]
    try:
        return tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
    except (AttributeError, ValueError):
        return f"{SYSTEM_PROMPT}\n\nQuestion:\n{prompt_text}\n\n<answer>\n"


class GSM8KScorePolicy:
    """Rule-based reward for GSM8K: 1.0 for exact answer match, else 0.0.

    Plug a different task in by writing a class with a ``score`` method
    matching the signature here:
    ``score(generations, gen_mask, ground_truth) -> rewards``.
    """

    def __init__(self, model_name: str) -> None:
        # Lazy tokenizer so this object pickles cheaply into the Scorer proc.
        self.model_name = model_name
        self._tokenizer: Any = None

    def _ensure_tokenizer(self) -> Any:
        if self._tokenizer is None:
            from transformers.models.auto.tokenization_auto import AutoTokenizer

            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        return self._tokenizer

    def score(
        self,
        generations: torch.Tensor,
        gen_mask: torch.Tensor,
        ground_truth: str,
    ) -> torch.Tensor:
        """Return per-completion total reward over the [G] completions."""
        tokenizer = self._ensure_tokenizer()
        n = generations.shape[0]
        token_lists = [generations[i][gen_mask[i].bool()].tolist() for i in range(n)]
        texts = tokenizer.batch_decode(token_lists, skip_special_tokens=True)
        rewards = torch.zeros(n)
        for i, text in enumerate(texts):
            rewards[i] = 1.0 if extract_xml_answer(text) == ground_truth else 0.0
        return rewards


# %%
# Generic helpers
# ---------------


def gather_logprobs(
    sliced_logits: torch.Tensor, target_ids: torch.Tensor
) -> torch.Tensor:
    """Memory-efficient per-token log-probability of ``target_ids``.

    Computes ``target_logit - logsumexp(all_logits)`` rather than materialising
    a full ``[B, L, V]`` fp32 ``log_softmax`` tensor, which saves several GB
    of fp32 activations at typical batch/sequence/vocab sizes.
    """
    target_logits = sliced_logits.gather(-1, target_ids.unsqueeze(-1)).squeeze(-1)
    lse = torch.logsumexp(sliced_logits.float(), dim=-1)
    return target_logits - lse


def select_runtime_device() -> str:
    return "cuda" if torch.cuda.is_available() else "cpu"


def select_torch_dtype() -> torch.dtype:
    return torch.bfloat16 if torch.cuda.is_available() else torch.float32


def pad_trailing(x: torch.Tensor, pad_len: int, fill: float) -> torch.Tensor:
    """Right-pad a ``[G, L]`` tensor along dim 1 with ``fill`` to width ``L + pad_len``."""
    return torch.nn.functional.pad(x, (0, pad_len), value=fill)


# %%
# Data structures
# ---------------


@dataclass
class TrajectorySlice:
    """One rollout: a prompt + G sampled completions.

    Attributes:
        policy_version: Policy version that generated this slice.
        prompt_ids: Tokenized prompt, shape [prompt_len].
        generations: Generated token ids, shape [G, MAX_NEW_TOKENS]. Padded
            after each sequence's first EOS.
        gen_mask: 1.0 for real generated tokens, 0.0 after first EOS,
            shape [G, MAX_NEW_TOKENS]. Masks the loss.
        rewards: Per-completion total reward, shape [G]. Zero until the
            score policy fills it in.
        ground_truth: Correct answer string passed to the score policy.
    """

    policy_version: int
    prompt_ids: torch.Tensor
    generations: torch.Tensor
    gen_mask: torch.Tensor
    rewards: torch.Tensor
    ground_truth: str


@dataclass
class PolicyBatch:
    """Tensor bundle for one learner-side gradient step."""

    input_ids: torch.Tensor
    attention_mask: torch.Tensor
    generations: torch.Tensor
    gen_mask: torch.Tensor
    advantages: torch.Tensor
    row_idx: torch.Tensor
    col_idx: torch.Tensor


# %%
# Actors
# ------


class ReplayBuffer(Actor):
    """Storage for scored slices with policy-version-weighted sampling."""

    def __init__(self) -> None:
        self.storage: List[Tuple[int, TrajectorySlice]] = []
        self.storage_event = asyncio.Event()

    @endpoint
    async def put(self, slice_: TrajectorySlice) -> None:
        # Keep storage to one policy version: drop older, ignore stale.
        # Sampling stale slices would bias the on-policy gradient estimator.
        v = slice_.policy_version
        if self.storage:
            cur = self.storage[0][0]
            if v > cur:
                self.storage.clear()
                self.storage_event.clear()
            elif v < cur:
                return
        self.storage.append((v, slice_))
        self.storage_event.set()

    async def _wait_for_storage(self) -> None:
        if not self.storage:
            await self.storage_event.wait()

    @endpoint
    async def sample_from(self, k: int) -> List[TrajectorySlice]:
        try:
            await asyncio.wait_for(self._wait_for_storage(), timeout=30.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timeout waiting for ReplayBuffer to populate")
        fresh = [s for _, s in self.storage]
        chosen = random.choices(range(len(fresh)), k=k)
        return [fresh[i] for i in chosen]


class Scorer(Actor):
    """Generic reward dispatcher.

    Owns the trajectory queue. Generators call ``put`` to enqueue completions;
    ``run`` consumes them, delegates scoring to ``score_policy``, and forwards
    the scored slice to the replay buffer. ``score_policy`` is any object
    exposing ``score(generations, gen_mask, ground_truth) -> rewards``.
    """

    def __init__(self, score_policy: Any, replay_buffer: Any) -> None:
        self.score_policy = score_policy
        self.replay_buffer = replay_buffer
        self.queue: asyncio.Queue[TrajectorySlice] = asyncio.Queue()
        self.running = False

    @endpoint
    async def put(self, slice_: TrajectorySlice) -> None:
        await self.queue.put(slice_)

    async def _score_slice(self, slice_: TrajectorySlice) -> None:
        rewards = self.score_policy.score(
            slice_.generations, slice_.gen_mask, slice_.ground_truth
        )
        scored = TrajectorySlice(
            policy_version=slice_.policy_version,
            prompt_ids=slice_.prompt_ids,
            generations=slice_.generations,
            gen_mask=slice_.gen_mask,
            rewards=rewards,
            ground_truth=slice_.ground_truth,
        )
        await self.replay_buffer.put.call(scored)

    @endpoint
    async def run(self) -> None:
        if self.running:
            return
        self.running = True
        try:
            while self.running:
                try:
                    # 10s wait_for bounds shutdown latency after scorer.shutdown().
                    slice_ = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=10.0,
                    )
                except asyncio.TimeoutError:
                    continue
                await self._score_slice(slice_)
        finally:
            self.running = False

    @endpoint
    async def shutdown(self) -> None:
        self.running = False


class GeneratorState(Enum):
    READY_TO_GENERATE = "READY_TO_GENERATE"
    READY_TO_UPDATE = "READY_TO_UPDATE"


class Generator(Actor):
    """Generates G completions per prompt and ships them to the Scorer.

    Holds an inference-only copy of the policy model. Between rollouts,
    syncs updated weights from the Learner via RDMABuffer.
    """

    def __init__(
        self,
        model_name: str,
        weight_buffers: Dict[str, RDMABuffer],
        scorer: Any,
    ) -> None:
        from transformers.models.auto.modeling_auto import AutoModelForCausalLM
        from transformers.models.auto.tokenization_auto import AutoTokenizer

        if torch.cuda.is_available():
            torch.cuda.set_device(current_rank()["gpus"])
        self.device = select_runtime_device()
        self.model_dtype = select_torch_dtype()

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token_id = self.tokenizer.eos_token_id

        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=self.model_dtype,
            attn_implementation="eager",
            trust_remote_code=True,
        ).to(self.device)
        self.model.eval()
        self.weight_buffers = weight_buffers
        # Cache uint8 views of the live policy weights once. Storage is stable
        # across in-place writes (RDMABuffer.read_into), so the same view
        # always points at the right bytes -- no need to rebuild from
        # state_dict() every weight sync. Iterate named_parameters() to mirror
        # the learner-side export and avoid hitting registered buffers (e.g.
        # rotary_emb.inv_freq) that may not support .view(torch.uint8).
        self._param_views: Dict[str, torch.Tensor] = {
            n: p.data.view(torch.uint8).flatten()
            for n, p in self.model.named_parameters()
            if n in weight_buffers
        }
        self.scorer = scorer
        self.state = GeneratorState.READY_TO_GENERATE
        self.cond = asyncio.Condition()
        self.policy_version = 0

    @endpoint
    async def generate(self, prompt_text: str, ground_truth: str) -> None:
        async with self.cond:
            await self.cond.wait_for(
                lambda: self.state == GeneratorState.READY_TO_GENERATE
            )
            templated = format_prompt(self.tokenizer, prompt_text)
            prompt_ids = (
                self.tokenizer(templated, return_tensors="pt")
                .input_ids[0]
                .to(self.device)
            )
            input_ids = prompt_ids.unsqueeze(0).repeat(G, 1)
            attention_mask = torch.ones_like(input_ids)

            with torch.no_grad():
                out = self.model.generate(
                    input_ids=input_ids,
                    attention_mask=attention_mask,
                    max_new_tokens=MAX_NEW_TOKENS,
                    do_sample=True,
                    temperature=1.0,
                    pad_token_id=self.tokenizer.pad_token_id,
                    return_dict_in_generate=True,
                )
            gen_tokens = out.sequences[:, input_ids.shape[1] :]  # noqa: E203
            eos_id = self.tokenizer.eos_token_id
            post_eos = (gen_tokens == eos_id).cumsum(dim=-1) > 1
            gen_mask = (~post_eos).to(torch.float32)

            # HuggingFace generate() returns sequences of length max_over_group,
            # which can be shorter than MAX_NEW_TOKENS when every sampled
            # completion emits EOS early. The Learner downstream reshapes
            # generations as [-1, MAX_NEW_TOKENS] and requires this exact
            # trailing dim, so pad out to the full budget here.
            gl_raw = gen_tokens.shape[1]
            if gl_raw < MAX_NEW_TOKENS:
                pad_len = MAX_NEW_TOKENS - gl_raw
                gen_tokens = pad_trailing(
                    gen_tokens, pad_len, self.tokenizer.pad_token_id
                )
                gen_mask = pad_trailing(gen_mask, pad_len, 0.0)

            slice_ = TrajectorySlice(
                policy_version=self.policy_version,
                prompt_ids=prompt_ids.cpu(),
                generations=gen_tokens.cpu(),
                gen_mask=gen_mask.cpu(),
                rewards=torch.zeros(G),
                ground_truth=ground_truth,
            )

        await self.scorer.put.call(slice_)

        async with self.cond:
            self.state = GeneratorState.READY_TO_UPDATE
            self.cond.notify_all()

    @endpoint
    async def update(self, version: int) -> None:
        async with self.cond:
            total_bytes = sum(v.numel() for v in self._param_views.values())
            t0 = time.monotonic()
            await asyncio.gather(
                *(
                    buf.read_into(self._param_views[n], timeout=30)
                    for n, buf in self.weight_buffers.items()
                )
            )
            dt = time.monotonic() - t0
            gb = total_bytes / 1e9
            print(
                f"[Generator] weight sync v{version}: "
                f"{gb:.3f} GB in {dt:.2f}s = {gb / dt if dt > 0 else 0:.2f} GB/s "
                f"({len(self.weight_buffers)} buffers)",
                flush=True,
            )
            self.policy_version = version
            self.state = GeneratorState.READY_TO_GENERATE
            self.cond.notify_all()

    @endpoint
    async def eval_shard(self, eval_prompts: List[Tuple[str, str]]) -> Tuple[int, int]:
        """Greedy-decode this proc's shard of ``eval_prompts``.

        Returns ``(correct, total)`` for this shard. ``.call()`` from the
        caller gathers one such tuple per generator proc into a
        ``ValueMesh``, which the caller sums to get the totals.

        Sharding uses ``current_rank().rank`` as a flat global rank across
        the full ``hosts * gpus_per_host`` generator mesh, so eval
        wall-clock scales with ``1 / world_size``.
        """
        rank = current_rank().rank
        world_size = math.prod(int(v) for v in current_size().values())
        my_shard = eval_prompts[rank::world_size]

        # Eval runs outside the rollout/update protocol (only between
        # training phases, with no concurrent ``generate`` or ``update`` in
        # flight), so it doesn't gate on ``state``. The lock is defensive,
        # to serialize with any unexpected concurrent endpoint call.
        async with self.cond:
            self.model.eval()
            correct = 0
            for prompt_text, ground_truth in my_shard:
                templated = format_prompt(self.tokenizer, prompt_text)
                input_ids = self.tokenizer(templated, return_tensors="pt").input_ids.to(
                    self.device
                )
                with torch.no_grad():
                    out = self.model.generate(
                        input_ids=input_ids,
                        max_new_tokens=MAX_NEW_TOKENS,
                        do_sample=False,
                        pad_token_id=self.tokenizer.pad_token_id,
                    )
                gen = out[0, input_ids.shape[1] :]  # noqa: E203
                text = self.tokenizer.decode(gen, skip_special_tokens=True)
                if is_correct(text, ground_truth):
                    correct += 1
        return correct, len(my_shard)


class Learner(Actor):
    """Token-level GRPO update: REINFORCE on group-relative advantages."""

    def __init__(self, model_name: str, replay_buffer: Any) -> None:
        # Use direct submodule imports rather than `from transformers import
        # AutoModelForCausalLM` because transformers 5.x uses a lazy-loading
        # __init__.py that intermittently fails in Monarch actor subprocesses.
        from transformers.models.auto.modeling_auto import AutoModelForCausalLM
        from transformers.models.auto.tokenization_auto import AutoTokenizer

        # Input tensors live on cuda:0; ``device_map="auto"`` puts the
        # embedding + first layers there, so this avoids a copy on the
        # forward path. Backward auto-shards across whichever devices hold
        # each layer's parameters.
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"
        self.model_dtype = select_torch_dtype()

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token_id = self.tokenizer.eos_token_id

        # ``device_map="auto"`` splits the trainable policy across all
        # visible GPUs (2 on the learner pod) so the model + AdamW state
        # + activations fit comfortably. Eager attention avoids flash_attn
        # dependencies; gradient checkpointing trades compute for activation
        # memory.
        device_map = "auto" if torch.cuda.is_available() else None
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=self.model_dtype,
            attn_implementation="eager",
            trust_remote_code=True,
            device_map=device_map,
        )
        self.model.gradient_checkpointing_enable()

        self.optim = optim.AdamW(self.model.parameters(), lr=5e-6)
        self.grad_clip_norm = 1.0
        self.policy_version = 0
        self.replay_buffer = replay_buffer
        self.batch_size = 1
        self.generators: Optional[Any] = None
        # Pins the underlying ``p.data`` tensor storage for the lifetime of
        # the RDMABuffer registrations; see ``weights_handle``.
        self._pinned_weight_buffers: Dict[str, Tuple[torch.Tensor, RDMABuffer]] = {}

    @endpoint
    async def init_generators(self, generators: Any) -> None:
        self.generators = generators

    @endpoint
    async def weights_handle(self) -> Dict[str, RDMABuffer]:
        # Iterate parameters (not state_dict) so non-param buffers like
        # rotary_emb.inv_freq -- which may not support .view(torch.uint8)
        # -- are skipped. The local ``(p.data, buf)`` pair pins the tensor
        # storage so the RDMA registration stays valid; only the buffer is
        # shipped across the mesh boundary. Idempotent so repeat calls
        # return the same buffers without re-registering.
        if not self._pinned_weight_buffers:
            self._pinned_weight_buffers = {
                n: (p.data, RDMABuffer(p.data.view(torch.uint8).flatten()))
                for n, p in self.model.named_parameters()
            }
        return {n: buf for n, (_, buf) in self._pinned_weight_buffers.items()}

    def _compute_advantages(self, rewards: torch.Tensor) -> torch.Tensor:
        """GRPO-paper per-prompt advantage normalisation.

        For each prompt group of ``G`` completions, subtract the group mean
        and divide by the group standard deviation.
        """
        batch_size = rewards.shape[0] // G
        reshaped = rewards.view(batch_size, G)
        means = reshaped.mean(dim=1, keepdim=True)
        stds = reshaped.std(dim=1, keepdim=True)
        advs = (reshaped - means) / (stds + 1e-4)
        return advs.reshape(-1)

    def _build_policy_batch(
        self,
        prompt_ids_list: List[torch.Tensor],
        generations: torch.Tensor,
        gen_mask: torch.Tensor,
        advantages: torch.Tensor,
    ) -> PolicyBatch:
        device = self.device
        pad_id = self.tokenizer.pad_token_id

        rows: List[torch.Tensor] = []
        prompt_lens: List[int] = []
        for i, prompt_ids in enumerate(prompt_ids_list):
            prompt_len = int(prompt_ids.shape[0])
            for g in range(G):
                rows.append(torch.cat([prompt_ids, generations[i * G + g]]))
                prompt_lens.append(prompt_len)

        max_len = max(row.shape[0] for row in rows)
        batch_size = len(rows)
        input_ids = torch.full(
            (batch_size, max_len), pad_id, dtype=torch.long, device=device
        )
        attention_mask = torch.zeros(
            (batch_size, max_len), dtype=torch.long, device=device
        )
        for i, row in enumerate(rows):
            row_on_device = row.to(device)
            input_ids[i, : row.shape[0]] = row_on_device
            attention_mask[i, : row.shape[0]] = 1

        generations_dev = generations.to(device)
        gen_len = generations_dev.shape[1]
        row_idx = torch.arange(batch_size, device=device).unsqueeze(1)
        prompt_len_tensor = torch.tensor(prompt_lens, device=device)
        col_idx = (prompt_len_tensor - 1).unsqueeze(1) + torch.arange(
            gen_len, device=device
        )

        return PolicyBatch(
            input_ids=input_ids,
            attention_mask=attention_mask,
            generations=generations_dev,
            gen_mask=gen_mask.to(device),
            advantages=advantages.to(device),
            row_idx=row_idx,
            col_idx=col_idx,
        )

    def _apply_policy_update(self, batch: PolicyBatch) -> Tuple[torch.Tensor, float]:
        """One REINFORCE update. Returns ``(loss, grad_norm)``."""
        # ``device_map="auto"`` may place the lm_head on cuda:1; move the
        # logits back to ``self.device`` so row_idx/col_idx (cuda:0) can
        # index them. ``.to()`` is differentiable, so backward flows back
        # to cuda:1 transparently.
        policy_logits = self.model(
            batch.input_ids, attention_mask=batch.attention_mask
        ).logits.to(self.device)
        policy_selected = policy_logits[batch.row_idx, batch.col_idx]
        new_logps = gather_logprobs(policy_selected, batch.generations)

        token_count = batch.gen_mask.sum().clamp(min=1.0)
        advantages_b = batch.advantages.unsqueeze(-1)
        loss = -(advantages_b * new_logps * batch.gen_mask).sum() / token_count

        self.optim.zero_grad()
        loss.backward()
        grad_norm = nn.utils.clip_grad_norm_(
            self.model.parameters(), self.grad_clip_norm
        )
        self.optim.step()
        self.policy_version += 1
        return loss.detach(), float(grad_norm)

    @endpoint
    async def step(self) -> Dict[str, float]:
        """Run one gradient update. Returns per-step training metrics."""
        # The weight push and the replay-buffer sample are independent;
        # overlap them so the RDMA round-trip hides behind the local fetch.
        sample_coro = self.replay_buffer.sample_from.call_one(self.batch_size)
        if self.generators is not None:
            _, slices = await asyncio.gather(
                self.generators.update.call(self.policy_version),
                sample_coro,
            )
        else:
            slices = await sample_coro
        prompt_ids_list = [s.prompt_ids for s in slices]
        generations = torch.stack([s.generations for s in slices]).view(
            -1, MAX_NEW_TOKENS
        )
        gen_mask = torch.cat([s.gen_mask for s in slices])
        rewards = torch.cat([s.rewards for s in slices])
        advs = self._compute_advantages(rewards)
        batch = self._build_policy_batch(prompt_ids_list, generations, gen_mask, advs)
        loss, grad_norm = self._apply_policy_update(batch)
        return {
            "loss": float(loss),
            "reward_mean": float(rewards.mean()),
            "grad_norm": grad_norm,
        }

    @endpoint
    async def sync_generators(self) -> None:
        """Push the learner's current policy weights to every generator.

        Eval runs on the generator mesh so it can shard the eval set and
        decode ``world_size``-way in parallel. ``step`` syncs generators
        once per gradient update, before the gradient itself runs, so by
        the end of training the generators carry the pre-final-gradient
        policy and are one step stale; this method is the explicit flush,
        called immediately before a sharded eval on the final policy.
        """
        if self.generators is not None:
            await self.generators.update.call(self.policy_version)


# %%
# Pod spec
# --------

# Installing Python dependencies at container startup is for DEMONSTRATION
# ONLY. In production, bake ``transformers``, ``tokenizers``, and
# ``accelerate`` into the worker container image.
PIP_INSTALL = textwrap.dedent("""\
    import subprocess, sys
    # --break-system-packages is required on the Debian-based monarch image
    # (PEP 668); without it pip refuses to install into the system Python.
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--quiet",
        "--break-system-packages",
        "--upgrade",
        "transformers",
        "tokenizers",
        "accelerate",
    ])
""")


def build_pod_template(gpus: int) -> V1PodTemplateSpec:
    """Shared pod template for learner and generator meshes.

    Prepends a pip-install prefix to the Monarch worker bootstrap so that
    ``transformers``, ``tokenizers``, and ``accelerate`` are present before
    ``run_worker_loop_forever`` is imported.

    Sets:
    - ``MONARCH_PORT=26600`` (matches the default label selector).
    - ``PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True`` (required by
      RDMABuffer's CUDA allocator check).
    - ``HF_HOME=/tmp/hf_cache`` so the Hugging Face cache lands on rootfs
      ephemeral storage.

    Mounts ``/dev/shm`` as a 16 GiB memory-backed emptyDir for shared memory
    used by NCCL / multi-worker inference.
    """
    bootstrap = PIP_INSTALL + _WORKER_BOOTSTRAP_SCRIPT
    resources = None
    env = [
        V1EnvVar(name="MONARCH_PORT", value="26600"),
        # /tmp/hf_cache forces the Hugging Face cache onto
        # rootfs ephemeral storage for DEMONSTRATION ONLY --
        # every pod redownloads the full model at startup. In
        # production, stage model weights onto a persistent
        # volume (e.g. a read-only PVC populated by an image
        # pre-puller or an init container) and point HF_HOME
        # there, so worker pods start instantly and do not
        # hammer the HF Hub.
        V1EnvVar(name="HF_HOME", value="/tmp/hf_cache"),
    ]
    if gpus > 0:
        gpu_resources = {"nvidia.com/gpu": str(gpus)}
        resources = V1ResourceRequirements(
            limits=gpu_resources,
            requests=gpu_resources,
        )
        env.insert(
            1,
            V1EnvVar(
                name="PYTORCH_CUDA_ALLOC_CONF",
                value="expandable_segments:True",
            ),
        )
    return V1PodTemplateSpec(
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="worker",
                    image=MONARCH_IMAGE,
                    command=["python", "-u", "-c", bootstrap],
                    env=env,
                    resources=resources,
                    # Mark pod Ready only once the Monarch worker loop is actually
                    # listening on :26600. Without this, K8s reports Ready as soon
                    # as the container starts -- which is during pip install, long
                    # before run_worker_loop_forever binds the port. The controller
                    # then connects too early and the 30s message delivery timeout
                    # fires before the worker is reachable.
                    readiness_probe=V1Probe(
                        tcp_socket=V1TCPSocketAction(port=26600),
                        initial_delay_seconds=30,
                        period_seconds=5,
                        timeout_seconds=5,
                        failure_threshold=60,
                    ),
                    volume_mounts=[
                        V1VolumeMount(name="dshm", mount_path="/dev/shm"),
                    ],
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


# %%
# Orchestration
# -------------


async def run_sharded_eval(
    learner: Any,
    generators: Any,
    prompts_for_eval: List[Tuple[str, str]],
    sync_first: bool,
) -> Tuple[int, int]:
    """Sharded greedy-decode eval across the generator mesh.

    When ``sync_first`` is True, push the learner's current policy weights to
    every generator first; otherwise rely on the caller to know the meshes
    are already aligned (e.g. baseline eval immediately after init). Each
    generator proc decodes ``prompts_for_eval[rank::world_size]``; this
    function sums the per-shard ``(correct, total)`` tuples.
    """
    if sync_first:
        await learner.sync_generators.call_one()
    value_mesh = await generators.eval_shard.call(prompts_for_eval)
    correct = 0
    total = 0
    for k, n in value_mesh.values():
        correct += k
        total += n
    return correct, total


async def main(
    model_name: str,
    num_generator_hosts: int,
    gpus_per_generator: int,
    training_steps: int,
    namespace: str,
    dataset_split: str,
    num_prompts: int,
    eval_size: int,
) -> None:
    """Run GRPO fine-tuning across the learner and generator meshes."""
    prompts = load_gsm8k_prompts(split=dataset_split, num_prompts=num_prompts)
    eval_prompts = load_gsm8k_prompts(split="test", num_prompts=eval_size)
    print("=" * 60)
    print(f"Kubernetes GRPO ({model_name})")
    print(
        f"Config: 1 learner pod x 2 GPUs | "
        f"{num_generator_hosts} generator pods x {gpus_per_generator} GPU(s)"
    )
    print(f"Namespace: {namespace} | Training steps: {training_steps}")
    print(f"Dataset: openai/gsm8k[{dataset_split}] ({len(prompts)} prompts)")
    print(f"Eval: openai/gsm8k[test] ({len(eval_prompts)} prompts)")
    print("=" * 60)

    # 600s timeout lets the meshes finish cold-start pip install + model
    # download before state() gives up.
    k8s_job = KubernetesJob(namespace=namespace, timeout=600)
    # Learner pod requests 2 GPUs; ``device_map="auto"`` inside the actor
    # spreads the trainable policy across both. ``spawn_procs({"gpus": 1})``
    # below still spawns a single learner proc that sees both GPUs in its
    # CUDA namespace. To fine-tune a larger base model, bump this to 4
    # (e.g. for a 4B-parameter policy, the static memory of params + bf16
    # gradients + fp32 AdamW state alone is ~48 GB, requiring 4 x 22 GB GPUs).
    k8s_job.add_mesh(
        "learner",
        num_replicas=1,
        pod_template=build_pod_template(gpus=2),
    )
    k8s_job.add_mesh(
        "generator",
        num_replicas=num_generator_hosts,
        pod_template=build_pod_template(gpus=gpus_per_generator),
    )

    learner_mesh = None
    gen_mesh = None
    try:
        job_state = k8s_job.state()
        learner_mesh = job_state.learner.spawn_procs({"gpus": 1})
        gen_mesh = job_state.generator.spawn_procs({"gpus": gpus_per_generator})

        await asyncio.gather(
            learner_mesh.logging_option(stream_to_client=True),
            gen_mesh.logging_option(stream_to_client=True),
        )

        score_policy = GSM8KScorePolicy(model_name)
        replay_buf = learner_mesh.spawn("rb", ReplayBuffer)
        learner = learner_mesh.spawn("learner", Learner, model_name, replay_buf)
        scorer = learner_mesh.spawn("scorer", Scorer, score_policy, replay_buf)

        wb = await learner.weights_handle.call_one()
        generators = gen_mesh.spawn("generator", Generator, model_name, wb, scorer)
        await learner.init_generators.call(generators)

        # Seed the replay buffer with one scored slice before training so
        # step 0's ``sample_from`` doesn't race with its concurrent rollout.
        seed_prompt, seed_gt = prompts[0]
        await generators.generate.call(seed_prompt, seed_gt)
        scorer_run_future = scorer.run.call_one()

        # Baseline eval before any training. Generators and learner are
        # both freshly loaded from the same HF checkpoint, so skip the sync.
        baseline_correct, baseline_total = await run_sharded_eval(
            learner, generators, eval_prompts, sync_first=False
        )
        baseline_acc = baseline_correct / max(1, baseline_total)
        print(
            f"[Baseline] acc={baseline_acc:.3f} ({baseline_correct}/{baseline_total})",
            flush=True,
        )

        for step in range(training_steps):
            idx = step % len(prompts)
            prompt, gt = prompts[idx]
            _, stats = await asyncio.gather(
                generators.generate.call(prompt, gt),
                learner.step.call_one(),
            )
            print(
                f"[Step {step:03d}] "
                f"loss={stats['loss']:+.4f} "
                f"reward={stats['reward_mean']:.3f} "
                f"gn={stats['grad_norm']:.3f}",
                flush=True,
            )

        final_correct, final_total = await run_sharded_eval(
            learner, generators, eval_prompts, sync_first=True
        )
        final_acc = final_correct / max(1, final_total)

        print("", flush=True)
        print("=" * 60, flush=True)
        print("Training summary", flush=True)
        print("-" * 60, flush=True)
        print(
            f"Baseline acc: {baseline_acc:.3f} ({baseline_correct}/{baseline_total})",
            flush=True,
        )
        print(
            f"Final acc:    {final_acc:.3f} ({final_correct}/{final_total})",
            flush=True,
        )
        print(f"Delta:        {final_acc - baseline_acc:+.3f}", flush=True)
        print("=" * 60, flush=True)

        # Shutdown latency bounded by the 10s wait_for in Scorer.run().
        print("Stopping scorer...", flush=True)
        await scorer.shutdown.call_one()
        await scorer_run_future
        print("Training complete.", flush=True)
    finally:
        # Best-effort per-mesh cleanup; one failure must not skip the other.
        if learner_mesh is not None:
            try:
                learner_mesh.stop().get()
            except Exception as e:
                print(f"[cleanup] learner_mesh.stop() failed: {e}")
        if gen_mesh is not None:
            try:
                gen_mesh.stop().get()
            except Exception as e:
                print(f"[cleanup] gen_mesh.stop() failed: {e}")
        # Unconditional so MonarchMesh CRDs and pods are always torn down.
        k8s_job.kill()


# %%
# CLI
# ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run GRPO fine-tuning on Kubernetes with Qwen3.5-0.8B-Base"
    )
    parser.add_argument(
        "--model_name",
        type=str,
        default=MODEL_NAME,
        help="HF model used for learner and generator actors",
    )
    parser.add_argument(
        "--num_generator_hosts",
        type=int,
        default=2,
        help="Number of generator worker pods",
    )
    parser.add_argument(
        "--gpus_per_generator",
        type=int,
        default=4,
        help="GPUs per generator pod; set 0 for CPU",
    )
    parser.add_argument(
        "--training_steps",
        type=int,
        default=100,
        help="Number of gradient update iterations",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default=MONARCH_NAMESPACE,
        help="Kubernetes namespace for the MonarchMesh CRDs",
    )
    parser.add_argument(
        "--dataset_split",
        type=str,
        default="train",
        help="GSM8K split to load (train or test)",
    )
    parser.add_argument(
        "--num_prompts",
        type=int,
        default=1000,
        help="Number of GSM8K prompts to load; 0 means the full split",
    )
    parser.add_argument(
        "--eval_size",
        type=int,
        default=512,
        help="Number of held-out GSM8K test prompts used for eval.",
    )
    args = parser.parse_args()
    asyncio.run(
        main(
            model_name=args.model_name,
            num_generator_hosts=args.num_generator_hosts,
            gpus_per_generator=args.gpus_per_generator,
            training_steps=args.training_steps,
            namespace=args.namespace,
            dataset_split=args.dataset_split,
            num_prompts=args.num_prompts,
            eval_size=args.eval_size,
        )
    )
