# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Orchestrator-agnostic worker logic for the example, shared by the Ray and Monarch front-ends.

The example originally lived entirely inside ``@ray.remote`` classes. Monarch RDMA only works from inside
a Monarch actor (``RDMABuffer``/``RDMAAction`` both resolve ``context()``), so the example needs a second
orchestrator — but none of the *work* is orchestrator-specific: building the toy model, constructing the
adapter, polling for an update, verifying, reporting transport stats. That logic lives here as plain
classes; :mod:`workers` wraps them in Ray actors and :mod:`workers_monarch` in Monarch actors, so there is
exactly one copy of the integration reference.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

import torch
from qwen_tiny import (
    build_rollout_wksd,
    build_trainer_wksd,
    make_rollout_load_weights,
    make_trainer_load_weights,
    QwenTinyConfig,
)
from utils import apply_network_env, gpu_link_info, make_hf_weights, network_env_vars
from wbridge.backend.sender import SenderArgs
from wbridge.frontend.adapters import AdapterContext, ReceiverAdapter, SenderAdapter
from wbridge.utils.specgen import HFWeightFetcher

CheckpointBuilder = Callable[[], dict[str, torch.Tensor]]


def _wrap_load_weights(orig_lw):
    """Wrap an old-style ``load_weights(Iterable[(name, Tensor)])`` as ``load_weights(HFWeightFetcher)``."""

    def lw(hf_weights: HFWeightFetcher):
        orig_lw((name, fn()) for name, fn in hf_weights.items())

    return lw


@dataclass
class EngineArgs:
    """``build_checkpoint`` must be picklable (e.g. ``functools.partial`` of a module-level function)."""

    rollout_host: str
    rollout_port: int
    rollout_scheduling_strategy: (
        object  # Ray NodeAffinitySchedulingStrategy; unused by Monarch
    )
    num_rollout_workers: int

    trainer_host: str
    trainer_pg_port: int
    trainer_scheduling_strategy: object  # Ray only
    num_trainer_workers: int

    model_config: QwenTinyConfig
    build_checkpoint: CheckpointBuilder
    dtype: torch.dtype = torch.float32
    rollout_controller_ipc_name: str = ""
    network_provider: str = "tcp"
    network_interface: str = ""
    # wbridge RDMA backend: "tcp"/"efa" -> Mooncake, "monarch" -> MonarchEngine.
    protocol: str = "tcp"


class RolloutWorkerBody:
    """One per Rollout Worker GPU. Receives weights and verifies against a pre-update backup."""

    def init(self, rank: int, args: EngineArgs) -> None:
        apply_network_env(args.network_provider, args.network_interface)
        self.rank = rank
        self.args = args
        cfg = args.model_config
        hf_cpu = args.build_checkpoint()
        hf_weights, hf_shapes = make_hf_weights(hf_cpu)
        self.state_dict = build_rollout_wksd(
            cfg,
            device="cuda",
            dtype=args.dtype,
            tp_rank=rank,
            tp_size=args.num_rollout_workers,
        )
        orig_lw = make_rollout_load_weights(
            self.state_dict,
            cfg,
            device="cuda",
            dtype=args.dtype,
            tp_rank=rank,
            tp_size=args.num_rollout_workers,
        )
        load_weights = _wrap_load_weights(orig_lw)
        load_weights(hf_weights)
        ctx = AdapterContext(
            hf_weights=hf_weights,
            hf_shapes=hf_shapes,
            wksd_factory=lambda: self.state_dict,
            load_weights=load_weights,
            rank=rank,
        )
        self.adapter = ReceiverAdapter(
            ctx, args.rollout_controller_ipc_name, num_workers=args.num_rollout_workers
        )
        # Snapshot the loaded weights before the first recv so verify() can diff backup vs received.
        self.state_dict_backup = {
            name: t.detach().clone() for name, t in self.state_dict.items()
        }

    def recv_weights(self) -> None:
        for _ in range(500):
            t0 = time.time()
            updated = self.adapter.poll_requests()
            t1 = time.time()
            if updated:
                print(
                    f"RolloutWorker rank {self.rank} recv_weights start wall_s={t0} return wall_s={t1}",
                    flush=True,
                )
                self.adapter.flush_profile_outputs()
                return
            time.sleep(0.05)
        raise TimeoutError("receiver never became ready for weights")

    def transport_stats(self) -> dict:
        """Bulk bytes this receiver landed, by transport, plus its GPU's NVLink state."""
        return {**self.adapter.receiver.transport_stats(), **gpu_link_info()}

    def verify(self) -> dict:
        for name, backup in self.state_dict_backup.items():
            received = self.state_dict[name]
            if not torch.allclose(backup, received):
                return {
                    "rank": self.rank,
                    "name": name,
                    "ok": False,
                    "detail": (
                        f"value mismatch for {name} on rank {self.rank}, "
                        f"expected: {backup[:, :1].view(-1)}, got: {received[:, :1].view(-1)}"
                    ),
                }
        return {"rank": self.rank, "ok": True, "detail": "all values match"}


class TrainerWorkerBody:
    """One per Trainer Worker GPU. Sends shards via :class:`SenderAdapter`."""

    def init(self, rank: int, args: EngineArgs) -> None:
        apply_network_env(args.network_provider, args.network_interface)
        self.args = args
        self.rank = rank
        cfg = args.model_config
        hf_cpu = args.build_checkpoint()
        hf_weights, hf_shapes = make_hf_weights(hf_cpu)
        self.state_dict = build_trainer_wksd(
            cfg,
            device="cuda",
            dtype=args.dtype,
            tp_rank=rank,
            tp_size=args.num_trainer_workers,
        )
        orig_lw = make_trainer_load_weights(
            self.state_dict,
            cfg,
            device="cuda",
            dtype=args.dtype,
            tp_rank=rank,
            tp_size=args.num_trainer_workers,
        )
        load_weights = _wrap_load_weights(orig_lw)
        load_weights(hf_weights)
        ctx = AdapterContext(
            hf_weights=hf_weights,
            hf_shapes=hf_shapes,
            wksd_factory=lambda: self.state_dict,
            load_weights=load_weights,
            rank=rank,
        )
        sender_args = SenderArgs(
            world_size=args.num_trainer_workers,
            protocol=args.protocol,
            receiver_urls=[f"tcp://{args.rollout_host}:{args.rollout_port}"],
            master_addr=args.trainer_host,
            master_port=args.trainer_pg_port,
        )
        self.adapter = SenderAdapter(ctx, sender_args)

    def send_weights(self) -> None:
        self.adapter.connect()
        t0 = time.time()
        print(
            f"TrainerWorker rank {self.rank} send_weights start wall_s={t0}", flush=True
        )
        self.adapter.send_weights()
        # The RDMA/pull completions run on the Stage-2 thread after send() returns; the byte counters are
        # only final once the epoch has drained, so block here (the example is not measuring overlap).
        self.adapter.wait_send_complete()
        t1 = time.time()
        print(
            f"TrainerWorker rank {self.rank} send_weights return wall_s={t1} elapsed_s={t1 - t0}",
            flush=True,
        )
        self.adapter.flush_profile_outputs()

    def transport_stats(self) -> dict:
        """Bulk bytes this sender moved, by transport, plus its GPU's NVLink state."""
        return {**self.adapter.sender.transport_stats(), **gpu_link_info()}
