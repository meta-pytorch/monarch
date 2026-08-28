# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Ray front-end for the WeightBridge example.

All the actual work lives in :mod:`worker_bodies` (orchestrator-agnostic); the actors here are thin
delegates, and :class:`RayOrchestrator` is the surface :mod:`train` drives. :mod:`workers_monarch`
provides the same surface on Monarch actors — which is what a Monarch RDMA run requires, since
``RDMABuffer``/``RDMAAction`` only work inside a Monarch actor.

HF weights are not serialized in ``EngineArgs``; each worker calls ``build_checkpoint()`` locally so
checkpoints are identical across nodes without shipping CPU tensors through Ray.
"""

from __future__ import annotations

import logging

import ray
from wbridge.backend import coordinator
from wbridge.backend.control_channel import coordinator_ipc

# Re-exported for callers that import them from here (train.py, bench_transfer.py).
from worker_bodies import (  # noqa: F401
    apply_network_env as _apply_network_env_for_process_group,
    EngineArgs,
    network_env_vars as _network_env_vars,
    RolloutWorkerBody,
    TrainerWorkerBody,
)

logger = logging.getLogger("wbridge.example.workers")


@ray.remote(num_gpus=1, num_cpus=1)
class RolloutWorker:
    def __init__(self) -> None:
        self._b = RolloutWorkerBody()

    def init(self, rank: int, args: EngineArgs):
        return self._b.init(rank, args)

    def recv_weights(self) -> None:
        return self._b.recv_weights()

    def transport_stats(self) -> dict:
        return self._b.transport_stats()

    def verify(self) -> dict:
        return self._b.verify()


@ray.remote(num_cpus=1)
class RolloutEngine:
    """Spawns the standalone ZMQ coordinator process and the :class:`RolloutWorker` actors.

    There is no HTTP server: the control plane is a per-engine coordinator subprocess (pure ZMQ).
    """

    def init(self, args: EngineArgs):
        # Standalone coordinator process: Trainer-facing tcp://*:rollout_port, rank0-facing IPC. The IPC
        # path is derived deterministically from the port so the (co-located) workers agree on it.
        ipc = coordinator_ipc(args.rollout_port)
        args.rollout_controller_ipc_name = ipc
        self._coord_proc = coordinator.spawn(ipc, args.rollout_port)
        print(
            f"RolloutEngine coordinator on {args.rollout_host}:{args.rollout_port} (ipc {ipc})"
        )

        n = args.num_rollout_workers
        runtime_env = {
            "env_vars": _network_env_vars(args.network_provider, args.network_interface)
        }
        self._workers = [
            RolloutWorker.options(
                scheduling_strategy=args.rollout_scheduling_strategy,
                runtime_env=runtime_env,
            ).remote()
            for _ in range(n)
        ]
        # rank 0 reports num_workers to the coordinator on connect — no set_worker_num needed.
        ray.get([w.init.remote(rank, args) for rank, w in enumerate(self._workers)])

    def recv_weights(self) -> None:
        ray.get([w.recv_weights.remote() for w in self._workers])

    def transport_stats_all(self) -> list[dict]:
        return ray.get([w.transport_stats.remote() for w in self._workers])

    def verify_all(self) -> str:
        results = ray.get([w.verify.remote() for w in self._workers])
        results = sorted(results, key=lambda r: r["rank"])
        for r in results:
            if not r["ok"]:
                return f"RolloutWorker rank {r['rank']} failed: {r['detail']}"
        return "All RolloutWorkers verified their shards independently."


@ray.remote(num_gpus=1, num_cpus=1)
class TrainerWorker:
    def __init__(self) -> None:
        self._b = TrainerWorkerBody()

    def init(self, rank: int, args: EngineArgs):
        return self._b.init(rank, args)

    def send_weights(self):
        return self._b.send_weights()

    def transport_stats(self) -> dict:
        return self._b.transport_stats()


class TrainerEngine:
    """Spawns :class:`TrainerWorker` actors (must run before rollout so LoadSpec exists on disk)."""

    def __init__(self, args: EngineArgs):
        n = args.num_trainer_workers
        runtime_env = {
            "env_vars": _network_env_vars(args.network_provider, args.network_interface)
        }
        self._workers = [
            TrainerWorker.options(
                scheduling_strategy=args.trainer_scheduling_strategy,
                runtime_env=runtime_env,
            ).remote()
            for _ in range(n)
        ]
        ray.get([w.init.remote(rank, args) for rank, w in enumerate(self._workers)])
        print(f"TrainerEngine started on {args.trainer_host}:{args.trainer_pg_port}")

    def send_weights(self):
        ray.get([w.send_weights.remote() for w in self._workers])

    def transport_stats_all(self) -> list[dict]:
        return ray.get([w.transport_stats.remote() for w in self._workers])


class RayOrchestrator:
    """Ray implementation of the surface :mod:`train` drives."""

    name = "ray"

    def __init__(self, args: EngineArgs) -> None:
        self.args = args

    def start(self) -> None:
        # Trainer first: LoadSpec inference writes to disk and the rollout side reads it.
        self._trainer = TrainerEngine(self.args)
        self._rollout = RolloutEngine.options(
            scheduling_strategy=self.args.rollout_scheduling_strategy
        ).remote()
        ray.get(self._rollout.init.remote(self.args))

    def run_transfer(self) -> None:
        recv = self._rollout.recv_weights.remote()  # poll concurrently with the send
        self._trainer.send_weights()
        ray.get(recv)

    def verify_all(self) -> str:
        return ray.get(self._rollout.verify_all.remote())

    def transport_stats(self) -> list[dict]:
        return self._trainer.transport_stats_all() + ray.get(
            self._rollout.transport_stats_all.remote()
        )

    def shutdown(self) -> None:
        ray.shutdown()
