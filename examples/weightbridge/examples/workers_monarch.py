# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Monarch front-end for the WeightBridge example — the same roles as :mod:`workers`, on actor meshes.

This exists because Monarch RDMA is only reachable from inside a Monarch actor: ``RDMABuffer`` and
``RDMAAction.submit`` both resolve ``context().actor_instance``. Ray workers can never host it, so running
WeightBridge over the Monarch transport requires the processes themselves to be Monarch actors. The work
each role does is unchanged — it delegates to :mod:`worker_bodies`.

Placement mirrors the Ray path: rollout workers on the rollout host, trainer workers on the trainer host,
matched by IP so ``--colocate`` (both hosts the same) keeps working. The ZMQ coordinator is transport-
independent and still runs as a subprocess, spawned by an actor on the rollout host so it lands on the
right machine.

The host mesh is built by attaching to worker loops that the surrounding scheduler or process manager
already started. Their addresses are passed to the example explicitly; WeightBridge does not assume a
particular cluster scheduler or launcher layout.
"""

from __future__ import annotations

import logging

from monarch.actor import Actor, endpoint
from monarch_common import (
    ActorThread as _ActorThread,
    attach_hosts,
    bind_gpu as _bind_gpu,
    host_index,
    my_rank as _my_rank,
)
from wbridge.backend import coordinator
from wbridge.backend.control_channel import coordinator_ipc
from worker_bodies import EngineArgs, RolloutWorkerBody, TrainerWorkerBody

logger = logging.getLogger("wbridge.example.workers_monarch")


class RolloutWorkerActor(Actor):
    def __init__(self) -> None:
        self._b = RolloutWorkerBody()
        self._t = _ActorThread("wb-rollout")

    @endpoint
    async def init(self, args: EngineArgs) -> None:
        # Rank comes from the mesh position, so init can be ONE broadcast to all actors. That matters:
        # ReceiverAdapter's ControlChannel constructor is a rendezvous across the engine's ranks, so
        # initializing them one at a time deadlocks (rank 0 waits for peers that were never started).
        rank = _my_rank()
        await self._t.run(
            _bind_gpu, rank
        )  # on the worker thread: set_device is thread-local
        await self._t.run(self._b.init, rank, args)

    @endpoint
    async def recv_weights(self) -> None:
        await self._t.run(self._b.recv_weights)

    @endpoint
    async def transport_stats(self) -> dict:
        return await self._t.run(self._b.transport_stats)

    @endpoint
    async def verify(self) -> dict:
        return await self._t.run(self._b.verify)


class TrainerWorkerActor(Actor):
    def __init__(self) -> None:
        self._b = TrainerWorkerBody()
        self._t = _ActorThread("wb-trainer")

    @endpoint
    async def init(self, args: EngineArgs) -> None:
        rank = _my_rank()
        await self._t.run(_bind_gpu, rank)
        await self._t.run(self._b.init, rank, args)

    @endpoint
    async def send_weights(self) -> None:
        await self._t.run(self._b.send_weights)

    @endpoint
    async def transport_stats(self) -> dict:
        return await self._t.run(self._b.transport_stats)


class CoordinatorActor(Actor):
    """Runs the ZMQ coordinator subprocess on the rollout host (it must share a machine with rank 0)."""

    def __init__(self) -> None:
        self._proc = None

    @endpoint
    async def spawn(self, ipc: str, port: int) -> str:
        import socket

        self._proc = coordinator.spawn(ipc, port)
        return f"coordinator on {socket.gethostname()}:{port} (ipc {ipc})"


class MonarchOrchestrator:
    """Monarch implementation of the surface :mod:`train` drives."""

    name = "monarch"

    def __init__(self, args: EngineArgs, worker_addrs: list[str]) -> None:
        self.args = args
        self.worker_addrs = worker_addrs

    def _host_index(self, ip: str) -> int:
        return host_index(self.worker_addrs, ip)

    def start(self) -> None:
        a = self.args
        self.hosts = attach_hosts(self.worker_addrs)
        t_idx = self._host_index(a.trainer_host)
        r_idx = self._host_index(a.rollout_host)
        print(
            f"[monarch] trainer host slice {t_idx}, rollout host slice {r_idx} "
            f"({'co-located' if t_idx == r_idx else 'separate nodes'})",
            flush=True,
        )

        # Trainer first: its LoadSpec inference writes to disk and the rollout side reads it. Every mesh
        # is awaited before use — pickling an uninitialized mesh into a later spawn blocks Monarch's
        # tokio runtime, which is how the 2-node spike first wedged.
        self._t_procs = self.hosts.slice(hosts=t_idx).spawn_procs(
            {"gpus": a.num_trainer_workers}
        )
        self._t_procs.initialized.get()
        self._trainers = self._t_procs.spawn("wb_trainer", TrainerWorkerActor)
        self._trainers.initialized.get()
        self._trainers.init.call(
            args=a
        ).get()  # ONE broadcast: all ranks init concurrently
        print(f"[monarch] {a.num_trainer_workers} trainer workers ready", flush=True)

        # Coordinator on the rollout host, then the rollout workers pointed at its IPC path.
        ipc = coordinator_ipc(a.rollout_port)
        a.rollout_controller_ipc_name = ipc
        self._c_procs = self.hosts.slice(hosts=r_idx).spawn_procs({"gpus": 1})
        self._c_procs.initialized.get()
        self._coord = self._c_procs.spawn("wb_coord", CoordinatorActor)
        self._coord.initialized.get()
        print(
            "[monarch] "
            + self._coord.spawn.call_one(ipc=ipc, port=a.rollout_port).get(),
            flush=True,
        )

        self._r_procs = self.hosts.slice(hosts=r_idx).spawn_procs(
            {"gpus": a.num_rollout_workers}
        )
        self._r_procs.initialized.get()
        self._rollouts = self._r_procs.spawn("wb_rollout", RolloutWorkerActor)
        self._rollouts.initialized.get()
        self._rollouts.init.call(
            args=a
        ).get()  # ONE broadcast (see RolloutWorkerActor.init)
        print(f"[monarch] {a.num_rollout_workers} rollout workers ready", flush=True)

    def run_transfer(self) -> None:
        # .call() fans out to every actor in the mesh and returns a future, so the receivers poll while
        # the senders run — the same concurrency the Ray path gets from a detached recv_weights task.
        recv = self._rollouts.recv_weights.call()
        send = self._trainers.send_weights.call()
        send.get()
        recv.get()

    def verify_all(self) -> str:
        results = sorted(
            (v for _, v in self._rollouts.verify.call().get()), key=lambda r: r["rank"]
        )
        for r in results:
            if not r["ok"]:
                return f"RolloutWorker rank {r['rank']} failed: {r['detail']}"
        return "All RolloutWorkers verified their shards independently."

    def transport_stats(self) -> list[dict]:
        return [v for _, v in self._trainers.transport_stats.call().get()] + [
            v for _, v in self._rollouts.transport_stats.call().get()
        ]

    def shutdown(self) -> None:
        # Worker loops are owned by the launcher script, which tears them down on exit.
        pass
