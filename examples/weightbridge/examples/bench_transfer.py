# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Frameworkless replay of a captured 30B weight transfer — fast WTT iteration.

Reads per-rank LoadSpecs dumped by ``WBRIDGE_DUMP_LOADSPEC`` (from one real run), rebuilds
``WeightSender``/``WeightReceiver`` **directly** (no Megatron/SGLang/specgen), and runs K timed
transfers over Ray with RDMA across nodes and CUDA IPC within a node. Startup is seconds; each iteration is
one real transfer (pack -> RDMA -> receiver<->receiver dedup exchange -> consume) with the *exact* captured
layout.

Worker tensor values are synthetic. By default they are uninitialized and the run is perf-only; with
``--seed`` they become deterministic and each receiver reports a post-consume digest, which is what makes
two transports comparable for *correctness* as well as speed (see :mod:`bench_bodies`). Topology comes
from the captured records: ``world_size`` trainer senders + N rollout engines each with ``num_workers``
receivers; both engines pin to the rollout node so corresponding ranks take the NVLink
receiver<->receiver path.

"""

from __future__ import annotations

import argparse
import asyncio
import os

import ray
import torch  # noqa: F401 — ensures torch/CUDA import in the driver
from bench_bodies import network_env_vars, ReplayReceiverBody, ReplaySenderBody
from bench_report import ENGINE_ENV_KEYS, report_run, SENDER_ENV_KEYS
from loadspec_replay import group_records, load_records, summary
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from utils import get_ray_nodes, visible_device_list
from wbridge.backend import coordinator
from wbridge.backend.control_channel import coordinator_ipc
from wbridge.backend.sender import SenderArgs

_network_env_vars = network_env_vars  # back-compat for importers of this module


@ray.remote(
    num_gpus=0, num_cpus=1
)  # num_gpus=0 -> actor sees ALL node GPUs (like SGLang); set_device manually
class ReplayReceiver(ReplayReceiverBody):
    """Ray wrapper; every method is :class:`ReplayReceiverBody`'s (shared with the Monarch front-end)."""


@ray.remote(num_cpus=0)
class ReplayDrainBarrier:
    """Replay-only epoch barrier proving complete drain before the next WT starts.

    This actor carries no WeightBridge protocol traffic.  Every sender and every physical receiver rank
    reports completion after its local full-drain boundary, then waits for the complete participant set.
    The method is async so one actor can retain all concurrent arrivals without head-of-line blocking.
    """

    def __init__(self, participants: list[str], timeout_s: float = 1800.0) -> None:
        if not participants or len(set(participants)) != len(participants):
            raise ValueError(
                "replay drain-barrier participants must be non-empty and unique"
            )
        self._participants = frozenset(participants)
        self._timeout_s = float(timeout_s)
        self._arrivals: dict[int, set[str]] = {}
        self._events: dict[int, asyncio.Event] = {}
        self._last_epoch: dict[str, int] = {}

    async def arrive(self, participant: str, epoch: int) -> dict[str, int]:
        if participant not in self._participants:
            raise ValueError(
                f"unknown replay drain-barrier participant {participant!r}"
            )
        epoch = int(epoch)
        previous = self._last_epoch.get(participant, -1)
        if epoch != previous + 1:
            raise RuntimeError(
                f"replay drain-barrier participant {participant!r} arrived at epoch {epoch} "
                f"after epoch {previous}"
            )
        self._last_epoch[participant] = epoch
        arrivals = self._arrivals.setdefault(epoch, set())
        arrivals.add(participant)
        event = self._events.setdefault(epoch, asyncio.Event())
        if arrivals == self._participants:
            event.set()
        try:
            await asyncio.wait_for(event.wait(), timeout=self._timeout_s)
        except TimeoutError as error:
            missing = sorted(self._participants - arrivals)
            raise TimeoutError(
                f"replay drain barrier epoch {epoch} timed out; missing {missing}"
            ) from error
        return {"epoch": epoch, "participants": len(arrivals)}


@ray.remote(num_cpus=1)
class ReplayEngine:
    """Spawns one coordinator + this engine's ReplayReceiver actors (all on the rollout node)."""

    def init(
        self,
        rank_to_path: dict,
        port: int,
        sched,
        provider: str,
        iface: str,
        gantt_dir: str,
        engine_base: int,
        visible_devices: str,
        extra_env: dict | None = None,
        seed: bool = False,
    ) -> str:
        self.ipc = coordinator_ipc(port)
        self._coord = coordinator.spawn(self.ipc, port)
        env = _network_env_vars(provider, iface)
        env["CUDA_VISIBLE_DEVICES"] = visible_devices
        env["WBRIDGE_GANTT"] = "1" if gantt_dir else "0"
        if gantt_dir:
            env["WBRIDGE_GANTT_DIR"] = gantt_dir
        for k in ENGINE_ENV_KEYS:
            if os.environ.get(k) is not None:
                env[k] = os.environ[k]
        if extra_env:  # per-config overrides (sweep: cap, num_buf)
            env.update({k: str(v) for k, v in extra_env.items()})
        runtime_env = {"env_vars": env}
        ranks = sorted(rank_to_path)
        self._ranks = ranks
        self._workers = [
            ReplayReceiver.options(
                scheduling_strategy=sched, runtime_env=runtime_env
            ).remote()
            for _ in ranks
        ]
        # phys_dev = engine_base + rank: distinct GPUs per rank, with engines packed contiguously.
        ray.get(
            [
                w.init.remote(
                    rank_to_path[r],
                    self.ipc,
                    len(ranks),
                    provider,
                    iface,
                    engine_base + r,
                    seed,
                )
                for r, w in zip(ranks, self._workers)
            ]
        )
        return self.ipc

    def run_recv(
        self, k_updates: int, drain_barrier=None, engine_idx: int = 0
    ) -> list[int]:
        return ray.get(
            [
                w.run_recv.remote(
                    k_updates,
                    drain_barrier=drain_barrier,
                    participant=f"receiver:{engine_idx}:{rank}",
                )
                for rank, w in zip(self._ranks, self._workers)
            ]
        )

    def digests(self) -> list[dict]:
        return ray.get([w.digest.remote() for w in self._workers])

    def timings(self) -> list[dict]:
        return ray.get([w.timings.remote() for w in self._workers])

    def shutdown(self) -> None:
        """Kill this engine's receiver actors + coordinator so GPU arenas free between sweep configs."""
        for w in self._workers:
            try:
                ray.kill(w)
            except Exception:  # noqa: BLE001
                pass
        try:
            if getattr(self, "_coord", None) is not None:
                self._coord.terminate()
        except Exception:  # noqa: BLE001
            pass


@ray.remote(
    num_gpus=0, num_cpus=1
)  # see all GPUs; set_device manually (match the trainer device layout)
class ReplaySender(ReplaySenderBody):
    """Ray wrapper; every method is :class:`ReplaySenderBody`'s (shared with the Monarch front-end)."""


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--specs",
        required=True,
        help="dir of loadspec_*.pkl from WBRIDGE_DUMP_LOADSPEC",
    )
    ap.add_argument("--iters", type=int, default=4)
    ap.add_argument(
        "--network-provider",
        default=os.environ.get("WB_NETWORK_PROVIDER", "efa"),
        choices=["tcp", "efa"],
    )
    ap.add_argument(
        "--network-interface", default=os.environ.get("WB_NETWORK_INTERFACE", "")
    )
    ap.add_argument("--rollout-ip", default=os.environ.get("WB_ROLLOUT_IP"))
    ap.add_argument("--trainer-ip", default=os.environ.get("WB_TRAINER_IP"))
    ap.add_argument(
        "--base-port",
        type=int,
        default=62000,
        help="first coordinator port (one per engine)",
    )
    ap.add_argument(
        "--pg-port", type=int, default=62100, help="Gloo metadata rendezvous port"
    )
    ap.add_argument("--gantt-dir", default="")
    ap.add_argument(
        "--allow-cross-wt-overlap",
        action="store_true",
        help="disable the replay-only full-drain barrier and allow epoch N+1 to overlap epoch N drain",
    )
    ap.add_argument(
        "--seed",
        action="store_true",
        help="deterministic worker values + per-receiver post-consume digests (correctness)",
    )
    ap.add_argument(
        "--digest-out", default="", help="write the run summary + digests as JSON here"
    )
    ap.add_argument(
        "--label", default="mooncake", help="name for this configuration in the report"
    )
    a = ap.parse_args()

    recs = load_records(a.specs)
    print(summary(recs), flush=True)
    senders, engines = group_records(recs)
    ws = len(senders)
    rec_ws = next(iter(senders.values())).get("world_size")
    if rec_ws is not None and rec_ws != ws:
        print(
            f"WARN: captured world_size={rec_ws} but found {ws} sender specs",
            flush=True,
        )

    rollout_ip, trainer_ip, rollout_node, trainer_node = get_ray_nodes(
        a.rollout_ip, a.trainer_ip
    )
    roll_sched = NodeAffinitySchedulingStrategy(node_id=rollout_node, soft=False)
    train_sched = NodeAffinitySchedulingStrategy(node_id=trainer_node, soft=False)
    print(
        f"rollout={rollout_ip} trainer={trainer_ip} engines={len(engines)} senders={ws} "
        f"iters={a.iters} provider={a.network_provider}",
        flush=True,
    )

    # 1) Rollout engines (distinct ports, all on the rollout node). engine_base packs engines onto
    #    contiguous GPUs so each receiver selects a distinct device for cross-device exchange.
    eids = list(engines.keys())
    ports = [a.base_port + i for i in range(len(eids))]
    bases, _acc = [], 0
    for eid in eids:
        bases.append(_acc)
        _acc += len(engines[eid])
    rollout_visible_devices = visible_device_list(_acc)
    engine_actors = [
        ReplayEngine.options(scheduling_strategy=roll_sched).remote() for _ in eids
    ]
    ray.get(
        [
            ea.init.remote(
                {r: engines[eid][r]["_path"] for r in engines[eid]},
                port,
                roll_sched,
                a.network_provider,
                a.network_interface,
                a.gantt_dir,
                base,
                rollout_visible_devices,
                None,
                a.seed,
            )
            for ea, eid, port, base in zip(engine_actors, eids, ports, bases)
        ]
    )
    receiver_urls = [f"tcp://{rollout_ip}:{p}" for p in ports]

    # 2) Trainer senders (on the trainer node).
    tenv = _network_env_vars(a.network_provider, a.network_interface)
    tenv["CUDA_VISIBLE_DEVICES"] = visible_device_list(ws)
    if a.gantt_dir:
        tenv.update({"WBRIDGE_GANTT": "1", "WBRIDGE_GANTT_DIR": a.gantt_dir})
    for k in SENDER_ENV_KEYS:
        if os.environ.get(k) is not None:
            tenv[k] = os.environ[k]
    truntime = {"env_vars": tenv}
    sender_actors = []
    for r in sorted(senders):
        args = SenderArgs(
            world_size=ws,
            receiver_urls=receiver_urls,
            master_addr=trainer_ip,
            master_port=a.pg_port,
            protocol=("efa" if a.network_provider == "efa" else "tcp"),
            sender_staging=bool(senders[r].get("sender_staging")),
        )
        s = ReplaySender.options(
            scheduling_strategy=train_sched, runtime_env=truntime
        ).remote()
        sender_actors.append((r, s, args))
    ray.get(
        [
            s.init.remote(
                senders[r]["_path"],
                args,
                a.network_provider,
                a.network_interface,
                r,
                a.seed,
            )
            for r, s, args in sender_actors
        ]
    )

    # 3) Run K transfers: receivers poll (drive connect + K receives); senders connect + K send/wait.
    participant_ids = [f"sender:{rank}" for rank in sorted(senders)] + [
        f"receiver:{engine_idx}:{rank}"
        for engine_idx, eid in enumerate(eids)
        for rank in sorted(engines[eid])
    ]
    drain_barrier = None
    if not a.allow_cross_wt_overlap:
        drain_barrier = ReplayDrainBarrier.remote(participant_ids)
        print(
            f"replay full-drain barrier: {len(participant_ids)} participants",
            flush=True,
        )
    recv_futs = [
        ea.run_recv.remote(a.iters, drain_barrier=drain_barrier, engine_idx=engine_idx)
        for engine_idx, ea in enumerate(engine_actors)
    ]
    send_futs = [
        s.run_send.remote(
            a.iters, drain_barrier=drain_barrier, participant=f"sender:{rank}"
        )
        for rank, s, _ in sender_actors
    ]
    send_res = ray.get(send_futs)
    ray.get(recv_futs)

    # 4) Report. Digests are collected only after the timed loop, so hashing never lands in a WTT.
    digests = None
    if a.seed:
        digests = []
        for i, ea in enumerate(engine_actors):
            digests += [
                dict(d, key=f"e{i}/r{d['rank']}") for d in ray.get(ea.digests.remote())
            ]
    report_run(
        a.label,
        send_res,
        digests,
        a.iters,
        a.digest_out,
        extra=f"{len(eids)} engines x {ws} senders",
    )
    if a.gantt_dir:
        print(f"gantt jsonl -> {a.gantt_dir}", flush=True)


if __name__ == "__main__":
    main()
