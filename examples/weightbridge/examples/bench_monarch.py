# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Frameworkless replay of a captured 30B weight transfer, on **Monarch** actors + the Monarch RDMA
transport — the counterpart of :mod:`bench_transfer` (Ray + Mooncake).

Same captured layout, same bodies (:mod:`bench_bodies`), same report (:mod:`bench_report`); only the
process layer and the transport differ. That is the point: with ``--seed`` both front-ends digest each
receiver's post-consume weights, so ``bench_report.compare_digests`` can assert Monarch delivered
byte-identical bytes through the real 30B sharding, dedup exchange and consume path — and the warm WTTs
sit side by side on identical work.

Monarch RDMA is only reachable from inside a Monarch actor (``RDMABuffer`` and ``RDMAAction.submit``
both resolve ``context().actor_instance``), so a Ray actor cannot host it and this second front-end is
required rather than convenient.

Placement mirrors ``bench_transfer``: all rollout engines on the rollout node (so cross-engine dedup
exchange takes NVLink), all senders on the trainer node, each rank on its own GPU.

"""

from __future__ import annotations

import argparse
import os

from bench_bodies import ReplayReceiverBody, ReplaySenderBody
from bench_report import report_run
from loadspec_replay import group_records, load_records, summary
from monarch.actor import Actor, endpoint
from monarch_common import ActorThread, attach_hosts, bind_gpu, host_index, my_rank
from wbridge.backend import coordinator
from wbridge.backend.control_channel import coordinator_ipc
from wbridge.backend.sender import SenderArgs


class BenchReceiverActor(Actor):
    """One captured rollout rank. All work is dispatched off the actor's event loop — see ActorThread."""

    def __init__(self) -> None:
        self._b = ReplayReceiverBody()
        self._t = ActorThread("wb-bench-recv")

    @endpoint
    async def init(
        self,
        rank_to_path: dict,
        ipc: str,
        provider: str,
        iface: str,
        engine_base: int,
        seed: bool,
    ) -> int:
        # ONE broadcast to the whole mesh: the receiver's ControlChannel constructor is a rendezvous
        # across the engine's ranks, so initializing them one at a time deadlocks on rank 0.
        rank = my_rank()
        path = rank_to_path[rank]
        dev = (
            engine_base + rank
        )  # distinct physical GPU per rank -> cross-device dedup exchange
        await self._t.run(bind_gpu, dev)
        return await self._t.run(
            self._b.init, path, ipc, len(rank_to_path), provider, iface, dev, seed
        )

    @endpoint
    async def run_recv(self, k_updates: int) -> int:
        return await self._t.run(self._b.run_recv, k_updates)

    @endpoint
    async def digest(self) -> dict:
        return await self._t.run(self._b.digest)


class BenchSenderActor(Actor):
    def __init__(self) -> None:
        self._b = ReplaySenderBody()
        self._t = ActorThread("wb-bench-send")

    @endpoint
    async def init(
        self,
        rank_to_path: dict,
        args: SenderArgs,
        provider: str,
        iface: str,
        seed: bool,
    ) -> int:
        rank = my_rank()
        await self._t.run(bind_gpu, rank)
        return await self._t.run(
            self._b.init, rank_to_path[rank], args, provider, iface, rank, seed
        )

    @endpoint
    async def run_send(self, k_updates: int):
        return await self._t.run(self._b.run_send, k_updates)


class BenchCoordinatorActor(Actor):
    """Runs one engine's ZMQ coordinator subprocess; must share a machine with that engine's rank 0."""

    def __init__(self) -> None:
        self._procs: list = []

    @endpoint
    async def spawn(self, ipcs: list[str], ports: list[int]) -> str:
        import socket

        for ipc, port in zip(ipcs, ports):
            self._procs.append(coordinator.spawn(ipc, port))
        return f"{len(ipcs)} coordinators on {socket.gethostname()} ports {ports}"


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--specs",
        required=True,
        help="dir of loadspec_*.pkl from WBRIDGE_DUMP_LOADSPEC",
    )
    ap.add_argument("--iters", type=int, default=6)
    ap.add_argument(
        "--monarch-workers",
        required=True,
        default=os.environ.get("WORKER_ADDRS", ""),
        help="comma-separated tcp://ip:port of the already-serving Monarch worker loops",
    )
    ap.add_argument(
        "--network-provider",
        default=os.environ.get("WB_NETWORK_PROVIDER", "efa"),
        choices=["tcp", "efa"],
    )
    ap.add_argument(
        "--network-interface", default=os.environ.get("WB_NETWORK_INTERFACE", "")
    )
    ap.add_argument("--rollout-ip", required=True)
    ap.add_argument("--trainer-ip", required=True)
    ap.add_argument(
        "--base-port",
        type=int,
        default=62000,
        help="first coordinator port (one per engine)",
    )
    ap.add_argument(
        "--pg-port", type=int, default=62100, help="Gloo metadata rendezvous port"
    )
    ap.add_argument(
        "--gpus-per-node",
        type=int,
        default=int(os.environ.get("WB_GPUS_PER_NODE", "0")),
        help="optional capacity check for each host; 0 lets Monarch validate the requested meshes",
    )
    ap.add_argument(
        "--seed",
        action="store_true",
        help="deterministic worker values + per-receiver post-consume digests (correctness)",
    )
    ap.add_argument("--digest-out", default="")
    ap.add_argument("--label", default="monarch")
    a = ap.parse_args()

    recs = load_records(a.specs)
    print(summary(recs), flush=True)
    senders, engines = group_records(recs)
    ws = len(senders)
    eids = list(engines.keys())
    if sorted(senders) != list(range(ws)):
        raise RuntimeError(
            f"sender ranks must be 0..{ws - 1} to map onto a proc mesh, got {sorted(senders)}"
        )

    addrs = [w.strip() for w in a.monarch_workers.split(",") if w.strip()]
    hosts = attach_hosts(addrs, name="wbbench")
    t_idx, r_idx = host_index(addrs, a.trainer_ip), host_index(addrs, a.rollout_ip)
    print(
        f"rollout={a.rollout_ip}(slice {r_idx}) trainer={a.trainer_ip}(slice {t_idx}) "
        f"engines={len(eids)} senders={ws} iters={a.iters} provider={a.network_provider}",
        flush=True,
    )

    # 1) One coordinator per engine, all on the rollout node (each engine's rank 0 dials its own).
    ports = [a.base_port + i for i in range(len(eids))]
    ipcs = [coordinator_ipc(p) for p in ports]
    c_procs = hosts.slice(hosts=r_idx).spawn_procs({"gpus": 1})
    c_procs.initialized.get()
    coord = c_procs.spawn("wb_bench_coord", BenchCoordinatorActor)
    coord.initialized.get()
    print("[monarch] " + coord.spawn.call_one(ipcs=ipcs, ports=ports).get(), flush=True)

    # 2) Rollout engines: one proc mesh per engine, packed onto contiguous GPUs so every receiver owns a
    #    distinct device — and therefore, under Monarch, a distinct NIC.
    engine_meshes, base, _acc = [], [], 0
    for eid in eids:
        base.append(_acc)
        _acc += len(engines[eid])
    if a.gpus_per_node and _acc > a.gpus_per_node:
        raise RuntimeError(
            f"{_acc} receiver ranks do not fit on a {a.gpus_per_node}-GPU rollout node"
        )
    for i, eid in enumerate(eids):
        ranks = engines[eid]
        procs = hosts.slice(hosts=r_idx).spawn_procs({"gpus": len(ranks)})
        procs.initialized.get()
        mesh = procs.spawn(f"wb_bench_recv{i}", BenchReceiverActor)
        mesh.initialized.get()
        mesh.init.call(
            rank_to_path={r: ranks[r]["_path"] for r in ranks},
            ipc=ipcs[i],
            provider=a.network_provider,
            iface=a.network_interface,
            engine_base=base[i],
            seed=a.seed,
        ).get()
        engine_meshes.append(mesh)
        print(
            f"[monarch] engine {i}: {len(ranks)} receivers ready (gpus {base[i]}..{base[i] + len(ranks) - 1})",
            flush=True,
        )
    receiver_urls = [f"tcp://{a.rollout_ip}:{p}" for p in ports]

    # 3) Trainer senders on the trainer node, one per captured rank.
    if a.gpus_per_node and ws > a.gpus_per_node:
        raise RuntimeError(
            f"{ws} sender ranks do not fit on a {a.gpus_per_node}-GPU trainer node"
        )
    s_procs = hosts.slice(hosts=t_idx).spawn_procs({"gpus": ws})
    s_procs.initialized.get()
    smesh = s_procs.spawn("wb_bench_send", BenchSenderActor)
    smesh.initialized.get()
    sender_args = SenderArgs(
        world_size=ws,
        receiver_urls=receiver_urls,
        master_addr=a.trainer_ip,
        master_port=a.pg_port,
        protocol="monarch",
        sender_staging=False,  # protocol='monarch' has no staging path (rdma/monarch.py refuses it)
    )
    smesh.init.call(
        rank_to_path={r: senders[r]["_path"] for r in senders},
        args=sender_args,
        provider=a.network_provider,
        iface=a.network_interface,
        seed=a.seed,
    ).get()
    print(f"[monarch] {ws} senders ready", flush=True)

    # 4) Run K transfers. Receivers poll while the senders drive, exactly as on the Ray path.
    recv = [m.run_recv.call(k_updates=a.iters) for m in engine_meshes]
    send = smesh.run_send.call(k_updates=a.iters)
    send_res = [v for _, v in send.get()]
    for f in recv:
        f.get()

    # 5) Report. Digests come after the timed loop so hashing never lands in a WTT.
    digests = None
    if a.seed:
        digests = []
        for i, m in enumerate(engine_meshes):
            digests += [
                dict(d, key=f"e{i}/r{d['rank']}") for _, d in m.digest.call().get()
            ]
    report_run(
        a.label,
        send_res,
        digests,
        a.iters,
        a.digest_out,
        extra=f"{len(eids)} engines x {ws} senders",
    )


if __name__ == "__main__":
    main()
