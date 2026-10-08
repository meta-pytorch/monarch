# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Sweep round-count (via WBRIDGE_ROUND_CAP_BYTES) x sender double-buffering (WBRIDGE_SENDER_NUM_BUF)
using the frameworkless replay, all in ONE Ray session.

For each (cap, num_buf) config it spins up fresh sender+engine actors (distinct ports), connects (which
plans the round partition for that cap), runs K timed transfers, records the actual round count + warm
WTT, then tears the actors down (freeing GPU arenas) before the next config. Prints a rounds x num_buf
WTT table.

"""

from __future__ import annotations

import argparse
import os
import statistics
import time

import ray
import torch  # noqa: F401
from bench_transfer import _network_env_vars, ReplayEngine, ReplaySender
from loadspec_replay import group_records, load_records, summary
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from utils import get_ray_nodes, visible_device_list
from wbridge.backend.sender import SenderArgs


def run_config(
    senders,
    engines,
    ws,
    rollout_ip,
    trainer_ip,
    roll_sched,
    train_sched,
    provider,
    iface,
    cap,
    num_buf,
    iters,
    base_port,
    pg_port,
):
    """One (cap, num_buf) point: build fresh actors, connect+run, return (rounds, warm_wtts). Tears down."""
    eids = list(engines.keys())
    ports = [base_port + i for i in range(len(eids))]
    bases, acc = [], 0
    for eid in eids:
        bases.append(acc)
        acc += len(engines[eid])
    rollout_visible_devices = visible_device_list(acc)
    # Constant-across-sweep planning flags; direct-async control publication has no runtime switch.
    pipe = {
        k: os.environ[k]
        for k in (
            "WBRIDGE_RECV_PIPELINE",
            "WBRIDGE_DEDUP_PAIR_BYTES",
        )
        if os.environ.get(k)
    }
    extra = {
        "WBRIDGE_ROUND_CAP_BYTES": str(cap),
        "WBRIDGE_SENDER_NUM_BUF": str(num_buf),
        **pipe,
    }

    engine_actors = [
        ReplayEngine.options(scheduling_strategy=roll_sched).remote() for _ in eids
    ]
    ray.get(
        [
            ea.init.remote(
                {r: engines[eid][r]["_path"] for r in engines[eid]},
                port,
                roll_sched,
                provider,
                iface,
                "",
                base,
                rollout_visible_devices,
                extra,
            )
            for ea, eid, port, base in zip(engine_actors, eids, ports, bases)
        ]
    )
    receiver_urls = [f"tcp://{rollout_ip}:{p}" for p in ports]

    tenv = _network_env_vars(provider, iface)
    tenv["CUDA_VISIBLE_DEVICES"] = visible_device_list(ws)
    tenv["WBRIDGE_ROUND_CAP_BYTES"] = str(cap)
    tenv["WBRIDGE_SENDER_NUM_BUF"] = str(num_buf)
    tenv.update(pipe)  # sender needs WBRIDGE_RECV_PIPELINE for the depth-back await
    truntime = {"env_vars": tenv}
    sender_actors = []
    for r in sorted(senders):
        args = SenderArgs(
            world_size=ws,
            receiver_urls=receiver_urls,
            master_addr=trainer_ip,
            master_port=pg_port,
            protocol=("efa" if provider == "efa" else "tcp"),
            sender_staging=bool(senders[r].get("sender_staging")),
        )
        s = ReplaySender.options(
            scheduling_strategy=train_sched, runtime_env=truntime
        ).remote()
        sender_actors.append((r, s, args))
    ray.get(
        [
            s.init.remote(senders[r]["_path"], args, provider, iface, r)
            for r, s, args in sender_actors
        ]
    )

    recv_futs = [ea.run_recv.remote(iters) for ea in engine_actors]
    send_futs = [s.run_send.remote(iters) for _, s, _ in sender_actors]
    send_res = ray.get(send_futs)
    ray.get(recv_futs)
    send_res.sort(key=lambda x: x[0])
    _, _connect_s, wtts, nr = send_res[0]
    # Each fresh actor set needs a few transfers to reach steady state (CUDA graph / pool / QP warmup),
    # so take the warmest tail rather than everything-after-WT0.
    warm = wtts[-3:] if len(wtts) >= 4 else (wtts[1:] or wtts)

    # Teardown: free GPU arenas before the next config.
    for _, s, _ in sender_actors:
        try:
            ray.kill(s)
        except Exception:  # noqa: BLE001
            pass
    ray.get([ea.shutdown.remote() for ea in engine_actors])
    for ea in engine_actors:
        try:
            ray.kill(ea)
        except Exception:  # noqa: BLE001
            pass
    time.sleep(5)
    return nr, warm


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--specs", required=True)
    ap.add_argument("--iters", type=int, default=4)
    ap.add_argument(
        "--caps",
        required=True,
        help="comma-separated round-cap byte values (one per target round count)",
    )
    ap.add_argument(
        "--numbuf", default="1,2", help="comma-separated sender NUM_BUF values"
    )
    ap.add_argument(
        "--network-provider",
        default=os.environ.get("WB_NETWORK_PROVIDER", "efa"),
        choices=["tcp", "efa"],
    )
    ap.add_argument(
        "--network-interface", default=os.environ.get("WB_NETWORK_INTERFACE", "")
    )
    ap.add_argument("--base-port", type=int, default=62000)
    ap.add_argument("--pg-port", type=int, default=62100)
    a = ap.parse_args()

    recs = load_records(a.specs)
    print(summary(recs), flush=True)
    senders, engines = group_records(recs)
    ws = len(senders)
    rollout_ip, trainer_ip, rollout_node, trainer_node = get_ray_nodes(None, None)
    roll_sched = NodeAffinitySchedulingStrategy(node_id=rollout_node, soft=False)
    train_sched = NodeAffinitySchedulingStrategy(node_id=trainer_node, soft=False)
    caps = [int(c) for c in a.caps.split(",")]
    numbufs = [int(n) for n in a.numbuf.split(",")]
    print(
        f"rollout={rollout_ip} trainer={trainer_ip} engines={len(engines)} senders={ws} "
        f"iters={a.iters} caps={len(caps)} numbuf={numbufs}",
        flush=True,
    )

    results = []  # (num_buf, rounds, cap, median, min, warm)
    cfg = 0
    for nb in numbufs:
        for cap in caps:
            bp = a.base_port + cfg * 20
            pgp = a.pg_port + cfg
            print(
                f"\n### config {cfg}: num_buf={nb} cap={cap / 2**30:.2f}GiB ports={bp}/{pgp} ###",
                flush=True,
            )
            try:
                nr, warm = run_config(
                    senders,
                    engines,
                    ws,
                    rollout_ip,
                    trainer_ip,
                    roll_sched,
                    train_sched,
                    a.network_provider,
                    a.network_interface,
                    cap,
                    nb,
                    a.iters,
                    bp,
                    pgp,
                )
                med, mn = statistics.median(warm), min(warm)
                print(
                    f"   -> rounds={nr} warm_median={med:.3f}s warm_min={mn:.3f}s "
                    f"warm={['%.3f' % w for w in warm]}",
                    flush=True,
                )
                results.append((nb, nr, cap, med, mn, warm))
            except Exception as e:  # noqa: BLE001
                print(f"   !! failed: {e}", flush=True)
                results.append((nb, -1, cap, float("nan"), float("nan"), []))
            cfg += 1

    # Table: rounds x num_buf -> warm-median WTT.
    print(
        "\n=== SWEEP: warm-median WTT (s) — rounds x sender double-buffering ===",
        flush=True,
    )
    by = {}
    for nb, nr, cap, med, mn, warm in results:
        by.setdefault(nr, {})[nb] = (med, mn)
    hdr = "rounds | " + " | ".join(f"NUM_BUF={nb} (med/min)" for nb in numbufs)
    print(hdr, flush=True)
    for nr in sorted(k for k in by if k > 0):
        row = f"{nr:>6} | "
        row += " | ".join(
            (f"{by[nr][nb][0]:.3f}/{by[nr][nb][1]:.3f}" if nb in by[nr] else "   -   ")
            for nb in numbufs
        )
        print(row, flush=True)
    print("\nraw (num_buf, rounds, cap_bytes, median_s, min_s):", flush=True)
    for nb, nr, cap, med, mn, warm in results:
        print(
            f"  nb={nb} rounds={nr} cap={cap} median={med:.3f} min={mn:.3f}", flush=True
        )


if __name__ == "__main__":
    main()
