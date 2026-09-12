# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Frameworkless replay of a 4-node 30B capture (16 senders across 2 trainer nodes, 16 receivers = 4
physical engines across 2 rollout nodes, matching the real disagg run that produced the loadspecs).

This is a synthetic workload: it replays the captured tensor layout and placement but does not execute
model training or generation. Tensor values are uninitialized for performance-only runs; ``--seed``
uses deterministic synthetic values so receivers can verify post-transfer digests.

bench_transfer.py packs everything onto ONE trainer + ONE rollout node (<=8 GPUs/side); this variant
spreads it across a 4-node lease (32 GPUs).

Engine grouping subtlety: the captured ``engine_id`` is a node-local coordinator socket
(``coordinator_ipc(<port>)``) that COLLIDES across rollout nodes — two physical engines on
different nodes share it. ``loadspec_replay.group_records`` keys purely on engine_id and would merge
them, silently dropping half the receivers. Here we split each engine_id into physical engines by
pid-clustering (one SGLang scheduler spawns its workers with clustered pids), then co-locate the engines
that shared a node in the real run (lowest-pid cluster -> rollout node 0, etc.) so same-node
cross-engine dedup exchange takes NVLink and cross-node takes RDMA — as in the real run.

"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
from collections import defaultdict

import ray
from bench_report import SENDER_ENV_KEYS
from bench_transfer import ReplayDrainBarrier, ReplayEngine, ReplaySender
from loadspec_replay import load_records, summary
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from utils import network_env_vars, visible_device_list
from wbridge.backend.sender import SenderArgs


def _pid_of(rec: dict) -> int:
    m = re.search(r"_pid(\d+)", rec.get("_path", ""))
    return int(m.group(1)) if m else 0


def physical_engines(recs: list[dict]) -> list[dict[int, dict]]:
    """Split receivers into PHYSICAL engines (list of {rank: rec}).

    engine_id collides across nodes, so within each engine_id we sort by pid and chunk into
    num_workers-sized, rank-complete groups — each chunk is one physical engine on one node. The result
    is sorted by each engine's min pid so co-located engines (same pid cluster == same node) are adjacent.
    """
    by_eid: dict[object, list[dict]] = defaultdict(list)
    for r in recs:
        if r.get("role") == "receiver":
            by_eid[r["engine_id"]].append(r)
    phys: list[dict[int, dict]] = []
    for eid in sorted(by_eid, key=str):
        rs = sorted(by_eid[eid], key=_pid_of)
        nw = rs[0].get("num_workers") or 4
        for i in range(0, len(rs), nw):
            chunk = rs[i : i + nw]
            eng = {r["rank"]: r for r in chunk}
            if len(eng) != len(chunk):
                raise RuntimeError(
                    f"engine split failed for {eid!r}: chunk ranks "
                    f"{sorted(r['rank'] for r in chunk)} (pid clustering ambiguous)"
                )
            phys.append(eng)
    phys.sort(
        key=lambda e: min(_pid_of(r) for r in e.values())
    )  # cluster by node (pid range)
    return phys


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--specs", required=True)
    ap.add_argument("--iters", type=int, default=6)
    ap.add_argument(
        "--network-provider",
        default=os.environ.get("WB_NETWORK_PROVIDER", "efa"),
        choices=["tcp", "efa"],
    )
    ap.add_argument(
        "--network-interface", default=os.environ.get("WB_NETWORK_INTERFACE", "")
    )
    ap.add_argument(
        "--gpus-per-node",
        type=int,
        default=int(os.environ.get("WB_GPUS_PER_NODE", "8")),
    )
    ap.add_argument(
        "--engines-per-rollout-node",
        type=int,
        default=int(os.environ.get("WB_ENGINES_PER_ROLLOUT_NODE", "2")),
    )
    ap.add_argument(
        "--rollout-dp",
        type=int,
        default=0,
        help=(
            "number of physical rollout engines to replay; 0 uses every captured engine. "
            "When larger than the capture, engine templates are repeated cyclically (A,B,A,B,...) "
            "with distinct actors/coordinators/global ranks"
        ),
    )
    ap.add_argument("--base-port", type=int, default=62000)
    ap.add_argument("--pg-port", type=int, default=62100)
    ap.add_argument("--gantt-dir", default="")
    ap.add_argument(
        "--allow-cross-wt-overlap",
        action="store_true",
        help="disable the replay-only full-drain barrier and allow epoch N+1 to overlap epoch N drain",
    )
    ap.add_argument(
        "--metrics-out",
        default="",
        help="optional JSON output for trainer block, rollout block, and E2E WTT",
    )
    ap.add_argument(
        "--seed",
        action="store_true",
        help="deterministic sender/receiver values for post-consume correctness digests",
    )
    ap.add_argument(
        "--digest-out",
        default="",
        help="optional JSON output containing every physical receiver digest",
    )
    ap.add_argument(
        "--discard-first",
        type=int,
        default=2,
        help="cold replay iterations excluded from the three-metric summary",
    )
    a = ap.parse_args()

    recs = load_records(a.specs)
    print(summary(recs), flush=True)
    senders = {r["rank"]: r for r in recs if r.get("role") == "sender"}
    ws = len(senders)
    captured_engs = physical_engines(
        recs
    )  # list of {rank: rec}, one per captured PHYSICAL engine
    if a.rollout_dp < 0:
        raise ValueError(f"--rollout-dp must be >= 0, got {a.rollout_dp}")
    if a.rollout_dp:
        if not captured_engs:
            raise RuntimeError(
                "--rollout-dp requires at least one captured rollout engine"
            )
        # A replay engine only reads the capture files. Reusing its rank->path mapping is therefore an exact
        # logical duplicate, while the actors/coordinator port and the WeightBridge global ranks allocated
        # below remain independent. Cycle all captured templates before repeating the first one so DP<=capture
        # retains the captured physical-engine ordering.
        engs = [captured_engs[i % len(captured_engs)] for i in range(a.rollout_dp)]
    else:
        engs = captured_engs
    n_engines = len(engs)
    gpn, epn = a.gpus_per_node, a.engines_per_rollout_node
    n_roll_nodes = (n_engines + epn - 1) // epn
    n_train_nodes = (ws + gpn - 1) // gpn
    print(
        f"physical engines={n_engines} (captured={len(captured_engs)}, "
        f"templates={[i % len(captured_engs) for i in range(n_engines)] if captured_engs else []}, "
        f"ranks/engine={[len(e) for e in engs]}); "
        f"need {n_roll_nodes} rollout + {n_train_nodes} trainer nodes",
        flush=True,
    )

    ray.init(address="auto", ignore_reinit_error=True)
    alive = sorted(
        (n for n in ray.nodes() if n["Alive"]),
        key=lambda n: str(n["NodeManagerAddress"]),
    )
    need = n_roll_nodes + n_train_nodes
    if len(alive) < need:
        raise RuntimeError(f"need {need} alive Ray nodes, have {len(alive)}")
    roll_nodes = alive[:n_roll_nodes]
    train_nodes = alive[n_roll_nodes : n_roll_nodes + n_train_nodes]
    roll_ips = [str(n["NodeManagerAddress"]) for n in roll_nodes]
    train_ips = [str(n["NodeManagerAddress"]) for n in train_nodes]
    print(
        f"rollout nodes={roll_ips} trainer nodes={train_ips} iters={a.iters} "
        f"provider={a.network_provider}",
        flush=True,
    )

    # 1) Rollout engines: engine i -> roll_nodes[i//epn]; engine_base=(i%epn)*ranks so a node's engines
    #    pack onto contiguous GPUs. Each engine has a unique global port.
    ports = [a.base_port + i for i in range(n_engines)]
    node_idx = [i // epn for i in range(n_engines)]
    bases = [(i % epn) * len(engs[i]) for i in range(n_engines)]
    roll_scheds = [
        NodeAffinitySchedulingStrategy(
            node_id=str(roll_nodes[node_idx[i]]["NodeID"]), soft=False
        )
        for i in range(n_engines)
    ]
    engine_actors = [
        ReplayEngine.options(scheduling_strategy=roll_scheds[i]).remote()
        for i in range(n_engines)
    ]
    rollout_visible_devices = visible_device_list(gpn)
    ray.get(
        [
            engine_actors[i].init.remote(
                {r: engs[i][r]["_path"] for r in engs[i]},
                ports[i],
                roll_scheds[i],
                a.network_provider,
                a.network_interface,
                a.gantt_dir,
                bases[i],
                rollout_visible_devices,
                None,
                a.seed,
            )
            for i in range(n_engines)
        ]
    )
    receiver_urls = [
        f"tcp://{roll_ips[node_idx[i]]}:{ports[i]}" for i in range(n_engines)
    ]

    # 2) Trainer senders: rank r -> train_nodes[r//gpn], phys_dev r%gpn. Gloo PG rendezvous at trainer 0.
    sender_actors = []
    for r in sorted(senders):
        tsched = NodeAffinitySchedulingStrategy(
            node_id=str(train_nodes[r // gpn]["NodeID"]), soft=False
        )
        args = SenderArgs(
            world_size=ws,
            receiver_urls=receiver_urls,
            master_addr=train_ips[0],
            master_port=a.pg_port,
            protocol=("efa" if a.network_provider == "efa" else "tcp"),
            sender_staging=bool(senders[r].get("sender_staging")),
        )
        sender_options = {"scheduling_strategy": tsched}
        sender_env = network_env_vars(a.network_provider, a.network_interface)
        sender_env["CUDA_VISIBLE_DEVICES"] = visible_device_list(gpn)
        sender_env.update(
            {
                key: os.environ[key]
                for key in SENDER_ENV_KEYS
                if os.environ.get(key) is not None
            }
        )
        if a.gantt_dir:
            # Gantt is read when wbridge is imported in the Ray worker, before ReplaySender.init().
            sender_env.update({"WBRIDGE_GANTT": "1", "WBRIDGE_GANTT_DIR": a.gantt_dir})
        if sender_env:
            sender_options["runtime_env"] = {"env_vars": sender_env}
        s = ReplaySender.options(**sender_options).remote()
        sender_actors.append((r, s, args))
    ray.get(
        [
            s.init.remote(
                senders[r]["_path"],
                args,
                a.network_provider,
                a.network_interface,
                r % gpn,
                a.seed,
            )
            for r, s, args in sender_actors
        ]
    )

    # 3) Run K transfers.
    participant_ids = [f"sender:{rank}" for rank in sorted(senders)] + [
        f"receiver:{engine_idx}:{rank}"
        for engine_idx, engine in enumerate(engs)
        for rank in sorted(engine)
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

    # Collect timestamps after the timed loops to keep file I/O and Ray RPCs off the transfer path:
    # trainer block ends when the pack-safe CUDA event fires;
    # rollout block spans poll_requests(); E2E runs from trainer-rank-0 send_start to the slowest
    # receiver's consume_end across both engines.
    sender_timings = ray.get([s.timings.remote() for _, s, _ in sender_actors])
    sender_plan_stats = ray.get([s.plan_stats.remote() for _, s, _ in sender_actors])
    engine_timings = ray.get([ea.timings.remote() for ea in engine_actors])
    digests: dict[str, str] = {}
    if a.seed:
        for engine_idx, engine in enumerate(engine_actors):
            for item in ray.get(engine.digests.remote()):
                key = f"e{engine_idx}/r{item['rank']}"
                if key in digests:
                    raise RuntimeError(f"duplicate receiver digest key {key}")
                digests[key] = item["digest"]
        print(f"receiver digests: {len(digests)} physical ranks", flush=True)

    # 4) Report timing.  Trainer blocking is the union of every physical trainer worker's
    # WeightBridge window: first send() entry through last pack-handoff completion.  Reporting only
    # rank 0 hides uneven sharding/packing work and is not comparable with a multi-worker run.
    send_res.sort(key=lambda x: x[0])
    rank0, connect_s, wtts, nr = send_res[0]
    print(
        f"\n=== replay WTT (rank {rank0}, {nr} rounds, {n_engines} engines x {ws} senders) ===",
        flush=True,
    )
    print(f"connect (cold setup): {connect_s:.3f} s", flush=True)
    for i, w in enumerate(wtts):
        print(f"  WT{i}: {w:.3f} s", flush=True)
    warm = wtts[1:] or wtts
    print(
        f"warm: min={min(warm):.3f} median={statistics.median(warm):.3f} mean={statistics.mean(warm):.3f} s",
        flush=True,
    )

    by_sender_rank = {x["rank"]: x["epochs"] for x in sender_timings}
    plan_by_sender_rank = {x["rank"]: x for x in sender_plan_stats}
    if 0 not in by_sender_rank:
        raise RuntimeError(
            f"replay timing is missing trainer rank 0: {sorted(by_sender_rank)}"
        )
    rank0_epochs = by_sender_rank[0]
    if len(rank0_epochs) != a.iters:
        raise RuntimeError(
            f"trainer rank 0 timing count {len(rank0_epochs)} != iters {a.iters}"
        )
    for sender_rank, epochs in sorted(by_sender_rank.items()):
        if len(epochs) != a.iters:
            raise RuntimeError(
                f"trainer rank {sender_rank} timing count {len(epochs)} != iters {a.iters}"
            )
    for engine_idx, ranks in enumerate(engine_timings):
        if len(ranks) != len(engs[engine_idx]):
            raise RuntimeError(
                f"engine {engine_idx}: timing ranks {len(ranks)} != captured ranks {len(engs[engine_idx])}"
            )
        for rank_timing in ranks:
            if len(rank_timing["epochs"]) != a.iters:
                raise RuntimeError(
                    f"engine {engine_idx} rank {rank_timing['rank']}: timing count "
                    f"{len(rank_timing['epochs'])} != iters {a.iters}"
                )

    rows = []
    for epoch in range(a.iters):
        trainer = rank0_epochs[epoch]
        trainer_workers = {
            rank: epochs[epoch] for rank, epochs in sorted(by_sender_rank.items())
        }
        trainer_rank_endpoints = {
            str(rank): {
                "start_ns": worker["send_start_ns"],
                "end_ns": worker["trainer_end_ns"],
                "duration_ns": worker["trainer_block_ns"],
                "full_delivery_end_ns": worker["local_complete_ns"],
            }
            for rank, worker in trainer_workers.items()
        }
        trainer_first_rank, trainer_first = min(
            trainer_workers.items(), key=lambda item: item[1]["send_start_ns"]
        )
        trainer_last_rank, trainer_last = max(
            trainer_workers.items(), key=lambda item: item[1]["trainer_end_ns"]
        )
        engine_blocks = {}
        engine_consumed = {}
        engine_endpoints = {}
        for engine_idx, ranks in enumerate(engine_timings):
            spans = [rank_timing["epochs"][epoch] for rank_timing in ranks]
            rank_spans = {
                str(rank_timing["rank"]): rank_timing["epochs"][epoch]
                for rank_timing in ranks
            }
            first_rank, first_span = min(
                rank_spans.items(), key=lambda item: item[1]["block_start_ns"]
            )
            last_rank, last_span = max(
                rank_spans.items(), key=lambda item: item[1]["block_end_ns"]
            )
            engine_start_ns = first_span["block_start_ns"]
            engine_end_ns = last_span["block_end_ns"]
            engine_key = str(engine_idx)
            engine_blocks[engine_key] = (engine_end_ns - engine_start_ns) / 1e6
            engine_consumed[engine_key] = max(span["consume_end_ns"] for span in spans)
            engine_endpoints[engine_key] = {
                "start_ns": engine_start_ns,
                "end_ns": engine_end_ns,
                "duration_ns": engine_end_ns - engine_start_ns,
                "first_rank": int(first_rank),
                "last_rank": int(last_rank),
                "rank_endpoints": {
                    rank: {
                        "start_ns": span["block_start_ns"],
                        "consume_end_ns": span["consume_end_ns"],
                        "end_ns": span["block_end_ns"],
                        "duration_ns": span["block_duration_ns"],
                    }
                    for rank, span in sorted(
                        rank_spans.items(), key=lambda item: int(item[0])
                    )
                },
            }
        row = {
            "epoch": epoch,
            "trainer_block_ms": (
                trainer_last["trainer_end_ns"] - trainer_first["send_start_ns"]
            )
            / 1e6,
            "trainer_rank0_block_ms": trainer["trainer_block_ns"] / 1e6,
            "trainer_interval": {
                "start_ns": trainer_first["send_start_ns"],
                "end_ns": trainer_last["trainer_end_ns"],
                "duration_ns": trainer_last["trainer_end_ns"]
                - trainer_first["send_start_ns"],
                "first_rank": trainer_first_rank,
                "last_rank": trainer_last_rank,
                "rank_endpoints": trainer_rank_endpoints,
            },
            "rollout_block_avg_ms": statistics.fmean(engine_blocks.values()),
            "rollout_block_by_engine_ms": engine_blocks,
            "rollout_engine_intervals": engine_endpoints,
            "e2e_wtt_ms": (max(engine_consumed.values()) - trainer["send_start_ns"])
            / 1e6,
        }
        rows.append(row)
        print(
            "INTERVAL_ENDPOINTS "
            + json.dumps(
                {
                    "epoch": epoch,
                    "trainer": row["trainer_interval"],
                    "rollout_engines": row["rollout_engine_intervals"],
                },
                sort_keys=True,
            ),
            flush=True,
        )

    print("\n=== replay three-metric WTT ===", flush=True)
    print("epoch trainer_block_ms rollout_block_avg_ms e2e_wtt_ms", flush=True)
    for row in rows:
        print(
            f"{row['epoch']:5d} {row['trainer_block_ms']:16.3f} "
            f"{row['rollout_block_avg_ms']:20.3f} {row['e2e_wtt_ms']:10.3f}",
            flush=True,
        )
    if not 0 <= a.discard_first < len(rows):
        raise ValueError(
            f"discard-first must be in [0, {len(rows) - 1}], got {a.discard_first}"
        )
    selected = rows[a.discard_first :]
    summary_out = {
        "threshold": os.environ.get("WBRIDGE_DEDUP_PAIR_BYTES", ""),
        "control_transport": (
            "tcp" if os.environ.get("WBRIDGE_TCP_CONTROL") == "1" else "rdma_flag_write"
        ),
        "rollout_dp": n_engines,
        "rounds": nr,
        "requested_num_rounds": (
            int(os.environ["WBRIDGE_NUM_ROUNDS"])
            if os.environ.get("WBRIDGE_NUM_ROUNDS")
            else None
        ),
        "rollout_rdma_cap_bytes": (
            int(os.environ["WBRIDGE_ROLLOUT_RDMA_CAP_BYTES"])
            if os.environ.get("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES")
            else None
        ),
        "planner_mode": plan_by_sender_rank[0]["planner_mode"],
        "rollout_rdma_peak_bytes": plan_by_sender_rank[0]["rollout_rdma_peak_bytes"],
        "drain_between_wts": not a.allow_cross_wt_overlap,
        "iters": a.iters,
        "discard_first": a.discard_first,
        "num_selected": len(selected),
        "interval_endpoint_schema": 1,
        "trainer_block_definition": "first trainer send_start to last trainer pack-handoff end",
        "rollout_block_definition": (
            "per engine: first receiver-rank block_start to last receiver-rank block_end; "
            "reported value is the mean across engines"
        ),
        "trainer_block_mean_ms": statistics.fmean(
            r["trainer_block_ms"] for r in selected
        ),
        "trainer_block_median_ms": statistics.median(
            r["trainer_block_ms"] for r in selected
        ),
        "trainer_rank0_block_mean_ms": statistics.fmean(
            r["trainer_rank0_block_ms"] for r in selected
        ),
        "trainer_rank0_block_median_ms": statistics.median(
            r["trainer_rank0_block_ms"] for r in selected
        ),
        "rollout_block_avg_mean_ms": statistics.fmean(
            r["rollout_block_avg_ms"] for r in selected
        ),
        "rollout_block_avg_median_ms": statistics.median(
            r["rollout_block_avg_ms"] for r in selected
        ),
        "e2e_wtt_mean_ms": statistics.fmean(r["e2e_wtt_ms"] for r in selected),
        "e2e_wtt_median_ms": statistics.median(r["e2e_wtt_ms"] for r in selected),
        "rows": rows,
    }
    if digests:
        summary_out["digests"] = dict(sorted(digests.items()))
    print(f"THREE_METRIC_SUMMARY {json.dumps(summary_out, sort_keys=True)}", flush=True)
    if a.metrics_out:
        out_dir = os.path.dirname(os.path.abspath(a.metrics_out))
        os.makedirs(out_dir, exist_ok=True)
        with open(a.metrics_out, "w") as f:
            json.dump(summary_out, f, indent=2, sort_keys=True)
        print(f"three-metric summary -> {a.metrics_out}", flush=True)
    if a.digest_out:
        if not digests:
            raise ValueError("--digest-out requires --seed")
        out_dir = os.path.dirname(os.path.abspath(a.digest_out))
        os.makedirs(out_dir, exist_ok=True)
        digest_report = {
            "label": f"cons-{summary_out['threshold']}",
            "iters": a.iters,
            "warm_median": statistics.median(warm),
            "digests": dict(sorted(digests.items())),
        }
        with open(a.digest_out, "w") as f:
            json.dump(digest_report, f, indent=2, sort_keys=True)
        print(f"digest summary -> {a.digest_out}", flush=True)


if __name__ == "__main__":
    main()
