# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Shared reporting + env plumbing for the replay benchmarks (Ray and Monarch front-ends).

Both front-ends must print the *same* numbers computed the *same* way, or the 3-way transport comparison
is comparing formatting as much as transports. They also have to forward the same set of ``WBRIDGE_*``
knobs to their workers, so those lists live here too rather than being duplicated and drifting.
"""

from __future__ import annotations

import json
import statistics

# Knobs that change what the transfer *does*, so a sweep or an A/B is meaningless unless the workers see
# them. Kept as one list per role because the receiver side has a few the sender has no use for.
ENGINE_ENV_KEYS = (
    "WBRIDGE_ROUND_CAP_BYTES",
    "WBRIDGE_NUM_ROUNDS",
    "WBRIDGE_ROLLOUT_RDMA_CAP_BYTES",
    "WBRIDGE_LOGICAL_TENSOR_CAP_BYTES",
    "WBRIDGE_REPLICA_RELAY",
    "WBRIDGE_SENDER_NUM_BUF",
    "WBRIDGE_RECV_PIPELINE",
    "WBRIDGE_RECV_3STAGE",
    "WBRIDGE_CTL_PROFILE",
    "WBRIDGE_XCHECK",
    "WBRIDGE_DEDUP_PAIR_BYTES",
    "WBRIDGE_DEDUP_DIAG",
    "WBRIDGE_TOPO_EXCHANGE",
    "WBRIDGE_SAME_NODE_IPC",
    "WBRIDGE_TCP_CONTROL",
    # The whole point of the 3-way table: 0 disables Mooncake's 16 MiB multi-NIC striping, giving the
    # 1-NIC-per-rank configuration that is apples-to-apples with Monarch.
    "WBRIDGE_EFA_SUBSLICE_BYTES",
    "WBRIDGE_MONARCH_CHUNK_BYTES",
    "WBRIDGE_HBM_DEBUG",
)

SENDER_ENV_KEYS = (
    # CTL_PROFILE matters most on the sender: _write_flag/_poll_flag both run on its Stage-2 thread.
    "WBRIDGE_ROUND_CAP_BYTES",
    "WBRIDGE_NUM_ROUNDS",
    "WBRIDGE_ROLLOUT_RDMA_CAP_BYTES",
    "WBRIDGE_LOGICAL_TENSOR_CAP_BYTES",
    "WBRIDGE_REPLICA_RELAY",
    "WBRIDGE_RECV_PIPELINE",
    "WBRIDGE_XCHECK",
    "WBRIDGE_CTL_PROFILE",
    "WBRIDGE_DEDUP_PAIR_BYTES",
    "WBRIDGE_DEDUP_DIAG",
    "WBRIDGE_TOPO_EXCHANGE",
    "WBRIDGE_SAME_NODE_IPC",
    "WBRIDGE_FUSE_SELFCHECK",
    "WBRIDGE_TCP_CONTROL",
    "WBRIDGE_EFA_SUBSLICE_BYTES",
    "WBRIDGE_MONARCH_CHUNK_BYTES",
    "WBRIDGE_HBM_DEBUG",
)


def report_run(
    label: str,
    send_res: list,
    digests: list[dict] | None,
    iters: int,
    digest_out: str = "",
    extra: str = "",
) -> dict:
    """Print rank 0's WTT series + the receiver digests, and return the machine-readable summary.

    The first iteration is dropped from the warm statistics: it pays arena first-touch, Mooncake/Monarch
    handle setup and the CUDA graph/JIT warm-up, none of which a steady-state RL loop repeats.
    """
    send_res = sorted(send_res, key=lambda x: x[0])
    rank0, connect_s, wtts, nr = send_res[0]
    print(
        f"\n=== {label}: replay WTT (rank {rank0}, {nr} rounds{', ' + extra if extra else ''}) ===",
        flush=True,
    )
    print(f"connect (cold setup): {connect_s:.3f} s", flush=True)
    for i, w in enumerate(wtts):
        print(
            f"  WT{i}: {w:.3f} s{'   <- cold, dropped' if i == 0 and len(wtts) > 1 else ''}",
            flush=True,
        )
    warm = wtts[1:] or wtts
    out = {
        "label": label,
        "iters": iters,
        "rounds": nr,
        "connect_s": connect_s,
        "wtts": wtts,
        "warm_min": min(warm),
        "warm_median": statistics.median(warm),
        "warm_mean": statistics.mean(warm),
    }
    print(
        f"warm: min={out['warm_min']:.3f} median={out['warm_median']:.3f} "
        f"mean={out['warm_mean']:.3f} s",
        flush=True,
    )

    if digests:
        # Keyed by engine AND rank: every engine numbers its receivers from 0, so keying on rank alone
        # would silently collapse N engines' digests into one set and compare a quarter of the weights.
        d = {x["key"]: x["digest"] for x in digests}
        if len(d) != len(digests):
            raise RuntimeError(
                f"duplicate digest keys: {len(digests)} receivers, {len(d)} unique keys"
            )
        out["digests"] = d
        print(f"receiver digests ({len(d)} receivers):", flush=True)
        for r in sorted(d):
            print(f"  {r}: {d[r]}", flush=True)
    if digest_out:
        with open(digest_out, "w") as f:
            json.dump(out, f, indent=2)
        print(f"summary -> {digest_out}", flush=True)
    return out


def compare_digests(a_path: str, b_path: str) -> bool:
    """Compare two ``--digest-out`` files; print a per-rank verdict. True iff every rank matches."""
    with open(a_path) as f:
        a = json.load(f)
    with open(b_path) as f:
        b = json.load(f)
    da, db = a.get("digests") or {}, b.get("digests") or {}
    if not da or not db:
        print(
            f"FAIL: missing digests ({a_path}: {len(da)} ranks, {b_path}: {len(db)} ranks). "
            "Both runs must use --seed.",
            flush=True,
        )
        return False
    if set(da) != set(db):
        print(f"FAIL: rank sets differ: {sorted(da)} vs {sorted(db)}", flush=True)
        return False
    ok = True
    for r in sorted(da):
        same = da[r] == db[r]
        ok &= same
        print(
            f"  {r}: {'match' if same else 'MISMATCH'}  {a['label']}={da[r]} {b['label']}={db[r]}",
            flush=True,
        )
    print(
        ("OK: " if ok else "FAIL: ") + f"{a['label']} and {b['label']} delivered "
        f"{'byte-identical' if ok else 'DIFFERENT'} weights to every receiver "
        f"({a['warm_median']:.3f} s vs {b['warm_median']:.3f} s warm median)",
        flush=True,
    )
    return ok
