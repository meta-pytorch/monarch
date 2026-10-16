# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Offline self-consistency check of the arena/exchange plan (no Ray/GPU), with consolidation on.

The real-run corruption of even UNTOUCHED tensors implies a sender/receiver plan disagreement. This
script rules the *plan* in or out by checking, purely from the router:

  1. reciprocity  — for every receiver pair (rl, p) sharing a sub-group in a round, the bytes rl writes
                    to p (send[p]) must equal what p expects from rl (grecv[rl]), and vice-versa. A
                    mismatch means the all-gather writes land wrong -> corruption.
  2. arena sizing — S >= s2r(r) + max(prep(r), prep(pred_D(r))); GRECV is outside PREP and parity-slotted.
  3. coverage     — own(rl) + sum_p grecv[p] bytes == rl's full-shard bytes for the round (nothing lost).

Run: python3 examples/verify_arena.py --specs <dir> [--threshold-mb 20]
"""

from __future__ import annotations

import argparse
import math
import os

import torch  # noqa: F401
from analyze_consolidate import build_all_specs
from loadspec_replay import load_records, summary
from wbridge.backend.router import (
    _arena_slot_predecessors,
    _arena_total_bytes,
    WeightRouter,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--specs", required=True)
    ap.add_argument("--threshold-mb", type=float, default=20.0)
    ap.add_argument("--consolidate", default="1")
    ap.add_argument("--depth", type=int, default=1)
    a = ap.parse_args()
    if a.depth < 1:
        ap.error("--depth must be >= 1")

    recs = load_records(a.specs)
    print(summary(recs), flush=True)
    all_specs, ws, dtype_spec = build_all_specs(recs)

    if a.consolidate == "1":
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = (
            "inf"
            if math.isinf(a.threshold_mb)
            else str(int(a.threshold_mb * 1024 * 1024))
        )
    else:
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = "0"
    r = WeightRouter(
        rank=0, sender_ws=ws, all_specs=all_specs, dtype_spec=dict(dtype_spec)
    )
    rws = r.receiver_ws
    classes = r.recv_tensor_classes()

    # arena_layout for every receiver (same `classes` object -> identical view every rank would compute).
    layouts = {rl: r.arena_layout(rl, classes, depth=a.depth) for rl in range(rws)}
    nrounds = len(r.global_rounds)
    print(
        f"consolidate={a.consolidate} receivers={rws} rounds={nrounds} depth={a.depth}",
        flush=True,
    )

    recip_bad = size_bad = cov_bad = 0
    arena_sizes = []
    for rl in range(rws):
        rounds_rl, S_rl, _ = layouts[rl]
        # (2) sizing: predecessor is the previous active global round in the same parity slot.
        active = [i for i, rd in enumerate(rounds_rl) if rd["s2r"]]
        pred = _arena_slot_predecessors(active, a.depth)
        for i, rd in enumerate(rounds_rl):
            pi = pred.get(i)
            prev = rounds_rl[pi]["prep"] if pi is not None else 0
            need = rd["s2r"] + max(rd["prep"], prev)
            if need > S_rl:
                size_bad += 1
                if size_bad <= 5:
                    print(f"  SIZE rl={rl} round={i}: need {need} > S {S_rl}")
        total = _arena_total_bytes(rounds_rl, a.depth, S_rl)
        arena_sizes.append((total, a.depth * S_rl, total - a.depth * S_rl))
        for p in layouts[rl][2]:
            for slot in range(a.depth):
                entries = [
                    rd["grecv"][p]
                    for ri, rd in enumerate(rounds_rl)
                    if ri % a.depth == slot and p in rd["grecv"]
                ]
                if len({off for off, _ in entries}) > 1 or any(
                    off < a.depth * S_rl or off + nb > total for off, nb in entries
                ):
                    size_bad += 1
                    if size_bad <= 5:
                        print(
                            f"  SIZE rl={rl} peer={p} parity={slot}: "
                            f"unstable/out-of-bank grecv slots {entries}"
                        )
        # (1) reciprocity + (3) coverage per round
        for i, rd in enumerate(rounds_rl):
            for p, (_, nb) in rd["send"].items():
                # p's grecv[rl] for the same round must equal rl's send[p]
                p_rd = layouts[p][0][i]
                pnb = p_rd["grecv"].get(rl, (0, 0))[1]
                if nb != pnb:
                    recip_bad += 1
                    if recip_bad <= 8:
                        print(
                            f"  RECIP rl={rl}->p={p} round={i}: send {nb} != peer.grecv {pnb}"
                        )
            # coverage: own + sum(grecv) == full-shard bytes rl holds this round
            own_nb = rd["own"][1]
            grecv_nb = sum(nb for _, nb in rd["grecv"].values())
            round_names = set(
                n for n in r.global_rounds[i] if n in r.recv_specs_full[rl].entries
            )
            full_nb = r.recv_specs_full[rl].subset(round_names).nbytes(r.dtype_spec)
            if own_nb + grecv_nb != full_nb:
                cov_bad += 1
                if cov_bad <= 8:
                    print(
                        f"  COVER rl={rl} round={i}: own {own_nb} + grecv {grecv_nb} "
                        f"= {own_nb + grecv_nb} != full {full_nb}"
                    )

    # (4) ELEMENT-level partition: within each sub-group, members' slices must be pairwise DISJOINT and
    # together cover the full shard exactly (byte-count coverage above can't see element overlap/gap).
    from wbridge.utils.data import ShardSpec as _SS

    part_bad = 0
    for name, subs in classes.items():
        for sg in subs:
            members = sorted(sg)
            if len(members) < 2:
                continue
            full_nb = r.recv_specs_full[members[0]].subset({name}).nbytes(r.dtype_spec)
            slices = [r.recv_specs[m].subset({name}) for m in members]
            cover = sum(s.nbytes(r.dtype_spec) for s in slices)
            if cover != full_nb:
                part_bad += 1
                if part_bad <= 8:
                    print(
                        f"  PART cover rl-group {members} {name}: sum {cover} != full {full_nb}"
                    )
                continue
            for i in range(len(slices)):
                for j in range(i + 1, len(slices)):
                    ov = _SS.compute_overlap(slices[i], slices[j]).nbytes(r.dtype_spec)
                    if ov != 0:
                        part_bad += 1
                        if part_bad <= 8:
                            print(
                                f"  PART overlap {members[i]}&{members[j]} {name}: {ov} bytes shared"
                            )

    print(f"\n=== arena consistency (consolidate={a.consolidate}) ===")
    print(f"reciprocity mismatches: {recip_bad}")
    print(f"sizing violations:      {size_bad}")
    print(f"coverage mismatches:    {cov_bad}")
    print(f"element-partition bad:  {part_bad}")
    gib = 1024**3
    print(
        "arena GiB range:        "
        f"total={min(x[0] for x in arena_sizes) / gib:.3f}..{max(x[0] for x in arena_sizes) / gib:.3f}, "
        f"parity={min(x[1] for x in arena_sizes) / gib:.3f}..{max(x[1] for x in arena_sizes) / gib:.3f}, "
        f"shared_grecv={min(x[2] for x in arena_sizes) / gib:.3f}..{max(x[2] for x in arena_sizes) / gib:.3f}"
    )
    print(
        "RESULT:",
        "PLAN OK"
        if (recip_bad == size_bad == cov_bad == part_bad == 0)
        else "PLAN BROKEN",
    )


if __name__ == "__main__":
    main()
