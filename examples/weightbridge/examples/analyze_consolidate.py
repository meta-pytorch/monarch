# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Offline analysis of the dedup group-consolidation pass on captured LoadSpecs (no Ray/GPU).

Rebuilds the exact WeightRouter the real run builds (all sender + receiver ShardSpecs in global rank
order), then compares the exchange plan with ``WBRIDGE_DEDUP_PAIR_BYTES=0`` vs the requested threshold:

  * # exchange groups (per-tensor classes of size >= 2), # singletons (0-peer direct receives)
  * total directed exchange pairs (proxy for control-plane flag handshakes)
  * trainer->rollout RDMA bytes (rises: each sub-group re-fetches its full shard from the trainer)
  * receiver<->receiver exchange bytes (falls: dissolved groups stop all-gathering)

Run inside the container on ONE node (no Ray needed):
    python3 examples/analyze_consolidate.py --specs <dir> [--threshold-mb 20]
"""

from __future__ import annotations

import argparse
import os

import torch  # noqa: F401  (ShardSpec dtypes)
from loadspec_replay import group_records, load_records, summary
from wbridge.backend.router import WeightRouter
from wbridge.utils.data import shards_nbytes, ShardSpec


def build_all_specs(recs):
    """All sender + receiver ShardSpecs in global rank order (senders 0..ws-1, then engines packed
    contiguously onto the receiver rank space, mirroring bench_transfer's engine_base)."""
    senders, engines = group_records(recs)
    ws = len(senders)
    all_specs = [ShardSpec(senders[r]["src_shard_spec"]) for r in sorted(senders)]
    dtype_spec: dict = {}
    for r in sorted(senders):
        for name, dt in senders[r]["dtype_spec"].items():
            dtype_spec.setdefault(name, dt)
    for eid in engines.keys():  # engine_base packs engines contiguously
        for lr in sorted(engines[eid]):
            rec = engines[eid][lr]
            all_specs.append(ShardSpec(rec["src_shard_spec"]))
            for name, dt in rec["dtype_spec"].items():
                dtype_spec.setdefault(name, dt)
    return all_specs, ws, dtype_spec


def plan_metrics(router, classes):
    """(groups>=2, singletons, directed_pairs, exchange_bytes, peerset) for a {name:[class,...]} plan.

    peerset[ri] = union of distinct exchange peers of receiver ri across ALL tensors — the real
    control-plane driver (a flag ping-pong is per (peer, round); piggybacking onto an already-active
    peer is free, so what matters is how many DISTINCT peers each receiver must handshake)."""
    from collections import defaultdict

    groups = singles = pairs = xbytes = 0
    peerset = defaultdict(set)
    for name, cls in classes.items():
        for c in cls:
            k = len(c)
            sb = shards_nbytes(
                router.recv_specs_full[c[0]][name], router.dtype_spec[name]
            )
            if k >= 2:
                groups += 1
                pairs += k * (
                    k - 1
                )  # directed peer pairs (each pulls from every other)
                xbytes += sb * (k - 1)  # total bytes pulled across the class
                cs = set(c)
                for ri in c:
                    peerset[ri] |= cs - {ri}
            else:
                singles += 1
    return groups, singles, pairs, xbytes, peerset


def trainer_bytes(router):
    """Total trainer->rollout RDMA bytes = sum over (receiver, sender) overlap of the (deduped) recv spec."""
    tot = 0
    for ri in range(router.receiver_ws):
        for si in range(router.sender_ws):
            ov = ShardSpec.compute_overlap(router.send_specs[si], router.recv_specs[ri])
            tot += ov.nbytes(router.dtype_spec)
    return tot


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--specs", required=True)
    ap.add_argument("--threshold-mb", type=float, default=20.0)
    a = ap.parse_args()

    recs = load_records(a.specs)
    print(summary(recs), flush=True)
    all_specs, ws, dtype_spec = build_all_specs(recs)
    thr = str(int(a.threshold_mb * 1024 * 1024))

    # OFF: natural class-based dedup.
    os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = "0"
    r_off = WeightRouter(
        rank=0, sender_ws=ws, all_specs=all_specs, dtype_spec=dict(dtype_spec)
    )
    cls_off = r_off.recv_tensor_classes()
    g0, s0, p0, xb0, ps0 = plan_metrics(r_off, cls_off)
    tb0 = trainer_bytes(r_off)

    # ON: consolidated sub-groups at the given pair threshold.
    os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = thr
    r_on = WeightRouter(
        rank=0, sender_ws=ws, all_specs=all_specs, dtype_spec=dict(dtype_spec)
    )
    cls_on = r_on.recv_tensor_classes()
    g1, s1, p1, xb1, ps1 = plan_metrics(r_on, cls_on)
    tb1 = trainer_bytes(r_on)

    GB = 1024**3
    print(
        f"\n=== dedup consolidation @ threshold {a.threshold_mb:.0f} MB (world={len(all_specs)}, "
        f"senders={ws}, receivers={r_off.receiver_ws}, tensors={len(cls_off)}) ==="
    )
    print(f"{'metric':<34}{'OFF':>16}{'ON':>16}{'delta':>16}")

    def row(lbl, off, on, unit=""):
        d = on - off
        print(f"{lbl:<34}{off:>16}{on:>16}{('%+d' % d):>14}{unit:>2}")

    row("exchange groups (size>=2)", g0, g1)
    row("singletons (0-peer direct)", s0, s1)
    row("directed exchange pairs", p0, p1)
    print(
        f"{'exchange bytes (recv<->recv)':<34}{xb0 / GB:>15.3f}G{xb1 / GB:>15.3f}G{(xb1 - xb0) / GB:>+14.3f}G"
    )
    print(
        f"{'trainer->rollout RDMA bytes':<34}{tb0 / GB:>15.3f}G{tb1 / GB:>15.3f}G{(tb1 - tb0) / GB:>+14.3f}G"
    )
    # Distinct exchange peers per receiver — the control-plane driver (fewer peers = fewer flag handshakes).
    tot0 = sum(len(ps0[r]) for r in range(r_off.receiver_ws))
    tot1 = sum(len(ps1[r]) for r in range(r_off.receiver_ws))
    mx0 = max(len(ps0[r]) for r in range(r_off.receiver_ws))
    mx1 = max(len(ps1[r]) for r in range(r_off.receiver_ws))
    row("sum distinct peers (all recv)", tot0, tot1)
    row("max distinct peers (one recv)", mx0, mx1)
    print(
        "per-receiver distinct peers  OFF:",
        [len(ps0[r]) for r in range(r_off.receiver_ws)],
        "\n                             ON :",
        [len(ps1[r]) for r in range(r_off.receiver_ws)],
    )

    # Show the tensors whose class was actually decomposed (the wide-small ones the pass targets).
    changed = [(n, cls_off[n], cls_on[n]) for n in cls_off if cls_off[n] != cls_on[n]]
    print(f"\ndecomposed tensors: {len(changed)}")
    for n, before, after in sorted(changed, key=lambda x: x[0])[:25]:
        sb = shards_nbytes(r_off.recv_specs_full[before[0][0]][n], r_off.dtype_spec[n])
        print(f"  {n[:60]:<60} {sb / 1024:.0f}KB  {before} -> {after}")


if __name__ == "__main__":
    main()
