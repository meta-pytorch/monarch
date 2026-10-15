# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Generate a scalable replay capture for the overlapping 11-group topology case.

The synthetic layout has eight trainer ranks and two eight-worker rollout engines, one engine per
rollout node.  Its tensors are split evenly among three replication patterns:

* ``wide_*`` is replicated across all 16 rollout workers (one replica group);
* ``pair_*`` is sharded by local GPU rank and replicated across the two nodes (eight groups); and
* ``half_*`` has one shard on local ranks 0..3 and one on ranks 4..7 (two groups).

Consequently the topology planner sees 1 + 8 + 2 = 11 groups, including workers that belong to three
different groups.  With the defaults, every source tensor is 1 GiB of BF16 and there are eight tensors
per pattern: 24 GiB of unique weights.  Every rollout worker receives 64 MiB from the trainers for each
tensor, so a 512 MiB round cap produces three balanced rounds.

The output has the same pickle schema as ``WBRIDGE_DUMP_LOADSPEC`` and is consumed directly by
``bench_transfer_4node.py``.
"""

from __future__ import annotations

import argparse
import json
import os
import pickle

import torch
from wbridge.utils.data import shards_numel


SENDERS = 8
ROLLOUT_NODES = 2
WORKERS_PER_ENGINE = 8
ROLLOUT_WORKERS = ROLLOUT_NODES * WORKERS_PER_ENGINE


def _d1(left: int, right: int, width: int):
    return [[(left, right, width)]]


def _identity(entries: dict, dtype: torch.dtype) -> tuple[dict, dict, dict]:
    """Return dtype/load/wksd metadata for flat tensors matching *entries*."""
    dtype_spec = {}
    load_spec = {}
    wksd_meta = {}
    for name, shards in entries.items():
        assert len(shards) == 1
        numel = shards_numel(shards)
        dtype_spec[name] = dtype
        load_spec[name] = {name: [(shards[0], [(0, numel, numel)])]}
        wksd_meta[name] = ((numel,), dtype, (1,))
    return dtype_spec, load_spec, wksd_meta


def _sender_entries(width: int, names: list[str]) -> dict:
    return {name: _d1(0, width, width) for name in names}


def _receiver_entries(worker: int, width: int, names_per_pattern: int) -> dict:
    lane = worker % WORKERS_PER_ENGINE
    half = 0 if lane < WORKERS_PER_ENGINE // 2 else 1
    pair_width = width // WORKERS_PER_ENGINE
    half_width = width // 2
    entries = {}
    for i in range(names_per_pattern):
        entries[f"half_{i:02d}"] = _d1(
            half * half_width, (half + 1) * half_width, width
        )
        entries[f"pair_{i:02d}"] = _d1(
            lane * pair_width, (lane + 1) * pair_width, width
        )
        entries[f"wide_{i:02d}"] = _d1(0, width, width)
    return entries


def _write(path: str, record: dict) -> None:
    with open(path, "wb") as f:
        pickle.dump(record, f)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--output", required=True, help="new or empty output directory")
    ap.add_argument(
        "--tensor-mib", type=int, default=1024, help="global bytes per source tensor"
    )
    ap.add_argument("--tensors-per-pattern", type=int, default=8)
    args = ap.parse_args()

    if args.tensor_mib <= 0 or args.tensors_per_pattern <= 0:
        ap.error("tensor size and count must be positive")
    tensor_bytes = args.tensor_mib * 1024**2
    dtype = torch.bfloat16
    if tensor_bytes % dtype.itemsize:
        ap.error(f"tensor bytes must be divisible by {dtype.itemsize}")
    width = tensor_bytes // dtype.itemsize
    if width % ROLLOUT_WORKERS:
        ap.error(f"tensor element count must be divisible by {ROLLOUT_WORKERS}")

    os.makedirs(args.output, exist_ok=True)
    existing = os.listdir(args.output)
    if existing:
        ap.error(
            f"output directory must be empty: {args.output} contains {existing[:3]}"
        )

    names = [
        f"{pattern}_{i:02d}"
        for pattern in ("half", "pair", "wide")
        for i in range(args.tensors_per_pattern)
    ]
    sentries = _sender_entries(width, names)
    sdtypes, sload, swksd = _identity(sentries, dtype)
    for rank in range(SENDERS):
        rec = {
            "role": "sender",
            "rank": rank,
            "engine_id": None,
            "load_spec": sload,
            "dtype_spec": sdtypes,
            "src_shard_spec": sentries,
            "wksd_meta": swksd,
            "world_size": SENDERS,
            "sender_staging": False,
        }
        _write(
            os.path.join(args.output, f"loadspec_sender_r{rank}_pid{1000 + rank}.pkl"),
            rec,
        )

    receiver_model_bytes = None
    for node in range(ROLLOUT_NODES):
        for rank in range(WORKERS_PER_ENGINE):
            worker = node * WORKERS_PER_ENGINE + rank
            rentries = _receiver_entries(worker, width, args.tensors_per_pattern)
            rdtypes, rload, rwksd = _identity(rentries, dtype)
            receiver_model_bytes = sum(
                meta[0][0] * meta[1].itemsize for meta in rwksd.values()
            )
            rec = {
                "role": "receiver",
                "rank": rank,
                "engine_id": f"synthetic://rollout-node-{node}/engine-0",
                "load_spec": rload,
                "dtype_spec": rdtypes,
                "src_shard_spec": rentries,
                "wksd_meta": rwksd,
                "num_workers": WORKERS_PER_ENGINE,
                "receiver_staging": False,
            }
            # The replay's physical-engine splitter uses pid clusters to retain captured node placement.
            fake_pid = 2000 + node * 1000 + rank
            _write(
                os.path.join(
                    args.output,
                    f"loadspec_receiver_n{node}_r{rank}_pid{fake_pid}.pkl",
                ),
                rec,
            )

    model_bytes = tensor_bytes * args.tensors_per_pattern * 3
    ingress_per_worker = model_bytes // ROLLOUT_WORKERS
    manifest = {
        "dtype": str(dtype),
        "senders": SENDERS,
        "rollout_nodes": ROLLOUT_NODES,
        "workers_per_rollout_node": WORKERS_PER_ENGINE,
        "replica_groups": 11,
        "tensors_per_pattern": args.tensors_per_pattern,
        "tensor_bytes": tensor_bytes,
        "unique_model_bytes": model_bytes,
        "trainer_tx_bytes": model_bytes,
        # Trainers seed half of every replica class on each rollout node.  The topology exchange then
        # imports the other half from the peer rollout node, so total cross-node RDMA RX per rollout node is
        # one model.
        "trainer_ingress_bytes_per_rollout_node": model_bytes // ROLLOUT_NODES,
        "rollout_exchange_rx_bytes_per_node": model_bytes // ROLLOUT_NODES,
        "total_rdma_rx_bytes_per_rollout_node": model_bytes,
        "trainer_ingress_bytes_per_worker": ingress_per_worker,
        "receiver_model_bytes_per_worker": receiver_model_bytes,
        "recommended_round_cap_bytes": ingress_per_worker // 3,
    }
    with open(os.path.join(args.output, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
