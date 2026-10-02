# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Measure individual WeightRouter RDMA-cap probes from captured LoadSpecs.

Unlike ``bench_plan.py``, this does not run the exponential/binary cap search.  It constructs the
round-invariant metadata once, then reports packing, topology, and arena time for each requested exact
round count.  That makes R=1/R=2 comparisons fast enough to use while optimizing Kimi-scale plans.
"""

from __future__ import annotations

import argparse
import json
import os
import time

from bench_plan import _host_from_path, _merge_dtype_specs, _ordered_records
from loadspec_replay import load_records
from wbridge.backend.router import WeightRouter
from wbridge.utils.data import ShardSpec


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--specs", required=True)
    parser.add_argument("--rounds", type=int, nargs="+", default=[1, 2])
    args = parser.parse_args()

    os.environ.setdefault("WBRIDGE_DEDUP_PAIR_BYTES", "0")
    os.environ.pop("WBRIDGE_NUM_ROUNDS", None)
    os.environ.pop("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", None)

    started = time.perf_counter()
    records = load_records(args.specs)
    ordered, sender_ws, replicas = _ordered_records(records)
    all_specs = [ShardSpec(record["src_shard_spec"]) for record in ordered]
    dtype_spec = _merge_dtype_specs(ordered)
    peer_ip = {
        rank: _host_from_path(record["_path"]) for rank, record in enumerate(ordered)
    }
    prepared_seconds = time.perf_counter() - started
    print(
        "INPUT "
        + json.dumps(
            {
                "world_size": len(all_specs),
                "sender_ws": sender_ws,
                "receiver_ws": len(all_specs) - sender_ws,
                "receiver_replicas": replicas,
                "tensor_names": len(dtype_spec),
                "cons": os.environ["WBRIDGE_DEDUP_PAIR_BYTES"],
                "prepared_seconds": prepared_seconds,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    # Supplying a complete plan skips automatic cap selection while retaining normal validation/local-plan
    # construction.  The benchmark replaces this plan before every measured probe below.
    construct_started = time.perf_counter()
    router = WeightRouter(
        rank=0,
        sender_ws=sender_ws,
        all_specs=all_specs,
        dtype_spec=dtype_spec,
        global_rounds=[set(dtype_spec)],
    )
    construct_seconds = time.perf_counter() - construct_started

    invariant_started = time.perf_counter()
    name_send, name_recv = router._name_rank_bytes()
    invariant_seconds = time.perf_counter() - invariant_started
    print(
        "INVARIANTS "
        + json.dumps(
            {
                "constructor_seconds": construct_seconds,
                "name_rank_and_cache_seconds": invariant_seconds,
                "production_cache_seconds": router.planner_invariant_seconds,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    results = []
    for round_count in args.rounds:
        pack_started = time.perf_counter()
        router.global_rounds = router._pack_exact_rounds(
            name_send, name_recv, round_count
        )
        pack_seconds = time.perf_counter() - pack_started

        probe_started = time.perf_counter()
        sizes = router.rollout_rdma_bytes(peer_ip)
        probe_seconds = time.perf_counter() - probe_started
        result = {
            "rounds": round_count,
            "pack_seconds": pack_seconds,
            "probe_seconds": probe_seconds,
            "peak_gib": max(sizes, default=0) / 1024**3,
            **router._last_rollout_rdma_timing,
        }
        results.append(result)
        print("PROBE " + json.dumps(result, sort_keys=True), flush=True)

    print("RESULT " + json.dumps({"probes": results}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
