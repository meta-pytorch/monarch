# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Benchmark deterministic independent WeightRouter planning in several worker processes.

The parent launches fresh Python interpreters with distinct ``PYTHONHASHSEED`` values.  Each child loads
the same captured metadata, builds the production rank-specific ``WeightRouter``, and hashes the complete
global round plan.  The run fails unless every child produces exactly the same digest.

Example::

    python3 -u examples/bench_plan_workers.py --specs /path/to/capture
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path


def _worker(spec_dir: str, rank: int) -> dict:
    from bench_plan import _host_from_path, _merge_dtype_specs, _ordered_records
    from loadspec_replay import load_records
    from wbridge.backend.router import WeightRouter
    from wbridge.utils.data import ShardSpec

    started = time.perf_counter()
    records = load_records(spec_dir)
    ordered, sender_ws, replicas = _ordered_records(records)
    all_specs = [ShardSpec(record["src_shard_spec"]) for record in ordered]
    dtype_spec = _merge_dtype_specs(ordered)
    peer_ip = {
        global_rank: _host_from_path(record["_path"])
        for global_rank, record in enumerate(ordered)
    }
    prepared = time.perf_counter()

    router = WeightRouter(
        rank=rank,
        sender_ws=sender_ws,
        all_specs=all_specs,
        dtype_spec=dtype_spec,
        peer_ip=peer_ip,
    )
    round_plan = [sorted(names) for names in router.global_rounds]
    planned = time.perf_counter()
    encoded = json.dumps(round_plan, ensure_ascii=True, separators=(",", ":")).encode()
    return {
        "rank": rank,
        "python_hash_seed": os.environ.get("PYTHONHASHSEED", "random"),
        "world_size": len(all_specs),
        "sender_ws": sender_ws,
        "receiver_replicas": replicas,
        "tensor_names": len(dtype_spec),
        "rounds": len(round_plan),
        "round_tensor_counts": [len(names) for names in round_plan],
        "plan_sha256": hashlib.sha256(encoded).hexdigest(),
        "prepare_seconds": prepared - started,
        "plan_seconds": planned - prepared,
        "max_rss_gib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024**2,
    }


def _parent(spec_dir: str, workers: int) -> None:
    if workers < 2:
        raise ValueError("--workers must be at least 2")
    # Captures contain one file per global worker. Spread samples across sender and receiver ranges
    # rather than benchmarking adjacent local projections.
    world_size = len(list(Path(spec_dir).glob("loadspec_*.pkl")))
    if world_size < workers:
        raise ValueError(f"capture has only {world_size} records for {workers} workers")
    ranks = [index * world_size // workers for index in range(workers)]
    processes: list[tuple[int, subprocess.Popen]] = []
    wall_started = time.perf_counter()
    for index, rank in enumerate(ranks):
        env = dict(os.environ)
        env["PYTHONHASHSEED"] = str(index + 1)
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--specs",
            spec_dir,
            "--worker-rank",
            str(rank),
        ]
        processes.append(
            (
                rank,
                subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                ),
            )
        )

    results = []
    for rank, process in processes:
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                f"planner worker rank {rank} exited {process.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
        lines = [
            line for line in stdout.splitlines() if line.startswith("WORKER_RESULT ")
        ]
        if len(lines) != 1:
            raise RuntimeError(
                f"planner worker rank {rank} emitted no unique result: {stdout!r}"
            )
        result = json.loads(lines[0].removeprefix("WORKER_RESULT "))
        results.append(result)
        print("WORKER_RESULT " + json.dumps(result, sort_keys=True), flush=True)

    wall_seconds = time.perf_counter() - wall_started
    digests = {result["plan_sha256"] for result in results}
    round_counts = {tuple(result["round_tensor_counts"]) for result in results}
    if len(digests) != 1 or len(round_counts) != 1:
        raise RuntimeError(
            f"independent planners diverged: digests={digests}, round_counts={round_counts}"
        )
    plan_times = [result["plan_seconds"] for result in results]
    summary = {
        "workers": workers,
        "ranks": ranks,
        "distinct_hash_seeds": len({result["python_hash_seed"] for result in results}),
        "distinct_plan_digests": len(digests),
        "plan_sha256": next(iter(digests)),
        "rounds": results[0]["rounds"],
        "round_tensor_counts": results[0]["round_tensor_counts"],
        "plan_seconds_min": min(plan_times),
        "plan_seconds_median": statistics.median(plan_times),
        "plan_seconds_max": max(plan_times),
        "concurrent_wall_seconds_including_capture_load": wall_seconds,
    }
    print("SUMMARY " + json.dumps(summary, sort_keys=True), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--specs", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--worker-rank", type=int, default=-1, help=argparse.SUPPRESS)
    args = parser.parse_args()

    os.environ.setdefault("WBRIDGE_ROUND_CAP_BYTES", str(1024**3))
    os.environ.setdefault("WBRIDGE_DEDUP_PAIR_BYTES", "inf")
    if args.worker_rank >= 0:
        print(
            "WORKER_RESULT "
            + json.dumps(_worker(args.specs, args.worker_rank), sort_keys=True),
            flush=True,
        )
    else:
        _parent(args.specs, args.workers)


if __name__ == "__main__":
    main()
