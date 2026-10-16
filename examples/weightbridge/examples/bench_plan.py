# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Time rank-0 WeightRouter planning from captured per-rank LoadSpecs.

This is intentionally a metadata-only replay: it reconstructs the captured ShardSpecs and merged dtype
map that ``WBEndpoint._finish_setup`` gives rank 0, then invokes the production ``WeightRouter`` once.
Tensor storage, process groups, GPUs, Ray, Megatron, and SGLang are not involved.

Large distributed captures may not preserve a globally unique receiver engine id.  When several replicas
contain the same receiver-local ranks, this script verifies that every copy of a local rank has identical
metadata and repeats that rank template in replica-major order.  That is equivalent for routing and avoids
guessing an engine identity that is absent from the capture.

Example (after installing from the directory containing ``pyproject.toml``)::

    python3 -u examples/bench_plan.py --specs /path/to/capture
"""

from __future__ import annotations

import argparse
import json
import os
import re
import resource
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Callable


def _seconds(value: float) -> str:
    return f"{value:.3f}s"


class PhaseTimer:
    """Low-overhead nested phase timers plus periodic progress/memory output."""

    def __init__(self, heartbeat_seconds: float) -> None:
        self.heartbeat_seconds = heartbeat_seconds
        self.started = time.perf_counter()
        self.timings: dict[str, list[float]] = defaultdict(list)
        self._stack: list[str] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @staticmethod
    def _max_rss_gib() -> float:
        # Linux reports ru_maxrss in KiB.
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024**2

    def start_heartbeat(self) -> None:
        if self.heartbeat_seconds <= 0:
            return

        def run() -> None:
            while not self._stop.wait(self.heartbeat_seconds):
                with self._lock:
                    current = self._stack[-1] if self._stack else "between phases"
                print(
                    f"HEARTBEAT elapsed={_seconds(time.perf_counter() - self.started)} "
                    f"phase={current} max_rss={self._max_rss_gib():.2f}GiB",
                    flush=True,
                )

        self._thread = threading.Thread(
            target=run, name="plan-bench-heartbeat", daemon=True
        )
        self._thread.start()

    def stop_heartbeat(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)

    @contextmanager
    def phase(self, label: str):
        with self._lock:
            self._stack.append(label)
        print(f"PHASE_START {label}", flush=True)
        started = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - started
            self.timings[label].append(elapsed)
            with self._lock:
                popped = self._stack.pop()
            assert popped == label
            print(
                f"PHASE_END {label} elapsed={_seconds(elapsed)} max_rss={self._max_rss_gib():.2f}GiB",
                flush=True,
            )

    def wrap(self, label: str, fn: Callable) -> Callable:
        def timed(*args, **kwargs):
            with self.phase(label):
                return fn(*args, **kwargs)

        timed.__name__ = getattr(fn, "__name__", label)
        timed.__doc__ = getattr(fn, "__doc__")
        return timed


def _host_from_path(path: str) -> str:
    match = re.search(r"loadspec_(.*?)_(?:sender|receiver)_", Path(path).name)
    return match.group(1) if match else "unknown"


def _ordered_records(records: list[dict]) -> tuple[list[dict], int, int]:
    """Return production-equivalent global-rank order and sender/replica counts."""
    senders: dict[int, dict] = {}
    receivers: dict[int, list[dict]] = defaultdict(list)
    for record in records:
        if record["role"] == "sender":
            rank = int(record["rank"])
            if rank in senders:
                raise ValueError(f"duplicate sender rank {rank}")
            senders[rank] = record
        elif record["role"] == "receiver":
            receivers[int(record["rank"])].append(record)
        else:
            raise ValueError(f"unknown role {record['role']!r}")

    sender_ranks = sorted(senders)
    receiver_ranks = sorted(receivers)
    if sender_ranks != list(range(len(sender_ranks))):
        raise ValueError(f"sender ranks are not contiguous: {sender_ranks}")
    if receiver_ranks != list(range(len(receiver_ranks))):
        raise ValueError(f"receiver-local ranks are not contiguous: {receiver_ranks}")

    replica_counts = {len(copies) for copies in receivers.values()}
    if len(replica_counts) != 1:
        raise ValueError(
            f"receiver ranks have unequal replica counts: {sorted(replica_counts)}"
        )
    replicas = replica_counts.pop()

    # Some captures record the same node-local IPC engine id for every tensor-parallel replica. The
    # receiver metadata must therefore be replica-identical; verify that fact rather than silently
    # depending on filename/PID clustering to recover an identity the pickle format does not contain.
    for rank, copies in receivers.items():
        copies.sort(key=lambda record: record["_path"])
        reference = copies[0]
        for copy in copies[1:]:
            if copy["src_shard_spec"] != reference["src_shard_spec"]:
                raise ValueError(
                    f"receiver rank {rank} has non-identical replica ShardSpecs"
                )
            if copy["dtype_spec"] != reference["dtype_spec"]:
                raise ValueError(
                    f"receiver rank {rank} has non-identical replica dtype specs"
                )

    ordered = [senders[rank] for rank in sender_ranks]
    # Any same-index selection is valid after the replica-identity check above. Keep replica-major
    # ordering because live registration packs each rollout replica contiguously.
    ordered.extend(
        receivers[rank][replica]
        for replica in range(replicas)
        for rank in receiver_ranks
    )
    return ordered, len(senders), replicas


def _merge_dtype_specs(records: list[dict]) -> dict:
    merged: dict = {}
    for record in records:
        for name, dtype in record["dtype_spec"].items():
            if name in merged and merged[name] != dtype:
                raise ValueError(
                    f"dtype mismatch for {name}: {merged[name]} versus {dtype}"
                )
            merged.setdefault(name, dtype)
    return {name: merged[name] for name in sorted(merged)}


def _install_router_timers(router_module, timer: PhaseTimer) -> None:
    # Inclusive timers are deliberately nested.  In particular, compute_global_rounds includes
    # name_rank_bytes; the report derives round packing as their difference.
    module_functions = (
        ("validate_logical_partitions", "validate_logical_tensor_partitions"),
        ("dedup_sender_specs", "dedup_send_specs"),
        ("consolidate_receiver_groups", "consolidate_groups"),
        ("dedup_receiver_specs", "_dedup_specs_by_subgroups"),
    )
    for label, name in module_functions:
        setattr(router_module, name, timer.wrap(label, getattr(router_module, name)))

    methods = (
        ("find_receiver_classes", "_natural_recv_classes"),
        ("name_rank_bytes", "_name_rank_bytes"),
        ("legacy_round_packing_inclusive", "_legacy_cap_rounds"),
        ("global_rounds_inclusive", "compute_global_rounds"),
        ("local_rounds", "compute_local_rounds"),
    )
    for label, name in methods:
        original = getattr(router_module.WeightRouter, name)
        setattr(router_module.WeightRouter, name, timer.wrap(label, original))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--specs", required=True, help="directory containing loadspec_*.pkl"
    )
    parser.add_argument("--heartbeat-seconds", type=float, default=30.0)
    parser.add_argument("--json-out", default="", help="optional result JSON path")
    args = parser.parse_args()

    # These are read while wbridge.backend.router is imported. Keep caller overrides, but make this
    # benchmark's defaults explicit for an invocation that supplies no environment.
    os.environ.setdefault("WBRIDGE_ROUND_CAP_BYTES", str(1024**3))
    os.environ.setdefault("WBRIDGE_DEDUP_PAIR_BYTES", "inf")

    import wbridge.backend.router as router_module
    from loadspec_replay import load_records
    from wbridge.utils.data import ShardSpec

    timer = PhaseTimer(args.heartbeat_seconds)
    timer.start_heartbeat()
    try:
        with timer.phase("load_and_translate_capture"):
            records = load_records(args.specs)
        with timer.phase("reconstruct_global_rank_order"):
            ordered, sender_ws, replicas = _ordered_records(records)
        with timer.phase("construct_shard_specs_and_merge_dtypes"):
            all_specs = [ShardSpec(record["src_shard_spec"]) for record in ordered]
            dtype_spec = _merge_dtype_specs(ordered)
            # The legacy planner ignores placement, but pass the same shape of map as production.
            peer_ip = {
                rank: _host_from_path(record["_path"])
                for rank, record in enumerate(ordered)
            }

        print(
            "INPUT "
            f"records={len(records)} world_size={len(all_specs)} senders={sender_ws} "
            f"receivers={len(all_specs) - sender_ws} receiver_replicas={replicas} "
            f"tensor_names={len(dtype_spec)} round_cap={os.environ['WBRIDGE_ROUND_CAP_BYTES']} "
            f"pair_bytes={os.environ['WBRIDGE_DEDUP_PAIR_BYTES']} "
            f"num_rounds_env={os.environ.get('WBRIDGE_NUM_ROUNDS', '') or 'unset'} "
            f"rdma_cap_env={os.environ.get('WBRIDGE_ROLLOUT_RDMA_CAP_BYTES', '') or 'unset'}",
            flush=True,
        )

        _install_router_timers(router_module, timer)
        with timer.phase("weight_router_total"):
            router = router_module.WeightRouter(
                rank=0,
                sender_ws=sender_ws,
                all_specs=all_specs,
                dtype_spec=dtype_spec,
                peer_ip=peer_ip,
            )
        with timer.phase("serialize_round_plan"):
            round_plan = [sorted(names) for names in router.global_rounds]

        global_rounds = timer.timings["global_rounds_inclusive"][0]
        name_rank_bytes = timer.timings["name_rank_bytes"][0]
        report = {
            "specs": str(Path(args.specs).resolve()),
            "world_size": len(all_specs),
            "sender_ws": sender_ws,
            "receiver_ws": len(all_specs) - sender_ws,
            "receiver_replicas": replicas,
            "tensor_names": len(dtype_spec),
            "rounds": len(round_plan),
            "round_tensor_counts": [len(names) for names in round_plan],
            "round_cap_bytes": int(os.environ["WBRIDGE_ROUND_CAP_BYTES"]),
            "dedup_pair_bytes": os.environ["WBRIDGE_DEDUP_PAIR_BYTES"],
            "planner_mode": router.planner_mode,
            "planner_invariant_seconds": router.planner_invariant_seconds,
            "planner_probe_timings": router.planner_probe_timings,
            "max_rss_gib": timer._max_rss_gib(),
            "timings_seconds": {
                label: values for label, values in timer.timings.items()
            },
            "derived_seconds": {
                "round_packing_excluding_name_rank_bytes": global_rounds
                - name_rank_bytes,
            },
        }
        print("RESULT_JSON " + json.dumps(report, sort_keys=True), flush=True)
        if args.json_out:
            Path(args.json_out).write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n"
            )
    finally:
        timer.stop_heartbeat()


if __name__ == "__main__":
    main()
