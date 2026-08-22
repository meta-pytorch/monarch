#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
All of the scheduler-independent logic to configure and drive a multihost RDMA
benchmark.

Each configuration is run in two directions. Both move the same bytes over the
same edges; only the side that initiates the transfers changes, so ``write`` has
the source pushing into the destination's buffers and ``read`` has the
destination pulling from the source's. Each side's memory kind is set
independently with ``--source-device`` and ``--dest-device``.

A *run* allocates and registers fresh tensors and then iterates. Within a run, the
first ``--warmup-iters-per-run`` iterations are discarded and the rest are warm.
The very first iteration of all needs to establish QP connections and is therefore
reported separately.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from bench_peer import VERIFY_MODES, VERIFY_SAMPLED
from bench_stats import (
    ConfigColumns,
    KeyColumns,
    MetricColumns,
    metrics_for,
    phase_table,
    RunRecord,
    SCHEMA_VERSION,
    ShapeColumns,
    summary_header,
    summary_row,
    write_rows,
)
from bench_topology import (
    bytes_per_iteration,
    describe,
    DIRECTIONS,
    max_ops_per_action,
    MemoryPlan,
    PAIRINGS,
    PATTERNS,
    PHASES,
    SAME,
    Topology,
    unused_hosts,
)
from monarch.config import get_global_config


RUN_COMMAND: str = "run"
BATCH_COMMAND: str = "run-batch"

# When to kill the job once the benchmark is done.
TEARDOWN_NEVER: str = "never"
TEARDOWN_ON_FAILURE: str = "on_failure"
TEARDOWN_ALWAYS: str = "always"
TEARDOWN_POLICIES: tuple[str, ...] = (
    TEARDOWN_NEVER,
    TEARDOWN_ON_FAILURE,
    TEARDOWN_ALWAYS,
)

_GB: int = 1000**3
_MB: int = 1000**2


@dataclass(frozen=True)
class BenchConfig:
    """Everything the benchmark needs that is not scheduler-specific."""

    # ibverbs or tcp
    transport: str
    # Shape of the edges between hosts (see `PATTERNS`)
    pattern: str
    # How many hosts take part in the benchmark.
    num_hosts: int
    # How procs on each host are paired with each other:
    # - same: proc i -> proc i
    # - shifted: proc i -> proc (i + lane_shift) % num_lanes
    # - all: proc i -> proc j for all i, j
    lane_pairing: str
    lane_shift: int
    # Size of the payload per op per edge between lanes.
    payload_size_mb: float
    # Number of concurrent ops per edge between lanes.
    concurrent_ops: int
    # Number of times the benchmark will run on a set of
    # freshly allocated buffers. Each run does
    # `warmup_iters_per_run + warm_iters_per_run` total iterations.
    runs: int
    # Number of iterations at the beginning of a run whose timings
    # will be discarded.
    warmup_iters_per_run: int
    # Number of iterations within a run whose timings will be counted.
    warm_iters_per_run: int
    procs_per_host: int
    # Whether each proc's outgoing buffers reside on gpu.
    source_on_gpu: bool
    # Whether each proc's incoming buffers reside on gpu.
    dest_on_gpu: bool
    # Verification mode (see `VERIFY_MODES`): whether or not, and to what
    # extent, to validate each tensor's value after each iteration.
    verify: str
    # In `VERIFY_SAMPLED` mode, instead of creating a hash digest of the
    # entire tensor, we create hash digest from a window of size
    # `verify_window_mb` at the front, middle and end of the tensor.
    verify_window_mb: float
    # How much gpu memory each proc is entitled to. Validated before
    # launching.
    max_device_gb_per_proc: float
    # How much host memory each host is entitled to. Validated before
    # launching.
    max_host_gb_per_host: float
    # How many threads the RDMA runtime was configured to use.
    rdma_runtime_threads: int | None
    # Path to the file where the output will live.
    output_csv: str
    # Batch or non-batch mode.
    command: str
    # Run the benchmark locally on the current host.
    local_only: bool
    # Path to the monarch job's cached state.
    cached_path: str | None
    # When to teardown the monarch job when the benchmark is done.
    teardown_policy: str

    @property
    def payload_bytes(self) -> int:
        return int(self.payload_size_mb * _MB)

    @property
    def iterations_per_run(self) -> int:
        return self.warmup_iters_per_run + self.warm_iters_per_run

    @property
    def job_hosts(self) -> int:
        """Hosts the job must provision. Zero under ``--local-only``, which puts
        every host on this machine and so needs no job at all."""
        return 0 if self.local_only else self.num_hosts

    @property
    def source_label(self) -> str:
        return "gpu" if self.source_on_gpu else "cpu"

    @property
    def dest_label(self) -> str:
        return "gpu" if self.dest_on_gpu else "cpu"


def add_benchmark_args(parser: argparse.ArgumentParser, *, batch: bool = True) -> None:
    """Add the scheduler-independent flags, then the subcommands.

    Wrappers add their own flags on top; because those land on the parent parser
    they are given before the subcommand, as in
    ``slurm_benchmark.py --partition x run --teardown-policy on_failure``.

    Set ``batch=False`` for a backend that cannot run the benchmark inside its
    own allocation, which drops the ``run-batch`` subcommand entirely rather
    than accepting it and failing later.
    """
    _add_shape_args(parser)
    _add_workload_args(parser)
    _add_reporting_args(parser)
    _add_subcommands(parser, batch=batch)


def _add_shape_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--pattern",
        choices=PATTERNS,
        default="p2p",
        help="Shape of the edges between hosts (default: p2p).",
    )
    parser.add_argument(
        "--num-hosts",
        type=int,
        default=2,
        help=(
            "How many hosts take part in the benchmark. Setting to 1 makes "
            "every pattern the loopback self-edge (default: 2)."
        ),
    )
    parser.add_argument(
        "--lane-pairing",
        choices=PAIRINGS,
        default=SAME,
        help=(
            "How procs on each host are paired with each other: 'same' pairs "
            "proc i with proc i, 'shifted' pairs proc i with proc "
            "(i + --lane-shift) %% --procs-per-host, and 'all' pairs proc i "
            "with proc j for all i, j (default: same)."
        ),
    )
    parser.add_argument(
        "--lane-shift",
        type=int,
        default=1,
        help="Offset used by --lane-pairing shifted (default: 1).",
    )
    parser.add_argument(
        "--procs-per-host",
        type=int,
        default=8,
        help=(
            "Procs to spawn per host. The local rank of a proc determines "
            "which gpu ordinal it uses, when relevant (default: 8)."
        ),
    )


def _add_workload_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--transport",
        choices=["ibverbs", "tcp"],
        default="ibverbs",
        help="Transport every proc must use: ibverbs or tcp (default: ibverbs).",
    )
    parser.add_argument(
        "--payload-size-mb",
        type=float,
        default=1024,
        help=(
            "Size of the payload per op per edge between lanes, in MB (default: 1024)."
        ),
    )
    parser.add_argument(
        "--concurrent-ops",
        type=int,
        default=1,
        help=(
            "Number of concurrent ops per edge between lanes. An initiator "
            "batches every edge it drives and all of its ops into one "
            "RDMAAction per iteration (default: 1)."
        ),
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help=(
            "Number of times to run against freshly allocated buffers. Each "
            "run does --warmup-iters-per-run + --warm-iters-per-run "
            "iterations (default: 3)."
        ),
    )
    parser.add_argument(
        "--warmup-iters-per-run",
        type=int,
        default=3,
        help=(
            "Number of iterations at the beginning of a run whose timings are "
            "discarded (default: 3)."
        ),
    )
    parser.add_argument(
        "--warm-iters-per-run",
        type=int,
        default=10,
        help=(
            "Number of iterations within a run whose timings are counted (default: 10)."
        ),
    )
    parser.add_argument(
        "--rdma-runtime-threads",
        type=int,
        default=None,
        help="How many threads the RDMA runtime should use (default: monarch's own).",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--gpu",
        action="store_true",
        default=False,
        help="Put both sides' buffers on gpu, which is the default.",
    )
    mode_group.add_argument(
        "--cpu",
        action="store_true",
        default=False,
        help="Put both sides' buffers on host memory.",
    )
    parser.add_argument(
        "--source-device",
        choices=["cpu", "gpu"],
        default=None,
        help=(
            "Where each proc's outgoing buffers reside. Defaults to the "
            "--gpu/--cpu setting, which sets both sides."
        ),
    )
    parser.add_argument(
        "--dest-device",
        choices=["cpu", "gpu"],
        default=None,
        help=(
            "Where each proc's incoming buffers reside. Defaults to the "
            "--gpu/--cpu setting, which sets both sides."
        ),
    )


def _add_reporting_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--verify",
        choices=VERIFY_MODES,
        default=VERIFY_SAMPLED,
        help=(
            "Whether, and to what extent, to validate each tensor's value "
            "after each iteration (default: sampled)."
        ),
    )
    parser.add_argument(
        "--verify-window-mb",
        type=float,
        default=1.0,
        help=(
            "Under --verify sampled, the size of the window to check at the "
            "front, middle and end of each tensor (default: 1.0)."
        ),
    )
    parser.add_argument(
        "--max-device-gb-per-proc",
        type=float,
        default=80.0,
        help=(
            "How much gpu memory each proc is entitled to. Validated before "
            "launching (default: 80)."
        ),
    )
    parser.add_argument(
        "--max-host-gb-per-host",
        type=float,
        default=256.0,
        help=(
            "How much host memory each host is entitled to. Validated "
            "before launching (default: 256)."
        ),
    )
    parser.add_argument(
        "--output-csv",
        default="/tmp/rdma_benchmark_results.csv",
        help=(
            "Path to the file where the output will live, one row per "
            "(pattern, direction, phase) (default: "
            "/tmp/rdma_benchmark_results.csv). Override it per parallel run so "
            "concurrent benchmarks do not clobber each other."
        ),
    )


def _add_subcommands(parser: argparse.ArgumentParser, *, batch: bool) -> None:
    """Attach the ``run`` / ``run-batch`` subcommands and their options."""

    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    run_parser = sub.add_parser(
        RUN_COMMAND,
        help="Drive the benchmark from this process, exiting non-zero on failure.",
    )
    run_parser.add_argument(
        "--local-only",
        action="store_true",
        default=False,
        help=(
            "Run the benchmark locally on the current host, provisioning no "
            "job. This uses a local proc mesh with a 'hosts' dimension whose "
            "size is equal to the number of requested hosts."
        ),
    )
    run_parser.add_argument(
        "--teardown-policy",
        choices=TEARDOWN_POLICIES,
        default=TEARDOWN_ALWAYS,
        help=(
            "When to tear the monarch job down once the benchmark is done "
            f"(default: {TEARDOWN_ALWAYS}). '{TEARDOWN_ON_FAILURE}' leaves a "
            "fully successful run's job up so the next run can reuse it via "
            "--cached-path, while still tearing down a failed one. "
            f"'{TEARDOWN_NEVER}' always leaves it up."
        ),
    )
    run_parser.add_argument(
        "--cached-path",
        default=None,
        help=(
            "Path to the monarch job's cached state, used to reconnect to a "
            "job instead of creating one. When unset (default) no cache is "
            "used and a fresh job is always created. Set it to a unique path "
            "per parallel benchmark run so concurrent runs do not collide."
        ),
    )

    if batch:
        sub.add_parser(
            BATCH_COMMAND,
            help=(
                "Submit the benchmark to run inside its own allocation and "
                "return immediately. Exits 0 once submitted: the scheduler "
                "exposes no completion status, so read the job's log or "
                "--output-csv for the result. --output-csv is written by the "
                "in-allocation client, so point it somewhere still visible "
                "to the launching node."
            ),
        )


def config_from_args(args: argparse.Namespace) -> BenchConfig:
    """Build a :py:class:`BenchConfig` from :py:func:`add_benchmark_args` flags."""
    # --gpu/--cpu set both sides; --source-device/--dest-device override
    # each side independently.
    default_on_gpu: bool = not args.cpu
    cfg = BenchConfig(
        transport=args.transport,
        pattern=args.pattern,
        num_hosts=int(args.num_hosts),
        lane_pairing=args.lane_pairing,
        lane_shift=int(args.lane_shift),
        payload_size_mb=float(args.payload_size_mb),
        concurrent_ops=int(args.concurrent_ops),
        runs=int(args.runs),
        warmup_iters_per_run=int(args.warmup_iters_per_run),
        warm_iters_per_run=int(args.warm_iters_per_run),
        procs_per_host=int(args.procs_per_host),
        source_on_gpu=(
            args.source_device == "gpu" if args.source_device else default_on_gpu
        ),
        dest_on_gpu=(args.dest_device == "gpu" if args.dest_device else default_on_gpu),
        verify=args.verify,
        verify_window_mb=float(args.verify_window_mb),
        max_device_gb_per_proc=float(args.max_device_gb_per_proc),
        max_host_gb_per_host=float(args.max_host_gb_per_host),
        rdma_runtime_threads=args.rdma_runtime_threads,
        output_csv=args.output_csv,
        command=args.command,
        # These exist only on the `run` subparser.
        local_only=getattr(args, "local_only", False),
        cached_path=getattr(args, "cached_path", None),
        teardown_policy=getattr(args, "teardown_policy", TEARDOWN_ALWAYS),
    )
    if cfg.warmup_iters_per_run < 1:
        raise ValueError(
            "--warmup-iters-per-run must be at least 1, so that the cold "
            "iteration takes a discarded slot rather than a warm one"
        )
    if cfg.warm_iters_per_run < 1:
        raise ValueError("--warm-iters-per-run must be at least 1")
    if cfg.runs < 1:
        raise ValueError("--runs must be at least 1")
    return cfg


def _shape_columns(
    cfg: BenchConfig, topo: Topology, plan: MemoryPlan, direction: str
) -> ShapeColumns:
    return ShapeColumns(
        num_hosts=topo.num_hosts,
        procs_per_host=topo.procs_per_host,
        lane_pairing=topo.pairing,
        lane_shift=topo.shift,
        num_edges=len(topo.edges),
        num_initiators=len(topo.initiators(direction)),
        max_degree=topo.max_degree(direction),
        max_ops_per_action=max_ops_per_action(topo, direction, ops=cfg.concurrent_ops),
        max_in_degree=max((topo.in_degree(s) for s in topo.slots()), default=0),
        bytes_per_iteration=bytes_per_iteration(
            topo, ops=cfg.concurrent_ops, payload_bytes=cfg.payload_bytes
        ),
        max_buffers_per_proc=max(plan.buffers.values(), default=0),
        max_device_bytes_per_proc=max(plan.device_bytes.values(), default=0),
        max_host_bytes_per_host=max(plan.host_bytes_per_host.values(), default=0),
    )


def _config_columns(cfg: BenchConfig, record: RunRecord) -> ConfigColumns:
    return ConfigColumns(
        schema_version=SCHEMA_VERSION,
        transport=cfg.transport,
        source_device=cfg.source_label,
        dest_device=cfg.dest_label,
        payload_size_mb=cfg.payload_size_mb,
        concurrent_ops=cfg.concurrent_ops,
        # One generation of procs, so `cold_qp` has exactly one iteration behind
        # it however many runs there were.
        cold_proc_runs=1,
        runs=cfg.runs,
        warmup_iters_per_run=cfg.warmup_iters_per_run,
        warm_iters_per_run=cfg.warm_iters_per_run,
        local_only=int(cfg.local_only),
        verify_mode=cfg.verify,
        rdma_runtime_threads=str(get_global_config()["rdma_runtime_worker_threads"]),
        integrity_ok=_flag(record.integrity_ok),
        negative_control_ok=_flag(record.negative_control_ok),
    )


def _flag(value: bool | None) -> str:
    return "skipped" if value is None else str(value)


def _report(
    cfg: BenchConfig,
    topo: Topology,
    plan: MemoryPlan,
    records: dict[str, RunRecord],
) -> None:
    """Write the CSV and print the per-phase digest."""
    rows: list[Sequence[Any]] = []
    table: list[tuple[KeyColumns, MetricColumns]] = []
    for direction, record in records.items():
        shape = _shape_columns(cfg, topo, plan, direction)
        config = _config_columns(cfg, record)
        for phase in PHASES:
            key = KeyColumns(pattern=topo.pattern, direction=direction, phase=phase)
            metrics = metrics_for(phase, record)
            rows.append(summary_row(key, shape, config, metrics))
            table.append((key, metrics))

    with open(cfg.output_csv, "w") as stream:
        write_rows(stream, summary_header(), rows)

    print()
    for line in phase_table(table):
        print(line)
    print(f"\nResults written to {cfg.output_csv}")


def _print_banner(
    cfg: BenchConfig, topo: Topology, plan: MemoryPlan, banner: Sequence[str]
) -> None:
    print("=" * 78)
    print("RDMA Benchmark")
    print("=" * 78)
    for line in banner:
        print(line)
    for direction in DIRECTIONS:
        print(
            describe(
                topo,
                direction,
                ops=cfg.concurrent_ops,
                payload_bytes=cfg.payload_bytes,
            )
        )
    idle = unused_hosts(topo)
    if idle:
        print(f"Hosts this pattern never touches: {list(idle)}")
    print(
        f"Memory: source={cfg.source_label} dest={cfg.dest_label}; "
        f"up to {max(plan.buffers.values(), default=0)} buffers per proc, "
        f"{_gb(max(plan.device_bytes.values(), default=0))} device per proc, "
        f"{_gb(max(plan.host_bytes_per_host.values(), default=0))} host per host"
    )
    print(
        f"Runs: {cfg.runs} x ({cfg.warmup_iters_per_run} ramp + "
        f"{cfg.warm_iters_per_run} warm) iterations, first iteration "
        f"cold; verify={cfg.verify}"
    )
    print(f"Transport: {cfg.transport}  |  Output: {cfg.output_csv}")


def _gb(num_bytes: int) -> str:
    return f"{num_bytes / _GB:.2f} GB"
