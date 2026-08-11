#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Multi-host RDMA Performance Benchmark, independent of any job scheduler.

Measures RDMA transfer throughput and latency between actors on a
configurable proc mesh, using either ibverbs or TCP transport.

Each configuration is run twice: once driving RDMA READ (peer-as-initiator,
pulling our local tensors) and once driving RDMA WRITE (us-as-initiator,
pushing into the peer's RDMA buffers). Both directions move data in the
same logical direction (A's tensors -> B's tensors); only the initiator
of the ibverbs op changes.

Each side's memory kind is set independently with ``--sender-device`` and
``--receiver-device``; both default to the ``--gpu``/``--cpu`` setting.

The host shape is selected with mutually-exclusive ``--cross-host`` (default)
or ``--same-host`` flags.

Wrappers expose two subcommands: ``run`` drives the benchmark from the calling
process, and ``run-batch`` submits it to run inside its own allocation. Shared
flags precede the subcommand; the flags that only mean something to a driving
process belong to ``run``.

This module knows nothing about how the hosts are provisioned. A wrapper
supplies a fully-configured :py:class:`~monarch.job.JobTrait` through
:py:func:`run`, which only ever calls ``state()``, ``spawn_procs``, ``kill()``,
and — for ``run-batch`` — ``apply()`` on it. See ``slurm_benchmark.py`` for an
open-source wrapper.
"""

from __future__ import annotations

import argparse
import asyncio
import shlex
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

import monarch
import torch
import xxhash
from monarch.actor import Actor, current_rank, endpoint, ProcMesh, this_host
from monarch.rdma import RDMAAction, RDMABuffer

# Avoid importing `monarch.job` at module scope. Workers import this module
# to unpickle `TestRDMA`, but the worker conda environments might not have
# all of the necessary dependencies.
if TYPE_CHECKING:
    from monarch.job import JobTrait


DIRECTIONS: tuple[str, ...] = ("read", "write")

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


class TestRDMA(Actor):
    """RDMA test actor for benchmarking performance between hosts."""

    def __init__(self) -> None:
        self.tensors: list[torch.Tensor] = []
        self.rdma_buffers: list[RDMABuffer] = []
        self.receiving_actor: Any = None
        self.gpu_id: int = current_rank()["gpus"]

    @endpoint
    async def receiving_actors(self, actors: Any) -> None:
        self.receiving_actor = actors.slice(**current_rank())

    @endpoint
    async def alloc(
        self, tensor_shape: tuple[int, ...], concurrent_ops: int, use_gpu: bool
    ) -> None:
        """Allocate this side's tensors and register them as RDMA buffers.

        ``use_gpu`` selects this side's memory kind, so the sender and the
        receiver can be allocated on different kinds.

        Every tensor is allocated before any buffer is registered. Interleaving
        the two makes each registration cover an allocator segment that the next
        allocation then grows, so the registrations cover ever-larger segments.
        """
        for buf in self.rdma_buffers:
            await buf.drop()
        self.tensors = []
        self.rdma_buffers = []
        device = f"cuda:{self.gpu_id}" if use_gpu else "cpu"
        for _ in range(concurrent_ops):
            self.tensors.append(
                torch.rand(tensor_shape, dtype=torch.float32, device=device)
            )
        for tensor in self.tensors:
            self.rdma_buffers.append(
                RDMABuffer(tensor.view(dtype=torch.uint8).flatten())
            )

    @endpoint
    async def expose_buffers(self) -> list[RDMABuffer]:
        return self.rdma_buffers

    @endpoint
    async def timed_read(self, remote_buffers: list[RDMABuffer]) -> float:
        """Pull from ``remote_buffers`` into local tensors via a single
        ``RDMAAction``. Returns the wall time of that batch.

        This is the inner timed step of the READ benchmark: the *peer*
        actor invokes this, so the peer is the initiator of the ibverbs
        RDMA-READ ops."""
        assert len(remote_buffers) == len(self.tensors), (
            f"timed_read received {len(remote_buffers)} buffers but actor was "
            f"allocated with {len(self.tensors)} tensors"
        )
        t = time.perf_counter()
        action = RDMAAction()
        for tensor, buffer in zip(self.tensors, remote_buffers):
            action.read_remote(tensor.view(dtype=torch.uint8).flatten(), buffer)
        await action.submit()
        return time.perf_counter() - t

    @endpoint
    async def execute_round(self, sends_per_actor: int, direction: str) -> float:
        """Drive ``sends_per_actor`` iterations of ``direction`` against
        ``self.receiving_actor`` and return the cumulative ibverbs-op time.

        ``direction`` is ``"read"`` or ``"write"``. Both directions move
        data from our local tensors into the peer's buffers; the
        difference is which side initiates the ibverbs op.

        - ``read``: the peer (``self.receiving_actor``) runs ``timed_read``
          and pulls our buffers (peer-initiated RDMA READ).
        - ``write``: we fetch the peer's buffers via ``expose_buffers``
          and push our local tensors with ``RDMAAction.write_remote``
          (us-initiated RDMA WRITE).
        """
        total = 0.0
        if direction == "read":
            for _ in range(sends_per_actor):
                total += await self.receiving_actor.timed_read.call_one(
                    self.rdma_buffers
                )
        elif direction == "write":
            peer_buffers = await self.receiving_actor.expose_buffers.call_one()
            assert len(peer_buffers) == len(self.tensors), (
                f"peer exposed {len(peer_buffers)} buffers but we have "
                f"{len(self.tensors)} tensors"
            )
            for _ in range(sends_per_actor):
                t = time.perf_counter()
                action = RDMAAction()
                for tensor, buffer in zip(self.tensors, peer_buffers):
                    action.write_remote(
                        buffer, tensor.view(dtype=torch.uint8).flatten()
                    )
                await action.submit()
                total += time.perf_counter() - t
        else:
            raise ValueError(f"unknown direction: {direction!r}")
        return total

    @endpoint
    async def reset(self) -> None:
        for buf in self.rdma_buffers:
            await buf.drop()
        self.tensors = []
        self.rdma_buffers = []

    @endpoint
    async def tensor_hashes(self) -> list[str]:
        """Return the xxh64 hex digest of each tensor's raw bytes.

        Used to verify data integrity after an RDMA round: after a
        successful read or write, the receiver's tensors should hash
        to the same values as the sender's.
        """
        out: list[str] = []
        for t in self.tensors:
            arr = (
                t.detach().contiguous().view(dtype=torch.uint8).flatten().cpu().numpy()
            )
            out.append(xxhash.xxh64(arr.tobytes()).hexdigest())
        return out


@dataclass(frozen=True)
class BenchConfig:
    """Everything the benchmark needs that is not scheduler-specific."""

    transport: str
    payload_size_mb: float
    runs_per_config: int
    sends_per_actor: int
    concurrent_ops: int
    warmup: int
    procs_per_host: int
    sender_use_gpu: bool
    receiver_use_gpu: bool
    cross_host: bool
    local_sender: bool
    output_csv: str
    command: str
    cached_path: str | None
    teardown_policy: str

    @property
    def num_hosts(self) -> int:
        """Hosts taking part in the measurement."""
        return 2 if self.cross_host else 1

    @property
    def job_hosts(self) -> int:
        """Hosts the job must provision. Under ``--local-sender`` the client's
        own host supplies one side, so the job provides one fewer — and for the
        same-host shape it provides none, meaning no job at all."""
        return self.num_hosts - 1 if self.local_sender else self.num_hosts

    @property
    def sender_label(self) -> str:
        return "gpu" if self.sender_use_gpu else "cpu"

    @property
    def receiver_label(self) -> str:
        return "gpu" if self.receiver_use_gpu else "cpu"

    @property
    def shape_label(self) -> str:
        return "cross-host" if self.cross_host else "same-host"


def configure_transport(transport: str) -> None:
    """Configure RDMA transport backend."""
    if transport == "ibverbs":
        monarch.configure(rdma_allow_tcp_fallback=False)
    else:
        monarch.configure(rdma_disable_ibverbs=True, rdma_allow_tcp_fallback=True)


def calculate_statistics(
    values: list[float],
) -> tuple[float, float, float, float]:
    """Calculate median, mean, standard deviation, and p90."""
    if not values:
        return 0.0, 0.0, 0.0, 0.0

    median = statistics.median(values)
    mean = statistics.mean(values)
    std_dev = statistics.stdev(values) if len(values) > 1 else 0.0
    p90 = (
        statistics.quantiles(values, n=10)[8]
        if len(values) >= 10
        else sorted(values)[int(len(values) * 0.9)]
        if values
        else 0.0
    )

    return median, mean, std_dev, p90


async def setup_single_run(
    sending_actors: Any,
    receiving_actors: Any,
    tensor_size_mb: float,
    concurrent_ops: int,
    sender_use_gpu: bool,
    receiver_use_gpu: bool,
) -> tuple[int, float]:
    """Reset actors and allocate tensors. Returns (tensor_size, nbyte_gb)."""
    await sending_actors.reset.call()
    await receiving_actors.reset.call()

    tensor_size = int(tensor_size_mb * 1024 * 1024 // 4)
    nbyte_gb = tensor_size_mb / 1000

    await sending_actors.receiving_actors.call(receiving_actors)
    await sending_actors.alloc.call((tensor_size,), concurrent_ops, sender_use_gpu)
    await receiving_actors.alloc.call((tensor_size,), concurrent_ops, receiver_use_gpu)

    return tensor_size, nbyte_gb


async def run_single_configuration(
    sending_actors: Any,
    receiving_actors: Any,
    tensor_size_mb: float,
    runs_per_config: int,
    sends_per_actor: int,
    concurrent_ops: int,
    warmup: int,
    direction: str,
    sender_use_gpu: bool,
    receiver_use_gpu: bool,
) -> tuple[list[float], list[float], list[float], bool]:
    """Run benchmark for a single (config, direction) pair.

    ``sender_use_gpu`` and ``receiver_use_gpu`` select each side's memory
    kind and are forwarded to :py:func:`setup_single_run`.

    ``direction`` is ``"read"`` or ``"write"`` and is forwarded to
    :py:meth:`TestRDMA.execute_round`. Each timed run is preceded by
    ``warmup`` untimed iterations of the *same* direction so the
    first-send costs (MR registration, lazy QP work,
    page-faulting fresh allocations) are paid before measurement begins.

    Returns ``(per_actor_throughputs, per_actor_latencies,
    aggregate_throughputs, success)``. Aggregate throughput is the sum of
    all per-actor throughputs within a single run, representing total
    bandwidth across all initiator/peer pairs. Each "send" transfers
    ``concurrent_ops`` tensors via a single ``RDMAAction``, so a run's
    transferred bytes are scaled accordingly.
    """
    throughput_results: list[float] = []
    latency_results: list[float] = []
    aggregate_throughput_results: list[float] = []

    print(
        f"\nTesting {tensor_size_mb}MB payload, direction={direction!r}, "
        f"{runs_per_config} timed runs, {sends_per_actor} sends per actor, "
        f"{warmup} warmup sends per run, {concurrent_ops} concurrent ops..."
    )

    for run_idx in range(runs_per_config):
        should_print = (run_idx + 1) % 10 == 0 or (run_idx + 1) == runs_per_config

        try:
            _tensor_size, nbyte_gb = await setup_single_run(
                sending_actors,
                receiving_actors,
                tensor_size_mb,
                concurrent_ops,
                sender_use_gpu,
                receiver_use_gpu,
            )

            if warmup > 0:
                await sending_actors.execute_round.call(warmup, direction)

            results_mesh = await sending_actors.execute_round.call(
                sends_per_actor, direction
            )

            # Verify data integrity: after read or write, each
            # receiver actor's tensors should be byte-identical to
            # the paired sender actor's tensors. We use xxh64 of each
            # tensor's raw bytes so we can compare per-tensor without
            # round-tripping the data through Python. Pair sender and
            # receiver actors by iteration order: both meshes have the
            # same per-host shape (procs-per-host), so the i-th
            # iteration entry on each side maps to the same logical
            # rank.
            sender_hashes_mesh = await sending_actors.tensor_hashes.call()
            receiver_hashes_mesh = await receiving_actors.tensor_hashes.call()
            sender_items = list(sender_hashes_mesh.items())
            receiver_items = list(receiver_hashes_mesh.items())
            if len(sender_items) != len(receiver_items):
                raise RuntimeError(
                    f"actor count mismatch: "
                    f"sender={len(sender_items)}, "
                    f"receiver={len(receiver_items)}"
                )
            for (sender_point, sender_hashes), (
                receiver_point,
                receiver_hashes,
            ) in zip(sender_items, receiver_items):
                rank = f"sender={sender_point} receiver={receiver_point}"
                if len(sender_hashes) != len(receiver_hashes):
                    raise RuntimeError(
                        f"hash count mismatch on {rank}: "
                        f"sender={len(sender_hashes)}, "
                        f"receiver={len(receiver_hashes)}"
                    )
                mismatches: list[int] = [
                    i
                    for i, (s, r) in enumerate(zip(sender_hashes, receiver_hashes))
                    if s != r
                ]
                if mismatches:
                    preview = mismatches[:10]
                    raise RuntimeError(
                        f"DATA CORRUPTION on {rank}: "
                        f"{len(mismatches)} of {len(sender_hashes)} "
                        f"tensors differ after {direction} on run "
                        f"{run_idx + 1}; first mismatched indices = "
                        f"{preview}; e.g. tensor[{preview[0]}]: "
                        f"sender={sender_hashes[preview[0]]}, "
                        f"receiver={receiver_hashes[preview[0]]}"
                    )

            run_throughputs: list[float] = []
            for _rank, receiver_time in results_mesh.items():
                if receiver_time and receiver_time > 0:
                    throughput_gbs = (
                        nbyte_gb * sends_per_actor * concurrent_ops
                    ) / receiver_time
                    latency_ms = receiver_time * 1000
                    throughput_results.append(throughput_gbs)
                    latency_results.append(latency_ms)
                    run_throughputs.append(throughput_gbs)

            if run_throughputs:
                aggregate_throughput_results.append(sum(run_throughputs))

            if should_print and run_throughputs:
                print(
                    f"  Run {run_idx + 1}/{runs_per_config} ({direction}) - "
                    f"Per-actor: {run_throughputs[-1]:.2f} GB/s, "
                    f"Aggregate: {sum(run_throughputs):.2f} GB/s, "
                    f"Latency: {latency_results[-1]:.2f} ms"
                )

        except Exception as e:
            print(f"  Run {run_idx + 1} ({direction}) failed: {e}")
            return (
                throughput_results,
                latency_results,
                aggregate_throughput_results,
                False,
            )

    return throughput_results, latency_results, aggregate_throughput_results, True


def print_summary_table(cfg: BenchConfig, rows: list[dict[str, Any]]) -> None:
    """Print a compact summary table of all configurations."""
    if not rows:
        return

    shape_width = max(8, max(len(r["shape"]) for r in rows))
    direction_width = max(9, max(len(r["direction"]) for r in rows))

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"  Memory: sender={cfg.sender_label}  |  receiver={cfg.receiver_label}")
    print(
        f"  Transport: {cfg.transport}  |  Payload: {cfg.payload_size_mb} MB  |  "
        f"Sends/actor: {cfg.sends_per_actor}  |  Concurrent ops: {cfg.concurrent_ops}  |  "
        f"Runs/config: {cfg.runs_per_config}"
    )
    print(f"{'=' * 70}")
    header = (
        f"{'Config':>{shape_width}}  {'Direction':>{direction_width}}  "
        f"{'Per-actor (GB/s)':>16}  {'Aggregate (GB/s)':>17}  "
        f"{'Latency med (ms)':>17}  {'Latency p90 (ms)':>17}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['shape']:>{shape_width}}  {r['direction']:>{direction_width}}  "
            f"{r['tp_med']:>16.2f}  {r['agg_med']:>17.2f}  "
            f"{r['lat_med']:>17.2f}  {r['lat_p90']:>17.2f}"
        )
    print()


def add_benchmark_args(parser: argparse.ArgumentParser, *, batch: bool = True) -> None:
    """Add the scheduler-independent flags, then the subcommands.

    Wrappers add their own flags on top; because those land on the parent parser
    they are given before the subcommand, as in
    ``slurm_benchmark.py --partition x run --teardown-policy on_failure``.

    Set ``batch=False`` for a backend that cannot run the benchmark inside its
    own allocation, which drops the ``run-batch`` subcommand entirely rather
    than accepting it and failing later.
    """
    parser.add_argument(
        "--transport",
        choices=["ibverbs", "tcp"],
        default="ibverbs",
        help="RDMA transport backend (default: ibverbs)",
    )
    parser.add_argument(
        "--payload-size-mb",
        type=float,
        default=1024,
        help="Payload size in MB (default: 1024)",
    )
    parser.add_argument(
        "--runs-per-config",
        type=int,
        default=3,
        help="Number of runs per configuration (default: 3)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=3,
        help=(
            "Number of untimed warmup sends issued against the freshly-allocated "
            "buffers before each timed run. Each timed run still does its own "
            "setup; warmup sends sit between setup and the timed loop so "
            "first-send costs like MR registration are amortized. "
            "Applied separately per direction (read and write each get this "
            "many warmup iterations) (default: 3)."
        ),
    )
    parser.add_argument(
        "--sends-per-actor",
        type=int,
        default=10,
        help="Number of sends per actor per run (default: 10)",
    )
    parser.add_argument(
        "--concurrent-ops",
        type=int,
        default=1,
        help=(
            "Number of concurrent RDMA ops per send. Each actor "
            "allocates this many tensors and uses one ``RDMAAction`` "
            "to transfer them in parallel (default: 1)."
        ),
    )
    parser.add_argument(
        "--procs-per-host",
        type=int,
        default=8,
        help="GPU processes per host (default: 8)",
    )
    parser.add_argument(
        "--output-csv",
        default="/tmp/rdma_benchmark_results.csv",
        help=(
            "Destination CSV for per-direction results (default: "
            "/tmp/rdma_benchmark_results.csv). Override per parallel run "
            "so concurrent benchmarks don't clobber each other's results."
        ),
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--gpu", action="store_true", default=False)
    mode_group.add_argument("--cpu", action="store_true", default=False)

    parser.add_argument(
        "--sender-device",
        choices=["cpu", "gpu"],
        default=None,
        help=(
            "Memory kind for the sending actors' tensors. Defaults to the "
            "--gpu/--cpu setting, which applies to both sides."
        ),
    )
    parser.add_argument(
        "--receiver-device",
        choices=["cpu", "gpu"],
        default=None,
        help=(
            "Memory kind for the receiving actors' tensors. Defaults to the "
            "--gpu/--cpu setting, which applies to both sides."
        ),
    )

    shape_group = parser.add_mutually_exclusive_group()
    shape_group.add_argument(
        "--cross-host",
        action="store_true",
        default=False,
        help="Run only the cross-host configuration (default).",
    )
    shape_group.add_argument(
        "--same-host",
        action="store_true",
        default=False,
        help="Run only the same-host (loopback) configuration instead of cross-host.",
    )

    _add_subcommands(parser, batch=batch)


def _add_subcommands(parser: argparse.ArgumentParser, *, batch: bool) -> None:
    """Attach the ``run`` / ``run-batch`` subcommands.

    Everything above is shared and so belongs before the subcommand on the
    command line. The flags below apply only when this process drives the
    benchmark, which is why none of them exist on ``run-batch``.
    """
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    run_parser = sub.add_parser(
        RUN_COMMAND,
        help="Drive the benchmark from this process, exiting non-zero if a direction fails.",
    )
    run_parser.add_argument(
        "--local-sender",
        action="store_true",
        default=False,
        help="Run sending actors on the client host via this_host(). "
        "Without this flag, all actors run on the job's hosts.",
    )
    run_parser.add_argument(
        "--teardown-policy",
        choices=TEARDOWN_POLICIES,
        default=TEARDOWN_ALWAYS,
        help=(
            f"When to kill the job (default: {TEARDOWN_ALWAYS}). "
            f"'{TEARDOWN_ON_FAILURE}' leaves a fully successful run's job up so "
            "the next run can reuse it via --cached-path, while still tearing "
            f"down a failed or partial one. '{TEARDOWN_NEVER}' always leaves it "
            f"up, and is what {BATCH_COMMAND} passes its in-allocation client, "
            "whose runner owns the allocation."
        ),
    )
    run_parser.add_argument(
        "--cached-path",
        default=None,
        help=(
            "Path to a pickle file used to cache + reconnect to the job state. "
            "When unset (default), an explicit None is passed so no cache is "
            "used and a fresh job is always created. Set this to a unique path "
            "per parallel benchmark run so concurrent runs don't collide on the "
            "same cache."
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
                "afterwards — the default lives in /tmp, which is node-local."
            ),
        )


def config_from_args(args: argparse.Namespace) -> BenchConfig:
    """Build a :py:class:`BenchConfig` from :py:func:`add_benchmark_args` flags."""
    # --gpu/--cpu set both sides; --sender-device/--receiver-device override
    # each side independently.
    default_use_gpu: bool = not args.cpu
    return BenchConfig(
        transport=args.transport,
        payload_size_mb=float(args.payload_size_mb),
        runs_per_config=int(args.runs_per_config),
        sends_per_actor=int(args.sends_per_actor),
        concurrent_ops=int(args.concurrent_ops),
        warmup=int(args.warmup),
        procs_per_host=int(args.procs_per_host),
        sender_use_gpu=(
            args.sender_device == "gpu" if args.sender_device else default_use_gpu
        ),
        receiver_use_gpu=(
            args.receiver_device == "gpu" if args.receiver_device else default_use_gpu
        ),
        cross_host=not args.same_host,
        output_csv=args.output_csv,
        command=args.command,
        # These exist only on the `run` subparser.
        local_sender=getattr(args, "local_sender", False),
        cached_path=getattr(args, "cached_path", None),
        teardown_policy=getattr(args, "teardown_policy", TEARDOWN_ALWAYS),
    )


def _spawn_job_mesh(job: JobTrait, mesh_name: str, cfg: BenchConfig) -> ProcMesh:
    state = job.state(cached_path=cfg.cached_path)
    return getattr(state, mesh_name).spawn_procs(per_host={"gpus": cfg.procs_per_host})


async def _drive(
    cfg: BenchConfig,
    job: JobTrait | None,
    mesh_name: str,
    banner: Sequence[str],
) -> tuple[int, int]:
    """Run every direction and return ``(successful, total)``."""
    configure_transport(cfg.transport)

    print("=" * 70)
    print("RDMA Benchmark")
    print("=" * 70)
    for line in banner:
        print(line)
    print(f"Transport: {cfg.transport}")
    print(f"Payload: {cfg.payload_size_mb} MB")
    print(f"Runs per config: {cfg.runs_per_config}")
    print(f"Sends per actor: {cfg.sends_per_actor}")
    print(f"Warmup: {cfg.warmup}")
    print(f"Concurrent ops: {cfg.concurrent_ops}")
    print(f"Procs per host: {cfg.procs_per_host}")
    print(f"Memory: sender={cfg.sender_label}, receiver={cfg.receiver_label}")
    print(f"Host shape: {cfg.shape_label}")
    print(f"Directions: {', '.join(DIRECTIONS)}")
    print(f"Output: {cfg.output_csv}")

    with open(cfg.output_csv, "w") as f:
        f.write(
            "payload_size_mb,num_hosts,num_gpus_per_host,sends_per_actor,concurrent_ops,transport,"
            "sender_device,receiver_device,direction,"
            "per_actor_tp_median_gbs,per_actor_tp_avg_gbs,per_actor_tp_std_gbs,per_actor_tp_p90_gbs,"
            "aggregate_tp_median_gbs,aggregate_tp_avg_gbs,aggregate_tp_std_gbs,aggregate_tp_p90_gbs,"
            "latency_median_ms,latency_avg_ms,latency_std_ms,latency_p90_ms\n"
        )

    # Accumulate per-(shape, direction) results for the summary table.
    summary_rows: list[dict[str, Any]] = []

    successful = 0
    proc_meshes: list[ProcMesh] = []
    try:
        sending_actors: Any
        receiving_actors: Any

        if cfg.local_sender:
            local_proc_mesh = this_host().spawn_procs(
                per_host={"gpus": cfg.procs_per_host}
            )
            proc_meshes.append(local_proc_mesh)

            if cfg.cross_host:
                assert job is not None, "cross-host --local-sender needs a job"
                sending_actors = local_proc_mesh.spawn("local_actors", TestRDMA)
                job_proc_mesh = _spawn_job_mesh(job, mesh_name, cfg)
                proc_meshes.append(job_proc_mesh)
                receiving_actors = job_proc_mesh.spawn("remote_receivers", TestRDMA)
                print(
                    f"Local: 1x{cfg.procs_per_host}, "
                    f"remote: {cfg.job_hosts}x{cfg.procs_per_host} "
                    f"(sender={cfg.sender_label}, receiver={cfg.receiver_label}); "
                    f"shape={cfg.shape_label}"
                )
            else:
                sending_actors = local_proc_mesh.spawn("local_senders", TestRDMA)
                receiving_actors = local_proc_mesh.spawn("local_receivers", TestRDMA)
                print(
                    f"Local: 1x{cfg.procs_per_host} (sender={cfg.sender_label}, "
                    f"receiver={cfg.receiver_label}); "
                    f"shape={cfg.shape_label} (loopback)"
                )
        else:
            assert job is not None, "a job is required unless running fully local"
            job_proc_mesh = _spawn_job_mesh(job, mesh_name, cfg)
            proc_meshes.append(job_proc_mesh)

            print(
                f"Job: {cfg.job_hosts}x{cfg.procs_per_host} "
                f"(sender={cfg.sender_label}, receiver={cfg.receiver_label}); "
                f"shape={cfg.shape_label}"
            )

            if cfg.cross_host:
                actors = job_proc_mesh.spawn("rdma_test_actors", TestRDMA)
                sending_actors = actors.slice(hosts=0)
                receiving_actors = actors.slice(hosts=1)
            else:
                sending_actors = job_proc_mesh.spawn("rdma_senders", TestRDMA)
                receiving_actors = job_proc_mesh.spawn("rdma_receivers", TestRDMA)

        for direction in DIRECTIONS:
            print(f"\n{'=' * 70}")
            print(f"Configuration: {cfg.shape_label} / {direction}")
            print("=" * 70)

            try:
                (
                    throughputs,
                    latencies,
                    agg_throughputs,
                    success,
                ) = await run_single_configuration(
                    sending_actors,
                    receiving_actors,
                    cfg.payload_size_mb,
                    cfg.runs_per_config,
                    cfg.sends_per_actor,
                    cfg.concurrent_ops,
                    cfg.warmup,
                    direction,
                    cfg.sender_use_gpu,
                    cfg.receiver_use_gpu,
                )

                if not success or not throughputs:
                    print(f"Configuration {cfg.shape_label}/{direction} failed")
                    continue

                tp_med, tp_avg, tp_std, tp_p90 = calculate_statistics(throughputs)
                agg_med, agg_avg, agg_std, agg_p90 = calculate_statistics(
                    agg_throughputs
                )
                lat_med, lat_avg, lat_std, lat_p90 = calculate_statistics(latencies)

                with open(cfg.output_csv, "a") as f:
                    f.write(
                        f"{cfg.payload_size_mb},{cfg.num_hosts},{cfg.procs_per_host},"
                        f"{cfg.sends_per_actor},{cfg.concurrent_ops},"
                        f"{cfg.transport},{cfg.sender_label},{cfg.receiver_label},{direction},"
                        f"{tp_med:.4f},{tp_avg:.4f},{tp_std:.4f},{tp_p90:.4f},"
                        f"{agg_med:.4f},{agg_avg:.4f},{agg_std:.4f},{agg_p90:.4f},"
                        f"{lat_med:.4f},{lat_avg:.4f},{lat_std:.4f},{lat_p90:.4f}\n"
                    )

                summary_rows.append(
                    {
                        "shape": cfg.shape_label,
                        "direction": direction,
                        "tp_med": tp_med,
                        "agg_med": agg_med,
                        "lat_med": lat_med,
                        "lat_p90": lat_p90,
                    }
                )

                print(f"\nResults for {cfg.shape_label} / {direction}:")
                print(
                    f"  Per-actor throughput  - Median: {tp_med:.2f} GB/s, "
                    f"Avg: {tp_avg:.2f} GB/s, Std: {tp_std:.2f} GB/s, "
                    f"P90: {tp_p90:.2f} GB/s"
                )
                print(
                    f"  Aggregate throughput  - Median: {agg_med:.2f} GB/s, "
                    f"Avg: {agg_avg:.2f} GB/s, Std: {agg_std:.2f} GB/s, "
                    f"P90: {agg_p90:.2f} GB/s"
                )
                print(
                    f"  Latency              - Median: {lat_med:.2f} ms, "
                    f"Avg: {lat_avg:.2f} ms, Std: {lat_std:.2f} ms, "
                    f"P90: {lat_p90:.2f} ms"
                )

                successful += 1
                await asyncio.sleep(2)

            except Exception as e:
                print(f"Failed to run {cfg.shape_label}/{direction}: {e}")
                continue

        print(f"\n{'=' * 70}")
        print(f"BENCHMARK COMPLETED: {successful}/{len(DIRECTIONS)} directions")
        print(f"Results saved to: {cfg.output_csv}")
        print("=" * 70)

        print_summary_table(cfg, summary_rows)

        return successful, len(DIRECTIONS)

    except Exception as e:
        print(f"BENCHMARK FAILED: {e}")
        raise
    finally:
        # Stop the meshes before killing the job whose hosts they run on.
        for mesh in proc_meshes:
            try:
                await mesh.stop()
            except Exception as stop_e:
                print(f"Warning: Failed to stop proc mesh: {stop_e}")
        if job is not None:
            failed = successful < len(DIRECTIONS)
            teardown = cfg.teardown_policy == TEARDOWN_ALWAYS or (
                cfg.teardown_policy == TEARDOWN_ON_FAILURE and failed
            )
            if teardown:
                try:
                    job.kill()
                    print("Killed job")
                except Exception as kill_e:
                    print(f"Warning: Failed to kill job: {kill_e}")
            else:
                print(f"Leaving job running (--teardown-policy {cfg.teardown_policy})")


def _batch_client_command() -> str:
    """This same invocation, rewritten to be the in-allocation client.

    ``run-batch`` becomes ``run``, and the two flags that make the client attach
    to the surrounding allocation are appended: ``--cached-path`` so it reads the
    ``BatchJob`` the scheduler dumped there instead of submitting a second
    allocation from inside the first, and ``--teardown-policy never`` because the
    runner — not the client — owns the allocation.
    """
    from monarch.job import DEFAULT_JOB_PATH

    argv = sys.argv[1:]
    # The subcommand is a bare word, so a flag *value* could also equal it (e.g.
    # --output-csv run-batch). Rewriting the wrong one would silently submit a
    # nested batch job, so require it to be unambiguous.
    positions = [i for i, arg in enumerate(argv) if arg == BATCH_COMMAND]
    if len(positions) != 1:
        raise SystemExit(
            f"cannot rebuild the client command: {BATCH_COMMAND!r} appears "
            f"{len(positions)} times in the arguments; it must appear exactly "
            "once, as the subcommand"
        )
    argv[positions[0]] = RUN_COMMAND

    return shlex.join(
        [
            sys.executable,
            str(Path(sys.argv[0]).resolve()),
            *argv,
            "--cached-path",
            DEFAULT_JOB_PATH,
            "--teardown-policy",
            TEARDOWN_NEVER,
        ]
    )


def run(
    cfg: BenchConfig,
    *,
    make_job: Callable[[BenchConfig], JobTrait],
    mesh_name: str,
    banner: Sequence[str] = (),
) -> int:
    """Run or submit the benchmark against a job built by ``make_job``.

    Returns a process exit code.

    ``make_job`` is called only when hosts are actually needed, so the fully
    local ``--same-host --local-sender`` shape never provisions anything. It is
    called before any ``this_host()`` call, which matters because some job types
    configure the channel transport as a side effect of construction.
    """
    job: JobTrait | None = None
    if cfg.job_hosts > 0:
        job = make_job(cfg)

    if cfg.command == BATCH_COMMAND:
        # job_hosts is only 0 under --local-sender, which run-batch does not offer.
        assert job is not None
        job.apply(client_script=_batch_client_command())
        print(
            f"Submitted batch run; results will be written to {cfg.output_csv}. "
            "The scheduler reports no completion status, so check the job's log."
        )
        return 0

    successful, total = asyncio.run(_drive(cfg, job, mesh_name, banner))
    if successful < total:
        print(
            f"BENCHMARK FAILED: {successful}/{total} directions succeeded",
            flush=True,
        )
        return 1
    return 0
