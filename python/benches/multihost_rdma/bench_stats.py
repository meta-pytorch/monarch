#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Measurements and reporting for the multi-host RDMA benchmark.

The benchmark's timings nest three deep. An *iteration* is one ``RDMAAction``
per initiator, submitted and awaited. A *run* is a set of freshly allocated
tensors and freshly registered RDMA buffers, exercised for several iterations. A
*phase* is a contiguous range of iterations within a run, pooled across runs, so
that a first iteration against fresh buffers is never averaged together with a
steady-state one.

Two clocks are in play and the difference matters. A :py:class:`Sample` is what
one initiator measured on its own clock. A *span* is what the driver measured on
its clock: from releasing every initiator to receiving the last reply.
Per-initiator throughput comes from the first, aggregate throughput from the
second.

Aggregate throughput is deliberately ``bytes_per_iteration / span``, not a sum
of per-initiator rates. A sum of rates cannot be checked against any wall clock
and silently assumes the initiators overlapped perfectly, so it flatters a
skewed run. Dividing by the span is measured rather than reconstructed, and it
is conservative: it includes release skew and the reply messages. That excess is
reported as ``overhead_ms`` so a reader knows exactly how pessimistic the
aggregate is, and ``submit_ms_std`` shows whether the initiators were balanced.

A cold iteration's time is dominated by one-time setup rather than by moving
bytes, so its throughput is not reported.

Every duration here is in milliseconds. Like :py:mod:`bench_topology`, this
module imports only the standard library.
"""

from __future__ import annotations

import csv
import math
import statistics
from dataclasses import dataclass, field, fields
from typing import Any, Iterable, Sequence, TextIO

from bench_topology import PHASES, Slot, WARM


SCHEMA_VERSION: int = 2


def _gbs(num_bytes: int, milliseconds: float) -> float:
    """Decimal GB/s from a byte count and a duration in milliseconds."""
    return num_bytes / (milliseconds * 1e6)


@dataclass(frozen=True)
class Sample:
    """One initiator's measurement of one iteration, on the initiator's clock.

    ``build_ms`` is the Python-side cost of assembling the ``RDMAAction``: one
    call per op, before any byte moves. ``submit_ms`` runs from handing that
    action to the RDMA layer until every op in it has completed, so it is the
    only part that measures the fabric. They are reported separately and only
    ``submit_ms`` is ever a throughput denominator.

    The slot travels with the measurement because how many bytes it stands for
    depends on how that slot fits into the topology of the run.
    """

    slot: Slot
    build_ms: float
    submit_ms: float

    @property
    def total_ms(self) -> float:
        """Everything the initiator spent, which the driver's span must cover."""
        return self.build_ms + self.submit_ms


def percentile(values: Sequence[float], pct: float) -> float:
    """Nearest-rank percentile, no interpolation.

    The same definition ``python/benches/rdma_orchestration/benchmark.py`` uses,
    so numbers from the two benchmarks are comparable.
    """
    if not values:
        raise ValueError("cannot take a percentile of no samples")
    ordered = sorted(values)
    rank = math.ceil(pct / 100.0 * len(ordered))
    return ordered[max(rank, 1) - 1]


@dataclass(frozen=True)
class Stats:
    """A sample set's shape. ``n`` travels with it so a single-sample median is
    never mistaken for a converged one."""

    n: int
    median: float
    mean: float
    stdev: float
    p90: float
    maximum: float


def summarize(values: Sequence[float]) -> Stats:
    """Statistics over one sample set. Empty input gives an all-zero
    ``Stats`` with ``n == 0``, which is how the CSV records a phase that
    produced nothing."""
    if not values:
        return Stats(n=0, median=0.0, mean=0.0, stdev=0.0, p90=0.0, maximum=0.0)
    return Stats(
        n=len(values),
        median=statistics.median(values),
        mean=statistics.mean(values),
        stdev=statistics.stdev(values) if len(values) > 1 else 0.0,
        p90=percentile(values, 90),
        maximum=max(values),
    )


@dataclass
class PhaseRecord:
    """Every measurement belonging to one phase of one ``(pattern, direction)``.

    A *phase* is a contiguous range of iterations within a run, pooled across
    the runs it appears in. Iteration 0 of a run against freshly spawned procs
    is ``cold_qp``; a run's first ``warmup_iters_per_run`` iterations are the
    ``ramp``, which is discarded; the rest are ``warm``.
    :py:func:`bench_topology.phase_of` is that mapping, and it is the only place
    the boundaries are decided.

    ``build_ms`` and ``submit_ms`` hold one entry per initiator per iteration.
    ``span_ms``, ``overhead_ms``, and ``agg_gbs`` hold one entry per iteration,
    because one span covers every initiator at once.
    """

    build_ms: list[float] = field(default_factory=list)
    submit_ms: list[float] = field(default_factory=list)
    initiator_gbs: list[float] = field(default_factory=list)
    span_ms: list[float] = field(default_factory=list)
    agg_gbs: list[float] = field(default_factory=list)
    overhead_ms: list[float] = field(default_factory=list)

    def add_sample(self, sample: Sample, initiator_bytes: int) -> None:
        """Record one initiator's iteration. ``initiator_bytes`` is what that
        initiator moved, which depends on its degree."""
        self.build_ms.append(sample.build_ms)
        self.submit_ms.append(sample.submit_ms)
        if sample.submit_ms > 0:
            self.initiator_gbs.append(_gbs(initiator_bytes, sample.submit_ms))

    def add_iteration(
        self, span_ms: float, iteration_bytes: int, slowest_ms: float
    ) -> None:
        """Record the driver-clock span of one iteration.

        ``slowest_ms`` is the largest ``Sample.total_ms`` in that iteration, so
        the overhead is the part of the span no initiator was working during.
        """
        self.span_ms.append(span_ms)
        self.overhead_ms.append(span_ms - slowest_ms)
        if span_ms > 0:
            self.agg_gbs.append(_gbs(iteration_bytes, span_ms))


@dataclass
class RunRecord:
    """Everything the runs of one ``(pattern, direction)`` produced.

    A *run* is one set of freshly allocated tensors and freshly registered RDMA
    buffers, exercised for ``warmup_iters_per_run + warm_iters_per_run``
    iterations. Repeating a run is what gives ``register_ms`` more than one
    sample, but it does not reset a proc's queue pairs: those are keyed by peer
    and outlive any buffer, so only a run against freshly spawned procs pays to
    establish them. That is why another ``cold_qp`` sample costs a proc
    respawn, counted by ``cold_proc_runs``, and cannot be had by adding runs.

    Everything measured inside an iteration lives in ``phases``.
    ``register_ms`` is the run's setup cost, kept out of every throughput
    denominator and reported in its own columns.
    """

    # What registration costs: with the run's tensors already allocated, how
    # long constructing all of that proc's RDMA buffers takes. Measured on the
    # actor, one entry per run per proc, so the spread across procs shows how
    # the cost tracks a proc's buffer count. Allocation itself is excluded.
    register_ms: list[float] = field(default_factory=list)
    phases: dict[str, PhaseRecord] = field(
        default_factory=lambda: {phase: PhaseRecord() for phase in PHASES}
    )
    integrity_ok: bool | None = None
    negative_control_ok: bool | None = None

    def record(self, phase: str) -> PhaseRecord:
        """The record for ``phase``, or a throwaway for a discarded ramp
        iteration."""
        return self.phases.get(phase, PhaseRecord())


@dataclass(frozen=True)
class KeyColumns:
    """What a row identifies. Leading, so the CSV is scannable."""

    pattern: str
    direction: str
    phase: str


@dataclass(frozen=True)
class ShapeColumns:
    """The graph the row measured, and what it cost to hold."""

    num_hosts: int
    procs_per_host: int
    lane_pairing: str
    lane_shift: int
    num_edges: int
    num_initiators: int
    max_degree: int
    max_ops_per_action: int
    max_in_degree: int
    bytes_per_iteration: int
    max_buffers_per_proc: int
    max_device_bytes_per_proc: int
    max_host_bytes_per_host: int


@dataclass(frozen=True)
class ConfigColumns:
    """The invocation's provenance: enough to reproduce the row.

    ``transport`` is the requested one, which the driver asserts every proc
    actually resolved to, so it names both what was asked for and what ran.
    """

    schema_version: int
    transport: str
    source_device: str
    dest_device: str
    payload_size_mb: float
    concurrent_ops: int
    cold_proc_runs: int
    runs: int
    warmup_iters_per_run: int
    warm_iters_per_run: int
    local_only: int
    verify_mode: str
    rdma_runtime_threads: str
    rdma_max_nics_per_buffer: str
    integrity_ok: str
    negative_control_ok: str


@dataclass(frozen=True)
class MetricColumns:
    """One phase's numbers. Throughput is ``None`` -- an empty CSV cell -- on
    cold rows."""

    # One span per iteration, but one sample per initiator per iteration, so
    # these are equal only for a single-initiator pattern. Both are reported
    # because `submit_ms_*` is backed by the samples and `agg_gbs_*` by the
    # spans, and a phase can be thin in one and not the other.
    n_samples: int
    n_spans: int
    register_ms_median: float
    register_ms_p90: float
    build_ms_median: float
    build_ms_p90: float
    submit_ms_median: float
    submit_ms_mean: float
    submit_ms_std: float
    submit_ms_p90: float
    submit_ms_max: float
    span_ms_median: float
    span_ms_p90: float
    overhead_ms_median: float
    overhead_ms_p90: float
    initiator_gbs_median: float | None
    initiator_gbs_mean: float | None
    initiator_gbs_std: float | None
    initiator_gbs_p90: float | None
    agg_gbs_median: float | None
    agg_gbs_mean: float | None
    agg_gbs_std: float | None
    agg_gbs_p90: float | None


def metrics_for(phase: str, runs: RunRecord) -> MetricColumns:
    """Turn one phase's raw measurements into its row.

    Blanks the throughput cells on every phase but ``warm``: a cold iteration
    pays for fresh registrations, and the first one in a process also pays to
    establish its queue pairs, so bytes divided by that time is not a bandwidth.
    """
    record = runs.phases[phase]
    register = summarize(runs.register_ms)
    build = summarize(record.build_ms)
    submit = summarize(record.submit_ms)
    span = summarize(record.span_ms)
    overhead = summarize(record.overhead_ms)
    initiator = summarize(record.initiator_gbs)
    aggregate = summarize(record.agg_gbs)
    warm = phase == WARM
    return MetricColumns(
        n_samples=submit.n,
        n_spans=span.n,
        register_ms_median=register.median,
        register_ms_p90=register.p90,
        build_ms_median=build.median,
        build_ms_p90=build.p90,
        submit_ms_median=submit.median,
        submit_ms_mean=submit.mean,
        submit_ms_std=submit.stdev,
        submit_ms_p90=submit.p90,
        submit_ms_max=submit.maximum,
        span_ms_median=span.median,
        span_ms_p90=span.p90,
        overhead_ms_median=overhead.median,
        overhead_ms_p90=overhead.p90,
        initiator_gbs_median=initiator.median if warm else None,
        initiator_gbs_mean=initiator.mean if warm else None,
        initiator_gbs_std=initiator.stdev if warm else None,
        initiator_gbs_p90=initiator.p90 if warm else None,
        agg_gbs_median=aggregate.median if warm else None,
        agg_gbs_mean=aggregate.mean if warm else None,
        agg_gbs_std=aggregate.stdev if warm else None,
        agg_gbs_p90=aggregate.p90 if warm else None,
    )


_SUMMARY_PARTS: tuple[Any, ...] = (
    KeyColumns,
    ShapeColumns,
    ConfigColumns,
    MetricColumns,
)


def _names(parts: Iterable[Any]) -> tuple[str, ...]:
    return tuple(f.name for part in parts for f in fields(part))


def _values(parts: Iterable[Any]) -> tuple[Any, ...]:
    return tuple(getattr(part, f.name) for part in parts for f in fields(part))


def summary_header() -> tuple[str, ...]:
    """The summary CSV's columns, derived from the row dataclasses so the
    header and the rows cannot drift apart."""
    return _names(_SUMMARY_PARTS)


def summary_row(
    key: KeyColumns,
    shape: ShapeColumns,
    config: ConfigColumns,
    metrics: MetricColumns,
) -> tuple[Any, ...]:
    """One summary CSV row, in ``summary_header`` order."""
    return _values((key, shape, config, metrics))


def write_rows(
    stream: TextIO, header: Sequence[str], rows: Iterable[Sequence[Any]]
) -> None:
    """Write the complete record of every row as a CSV.

    ``None`` becomes an empty cell, so a consumer reads a cold row's throughput
    as missing rather than as zero. :py:func:`phase_table` renders that same
    absence as a dash, because a dash is legible where an empty column is not,
    and an empty cell parses where a dash does not.
    """
    writer = csv.writer(stream)
    writer.writerow(header)
    writer.writerows(rows)


def _cell(value: float | None, width: int, digits: int = 2) -> str:
    """A right-aligned number, or a dash where there is none. The CSV writes the
    same absence as an empty cell; see :py:func:`write_rows`."""
    return f"{'-':>{width}}" if value is None else f"{value:>{width}.{digits}f}"


def phase_table(
    rows: Sequence[tuple[KeyColumns, MetricColumns]],
) -> list[str]:
    """An abridged, human-readable digest of one pattern's results.

    One line per ``(direction, phase)``, carrying only the numbers worth reading
    at a glance; the CSV stays the complete record. The driver prints one of
    these per pattern, under a banner naming the shape that produced it, which
    is why the shape appears nowhere in the table itself and why every row must
    come from the same pattern.

    Cold rows show a dash where a warm row shows throughput, so a reader cannot
    mistake a setup cost for a bandwidth.
    """
    patterns = {key.pattern for key, _ in rows}
    assert len(patterns) <= 1, f"one table per pattern, got {sorted(patterns)}"
    header = (
        f"{'Dir':<6}{'Phase':<9}{'n':>5}{'build p50 (ms)':>15}"
        f"{'submit p50 (ms)':>16}{'submit p90 (ms)':>16}{'span p50 (ms)':>14}"
        f"{'ovhd p50 (ms)':>14}{'init (GB/s)':>12}{'agg (GB/s)':>11}"
    )
    lines = [header, "-" * len(header)]
    for key, m in rows:
        lines.append(
            f"{key.direction:<6}{key.phase:<9}{m.n_samples:>5}"
            f"{m.build_ms_median:>15.2f}{m.submit_ms_median:>16.2f}"
            f"{m.submit_ms_p90:>16.2f}{m.span_ms_median:>14.2f}"
            f"{m.overhead_ms_median:>14.2f}"
            f"{_cell(m.initiator_gbs_median, 12)}{_cell(m.agg_gbs_median, 11)}"
        )
    return lines


def cold_warm_ratio(runs: RunRecord, cold_phase: str) -> float | None:
    """How much slower a cold iteration was than a warm one. ``None`` when either
    pair has no samples."""
    cold = runs.phases[cold_phase].submit_ms
    warm = runs.phases[WARM].submit_ms
    if not cold or not warm:
        return None
    warm_median = statistics.median(warm)
    if warm_median <= 0:
        return None
    return statistics.median(cold) / warm_median
