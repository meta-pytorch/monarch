#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Tests for the multi-host RDMA benchmark's statistics and reporting.

``bench_stats`` imports only the standard library, so these run with no
cluster.
"""

from __future__ import annotations

import io

import bench_stats as bs
import bench_topology as bt
import pytest


GB: int = 1000**3


def test_percentile_is_nearest_rank() -> None:
    assert bs.percentile([5.0], 90) == 5.0
    assert bs.percentile([1.0, 2.0], 90) == 2.0
    assert bs.percentile([2.0, 1.0], 50) == 1.0, "sorts first"

    nine = [float(i) for i in range(1, 10)]
    assert bs.percentile(nine, 90) == 9.0
    ten = [float(i) for i in range(1, 11)]
    assert bs.percentile(ten, 90) == 9.0
    eleven = [float(i) for i in range(1, 12)]
    assert bs.percentile(eleven, 90) == 10.0

    assert bs.percentile(ten, 100) == 10.0
    assert bs.percentile(ten, 0) == 1.0, "rank clamps to the first element"

    with pytest.raises(ValueError, match="percentile of no samples"):
        bs.percentile([], 90)


def test_summarize_handles_thin_sample_sets() -> None:
    empty = bs.summarize([])
    assert empty.n == 0
    assert (empty.median, empty.mean, empty.stdev, empty.p90, empty.maximum) == (
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    )

    one = bs.summarize([4.0])
    assert one.n == 1
    assert one.median == one.mean == one.p90 == one.maximum == 4.0
    assert one.stdev == 0.0, "a single sample has no spread, not an error"

    two = bs.summarize([1.0, 3.0])
    assert two.n == 2
    assert two.median == 2.0
    assert two.mean == 2.0
    assert two.stdev == pytest.approx(1.4142135, rel=1e-6)
    assert two.maximum == 3.0


def test_sample_total() -> None:
    sample = bs.Sample(slot=bt.Slot(0, 0), iteration=3, build_ms=2.0, submit_ms=100.0)
    assert sample.total_ms == pytest.approx(102.0)
    assert sample.error is None


def test_phase_record_derives_throughput_from_the_right_clock() -> None:
    record = bs.PhaseRecord()
    record.add_sample(
        bs.Sample(bt.Slot(0, 0), 0, build_ms=1.0, submit_ms=500.0), initiator_bytes=GB
    )
    record.add_sample(
        bs.Sample(bt.Slot(1, 0), 0, build_ms=1.0, submit_ms=1000.0),
        initiator_bytes=2 * GB,
    )
    assert record.initiator_gbs == [2.0, 2.0], "each initiator's own bytes and clock"

    # The span is longer than the slowest initiator: that excess is the overhead.
    record.add_iteration(span_ms=1100.0, iteration_bytes=3 * GB, slowest_ms=1001.0)
    assert record.agg_gbs == [pytest.approx(3 / 1.1)]
    assert record.overhead_ms == [pytest.approx(99.0)]


def test_aggregate_throughput_is_measured_against_the_span() -> None:
    """Two initiators moving 1 GB each, one taking twice as long as the other.

    The aggregate is the iteration's bytes over the span that actually elapsed,
    which is 2 GB/s. Adding up the per-initiator rates would say 3 GB/s, a
    figure no clock in the run supports; the two are built to differ here so
    that the assertion pins which one is reported.
    """
    record = bs.PhaseRecord()
    record.add_sample(
        bs.Sample(bt.Slot(0, 0), 0, build_ms=0.0, submit_ms=500.0), initiator_bytes=GB
    )
    record.add_sample(
        bs.Sample(bt.Slot(1, 0), 0, build_ms=0.0, submit_ms=1000.0), initiator_bytes=GB
    )
    record.add_iteration(span_ms=1000.0, iteration_bytes=2 * GB, slowest_ms=1000.0)

    assert record.agg_gbs == [2.0]
    assert sum(record.initiator_gbs) == 3.0, "the sum the span rules out"
    assert record.overhead_ms == [0.0], "the span covered exactly the slowest"


def test_zero_durations_do_not_produce_infinite_throughput() -> None:
    record = bs.PhaseRecord()
    record.add_sample(
        bs.Sample(bt.Slot(0, 0), 0, build_ms=0.0, submit_ms=0.0), initiator_bytes=GB
    )
    record.add_iteration(span_ms=0.0, iteration_bytes=GB, slowest_ms=0.0)
    assert record.initiator_gbs == []
    assert record.agg_gbs == []
    assert record.submit_ms == [0.0], "the sample is still counted"


def test_run_record_discards_ramp_without_a_special_case() -> None:
    runs = bs.RunRecord()
    assert set(runs.phases) == set(bt.PHASES)

    runs.record(bt.WARM).add_sample(
        bs.Sample(bt.Slot(0, 0), 4, 0.0, 500.0), initiator_bytes=GB
    )
    runs.record(bt.RAMP).add_sample(
        bs.Sample(bt.Slot(0, 0), 1, 0.0, 9000.0), initiator_bytes=GB
    )

    assert runs.phases[bt.WARM].submit_ms == [500.0]
    assert bt.RAMP not in runs.phases, "the ramp record is a throwaway"


_RUNS: int = 3
_WARMUP_ITERS: int = 1
_WARM_ITERS: int = 2
_INITIATORS: tuple[bt.Slot, ...] = (bt.Slot(0, 0), bt.Slot(1, 0))

# Per-phase submit time, so cold is visibly slower than warm and the ramp sits
# between them. Every initiator in an iteration is given the same duration, so
# the expected medians are exact.
_SUBMIT_MS: dict[str, float] = {
    bt.COLD_QP: 800.0,
    bt.RAMP: 300.0,
    bt.WARM: 200.0,
}

# Iterations per phase, given the shape above: only the first iteration of the
# first run is cold_qp, and every run contributes its warm iterations.
_ITERATIONS: dict[str, int] = {
    bt.COLD_QP: 1,
    bt.WARM: _RUNS * _WARM_ITERS,
}


def _run_record() -> bs.RunRecord:
    """A record shaped like a real ``(pattern, direction)``.

    Three runs of three iterations each, driven by two initiators, with phases
    assigned by the same `phase_of` the driver uses. Every run ends on two warm
    iterations and opens on a discarded ramp iteration, which in the first run
    is the cold queue-pair iteration instead.
    """
    runs = bs.RunRecord()
    for run in range(_RUNS):
        # One registration measurement per proc per run.
        runs.register_ms.extend(400.0 + 100.0 * run + 50.0 * i for i in range(2))
        for iteration in range(_WARMUP_ITERS + _WARM_ITERS):
            phase = bt.phase_of(run, iteration, _WARMUP_ITERS)
            record = runs.record(phase)
            submit_ms = _SUBMIT_MS[phase]
            for slot in _INITIATORS:
                record.add_sample(
                    bs.Sample(slot, iteration, build_ms=2.0, submit_ms=submit_ms),
                    initiator_bytes=GB,
                )
            record.add_iteration(
                span_ms=submit_ms + 5.0,
                iteration_bytes=2 * GB,
                slowest_ms=submit_ms + 2.0,
            )
    return runs


def test_run_record_fixture_has_the_shape_the_driver_produces() -> None:
    runs = _run_record()
    assert [len(runs.phases[p].span_ms) for p in bt.PHASES] == [1, 6]
    assert len(runs.register_ms) == _RUNS * len(_INITIATORS)
    assert bt.RAMP not in runs.phases, "the ramp iterations were discarded"


def test_n_samples_counts_initiators_and_n_spans_counts_iterations() -> None:
    """The two counts differ whenever a pattern has more than one initiator: an
    iteration yields one span but one sample per initiator. Reported separately
    because `submit_ms_*` is backed by the samples and `agg_gbs_*` by the spans.
    """
    runs = _run_record()
    for phase, iterations in _ITERATIONS.items():
        metrics = bs.metrics_for(phase, runs)
        assert metrics.n_spans == iterations, phase
        assert metrics.n_samples == iterations * len(_INITIATORS), phase
        assert metrics.n_samples != metrics.n_spans, "two initiators, so distinct"


def test_only_warm_rows_carry_throughput() -> None:
    runs = _run_record()

    warm = bs.metrics_for(bt.WARM, runs)
    assert warm.agg_gbs_median == pytest.approx(2 / 0.205)
    assert warm.initiator_gbs_median == pytest.approx(5.0)
    assert warm.submit_ms_median == pytest.approx(200.0)

    cold = bs.metrics_for(bt.COLD_QP, runs)
    assert cold.agg_gbs_median is None
    assert cold.agg_gbs_p90 is None
    assert cold.initiator_gbs_median is None
    assert cold.submit_ms_median > 0, "cold rows still report latency"
    # Registration is a per-run cost, so every phase of a configuration reports
    # the same spread over all six measurements.
    assert cold.register_ms_median == pytest.approx(525.0)
    assert cold.register_ms_p90 == pytest.approx(650.0)


def test_cold_warm_ratio() -> None:
    runs = _run_record()
    assert bs.cold_warm_ratio(runs, bt.COLD_QP) == pytest.approx(4.0)

    assert bs.cold_warm_ratio(bs.RunRecord(), bt.COLD_QP) is None, (
        "no samples, no claim"
    )


def _columns() -> tuple[
    bs.KeyColumns, bs.ShapeColumns, bs.ConfigColumns, bs.MetricColumns
]:
    topo = bt.build_topology("all-to-all", 4, 8)
    return (
        bs.KeyColumns(pattern=topo.pattern, direction=bt.WRITE, phase=bt.WARM),
        bs.ShapeColumns(
            num_hosts=topo.num_hosts,
            procs_per_host=topo.procs_per_host,
            lane_pairing=topo.pairing,
            lane_shift=topo.shift,
            num_edges=len(topo.edges),
            num_initiators=len(topo.initiators(bt.WRITE)),
            max_degree=topo.max_degree(bt.WRITE),
            max_ops_per_action=bt.max_ops_per_action(topo, bt.WRITE, ops=1),
            max_in_degree=3,
            bytes_per_iteration=bt.bytes_per_iteration(topo, ops=1, payload_bytes=GB),
            max_buffers_per_proc=4,
            max_device_bytes_per_proc=4 * GB,
            max_host_bytes_per_host=0,
        ),
        bs.ConfigColumns(
            schema_version=bs.SCHEMA_VERSION,
            transport="ibverbs",
            tcp_serialized=0,
            source_device="gpu",
            dest_device="gpu",
            payload_size_mb=1024.0,
            concurrent_ops=1,
            cold_proc_runs=1,
            runs=3,
            warmup_iters_per_run=3,
            warm_iters_per_run=10,
            local_sender=0,
            verify_mode="sampled",
            rdma_runtime_threads="16",
            integrity_ok="True",
            negative_control_ok="True",
        ),
        bs.metrics_for(bt.WARM, _run_record()),
    )


def test_summary_header_and_row_cannot_drift() -> None:
    key, shape, config, metrics = _columns()
    header = bs.summary_header()
    row = bs.summary_row(key, shape, config, metrics)
    assert len(header) == len(row)
    assert len(header) == len(set(header)), "no duplicated column names"

    # Leading columns identify the row, so the CSV is scannable.
    assert header[:3] == ("pattern", "direction", "phase")
    assert row[:3] == ("all-to-all", "write", "warm")
    assert header[-1] == "agg_gbs_p90"


def test_cold_rows_render_as_empty_cells_not_zeros() -> None:
    key, shape, config, _ = _columns()
    cold = bs.metrics_for(bt.COLD_QP, _run_record())

    stream = io.StringIO()
    bs.write_rows(
        stream,
        bs.summary_header(),
        [bs.summary_row(key, shape, config, cold)],
    )
    header_line, row_line = stream.getvalue().splitlines()
    columns = dict(zip(header_line.split(","), row_line.split(",")))
    assert columns["agg_gbs_median"] == "", "empty, so nobody quotes a cold rate"
    assert columns["initiator_gbs_p90"] == ""
    assert float(columns["submit_ms_median"]) > 0
    assert float(columns["register_ms_median"]) > 0, "cold rows keep registration"


def test_phase_table_marks_cold_rows_with_a_dash() -> None:
    runs = _run_record()
    rows = [
        (bs.KeyColumns("ring", bt.READ, phase), bs.metrics_for(phase, runs))
        for phase in bt.PHASES
    ]
    lines = bs.phase_table(rows)
    assert lines[0].startswith("Dir")
    assert "build p50 (ms)" in lines[0], "every timing column names its unit"
    assert "agg (GB/s)" in lines[0]
    body = lines[2:]
    assert len(body) == 2
    assert body[0].split()[-1] == "-", "cold_qp has no aggregate throughput"
    assert float(body[1].split()[-1]) > 0, "warm does"
    assert "cold_qp" in body[0] and "warm" in body[1]
