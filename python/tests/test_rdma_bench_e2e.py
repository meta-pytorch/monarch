#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
End-to-end tests for the multi-host RDMA benchmark, over real RDMA.

``test_rdma_bench_driver`` covers the driver's logic with its collaborators
substituted; these run the same code against real procs, real registrations and
a real fabric. ``--local-only`` is what makes that possible without a
scheduler: every host lands on this machine, so a whole invocation -- build the
graph, provision, measure every phase of both directions, verify, write the CSV
-- runs here. The lane pairing supplies the proc-to-proc edges a single host
would otherwise lack.

``tmp_path`` is pytest's per-test temporary directory fixture, which is where
each case puts its CSV.

What these are really for is the CSV. Every number the benchmark reports passes
through it, and a row whose phase, sample counts, or empty throughput cells are
wrong is a number somebody would go on to quote.
"""

import csv

import bench_stats as bs
import bench_topology as bt
import benchmark_driver as bd
import pytest
from rdma_test_utils import skip_if_ibverbs_unavailable


_LANES = 2
_OPS = 2
_RUNS = 2
_WARMUP_ITERS = 1
_WARM_ITERS = 2
_PAYLOAD_MB = 0.004096

# What the shape above should produce per direction: one cold_qp iteration
# however many runs there are, plus every run's warm iterations.
_SPANS = {
    bt.COLD_QP: 1,
    bt.WARM: _RUNS * _WARM_ITERS,
}


def _config(
    output_csv: str,
    transport: str,
    *,
    pattern: str = "p2p",
    num_hosts: int = 1,
    # `all` gives every slot several peers on one host
    pairing: str = bt.ALL,
    lanes: int = _LANES,
) -> bd.BenchConfig:
    """The configuration as the CLI would build it."""
    return bd.BenchConfig(
        transport=transport,
        pattern=pattern,
        num_hosts=num_hosts,
        lane_pairing=pairing,
        # Unused when lane pairing is ALL or SAME
        lane_shift=1,
        payload_size_mb=_PAYLOAD_MB,
        concurrent_ops=_OPS,
        runs=_RUNS,
        warmup_iters_per_run=_WARMUP_ITERS,
        warm_iters_per_run=_WARM_ITERS,
        procs_per_host=lanes,
        source_on_gpu=False,
        dest_on_gpu=False,
        verify="full",
        verify_window_mb=_PAYLOAD_MB,
        max_device_gb_per_proc=1.0,
        max_host_gb_per_host=1.0,
        rdma_runtime_threads=None,
        rdma_max_nics_per_buffer=1,
        output_csv=output_csv,
        command=bd.RUN_COMMAND,
        local_only=True,
        cached_path=None,
        teardown_policy=bd.TEARDOWN_NEVER,
    )


def _rows(path: str) -> list[dict[str, str]]:
    with open(path) as stream:
        return list(csv.DictReader(stream))


@pytest.mark.parametrize("transport", ["ibverbs", "tcp"])
async def test_a_whole_invocation_reports_every_phase(transport, tmp_path) -> None:
    if transport == "ibverbs":
        skip_if_ibverbs_unavailable()
    output_csv = str(tmp_path / "results.csv")

    await bd._drive(_config(output_csv, transport), None, "unused", ())

    rows = _rows(output_csv)
    assert list(rows[0]) == list(bs.summary_header())
    assert len(rows) == len(bt.DIRECTIONS) * len(bt.PHASES)
    assert {(row["direction"], row["phase"]) for row in rows} == {
        (direction, phase) for direction in bt.DIRECTIONS for phase in bt.PHASES
    }

    for row in rows:
        assert row["pattern"] == "p2p"
        assert row["transport"] == transport
        assert row["num_edges"] == "4", "2 lanes paired every way"
        assert int(row["n_spans"]) == _SPANS[row["phase"]], row["phase"]
        assert int(row["n_samples"]) == _SPANS[row["phase"]] * _LANES
        assert float(row["register_ms_median"]) > 0.0
        assert row["integrity_ok"] == "True"
        assert row["negative_control_ok"] == "True"


@pytest.mark.parametrize("transport", ["ibverbs", "tcp"])
async def test_only_warm_rows_report_throughput(transport, tmp_path) -> None:
    """Cold cells are empty rather than zero."""
    if transport == "ibverbs":
        skip_if_ibverbs_unavailable()
    output_csv = str(tmp_path / "results.csv")

    await bd._drive(_config(output_csv, transport), None, "unused", ())

    for row in _rows(output_csv):
        if row["phase"] == bt.WARM:
            assert float(row["agg_gbs_median"]) > 0.0
            assert float(row["initiator_gbs_median"]) > 0.0
            assert float(row["span_ms_median"]) > 0.0
        else:
            assert row["agg_gbs_median"] == "", row["phase"]
            assert row["initiator_gbs_median"] == "", row["phase"]
            assert float(row["submit_ms_median"]) > 0.0, "cold still has a latency"


# One proc per host, so each pattern's host graph is its proc graph.
_SWEEP_HOSTS = 4
_SWEEP_EDGES = {
    "p2p": 1,
    "fan-out": _SWEEP_HOSTS - 1,
    "fan-in": _SWEEP_HOSTS - 1,
    "all-to-all": _SWEEP_HOSTS * (_SWEEP_HOSTS - 1),
    "ring": _SWEEP_HOSTS,
}

# Initiators under (write, read). Direction picks which end of an edge issues
# the ops, so fan-out and fan-in are mirrors of one another.
_SWEEP_INITIATORS = {
    "p2p": (1, 1),
    "fan-out": (1, _SWEEP_HOSTS - 1),
    "fan-in": (_SWEEP_HOSTS - 1, 1),
    "all-to-all": (_SWEEP_HOSTS, _SWEEP_HOSTS),
    "ring": (_SWEEP_HOSTS, _SWEEP_HOSTS),
}


@pytest.mark.parametrize("pattern", bt.PATTERNS)
@pytest.mark.parametrize("transport", ["ibverbs", "tcp"])
async def test_every_pattern_moves_its_own_graph(pattern, transport, tmp_path) -> None:
    """Each pattern end to end, with all four 'hosts' on this machine.

    Loopback makes the timings meaningless, but the graph, the routing and the
    integrity check are the code a real run uses.
    """
    if transport == "ibverbs":
        skip_if_ibverbs_unavailable()
    output_csv = str(tmp_path / "results.csv")

    await bd._drive(
        _config(
            output_csv,
            transport,
            pattern=pattern,
            num_hosts=_SWEEP_HOSTS,
            pairing=bt.SAME,
            lanes=1,
        ),
        None,
        "unused",
        (),
    )

    rows = _rows(output_csv)
    assert {row["pattern"] for row in rows} == {pattern}
    for row in rows:
        assert int(row["num_edges"]) == _SWEEP_EDGES[pattern], pattern
        write, read = _SWEEP_INITIATORS[pattern]
        expected = write if row["direction"] == bt.WRITE else read
        assert int(row["num_initiators"]) == expected, (pattern, row["direction"])
        assert row["integrity_ok"] == "True", "every edge delivered its own bytes"
        assert row["negative_control_ok"] == "True"
