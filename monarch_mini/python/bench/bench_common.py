# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Shared configuration for the minimonarch round-trip benchmarks.

This module is imported by both the orchestrator (``run_bench.py``) and the
subprocess workers (``bench_worker.py``) so that they agree on the set of
metrics, topologies, message sizes, and the protocol idents.

Nothing here touches minimonarch — it is pure metadata so it is cheap to import
in either process.
"""

from __future__ import annotations

# --- Protocol idents -------------------------------------------------------
# The sender, receiver, and the topology's structural actors use these fixed
# idents. Idents must be unique across a single minimonarch run; since each
# benchmark run is a fresh set of processes, fixed names are fine.
SENDER = b"s"
RECEIVER = b"r"

# Marker the sender prints (on its own stdout line) wrapping the JSON results
# blob, so the orchestrator can pick the results out of the worker's stdout.
RESULT_MARKER = "MM_BENCH_RESULT "


# --- Metrics ---------------------------------------------------------------
# Each metric is one "number" the user asked for. ``unit``/``higher_better``
# drive the plot axes and labels.
METRICS: list[dict[str, object]] = [
    {
        "key": "latency",
        "title": "Round-trip latency",
        "unit": "us",
        "monitor": False,
        "throughput": False,
        "higher_better": False,
    },
    {
        "key": "throughput",
        "title": "Round-trip throughput",
        "unit": "GB/s",
        "monitor": False,
        "throughput": True,
        "higher_better": True,
    },
    {
        "key": "monitor_latency",
        "title": "Round-trip latency w/ subscribe+unsubscribe",
        "unit": "us",
        "monitor": True,
        "throughput": False,
        "higher_better": False,
    },
    {
        "key": "monitor_throughput",
        "title": "Round-trip throughput w/ subscribe+unsubscribe",
        "unit": "GB/s",
        "monitor": True,
        "throughput": True,
        "higher_better": True,
    },
]


# --- Topologies ------------------------------------------------------------
# Each topology describes how sender ``s`` and receiver ``r`` are wired up and
# which worker roles must be spawned. The orchestrator launches the helper
# roles, then runs the sender role and reads its JSON.
TOPOLOGIES: list[dict[str, object]] = [
    {
        "key": "inproc",
        "title": "(a) in-process, common inproc parent",
        "blurb": "s and r live in one process/context, both inproc children of a common parent p.",
        # One process does everything: it hosts p, s, and r and runs r's echo
        # loop as a background task while s measures.
        "sender_role": "sender",
        "helper_roles": [],
        "n_procs": 1,
    },
    {
        "key": "unix",
        "title": "(b) two processes, common unix parent",
        "blurb": "s and r are in separate processes, each an inproc-free child of a common parent p (third process) over unix://.",
        "sender_role": "sender",
        "helper_roles": ["parent", "receiver"],
        "n_procs": 3,
    },
    {
        "key": "manager",
        "title": "(c) process/host manager",
        "blurb": "s -inproc-> p0 -unix-> h <-unix- p1 <-inproc- r. h is a host manager; p0/p1 are process managers.",
        "sender_role": "pm_sender",
        "helper_roles": ["host", "pm_receiver"],
        "n_procs": 3,
    },
]


# --- Default workload ------------------------------------------------------
# 64 B .. 16 MiB. Cross-process topologies copy real bytes over unix sockets,
# so the top end is kept to 16 MiB to bound per-run cost; override on the CLI.
DEFAULT_SIZES: list[int] = [64, 1 << 10, 1 << 14, 1 << 18, 1 << 20, 1 << 24]

DEFAULT_LATENCY_ITERS = 50
DEFAULT_THROUGHPUT_REPS = 10
DEFAULT_THROUGHPUT_N = 100
# Cap the bytes in flight per throughput rep so large sizes don't allocate
# gigabytes: n is reduced to keep n*size under this.
DEFAULT_THROUGHPUT_CAP_BYTES = 64 << 20


def size_label(size: int) -> str:
    """Human-readable size label, e.g. 1024 -> '1 KiB'."""
    units = [(1 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")]
    for scale, name in units:
        if size >= scale and size % scale == 0:
            return f"{size // scale} {name}"
    return f"{size} B"


def metric_by_key(key: str) -> dict[str, object]:
    for m in METRICS:
        if m["key"] == key:
            return m
    raise KeyError(key)
