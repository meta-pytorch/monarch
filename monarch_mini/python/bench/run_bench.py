# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Orchestrate the minimonarch round-trip benchmarks and build a Markdown report.

For each topology this launches the required helper processes (parent / host /
receiver / process managers) and then the sender process, which measures all
four metrics across every message size and prints its results as JSON. The
collected numbers are written to ``results.json`` and summarized as one median
table per metric in an ``index.md`` report.

Run it under uv so the worker subprocesses inherit an interpreter that can
import the built minimonarch extension:

    uv run python bench/run_bench.py
    uv run python bench/run_bench.py --quick           # tiny, fast smoke run
    uv run python bench/run_bench.py --report-only      # rebuild report from results.json

The four metrics:
  1. latency             - round-trip latency, one message in flight.
  2. throughput          - round-trip throughput, many messages in flight.
  3. monitor_latency     - latency with subscribe(r) before / unsubscribe after each round trip.
  4. monitor_throughput  - throughput with a per-message subscribe / unsubscribe.

The three topologies are described in bench_common.TOPOLOGIES.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_common import (  # noqa: E402
    DEFAULT_LATENCY_ITERS,
    DEFAULT_SIZES,
    DEFAULT_THROUGHPUT_CAP_BYTES,
    DEFAULT_THROUGHPUT_N,
    DEFAULT_THROUGHPUT_REPS,
    metric_by_key,
    METRICS,
    RESULT_MARKER,
    size_label,
    TOPOLOGIES,
)

_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKER = os.path.join(_HERE, "bench_worker.py")


# ---------------------------------------------------------------------------
# Running one topology.
# ---------------------------------------------------------------------------
def _topology_urls(topology: str, sockdir: str) -> dict[str, str]:
    """The unix socket urls each role uses. inproc needs none."""
    if topology == "unix":
        return {
            "sender": f"unix://{sockdir}/s.sock",
            "receiver": f"unix://{sockdir}/r.sock",
        }
    if topology == "manager":
        return {
            "p0": f"unix://{sockdir}/p0.sock",
            "p1": f"unix://{sockdir}/p1.sock",
        }
    return {}


def _spawn(cfg: dict, capture: bool) -> subprocess.Popen:
    """Launch one worker process with its JSON config."""
    return subprocess.Popen(
        [sys.executable, _WORKER, json.dumps(cfg)],
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
    )


def _kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def run_topology(topology: dict, base_cfg: dict, timeout_s: float) -> dict:
    """Spawn the helpers and the sender for one topology; return the sender's
    parsed results dict."""
    key = topology["key"]
    sockdir = tempfile.mkdtemp(prefix=f"mm-bench-{key}-")
    try:
        urls = _topology_urls(key, sockdir)
        cfg = {**base_cfg, "topology": key, "urls": urls}

        helpers = [
            _spawn({**cfg, "role": r}, capture=False) for r in topology["helper_roles"]
        ]
        time.sleep(0.2)  # let helpers bind before the sender warms up
        sender = _spawn({**cfg, "role": topology["sender_role"]}, capture=True)
        try:
            out, err = sender.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            sender.kill()
            out, err = sender.communicate()
            raise RuntimeError(f"[{key}] sender timed out after {timeout_s}s\n{err}")
        finally:
            for h in helpers:
                _kill(h)

        if sender.returncode != 0:
            raise RuntimeError(f"[{key}] sender exited {sender.returncode}\n{err}")

        for line in out.splitlines():
            if line.startswith(RESULT_MARKER):
                return json.loads(line[len(RESULT_MARKER) :])
        raise RuntimeError(f"[{key}] no results line in sender stdout\n{out}\n{err}")
    finally:
        shutil.rmtree(sockdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Extracting samples.
# ---------------------------------------------------------------------------
def _samples_for(metric_key: str, size_results: list) -> list[float]:
    """Pull the samples for one size out of the worker's records. For latency
    metrics the records are already the per-iter microsecond samples; for
    throughput metrics each record is a dict and we report GB/s."""
    if metric_by_key(metric_key)["throughput"]:
        return [rec["gb_per_s"] for rec in size_results]
    return list(size_results)


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    mid = len(s) // 2
    return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2


# ---------------------------------------------------------------------------
# Report.
# ---------------------------------------------------------------------------
def _medians_per_size(
    metric_key: str, topology_key: str, collected: dict
) -> dict[int, float]:
    """Median value per message size for one (metric, topology), or {} if the
    topology was not run / has no data."""
    blob = collected.get(topology_key)
    if not blob:
        return {}
    per_size = blob["results"].get(metric_key, {})
    medians: dict[int, float] = {}
    for size_key, samples in per_size.items():
        ys = _samples_for(metric_key, samples)
        if ys:
            medians[int(size_key)] = _median(ys)
    return medians


def build_report(collected: dict, out_dir: str) -> None:
    """Render the median summary tables (one per metric) into index.md."""
    md = _render_markdown(collected)
    with open(os.path.join(out_dir, "index.md"), "w") as f:
        f.write(md)
    print(f"report: {os.path.join(out_dir, 'index.md')}")


def _render_markdown(collected: dict) -> str:
    lines: list[str] = [
        "# minimonarch round-trip benchmarks",
        "",
        "Each cell is the median over all samples for that message size.",
        "",
        "## Topologies",
        "",
    ]
    for topo in TOPOLOGIES:
        suffix = "" if topo["key"] in collected else " _(not run)_"
        lines.append(f"- **{topo['title']}** — {topo['blurb']}{suffix}")
    lines.append("")

    for metric in METRICS:
        lines.append(f"## {metric['title']} ({metric['unit']})")
        lines.append("")
        lines.append(_median_table(metric, collected))
        lines.append("")

    return "\n".join(lines)


def _median_table(metric: dict, collected: dict) -> str:
    per_topo = {
        topo["key"]: _medians_per_size(metric["key"], topo["key"], collected)
        for topo in TOPOLOGIES
    }
    sizes: set[int] = set()
    for medians in per_topo.values():
        sizes |= set(medians)
    if not sizes:
        return "_no data_"

    header = "| size | " + " | ".join(t["key"] for t in TOPOLOGIES) + " |"
    sep = "| --- | " + " | ".join("---" for _ in TOPOLOGIES) + " |"
    rows = [f"_median {metric['unit']} per size_", "", header, sep]
    for size in sorted(sizes):
        cols = []
        for topo in TOPOLOGIES:
            med = per_topo[topo["key"]].get(size)
            cols.append(f"{med:.3g}" if med is not None else "—")
        rows.append(f"| {size_label(size)} | " + " | ".join(cols) + " |")
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=os.path.join(_HERE, "report"), help="output dir")
    ap.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=DEFAULT_SIZES,
        help="message sizes in bytes",
    )
    ap.add_argument("--latency-iters", type=int, default=DEFAULT_LATENCY_ITERS)
    ap.add_argument("--throughput-reps", type=int, default=DEFAULT_THROUGHPUT_REPS)
    ap.add_argument("--throughput-n", type=int, default=DEFAULT_THROUGHPUT_N)
    ap.add_argument(
        "--throughput-cap-bytes", type=int, default=DEFAULT_THROUGHPUT_CAP_BYTES
    )
    ap.add_argument("--topologies", nargs="+", default=[t["key"] for t in TOPOLOGIES])
    ap.add_argument(
        "--timeout", type=float, default=900.0, help="per-topology sender timeout (s)"
    )
    ap.add_argument("--quick", action="store_true", help="tiny fast run (smoke test)")
    ap.add_argument(
        "--report-only",
        action="store_true",
        help="rebuild report from existing results.json",
    )
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    results_path = os.path.join(args.out, "results.json")

    if args.report_only:
        with open(results_path) as f:
            collected = json.load(f)
        build_report(collected, args.out)
        return

    if args.quick:
        args.sizes = [64, 1 << 12, 1 << 16]
        args.latency_iters = 10
        args.throughput_reps = 3
        args.throughput_n = 20

    base_cfg = {
        "sizes": args.sizes,
        "latency_iters": args.latency_iters,
        "throughput_reps": args.throughput_reps,
        "throughput_n": args.throughput_n,
        "throughput_cap_bytes": args.throughput_cap_bytes,
    }

    collected: dict[str, dict] = {}
    for topo in TOPOLOGIES:
        if topo["key"] not in args.topologies:
            continue
        print(f"==> {topo['key']}: {topo['title']}")
        t0 = time.perf_counter()
        collected[topo["key"]] = run_topology(topo, base_cfg, args.timeout)
        print(f"    done in {time.perf_counter() - t0:.1f}s")
        with open(results_path, "w") as f:  # checkpoint after each topology
            json.dump(collected, f, indent=2)

    build_report(collected, args.out)


if __name__ == "__main__":
    main()
