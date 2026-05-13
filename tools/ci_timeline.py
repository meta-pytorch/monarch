# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

"""
CI timeline tool: show test execution timeline and critical path for a Phabricator diff.

Port of phps CIActionGraph to Python, usable without a www devserver.

Usage:
    buck run //monarch/tools:ci_timeline -- D104448235
    buck run //monarch/tools:ci_timeline -- D104448235 --chrome-trace > /tmp/trace.json
    buck run //monarch/tools:ci_timeline -- D104448235 --dot | pastry
    buck run //monarch/tools:ci_timeline -- D104448235 --dot --critical-path
"""

import argparse
import asyncio
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

from security.frameworks.python.exec.subprocess import TrustedSubprocessWithList


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Job:
    id: int
    alias: str
    oncall: str
    create_ts: Optional[int]  # unix seconds
    start_ts: Optional[int]
    end_ts: Optional[int]
    parent_id: Optional[int]
    retry_parent_id: Optional[int]
    nonce: Optional[str]
    status: Optional[str]
    on_critical_path: bool = False

    @property
    def create_ms(self) -> Optional[int]:
        return self.create_ts * 1000 if self.create_ts else None

    @property
    def start_ms(self) -> Optional[int]:
        return self.start_ts * 1000 if self.start_ts else None

    @property
    def end_ms(self) -> Optional[int]:
        return self.end_ts * 1000 if self.end_ts else None

    @property
    def duration_ms(self) -> int:
        if self.end_ts and self.create_ts:
            return max(0, (self.end_ts - self.create_ts) * 1000)
        return 0

    @property
    def queue_ms(self) -> int:
        if self.start_ts and self.create_ts:
            return max(0, (self.start_ts - self.create_ts) * 1000)
        return 0

    @property
    def event_attribution(self) -> str:
        """Classify job as RETRY / INFRA / VALIDATION / UNKNOWN (from CIActionGraphTrait)."""
        if self.retry_parent_id:
            return "RETRY"
        alias = self.alias.lower()
        # Infra jobs from CIActionGraphTrait::INFRA_ALIASES pattern
        infra_prefixes = (
            "target-determinator",
            "citadel-orchestrator",
            "land",
            "push",
            "base_retry",
            "brr_process",
            "sl_",
            "hg_",
        )
        if any(alias.startswith(p) or alias.endswith(p) for p in infra_prefixes):
            return "INFRA"
        return "VALIDATION"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def get_all_versions(diff_num: int) -> list[int]:
    """Return all phabricator version numbers for a diff, newest first."""
    query = (
        "{ phabricator_diff(number: %d) "
        "{ phabricator_versions { edges { node { number } } } } }" % diff_num
    )
    result = TrustedSubprocessWithList.run(
        executable="jf",
        cmd_args=["graphql", "--query", query],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"jf graphql failed for D{diff_num}:\n{result.stderr}\n"
            "Ensure `jf` is on PATH and you're authenticated."
        )
    data = json.loads(result.stdout)
    edges = (
        data.get("phabricator_diff", {})
        .get("phabricator_versions", {})
        .get("edges", [])
    )
    versions = [
        int(e["node"]["number"]) for e in edges if e.get("node", {}).get("number")
    ]
    return list(reversed(versions))  # newest first


async def fetch_jobs(version_number: int) -> list[dict[str, Any]]:
    """Fetch all Sandcastle instances for a phabricator version number."""
    from libfb.py.asyncio.sandcastle import ApiException, AsyncSandcastleClient

    async with AsyncSandcastleClient() as client:
        try:
            instances = await client.get_info(job_id=None, diff_id=version_number)
        except ApiException as e:
            if "No Sandcastle instances found" in str(e):
                return []
            raise
    return instances


def parse_jobs(instances: list[dict[str, Any]]) -> list[Job]:
    jobs = []
    for inst in instances:
        inst_id = inst.get("id")
        if not inst_id:
            continue

        date_started = inst.get("dateStarted")
        elapsed = inst.get("elapsedTime")  # seconds
        date_finished = inst.get("dateFinished")
        if date_finished is None and date_started and elapsed:
            date_finished = date_started + elapsed

        jobs.append(
            Job(
                id=int(inst_id),
                alias=inst.get("alias") or str(inst_id),
                oncall=inst.get("oncall") or "",
                create_ts=inst.get("dateCreated"),
                start_ts=date_started,
                end_ts=date_finished,
                parent_id=inst.get("parentInstanceID"),
                retry_parent_id=inst.get("retryInstanceID")
                or inst.get("retryParentInstanceID"),
                nonce=str(inst.get("nonce")) if inst.get("nonce") else None,
                status=inst.get("statusString"),
            )
        )
    return jobs


# ---------------------------------------------------------------------------
# Critical path (port of CIActionGraphTrait::getCriticalPathHelperForExecutionUnit)
# ---------------------------------------------------------------------------


@dataclass
class _CritPathResult:
    path: list[Job]
    end_ts: Optional[int]  # ms
    compute_time: int  # ms (sum of durations on path)


def compute_critical_path(jobs: list[Job]) -> None:
    """Mark jobs on the critical path. Mutates job.on_critical_path."""
    by_id: dict[int, Job] = {j.id: j for j in jobs}
    children: dict[int, list[int]] = defaultdict(list)
    for j in jobs:
        if j.parent_id and j.parent_id in by_id:
            children[j.parent_id].append(j.id)

    memo: dict[int, _CritPathResult] = {}

    def _recurse(job_id: int) -> _CritPathResult:
        if job_id in memo:
            return memo[job_id]
        j = by_id[job_id]
        create_ms = j.create_ms
        end_ms = j.end_ms
        self_compute = j.duration_ms

        best: Optional[_CritPathResult] = None
        for child_id in children.get(job_id, []):
            child_result = _recurse(child_id)
            if best is None:
                best = child_result
            else:
                # Prefer later end_ts; break ties by larger compute_time
                c_end = child_result.end_ts or 0
                b_end = best.end_ts or 0
                if c_end > b_end or (
                    c_end == b_end and child_result.compute_time > best.compute_time
                ):
                    best = child_result

        if best is None:
            result = _CritPathResult(path=[j], end_ts=end_ms, compute_time=self_compute)
        else:
            result = _CritPathResult(
                path=[j] + best.path,
                end_ts=best.end_ts or end_ms,
                compute_time=self_compute + best.compute_time,
            )
        memo[job_id] = result
        return result

    # Find roots (jobs with no parent in the set)
    roots = [j for j in jobs if not j.parent_id or j.parent_id not in by_id]
    if not roots:
        roots = jobs

    # Compute critical path from each root, pick the one with latest finish
    all_results = [_recurse(r.id) for r in roots]
    if not all_results:
        return

    best_root = max(
        all_results,
        key=lambda r: ((r.end_ts or 0), r.compute_time),
    )
    for j in best_root.path:
        j.on_critical_path = True


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def render_gantt(jobs: list[Job]) -> None:
    if not jobs:
        print("No jobs found.")
        return

    timed = [j for j in jobs if j.create_ms is not None]
    if not timed:
        print("No jobs with timing data.")
        return

    t0 = min(j.create_ms for j in timed)  # type: ignore[arg-type]
    t1 = max(j.end_ms or j.create_ms or 0 for j in timed)
    total = max(t1 - t0, 1)
    BAR = 40
    NAME = 50

    cp_jobs = [j for j in timed if j.on_critical_path]
    print(
        f"\nCI Timeline  (wall time: {_fmt(total)}  |  jobs: {len(timed)}  |  critical path: {len(cp_jobs)})\n"
    )
    header = (
        f"{'Job':<{NAME}}  {'Start':>8}  {'Dur':>8}  {'Queue':>8}  {'Status':>12}  CP"
    )
    print(header)
    print("-" * (len(header) + BAR + 5))

    for j in sorted(timed, key=lambda j: j.create_ms or 0):
        rel_start = (j.create_ms or t0) - t0
        rel_end = (j.end_ms or (j.create_ms or t0)) - t0
        b0 = int(rel_start / total * BAR)
        b1 = max(b0 + 1, int(rel_end / total * BAR))
        bar = " " * b0 + "█" * (b1 - b0) + " " * (BAR - b1)
        cp = "★" if j.on_critical_path else " "
        name = j.alias[:NAME].ljust(NAME)
        status = (j.status or "")[:12].rjust(12)
        print(
            f"{name}  {_fmt(rel_start):>8}  {_fmt(j.duration_ms):>8}  "
            f"{_fmt(j.queue_ms):>8}  {status}  {cp}   |{bar}|"
        )

    print()
    if cp_jobs:
        print("Critical path (★)  —  in execution order:")
        for j in sorted(cp_jobs, key=lambda j: j.create_ms or 0):
            rel = (j.create_ms or t0) - t0
            print(f"  {_fmt(rel):>8} → {_fmt(rel + j.duration_ms):>8}  {j.alias}")
        cp_exec = sum(j.duration_ms for j in cp_jobs)
        cp_queue = sum(j.queue_ms for j in cp_jobs)
        print(f"\n  Total wall time:              {_fmt(total)}")
        print(f"  Critical path execution time: {_fmt(cp_exec)}")
        if cp_queue:
            print(f"  Queue time on critical path:  {_fmt(cp_queue)}")

    print()
    print("Tip: --chrome-trace > /tmp/trace.json  → open in chrome://tracing/")
    print("     --dot | pastry                    → view dependency graph")


def render_chrome_trace(jobs: list[Job]) -> str:
    timed = [j for j in jobs if j.create_ms is not None]
    t0 = min(j.create_ms for j in timed) if timed else 0  # type: ignore[arg-type]
    events = []
    for j in timed:
        rel_start_us = ((j.create_ms or t0) - t0) * 1000  # microseconds
        dur_us = max(j.duration_ms * 1000, 1)
        events.append(
            {
                "name": j.alias,
                "cat": j.event_attribution,
                "ph": "X",
                "ts": rel_start_us,
                "dur": dur_us,
                "pid": 1,
                "tid": j.id % 500,
                "args": {
                    "execution_unit_id": j.id,
                    "parent_execution_unit_id": j.parent_id,
                    "event_attribution": j.event_attribution,
                    "oncall": j.oncall,
                    "queue_time_ms": j.queue_ms,
                    "on_critical_path": j.on_critical_path,
                    "status": j.status,
                },
            }
        )
    return json.dumps({"traceEvents": events, "displayTimeUnit": "ms"}, indent=2)


def render_dot(jobs: list[Job], critical_only: bool = False) -> str:
    by_id = {j.id: j for j in jobs}
    lines = [
        "digraph ci_deps {",
        "  rankdir=LR;",
        "  node [shape=box fontsize=9 style=filled];",
    ]
    for j in jobs:
        if critical_only and not j.on_critical_path:
            continue
        color = '"#ff6b35"' if j.on_critical_path else '"#e8e8e8"'
        label = f"{j.alias}\\n{_fmt(j.duration_ms)}"
        lines.append(f'  n{j.id} [label="{label}" fillcolor={color}];')
    for j in jobs:
        if critical_only and not j.on_critical_path:
            continue
        if j.parent_id and j.parent_id in by_id:
            parent = by_id[j.parent_id]
            if critical_only and not parent.on_critical_path:
                continue
            lines.append(f"  n{j.parent_id} -> n{j.id};")
        if j.retry_parent_id and j.retry_parent_id in by_id:
            retry = by_id[j.retry_parent_id]
            if critical_only and not retry.on_critical_path:
                continue
            lines.append(
                f'  n{j.retry_parent_id} -> n{j.id} [style=dashed label="retry"];'
            )
    lines.append("}")
    return "\n".join(lines)


def _fmt(ms: int) -> str:
    if ms <= 0:
        return "0s"
    s = ms // 1000
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def _async_main(args: argparse.Namespace) -> None:
    diff_id = args.diff_id
    if not diff_id.startswith("D"):
        diff_id = "D" + diff_id

    # Resolve diff → version number with CI data
    if args.version:
        version_number = args.version
        instances = await fetch_jobs(version_number)
    else:
        diff_num = int(diff_id.lstrip("D"))
        print(f"Resolving {diff_id} → phabricator versions...", file=sys.stderr)
        versions = get_all_versions(diff_num)
        print(f"  versions (newest first): {versions}", file=sys.stderr)

        instances = []
        version_number = versions[0] if versions else 0
        for v in versions:
            print(f"  trying version {v}...", file=sys.stderr)
            instances = await fetch_jobs(v)
            if instances:
                version_number = v
                break

    print(
        f"  fetched {len(instances)} instances from version {version_number}",
        file=sys.stderr,
    )

    if not instances:
        print(
            f"\nNo Sandcastle jobs found for {diff_id} (tried all versions).\n"
            "This may happen if the jobs have expired from the live Sandcastle DB.\n"
            "Try downloading the Chrome trace from the Nonce UI:\n"
            "  https://www.internalfb.com/sandcastle/group/nonce/<nonce_id>/\n",
            file=sys.stderr,
        )
        sys.exit(1)

    jobs = parse_jobs(instances)
    compute_critical_path(jobs)

    if args.chrome_trace:
        print(render_chrome_trace(jobs))
    elif args.dot:
        print(render_dot(jobs, critical_only=args.critical_path))
    else:
        render_gantt(jobs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CI timeline and critical path for a Phabricator diff."
    )
    parser.add_argument("diff_id", help="Phabricator diff ID, e.g. D104448235")
    parser.add_argument(
        "--version",
        type=int,
        metavar="VERSION_NUM",
        help="Phabricator version number (skip conduit resolution)",
    )
    parser.add_argument(
        "--chrome-trace",
        action="store_true",
        help="Output Chrome trace JSON (open in chrome://tracing/)",
    )
    parser.add_argument(
        "--dot", action="store_true", help="Output Graphviz DOT dependency graph"
    )
    parser.add_argument(
        "--critical-path",
        action="store_true",
        help="With --dot: show only critical path nodes",
    )
    args = parser.parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
