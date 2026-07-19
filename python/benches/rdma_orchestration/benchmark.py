#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
RDMA orchestration benchmark -- a stable speedometer for Monarch's RDMA control
path (pytokio-removal). See pytokio-removal-rdma-benchmark-plan.md.

This diff (Diff A) implements the **pure analysis half** only: the artifact
schema + atomic IO, `summarize`, `CompatiblePair`, and the comparator/verdict
algebra. The collector (`bench` / `_cold-init-child`) lands in Diff B; those
subcommands raise here.

Invariant registry (ROB-*, stable + append-only; never renumber or reuse an ID):

- ROB-1 (stable observer): the same benchmark source drives only the named public
  Monarch surface before/after a stage diff; its source hash is part of artifact
  compatibility. (Collector diff.)
- ROB-2 (differential comparability): only run-shape-compatible artifacts compare;
  under the external frozen-data-plane premise a delta is attributable to the
  changed orchestration set. **Owned here.**
- ROB-3 (sample boundary): steady-state sample uses the driver clock from issue
  through observation. (Collector diff.)
- ROB-4 (parallel composition): a K batch dispatches all K lazy ops before
  observing the first; targets pairwise disjoint. (Collector diff.)
- ROB-5 (fresh witness): every measured transfer starts from a pre-state distinct
  from its expected post-state and is validated after timing. (Collector diff.)
- ROB-6 (coldness): every cold-init sample starts in a fresh process. (Collector.)
- ROB-7 (artifact totality): a run publishes one complete valid artifact or none;
  raw samples are canonical, summaries are pure projections. **Owned here.**
- ROB-8 (verdict algebra): eligibility precedes comparison; RDMA regression is
  one-sided, ping movement is two-sided and absorbing:
  `ping_moved -> Inconclusive`, else `any(rdma_regressed) -> Regression`, else
  `Pass`; ineligible input yields no verdict. **Owned here.**
- ROB-9 (bounded observer): every externally blocking harness wait has one named
  bound. (Collector diff.)

`ROB-*` governs the instrument; `RMO-*`/`RMR-*` (Stage 3.3 plan) govern the system
under test.
"""

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Mapping, Sequence

SCHEMA_VERSION: int = 1

# Gate shape (fixed dimensions; see plan "One run, one artifact"). These document
# the gate and are consumed by the collector (Diff B); the comparator reads the
# realized shape off each artifact.
PAYLOAD_BYTES: int = 64
K: int = 32
ROUNDS: int = 5
SERIAL_WARMUPS: int = 20
SERIAL_SAMPLES: int = 400
CONCURRENT_WARMUPS: int = 1
CONCURRENT_BATCHES: int = 50
COLD_SAMPLES: int = 10
PAYLOAD_MIN: int = 16  # 8B write counter + pattern-family encoding (ER-4)

# A gate compare needs at least this many independent artifacts per side.
MIN_ARTIFACTS_PER_SIDE: int = 5

# Gated statistics per metric, and the reportability floor (min samples) each
# statistic needs (review disposition 7: p99 >= 1000, p90 >= 100).
GATED_STATS: Mapping[str, tuple[str, ...]] = {
    "ping": ("p50", "p99"),
    "serial_read": ("p50", "p99"),
    "serial_write": ("p50", "p99"),
    "concurrent_read": ("p50", "p90"),
    "concurrent_write": ("p50", "p90"),
    "cold_init": ("median",),
}
PING_METRICS: frozenset[str] = frozenset({"ping"})
STAT_MIN_N: Mapping[str, int] = {"p50": 1, "median": 1, "p90": 100, "p99": 1000}


class Verdict(str, Enum):
    PASS = "pass"
    REGRESSION = "regression"
    INCONCLUSIVE = "inconclusive"
    REFUSED = "refused"


# Exit-code mapping (ROB-8). 1 is reserved for collection/harness failure (Diff B).
EXIT_CODE: Mapping[Verdict, int] = {
    Verdict.PASS: 0,
    Verdict.REGRESSION: 2,
    Verdict.INCONCLUSIVE: 3,
    Verdict.REFUSED: 4,
}


class GateRefused(Exception):
    """A comparison is not gate-eligible; carries the human reason (ROB-2/8)."""


@dataclass(frozen=True)
class TimeoutPolicy:
    op_timeout_s: int = 60
    block_watchdog_s: int = 120
    setup_s: int = 120
    cold_child_s: int = 180
    teardown_s: int = 30


@dataclass(frozen=True)
class RunShape:
    """The gate-defining dimensions of a run (compatibility compares these ==)."""

    backend: str
    payload_bytes: int
    k: int
    rounds: int
    serial_warmups: int
    serial_samples: int
    concurrent_warmups: int
    concurrent_batches: int
    cold_samples: int
    smoke: bool


@dataclass(frozen=True)
class Environment:
    hostname: str
    platform: str
    cpu: str
    python_version: str
    monarch_revision: str  # best-effort, may be ""
    timestamp_utc: str
    label: str  # annotation only; never part of compatibility


@dataclass(frozen=True)
class Floor:
    relative: float
    absolute_ns: int


@dataclass(frozen=True)
class ThresholdPolicy:
    version: str
    backend: str
    source_sha256: str
    run_shape: RunShape
    floors: Mapping[str, Mapping[str, Floor]]  # metric -> stat -> Floor
    calibration_artifacts: tuple[str, ...]


@dataclass(frozen=True)
class RunArtifact:
    schema_version: int
    source_sha256: str
    run_shape: RunShape
    config: Mapping[str, str]
    environment: Environment
    timeout_policy: TimeoutPolicy
    # metric -> raw nanosecond samples (canonical; no derived stats stored).
    samples: Mapping[str, list[int]]


# ---------------------------------------------------------------------------
# Summaries (pure projection of raw samples; ROB-7).
# ---------------------------------------------------------------------------


def _percentile_nearest_rank(sorted_samples: Sequence[int], p: float) -> int:
    """Nearest-rank percentile over an already-sorted, non-empty sequence."""
    n = len(sorted_samples)
    if n == 0:
        raise ValueError("percentile of empty sample list")
    rank = math.ceil((p / 100.0) * n)
    idx = min(max(rank, 1), n) - 1
    return sorted_samples[idx]


def _stat_value(sorted_samples: Sequence[int], stat: str) -> int:
    if stat in ("p50", "median"):
        return _percentile_nearest_rank(sorted_samples, 50.0)
    if stat == "p90":
        return _percentile_nearest_rank(sorted_samples, 90.0)
    if stat == "p99":
        return _percentile_nearest_rank(sorted_samples, 99.0)
    raise ValueError(f"unknown statistic {stat!r}")


def summarize(artifact: RunArtifact) -> dict[str, dict[str, int]]:
    """Derive the gated statistics from an artifact's raw samples (pure)."""
    out: dict[str, dict[str, int]] = {}
    for metric, stats in GATED_STATS.items():
        raw = artifact.samples.get(metric, [])
        ordered = sorted(raw)
        out[metric] = {stat: _stat_value(ordered, stat) for stat in stats}
    return out


def _median_int(values: Sequence[int]) -> int:
    return _percentile_nearest_rank(sorted(values), 50.0)


# ---------------------------------------------------------------------------
# Source identity.
# ---------------------------------------------------------------------------


def benchmark_source_sha256() -> str:
    """SHA-256 of this benchmark source file (ROB-1 artifact identity)."""
    with open(os.path.abspath(__file__), "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


# ---------------------------------------------------------------------------
# Artifact IO (JSON; atomic; ROB-7).
# ---------------------------------------------------------------------------


def _artifact_to_dict(a: RunArtifact) -> dict[str, object]:
    return {
        "schema_version": a.schema_version,
        "source_sha256": a.source_sha256,
        "run_shape": asdict(a.run_shape),
        "config": dict(a.config),
        "environment": asdict(a.environment),
        "timeout_policy": asdict(a.timeout_policy),
        "samples": {m: list(v) for m, v in a.samples.items()},
    }


def _artifact_from_dict(d: Mapping[str, object]) -> RunArtifact:
    schema = int(d["schema_version"])  # type: ignore[call-overload]
    if schema != SCHEMA_VERSION:
        raise GateRefused(f"artifact schema_version {schema} != {SCHEMA_VERSION}")
    return RunArtifact(
        schema_version=schema,
        source_sha256=str(d["source_sha256"]),
        run_shape=RunShape(**d["run_shape"]),  # type: ignore[arg-type]
        config={str(k): str(v) for k, v in dict(d["config"]).items()},  # type: ignore[arg-type]
        environment=Environment(**d["environment"]),  # type: ignore[arg-type]
        timeout_policy=TimeoutPolicy(**d["timeout_policy"]),  # type: ignore[arg-type]
        samples={
            str(m): [int(x) for x in v]  # type: ignore[union-attr]
            for m, v in dict(d["samples"]).items()  # type: ignore[arg-type]
        },
    )


def write_artifact(artifact: RunArtifact, path: str, overwrite: bool) -> None:
    """Atomically write one artifact; refuse an existing path unless overwrite."""
    if os.path.exists(path) and not overwrite:
        raise FileExistsError(f"{path} exists; pass --overwrite to replace it")
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(_artifact_to_dict(artifact), fh, indent=1, sort_keys=True)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def read_artifact(path: str) -> RunArtifact:
    with open(path, "r") as fh:
        return _artifact_from_dict(json.load(fh))


# ---------------------------------------------------------------------------
# Compatibility (ROB-2) -- construct only when the pair is gate-eligible.
# ---------------------------------------------------------------------------

# Environment fields that must match across all compared artifacts (revision,
# timestamp, and label deliberately differ).
_ENV_COMPAT_FIELDS: tuple[str, ...] = (
    "hostname",
    "platform",
    "cpu",
    "python_version",
)


def _compat_key(a: RunArtifact) -> tuple[object, ...]:
    env = a.environment
    return (
        a.schema_version,
        a.source_sha256,
        a.run_shape,
        tuple(sorted(a.config.items())),
        tuple(getattr(env, f) for f in _ENV_COMPAT_FIELDS),
    )


@dataclass(frozen=True)
class CompatiblePair:
    baseline: tuple[RunArtifact, ...]
    candidate: tuple[RunArtifact, ...]

    @staticmethod
    def build(
        baseline: Sequence[RunArtifact], candidate: Sequence[RunArtifact]
    ) -> "CompatiblePair":
        if len(baseline) < MIN_ARTIFACTS_PER_SIDE:
            raise GateRefused(
                f"baseline has {len(baseline)} artifacts; need "
                f">= {MIN_ARTIFACTS_PER_SIDE}"
            )
        if len(candidate) < MIN_ARTIFACTS_PER_SIDE:
            raise GateRefused(
                f"candidate has {len(candidate)} artifacts; need "
                f">= {MIN_ARTIFACTS_PER_SIDE}"
            )
        every = list(baseline) + list(candidate)
        for a in every:
            if a.run_shape.smoke:
                raise GateRefused("smoke artifact is never gate-eligible")
        keys = {_compat_key(a) for a in every}
        if len(keys) != 1:
            raise GateRefused(
                "artifacts are not run-shape/source/environment compatible"
            )
        for a in every:
            _require_reportable(a)
        return CompatiblePair(tuple(baseline), tuple(candidate))


def _require_reportable(a: RunArtifact) -> None:
    for metric, stats in GATED_STATS.items():
        n = len(a.samples.get(metric, []))
        for stat in stats:
            need = STAT_MIN_N[stat]
            if n < need:
                raise GateRefused(f"{metric}.{stat} needs >= {need} samples, has {n}")


# ---------------------------------------------------------------------------
# Threshold policy (calibrated; loaded separately -- never mutates the source).
# ---------------------------------------------------------------------------


def load_threshold_policy(path: str) -> ThresholdPolicy:
    with open(path, "r") as fh:
        d = json.load(fh)
    floors: dict[str, dict[str, Floor]] = {}
    for metric, stat_map in dict(d["floors"]).items():
        floors[str(metric)] = {
            str(stat): Floor(
                relative=float(f["relative"]),
                absolute_ns=int(f["absolute_ns"]),
            )
            for stat, f in dict(stat_map).items()
        }
    return ThresholdPolicy(
        version=str(d["version"]),
        backend=str(d["backend"]),
        source_sha256=str(d["source_sha256"]),
        run_shape=RunShape(**d["run_shape"]),
        floors=floors,
        calibration_artifacts=tuple(str(x) for x in d.get("calibration_artifacts", [])),
    )


def _check_policy_applies(pair: CompatiblePair, policy: ThresholdPolicy) -> None:
    ref = pair.baseline[0]
    if policy.backend != ref.run_shape.backend:
        raise GateRefused(f"no calibrated policy for backend {ref.run_shape.backend!r}")
    if policy.source_sha256 != ref.source_sha256:
        raise GateRefused("threshold policy source hash does not match artifacts")
    if policy.run_shape != ref.run_shape:
        raise GateRefused("threshold policy run shape does not match artifacts")
    for metric, stats in GATED_STATS.items():
        for stat in stats:
            if stat not in policy.floors.get(metric, {}):
                raise GateRefused(f"policy is missing a floor for {metric}.{stat}")


# ---------------------------------------------------------------------------
# Verdict (ROB-8).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StatComparison:
    metric: str
    stat: str
    is_ping: bool
    baseline_ns: int
    candidate_ns: int
    delta_ns: int
    relative: float
    regressed: bool
    moved: bool


@dataclass(frozen=True)
class GateOutcome:
    verdict: Verdict
    reason: str
    comparisons: tuple[StatComparison, ...]

    @property
    def exit_code(self) -> int:
        return EXIT_CODE[self.verdict]


def _aggregate(side: Sequence[RunArtifact], metric: str, stat: str) -> int:
    return _median_int([summarize(a)[metric][stat] for a in side])


def _verdict(pair: CompatiblePair, policy: ThresholdPolicy) -> GateOutcome:
    comparisons: list[StatComparison] = []
    ping_moved = False
    rdma_regressed = False
    for metric, stats in GATED_STATS.items():
        is_ping = metric in PING_METRICS
        for stat in stats:
            b = _aggregate(pair.baseline, metric, stat)
            c = _aggregate(pair.candidate, metric, stat)
            floor = policy.floors[metric][stat]
            delta = c - b
            rel = (delta / b) if b > 0 else math.inf
            regressed = False
            moved = False
            if is_ping:
                # two-sided; a faster ping is also an environment move.
                moved = abs(delta) > floor.absolute_ns and (abs(rel) > floor.relative)
                ping_moved = ping_moved or moved
            else:
                # one-sided; a speedup is never a regression.
                regressed = delta > floor.absolute_ns and rel > floor.relative
                rdma_regressed = rdma_regressed or regressed
            comparisons.append(
                StatComparison(
                    metric=metric,
                    stat=stat,
                    is_ping=is_ping,
                    baseline_ns=b,
                    candidate_ns=c,
                    delta_ns=delta,
                    relative=rel,
                    regressed=regressed,
                    moved=moved,
                )
            )
    if ping_moved:
        verdict = Verdict.INCONCLUSIVE
    elif rdma_regressed:
        verdict = Verdict.REGRESSION
    else:
        verdict = Verdict.PASS
    return GateOutcome(verdict, "", tuple(comparisons))


def evaluate(
    baseline: Sequence[RunArtifact],
    candidate: Sequence[RunArtifact],
    policy: ThresholdPolicy,
) -> GateOutcome:
    """Full gate evaluation: eligibility, then verdict (ROB-2/8)."""
    try:
        pair = CompatiblePair.build(baseline, candidate)
        _check_policy_applies(pair, policy)
    except GateRefused as e:
        return GateOutcome(Verdict.REFUSED, str(e), ())
    return _verdict(pair, policy)


def render(outcome: GateOutcome) -> str:
    lines = [f"verdict: {outcome.verdict.value} (exit {outcome.exit_code})"]
    if outcome.reason:
        lines.append(f"reason: {outcome.reason}")
    for c in outcome.comparisons:
        flag = "REGRESSED" if c.regressed else ("MOVED" if c.moved else "ok")
        lines.append(
            f"  {c.metric}.{c.stat}: {c.baseline_ns} -> {c.candidate_ns} ns "
            f"({c.relative:+.3%}) [{flag}]"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI. `compare` is implemented here; `bench` / `_cold-init-child` land in Diff B.
# ---------------------------------------------------------------------------


def _cmd_compare(args: argparse.Namespace) -> int:
    baseline = [read_artifact(p) for p in args.baseline]
    candidate = [read_artifact(p) for p in args.candidate]
    policy = load_threshold_policy(args.policy)
    outcome = evaluate(baseline, candidate, policy)
    print(render(outcome))
    return outcome.exit_code


def _cmd_not_yet(_args: argparse.Namespace) -> int:
    raise SystemExit("this subcommand lands in Diff B (the collector)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_bench = sub.add_parser("bench", help="collect one artifact (Diff B)")
    p_bench.set_defaults(func=_cmd_not_yet)

    p_cold = sub.add_parser("_cold-init-child", help=argparse.SUPPRESS)
    p_cold.set_defaults(func=_cmd_not_yet)

    p_cmp = sub.add_parser("compare", help="gate two sets of artifacts")
    p_cmp.add_argument("--baseline", nargs="+", required=True)
    p_cmp.add_argument("--candidate", nargs="+", required=True)
    p_cmp.add_argument("--policy", required=True, help="threshold policy JSON")
    p_cmp.set_defaults(func=_cmd_compare)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
