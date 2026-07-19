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

This module is the Monarch-free half: the artifact schema + atomic JSON IO,
`summarize`, `CompatiblePair`, the comparator/verdict algebra, the measurement
helpers (`measure_steady`, `issue_all_then_observe`, the read/write correctness
checks), and the collector's pure helpers (`backend_plan`, `gate_shape`, byte
patterns). The RDMA-driving collector and the CLI live in `collector.py`.

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

import hashlib
import json
import math
import os
import platform
import socket
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Awaitable, Callable, Mapping, Optional, Protocol, Sequence

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


def _sha256_over_files(paths: Sequence[str]) -> str:
    h = hashlib.sha256()
    for path in paths:
        with open(path, "rb") as fh:
            h.update(fh.read())
    return h.hexdigest()


def benchmark_source_sha256() -> str:
    """SHA-256 over the benchmark's source -- both modules, so a change to how a
    measurement is produced (either file) changes artifact identity (ROB-1). The
    analysis half alone would miss changes to the RDMA-driving collector."""
    here = os.path.dirname(os.path.abspath(__file__))
    return _sha256_over_files(
        [os.path.join(here, name) for name in ("benchmark.py", "collector.py")]
    )


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
# Steady-state measurement (ROB-3/4/5). Harness-level and Monarch-free; the
# actors and the RDMA data plane are lazily imported inside the collector so
# importing this module for `compare` never pulls in Monarch.
# ---------------------------------------------------------------------------


class LazyOp(Protocol):
    """A lazy Monarch future: calling `.as_asyncio()` is what dispatches it."""

    def as_asyncio(self) -> Awaitable[object]: ...


@dataclass(frozen=True)
class SteadyTrial:
    prepare: Callable[[], Awaitable[object]]
    issue_and_observe: Callable[[], Awaitable[object]]
    validate: Callable[[object, object], Awaitable[None]]


async def measure_steady(trial: SteadyTrial) -> int:
    """The sole steady-state clock seam (ROB-3): prepare, clock, issue/observe,
    clock, validate. Preparation and validation sit outside the timed region."""
    expected = await trial.prepare()
    t0 = time.perf_counter_ns()
    observed = await trial.issue_and_observe()
    sample_ns = time.perf_counter_ns() - t0
    await trial.validate(expected, observed)
    return sample_ns


async def issue_all_then_observe(make_op: Callable[[int], LazyOp], k: int) -> None:
    """ROB-4: dispatch all K lazy ops before observing the first
    (`issue^K ; observe_all`), never `fold(issue ; observe)`. Monarch futures are
    lazy, so `.as_asyncio()` is what starts each op; hoisting the K calls above the
    drain is what makes the batch concurrent rather than K serial ops."""
    pending = [make_op(i).as_asyncio() for i in range(k)]
    for aw in pending:
        await aw


class WitnessError(AssertionError):
    """A measured transfer failed its correctness witness (ROB-5)."""


def check_read_witness(expected: bytes, observed: bytes) -> None:
    """ROB-5: a read destination is poisoned (to a pattern != source) before every
    timed sample and must equal the source afterward; a dropped/no-op read leaves
    the poison and fails here."""
    if observed != expected:
        raise WitnessError("read destination does not match the source pattern")


def check_write_witness(expected_digest: bytes, holder_digest: bytes) -> None:
    """ROB-5: the holder's digest of the written slot must match the payload's
    digest; a dropped write leaves the prior bytes and fails here."""
    if holder_digest != expected_digest:
        raise WitnessError("write target digest does not match the payload")


# ---------------------------------------------------------------------------
# Collector-side pure helpers (backends, run shape, byte patterns). Monarch-free
# and independently testable; consumed by `collector.py`.
# ---------------------------------------------------------------------------


class UnsupportedBackend(Exception):
    """The requested backend is unavailable and no fallback is allowed."""


def source_pattern(nbytes: int, seed: int) -> bytes:
    return bytes((seed * 31 + i) & 0xFF for i in range(nbytes))


def poison_pattern(nbytes: int) -> bytes:
    return b"\xa5" * nbytes


def write_payload(nbytes: int, counter: int) -> bytes:
    """A write payload stamped with an 8-byte counter (ROB-5), in a byte family
    distinct from the source and poison patterns."""
    body = bytes((0xC0 ^ (i & 0xFF)) for i in range(max(nbytes - 8, 0)))
    return (counter.to_bytes(8, "little") + body)[:nbytes]


PING_PAYLOAD: bytes = bytes(range(PAYLOAD_BYTES))


def backend_plan(
    backend: str, ibverbs_available: bool, skip_unsupported: bool
) -> Optional[tuple[dict[str, str], dict[str, object]]]:
    """Pure backend resolution: returns (recorded config, `configured` kwargs), or
    None to skip (ibverbs unavailable under --skip-unsupported). Raises for an
    unsupported request without --skip-unsupported -- silent TCP fallback is
    forbidden."""
    if backend == "tcp":
        return ({"rdma_disable_ibverbs": "true"}, {"rdma_disable_ibverbs": True})
    if backend == "ibverbs":
        if ibverbs_available:
            return (
                {"rdma_allow_tcp_fallback": "false"},
                {"rdma_allow_tcp_fallback": False},
            )
        if skip_unsupported:
            return None
        raise UnsupportedBackend("ibverbs requested but not available")
    raise ValueError(f"unknown backend {backend!r}")


def gate_shape(backend: str, smoke: bool) -> RunShape:
    if smoke:
        return RunShape(backend, PAYLOAD_BYTES, K, 1, 2, 20, 1, 5, 1, True)
    return RunShape(
        backend,
        PAYLOAD_BYTES,
        K,
        ROUNDS,
        SERIAL_WARMUPS,
        SERIAL_SAMPLES,
        CONCURRENT_WARMUPS,
        CONCURRENT_BATCHES,
        COLD_SAMPLES,
        False,
    )


def environment(label: str) -> Environment:
    return Environment(
        hostname=socket.gethostname(),
        platform=platform.platform(),
        cpu=platform.processor() or "unknown",
        python_version=platform.python_version(),
        monarch_revision="",
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        label=label,
    )
