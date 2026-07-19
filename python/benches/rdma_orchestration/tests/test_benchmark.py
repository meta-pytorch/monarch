# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Pure tests for the RDMA orchestration benchmark: the analysis half and the
measurement helpers. Each test cites the ROB-* law it witnesses. No Monarch/RDMA
import; runs anywhere.
"""

import asyncio
import hashlib
import os
import tempfile
import unittest
from typing import Awaitable, Mapping, Optional

from monarch.python.benches.rdma_orchestration.benchmark import (
    _percentile_nearest_rank,
    _sha256_over_files,
    backend_plan,
    check_read_witness,
    check_write_witness,
    COLD_SAMPLES,
    CompatiblePair,
    CONCURRENT_BATCHES,
    CONCURRENT_WARMUPS,
    Environment,
    evaluate,
    Floor,
    gate_shape,
    GATED_STATS,
    GateRefused,
    issue_all_then_observe,
    K,
    measure_steady,
    MIN_ARTIFACTS_PER_SIDE,
    PAYLOAD_BYTES,
    poison_pattern,
    read_artifact,
    ROUNDS,
    run_block,
    RunArtifact,
    RunShape,
    SERIAL_SAMPLES,
    SERIAL_WARMUPS,
    source_pattern,
    SteadyTrial,
    summarize,
    ThresholdPolicy,
    TimeoutPolicy,
    UnsupportedBackend,
    Verdict,
    WitnessError,
    write_artifact,
    write_payload,
)

# Realized sample counts per metric in the gate shape (5 rounds).
_METRIC_N: Mapping[str, int] = {
    "ping": 2000,
    "serial_read": 2000,
    "serial_write": 2000,
    "concurrent_read": 250,
    "concurrent_write": 250,
    "cold_init": COLD_SAMPLES,
}
_BASE_NS: int = 100_000


def _gate_shape(backend: str = "tcp", smoke: bool = False) -> RunShape:
    return RunShape(
        backend=backend,
        payload_bytes=PAYLOAD_BYTES,
        k=K,
        rounds=ROUNDS,
        serial_warmups=SERIAL_WARMUPS,
        serial_samples=SERIAL_SAMPLES,
        concurrent_warmups=CONCURRENT_WARMUPS,
        concurrent_batches=CONCURRENT_BATCHES,
        cold_samples=COLD_SAMPLES,
        smoke=smoke,
    )


def _artifact(
    values: Optional[Mapping[str, int]] = None,
    *,
    source: str = "hash0",
    backend: str = "tcp",
    smoke: bool = False,
    counts: Optional[Mapping[str, int]] = None,
    hostname: str = "host0",
    label: str = "",
) -> RunArtifact:
    vals = dict(values or {})
    ns = dict(counts or {})
    samples: dict[str, list[int]] = {}
    for metric in GATED_STATS:
        v = vals.get(metric, _BASE_NS)
        n = ns.get(metric, _METRIC_N[metric])
        samples[metric] = [v] * n
    return RunArtifact(
        schema_version=1,
        source_sha256=source,
        run_shape=_gate_shape(backend=backend, smoke=smoke),
        config={},
        environment=Environment(
            hostname=hostname,
            platform="linux",
            cpu="cpu0",
            python_version="3.12",
            monarch_revision="",
            timestamp_utc="2026-07-18T00:00:00Z",
            label=label,
        ),
        timeout_policy=TimeoutPolicy(),
        samples=samples,
    )


def _side(
    values: Optional[Mapping[str, int]] = None, **kw: object
) -> list[RunArtifact]:
    # MIN_ARTIFACTS_PER_SIDE identical artifacts (median aggregate == the value).
    return [_artifact(values, **kw) for _ in range(MIN_ARTIFACTS_PER_SIDE)]  # type: ignore[arg-type]


def _policy(backend: str = "tcp", source: str = "hash0") -> ThresholdPolicy:
    floors: dict[str, dict[str, Floor]] = {
        metric: {stat: Floor(relative=0.10, absolute_ns=1000) for stat in stats}
        for metric, stats in GATED_STATS.items()
    }
    return ThresholdPolicy(
        version="test-1",
        backend=backend,
        source_sha256=source,
        run_shape=_gate_shape(backend=backend),
        floors=floors,
        calibration_artifacts=(),
    )


class PercentileTest(unittest.TestCase):
    def test_nearest_rank(self) -> None:
        # ROB-7: nearest-rank over 1..100.
        xs = list(range(1, 101))
        self.assertEqual(_percentile_nearest_rank(xs, 50.0), 50)
        self.assertEqual(_percentile_nearest_rank(xs, 90.0), 90)
        self.assertEqual(_percentile_nearest_rank(xs, 99.0), 99)
        self.assertEqual(_percentile_nearest_rank([42], 99.0), 42)

    def test_summarize_projects_raw(self) -> None:
        # ROB-7: summary is a pure projection of the raw samples.
        art = _artifact({"serial_read": 7000})
        self.assertEqual(summarize(art)["serial_read"], {"p50": 7000, "p99": 7000})
        self.assertEqual(summarize(art)["cold_init"], {"median": _BASE_NS})


class ArtifactIoTest(unittest.TestCase):
    def test_json_round_trip(self) -> None:
        # ROB-7: raw samples are canonical; round-trip is identity.
        art = _artifact({"ping": 123, "concurrent_write": 456})
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "a.json")
            write_artifact(art, p, overwrite=False)
            self.assertEqual(read_artifact(p), art)

    def test_refuse_existing_without_overwrite(self) -> None:
        # ROB-7: a run publishes atomically and never clobbers silently.
        art = _artifact()
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "a.json")
            write_artifact(art, p, overwrite=False)
            with self.assertRaises(FileExistsError):
                write_artifact(art, p, overwrite=False)
            write_artifact(art, p, overwrite=True)  # explicit overwrite ok


class VerdictTest(unittest.TestCase):
    def test_pass_within_floors(self) -> None:
        # ROB-8: no gated statistic moves beyond its floors.
        out = evaluate(_side(), _side({"serial_read": _BASE_NS + 50}), _policy())
        self.assertEqual(out.verdict, Verdict.PASS)
        self.assertEqual(out.exit_code, 0)

    def test_regression_one_sided(self) -> None:
        # ROB-8: an RDMA metric past both floors is a Regression.
        out = evaluate(_side(), _side({"serial_read": 200_000}), _policy())
        self.assertEqual(out.verdict, Verdict.REGRESSION)
        self.assertEqual(out.exit_code, 2)

    def test_speedup_is_not_regression(self) -> None:
        # ROB-8: a faster RDMA metric is never a regression.
        out = evaluate(_side(), _side({"serial_read": 10_000}), _policy())
        self.assertEqual(out.verdict, Verdict.PASS)

    def test_below_absolute_floor_passes(self) -> None:
        # ROB-8: both floors required; below the absolute floor is not a regression.
        out = evaluate(_side(), _side({"serial_write": _BASE_NS + 500}), _policy())
        self.assertEqual(out.verdict, Verdict.PASS)

    def test_below_relative_floor_passes(self) -> None:
        # ROB-8: above absolute but below relative floor is not a regression.
        out = evaluate(_side(), _side({"serial_write": _BASE_NS + 2000}), _policy())
        self.assertEqual(out.verdict, Verdict.PASS)

    def test_ping_moved_is_inconclusive(self) -> None:
        # ROB-8: ping movement is two-sided and yields Inconclusive.
        out = evaluate(_side(), _side({"ping": 200_000}), _policy())
        self.assertEqual(out.verdict, Verdict.INCONCLUSIVE)
        self.assertEqual(out.exit_code, 3)

    def test_faster_ping_also_inconclusive(self) -> None:
        # ROB-8: a materially faster ping is also an environment move.
        out = evaluate(_side(), _side({"ping": 10_000}), _policy())
        self.assertEqual(out.verdict, Verdict.INCONCLUSIVE)

    def test_ping_absorbs_regression(self) -> None:
        # ROB-8: simultaneous ping move + RDMA regression is Inconclusive, not Regression.
        out = evaluate(
            _side(), _side({"ping": 200_000, "serial_read": 200_000}), _policy()
        )
        self.assertEqual(out.verdict, Verdict.INCONCLUSIVE)


class EligibilityTest(unittest.TestCase):
    def test_refuse_too_few_artifacts(self) -> None:
        # ROB-2/8: fewer than the minimum artifacts per side is not gate-eligible.
        out = evaluate([_artifact()], _side(), _policy())
        self.assertEqual(out.verdict, Verdict.REFUSED)
        self.assertEqual(out.exit_code, 4)

    def test_refuse_smoke(self) -> None:
        # ROB-2: a smoke artifact is never gate-eligible.
        out = evaluate(_side(smoke=True), _side(), _policy())
        self.assertEqual(out.verdict, Verdict.REFUSED)

    def test_refuse_incompatible_source(self) -> None:
        # ROB-1/2: differing source hash breaks compatibility.
        out = evaluate(_side(), _side(source="hashX"), _policy())
        self.assertEqual(out.verdict, Verdict.REFUSED)

    def test_refuse_uncalibrated_backend(self) -> None:
        # ROB-8: no calibrated policy for the artifacts' backend.
        out = evaluate(_side(), _side(), _policy(backend="ibverbs"))
        self.assertEqual(out.verdict, Verdict.REFUSED)

    def test_refuse_undersized_reportability(self) -> None:
        # ROB-2: p99 needs >= 1000 samples; 500 is not reportable.
        thin = {"serial_read": 500}
        out = evaluate(_side(counts=thin), _side(counts=thin), _policy())
        self.assertEqual(out.verdict, Verdict.REFUSED)

    def test_compatible_pair_builds_when_eligible(self) -> None:
        # ROB-2: a compatible, reportable, non-smoke pair constructs.
        pair = CompatiblePair.build(_side(), _side())
        self.assertEqual(len(pair.baseline), MIN_ARTIFACTS_PER_SIDE)

    def test_compatible_pair_raises_on_smoke(self) -> None:
        with self.assertRaises(GateRefused):
            CompatiblePair.build(_side(smoke=True), _side())


class SteadyMeasurementTest(unittest.TestCase):
    def test_measure_steady_orders_phases(self) -> None:
        # ROB-3: prepare -> clock -> issue/observe -> clock -> validate.
        events: list[str] = []

        async def prep() -> object:
            events.append("prepare")
            return "exp"

        async def issue() -> object:
            events.append("issue")
            return "obs"

        async def val(expected: object, observed: object) -> None:
            events.append("validate")
            self.assertEqual((expected, observed), ("exp", "obs"))

        ns = asyncio.run(measure_steady(SteadyTrial(prep, issue, val)))
        self.assertEqual(events, ["prepare", "issue", "validate"])
        self.assertGreaterEqual(ns, 0)

    def test_all_k_dispatched_before_first_observed(self) -> None:
        # ROB-4: every op is dispatched (.as_asyncio) before any is observed.
        order: list[tuple[str, int]] = []

        class FakeOp:
            def __init__(self, i: int) -> None:
                self._i = i

            def as_asyncio(self) -> Awaitable[object]:
                order.append(("dispatch", self._i))

                async def _observe() -> object:
                    order.append(("observe", self._i))
                    return None

                return _observe()

        asyncio.run(issue_all_then_observe(lambda i: FakeOp(i), 3))
        first_observe = next(idx for idx, ev in enumerate(order) if ev[0] == "observe")
        dispatch_idxs = [idx for idx, ev in enumerate(order) if ev[0] == "dispatch"]
        self.assertEqual(len(dispatch_idxs), 3)
        self.assertTrue(all(idx < first_observe for idx in dispatch_idxs))


class WitnessTest(unittest.TestCase):
    def test_read_witness_detects_noop(self) -> None:
        # ROB-5: a poisoned destination left unchanged by a no-op read fails.
        source = bytes(range(64))
        poison = bytes([0xFF] * 64)
        check_read_witness(source, source)
        with self.assertRaises(WitnessError):
            check_read_witness(source, poison)

    def test_write_witness_detects_drop(self) -> None:
        # ROB-5: a dropped write leaves prior bytes; the digest mismatches.
        payload = b"payload-with-counter"
        good = hashlib.sha256(payload).digest()
        check_write_witness(good, good)
        with self.assertRaises(WitnessError):
            check_write_witness(good, hashlib.sha256(b"stale").digest())


class RunBlockTest(unittest.TestCase):
    def test_records_samples_not_warmups_monotonic_index(self) -> None:
        # rounds x (warmups discarded, samples recorded); index monotonic (ROB-5).
        seen: list[int] = []

        def make_trial(i: int) -> SteadyTrial:
            async def prepare() -> object:
                return None

            async def issue() -> object:
                seen.append(i)
                return None

            async def validate(_e: object, _o: object) -> None:
                return None

            return SteadyTrial(prepare, issue, validate)

        out = asyncio.run(run_block(make_trial, rounds=2, warmups=1, samples=3))
        self.assertEqual(len(out), 6)  # 2 rounds x 3 recorded samples
        self.assertEqual(seen, list(range(8)))  # 2 x (1 warmup + 3), monotonic


class BackendPlanTest(unittest.TestCase):
    def test_tcp_disables_ibverbs(self) -> None:
        result = backend_plan("tcp", ibverbs_available=False, skip_unsupported=False)
        assert result is not None
        config, kwargs = result
        self.assertEqual(kwargs, {"rdma_disable_ibverbs": True})
        self.assertEqual(config, {"rdma_disable_ibverbs": "true"})

    def test_ibverbs_when_available(self) -> None:
        result = backend_plan("ibverbs", ibverbs_available=True, skip_unsupported=False)
        assert result is not None
        _config, kwargs = result
        self.assertEqual(kwargs, {"rdma_allow_tcp_fallback": False})

    def test_ibverbs_unavailable_skips(self) -> None:
        # --skip-unsupported turns an unavailable backend into a clean skip.
        self.assertIsNone(
            backend_plan("ibverbs", ibverbs_available=False, skip_unsupported=True)
        )

    def test_ibverbs_unavailable_without_skip_raises(self) -> None:
        # no silent TCP fallback: an unsupported request is an error.
        with self.assertRaises(UnsupportedBackend):
            backend_plan("ibverbs", ibverbs_available=False, skip_unsupported=False)


class ShapeAndPayloadTest(unittest.TestCase):
    def test_smoke_shape_is_smaller(self) -> None:
        gate = gate_shape("tcp", smoke=False)
        smoke = gate_shape("tcp", smoke=True)
        self.assertFalse(gate.smoke)
        self.assertTrue(smoke.smoke)
        self.assertLess(smoke.serial_samples, gate.serial_samples)

    def test_write_payload_stamps_counter(self) -> None:
        # ROB-5: the 8-byte counter prefix makes each write distinguishable.
        p7 = write_payload(PAYLOAD_BYTES, 7)
        p8 = write_payload(PAYLOAD_BYTES, 8)
        self.assertEqual(len(p7), PAYLOAD_BYTES)
        self.assertEqual(int.from_bytes(p7[:8], "little"), 7)
        self.assertNotEqual(p7, p8)

    def test_poison_differs_from_source(self) -> None:
        # ROB-5: poison must differ from the source so a no-op read is caught.
        self.assertNotEqual(
            source_pattern(PAYLOAD_BYTES, 0), poison_pattern(PAYLOAD_BYTES)
        )


class SourceHashTest(unittest.TestCase):
    def test_every_file_affects_the_hash(self) -> None:
        # ROB-1: the identity hash must cover every source file, so a change in any
        # one of them changes it -- this is what a single-file hash missed.
        with tempfile.TemporaryDirectory() as d:
            a = os.path.join(d, "a.py")
            b = os.path.join(d, "b.py")
            with open(a, "wb") as fh:
                fh.write(b"aaa")
            with open(b, "wb") as fh:
                fh.write(b"bbb")
            before = _sha256_over_files([a, b])
            with open(b, "wb") as fh:
                fh.write(b"bbb-changed")
            self.assertNotEqual(before, _sha256_over_files([a, b]))


if __name__ == "__main__":
    unittest.main()
