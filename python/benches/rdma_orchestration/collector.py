#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
RDMA orchestration benchmark collector -- drives the public Monarch surface to
produce one artifact. See `benchmark.py` for the Monarch-free analysis half and
the ROB-* invariant registry.

`bench` runs the steady-state harness (ping sentinel, serial and concurrent
read/write) plus the cold-start measurement, and writes an artifact. `compare`
gates two artifact sets. `_cold-init-child` is a private, one-sample-per-process
subcommand the parent re-invokes for the cold measurement (ROB-6).
"""

import argparse
import asyncio
import hashlib
import os
import subprocess
import sys
import time
from typing import Mapping, Sequence

from monarch.actor import Actor, endpoint, ProcMesh, this_host
from monarch.config import configured
from monarch.python.benches.rdma_orchestration import benchmark as bm
from monarch.rdma import is_ibverbs_available, RDMABuffer

# The cold child prints its one sample on this tagged stdout line so the parent
# can pick it out of any surrounding runtime chatter.
_COLD_PREFIX: str = "COLD_NS "


class _Holder(Actor):
    """Owns the registered RDMA memory: one read source and K write targets."""

    def __init__(self, payload_bytes: int, k: int) -> None:
        self._source: bytearray = bytearray(bm.source_pattern(payload_bytes, 0))
        self._targets: list[bytearray] = [bytearray(payload_bytes) for _ in range(k)]

    @endpoint
    async def make_buffers(self) -> tuple[RDMABuffer, list[RDMABuffer]]:
        read_src = RDMABuffer(memoryview(self._source))
        targets = [RDMABuffer(memoryview(t)) for t in self._targets]
        return read_src, targets

    @endpoint
    async def source_bytes(self) -> bytes:
        return bytes(self._source)

    @endpoint
    async def slot_digest(self, slot: int) -> bytes:
        return hashlib.sha256(bytes(self._targets[slot])).digest()

    @endpoint
    async def ping(self, payload: bytes) -> bytes:
        return payload

    @endpoint
    async def pid(self) -> int:
        return os.getpid()


class _Driver(Actor):
    """Initiates all transfers against the holder's registered memory."""

    @endpoint
    async def pid(self) -> int:
        return os.getpid()

    @endpoint
    async def smoke(
        self,
        holder: _Holder,
        read_src: RDMABuffer,
        write0: RDMABuffer,
        op_timeout: int,
    ) -> None:
        expected = bytes(await holder.source_bytes.call_one())
        dst = bytearray(bm.poison_pattern(read_src.size()))
        await read_src.read_into(memoryview(dst), timeout=op_timeout)
        bm.check_read_witness(expected, bytes(dst))
        payload = bm.write_payload(write0.size(), 1)
        await write0.write_from(memoryview(payload), timeout=op_timeout)
        bm.check_write_witness(
            hashlib.sha256(payload).digest(),
            bytes(await holder.slot_digest.call_one(0)),
        )

    @endpoint
    async def cold_read(self, read_src: RDMABuffer, op_timeout: int) -> bytes:
        # one read into a poisoned local destination; the caller validates.
        dst = bytearray(bm.poison_pattern(read_src.size()))
        await read_src.read_into(memoryview(dst), timeout=op_timeout)
        return bytes(dst)

    @endpoint
    async def ping_block(
        self,
        holder: _Holder,
        rounds: int,
        warmups: int,
        samples: int,
        payload: bytes,
    ) -> list[int]:
        def make_trial(_i: int) -> bm.SteadyTrial:
            async def prepare() -> object:
                return None

            async def issue() -> object:
                return await holder.ping.call_one(payload)

            async def validate(_e: object, observed: object) -> None:
                if observed != payload:
                    raise bm.WitnessError("ping echo mismatch")

            return bm.SteadyTrial(prepare, issue, validate)

        return await bm.run_block(make_trial, rounds, warmups, samples)

    @endpoint
    async def serial_read_block(
        self,
        holder: _Holder,
        read_src: RDMABuffer,
        rounds: int,
        warmups: int,
        samples: int,
        op_timeout: int,
    ) -> list[int]:
        expected = bytes(await holder.source_bytes.call_one())
        dst = bytearray(read_src.size())

        def make_trial(_i: int) -> bm.SteadyTrial:
            async def prepare() -> object:
                # poison before every sample so a no-op read is caught (ROB-5).
                dst[:] = bm.poison_pattern(len(dst))
                return None

            async def issue() -> object:
                await read_src.read_into(memoryview(dst), timeout=op_timeout)
                return None

            async def validate(_e: object, _o: object) -> None:
                bm.check_read_witness(expected, bytes(dst))

            return bm.SteadyTrial(prepare, issue, validate)

        return await bm.run_block(make_trial, rounds, warmups, samples)

    @endpoint
    async def serial_write_block(
        self,
        holder: _Holder,
        write_target: RDMABuffer,
        slot: int,
        rounds: int,
        warmups: int,
        samples: int,
        op_timeout: int,
    ) -> list[int]:
        box: dict[str, bytes] = {}

        def make_trial(i: int) -> bm.SteadyTrial:
            async def prepare() -> object:
                # a monotonic counter makes each write distinguishable (ROB-5).
                box["payload"] = bm.write_payload(write_target.size(), i + 1)
                return None

            async def issue() -> object:
                await write_target.write_from(
                    memoryview(box["payload"]), timeout=op_timeout
                )
                return None

            async def validate(_e: object, _o: object) -> None:
                digest = bytes(await holder.slot_digest.call_one(slot))
                bm.check_write_witness(hashlib.sha256(box["payload"]).digest(), digest)

            return bm.SteadyTrial(prepare, issue, validate)

        return await bm.run_block(make_trial, rounds, warmups, samples)

    @endpoint
    async def concurrent_read_block(
        self,
        holder: _Holder,
        read_src: RDMABuffer,
        k: int,
        rounds: int,
        warmups: int,
        batches: int,
        op_timeout: int,
    ) -> list[int]:
        # one batch of K parallel reads is one timed sample (ROB-4). K disjoint
        # local destinations so the reads don't collide.
        expected = bytes(await holder.source_bytes.call_one())
        dsts = [bytearray(read_src.size()) for _ in range(k)]

        def make_trial(_i: int) -> bm.SteadyTrial:
            async def prepare() -> object:
                for d in dsts:
                    d[:] = bm.poison_pattern(len(d))
                return None

            async def issue() -> object:
                def make_op(j: int) -> bm.LazyOp:
                    return read_src.read_into(memoryview(dsts[j]), timeout=op_timeout)

                await bm.issue_all_then_observe(make_op, k)
                return None

            async def validate(_e: object, _o: object) -> None:
                for d in dsts:
                    bm.check_read_witness(expected, bytes(d))

            return bm.SteadyTrial(prepare, issue, validate)

        return await bm.run_block(make_trial, rounds, warmups, batches)

    @endpoint
    async def concurrent_write_block(
        self,
        holder: _Holder,
        write_targets: list[RDMABuffer],
        k: int,
        rounds: int,
        warmups: int,
        batches: int,
        op_timeout: int,
    ) -> list[int]:
        # one batch of K parallel writes into K disjoint targets is one timed
        # sample (ROB-4); each slot carries a distinct counter (ROB-5).
        payloads: list[bytes] = [b""] * k

        def make_trial(i: int) -> bm.SteadyTrial:
            async def prepare() -> object:
                for j in range(k):
                    payloads[j] = bm.write_payload(
                        write_targets[j].size(), i * k + j + 1
                    )
                return None

            async def issue() -> object:
                def make_op(j: int) -> bm.LazyOp:
                    return write_targets[j].write_from(
                        memoryview(payloads[j]), timeout=op_timeout
                    )

                await bm.issue_all_then_observe(make_op, k)
                return None

            async def validate(_e: object, _o: object) -> None:
                for j in range(k):
                    digest = bytes(await holder.slot_digest.call_one(j))
                    bm.check_write_witness(hashlib.sha256(payloads[j]).digest(), digest)

            return bm.SteadyTrial(prepare, issue, validate)

        return await bm.run_block(make_trial, rounds, warmups, batches)


async def _assert_distinct(holder: _Holder, driver: _Driver) -> None:
    holder_pid = int(await holder.pid.call_one())
    driver_pid = int(await driver.pid.call_one())
    if len({os.getpid(), holder_pid, driver_pid}) != 3:
        raise bm.WitnessError("holder, driver, and client must be distinct procs")


async def _teardown(holder_procs: ProcMesh, driver_procs: ProcMesh) -> None:
    # Stopping the workers sends them SIGTERM; folly's fatal-signal handler prints
    # a stack trace for it even though this is a clean, intentional shutdown. Warn
    # first so that trace is not mistaken for a crash. Goes to stderr so it never
    # mixes with the artifact path on stdout or the cold child's COLD_NS line.
    print(
        "note: shutting down benchmark worker procs -- any '*** Signal 15 "
        "(SIGTERM) ***' stack trace below is the expected teardown of the procs "
        "this benchmark spawned, and is benign.",
        file=sys.stderr,
        flush=True,
    )
    for procs in (holder_procs, driver_procs):
        try:
            await asyncio.wait_for(procs.stop(), timeout=bm.TimeoutPolicy().teardown_s)
        except Exception:
            pass


async def _bench_async(
    shape: bm.RunShape,
    config: Mapping[str, str],
    cm_kwargs: Mapping[str, object],
    out: str,
    overwrite: bool,
    label: str,
    timeout: int,
    cold_init: list[int],
) -> None:
    with configured(**cm_kwargs):
        holder_procs = this_host().spawn_procs(per_host={"processes": 1})
        driver_procs = this_host().spawn_procs(per_host={"processes": 1})
        holder = holder_procs.spawn(
            "rdma_bench_holder", _Holder, shape.payload_bytes, shape.k
        )
        driver = driver_procs.spawn("rdma_bench_driver", _Driver)
        try:
            await _assert_distinct(holder, driver)
            read_src, targets = await holder.make_buffers.call_one()
            # smoke: prove the data plane end to end and resolve lazy init.
            await driver.smoke.call_one(holder, read_src, targets[0], timeout)
            ping = await driver.ping_block.call_one(
                holder,
                shape.rounds,
                shape.serial_warmups,
                shape.serial_samples,
                bm.PING_PAYLOAD,
            )
            serial_read = await driver.serial_read_block.call_one(
                holder,
                read_src,
                shape.rounds,
                shape.serial_warmups,
                shape.serial_samples,
                timeout,
            )
            serial_write = await driver.serial_write_block.call_one(
                holder,
                targets[0],
                0,
                shape.rounds,
                shape.serial_warmups,
                shape.serial_samples,
                timeout,
            )
            concurrent_read = await driver.concurrent_read_block.call_one(
                holder,
                read_src,
                shape.k,
                shape.rounds,
                shape.concurrent_warmups,
                shape.concurrent_batches,
                timeout,
            )
            concurrent_write = await driver.concurrent_write_block.call_one(
                holder,
                targets,
                shape.k,
                shape.rounds,
                shape.concurrent_warmups,
                shape.concurrent_batches,
                timeout,
            )
            samples: dict[str, list[int]] = {
                "ping": ping,
                "serial_read": serial_read,
                "serial_write": serial_write,
                "concurrent_read": concurrent_read,
                "concurrent_write": concurrent_write,
                "cold_init": cold_init,
            }
            artifact = bm.RunArtifact(
                schema_version=bm.SCHEMA_VERSION,
                source_sha256=bm.benchmark_source_sha256(),
                run_shape=shape,
                config=dict(config),
                environment=bm.environment(label),
                timeout_policy=bm.TimeoutPolicy(),
                samples=samples,
            )
            bm.write_artifact(artifact, out, overwrite)
        finally:
            await _teardown(holder_procs, driver_procs)


async def _cold_child_async(
    payload_bytes: int, k: int, cm_kwargs: Mapping[str, object], op_timeout: int
) -> int:
    """One cold-start sample in this fresh process (ROB-6). The topology is proven
    live before t0 with no RDMA; the timed interval is the first holder-side init
    (`make_buffers`) plus the first driver read, on this process's clock."""
    with configured(**cm_kwargs):
        holder_procs = this_host().spawn_procs(per_host={"processes": 1})
        driver_procs = this_host().spawn_procs(per_host={"processes": 1})
        holder = holder_procs.spawn("rdma_cold_holder", _Holder, payload_bytes, k)
        driver = driver_procs.spawn("rdma_cold_driver", _Driver)
        try:
            await _assert_distinct(holder, driver)
            expected = bytes(await holder.source_bytes.call_one())
            t0 = time.perf_counter_ns()
            read_src, _targets = await holder.make_buffers.call_one()
            observed = bytes(await driver.cold_read.call_one(read_src, op_timeout))
            t1 = time.perf_counter_ns()
            bm.check_read_witness(expected, observed)
            return t1 - t0
        finally:
            await _teardown(holder_procs, driver_procs)


def _collect_cold_samples(
    backend: str, n: int, timeout_s: int, ibverbs_target: str | None
) -> list[int]:
    """Run `n` fresh `_cold-init-child` subprocesses and collect one sample each
    (ROB-6). Each child is bounded by `timeout_s` (ROB-9); any failure or timeout
    aborts the whole run so no partial artifact is published. The child inherits
    `ibverbs_target` so cold samples pin the same NIC as the steady-state ones."""
    cmd = [sys.argv[0], "_cold-init-child", "--backend", backend]
    if ibverbs_target is not None:
        cmd += ["--ibverbs-target", ibverbs_target]
    samples: list[int] = []
    for _ in range(n):
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"cold-init child failed (exit {proc.returncode}): "
                f"{proc.stderr.strip()[-500:]}"
            )
        value: int | None = None
        for line in proc.stdout.splitlines():
            if line.startswith(_COLD_PREFIX):
                value = int(line[len(_COLD_PREFIX) :])
                break
        if value is None:
            raise RuntimeError("cold-init child produced no sample line")
        samples.append(value)
    return samples


def _cmd_bench(args: argparse.Namespace) -> int:
    shape = bm.gate_shape(args.backend, args.smoke)
    try:
        plan = bm.backend_plan(
            args.backend,
            is_ibverbs_available(),
            args.skip_unsupported,
            args.ibverbs_target,
        )
    except bm.UnsupportedBackend as e:
        print(f"error: {e}")
        return 1
    if plan is None:
        print("SKIPPED: ibverbs requested but unavailable")
        return 0
    config, cm_kwargs = plan
    if args.backend == "ibverbs":
        # This benchmark registers host (CPU) memory, so RdmaXcel finds no CUDA
        # context and logs "[RdmaXcel] Failed to get current CUDA context" once
        # per registration. That is expected on the host-memory path and is
        # benign -- the ibverbs data plane is unaffected. Warn once so the noise
        # is not mistaken for a failure. Goes to stderr so it never mixes with
        # the artifact path on stdout.
        print(
            "note: the '[RdmaXcel] Failed to get current CUDA context' lines "
            "below are expected -- this benchmark registers host (CPU) memory, "
            "which has no CUDA context. The ibverbs transfers still run over the "
            "NIC; the messages are benign.",
            file=sys.stderr,
            flush=True,
        )
    cold_init = _collect_cold_samples(
        args.backend,
        shape.cold_samples,
        bm.TimeoutPolicy().cold_child_s,
        args.ibverbs_target,
    )
    asyncio.run(
        _bench_async(
            shape,
            config,
            cm_kwargs,
            args.out,
            args.overwrite,
            args.label,
            args.timeout,
            cold_init,
        )
    )
    print(f"wrote {args.out}")
    return 0


def _cmd_compare(args: argparse.Namespace) -> int:
    baseline = [bm.read_artifact(p) for p in args.baseline]
    candidate = [bm.read_artifact(p) for p in args.candidate]
    policy = bm.load_threshold_policy(args.policy)
    outcome = bm.evaluate(baseline, candidate, policy)
    print(bm.render(outcome))
    return outcome.exit_code


def _cmd_calibrate(args: argparse.Namespace) -> int:
    artifacts = [bm.read_artifact(p) for p in args.artifacts]
    try:
        policy, diagnostics = bm.calibrate(artifacts, args.version)
    except bm.GateRefused as e:
        print(f"error: {e}")
        return 1
    bm.write_threshold_policy(policy, args.out, args.overwrite)
    print(f"wrote {args.out} ({policy.backend}, {len(artifacts)} artifacts)")
    print("minimum detectable effect per statistic:")
    for d in diagnostics:
        print(
            f"  {d.metric}.{d.stat}: MDE {d.mde:.2%} "
            f"(floor {d.floor.absolute_ns} ns / {d.floor.relative:.2%}, "
            f"median {d.median_ns} ns, Dmax {d.dmax_ns} ns)"
        )
    return 0


def _cmd_cold_child(args: argparse.Namespace) -> int:
    plan = bm.backend_plan(
        args.backend,
        is_ibverbs_available(),
        skip_unsupported=False,
        ibverbs_target=args.ibverbs_target,
    )
    if plan is None:  # pragma: no cover - the parent only spawns supported backends
        raise SystemExit("cold-init child: backend unavailable")
    _config, cm_kwargs = plan
    shape = bm.gate_shape(args.backend, smoke=False)
    ns = asyncio.run(
        _cold_child_async(shape.payload_bytes, shape.k, cm_kwargs, args.timeout)
    )
    print(f"{_COLD_PREFIX}{ns}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_bench = sub.add_parser("bench", help="collect one artifact")
    p_bench.add_argument("--out", required=True)
    p_bench.add_argument("--overwrite", action="store_true")
    p_bench.add_argument("--backend", choices=["tcp", "ibverbs"], default="tcp")
    p_bench.add_argument("--smoke", action="store_true")
    p_bench.add_argument("--skip-unsupported", action="store_true")
    p_bench.add_argument("--label", default="")
    p_bench.add_argument("--timeout", type=int, default=bm.TimeoutPolicy().op_timeout_s)
    p_bench.add_argument(
        "--ibverbs-target",
        default=None,
        help=(
            "ibverbs device to pin: cpu:<numa>, gpu:<ordinal>, or nic:<name> "
            "(e.g. nic:mlx5_0). Required for --backend ibverbs; without it host "
            "memory is hash-spread across NICs and the data plane is not "
            "reproducible."
        ),
    )
    p_bench.set_defaults(func=_cmd_bench)

    p_cold = sub.add_parser("_cold-init-child", help=argparse.SUPPRESS)
    p_cold.add_argument("--backend", choices=["tcp", "ibverbs"], default="tcp")
    p_cold.add_argument("--timeout", type=int, default=bm.TimeoutPolicy().op_timeout_s)
    p_cold.add_argument("--ibverbs-target", default=None)
    p_cold.set_defaults(func=_cmd_cold_child)

    p_cmp = sub.add_parser("compare", help="gate two sets of artifacts")
    p_cmp.add_argument("--baseline", nargs="+", required=True)
    p_cmp.add_argument("--candidate", nargs="+", required=True)
    p_cmp.add_argument("--policy", required=True, help="threshold policy JSON")
    p_cmp.set_defaults(func=_cmd_compare)

    p_cal = sub.add_parser("calibrate", help="derive a threshold policy from artifacts")
    p_cal.add_argument("--artifacts", nargs="+", required=True)
    p_cal.add_argument("--out", required=True, help="output threshold policy JSON")
    p_cal.add_argument("--version", required=True, help="policy version label")
    p_cal.add_argument("--overwrite", action="store_true")
    p_cal.set_defaults(func=_cmd_calibrate)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
