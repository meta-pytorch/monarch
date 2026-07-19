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

`bench` runs the steady-state harness -- currently distinct-proc setup, one
validated read + write smoke, and the ping sentinel -- and writes an artifact
(ROB-1/9). `compare` gates two artifact sets. `_cold-init-child` is private.
"""

import argparse
import asyncio
import hashlib
import os
import sys
from typing import Mapping, Sequence

from monarch.actor import Actor, endpoint, this_host
from monarch.config import configured
from monarch.python.benches.rdma_orchestration import benchmark as bm
from monarch.rdma import is_ibverbs_available, RDMABuffer


async def _bench_async(
    shape: bm.RunShape,
    config: Mapping[str, str],
    cm_kwargs: Mapping[str, object],
    out: str,
    overwrite: bool,
    label: str,
    timeout: int,
) -> None:
    payload_bytes = shape.payload_bytes
    k = shape.k

    class _Holder(Actor):
        def __init__(self) -> None:
            self._source: bytearray = bytearray(bm.source_pattern(payload_bytes, 0))
            self._targets: list[bytearray] = [
                bytearray(payload_bytes) for _ in range(k)
            ]

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
                    bm.check_write_witness(
                        hashlib.sha256(box["payload"]).digest(), digest
                    )

                return bm.SteadyTrial(prepare, issue, validate)

            return await bm.run_block(make_trial, rounds, warmups, samples)

    with configured(**cm_kwargs):
        holder_procs = this_host().spawn_procs(per_host={"processes": 1})
        driver_procs = this_host().spawn_procs(per_host={"processes": 1})
        holder = holder_procs.spawn("rdma_bench_holder", _Holder)
        driver = driver_procs.spawn("rdma_bench_driver", _Driver)
        try:
            holder_pid = int(await holder.pid.call_one())
            driver_pid = int(await driver.pid.call_one())
            if len({os.getpid(), holder_pid, driver_pid}) != 3:
                raise bm.WitnessError(
                    "holder, driver, and client must be distinct procs"
                )
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
            samples: dict[str, list[int]] = {
                "ping": ping,
                "serial_read": serial_read,
                "serial_write": serial_write,
                "concurrent_read": [],
                "concurrent_write": [],
                "cold_init": [],
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
            for procs in (holder_procs, driver_procs):
                try:
                    await asyncio.wait_for(
                        procs.stop(), timeout=bm.TimeoutPolicy().teardown_s
                    )
                except Exception:
                    pass


def _cmd_bench(args: argparse.Namespace) -> int:
    shape = bm.gate_shape(args.backend, args.smoke)
    try:
        plan = bm.backend_plan(
            args.backend, is_ibverbs_available(), args.skip_unsupported
        )
    except bm.UnsupportedBackend as e:
        print(f"error: {e}")
        return 1
    if plan is None:
        print("SKIPPED: ibverbs requested but unavailable")
        return 0
    config, cm_kwargs = plan
    asyncio.run(
        _bench_async(
            shape,
            config,
            cm_kwargs,
            args.out,
            args.overwrite,
            args.label,
            args.timeout,
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


def _cmd_cold_child(_args: argparse.Namespace) -> int:
    raise SystemExit("the cold-init child is not implemented yet")


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
    p_bench.set_defaults(func=_cmd_bench)

    p_cold = sub.add_parser("_cold-init-child", help=argparse.SUPPRESS)
    p_cold.set_defaults(func=_cmd_cold_child)

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
