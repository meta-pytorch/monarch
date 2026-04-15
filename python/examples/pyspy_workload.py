# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
py-spy Validation Workload
==========================

A configurable workload designed for validating the ``GET /v1/pyspy``
endpoint.  Each worker proc runs a tight loop of synchronous work so
that py-spy snapshots capture meaningful Python frames instead of
idle event-loop stacks.

Three modes:

- ``cpu``   — iterative CPU burn (arithmetic loop against a monotonic
  deadline).  py-spy stacks show ``process_batch -> do_cpu_work ->
  _cpu_burn_loop``.
- ``block`` — ``time.sleep`` inside the handler.  py-spy stacks show
  ``process_batch -> do_blocking_work -> time.sleep``.
- ``mixed`` — alternates CPU work and ``asyncio.sleep`` phases.

Usage::

    buck2 run fbcode//monarch/python/examples:pyspy_workload -- \\
        --mode cpu --work-ms 500 --concurrency 3

Then verify with::

    buck2 run fbcode//monarch/python/examples:verify_pyspy -- \\
        --admin-url <url> --mode cpu --samples 10
"""

import argparse
import asyncio
import time

from monarch.actor import Actor, endpoint
from monarch.job import ProcessJob, TelemetryConfig


# -- Work helpers with named frames for py-spy visibility ----------
#
# Call chain: work_forever -> process_batch -> do_cpu_work -> _cpu_burn_loop
# Each layer has a distinctive name so py-spy stacks are easy to
# interpret and grep.


def _cpu_burn_loop(deadline: float) -> None:
    """Tight arithmetic loop until deadline."""
    x = 0
    while time.monotonic() < deadline:
        for _ in range(1000):
            x = (x * 1103515245 + 12345) & 0xFFFFFFFF


def do_cpu_work(ms: int) -> None:
    """Busy-loop for *ms* milliseconds."""
    _cpu_burn_loop(time.monotonic() + ms / 1000)


def do_blocking_work(ms: int) -> None:
    """Blocking sleep for *ms* milliseconds."""
    time.sleep(ms / 1000)


def process_batch(mode: str, work_ms: int) -> None:
    """Execute one work phase.  Synchronous — keeps frames on the
    call stack for py-spy to capture."""
    if mode == "cpu":
        do_cpu_work(work_ms)
    elif mode == "block":
        do_blocking_work(work_ms)
    elif mode == "mixed":
        do_cpu_work(work_ms // 2)


# -- Actor -------------------------------------------------------------


class Worker(Actor):
    """Worker that loops doing configurable synchronous work."""

    def __init__(self, mode: str, work_ms: int) -> None:
        self.mode = mode
        self.work_ms = work_ms

    @endpoint
    async def work_forever(self) -> None:
        """Loop indefinitely, doing work in each iteration."""
        while True:
            process_batch(self.mode, self.work_ms)
            if self.mode == "mixed":
                await asyncio.sleep(self.work_ms / 2000)
            # Brief yield lets the event loop make progress between
            # work phases.
            await asyncio.sleep(0.01)


# -- Entry point -------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="py-spy validation workload")
    p.add_argument(
        "--mode",
        choices=["cpu", "block", "mixed"],
        default="cpu",
        help="Work mode (default: cpu)",
    )
    p.add_argument(
        "--work-ms",
        type=int,
        default=500,
        help="Duration of each work phase in ms (default: 500)",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of worker procs (default: 3)",
    )
    return p.parse_args()


async def async_main() -> None:
    args = parse_args()

    job = (
        ProcessJob({"hosts": 1})
        .enable_telemetry(TelemetryConfig(snapshot_interval_secs=30.0))
        .enable_admin()
    )
    state = job.state(cached_path=None)
    host = state.hosts

    admin_url = state.admin_url
    assert admin_url is not None
    mtls_flags = (
        "--cacert /var/facebook/rootcanal/ca.pem "
        "--cert /var/facebook/x509_identities/server.pem "
        "--key /var/facebook/x509_identities/server.pem "
        if admin_url.startswith("https")
        else ""
    )
    print(
        f"\npy-spy workload: mode={args.mode}, "
        f"work_ms={args.work_ms}, concurrency={args.concurrency}"
    )
    print(f"\nMesh admin server listening on {admin_url}")
    print(f"  - Mesh tree:     curl {mtls_flags}{admin_url}/v1/tree")
    print(f"  - API docs:      curl {mtls_flags}{admin_url}/SKILL.md")
    print("\nVerify with:")
    print("  buck2 run fbcode//monarch/python/examples:verify_pyspy -- \\")
    if mtls_flags:
        print(f"    --admin-url {admin_url} --mode {args.mode} --samples 10 \\")
        print(f"    {mtls_flags.strip()}")
    else:
        print(f"    --admin-url {admin_url} --mode {args.mode} --samples 10")
    print("\nPress Ctrl+C to stop.\n", flush=True)

    # Spawn worker procs. The actor name pyspy_worker lets the
    # verifier filter to workload procs by prefix.
    procs = host.spawn_procs(per_host={"replica": args.concurrency})
    workers = procs.spawn("pyspy_worker", Worker, args.mode, args.work_ms)

    # Start all workers.
    workers.work_forever.broadcast()

    try:
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        await procs.stop()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
