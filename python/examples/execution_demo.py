# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Execution-surface observability demo (dining philosophers).

A run-and-watch companion to the ``execution`` mesh-admin surface: point the
TUI at it and watch live, human-readable work move through the system. Unlike
``execution_workload.py`` (the deterministic, stdin-driven proof), this needs
no manual control -- it just runs.

Each philosopher cycles two REAL endpoints, ``think`` and ``eat``, driven
externally so the active handler name turns over in the TUI (a single internal
``run()`` loop would only ever show ``run``). Forks are arbitrated by one
``ForkManager``, so browsing it shows ``acquire xK`` -- the philosophers
currently blocked on a neighbor's fork, i.e. live contention.

What to watch in the TUI:

  - a ``philosopher`` actor's Execution pane: ``think`` / ``eat`` turning over.
  - the ``fork_manager`` actor's Execution pane: ``acquire xK`` + oldest age.
  - either actor's Flight Recorder pane: recent ``think`` / ``eat`` /
    ``acquire`` / ``release`` lines.

Usage::

    buck2 run fbcode//monarch/python/examples:execution_demo -- --philosophers 5
"""

import argparse
import asyncio
import logging
import random

from monarch._src.actor.telemetry import TracingForwarder
from monarch.actor import Actor, context, endpoint
from monarch.job import ProcessJob

logger: logging.Logger = logging.getLogger("execution_demo")
logger.addHandler(TracingForwarder())
logger.setLevel(logging.INFO)


class ForkManager(Actor):
    """Arbitrates the shared forks: one ``asyncio.Lock`` per fork id.

    Both endpoints sort the ``(left, right)`` pair internally, so a sorted
    acquire is deadlock-safe and the manager does not trust callers for
    ordering. The locks coordinate only the calls landing on this single actor
    -- exactly the model -- and acquiring in ``acquire`` / releasing in
    ``release`` is sound since ``asyncio.Lock`` has no owner check.
    """

    def __init__(self, num_forks: int) -> None:
        self._forks: list[asyncio.Lock] = [asyncio.Lock() for _ in range(num_forks)]

    @endpoint
    async def acquire(self, left: int, right: int) -> None:
        lo, hi = sorted((left, right))
        logger.info("acquire forks %d,%d", lo, hi)
        await self._forks[lo].acquire()
        await self._forks[hi].acquire()

    @endpoint
    async def release(self, left: int, right: int) -> None:
        lo, hi = sorted((left, right))
        logger.info("release forks %d,%d", lo, hi)
        self._forks[hi].release()
        self._forks[lo].release()

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


class Philosopher(Actor):
    """One philosopher cycling ``think`` -> ``eat``. Both are real endpoints so
    the TUI's Execution pane shows the phase name turning over."""

    def __init__(
        self,
        forks,
        num_seats: int,
        think_ms: tuple[int, int],
        eat_ms: tuple[int, int],
    ) -> None:
        # `forks` is the ForkManager actor-mesh handle returned by spawn() -- a
        # proxy exposing endpoint adverbs (`.acquire.call_one(...)`), not a
        # ForkManager instance. Left unannotated to match the sibling examples.
        self._forks = forks
        self._num_seats = num_seats
        self._think_ms = think_ms
        self._eat_ms = eat_ms

    @endpoint
    async def think(self, rank: int) -> None:
        # `rank` is passed by the driver: `current_rank()` re-bases to 0 under
        # `slice(replica=i)`, so it can't identify the philosopher here.
        logger.info("think rank=%d", rank)
        await asyncio.sleep(random.uniform(*self._think_ms) / 1000)

    @endpoint
    async def eat(self, rank: int) -> None:
        left = rank % self._num_seats
        right = (rank + 1) % self._num_seats
        await self._forks.acquire.call_one(left, right)
        try:
            logger.info("eat rank=%d forks %d,%d", rank, left, right)
            await asyncio.sleep(random.uniform(*self._eat_ms) / 1000)
        finally:
            # Always release, even if the eat is interrupted, so a stray
            # cancellation never leaks forks and wedges the demo.
            await self._forks.release.call_one(left, right)

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


async def async_main(args: argparse.Namespace) -> None:
    n = args.philosophers
    think_ms = (args.think_ms_min, args.think_ms_max)
    eat_ms = (args.eat_ms_min, args.eat_ms_max)

    job = ProcessJob({"hosts": 1}).enable_admin()
    state = job.state(cached_path=None)
    host = state.hosts

    # N philosophers (one per replica) + a single fork manager on its own proc
    # so it is a distinct, easy-to-find node in the TUI tree.
    phil_procs = host.spawn_procs(per_host={"replica": n})
    fork_proc = host.spawn_procs(name="fork_manager")

    fork_manager = fork_proc.spawn("fork_manager", ForkManager, n)
    philosophers = phil_procs.spawn(
        "philosopher", Philosopher, fork_manager, n, think_ms, eat_ms
    )

    fork_ref = await fork_manager.whoami.call_one()
    phil0_ref = await philosophers.slice(replica=0).whoami.call_one()

    admin_url = state.admin_url
    assert admin_url is not None
    mtls_flags = (
        "--cacert /var/facebook/rootcanal/ca.pem "
        "--cert /var/facebook/x509_identities/server.pem "
        "--key /var/facebook/x509_identities/server.pem "
        if admin_url.startswith("https")
        else ""
    )
    tui = (
        "buck2 run fbcode//monarch/hyperactor_mesh_admin_tui:hyperactor_mesh_admin_tui"
    )
    print(f"\nMesh admin server listening on {admin_url}")
    print(f"  - Mesh tree:  curl {mtls_flags}{admin_url}/v1/tree")
    print(f"  - TUI:        {tui} -- --addr {admin_url}")
    print("\nWatch list (find these in the TUI tree by label):")
    print(
        "  - actor `fork_manager`  -> Execution: `acquire xK` (live contention) + flight recorder"
    )
    print(
        f"  - actor `philosopher` (replicas 0..{n - 1}) -> Execution: `think`/`eat` turnover + flight recorder"
    )
    print(f"  raw ids: fork_manager={fork_ref}  philosopher[0]={phil0_ref}")
    print(
        f"\n{n} philosophers; think {think_ms[0]}-{think_ms[1]}ms, "
        f"eat {eat_ms[0]}-{eat_ms[1]}ms. Press Ctrl+C to stop.\n",
        flush=True,
    )

    async def run_philosopher(i: int) -> None:
        seat = philosophers.slice(replica=i)
        while True:
            await seat.think.call_one(i)
            await seat.eat.call_one(i)

    # Fail-fast: gather propagates the first loop's failure and tears the demo
    # down, surfacing bugs loudly rather than silently degrading.
    loops = [asyncio.ensure_future(run_philosopher(i)) for i in range(n)]
    try:
        await asyncio.gather(*loops)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        for loop in loops:
            loop.cancel()
        await phil_procs.stop()
        await fork_proc.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="execution-surface dining-philosophers observability demo"
    )
    parser.add_argument("--philosophers", type=int, default=5)
    parser.add_argument("--think-ms-min", type=int, default=1500)
    parser.add_argument("--think-ms-max", type=int, default=4000)
    parser.add_argument("--eat-ms-min", type=int, default=1000)
    parser.add_argument("--eat-ms-max", type=int, default=2000)
    args = parser.parse_args()
    if args.philosophers < 2:
        parser.error(
            "--philosophers must be >= 2 (with 1, a philosopher's two forks are "
            "the same fork and acquire() would self-deadlock)"
        )
    for lo, hi, name in (
        (args.think_ms_min, args.think_ms_max, "think"),
        (args.eat_ms_min, args.eat_ms_max, "eat"),
    ):
        if lo <= 0 or lo > hi:
            parser.error(f"--{name}-ms-min must be > 0 and <= --{name}-ms-max")
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
