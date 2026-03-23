# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Dining Philosophers
===================

A Python implementation of the Dining Philosophers problem using the
Monarch actor API.  Five philosophers sit around a table, each needing
two chopsticks (shared with neighbours) to eat.  A Waiter actor
arbitrates access to prevent deadlock.

The waiter proc mesh demonstrates overlapping proc-to-actor topology:

- **3 waiter proc units** (P0, P1, P2) with **2 overlapping actor meshes**:
  waiter_a on P0+P1, waiter_b on P1+P2 (overlap on P1).
- ``--kill-waiter-proc-after N`` kills the waiter proc mesh after N seconds.
  All 3 proc units die and both actor meshes inherit terminal status via
  proc-level DAG propagation.

Usage::

    buck2 run fbcode//monarch/python/examples:dining_philosophers -- --dashboard

    # Test proc-level DAG propagation:
    buck2 run fbcode//monarch/python/examples:dining_philosophers -- \
        --dashboard --kill-waiter-proc-after 30
"""

import argparse
import asyncio
import os
from enum import auto, Enum
from typing import Any

import monarch.actor
from monarch.actor import Actor, current_rank, endpoint, this_host
from monarch.distributed_telemetry.actor import start_telemetry


def _unhandled_fault(fault):
    """Log but don't crash on unhandled actor faults (e.g. waiter proc death)."""
    print(f"[fault] {fault}", flush=True)


monarch.actor.unhandled_fault_hook = _unhandled_fault


class ChopstickStatus(Enum):
    NONE = auto()
    REQUESTED = auto()
    GRANTED = auto()


class Philosopher(Actor):
    """A philosopher that alternates between thinking and eating."""

    def __init__(self, size: int) -> None:
        self.size = size
        self.rank: int = 0
        self.left_status = ChopstickStatus.NONE
        self.right_status = ChopstickStatus.NONE
        self.waiter: Any = None
        self.meals_eaten: int = 0

    def _chopstick_indices(self) -> tuple[int, int]:
        left = self.rank % self.size
        right = (self.rank + 1) % self.size
        return left, right

    async def _request_chopsticks(self) -> None:
        left, right = self._chopstick_indices()
        self.left_status = ChopstickStatus.REQUESTED
        self.right_status = ChopstickStatus.REQUESTED
        try:
            await self.waiter.request_chopsticks.call_one(self.rank, left, right)
        except Exception:
            print(f"philosopher {self.rank}: waiter unavailable, stopping", flush=True)

    async def _release_chopsticks(self) -> None:
        left, right = self._chopstick_indices()
        self.left_status = ChopstickStatus.NONE
        self.right_status = ChopstickStatus.NONE
        try:
            await self.waiter.release_chopsticks.call_one(left, right)
        except Exception:
            print(f"philosopher {self.rank}: waiter unavailable, stopping", flush=True)

    @endpoint
    async def start(self, waiter: Any) -> None:
        self.rank = current_rank().rank
        self.waiter = waiter
        await self._request_chopsticks()

    @endpoint
    async def grant_chopstick(self, chopstick: int) -> None:
        left, right = self._chopstick_indices()
        if chopstick == left:
            self.left_status = ChopstickStatus.GRANTED
        elif chopstick == right:
            self.right_status = ChopstickStatus.GRANTED

        if (
            self.left_status == ChopstickStatus.GRANTED
            and self.right_status == ChopstickStatus.GRANTED
        ):
            self.meals_eaten += 1
            print(
                f"philosopher {self.rank} is eating (meal {self.meals_eaten})",
                flush=True,
            )
            await asyncio.sleep(1)
            await self._release_chopsticks()
            await asyncio.sleep(0.5)
            await self._request_chopsticks()


class Waiter(Actor):
    """Arbitrates chopstick access to prevent deadlock."""

    def __init__(self, philosophers: Any) -> None:
        self.philosophers = philosophers
        self.assignments: dict[int, int] = {}  # chopstick -> philosopher rank
        self.requests: dict[int, int] = {}  # chopstick -> waiting philosopher rank

    def _try_grant(self, rank: int, chopstick: int) -> None:
        if chopstick not in self.assignments:
            self.assignments[chopstick] = rank
            self.philosophers.slice(replica=rank).grant_chopstick.broadcast(chopstick)
        else:
            self.requests[chopstick] = rank

    def _release(self, chopstick: int) -> None:
        self.assignments.pop(chopstick, None)
        if chopstick in self.requests:
            rank = self.requests.pop(chopstick)
            self._try_grant(rank, chopstick)

    @endpoint
    async def request_chopsticks(self, rank: int, left: int, right: int) -> None:
        self._try_grant(rank, left)
        self._try_grant(rank, right)

    @endpoint
    async def release_chopsticks(self, left: int, right: int) -> None:
        self._release(left)
        self._release(right)


NUM_PHILOSOPHERS = 5


async def async_main(
    dashboard: bool = False,
    dashboard_port: int = 8265,
    kill_waiter_proc_after: float | None = None,
) -> None:
    if dashboard:
        start_telemetry(include_dashboard=True, dashboard_port=dashboard_port)

    host = this_host()

    admin_url = await host._spawn_admin()
    mtls_flags = (
        "--cacert /var/facebook/rootcanal/ca.pem "
        "--cert /var/facebook/x509_identities/server.pem "
        "--key /var/facebook/x509_identities/server.pem "
        if admin_url.startswith("https")
        else ""
    )
    print(f"\nMesh admin server listening on {admin_url}")
    print(f"  - Mesh tree:     curl {mtls_flags}{admin_url}/v1/tree")
    if dashboard:
        dashboard_url = os.environ.get(
            "MONARCH_DASHBOARD_URL", f"http://localhost:{dashboard_port}"
        )
        print(f"  - Dashboard:     {dashboard_url}")
    print("\nPress Ctrl+C to stop.\n", flush=True)

    # --- Philosopher proc mesh ---
    phil_procs = host.spawn_procs(per_host={"replica": NUM_PHILOSOPHERS})
    philosophers = phil_procs.spawn("philosopher", Philosopher, NUM_PHILOSOPHERS)

    # --- Waiter proc mesh: 3 procs, two overlapping actor meshes ---
    # waiter_a on P0+P1, waiter_b on P1+P2 (overlap on P1).
    # Killing waiter_procs tests proc-level DAG propagation:
    # all 3 proc units die → both actor meshes inherit terminal status.
    waiter_procs = host.spawn_procs(name="waiter", per_host={"replica": 3})
    waiter_a = waiter_procs.slice(replica=slice(0, 2)).spawn(
        "waiter_a", Waiter, philosophers
    )
    waiter_b = waiter_procs.slice(replica=slice(1, 3)).spawn(
        "waiter_b", Waiter, philosophers
    )  # noqa: F841 — exists for overlapping topology

    # All philosophers use waiter_a[0] for arbitration.
    # waiter_b exists solely to create the overlapping proc-to-actor mapping.
    philosophers.start.broadcast(waiter_a.slice(replica=0))

    try:
        if kill_waiter_proc_after is not None:
            await asyncio.sleep(kill_waiter_proc_after)
            print("Killing waiter proc mesh...", flush=True)
            await waiter_procs.stop()

        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dining Philosophers")
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch the Monarch Dashboard",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8265,
        help="Dashboard port (default: 8265)",
    )
    parser.add_argument(
        "--kill-waiter-proc-after",
        type=float,
        default=None,
        help="Kill the waiter proc mesh after N seconds (tests proc-level DAG propagation)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            async_main(
                dashboard=args.dashboard,
                dashboard_port=args.dashboard_port,
                kill_waiter_proc_after=args.kill_waiter_proc_after,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
