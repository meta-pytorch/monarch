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

This example also spawns a MeshAdminAgent so you can attach the admin
TUI to observe the running mesh topology in real time.

Usage::

    buck2 run fbcode//monarch/python/examples:dining_philosophers

Then, in another terminal::

    buck2 run fbcode//monarch/hyperactor_mesh_admin_tui:hyperactor_mesh_admin_tui -- --addr <addr>

where ``<addr>`` is the address printed by the example.

To also launch the Monarch Dashboard for live telemetry::

    buck2 run fbcode//monarch/python/examples:dining_philosophers -- --dashboard
    # Then SSH-tunnel: ssh -L 8265:localhost:8265 <devserver>
    # Open http://localhost:8265 in your browser.

The Buck build automatically bundles the React frontend assets via esbuild,
so the full dashboard UI is available out of the box.
"""

import argparse
import asyncio
import os
from enum import auto, Enum
from typing import Any, cast

from monarch._src.actor.actor_mesh import ActorMesh
from monarch.actor import Actor, current_rank, endpoint, this_host
from monarch.distributed_telemetry.actor import start_telemetry


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
        self.waiter: Any = None  # ActorMesh reference to the Waiter
        self.meals_eaten: int = 0

    def _chopstick_indices(self) -> tuple[int, int]:
        left = self.rank % self.size
        right = (self.rank + 1) % self.size
        return left, right

    async def _request_chopsticks(self) -> None:
        left, right = self._chopstick_indices()
        self.left_status = ChopstickStatus.REQUESTED
        self.right_status = ChopstickStatus.REQUESTED
        await self.waiter.request_chopsticks.call_one(self.rank, left, right)

    async def _release_chopsticks(self) -> None:
        left, right = self._chopstick_indices()
        self.left_status = ChopstickStatus.NONE
        self.right_status = ChopstickStatus.NONE
        await self.waiter.release_chopsticks.call_one(left, right)

    @endpoint
    async def start(self, waiter: Any) -> None:
        """Begin the philosopher's lifecycle."""
        self.rank = current_rank().rank
        self.waiter = waiter
        await self._request_chopsticks()

    @endpoint
    async def grant_chopstick(self, chopstick: int) -> None:
        """Called by the Waiter when a chopstick is granted."""
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
            await asyncio.sleep(1)  # savor the meal
            await self._release_chopsticks()
            await asyncio.sleep(0.5)  # think for a bit
            await self._request_chopsticks()

    @endpoint
    async def get_meals_eaten(self) -> int:
        return self.meals_eaten


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
    kill_waiter_after: float | None = None,
) -> None:
    if dashboard:
        _, telemetry_url = start_telemetry(
            include_dashboard=True, dashboard_port=dashboard_port
        )
        print(f"  - Dashboard:     {telemetry_url}")

    host = this_host()

    # Spawn the admin agent so the TUI can attach.
    admin_url = await host._spawn_admin()
    mtls_flags = (
        "--cacert /var/facebook/rootcanal/ca.pem "
        "--cert /var/facebook/x509_identities/server.pem "
        "--key /var/facebook/x509_identities/server.pem "
        if admin_url.startswith("https")
        else ""
    )
    print(f"\nMesh admin server listening on {admin_url}")
    print(f"  - Root node:     curl {mtls_flags}{admin_url}/v1/root")
    print(f"  - Mesh tree:     curl {mtls_flags}{admin_url}/v1/tree")
    print(f"  - API docs:      curl {mtls_flags}{admin_url}/SKILL.md")
    print(
        f"  - TUI:           buck2 run fbcode//monarch/hyperactor_mesh_admin_tui:hyperactor_mesh_admin_tui -- --addr {admin_url}"
    )
    print("\nPress Ctrl+C to stop.\n", flush=True)

    # Spawn philosopher processes and actors.
    procs = host.spawn_procs(per_host={"replica": NUM_PHILOSOPHERS})

    # Spawn waiter on its own proc mesh so it appears in the dashboard hierarchy.
    waiter_proc = host.spawn_procs(name="waiter")
    philosophers = procs.spawn("philosopher", Philosopher, NUM_PHILOSOPHERS)
    waiter = waiter_proc.spawn("waiter", Waiter, philosophers)

    # Start all philosophers — each will begin requesting chopsticks.
    philosophers.start.broadcast(waiter)

    # Run until interrupted.
    try:
        if kill_waiter_after is not None:
            await asyncio.sleep(kill_waiter_after)
            print("Killing the waiter...")
            cast(ActorMesh[Waiter], waiter).stop().get()
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        await waiter_proc.stop()
        await procs.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Dining Philosophers")
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch the Monarch Dashboard for live telemetry",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8265,
        help="Dashboard port (default: 8265)",
    )
    parser.add_argument(
        "--kill-waiter-after",
        type=float,
        default=None,
        help="Kill the waiter actor after N seconds (for testing error display)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            async_main(
                dashboard=args.dashboard,
                dashboard_port=args.dashboard_port,
                kill_waiter_after=args.kill_waiter_after,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
