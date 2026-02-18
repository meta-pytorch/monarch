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

    buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr <addr>

where ``<addr>`` is the address printed by the example.
"""

import asyncio
from enum import auto, Enum
from typing import Any

from monarch.actor import Actor, current_rank, endpoint, this_host, this_proc


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


async def async_main() -> None:
    host = this_host()

    # Spawn the admin agent so the TUI can attach.
    admin_addr = await host._spawn_admin()
    print(f"\nMesh admin server listening on http://{admin_addr}")
    print(
        f"  TUI: buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr {admin_addr}"
    )
    print("\nPress Ctrl+C to stop.\n", flush=True)

    # Spawn philosopher processes and actors.
    procs = host.spawn_procs(per_host={"replica": NUM_PHILOSOPHERS})

    # Spawn waiter on the local proc (single instance).
    philosophers = procs.spawn("philosopher", Philosopher, NUM_PHILOSOPHERS)
    waiter = this_proc().spawn("waiter", Waiter, philosophers)

    # Start all philosophers — each will begin requesting chopsticks.
    philosophers.start.broadcast(waiter)

    # Run until interrupted.
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
