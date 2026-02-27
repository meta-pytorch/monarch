# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Sleep Actors
============

Spawns actors that sleep for random durations, then terminate. New
actors are continuously spawned to replace them, producing a steady
stream of actor starts and stops. Useful for exercising the admin
TUI's visibility of terminated actors (the ``h`` toggle).

Usage::

    buck2 run fbcode//monarch/python/examples:sleep_actors -- --procs 2

Then, in another terminal::

    buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr <addr>

where ``<addr>`` is the address printed by the example.

"""

import argparse
import asyncio
import random

from monarch.actor import Actor, context, current_rank, endpoint, this_host


class Sleeper(Actor):
    """An actor that sleeps for a random duration and then exits."""

    def __init__(self, min_seconds: float, max_seconds: float) -> None:
        self.min_seconds = min_seconds
        self.max_seconds = max_seconds

    @endpoint
    async def go(self) -> None:
        name = context().actor_instance.name
        rank = current_rank().rank
        duration = random.uniform(self.min_seconds, self.max_seconds)
        print(f"  {name}[{rank}]: sleeping {duration:.1f}s", flush=True)
        await asyncio.sleep(duration)
        print(f"  {name}[{rank}]: done, stopping", flush=True)
        context().actor_instance.stop(f"sleep completed ({duration:.1f}s)")


MIN_SLEEP = 1.0
MAX_SLEEP = 5.0


async def async_main(num_procs: int) -> None:
    host = this_host()

    # Spawn the admin agent so the TUI can attach.
    admin_addr = await host._spawn_admin()
    print(f"\nMesh admin server listening on http://{admin_addr}")
    print(
        f"  TUI: buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr {admin_addr}"
    )
    print(f"\nSpawning batches of sleepers across {num_procs} procs.")
    print("Press Ctrl+C to stop.\n", flush=True)

    procs = host.spawn_procs(per_host={"replica": num_procs})

    batch = 0
    # Keep references alive so actors aren't torn down prematurely.
    batches = []
    try:
        while True:
            name = f"sleeper_batch_{batch}"
            actors = procs.spawn(name, Sleeper, MIN_SLEEP, MAX_SLEEP)
            batches.append(actors)
            print(
                f"batch {batch}: spawned {num_procs} sleepers",
                flush=True,
            )

            # Fire-and-forget: tell each actor to sleep then exit.
            actors.go.broadcast()

            # Wait a bit before the next batch so waves overlap.
            await asyncio.sleep(2.0)
            batch += 1
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        await procs.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sleep actors demo")
    parser.add_argument(
        "--procs", type=int, default=1, help="Number of procs (default: 1)"
    )
    args = parser.parse_args()
    try:
        asyncio.run(async_main(args.procs))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
