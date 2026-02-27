# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Poisoned Mesh
================

Spawns a mesh of workers across multiple procs, then deliberately
crashes one worker. The failure poisons its proc (no new spawns
accepted), while sibling procs remain healthy. The mesh stays up
so the admin TUI can inspect the post-mortem state:

- The failed actor's ``FailureInfo`` (error message, root cause,
  timestamp, ``is_propagated``).
- The poisoned proc's ``is_poisoned=true`` and ``failed_actor_count``.
- Healthy procs and running actors for contrast.

Usage::

    buck2 run fbcode//monarch/python/examples:poisoned_mesh -- --procs 3

Then, in another terminal::

    buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr <addr>

where ``<addr>`` is the address printed by the example.

Press Ctrl+C to exit.
"""

import argparse
import asyncio
import sys

import monarch.actor
from monarch.actor import Actor, current_rank, endpoint, this_host


def _fault_hook(failure) -> None:
    """Override the default unhandled_fault_hook (which calls sys.exit(1))
    to just log the failure and keep the client process alive."""
    print(
        f"\n  [fault hook] {failure.report()}",
        file=sys.stderr,
        flush=True,
    )


# Install before any mesh operations so the RootClientActor picks it up.
monarch.actor.unhandled_fault_hook = _fault_hook


class ActorCrash(BaseException):
    """A BaseException subclass that triggers actor supervision.

    Regular Exception subclasses are caught by the endpoint handler
    and returned to the caller via the response port — the actor
    survives.  BaseException bypasses that catch, so the actor dies
    and a supervision event is generated.
    """

    pass


class Worker(Actor):
    """A worker that can do work or crash on command."""

    @endpoint
    async def work(self) -> None:
        rank = current_rank().rank
        print(f"  worker[{rank}]: doing work", flush=True)

    @endpoint
    async def crash(self, reason: str) -> None:
        rank = current_rank().rank
        print(f"  worker[{rank}]: crashing: {reason}", flush=True)
        raise ActorCrash(reason)


async def async_main(num_procs: int) -> None:
    host = this_host()

    admin_addr = await host._spawn_admin()
    print(f"\nMesh admin: http://{admin_addr}")
    print(
        f"  TUI: buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr {admin_addr}"
    )
    print(flush=True)

    procs = host.spawn_procs(per_host={"replica": num_procs})
    workers = procs.spawn("worker", Worker)

    # Let every worker do some work first.
    await workers.work.call()
    print(f"\n{num_procs} workers alive and working.", flush=True)

    # Crash worker at rank 0.  The BaseException subclass bypasses the
    # endpoint handler's Exception catch, killing the actor and
    # triggering supervision.
    print("\nCrashing worker[0] with 'GPU memory corruption'...", flush=True)
    try:
        await workers.slice(replica=0).crash.call_one("GPU memory corruption")
    except Exception:
        pass  # Expected — the actor died

    # Give supervision time to propagate.
    await asyncio.sleep(3)

    print("\nFailure injected. Point the TUI at this mesh to inspect.")
    print("Press Ctrl+C to exit.\n", flush=True)

    try:
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        await procs.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Poisoned mesh example")
    parser.add_argument(
        "--procs", type=int, default=3, help="Number of procs (default: 3)"
    )
    args = parser.parse_args()
    try:
        asyncio.run(async_main(args.procs))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
