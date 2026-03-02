# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Stop Mesh
==========

Spawns a mesh of actors, performs a coordinated ``ActorMesh.stop()``,
then idles so the admin TUI can observe the post-mortem state
(tombstones visible via the ``h`` toggle).

Usage::

    buck2 run fbcode//monarch/python/examples:stop_mesh -- --procs 3

Then, in another terminal::

    buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr <addr>

where ``<addr>`` is the address printed by the example.

Press ``h`` in the TUI to toggle visibility of stopped actors.
Press Ctrl+C in this terminal to exit.
"""

import argparse
import asyncio

from monarch.actor import Actor, current_rank, endpoint, this_host


class Worker(Actor):
    """An actor that announces itself and waits to be stopped."""

    @endpoint
    async def hello(self) -> None:
        rank = current_rank().rank
        print(f"  worker[{rank}]: alive", flush=True)


async def async_main(num_procs: int) -> None:
    host = this_host()

    admin_url = await host._spawn_admin()
    cacert = (
        "--cacert /var/facebook/rootcanal/ca.pem "
        if admin_url.startswith("https")
        else ""
    )
    print(f"\nMesh admin server listening on {admin_url}")
    print(f"  - Root node:     curl {cacert}{admin_url}/v1/root")
    print(f"  - Mesh tree:     curl {cacert}{admin_url}/v1/tree")
    print(f"  - API docs:      curl {cacert}{admin_url}/SKILL.md")
    print(
        f"  - TUI:           buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr {admin_url}"
    )
    print(flush=True)

    procs = host.spawn_procs(per_host={"replica": num_procs})
    workers = procs.spawn("worker", Worker)

    # Let each actor announce itself.
    await workers.hello.call()
    print(f"\n{num_procs} workers alive. Stopping the actor mesh...", flush=True)

    # Coordinated mesh-level stop.
    # pyre-ignore[16]: `stop` is on `ActorMesh`, not the proxy type.
    await workers.stop("mesh stopped by example")

    print("Actor mesh stopped. Tombstones are now visible in the TUI (press h).")
    print("Press Ctrl+C to exit.\n", flush=True)

    try:
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        await procs.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stop mesh demo")
    parser.add_argument(
        "--procs", type=int, default=2, help="Number of procs (default: 2)"
    )
    args = parser.parse_args()
    try:
        asyncio.run(async_main(args.procs))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
