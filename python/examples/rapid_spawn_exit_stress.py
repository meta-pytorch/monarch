# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Stress test: rapid spawn_tensor_engine/exit cycles with mesh admin server.

Spawns the admin server and loops spawn/exit to stress-test DashMap
contention between introspection queries and actor lifecycle operations.
See mesh_admin_rca.md for the root cause analysis.

Run with:
    buck2 run fbcode//monarch/python/examples:rapid_spawn_exit_stress -- [--sleep SECONDS] [--iterations N]
"""

import argparse
import asyncio
import time

import monarch.actor
from monarch._src.job.process import ProcessJob
from monarch.actor import Actor, endpoint, this_host
from monarch.mesh_controller import spawn_tensor_engine

job = ProcessJob({"hosts": 1})
proc_mesh = job.state(cached_path=None).hosts.spawn_procs(per_host={"gpus": 1})


def unhandled_fault(fault):
    print(f"Unhandled fault: {fault}", flush=True)
    time.sleep(10000)


monarch.actor.unhandled_fault_hook = unhandled_fault


class SimpleActor(Actor):
    @endpoint
    def ping(self) -> str:
        return "pong"


# for i in range(1000):
#     actor = proc_mesh.spawn("simple", SimpleActor)
#     actor.ping.call_one().get()
#     actor.stop().get()
#     print(f"actor iter {i}", flush=True)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sleep",
        type=float,
        default=0,
        help="Seconds to sleep between spawn and exit (useful for TUI observation)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5000,
        help="Number of spawn/exit iterations",
    )
    args = parser.parse_args()

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

    for i in range(args.iterations):
        dm = spawn_tensor_engine(proc_mesh)
        if args.sleep > 0:
            await asyncio.sleep(args.sleep)
        dm.exit()
        print(f"iter {i}", flush=True)


asyncio.run(main())
