# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Minimal workload that starts telemetry and exposes /v1/query.

Used by mesh_admin_integration tests to exercise the SQL query
endpoint end-to-end.
"""

import asyncio

from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint
from monarch.distributed_telemetry.actor import start_telemetry


class PingWorker(Actor):
    """Trivial actor that populates the actors/messages tables."""

    @endpoint
    async def ping(self) -> None:
        pass


async def async_main() -> None:
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    host = this_host()
    admin_url = await host._spawn_admin(query_engine=engine)

    # Spawn workers and populate data BEFORE printing the sentinel.
    # The harness treats the sentinel as "ready to query", so all
    # telemetry tables must be populated first.
    procs = host.spawn_procs(per_host={"replica": 2})
    workers = procs.spawn("query_worker", PingWorker)
    await workers.initialized
    await workers.ping.call()

    print(f"Mesh admin server listening on {admin_url}", flush=True)

    try:
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await procs.stop()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
