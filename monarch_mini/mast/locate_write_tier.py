# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Print the scheduler write tier that serves a MAST cluster.

``build_mast.py`` is stdlib-only, so it runs this through ``buck2 run`` to ask the
shared scheduler router which frontend serves the cluster.
"""

from __future__ import annotations

import argparse
import asyncio

from msl_scheduler.lib.mast_scheduler_locator import get_router, SchedulerType

_CLIENT_ID = "monarch_mini_build_mast"


async def locate_write_tier(cluster: str) -> str:
    route = await get_router(_CLIENT_ID).get_read_route_for_cluster(
        cluster, cache_incomplete_routes=False, fail_open=False
    )
    write_tier = route.located_write_tier
    if (
        route.scheduler_type not in (SchedulerType.MAST, SchedulerType.MSL)
        or not route.scheduler_type_was_located
        or write_tier is None
    ):
        raise SystemExit(f"the locator did not identify a frontend for {cluster}")
    return write_tier


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cluster", help="MAST cluster, e.g. CPUTrainingWorkloads")
    args = parser.parse_args()
    print(asyncio.run(locate_write_tier(args.cluster)))


if __name__ == "__main__":
    main()
