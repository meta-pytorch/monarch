# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import asyncio
import sys

import pytest

if sys.platform != "linux":
    pytest.skip("linux-only", allow_module_level=True)

from isolate_in_subprocess import isolate_in_subprocess
from monarch._src.actor.proc_mesh import get_or_spawn_controller
from monarch.actor import Actor, endpoint


class _DedupProbe(Actor):
    _constructions: int = 0

    def __init__(self) -> None:
        type(self)._constructions += 1

    @endpoint
    async def instance_id(self) -> int:
        return id(self)

    @endpoint
    async def construction_count(self) -> int:
        return type(self)._constructions


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_concurrent_controller_init_dedups() -> None:
    """Concurrent get_or_spawn_controller calls resolve to one controller."""
    controllers = await asyncio.gather(
        *(
            get_or_spawn_controller("controller_dedup_probe", _DedupProbe)
            for _ in range(8)
        )
    )
    # Each caller gets a distinct client-side wrapper, so compare a stable value, not `is`.
    instance_ids = await asyncio.gather(
        *(c.instance_id.call_one() for c in controllers)
    )
    assert len(set(instance_ids)) == 1, instance_ids

    # id(self) can't see a check-then-spawn that overwrote the cache slot; the count can.
    assert await controllers[0].construction_count.call_one() == 1
