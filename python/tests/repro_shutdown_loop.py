# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

"""Hot loop repro for test_shutdown_host_mesh.

Usage (single iteration):
    buck run @fbcode//mode/opt fbcode//monarch/python/tests:repro_shutdown_loop

Wrap with run_shutdown_loop.sh to loop until failure.
"""

from __future__ import annotations

from monarch._src.actor.actor_mesh import Actor, context
from monarch._src.actor.endpoint import endpoint
from monarch._src.job.process import ProcessJob
from scoped_state import scoped_state


class RankActor(Actor):
    @endpoint
    async def get_rank(self) -> int:
        return context().actor_instance.rank.rank


def main() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        hm = state.hosts
        pm = hm.spawn_procs(per_host={"gpus": 2})
        am = pm.spawn("actor", RankActor)
        am.get_rank.choose().get()
        hm.shutdown().get()


if __name__ == "__main__":
    main()
