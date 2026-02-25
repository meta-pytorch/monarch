# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging

import click
from monarch.actor import Actor, endpoint, this_host
from monarch.config import configure


@click.group()
def main() -> None:
    pass


class Printer(Actor):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger()

    @endpoint
    async def print(self, content: str) -> None:
        print(f"{content}", flush=True)


async def _flush_logs() -> None:
    configure(
        enable_log_forwarding=True,
    )
    pm = this_host().spawn_procs(per_host={"gpus": 4})
    # Never flush
    await pm.logging_option(stream_to_client=True, aggregate_window_sec=1000)
    am = pm.spawn("printer", Printer)
    # These should be streamed to client
    await asyncio.gather(*[am.print.call("has print streaming") for _ in range(5)])
    await pm.stop()


@main.command("flush-logs")
def flush_logs() -> None:
    asyncio.run(_flush_logs())


if __name__ == "__main__":
    main()
