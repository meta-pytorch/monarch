# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Simple CLI for manually testing gather_mount with two fake local hosts.

Usage:
    uv run python examples/hello_gather_mount.py <mount> <dir0> <dir1>

Creates two ProcessJob hosts serving <dir0> and <dir1> respectively, mounts
them at <mount>/hosts_0 and <mount>/hosts_1, then waits for Ctrl-C.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mount two fake remote directories via gather_mount."
    )
    parser.add_argument("mount", help="Local mount point")
    parser.add_argument("dir0", help="Directory to serve as hosts_0")
    parser.add_argument("dir1", help="Directory to serve as hosts_1")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Suppress noisy third-party loggers
    for noisy in ("monarch", "asyncio", "fuse"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    logging.getLogger("monarch._src.gather_mount.gather_mount").setLevel(logging.DEBUG)

    dirs = [args.dir0, args.dir1]

    from monarch._src.job.process import ProcessJob
    from monarch.gather_mount import gather_mount

    host_mesh = ProcessJob({"hosts": 2}).state(cached_path=None).hosts

    print(f"Mounting at {args.mount}")
    print(f"  hosts_0 → {args.dir0}")
    print(f"  hosts_1 → {args.dir1}")

    with gather_mount(host_mesh, args.mount, lambda rank: dirs[rank["hosts"]]) as mount:
        # Ensure SIGTERM (e.g. kill) also triggers clean unmount.
        signal.signal(signal.SIGTERM, lambda *_: mount.close())
        print("Mounted. Press Enter or Ctrl-C to unmount and exit.")
        try:
            sys.stdin.readline()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
