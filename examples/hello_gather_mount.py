# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Simple CLI for manually testing gather_mount with two fake local hosts.

Usage:
    uv run python examples/hello_gather_mount.py <base> <mount>

Expects <base>/hosts_0 and <base>/hosts_1 to exist as the per-host source
directories. Mounts them at <mount>/hosts_0 and <mount>/hosts_1 respectively
via gather_mount with $SUBDIR substitution, then waits for Ctrl-C.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mount two fake remote directories via gather_mount."
    )
    parser.add_argument(
        "base",
        help="Base directory containing hosts_0/ and hosts_1/ subdirectories",
    )
    parser.add_argument("mount", help="Local mount point")
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

    from monarch._src.job.process import ProcessJob
    from monarch.gather_mount import gather_mount

    host_mesh = ProcessJob({"hosts": 2}).state(cached_path=None).hosts

    remote_path = os.path.join(args.base, "$SUBDIR")
    print(f"Mounting at {args.mount}")
    print(f"  hosts_0 → {os.path.join(args.base, 'hosts_0')}")
    print(f"  hosts_1 → {os.path.join(args.base, 'hosts_1')}")

    with gather_mount(host_mesh, remote_path, args.mount) as mount:
        # Ensure SIGTERM (e.g. kill) also triggers clean unmount.
        signal.signal(signal.SIGTERM, lambda *_: mount.close())
        print("Mounted. Press Enter or Ctrl-C to unmount and exit.")
        try:
            sys.stdin.readline()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
