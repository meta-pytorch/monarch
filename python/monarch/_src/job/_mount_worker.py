# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Entry point for the background mount process.

Launched by :func:`~monarch._src.job.mount_config.Mounts.ensure_open`.
Receives the socket path and lock fd as arguments. Binds the socket
immediately to signal readiness, then serves refresh/shutdown requests.
The first ``refresh`` initialises the mounts; subsequent ones refresh them.

Usage::

    python -m monarch._src.job._mount_worker <socket_path> <lock_fd>
"""

import sys


def main() -> None:
    socket_path = sys.argv[1]
    int(sys.argv[2])  # keep fd open to hold the flock for the process lifetime

    from monarch._src.job.mount_config import _run_mount_process

    _run_mount_process(socket_path)


if __name__ == "__main__":
    main()
