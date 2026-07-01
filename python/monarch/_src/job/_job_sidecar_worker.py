# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Entry point for the background job sidecar process.

Launched by :func:`~monarch._src.job.job_sidecar.create_job_sidecar`.
Receives optional startup arguments followed by the socket path and lock fd.
Binds the socket immediately to signal readiness, then serves job-scoped
refresh/shutdown requests.

Usage::

    python -m monarch._src.job._job_sidecar_worker [--runtime-transport TRANSPORT] <socket_path> <lock_fd>
"""

import sys


def main() -> None:
    args = sys.argv[1:]
    if len(args) < 2:
        raise RuntimeError("job sidecar worker requires socket path and lock fd")

    runtime_transport = None
    startup_args = args[:-2]
    if startup_args:
        if len(startup_args) != 2 or startup_args[0] != "--runtime-transport":
            raise RuntimeError(f"unexpected job sidecar worker args: {startup_args!r}")
        runtime_transport = startup_args[1]

    socket_path = args[-2]
    int(args[-1])  # keep fd open to hold the flock for the process lifetime

    from monarch._src.job.job_sidecar import _run_job_sidecar

    _run_job_sidecar(socket_path, runtime_transport=runtime_transport)


if __name__ == "__main__":
    main()
