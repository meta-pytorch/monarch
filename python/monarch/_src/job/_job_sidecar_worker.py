# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Entry point for the background job sidecar process.

Launched by :func:`~monarch._src.job.job_sidecar.create_job_sidecar`.
Receives the socket path and lock fd as arguments. Binds the socket
immediately to signal readiness, then serves job-scoped refresh/shutdown
requests.

Usage::

    python -m monarch._src.job._job_sidecar_worker <socket_path> <lock_fd>
"""

import sys


def main() -> None:
    socket_path = sys.argv[1]
    int(sys.argv[2])  # keep fd open to hold the flock for the process lifetime

    from monarch._src.job.job_sidecar import _run_job_sidecar

    _run_job_sidecar(socket_path)


if __name__ == "__main__":
    main()
