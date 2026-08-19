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

    python -m monarch._src.job._job_sidecar_worker \
        [--runtime-transport TRANSPORT] [--attach-to ADDRESS] \
        <socket_path> <lock_fd>
"""

import argparse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-transport")
    parser.add_argument("--attach-to")
    parser.add_argument("socket_path")
    parser.add_argument("lock_fd", type=int)
    args = parser.parse_args()

    from monarch._src.job.job_sidecar import _run_job_sidecar

    _run_job_sidecar(
        args.socket_path,
        runtime_transport=args.runtime_transport,
        attach_to=args.attach_to,
    )


if __name__ == "__main__":
    main()
