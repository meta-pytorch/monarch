# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Child-side entry point for workers spawned by
:func:`monarch._src.spmd.host_mesh._spawn_worker_process`.

Reads the listen address from ``_MONARCH_WORKER_ADDR`` and spawns a
daemon thread that blocks reading the parent pipe, exiting the worker on
EOF — which the kernel delivers when the parent process closes its end,
i.e. when the parent exits. Then delegates to ``run_worker_loop_forever``.

Invoked either as ``python -m monarch._src.spmd._worker_entry`` under
standard Python, or via ``PAR_MAIN_OVERRIDE`` under PAR/XAR.
"""

import os
import sys
import threading

from monarch._src.spmd.host_mesh import _ADDR_ENV, _PARENT_WATCH_FD_ENV
from monarch.actor import run_worker_loop_forever


def _wait_for_parent_death(fd: int) -> None:
    """Block reading ``fd`` until the parent closes its end, then exit.

    The parent never writes to the other end of the pipe, so ``os.read``
    blocks indefinitely without consuming CPU. When the parent process
    exits the kernel closes its write end, ``os.read`` returns ``b""``,
    and we exit the worker via ``os._exit`` — even if
    ``run_worker_loop_forever`` is blocked in native code.
    """
    while True:
        try:
            data = os.read(fd, 4096)
        except OSError as e:
            print(
                f"monarch worker: reading from parent pipe failed ({e}); exiting",
                file=sys.stderr,
                flush=True,
            )
            os._exit(1)
            return
        if not data:
            # Parent closed the pipe — parent process exited. Our job is done.
            os._exit(0)


def main() -> None:
    """Entry point for the worker subprocess.

    Reads the listen address from ``_MONARCH_WORKER_ADDR`` and spawns
    :func:`_wait_for_parent_death` as a daemon thread before handing off
    to ``run_worker_loop_forever``.
    """
    addr = os.environ[_ADDR_ENV]
    fd_str = os.environ.get(_PARENT_WATCH_FD_ENV)
    if fd_str is not None:
        threading.Thread(
            target=_wait_for_parent_death,
            args=(int(fd_str),),
            daemon=True,
            name="monarch-worker-parent-watch",
        ).start()
    run_worker_loop_forever(address=addr, ca="trust_all_connections")


if __name__ == "__main__":
    main()
