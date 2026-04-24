# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Entry point invoked by ``test_parent_death_kills_worker_via_pipe_eof``
via ``PAR_MAIN_OVERRIDE``.

Spawns a worker, prints its PID, and exits. Exiting closes our end of
the parent-watch pipe; the worker's parent-watch thread should see EOF
and exit on its own, which is what the test verifies. This file is not
a test module — pytest ignores it because its name does not start with
``test_``.
"""

from monarch._src.spmd.host_mesh import _spawn_worker_process


def main() -> None:
    _addr, pid = _spawn_worker_process(transport="ipc")
    print(pid, flush=True)


if __name__ == "__main__":
    main()
