# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Entry point for ProcessJob worker subprocesses.

When running inside a PAR/XAR binary, ``sys.executable`` points to the
bare Python interpreter which cannot import modules bundled in the
archive.  ``ProcessJob`` sets ``PAR_MAIN_OVERRIDE`` to this module so
that the PAR binary re-executes itself with the correct import
environment.  The worker address and CA are passed via environment
variables.
"""

import os

from monarch.actor import run_worker_loop_forever

if __name__ == "__main__":
    run_worker_loop_forever(
        address=os.environ["_MONARCH_WORKER_ADDR"],
        ca="trust_all_connections",
    )
