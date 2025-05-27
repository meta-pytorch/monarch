# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
This is the main function for the boostrapping a new process using a ProcessAllocator.
"""

import asyncio
import importlib.resources
import os
import sys


async def main():
    from monarch._rust_bindings.monarch_hyperactor.bootstrap import bootstrap_main

    await bootstrap_main()


def invoke_main():
    # if this is invoked with the stdout piped somewhere, then print
    # changes its buffering behavior. So we default to the standard
    # behavior of std out as if it were a terminal.
    sys.stdout.reconfigure(line_buffering=True)
    # TODO: figure out what from worker_main.py we should reproduce here.

    with (
        importlib.resources.path("monarch", "py-spy") as pyspy,
    ):
        if pyspy.exists():
            os.environ["PYSPY_BIN"] = str(pyspy)
        # fallback to using local py-spy

    # Start an event loop for PythonActors to use.
    asyncio.run(main())


if __name__ == "__main__":
    invoke_main()  # pragma: no cover
