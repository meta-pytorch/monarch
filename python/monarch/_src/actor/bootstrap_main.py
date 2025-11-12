# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
This is the main function for the boostrapping a new process using a ProcessAllocator.
"""

import asyncio
import importlib.resources
import logging
import multiprocessing
import os
import sys


# Import torch to avoid import-time races if a spawned actor tries to import torch.
try:
    import monarch._rust_bindings  # @manual  # noqa: F401
except ImportError:
    try:
        import torch  # @manual  # noqa: F401
    except ImportError:
        pass
    import monarch._rust_bindings  # @manual  # noqa: F401


async def main() -> None:
    from monarch._rust_bindings.monarch_hyperactor.bootstrap import bootstrap_main

    # pyre-ignore[12]: bootstrap_main is async but imported from Rust bindings
    await bootstrap_main()


def invoke_main() -> None:
    try:
        # if this is invoked with the stdout piped somewhere, then print
        # changes its buffering behavior. So we default to the standard
        # behavior of std out as if it were a terminal.
        sys.stdout.reconfigure(line_buffering=True)
        global bootstrap_main

        # TODO: figure out what from worker_main.py we should reproduce here.

        from monarch._src.actor.telemetry import TracingForwarder  # noqa

        if os.environ.get("MONARCH_ERROR_DURING_BOOTSTRAP_FOR_TESTING") == "1":
            raise RuntimeError("Error during bootstrap for testing")

        # forward logs to rust tracing. Defaults to on.
        if os.environ.get("MONARCH_PYTHON_LOG_TRACING", "1") == "1":
            # we can stream python logs now; no need to forward them to rust processes
            pass
            # install opentelemetry tracing

        try:
            with (
                importlib.resources.as_file(
                    importlib.resources.files("monarch") / "py-spy"
                ) as pyspy,
            ):
                if pyspy.exists():
                    os.environ["PYSPY_BIN"] = str(pyspy)
                # fallback to using local py-spy
        except Exception as e:
            logging.warning(f"Failed to set up py-spy: {e}")

        from monarch._src.actor.debugger.breakpoint import remote_breakpointhook

        sys.breakpointhook = remote_breakpointhook
    except Exception as e:
        bootstrap_err = RuntimeError(
            f"Failed to bootstrap proc due to: {e}\nMake sure your proc bootstrap command is correct. "
            f"Provided command:\n{' '.join([sys.executable, *sys.argv])}\nTo specify your proc bootstrap command, use the "
            f"`bootstrap_cmd` kwarg in `monarch.actor.HostMesh.allocate_nonblocking(...)`."
        )
        raise bootstrap_err from e

    # Start an event loop for PythonActors to use.
    asyncio.run(main())


if __name__ == "__main__":
    # Ensure that processes started via `multiprocessing` are spawned, not forked.
    # forking is a terrible default, see: https://github.com/python/cpython/issues/84559
    multiprocessing.set_start_method("spawn", force=True)
    invoke_main()  # pragma: no cover
