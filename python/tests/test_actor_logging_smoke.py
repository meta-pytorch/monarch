# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Actor-based logging smoke test.


Defines a `Logger` actor that routes INFO/WARNING to stdout and ERROR+
to stderr using two `logging.StreamHandler`s. The test captures
**process-level** stdout/stderr by temporarily redirecting file
descriptors (FD 1/2), so both Python and any Rust / native output
would be captured. It then spins up a small mesh, invokes the actor's
endpoints, and asserts the messages landed on the expected streams
(and include the expected actor prefix).

"""

import asyncio
import logging
import os
import re
import sys
import tempfile

import pytest
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint


class Logger(Actor):
    """Actor that emits log lines at different severities and routes them
    to separate files.

    Setup:

      - Adds a file handler for INFO/WARNING and another for ERROR+.

      - Flushes handlers after each endpoint call to minimize
        buffering effects.

    Notes:
      - We attach handlers to the *root* logger returned by
        `logging.getLogger()`.

      - The INFO/WARNING routing is enforced via a simple level
        filter: records with `levelno < logging.ERROR` go to one file;
        others go to another file.

    """

    def __init__(self, stdout_path: str, stderr_path: str) -> None:
        self._logger: logging.Logger = logging.getLogger()

        stdout_handler = logging.FileHandler(stdout_path, mode="a")
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.addFilter(lambda record: record.levelno < logging.ERROR)

        stderr_handler = logging.FileHandler(stderr_path, mode="a")
        stderr_handler.setLevel(logging.ERROR)

        self._logger.addHandler(stdout_handler)
        self._logger.addHandler(stderr_handler)

        self._stdout_handler = stdout_handler
        self._stderr_handler = stderr_handler

    @endpoint
    async def log_warn(self, content: str) -> None:
        """Emit a WARNING-level message and flush all handlers.

        Args:
            content: The message body to log.

        """
        self._logger.warning(f"{content}")
        self._stdout_handler.flush()
        self._stderr_handler.flush()

    @endpoint
    async def log_info(self, content: str) -> None:
        """
        Emit an INFO-level message and flush all handlers.

        Args:
            content: The message body to log.
        """
        self._logger.info(f"{content}")
        self._stdout_handler.flush()
        self._stderr_handler.flush()

    @endpoint
    async def log_error(self, content: str) -> None:
        """
        Emit an ERROR-level message and flush all handlers.

        Args:
            content: The message body to log.
        """
        self._logger.error(f"{content}")
        self._stdout_handler.flush()
        self._stderr_handler.flush()


@pytest.mark.timeout(60)
async def test_actor_logging_smoke() -> None:
    """End-to-end smoke test of file-based logging for the Logger actor.

    Flow:

      1. Create temporary files for stdout/stderr output.

      2. Start a small per-host mesh, enable logging, and spawn the
         `Logger` actor with paths to the temp files.

      3. Invoke `log_warn`, `log_info`, and `log_error`.

      4. Read back the files and assert:
         - WARNING/INFO appear in the stdout file,
         - ERROR appears in the stderr file,
         - an actor prefix like `[actor=...Logger...]` is present.

    This test validates logging without relying on FD-level redirection,
    which may not work reliably in all CI environments.

    """
    # Create temporary files to capture output.
    with tempfile.NamedTemporaryFile(
        mode="w+", delete=False, suffix="_stdout.log"
    ) as stdout_file, tempfile.NamedTemporaryFile(
        mode="w+", delete=False, suffix="_stderr.log"
    ) as stderr_file:
        stdout_path = stdout_file.name
        stderr_path = stderr_file.name

    try:
        # Make a logger mesh.
        pm = this_host().spawn_procs(per_host={"gpus": 2})
        await pm.logging_option(level=logging.INFO)
        am = pm.spawn("logger", Logger, stdout_path, stderr_path)

        # Do some logging actions.
        await am.log_warn.call("hello 1")
        await am.log_info.call("hello 2")
        await am.log_error.call("hello 3")

        # Wait a bit for output to be written.
        await asyncio.sleep(1)

        await pm.stop()

        # Read the captured output.
        with open(stdout_path, "r") as f:
            stdout_content = f.read()
        with open(stderr_path, "r") as f:
            stderr_content = f.read()

        # Print the captured output.
        print("")
        print("=== Captured stdout ===")
        print(stdout_content)
        print("=== Captured stderr ===")
        print(stderr_content)

        # Assertions on the captured output.
        assert re.search(
            r"hello 1", stdout_content
        ), f"Expected 'hello 1' in stdout: {stdout_content}"
        assert re.search(
            r"hello 2", stdout_content
        ), f"Expected 'hello 2' in stdout: {stdout_content}"
        assert re.search(
            r"hello 3", stderr_content
        ), f"Expected 'hello 3' in stderr: {stderr_content}"
        assert re.search(
            r"\[actor=.*Logger.*\]", stdout_content
        ), f"Expected actor prefix in stdout: {stdout_content}"

    finally:
        # Clean up temp files.
        try:
            os.unlink(stdout_path)
        except OSError:
            pass
        try:
            os.unlink(stderr_path)
        except OSError:
            pass
