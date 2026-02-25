# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import asyncio
import io
import logging
import os
import re
import sys
import tempfile

import pytest
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint


class Logger(Actor):
    def __init__(
        self, stdout_path: str | None = None, stderr_path: str | None = None
    ) -> None:
        self._logger: logging.Logger = logging.getLogger()

        # If file paths are provided, remove existing handlers to log
        # only to files.
        if stdout_path or stderr_path:
            self._logger.handlers.clear()

        stdout_handler = (
            logging.FileHandler(stdout_path, mode="a")
            if stdout_path
            else logging.StreamHandler(sys.stdout)
        )
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.addFilter(lambda record: record.levelno < logging.ERROR)

        stderr_handler = (
            logging.FileHandler(stderr_path, mode="a")
            if stderr_path
            else logging.StreamHandler(sys.stderr)
        )
        stderr_handler.setLevel(logging.ERROR)

        self._logger.addHandler(stdout_handler)
        self._logger.addHandler(stderr_handler)

        self._stdout_handler = stdout_handler
        self._stderr_handler = stderr_handler

    @endpoint
    async def log_warn(self, content: str) -> None:
        self._logger.warning(f"{content}")
        self._stdout_handler.flush()
        self._stderr_handler.flush()

    @endpoint
    async def log_info(self, content: str) -> None:
        self._logger.info(f"{content}")
        self._stdout_handler.flush()
        self._stderr_handler.flush()

    @endpoint
    async def log_error(self, content: str) -> None:
        self._logger.error(f"{content}")
        self._stdout_handler.flush()
        self._stderr_handler.flush()

    @endpoint
    async def log_structured(self) -> dict:
        """
        Log with empty message like torch.compile's trace_structured does.

        torch uses: logger.debug("") with structured data in record attributes.
        The actor filter must not modify empty messages.
        """
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        handler.setFormatter(logging.Formatter("%(message)s"))

        logger = logging.getLogger("test.structured")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)

        try:
            # Log with empty message like torch does
            logger.debug("")
        finally:
            logger.removeHandler(handler)

        captured = output.getvalue().strip()
        return {"captured": captured, "is_empty": captured == ""}


@pytest.mark.timeout(60)
async def test_actor_logging_smoke() -> None:
    # Create temporary files to capture output.
    with (
        tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix="_stdout.log"
        ) as stdout_file,
        tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix="_stderr.log"
        ) as stderr_file,
    ):
        stdout_path = stdout_file.name
        stderr_path = stderr_file.name

    try:
        pm = this_host().spawn_procs(per_host={"gpus": 2})
        await pm.logging_option(level=logging.INFO)

        # Log to the terminal.
        am_1 = pm.spawn("logger_1", Logger)
        await am_1.log_warn.call("hello 1")
        await am_1.log_info.call("hello 2")
        await am_1.log_error.call("hello 3")

        # Log to files.
        am_2 = pm.spawn("logger_2", Logger, stdout_path, stderr_path)
        await am_2.log_warn.call("hello 1")
        await am_2.log_info.call("hello 2")
        await am_2.log_error.call("hello 3")

        # Wait for output to be written.
        await asyncio.sleep(1)

        # Read the captured output.
        with open(stdout_path, "r") as f:
            stdout_content = f.read()
        with open(stderr_path, "r") as f:
            stderr_content = f.read()

        # Assertions on the captured output.
        assert re.search(r"hello 1", stdout_content), (
            f"Expected 'hello 1' in stdout: {stdout_content}"
        )
        assert re.search(r"hello 2", stdout_content), (
            f"Expected 'hello 2' in stdout: {stdout_content}"
        )
        assert re.search(r"hello 3", stderr_content), (
            f"Expected 'hello 3' in stderr: {stderr_content}"
        )
        assert re.search(r"\[actor=.*Logger.*\]", stdout_content), (
            f"Expected actor prefix in stdout: {stdout_content}"
        )

        await pm.stop()

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


@pytest.mark.timeout(60)
async def test_structured_logging():
    """
    Tests that empty-message logging works correctly in actors.

    torch.compile uses empty messages for structured trace logging.
    The actor filter must not modify empty messages, or torch's
    formatter fails with "expected empty string for trace".
    """
    pm = this_host().spawn_procs()
    actor = pm.spawn("logger", Logger)
    result = await actor.log_structured.call_one()

    assert result["is_empty"], (
        f"Actor prefix corrupted empty message: got {result['captured']!r}"
    )
