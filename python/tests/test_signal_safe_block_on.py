#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Integration test for signal_safe_block_on.

This test spawns a Python binary that calls a Rust function which sleeps indefinitely.
The test then sends SIGINT to the process and confirms that it exits properly,
verifying that signal_safe_block_on correctly handles signals.
"""

import importlib.resources
import os
import signal
import subprocess
import time
import unittest

import pytest


# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
class TestSignalSafeBlockOn(unittest.TestCase):
    # pyre-ignore[56]
    @pytest.mark.oss_skip
    def test_sigint_handling(self) -> None:
        """
        Test that a process using signal_safe_block_on can be interrupted with SIGINT.

        This test:
        1. Spawns a subprocess running sleep_binary.py
        2. Waits for it to start
        3. Sends SIGINT to the process
        4. Verifies that the process exits within a reasonable timeout

        To validate that it will behave in the same way as a ctl-c in the shell,
        we launch the process in it's own process group and send the signal to the process
        group instead of the process itself.
        """
        test_bin = importlib.resources.files("monarch.python.tests").joinpath(
            "test_bin"
        )
        # Start the subprocess
        process = subprocess.Popen(
            [str(test_bin)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )

        gpig = os.getpgid(process.pid)

        try:
            # Wait for the process to start and print its startup message
            start_time = time.time()
            startup_timeout = 10  # seconds

            while time.time() - start_time < startup_timeout:
                if process.stdout and "Starting sleep_binary" in (
                    process.stdout.readline() or ""
                ):
                    break
                time.sleep(0.1)
            else:
                self.fail("Subprocess did not start properly within timeout")

            # Give the process a moment to enter the sleep_indefinitely_for_unit_tests function
            time.sleep(1)

            # Send SIGINT to the process
            os.killpg(gpig, signal.SIGINT)

            # Wait for the process to exit with a timeout
            exit_timeout = 5  # seconds
            exit_time = time.time()

            while time.time() - exit_time < exit_timeout:
                if process.poll() is not None:
                    # Process has exited
                    break
                time.sleep(0.1)
            else:
                self.fail("Process did not exit after receiving SIGINT")

            # Check that the process exited with code 0 (clean exit)
            self.assertEqual(process.returncode, 0, "Process did not exit cleanly")

        finally:
            # Clean up in case the test fails
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    unittest.main()
