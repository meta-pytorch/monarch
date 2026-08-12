# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import shlex
import sys
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import this_host
from monarch.job import shell


def test_shell_requires_singleton_host_mesh() -> None:
    host_mesh = MagicMock()
    host_mesh.size.return_value = 2

    with pytest.raises(ValueError, match="singleton HostMesh"):
        shell(host_mesh)

    host_mesh.spawn_procs.assert_not_called()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_shell_transfers_terminal_data_and_returns_exit_status() -> None:
    input_read, input_write = os.pipe()
    output_read, output_write = os.pipe()
    os.write(
        input_write,
        b'printf "shell:%s\\n" "$MONARCH_SHELL_TEST"; exit 7\n',
    )

    try:
        with (
            os.fdopen(input_read, "r") as stdin,
            os.fdopen(output_write, "w") as stdout,
            patch.object(sys, "stdin", stdin),
            patch.object(sys, "stdout", stdout),
        ):
            returncode = shell(this_host(), env={"MONARCH_SHELL_TEST": "connected"})
        output = os.read(output_read, 64 * 1024).decode(errors="replace")
    finally:
        os.close(input_write)
        os.close(output_read)

    assert returncode == 7
    assert "shell:connected" in output


@pytest.mark.timeout(20)
@isolate_in_subprocess
def test_shell_eof_stops_foreground_process() -> None:
    input_read, input_write = os.pipe()
    output_read, output_write = os.pipe()
    input_writer = os.fdopen(input_write, "wb", buffering=0)
    stop_closer = threading.Event()

    with tempfile.TemporaryDirectory() as directory:
        child_pid_path = os.path.join(directory, "child-pid")
        child_command = f"echo $$ > {shlex.quote(child_pid_path)}; exec sleep 30"
        input_writer.write(f"sh -c {shlex.quote(child_command)}\n".encode())

        def close_input_when_command_starts() -> None:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not os.path.exists(child_pid_path):
                if stop_closer.wait(0.01):
                    return
            input_writer.close()

        closer = threading.Thread(target=close_input_when_command_starts, daemon=True)
        closer.start()
        try:
            with (
                os.fdopen(input_read, "r") as stdin,
                os.fdopen(output_write, "w") as stdout,
                patch.object(sys, "stdin", stdin),
                patch.object(sys, "stdout", stdout),
            ):
                started = time.monotonic()
                returncode = shell(this_host(), env={"SHELL": "/bin/bash"})
                elapsed = time.monotonic() - started
            os.read(output_read, 64 * 1024)
        finally:
            stop_closer.set()
            input_writer.close()
            closer.join(timeout=1)
            os.close(output_read)

        assert os.path.exists(child_pid_path)
        assert not closer.is_alive()
        assert elapsed < 10
        assert returncode != 0


@pytest.mark.timeout(20)
@isolate_in_subprocess
def test_shell_propagates_terminal_copy_failure() -> None:
    input_read, input_write = os.pipe()
    output_read, output_write = os.pipe()
    os.close(output_read)
    os.write(input_write, b'printf "output that cannot be written\\n"; exit\n')
    os.close(input_write)

    with (
        os.fdopen(input_read, "r") as stdin,
        os.fdopen(output_write, "w") as stdout,
        patch.object(sys, "stdin", stdin),
        patch.object(sys, "stdout", stdout),
        pytest.raises((BrokenPipeError, ConnectionResetError)),
    ):
        shell(this_host(), env={"SHELL": "/bin/bash"})
