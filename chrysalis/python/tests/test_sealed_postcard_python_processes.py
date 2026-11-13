# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs the Python Alice, Relay, and Bob as three processes over Unix sockets.

Each role is a mode of `sealed_postcard.py` on a `VsockCarrier`. Its
`unix:<path>` addresses use the carrier's Unix-stream mode, as the Rust roles
do in `test_sealed_postcard_rust_processes.py`.
"""

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from sealed_postcard_python_roles import binary, cleanup, identity_env, parse_pids, Role


def test_sealed_postcard_as_three_processes() -> None:
    directory = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-py-"))
    roles: list[Role] = []
    try:
        identities = subprocess.run(
            [binary(), "identities", "--out", str(directory / "ids")],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        pids = parse_pids(identities.stdout, {"alice", "relay", "bob"})
        relay_path = directory / "r"

        relay = Role(
            [binary(), "relay", "--listen", f"unix:{relay_path}"],
            env=identity_env(directory / "ids" / "relay.pem"),
            log=directory / "relay.log",
        )
        roles.append(relay)
        alice = Role(
            [
                binary(),
                "alice",
                "--parent-pid",
                pids["relay"],
                "--parent",
                f"unix:{relay_path}",
                "--bob-pid",
                pids["bob"],
            ],
            env=identity_env(directory / "ids" / "alice.pem"),
            log=directory / "alice.log",
        )
        roles.append(alice)
        bob = Role(
            [
                binary(),
                "bob",
                "--parent-pid",
                pids["relay"],
                "--parent",
                f"unix:{relay_path}",
                "--alice-pid",
                pids["alice"],
            ],
            env=identity_env(directory / "ids" / "bob.pem"),
            log=directory / "bob.log",
        )
        roles.append(bob)

        assert alice.wait(timeout=120) == 0, alice.output()
        assert bob.stop() == 0, bob.output()
        assert "bob served 33 streams, all from alice: true" in bob.output()
        assert relay.stop() == 0, relay.output()
    finally:
        cleanup(roles, directory)


def test_cleanup_reaps_a_process_that_ignores_sigterm() -> None:
    directory = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-py-cleanup-"))
    role = Role(
        [
            sys.executable,
            "-c",
            "import signal, subprocess, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "subprocess.Popen(['/bin/sleep', '600']); "
            "print('ready', flush=True); time.sleep(600)",
        ],
        env=os.environ.copy(),
        log=directory / "stubborn.log",
    )
    deadline = time.monotonic() + 5
    while "ready" not in role.output():
        if time.monotonic() >= deadline:
            cleanup([role], directory)
            raise TimeoutError("stubborn process did not become ready")
        time.sleep(0.01)

    started = time.monotonic()
    cleanup([role], directory)
    assert time.monotonic() - started < 20
    try:
        os.killpg(role.pgid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("process group survived cleanup")
    assert not directory.exists()
