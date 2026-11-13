# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs Relay, Alice with her child Carol, and Bob as processes.

Alice and Bob join Relay at a `unix:` address, as in
`test_sealed_postcard_python_processes.py`. Carol is a second node in Alice's
process, joined to Alice over loopback UDP. She sends Bob her own postcard,
which reaches him through Alice and Relay.
"""

import subprocess
import tempfile
from pathlib import Path

from sealed_postcard_python_roles import binary, cleanup, identity_env, parse_pids, Role


def test_sealed_postcard_with_carol() -> None:
    """Bob gets Alice's 33 streams and Carol's one postcard, each from its sender.

    Carol's postcard arrives after Alice's streams, so Bob's report on Alice
    also shows he didn't count Carol's postcard among hers.
    """
    directory = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-py-carol-"))
    roles: list[Role] = []
    try:
        ids = directory / "ids"
        identities = subprocess.run(
            [binary(), "identities", "--out", str(ids), "--carol"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        pids = parse_pids(identities.stdout, {"alice", "relay", "bob", "carol"})
        relay_address = f"unix:{directory / 'r'}"

        relay = Role(
            [binary(), "relay", "--listen", relay_address],
            env=identity_env(ids / "relay.pem"),
            log=directory / "relay.log",
        )
        roles.append(relay)
        bob = Role(
            [
                binary(),
                "bob",
                "--parent-pid",
                pids["relay"],
                "--parent",
                relay_address,
                "--alice-pid",
                pids["alice"],
                "--carol-pid",
                pids["carol"],
            ],
            env=identity_env(ids / "bob.pem"),
            log=directory / "bob.log",
        )
        roles.append(bob)
        alice = Role(
            [
                binary(),
                "alice",
                "--parent-pid",
                pids["relay"],
                "--parent",
                relay_address,
                "--bob-pid",
                pids["bob"],
                "--carol",
            ],
            env={
                **identity_env(ids / "alice.pem"),
                "SEALED_POSTCARD_CAROL_IDENTITY": (ids / "carol.pem").read_text(),
            },
            log=directory / "alice.log",
        )
        roles.append(alice)

        assert alice.wait(timeout=120) == 0, alice.output()
        assert bob.stop() == 0, bob.output()
        assert "bob served 33 streams, all from alice: true" in bob.output()
        assert "bob got postcards from carol: 1" in bob.output()
        assert relay.stop() == 0, relay.output()
    finally:
        cleanup(roles, directory)
