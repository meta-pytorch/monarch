# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs the hello-world program that `python/README.md` walks through."""

import os
import re
import subprocess


def test_hello_world() -> None:
    """Alice's postcard reaches Bob through Relay, and Bob's reply returns.

    The program first prints each node's name, PID and the loopback UDP address
    it bound, then one line as each side receives its message.
    """
    result = subprocess.run(
        [os.environ["HELLO_WORLD"]],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) == 5, result.stdout
    for name, line in zip(("alice", "relay", "bob"), lines):
        pattern = rf"{name} +[0-9a-f]{{32}} udp://127\.0\.0\.1:\d+"
        assert re.fullmatch(pattern, line), line
    assert lines[3:] == [
        "bob received b'wish you were here' from alice",
        'alice received b"we\'re just two lost souls swimming in a fish bowl, year after year" from bob',
    ]
