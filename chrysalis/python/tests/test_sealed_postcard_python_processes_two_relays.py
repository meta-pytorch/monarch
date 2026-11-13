# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs Python Alice and Bob through two Relays joined over loopback UDP.

    Relay 1 (root) ───── UDP ───── Relay 2
          │                              │
        unix:                          unix:
          │                              │
        Alice                           Bob

Both Relays bind `(UdpCarrier, VsockCarrier)`. Relay 2 joins Relay 1 through
UDP; each Relay reaches its endpoint through the vsock carrier's `unix:` mode.

Running all four roles on one host models the later topology with one Relay
per host and a guest beneath each Relay.
"""

import re
import subprocess
import tempfile
import time
from pathlib import Path

# Process ownership and cleanup shared by the Python sealed-postcard tests.
import sealed_postcard_python_roles as process_roles

# Maximum seconds to wait for Relay 1 to report the UDP address it bound.
RELAY_ADDRESS_TIMEOUT = 30
# Ask the system to choose a loopback UDP port for each Relay.
EPHEMERAL_LOOPBACK_UDP = "127.0.0.1:0"


def _udp_address(relay: process_roles.Role) -> str:
    """Wait for Relay 1's system-selected UDP address.

    Relay 1 binds port 0 and prints the resulting `relay address udp://...`
    line. Return that address for Relay 2's parent configuration, or raise
    `TimeoutError` if it is not reported in time.
    """
    deadline = time.monotonic() + RELAY_ADDRESS_TIMEOUT
    while time.monotonic() < deadline:
        match = re.search(r"^relay address (udp://\S+)$", relay.output(), re.MULTILINE)
        if match:
            return match[1]
        time.sleep(0.05)
    raise TimeoutError(
        f"Relay printed no UDP address within {RELAY_ADDRESS_TIMEOUT} s:\n{relay.output()}"
    )


def test_sealed_postcard_through_two_relays() -> None:
    """Run the Python sealed-postcard program in the two-Relay topology above.

    The test starts Relay 1 as the root, reads the loopback UDP port the system
    assigned it, and starts Relay 2 as its child at that address. Alice joins
    Relay 1 over a Unix stream and Bob joins Relay 2 over another.

    Alice sends Bob the postcard and 32 numbered streams through both Relays.
    Require Alice to finish successfully, Bob to authenticate every stream as
    Alice's, and both Relays to stop successfully. Cleanup always stops every
    role and removes the run directory.
    """
    directory = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-py-relays-"))
    roles: list[process_roles.Role] = []
    try:
        ids = directory / "ids"
        identities = subprocess.run(
            [process_roles.binary(), "identities", "--out", str(ids), "--relays", "2"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        pids = process_roles.parse_pids(
            identities.stdout, {"alice", "relay", "relay2", "bob"}
        )
        r1, r2 = f"unix:{directory / 'r1'}", f"unix:{directory / 'r2'}"

        relay1 = process_roles.Role(
            [
                process_roles.binary(),
                "relay",
                "--udp",
                EPHEMERAL_LOOPBACK_UDP,
                "--listen",
                r1,
            ],
            env=process_roles.identity_env(ids / "relay.pem"),
            log=directory / "relay1.log",
        )
        roles.append(relay1)
        relay2 = process_roles.Role(
            [
                process_roles.binary(),
                "relay",
                "--udp",
                EPHEMERAL_LOOPBACK_UDP,
                "--listen",
                r2,
                "--parent-pid",
                pids["relay"],
                "--parent",
                _udp_address(relay1),
            ],
            env=process_roles.identity_env(ids / "relay2.pem"),
            log=directory / "relay2.log",
        )
        roles.append(relay2)
        alice = process_roles.Role(
            [
                process_roles.binary(),
                "alice",
                "--parent-pid",
                pids["relay"],
                "--parent",
                r1,
                "--bob-pid",
                pids["bob"],
            ],
            env=process_roles.identity_env(ids / "alice.pem"),
            log=directory / "alice.log",
        )
        roles.append(alice)
        bob = process_roles.Role(
            [
                process_roles.binary(),
                "bob",
                "--parent-pid",
                pids["relay2"],
                "--parent",
                r2,
                "--alice-pid",
                pids["alice"],
            ],
            env=process_roles.identity_env(ids / "bob.pem"),
            log=directory / "bob.log",
        )
        roles.append(bob)

        assert alice.wait(timeout=120) == 0, alice.output()
        assert bob.stop() == 0, bob.output()
        assert "bob served 33 streams, all from alice: true" in bob.output()
        assert relay2.stop() == 0, relay2.output()
        assert relay1.stop() == 0, relay1.output()
    finally:
        process_roles.cleanup(roles, directory)
