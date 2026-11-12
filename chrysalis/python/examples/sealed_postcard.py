# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Sealed postcard: Alice, Relay, and Bob as separate Python processes.

These bindings are experimental, private, and subject to change. They are not
part of Chrysalis's public API.

Relay is the root of a namespace, and Alice and Bob join Relay. Alice finds Bob
by his PID and sends him a postcard through Relay, checks his acknowledgement,
then opens 32 numbered streams that Bob echoes. Bob checks that every stream
came from Alice.

Each role is one mode of this program, with the command line of the Rust
`sealed-postcard` example's role modes. Every mode except `identities` reads
its identity bundle from `SEALED_POSTCARD_IDENTITY`:

    sealed_postcard identities --out <dir>
    sealed_postcard relay --listen <addr>...
    sealed_postcard alice --parent-pid <pid> --parent <addr> --bob-pid <pid>
    sealed_postcard bob --parent-pid <pid> --parent <addr> --alice-pid <pid>
"""

import argparse
import asyncio
import os
import signal
from dataclasses import dataclass
from pathlib import Path

from chrysalis._rust_bindings import (
    ChrysalisError,
    generate_test_identities,
    Identity,
    Node,
    Pid,
    Stream,
    VsockCarrier,
)

POSTCARD = b"Tie a yellow ribbon round the old oak tree."
ACKNOWLEDGEMENT = b"The porch light is on."
# Numbered streams Alice opens after the postcard. Each carries its number as
# one byte.
STREAM_COUNT = 32
# Seconds Alice waits for Bob to become visible through Relay.
PEER_WAIT = 120.0
ROLES = ("alice", "relay", "bob")


@dataclass
class Served:
    """The streams Bob has answered, and whether Alice opened every one."""

    count: int = 0
    all_from_alice: bool = True


def write_identities(out: Path) -> None:
    """Write mutually trusted identity bundles for the roles and print their PIDs."""
    out.mkdir(parents=True, exist_ok=True)
    for name, bundle in zip(ROLES, generate_test_identities(len(ROLES))):
        # The file gets mode 0600 before any key bytes exist, and `O_EXCL`
        # refuses a file or symlink already at this path.
        fd = os.open(out / f"{name}.pem", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as file:
            file.write(bundle)
        print(f"{name} {Identity.from_pem(bundle).pid}")


def stop_requested() -> asyncio.Event:
    """Return an event that Ctrl-C or SIGTERM sets."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sent in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sent, stop.set)
    return stop


async def read_bounded(stream: Stream, limit: int) -> bytes:
    """Read until the other end finishes, rejecting more than `limit` bytes."""
    data = b""
    while chunk := await stream.read(limit + 1 - len(data)):
        data += chunk
        if len(data) > limit:
            raise ValueError("stream exceeded application limit")
    return data


async def dial_when_visible(node: Node, pid: Pid, within: float) -> Stream:
    """Open a stream to `pid` once `node` can see it.

    Bob's PID takes a moment to reach Relay, and `dial` fails until it does.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while True:
        try:
            return await node.dial(pid)
        except ChrysalisError:
            if loop.time() > deadline:
                raise
            await asyncio.sleep(0.05)


async def exchange_postcard(node: Node, bob: Pid) -> None:
    """Send Bob the postcard and check his acknowledgement."""
    stream = await dial_when_visible(node, bob, PEER_WAIT)
    await stream.write(POSTCARD)
    # Finishing Alice's side lets Bob read the postcard to its end, and leaves
    # his side open for the acknowledgement.
    await stream.finish()
    if await read_bounded(stream, len(ACKNOWLEDGEMENT)) != ACKNOWLEDGEMENT:
        raise ValueError("Bob returned the wrong acknowledgement")


async def exchange_numbered_streams(node: Node, bob: Pid) -> None:
    """Open the numbered streams, then check that Bob echoes each number."""
    streams: list[tuple[int, Stream]] = []
    for number in range(STREAM_COUNT):
        stream = await node.dial(bob)
        await stream.write(bytes([number]))
        await stream.finish()
        streams.append((number, stream))
    for number, stream in streams:
        echo = await read_bounded(stream, 1)
        if echo != bytes([number]):
            raise ValueError(f"Bob returned {echo!r} for stream {number}")


async def run_alice(identity: Identity, parent: tuple[Pid, str], bob: Pid) -> None:
    """Join Relay, then complete both exchanges with Bob."""
    node = Node(identity, carrier=VsockCarrier(), parent=parent)
    try:
        await exchange_postcard(node, bob)
        await exchange_numbered_streams(node, bob)
    finally:
        await node.close()


async def run_relay(identity: Identity, listen: list[str]) -> None:
    """Serve as the namespace root until Ctrl-C or SIGTERM."""
    stop = stop_requested()
    node = Node(identity, carrier=VsockCarrier(listen=listen))
    try:
        await stop.wait()
    finally:
        await node.close()


async def answer(stream: Stream, first: bool) -> None:
    """Acknowledge the postcard on the first stream, and echo every later one."""
    if first:
        if await read_bounded(stream, len(POSTCARD)) != POSTCARD:
            raise ValueError("Alice sent the wrong postcard")
        await stream.write(ACKNOWLEDGEMENT)
    else:
        number = await read_bounded(stream, 1)
        if len(number) != 1:
            raise ValueError("numbered stream did not contain one byte")
        await stream.write(number)
    await stream.finish()


async def serve(node: Node, alice: Pid, served: Served) -> None:
    """Answer streams one at a time, recording each in `served`."""
    while True:
        # `accept` returns the PID that the stream's mutual TLS authenticated.
        source, stream = await node.accept()
        await answer(stream, first=served.count == 0)
        served.count += 1
        served.all_from_alice = served.all_from_alice and source == alice


async def run_bob(identity: Identity, parent: tuple[Pid, str], alice: Pid) -> Served:
    """Join Relay and answer Alice's streams until Ctrl-C or SIGTERM."""
    stop = stop_requested()
    node = Node(identity, carrier=VsockCarrier(), parent=parent)
    served = Served()
    serving = asyncio.create_task(serve(node, alice, served))
    stopping = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({serving, stopping}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        serving.cancel()
        stopping.cancel()
        await asyncio.wait({serving, stopping})
        await node.close()
    # `serve` ends only by failing or by being stopped.
    if not serving.cancelled():
        serving.result()
    return served


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="sealed_postcard")
    modes = parser.add_subparsers(dest="mode", required=True)

    identities = modes.add_parser(
        "identities", help="write identity bundles for Alice, Relay, and Bob"
    )
    identities.add_argument("--out", type=Path, required=True)

    relay = modes.add_parser("relay", help="run Relay, the namespace root")
    relay.add_argument("--listen", action="append", required=True)

    alice = modes.add_parser("alice", help="run Alice until her exchanges finish")
    alice.add_argument("--parent-pid", type=Pid, required=True)
    alice.add_argument("--parent", required=True)
    alice.add_argument("--bob-pid", type=Pid, required=True)

    bob = modes.add_parser("bob", help="run Bob until Ctrl-C or SIGTERM")
    bob.add_argument("--parent-pid", type=Pid, required=True)
    bob.add_argument("--parent", required=True)
    bob.add_argument("--alice-pid", type=Pid, required=True)

    return parser.parse_args()


def role_identity() -> Identity:
    """Load the identity bundle that the launcher passed in the environment."""
    return Identity.from_pem(os.environ["SEALED_POSTCARD_IDENTITY"])


def main() -> None:
    args = parse_args()
    match args.mode:
        case "identities":
            write_identities(args.out)
        case "relay":
            asyncio.run(run_relay(role_identity(), args.listen))
        case "alice":
            parent = (args.parent_pid, args.parent)
            asyncio.run(run_alice(role_identity(), parent, args.bob_pid))
        case "bob":
            parent = (args.parent_pid, args.parent)
            served = asyncio.run(run_bob(role_identity(), parent, args.alice_pid))
            all_from_alice = str(served.all_from_alice).lower()
            print(
                f"bob served {served.count} streams, all from alice: {all_from_alice}"
            )
            if served.count != STREAM_COUNT + 1 or not served.all_from_alice:
                raise SystemExit(1)


if __name__ == "__main__":
    main()
