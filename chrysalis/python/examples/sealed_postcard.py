# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""The executable for the Python sealed-postcard roles.

The process tests use `sealed_postcard_python_roles.py` to start this program
once for each separate Alice, Relay, or Bob process. This file implements those
roles; the test harness chooses their topology and manages their processes.

These bindings are experimental, private, and subject to change. They are not
part of Chrysalis's public API.

Relay is the root of a namespace, and Alice and Bob join Relay. Alice finds Bob
by his PID and sends him a postcard through Relay, checks his acknowledgement,
then opens 32 numbered streams that Bob echoes. Bob checks that every stream
came from Alice.

With `--carol`, Alice also has a child, Carol: a second node in Alice's
process, which joins Alice over loopback UDP. After Alice's exchanges, Carol
sends Bob her own postcard, through Alice and Relay. Alice forwards it but
can't read it. Bob, given `--carol-pid`, checks that it came from Carol and
counts it apart from Alice's streams.

With `--udp`, Relay also binds a UDP socket, and a second Relay can join it
over UDP: Alice joins Relay 1, Bob joins Relay 2, and Alice's streams cross
both Relays. Relay prints each of its addresses as `relay address <address>`.

Each role is one mode of this program, with the command line of the Rust
`sealed-postcard` example's role modes, plus Carol's and the second Relay's
options. Every mode except `identities` reads its identity bundle from
`SEALED_POSTCARD_IDENTITY`, and Alice with `--carol` reads Carol's from
`SEALED_POSTCARD_CAROL_IDENTITY`:

    sealed_postcard identities --out <dir> [--relays {1,2}] [--carol]
    sealed_postcard relay --listen <addr>... [--udp <ip:port>] [--parent-pid <pid> --parent <addr>]
    sealed_postcard alice --parent-pid <pid> --parent <addr> --bob-pid <pid> [--carol]
    sealed_postcard bob --parent-pid <pid> --parent <addr> --alice-pid <pid> [--carol-pid <pid>]
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
    UdpCarrier,
    VsockCarrier,
)

POSTCARD = b"Tie a yellow ribbon round the old oak tree."
ACKNOWLEDGEMENT = b"The porch light is on."
# Numbered streams Alice opens after the postcard. Each carries its number as
# one byte.
STREAM_COUNT = 32
# Seconds Alice waits for Bob to become visible through Relay.
PEER_WAIT = 120.0


@dataclass
class Served:
    """The streams Bob has answered apart from Carol's, whether Alice opened
    every one, and how many postcards Carol sent."""

    count: int = 0
    all_from_alice: bool = True
    carol_postcards: int = 0


def write_identities(out: Path, relays: int, carol: bool) -> None:
    """Write mutually trusted identity bundles for the roles, with a second
    Relay's and Carol's if asked, and print their PIDs."""
    out.mkdir(parents=True, exist_ok=True)
    roles = ["alice", "relay", "bob"]
    if relays == 2:
        roles.insert(2, "relay2")
    if carol:
        roles.append("carol")
    # One call, so every bundle trusts the same test CA.
    for name, bundle in zip(roles, generate_test_identities(len(roles))):
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


async def run_alice(
    identity: Identity, parent: tuple[Pid, str], bob: Pid, carol: Identity | None
) -> None:
    """Join Relay, then complete both exchanges with Bob.

    Given `carol`, Alice's carrier is the pair `(VsockCarrier(), UdpCarrier())`:
    she joins Relay over vsock and serves Carol over loopback UDP. Carol, a
    second node in this process, then sends Bob her own postcard.
    """
    carrier = VsockCarrier() if carol is None else (VsockCarrier(), UdpCarrier())
    node = Node(identity, carrier=carrier, parent=parent)
    child: Node | None = None
    try:
        if carol is not None:
            # Alice's vsock carrier only connects, so her one address is her
            # UDP address, where Carol joins her.
            (address,) = node.addresses
            child = Node(carol, parent=(identity.pid, address))
        await exchange_postcard(node, bob)
        await exchange_numbered_streams(node, bob)
        if child is not None:
            await exchange_postcard(child, bob)
    finally:
        if child is not None:
            await child.close()
        await node.close()


async def run_relay(
    identity: Identity,
    listen: list[str],
    udp: str | None,
    parent: tuple[Pid, str] | None,
) -> None:
    """Serve until Ctrl-C or SIGTERM: as the namespace root, or joined to
    `parent`.

    Given `udp`, Relay's carrier is the pair `(UdpCarrier(bind=udp),
    VsockCarrier(listen=listen))`: it joins a parent over UDP and serves
    children over both.
    """
    stop = stop_requested()
    vsock = VsockCarrier(listen=listen)
    carrier = vsock if udp is None else (UdpCarrier(bind=udp), vsock)
    node = Node(identity, carrier=carrier, parent=parent)
    try:
        # Bound at port 0, Relay's UDP address is known only from these lines.
        for address in node.addresses:
            print(f"relay address {address}", flush=True)
        await stop.wait()
    finally:
        await node.close()


async def acknowledge_postcard(stream: Stream, sender: str) -> None:
    """Check the postcard on `stream`, then acknowledge it."""
    if await read_bounded(stream, len(POSTCARD)) != POSTCARD:
        raise ValueError(f"{sender} sent the wrong postcard")
    await stream.write(ACKNOWLEDGEMENT)
    await stream.finish()


async def answer(stream: Stream, first: bool) -> None:
    """Acknowledge the postcard on the first stream, and echo every later one."""
    if first:
        await acknowledge_postcard(stream, "Alice")
        return
    number = await read_bounded(stream, 1)
    if len(number) != 1:
        raise ValueError("numbered stream did not contain one byte")
    await stream.write(number)
    await stream.finish()


async def serve(node: Node, alice: Pid, carol: Pid | None, served: Served) -> None:
    """Answer streams one at a time, recording each in `served`."""
    while True:
        # `accept` returns the PID that the stream's mutual TLS authenticated.
        source, stream = await node.accept()
        # Carol's streams are postcards, counted apart from the rest, so they
        # can't change which stream is Alice's postcard.
        if carol is not None and source == carol:
            await acknowledge_postcard(stream, "Carol")
            served.carol_postcards += 1
            continue
        await answer(stream, first=served.count == 0)
        served.count += 1
        served.all_from_alice = served.all_from_alice and source == alice


async def run_bob(
    identity: Identity, parent: tuple[Pid, str], alice: Pid, carol: Pid | None
) -> Served:
    """Join Relay and answer Alice's streams, and Carol's if given, until
    Ctrl-C or SIGTERM."""
    stop = stop_requested()
    node = Node(identity, carrier=VsockCarrier(), parent=parent)
    served = Served()
    serving = asyncio.create_task(serve(node, alice, carol, served))
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
    identities.add_argument(
        "--relays",
        type=int,
        choices=(1, 2),
        default=1,
        help="write bundles for this many Relays",
    )
    identities.add_argument(
        "--carol", action="store_true", help="also write Carol's identity bundle"
    )

    relay = modes.add_parser("relay", help="run Relay, the namespace root by default")
    relay.add_argument("--listen", action="append", required=True)
    relay.add_argument("--udp", help="also bind UDP at this IP and port")
    relay.add_argument("--parent-pid", type=Pid)
    relay.add_argument("--parent")

    alice = modes.add_parser("alice", help="run Alice until her exchanges finish")
    alice.add_argument("--parent-pid", type=Pid, required=True)
    alice.add_argument("--parent", required=True)
    alice.add_argument("--bob-pid", type=Pid, required=True)
    alice.add_argument(
        "--carol",
        action="store_true",
        help="serve Carol, who sends Bob her own postcard",
    )

    bob = modes.add_parser("bob", help="run Bob until Ctrl-C or SIGTERM")
    bob.add_argument("--parent-pid", type=Pid, required=True)
    bob.add_argument("--parent", required=True)
    bob.add_argument("--alice-pid", type=Pid, required=True)
    bob.add_argument("--carol-pid", type=Pid, help="also expect Carol's postcard")

    args = parser.parse_args()
    if args.mode == "relay" and (args.parent_pid is None) != (args.parent is None):
        relay.error("--parent-pid and --parent come together")
    return args


def role_identity() -> Identity:
    """Load the identity bundle that the launcher passed in the environment."""
    return Identity.from_pem(os.environ["SEALED_POSTCARD_IDENTITY"])


def main() -> None:
    args = parse_args()
    match args.mode:
        case "identities":
            write_identities(args.out, args.relays, args.carol)
        case "relay":
            parent = None if args.parent is None else (args.parent_pid, args.parent)
            asyncio.run(run_relay(role_identity(), args.listen, args.udp, parent))
        case "alice":
            parent = (args.parent_pid, args.parent)
            carol = (
                Identity.from_pem(os.environ["SEALED_POSTCARD_CAROL_IDENTITY"])
                if args.carol
                else None
            )
            asyncio.run(run_alice(role_identity(), parent, args.bob_pid, carol))
        case "bob":
            parent = (args.parent_pid, args.parent)
            served = asyncio.run(
                run_bob(role_identity(), parent, args.alice_pid, args.carol_pid)
            )
            all_from_alice = str(served.all_from_alice).lower()
            print(
                f"bob served {served.count} streams, all from alice: {all_from_alice}"
            )
            alice_ok = served.count == STREAM_COUNT + 1 and served.all_from_alice
            carol_ok = args.carol_pid is None or served.carol_postcards == 1
            if args.carol_pid is not None:
                print(f"bob got postcards from carol: {served.carol_postcards}")
            if not (alice_ok and carol_ok):
                raise SystemExit(1)


if __name__ == "__main__":
    main()
