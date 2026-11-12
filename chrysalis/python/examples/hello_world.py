# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Hello, world: a three-node Chrysalis network in one Python process.

These bindings are experimental, private, and subject to change. They are not
part of Chrysalis's public API.

Relay is the root of a namespace, and Alice and Bob join Relay. Alice finds Bob
by his PID, opens an authenticated stream to him through Relay, and sends a
postcard. Bob replies on the same stream.
"""

import asyncio

from chrysalis._rust_bindings import (
    ChrysalisError,
    generate_test_identities,
    Identity,
    Node,
    Pid,
    Stream,
)

POSTCARD = b"wish you were here"
REPLY = b"we're just two lost souls swimming in a fish bowl, year after year"


async def dial_when_visible(node: Node, pid: Pid, within: float = 10.0) -> Stream:
    """Open a stream to `pid` once `node` can see it.

    A new node's PID takes a moment to reach the rest of the namespace, and
    `dial` fails until it does.
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


async def read_to_end(stream: Stream) -> bytes:
    """Read until the other end finishes sending."""
    chunks = []
    while chunk := await stream.read():
        chunks.append(chunk)
    return b"".join(chunks)


async def hello() -> None:
    # Three identities signed by one new test CA, so the nodes trust each other.
    # Each node's PID comes from its certificate.
    alice, relay, bob = (
        Identity.from_pem(bundle) for bundle in generate_test_identities(3)
    )

    # Relay is the root. Alice and Bob join Relay, each naming Relay's PID and
    # address. Every node binds a fresh UDP port on 127.0.0.1, and `addresses`
    # reports the port it got.
    relay_node = Node(relay)
    alice_node = Node(alice, parent=(relay.pid, relay_node.addresses[0]))
    bob_node = Node(bob, parent=(relay.pid, relay_node.addresses[0]))
    nodes = [alice_node, relay_node, bob_node]
    names = ("alice", "relay", "bob")
    for name, identity, node in zip(names, (alice, relay, bob), nodes):
        print(f"{name:<5} {identity.pid} {node.addresses[0]}")

    async def bob_replies() -> None:
        # `accept` returns the PID that the stream's mutual TLS authenticated.
        source, stream = await bob_node.accept()
        postcard = await read_to_end(stream)
        sender = "alice" if source == alice.pid else str(source)
        print(f"bob received {postcard!r} from {sender}")
        await stream.write(REPLY)
        await stream.finish()

    async def alice_sends() -> None:
        # Alice knows only Bob's PID. Her namespace says to reach him through Relay.
        stream = await dial_when_visible(alice_node, bob.pid)
        await stream.write(POSTCARD)
        await stream.finish()
        print(f"alice received {await read_to_end(stream)!r} from bob")

    try:
        await asyncio.wait_for(asyncio.gather(bob_replies(), alice_sends()), 30)
    finally:
        await asyncio.gather(*(node.close() for node in nodes))


def main() -> None:
    asyncio.run(hello())


if __name__ == "__main__":
    main()
