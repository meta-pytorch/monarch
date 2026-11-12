# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for the experimental Chrysalis Python bindings.

The tests use real Chrysalis nodes and streams rather than mocks. They cover
identity loading, carriers and addresses, namespace discovery, authenticated
streams, cancellation, concurrent reading and writing, and shutdown.

Each test runs in its own asyncio loop. Most nodes use the default carrier, a
fresh UDP socket on 127.0.0.1. The tests where framing matters also run over
`VsockCarrier` with `unix:` endpoints, so framed datagrams travel over Unix
stream sockets. Those sockets live in a fresh, short `/tmp` directory that is
removed afterward.
"""

import asyncio
import re
import shutil
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
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

# Arbitrary application messages used by the round-trip test.
POSTCARD = b"wish you were here"
ACKNOWLEDGEMENT = b"got it"
# Maximum seconds to retry while a peer's namespace entry propagates.
VISIBLE_WITHIN = 10.0
# Number of bytes in one mebibyte.
MIB = 1 << 20
# The carriers that the carrier-dependent tests run over.
CARRIERS = ["udp", "vsock"]

# Builds one carrier configuration per test node. The optional name lets a
# carrier create a listener; `_carriers` keeps any shared resources alive.
CarrierFactory = Callable[[str | None], UdpCarrier | VsockCarrier]


@contextmanager
def _socket_dir() -> Iterator[Path]:
    """Create a short directory for one test's Unix sockets, then remove it."""
    path = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _udp_carrier(_name: str | None) -> UdpCarrier:
    """Return a fresh loopback UDP carrier; UDP nodes need no name."""
    return UdpCarrier()


@contextmanager
def _carriers(kind: str) -> Iterator[CarrierFactory]:
    """Yield the carrier factory for one case of a carrier-parametrized test.

    The vsock factory makes a named node listen at `unix:<directory>/<name>`, in
    one temporary directory removed when the context exits. It gives an unnamed
    node a `VsockCarrier` that only connects.
    """
    if kind == "udp":
        yield _udp_carrier
        return
    with _socket_dir() as directory:

        def vsock(name: str | None) -> VsockCarrier:
            return VsockCarrier(listen=[f"unix:{directory}/{name}"] if name else [])

        yield vsock


def _identities(count: int) -> list[Identity]:
    """Generate and load `count` mutually trusted test identities."""
    return [Identity.from_pem(bundle) for bundle in generate_test_identities(count)]


def _pair(
    alice: Identity, bob: Identity, carrier: CarrierFactory = _udp_carrier
) -> tuple[Node, Node]:
    """Start Alice as a namespace root and Bob as her child.

    `carrier` gives each node its carrier; by default both use UDP. Bob joins
    Alice at her first address and requires the parent to authenticate as
    Alice's PID. The nodes return before Bob's namespace entry has necessarily
    reached Alice.
    """
    alice_node = Node(alice, carrier=carrier("a"))
    bob_node = Node(
        bob, carrier=carrier(None), parent=(alice.pid, alice_node.addresses[0])
    )
    return alice_node, bob_node


def _udp_port(node: Node) -> int:
    """Return the port of a node's one loopback UDP address."""
    (address,) = node.addresses
    match = re.fullmatch(r"udp://127\.0\.0\.1:(\d+)", address)
    assert match, address
    port = int(match[1])
    assert port != 0, "the address should name the port the system chose"
    return port


async def _dial_when_visible(node: Node, pid: Pid) -> Stream:
    """Wait for `pid` to become visible, then open a stream to it.

    A child publishes its namespace entry asynchronously after joining its
    parent. `Node.dial` checks only the node's current view, so it fails until
    that publication arrives. The version-zero bindings expose one
    `ChrysalisError` for both that temporary failure and other dial failures;
    this test helper retries for at most `VISIBLE_WITHIN` seconds, then raises
    the last error.
    """
    # The event loop's clock is monotonic, so wall-clock changes cannot extend
    # or shorten the retry window.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + VISIBLE_WITHIN
    while True:
        try:
            return await node.dial(pid)
        except ChrysalisError:
            if loop.time() > deadline:
                raise
            # Yield to the parent-link and namespace tasks without busy-waiting.
            await asyncio.sleep(0.01)


async def _read_to_end(stream: Stream) -> bytes:
    """Read every byte from `stream`, stopping when the peer finishes sending."""
    chunks = []
    while chunk := await stream.read():
        chunks.append(chunk)
    return b"".join(chunks)


async def _read_exactly(stream: Stream, count: int) -> bytes:
    """Read exactly `count` bytes, assuming nothing about how many bytes one
    `read` returns, and failing on an early end of stream."""
    received = b""
    while len(received) < count:
        chunk = await stream.read(count - len(received))
        assert chunk, f"end of stream after {len(received)} of {count} bytes"
        received += chunk
    return received


async def _close_all(*nodes: Node) -> None:
    """Close all nodes concurrently and wait for every shutdown to finish."""
    await asyncio.gather(*(node.close() for node in nodes))


def test_identities() -> None:
    """Generate three identity bundles and load each one into Python.

    Check that their PIDs are distinct, use the 32-digit lowercase form,
    round-trip through `Pid`, and work as set keys. Reject malformed identity
    and PID text. Confirm that every native type belongs to the private
    `chrysalis._rust_bindings` module.

    Invariants: CPY-2, CPY-7, CPY-8.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """
    bundles = generate_test_identities(3)
    pids = [Identity.from_pem(bundle).pid for bundle in bundles]
    assert len(set(pids)) == 3, f"each bundle should have its own PID: {pids}"
    for pid in pids:
        assert re.fullmatch(r"[0-9a-f]{32}", str(pid)), pid
        assert Pid(value=str(pid)) == pid
        assert repr(pid) == f"Pid('{pid}')"
    with pytest.raises(ValueError):
        Identity.from_pem(bundles[0][: len(bundles[0]) // 2])
    with pytest.raises(ValueError):
        Pid("not-a-pid")
    natives = (Pid, Identity, Node, Stream, UdpCarrier, VsockCarrier, ChrysalisError)
    for native in natives:
        assert native.__module__ == "chrysalis._rust_bindings", native


@pytest.mark.parametrize("kind", CARRIERS)
def test_postcard_through_relay(kind: str) -> None:
    """Send a postcard from Alice to Bob through Relay and return an acknowledgement.

    All three nodes run in one Python process. Relay is the root, Alice and Bob
    join it, and each names Relay's address from `Node.addresses`. Alice finds
    Bob by PID through Relay. Check that Bob sees Alice's authenticated PID
    rather than Relay's and that both application messages arrive unchanged.

    In the `udp` case every node uses the default carrier, so Relay's UDP
    socket must forward Alice's packets to Bob. A UDP node built without a PID
    router would answer them itself, and Alice's handshake would fail because
    Relay authenticated in Bob's place. In the `vsock` case every node uses a
    `VsockCarrier` with `unix:` endpoints, which frames Chrysalis datagrams
    onto Unix byte streams, as it does over vsock.

    Invariants: CPY-2, CPY-3, CPY-4, CPY-9.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """

    async def scenario(carrier: CarrierFactory) -> None:
        alice_id, relay_id, bob_id = _identities(3)
        relay = Node(relay_id, carrier=carrier("r"))
        alice = Node(
            alice_id, carrier=carrier(None), parent=(relay_id.pid, relay.addresses[0])
        )
        bob = Node(
            bob_id, carrier=carrier(None), parent=(relay_id.pid, relay.addresses[0])
        )

        async def send() -> bytes:
            stream = await _dial_when_visible(alice, bob_id.pid)
            await stream.write(POSTCARD)
            await stream.finish()
            return await _read_to_end(stream)

        async def receive() -> tuple[Pid, bytes]:
            source, stream = await bob.accept()
            postcard = await _read_to_end(stream)
            await stream.write(ACKNOWLEDGEMENT)
            await stream.finish()
            return source, postcard

        try:
            acknowledgement, (source, postcard) = await asyncio.wait_for(
                asyncio.gather(send(), receive()), 30
            )
        finally:
            await _close_all(alice, relay, bob)
        assert source == alice_id.pid, "Bob should see Alice's PID, not Relay's"
        assert postcard == POSTCARD
        assert acknowledgement == ACKNOWLEDGEMENT

    with _carriers(kind) as carrier:
        asyncio.run(scenario(carrier))


@pytest.mark.parametrize("kind", CARRIERS)
def test_concurrent_streams(kind: str) -> None:
    """Open eight streams from Alice to Bob at once and echo every message.

    The dials begin before Alice has a pooled connection to Bob, so they create
    a burst of independent handshakes. In the `vsock` case this guards against
    the framed carrier losing queued datagrams when several connections start
    together; the `udp` case sends the same burst over UDP.

    Check that every stream completes and that each reply matches the message
    sent on that stream.

    Invariant: CPY-4.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """
    streams = 8

    async def scenario(carrier: CarrierFactory) -> None:
        alice_id, bob_id = _identities(2)
        alice, bob = _pair(alice_id, bob_id, carrier)

        async def echo(stream: Stream) -> None:
            await stream.write(await _read_to_end(stream))
            await stream.finish()

        async def serve() -> None:
            echoes = []
            try:
                for _ in range(streams):
                    _, stream = await bob.accept()
                    echoes.append(asyncio.ensure_future(echo(stream)))
                await asyncio.gather(*echoes)
            finally:
                for task in echoes:
                    task.cancel()

        async def exchange(index: int) -> bytes:
            stream = await _dial_when_visible(alice, bob_id.pid)
            await stream.write(f"stream {index}".encode())
            await stream.finish()
            return await _read_to_end(stream)

        try:
            _, *echoes = await asyncio.wait_for(
                asyncio.gather(serve(), *(exchange(i) for i in range(streams))), 30
            )
        finally:
            await _close_all(alice, bob)
        assert echoes == [f"stream {i}".encode() for i in range(streams)]

    with _carriers(kind) as carrier:
        asyncio.run(scenario(carrier))


def test_dial_unknown_pid_raises() -> None:
    """Try to dial a PID that no node has published.

    Check that `dial` reports the missing node as `ChrysalisError`.

    Invariants: CPY-3, CPY-7.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """

    async def scenario() -> None:
        node_id, absent_id = _identities(2)
        node = Node(node_id)
        try:
            with pytest.raises(ChrysalisError):
                await node.dial(absent_id.pid)
        finally:
            await node.close()

    asyncio.run(scenario())


def test_cancelled_accept_leaves_node_usable() -> None:
    """Cancel Bob's wait for a stream before any stream arrives.

    After the timeout, Alice opens a stream and Bob accepts it normally. Check
    that Bob receives Alice's authenticated PID and postcard. This proves that
    cancelling one wait does not close the node or consume the next incoming
    stream.

    Invariant: CPY-5.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """

    async def scenario() -> None:
        alice_id, bob_id = _identities(2)
        alice, bob = _pair(alice_id, bob_id)

        async def send() -> None:
            stream = await _dial_when_visible(alice, bob_id.pid)
            await stream.write(POSTCARD)
            await stream.finish()

        async def receive() -> tuple[Pid, bytes]:
            source, stream = await bob.accept()
            return source, await _read_to_end(stream)

        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(bob.accept(), 0.2)
            _, (source, postcard) = await asyncio.wait_for(
                asyncio.gather(send(), receive()), 30
            )
        finally:
            await _close_all(alice, bob)
        assert source == alice_id.pid
        assert postcard == POSTCARD

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", CARRIERS)
def test_node_constructs_outside_asyncio(kind: str) -> None:
    """Create a node in ordinary Python code, before asyncio starts.

    The Rust constructor binds the carrier and starts background Tokio tasks.
    Check that it enters the extension's Tokio runtime itself instead of
    requiring a Python event loop. The carriers bind differently, and binding a
    `VsockCarrier` is itself a call that needs the runtime. Then start an
    asyncio loop and close the node normally.

    Invariant: CPY-1.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """
    with _carriers(kind) as carrier:
        (identity,) = _identities(1)
        node = Node(identity, carrier=carrier("n"))
        asyncio.run(_close_all(node))


def test_read_and_write_on_one_stream_concurrently() -> None:
    """Read and write at the same time on one stream.

    Alice sends more than the stream's flow-control window while waiting for an
    acknowledgement. Bob sends that acknowledgement, then waits until Alice
    reads it before consuming the rest. The exchange can finish only if reading
    and writing use separate locks.

    Invariant: CPY-4.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """
    # Alice writes more than the 64 MiB flow window while reading on the same
    # stream. Her write can complete only after Bob resumes reading, and Bob
    # resumes only after her read has returned `ack`. A single lock shared by
    # both halves of the stream would deadlock here, whichever side took it.
    total = 65 * MIB
    prefix = 64 * 1024

    async def scenario() -> None:
        alice_id, bob_id = _identities(2)
        alice, bob = _pair(alice_id, bob_id)
        # In-process, so the barrier needs no flow-control credit on the
        # connection that Alice's write has filled.
        release = asyncio.Event()

        async def alice_side() -> None:
            stream = await _dial_when_visible(alice, bob_id.pid)

            async def write() -> None:
                await stream.write(bytes(total))
                await stream.finish()

            async def read_ack() -> None:
                assert await _read_exactly(stream, 3) == b"ack"
                release.set()

            await asyncio.gather(write(), read_ack())

        async def bob_side() -> int:
            _, stream = await bob.accept()
            received = len(await _read_exactly(stream, prefix))
            await stream.write(b"ack")
            await release.wait()
            received += len(await _read_to_end(stream))
            await stream.finish()
            return received

        try:
            _, received = await asyncio.wait_for(
                asyncio.gather(alice_side(), bob_side()), 60
            )
        finally:
            await _close_all(alice, bob)
        assert received == total

    asyncio.run(scenario())


def test_bad_arguments() -> None:
    """Pass invalid PID, carrier, address, and read arguments through the Python API.

    Reject with `ValueError` an abbreviated PID; a malformed, dial-only, or
    peer-name `VsockCarrier` address; a peer-name parent address; a malformed
    or unspecified `UdpCarrier` bind address; a UDP parent address with an
    unspecified IP or port 0, which no node can be reached at; a parent address
    that belongs to the other carrier; and zero or negative read sizes. Reject
    a carrier of the wrong type with `TypeError`.

    Invariants: CPY-2, CPY-3, CPY-4, CPY-7.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """

    async def scenario() -> None:
        alice_id, bob_id = _identities(2)
        with pytest.raises(ValueError, match="32 hex digits"):
            Pid("0123abcd")
        with pytest.raises(ValueError):
            VsockCarrier(listen=["bogus"])
        with pytest.raises(ValueError):
            VsockCarrier(listen=["hybrid:5000:/tmp/v.sock"])
        with pytest.raises(ValueError, match="peer's name"):
            VsockCarrier(listen=["node:a"])
        with pytest.raises(ValueError, match="peer's name"):
            Node(alice_id, carrier=VsockCarrier(), parent=(bob_id.pid, "node:b"))
        with pytest.raises(ValueError):
            UdpCarrier(bind="bogus")
        for unspecified in ("0.0.0.0:0", "[::]:0"):
            with pytest.raises(ValueError, match="specific IP"):
                UdpCarrier(bind=unspecified)
        for unreachable in (
            "udp://0.0.0.0:5000",
            "udp://[::]:5000",
            "udp://127.0.0.1:0",
        ):
            with pytest.raises(ValueError, match="specific IP and a non-zero port"):
                Node(alice_id, parent=(bob_id.pid, unreachable))
        with pytest.raises(ValueError, match="udp://<ip>:<port>"):
            Node(alice_id, parent=(bob_id.pid, "unix:/tmp/x"))
        with pytest.raises(ValueError, match="unix:, native:, or hybrid:"):
            Node(
                alice_id,
                carrier=VsockCarrier(),
                parent=(bob_id.pid, "udp://127.0.0.1:1"),
            )
        not_a_carrier: Any = "udp"
        with pytest.raises(TypeError):
            Node(alice_id, carrier=not_a_carrier)
        alice, bob = _pair(alice_id, bob_id)
        try:
            stream = await _dial_when_visible(alice, bob_id.pid)
            with pytest.raises(ValueError):
                await stream.read(0)
            with pytest.raises(ValueError):
                await stream.read(-1)
        finally:
            await _close_all(alice, bob)

    asyncio.run(scenario())


def test_close() -> None:
    """Exercise the node's close transition.

    A close call made without an asyncio loop must fail without changing the
    node. Once a close future is returned, new dials and accepts must fail even
    before that future is awaited. Awaiting close finishes shutdown, and
    closing again is safe.

    Invariants: CPY-6, CPY-7.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """

    # `other` is never started, so a dial to it from an open node also raises
    # `ChrysalisError`; only the message shows the node is closed.
    async def accept_is_pending(node: Node) -> None:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(node.accept(), 0.2)

    async def scenario(node: Node, other: Pid) -> None:
        closing = node.close()
        with pytest.raises(ChrysalisError, match="the node is closed"):
            await node.dial(other)
        with pytest.raises(ChrysalisError, match="the node is closed"):
            await node.accept()
        await closing
        await node.close()
        with pytest.raises(ChrysalisError, match="the node is closed"):
            await node.dial(other)

    node_id, other_id = _identities(2)
    node = Node(node_id)
    # Without a running loop `close` cannot create its awaitable, and the
    # node stays open.
    pytest.raises(RuntimeError, node.close)
    asyncio.run(accept_is_pending(node))
    asyncio.run(scenario(node, other_id.pid))


def test_addresses() -> None:
    """Report each node's addresses.

    Default UDP nodes, and a node given `UdpCarrier(bind="127.0.0.1:0")`, each
    report one loopback `udp://` address with the distinct, non-zero port the
    system chose. Passing `bind` by keyword keeps the stub's parameter name
    from drifting from the runtime's. A listening `VsockCarrier` node reports
    its `listen` addresses, and one that only connects reports none. The other
    tests join and dial through these addresses, which shows that the reported
    UDP address is reachable.

    Invariant: CPY-9.
    Registry: `crates/chrysalis-extension/src/lib.rs`.
    """

    async def scenario(directory: Path) -> None:
        first_id, second_id, bound_id, listener_id, dialer_id = _identities(5)
        listen = f"unix:{directory}/l"
        udp = [
            Node(first_id),
            Node(second_id),
            Node(bound_id, carrier=UdpCarrier(bind="127.0.0.1:0")),
        ]
        listener = Node(listener_id, carrier=VsockCarrier(listen=[listen]))
        dialer = Node(dialer_id, carrier=VsockCarrier())
        try:
            ports = [_udp_port(node) for node in udp]
            assert len(set(ports)) == len(udp), f"ports should differ: {ports}"
            assert listener.addresses == [listen]
            assert dialer.addresses == []
        finally:
            await _close_all(*udp, listener, dialer)

    with _socket_dir() as directory:
        asyncio.run(scenario(directory))
