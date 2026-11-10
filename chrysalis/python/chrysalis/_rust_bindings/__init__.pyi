# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Experimental Python bindings for Chrysalis identities, nodes, and streams.

This private, version-zero module supports asyncio programs and is subject to
change. It lets a program load an identity, start a node on a UDP or vsock
carrier, join a parent, find another node by PID, and exchange bytes over
authenticated streams.
"""

from collections.abc import Sequence

class ChrysalisError(Exception):
    """A Chrysalis node or stream operation failed.

    Input validation errors use `ValueError` instead.
    """

class Pid:
    """A complete PID that identifies one Chrysalis node.

    A PID contains 32 hexadecimal digits. PIDs compare by value and can be
    used as dictionary keys.
    """

    def __init__(self, value: str) -> None:
        """Create a PID from 32 hexadecimal digits.

        Raises `ValueError` if the text is malformed or abbreviated.
        """
        ...

    def __str__(self) -> str:
        """Return the PID as 32 lowercase hexadecimal digits."""
        ...

    def __repr__(self) -> str:
        """Return the PID as `Pid('...')`."""
        ...

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class Identity:
    """The credentials a node uses for mutual TLS.

    The leaf certificate determines the node's PID. The node uses its
    certificate chain and private key to identify itself, and its trust roots
    to decide which peer certificates to accept.
    """

    @staticmethod
    def from_pem(bundle: str) -> Identity:
        """Load an identity from one PEM string.

        The string contains, in order, the node certificate, one or more
        certificates forming its chain and trust roots, and the node's private
        key. A malformed bundle raises `ValueError`; another loading failure
        raises `ChrysalisError`.
        """
        ...

    @property
    def pid(self) -> Pid:
        """The PID derived from the leaf certificate."""
        ...

def generate_test_identities(count: int) -> list[str]:
    """Generate `count` mutually trusted identity bundles for tests.

    Each returned PEM string contains a new node certificate, the shared CA
    certificate, and the node's private key. Every call creates a new CA and
    uses it to sign all of that call's node certificates.
    """
    ...

class UdpCarrier:
    """A UDP socket for a node to bind.

    The carrier only describes the socket. The `Node` that uses it binds it.
    """

    def __init__(self, bind: str = "127.0.0.1:0") -> None:
        """Describe a UDP socket bound at `bind`.

        `bind` is a specific IP and port, such as `"127.0.0.1:5000"` or
        `"[::1]:0"`. Port 0 lets the system choose a free port. Raises
        `ValueError` if `bind` is malformed or its IP is unspecified, such as
        `0.0.0.0`, because a node advertises the address it binds.
        """
        ...

class VsockCarrier:
    """The vsock carrier, which frames datagrams onto byte streams.

    It connects nodes over Firecracker's vsock links, or over Unix stream
    sockets on one host. The carrier only describes its endpoints. The `Node`
    that uses it binds them.
    """

    def __init__(self, listen: Sequence[str] = ()) -> None:
        """Describe a carrier that accepts connections at `listen`.

        Each address is `unix:<path>` or `native:<cid>:<port>`. With no
        addresses, the node only connects to its parent. Any other address
        raises `ValueError`.
        """
        ...

class Node:
    """A participant in a Chrysalis process namespace.

    A node presents its identity to peers, learns where other nodes are, and
    opens or accepts authenticated byte streams. It may be the root of a
    namespace or join an existing namespace through one parent.
    """

    def __init__(
        self,
        identity: Identity,
        *,
        carrier: UdpCarrier | VsockCarrier | None = None,
        parent: tuple[Pid, str] | None = None,
    ) -> None:
        """Create and start a node using `identity`.

        `carrier` says how the node reaches other nodes. Without one, the node
        binds a new UDP socket on 127.0.0.1, on a port the system chooses.
        `parent`, when present, gives the PID and address of the parent this
        node joins. Without a parent, this node is the root of its namespace.
        A UDP node's parent address is `udp://<ip>:<port>`, with a specific IP
        and a non-zero port. A `VsockCarrier` node's parent address is `unix:`,
        `native:`, or `hybrid:<port>:<uds>`.

        An invalid address raises `ValueError`. A failure to bind the carrier or
        start the node raises `ChrysalisError`.
        """
        ...

    @property
    def addresses(self) -> list[str]:
        """This node's addresses.

        A UDP node has one, `udp://<ip>:<port>`, with the port it bound; other
        nodes dial it there. A `VsockCarrier` node has its `listen` addresses
        in canonical form, or none if it only connects. How another node dials
        a vsock listener depends on the deployment: a host reaches a guest's
        `native:` listener through a `hybrid:` address.
        """
        ...

    async def dial(self, pid: Pid) -> Stream:
        """Open a bidirectional stream to another node.

        This uses the node's current namespace view. It does not wait for a
        newly published PID to appear; if `pid` is not known yet, it raises
        `ChrysalisError` and the caller may retry.
        """
        ...

    async def accept(self) -> tuple[Pid, Stream]:
        """Wait until another node opens a stream to this node.

        Returns the authenticated PID of that node and the new stream. Raises
        `ChrysalisError` if this node is closed or the wait fails.
        """
        ...

    async def close(self) -> None:
        """Shut down the node and wait for all of its work to finish.

        Calling `close` starts shutdown immediately; awaiting it waits for
        shutdown to finish. New `dial` and `accept` calls then raise
        `ChrysalisError`. Calling `close` again is safe and waits for the same
        shutdown.
        """
        ...

class Stream:
    """A bidirectional byte stream between two authenticated nodes.

    Reading and writing are independent: one read can run while one `write` or
    `finish` runs. Calls in the same direction take turns.
    """

    async def write(self, data: bytes) -> None:
        """Send all of `data` to the other end of the stream.

        Await until the stream has accepted every byte. This does not end the
        stream; call `finish` when no more bytes will be sent.
        """
        ...

    async def finish(self) -> None:
        """Tell the other end that this side will send no more bytes.

        Await until the send side has closed. The receive side stays open, so
        the caller may continue reading.
        """
        ...

    async def read(self, max_bytes: int = 65536) -> bytes:
        """Read up to `max_bytes` from the other end of the stream.

        One read completes when at least one byte is available, the other end
        has finished sending, or the read fails. It does not wait to fill
        `max_bytes`; call `read` again to continue. An empty result means the
        other end has finished sending.

        `max_bytes` must be at least 1. This version allocates at most 1 MiB for
        one read, so a larger request still returns at most 1 MiB. If the
        connection closes before the other end finishes, this raises
        `ChrysalisError`.
        """
        ...
