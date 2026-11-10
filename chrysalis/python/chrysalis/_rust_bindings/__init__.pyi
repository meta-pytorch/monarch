# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Experimental Python bindings for Chrysalis identities, nodes, and streams.

This private, version-zero module supports asyncio programs and is subject to
change. It lets a program load an identity, join a parent, find another node by
PID, and exchange bytes over authenticated streams.
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
        listen: Sequence[str] = (),
        parent: tuple[Pid, str] | None = None,
    ) -> None:
        """Create and start a node using `identity`.

        `listen` gives the `unix:<path>` or `native:<cid>:<port>` addresses
        where other nodes may connect. `parent`, when present, gives the PID
        and address of the parent this node joins. A parent address may be
        `unix:`, `native:`, or `hybrid:<port>:<uds>`. Without a parent, this
        node is the root of its namespace.

        An invalid address raises `ValueError`. A failure to bind an address or
        start the node raises `ChrysalisError`.
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
