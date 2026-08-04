# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Type stubs for the minimonarch CPython extension (see minimonarch.c).

Single small values (idents, reasons) are plain ``bytes`` and copied. Multipart
message bodies are lists of ``minimonarch.bytearray`` and moved into / out of the
message zero-copy.
"""

from __future__ import annotations

from collections.abc import Awaitable, Sequence
from typing import Literal

Role = Literal["parent", "child"]

class bytearray:
    """A writable, growable byte buffer whose storage can be *moved* into a
    message part (after which the part owns it and the bytearray is left empty).

    Exposed to Python as ``minimonarch.bytearray``. Supports the buffer protocol,
    indexing, ``len()``, and equality against any bytes-like object.
    """

    def __init__(
        self, source: int | bytes | bytearray | memoryview | None = ...
    ) -> None:
        """Construct empty (``None``), zero-filled of length ``source`` (``int``),
        or copied from a bytes-like ``source``."""
        ...

    def append(self, byte: int) -> None: ...
    def extend(self, data: bytes | bytearray | memoryview) -> None: ...
    def tobytes(self) -> bytes: ...
    def __len__(self) -> int: ...
    def __getitem__(self, index: int) -> int: ...
    def __setitem__(self, index: int, value: int) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...

class Actor:
    """An addressable messaging endpoint bound to the current context's poller.

    Creating the first Actor in an asyncio context lazily creates the underlying
    minimonarch runtime; see ``close()``.
    """

    def __init__(self, ident: bytes | None = ..., gateway: bool = ...) -> None:
        """Create an actor. ``ident`` must be unique across the run; if ``None``,
        a name must be assigned by the peer in a later ``serve``/``join``.

        ``gateway`` declares this actor as the entry point for its process group:
        it must have no parent or a network (tcp/quic) parent, and joining it to a
        ``unix://``/``inproc://`` parent is rejected. Fixed at creation."""
        ...

    def send(self, receiver: bytes, parts: Sequence[bytearray]) -> None:
        """Send a multipart message to ``receiver``. Each part is moved (left
        empty) into the message."""
        ...

    def next(self) -> Awaitable[list[bytearray]]:
        """Await the next delivered message as a list of received parts."""
        ...

    def serve(
        self,
        url: str,
        role: Role,
        name: bytes | None = ...,
        hello: Sequence[bytearray] | None = ...,
        failure: Sequence[bytearray] | None = ...,
    ) -> None:
        """Serve (listen) on ``url``. ``role`` is this actor's role in the pair:
        ``"parent"`` or ``"child"``."""
        ...

    def join(
        self,
        url: str,
        role: Role,
        name: bytes | None = ...,
        hello: Sequence[bytearray] | None = ...,
        failure: Sequence[bytearray] | None = ...,
    ) -> None:
        """Join (connect to) ``url``. ``role`` is this actor's role in the pair:
        ``"parent"`` or ``"child"``."""
        ...

    def die(self, reason: bytes) -> None:
        """Signal that this actor is dead to its parent, children, and monitors."""
        ...

    def monitor(
        self,
        ident: bytes,
        failure: Sequence[bytearray] | None = ...,
        timeout_for_nonexistence: int = ...,
    ) -> MonitorHandle:
        """Monitor ``ident``. If it dies (or is already dead), this actor is sent
        ``[*failure, ident, b"actor died"]``.

        If ``timeout_for_nonexistence`` is non-zero and ``ident`` is still not
        known anywhere in the system after that many milliseconds, the monitor
        fires once with reason ``b"actor does not exist"`` and is consumed (a
        later appearance-then-death delivers nothing more). ``0`` (the default)
        disables the timeout. Only the first monitor on a given target arms a
        timeout; later monitors of the same target ignore it."""
        ...

class MonitorHandle:
    """Handle for a registered monitor; call ``cancel()`` to deregister."""

    def cancel(self) -> None:
        """Stop delivering the monitor's failure message. Idempotent."""
        ...

def close() -> bool:
    """Tear down the current context's minimonarch runtime, returning whether one
    was installed. Actors survive as objects but error on further use; the next
    Actor created in the context transparently makes a fresh runtime."""
    ...
