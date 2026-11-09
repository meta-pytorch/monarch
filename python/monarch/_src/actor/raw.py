# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Framed control metadata and opaque application bytes for Python actors."""

import struct
from types import MappingProxyType
from typing import Any, cast, Mapping

from monarch._rust_bindings.monarch_hyperactor.actor import (
    MethodSpecifier,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer, FrozenBuffer
from monarch._rust_bindings.monarch_hyperactor.pickle import (
    PendingMessage,
    pickle,
    PicklingState,
)

_METHOD = "__monarch_raw_message_v1__"
_FRAME = struct.Struct("!8sQQ")
_MAGIC = b"MNRW0001"


def _freeze(data: Any) -> FrozenBuffer:
    buffer = Buffer()
    buffer.write(data)
    return buffer.freeze()


class MessageEnvelope:
    """Decoded control headers, an opaque read-only payload, and an optional Port.

    Outgoing payloads are snapshotted. Headers are shallow read-only snapshots;
    sending serializes their current values independently of the payload. Control
    capabilities use trusted Monarch pickle, regardless of the application codec.
    A received envelope privately owns any reference attachments to its payload.
    """

    def __init__(
        self,
        headers: Mapping[str, Any],
        payload: bytes | bytearray | memoryview = b"",
        reply_port: Any = None,
    ) -> None:
        self.headers: Mapping[str, Any] = MappingProxyType(dict(headers))
        self._payload: memoryview | None = memoryview(bytes(memoryview(payload)))
        self.reply_port: Any = reply_port
        self._local_state: list[Any] = []
        self._mesh_refs: list[Any] = []
        self._buffer: FrozenBuffer | None = None
        self._endpoint: tuple[str, bool, int | None] | None = None
        self._runtime: Any = None
        self._context: Any = None
        self._async = True

    @property
    def payload(self) -> memoryview:
        """A stable read-only byte view; ordinary Actor payloads snapshot lazily."""
        if self._payload is None:
            assert self._buffer is not None
            # Clone the cursor before reading, so viewing cannot consume the
            # original state that Actor's decoder will use with its references.
            snapshot = PicklingState(self._buffer).buffer().read()
            self._payload = memoryview(snapshot)
        return self._payload

    def replace(self, *, headers: Mapping[str, Any]) -> "MessageEnvelope":
        """Replace headers while retaining payload, reply port, and attachments."""
        result = object.__new__(MessageEnvelope)
        result.__dict__ = {**self.__dict__, "headers": MappingProxyType(dict(headers))}
        return result

    def _decode(self) -> Any:
        buffer = self._buffer
        if buffer is None:
            buffer = _freeze(self.payload)
        return PicklingState(buffer, self._local_state, self._mesh_refs).unpickle()


def _received(
    headers: Mapping[str, Any],
    payload: memoryview | None,
    reply_port: Any,
    local_state: list[Any],
    mesh_refs: list[Any],
    *,
    buffer: FrozenBuffer | None = None,
    endpoint: tuple[str, bool, int | None] | None = None,
) -> MessageEnvelope:
    message = object.__new__(MessageEnvelope)
    message.headers = MappingProxyType(dict(headers))
    message._payload = None if payload is None else payload.toreadonly()
    message.reply_port = reply_port
    message._local_state = local_state
    message._mesh_refs = mesh_refs
    message._buffer = buffer
    message._endpoint = endpoint
    message._runtime = None
    message._context = None
    message._async = True
    return message


def _unframe(
    buffer: FrozenBuffer, local_state: list[Any], refs: list[Any]
) -> MessageEnvelope:
    view = memoryview(buffer.read())
    if len(view) < _FRAME.size:
        raise ValueError("truncated raw message frame")
    magic, length, header_refs = _FRAME.unpack_from(view)
    if magic != _MAGIC or length > len(view) - _FRAME.size or header_refs > len(refs):
        raise ValueError("invalid raw message frame or header length")
    start = _FRAME.size
    headers, reply, endpoint = PicklingState(
        _freeze(view[start : start + length]), None, refs[:header_refs]
    ).unpickle()
    if not isinstance(headers, dict):
        raise ValueError("raw message headers must be a dictionary")
    return _received(
        headers,
        view[start + length :],
        reply,
        local_state,
        refs[header_refs:],
        endpoint=endpoint,
    )


def send_message(
    target: Any, message: MessageEnvelope, *, selection: str | None = None
) -> None:
    """Send or forward opaque bytes; default selection requires one actor.

    Explicit ``selection='all'`` broadcasts and ``'choose'`` selects one actor.
    Sending does not acknowledge processing. Framing may copy bytes, but never
    decodes or re-encodes the application payload. Resolved tensor-engine local
    state cannot be forwarded opaquely; ordinary Actor delivery retains it.
    Control metadata preserves resolved Monarch mesh references and rejects
    pending references rather than blocking the caller's loop. Await their
    initialization before sending them as headers. Tensor-engine references in
    headers are unsupported. Payload reference attachments remain independent.
    """
    from monarch._src.actor.actor_mesh import ActorMesh, context, Port

    if not isinstance(target, ActorMesh):
        raise TypeError("send_message requires a spawned actor mesh")
    if selection is None:
        if target.size() != 1:
            raise ValueError("send_message requires exactly 1 actor by default")
        selection = "all"
    elif selection not in ("all", "choose"):
        raise ValueError("selection must be 'all' or 'choose'")
    if message._local_state:
        raise ValueError(
            "opaque forwarding of tensor-engine local state is unsupported"
        )
    reply = message.reply_port
    if message._endpoint is not None and not isinstance(reply, Port):
        reply = None
    elif reply is not None and not isinstance(reply, Port):
        raise TypeError("reply_port must be a Monarch Port or None")
    kind = cast(Any, PythonMessageKind.CallMethod)(
        cast(Any, MethodSpecifier.ReturnsResponse)(_METHOD), None
    )
    state = pickle(
        (dict(message.headers), reply, message._endpoint),
        allow_tensor_engine_references=False,
        allow_mesh_references=True,
    )
    control = state.buffer()
    resolved = PendingMessage(kind, state).try_resolve_now()
    if resolved is None:
        raise ValueError(
            "pending mesh references in raw headers must be initialized before sending"
        )
    header_refs = resolved.refs
    framed = Buffer()
    framed.write(_FRAME.pack(_MAGIC, len(control), len(header_refs)))
    framed.write(control.read())
    framed.write(message.payload)
    state = PicklingState(framed.freeze(), None, [*header_refs, *message._mesh_refs])
    target._inner.cast_unresolved(
        PendingMessage(kind, state), selection, context().actor_instance._as_rust()
    )
