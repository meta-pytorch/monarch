# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""pytest smoke tests for the minimonarch asyncio bindings.

From this directory:

    uv run pytest
"""

import asyncio

import minimonarch
import pytest
from minimonarch import Actor

# Single-value arguments (idents, reasons) are plain bytes and copied. Multipart
# message bodies are lists of minimonarch.bytearray, moved into the message.
ba = minimonarch.bytearray


def test_bytearray_behaves_like_bytearray() -> None:
    b = ba()
    assert len(b) == 0
    b.append(0x61)  # 'a'
    b.extend(b"bc")
    assert len(b) == 3
    assert bytes(memoryview(b)) == b"abc"
    assert b.tobytes() == b"abc"
    assert b[0] == 0x61
    b[0] = 0x7A  # 'z'
    assert b.tobytes() == b"zbc"
    # range checks
    with pytest.raises(IndexError):
        _ = b[3]
    with pytest.raises(IndexError):
        b[3] = 0
    with pytest.raises(ValueError):
        b.append(256)
    # construct from size (zero-filled) and from bytes
    assert ba(3).tobytes() == b"\x00\x00\x00"
    assert ba(b"hi").tobytes() == b"hi"


def test_bytearray_cannot_resize_while_exported() -> None:
    b = ba(b"abc")
    mv = memoryview(b)
    with pytest.raises(BufferError):
        b.append(0)
    mv.release()
    b.append(0)  # fine once the view is gone
    assert len(b) == 4


async def test_self_send() -> None:
    a = Actor(b"hello-actor")
    a.send(b"hello-actor", [ba(b"hello, "), ba(b"self")])
    parts = await a.next()
    assert parts == [b"hello, ", b"self"]


async def test_buffer_before_next() -> None:
    # A message sent before next() is awaited must be buffered and delivered.
    a = Actor(b"buf-actor")
    a.send(b"buf-actor", [ba(b"one")])
    a.send(b"buf-actor", [ba(b"two")])
    # Give the poller a chance to drain into the queue.
    await asyncio.sleep(0.05)
    first = await a.next()
    second = await a.next()
    assert first == [b"one"]
    assert second == [b"two"]


async def test_next_blocks_until_message() -> None:
    a = Actor(b"wait-actor")

    async def delayed_send() -> None:
        await asyncio.sleep(0.05)
        a.send(b"wait-actor", [ba(b"late")])

    asyncio.ensure_future(delayed_send())
    parts = await a.next()
    assert parts == [b"late"]


async def test_serve_join_inproc() -> None:
    parent = Actor(b"py-parent")
    child = Actor(b"py-child")

    parent.serve("inproc://py-smoke", "parent")
    child.join("inproc://py-smoke", "child")

    assert await parent.next() == [b"py-parent", b"py-child"]
    assert await child.next() == [b"py-child", b"py-parent"]


async def test_unimplemented_monitor_raises() -> None:
    a = Actor(b"srv-actor")
    with pytest.raises(RuntimeError, match="implement"):
        a.monitor(b"other@root")


async def test_die_is_void() -> None:
    # die is a void call; it should not raise even though unimplemented.
    a = Actor(b"die-actor")
    assert a.die(b"bye") is None


async def test_ident_is_bytes_parts_are_bytearray() -> None:
    a = Actor(b"strict")
    # The receiver ident is bytes; a bytearray there is rejected.
    with pytest.raises(TypeError, match="bytes"):
        a.send(ba(b"strict"), [ba(b"x")])
    # Message parts are bytearray; bytes there is rejected.
    with pytest.raises(TypeError, match="minimonarch.bytearray"):
        a.send(b"strict", [b"x"])


async def test_send_moves_bytearray_parts() -> None:
    a = Actor(b"mv")
    payload = ba(b"payload")
    a.send(b"mv", [payload])
    # send() moved the bytearray part: it is now empty.
    assert len(payload) == 0
    assert payload.tobytes() == b""
    parts = await a.next()
    assert parts == [b"payload"]


async def test_recv_returns_reusable_bytearray() -> None:
    a = Actor(b"zc")
    a.send(b"zc", [ba(b"zero-copy-payload")])
    (part,) = await a.next()
    # A received part is itself a minimonarch.bytearray (not a memoryview), so
    # it can be moved straight back into another send.
    assert isinstance(part, ba)
    assert part == b"zero-copy-payload"
    a.send(b"zc", [part])  # reuse: move the same buffer back
    assert len(part) == 0  # moved out again
    assert await a.next() == [b"zero-copy-payload"]


async def test_send_rejects_exported_bytearray() -> None:
    a = Actor(b"exp")
    payload = ba(b"x")
    mv = memoryview(payload)
    with pytest.raises(BufferError):
        a.send(b"exp", [payload])
    mv.release()


async def test_bad_role_rejected() -> None:
    a = Actor(b"role-actor")
    with pytest.raises(ValueError, match="'parent' or 'child'"):
        a.serve("inproc://a", "boss")


async def test_close_returns_false_without_context() -> None:
    # Nothing has created a context in this (fresh per-test) event loop yet.
    assert minimonarch.close() is False


async def test_close_destroys_context_but_actors_survive() -> None:
    a = Actor(b"closing-actor")
    a.send(b"closing-actor", [ba(b"hi")])
    assert await a.next() == [b"hi"]

    # close() finds the context via the contextvar and tears it down.
    assert minimonarch.close() is True
    # A second close is a no-op now that the var has been reset.
    assert minimonarch.close() is False

    # The Actor object survives (it is not deallocated/invalidated), but the
    # API errors on further use against the destroyed runtime.
    assert isinstance(a, Actor)
    with pytest.raises(RuntimeError, match="closed"):
        a.send(b"closing-actor", [ba(b"again")])
    with pytest.raises(RuntimeError, match="closed"):
        a.next()

    # Destroying the actor (dropping the last reference) is still fine.
    del a


async def test_actor_after_close_gets_fresh_context() -> None:
    Actor(b"first")
    assert minimonarch.close() is True
    # A new Actor after close transparently creates a new context and works.
    b = Actor(b"second")
    b.send(b"second", [ba(b"fresh")])
    assert await b.next() == [b"fresh"]
