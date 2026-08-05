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
import os
import subprocess
import sys
import tempfile

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


async def _connect(
    parent: Actor,
    parent_ident: bytes,
    child: Actor,
    child_ident: bytes,
    url: str,
) -> None:
    """Establish a parent/child inproc link and consume both hello messages.

    Mirrors the serve/join handshake exercised by examples/hello.c: after the
    pair matches, each side receives [self_ident, other_ident].
    """
    parent.serve(url, "parent")
    child.join(url, "child")
    assert await parent.next() == [parent_ident, child_ident]
    assert await child.next() == [child_ident, parent_ident]


async def test_parent_child_bidirectional_send() -> None:
    # A directly-joined parent and child can message each other both ways.
    parent = Actor(b"parent")
    child = Actor(b"child")
    await _connect(parent, b"parent", child, b"child", "inproc://pc")

    parent.send(b"child", [ba(b"down")])
    assert await child.next() == [b"down"]

    child.send(b"parent", [ba(b"up")])
    assert await parent.next() == [b"up"]


async def test_grandchild_routes_up_to_grandparent() -> None:
    # A message with no local route is forwarded up the ancestry until an actor
    # that knows the destination is reached (here the root is the destination).
    root = Actor(b"root")
    child = Actor(b"child")
    grandchild = Actor(b"grandchild")
    await _connect(root, b"root", child, b"child", "inproc://root-child")
    await _connect(
        child, b"child", grandchild, b"grandchild", "inproc://child-grandchild"
    )

    grandchild.send(b"root", [ba(b"hi-grandparent")])
    assert await root.next() == [b"hi-grandparent"]


async def test_grandparent_routes_down_to_grandchild() -> None:
    # The root's routing table is populated up the chain when grandchild joins,
    # so a send from the root finds its way two hops down.
    root = Actor(b"root")
    child = Actor(b"child")
    grandchild = Actor(b"grandchild")
    await _connect(root, b"root", child, b"child", "inproc://root-child")
    await _connect(
        child, b"child", grandchild, b"grandchild", "inproc://child-grandchild"
    )

    root.send(b"grandchild", [ba(b"hi-grandchild")])
    assert await grandchild.next() == [b"hi-grandchild"]


async def test_message_routes_across_subtrees_via_common_ancestor() -> None:
    # Mirrors the grandchild2 -> child hop in examples/hello.c: a message routes
    # up from one subtree to the common ancestor (root) and back down another.
    root = Actor(b"root")
    child = Actor(b"child")
    child2 = Actor(b"child2")
    grandchild2 = Actor(b"grandchild2")
    await _connect(root, b"root", child, b"child", "inproc://root-child")
    await _connect(root, b"root", child2, b"child2", "inproc://root-child2")
    await _connect(
        child2, b"child2", grandchild2, b"grandchild2", "inproc://child2-grandchild2"
    )

    grandchild2.send(b"child", [ba(b"cousin-hello")])
    assert await child.next() == [b"cousin-hello"]


async def test_die_delivers_failure_to_parent() -> None:
    # When a child dies, its parent's connection breaks and the parent receives
    # [failure_prefix..., other_ident, reason].
    parent = Actor(b"parent")
    child = Actor(b"child")
    parent.serve("inproc://dies", "parent", failure=[ba(b"parent-failed")])
    child.join("inproc://dies", "child", failure=[ba(b"child-failed")])
    assert await parent.next() == [b"parent", b"child"]
    assert await child.next() == [b"child", b"parent"]

    child.die(b"done")
    assert await parent.next() == [b"parent-failed", b"child", b"done"]


def _unix_url(name: str) -> str:
    # A short path under a fresh temp dir keeps us under the sun_path limit and
    # avoids collisions between tests (each uses its own socket file).
    return f"unix://{tempfile.mkdtemp(prefix='mm-smoke-')}/{name}.sock"


async def test_serve_join_unix() -> None:
    # The unix:// transport establishes a parent/child pair over a real socket,
    # here looping back within a single process/context.
    url = _unix_url("hello")
    parent = Actor(b"ux-parent")
    child = Actor(b"ux-child")

    parent.serve(url, "parent")
    child.join(url, "child")

    assert await parent.next() == [b"ux-parent", b"ux-child"]
    assert await child.next() == [b"ux-child", b"ux-parent"]


async def test_unix_join_before_serve() -> None:
    # The joiner's connector polls until the server binds, so join may go first.
    url = _unix_url("late")
    child = Actor(b"ux-late-child")
    parent = Actor(b"ux-late-parent")

    child.join(url, "child")
    await asyncio.sleep(0.05)  # let the connector spin with nothing to connect to
    parent.serve(url, "parent")

    assert await parent.next() == [b"ux-late-parent", b"ux-late-child"]
    assert await child.next() == [b"ux-late-child", b"ux-late-parent"]


async def test_unix_bidirectional_send() -> None:
    # Messages flow both directions across the socket; payload bytes are framed
    # without extra copies.
    url = _unix_url("send")
    parent = Actor(b"ux-send-parent")
    child = Actor(b"ux-send-child")
    parent.serve(url, "parent")
    child.join(url, "child")
    assert await parent.next() == [b"ux-send-parent", b"ux-send-child"]
    assert await child.next() == [b"ux-send-child", b"ux-send-parent"]

    parent.send(b"ux-send-child", [ba(b"down"), ba(b"stream")])
    assert await child.next() == [b"down", b"stream"]

    child.send(b"ux-send-parent", [ba(b"up")])
    assert await parent.next() == [b"up"]


async def test_unix_die_delivers_failure() -> None:
    # Closing one end (here via die) severs the connection; the peer's reader
    # sees the Severed frame / EOF and the failure message is delivered.
    url = _unix_url("die")
    parent = Actor(b"ux-die-parent")
    child = Actor(b"ux-die-child")
    parent.serve(url, "parent", failure=[ba(b"parent-failed")])
    child.join(url, "child", failure=[ba(b"child-failed")])
    assert await parent.next() == [b"ux-die-parent", b"ux-die-child"]
    assert await child.next() == [b"ux-die-child", b"ux-die-parent"]

    child.die(b"bye")
    assert await parent.next() == [b"parent-failed", b"ux-die-child", b"bye"]


def _spawn_worker(url: str, mode: str) -> "subprocess.Popen[bytes]":
    # Run the worker on the same interpreter so it imports the built extension.
    worker = os.path.join(os.path.dirname(__file__), "unix_worker.py")
    return subprocess.Popen([sys.executable, worker, url, mode])


async def _await_worker_exit(proc: "subprocess.Popen[bytes]") -> None:
    # Wait off the event loop so the poller keeps draining while we join.
    loop = asyncio.get_running_loop()
    try:
        rc = await loop.run_in_executor(None, lambda: proc.wait(timeout=30))
    except subprocess.TimeoutExpired:
        proc.kill()
        await loop.run_in_executor(None, proc.wait)
        raise
    assert rc == 0, f"worker subprocess exited with code {rc}"


async def _kill_worker(proc: "subprocess.Popen[bytes]") -> None:
    proc.kill()
    await asyncio.get_running_loop().run_in_executor(None, proc.wait)


async def test_unix_subprocess_parent_serves_routes_inproc_and_unix() -> None:
    # Parent SERVES over unix; a real subprocess child JOINS. The main process
    # also has an inproc child (`client`) and the subprocess has an inproc child
    # (`worker`), so the `client` -> `worker` message crosses inproc -> unix ->
    # inproc and is echoed back the same way.
    url = _unix_url("bridge")
    proc = _spawn_worker(url, "joiner")
    try:
        root = Actor(b"root")
        client = Actor(b"client")
        root.serve("inproc://client-link", "parent")
        client.join("inproc://client-link", "child")
        assert await client.next() == [b"client", b"root"]
        assert await root.next() == [b"root", b"client"]

        root.serve(url, "parent")
        assert await asyncio.wait_for(root.next(), 30) == [b"root", b"bridge"]

        client.send(b"worker", [ba(b"hi-worker")])  # inproc -> unix -> inproc
        assert await asyncio.wait_for(client.next(), 30) == [b"hi-worker"]
    finally:
        await _await_worker_exit(proc)


async def test_unix_subprocess_child_serves_join_before_serve() -> None:
    # The roles are flipped: the subprocess actor is the CHILD and SERVES (binds),
    # while the main actor is the PARENT and JOINS. The main joins immediately but
    # the subprocess sleeps before serving, so the connector must retry until the
    # socket appears (join-before-serve).
    url = _unix_url("server")
    proc = _spawn_worker(url, "server")
    try:
        boss = Actor(b"boss")
        boss.join(url, "parent")
        assert await asyncio.wait_for(boss.next(), 30) == [b"boss", b"server"]

        boss.send(b"server", [ba(b"ping-data")])
        assert await asyncio.wait_for(boss.next(), 30) == [b"ping-data"]
    finally:
        await _await_worker_exit(proc)


async def test_unix_subprocess_kill_propagates_failure_and_cascades() -> None:
    # The subprocess hosts the PARENT (`up`) and serves over unix; the main joins
    # as its child `mid`, which in turn serves a local inproc child `leaf`. Hard-
    # killing the subprocess drops the socket: `mid` (child of the now-dead parent)
    # gets a failure and dies, and that death cascades locally to `leaf`.
    url = _unix_url("up")
    proc = _spawn_worker(url, "parent")
    try:
        mid = Actor(b"mid")
        mid.join(url, "child", failure=[ba(b"mid-failed")])
        assert await asyncio.wait_for(mid.next(), 30) == [b"mid", b"up"]

        leaf = Actor(b"leaf")
        mid.serve("inproc://leaf-link", "parent")
        leaf.join("inproc://leaf-link", "child", failure=[ba(b"leaf-failed")])
        assert await mid.next() == [b"mid", b"leaf"]
        assert await leaf.next() == [b"leaf", b"mid"]

        # Hard-kill the subprocess; the unix socket drops.
        await _kill_worker(proc)

        # The dead parent severs mid's parent link: mid gets the failure (naming the
        # dead peer "up") and, being a child that lost its parent, dies — which
        # delivers a die message to its local inproc child leaf.
        assert await asyncio.wait_for(mid.next(), 30) == [
            b"mid-failed",
            b"up",
            b"unix connection closed",
        ]
        assert await asyncio.wait_for(leaf.next(), 30) == [
            b"leaf-failed",
            b"mid",
            b"unix connection closed",
        ]
    finally:
        if proc.poll() is None:
            await _kill_worker(proc)


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
