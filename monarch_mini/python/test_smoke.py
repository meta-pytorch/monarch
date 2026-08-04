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


def _quic_certs_env() -> None:
    # The quic transport reads its TLS material from these env vars; set them before
    # any quic serve/join so both this process and the worker subprocess (which
    # inherits the environment) can build their configs. Same values everywhere.
    certs = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test_certs"))
    os.environ["MM_QUIC_CERT"] = os.path.join(certs, "cert.pem")
    os.environ["MM_QUIC_KEY"] = os.path.join(certs, "key.pem")
    os.environ["MM_QUIC_CA"] = os.path.join(certs, "ca.pem")


def _quic_url() -> str:
    # Grab a free UDP port; join/serve retry, so the brief gap before it is rebound
    # by quic is harmless.
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return f"quic://127.0.0.1:{port}"


def _spawn_quic_worker(url: str, mode: str) -> "subprocess.Popen[bytes]":
    worker = os.path.join(os.path.dirname(__file__), "quic_worker.py")
    return subprocess.Popen([sys.executable, worker, url, mode])


async def test_quic_subprocess_echo() -> None:
    # End-to-end across two real processes over QUIC: this process is the parent
    # `q-srv`; a subprocess child `q-echo` joins, echoes one message back, and exits
    # cleanly. Exercises handshake + framed send/receive across the stream.
    _quic_certs_env()
    url = _quic_url()
    srv = Actor(b"q-srv")
    srv.serve(url, "parent")
    proc = _spawn_quic_worker(url, "echo_child")
    try:
        assert await asyncio.wait_for(srv.next(), 30) == [b"q-srv", b"q-echo"]
        srv.send(b"q-echo", [ba(b"ping")])
        assert await asyncio.wait_for(srv.next(), 30) == [b"ping"]
    finally:
        await _await_worker_exit(proc)


async def test_quic_kill_parent_end_caught_by_heartbeat() -> None:
    # The subprocess hosts the PARENT (`q-up`) and serves over QUIC; this process
    # joins as child `q-mid`. Hard-killing the subprocess sends no clean close (it's
    # UDP — there is no FIN), so the only way `q-mid` learns its parent is gone is
    # the bidirectional heartbeat lapsing. Losing its parent, the child also dies.
    _quic_certs_env()
    url = _quic_url()
    proc = _spawn_quic_worker(url, "parent")
    try:
        mid = Actor(b"q-mid")
        mid.join(url, "child", failure=[ba(b"mid-failed")])
        assert await asyncio.wait_for(mid.next(), 30) == [b"q-mid", b"q-up"]

        proc.kill()  # SIGKILL: no clean QUIC close, so the heartbeat must catch it
        await asyncio.get_running_loop().run_in_executor(None, proc.wait)

        assert await asyncio.wait_for(mid.next(), 30) == [
            b"mid-failed",
            b"q-up",
            b"quic heartbeat timeout",
        ]
    finally:
        if proc.poll() is None:
            await _kill_worker(proc)


async def test_quic_kill_child_end_caught_by_heartbeat() -> None:
    # The mirror image: this process is the PARENT `q-boss` serving over QUIC; the
    # subprocess child `q-down` joins, then is hard-killed. The parent's child
    # connection produces no clean close, so the heartbeat timeout is what severs it
    # and delivers the failure naming the dead child.
    _quic_certs_env()
    url = _quic_url()
    boss = Actor(b"q-boss")
    boss.serve(url, "parent", failure=[ba(b"boss-failed")])
    proc = _spawn_quic_worker(url, "child")
    try:
        assert await asyncio.wait_for(boss.next(), 30) == [b"q-boss", b"q-down"]

        proc.kill()  # SIGKILL: no clean QUIC close, so the heartbeat must catch it
        await asyncio.get_running_loop().run_in_executor(None, proc.wait)

        assert await asyncio.wait_for(boss.next(), 30) == [
            b"boss-failed",
            b"q-down",
            b"quic heartbeat timeout",
        ]
    finally:
        if proc.poll() is None:
            await _kill_worker(proc)


def _spawn_gateway_worker(
    root_url: str, b_url: str, a_tag: str
) -> "subprocess.Popen[bytes]":
    worker = os.path.join(os.path.dirname(__file__), "gateway_worker.py")
    return subprocess.Popen([sys.executable, worker, root_url, b_url, a_tag])


async def test_quic_gateways_cross_gateway_bypasses_root_and_reaches_root() -> None:
    # Two QUIC gateways joined to a shared root. gwA (and its inproc child a1) lives
    # in this process; gwB (and its inproc child b1) is a subprocess. We verify:
    #   * a1@A -> b1@B reaches b1 over a *direct* gateway-to-gateway side-channel
    #     (b1 replies pong straight back to a1), and
    #   * a1@A -> root and a1@A -> rootchild still climb gwA's parent link to the
    #     root domain, and
    #   * the root actor never sees the cross-gateway traffic (its only deliveries
    #     are the join hellos and the message explicitly addressed to it).
    _quic_certs_env()
    root_url = _quic_url()
    a_url = _quic_url()
    b_url = _quic_url()
    a_tag = a_url.split("://", 1)[1]
    b_tag = b_url.split("://", 1)[1]

    root = Actor(b"root", gateway=True)
    rootchild = Actor(b"rootchild")
    gwa = Actor(f"gwA@{a_tag}".encode(), gateway=True)
    a1 = Actor(f"a1@{a_tag}".encode())

    # root is the shared rendezvous: it serves once per joining gateway (gwA here,
    # gwB in the worker).
    root.serve(root_url, "parent")
    root.serve(root_url, "parent")
    gwa.join(root_url, "child")
    gwa.serve(a_url, "parent")  # listener so gwB can side-channel its reply to us
    root.serve("inproc://root-rc", "parent")
    rootchild.join("inproc://root-rc", "child")
    gwa.serve("inproc://a-a1", "parent")
    a1.join("inproc://a-a1", "child")

    proc = _spawn_gateway_worker(root_url, b_url, a_tag)
    try:
        # Local establishment hellos.
        assert await asyncio.wait_for(a1.next(), 30) == [
            f"a1@{a_tag}".encode(),
            f"gwA@{a_tag}".encode(),
        ]
        assert await asyncio.wait_for(rootchild.next(), 30) == [b"rootchild", b"root"]

        # root sees all three join hellos (gwA, gwB, rootchild) in any order; drain
        # them so the only further delivery to root is the message addressed to it.
        expected_peers = {
            f"gwA@{a_tag}".encode(),
            f"gwB@{b_tag}".encode(),
            b"rootchild",
        }
        seen: set[bytes] = set()
        while seen != expected_peers:
            hello = await asyncio.wait_for(root.next(), 30)
            assert bytes(hello[0]) == b"root"
            seen.add(bytes(hello[1]))

        # a1@A -> root: the empty (root-domain) specifier climbs gwA's parent link.
        a1.send(b"root", [ba(b"hi-root")])
        assert await asyncio.wait_for(root.next(), 30) == [b"hi-root"]

        # a1@A -> rootchild: up to root, then down its inproc child link.
        a1.send(b"rootchild", [ba(b"hi-rc")])
        assert await asyncio.wait_for(rootchild.next(), 30) == [b"hi-rc"]

        # a1@A -> b1@B crosses gateways directly; b1 replies pong straight back.
        a1.send(f"b1@{b_tag}".encode(), [ba(b"ping")])
        assert await asyncio.wait_for(a1.next(), 30) == [b"pong"]

        # Bypass: the cross-gateway ping/pong and the hi-rc message never reached the
        # root actor — its queue is empty now that the hellos and hi-root are drained.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(root.next(), 0.5)
    finally:
        if proc.poll() is None:
            await _kill_worker(proc)


async def test_monitor_fires_when_target_dies() -> None:
    # watcher and target are siblings under root. watcher monitors target; when
    # target dies, the failure climbs to their common ancestor (root) and fires
    # back down to watcher as [failure_prefix..., target_ident, "actor died"].
    root = Actor(b"root")
    watcher = Actor(b"watcher")
    target = Actor(b"target")
    await _connect(root, b"root", watcher, b"watcher", "inproc://mon-watcher")
    await _connect(root, b"root", target, b"target", "inproc://mon-target")

    handle = watcher.monitor(b"target", failure=[ba(b"DOWN")])
    assert isinstance(handle, minimonarch.MonitorHandle)

    target.die(b"boom")
    assert await watcher.next() == [b"DOWN", b"target", b"actor died"]


async def test_monitor_fires_when_parent_of_target_dies() -> None:
    # target dies indirectly: its parent `mid` dies, so target is unreachable.
    # The death is reported up by root (mid's link carried both mid and target),
    # so the monitor still fires even though target never reported its own death.
    root = Actor(b"root")
    watcher = Actor(b"watcher")
    mid = Actor(b"mid")
    target = Actor(b"target")
    await _connect(root, b"root", watcher, b"watcher", "inproc://mon-pw")
    await _connect(root, b"root", mid, b"mid", "inproc://mon-pm")
    await _connect(mid, b"mid", target, b"target", "inproc://mon-mt")

    watcher.monitor(b"target", failure=[ba(b"DOWN")])
    mid.die(b"crash")
    assert await watcher.next() == [b"DOWN", b"target", b"actor died"]


async def test_cancelled_monitor_does_not_fire() -> None:
    # A cancelled monitor must not deliver, even after the target dies.
    root = Actor(b"root")
    watcher = Actor(b"watcher")
    target = Actor(b"target")
    await _connect(root, b"root", watcher, b"watcher", "inproc://mon-cw")
    await _connect(root, b"root", target, b"target", "inproc://mon-ct")

    handle = watcher.monitor(b"target", failure=[ba(b"DOWN")])
    handle.cancel()
    target.die(b"boom")

    # Prove nothing fired by sending watcher a sentinel: it must be the only,
    # and first, message watcher receives.
    watcher.send(b"watcher", [ba(b"sentinel")])
    assert await watcher.next() == [b"sentinel"]


async def test_monitor_on_already_dead_actor_fires_immediately() -> None:
    # Monitoring an actor that is already known dead fires right away rather than
    # waiting forever.
    root = Actor(b"root")
    watcher = Actor(b"watcher")
    target = Actor(b"target")
    await _connect(root, b"root", watcher, b"watcher", "inproc://mon-aw")
    await _connect(root, b"root", target, b"target", "inproc://mon-at")

    target.die(b"boom")
    # root is target's parent, so it receives the connection-failure notification.
    # Consuming it confirms root has processed (and recorded as dead) target's
    # death before we subscribe.
    assert await root.next() == [b"target", b"boom"]

    watcher.monitor(b"target", failure=[ba(b"DOWN")])
    assert await watcher.next() == [b"DOWN", b"target", b"actor died"]


async def test_monitor_timeout_fires_when_target_never_exists() -> None:
    # With a non-existence timeout, monitoring a target that never appears fires
    # once with reason "actor does not exist" after the timeout elapses.
    root = Actor(b"root")
    watcher = Actor(b"watcher")
    await _connect(root, b"root", watcher, b"watcher", "inproc://mon-to")

    watcher.monitor(b"target", failure=[ba(b"DOWN")], timeout_for_nonexistence=30)
    assert await watcher.next() == [b"DOWN", b"target", b"actor does not exist"]


async def test_monitor_timeout_disabled_by_default() -> None:
    # The default timeout of 0 disables non-existence firing: an absent target
    # produces nothing on its own.
    root = Actor(b"root")
    watcher = Actor(b"watcher")
    await _connect(root, b"root", watcher, b"watcher", "inproc://mon-to-off")

    watcher.monitor(b"target", failure=[ba(b"DOWN")])
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(watcher.next(), timeout=0.2)


async def test_monitor_returns_cancellable_handle() -> None:
    # cancel() is idempotent and returns None.
    a = Actor(b"mon-handle")
    handle = a.monitor(b"nonexistent@root", failure=[ba(b"DOWN")])
    assert isinstance(handle, minimonarch.MonitorHandle)
    assert handle.cancel() is None
    assert handle.cancel() is None


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
