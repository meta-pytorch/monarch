# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Offline tests for :class:`~wbridge.backend.rdma.monarch.MonarchEngine`.

No Monarch install and no fabric: a fake ``monarch.*`` is injected into ``sys.modules`` so the parts of
the engine that are pure bookkeeping can be tested on any box with torch. What that covers is exactly the
logic Monarch's API forced on us, and each case here stands for a failure that is invisible or
catastrophic on real hardware:

* **region -> handle mapping.** ``RDMABuffer`` has no remote offset, so a write is addressed by looking up
  a handle published for that exact ``(addr, size)``. A tiling mismatch between publisher and writer
  silently addresses the wrong tile, or raises deep inside the data path mid-transfer.
* **the configurable tiling itself**, which must be byte-exact and identical on both sides. Tests use a
  small 4 MiB tile to keep their CPU-memory footprint low; production defaults to 64 MiB.
* **the threaded actor context.** WeightBridge calls ``write`` from daemon threads that start with an
  empty context; getting this wrong is an *uncatchable Rust panic* that poisons the actor. The concurrent
  case is here because the obvious implementation (one shared ``contextvars.Context`` + ``ctx.run``) works
  single-threaded and fails only under concurrency, with ``cannot enter context: already entered``.
* **the event-loop guard**, which turns an actor-loop deadlock (an unbounded hang with nothing in the log) into
  an immediate error.

Run in-container:  python -m pytest tests/test_monarch_engine.py -q
"""

from __future__ import annotations

import asyncio
import sys
import threading
import types

import pytest
import torch

CHUNK = 4 * 1024 * 1024


# --------------------------------------------------------------------------- fake monarch
class FakeRDMABuffer:
    """Stands in for ``monarch.rdma.RDMABuffer``: an opaque handle over one tensor view."""

    def __init__(self, view) -> None:
        self.view = view
        self.nbytes = view.numel() * view.element_size()


class FakeRDMAAction:
    def __init__(self) -> None:
        self.ops: list[tuple[FakeRDMABuffer, object]] = []

    def write_remote(self, dst, src):
        self.ops.append((dst, src))
        return self

    def submit(self, *, timeout: int = 60):
        # Perform the copies eagerly so tests can assert on delivered bytes, and hand back a Future-alike.
        for dst, src in self.ops:
            dst.view.copy_(src)
        return FakeFuture(len(self.ops))


class FakeFuture:
    def __init__(self, n_ops: int) -> None:
        self.n_ops = n_ops
        self.got = False

    def get(self):
        self.got = True
        return None


class FakeContext:
    """Stands in for a monarch ``Context``; identity is all the engine needs."""

    def __init__(self, tag: str = "actor") -> None:
        self.tag = tag


@pytest.fixture
def fake_monarch(monkeypatch):
    """Install a fake ``monarch`` package and return its mutable state.

    ``_context`` is a real :class:`contextvars.ContextVar` so the threading tests exercise genuine
    contextvar semantics (empty in a fresh thread) rather than a mock's.
    """
    import contextvars

    ctxvar = contextvars.ContextVar("fake_monarch_context", default=None)
    state = types.SimpleNamespace(
        ctx=FakeContext(), ctxvar=ctxvar, backend="ibverbs", set_calls=[], buffers=[]
    )

    def _make(name: str, **attrs):
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        monkeypatch.setitem(sys.modules, name, m)
        # `from monarch.rdma import X` resolves the leaf as an attribute of its parent package, so the
        # sys.modules entry alone is not enough.
        parent, _, leaf = name.rpartition(".")
        if parent:
            setattr(sys.modules[parent], leaf, m)
        return m

    def _rdma_buffer(view):
        b = FakeRDMABuffer(view)
        state.buffers.append(b)
        return b

    def _set_context(c):
        state.set_calls.append((threading.current_thread().name, c))
        return ctxvar.set(c)

    _make("monarch")
    _make(
        "monarch.rdma",
        RDMABuffer=_rdma_buffer,
        RDMAAction=FakeRDMAAction,
        get_rdma_backend=lambda: state.backend,
    )
    _make("monarch.actor", context=lambda: ctxvar.get() or state.ctx)
    _make("monarch._src")
    _make("monarch._src.actor")
    _make("monarch._src.actor.actor_mesh", _context=ctxvar, _set_context=_set_context)
    return state


@pytest.fixture
def engine(fake_monarch):
    from wbridge.backend.rdma.monarch import MonarchEngine

    eng = MonarchEngine()
    eng._chunk = CHUNK  # test-sized override; production's documented default is 64 MiB
    fake_monarch.ctxvar.set(fake_monarch.ctx)  # pretend this is the actor's own thread
    eng.init("10.0.0.1", "rdma")
    return eng


def _buf(nbytes: int, fill: int = 0) -> torch.Tensor:
    return torch.full((nbytes,), fill, dtype=torch.uint8)


# --------------------------------------------------------------------------- tiling
@pytest.mark.parametrize(
    "size,expect",
    [
        (0, []),
        (1, [(0x1000, 1)]),
        (CHUNK, [(0x1000, CHUNK)]),
        (CHUNK + 1, [(0x1000, CHUNK), (0x1000 + CHUNK, 1)]),
        (
            3 * CHUNK,
            [(0x1000, CHUNK), (0x1000 + CHUNK, CHUNK), (0x1000 + 2 * CHUNK, CHUNK)],
        ),
    ],
)
def test_tile_is_exact_and_covers(size, expect):
    from wbridge.backend.rdma.monarch import _tile

    got = list(_tile(0x1000, size, CHUNK))
    assert got == expect
    assert sum(n for _, n in got) == size, (
        "tiling must cover the region exactly, no gaps or overlap"
    )


# --------------------------------------------------------------------------- registration / lookup
def test_register_requires_the_owning_tensor(engine):
    with pytest.raises(RuntimeError, match="needs the owning tensor"):
        engine.register(0x1000, 64)


def test_local_view_resolves_an_interior_pointer(engine):
    t = _buf(1024)
    engine.register(t.data_ptr(), 1024, tensor=t)
    v = engine._local_view(t.data_ptr() + 256, 128)
    assert v.numel() == 128
    assert v.data_ptr() == t.data_ptr() + 256


def test_local_view_rejects_an_unregistered_pointer(engine):
    t = _buf(1024)
    engine.register(t.data_ptr(), 1024, tensor=t)
    with pytest.raises(KeyError, match="not in any registered buffer"):
        engine._local_view(t.data_ptr() + 1024, 8)  # one byte past the end


def test_publish_tiles_large_regions(engine, fake_monarch):
    t = _buf(3 * CHUNK)
    engine.register(t.data_ptr(), 3 * CHUNK, tensor=t)
    engine.publish_regions([(t.data_ptr(), 3 * CHUNK)])

    assert len(engine._published) == 3, (
        "a 12 MiB region must publish three 4 MiB handles"
    )
    assert set(engine._published) == {
        (t.data_ptr() + i * CHUNK, CHUNK) for i in range(3)
    }
    assert all(b.nbytes == CHUNK for b in fake_monarch.buffers)


def test_publish_skips_empty_and_dedupes(engine):
    t = _buf(4096)
    engine.register(t.data_ptr(), 4096, tensor=t)
    engine.publish_regions(
        [(t.data_ptr(), 0), (t.data_ptr(), 512), (t.data_ptr(), 512)]
    )
    assert set(engine._published) == {(t.data_ptr(), 512)}


def test_write_to_an_unpublished_region_names_the_region(engine):
    """The failure mode this guards is a publisher/writer disagreement; the error must be diagnosable."""
    src = _buf(64, fill=7)
    engine.register(src.data_ptr(), 64, tensor=src)
    engine.attach_peer("peer:1", {"regions": {}})
    with pytest.raises(KeyError, match="published no region"):
        engine.write("peer:1", [src.data_ptr()], [0xDEAD000], [64])


# --------------------------------------------------------------------------- data path
def _pair(fake_monarch):
    """Two engines sharing the fake fabric: ``a`` writes into ``b``'s published regions."""
    from wbridge.backend.rdma.monarch import MonarchEngine

    a, b = MonarchEngine(), MonarchEngine()
    a._chunk = b._chunk = CHUNK
    a.init("10.0.0.1", "rdma")
    b.init("10.0.0.2", "rdma")
    return a, b


def test_write_delivers_bytes_through_the_handle_map(fake_monarch):
    a, b = _pair(fake_monarch)
    src = torch.arange(8192, dtype=torch.int32).view(torch.uint8).clone()
    dst = _buf(src.numel())

    a.register(src.data_ptr(), src.numel(), tensor=src)
    b.register(dst.data_ptr(), dst.numel(), tensor=dst)
    b.publish_regions([(dst.data_ptr(), dst.numel())])
    a.attach_peer(b.session_id(), b.publish_payload())

    a.write(b.session_id(), [src.data_ptr()], [dst.data_ptr()], [src.numel()])
    assert torch.equal(dst, src)


def test_write_of_a_multi_tile_region_reassembles_in_order(fake_monarch):
    """A >4 MiB write is split; a mismatch between the two tilings would scramble or drop a tile."""
    a, b = _pair(fake_monarch)
    n = 2 * CHUNK + 12345
    src = torch.randint(0, 256, (n,), dtype=torch.uint8)
    dst = _buf(n)

    a.register(src.data_ptr(), n, tensor=src)
    b.register(dst.data_ptr(), n, tensor=dst)
    b.publish_regions([(dst.data_ptr(), n)])
    a.attach_peer(b.session_id(), b.publish_payload())

    h = a.write_async(b.session_id(), [src.data_ptr()], [dst.data_ptr()], [n])
    assert h.n_ops == 3, "two full tiles plus a tail must share one action"
    a.wait([h])
    assert h.got
    assert torch.equal(dst, src)


def test_write_batches_every_op_into_one_action(fake_monarch):
    a, b = _pair(fake_monarch)
    src = torch.randint(0, 256, (3 * 1024,), dtype=torch.uint8)
    dst = _buf(3 * 1024)
    a.register(src.data_ptr(), src.numel(), tensor=src)
    b.register(dst.data_ptr(), dst.numel(), tensor=dst)
    offs = [(dst.data_ptr() + i * 1024, 1024) for i in range(3)]
    b.publish_regions(offs)
    a.attach_peer(b.session_id(), b.publish_payload())

    h = a.write_async(
        b.session_id(),
        [src.data_ptr() + i * 1024 for i in range(3)],
        [o for o, _ in offs],
        [1024, 1024, 1024],
    )
    assert h.n_ops == 3
    a.wait([h])
    assert torch.equal(dst, src)


def test_write_async_of_nothing_is_a_no_op(engine):
    assert engine.write_async("peer:1", [], [], []) is None
    engine.wait([None])  # must tolerate the None handle write() would hand back


def test_write_rejects_mismatched_lengths(engine):
    with pytest.raises(AssertionError, match="length mismatch"):
        engine.write_async("peer:1", [1, 2], [3], [4])


# --------------------------------------------------------------------------- threading
def test_write_from_a_bare_thread_installs_the_actor_context(fake_monarch):
    """WeightBridge's sender Stage-2 thread starts with an empty context.

    Without the install this is a Rust panic that poisons the actor, not a catchable error — so the check
    is that the engine set the context *on that thread*, not merely that the call returned.
    """
    a, b = _pair(fake_monarch)
    src = torch.randint(0, 256, (2048,), dtype=torch.uint8)
    dst = _buf(2048)
    a.register(src.data_ptr(), 2048, tensor=src)
    b.register(dst.data_ptr(), 2048, tensor=dst)
    b.publish_regions([(dst.data_ptr(), 2048)])
    a.attach_peer(b.session_id(), b.publish_payload())

    seen = {}

    def worker():
        assert fake_monarch.ctxvar.get() is None, (
            "a fresh thread must start with no context"
        )
        a.write(b.session_id(), [src.data_ptr()], [dst.data_ptr()], [2048])
        seen["ctx"] = fake_monarch.ctxvar.get()

    t = threading.Thread(target=worker, name="wb-stage2")
    t.start()
    t.join()

    assert seen["ctx"] is fake_monarch.ctx, (
        "the actor context must be live on the worker thread"
    )
    assert any(name == "wb-stage2" for name, _ in fake_monarch.set_calls)
    assert torch.equal(dst, src)


def test_concurrent_threads_can_write(fake_monarch):
    """The regression guard for a shared ``contextvars.Context``.

    ``ctx.run()`` raises ``cannot enter context: already entered`` when a second thread enters a Context
    the first is still inside — which only ever shows up under concurrency, on real hardware, mid-run.
    """
    a, b = _pair(fake_monarch)
    n_threads, sz = 8, 4096
    src = torch.randint(0, 256, (n_threads * sz,), dtype=torch.uint8)
    dst = _buf(n_threads * sz)
    a.register(src.data_ptr(), src.numel(), tensor=src)
    b.register(dst.data_ptr(), dst.numel(), tensor=dst)
    b.publish_regions([(dst.data_ptr() + i * sz, sz) for i in range(n_threads)])
    a.attach_peer(b.session_id(), b.publish_payload())

    start = threading.Barrier(
        n_threads
    )  # maximise the overlap the shared-Context bug needs
    errors: list[BaseException] = []

    def worker(i):
        try:
            start.wait(timeout=10)
            a.write(
                b.session_id(),
                [src.data_ptr() + i * sz],
                [dst.data_ptr() + i * sz],
                [sz],
            )
        except BaseException as e:  # noqa: BLE001 — the Context error is what we are hunting
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=(i,), name=f"wb-w{i}")
        for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent writes raised: {errors!r}"
    assert torch.equal(dst, src)


def test_an_existing_foreign_context_is_left_alone(fake_monarch):
    """A thread that already carries a context (the actor's own) must not be re-pointed."""
    a, b = _pair(fake_monarch)
    src, dst = _buf(64, fill=3), _buf(64)
    a.register(src.data_ptr(), 64, tensor=src)
    b.register(dst.data_ptr(), 64, tensor=dst)
    b.publish_regions([(dst.data_ptr(), 64)])
    a.attach_peer(b.session_id(), b.publish_payload())

    other = FakeContext("other")
    fake_monarch.ctxvar.set(other)
    a.write(b.session_id(), [src.data_ptr()], [dst.data_ptr()], [64])
    assert fake_monarch.ctxvar.get() is other


def test_wait_on_a_live_event_loop_raises_instead_of_hanging(fake_monarch):
    """Guard against blocking the actor loop on its own completion message.

    Blocking an actor's loop on a completion that only that loop can deliver is an unbounded hang with no
    log line. It must fail immediately, and the message must say where the work belongs.
    """
    a, b = _pair(fake_monarch)
    src, dst = _buf(64, fill=5), _buf(64)
    a.register(src.data_ptr(), 64, tensor=src)
    b.register(dst.data_ptr(), 64, tensor=dst)
    b.publish_regions([(dst.data_ptr(), 64)])
    a.attach_peer(b.session_id(), b.publish_payload())

    async def on_the_loop():
        h = a.write_async(b.session_id(), [src.data_ptr()], [dst.data_ptr()], [64])
        with pytest.raises(RuntimeError, match="would deadlock"):
            a.wait([h])

    asyncio.run(on_the_loop())


# --------------------------------------------------------------------------- lifecycle
def test_init_refuses_the_tcp_fallback(fake_monarch):
    """A silent TCP fallback measures the wrong thing while looking like success."""
    from wbridge.backend.rdma.monarch import MonarchEngine

    fake_monarch.backend = "tcp"
    with pytest.raises(RuntimeError, match="not 'ibverbs'"):
        MonarchEngine().init("10.0.0.1", "rdma")

    fake_monarch.backend = "none"
    with pytest.raises(RuntimeError, match="no RDMA backend"):
        MonarchEngine().init("10.0.0.1", "rdma")


def test_session_id_stays_host_shaped(engine):
    """router._setup_rdma_buffers parses the session to detect co-located peers, so it cannot be a blob."""
    host, _, pid = engine.session_id().partition(":")
    assert host == "10.0.0.1"
    assert pid.isdigit()


def test_calls_before_init_do_not_reach_monarch(fake_monarch):
    from wbridge.backend.rdma.monarch import MonarchEngine

    eng = MonarchEngine()
    with pytest.raises(AssertionError, match="init not called"):
        eng.publish_regions([(0x1000, 64)])


def test_close_drops_every_handle(engine):
    t = _buf(4096)
    engine.register(t.data_ptr(), 4096, tensor=t)
    engine.publish_regions([(t.data_ptr(), 4096)])
    engine.attach_peer("peer:1", {"regions": {(1, 2): object()}})
    engine.close()
    assert not engine._published and not engine._peer and not engine._bufs


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
