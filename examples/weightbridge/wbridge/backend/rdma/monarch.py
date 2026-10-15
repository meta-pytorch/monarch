# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Monarch backend for :class:`~wbridge.backend.rdma.base.RdmaEngine` — one-sided RDMA over libibverbs.

A second transport alongside Mooncake, using `Monarch <https://github.com/meta-pytorch/monarch>`_'s RDMA
layer. Six properties of that layer shape everything here; each was established with focused transport
microbenchmarks on an RDMA cluster:

1. **An ``RDMABuffer`` has no remote offset.** ``write_from``/``read_into`` always target the buffer's
   base, so a peer cannot address "arena + 3 MiB". The buffer *can* wrap a tensor view, though, and every
   region WeightBridge writes into is fixed and enumerable at connect (per-(sender,round) RECV slots, flag
   slots, per-peer ``grecv`` slots). So the owner publishes one buffer per exact destination region and
   the writer looks it up — see :meth:`publish_regions`.

2. **Regions are tiled** into ``_chunk``-sized pieces on both the publish and the write side, using the
   same deterministic tiling so lookups match exactly — the direct analogue of
   :meth:`MooncakeEngine._stripe`. Tiling is required by point 1 (it is what makes an offset addressable
   at all); the tile *size* is only a tuning knob. It is not a correctness ceiling: the
   ``IBV_WC_REM_ACCESS_ERR`` / ``IBV_WC_LOC_PROT_ERR`` failures that once looked like a small per-op
   limit were an artifact of ``expandable_segments`` (point 6). With stock ``cudaMalloc``, much larger
   operations are clean; steady-state transfer time was insensitive across the tested tile sizes, while
   larger tiles reduced connect overhead by publishing fewer ``RDMABuffer`` handles.

3. **Every call needs a live Monarch actor context**, and it is a ``contextvar``, so it does not reach the
   threads WeightBridge writes from (the sender's Stage-2 daemon thread, the receiver's RS/ctl threads).
   Calling without it is not a catchable error — it trips a Rust panic that poisons the actor's dispatch
   loop. :meth:`init` captures the ``Context`` object and :meth:`_in_ctx` installs it on whatever thread
   makes a call. Note it installs the *value* rather than running inside a captured
   ``contextvars.Context``: a ``Context`` cannot be entered twice, so a shared one would raise
   ``cannot enter context: already entered`` the moment two WeightBridge threads submit concurrently.

4. **Nothing may block the actor's event loop.** An RDMA completion comes back as a message to the
   submitting actor (``RDMAAction.submit`` passes ``context().actor_instance`` to Rust), so an endpoint
   that blocks its own loop on ``Future.get()`` can never observe it — the transfer wedges forever. All
   WeightBridge work therefore runs on a dedicated thread off the loop; :meth:`wait` refuses to run on a
   live loop rather than hang.

5. **One NIC per GPU.** Monarch selects a NIC per memory region: device memory gets the NIC co-located
   with its GPU, host memory is hash-spread. Nothing to configure here, but it means per-rank bandwidth is
   limited to one NIC rather than Mooncake's multi-path striping.

6. **Do NOT enable ``PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True``**, despite Monarch warning at
   import that it is "required to maximize RDMA performance with CUDA tensors". A CUDA VMM segment is
   mapped from physical granules piecewise, and one ibverbs MR cannot span such a range: large writes
   failed in a repeatable periodic pattern with ``IBV_WC_REM_ACCESS_ERR`` /
   ``IBV_WC_LOC_PROT_ERR``. The same workload is clean with stock ``cudaMalloc``. Small transfers (the toy
   example) do not trip it, which is why it survived early testing.

Handles reach peers through the connect-time metadata exchange (:meth:`publish_payload` /
:meth:`attach_peer`), NOT through :meth:`session_id` — the router parses the session string to decide
co-location, so it has to stay an ``ip:port``-shaped string.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Sequence

import torch
from wbridge.backend.rdma.base import RdmaEngine

logger = logging.getLogger(__name__)

# Largest single RDMA op. Not a reliability limit (see docstring point 2) — 64 MiB is chosen because
# steady-state transfer time was insensitive across tested sizes while larger tiles reduced the number of
# published handles, and it stays clear of sizes that were only problematic under expandable_segments.
DEFAULT_CHUNK_BYTES = int(
    os.environ.get("WBRIDGE_MONARCH_CHUNK_BYTES", str(64 * 1024 * 1024))
)


def _tile(addr: int, size: int, chunk: int):
    """Deterministic tiling of ``[addr, addr+size)`` into <= ``chunk`` pieces.

    Publisher and writer both call this, so a writer's ``(offset, length)`` always matches a published
    buffer exactly. Any change here must change both sides at once.
    """
    off = 0
    while off < size:
        n = min(chunk, size - off)
        yield addr + off, n
        off += n


class _Submission:
    """The in-flight handle: a Monarch ``Future`` plus the op table that produced it.

    Monarch reports a submit failure as ``N/M ops failed`` with bare op *indices*, which say nothing about
    which region went wrong. Keeping the table lets :meth:`MonarchEngine.wait` translate those indices back
    into ``src -> dst+len`` and makes an addressing bug diagnosable from one log line.
    """

    __slots__ = ("fut", "ops", "dst")

    def __init__(self, fut, ops, dst: str) -> None:
        self.fut, self.ops, self.dst = fut, ops, dst

    def __getattr__(
        self, name: str
    ):  # transparently expose the underlying Future's attributes
        return getattr(self.fut, name)

    def get(self):
        return self.fut.get()

    def describe(self, indices: Sequence[int]) -> str:
        rows = [
            f"    op {i}: src {self.ops[i][0]:#x} -> dst {self.ops[i][1]:#x} + {self.ops[i][2]}"
            for i in indices
            if 0 <= i < len(self.ops)
        ]
        return f"  to {self.dst}, {len(self.ops)} ops:\n" + "\n".join(rows)


def _failed_op_indices(msg: str) -> list[int]:
    """Op indices out of a Monarch submit error ('  op 12: send completion failed ...')."""
    out = []
    for line in msg.splitlines():
        line = line.strip()
        if line.startswith("op ") and ":" in line:
            try:
                out.append(int(line[3 : line.index(":")]))
            except ValueError:
                pass
    return out


class MonarchEngine(RdmaEngine):
    """:class:`RdmaEngine` over ``monarch.rdma``. Must be constructed inside a Monarch actor."""

    def __init__(self) -> None:
        self._actor_ctx: object | None = None  # monarch Context, captured at init
        self._session = ""
        # (base, end, tensor) for every locally registered buffer, so a raw (ptr, size) from the router
        # can be turned back into the 1-D uint8 view Monarch's API needs.
        self._bufs: list[tuple[int, int, torch.Tensor]] = []
        self._published: dict[tuple[int, int], object] = {}  # our regions -> RDMABuffer
        self._peer: dict[
            str, dict[tuple[int, int], object]
        ] = {}  # peer session -> their regions
        self._chunk = DEFAULT_CHUNK_BYTES

    # ----------------------------------------------------------------- lifecycle
    def init(
        self,
        local_host: str,
        protocol: str,
        device: str = "",
        pin_local_nic: bool = False,
    ) -> None:
        from monarch.rdma import get_rdma_backend

        backend = get_rdma_backend()
        if backend == "none":
            raise RuntimeError(
                "MonarchEngine: no RDMA backend available in this process"
            )
        if backend != "ibverbs":
            # A silent TCP fallback would look like it works while measuring the wrong thing — the same
            # trap that can produce a bogus peer-to-peer number. Refuse rather than mislead.
            raise RuntimeError(
                f"MonarchEngine: Monarch reports backend {backend!r}, not 'ibverbs'. Refusing to run on "
                "the TCP fallback; set MONARCH_RDMA_ALLOW_TCP_FALLBACK=false to diagnose."
            )
        # Captured on the actor's thread. _in_ctx replays it onto WeightBridge's worker threads, which
        # start with an empty context and would otherwise trip the Rust panic (see docstring point 3).
        from monarch.actor import context

        # A completion is an actor message, so Python must run for this process to observe that a transfer
        # finished. CPython hands the GIL between threads only every sys.getswitchinterval() (default
        # 5 ms), so competing Python work can delay each completion by a full handoff. The synthetic
        # ladder showed a severe throughput loss and recovered most of it with a shorter interval. It is
        # handoff latency, not CPU starvation.
        #
        # The representative workload showed no meaningful improvement when this was applied across all
        # ranks, so it was not GIL-bound. This knob only matters for a process that really does run
        # competing Python during transfers.
        # Off by default: process-global, and shortening it costs context switches in compute-heavy Python.
        sw = float(os.environ.get("WBRIDGE_MONARCH_SWITCH_INTERVAL", "0"))
        if sw > 0:
            import sys

            sys.setswitchinterval(sw)
            logger.warning("MonarchEngine: sys.setswitchinterval(%g) applied", sw)
        self._actor_ctx = context()
        # Session stays ip:port-shaped: router._setup_rdma_buffers parses it to detect co-located peers.
        self._session = f"{local_host}:{os.getpid()}"
        logger.info(
            "MonarchEngine up: session=%s backend=%s chunk=%d B",
            self._session,
            backend,
            self._chunk,
        )

    def _in_ctx(self, fn):
        """Run *fn* with this engine's actor context installed on the calling thread.

        The install is sticky: WeightBridge's worker threads are long-lived and each one only needs the
        contextvar set once. A thread that already carries a context (the actor's own, or one inherited
        via ``asyncio.to_thread``) is left alone.
        """
        assert self._actor_ctx is not None, "MonarchEngine.init not called"
        from monarch._src.actor.actor_mesh import _context, _set_context

        if _context.get() is None:
            _set_context(self._actor_ctx)  # type: ignore[arg-type]
        return fn()

    def session_id(self) -> str:
        assert self._session, "MonarchEngine.init not called"
        return self._session

    def close(self) -> None:
        self._published.clear()
        self._peer.clear()
        self._bufs.clear()
        self._actor_ctx = None

    # ----------------------------------------------------------------- registration
    def register(
        self,
        ptr: int,
        size: int,
        is_flag: bool = False,
        tensor: torch.Tensor | None = None,
    ) -> None:
        """Record a local buffer. Monarch has no separate pin step — registration happens when an
        ``RDMABuffer`` is created — so this only keeps the tensor needed to build views later."""
        if tensor is None:
            raise RuntimeError(
                "MonarchEngine.register needs the owning tensor (pass tensor=...); Monarch's API takes "
                "tensors/memoryviews, not raw pointers"
            )
        base = int(ptr)
        self._bufs.append(
            (base, base + int(size), tensor.view(torch.uint8).reshape(-1))
        )

    def _local_view(self, ptr: int, size: int) -> torch.Tensor:
        p, n = int(ptr), int(size)
        for base, end, t in self._bufs:
            if base <= p and p + n <= end:
                off = p - base
                return t[off : off + n]
        raise KeyError(
            f"MonarchEngine: local ptr {p:#x} size {n} is not in any registered buffer"
        )

    def publish_regions(self, regions: Sequence[tuple[int, int]]) -> None:
        """Export every region a peer will write into, tiled to the per-op ceiling.

        Called once at connect, before the metadata exchange. Tiling here (rather than only on the write
        side) is what makes an offset addressable at all: the writer targets the tile whose base is the
        offset it wants.
        """
        from monarch.rdma import RDMABuffer

        def _publish() -> None:
            for addr, size in regions:
                if size <= 0:
                    continue
                for c_addr, c_size in _tile(int(addr), int(size), self._chunk):
                    key = (c_addr, c_size)
                    if key not in self._published:
                        self._published[key] = RDMABuffer(
                            self._local_view(c_addr, c_size)
                        )

        self._in_ctx(_publish)
        logger.info(
            "MonarchEngine %s: published %d RDMA regions (%d requested, chunk=%d B)",
            self._session,
            len(self._published),
            len(regions),
            self._chunk,
        )

    def publish_payload(self) -> object:
        """What goes into the connect-time metadata exchange: our region -> handle map."""
        return {"session": self._session, "regions": dict(self._published)}

    def attach_peer(self, session: str, payload: object) -> None:
        if not payload:
            return
        self._peer[session] = dict(payload["regions"])  # type: ignore[index]
        logger.debug(
            "MonarchEngine %s: attached %d regions from peer %s",
            self._session,
            len(self._peer[session]),
            session,
        )

    def _peer_buf(self, session: str, addr: int, size: int):
        try:
            return self._peer[session][(int(addr), int(size))]
        except KeyError:
            raise KeyError(
                f"MonarchEngine: peer {session} published no region at {int(addr):#x}+{size}. Every "
                "destination must be declared via publish_regions() at connect."
            ) from None

    # ----------------------------------------------------------------- data path
    def _build_action(self, dst_session: str, src_ptrs, dst_ptrs, sizes):
        from monarch.rdma import RDMAAction

        act = RDMAAction()
        ops: list[
            tuple[int, int, int]
        ] = []  # (src_addr, dst_addr, n), positionally = Monarch's op index
        for s, d, size in zip(src_ptrs, dst_ptrs, sizes):
            s, d, size = int(s), int(d), int(size)
            # Walk source and destination in lockstep through the SAME tiling the publisher used.
            for (d_addr, n), (s_addr, _n) in zip(
                _tile(d, size, self._chunk), _tile(s, size, self._chunk)
            ):
                act.write_remote(
                    self._peer_buf(dst_session, d_addr, n), self._local_view(s_addr, n)
                )
                ops.append((s_addr, d_addr, n))
        return act, ops

    def write(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ) -> None:
        h = self.write_async(dst_session, src_ptrs, dst_ptrs, sizes)
        self.wait([h])

    def write_async(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ):
        """Stage the whole batch into one :class:`RDMAAction` and submit it.

        One action per call, not one per chunk: ``RDMAAction`` exists to dispatch its ops together, which
        is what keeps a multi-MiB transfer from paying per-op latency serially.
        """
        if not src_ptrs:
            return None
        assert len(src_ptrs) == len(dst_ptrs) == len(sizes), (
            f"ptr/size length mismatch: {len(src_ptrs)}/{len(dst_ptrs)}/{len(sizes)}"
        )

        def _submit():
            act, ops = self._build_action(dst_session, src_ptrs, dst_ptrs, sizes)
            return _Submission(act.submit(timeout=300), ops, dst_session)

        # Submitting inside the captured context starts the transfer; the returned Future is completed
        # later by wait(), also inside the context.
        return self._in_ctx(_submit)

    def write_batch(self, items):
        """Every destination's ops in ONE :class:`RDMAAction` — one submit, one Future, one completion.

        An action's ops may target different peers, so a whole round's fan-out fits in a single action.
        See :meth:`RdmaEngine.write_batch` for why this matters (straggler spread, not median bandwidth).
        """
        from monarch.rdma import RDMAAction

        live = [(s, sp, dp, sz) for s, sp, dp, sz in items if sp]
        if not live:
            return []

        def _submit():
            act = RDMAAction()
            ops: list[tuple[int, int, int]] = []
            sessions = []
            for dst_session, src_ptrs, dst_ptrs, sizes in live:
                sessions.append(dst_session)
                for s, d, size in zip(src_ptrs, dst_ptrs, sizes):
                    s, d, size = int(s), int(d), int(size)
                    for (d_addr, n), (s_addr, _n) in zip(
                        _tile(d, size, self._chunk), _tile(s, size, self._chunk)
                    ):
                        act.write_remote(
                            self._peer_buf(dst_session, d_addr, n),
                            self._local_view(s_addr, n),
                        )
                        ops.append((s_addr, d_addr, n))
            return _Submission(
                act.submit(timeout=300), ops, "+".join(sorted(set(sessions)))
            )

        return [self._in_ctx(_submit)]

    def wait(self, handles: Sequence[object]) -> None:
        live = [h for h in handles if h is not None]
        if not live:
            return
        if asyncio._get_running_loop() is not None:
            # Monarch delivers the completion as a message to this actor, which only its event loop can
            # process; blocking that loop here means the Future never resolves. Fail loudly — the silent
            # version of this is an unbounded hang with no error in the log (see docstring point 4).
            raise RuntimeError(
                "MonarchEngine.wait() called on a live asyncio event loop, which would deadlock: the "
                "actor cannot process the RDMA completion while blocked here. Run WeightBridge work on a "
                "dedicated thread off the actor's loop (see examples/workers_monarch.py::_ActorThread)."
            )

        def _get() -> None:
            for h in live:
                try:
                    h.get()  # type: ignore[attr-defined]
                except Exception as e:  # noqa: BLE001 — re-raised, annotated with the offending regions
                    if isinstance(h, _Submission):
                        idx = _failed_op_indices(str(e))
                        raise RuntimeError(
                            f"MonarchEngine.wait: submit failed ({len(idx)} bad ops)\n"
                            f"  bad op indices: {idx}\n"
                            f"{h.describe(idx[:64])}\n{e}"
                        ) from e
                    raise

        self._in_ctx(_get)
