# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Abstract one-sided RDMA transport for the WeightBridge data plane.

WeightBridge transfers weights with **exactly two RDMA primitives** — ``register`` (pin a local GPU or
CPU-pinned buffer so a peer may write into it) and ``write`` (a blocking one-sided write into a peer's
*already-registered* buffer). There is deliberately no send/recv or notify primitive: "finish" and "ack"
signals are themselves tiny :meth:`RdmaEngine.write` calls into a flag buffer the peer polls.

Concrete implementations include :class:`~wbridge.backend.rdma.mooncake.MooncakeEngine` and
:class:`~wbridge.backend.rdma.monarch.MonarchEngine`. Composite and local-staging implementations use
the same interface, keeping backend selection out of the data-plane protocol.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence


class RdmaEngine(ABC):
    """One-sided RDMA transport: ``register`` + ``write`` plus lifecycle.

    Lifecycle: :meth:`init` once, then :meth:`register` every local buffer a peer will write into (and
    every local buffer used as a write *source*), exchange :meth:`session_id` and registered addresses
    out of band (WeightBridge uses a one-time Gloo ``all_gather_object``), then :meth:`write` repeatedly,
    and :meth:`close` at teardown.
    """

    @abstractmethod
    def init(
        self,
        local_host: str,
        protocol: str,
        device: str = "",
        pin_local_nic: bool = False,
    ) -> None:
        """Bring up the transport on ``local_host``.

        ``protocol`` selects the transport (``"tcp"`` for the local toy, ``"rdma"`` for RDMA fabrics).
        ``device`` is an optional device filter passed through to the backend. ``pin_local_nic`` asks a
        host-RDMA backend to pin this rank to its GPU's local NIC (staging bandwidth fix; see
        :meth:`MooncakeEngine._local_gpu_nic`) — backends without a NIC notion ignore it.
        """

    @abstractmethod
    def session_id(self) -> str:
        """Return this engine's session id (``"host:port"``); peers pass it to :meth:`write`."""

    @abstractmethod
    def register(
        self, ptr: int, size: int, is_flag: bool = False, tensor: "object | None" = None
    ) -> None:
        """Pin a local buffer at ``ptr`` (a ``tensor.data_ptr()``) of ``size`` bytes for RDMA.

        The buffer may be GPU (VRAM) or CPU-pinned; the backend classifies it. Raises on failure.
        ``is_flag`` marks the tiny host-pinned sync buffers (the ``_write_flag`` ping-pong): a multi-engine
        backend may route these over a *separate* transport from bulk data (see :class:`DualMooncakeEngine`,
        which keeps flags off the bandwidth-pinned NIC so a saturating bulk write can't starve a flag). Single-
        engine backends ignore it.

        ``tensor`` is the owning tensor, passed by callers that have it. Pointer-based backends (Mooncake)
        ignore it; :class:`~wbridge.backend.rdma.monarch.MonarchEngine` requires it, because Monarch's API
        registers tensors/memoryviews rather than raw addresses.
        """

    # ----- optional connect-time metadata exchange (default: nothing to exchange) -----
    def publish_regions(self, regions: "Sequence[tuple[int, int]]") -> None:
        """Declare every ``(addr, size)`` a peer may write into, before the metadata exchange.

        Backends whose remote addressing is a raw pointer (Mooncake) need nothing here. Monarch's
        ``RDMABuffer`` has no remote offset, so each destination region must be exported as its own
        handle; this is where the router hands over the exact list.
        """
        return

    def publish_payload(self) -> object | None:
        """Backend-specific blob to include in the connect-time ``all_gather_object``, or ``None``."""
        return None

    def attach_peer(self, session: str, payload: object) -> None:
        """Receive a peer's :meth:`publish_payload` after the gather. Default: ignore."""
        return

    @abstractmethod
    def write(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ) -> None:
        """Blocking one-sided batch write of local ``src_ptrs`` into remote ``dst_ptrs`` on ``dst_session``.

        ``src_ptrs[i]`` (local, registered) is copied into ``dst_ptrs[i]`` (remote, registered by that
        peer) for ``sizes[i]`` bytes. Returns only after every write has landed remotely (synchronous),
        which is what lets a subsequent flag write safely signal "data arrived". Raises on failure.
        """

    # ----- optional async write (default: fall back to the blocking write) -----
    def write_async(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ) -> object | None:
        """Submit a **non-blocking** one-sided batch write; return an opaque handle for :meth:`wait`.

        Same semantics as :meth:`write` except it returns immediately (data has *not* landed yet), so a
        caller can submit writes to many peers concurrently and then :meth:`wait` on all handles at once —
        hiding per-write latency. The default performs the blocking :meth:`write` and returns ``None`` so
        backends without an async path (and test loopbacks) keep working unchanged.

        The handle is opaque and backend-specific (a Mooncake batch id, a ``(engine, id)`` tuple, or a
        :class:`torch.cuda.Event` for :class:`~wbridge.backend.rdma.local.LocalStagingEngine`). Handles must
        be passed to :meth:`wait` on the **same engine** that produced them — do not mix engines in one
        ``wait`` list.
        """
        self.write(dst_session, src_ptrs, dst_ptrs, sizes)
        return None

    def write_batch(
        self, items: Sequence[tuple[str, Sequence[int], Sequence[int], Sequence[int]]]
    ):
        """Submit writes to *several* destinations as one unit; return handles for :meth:`wait`.

        Semantically identical to calling :meth:`write_async` per item, and that is exactly the default —
        backends with no notion of a cross-destination batch keep their existing behaviour. It exists
        because a backend can be much faster when it knows the whole round up front: Monarch's completion
        is an actor message per submitted action, and issuing one action per peer leaves the per-peer
        completions to be observed one after another, which shows up as a wide straggler spread. Focused
        testing found little change in median throughput but a material improvement for the slowest rank
        when the peers shared a single action.

        The progressive sender path deliberately uses :meth:`write_async` per destination: each receiver's
        slot-reuse ACK independently enables its write, and its DATA-ready flag must follow that exact
        completion. ``write_batch`` remains available to synchronized callers/backends for which a whole
        fan-out really does share one dependency and one completion boundary. The control plane likewise
        keeps :meth:`write_async` per flag.
        """
        return [self.write_async(s, sp, dp, sz) for s, sp, dp, sz in items]

    def wait(self, handles: Sequence[object]) -> None:
        """Block until every handle returned by :meth:`write_async` has completed. ``None`` handles are
        no-ops (already landed via the default sync fallback). Raises on failure.

        Calls for disjoint handles may run concurrently. Receiver external exchange and trainer bulk sends
        use that property to publish each destination's READY/DATA-ready independently instead of waiting
        for the round's slowest peer.
        """
        return

    @abstractmethod
    def close(self) -> None:
        """Unregister buffers and tear down the transport. Idempotent."""
