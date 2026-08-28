# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Dual-engine facade routing each write to NVLink or the configured Mooncake network transport.

Mooncake selects a transport by the *target segment's protocol*, and the high-bandwidth intra-node NVLink
transport (``nvlink_intra``, CUDA-IPC / GPU-P2P) only works between GPUs on the SAME node, while the
configured network transport handles cross-node peers. To exploit NVLink wherever peers happen to be
co-located — without assuming any particular placement — each rank runs a ``nvlink_intra`` engine and one
or two network engines, and this facade picks per destination:

  * same-node peer AND a device (NVLink-registered) source   -> NVLink engine
  * a tiny host-pinned FLAG buffer                            -> the network *flag* engine
  * otherwise (cross-node bulk, device or host)              -> the network *bulk* engine

Why two network engines under EFA staging: host RDMA is pinned to one NIC per rank for bandwidth (see
``MooncakeEngine._local_gpu_nic``). But the latency-bound same-node flag ping-pong must not ride the same NIC
as bulk — a saturating multi-GiB bulk write starves the flag's connection handshake on that one NIC, which
surfaced as ``batch_transfer_sync_write rc=-1`` ("malformed handshake") and an AGH deadlock. So when pinning,
bulk takes the switch's primary NIC (index 0) and flags a secondary NIC (index 1) — same switch, disjoint
from every rank's bulk NIC. Without a pin (the both-OFF GPUDirect path), one network engine carries both.

Full registration overlap holds for **device** memory (registered in the bulk network engine AND NVLink, same
``data_ptr`` as the one-sided-write destination on either); **host-pinned** memory cannot register with the
NVLink transport (CUDA IPC needs device memory), so those transfers use the configured network transport —
flags through the flag engine and staging arenas through the bulk engine.

NOTE on the NVLink engine's role: same-node **bulk** normally never reaches this facade at all. Co-located
trainer->rollout and rollout<->rollout pairs move their bytes with a direct CUDA-IPC copy that skips Mooncake
entirely (``router.SAME_NODE_IPC``); the ``nvlink_intra`` engine here is the fallback for the cases that
bypass can't cover — a staging endpoint (host DRAM is not IPC-mappable), an allocator that cannot export IPC
handles, or ``WBRIDGE_SAME_NODE_IPC=0``. Consequently a Mooncake wheel without the nvlink_intra patch is not
fatal: :meth:`DualMooncakeEngine.init` logs and leaves ``_nvl`` as ``None``, and those writes take the
configured network transport.

Because the routing lives entirely here, the sender/receiver/router code is unchanged: it holds one engine,
calls ``register``/``write``/``write_async``/``wait`` as before (marking flag buffers with ``is_flag=True``),
and passes around the composite :meth:`session_id`. This is the sole RDMA engine (``router._init_engine``);
it transparently uses the configured network transport for every cross-node peer.
"""

from __future__ import annotations

import logging
from typing import Sequence

from wbridge.backend.rdma.base import RdmaEngine
from wbridge.backend.rdma.mooncake import MooncakeEngine

logger = logging.getLogger(__name__)

_SEP = "|"  # composite session = "<network_flags>|<nvl>|<network_bulk>"; ip:port tokens never contain '|'


class DualMooncakeEngine(RdmaEngine):
    """Route writes over same-node NVLink or the configured Mooncake network engines."""

    def __init__(self) -> None:
        self._efa = MooncakeEngine()  # flags always; also bulk when not split (no pin)
        self._efa_bulk: MooncakeEngine = (
            self._efa
        )  # separate PINNED bulk engine when staging (set in init)
        self._nvl: MooncakeEngine | None = (
            MooncakeEngine()
        )  # None if the wheel has no nvlink_intra
        self._ip = ""
        self._nvl_ranges: list[
            tuple[int, int]
        ] = []  # (base, base+size) device regions registered in nvl
        self._flag_ranges: list[
            tuple[int, int]
        ] = []  # host flag buffers -> routed over the un-pinned _efa

    def init(
        self,
        local_host: str,
        protocol: str,
        device: str = "",
        pin_local_nic: bool = False,
    ) -> None:
        self._ip = local_host
        if pin_local_nic:
            # Split: bulk on the switch's primary NIC (index 0), flags on a secondary NIC (index 1) so a
            # saturating bulk write can't starve a same-node flag handshake on the same NIC.
            self._efa.init(
                local_host, protocol, device, pin_local_nic=True, nic_index=1
            )  # flags
            self._efa_bulk = MooncakeEngine()
            self._efa_bulk.init(
                local_host, protocol, device, pin_local_nic=True, nic_index=0
            )  # bulk data
        else:
            # Both-OFF (GPUDirect) path: one network engine carries flags and bulk.
            self._efa.init(local_host, protocol, device)
            self._efa_bulk = self._efa
        # NVLink is same-node only; it keys off the buffer's GPU, so no NIC device string / no pin. This
        # transport is now a FALLBACK: same-node bulk normally bypasses Mooncake altogether via the direct
        # CUDA-IPC pull (``router.SAME_NODE_IPC``), and only lands here when that is off or unavailable
        # (staging, a non-exportable allocator). So a wheel without the nvlink_intra patch is no longer fatal
        # — we log it and route every same-node write that does reach us over the network transport instead.
        try:
            self._nvl.init(local_host, "nvlink_intra", "")
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "DualMooncakeEngine: nvlink_intra transport unavailable (%s); same-node writes "
                "that do not take the CUDA-IPC bypass will use the network transport",
                e,
            )
            self._nvl = None
        logger.info(
            "DualMooncakeEngine up: network(flags)=%s network_bulk=%s nvl=%s",
            self._efa.session_id(),
            self._efa_bulk.session_id(),
            self._nvl.session_id() if self._nvl is not None else "-",
        )

    def session_id(self) -> str:
        # "<network_flags>|<nvl>|<network_bulk>"; bulk == flags when not split (peers parse three tokens).
        # An empty nvl token means "no NVLink transport here" — _use_nvl already treats that as cross-node.
        nvl = self._nvl.session_id() if self._nvl is not None else ""
        return f"{self._efa.session_id()}{_SEP}{nvl}{_SEP}{self._efa_bulk.session_id()}"

    def bulk_numa_node(self) -> int:
        """NUMA node of the bulk NIC, so callers can allocate host staging buffers NUMA-local to it. -1 if
        unpinned/unknown."""
        return self._efa_bulk.pinned_numa_node()

    @staticmethod
    def _split(session: str) -> tuple[str, str, str]:
        """Return ``(network_flags, nvl, network_bulk)`` from a composite or legacy plain session."""
        parts = session.split(_SEP)
        efa = parts[0]
        nvl = parts[1] if len(parts) > 1 else ""
        bulk = parts[2] if len(parts) > 2 else efa
        return efa, nvl, bulk

    @staticmethod
    def _ip_of(session: str) -> str:
        return session.rsplit(":", 1)[0]

    def register(self, ptr: int, size: int, is_flag: bool = False, tensor=None) -> None:
        if is_flag:
            # Tiny host sync buffer -> the un-pinned flag engine ONLY (kept off the bandwidth-pinned bulk NIC).
            self._efa.register(ptr, size, is_flag=True)
            self._flag_ranges.append((int(ptr), int(ptr) + int(size)))
            return
        # Bulk: the bulk engine handles cross-node landing/reads over the pinned NIC. Device memory ALSO
        # registers in NVLink (same-node peers); host-pinned bulk cannot, so it stays on the network engine.
        self._efa_bulk.register(ptr, size)
        if self._nvl is None:
            return  # no NVLink transport: everything routes to the network engine below
        try:
            self._nvl.register(ptr, size)
            self._nvl_ranges.append((int(ptr), int(ptr) + int(size)))
        except RuntimeError:
            pass  # host-pinned bulk buffer: network-only, routed to the bulk engine below

    def _nvl_covers(self, ptr: int, size: int) -> bool:
        p, end = int(ptr), int(ptr) + int(size)
        return any(base <= p and end <= lim for base, lim in self._nvl_ranges)

    def _in_flag_range(self, ptr: int) -> bool:
        p = int(ptr)
        return any(base <= p < end for base, end in self._flag_ranges)

    def _use_nvl(self, peer_session: str, src_ptrs, sizes) -> bool:
        if self._nvl is None:
            return False  # no local NVLink transport (unpatched wheel)
        efa_sess, nvl_sess, _bulk = self._split(peer_session)
        if not nvl_sess or self._ip_of(efa_sess) != self._ip:
            return False  # cross-node peer (or peer has no nvl engine)
        # All sources must be NVLink-registered device memory (excludes the host flag buffers).
        return all(self._nvl_covers(s, z) for s, z in zip(src_ptrs, sizes))

    def _target(self, peer_session: str, src_ptrs, sizes):
        efa_sess, nvl_sess, bulk_sess = self._split(peer_session)
        # Flags (never batched with bulk) -> un-pinned flag engine.
        if any(self._in_flag_range(s) for s in src_ptrs):
            return self._efa, efa_sess
        # Same-node device bulk -> NVLink.
        if self._use_nvl(peer_session, src_ptrs, sizes):
            return self._nvl, nvl_sess
        # Cross-node bulk (device or host) -> pinned bulk engine.
        return self._efa_bulk, bulk_sess

    def write(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ) -> None:
        eng, sess = self._target(dst_session, src_ptrs, sizes)
        eng.write(sess, src_ptrs, dst_ptrs, sizes)

    def write_async(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ):
        eng, sess = self._target(dst_session, src_ptrs, sizes)
        bid = eng.write_async(sess, src_ptrs, dst_ptrs, sizes)
        return None if bid is None else (eng, bid)

    def wait(self, handles: Sequence[object]) -> None:
        groups: dict[int, tuple[RdmaEngine, list]] = {}
        for h in handles:
            if h is None:
                continue
            eng, bid = h  # type: ignore[misc]
            groups.setdefault(id(eng), (eng, []))[1].append(bid)
        for eng, bids in groups.values():
            eng.wait(bids)

    def close(self) -> None:
        self._efa.close()
        if self._efa_bulk is not self._efa:
            self._efa_bulk.close()
        if self._nvl is not None:
            self._nvl.close()
