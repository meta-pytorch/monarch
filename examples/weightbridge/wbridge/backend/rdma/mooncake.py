# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Mooncake backend for :class:`~wbridge.backend.rdma.base.RdmaEngine`.

Wraps ``mooncake.engine.TransferEngine`` for engine initialization, local-memory registration,
synchronous batched writes, and rollout-side session discovery. Mooncake's return-code conventions are
``initialize``/``register_memory`` using ``!= 0`` for error and ``batch_transfer_sync_write`` using
``< 0`` for error.

Protocols:

* ``"tcp"`` — TCP transport (the local toy). Stock Mooncake auto-installs an RDMA (RC-QP) transport that
  fails on EFA, so :meth:`MooncakeEngine.init` forces ``MC_FORCE_TCP=1``.
* ``"efa"`` — AWS EFA transport. Requires Mooncake built with ``-DUSE_EFA=ON``, a compatible system
  ``libfabric``/``libefa`` visible to the dynamic linker, and ``FI_EFA_ENABLE_SHM_TRANSFER=0``. The
  installation and loader paths belong to the deployment environment rather than this library.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

from wbridge.backend.rdma.base import RdmaEngine

logger = logging.getLogger(__name__)

# Mooncake's built-in peer-to-peer metadata handshake (no external etcd/redis metadata server).
_METADATA_MODE = "P2PHANDSHAKE"


class MooncakeEngine(RdmaEngine):
    """:class:`RdmaEngine` backed by ``mooncake.engine.TransferEngine``."""

    def __init__(self) -> None:
        self._te = None
        self._session = ""
        self._regions: list[int] = []  # registered base ptrs, for unregister on close
        self._pinned_nic = (
            ""  # NIC this engine pinned to (for NUMA-local staging alloc); "" if none
        )
        # EFA round-robins one NIC per transfer request, so a single large write can ride only one path.
        # Striping each write into contiguous sub-transfers (see _stripe) lets the engine spread them across
        # available NICs. The default 16 MiB was selected empirically and remains configurable; 0 disables.
        # WBRIDGE_-prefixed (not MC_) on purpose: wbridge reads this itself, HERE — it is NOT consumed by the
        # Mooncake/libfabric C layer (unlike MC_PATH_ROUNDROBIN / MC_NUM_QP_PER_EP, which that layer reads).
        self._subslice = int(
            os.environ.get("WBRIDGE_EFA_SUBSLICE_BYTES", str(16 * 1024 * 1024))
        )

    def init(
        self,
        local_host: str,
        protocol: str,
        device: str = "",
        pin_local_nic: bool = False,
        nic_index: int = 0,
    ) -> None:
        # Nudge transport env vars before importing the extension (``setdefault`` so explicit overrides
        # win). See the module docstring for the EFA deployment requirements not settable from here
        # (EFA wheel + libfabric symlink).
        if protocol == "tcp":
            # Stock Mooncake auto-installs an RC-QP RDMA transport that fails on EFA ("Failed to create
            # QP: Operation not supported"); MC_FORCE_TCP=1 keeps it on TCP. Verified on EFA (host+VRAM).
            os.environ.setdefault("MC_FORCE_TCP", "1")
        elif protocol == "efa":
            os.environ.setdefault("FI_EFA_ENABLE_SHM_TRANSFER", "0")
            # Stripe each transfer across the GPU's affinity NICs. Mooncake discovers the available EFA
            # paths; round-robin paths plus multiple QPs per endpoint spread one write across them.
            # Sweep MC_NUM_QP_PER_EP for the target fabric if needed.
            os.environ.setdefault("MC_PATH_ROUNDROBIN", "1")
            os.environ.setdefault("MC_NUM_QP_PER_EP", "4")
            # Host-memory staging: pin this rank to a NIC local to its GPU. Host DRAM has no per-GPU NIC
            # affinity, so leaving every rank on the same first NIC can create a bottleneck. ``nic_index``
            # selects among the local NICs: DualMooncakeEngine uses separate indices for bulk and flags so a
            # saturating bulk write cannot starve a flag. GPUDirect transfers carry their own GPU-to-NIC
            # affinity, so pin only when the caller (a staging endpoint) requests it.
            if pin_local_nic and not device:
                nic = self._local_gpu_nic(nic_index)
                if nic:
                    device = nic
                    logger.info(
                        "MooncakeEngine: pinning host-RDMA to local NIC %s (staging, nic_index=%d)",
                        nic,
                        nic_index,
                    )
                else:
                    logger.warning(
                        "MooncakeEngine: pin_local_nic requested but no local NIC found; "
                        "falling back to default (multi-NIC) selection"
                    )

        self._pinned_nic = (
            device if protocol == "efa" else ""
        )  # a NIC name when pinned; drives NUMA-local alloc

        from mooncake.engine import TransferEngine

        self._te = TransferEngine()
        rc = self._te.initialize(local_host, _METADATA_MODE, protocol, device)
        if rc != 0:
            raise RuntimeError(
                f"Mooncake initialize failed (rc={rc}) host={local_host!r} protocol={protocol!r} device={device!r}"
            )
        self._session = f"{local_host}:{self._te.get_rpc_port()}"
        logger.info(
            "MooncakeEngine up: session=%s protocol=%s", self._session, protocol
        )

    # ----- local NIC selection for host-memory pinning (below the RdmaEngine ABC, like _stripe) -----
    def _local_gpu_nic(self, index: int = 0) -> str:
        """The ``index``-th EFA NIC on the same PCIe switch as this rank's current CUDA device.

        Index 0 selects the first NIC in the GPU's PCIe locality group, and higher indices select the
        remaining local NICs (used to keep the flag channel disjoint from bulk traffic). ``index`` is
        clamped to the discovered group size.
        Returns ``""`` if it can't be determined, in which case the caller leaves ``device`` empty and
        Mooncake falls back to its default selection.
        """
        try:
            import glob

            import torch

            bus = self._gpu_pci_bus(torch.cuda.current_device())
            if bus is None:
                return ""
            groups: dict[
                int, list[str]
            ] = {}  # PCIe-root-domain -> NIC names sharing it
            for d in sorted(glob.glob("/sys/class/infiniband/*")):
                real = os.path.realpath(os.path.join(d, "device"))
                seg = [p for p in real.split("/") if p.startswith("pci0000:")]
                if seg:
                    groups.setdefault(int(seg[0].split(":")[1], 16), []).append(
                        os.path.basename(d)
                    )
            if not groups:
                return ""
            dom = max((r for r in groups if r <= bus), default=min(groups))
            nics = groups.get(dom, [])
            return nics[min(index, len(nics) - 1)] if nics else ""
        except Exception:  # noqa: BLE001 — best-effort; empty -> default multi-NIC selection
            return ""

    @staticmethod
    def _gpu_pci_bus(dev: int) -> "int | None":
        """Physical PCIe bus number (e.g. 0x53) of CUDA device *dev*.

        Uses ``cudaDeviceGetPCIBusId`` (CUDA runtime) so it is CUDA_VISIBLE_DEVICES-safe — SGLang/Ray set
        CVD per worker, so ``current_device()`` is a *visible* index that would NOT match a physical NIC/
        nvidia-smi index; the CUDA API maps it to the real device and returns the physical bus. None on error.
        """
        try:
            import ctypes

            lib = ctypes.CDLL("libcudart.so")
            buf = ctypes.create_string_buffer(64)
            if (
                lib.cudaDeviceGetPCIBusId(buf, ctypes.c_int(64), ctypes.c_int(int(dev)))
                != 0
            ):
                return None
            return int(buf.value.decode().split(":")[1], 16)  # "0000:53:00.0" -> 0x53
        except Exception:  # noqa: BLE001
            return None

    def pinned_numa_node(self) -> int:
        """NUMA node of the NIC this engine pinned to, or -1 if not pinned / unknown.

        Used to allocate the host staging buffers NUMA-local to the NIC that DMAs them — a cross-NUMA
        DRAM<->NIC path materially reduces host-RDMA bandwidth, while NUMA-local first-touch closed most of
        that gap in isolated testing.
        """
        nic = self._pinned_nic
        if not nic:
            return -1
        try:
            return int(open(f"/sys/class/infiniband/{nic}/device/numa_node").read())
        except (OSError, ValueError):
            return -1

    def session_id(self) -> str:
        assert self._session, "MooncakeEngine.init not called"
        return self._session

    def register(self, ptr: int, size: int, is_flag: bool = False, tensor=None) -> None:
        # is_flag is a routing hint for multi-engine backends; a single Mooncake engine registers all the same.
        # tensor is for tensor-based backends (MonarchEngine); Mooncake registers by address.
        assert self._te is not None, "MooncakeEngine.init not called"
        rc = self._te.register_memory(int(ptr), int(size))
        if rc != 0:
            raise RuntimeError(
                f"Mooncake register_memory failed (rc={rc}) ptr={int(ptr):#x} size={size}"
            )
        self._regions.append(int(ptr))

    def _stripe(self, src_ptrs, dst_ptrs, sizes):
        """Split each (src,dst,size) transfer into contiguous sub-transfers of <= ``_subslice`` bytes.

        EFA assigns one NIC per transfer request; many sub-transfers let the engine round-robin them
        across all NICs, which produced a large multi-path throughput gain. Transfers already <= _subslice pass through unchanged, so
        tiny writes (e.g. 8-byte flags) and the TCP toy are unaffected. ``_subslice<=0`` disables.
        """
        ss = self._subslice
        if ss <= 0:
            return (
                [int(p) for p in src_ptrs],
                [int(p) for p in dst_ptrs],
                [int(s) for s in sizes],
            )
        s2: list[int] = []
        d2: list[int] = []
        z2: list[int] = []
        for s, d, n in zip(src_ptrs, dst_ptrs, sizes):
            s, d, n = int(s), int(d), int(n)
            off = 0
            while off < n:
                c = ss if n - off > ss else n - off
                s2.append(s + off)
                d2.append(d + off)
                z2.append(c)
                off += c
        return s2, d2, z2

    def write(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ) -> None:
        assert self._te is not None, "MooncakeEngine.init not called"
        if not src_ptrs:
            return
        assert len(src_ptrs) == len(dst_ptrs) == len(sizes), (
            f"ptr/size length mismatch: {len(src_ptrs)}/{len(dst_ptrs)}/{len(sizes)}"
        )
        s2, d2, z2 = self._stripe(src_ptrs, dst_ptrs, sizes)
        rc = self._te.batch_transfer_sync_write(dst_session, s2, d2, z2)
        if rc < 0:
            raise RuntimeError(
                f"Mooncake batch_transfer_sync_write failed (rc={rc}) dst_session={dst_session!r}"
            )

    def write_async(self, dst_session, src_ptrs, dst_ptrs, sizes):
        """Submit a non-blocking batch write; returns a batch handle to pass to :meth:`wait`.

        ``batch_transfer_async_write`` returns a batch id promptly; the data lands later. This lets the
        sender submit writes to all peers concurrently and
        :meth:`wait` on all handles at once. Each transfer is striped into sub-transfers (:meth:`_stripe`)
        so a single large write fans out across all EFA NICs instead of riding one.
        """
        assert self._te is not None, "MooncakeEngine.init not called"
        if not src_ptrs:
            return None
        s2, d2, z2 = self._stripe(src_ptrs, dst_ptrs, sizes)
        bid = self._te.batch_transfer_async_write(dst_session, s2, d2, z2)
        if bid < 0:
            raise RuntimeError(
                f"Mooncake batch_transfer_async_write failed (rc={bid}) dst_session={dst_session!r}"
            )
        return bid

    def wait(self, handles) -> None:
        """Block until all *handles* (batch ids from :meth:`write_async`) have completed.

        ``get_batch_transfer_status(ids)`` blocks until every batch in the list finishes and returns 0 on
        success; focused testing confirmed that it does not return before the transfer lands.
        """
        assert self._te is not None, "MooncakeEngine.init not called"
        ids = [int(h) for h in handles if h is not None]
        if not ids:
            return
        rc = self._te.get_batch_transfer_status(ids)
        if rc != 0:
            raise RuntimeError(
                f"Mooncake get_batch_transfer_status failed (rc={rc}) for {len(ids)} batches"
            )

    def close(self) -> None:
        te = self._te
        if te is None:
            return
        for ptr in self._regions:
            try:
                te.unregister_memory(ptr)
            except Exception:  # best-effort teardown
                logger.debug(
                    "unregister_memory(%#x) failed during close", ptr, exc_info=True
                )
        self._regions.clear()
        self._te = None
        self._session = ""
