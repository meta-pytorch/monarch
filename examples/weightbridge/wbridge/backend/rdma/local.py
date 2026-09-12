# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""In-process staging "engine": GPU<->CPU copies behind the :class:`RdmaEngine` interface.

WeightBridge's staging modes (sender-staging / receiver-staging) offload the packed wire bytes to CPU
before the RDMA write (SS) or land them in CPU before the GPU load (RS). To keep the four modes a single
parameterized data path — rather than four branches — the GPU<->CPU hop is expressed as *one more*
:class:`RdmaEngine` so every movement in the pipeline is the same ``write_async``/``wait`` protocol,
whether the hop is trainer-GPU->trainer-CPU, CPU->remote, or rollout-CPU->rollout-GPU.

Unlike a real transport, a local copy needs the *tensors* (torch ``.copy_``), not just raw addresses. So
callers register buffers with :meth:`register_buffer` (which keeps the tensor); :meth:`write`/:meth:`write_async`
reconstruct contiguous ``uint8`` views from the raw ``src_ptrs``/``dst_ptrs`` via an interval map (mirrors
:meth:`DualMooncakeEngine._nvl_covers`). Copies run on a private CUDA stream and ``write_async`` returns a
recorded :class:`torch.cuda.Event`; ``wait`` synchronizes it. A ``torch.cuda.Event`` recorded on one thread
may be waited from another, which is exactly the sender/receiver adapter-thread handoff.

On a host without CUDA (offline unit tests on the login node) the copies run synchronously and
``write_async`` returns ``None`` (the ABC's no-op-handle convention), so register/classify/copy logic is
testable with CPU tensors.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import torch
from wbridge.backend.rdma.base import RdmaEngine

logger = logging.getLogger(__name__)


class LocalStagingEngine(RdmaEngine):
    """Single-process loopback engine: ``write`` is a local ``cudaMemcpyAsync`` (GPU<->CPU / D2D)."""

    def __init__(self) -> None:
        self._session = ""
        self._stream: torch.cuda.Stream | None = None
        # (base_ptr, end_ptr, flat uint8 tensor) for every register_buffer'd buffer.
        self._bufs: list[tuple[int, int, torch.Tensor]] = []

    def init(
        self,
        local_host: str,
        protocol: str,
        device: str = "",
        pin_local_nic: bool = False,
    ) -> None:
        # pin_local_nic is a NIC-level knob (see MooncakeEngine); the local GPU<->CPU copy engine has no NIC.
        self._session = f"local://{id(self):#x}"
        self._stream = torch.cuda.Stream() if torch.cuda.is_available() else None

    def session_id(self) -> str:
        return self._session or f"local://{id(self):#x}"

    def register_buffer(self, t: torch.Tensor) -> None:
        """Register a contiguous 1-D ``uint8`` buffer so its bytes can be a staging src/dst.

        This is the real registration for local staging (it keeps the tensor for ``.copy_``); the ABC's
        :meth:`register` is a no-op because local memory needs no pinning for a torch copy.
        """
        assert t.dtype == torch.uint8 and t.dim() == 1 and t.is_contiguous(), (
            "LocalStagingEngine expects a contiguous 1-D uint8 buffer"
        )
        base = t.data_ptr()
        self._bufs.append((base, base + t.numel(), t))

    def register(self, ptr: int, size: int, is_flag: bool = False, tensor=None) -> None:
        # Local memory needs no RDMA pinning; buffers are supplied via register_buffer (which keeps the
        # tensor). A no-op keeps LocalStagingEngine drop-in wherever engine.register(ptr, size) is called.
        # is_flag is a NIC-routing hint (see DualMooncakeEngine); the local copy engine has no NIC.
        return

    def _view(self, ptr: int, size: int) -> torch.Tensor:
        p, n = int(ptr), int(size)
        for base, end, t in self._bufs:
            if base <= p and p + n <= end:
                off = p - base
                return t[off : off + n]
        raise KeyError(
            f"LocalStagingEngine: ptr {p:#x} size {n} not covered by any register_buffer"
        )

    def write(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ) -> None:
        self.wait([self.write_async(dst_session, src_ptrs, dst_ptrs, sizes)])

    def write_async(
        self,
        dst_session: str,
        src_ptrs: Sequence[int],
        dst_ptrs: Sequence[int],
        sizes: Sequence[int],
    ) -> object | None:
        if not src_ptrs:
            return None
        if self._stream is None:  # no CUDA (offline): synchronous copy, no-op handle
            for s, d, n in zip(src_ptrs, dst_ptrs, sizes):
                self._view(d, n).copy_(self._view(s, n))
            return None
        # Order after whatever produced the source (pack / RDMA landing) on the caller's current stream,
        # then copy on the private stream so the returned event is a precise "staging done" marker.
        self._stream.wait_stream(torch.cuda.current_stream())
        with torch.cuda.stream(self._stream):
            for s, d, n in zip(src_ptrs, dst_ptrs, sizes):
                self._view(d, n).copy_(self._view(s, n), non_blocking=True)
            ev = torch.cuda.Event()
            ev.record()
        return ev

    def wait(self, handles: Sequence[object]) -> None:
        for h in handles:
            if h is not None:
                h.synchronize()  # torch.cuda.Event

    def close(self) -> None:
        self._bufs.clear()
        self._stream = None
