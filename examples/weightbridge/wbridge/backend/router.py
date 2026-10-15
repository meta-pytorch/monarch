# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Multi-round P2P routing plans under per-rank byte caps."""

from __future__ import annotations

import contextlib
import ctypes
import json
import logging
import mmap
import os
import queue
import struct
import tempfile
import threading
import time
from typing import Callable, TypeAlias

import torch
import torch.distributed as dist
from torch.multiprocessing.reductions import reduce_tensor
from wbridge.backend.gantt import span as gantt_span
from wbridge.backend.rdma import DualMooncakeEngine, LocalStagingEngine, RdmaEngine
from wbridge.backend.tcp_control import TcpControlTransport
from wbridge.utils.data import (
    batched_copy,
    CopyPlan,
    FuseUnsupported,
    LoadSpec,
    Shards,
    shards_nbytes,
    shards_numel,
    ShardSpec,
    split_shard_evenly,
    validate_logical_tensor_partitions,
)
from wbridge.utils.distributed import get_local_ip, init_custom_process_group

# Per-round cap for total bytes a receiver may take (sum over all senders). Env-overridable so we can
# trade round count (fewer rounds = fewer, larger RDMA writes = less per-write fixed latency) against the
# double-buffered wire-buffer memory (which grows with round size).
RECEIVER_ROUND_CAP_BYTES = int(
    os.environ.get("WBRIDGE_ROUND_CAP_BYTES") or str(2 * 1024**3)
)

# Same-node bulk data plane: bypass the network RDMA engine and move the bytes with a direct CUDA-IPC copy
# over NVLink. This is the SAME mechanism the receiver<->receiver dedup exchange already uses for co-located
# class peers; setting it here unifies both bulk legs. Same-node replica ready/consumed sequences use shared
# memory, while other tiny flags still use the selected backend. Set WBRIDGE_SAME_NODE_IPC=0 to force bulk
# back through that backend (A/B measurement, or if a peer's allocator cannot export IPC handles).
SAME_NODE_IPC = os.environ.get("WBRIDGE_SAME_NODE_IPC", "1") == "1"

# Bulk (weight) bytes by transport since connect, split by leg — see :meth:`WBEndpoint.transport_stats`.
# ``ipc`` = direct CUDA-IPC copy over NVLink; ``rdma`` = the selected :class:`RdmaEngine` data path.
# Cross-node flags ride that backend; CUDA-IPC replica peers use a
# node-local shared sequence bank so their control path does not dominate small payloads.
_EMPTY_TSTATS: dict = {
    "wire_ipc_bytes": 0,  # trainer->rollout leg (sender: written; receiver: landed)
    "wire_rdma_bytes": 0,
    "agh_ipc_bytes": 0,  # rollout<->rollout dedup exchange leg (receivers only)
    "agh_rdma_bytes": 0,
}

logger = logging.getLogger(__name__)


def _hbm_debug(stage: str, **fields) -> None:
    """Emit an opt-in allocator/device snapshot without perturbing normal runs."""
    mode = os.environ.get("WBRIDGE_HBM_DEBUG", "").strip().lower()
    if mode not in {"1", "true", "sync"} or not torch.cuda.is_available():
        return
    if mode == "sync":
        torch.cuda.synchronize()
    free_bytes, total_bytes = torch.cuda.mem_get_info()
    record = {
        "stage": stage,
        "pid": os.getpid(),
        "device": torch.cuda.current_device(),
        "free_bytes": free_bytes,
        "total_bytes": total_bytes,
        "allocated_bytes": torch.cuda.memory_allocated(),
        "reserved_bytes": torch.cuda.memory_reserved(),
        **fields,
    }
    print(
        "WBHBM " + json.dumps(record, sort_keys=True, separators=(",", ":")), flush=True
    )


def _ipc_event_readers(
    peers: list[int],
    peer_ip: dict[int, str],
    local_ip: str,
    *,
    enabled: bool,
) -> list[int]:
    """Return only peers able to import this process's CUDA-IPC events."""
    if not enabled:
        return []
    return [peer for peer in peers if peer_ip.get(peer) == local_ip]


def _relay_round_predecessors(
    active_rounds: list[int] | tuple[int, ...],
    depth: int = 2,
) -> dict[int, tuple[int | None, int | None]]:
    """Return ``round -> (previous operation, previous parity use)``.

    Relay buffers are depth two, but adjacent rounds use different parities and can otherwise run far ahead
    of one another.  The first predecessor keeps one operation stream ordered for a replica group/edge; the
    second is the exact lifetime fence for the physical parity slot.  Missing rounds are skipped, so a sparse
    group depends on its previous *actual* operation and its previous actual use of that parity.
    """
    if depth < 1:
        raise ValueError(f"relay buffer depth must be positive, got {depth}")
    previous_operation: int | None = None
    previous_parity: dict[int, int] = {}
    dependencies: dict[int, tuple[int | None, int | None]] = {}
    for ri in active_rounds:
        if ri in dependencies:
            raise ValueError(f"duplicate relay round {ri}")
        parity = ri % depth
        dependencies[ri] = (previous_operation, previous_parity.get(parity))
        previous_operation = ri
        previous_parity[parity] = ri
    return dependencies


def _enable_cuda_peer_access(dst_device: int, src_device: int) -> None:
    """Enable SM-issued loads from ``src_device`` while executing on ``dst_device``."""
    if dst_device == src_device:
        return
    if not torch.cuda.can_device_access_peer(dst_device, src_device):
        raise RuntimeError(
            f"CUDA device {dst_device} cannot access peer device {src_device}"
        )
    try:
        from cuda.bindings import runtime as cudart
    except (
        ImportError
    ) as exc:  # pragma: no cover - cuda-python is a required runtime dependency
        raise RuntimeError(
            "fused internal consume requires cuda-python for CUDA peer mapping"
        ) from exc
    with torch.cuda.device(dst_device):
        error = cudart.cudaDeviceEnablePeerAccess(src_device, 0)[0]
    allowed = {
        cudart.cudaError_t.cudaSuccess,
        cudart.cudaError_t.cudaErrorPeerAccessAlreadyEnabled,
    }
    if error not in allowed:
        raise RuntimeError(
            f"cudaDeviceEnablePeerAccess({src_device}) on device {dst_device} failed: {error}"
        )


def _open_cuda_ipc_mapping(dst_device: int, metadata: dict) -> tuple[int, int]:
    """Open an exported allocation in ``dst_device``'s address space.

    Returns ``(tensor_base, allocation_base)``. The first is the pointer corresponding to the exporter's
    tensor ``data_ptr`` and is used to translate source views; the second must be passed to
    :func:`_close_cuda_ipc_mapping`. PyTorch's normal tensor rebuild opens on the exporter's device, which is
    suitable for ``cudaMemcpyPeerAsync`` but is not a dereferenceable address for a destination-device
    kernel.
    """
    try:
        from cuda.bindings import runtime as cudart
    except (
        ImportError
    ) as exc:  # pragma: no cover - cuda-python is a required runtime dependency
        raise RuntimeError(
            "fused internal consume requires cuda-python for destination-side IPC mapping"
        ) from exc

    raw = bytes(metadata["handle"])
    if len(raw) != 64:
        raise RuntimeError(f"invalid CUDA IPC memory handle size {len(raw)}")
    handle = cudart.cudaIpcMemHandle_t()
    handle.reserved = raw
    with torch.cuda.device(dst_device):
        error, device_ptr = cudart.cudaIpcOpenMemHandle(
            handle,
            cudart.cudaIpcMemLazyEnablePeerAccess,
        )
    if error != cudart.cudaError_t.cudaSuccess:
        raise RuntimeError(
            f"cudaIpcOpenMemHandle on device {dst_device} failed: {error}"
        )
    allocation_base = int(device_ptr)
    return allocation_base + int(metadata["tensor_offset_bytes"]), allocation_base


def _close_cuda_ipc_mapping(dst_device: int, allocation_base: int) -> None:
    from cuda.bindings import runtime as cudart

    with torch.cuda.device(dst_device):
        error = cudart.cudaIpcCloseMemHandle(allocation_base)[0]
    if error != cudart.cudaError_t.cudaSuccess:
        raise RuntimeError(
            f"cudaIpcCloseMemHandle on device {dst_device} failed: {error}"
        )


def _cuda_ipc_kernel_metadata(reduced: tuple) -> dict:
    """Return the raw CUDA-IPC allocation metadata needed by a peer-load kernel.

    ``torch.multiprocessing.reductions.reduce_tensor`` is still published alongside this metadata so the
    remote process owns a normal tensor object that keeps the exported allocation alive.  The raw mapping is
    opened separately on the *consumer* GPU: a kernel launched there can then dereference the producer's
    bytes directly instead of first copying them into a local staging tensor.
    """
    _rebuild, reduce_args = reduced
    storage_handle = bytes(reduce_args[7])
    if len(storage_handle) < 64:
        raise RuntimeError(
            f"wbridge: invalid exported CUDA IPC handle size {len(storage_handle)}"
        )
    tensor_itemsize = torch.empty((), dtype=reduce_args[5]).element_size()
    return {
        # PyTorch prefixes its allocator handle with bookkeeping bytes on current releases;
        # cudaIpcOpenMemHandle consumes the trailing cudaIpcMemHandle_t payload itself.
        "handle": storage_handle[-64:],
        "tensor_offset_bytes": int(reduce_args[9])
        + int(reduce_args[3]) * tensor_itemsize,
    }


class _LocalReplFlagBank:
    """Node-local ready/consumed sequences for CUDA-IPC replica peers.

    Bulk same-node exchange already bypasses the network RDMA backend and uses CUDA IPC. Sending its tiny
    host-side rendezvous flags through one RDMA operation per peer is both unnecessary and, for an
    eight-GPU node, much more expensive than the payload. One receiver owns a small mmap-backed bank:

    Each channel contains one producer-ready sequence followed by one consumed sequence per reader. Channel
    zero retains the original whole-column/PREP lifetime. Topology adds one channel per external
    ``grecv[k, parity]`` slot, allowing adjacent rounds to become ready and be released independently.

    The ready sequence does not replace the CUDA IPC event.  The producer records every per-reader event
    before publishing the CPU sequence; the reader first observes that sequence and then enqueues its event
    wait, preserving the existing GPU visibility fence. Cross-node peers continue to use RDMA flags.
    """

    _I64 = struct.Struct("=q")

    def __init__(
        self,
        fd: int,
        path: str,
        view: mmap.mmap,
        *,
        owner: bool,
        slots: int,
        channels: int,
    ) -> None:
        self._fd = fd
        self.path = path
        self._view = view
        self._owner = owner
        self.slots = slots
        self.channels = channels

    @classmethod
    def create(cls, slots: int, *, channels: int = 1) -> "_LocalReplFlagBank":
        assert slots >= 0 and channels >= 1
        shared_dir = os.environ.get("WBRIDGE_LOCAL_SHM_DIR")
        if not shared_dir:
            shared_dir = (
                "/dev/shm" if os.path.isdir("/dev/shm") else tempfile.gettempdir()
            )
        shared_dir = os.path.abspath(os.path.expanduser(shared_dir))
        fd, path = tempfile.mkstemp(prefix="wbridge-repl-", dir=shared_dir)
        size = cls._I64.size * channels * (1 + slots)
        os.ftruncate(fd, size)
        return cls(
            fd,
            path,
            mmap.mmap(fd, size, access=mmap.ACCESS_WRITE),
            owner=True,
            slots=slots,
            channels=channels,
        )

    @classmethod
    def open(
        cls,
        path: str,
        *,
        slots: int | None = None,
        channels: int = 1,
    ) -> "_LocalReplFlagBank":
        fd = os.open(path, os.O_RDWR)
        size = os.fstat(fd).st_size
        if size < cls._I64.size:
            os.close(fd)
            raise RuntimeError(
                f"invalid local replica flag bank {path!r}: {size} bytes"
            )
        words = size // cls._I64.size
        if slots is None:
            if channels != 1:
                os.close(fd)
                raise ValueError(
                    "slots must be provided when opening a multi-channel replica flag bank"
                )
            slots = words - 1
        if words != channels * (1 + slots):
            os.close(fd)
            raise RuntimeError(
                f"replica flag bank shape mismatch {path!r}: words={words}, "
                f"slots={slots}, channels={channels}"
            )
        return cls(
            fd,
            path,
            mmap.mmap(fd, size, access=mmap.ACCESS_WRITE),
            owner=False,
            slots=slots,
            channels=channels,
        )

    def _base(self, channel: int) -> int:
        if not 0 <= channel < self.channels:
            raise IndexError(f"flag channel {channel} outside [0,{self.channels})")
        return self._I64.size * channel * (1 + self.slots)

    def publish_ready(self, seq: int, channel: int = 0) -> None:
        self._I64.pack_into(self._view, self._base(channel), seq)

    def ready(self, channel: int = 0) -> int:
        return self._I64.unpack_from(self._view, self._base(channel))[0]

    def publish_consumed(self, slot: int, seq: int, channel: int = 0) -> None:
        if not 0 <= slot < self.slots:
            raise IndexError(f"reader slot {slot} outside [0,{self.slots})")
        self._I64.pack_into(
            self._view, self._base(channel) + self._I64.size * (1 + slot), seq
        )

    def consumed(self, slot: int, channel: int = 0) -> int:
        if not 0 <= slot < self.slots:
            raise IndexError(f"reader slot {slot} outside [0,{self.slots})")
        return self._I64.unpack_from(
            self._view,
            self._base(channel) + self._I64.size * (1 + slot),
        )[0]

    def close(self) -> None:
        try:
            self._view.close()
        finally:
            try:
                os.close(self._fd)
            finally:
                if self._owner:
                    try:
                        os.unlink(self.path)
                    except FileNotFoundError:
                        pass


CommRoundPlan: TypeAlias = tuple[ShardSpec, dict[int, ShardSpec]]


_EVEN_ROUND_SOFT_HEADROOM_PCT = 5


def _even_round_soft_target(total: int, rounds: int, hard_cap: int) -> int:
    """Per-rank balancing threshold with a little room for indivisible tensor names.

    The target is deliberately softer than ``ceil(total / rounds)``: without headroom, one rank crossing
    that exact average can reject a tiny tensor that still fits every hard cap and create a runt round.
    """
    assert total >= 0 and rounds >= 1 and hard_cap >= 0
    target = -(-total // rounds)
    target = -(-(target * (100 + _EVEN_ROUND_SOFT_HEADROOM_PCT)) // 100)
    return min(target, hard_cap)


def _canonical_dtype_spec(
    dtype_specs: list[dict[str, torch.dtype]],
) -> dict[str, torch.dtype]:
    """Merge per-rank dtype maps into one name-sorted map shared identically by every rank."""
    merged: dict[str, torch.dtype] = {}
    for dtype_spec in dtype_specs:
        for name, dtype in dtype_spec.items():
            if name in merged:
                assert merged[name] == dtype, f"Dtype mismatch for {name}"
            else:
                merged[name] = dtype
    return {name: merged[name] for name in sorted(merged)}


def _arena_slot_offset(off: int, ri: int, depth: int, stride: int) -> int:
    """Absolute byte offset of a per-round region inside a depth-buffered arena slot.

    ``arena_layout``'s RECV and fused-prepare (own/send) offsets are relative to one ``stride``-byte slot.
    The shared ``grecv`` bank is already absolute and must not pass through this helper. Keeping the slot
    arithmetic here prevents a slotted path from silently falling back to parity zero.
    """
    assert depth >= 1 and stride >= 0 and ri >= 0 and off >= 0
    return (ri % depth) * stride + off


def _arena_slot_predecessors(rounds: list[int], depth: int) -> dict[int, int | None]:
    """Map each active round to the previous active round that used the same arena slot.

    Slot ownership follows the *global round number* (``ri % depth``), not position in the filtered list of
    rounds active on one endpoint.  Those differ whenever an endpoint has an empty round; using
    ``rounds[idx - depth]`` then waits for the wrong consumer and can either hang or overwrite live bytes.
    """
    assert depth >= 1
    last: dict[int, int] = {}
    out: dict[int, int | None] = {}
    for ri in rounds:
        assert ri >= 0
        slot = ri % depth
        out[ri] = last.get(slot)
        last[slot] = ri
    return out


def _arena_peer_predecessors(
    rounds: list[dict],
    field: str = "send",
    *,
    depth: int = 1,
) -> list[dict[int, tuple[int, int]]]:
    """For every round/peer write, return the prior generation of its peer/parity GRECV slot.

    The shared ``grecv`` bank has ``depth`` stable destination slots per peer, selected by global round
    parity. Before writing peer ``p`` in round ``ri``, the writer observes the consumed flag for the prior
    use of ``(p, ri % depth)``. A predecessor is ``(epoch_delta, round)``: ``(0, r)`` is an earlier round in
    this epoch, while ``(-1, r)`` wraps the first use of one parity to that parity's final use in the prior
    epoch. The caller skips a negative epoch on epoch zero, when every slot is initially free.

    Peer sets may vary between rounds, so the predecessor is tracked independently for every
    ``(peer, parity)`` rather than inferred as simply ``ri - depth``.
    """
    assert depth >= 1
    active: list[list[int]] = []
    final: dict[tuple[int, int], int] = {}
    for ri, rd in enumerate(rounds):
        peers = [
            peer for peer, (_off, nb) in sorted(rd.get(field, {}).items()) if nb > 0
        ]
        active.append(peers)
        for peer in peers:
            final[(peer, ri % depth)] = ri

    last: dict[tuple[int, int], int] = {}
    out: list[dict[int, tuple[int, int]]] = []
    for ri, peers in enumerate(active):
        pred: dict[int, tuple[int, int]] = {}
        for peer in peers:
            key = (peer, ri % depth)
            pred[peer] = (0, last[key]) if key in last else (-1, final[key])
            last[key] = ri
        out.append(pred)
    return out


def _arena_total_bytes(rounds: list[dict], depth: int, stride: int) -> int:
    """Physical bytes for ``depth`` rollout PREP slots plus depth-buffered ``grecv`` slots."""
    assert depth >= 1 and stride >= 0
    total = depth * stride
    for rd in rounds:
        for off, nb in rd.get("grecv", {}).values():
            assert off >= depth * stride and nb >= 0
            total = max(total, off + nb)
    return total


def _doff_arena_layout(
    rounds: list[dict],
    prep_stride: int,
    depth: int,
) -> tuple[list[dict], int, int]:
    """Build the compact, non-RDMA internal-offload (DOFF) arena.

    A DOFF generation is a shadow of one rollout SEND/PREP slot plus one exclusive region for every direct
    external source.  External payload sizes are intentionally *not* pooled: each source receives its own
    maximum-sized region, so a small source can never be blocked behind or fragmented by a large one.  The
    returned per-round offsets select ``round % depth`` while retaining each round's exact payload length.

    Returns ``(round_layout, total_bytes, generation_stride)``.  ``round_layout[ri]["own"]`` covers the
    complete SEND/PREP slot and ``round_layout[ri]["grecv"][source]`` covers the offloaded copy of that
    source's GRECV payload.
    """
    assert depth >= 1 and prep_stride >= 0
    source_max: dict[int, int] = {}
    for rd in rounds:
        for source, (_off, nb) in rd.get("grecv", {}).items():
            assert nb >= 0
            source_max[source] = max(source_max.get(source, 0), nb)

    source_rel: dict[int, int] = {}
    generation_stride = prep_stride
    for source in sorted(source_max):
        source_rel[source] = generation_stride
        generation_stride += source_max[source]

    out: list[dict] = []
    for ri, rd in enumerate(rounds):
        base = (ri % depth) * generation_stride
        out.append(
            {
                "slot": ri % depth,
                "own": (base, prep_stride),
                "grecv": {
                    source: (base + source_rel[source], nb)
                    for source, (_off, nb) in rd.get("grecv", {}).items()
                },
            }
        )
    return out, depth * generation_stride, generation_stride


def _arena_recv_total_bytes(rounds: list[dict], depth: int) -> int:
    """Physical bytes for the isolated trainer-ingress allocation.

    :meth:`WeightRouter.arena_layout` gives every trainer sender a stable, non-overlapping lane within each
    parity slot and records the common slot stride as ``recv_stride``.  Keeping this allocation separate from
    the rollout PREP/GRECV arena means neither an RDMA-capable trainer nor a same-node trainer pull can touch a
    buffer that rollout peers are concurrently reading.
    """
    assert depth >= 1
    strides = {rd.get("recv_stride", 0) for rd in rounds}
    assert len(strides) <= 1, f"inconsistent RECV strides: {strides}"
    stride = next(iter(strides), 0)
    assert stride >= 0
    for ri, rd in enumerate(rounds):
        for off, nb in rd.get("recv", {}).values():
            assert off >= 0 and nb >= 0 and off + nb <= stride, (ri, off, nb, stride)
    return depth * stride


def _merge_recv_prep_layout(rounds: list[dict], depth: int, prep_stride: int) -> int:
    """Overlay each RECV parity slot with its PREP parity slot and relocate GRECV after them.

    ``arena_layout`` initially addresses PREP at ``depth * prep_stride`` and GRECV immediately after it.
    The merged experimental layout uses a physical slot large enough for either the stable trainer-ingress
    lanes or PREP.  RECV is snapshotted to an epoch-scoped non-RDMA buffer before A+R overwrites the slot.
    Only absolute GRECV offsets need relocation; RECV/PREP offsets stay relative to their physical slot.
    """
    assert depth >= 1 and prep_stride >= 0
    recv_strides = {rd.get("recv_stride", 0) for rd in rounds}
    assert len(recv_strides) <= 1, f"inconsistent RECV strides: {recv_strides}"
    recv_stride = next(iter(recv_strides), 0)
    slot_stride = max(recv_stride, prep_stride)
    shift = depth * (slot_stride - prep_stride)
    if shift:
        for rd in rounds:
            rd["grecv"] = {
                peer: (off + shift, nb)
                for peer, (off, nb) in rd.get("grecv", {}).items()
            }
    return slot_stride


def _numa_cpus(node: int) -> list[int] | None:
    """CPU ids belonging to NUMA `node`, parsed from /sys; None if unavailable."""
    try:
        spec = open(f"/sys/devices/system/node/node{node}/cpulist").read().strip()
    except OSError:
        return None
    cpus: list[int] = []
    for part in spec.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            cpus += list(range(int(a), int(b) + 1))
        else:
            cpus.append(int(part))
    return cpus or None


@contextlib.contextmanager
def _numa_local_alloc(node: int):
    """Temporarily bind this thread to `node`'s CPUs so pinned-host allocations first-touch NUMA-local.

    Host RDMA to/from DRAM that is cross-NUMA from the NIC materially reduces bandwidth; allocating the
    staging buffers on the NIC's NUMA node closed most of the gap in isolated testing. We restore the
    prior affinity on exit so we don't confine the SGLang/Megatron worker threads to one socket. No-op when
    the node is unknown (-1) or its cpulist can't be read (falls back to default placement).
    """
    cpus = _numa_cpus(node) if node is not None and node >= 0 else None
    if not cpus:
        yield
        return
    try:
        prev = os.sched_getaffinity(0)
    except OSError:
        prev = None
    try:
        try:
            os.sched_setaffinity(0, set(cpus))
        except OSError:
            pass
        yield
    finally:
        if prev:
            with contextlib.suppress(OSError):
                os.sched_setaffinity(0, set(prev))


def _shards_canonical_key(shards: Shards):
    """Order-independent hashable identity of a worker's shard set for one tensor."""
    return tuple(sorted(tuple(tuple(dim) for dim in shard) for shard in shards))


def _carve_named(spec: ShardSpec, buf_u8: "torch.Tensor", dtype_spec: dict) -> dict:
    """Present a contiguous uint8 buffer as a per-name logical dict matching ``ShardSpec.make_named_buffer``
    (flat ``shards_numel``-element tensor per name, in spec order). Lets the persistent, RDMA-registered
    all-gather buffers be driven by the existing 2-stage machinery (``setitem_pairs``/``copy_fromto_pairs``)
    with no transient allocation."""
    out: dict = {}
    off = 0
    for name, shards in spec:
        it = dtype_spec[name].itemsize
        nbytes = shards_numel(shards) * it
        out[name] = buf_u8[off : off + nbytes].view(dtype_spec[name])
        off += nbytes
    return out


def _packed_name_regions(
    spec: ShardSpec, dtype_spec: dict[str, torch.dtype]
) -> dict[str, tuple[int, int]]:
    """Byte region of each tensor name inside a buffer packed in ``ShardSpec`` iteration order."""
    out: dict[str, tuple[int, int]] = {}
    off = 0
    for name, shards in spec:
        nb = shards_nbytes(shards, dtype_spec[name])
        out[name] = (off, nb)
        off += nb
    return out


def _packed_copy_spans(
    src_spec: ShardSpec,
    dst_spec: ShardSpec,
    names: tuple[str, ...] | list[str] | set[str],
    dtype_spec: dict[str, torch.dtype],
    *,
    src_base: int = 0,
    dst_base: int = 0,
) -> list[tuple[int, int, int]]:
    """Direct byte-copy spans for whole tensor names between two packed layouts.

    Multi-group topology packs the exact external payload once during assemble+repack, but its destination
    is the stable ``grecv[source]`` layout, which can contain other groups' names between the selected ones.
    Likewise, an internal peer's ``own`` / ``grecv`` layouts can feed several disjoint destinations without
    a repack between the external and internal phases. Adjacent source+destination spans are coalesced.
    """
    src_regions = _packed_name_regions(src_spec, dtype_spec)
    dst_regions = _packed_name_regions(dst_spec, dtype_spec)
    spans: list[tuple[int, int, int]] = []
    for name in sorted(set(names)):
        assert name in src_regions and name in dst_regions, (
            f"missing packed topology name {name}"
        )
        assert src_spec[name] == dst_spec[name], f"topology shard mismatch for {name}"
        so, sn = src_regions[name]
        do, dn = dst_regions[name]
        assert sn == dn
        cur = (src_base + so, dst_base + do, sn)
        if (
            spans
            and spans[-1][0] + spans[-1][2] == cur[0]
            and spans[-1][1] + spans[-1][2] == cur[1]
        ):
            ps, pd, pn = spans[-1]
            spans[-1] = (ps, pd, pn + cur[2])
        else:
            spans.append(cur)
    return spans


def _dedup_specs(specs: list[ShardSpec]) -> list[ShardSpec]:
    """De-replicate a role's specs (per tensor name): workers holding an *identical* shard set form a
    replication class; each shared shard is split evenly along its longest axis so member ``j`` of a
    size-``k`` class is viewed as owning only sub-shard ``j``. The sub-shards partition the class's
    shards. Deterministic (sorted names / classes / members / shard keys) so every rank derives the
    identical reduction — required for the per-pair flag ping-pong to stay in lockstep.
    """
    ns = len(specs)
    reduced: list[dict[str, Shards]] = [{} for _ in range(ns)]
    names: set[str] = set()
    for spec in specs:
        names |= set(spec.entries.keys())
    for name in sorted(names):
        classes: dict = {}  # canonical shard key -> [member ranks], insertion-ordered by rank
        for i in range(ns):
            if name in specs[i].entries:
                classes.setdefault(_shards_canonical_key(specs[i][name]), []).append(i)
        for members in classes.values():
            members.sort()
            k = len(members)
            # Canonical (sorted) shard list so every rank agrees which sub-shard is the j-th.
            canon = sorted(
                (list(shard) for shard in specs[members[0]][name]),
                key=lambda sh: tuple(tuple(dim) for dim in sh),
            )
            for j, i in enumerate(members):
                subs = [
                    ss
                    for shard in canon
                    if (ss := split_shard_evenly(shard, k, j)) is not None
                ]
                if subs:
                    reduced[i][name] = subs
    return [ShardSpec(entries) for entries in reduced]


def dedup_send_specs(send_specs: list[ShardSpec]) -> list[ShardSpec]:
    """Sender-side de-replication: replicated trainers each send only a disjoint sub-slice, so
    ``compute_overlap`` yields no duplicate sends and the receiver reconstructs the shard as the union
    over its sender peers. (Physical model params unchanged — ``_fuse_copy_pairs`` intersects the reduced
    overlap with the full LoadSpec, so each sender packs just its sub-slice.) See :func:`_dedup_specs`."""
    return _dedup_specs(send_specs)


def consolidate_groups(
    nat_classes: dict[str, list[list[int]]],
    class_bytes: dict[tuple[str, tuple[int, ...]], int],
    threshold: float,
) -> dict[str, list[list[int]]]:
    """Consolidate small, widely-replicated exchange groups (opt-in dedup refinement).

    A tensor replicated across ``k`` rollout workers is split k-way + reconstructed by a k-way all-gather;
    for a *small* tensor with *large* ``k`` that all-gather costs many per-pair flag handshakes while
    saving almost no RDMA. This pass repeatedly dissolves a multi-worker group that carries a low-traffic
    receiver<->receiver pair (edge traffic < ``threshold`` bytes) into a PARTITION of smaller pieces —
    each either an EXISTING group (piggyback its already-paid sync edges) or a singleton (direct full send
    from the trainer, no exchange) — trading a little sender RDMA for much less exchange sync.

    Inputs: ``nat_classes`` = natural per-tensor replication classes ``{name: [sorted class, ...]}``;
    ``class_bytes[(name, tuple(class))]`` = that shard's bytes. ``threshold`` may be ``float('inf')`` for
    the most aggressive decomposition (used to activate the topology-aware exchange, where we want every
    dissolvable group folded onto the standalone same-rank classes). Returns the refined grouping (same
    shape) — a partition of each natural class. Pure + deterministic (sorted iteration everywhere), so every
    rank derives the identical grouping (required for the flag ping-pong lockstep). Terminates: each round
    removes exactly one distinct multi-worker group and adds none (cover pieces are existing groups or
    singletons), and — via the overlap guard below — a group with no other multi-worker group overlapping
    it is never targeted, so ``threshold=inf`` still terminates (and standalone classes stay intact)."""
    # Per tensor: list of (sub-group, shard bytes), starting at the natural classes.
    tsg: dict[str, list[tuple[frozenset, int]]] = {
        name: [(frozenset(c), class_bytes[(name, tuple(c))]) for c in nat_classes[name]]
        for name in sorted(nat_classes)
    }

    def group_bytes() -> dict[frozenset, int]:
        gb: dict[frozenset, int] = {}
        for name in sorted(tsg):
            for sg, b in tsg[name]:
                gb[sg] = gb.get(sg, 0) + b
        return gb

    while True:
        gb = group_bytes()
        # (1) per-pair edge traffic: a size-k group of B bytes puts 2B/k on each of its worker pairs.
        pair_t: dict[tuple[int, int], float] = {}
        for rs, b in gb.items():
            k = len(rs)
            if k < 2:
                continue
            contrib = 2.0 * b / k
            ms = sorted(rs)
            for a in range(k):
                for c in range(a + 1, k):
                    key = (ms[a], ms[c])
                    pair_t[key] = pair_t.get(key, 0.0) + contrib
        low = {p for p, t in pair_t.items() if t < threshold}
        if not low:
            break
        # Pick a dissolvable group: a multi-worker replica-set carrying some low pair (deterministic:
        # largest first, then lexicographic — every rank dissolves the same one).
        target = None
        for rs in sorted(gb, key=lambda s: (-len(s), sorted(s))):
            if len(rs) < 2:
                continue
            ms = sorted(rs)
            if not any(
                (ms[a], ms[c]) in low
                for a in range(len(ms))
                for c in range(a + 1, len(ms))
            ):
                continue
            # Overlap guard: only dissolve a group that another multi-worker group can help cover (shares
            # a worker with it). A standalone group can only be covered by singletons, which would destroy
            # its dedup entirely — leave it intact. This preserves the same-rank classes the topo-aware
            # exchange rides on, and guarantees termination at threshold=inf (a group with no overlapping
            # peer group is never targeted, so the multi-worker group count still strictly decreases).
            if any(rs & other for other in gb if other != rs and len(other) >= 2):
                target = rs
                break
        if target is None:
            break
        # Greedy set-cover of `target` by existing strictly-smaller subsets (worker-count desc); remainder
        # -> singletons. Each piece is a valid sub-group (subset of the class -> holds the shard).
        remaining = set(target)
        cover: list[frozenset] = []
        for g in sorted(
            (s for s in gb if s != target and len(s) < len(target)),
            key=lambda s: (-len(s), sorted(s)),
        ):
            if g <= remaining:
                cover.append(g)
                remaining -= g
        cover += [frozenset((w,)) for w in sorted(remaining)]
        # Reassign every tensor that used `target` to the cover pieces; each piece re-fetches the full
        # shard (keeps that tensor's bytes) -> its bytes fold onto the existing piggyback groups.
        for name in sorted(tsg):
            new: list[tuple[frozenset, int]] = []
            for sg, b in tsg[name]:
                if sg == target:
                    new.extend((piece, b) for piece in cover)
                else:
                    new.append((sg, b))
            tsg[name] = new

    return {name: sorted([sorted(sg) for sg, _ in tsg[name]]) for name in sorted(tsg)}


def _dedup_specs_by_subgroups(
    specs: list[ShardSpec], subgroups: dict[str, list[list[int]]]
) -> list[ShardSpec]:
    """Like :func:`_dedup_specs` but split each tensor within the PROVIDED sub-group (a refinement of its
    natural class from :func:`consolidate_groups`) instead of the internally-derived class. Member ``j`` of
    a size-``m`` sub-group owns sub-shard ``j``; a size-1 sub-group keeps the full shard (0-peer receive)."""
    reduced: list[dict[str, Shards]] = [{} for _ in specs]
    for name in sorted(subgroups):
        for sg in subgroups[name]:
            members = sorted(sg)
            m = len(members)
            canon = sorted(
                (list(shard) for shard in specs[members[0]][name]),
                key=lambda sh: tuple(tuple(dim) for dim in sh),
            )
            for j, i in enumerate(members):
                subs = [
                    ss
                    for shard in canon
                    if (ss := split_shard_evenly(shard, m, j)) is not None
                ]
                if subs:
                    reduced[i][name] = subs
    return [ShardSpec(entries) for entries in reduced]


class WeightRouter:
    """Computes Data Plane P2P routing between Trainer Workers and Rollout Workers."""

    def __init__(
        self,
        rank: int,
        sender_ws: int,
        all_specs: list[ShardSpec],
        dtype_spec: dict[str, torch.dtype],
        *,
        global_rounds: list[set[str]] | None = None,
        peer_ip: dict[int, str] | None = None,
        direct_same_node: bool = False,
    ) -> None:
        self.rank = rank
        self.sender_ws = sender_ws
        self.world_size = len(all_specs)
        self.receiver_ws = self.world_size - sender_ws
        assert all(isinstance(spec, ShardSpec) for spec in all_specs), (
            "all_specs must be a list of ShardSpec"
        )
        validate_logical_tensor_partitions(all_specs)
        self.send_specs = all_specs[:sender_ws]
        self.recv_specs = all_specs[sender_ws:]
        self.direct_same_node = bool(direct_same_node)
        # Sender-side de-replication: workers holding identical shards each send only a disjoint sub-slice.
        # Always on — a no-op when nothing is replicated (a class of size 1 splits to itself).
        self.send_specs = dedup_send_specs(self.send_specs)
        self.dtype_spec = dtype_spec
        # Receiver-side de-replication (per-tensor): every tensor is split by its OWN replication-class
        # size, so replicated rollouts each receive only a disjoint sub-slice from the senders, then
        # reconstruct the full shard by a per-tensor all-to-all over the split ingress/exchange buffers (see
        # `_setup_rdma_buffers`). Keep the ORIGINAL recv specs — class discovery and the full-shard consume
        # need them. Always on (no-op when unreplicated: a size-1 class splits to itself; a rank with no
        # class peer just receives + consumes its own slice — the 0-peer arena case).
        self.recv_specs_full = list(self.recv_specs)
        # Group consolidation, threshold-controlled via WBRIDGE_DEDUP_PAIR_BYTES (per-pair edge bytes):
        #   0            -> OFF: natural class dedup, byte-identical to the original class-based dedup.
        #   large / inf (default) -> most aggressive: fold every dissolvable group onto the standalone
        #                   same-rank classes, reducing exchange fan-out and duplicate traffic.  The
        #                   topology-aware exchange can also consume the unconsolidated overlapping groups
        #                   as long as each individual group is balanced across its participating nodes.
        # When on, the deduped recv specs + recv_tensor_classes both use the sub-groups.
        self._recv_subgroups: dict[str, list[list[int]]] | None = None
        # These structures depend only on the canonical sender/receiver specs, not on how names are split
        # into communication rounds.  The RDMA-cap planner evaluates several candidate round counts; without
        # caching, each probe rebuilt the full Cartesian shard-overlap matrix and all natural receiver classes.
        self._recv_tensor_classes_cache: dict[str, list[list[int]]] | None = None
        self._arena_class_index_source: dict[str, list[list[int]]] | None = None
        self._arena_class_index: dict[str, dict[int, list[int]]] = {}
        self._pair_name_bytes_by_recv: list[dict[int, dict[str, int]]] | None = None
        self._trainer_peer_counts_cache: list[int] | None = None
        self._recv_name_bytes_cache: list[dict[str, int]] | None = None
        self.planner_invariant_seconds = 0.0
        self.planner_probe_timings: list[dict[str, float | int]] = []
        self._last_rollout_rdma_timing: dict[str, float] = {}
        # The library default consolidates aggressively to reduce exchange fan-out. Keep an explicit
        # 0 escape hatch for tensors that are too large to duplicate while dissolving a replication group.
        thr = float(os.environ.get("WBRIDGE_DEDUP_PAIR_BYTES", "inf") or "0")
        if self.direct_same_node:
            # A local receiver consumes trainer pack buffers directly, so it needs its complete runtime
            # shard from the trainers and has no receiver-receiver reconstruction class.  Keep sender-side
            # de-duplication above (e.g. CP replicas still split their source ownership), but route that
            # complete union independently to every rollout replica.  Singleton classes make every existing
            # arena/topology query agree that no rollout exchange is required.
            self._recv_subgroups = {
                name: [
                    [ri]
                    for ri, spec in enumerate(self.recv_specs_full)
                    if name in spec.entries
                ]
                for name in sorted(
                    {name for spec in self.recv_specs_full for name in spec.entries}
                )
            }
            if self.rank in (0, self.sender_ws):
                role = "sender" if self.rank < self.sender_ws else "receiver"
                logger.info(
                    "wbridge direct-same-node [rank %d %s]: full receiver routes; rollout exchange off",
                    self.rank,
                    role,
                )
        elif thr > 0:
            nat = self._natural_recv_classes()
            class_bytes = {
                (name, tuple(c)): shards_nbytes(
                    self.recv_specs_full[c[0]][name], self.dtype_spec[name]
                )
                for name in nat
                for c in nat[name]
            }
            self._recv_subgroups = consolidate_groups(nat, class_bytes, thr)
            self.recv_specs = _dedup_specs_by_subgroups(
                self.recv_specs, self._recv_subgroups
            )
            if self.rank in (
                0,
                self.sender_ws,
            ):  # one sender + one receiver report the effect
                n_nat = sum(len(v) for v in nat.values())
                n_sub = sum(len(v) for v in self._recv_subgroups.values())
                n_single = sum(
                    1 for v in self._recv_subgroups.values() for sg in v if len(sg) == 1
                )
                thr_str = (
                    "inf" if thr == float("inf") else "%.0fMB" % (thr / (1024 * 1024))
                )
                role = "sender" if self.rank < self.sender_ws else "receiver"
                msg = (
                    "wbridge dedup-consolidate [rank %d %s]: %d tensors, %d natural groups -> %d "
                    "sub-groups (%d singletons), thr=%s"
                    % (self.rank, role, len(nat), n_nat, n_sub, n_single, thr_str)
                )
                logger.info(msg)
                if (
                    os.environ.get("WBRIDGE_DEDUP_DIAG") == "1"
                ):  # frameworkless replay has no log handler
                    print("[" + msg + "]", flush=True)
        else:
            self.recv_specs = _dedup_specs(self.recv_specs)
            if (
                self.rank in (0, self.sender_ws)
                and os.environ.get("WBRIDGE_DEDUP_DIAG") == "1"
            ):
                role = "sender" if self.rank < self.sender_ws else "receiver"
                print(
                    "[wbridge dedup-consolidate [rank %d %s]: OFF (natural class dedup)]"
                    % (self.rank, role),
                    flush=True,
                )
        # Filled once physical receiver placement is known (after the session/IP gather, before arena
        # allocation).  Keeping this on WeightRouter lets every receiver independently derive exactly the
        # same multi-group routes without another control-plane exchange.
        self._topology_ok = False
        self._topology_groups: list[dict] = []
        self._topology_plans: list[list[dict]] = []
        self._topology_group_cache_key: tuple[str | None, ...] | None = None
        self._topology_group_cache: list[dict] | None = None
        self._topology_name_groups_cache: dict[str, tuple[dict, ...]] = {}
        self._topology_group_cache_valid = False
        # Replica-group relay is configured only after physical receiver placement is known.  It is a
        # separate data plane from the column all-gather above: trainers address one head per replication
        # group, and one representative per participating node forwards the already-assembled payload down
        # a chain.  Local group members consume the representative's PREP buffer over CUDA IPC.
        self._relay_ok = False
        self._relay_groups: list[dict] = []
        self.planner_mode = (
            "broadcast" if global_rounds is not None else "legacy_round_cap"
        )
        self.planned_rollout_rdma_peak_bytes: int | None = None
        if global_rounds is None:
            self.global_rounds = self.compute_global_rounds(peer_ip=peer_ip)
        else:
            self.global_rounds = [set(names) for names in global_rounds]
            self._validate_global_rounds(self.global_rounds)
        self.local_rounds = self.compute_local_rounds()
        if self.rank == 0 and os.environ.get("WBRIDGE_DEDUP_DIAG") == "1":
            try:
                self._dedup_diag()
            except Exception as e:  # noqa: BLE001 — diagnostics must never break a run
                logger.warning("wbridge dedup-diag failed: %s", e)

    def _pack_rounds(
        self, name_send, name_recv, hard_send, hard_recv, soft_send, soft_recv
    ):
        """Greedy name-priority packing into BALANCED rounds under per-rank byte caps.

        Per round, walk ``sorted(remaining names)`` and add a name only if it (a) touches no rank already
        in *critical state* — a sender/receiver that has reached its individual even target ``C'`` — and
        (b) keeps every rank it touches within the hard cap ``C``. Adding a name may push ranks to/over
        ``C'``; those go critical and no later name this round may touch them. The round closes when every
        rank is critical (all reached ``C'``) or the names run out. Tracking criticality PER RANK (not an
        aggregate total) keeps each round balanced even when the name order front-loads a subset of ranks:
        once the early ranks are satisfied we keep scanning for names that fill only the lagging ranks.
        ``soft=inf`` on every rank => no rank ever goes critical => plain fill-to-hard-cap (legacy).
        """
        S, Rw = self.sender_ws, self.receiver_ws
        tsend = {
            n: tuple(si for si in range(S) if name_send[n][si]) for n in self.dtype_spec
        }
        trecv = {
            n: tuple(ri for ri in range(Rw) if name_recv[n][ri])
            for n in self.dtype_spec
        }
        remaining = set(self.dtype_spec)
        rounds: list[set[str]] = []
        while remaining:
            send_used = [0] * S
            recv_used = [0] * Rw
            crit_send = [
                soft_send[si] <= 0 for si in range(S)
            ]  # zero-target ranks start satisfied
            crit_recv = [soft_recv[ri] <= 0 for ri in range(Rw)]
            round_plan: set[str] = set()
            for name in sorted(remaining):
                if any(crit_send[si] for si in tsend[name]) or any(
                    crit_recv[ri] for ri in trecv[name]
                ):
                    continue  # touches a rank that already hit its even target C' -> don't over-fill it
                ns, nr = name_send[name], name_recv[name]
                if any(send_used[si] + ns[si] > hard_send for si in tsend[name]) or any(
                    recv_used[ri] + nr[ri] > hard_recv for ri in trecv[name]
                ):
                    continue  # would exceed the hard cap C on some rank -> defer to a later round
                for si in tsend[name]:
                    send_used[si] += ns[si]
                    if send_used[si] >= soft_send[si]:
                        crit_send[si] = True
                for ri in trecv[name]:
                    recv_used[ri] += nr[ri]
                    if recv_used[ri] >= soft_recv[ri]:
                        crit_recv[ri] = True
                round_plan.add(name)
                if all(crit_send) and all(crit_recv):
                    break  # every rank reached its even target -> round is balanced and full
            if not round_plan:
                raise RuntimeError(
                    "routing deadlock: no tensor fits per-round caps (try smaller per-tensor overlap or raise caps)"
                )
            remaining -= round_plan
            rounds.append(round_plan)
        return rounds

    def _ensure_round_invariants(self) -> list[dict[int, dict[str, int]]]:
        """Cache sender/receiver overlap bytes and other data independent of the round partition."""
        if self._pair_name_bytes_by_recv is not None:
            return self._pair_name_bytes_by_recv

        started = time.perf_counter()
        # A Kimi-scale plan has O(70k) logical tensor names but each rank-pair overlaps only O(10-100)
        # of them.  Scanning every name for every (sender, receiver) pair made this phase perform billions
        # of negative dict lookups.  Give each name a stable bit position and intersect rank presence maps
        # in C with one Python-int AND; only set bits reach the shard-geometry overlap calculation.
        names = tuple(self.dtype_spec)
        name_order = {name: index for index, name in enumerate(names)}

        def presence_bitmap(spec: ShardSpec) -> int:
            bitmap = 0
            for name in spec.entries:
                index = name_order.get(name)
                if index is not None:
                    bitmap |= 1 << index
            return bitmap

        def bitmap_names(bitmap: int):
            while bitmap:
                least_bit = bitmap & -bitmap
                yield names[least_bit.bit_length() - 1]
                bitmap ^= least_bit

        send_presence = [presence_bitmap(spec) for spec in self.send_specs]
        recv_presence = [presence_bitmap(spec) for spec in self.recv_specs]
        pair_name_bytes_by_recv: list[dict[int, dict[str, int]]] = [
            {} for _ in range(self.receiver_ws)
        ]
        for si, send_spec in enumerate(self.send_specs):
            for ri, recv_spec in enumerate(self.recv_specs):
                common = send_presence[si] & recv_presence[ri]
                if not common:
                    continue
                overlap = ShardSpec.compute_overlap(
                    send_spec, recv_spec, bitmap_names(common)
                )
                if overlap:
                    pair_name_bytes_by_recv[ri][si] = {
                        name: shards_nbytes(shards, self.dtype_spec[name])
                        for name, shards in overlap.entries.items()
                    }

        self._pair_name_bytes_by_recv = pair_name_bytes_by_recv
        self._trainer_peer_counts_cache = [
            len(sender_bytes) for sender_bytes in pair_name_bytes_by_recv
        ]
        self._recv_name_bytes_cache = [
            {
                name: shards_nbytes(shards, self.dtype_spec[name])
                for name, shards in spec.entries.items()
            }
            for spec in self.recv_specs
        ]
        # Every byte in the de-replicated receiver spec must have a trainer source.  A framework mapping
        # omission otherwise survives LoadSpec verification (the missing worker parameter is simply absent
        # from the sender spec) and fails much later as an opaque arena-layout assertion.  Validate the
        # actual shard-overlap bytes here and report exact tensor names/ranks.
        for ri, expected_by_name in enumerate(self._recv_name_bytes_cache):
            covered_by_name: dict[str, int] = {}
            for name_bytes in pair_name_bytes_by_recv[ri].values():
                for name, nbytes in name_bytes.items():
                    covered_by_name[name] = covered_by_name.get(name, 0) + nbytes
            mismatch = {
                name: (expected, covered_by_name.get(name, 0))
                for name, expected in expected_by_name.items()
                if covered_by_name.get(name, 0) != expected
            }
            if mismatch:
                sample = list(sorted(mismatch.items()))[:20]
                raise ValueError(
                    "incomplete trainer coverage for receiver "
                    f"{ri}: {len(mismatch)} tensor(s) differ; "
                    f"sample(name: (expected_bytes, covered_bytes))={sample}"
                )
        self.planner_invariant_seconds += time.perf_counter() - started
        return pair_name_bytes_by_recv

    def _name_rank_bytes(self) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
        """Per-tensor wire bytes charged to every trainer and rollout rank."""
        names = tuple(self.dtype_spec)
        pair_name_bytes_by_recv = self._ensure_round_invariants()
        name_send = {name: [0] * self.sender_ws for name in names}
        name_recv = {name: [0] * self.receiver_ws for name in names}
        for ri, sender_bytes in enumerate(pair_name_bytes_by_recv):
            for si, name_bytes in sender_bytes.items():
                for name, nb in name_bytes.items():
                    name_send[name][si] += nb
                    name_recv[name][ri] += nb
        return name_send, name_recv

    def _validate_global_rounds(self, rounds: list[set[str]]) -> None:
        if not rounds or any(not names for names in rounds):
            raise ValueError("the round plan must contain one or more non-empty rounds")
        seen: set[str] = set()
        duplicate: set[str] = set()
        for names in rounds:
            duplicate |= seen & names
            seen |= names
        expected = set(self.dtype_spec)
        if seen != expected or duplicate:
            raise ValueError(
                f"invalid round plan: missing={sorted(expected - seen)}, "
                f"extra={sorted(seen - expected)}, duplicate={sorted(duplicate)}"
            )

    def _pack_exact_rounds(
        self,
        name_send: dict[str, list[int]],
        name_recv: dict[str, list[int]],
        num_rounds: int,
    ) -> list[set[str]]:
        """Use the existing per-rank critical-target greedy to make exactly ``num_rounds`` rounds.

        Targets are recomputed from the bytes still unassigned at the start of every round. This retains the
        name-priority and per-rank balancing behavior of :meth:`_pack_rounds`, while reserving at least one
        tensor for every future round and assigning all remainder to the final round.
        """
        names = sorted(self.dtype_spec)
        if not 1 <= num_rounds <= len(names):
            raise ValueError(
                f"requested {num_rounds} rounds, but the plan has {len(names)} tensor names; "
                "the valid range is [1, number of tensors]"
            )
        remaining = set(names)
        send_remaining = [
            sum(name_send[n][si] for n in names) for si in range(self.sender_ws)
        ]
        recv_remaining = [
            sum(name_recv[n][ri] for n in names) for ri in range(self.receiver_ws)
        ]
        send_touched = {
            n: tuple(i for i, nb in enumerate(name_send[n]) if nb) for n in names
        }
        recv_touched = {
            n: tuple(i for i, nb in enumerate(name_recv[n]) if nb) for n in names
        }
        rounds: list[set[str]] = []
        for round_index in range(num_rounds):
            rounds_left = num_rounds - round_index
            if rounds_left == 1:
                round_plan = set(remaining)
            else:
                # Unlike the legacy hard-cap repack, exact-R planning does not need 5% headroom to avoid
                # stranding a cap-fitting tensor: there is no hard cap. Aim at the exact remaining average;
                # an indivisible tensor may cross it, then that rank becomes critical for this round.
                soft_send = [-(-total // rounds_left) for total in send_remaining]
                soft_recv = [-(-total // rounds_left) for total in recv_remaining]
                send_used = [0] * self.sender_ws
                recv_used = [0] * self.receiver_ws
                crit_send = [target <= 0 for target in soft_send]
                crit_recv = [target <= 0 for target in soft_recv]
                # Leave at least one name for every future round.
                max_names = len(remaining) - (rounds_left - 1)
                round_plan = set()
                for name in sorted(remaining):
                    if len(round_plan) >= max_names:
                        break
                    if any(crit_send[si] for si in send_touched[name]) or any(
                        crit_recv[ri] for ri in recv_touched[name]
                    ):
                        continue
                    for si in send_touched[name]:
                        send_used[si] += name_send[name][si]
                        if send_used[si] >= soft_send[si]:
                            crit_send[si] = True
                    for ri in recv_touched[name]:
                        recv_used[ri] += name_recv[name][ri]
                        if recv_used[ri] >= soft_recv[ri]:
                            crit_recv[ri] = True
                    round_plan.add(name)
                    if all(crit_send) and all(crit_recv):
                        break
                if not round_plan:
                    # A sparse overlap pattern can make every remaining name touch one already-critical
                    # rank. Advancing the first deterministic name preserves exact progress; the next round
                    # recomputes its targets from the resulting remainder.
                    round_plan = {min(remaining)}

            for name in round_plan:
                for si, nb in enumerate(name_send[name]):
                    send_remaining[si] -= nb
                for ri, nb in enumerate(name_recv[name]):
                    recv_remaining[ri] -= nb
            remaining -= round_plan
            rounds.append(round_plan)

        assert not remaining and all(
            total == 0 for total in send_remaining + recv_remaining
        )
        self._validate_global_rounds(rounds)
        return rounds

    def _legacy_cap_rounds(
        self,
        name_send: dict[str, list[int]],
        name_recv: dict[str, list[int]],
    ) -> list[set[str]]:
        """The original per-round wire-cap planner, retained as a compatibility fallback."""

        hard_recv = RECEIVER_ROUND_CAP_BYTES
        hard_send = RECEIVER_ROUND_CAP_BYTES * self.receiver_ws // self.sender_ws

        inf = float("inf")
        inf_send = [inf] * self.sender_ws
        inf_recv = [inf] * self.receiver_ws

        # Pass 1: minimum round count R at the hard cap C.
        R = len(
            self._pack_rounds(
                name_send, name_recv, hard_send, hard_recv, inf_send, inf_recv
            )
        )
        if R <= 1:
            return self._pack_rounds(
                name_send, name_recv, hard_send, hard_recv, inf_send, inf_recv
            )

        # Pass 2: per-rank even target C' = ceil(rank total / R). A round fills each rank to its own C'
        # (critical) and won't over-fill an already-critical rank, so traffic is balanced across ranks
        # regardless of tensor-name order.
        send_tot = [
            sum(name_send[n][si] for n in self.dtype_spec)
            for si in range(self.sender_ws)
        ]
        recv_tot = [
            sum(name_recv[n][ri] for n in self.dtype_spec)
            for ri in range(self.receiver_ws)
        ]
        soft_send = [
            _even_round_soft_target(send_tot[si], R, hard_send)
            for si in range(self.sender_ws)
        ]
        soft_recv = [
            _even_round_soft_target(recv_tot[ri], R, hard_recv)
            for ri in range(self.receiver_ws)
        ]
        rounds = self._pack_rounds(
            name_send, name_recv, hard_send, hard_recv, soft_send, soft_recv
        )
        logger.info(
            "wbridge router: %d rounds, even-split per-rank C'(+5%%)~%.2f GiB/recv "
            "(R=%d, hard cap %.2f GiB/recv)",
            len(rounds),
            max(soft_recv) / 1024**3,
            R,
            hard_recv / 1024**3,
        )
        return rounds

    def compute_global_rounds(
        self, *, peer_ip: dict[int, str] | None = None
    ) -> list[set[str]]:
        """Build a balanced plan from one of the two production planner modes.

        ``WBRIDGE_NUM_ROUNDS`` requests an exact number of balanced rounds.
        ``WBRIDGE_ROLLOUT_RDMA_CAP_BYTES`` selects the smallest exact round count whose largest rollout
        worker's registered permanent buffers fit the specified cap. The latter requires physical placement
        in ``peer_ip`` because topology-aware GRECV storage is placement-dependent.

        If neither is set, the former per-round wire cap remains available for compatibility with old runs.
        """
        requested = os.environ.get("WBRIDGE_NUM_ROUNDS", "").strip()
        rdma_cap = os.environ.get("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", "").strip()
        if requested and rdma_cap:
            raise ValueError(
                "WBRIDGE_NUM_ROUNDS and WBRIDGE_ROLLOUT_RDMA_CAP_BYTES are mutually exclusive"
            )
        name_send, name_recv = self._name_rank_bytes()
        if requested:
            rounds = self._pack_exact_rounds(name_send, name_recv, int(requested))
            self.planner_mode = "explicit_rounds"
            logger.info(
                "wbridge router: explicit balanced round plan R=%d", len(rounds)
            )
            return rounds
        if rdma_cap:
            cap = int(rdma_cap)
            if cap <= 0:
                raise ValueError(
                    f"WBRIDGE_ROLLOUT_RDMA_CAP_BYTES must be positive, got {cap}"
                )
            if peer_ip is None:
                raise ValueError(
                    "rollout RDMA-cap planning requires physical peer_ip placement"
                )
            rounds, peak = self._rounds_for_rollout_rdma_cap(
                name_send, name_recv, peer_ip, cap
            )
            self.planner_mode = "rollout_rdma_cap"
            self.planned_rollout_rdma_peak_bytes = peak
            logger.info(
                "wbridge router: rollout RDMA cap %.3f GiB selected R=%d, peak %.3f GiB",
                cap / 1024**3,
                len(rounds),
                peak / 1024**3,
            )
            return rounds
        return self._legacy_cap_rounds(name_send, name_recv)

    def compute_local_rounds(self) -> list[CommRoundPlan]:
        """
        Plan for *rank*: round *i* is :class:`CommRoundPlan` — ``peer_specs[peer]`` is the wire overlap
        with global rank *peer*; on receivers ``recv_subspec`` is this round's buffer layout (merged overlaps).
        """
        out: list[tuple[ShardSpec, dict[int, ShardSpec]]] = []
        is_sender = self.rank < self.sender_ws
        if is_sender:
            full_spec = self.send_specs[self.rank]
            overlaps = {
                ri + self.sender_ws: ShardSpec.compute_overlap(
                    full_spec, self.recv_specs[ri]
                )
                for ri in range(self.receiver_ws)
            }
        else:
            full_spec = self.recv_specs[self.rank - self.sender_ws]
            overlaps = {
                si: ShardSpec.compute_overlap(self.send_specs[si], full_spec)
                for si in range(self.sender_ws)
            }
        for round_plan in self.global_rounds:
            round_overlaps = {
                rank: sub_overlap
                for rank, overlap in overlaps.items()
                if (sub_overlap := overlap.subset(round_plan))
            }
            out.append((full_spec.subset(round_plan), round_overlaps))
        return out

    def _natural_recv_classes(self) -> dict[str, list[list[int]]]:
        """Per-tensor replication classes over rollout workers (for tensor-level dedup). Returns
        ``{name: [class, ...]}`` where each class is the sorted local indices of receivers holding an
        IDENTICAL shard set for that tensor; a member's slice index is its position in its class. Matches
        how :func:`_dedup_specs` splits (per name, per canonical shard key). Deterministic across ranks
        (uses the shared originals ``recv_specs_full``)."""
        names: set[str] = set()
        for s in self.recv_specs_full:
            names |= set(s.entries.keys())
        out: dict[str, list[list[int]]] = {}
        for name in sorted(names):
            groups: dict = {}
            for rl in range(self.receiver_ws):
                s = self.recv_specs_full[rl]
                if name in s.entries:
                    groups.setdefault(_shards_canonical_key(s[name]), []).append(rl)
            out[name] = [sorted(m) for m in groups.values()]
        return out

    def recv_tensor_classes(self) -> dict[str, list[list[int]]]:
        """Per-tensor exchange groups: the natural replication classes, OR the consolidated sub-groups when
        WBRIDGE_DEDUP_PAIR_BYTES>0 (a partition-refinement — same shape, consumed identically downstream)."""
        if self._recv_subgroups is not None:
            return self._recv_subgroups
        if self._recv_tensor_classes_cache is None:
            self._recv_tensor_classes_cache = self._natural_recv_classes()
        return self._recv_tensor_classes_cache

    @staticmethod
    def topo_subgroups(
        members_global: list[int], ip_of: dict[int, str]
    ) -> tuple[list[list[int]], bool]:
        """Partition an exchange group into ``n`` subgroups of exactly ONE worker per node, for the
        topology-aware 2-phase all-gather. ``members_global`` = the group's global ranks; ``ip_of`` =
        ``{global_rank: host_ip}`` (== ``WBEndpoint._peer_ip``). Returns ``(subgroups, ok)`` where
        ``subgroups[j]`` = sorted global ranks = the j-th worker (by rank) on each node.

        ``ok=False`` when the nodes hold UNEQUAL member counts (the same-``n`` property fails) or a member's
        IP is unknown — the caller then falls back to the single-phase all-gather. Pure + deterministic
        (sorted throughout), so every member derives the identical partition with no extra handshake (the
        same determinism guarantee ``consolidate_groups`` relies on, since ``ip_of`` is globally gathered)."""
        by_node: dict[str, list[int]] = {}
        for r in sorted(members_global):
            ip = ip_of.get(r)
            if ip is None:
                return [], False
            by_node.setdefault(ip, []).append(r)
        counts = [len(rs) for rs in by_node.values()]
        n = counts[0] if counts else 0
        if n == 0 or any(c != n for c in counts):
            return [], False  # unequal workers per node -> not 1-per-node partitionable
        nodes = [by_node[ip] for ip in sorted(by_node)]  # each list already rank-sorted
        subgroups = [sorted(node[j] for node in nodes) for j in range(n)]
        return subgroups, True

    def configure_topology(self, ip_of: dict[int, str]) -> bool:
        """Build the topology-aware exchange plan for every receiver and round.

        A receiver may belong to any number of replication groups.  Each multi-worker group is independently
        eligible when every participating node contributes the same number of workers.  For one such group,
        :meth:`topo_subgroups` makes one-worker-per-node *columns*: workers first exchange only their exact
        group payload inside their column, then each worker pulls the other columns from its local peers.

        The returned per-round plan uses receiver-local indices and contains:

        ``external``
            ``peer -> names`` packed into one cross-node write during fused assemble+repack.
        ``pull``
            ``local_peer -> (source-kind, source-worker, names)`` routes.  Multiple routes between the same
            local pair are intentional because there is no repack between external and internal exchange.
        Singleton classes need no exchange and are ignored.  A group wholly inside one node is valid (its
        columns are singletons and only the internal phase runs), as is one worker per node (only external).
        If *any* multi-worker group is unbalanced, the entire topology path is disabled so all ranks take the
        existing single-phase protocol consistently.
        """
        sw = self.sender_ws
        classes = self.recv_tensor_classes()
        placement_key = tuple(
            ip_of.get(sw + member) for member in range(self.receiver_ws)
        )
        if self._topology_group_cache_key == placement_key:
            groups = self._topology_group_cache or []
            groups_valid = self._topology_group_cache_valid
        else:
            group_names: dict[tuple[int, ...], set[str]] = {}
            for name in sorted(classes):
                for members in classes[name]:
                    key = tuple(sorted(members))
                    if len(key) >= 2:
                        group_names.setdefault(key, set()).add(name)

            groups = []
            groups_valid = True
            for members in sorted(group_names):
                columns_g, ok = self.topo_subgroups([sw + m for m in members], ip_of)
                if not ok:
                    groups_valid = False
                    groups = []
                    break
                columns = tuple(tuple(g - sw for g in col) for col in columns_g)
                column_of = {m: col for col in columns for m in col}
                same_node_of = {
                    m: tuple(
                        p
                        for p in members
                        if p != m and ip_of.get(sw + p) == ip_of.get(sw + m)
                    )
                    for m in members
                }
                groups.append(
                    {
                        "members": members,
                        "names": tuple(sorted(group_names[members])),
                        "columns": columns,
                        "column_of": column_of,
                        "same_node_of": same_node_of,
                    }
                )
            self._topology_group_cache_key = placement_key
            self._topology_group_cache = groups
            name_groups: dict[str, list[dict]] = {}
            for group in groups:
                for name in group["names"]:
                    name_groups.setdefault(name, []).append(group)
            self._topology_name_groups_cache = {
                name: tuple(tensor_groups)
                for name, tensor_groups in name_groups.items()
            }
            self._topology_group_cache_valid = groups_valid

        # No replicated tensors means there is no topology exchange to enable.  This is not a structural
        # failure for any group; it merely leaves the normal zero-peer consume path in charge.
        if not groups_valid or not groups:
            self._topology_ok = False
            self._topology_groups = []
            self._topology_plans = []
            return False

        plans: list[list[dict]] = [[] for _ in range(self.receiver_ws)]
        for rl in range(self.receiver_ws):
            for round_set in self.global_rounds:
                round_names = sorted(n for n in round_set if n in self.recv_specs[rl])
                external: dict[int, set[str]] = {}
                routes: dict[int, dict[tuple[str, int], set[str]]] = {}
                peers: set[int] = set()
                for name in round_names:
                    for grp in self._topology_name_groups_cache.get(name, ()):
                        if rl not in grp["members"]:
                            continue
                        peers.update(p for p in grp["members"] if p != rl)
                        for p in grp["column_of"][rl]:
                            if p != rl:
                                external.setdefault(p, set()).add(name)
                        for p in grp["same_node_of"][rl]:
                            proutes = routes.setdefault(p, {})
                            proutes.setdefault(("own", p), set()).add(name)
                            for q in grp["column_of"][p]:
                                if q != p:
                                    proutes.setdefault(("grecv", q), set()).add(name)

                external_out = {
                    p: tuple(sorted(names))
                    for p, names in sorted(external.items())
                    if names
                }
                pull_out = {
                    p: tuple(
                        (kind, source, tuple(sorted(names)))
                        for (kind, source), names in sorted(proutes.items())
                        if names
                    )
                    for p, proutes in sorted(routes.items())
                }
                # Prove the two phases cover the generic all-gather exactly once, name by name.  A source
                # may be split across phases (direct in one group, routed through a local column in another),
                # which is precisely why a peer-level whole-buffer assumption is insufficient.
                delivered: dict[int, set[str]] = {}
                for source, selected in external_out.items():
                    delivered.setdefault(source, set()).update(selected)
                for peer_routes in pull_out.values():
                    for _kind, source, selected in peer_routes:
                        prior = delivered.setdefault(source, set())
                        assert not prior.intersection(selected), (
                            f"duplicate topology delivery rl={rl} source={source} "
                            f"names={sorted(prior.intersection(selected))}"
                        )
                        prior.update(selected)
                payloads = self._arena_payloads(rl, round_names, classes)
                expected = {p: set(payloads.get(p, ())) for p in peers}
                assert delivered == expected, (
                    f"incomplete topology delivery rl={rl}: delivered={delivered} expected={expected}"
                )
                internal = tuple(sorted(pull_out))
                plans[rl].append(
                    {
                        "external": external_out,
                        "pull": pull_out,
                        "peers": tuple(sorted(peers)),
                        "internal": internal,
                    }
                )

        self._topology_ok = True
        self._topology_groups = groups
        self._topology_plans = plans
        return True

    def topology_plan(self, rl: int, ri: int) -> dict:
        """Return the configured receiver-local topology plan for one global round."""
        assert self._topology_ok, (
            "topology was not configured or is structurally ineligible"
        )
        return self._topology_plans[rl][ri]

    def configure_relay(self, ip_of: dict[int, str]) -> bool:
        """Build deterministic replica-group head/relay chains for the current round plan.

        Tensor replication classes are first coalesced by identical member set.  For each resulting group,
        one representative (the lowest global rank) is selected on every participating node.  Representatives
        form a global-rank-ordered chain; the first is the trainer-facing head.  Every member consumes from
        its node's representative, so cross-node traffic carries one full canonical group payload per link,
        while same-node replication is serviced by direct CUDA-IPC reads.

        The old global name-to-round assignment is retained verbatim.  ``round_specs[ri]`` is the canonical
        full receiver shard for the group's names in that round, and ``trainer_specs[ri]`` partitions it by
        the already de-replicated trainer sources.  No receiver-side repack is required: every chain hop uses
        the same packed ``round_specs[ri]`` byte layout.
        """
        sw = self.sender_ws
        # Relay uses the same threshold-controlled refinement as exchange.  In particular CONS=INF
        # dissolves the small, wide natural classes onto the already-required TP/EP replica groups.  A
        # dissolved tensor is deliberately duplicated once per covering subgroup; that small byte cost lets
        # it piggyback on an existing relay chain instead of creating another independently synchronized
        # group.  CONS=0 still returns the unmodified natural classes.
        classes = self.recv_tensor_classes()
        grouped_names: dict[tuple[int, ...], set[str]] = {}
        for name in sorted(classes):
            for members in classes[name]:
                key = tuple(sorted(members))
                if not key:
                    continue
                grouped_names.setdefault(key, set()).add(name)

        groups: list[dict] = []
        for gid, members in enumerate(sorted(grouped_names)):
            names = tuple(sorted(grouped_names[members]))
            by_node: dict[str, list[int]] = {}
            for member in members:
                global_rank = sw + member
                node = ip_of.get(global_rank)
                if node is None:
                    self._relay_ok = False
                    self._relay_groups = []
                    return False
                by_node.setdefault(node, []).append(member)

            # Lowest rank per node owns the group's depth-2 RECV/PREP buffers.  Sorting the owners by global
            # rank makes the lowest owner the head and is stable across every process without coordination.
            owners = tuple(
                sorted(min(node_members) for node_members in by_node.values())
            )
            chain = tuple(sw + owner for owner in owners)
            owner_of = {member: min(by_node[ip_of[sw + member]]) for member in members}
            local_readers = {
                owner: tuple(
                    sorted(member for member in members if owner_of[member] == owner)
                )
                for owner in owners
            }

            canonical = self.recv_specs_full[members[0]].subset(set(names))
            for member in members[1:]:
                candidate = self.recv_specs_full[member].subset(set(names))
                if candidate.entries != canonical.entries:
                    raise RuntimeError(
                        f"relay group {members} is not byte-canonical between members "
                        f"{members[0]} and {member}"
                    )

            round_specs: list[ShardSpec] = []
            trainer_specs: list[dict[int, ShardSpec]] = []
            for round_names in self.global_rounds:
                group_spec = canonical.subset(set(round_names))
                round_specs.append(group_spec)
                trainer_specs.append(
                    {
                        si: overlap
                        for si, sender_spec in enumerate(self.send_specs)
                        if (
                            overlap := ShardSpec.compute_overlap(
                                sender_spec, group_spec
                            )
                        ).entries
                    }
                )
                if group_spec.entries:
                    expected_bytes = group_spec.nbytes(self.dtype_spec)
                    covered_bytes = sum(
                        spec.nbytes(self.dtype_spec)
                        for spec in trainer_specs[-1].values()
                    )
                    if covered_bytes != expected_bytes:
                        raise RuntimeError(
                            f"relay group {gid} round {len(round_specs) - 1} trainer coverage is "
                            f"{covered_bytes} bytes, expected {expected_bytes}"
                        )

            groups.append(
                {
                    "id": gid,
                    "members": members,
                    "names": names,
                    "owners": owners,
                    "chain": chain,
                    "head": chain[0],
                    "owner_of": owner_of,
                    "local_readers": local_readers,
                    "round_specs": tuple(round_specs),
                    "trainer_specs": tuple(trainer_specs),
                }
            )

        self._relay_ok = bool(groups)
        self._relay_groups = groups
        return self._relay_ok

    def relay_group(self, gid: int) -> dict:
        if not self._relay_ok:
            raise RuntimeError("replica-group relay was not configured")
        return self._relay_groups[gid]

    def _arena_class_of(self, classes: dict, name: str, member: int) -> list[int]:
        if classes is not self._arena_class_index_source:
            self._arena_class_index = {
                tensor_name: {
                    member_rank: members
                    for members in tensor_classes
                    for member_rank in members
                }
                for tensor_name, tensor_classes in classes.items()
            }
            self._arena_class_index_source = classes
        return self._arena_class_index.get(name, {}).get(member, [])

    def _arena_payloads(
        self,
        rl: int,
        round_names: list[str],
        classes: dict,
    ) -> dict[int, tuple[str, ...]]:
        """Build every peer payload in one name-major pass over a receiver's sorted round names."""
        payloads: dict[int, list[str]] = {}
        for name in round_names:
            for peer in self._arena_class_of(classes, name, rl):
                if peer != rl:
                    payloads.setdefault(peer, []).append(name)
        return {peer: tuple(names) for peer, names in payloads.items()}

    def _arena_shared(
        self, rl: int, p: int, round_names: list[str], classes: dict
    ) -> list[str]:
        """Tensors that receivers *rl* and *p* both hold in the SAME per-tensor class this round — the
        slices they exchange in the all-gather. Sorted (deterministic)."""
        return list(self._arena_payloads(rl, sorted(round_names), classes).get(p, ()))

    def arena_layout(self, rl: int, classes: dict | None = None, *, depth: int = 1):
        """Split ingress/exchange layout for receiver local index *rl* (pure; no CUDA).

        Two physically independent allocations have deliberately different reachability and lifetimes:

        * **RECV ingress** — trainer-reachable only. There are ``depth`` parity slots with common stride
          ``recv_stride``. Within each parity, every trainer sender ``si`` owns a stable lane sized to its
          maximum contribution among rounds of that parity. ``recv[si] = (off, current_nb)`` selects the
          current prefix of that lane. Consequently one trainer can never overwrite another trainer's
          unassembled input; a sender only needs the ACK for its own previous use of ``(receiver, parity)``.
        * **PREP** ``[0, depth*S)`` — rollout-readable fused-prepare output: ``own`` plus one staging range per
          unique partial ``send`` or topology-packed ``topo_send`` payload. Identical payloads alias, and a
          full payload aliases ``own``. Each round's offsets are relative to its ``S``-byte parity slot.
        * **GRECV** ``[depth*S, total)`` — one rollout-writable shared bank. In the topology path it contains
          only exact cross-node ingress payloads; same-node columns are consumed directly from their owner's
          CUDA-IPC arena and need no local staging. The generic fallback retains one full slot per peer.
          Every allocated source gets one stable subrange for each parity it uses, sized to that source's
          maximum payload on that parity. Thus depth 2 lets round ``r+1`` land without waiting for round ``r``
          consumption; reuse of a physical slot is gated by round ``r-2``.

        Returns ``(rounds, S, peer_set)`` where ``S = max_r prep(r)`` is the rollout PREP stride. RECV offsets
        use the separate ``recv_stride`` recorded in every round; own/send use ``S``; GRECV offsets are already
        absolute in the rollout allocation::

            {"s2r", "recv_stride", "prep", "prep_base", "agh",
             "recv":      {si: (off, nb)}, # isolated ingress, stable per (parity, sender)
             "own":       (off, nb),       # rollout PREP slot
             "send":      {p: (off, nb)},  # generic fallback; identical payloads alias / own
             "topo_send": {p: (off, nb)},  # exact per-column payload, packed across groups
             "grecv":       {p: (off, nb)}, # absolute offsets, stable per (external source, round parity)
             "grecv_names": {p: names}}     # exact packed layout of each current grecv payload

        ``agh`` remains logical accounting (``prep + Σ current grecv bytes``), not a contiguous region.
        :func:`_arena_recv_total_bytes` and :func:`_arena_total_bytes` size the two allocations.

        ``peer_set`` is the sorted union of class peers across rounds. Deterministic (senders, peers, and
        names are all iterated in sorted order). Every :meth:`ShardSpec.subset` call is passed a FRESH
        ``set(...)`` — ``subset`` does ``names &= entries.keys()`` (a list raises ``TypeError``)."""
        assert depth >= 1
        if self.direct_same_node:
            # Direct local consume never lands trainer bytes in a receiver allocation: the model-copy kernel
            # reads the sender's exported pack buffer itself.  Return a structurally complete zero-payload
            # layout so setup, RDMA-cap planning, and metadata publication can retain their normal control
            # flow without allocating RECV/PREP/GRECV/DOFF storage.
            return (
                [
                    {
                        "s2r": 0,
                        "recv_stride": 0,
                        "prep": 0,
                        "prep_base": 0,
                        "agh": 0,
                        "agh_base": 0,
                        "recv": {},
                        "own": (0, 0),
                        "send": {},
                        "topo_send": {},
                        "grecv": {},
                        "grecv_names": {},
                    }
                    for _ in self.global_rounds
                ],
                0,
                [],
            )
        my = self.recv_specs[rl]
        classes = classes if classes is not None else self.recv_tensor_classes()
        pair_name_bytes = self._ensure_round_invariants()[rl]
        assert self._recv_name_bytes_cache is not None
        recv_name_bytes = self._recv_name_bytes_cache
        my_name_bytes = recv_name_bytes[rl]
        rounds: list[dict] = []
        peer_set: set[int] = set()
        # ---- Pass A: logical per-round RECV sizes + compact PREP payloads ----
        for ri, names in enumerate(self.global_rounds):
            round_names = sorted(
                n for n in names if n in my.entries
            )  # tensors I hold this round
            round_name_set = set(round_names)
            recv: dict[int, tuple[int, int]] = {}
            cursor = 0
            for si in range(self.sender_ws):
                name_bytes = pair_name_bytes.get(si)
                nb = (
                    sum(
                        value
                        for name, value in name_bytes.items()
                        if name in round_name_set
                    )
                    if name_bytes
                    else 0
                )
                if nb:
                    recv[si] = (cursor, nb)
                    cursor += nb
            s2r = cursor
            payload_of = self._arena_payloads(rl, round_names, classes)
            peers_this = set(payload_of)
            cur = 0
            own_rel = (cur, s2r)
            cur += s2r
            # Step 1: deduplicate logical send payloads before making any physical-buffer decision. An
            # all-gather normally sends the same local slice to every member of a replication class.
            payload_nb = {
                payload: sum(my_name_bytes[name] for name in payload)
                for payload in dict.fromkeys(payload_of.values())
            }
            # Step 2: assign one physical source per unique payload. If it is the entire canonical `own`
            # slice, use `own` directly; assembly will populate the send source in the same copy kernel.
            # Otherwise reserve one compact staging region for that unique subset. All send sources are
            # read-only once prepared, so concurrent RDMA reads / CUDA-IPC pulls may safely alias.
            full_payload = tuple(round_names)
            send_payloads: dict[tuple[str, ...], tuple[int, int]] = {}
            send_rel: dict[int, tuple[int, int]] = {}
            for p in sorted(peers_this):
                shared = payload_of[p]
                nb = payload_nb[shared]
                slot = send_payloads.get(shared)
                if slot is None:
                    if shared == full_payload:
                        assert nb == s2r
                        slot = own_rel
                    else:
                        slot = (cur, nb)
                        cur += nb
                    send_payloads[shared] = slot
                send_rel[p] = slot
            # Topology-aware multi-group exchange may send only a SUBSET of the names shared with a peer:
            # precisely those groups for which the two workers occupy the same cross-node column.  Pack all
            # such names for one peer contiguously during the same assemble+repack kernel.  Reuse a generic
            # send/own slot whenever the payload happens to be identical, so the common one-group case costs
            # no additional arena memory or copy.
            topo_send_rel: dict[int, tuple[int, int]] = {}
            if self._topology_ok:
                external = self.topology_plan(rl, ri)["external"]
                for p, topo_names in sorted(external.items()):
                    assert p in payload_of
                    assert set(topo_names) <= set(payload_of[p])
                    payload = tuple(topo_names)
                    nb = sum(my_name_bytes[name] for name in payload)
                    slot = send_payloads.get(payload)
                    if slot is None:
                        if payload == full_payload:
                            assert nb == s2r
                            slot = own_rel
                        else:
                            slot = (cur, nb)
                            cur += nb
                        send_payloads[payload] = slot
                    topo_send_rel[p] = slot
            prep = cur
            grecv_names: dict[int, tuple[str, ...]] = {}
            if self._topology_ok:
                # Only cross-node columns land in this worker. Internal columns are read directly from the
                # owning worker by the fused internal-consume kernel, so allocating their former staging
                # slots would retain the extra model-sized GRECV copy this path is intended to remove.
                for source in range(self.receiver_ws):
                    incoming = self.topology_plan(source, ri)["external"].get(rl, ())
                    if incoming:
                        assert source in peers_this
                        grecv_names[source] = tuple(incoming)
            else:
                grecv_names = dict(payload_of)
            grecv_nb = {
                p: sum(recv_name_bytes[p][name] for name in names)
                for p, names in grecv_names.items()
            }
            agh = prep + sum(grecv_nb.values())
            rounds.append(
                {
                    "s2r": s2r,
                    "prep": prep,
                    "agh": agh,
                    "recv": recv,
                    "_own": own_rel,
                    "_send": send_rel,
                    "_topo_send": topo_send_rel,
                    "_grecv_nb": grecv_nb,
                    "grecv_names": grecv_names,
                }
            )
            peer_set |= peers_this
        # ---- Pass B: stable trainer lanes in the separate RECV allocation ----
        # A compact per-round layout is unsafe: e.g. sender A in round 0 and sender B in round 2 can both get
        # offset zero, while B only waits for B's round-0 destinations and may overwrite A's still-unassembled
        # bytes. Stable lanes turn slot ownership into (receiver, parity, sender), matching the ACK endpoints.
        lane_nb = [[0] * self.sender_ws for _ in range(depth)]
        for ri, rd in enumerate(rounds):
            par = ri % depth
            for si, (_off, nb) in rd["recv"].items():
                lane_nb[par][si] = max(lane_nb[par][si], nb)
        lane_off: list[list[int]] = []
        parity_bytes: list[int] = []
        for par in range(depth):
            offsets = []
            cursor = 0
            for si in range(self.sender_ws):
                offsets.append(cursor)
                cursor += lane_nb[par][si]
            lane_off.append(offsets)
            parity_bytes.append(cursor)
        recv_stride = max(parity_bytes, default=0)
        for ri, rd in enumerate(rounds):
            par = ri % depth
            rd["recv"] = {
                si: (lane_off[par][si], nb) for si, (_old_off, nb) in rd["recv"].items()
            }
            rd["recv_stride"] = recv_stride

        # ---- Pass C: top-anchor PREP in each rollout parity slot ----
        S = max((rd["prep"] for rd in rounds), default=0)
        for rd in rounds:
            base = S - rd["prep"]
            rd["prep_base"] = base
            rd["agh_base"] = (
                base  # compatibility alias; AGH itself is no longer contiguous
            )
            o, n = rd.pop("_own")
            rd["own"] = (o + base, n)
            rd["send"] = {
                p: (off + base, nb) for p, (off, nb) in rd.pop("_send").items()
            }
            rd["topo_send"] = {
                p: (off + base, nb) for p, (off, nb) in rd.pop("_topo_send").items()
            }
        # ---- Pass D: one absolute, stable slot per (peer, round parity) in the shared GRECV bank ----
        grecv_max = {
            (p, slot): max(
                (
                    rd["_grecv_nb"].get(p, 0)
                    for ri, rd in enumerate(rounds)
                    if ri % depth == slot
                ),
                default=0,
            )
            for p in sorted(peer_set)
            for slot in range(depth)
        }
        bank = depth * S
        grecv_slot: dict[tuple[int, int], tuple[int, int]] = {}
        for p in sorted(peer_set):
            for slot in range(depth):
                slot_nb = grecv_max[(p, slot)]
                if slot_nb:
                    grecv_slot[(p, slot)] = (bank, slot_nb)
                    bank += slot_nb
        for ri, rd in enumerate(rounds):
            sizes = rd.pop("_grecv_nb")
            rd["grecv"] = {
                p: (grecv_slot[(p, ri % depth)][0], nb) for p, nb in sizes.items()
            }
        return rounds, S, sorted(peer_set)

    def rollout_rdma_bytes(self, peer_ip: dict[int, str]) -> list[int]:
        """Registered permanent bytes for every rollout worker under the current round plan.

        This mirrors :meth:`WBEndpoint._setup_rdma_buffers`: isolated RECV plus PREP/GRECV (or their merged
        allocation) and all registered control-flag banks. DOFF is intentionally excluded because it is a
        CUDA-IPC-only, non-RDMA allocation. The result is topology- and placement-aware.
        """
        total_started = time.perf_counter()
        if os.environ.get("WBRIDGE_REPLICA_RELAY", "0") == "1":
            return self.relay_rollout_rdma_bytes(peer_ip)

        depth = 2 if os.environ.get("WBRIDGE_RECV_PIPELINE", "1") == "1" else 1
        merged = os.environ.get("WBRIDGE_MERGED_RECV_PREP", "0") == "1"
        topo_enabled = os.environ.get("WBRIDGE_TOPO_EXCHANGE", "1") == "1"
        topology_started = time.perf_counter()
        configured = bool(topo_enabled and self.configure_topology(peer_ip))
        if (
            configured
            and not SAME_NODE_IPC
            and any(
                self.topology_plan(member, ri)["internal"]
                for member in range(self.receiver_ws)
                for ri in range(len(self.global_rounds))
            )
        ):
            self._topology_ok = False
            configured = False
        if merged and (depth != 2 or not configured):
            raise ValueError(
                "rollout RDMA-cap planning with WBRIDGE_MERGED_RECV_PREP=1 requires "
                "depth-2 topology exchange"
            )

        topology_seconds = time.perf_counter() - topology_started
        invariant_started = time.perf_counter()
        self._ensure_round_invariants()
        assert self._trainer_peer_counts_cache is not None
        trainer_peer_counts = self._trainer_peer_counts_cache
        invariant_seconds = time.perf_counter() - invariant_started

        out = []
        rounds = len(self.global_rounds)
        arena_started = time.perf_counter()
        for receiver_local_rank in range(self.receiver_ws):
            layout, prep_stride, class_peers = self.arena_layout(
                receiver_local_rank,
                depth=depth,
            )
            recv_bytes = _arena_recv_total_bytes(layout, depth)
            if merged:
                slot_stride = _merge_recv_prep_layout(layout, depth, prep_stride)
                arena_bytes = _arena_total_bytes(layout, depth, slot_stride)
                data_bytes = arena_bytes
            else:
                arena_bytes = _arena_total_bytes(layout, depth, prep_stride)
                data_bytes = recv_bytes + arena_bytes
            # Base peer flags: incoming + source. Replica exchange: READY and consumed, each with incoming
            # + source. Every bank owns one int64 word per (peer, round).
            flag_bytes = (
                max(trainer_peer_counts[receiver_local_rank], 1) * rounds * 2 * 8
                + len(class_peers) * rounds * 4 * 8
            )
            out.append(data_bytes + flag_bytes)
        arena_seconds = time.perf_counter() - arena_started
        self._last_rollout_rdma_timing = {
            "topology_seconds": topology_seconds,
            "invariant_cache_seconds": invariant_seconds,
            "arena_seconds": arena_seconds,
            "total_seconds": time.perf_counter() - total_started,
        }
        return out

    def relay_rollout_rdma_bytes(self, peer_ip: dict[int, str]) -> list[int]:
        """Registered permanent bytes for each rollout worker in replica-relay mode.

        This mirrors :meth:`WBEndpoint._setup_relay_buffers`. Every node representative owns two registered
        PREP buffers per represented group. Downstream writes land directly in PREP. At a head the same PREP
        parity is temporarily interpreted as stable per-trainer ingress lanes, then snapshotted and assembled
        in place; its allocation is therefore the maximum of the canonical payload and the lane bank. DOFF
        consumption shadows and head assembly scratch are epoch/local CUDA allocations and intentionally do
        not count toward permanent registered memory. All workers also register the four DATA/ACK flag banks.
        """
        if not self.configure_relay(peer_ip):
            raise ValueError("rollout RDMA-cap planning found no replica-relay groups")

        rounds = len(self.global_rounds)
        world_size = self.sender_ws + self.receiver_ws
        flag_bytes = 4 * world_size * len(self._relay_groups) * rounds * 8
        out = [flag_bytes for _ in range(self.receiver_ws)]

        for group in self._relay_groups:
            parity_payload_max = [0, 0]
            for ri, spec in enumerate(group["round_specs"]):
                parity_payload_max[ri % 2] = max(
                    parity_payload_max[ri % 2],
                    spec.nbytes(self.dtype_spec),
                )

            for owner in group["owners"]:
                if self.sender_ws + owner == group["head"]:
                    lane_max = [
                        {sender: 0 for sender in range(self.sender_ws)}
                        for _ in range(2)
                    ]
                    for ri, trainer_specs in enumerate(group["trainer_specs"]):
                        for sender, spec in trainer_specs.items():
                            lane_max[ri % 2][sender] = max(
                                lane_max[ri % 2][sender],
                                spec.nbytes(self.dtype_spec),
                            )
                    head_lane_bytes = [sum(lanes.values()) for lanes in lane_max]
                else:
                    head_lane_bytes = [0, 0]
                prep_bytes = sum(
                    max(payload, lanes, 1)
                    for payload, lanes in zip(parity_payload_max, head_lane_bytes)
                )
                out[owner] += prep_bytes
        return out

    def _rounds_for_rollout_rdma_cap(
        self,
        name_send: dict[str, list[int]],
        name_recv: dict[str, list[int]],
        peer_ip: dict[int, str],
        cap: int,
    ) -> tuple[list[set[str]], int]:
        """Binary-search the smallest balanced round count whose rollout peak fits ``cap``."""
        max_rounds = len(self.dtype_spec)
        if max_rounds < 1:
            raise ValueError("cannot plan an empty dtype specification")
        cache: dict[int, tuple[list[set[str]], int]] = {}

        def evaluate(num_rounds: int) -> tuple[list[set[str]], int]:
            cached = cache.get(num_rounds)
            if cached is not None:
                return cached
            probe_started = time.perf_counter()
            pack_started = probe_started
            planned = self._pack_exact_rounds(name_send, name_recv, num_rounds)
            pack_seconds = time.perf_counter() - pack_started
            self.global_rounds = planned
            rdma_started = time.perf_counter()
            peak = max(self.rollout_rdma_bytes(peer_ip), default=0)
            rdma_seconds = time.perf_counter() - rdma_started
            probe_seconds = time.perf_counter() - probe_started
            timing: dict[str, float | int] = {
                "rounds": num_rounds,
                "pack_seconds": pack_seconds,
                "rdma_seconds": rdma_seconds,
                "probe_seconds": probe_seconds,
                **self._last_rollout_rdma_timing,
            }
            self.planner_probe_timings.append(timing)
            cache[num_rounds] = (planned, peak)
            logger.info(
                "wbridge router RDMA-cap probe: R=%d peak=%.3f GiB cap=%.3f GiB fit=%s "
                "time=%.3fs (pack=%.3fs topology=%.3fs arena=%.3fs)",
                num_rounds,
                peak / 1024**3,
                cap / 1024**3,
                peak <= cap,
                probe_seconds,
                pack_seconds,
                self._last_rollout_rdma_timing.get("topology_seconds", 0.0),
                self._last_rollout_rdma_timing.get("arena_seconds", 0.0),
            )
            return planned, peak

        planned, peak = evaluate(1)
        if peak <= cap:
            self.global_rounds = planned
            return planned, peak

        # Exponentially bracket the transition before binary search. Starting by materializing the maximum
        # one-tensor-per-round topology is needlessly expensive for large models when a small R already fits.
        low, high = 1, min(2, max_rounds)
        while True:
            _planned, high_peak = evaluate(high)
            if high_peak <= cap:
                break
            if high == max_rounds:
                raise ValueError(
                    "rollout RDMA cap is infeasible: even one tensor per round needs "
                    f"{high_peak / 1024**3:.3f} GiB, cap is {cap / 1024**3:.3f} GiB"
                )
            low = high
            high = min(2 * high, max_rounds)

        low += 1
        while low < high:
            middle = (low + high) // 2
            _planned, peak = evaluate(middle)
            if peak <= cap:
                high = middle
            else:
                low = middle + 1

        selected = low
        planned, peak = evaluate(selected)
        # Registered-buffer size is expected to decrease with finer balanced rounds. Guard the boundary
        # explicitly: if parity packing produced a local non-monotonicity, walk down to the true adjacent
        # transition rather than silently wasting rounds.
        while selected > 1:
            prior_plan, prior_peak = evaluate(selected - 1)
            if prior_peak > cap:
                break
            selected -= 1
            planned, peak = prior_plan, prior_peak

        self.global_rounds = planned
        peak = max(self.rollout_rdma_bytes(peer_ip), default=0)
        assert peak <= cap
        return planned, peak

    def cpu_recv_layout(self, rl: int):
        """Full-depth CPU RECV layout for receiver local index *rl* (RS-on; pure, no CUDA).

        RS-off's GPU RECV lanes live in the isolated parity slot (``[slot*R, (slot+1)*R)``). Under
        receiver-staging the standalone receiving thread lands EVERY round's data in CPU before the main
        thread's GPU consume, so the CPU landing arena must be **full-depth**: a distinct slot per
        ``(round, sender)``, laid contiguously across all rounds. The senders' per-round RECV byte layout
        WITHIN a round has the same per-sender overlap sizes as :meth:`arena_layout`'s ``recv``; the CPU arena
        remains compact and advances round-to-round instead of using stable GPU sender lanes. Returns
        ``(rounds_recv, total)`` where ``rounds_recv[ri] = {si: (abs_off, nb)}`` are absolute offsets into a
        ``total``-byte CPU buffer. Deterministic (sorted names/senders)."""
        my = self.recv_specs[rl]
        ov_full = {
            si: ShardSpec.compute_overlap(self.send_specs[si], my)
            for si in range(self.sender_ws)
        }
        rounds_recv: list[dict[int, tuple[int, int]]] = []
        off = 0
        for names in self.global_rounds:
            round_names = sorted(n for n in names if n in my.entries)
            recv: dict[int, tuple[int, int]] = {}
            for si in range(self.sender_ws):
                nb = ov_full[si].subset(set(round_names)).nbytes(self.dtype_spec)
                if nb:
                    recv[si] = (off, nb)
                    off += nb
            rounds_recv.append(recv)
        return rounds_recv, off

    def _dedup_diag(self) -> None:
        """Quantify how much the per-tensor receiver dedup (the implemented arena path) saves vs a
        hypothetical whole-spec (worker-level) dedup. For each tensor, the trainer->rollout receive traffic
        is ``sum_w bytes_w / class_size_w``: whole-spec uses the worker's full-spec class size, per-tensor
        uses that tensor's own replication-class size. Logs total savings + the top contributing tensors.
        Gated by WBRIDGE_DEDUP_DIAG=1, run once on rank 0. Pure read-only accounting (no transfer change)."""
        specs = self.recv_specs_full
        n = len(specs)

        def whole_key(s):
            return tuple(
                sorted((nm, _shards_canonical_key(sh)) for nm, sh in s.entries.items())
            )

        ws_classes: dict = {}
        for w in range(n):
            ws_classes.setdefault(whole_key(specs[w]), []).append(w)
        m_ws = {
            w: len(g) for g in ws_classes.values() for w in g
        }  # whole-spec class size per worker

        names = sorted({nm for s in specs for nm in s.entries})
        tot_ws = tot_pt = 0.0
        rows = []
        for name in names:
            holders = [w for w in range(n) if name in specs[w].entries]
            pt: dict = {}
            for w in holders:
                pt.setdefault(_shards_canonical_key(specs[w][name]), []).append(w)
            c_t = {
                w: len(g) for g in pt.values() for w in g
            }  # per-tensor class size per worker
            ws_b = pt_b = 0.0
            for w in holders:
                b = specs[w].subset([name]).nbytes(self.dtype_spec)
                ws_b += b / m_ws[w]
                pt_b += b / c_t[w]
            tot_ws += ws_b
            tot_pt += pt_b
            rows.append(
                (
                    ws_b - pt_b,
                    name,
                    sorted({m_ws[w] for w in holders}),
                    sorted(set(c_t.values())),
                    ws_b,
                )
            )

        save = tot_ws - tot_pt
        logger.info(
            "wbridge dedup-diag: recv traffic whole-spec=%.2f GiB, per-tensor=%.2f GiB, "
            "savings=%.2f GiB (%.1f%%) over %d tensors, %d receivers",
            tot_ws / 1024**3,
            tot_pt / 1024**3,
            save / 1024**3,
            100 * save / tot_ws if tot_ws else 0.0,
            len(names),
            n,
        )
        rows.sort(reverse=True)
        for sv, name, mws, cc, _wsb in rows[:12]:
            if sv <= 1024**2:  # skip < 1 MiB
                break
            logger.info(
                "  dedup-diag save=%.3f GiB  whole-spec m=%s -> per-tensor c=%s  %s",
                sv / 1024**3,
                mws,
                cc,
                name,
            )

        # Consolidation effectiveness: one group per worker remains a useful low-fanout target, but is no
        # longer a topology precondition. Overlapping groups are eligible when each group is node-balanced.
        classes = self.recv_tensor_classes()
        worker_groups: dict[int, set] = {}
        for cls in classes.values():
            for c in cls:
                if len(c) >= 2:
                    for rl in c:
                        worker_groups.setdefault(rl, set()).add(tuple(c))
        n_one = sum(1 for gs in worker_groups.values() if len(gs) == 1)
        n_multi = sum(1 for gs in worker_groups.values() if len(gs) > 1)
        logger.info(
            "wbridge dedup-diag: one-group-per-worker: %d receiver(s) in exactly one multi-worker "
            "group, %d in >1 (multi-group topology), %d in none (0-peer)",
            n_one,
            n_multi,
            n - n_one - n_multi,
        )


class WBEndpoint:
    """Shared RDMA endpoint base for Trainer Worker senders and Rollout Worker receivers.

    Weight data moves by **one-sided RDMA writes** (:class:`~wbridge.backend.rdma.base.RdmaEngine`), never
    a process-group collective. At connect, a one-time CPU **Gloo** group is used only to
    ``all_gather_object`` the routing specs, engine session ids, and registered buffer addresses; it
    carries no weight data. Each (sender, receiver) pair then ping-pongs per round over dedicated,
    pre-registered buffers: the sender writes the packed chunk then a monotonic "done" flag; the receiver
    polls that flag, consumes, then writes a "consumed" flag the sender polls before reusing the buffer.
    """

    cuda_device: str
    shard_spec: ShardSpec
    dtype_spec: dict[str, torch.dtype]
    load_spec: (
        LoadSpec  # HF<->worker mapping (set by subclass); drives the fused copy plan
    )
    wksd: dict[
        str, torch.Tensor
    ]  # live parameter tensors (set by subclass); stable addresses across WTs
    router: WeightRouter | None = None
    group: dist.ProcessGroup | None = (
        None  # one-time Gloo metadata group; unused during transfer
    )
    engine: RdmaEngine | None = None
    # In-process GPU<->CPU staging "engine" (built only when a staging switch is on). SS uses it to offload
    # the packed wire buffer to CPU before the RDMA write; RS uses it to load the CPU-landed bytes into the
    # GPU arena before assemble. None when both staging switches are off (the default path).
    local_engine: LocalStagingEngine | None = None
    device: str | None = None
    session: str = ""

    # Staging switches (set by the subclass before set_up_connection). Independent: SS offloads the sender's
    # packed bytes to CPU before RDMA; RS lands the receiver's incoming bytes in CPU before the GPU load.
    sender_staging: bool = False
    receiver_staging: bool = False

    # Flag slots hold monotonic int64 sequence numbers.
    _FLAG_DTYPE = torch.int64
    _FLAG_ITEMSIZE = 8
    _RELAY_DATA_KIND = 3
    _RELAY_ACK_KIND = 4
    _RELAY_SEQ_BITS = 48

    # The sender's per-peer pack buffers (`_data_buf`) are double-buffered by round parity (ri % _NUM_BUF)
    # to pipeline rounds: with 2 buffers the sender keeps one round's RDMA writes in flight while packing the
    # next, which is what fills the fabric. Depth 2 is the validated sweet spot — higher plateaus/regresses
    # and costs ~N/2× the pack memory on the trainer. The receiver independently selects an arena depth;
    # both sides address it by global-round parity.
    _NUM_BUF = 2

    def _init_profile_output(self) -> None:
        """Initialize the per-epoch gate that keeps profiling output after report ``block_end``.

        Sender completion is asynchronous: the application may record ``block_end`` before the Stage-2
        thread has produced the final spans.  Receivers have the inverse ordering.  The pending/released
        handshake handles both without making either side wait, and every epoch contributes exactly one
        immutable output callback.
        """
        if hasattr(self, "_profile_output_lock"):
            return
        self._profile_output_lock = threading.Lock()
        self._profile_output_pending: dict[tuple[int, int], Callable[[], None]] = {}
        self._profile_output_released: set[tuple[int, int]] = set()
        self._profile_output_generation = -1

    def _profile_output_key(self, epoch: int) -> tuple[int, int]:
        return self._profile_output_generation, epoch

    @staticmethod
    def _emit_profile_output(output: Callable[[], None]) -> None:
        """Best-effort profiling output must never turn a successful transfer into a failure."""
        try:
            output()
        except Exception:  # noqa: BLE001 - diagnostics are intentionally non-fatal
            logger.exception("wbridge: failed to emit deferred profiling output")

    def _defer_profile_output(self, epoch: int, output: Callable[[], None]) -> None:
        """Publish a complete epoch snapshot, emitting now only if ``block_end`` was already released."""
        key = self._profile_output_key(epoch)
        with self._profile_output_lock:
            if key in self._profile_output_released:
                self._profile_output_released.remove(key)
                emit = True
            else:
                if key in self._profile_output_pending:
                    raise RuntimeError(
                        f"duplicate profiling snapshot for epoch {epoch}"
                    )
                self._profile_output_pending[key] = output
                emit = False
        if emit:
            self._emit_profile_output(output)

    def flush_profile_outputs(self, epoch: int | None = None) -> None:
        """Release one epoch's captured output after the caller records its report ``block_end``.

        This method intentionally does not wait for an asynchronous sender epoch to finish.  If its final
        snapshot is not ready yet, the Stage-2 thread emits it after publishing transfer completion.
        """
        if epoch is None:
            epoch = getattr(self, "_epoch", 0) - 1
        if epoch < 0:
            return
        key = self._profile_output_key(epoch)
        with self._profile_output_lock:
            output = self._profile_output_pending.pop(key, None)
            if output is None:
                self._profile_output_released.add(key)
        if output is not None:
            self._emit_profile_output(output)

    def _take_profile_output(
        self,
        epoch: int,
        nrounds: int,
        extra_outputs: tuple[Callable[[], None], ...] = (),
    ) -> Callable[[], None]:
        """Snapshot Gantt/control profiling in memory and return its deferred output callback."""
        # Every producer that records profile events is quiescent at the two call sites. Async flag-handle
        # reaping may continue, but deliberately records no spans and cannot contaminate the next snapshot.
        # No file or logger output occurs while taking either snapshot.
        from wbridge.backend import gantt

        events = gantt.take()
        ctl_lines = self._ctl_take_report(epoch, nrounds)

        def output() -> None:
            gantt.dump(events)
            for line in ctl_lines:
                logger.warning("%s", line)
            for emit in extra_outputs:
                emit()

        return output

    def _capture_profile_outputs(
        self,
        epoch: int,
        nrounds: int,
        extra_outputs: tuple[Callable[[], None], ...] = (),
    ) -> None:
        self._defer_profile_output(
            epoch, self._take_profile_output(epoch, nrounds, extra_outputs)
        )

    def _trace_state(self, stage: str, **fields) -> None:
        """Emit one machine-readable protocol state transition for deadlock reconstruction.

        This is deliberately gated by ``WBRIDGE_TOPO_DEBUG`` and has no synchronization side effects.  The
        thread name distinguishes the receiver main/EC lanes and the sender main/background lanes.  A hang
        can therefore be reconstructed by taking the last ``WBSTATE`` record for every ``(rank, thread)``.
        """
        if os.environ.get("WBRIDGE_TOPO_DEBUG") != "1":
            return
        record = {
            "rank": getattr(self, "_rank", getattr(self, "rank", -1)),
            "role": "sender" if getattr(self, "_is_sender", False) else "receiver",
            "thread": threading.current_thread().name,
            "stage": stage,
            **fields,
        }
        print(
            "WBSTATE " + json.dumps(record, sort_keys=True, separators=(",", ":")),
            flush=True,
        )

    # ---------------------------------------------------------------- connect
    def set_up_connection(self, **pg_args) -> None:
        """Bring up the Gloo metadata group + RDMA engine, then exchange routing + buffer metadata."""
        protocol = pg_args.pop("protocol", "efa")
        sender_ws = pg_args.pop("sender_world_size")
        rank = pg_args["rank"]
        ws = pg_args["world_size"]
        init_method = pg_args["init_method"]
        group_name = pg_args.get("group_name", "wbridge")

        self._init_profile_output()
        self._profile_output_generation += 1
        self._teardown()
        self.device = self.cuda_device
        self.group = init_custom_process_group(
            backend="gloo",
            init_method=init_method,
            world_size=ws,
            rank=rank,
            group_name=f"{group_name}-meta",
        )
        self._init_engine(protocol)
        self._finish_setup(rank, ws, sender_ws)

    def _init_engine(self, protocol: str) -> None:
        # The selected network implementation stays behind the RdmaEngine interface. Mooncake uses a
        # composite engine for its network and optional same-node NVLink transports; Monarch supplies its
        # libibverbs implementation directly.
        if protocol == "monarch":
            # Lazy import: pulling in monarch loads a large native extension, and it is only importable
            # inside a Monarch actor process. See rdma/monarch.py for why this backend exists.
            from wbridge.backend.rdma.monarch import MonarchEngine

            engine: RdmaEngine = MonarchEngine()
            engine.init(get_local_ip(), protocol)
            self.engine = engine
            self.session = engine.session_id()
            self.local_engine = None
            if self.sender_staging or self.receiver_staging:
                raise RuntimeError(
                    "protocol='monarch' does not support sender/receiver staging yet"
                )
            return
        engine: RdmaEngine = DualMooncakeEngine()
        # Supplying a transport-capable Mooncake build is the deployment environment's responsibility.
        # Under staging, host DRAM is on the network RDMA path. For Mooncake/EFA, pin each rank to a NIC
        # local to its GPU so concurrent ranks do not funnel through the same device. The both-OFF
        # GPU-direct path carries its own GPU-to-NIC affinity and does not request this host-memory pin.
        engine.init(
            get_local_ip(),
            protocol,
            "",
            pin_local_nic=(self.sender_staging or self.receiver_staging),
        )
        self.engine = engine
        self.session = engine.session_id()
        # A staging endpoint additionally runs an in-process GPU<->CPU engine (D2H for SS, H2D for RS). A
        # process is exclusively a sender or a receiver, so at most one switch is ever set here.
        self.local_engine = None
        if self.sender_staging or self.receiver_staging:
            le = LocalStagingEngine()
            le.init(get_local_ip(), protocol, "")
            self.local_engine = le

    def transport_stats(self) -> dict:
        """Bulk weight bytes moved since connect, split by transport and by leg.

        ``wire_*`` is the trainer->rollout leg (bytes this rank sent, or landed, depending on role) and
        ``agh_*`` the rollout<->rollout dedup exchange. ``ipc`` counts direct CUDA-IPC copies over NVLink,
        ``rdma`` counts the selected :class:`RdmaEngine`. Flag traffic is excluded: cross-node flags use it
        or the optional host TCP control plane, while same-node replica sequences use a shared CPU bank. A
        co-located deployment should report ``*_rdma_bytes == 0``; that assertion is what
        :mod:`examples.train` ``--colocate`` checks.
        """
        st = dict(getattr(self, "_tstats", _EMPTY_TSTATS))
        st["role"] = "sender" if getattr(self, "_is_sender", False) else "receiver"
        st["rank"] = getattr(self, "_rank", -1)
        st["same_node_peers"] = sorted(getattr(self, "_same_node_peers", []))
        st["ipc_peers"] = sorted(
            getattr(self, "_sn_peers" if st["role"] == "sender" else "_sn_senders", ())
        )
        st["rdma_peers"] = sorted(
            set(getattr(self, "peers", [])) - set(st["ipc_peers"])
        )
        return st

    def _ensure_relay_bulk_waiters(self, edges: set[tuple[int, int]]) -> None:
        """Create one persistent completion/DATA publisher per ``(destination, group)`` edge."""
        if not hasattr(self, "_relay_bulk_wait_lock"):
            self._relay_bulk_wait_lock = threading.Lock()
            self._relay_bulk_wait_queues = {}
            self._relay_bulk_wait_threads = {}
        with self._relay_bulk_wait_lock:
            for edge in sorted(edges):
                if edge in self._relay_bulk_wait_threads:
                    continue
                peer, gid = edge
                work_q: queue.Queue = queue.Queue()
                thread = threading.Thread(
                    target=self._relay_bulk_waiter,
                    args=(peer, gid, work_q),
                    name=f"wbridge-relay-wire-{peer}-g{gid}",
                    daemon=True,
                )
                self._relay_bulk_wait_queues[edge] = work_q
                self._relay_bulk_wait_threads[edge] = thread
                thread.start()

    def _relay_bulk_waiter(self, peer: int, gid: int, work_q: queue.Queue) -> None:
        while True:
            task = work_q.get()
            if task is None:
                return
            wt, ri, seq, handle, submitted_at, leg, result_q = task
            error: BaseException | None = None
            landed_at = submitted_at
            try:
                if handle is not None:
                    self.engine.wait([handle])
                    landed_at = time.time()
                # DATA publication is the data-before-flag fence for this exact group/round edge.
                self._relay_emit(self._RELAY_DATA_KIND, peer, gid, seq)
                from wbridge.backend import gantt

                gantt.rec(
                    "relay-wire",
                    self._rank,
                    wt,
                    f"{leg}_peer_{peer}_group_{gid}",
                    ri,
                    submitted_at,
                    landed_at,
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced by the owning progress loop
                error = exc
            result_q.put((peer, gid, ri, seq, error, landed_at))

    def _stop_relay_bulk_waiters(self) -> None:
        if not hasattr(self, "_relay_bulk_wait_lock"):
            return
        with self._relay_bulk_wait_lock:
            queues = list(self._relay_bulk_wait_queues.values())
            threads = list(self._relay_bulk_wait_threads.values())
            self._relay_bulk_wait_queues = {}
            self._relay_bulk_wait_threads = {}
        for work_q in queues:
            work_q.put(None)
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=5.0)

    def _teardown(self) -> None:
        """Close the engine (unregisters buffers) and destroy the metadata group, for reconnects."""
        self._stop_relay_bulk_waiters()
        tcp_control = getattr(self, "_tcp_control", None)
        if tcp_control is not None:
            tcp_control.close()
        self._tcp_control = None
        # Async flag handles reference both the engine and registered source words. Retire them before either
        # is torn down. This is lifecycle cleanup only; normal epoch completion never waits here.
        self._flag_reaper_stop()
        for mappings in (
            getattr(self, "_repl_peer_ipc_mapping", {}),
            getattr(self, "_repl_peer_doff_ipc_mapping", {}),
            getattr(self, "_relay_peer_ipc_mapping", {}),
        ):
            for device, allocation_base in mappings.values():
                with contextlib.suppress(Exception):
                    _close_cuda_ipc_mapping(device, allocation_base)
        for mapping_list in getattr(self, "_peer_pack_ipc_mapping", {}).values():
            for device, allocation_base in mapping_list:
                with contextlib.suppress(Exception):
                    _close_cuda_ipc_mapping(device, allocation_base)
        for bank in getattr(self, "_repl_peer_local_flags", {}).values():
            bank.close()
        for bank in getattr(self, "_relay_peer_local_flags", {}).values():
            bank.close()
        own_bank = getattr(self, "_repl_local_flags", None)
        if own_bank is not None:
            own_bank.close()
        relay_bank = getattr(self, "_relay_local_flags", None)
        if relay_bank is not None:
            relay_bank.close()
        self._repl_peer_local_flags = {}
        self._repl_local_flags = None
        self._relay_peer_local_flags = {}
        self._relay_local_flags = None
        if self.engine is not None:
            self.engine.close()
            self.engine = None
        if self.local_engine is not None:
            self.local_engine.close()
            self.local_engine = None
        if self.group is not None:
            try:
                dist.destroy_process_group(self.group)
            except Exception:
                pass
            self.group = None
        self._data_buf: dict[int, torch.Tensor] = {}
        self._relay_send_buf = {}
        self._relay_prep_buf = {}
        self._relay_doff_buf = {}
        self._relay_peer_doff = {}
        self._relay_peer_kernel_base = {}
        self._relay_peer_ipc_mapping = {}
        self._relay_peer_ready_event = {}
        self._relay_prepare_plan = {}
        self._relay_offload_stream = {}
        self._relay_offload_event = {}
        self._relay_consume_plan = {}
        self._cpu_grid: dict[
            int, list[torch.Tensor | None]
        ] = {}  # SS: per-(peer, round) CPU offload
        self._cpu_recv = None  # RS: full-depth CPU receive arena
        self._flag_buf = None
        self._flag_src = None
        # Drop same-node CUDA-IPC state so a reconnect re-imports against the freshly reallocated arena (torch
        # GC closes the imported mem/event handles). Guarded — these exist only for receivers with class peers.
        self._repl_peer_arena = {}
        self._repl_peer_kernel_base = {}
        self._repl_peer_ipc_mapping = {}
        self._repl_peer_doff_arena = {}
        self._repl_peer_doff_kernel_base = {}
        self._repl_peer_doff_ipc_mapping = {}
        self._repl_peer_ready_event = {}
        self._repl_peer_doff_ready_event = {}
        self._repl_ready_event = {}
        self._topo_slot_ready_event = {}
        self._repl_peer_topo_slot_ready_event = {}
        self._topo_local_slot_channel = {}
        self._topo_peer_slot_channel = {}
        self._repl_same_node = set()
        self._repl_peer_grecv_off = {}
        self._repl_peer_send_off = {}  # PULL: per-round offset of peer's send[me] within peer's arena
        self._repl_peer_slot_of_me = {}
        # Topology-aware exchange state (rebuilt per connect against fresh arenas). See the per-peer
        # resolve block + WeightRouter.configure_topology.
        self._topo_ok = False
        self._topo_structure_ok = False
        self._topo_ext_peers = []  # subgroup cross-node PUSH peers (phase 1)
        self._topo_int_peers = []  # same-node source columns consumed directly over CUDA IPC
        # peer -> per-round (external slot source or None, src_off, packed spec, selected names)
        self._topo_internal_consume_src = {}
        self._topo_internal_consume_bytes = {}
        self._topo_internal_consume_bytes_by_lane = []
        self._topo_ext_send_peers_by_round = []
        self._topo_ext_recv_peers_by_round = []
        self._topo_int_peers_by_round = []
        self._topo_int_readers_by_round = []
        self._topo_ext_release_readers_by_round = []
        self._topo_ext_xfer = {}
        self._topo_peer_predecessors = []
        self._topo_internal_consume_plan = []
        self._topo_internal_consume_own_lane = []
        self._topo_internal_consume_stream = {}
        self._topo_internal_consume_event = {}
        self._topo_internal_consume_lane_sources = []
        self._topo_internal_consume_source_lane = []
        self._direct_consume_plan = []
        self._doff_copy_stream = {}
        self._doff_copy_event = {}
        self._doff_slot_released = {}
        # Same-node trainer->rollout CUDA-IPC bypass (see the export/import blocks in _setup_rdma_buffers).
        # Dropped here so a reconnect re-exports/re-imports against freshly allocated buffers.
        self._same_node_peers: list[
            int
        ] = []  # peers on this host, by the session-gather's IPs
        self._sn_peers: set[int] = (
            set()
        )  # sender: receivers that PULL our pack buffers (no RDMA)
        self._sn_senders: set[int] = (
            set()
        )  # receiver: senders whose pack buffers we PULL
        self._pack_ready_event = {}  # sender: per-receiver exported "pack landed" event
        self._peer_pack_buf = {}  # receiver: {sender: [imported pack buffer per parity]}
        self._peer_pack_kernel_base = {}  # receiver: raw peer mappings for direct consume kernels
        self._peer_pack_ipc_mapping = {}  # receiver: mappings closed explicitly on reconnect
        self._peer_pack_event = {}  # receiver: {sender: imported pack-landed event}
        self._peer_pack_num_buf = {}  # receiver: {sender: its pack-buffer parity depth}
        self._direct_same_node = False
        self._tstats = dict(_EMPTY_TSTATS)

    def _finish_setup(self, rank: int, ws: int, sender_ws: int) -> None:
        """Exchange specs (build router), then allocate/register buffers and exchange their addresses."""
        grp = self.group
        all_shard_specs: list = [None] * ws
        dist.all_gather_object(all_shard_specs, self.shard_spec, group=grp)

        all_dtype_specs: list = [None] * ws
        dist.all_gather_object(all_dtype_specs, self.dtype_spec, group=grp)
        # All workers plan independently below.  Start them from the same stable name order even though
        # each worker's local dtype map contains a different subset/insertion order.
        self.dtype_spec = _canonical_dtype_spec(all_dtype_specs)

        # Who is physically co-located with whom. Gathered BEFORE the big metadata exchange because the
        # same-node CUDA-IPC exports below (pack buffers + events) are built into that exchange, and we only
        # want to pay for them — and only risk reduce_tensor's allocator constraint — for peers on this host.
        all_sessions: list = [None] * ws
        dist.all_gather_object(all_sessions, self.session, group=grp)
        self._local_ip = get_local_ip()
        self._peer_ip = {
            r: DualMooncakeEngine._ip_of(DualMooncakeEngine._split(s)[0])
            for r, s in enumerate(all_sessions)
        }

        # Every worker has the same globally ordered specs, canonical dtype map, placement map, and planner
        # configuration.  Compute the deterministic global rounds independently so setup has no rank-0
        # planner/broadcast dependency; `rank` affects only the local-round projection after global planning.
        direct_requested = os.environ.get("WBRIDGE_DIRECT_SAME_NODE", "0") == "1"
        one_physical_node = len(set(self._peer_ip.values())) == 1
        direct_same_node = bool(
            direct_requested
            and SAME_NODE_IPC
            and one_physical_node
            and not self.sender_staging
            and not self.receiver_staging
            and os.environ.get("WBRIDGE_REPLICA_RELAY", "0") != "1"
        )
        if direct_requested and not direct_same_node and rank == 0:
            logger.warning(
                "WBRIDGE_DIRECT_SAME_NODE=1 requested but unavailable "
                "(one_node=%s same_node_ipc=%s sender_staging=%s receiver_staging=%s relay=%s); "
                "using the normal exchange path",
                one_physical_node,
                SAME_NODE_IPC,
                self.sender_staging,
                self.receiver_staging,
                os.environ.get("WBRIDGE_REPLICA_RELAY", "0"),
            )
        self.router = WeightRouter(
            rank,
            sender_ws,
            all_shard_specs,
            self.dtype_spec,
            peer_ip=self._peer_ip,
            direct_same_node=direct_same_node,
        )
        self._direct_same_node = direct_same_node
        self.num_rounds = len(self.router.local_rounds)
        self._is_sender = rank < sender_ws
        self._rank = rank
        self._epoch = 0
        self._relay_enabled = os.environ.get("WBRIDGE_REPLICA_RELAY", "0") == "1"
        if self._relay_enabled:
            if self.sender_staging or self.receiver_staging:
                raise RuntimeError(
                    "WBRIDGE_REPLICA_RELAY=1 does not support host staging"
                )
            if not SAME_NODE_IPC:
                raise RuntimeError(
                    "WBRIDGE_REPLICA_RELAY=1 requires WBRIDGE_SAME_NODE_IPC=1"
                )
            if not self.router.configure_relay(self._peer_ip):
                raise RuntimeError(
                    "replica-group relay found no routable replication groups"
                )
            self._setup_relay_buffers(rank, ws)
            self._build_relay_plans()
        else:
            self._setup_rdma_buffers(rank, ws)
            self._build_fuse_plans()
        self._trace_state("connected", rounds=self.num_rounds)
        logger.info(
            "wbridge rank %d connected: %d peers, %d rounds, session=%s",
            rank,
            len(self.peers),
            self.num_rounds,
            self.session,
        )

    def _setup_relay_buffers(self, rank: int, ws: int) -> None:
        """Allocate and resolve the replica-group relay protocol's depth-2 buffers.

        Senders own one SEND parity pair per group they contribute to. A node representative owns only one
        registered PREP parity pair per represented group: trainer lanes and downstream relay writes land
        directly there. Heads snapshot the lane image into epoch scratch before assembling back in place.
        Every representative additionally owns non-RDMA DOFF generations used by local/model consumers, so
        PREP lifetime ends after forwarding plus offload rather than after model consumption.
        """
        assert (
            self.engine is not None
            and self.router is not None
            and self.device is not None
        )
        self.world_size = ws
        self._relay_num_groups = len(self.router._relay_groups)
        if self._relay_num_groups == 0:
            raise RuntimeError("relay setup requires at least one group")
        self._recv_depth = 2
        self._NUM_BUF = 2
        if os.environ.get("WBRIDGE_RECV_PIPELINE", "1") != "1":
            raise RuntimeError(
                "WBRIDGE_REPLICA_RELAY requires depth-2 receiver buffers"
            )
        if int(os.environ.get("WBRIDGE_SENDER_NUM_BUF", "2")) != 2:
            raise RuntimeError(
                "WBRIDGE_REPLICA_RELAY requires WBRIDGE_SENDER_NUM_BUF=2"
            )

        self._ctlp = os.environ.get("WBRIDGE_CTL_PROFILE") == "1"
        self._ctl = {}
        self._flag_reaper_q = None
        self._flag_reaper_thread = None
        self._flag_reaper_lock = threading.Lock()
        self._flag_reaper_errors = []
        self._flag_submit_lock = threading.Lock()
        self._tstats = dict(_EMPTY_TSTATS)
        self._same_node_peers = []
        self._sn_peers = set()
        self._sn_senders = set()

        # Every control word is exclusive to (writer peer, group, global round).  DATA and ACK have separate
        # banks, and each bank has a matching immutable async-write source allocation.
        flag_words = ws * self._relay_num_groups * self.num_rounds
        self._relay_data_buf = torch.zeros(
            flag_words, dtype=self._FLAG_DTYPE
        ).pin_memory()
        self._relay_data_src = torch.zeros(
            flag_words, dtype=self._FLAG_DTYPE
        ).pin_memory()
        self._relay_ack_buf = torch.zeros(
            flag_words, dtype=self._FLAG_DTYPE
        ).pin_memory()
        self._relay_ack_src = torch.zeros(
            flag_words, dtype=self._FLAG_DTYPE
        ).pin_memory()
        for tensor in (
            self._relay_data_buf,
            self._relay_data_src,
            self._relay_ack_buf,
            self._relay_ack_src,
        ):
            self.engine.register(
                tensor.data_ptr(),
                tensor.numel() * self._FLAG_ITEMSIZE,
                is_flag=True,
                tensor=tensor,
            )

        sw = self.router.sender_ws
        rl = rank - sw
        self._relay_send_specs: list[dict[int, ShardSpec]] = [
            {} for _ in range(self.num_rounds)
        ]
        self._relay_send_buf: dict[int, list[torch.Tensor]] = {}
        self._relay_owned_gids: list[int] = []
        self._relay_consume_owner: dict[int, int] = {}
        self._relay_local_readers: dict[int, tuple[int, ...]] = {}
        self._relay_prep_buf: dict[int, list[torch.Tensor]] = {}
        self._relay_prep_offsets: dict[int, list[dict[int, int]]] = {}
        self._relay_prep_sizes: dict[int, list[int]] = {}
        self._relay_head_scratch_size: dict[int, int] = {}
        self._relay_doff_depth = int(os.environ.get("WBRIDGE_DOFF", "1"))
        if self._relay_doff_depth < 1:
            raise ValueError(f"WBRIDGE_DOFF must be >= 1, got {self._relay_doff_depth}")
        self._relay_doff_buf: dict[int, list[torch.Tensor]] = {}

        registered_destinations: list[torch.Tensor] = []
        total_send = total_prep = total_doff = 0
        if self._is_sender:
            for group in self.router._relay_groups:
                gid = group["id"]
                for ri in range(self.num_rounds):
                    spec = group["trainer_specs"][ri].get(rank)
                    if spec is not None:
                        self._relay_send_specs[ri][gid] = spec
                if not any(gid in by_group for by_group in self._relay_send_specs):
                    continue
                parity_max = [0, 0]
                for ri, by_group in enumerate(self._relay_send_specs):
                    if gid in by_group:
                        parity_max[ri % 2] = max(
                            parity_max[ri % 2],
                            by_group[gid].nbytes(self.dtype_spec),
                        )
                bufs = []
                for parity in range(2):
                    size = max(parity_max[parity], 1)
                    tensor = torch.zeros(size, dtype=torch.uint8, device=self.device)
                    self.engine.register(
                        tensor.data_ptr(), tensor.numel(), tensor=tensor
                    )
                    bufs.append(tensor)
                    total_send += tensor.numel()
                self._relay_send_buf[gid] = bufs
        else:
            for group in self.router._relay_groups:
                gid = group["id"]
                if rl in group["members"]:
                    self._relay_consume_owner[gid] = sw + group["owner_of"][rl]
                if rank not in group["chain"]:
                    continue
                self._relay_owned_gids.append(gid)
                local_members = group["local_readers"][rl]
                self._relay_local_readers[gid] = tuple(
                    sw + member for member in local_members if sw + member != rank
                )
                parity_payload_max = [0, 0]
                round_sizes = []
                for ri, spec in enumerate(group["round_specs"]):
                    size = spec.nbytes(self.dtype_spec)
                    round_sizes.append(size)
                    parity_payload_max[ri % 2] = max(parity_payload_max[ri % 2], size)
                self._relay_prep_sizes[gid] = round_sizes

                is_head = rank == group["head"]
                offsets_by_round: list[dict[int, int]] = [
                    {} for _ in range(self.num_rounds)
                ]
                ingress_parity_size = [0, 0]
                if is_head:
                    # Trainers land directly in PREP. Stable lanes within each parity prevent one trainer's
                    # contribution from aliasing another's. Once every lane lands, the whole ingress image
                    # is snapshotted to epoch scratch and assembled back into this same PREP allocation.
                    lane_max = [{si: 0 for si in range(sw)} for _ in range(2)]
                    for ri, trainer_specs in enumerate(group["trainer_specs"]):
                        for si, spec in trainer_specs.items():
                            lane_max[ri % 2][si] = max(
                                lane_max[ri % 2][si],
                                spec.nbytes(self.dtype_spec),
                            )
                    lane_off: list[dict[int, int]] = []
                    for parity in range(2):
                        cursor = 0
                        current = {}
                        for si in range(sw):
                            current[si] = cursor
                            cursor += lane_max[parity][si]
                        lane_off.append(current)
                        ingress_parity_size[parity] = cursor
                    for ri, trainer_specs in enumerate(group["trainer_specs"]):
                        offsets_by_round[ri] = {
                            si: lane_off[ri % 2][si] for si in trainer_specs
                        }
                else:
                    pred = group["chain"][group["chain"].index(rank) - 1]
                    for ri, size in enumerate(round_sizes):
                        if size:
                            offsets_by_round[ri] = {pred: 0}
                self._relay_prep_offsets[gid] = offsets_by_round

                prep_bufs: list[torch.Tensor] = []
                for parity in range(2):
                    prep_tensor = torch.zeros(
                        max(parity_payload_max[parity], ingress_parity_size[parity], 1),
                        dtype=torch.uint8,
                        device=self.device,
                    )
                    self.engine.register(
                        prep_tensor.data_ptr(),
                        prep_tensor.numel(),
                        tensor=prep_tensor,
                    )
                    registered_destinations.append(prep_tensor)
                    prep_bufs.append(prep_tensor)
                    total_prep += prep_tensor.numel()
                self._relay_prep_buf[gid] = prep_bufs
                if is_head:
                    self._relay_head_scratch_size[gid] = max(
                        (tensor.numel() for tensor in prep_bufs),
                        default=1,
                    )

                # Internal/model consumption reads a non-RDMA DOFF generation, never PREP. PREP therefore
                # becomes reusable as soon as its successor write and PREP->DOFF copy finish, independently
                # of local model-copy latency. DOFF defaults to depth one and is source-exclusive per group.
                doff_slot_max = [0] * self._relay_doff_depth
                for ri, size in enumerate(round_sizes):
                    doff_slot_max[ri % self._relay_doff_depth] = max(
                        doff_slot_max[ri % self._relay_doff_depth],
                        size,
                    )
                doff_bufs = [
                    torch.empty(max(size, 1), dtype=torch.uint8, device=self.device)
                    for size in doff_slot_max
                ]
                self._relay_doff_buf[gid] = doff_bufs
                total_doff += sum(tensor.numel() for tensor in doff_bufs)

        # Same-node DOFF READY/DONE bank. One channel per represented (group, DOFF slot), one consumed slot
        # per local reader. The paired CUDA IPC event is the visibility fence; PREP itself is never exposed to
        # readers and can be forwarded/reused without waiting for their model-copy kernels.
        self._relay_local_flags = None
        self._relay_local_channel: dict[tuple[int, int], int] = {}
        self._relay_local_slot_of: dict[int, int] = {}
        self._relay_ready_event: dict[tuple[int, int, int], torch.cuda.Event] = {}
        if not self._is_sender:
            local_reader_union = sorted(
                {
                    reader
                    for readers in self._relay_local_readers.values()
                    for reader in readers
                }
            )
            self._relay_local_slot_of = {
                reader: slot for slot, reader in enumerate(local_reader_union)
            }
            active_channels = [
                (gid, slot)
                for gid in self._relay_owned_gids
                if self._relay_local_readers.get(gid)
                for slot in range(self._relay_doff_depth)
            ]
            self._relay_local_channel = {
                key: channel for channel, key in enumerate(active_channels)
            }
            if local_reader_union:
                self._relay_local_flags = _LocalReplFlagBank.create(
                    len(local_reader_union),
                    channels=max(1, len(active_channels)),
                )
                for gid, slot in active_channels:
                    for reader in self._relay_local_readers[gid]:
                        self._relay_ready_event[(gid, slot, reader)] = torch.cuda.Event(
                            interprocess=True,
                        )

        doff_reduce: dict[int, list[tuple | None]] = {}
        doff_kernel_ipc: dict[int, list[dict | None]] = {}
        relay_event_ipc: dict[tuple[int, int], dict[int, bytes]] = {}
        ipc_device = -1

        def _kernel_ipc(reduced: tuple) -> dict:
            _rebuild, reduce_args = reduced
            storage_handle = bytes(reduce_args[7])
            if len(storage_handle) < 64:
                raise RuntimeError(
                    f"invalid relay CUDA IPC handle size {len(storage_handle)}"
                )
            itemsize = torch.empty((), dtype=reduce_args[5]).element_size()
            return {
                "handle": storage_handle[-64:],
                "tensor_offset_bytes": int(reduce_args[9])
                + int(reduce_args[3]) * itemsize,
            }

        if not self._is_sender and self._relay_local_flags is not None:
            with torch.cuda.device(self.device):
                for event in self._relay_ready_event.values():
                    event.record()
            ipc_device = torch.device(self.device).index
            for gid in self._relay_owned_gids:
                if not self._relay_local_readers.get(gid):
                    continue
                reductions = [
                    reduce_tensor(tensor) for tensor in self._relay_doff_buf[gid]
                ]
                doff_reduce[gid] = reductions
                doff_kernel_ipc[gid] = [_kernel_ipc(reduced) for reduced in reductions]
                for slot in range(self._relay_doff_depth):
                    relay_event_ipc[(gid, slot)] = {
                        reader: self._relay_ready_event[
                            (gid, slot, reader)
                        ].ipc_handle()
                        for reader in self._relay_local_readers[gid]
                    }

        self._tcp_control = None
        if os.environ.get("WBRIDGE_TCP_CONTROL", "0") == "1":
            self._tcp_control = TcpControlTransport(
                rank, self._local_ip, self._tcp_flag_landed
            )

        fi = self._FLAG_ITEMSIZE
        regions = [
            (tensor.data_ptr() + index * fi, fi)
            for tensor in (self._relay_data_buf, self._relay_ack_buf)
            for index in range(tensor.numel())
        ]
        regions.extend(
            (tensor.data_ptr(), tensor.numel()) for tensor in registered_destinations
        )
        self.engine.publish_regions(regions)

        head_prep: dict[int, list[dict[int, int]]] = {}
        chain_prep: dict[int, list[int | None]] = {}
        if not self._is_sender:
            for gid in self._relay_owned_gids:
                group = self.router.relay_group(gid)
                if rank == group["head"]:
                    head_prep[gid] = [
                        {
                            si: self._relay_prep_buf[gid][ri % 2].data_ptr() + off
                            for si, off in self._relay_prep_offsets[gid][ri].items()
                        }
                        for ri in range(self.num_rounds)
                    ]
                else:
                    chain_prep[gid] = [
                        self._relay_prep_buf[gid][ri % 2].data_ptr()
                        if self._relay_prep_sizes[gid][ri]
                        else None
                        for ri in range(self.num_rounds)
                    ]

        info = {
            "session": self.session,
            "engine_payload": self.engine.publish_payload(),
            "tcp_control_endpoint": (
                self._tcp_control.endpoint if self._tcp_control is not None else None
            ),
            "relay_groups": self._relay_num_groups,
            "relay_data_addr": self._relay_data_buf.data_ptr(),
            "relay_ack_addr": self._relay_ack_buf.data_ptr(),
            "relay_head_prep": head_prep,
            "relay_chain_prep": chain_prep,
            "relay_local_flags": (
                self._relay_local_flags.path
                if self._relay_local_flags is not None
                else ""
            ),
            "relay_local_slots": self._relay_local_slot_of,
            "relay_local_channels": self._relay_local_channel,
            "relay_doff_depth": self._relay_doff_depth,
            "relay_doff_reduce": doff_reduce,
            "relay_doff_kernel_ipc": doff_kernel_ipc,
            "relay_ready_event_ipc": relay_event_ipc,
            "device": ipc_device,
        }
        all_info: list[dict | None] = [None] * ws
        dist.all_gather_object(all_info, info, group=self.group)
        if any(
            item is None or item.get("relay_groups") != self._relay_num_groups
            for item in all_info
        ):
            raise RuntimeError(
                "replica-group relay configuration mismatch across ranks"
            )

        # Direct writer/reader graph for this rank.
        data_peers: set[int] = set()
        control_peers: set[int] = set()
        if self._is_sender:
            for ri, specs in enumerate(self._relay_send_specs):
                for gid in specs:
                    data_peers.add(self.router.relay_group(gid)["head"])
        else:
            for gid in self._relay_owned_gids:
                group = self.router.relay_group(gid)
                pos = group["chain"].index(rank)
                if pos == 0:
                    control_peers.update(
                        si for specs in group["trainer_specs"] for si in specs
                    )
                else:
                    control_peers.add(group["chain"][pos - 1])
                if pos + 1 < len(group["chain"]):
                    succ = group["chain"][pos + 1]
                    data_peers.add(succ)
                    control_peers.add(succ)
        control_peers |= data_peers
        self.peers = sorted(control_peers)
        self.flag_slot_of = {}
        self._relay_peer_session: dict[int, str] = {}
        self._relay_data_dst: dict[int, int] = {}
        self._relay_ack_dst: dict[int, int] = {}
        for peer in sorted(control_peers):
            pinfo = all_info[peer]
            assert pinfo is not None
            self._relay_peer_session[peer] = pinfo["session"]
            self._relay_data_dst[peer] = pinfo["relay_data_addr"]
            self._relay_ack_dst[peer] = pinfo["relay_ack_addr"]
            self.engine.attach_peer(pinfo["session"], pinfo.get("engine_payload"))

        self._relay_send_dst: dict[int, list[int | None]] = {}
        if self._is_sender:
            for gid in self._relay_send_buf:
                head = self.router.relay_group(gid)["head"]
                pinfo = all_info[head]
                assert pinfo is not None
                table = pinfo["relay_head_prep"].get(gid)
                if table is None:
                    raise RuntimeError(
                        f"relay head {head} omitted group {gid} PREP metadata"
                    )
                self._relay_send_dst[gid] = [row.get(rank) for row in table]

        self._relay_forward_dst: dict[int, list[int | None]] = {}
        if not self._is_sender:
            for gid in self._relay_owned_gids:
                chain = self.router.relay_group(gid)["chain"]
                pos = chain.index(rank)
                if pos + 1 >= len(chain):
                    continue
                succ = chain[pos + 1]
                pinfo = all_info[succ]
                assert pinfo is not None
                table = pinfo["relay_chain_prep"].get(gid)
                if table is None:
                    raise RuntimeError(
                        f"relay successor {succ} omitted group {gid} PREP metadata"
                    )
                self._relay_forward_dst[gid] = table

        if self._tcp_control is not None:
            tcp_peers = {
                peer
                for peer in control_peers
                if self._peer_ip.get(peer) != self._local_ip
            }
            endpoints = {
                peer: all_info[peer]["tcp_control_endpoint"] for peer in tcp_peers
            }
            missing = sorted(
                peer for peer, endpoint in endpoints.items() if endpoint is None
            )
            if missing:
                raise RuntimeError(
                    f"relay TCP control peers {missing} did not publish endpoints"
                )
            self._tcp_control.configure(tcp_peers, endpoints)

        # Import the local representative's DOFF buffers/events for every remotely-owned group consumed here.
        self._relay_peer_local_flags: dict[int, _LocalReplFlagBank] = {}
        self._relay_peer_channel: dict[int, dict[tuple[int, int], int]] = {}
        self._relay_peer_slot_of_me: dict[int, int] = {}
        self._relay_peer_doff: dict[tuple[int, int], torch.Tensor] = {}
        self._relay_peer_kernel_base: dict[tuple[int, int], int] = {}
        self._relay_peer_ipc_mapping: dict[tuple[int, int], tuple[int, int]] = {}
        self._relay_peer_ready_event: dict[tuple[int, int], torch.cuda.Event] = {}
        if not self._is_sender:
            for gid, owner in self._relay_consume_owner.items():
                if owner == rank:
                    continue
                if self._peer_ip.get(owner) != self._local_ip:
                    raise RuntimeError(
                        f"relay group {gid} local owner {owner} is not co-located with reader {rank}"
                    )
                pinfo = all_info[owner]
                assert pinfo is not None
                path = pinfo["relay_local_flags"]
                if not path:
                    raise RuntimeError(f"relay owner {owner} omitted local flag bank")
                if owner not in self._relay_peer_local_flags:
                    slots = pinfo["relay_local_slots"]
                    channels = pinfo["relay_local_channels"]
                    self._relay_peer_local_flags[owner] = _LocalReplFlagBank.open(
                        path,
                        slots=len(slots),
                        channels=max(1, len(channels)),
                    )
                    self._relay_peer_channel[owner] = dict(channels)
                    self._relay_peer_slot_of_me[owner] = slots[rank]
                if int(pinfo.get("relay_doff_depth", 0)) != self._relay_doff_depth:
                    raise RuntimeError(
                        f"relay DOFF mismatch with owner {owner}: local={self._relay_doff_depth} "
                        f"remote={pinfo.get('relay_doff_depth')}"
                    )
                reductions = pinfo["relay_doff_reduce"].get(gid)
                kernel_metadata = pinfo["relay_doff_kernel_ipc"].get(gid)
                if reductions is None or kernel_metadata is None:
                    raise RuntimeError(
                        f"relay owner {owner} omitted group {gid} DOFF IPC metadata"
                    )
                for slot in range(self._relay_doff_depth):
                    rebuild, args = reductions[slot]
                    tensor = rebuild(*args)
                    self._relay_peer_doff[(gid, slot)] = tensor
                    local_device = torch.device(self.device).index
                    _enable_cuda_peer_access(local_device, int(pinfo["device"]))
                    kernel_base, allocation_base = _open_cuda_ipc_mapping(
                        local_device,
                        kernel_metadata[slot],
                    )
                    self._relay_peer_kernel_base[(gid, slot)] = kernel_base
                    self._relay_peer_ipc_mapping[(gid, slot)] = (
                        local_device,
                        allocation_base,
                    )
                    handle = pinfo["relay_ready_event_ipc"][(gid, slot)][rank]
                    self._relay_peer_ready_event[(gid, slot)] = (
                        torch.cuda.Event.from_ipc_handle(
                            local_device,
                            handle,
                        )
                    )

        logger.info(
            "wbridge rank %d: replica relay groups=%d owned=%d consumed=%d buffers SEND %.2f GiB "
            "PREP %.2f GiB DOFF(non-RDMA) %.2f GiB peers=%s",
            rank,
            self._relay_num_groups,
            len(self._relay_owned_gids),
            len(self._relay_consume_owner),
            total_send / 1024**3,
            total_prep / 1024**3,
            total_doff / 1024**3,
            self.peers,
        )

    def _setup_rdma_buffers(self, rank: int, ws: int) -> None:
        """Allocate + register the data buffers (sender pack buffers / receiver arena) + flag channels,
        then all-gather their addresses over Gloo."""
        assert (
            self.engine is not None
            and self.router is not None
            and self.device is not None
        )

        # Largest byte chunk this rank exchanges with each peer across all rounds (buffer is reused).
        max_bytes: dict[int, int] = {}
        for _full_spec, overlap_specs in self.router.local_rounds:
            for peer, ospec in overlap_specs.items():
                nb = ospec.nbytes(self.dtype_spec)
                if nb > max_bytes.get(peer, 0):
                    max_bytes[peer] = nb
        self.peers = sorted(max_bytes)
        self.flag_slot_of = {peer: i for i, peer in enumerate(self.peers)}
        n = max(len(self.peers), 1)

        # Peers on THIS host (from the session pre-gather). Their bulk bytes can skip network RDMA entirely:
        # the receiver reads them straight out of the sender's pack buffer over CUDA-IPC (see the export
        # block below and WeightReceiver's ipc_pull). Staging puts the bytes in host DRAM, which CUDA IPC cannot
        # map, so a staging endpoint never takes part (both switches are off by default).
        self._same_node_peers = [
            p for p in self.peers if self._peer_ip.get(p) == self._local_ip
        ]
        staged = self.sender_staging if self._is_sender else self.receiver_staging
        self._sn_ipc_ok = SAME_NODE_IPC and not staged
        sn_candidates = self._same_node_peers if self._sn_ipc_ok else []

        # Incoming flags (peers write here) + outgoing flag sources.  Give every (peer, round) message an
        # exclusive word: write_async() may return before the NIC has fetched its source, so a single reused
        # scratch word would require a completion wait.  Round slots are safe to reuse in a later epoch by
        # protocol causality: the peer must observe this generation before it can produce the response that
        # permits the same round slot to recur.  The CPU footprint is only peers*rounds*8 bytes.
        flag_words = n * self.num_rounds
        self._flag_buf = torch.zeros(flag_words, dtype=self._FLAG_DTYPE).pin_memory()
        self._flag_src = torch.zeros(flag_words, dtype=self._FLAG_DTYPE).pin_memory()
        self.engine.register(
            self._flag_buf.data_ptr(),
            self._flag_buf.numel() * self._FLAG_ITEMSIZE,
            is_flag=True,
            tensor=self._flag_buf,
        )
        self.engine.register(
            self._flag_src.data_ptr(),
            self._flag_src.numel() * self._FLAG_ITEMSIZE,
            is_flag=True,
            tensor=self._flag_src,
        )

        # NUMA node of the pinned bulk NIC (-1 if unpinned): the big host staging buffers below are allocated
        # first-touch-local to it so the NIC's DMA isn't cross-socket (a major host-RDMA throughput lever).
        self._numa_node = getattr(self.engine, "bulk_numa_node", lambda: -1)()

        # Per-(sender,round) RECV destination this rank publishes so its senders write to the right place
        # regardless of THIS receiver's staging mode. Senders leave these empty (set in the receiver branch).
        self._recv_base = 0
        self._recv_off_of: dict[int, list[int | None]] = {}

        # Only SENDERS allocate per-peer pack buffers (GPU uint8) as the RDMA write SOURCE; a receiver instead
        # allocates isolated ingress + rollout exchange buffers below. DOUBLE-BUFFERED by round parity to
        # pipeline the sender's pack/write rounds. Registered once, reused across all WTs.
        self._data_buf: dict[int, list[torch.Tensor]] = {}
        self._cpu_grid = {}
        # Sender pack-buffer depth (double-buffering). Default 2 (pipeline pack(ri+1) with RDMA(ri)); set
        # WBRIDGE_SENDER_NUM_BUF=1 to single-buffer (no pack/RDMA overlap). Sweepable for perf studies.
        self._NUM_BUF = int(
            os.environ.get("WBRIDGE_SENDER_NUM_BUF", str(type(self)._NUM_BUF))
        )
        if self._is_sender:
            total = 0
            for peer in self.peers:
                bufs = []
                for _ in range(self._NUM_BUF):
                    buf = torch.zeros(
                        max_bytes[peer], dtype=torch.uint8, device=self.device
                    )
                    self.engine.register(buf.data_ptr(), max_bytes[peer], tensor=buf)
                    if self.local_engine is not None:
                        self.local_engine.register_buffer(
                            buf
                        )  # SS: GPU wire is the D2H source
                    bufs.append(buf)
                    total += max_bytes[peer]
                self._data_buf[peer] = bufs
            free_b = torch.cuda.mem_get_info()[0]
            logger.info(
                "wbridge rank %d: %d pack buffers (%d peers x%d parity) = %.2f GiB; %.2f GiB free",
                rank,
                len(self.peers) * self._NUM_BUF,
                len(self.peers),
                self._NUM_BUF,
                total / 1024**3,
                free_b / 1024**3,
            )
            # SS: a FULL CPU-pinned grid indexed by (peer, round) — the D2H offload target and the
            # CPU->remote RDMA source (host-pinned => the network path in the dual engine). Sized from each
            # round's per-peer overlap; ~one model's wire bytes total (covered by the >=-CPU-RAM assumption).
            if self.sender_staging:
                assert self.local_engine is not None
                cpu_total = 0
                with _numa_local_alloc(
                    self._numa_node
                ):  # first-touch NUMA-local to the bulk NIC
                    for peer in self.peers:
                        slots: list[torch.Tensor | None] = []
                        for _ri, (_fs, ov) in enumerate(self.router.local_rounds):
                            nb = ov[peer].nbytes(self.dtype_spec) if peer in ov else 0
                            if nb:
                                t = torch.zeros(nb, dtype=torch.uint8).pin_memory()
                                self.engine.register(t.data_ptr(), nb, tensor=t)
                                self.local_engine.register_buffer(t)
                                slots.append(t)
                                cpu_total += nb
                            else:
                                slots.append(None)
                        self._cpu_grid[peer] = slots
                logger.info(
                    "wbridge rank %d: SS CPU grid = %.2f GiB (pinned, %d peers x %d rounds, numa=%d)",
                    rank,
                    cpu_total / 1024**3,
                    len(self.peers),
                    self.num_rounds,
                    self._numa_node,
                )

        # ---- Receiver-side de-replication: isolated ingress + rollout exchange arena ----
        # A receiver receives only its per-tensor 1/m slice from trainers, then reconstructs the full shard by
        # a per-tensor all-to-all. `_recv_arena` is trainer-reachable and parity-buffered with a stable lane per
        # trainer. `_arena` is rollout-peer-reachable and holds parity-buffered fused own/send PREP followed by
        # exact cross-node ingress parity slots (the generic fallback uses one GRECV slot per peer/parity). Same-node
        # columns are consumed directly from peer IPC mappings. Keeping the allocations physically separate
        # makes the 3-stage lifetime explicit: receive(r+1), prepare(r), external+internal-consume(r-1).
        self._repl_peers: list[int] = []
        # Depth-2 RECV pipelining (WBRIDGE_RECV_PIPELINE=1): the arena is double-buffered by round parity so
        # the sender can keep 2 rounds in flight (stream) while the receiver processes serially underneath.
        # Round ri lives in arena[(ri % _recv_depth)*S : ...]. depth=1 is the original single-buffer path.
        self._recv_depth = (
            2 if os.environ.get("WBRIDGE_RECV_PIPELINE", "1") == "1" else 1
        )
        # Experimental memory profile: overlay each trainer RECV parity with rollout PREP and snapshot RECV
        # into one epoch-scoped, non-RDMA scratch buffer before A+R overwrites the slot. This trades one extra
        # D2D copy and stricter whole-slot backpressure for substantially lower persistent receiver HBM.
        self._merged_recv_prep = (
            not self._is_sender
            and os.environ.get("WBRIDGE_MERGED_RECV_PREP", "0") == "1"
        )
        # Topology-aware internal consume (WBRIDGE_TOPO_EXCHANGE=1): do cross-node exchange only within
        # one-worker-per-node columns, then let each local GPU consume every required column directly over
        # NVLink. With one worker per node only the self internal-consume kernel remains.
        # Resolved per peer at connect (needs _peer_ip); falls back when the structure doesn't hold.
        self._topo_exchange = os.environ.get("WBRIDGE_TOPO_EXCHANGE", "1") == "1"
        # Control-plane profiler (WBRIDGE_CTL_PROFILE=1): per-primitive time/count/immediate-hit for the
        # three direct-async flag submissions plus three pinned-CPU flag polls.
        self._ctlp = os.environ.get("WBRIDGE_CTL_PROFILE") == "1"
        self._ctl: dict = {}
        # Flag publication is direct write_async() from its causal producer. A completion reaper exists only
        # to retire backend handles and surface errors; it never publishes a flag and never gates progress.
        self._flag_reaper_q: queue.Queue | None = None
        self._flag_reaper_thread: threading.Thread | None = None
        self._flag_reaper_lock = threading.Lock()
        self._flag_reaper_errors: list[BaseException] = []
        # The RdmaEngine contract does not require concurrent submissions on one engine object to be safe.
        # Serialize only the tens-of-microseconds write_async() call; transfers still run in parallel and no
        # completion can hold this lock.
        self._flag_submit_lock = threading.Lock()
        if not self._is_sender:
            sw = self.router.sender_ws
            rl = rank - sw
            # Placement is known before arena allocation, so the topology planner can reserve exact packed
            # cross-node payloads in PREP.  A global structural decision keeps all receiver ranks on the same
            # protocol if even one replica group has unequal workers per participating node.
            configured_topology = bool(
                self._topo_exchange and self.router.configure_topology(self._peer_ip)
            )
            needs_internal_ipc = bool(
                configured_topology
                and any(
                    self.router.topology_plan(member, ri)["internal"]
                    for member in range(self.router.receiver_ws)
                    for ri in range(self.num_rounds)
                )
            )
            if needs_internal_ipc and not SAME_NODE_IPC:
                # Keep the full generic GRECV layout so the explicit transport A/B remains a valid fallback.
                self.router._topology_ok = False
                configured_topology = False
            self._topo_structure_ok = configured_topology
            layout, S, peer_set = self.router.arena_layout(rl, depth=self._recv_depth)
            self._arena_layout = (
                layout  # per-round offset tables, consumed by _build_arena_plans
            )
            recv_stride = layout[0]["recv_stride"] if layout else 0
            if self._merged_recv_prep:
                if self.receiver_staging:
                    raise RuntimeError(
                        "WBRIDGE_MERGED_RECV_PREP=1 does not support receiver staging"
                    )
                if self._recv_depth != 2:
                    raise RuntimeError(
                        "WBRIDGE_MERGED_RECV_PREP=1 requires depth-2 RECV pipelining"
                    )
                if not self._topo_structure_ok:
                    raise RuntimeError(
                        "WBRIDGE_MERGED_RECV_PREP=1 requires topology-aware exchange"
                    )
                slot_stride = _merge_recv_prep_layout(layout, self._recv_depth, S)
                self._arena_S = slot_stride
                self._recv_S = slot_stride
            else:
                self._arena_S = S  # PREP parity p -> arena[p*S:(p+1)*S]
                self._recv_S = recv_stride
            self._prep_payload_S = S
            self._recv_payload_S = recv_stride
            self._doff_depth = int(os.environ.get("WBRIDGE_DOFF", "1"))
            if self._doff_depth < 1:
                raise ValueError(f"WBRIDGE_DOFF must be >= 1, got {self._doff_depth}")
            self._doff_layout, doff_bytes, self._doff_S = _doff_arena_layout(
                layout,
                S,
                self._doff_depth,
            )
            arena_bytes = _arena_total_bytes(layout, self._recv_depth, self._arena_S)
            recv_bytes = _arena_recv_total_bytes(layout, self._recv_depth)
            _hbm_debug(
                "receiver_buffers_prealloc",
                rank=rank,
                arena_bytes=arena_bytes,
                recv_bytes=recv_bytes,
                doff_bytes=doff_bytes,
            )
            shared_grecv_bytes = arena_bytes - self._recv_depth * self._arena_S
            self._repl_peers = [
                sw + p for p in peer_set
            ]  # arena union of the per-tensor class peers
            direct_grecv_peers = sorted({p for rd in layout for p in rd["grecv"]})
            direct_grecv_slots = sorted(
                {
                    (sw + source, ri % self._recv_depth)
                    for ri, rd in enumerate(layout)
                    for source in rd["grecv"]
                }
            )
            direct_doff_slots = sorted(
                {
                    (rank, ri % self._doff_depth)
                    for ri, (_full, overlaps) in enumerate(self.router.local_rounds)
                    if overlaps
                }
                | {
                    (sw + source, ri % self._doff_depth)
                    for ri, rd in enumerate(layout)
                    for source, (_off, nb) in rd["grecv"].items()
                    if nb
                }
            )
            # One node-local READY/DONE channel per fixed (source, DOFF slot). Unlike the old GRECV channel,
            # this lifetime is independent of the registered ingress parity: GRECV is released immediately
            # after its D2D offload, while this channel remains live until all local readers finish DOFF.
            self._topo_local_slot_channel = (
                {slot: channel for channel, slot in enumerate(direct_doff_slots)}
                if self._topo_structure_ok
                else {}
            )
            self._arena = torch.zeros(
                max(arena_bytes, 1), dtype=torch.uint8, device=self.device
            )
            _hbm_debug("receiver_arena_allocated", rank=rank, arena_bytes=arena_bytes)
            # DOFF is deliberately not registered with the RDMA engine. It is a fixed, per-source shadow of
            # SEND+GRECV, shared only with same-node readers over CUDA IPC. Depth defaults to one.
            self._doff_arena = torch.empty(
                max(doff_bytes, 1), dtype=torch.uint8, device=self.device
            )
            _hbm_debug("receiver_doff_allocated", rank=rank, doff_bytes=doff_bytes)
            if self._merged_recv_prep:
                # A view of the merged parity bank. Register the owning arena only once; trainer RECV and
                # rollout PREP/GRECV addresses all live in this one CUDA allocation.
                self._recv_arena = self._arena[: self._recv_depth * self._arena_S]
            else:
                self._recv_arena = torch.zeros(
                    max(recv_bytes, 1), dtype=torch.uint8, device=self.device
                )
                _hbm_debug("receiver_recv_allocated", rank=rank, recv_bytes=recv_bytes)
                self.engine.register(
                    self._recv_arena.data_ptr(),
                    self._recv_arena.numel(),
                    tensor=self._recv_arena,
                )
            self.engine.register(
                self._arena.data_ptr(), self._arena.numel(), tensor=self._arena
            )
            free_b = torch.cuda.mem_get_info()[0]
            persistent_bytes = (
                arena_bytes if self._merged_recv_prep else recv_bytes + arena_bytes
            )
            if self._merged_recv_prep:
                logger.info(
                    "wbridge rank %d: tensor-dedup buffers %.2f GiB persistent = merged RECV/PREP "
                    "%.2f GiB (M %.2f GiB x%d; R %.2f, S %.2f) + %.2f GiB external grecv; "
                    "epoch scratch %.2f GiB; %.2f GiB free",
                    rank,
                    persistent_bytes / 1024**3,
                    (self._recv_depth * self._arena_S) / 1024**3,
                    self._arena_S / 1024**3,
                    self._recv_depth,
                    recv_stride / 1024**3,
                    S / 1024**3,
                    shared_grecv_bytes / 1024**3,
                    recv_stride / 1024**3,
                    free_b / 1024**3,
                )
            else:
                logger.info(
                    "wbridge rank %d: tensor-dedup buffers %.2f GiB = isolated RECV %.2f GiB "
                    "(R %.2f GiB x%d) + rollout PREP %.2f GiB (S %.2f GiB x%d) + "
                    "%.2f GiB external grecv (%d parity slots across %d sources; %d class peers); "
                    "%.2f GiB free",
                    rank,
                    persistent_bytes / 1024**3,
                    recv_bytes / 1024**3,
                    self._recv_S / 1024**3,
                    self._recv_depth,
                    (self._recv_depth * S) / 1024**3,
                    S / 1024**3,
                    self._recv_depth,
                    shared_grecv_bytes / 1024**3,
                    len(direct_grecv_slots),
                    len(direct_grecv_peers),
                    len(self._repl_peers),
                    free_b / 1024**3,
                )
            logger.info(
                "wbridge rank %d: DOFF=%d non-RDMA internal shadow %.2f GiB "
                "(one generation %.2f GiB, %d exclusive source slots)",
                rank,
                self._doff_depth,
                doff_bytes / 1024**3,
                self._doff_S / 1024**3,
                len({source for source, _slot in direct_doff_slots}),
            )
            if persistent_bytes >= free_b * 0.8:
                # This is deliberately advisory. ``free_b`` is sampled AFTER the allocation, so the former
                # assertion compared the arena against the memory left over and rejected allocations that had
                # already succeeded despite substantial free memory remaining. SGLang has already
                # sized its model/KV pools by this point; let the deployment choose its desired headroom.
                logger.warning(
                    "wbridge rank %d: persistent tensor-dedup buffers %.2f GiB leave %.2f GiB free; "
                    "continuing without the former post-allocation headroom assertion",
                    rank,
                    persistent_bytes / 1024**3,
                    free_b / 1024**3,
                )
            if self.local_engine is not None:
                self.local_engine.register_buffer(
                    self._recv_arena
                )  # RS: isolated GPU RECV is the H2D destination
            # Publish per-(sender, round) RECV destination. RS-off: senders write directly into their stable
            # lane in the parity-selected GPU ingress slot. RS-on: senders write into a FULL-depth CPU-pinned
            # arena (a slot per round), which the main thread then H2D-loads into that GPU ingress lane before
            # assemble. Publishing (rather than recomputing on the sender) is what makes RS
            # transparent to the sender: it just writes to recv_base + recv_off_of[my_rank][ri].
            nr = self.num_rounds
            if self.receiver_staging:
                cpu_rounds, cpu_total = self.router.cpu_recv_layout(rl)
                self._cpu_recv_layout = cpu_rounds
                with _numa_local_alloc(
                    self._numa_node
                ):  # first-touch NUMA-local to the bulk NIC
                    self._cpu_recv = torch.zeros(
                        max(cpu_total, 1), dtype=torch.uint8
                    ).pin_memory()
                self.engine.register(
                    self._cpu_recv.data_ptr(),
                    self._cpu_recv.numel(),
                    tensor=self._cpu_recv,
                )  # senders' RDMA dst
                self.local_engine.register_buffer(self._cpu_recv)  # H2D source
                self._recv_base = self._cpu_recv.data_ptr()
                self._recv_off_of = {
                    si: [
                        (cpu_rounds[ri][si][0] if si in cpu_rounds[ri] else None)
                        for ri in range(nr)
                    ]
                    for si in range(sw)
                }
                logger.info(
                    "wbridge rank %d: RS CPU recv = %.2f GiB (pinned, full-depth over %d rounds, numa=%d)",
                    rank,
                    cpu_total / 1024**3,
                    nr,
                    self._numa_node,
                )
            else:
                self._recv_base = self._recv_arena.data_ptr()
                # Bake the round-parity base into the published RECV offset so senders write round ri into
                # recv_arena[(ri%depth)*R]. Stable per-sender offsets inside R prevent cross-sender overwrite.
                self._recv_off_of = {
                    si: [
                        (
                            (
                                _arena_slot_offset(
                                    layout[ri]["recv"][si][0],
                                    ri,
                                    self._recv_depth,
                                    self._recv_S,
                                )
                            )
                            if si in layout[ri]["recv"]
                            else None
                        )
                        for ri in range(nr)
                    ]
                    for si in range(sw)
                }
            # Ready + consumed flag channels, sized to _repl_peers.
            if self._repl_peers:
                nr = len(self._repl_peers)
                repl_words = nr * self.num_rounds
                self._repl_flag_buf = torch.zeros(
                    repl_words, dtype=self._FLAG_DTYPE
                ).pin_memory()
                self._repl_flag_src = torch.zeros(
                    repl_words, dtype=self._FLAG_DTYPE
                ).pin_memory()
                self.engine.register(
                    self._repl_flag_buf.data_ptr(),
                    repl_words * self._FLAG_ITEMSIZE,
                    is_flag=True,
                    tensor=self._repl_flag_buf,
                )
                self.engine.register(
                    self._repl_flag_src.data_ptr(),
                    repl_words * self._FLAG_ITEMSIZE,
                    is_flag=True,
                    tensor=self._repl_flag_src,
                )
                # Second flag channel: "I have consumed round r". Before a peer overwrites one parity slot
                # in OUR shared GRECV bank, it waits for our CONS from its previous write to that same parity.
                self._repl_cons_buf = torch.zeros(
                    repl_words, dtype=self._FLAG_DTYPE
                ).pin_memory()
                self._repl_cons_src = torch.zeros(
                    repl_words, dtype=self._FLAG_DTYPE
                ).pin_memory()
                self.engine.register(
                    self._repl_cons_buf.data_ptr(),
                    repl_words * self._FLAG_ITEMSIZE,
                    is_flag=True,
                    tensor=self._repl_cons_buf,
                )
                self.engine.register(
                    self._repl_cons_src.data_ptr(),
                    repl_words * self._FLAG_ITEMSIZE,
                    is_flag=True,
                    tensor=self._repl_cons_src,
                )
                self._repl_flag_slot_of = {
                    peer: i for i, peer in enumerate(self._repl_peers)
                }
                # Same-node CUDA-IPC peers rendezvous through this shared CPU bank.  It is published in the
                # metadata gather below and opened only by peers on this host; remote peers retain the
                # registered RDMA flag buffers above.
                self._repl_local_flags = _LocalReplFlagBank.create(
                    nr,
                    channels=max(1, len(self._topo_local_slot_channel)),
                )
                self._repl_peer_local_flags: dict[int, _LocalReplFlagBank] = {}
                # Same-node class peers exchange their slice over a DIRECT CUDA-IPC P2P copy (NVLink) gated by
                # a CUDA-IPC EVENT, instead of a network RDMA flag. Cross-node peers must not own
                # CUDA-IPC events: at 256 rollout ranks, allocating every event for all 255 class peers would
                # materialize thousands of CUDA event resources per GPU even though only the other workers on
                # this host can import them. The remote side's published agh_ipc_ok is checked during resolve;
                # allocating for the local same-node candidates is a safe superset of the final IPC set.
                ipc_event_readers = _ipc_event_readers(
                    self._repl_peers,
                    self._peer_ip,
                    self._local_ip,
                    enabled=SAME_NODE_IPC,
                )
                self._repl_ready_event = {
                    peer: torch.cuda.Event(interprocess=True)
                    for peer in ipc_event_readers
                }
                self._topo_slot_ready_event = {
                    slot: {
                        reader: torch.cuda.Event(interprocess=True)
                        for reader in ipc_event_readers
                    }
                    for slot in self._topo_local_slot_channel
                }
                self._repl_same_node: set[int] = set()
                self._repl_peer_arena: dict[
                    int, torch.Tensor
                ] = {}  # imported peer arena (aliased)
                self._repl_peer_kernel_base: dict[
                    int, int
                ] = {}  # peer tensor base in OUR GPU VA
                self._repl_peer_ipc_mapping: dict[
                    int, tuple[int, int]
                ] = {}  # peer -> (device, alloc base)
                self._repl_peer_doff_arena: dict[int, torch.Tensor] = {}
                self._repl_peer_doff_kernel_base: dict[int, int] = {}
                self._repl_peer_doff_ipc_mapping: dict[int, tuple[int, int]] = {}
                self._repl_peer_ready_event: dict[
                    int, torch.cuda.Event
                ] = {}  # peer's event we wait on
                self._repl_peer_topo_slot_ready_event: dict[
                    int, dict[tuple[int, int], torch.cuda.Event]
                ] = {}
                self._topo_peer_slot_channel: dict[int, dict[tuple[int, int], int]] = {}
                self._repl_peer_grecv_off: dict[
                    int, list[int | None]
                ] = {}  # per-round grecv byte offset in peer arena
                self._repl_peer_send_off: dict[
                    int, list[int | None]
                ] = {}  # per-round send[me] offset in peer arena (PULL)

        # CUDA-IPC handles for the same-node direct-P2P path (receivers with class peers only). The arena is a
        # persistent cudaMalloc block so ONE storage handle covers all rounds; each per-reader ready event is
        # recorded once here to materialize the underlying cudaEvent before exporting its handle. ``device`` is
        # OUR device index — readers must open our event against the exporter's device in from_ipc_handle.
        arena_reduce = None
        arena_kernel_ipc = None
        doff_reduce = None
        doff_kernel_ipc = None
        ready_event_ipc = None
        topo_slot_ready_event_ipc = None
        ipc_device = -1
        if not self._is_sender and self._repl_ready_event:
            _hbm_debug(
                "receiver_ipc_export_start",
                rank=rank,
                repl_peers=len(self._repl_peers),
                topo_slots=len(self._topo_slot_ready_event),
                ipc_event_readers=len(self._repl_ready_event),
                ipc_event_count=(
                    len(self._repl_ready_event)
                    + sum(
                        len(events) for events in self._topo_slot_ready_event.values()
                    )
                ),
            )
            try:
                arena_reduce = reduce_tensor(self._arena)
                doff_reduce = reduce_tensor(self._doff_arena)
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(
                    "wbridge: failed to export the arena/DOFF CUDA-IPC handle (reduce_tensor) for the same-node "
                    "NVLink P2P path — the rollout allocator must NOT use expandable_segments"
                ) from e

            arena_kernel_ipc = _cuda_ipc_kernel_metadata(arena_reduce)
            doff_kernel_ipc = _cuda_ipc_kernel_metadata(doff_reduce)
            with torch.cuda.device(self._arena.device):
                for event in self._repl_ready_event.values():
                    event.record()
                for events in self._topo_slot_ready_event.values():
                    for event in events.values():
                        event.record()
            _hbm_debug("receiver_events_recorded", rank=rank)
            ready_event_ipc = {
                reader: event.ipc_handle()
                for reader, event in self._repl_ready_event.items()
            }
            topo_slot_ready_event_ipc = {
                slot: {reader: event.ipc_handle() for reader, event in events.items()}
                for slot, events in self._topo_slot_ready_event.items()
            }
            ipc_device = self._arena.device.index

        # Same-node trainer->rollout bypass, SENDER side. Export this rank's per-peer pack buffers (all
        # parities — the receiver picks ri % pack_num_buf, exactly as pack does) plus one production event per
        # co-located receiver. That receiver then PULLs round ri's bytes straight out of our pack buffer over
        # NVLink and we skip the network RDMA write altogether. PULL, not push, for the same reason the dedup
        # exchange pulls: writing peer CUDA-IPC memory measured much slower than reading it.
        #
        # Export is best-effort: reduce_tensor needs a plain cudaMalloc block, so a trainer whose allocator
        # uses expandable_segments cannot export. That is not fatal — we simply publish no handles, no
        # receiver confirms, and every peer stays on the RDMA path.
        pack_ipc = None
        pack_kernel_ipc = None
        pack_event_ipc = None
        if self._is_sender and sn_candidates:
            try:
                pack_ipc = {
                    p: [reduce_tensor(b) for b in self._data_buf[p]]
                    for p in sn_candidates
                }
                pack_kernel_ipc = {
                    p: [_cuda_ipc_kernel_metadata(reduced) for reduced in reductions]
                    for p, reductions in pack_ipc.items()
                }
                dev = self._data_buf[sn_candidates[0]][0].device
                self._pack_ready_event = {
                    p: torch.cuda.Event(interprocess=True) for p in sn_candidates
                }
                with torch.cuda.device(dev):
                    for p in sn_candidates:
                        self._pack_ready_event[
                            p
                        ].record()  # materialize the cudaEvent before exporting
                pack_event_ipc = {
                    p: self._pack_ready_event[p].ipc_handle() for p in sn_candidates
                }
                ipc_device = dev.index
            except Exception as e:  # noqa: BLE001 — degrade to RDMA rather than fail the connect
                logger.warning(
                    "wbridge rank %d: cannot export pack buffers over CUDA-IPC (%s); same-node "
                    "peers %s stay on the RDMA path (is expandable_segments on?)",
                    rank,
                    e,
                    sn_candidates,
                )
                pack_ipc = pack_kernel_ipc = pack_event_ipc = None
                self._pack_ready_event = {}

        # Optional host-network control plane. Every rank binds before the metadata gather, so the gathered
        # endpoint is immediately connectable. Only inter-node peer pairs are connected below; same-node
        # replica flags retain their mmap path and same-node trainer flags retain the existing engine path.
        self._tcp_control = None
        if os.environ.get("WBRIDGE_TCP_CONTROL", "0") == "1":
            self._tcp_control = TcpControlTransport(
                rank, self._local_ip, self._tcp_flag_landed
            )

        # ---- Declare every region a peer may write into, for backends whose remote addressing is a
        # handle rather than an address (MonarchEngine). No-op for Mooncake. Must precede the metadata
        # exchange below, since the handles travel in it.
        regions: list[tuple[int, int]] = []
        fi = self._FLAG_ITEMSIZE
        regions += [
            (self._flag_buf.data_ptr() + i * fi, fi)
            for i in range(self._flag_buf.numel())
        ]
        if not self._is_sender:
            if self._repl_peers:
                regions += [
                    (self._repl_flag_buf.data_ptr() + i * fi, fi)
                    for i in range(self._repl_flag_buf.numel())
                ]
                regions += [
                    (self._repl_cons_buf.data_ptr() + i * fi, fi)
                    for i in range(self._repl_cons_buf.numel())
                ]
            # Sender->receiver landing slots: address from the published recv_off_of (RS-aware), size
            # from this round's arena layout.
            for ri, rd in enumerate(self._arena_layout):
                for si, (off, nb) in rd["recv"].items():
                    pub = self._recv_off_of.get(si, [])
                    if ri < len(pub) and pub[ri] is not None and nb:
                        regions.append((self._recv_base + pub[ri], nb))
                # Class peers push their slice into our grecv[peer] (cross-node path).
                for p_rl, (goff, gnb) in rd["grecv"].items():
                    if gnb:
                        regions.append((self._arena.data_ptr() + goff, gnb))
        self.engine.publish_regions(regions)

        merged_slot_pred: dict[int, int | None] = {}
        merged_slot_last: dict[int, int] = {}
        if not self._is_sender and self._merged_recv_prep:
            active_rounds = [
                ri
                for ri, (_full, overlaps) in enumerate(self.router.local_rounds)
                if overlaps
            ]
            merged_slot_pred = _arena_slot_predecessors(active_rounds, self._recv_depth)
            for ri in active_rounds:
                merged_slot_last[ri % self._recv_depth] = ri

        info = {
            "session": self.session,
            "tcp_control_endpoint": (
                self._tcp_control.endpoint if self._tcp_control is not None else None
            ),
            "flag_addr": self._flag_buf.data_ptr(),
            "flag_slot_of": self.flag_slot_of,
            "flag_rounds": self.num_rounds,
            # Arena base (0 on senders). Class peers add the absolute shared-bank grecv offset computed by
            # arena_layout; own/send offsets still receive their local round-parity base at the source.
            "arena_addr": self._arena.data_ptr() if not self._is_sender else 0,
            # Sender->receiver RECV destination, published so senders are agnostic to this receiver's RS mode
            # (parity-selected GPU arena for RS-off; full-depth CPU arena for RS-on). Empty on senders.
            "recv_base": self._recv_base,
            "recv_off_of": self._recv_off_of,
            "merged_recv_prep": bool(getattr(self, "_merged_recv_prep", False)),
            "merged_slot_pred": merged_slot_pred,
            "merged_slot_last": merged_slot_last,
            # Receiver<->receiver all-to-all flags (empty unless this receiver has class peers):
            "repl_flag_addr": self._repl_flag_buf.data_ptr() if self._repl_peers else 0,
            "repl_cons_addr": self._repl_cons_buf.data_ptr() if self._repl_peers else 0,
            "repl_flag_slot_of": getattr(self, "_repl_flag_slot_of", {}),
            "repl_local_flags": (
                self._repl_local_flags.path
                if getattr(self, "_repl_local_flags", None) is not None
                else ""
            ),
            "topo_slot_channels": getattr(self, "_topo_local_slot_channel", {}),
            # Same-node direct-P2P handshake (None on senders / receivers without class peers):
            "arena_reduce": arena_reduce,  # torch CUDA-IPC handle for the peer's arena
            "arena_kernel_ipc": arena_kernel_ipc,  # destination-device mapping for SM peer loads
            "doff_reduce": doff_reduce,  # non-RDMA fixed internal-offload arena
            "doff_kernel_ipc": doff_kernel_ipc,  # destination-device mapping for DOFF SM loads
            "doff_depth": getattr(self, "_doff_depth", 0),
            "ready_event_ipc": ready_event_ipc,  # {reader_rank: event ipc_handle} for our copies to them
            # {(external_source, parity): {reader_rank: event}}; one timeline per physical GRECV slot.
            "topo_slot_ready_event_ipc": topo_slot_ready_event_ipc,
            "device": ipc_device,  # exporter device index for from_ipc_handle
            # Same-node trainer->rollout bypass (None unless this is a sender with co-located receivers):
            "pack_ipc": pack_ipc,  # {receiver_rank: [CUDA-IPC handle per pack parity]}
            "pack_kernel_ipc": pack_kernel_ipc,  # raw mappings for direct sender-pack consume kernels
            "pack_event_ipc": pack_event_ipc,  # {receiver_rank: pack-landed event ipc_handle}
            "pack_num_buf": self._NUM_BUF
            if self._is_sender
            else 0,  # parity depth (env-overridable)
            # WBRIDGE_SAME_NODE_IPC as THIS rank sees it. Published (not read locally) so the class-peer
            # gate below is the same conjunction on both sides: a mixed setting must disable the direct
            # path for the pair, never leave one peer pulling while the other pushes. Deliberately not
            # folded into the staging-aware _sn_ipc_ok — the AGH exchange is GPU-arena even under RS.
            "agh_ipc_ok": SAME_NODE_IPC,
            # Backend-specific blob (MonarchEngine: its region -> RDMABuffer handle map). None for Mooncake.
            "engine_payload": self.engine.publish_payload(),
        }
        all_info: list = [None] * ws
        dist.all_gather_object(all_info, info, group=self.group)

        if self._tcp_control is not None:
            control_peers = {
                peer
                for peer in set(self.peers) | set(self._repl_peers)
                if self._peer_ip.get(peer) != self._local_ip
            }
            endpoints = {
                peer: all_info[peer].get("tcp_control_endpoint")
                for peer in control_peers
            }
            missing = sorted(
                peer for peer, endpoint in endpoints.items() if endpoint is None
            )
            if missing:
                raise RuntimeError(
                    f"WBRIDGE_TCP_CONTROL mismatch: peer rank(s) {missing} did not enable it"
                )
            self._tcp_control.configure(control_peers, endpoints)
            logger.info(
                "wbridge rank %d: host-network TCP control connected to %d inter-node peer(s) %s",
                rank,
                len(control_peers),
                sorted(control_peers),
            )

        # Resolve, for each peer we write to, its (session, remote arena addr, remote flag addr). The data
        # destination is the peer's ARENA (base + per-round RECV offset from arena_layout), so no per-round
        # table crosses the wire — only the arena base does.
        sw = self.router.sender_ws
        rl = rank - sw
        nr_rounds = self.num_rounds
        self.peer_session: dict[int, str] = {}
        self._arena_send_dst: dict[
            int, list[int | None]
        ] = {}  # per-round RECV addr in receiver's arena
        self._merged_recv_peers: set[int] = set()
        self._merged_recv_pred: dict[int, dict[int, int | None]] = {}
        self._merged_recv_last: dict[int, dict[int, int]] = {}
        self._flag_dst: dict[int, int] = {}
        for peer in self.peers:
            pinfo = all_info[peer]
            self.peer_session[peer] = pinfo["session"]
            self.engine.attach_peer(pinfo["session"], pinfo.get("engine_payload"))
            if pinfo.get("flag_rounds") != self.num_rounds:
                raise RuntimeError(
                    f"flag round-slot mismatch with peer {peer}: "
                    f"local={self.num_rounds} remote={pinfo.get('flag_rounds')}"
                )
            slot = pinfo["flag_slot_of"][
                rank
            ]  # our slot in the peer's incoming flag buffer
            self._flag_dst[peer] = (
                pinfo["flag_addr"] + slot * self.num_rounds * self._FLAG_ITEMSIZE
            )
            if self._is_sender:
                # Use the receiver's PUBLISHED per-round RECV offsets (RS-aware) rather than recomputing the
                # layout locally — so the same sender code targets a GPU arena (RS-off) or a full-depth CPU
                # arena (RS-on) transparently.
                base = pinfo["recv_base"]
                offs = pinfo["recv_off_of"].get(rank, [])
                self._arena_send_dst[peer] = [
                    (base + offs[ri])
                    if (ri < len(offs) and offs[ri] is not None)
                    else None
                    for ri in range(nr_rounds)
                ]
                if pinfo.get("merged_recv_prep", False):
                    self._merged_recv_peers.add(peer)
                    self._merged_recv_pred[peer] = {
                        int(ri): (None if pred is None else int(pred))
                        for ri, pred in pinfo.get("merged_slot_pred", {}).items()
                    }
                    self._merged_recv_last[peer] = {
                        int(parity): int(ri)
                        for parity, ri in pinfo.get("merged_slot_last", {}).items()
                    }
            elif self._sn_ipc_ok and pinfo["pack_ipc"] and rank in pinfo["pack_ipc"]:
                # Same-node trainer->rollout bypass, RECEIVER side. The sender published a handle for OUR
                # rank, which is its statement that it is co-located with us and able to export; import its
                # pack buffers (one per parity) + its pack-landed event and we will PULL each round's bytes
                # instead of waiting for an RDMA write. The event is reconstructed on OUR device (the one
                # the pull's stream runs on) — opening it on the exporter's device segfaults on the wait.
                self._peer_pack_buf[peer] = [
                    rebuild(*args) for rebuild, args in pinfo["pack_ipc"][rank]
                ]
                kernel_metadata = (pinfo.get("pack_kernel_ipc") or {}).get(rank)
                if kernel_metadata is None or len(kernel_metadata) != len(
                    self._peer_pack_buf[peer]
                ):
                    raise RuntimeError(
                        f"wbridge rank {rank}: sender {peer} omitted direct pack-buffer CUDA-IPC metadata"
                    )
                local_device = self._arena.device.index
                _enable_cuda_peer_access(local_device, int(pinfo["device"]))
                self._peer_pack_kernel_base[peer] = []
                self._peer_pack_ipc_mapping[peer] = []
                for metadata in kernel_metadata:
                    kernel_base, allocation_base = _open_cuda_ipc_mapping(
                        local_device, metadata
                    )
                    self._peer_pack_kernel_base[peer].append(kernel_base)
                    self._peer_pack_ipc_mapping[peer].append(
                        (local_device, allocation_base)
                    )
                self._peer_pack_event[peer] = torch.cuda.Event.from_ipc_handle(
                    self._arena.device.index, pinfo["pack_event_ipc"][rank]
                )
                self._peer_pack_num_buf[peer] = pinfo["pack_num_buf"]
                self._sn_senders.add(peer)

        # Agree on the bypass set. A sender may drop its RDMA write ONLY for a receiver that really
        # imported its handles: if the two sides disagree the sender never writes and the receiver polls a
        # flag forever, so the sender's set is derived from the receivers' CONFIRMATION, never from its own
        # view of co-location. One tiny extra gather at connect buys immunity to that deadlock.
        sn_confirm = sorted(self._sn_senders) if not self._is_sender else []
        all_confirm: list = [None] * ws
        dist.all_gather_object(all_confirm, sn_confirm, group=self.group)
        if self._is_sender:
            self._sn_peers = {p for p in self.peers if rank in (all_confirm[p] or [])}
        active = self._sn_peers if self._is_sender else self._sn_senders
        if active:
            logger.info(
                "wbridge rank %d: same-node CUDA-IPC bypass ON for %d/%d peer(s) %s — their bulk "
                "weight bytes skip RDMA (NVLink copy)",
                rank,
                len(active),
                len(self.peers),
                sorted(active),
            )
        elif self._same_node_peers:
            logger.info(
                "wbridge rank %d: %d co-located peer(s) %s stay on RDMA (same_node_ipc=%s, "
                "staging=%s)",
                rank,
                len(self._same_node_peers),
                self._same_node_peers,
                SAME_NODE_IPC,
                self.sender_staging or self.receiver_staging,
            )

        # Resolve the class peers we write our slice to: the peer's arena grecv[us] offset + their repl flag
        # slot (no per-round table crosses the wire — only the arena base does).
        self._repl_peer_session: dict[int, str] = {}
        self._arena_peer_dst: dict[
            int, list[int | None]
        ] = {}  # per-round grecv addr in peer's arena
        self._repl_flag_dst: dict[int, int] = {}
        self._repl_cons_dst: dict[int, int] = {}
        self._repl_peer_slot_of_me: dict[int, int] = {}
        local_ip = get_local_ip()
        for peer in self._repl_peers:
            pinfo = all_info[peer]
            peer_merged = bool(pinfo.get("merged_recv_prep", False))
            if peer_merged != self._merged_recv_prep:
                raise RuntimeError(
                    f"merged RECV/PREP mismatch with receiver peer {peer}: "
                    f"local={self._merged_recv_prep} remote={peer_merged}"
                )
            self._repl_peer_session[peer] = pinfo["session"]
            self.engine.attach_peer(pinfo["session"], pinfo.get("engine_payload"))
            lp, lp_S, _ = self.router.arena_layout(
                peer - sw, depth=self._recv_depth
            )  # class peer's per-round layout + its arena stride
            if peer_merged:
                lp_S = _merge_recv_prep_layout(lp, self._recv_depth, lp_S)
            base = pinfo["arena_addr"]
            self._arena_peer_dst[peer] = [
                (base + lp[ri]["grecv"][rl][0]) if rl in lp[ri]["grecv"] else None
                for ri in range(nr_rounds)
            ]
            slot = pinfo["repl_flag_slot_of"][rank]
            self._repl_peer_slot_of_me[peer] = slot
            row_off = slot * self.num_rounds * self._FLAG_ITEMSIZE
            self._repl_flag_dst[peer] = pinfo["repl_flag_addr"] + row_off
            self._repl_cons_dst[peer] = pinfo["repl_cons_addr"] + row_off
            # Same-node class peer -> direct CUDA-IPC P2P + event; cross-node stays on RDMA + flags.
            # WBRIDGE_SAME_NODE_IPC gates this leg too, so the switch covers BOTH same-node data paths and
            # a run with it off is a clean all-RDMA A/B. Read from both sides' published flag (see
            # "agh_ipc_ok" above) so the decision can never come out asymmetric.
            peer_ip = DualMooncakeEngine._ip_of(
                DualMooncakeEngine._split(pinfo["session"])[0]
            )
            if (
                peer_ip == local_ip
                and pinfo["arena_reduce"] is not None
                and pinfo.get("doff_reduce") is not None
                and SAME_NODE_IPC
                and pinfo["agh_ipc_ok"]
            ):
                self._repl_same_node.add(peer)
                local_flag_path = pinfo.get("repl_local_flags", "")
                if not local_flag_path:
                    raise RuntimeError(
                        f"wbridge rank {rank}: same-node replica peer {peer} did not publish local flags"
                    )
                peer_slot_channels = dict(pinfo.get("topo_slot_channels") or {})
                self._repl_peer_local_flags[peer] = _LocalReplFlagBank.open(
                    local_flag_path,
                    slots=len(pinfo["repl_flag_slot_of"]),
                    channels=max(1, len(peer_slot_channels)),
                )
                self._topo_peer_slot_channel[peer] = peer_slot_channels
                if int(pinfo.get("doff_depth", 0)) != self._doff_depth:
                    raise RuntimeError(
                        f"DOFF mismatch with receiver peer {peer}: "
                        f"local={self._doff_depth} remote={pinfo.get('doff_depth')}"
                    )
                local_device = self._arena.device.index
                _enable_cuda_peer_access(local_device, int(pinfo["device"]))
                kernel_ipc = pinfo.get("arena_kernel_ipc")
                if kernel_ipc is None:
                    raise RuntimeError(
                        f"wbridge rank {rank}: same-node peer {peer} omitted kernel CUDA-IPC metadata"
                    )
                kernel_base, allocation_base = _open_cuda_ipc_mapping(
                    local_device, kernel_ipc
                )
                self._repl_peer_kernel_base[peer] = kernel_base
                self._repl_peer_ipc_mapping[peer] = (local_device, allocation_base)
                rebuild, args = pinfo["arena_reduce"]
                self._repl_peer_arena[peer] = rebuild(
                    *args
                )  # uint8 tensor aliasing peer's arena
                doff_kernel_ipc = pinfo.get("doff_kernel_ipc")
                if doff_kernel_ipc is None:
                    raise RuntimeError(
                        f"wbridge rank {rank}: same-node peer {peer} omitted DOFF CUDA-IPC metadata"
                    )
                doff_kernel_base, doff_allocation_base = _open_cuda_ipc_mapping(
                    local_device,
                    doff_kernel_ipc,
                )
                self._repl_peer_doff_kernel_base[peer] = doff_kernel_base
                self._repl_peer_doff_ipc_mapping[peer] = (
                    local_device,
                    doff_allocation_base,
                )
                doff_rebuild, doff_args = pinfo["doff_reduce"]
                self._repl_peer_doff_arena[peer] = doff_rebuild(*doff_args)
                # Reconstruct the peer's ready-event on OUR (reader's) device — the same device _con_stream
                # runs on, so wait_event is a same-device op. Reconstructing on the exporter's device segfaults
                # on the cross-device wait. The IPC event still tracks the exporter's record. Key by OUR rank
                # (the peer created one event per reader it writes to).
                self._repl_peer_ready_event[peer] = torch.cuda.Event.from_ipc_handle(
                    self._arena.device.index, pinfo["ready_event_ipc"][rank]
                )
                slot_event_ipc = pinfo.get("topo_slot_ready_event_ipc") or {}
                self._repl_peer_topo_slot_ready_event[peer] = {
                    slot: torch.cuda.Event.from_ipc_handle(
                        self._arena.device.index,
                        handles[rank],
                    )
                    for slot, handles in slot_event_ipc.items()
                    if rank in handles
                }
                # Per-round grecv byte offset in the peer's arena = absolute dst - peer arena base.
                self._repl_peer_grecv_off[peer] = [
                    (self._arena_peer_dst[peer][ri] - base)
                    if self._arena_peer_dst[peer][ri] is not None
                    else None
                    for ri in range(nr_rounds)
                ]
                # PULL: per-round offset of the peer's send[me] slice WITHIN the peer's arena tensor (which
                # aliases from the peer's base). This is what we READ over NVLink into our grecv[peer]. By the
                # layout's symmetry peer.send[rl] and our grecv[peer] are the same bytes and size, so the read
                # replaces the (slow) push-write of our send[peer] into the peer's grecv[me].
                self._repl_peer_send_off[peer] = [
                    _arena_slot_offset(
                        lp[ri]["send"][rl][0], ri, self._recv_depth, lp_S
                    )
                    if rl in lp[ri]["send"]
                    else None
                    for ri in range(nr_rounds)
                ]
        if self._repl_peers:
            logger.info(
                "wbridge rank %d: %d repl peers (same-node NVLink-IPC %d, cross-node RDMA %d)",
                rank,
                len(self._repl_peers),
                len(self._repl_same_node),
                len(self._repl_peers) - len(self._repl_same_node),
            )
        # Topology-aware resolution. A structurally enabled receiver already allocated compact external-only
        # GRECV, so a later CUDA-IPC/runtime failure cannot use the generic staged fallback. Gather both facts
        # and fail the entire connection consistently if any compact receiver cannot resolve.
        local_topo_ok = bool(
            self._topo_exchange
            and not self._is_sender
            and self._resolve_topo_exchange(rank, sw, rl, nr_rounds)
        )
        topo_votes: list[tuple[bool, bool] | None] = [None] * ws
        dist.all_gather_object(
            topo_votes,
            (
                (bool(self._topo_structure_ok), local_topo_ok)
                if not self._is_sender
                else None
            ),
            group=self.group,
        )
        receiver_votes = [topo_votes[r] for r in range(sw, ws)]
        compact_layout = any(v is not None and v[0] for v in receiver_votes)
        global_topo_ok = all(v is not None and v[0] and v[1] for v in receiver_votes)
        if compact_layout and not global_topo_ok:
            failed = [
                sw + i
                for i, v in enumerate(receiver_votes)
                if v is None or not (v[0] and v[1])
            ]
            raise RuntimeError(
                "wbridge fused internal-consume topology allocated compact GRECV but runtime resolution "
                f"failed on receiver rank(s) {failed}; refusing an unsafe staged fallback"
            )
        self._topo_ok = bool(local_topo_ok and global_topo_ok)
        if self._topo_ok:
            logger.info(
                "wbridge rank %d: fused internal consume ON (ext=%d cross-node, int=%d same-node)",
                rank,
                len(self._topo_ext_peers),
                len(self._topo_int_peers),
            )
        elif self._topo_exchange and self._repl_peers and not self._is_sender:
            logger.info(
                "wbridge rank %d: topo-aware requested but structure unsuitable -> single-phase",
                rank,
            )

    def _resolve_topo_exchange(
        self, rank: int, sw: int, rl: int, nr_rounds: int
    ) -> bool:
        """Bind the configured multi-group topology plan to this receiver's concrete arena addresses.

        ``WeightRouter.configure_topology`` has already decided eligibility and reserved exact packed
        ``topo_send`` payloads.  This method validates the runtime peer/IPC imports and turns its symbolic
        name routes into batched external RDMA spans and per-peer direct internal-consume source
        descriptors. The latter retain source layout metadata rather than a local destination: the consume
        plan built after resolution maps the imported peer bytes straight into model tensors.

        A structurally enabled topology uses a compact external-only GRECV layout. Consequently a runtime
        resolution failure is made fatal by the global vote in :meth:`_setup_rdma_buffers`; it cannot fall
        back to the generic staged all-gather without reallocating the arena.
        """
        _td = os.environ.get("WBRIDGE_TOPO_DEBUG") == "1"

        def _no(reason):
            if _td:
                print(f"TDBG-RESOLVE rank {rank}: fallback ({reason})", flush=True)
            return False

        if not self._topo_structure_ok or not self.router._topology_ok:
            return _no("global topology structure is ineligible")

        plans = [self.router.topology_plan(rl, ri) for ri in range(nr_rounds)]
        expected = {sw + p for plan in plans for p in plan["peers"]}
        if set(self._repl_peers) != expected:
            return _no(
                f"repl_peers {sorted(self._repl_peers)} != planned {sorted(expected)}"
            )

        ext_send_by_round = [tuple(sw + p for p in plan["external"]) for plan in plans]
        ext_recv_by_round = [
            tuple(
                sw + source
                for source in range(self.router.receiver_ws)
                if rl in self.router.topology_plan(source, ri)["external"]
            )
            for ri in range(nr_rounds)
        ]
        int_by_round = [tuple(sw + p for p in plan["internal"]) for plan in plans]
        int_readers_by_round = [
            tuple(
                sw + reader
                for reader in range(self.router.receiver_ws)
                if rl in self.router.topology_plan(reader, ri)["internal"]
            )
            for ri in range(nr_rounds)
        ]
        ext_release_readers_by_round: list[dict[int, tuple[int, ...]]] = []
        for ri, ext_sources in enumerate(ext_recv_by_round):
            deps: dict[int, tuple[int, ...]] = {}
            for peer in ext_sources:
                source_rl = peer - sw
                readers = []
                for reader in range(self.router.receiver_ws):
                    routes = self.router.topology_plan(reader, ri)["pull"].get(rl, ())
                    if any(
                        kind == "grecv" and source == source_rl
                        for kind, source, _names in routes
                    ):
                        readers.append(sw + reader)
                deps[peer] = tuple(readers)
            ext_release_readers_by_round.append(deps)
        ext_send_union = sorted({p for peers in ext_send_by_round for p in peers})
        ext_recv_union = sorted({p for peers in ext_recv_by_round for p in peers})
        ext_union = sorted(set(ext_send_union) | set(ext_recv_union))
        int_union = sorted({p for peers in int_by_round for p in peers})
        if any(self._peer_ip.get(p) == self._local_ip for p in ext_union):
            return _no(f"external peer is same-node: {ext_union}")
        if not set(int_union) <= self._repl_same_node:
            return _no(
                f"internal peers not all IPC-imported: int={int_union} "
                f"same_node={sorted(self._repl_same_node)}"
            )

        recv_specs = self.router.recv_specs
        depth = self._recv_depth
        layout_cache: dict[int, tuple[list[dict], int]] = {
            rl: (self._arena_layout, self._arena_S),
        }
        doff_layout_cache: dict[int, list[dict]] = {rl: self._doff_layout}

        def _layout(member: int) -> tuple[list[dict], int]:
            cached = layout_cache.get(member)
            if cached is None:
                rounds, stride, _ = self.router.arena_layout(member, depth=depth)
                if getattr(self, "_merged_recv_prep", False):
                    stride = _merge_recv_prep_layout(rounds, depth, stride)
                cached = (rounds, stride)
                layout_cache[member] = cached
            return cached

        def _round_names(member: int, ri: int) -> tuple[str, ...]:
            return tuple(
                sorted(
                    n for n in self.router.global_rounds[ri] if n in recv_specs[member]
                )
            )

        def _doff_layout(member: int) -> list[dict]:
            cached = doff_layout_cache.get(member)
            if cached is None:
                member_rounds, prep_stride, _ = self.router.arena_layout(
                    member, depth=depth
                )
                cached, _total, _stride = _doff_arena_layout(
                    member_rounds,
                    prep_stride,
                    self._doff_depth,
                )
                doff_layout_cache[member] = cached
            return cached

        def _own_spec(member: int, ri: int) -> ShardSpec:
            return recv_specs[member].subset(set(_round_names(member, ri)))

        def _grecv_spec(
            dst_member: int, source_member: int, ri: int
        ) -> ShardSpec | None:
            dst_layout, _ = _layout(dst_member)
            names = dst_layout[ri]["grecv_names"].get(source_member)
            return recv_specs[source_member].subset(set(names)) if names else None

        # External source and destination are both exact packed column payloads. The RDMA backend receives a
        # span list because non-adjacent names can exist in a reused PREP source, but compact GRECV itself has
        # no holes for internally delivered names.
        ext_xfer: dict[int, list[list[tuple[int, int, int]]]] = {
            p: [[] for _ in range(nr_rounds)] for p in ext_send_union
        }
        arena_base = self._arena.data_ptr()
        for ri, plan in enumerate(plans):
            rd = self._arena_layout[ri]
            for p_rl, names in plan["external"].items():
                peer = sw + p_rl
                if p_rl not in rd["topo_send"]:
                    return _no(f"missing topo_send peer={peer} ri={ri}")
                remote_base = self._arena_peer_dst[peer][ri]
                if remote_base is None:
                    return _no(f"missing remote grecv peer={peer} ri={ri}")
                src_spec = recv_specs[rl].subset(set(names))
                dst_spec = _grecv_spec(p_rl, rl, ri)
                if dst_spec is None:
                    return _no(f"missing compact remote grecv spec peer={peer} ri={ri}")
                s_off, s_nb = rd["topo_send"][p_rl]
                if src_spec.nbytes(self.dtype_spec) != s_nb:
                    return _no(f"topo_send size peer={peer} ri={ri}")
                spans = _packed_copy_spans(
                    src_spec,
                    dst_spec,
                    names,
                    self.dtype_spec,
                    src_base=arena_base
                    + _arena_slot_offset(s_off, ri, depth, self._arena_S),
                    dst_base=remote_base,
                )
                if not spans:
                    return _no(f"empty external payload peer={peer} ri={ri}")
                ext_xfer[peer][ri] = spans

        # Internal consume reads either a peer's parity-slotted own payload or one of that peer's compact
        # external GRECV slots. Preserve the packed source spec and selected names so _build_arena_plans can
        # compose wire->model mappings directly, with no local staging destination.
        internal_src: dict[
            int, list[list[tuple[int | None, int, ShardSpec, tuple[str, ...]]]]
        ] = {p: [[] for _ in range(nr_rounds)] for p in int_union}
        internal_bytes: dict[int, list[int]] = {
            p: [0 for _ in range(nr_rounds)] for p in int_union
        }
        for ri, plan in enumerate(plans):
            for p_rl, routes in plan["pull"].items():
                peer = sw + p_rl
                peer_layout, peer_S = _layout(p_rl)
                peer_rd = peer_layout[ri]
                peer_doff_rd = _doff_layout(p_rl)[ri]
                sources: list[tuple[int | None, int, ShardSpec, tuple[str, ...]]] = []
                selected: set[tuple[int, str]] = set()
                for kind, source, names in routes:
                    for name in names:
                        key = (source, name)
                        if key in selected:
                            return _no(
                                f"duplicate internal route peer={peer} source={source} name={name}"
                            )
                        selected.add(key)
                    if kind == "own":
                        if source != p_rl:
                            return _no(f"invalid own route peer={peer} source={source}")
                        src_spec = _own_spec(p_rl, ri)
                        src_region = peer_rd["own"]
                        src_base = peer_doff_rd["own"][0] + src_region[0]
                        slot_source = None
                    elif kind == "grecv":
                        src_spec = _grecv_spec(p_rl, source, ri)
                        if src_spec is None:
                            return _no(
                                f"missing peer grecv spec peer={peer} source={source} ri={ri}"
                            )
                        src_region = peer_rd["grecv"].get(source)
                        if src_region is None:
                            return _no(
                                f"missing peer grecv peer={peer} source={source} ri={ri}"
                            )
                        src_base = peer_doff_rd["grecv"][source][0]
                        slot_source = sw + source
                    else:
                        return _no(f"unknown internal route kind={kind}")
                    if src_spec.nbytes(self.dtype_spec) != src_region[1]:
                        return _no(
                            f"internal source size peer={peer} source={source} ri={ri}"
                        )
                    regions = _packed_name_regions(src_spec, self.dtype_spec)
                    if not set(names) <= regions.keys():
                        return _no(
                            f"internal source names peer={peer} source={source} ri={ri}"
                        )
                    selected_names = tuple(sorted(names))
                    sources.append((slot_source, src_base, src_spec, selected_names))
                    internal_bytes[peer][ri] += sum(
                        regions[name][1] for name in selected_names
                    )
                if not sources:
                    return _no(f"empty internal payload peer={peer} ri={ri}")
                internal_src[peer][ri] = sources

        # Compact GRECV has one slot per (direct external source, round parity). Gate each write on the
        # target's previous direct generation of this same physical slot; target CONS is emitted only after
        # its own local internal-consume kernel and every downstream local reader kernel have finished.
        incoming_external = {
            peer: tuple(
                ri
                for ri in range(nr_rounds)
                if rl in self.router.topology_plan(peer - sw, ri)["external"]
            )
            for peer in ext_send_union
        }
        missing_external = sorted(
            p for p, rounds in incoming_external.items() if not rounds
        )
        if missing_external:
            return _no(
                f"external peer(s) have no reciprocal generation: {missing_external}"
            )
        peer_pred: list[dict[int, tuple[int, int]]] = []
        for ri, ext_peers in enumerate(ext_send_by_round):
            pred: dict[int, tuple[int, int]] = {}
            for peer in ext_peers:
                same_slot = [
                    r for r in incoming_external[peer] if r % depth == ri % depth
                ]
                if not same_slot:
                    return _no(
                        f"external peer={peer} has no reciprocal parity for ri={ri}"
                    )
                earlier = [r for r in same_slot if r < ri]
                pred[peer] = (0, earlier[-1]) if earlier else (-1, same_slot[-1])
            peer_pred.append(pred)

        self._topo_ext_peers = ext_union
        self._topo_int_peers = int_union
        self._topo_ext_send_peers_by_round = ext_send_by_round
        self._topo_ext_recv_peers_by_round = ext_recv_by_round
        self._topo_int_peers_by_round = int_by_round
        self._topo_int_readers_by_round = int_readers_by_round
        self._topo_ext_release_readers_by_round = ext_release_readers_by_round
        self._topo_ext_xfer = ext_xfer
        self._topo_internal_consume_src = internal_src
        self._topo_internal_consume_bytes = internal_bytes
        self._topo_peer_predecessors = peer_pred
        if _td:
            print(
                f"TDBG-RESOLVE rank {rank}: TOPO OK groups={len(self.router._topology_groups)} "
                f"ext={ext_union} int={int_union}",
                flush=True,
            )
        return True

    def _build_relay_plans(self) -> None:
        """Bind static model↔group-buffer copy plans for replica relay."""
        assert self.router is not None
        sw = self.router.sender_ws
        if self._is_sender:
            self._relay_pack_plans: list[CopyPlan | None] = []
            self._relay_sizes: list[dict[int, int]] = []
            for ri, specs in enumerate(self._relay_send_specs):
                pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
                sizes: dict[int, int] = {}
                for gid, spec in sorted(specs.items()):
                    size = spec.nbytes(self.dtype_spec)
                    sizes[gid] = size
                    pairs.extend(
                        self.load_spec.fuse_copy_pairs(
                            spec,
                            self._relay_send_buf[gid][ri % 2],
                            self.wksd,
                            self.dtype_spec,
                            src_to_dst=False,
                        )
                    )
                self._relay_pack_plans.append(CopyPlan(pairs) if pairs else None)
                self._relay_sizes.append(sizes)
            logger.info(
                "wbridge rank %d: built %d replica-group sender pack plans",
                self._rank,
                sum(plan is not None for plan in self._relay_pack_plans),
            )
            return

        # Only heads prepare: trainer lanes already live in PREP, so the head snapshots them into an
        # epoch-scoped scratch allocation and assembles scratch -> canonical PREP in place. Rebindable source
        # descriptors let these static plans follow a fresh scratch address each epoch.
        self._relay_prepare_plan: dict[tuple[int, int], CopyPlan] = {}
        self._relay_snapshot_bytes: dict[tuple[int, int], int] = {}
        self._relay_consume_plan: dict[tuple[int, int], CopyPlan] = {}
        self._relay_prepare_stream: dict[tuple[int, int], torch.cuda.Stream] = {}
        self._relay_prepare_event: dict[tuple[int, int], torch.cuda.Event] = {}
        self._relay_offload_stream: dict[tuple[int, int], torch.cuda.Stream] = {}
        self._relay_offload_event: dict[tuple[int, int], torch.cuda.Event] = {}
        self._relay_consume_stream: dict[tuple[int, int], torch.cuda.Stream] = {}
        self._relay_consume_event: dict[tuple[int, int], torch.cuda.Event] = {}
        rl = self._rank - sw
        scratch_templates: dict[int, torch.Tensor] = {}

        for group in self.router._relay_groups:
            gid = group["id"]
            owner = sw + group["owner_of"].get(rl, -1)
            for ri, group_spec in enumerate(group["round_specs"]):
                if not group_spec.entries:
                    continue
                parity = ri % 2
                doff_slot = ri % self._relay_doff_depth
                size = group_spec.nbytes(self.dtype_spec)
                if gid in self._relay_owned_gids:
                    if self._rank == group["head"]:
                        scratch = scratch_templates.get(gid)
                        if scratch is None:
                            scratch = torch.empty(
                                self._relay_head_scratch_size[gid],
                                dtype=torch.uint8,
                                device=self.device,
                            )
                            scratch_templates[gid] = scratch
                        prep = self._relay_prep_buf[gid][parity][:size]
                        prep_named = _carve_named(group_spec, prep, self.dtype_spec)
                        trainer_specs = group["trainer_specs"][ri]
                        scratch_named = {
                            si: _carve_named(
                                spec,
                                scratch[
                                    self._relay_prep_offsets[gid][ri][
                                        si
                                    ] : self._relay_prep_offsets[gid][ri][si]
                                    + spec.nbytes(self.dtype_spec)
                                ],
                                self.dtype_spec,
                            )
                            for si, spec in trainer_specs.items()
                        }
                        pairs = group_spec(prep_named).setitem_named_pairs(
                            trainer_specs,
                            scratch_named,
                        )
                        self._relay_prepare_plan[(gid, ri)] = CopyPlan(
                            pairs,
                            source_region=(scratch.data_ptr(), scratch.numel()),
                        )
                        self._relay_snapshot_bytes[(gid, ri)] = max(
                            self._relay_prep_offsets[gid][ri][si]
                            + spec.nbytes(self.dtype_spec)
                            for si, spec in trainer_specs.items()
                        )
                        self._relay_prepare_stream.setdefault(
                            (gid, parity),
                            torch.cuda.Stream(device=self.device),
                        )
                        self._relay_prepare_event.setdefault(
                            (gid, parity), torch.cuda.Event()
                        )
                    self._relay_offload_stream.setdefault(
                        (gid, doff_slot),
                        torch.cuda.Stream(device=self.device),
                    )
                    self._relay_offload_event.setdefault(
                        (gid, doff_slot),
                        torch.cuda.Event(),
                    )

                if rl not in group["members"]:
                    continue
                if owner == self._rank:
                    source_tensor = self._relay_doff_buf[gid][doff_slot][:size]
                    pairs = self.load_spec.fuse_copy_pairs(
                        group_spec,
                        source_tensor,
                        self.wksd,
                        self.dtype_spec,
                        src_to_dst=True,
                    )
                    plan = CopyPlan(pairs)
                else:
                    source_tensor = self._relay_peer_doff[(gid, doff_slot)][:size]
                    pairs = self.load_spec.fuse_copy_pairs(
                        group_spec,
                        source_tensor,
                        self.wksd,
                        self.dtype_spec,
                        src_to_dst=True,
                    )
                    tensor_base = self._relay_peer_doff[(gid, doff_slot)].data_ptr()
                    kernel_base = self._relay_peer_kernel_base[(gid, doff_slot)]
                    source_ptrs = [
                        kernel_base + (source.data_ptr() - tensor_base)
                        for _destination, source in pairs
                    ]
                    plan = CopyPlan(pairs, source_ptrs=source_ptrs)
                self._relay_consume_plan[(gid, ri)] = plan
                self._relay_consume_stream.setdefault(
                    (gid, doff_slot),
                    torch.cuda.Stream(device=self.device),
                )
                self._relay_consume_event.setdefault(
                    (gid, doff_slot), torch.cuda.Event()
                )

        logger.info(
            "wbridge rank %d: built replica relay prepare=%d consume=%d plans",
            self._rank,
            len(self._relay_prepare_plan),
            len(self._relay_consume_plan),
        )

    # ------------------------------------------------- fused model<->wire plan
    def _build_fuse_plans(self) -> None:
        """Precompute the per-round :class:`CopyPlan`\\ s replayed each WT.

        Receivers build tensor-dedup arena plans (fused prepare + consume over the single arena, via
        :meth:`_build_arena_plans`). Senders build one fused model->wire pack plan per round: the fused copy
        (``LoadSpec.fuse_copy_pairs``) collapses the two packing stages (model<->logical, logical<->wire)
        into a single copy bound to the persistent model params and the persistent RDMA pack buffers — no
        transient logical buffer. Rounds whose shard shapes the fast path can't map
        (:class:`FuseUnsupported`) fall back to the transient 2-stage path. When
        ``WBRIDGE_FUSE_SELFCHECK=1`` each round is validated byte-for-byte against the 2-stage path at
        connect (opt-in: it allocates transient scratch and can be memory-heavy).
        """
        if not self._is_sender:
            if self._direct_same_node:
                self._build_direct_consume_plans()
            else:
                self._build_arena_plans()  # tensor-dedup receiver: fused prepare + consume over the arena
            return

        # Sender: one fused model->wire pack plan per round.
        selfcheck = os.environ.get("WBRIDGE_FUSE_SELFCHECK", "0") == "1"
        self._fuse_plans: list[CopyPlan | None] = []
        self._fuse_sizes: list[dict[int, int]] = []
        self._fuse_fallback: list[bool] = []
        n_fused = n_fallback = 0
        for ri, (full_spec, overlap_specs) in enumerate(self.router.local_rounds):
            self._fuse_sizes.append(
                {p: o.nbytes(self.dtype_spec) for p, o in overlap_specs.items()}
            )
            if not overlap_specs:
                self._fuse_plans.append(None)
                self._fuse_fallback.append(False)
                continue
            try:
                pairs = []
                for peer, ospec in overlap_specs.items():
                    pairs += self.load_spec.fuse_copy_pairs(
                        ospec,
                        self._data_buf[peer][ri % self._NUM_BUF],
                        self.wksd,
                        self.dtype_spec,
                        src_to_dst=False,
                    )
                plan = CopyPlan(pairs)
                if selfcheck:
                    self._selfcheck_round(ri, full_spec, overlap_specs)
                self._fuse_plans.append(plan)
                self._fuse_fallback.append(False)
                n_fused += 1
            except (FuseUnsupported, AssertionError) as e:
                logger.warning(
                    "wbridge rank %d: round %d fuse->2-stage fallback (%s)",
                    self._rank,
                    ri,
                    e,
                )
                self._fuse_plans.append(None)
                self._fuse_fallback.append(True)
                n_fallback += 1
        logger.info(
            "wbridge rank %d: fused %d rounds, %d fallback",
            self._rank,
            n_fused,
            n_fallback,
        )
        if os.environ.get("WBRIDGE_TIMING") == "1":
            # Snapshot per-(this rank, peer) wire bytes per round, for collective-vs-RDMA comparisons.
            for ri, sz in enumerate(self._fuse_sizes):
                if sz:
                    logger.info(
                        "wbridge-snap rank %d round %d bytes %s",
                        self._rank,
                        ri,
                        dict(sorted(sz.items())),
                    )

    def _build_direct_consume_plans(self) -> None:
        """Bind same-node trainer pack buffers directly to this receiver's live model parameters.

        In this mode receiver de-duplication/exchange is disabled: every rollout replica has a complete
        trainer route, while sender-side replicated shards remain de-duplicated.  A round therefore consists
        only of sender pack followed by this plan.  ``source_ptrs`` point at CUDA-IPC mappings opened on the
        receiver GPU, so the copy/transpose/reshard kernel reads remote pack bytes over NVLink and writes the
        final runtime tensors without RECV, PREP, GRECV, DOFF, or a second consume pass.
        """
        self._direct_consume_plan: list[CopyPlan | None] = []
        total_launches = 0
        for ri, (full_spec, overlap_specs) in enumerate(self.router.local_rounds):
            if not overlap_specs:
                self._direct_consume_plan.append(None)
                continue
            missing = sorted(set(overlap_specs) - self._sn_senders)
            if missing:
                raise RuntimeError(
                    f"direct same-node consume round {ri} has non-IPC sender(s) {missing}"
                )
            covered = sum(
                spec.nbytes(self.dtype_spec) for spec in overlap_specs.values()
            )
            expected = full_spec.nbytes(self.dtype_spec)
            if covered != expected:
                raise RuntimeError(
                    f"direct same-node consume round {ri} coverage mismatch: "
                    f"covered={covered} expected={expected}"
                )
            pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
            source_ptrs: list[int] = []
            for peer, source_spec in sorted(overlap_specs.items()):
                parity = ri % self._peer_pack_num_buf[peer]
                source_buf = self._peer_pack_buf[peer][parity]
                source_bytes = source_spec.nbytes(self.dtype_spec)
                peer_pairs = self.load_spec.fuse_copy_pairs(
                    source_spec,
                    source_buf[:source_bytes],
                    self.wksd,
                    self.dtype_spec,
                    src_to_dst=True,
                )
                kernel_base = self._peer_pack_kernel_base[peer][parity]
                source_ptrs.extend(
                    kernel_base + (source.data_ptr() - source_buf.data_ptr())
                    for _destination, source in peer_pairs
                )
                pairs.extend(peer_pairs)
            plan = CopyPlan(pairs, unified_dtype_kernels=True, source_ptrs=source_ptrs)
            self._direct_consume_plan.append(plan)
            total_launches += plan.launch_count
        logger.info(
            "wbridge rank %d: direct same-node consume ON (%d rounds, %d total kernel launches)",
            self._rank,
            sum(plan is not None for plan in self._direct_consume_plan),
            total_launches,
        )

    def _build_arena_plans(self) -> None:
        """Tensor-dedup receiver: per-round CopyPlans across isolated RECV and rollout arenas.

        RECV offsets normally use ``_recv_arena`` and its parity stride. With merged RECV/PREP they name
        offsets in a template slot and are rebound each epoch to the temporary snapshot buffer. Own/send use
        the rollout ``_arena`` PREP stride; shared-bank grecv offsets are absolute in ``_arena``. Two plans:

        * ``_arena_prepare[ri]``: one direct RECV -> destinations plan containing ``own`` plus each unique
          non-aliased generic ``send`` or exact topology ``topo_send`` payload. Deduplication happens in
          :meth:`arena_layout` first; a full-slice send aliases ``own`` and therefore adds no destination.
          Partial payloads are copied directly from their original RECV name views in the same kernel—never
          from ``own``—so there is no inter-program dependency and no second repack launch.
        * Generic fallback ``_arena_consume[ri]``: my ``own`` slice + each peer's ``grecv`` slice -> model.
        * Topology ``_topo_internal_consume_plan[ri][source_lane]``: one direct wire->model kernel per active
          ``(same-node owner, external source slot)`` lane. The owner's own/PREP descriptor joins its first
          slot, or a source-less lane when it has no external input. Peer plans read imported CUDA-IPC
          addresses over NVLink; there is no local internal GRECV copy or final fan-in consume kernel.

        ``full_spec`` (``local_rounds[ri][0]`` = my deduped recv spec for the round) has
        ``nbytes == own.nb == s2r``. The prepare remains one fused kernel even though its sources and
        destinations now live in different allocations.
        """
        sw = self.router.sender_ws
        rl = self._rank - sw
        classes = self.router.recv_tensor_classes()
        layout = self._arena_layout
        arena = self._arena
        doff_arena = getattr(
            self, "_doff_arena", arena
        )  # generic/offline fallback does not allocate DOFF
        recv_arena = self._recv_arena
        my_spec = self.router.recv_specs[rl]
        self._arena_prepare: list = []
        self._arena_consume: list = []
        Lane = tuple[
            int, int | None
        ]  # (same-node source-column owner, external grecv source or None)
        self._topo_internal_consume_plan: list[dict[Lane, CopyPlan]] = []
        self._topo_internal_consume_own_lane: list[dict[int, Lane]] = []
        self._topo_internal_consume_bytes_by_lane: list[dict[Lane, int]] = []
        self._topo_internal_consume_lane_sources: list[dict[Lane, tuple[int, ...]]] = []
        self._topo_internal_consume_source_lane: list[dict[tuple[int, int], Lane]] = []
        # Streams/events are persistent per independently-ready slot lane and populated lazily below. A
        # column's own PREP payload is attached to its first external slot (or a None lane if it has none),
        # preserving one kernel/column in 1T2R while allowing multiple external slots to commit separately.
        self._topo_internal_consume_stream: dict[Lane, torch.cuda.Stream] = {}
        self._topo_internal_consume_event: dict[Lane, torch.cuda.Event] = {}

        def _selected_consume_pairs(
            source_spec: ShardSpec,
            source_buf: torch.Tensor,
            selected_names: tuple[str, ...] | list[str],
        ) -> list[tuple[torch.Tensor, torch.Tensor]]:
            """Compose selected packed names directly into this worker's model destinations."""
            regions = _packed_name_regions(source_spec, self.dtype_spec)
            pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
            for name in sorted(set(selected_names)):
                assert name in regions, f"internal consume source missing {name}"
                off, nb = regions[name]
                name_spec = source_spec.subset({name})
                pairs.extend(
                    self.load_spec.fuse_copy_pairs(
                        name_spec,
                        source_buf[off : off + nb],
                        self.wksd,
                        self.dtype_spec,
                        src_to_dst=True,
                    )
                )
            return pairs

        for ri, (full_spec, overlap_specs) in enumerate(self.router.local_rounds):
            rd = layout[ri]
            ab = _arena_slot_offset(0, ri, self._recv_depth, self._arena_S)
            # Merged mode snapshots the live parity into one scratch buffer at offset zero before A+R. Build
            # descriptors against slot zero as a pointer template; poll_requests rebases them to the actual
            # epoch allocation. Non-merged mode binds its persistent RECV parity directly.
            rb = (
                0
                if getattr(self, "_merged_recv_prep", False)
                else _arena_slot_offset(0, ri, self._recv_depth, self._recv_S)
            )
            round_names = sorted(
                n for n in self.router.global_rounds[ri] if n in my_spec.entries
            )
            own_off, own_nb = rd["own"]
            prepare_pairs = []
            if overlap_specs:
                own_view = _carve_named(
                    full_spec,
                    arena[ab + own_off : ab + own_off + own_nb],
                    self.dtype_spec,
                )
                # Carve each sender's ORIGINAL packed layout once. A partial send payload can then select
                # whole names from these views without incorrectly rebasing its first selected name to byte 0.
                recv_named = {
                    si: _carve_named(
                        ospec,
                        recv_arena[
                            rb + rd["recv"][si][0] : rb
                            + rd["recv"][si][0]
                            + rd["recv"][si][1]
                        ],
                        self.dtype_spec,
                    )
                    for si, ospec in overlap_specs.items()
                }
                prepare_pairs += full_spec(own_view).setitem_named_pairs(
                    overlap_specs, recv_named
                )

                prepared: dict[tuple[int, int], tuple[str, ...]] = {
                    (own_off, own_nb): tuple(name for name, _ in full_spec),
                }

                def _prepare_payload(
                    shared: tuple[str, ...], slot: tuple[int, int]
                ) -> None:
                    """Add one unique packed payload destination to the fused prepare CopyPlan."""
                    if not shared:
                        return
                    if slot in prepared:
                        # Exact alias: another peer uses this deduplicated payload, or the complete payload
                        # is `own` itself. It has already been added to the fused destination plan.
                        assert prepared[slot] == shared
                        return
                    prepared[slot] = shared
                    s_off, s_nb = slot
                    send_spec = my_spec.subset(set(shared))
                    assert send_spec.nbytes(self.dtype_spec) == s_nb
                    send_view = _carve_named(
                        send_spec,
                        arena[ab + s_off : ab + s_off + s_nb],
                        self.dtype_spec,
                    )
                    send_src_specs = {
                        si: sub
                        for si, ospec in overlap_specs.items()
                        if (sub := ospec.subset(set(shared))).entries
                    }
                    prepare_pairs.extend(
                        send_spec(send_view).setitem_named_pairs(
                            send_src_specs, recv_named
                        )
                    )

                if self._topo_ok:
                    for p_rl, shared in self.router.topology_plan(rl, ri)[
                        "external"
                    ].items():
                        assert p_rl in rd["topo_send"]
                        _prepare_payload(tuple(shared), rd["topo_send"][p_rl])
                else:
                    # Runtime topology resolution can still reject a structurally valid plan (for example,
                    # CUDA-IPC import was disabled). In that case populate the generic single-phase sends.
                    # Once topology resolves successfully these potentially much larger payloads are unused,
                    # so omit them from the assemble kernel even though their fallback arena slots remain.
                    for peer in self._repl_peers:
                        p_rl = peer - sw
                        if p_rl not in rd["send"]:
                            continue
                        shared = tuple(
                            self.router._arena_shared(rl, p_rl, round_names, classes)
                        )
                        _prepare_payload(shared, rd["send"][p_rl])
            self._arena_prepare.append(
                CopyPlan(
                    prepare_pairs,
                    source_region=(recv_arena.data_ptr(), self._recv_payload_S)
                    if getattr(self, "_merged_recv_prep", False)
                    else None,
                )
            )
            if self._topo_ok:
                plans: dict[Lane, CopyPlan] = {}
                own_lane: dict[int, Lane] = {}
                lane_pairs: dict[Lane, list[tuple[torch.Tensor, torch.Tensor]]] = {}
                lane_ptrs: dict[Lane, list[int]] = {}
                lane_sources: dict[Lane, set[int]] = {}

                def _extend_lane(
                    lane: Lane,
                    pairs: list[tuple[torch.Tensor, torch.Tensor]],
                    source_ptrs: list[int] | None = None,
                    source_key: int | None = None,
                ) -> None:
                    if not pairs:
                        return
                    lane_pairs.setdefault(lane, []).extend(pairs)
                    if source_ptrs is not None:
                        lane_ptrs.setdefault(lane, []).extend(source_ptrs)
                    if source_key is not None:
                        lane_sources.setdefault(lane, set()).add(source_key)

                # Self column: attach own PREP to the first external slot. Every exact grecv source remains
                # an independent lane, so its external writer can be released as soon as that lane commits.
                self_slots = [sw + source for source in sorted(rd["grecv_names"])]
                self_primary = self_slots[0] if self_slots else None
                self_own_lane: Lane = (self._rank, self_primary)
                own_lane[self._rank] = self_own_lane
                doff_rd = self._doff_layout[ri]
                doff_own_off = doff_rd["own"][0]
                self_pairs = list(
                    self.load_spec.fuse_copy_pairs(
                        full_spec,
                        doff_arena[
                            doff_own_off + own_off : doff_own_off + own_off + own_nb
                        ],
                        self.wksd,
                        self.dtype_spec,
                        src_to_dst=True,
                    )
                )
                _extend_lane(self_own_lane, self_pairs, source_key=self._rank)
                for source_rl, names in rd["grecv_names"].items():
                    _g_off, g_nb = rd["grecv"][source_rl]
                    doff_g_off, doff_g_nb = doff_rd["grecv"][source_rl]
                    assert doff_g_nb == g_nb
                    source_spec = self.router.recv_specs[source_rl].subset(set(names))
                    assert source_spec.nbytes(self.dtype_spec) == g_nb
                    _extend_lane(
                        (self._rank, sw + source_rl),
                        _selected_consume_pairs(
                            source_spec,
                            doff_arena[doff_g_off : doff_g_off + g_nb],
                            names,
                        ),
                        source_key=sw + source_rl,
                    )

                # Remote columns: split descriptors by exact external source slot. The own descriptor joins
                # the first slot, keeping the common 1T2R case at one kernel per source column.
                for peer in self._topo_int_peers_by_round[ri]:
                    peer_arena = self._repl_peer_doff_arena[peer]
                    peer_tensor_base = peer_arena.data_ptr()
                    peer_kernel_base = self._repl_peer_doff_kernel_base[peer]
                    descriptors = self._topo_internal_consume_src[peer][ri]
                    peer_slots = sorted(
                        {slot for slot, *_rest in descriptors if slot is not None}
                    )
                    peer_primary = peer_slots[0] if peer_slots else None
                    peer_own_lane: Lane = (peer, peer_primary)
                    if any(slot is None for slot, *_rest in descriptors):
                        own_lane[peer] = peer_own_lane
                    for slot_source, src_off, source_spec, names in descriptors:
                        src_nb = source_spec.nbytes(self.dtype_spec)
                        new_pairs = _selected_consume_pairs(
                            source_spec,
                            peer_arena[src_off : src_off + src_nb],
                            names,
                        )
                        new_ptrs = [
                            peer_kernel_base + (source.data_ptr() - peer_tensor_base)
                            for _destination, source in new_pairs
                        ]
                        lane = (
                            peer,
                            peer_primary if slot_source is None else slot_source,
                        )
                        _extend_lane(
                            lane,
                            new_pairs,
                            new_ptrs,
                            source_key=(peer if slot_source is None else slot_source),
                        )

                lane_bytes: dict[Lane, int] = {}
                for lane in sorted(
                    lane_pairs,
                    key=lambda item: (item[0], -1 if item[1] is None else item[1]),
                ):
                    pairs = lane_pairs[lane]
                    source_ptrs = lane_ptrs.get(lane)
                    if lane[0] != self._rank:
                        assert source_ptrs is not None and len(source_ptrs) == len(
                            pairs
                        )
                    else:
                        assert source_ptrs is None
                    # A lane can contain both BF16 model weights and FP32 norms. Keep flat and transformed
                    # mappings unified within each dtype, while issuing the unavoidable one kernel per dtype.
                    plan = CopyPlan(
                        pairs,
                        unified_dtype_kernels=True,
                        source_ptrs=source_ptrs,
                    )
                    assert plan.launch_count == len({dst.dtype for dst, _src in pairs})
                    plans[lane] = plan
                    lane_bytes[lane] = sum(
                        src.numel() * src.element_size() for _dst, src in pairs
                    )
                    self._topo_internal_consume_stream.setdefault(
                        lane,
                        torch.cuda.Stream(device=arena.device),
                    )
                    self._topo_internal_consume_event.setdefault(
                        lane, torch.cuda.Event()
                    )

                expected_owners = {self._rank, *self._topo_int_peers_by_round[ri]}
                if full_spec.entries:
                    assert {owner for owner, _source in plans} == expected_owners, (
                        f"internal consume owners ri={ri}: "
                        f"built={sorted({owner for owner, _source in plans})} "
                        f"expected={sorted(expected_owners)}"
                    )
                self._topo_internal_consume_plan.append(plans)
                self._topo_internal_consume_own_lane.append(own_lane)
                self._topo_internal_consume_bytes_by_lane.append(lane_bytes)
                frozen_lane_sources = {
                    lane: tuple(sorted(sources))
                    for lane, sources in lane_sources.items()
                }
                self._topo_internal_consume_lane_sources.append(frozen_lane_sources)
                self._topo_internal_consume_source_lane.append(
                    {
                        (lane[0], source): lane
                        for lane, sources in frozen_lane_sources.items()
                        for source in sources
                    }
                )
                self._arena_consume.append(None)
            else:
                # Generic staged fallback: consume own + every filled peer GRECV slot in one local plan.
                cpairs = list(
                    self.load_spec.fuse_copy_pairs(
                        full_spec,
                        arena[ab + own_off : ab + own_off + own_nb],
                        self.wksd,
                        self.dtype_spec,
                        src_to_dst=True,
                    )
                )
                for peer in self._repl_peers:
                    p_rl = peer - sw
                    if p_rl not in rd["grecv"]:
                        continue
                    shared = self.router._arena_shared(rl, p_rl, round_names, classes)
                    qspec = self.router.recv_specs[p_rl].subset(set(shared))
                    if qspec.entries:
                        g_off, g_nb = rd["grecv"][p_rl]
                        cpairs += self.load_spec.fuse_copy_pairs(
                            qspec,
                            arena[g_off : g_off + g_nb],
                            self.wksd,
                            self.dtype_spec,
                            src_to_dst=True,
                        )
                self._arena_consume.append(CopyPlan(cpairs))
                self._topo_internal_consume_plan.append({})
                self._topo_internal_consume_own_lane.append({})
                self._topo_internal_consume_bytes_by_lane.append({})
                self._topo_internal_consume_lane_sources.append({})
                self._topo_internal_consume_source_lane.append({})
        # RS-on: per-round CPU->GPU staging hops driving the LocalStagingEngine. Senders land ALL rounds in
        # the full-depth CPU arena; before assemble(ri) the main thread H2D-copies round ri's per-sender
        # slices into the parity-selected GPU arena RECV zone (same offsets assemble reads). (src,dst,size) lists
        # are stable across WTs. Empty list => no-op (RS-off, or a round with no incoming data).
        self._rs_h2d: list[tuple[list[int], list[int], list[int]]] = []
        if self.receiver_staging:
            cpu_base = self._cpu_recv.data_ptr()
            gpu_base = recv_arena.data_ptr()
            for ri in range(len(self.router.local_rounds)):
                cpu_r = self._cpu_recv_layout[ri]
                src, dst, sz = [], [], []
                for si, (goff, nb) in layout[ri]["recv"].items():
                    src.append(cpu_base + cpu_r[si][0])
                    dst.append(
                        gpu_base
                        + _arena_slot_offset(goff, ri, self._recv_depth, self._recv_S)
                    )
                    sz.append(nb)
                self._rs_h2d.append((src, dst, sz))
        else:
            self._rs_h2d = [([], [], []) for _ in self.router.local_rounds]
        logger.info(
            "wbridge rank %d: built %d fused-prepare tensor-dedup rounds "
            "(recv %.2f GiB, rollout %.2f GiB, %d peers)",
            self._rank,
            len(self._arena_prepare),
            recv_arena.numel() / 1024**3,
            arena.numel() / 1024**3,
            len(self._repl_peers),
        )

    def _two_stage_save(self, full_spec, overlap_specs, wire_bufs) -> None:
        """model -> wire via the transient logical buffer (fallback path / self-check reference)."""
        buf = full_spec.make_named_buffer(self.dtype_spec, self.device)
        batched_copy(
            self.load_spec.copy_fromto_pairs(
                full_spec, buf, self.wksd, src_to_dst=False
            )
        )
        bound = full_spec(buf)
        pairs = []
        for peer, ospec in overlap_specs.items():
            _nb, pp = bound.pack_into_pairs(ospec, wire_bufs[peer])
            pairs += pp
        batched_copy(pairs)

    def _selfcheck_round(self, ri, full_spec, overlap_specs) -> None:
        """Assert the fused SAVE matches the 2-stage path byte-for-byte (opt-in; transient scratch).

        Sender-only: compares the packed wire bytes (non-destructive; reads the real model) and raises
        :class:`FuseUnsupported` on mismatch so the round falls back to the 2-stage path.
        """
        a = {p: torch.zeros_like(self._data_buf[p][0]) for p in overlap_specs}
        b = {p: torch.zeros_like(self._data_buf[p][0]) for p in overlap_specs}
        self._two_stage_save(full_spec, overlap_specs, a)
        pairs = []
        for peer, ospec in overlap_specs.items():
            pairs += self.load_spec.fuse_copy_pairs(
                ospec, b[peer], self.wksd, self.dtype_spec, src_to_dst=False
            )
        batched_copy(pairs)
        torch.cuda.synchronize()
        for peer, ospec in overlap_specs.items():
            nb = ospec.nbytes(self.dtype_spec)
            if not torch.equal(a[peer][:nb], b[peer][:nb]):
                # Localize the first differing tensor (per-ospec byte walk) — pinpoints which consolidated
                # slice shape the fuse mis-maps. Diagnostic only; still raises to fall back to 2-stage.
                if os.environ.get("WBRIDGE_DEDUP_DIAG") == "1":
                    off = 0
                    for name, shards in ospec:
                        ln = shards_nbytes(shards, self.dtype_spec[name])
                        if ln and not torch.equal(
                            a[peer][off : off + ln], b[peer][off : off + ln]
                        ):
                            print(
                                f"[FUSE-DIAG] SAVE mismatch round {ri} peer {peer} tensor={name} "
                                f"nbytes={ln} shards={[list(s) for s in shards]}",
                                flush=True,
                            )
                        off += ln
                raise FuseUnsupported(f"selfcheck SAVE mismatch round {ri} peer {peer}")

    # -------------------------------------------------------------- transfer
    def _seq(self, ri: int) -> int:
        """Globally-monotonic flag value for round *ri* of the current update epoch (1-indexed)."""
        return self._seq_at(self._epoch, ri)

    def _seq_at(self, epoch: int, ri: int) -> int:
        """Sequence value for an explicit epoch/round generation."""
        assert epoch >= 0 and 0 <= ri < self.num_rounds
        return epoch * self.num_rounds + ri + 1

    def _ctl_acc(self, key: str, dt: float, imm: int) -> None:
        d = self._ctl.setdefault(
            key, [0.0, 0, 0]
        )  # [total_s, count, immediate-hit count]
        d[0] += dt
        d[1] += 1
        d[2] += imm

    def _ctl_take_report(self, wt: int, nrounds: int) -> list[str]:
        """Snapshot and reset one epoch's control profile without performing logger output."""
        if not self._ctlp or not self._ctl:
            return []
        lines = []
        for k in sorted(self._ctl):
            t, n, imm = self._ctl[k]
            lines.append(
                "[ctl-prof] wt=%d %-8s total=%6.1fms n=%3d per=%.2fms imm=%d/%d /round=%.1fms"
                % (
                    wt,
                    k,
                    t * 1e3,
                    n,
                    (t / n * 1e3 if n else 0.0),
                    imm,
                    n,
                    t * 1e3 / max(nrounds, 1),
                )
            )
        nb = getattr(self, "_ctl_bytes", 0)
        if nb:
            # Absolute achieved bulk bandwidth for THIS rank this epoch — the number that is directly
            # comparable to the standalone ladder benchmark's GB/s/rank.
            bw = self._ctl.get("b_wait", (0.0, 0, 0))[0]
            lines.append(
                "[ctl-prof] wt=%d b_bytes  total=%.3f GiB over b_wait=%.1fms -> %.2f GB/s"
                % (wt, nb / 1024**3, bw * 1e3, (nb / bw / 1e9) if bw else 0.0)
            )
            self._ctl_bytes = 0
        self._ctl = {}
        return lines

    def _flag_reaper_ensure(self) -> None:
        """Start the off-path async-handle reaper.

        This is deliberately not a control-plane publisher: producers submit their own writes immediately.
        The daemon only calls ``wait`` later so backend batch handles are retired and failures are reported.
        A stalled handle can therefore delay cleanup, but can never delay a different flag submission.
        """
        if not hasattr(self, "_flag_reaper_lock"):
            self._flag_reaper_lock = threading.Lock()
            self._flag_reaper_q = None
            self._flag_reaper_thread = None
            self._flag_reaper_errors = []
            self._flag_submit_lock = threading.Lock()
        with self._flag_reaper_lock:
            if self._flag_reaper_thread is not None:
                return
            self._flag_reaper_q = queue.Queue()
            self._flag_reaper_thread = threading.Thread(
                target=self._flag_reaper_worker,
                name="wbridge-flag-reaper",
                daemon=True,
            )
            self._flag_reaper_thread.start()

    def _flag_reaper_worker(self) -> None:
        """Retire already-submitted flag handles without participating in protocol progress."""
        assert self._flag_reaper_q is not None
        while True:
            item = self._flag_reaper_q.get()
            if item is None:
                return
            handle, kind, peer, seq = item
            try:
                self.engine.wait([handle])
                self._trace_state("flag_reaped", kind=kind, peer=peer, seq=seq)
            except BaseException as exc:  # noqa: BLE001 - surfaced by the producer-side health check
                with self._flag_reaper_lock:
                    self._flag_reaper_errors.append(exc)
                logger.exception(
                    "wbridge rank %d: asynchronous flag failed kind=%d peer=%d seq=%d",
                    self._rank,
                    kind,
                    peer,
                    seq,
                )

    def _flag_reaper_check(self) -> None:
        """Raise an already-observed asynchronous flag error without waiting for outstanding handles."""
        if not hasattr(self, "_flag_reaper_lock"):
            return
        with self._flag_reaper_lock:
            error = self._flag_reaper_errors[0] if self._flag_reaper_errors else None
        if error is not None:
            raise RuntimeError(
                f"wbridge rank {self._rank}: asynchronous flag write failed"
            ) from error

    def _flag_reaper_stop(self) -> None:
        """Drain and stop the handle reaper during endpoint teardown only."""
        if not hasattr(self, "_flag_reaper_lock"):
            return
        with self._flag_reaper_lock:
            work_q = self._flag_reaper_q
            thread = self._flag_reaper_thread
        if work_q is None or thread is None:
            return
        work_q.put(None)
        thread.join()
        with self._flag_reaper_lock:
            self._flag_reaper_q = None
            self._flag_reaper_thread = None

    def _flag_message_slot(self, peer_slot: int, seq: int) -> tuple[int, int]:
        """Return ``(flat slot, round)`` for one peer's exclusive message word."""
        ri = (seq - 1) % self.num_rounds
        return peer_slot * self.num_rounds + ri, ri

    def _control_flag_landed(
        self, transport: str, kind: int, peer: int, seq: int
    ) -> None:
        """Publish a control record into the aligned int64 word polled by the protocol.

        Required host contract: naturally aligned 64-bit loads and stores to cache-coherent pinned
        host memory must be single-copy atomic across threads. The Torch allocation is aligned, and
        the int64 slot stride preserves that alignment. The receive thread writes it directly to avoid
        constructing a Torch operation for every 16-byte socket record. A host that does not provide
        this atomicity is unsupported: a torn read could fabricate a future sequence and let the
        protocol consume or reuse a buffer prematurely. TCP preserves order per peer, and the max
        guard makes a stale duplicate harmless; neither property protects against a torn load/store.
        """
        if kind in (self._RELAY_DATA_KIND, self._RELAY_ACK_KIND):
            gid, actual_seq = self._decode_relay_token(seq)
            wt, ri = divmod(actual_seq - 1, self.num_rounds)
            slot = self._relay_flag_slot(peer, gid, actual_seq)
            buf = (
                self._relay_data_buf
                if kind == self._RELAY_DATA_KIND
                else self._relay_ack_buf
            )
            stored_seq = actual_seq
            op = "relay_data" if kind == self._RELAY_DATA_KIND else "relay_ack"
        else:
            wt, ri = divmod(seq - 1, self.num_rounds)
            stored_seq = seq
            op = ("ack", "ready", "cons")[kind] if 0 <= kind <= 2 else "invalid"
        if kind == 0:
            slot, _ = self._flag_message_slot(self.flag_slot_of[peer], seq)
            buf = self._flag_buf
        elif kind == 1:
            slot, _ = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
            buf = self._repl_flag_buf
        elif kind == 2:
            slot, _ = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
            buf = self._repl_cons_buf
        elif kind not in (self._RELAY_DATA_KIND, self._RELAY_ACK_KIND):
            raise ValueError(
                f"invalid {transport} control kind {kind} from peer {peer}"
            )
        word = ctypes.c_int64.from_address(buf.data_ptr() + slot * self._FLAG_ITEMSIZE)
        if stored_seq > word.value:
            word.value = stored_seq
        from wbridge.backend import gantt

        now = time.time()
        gantt.rec(
            f"ctl-{transport}",
            self._rank,
            wt,
            f"{transport}_{op}_recv_peer_{peer}",
            ri,
            now,
            now,
        )

    def _encode_relay_token(self, gid: int, seq: int) -> int:
        if not 0 <= gid < getattr(self, "_relay_num_groups", 0):
            raise ValueError(f"invalid relay group id {gid}")
        if not 0 < seq < (1 << self._RELAY_SEQ_BITS):
            raise ValueError(
                f"relay sequence {seq} exceeds {self._RELAY_SEQ_BITS}-bit token field"
            )
        return (gid << self._RELAY_SEQ_BITS) | seq

    def _decode_relay_token(self, token: int) -> tuple[int, int]:
        mask = (1 << self._RELAY_SEQ_BITS) - 1
        gid, seq = token >> self._RELAY_SEQ_BITS, token & mask
        if not 0 <= gid < getattr(self, "_relay_num_groups", 0) or seq <= 0:
            raise ValueError(f"invalid relay control token {token}")
        return gid, seq

    def _relay_flag_slot(self, peer: int, gid: int, seq: int) -> int:
        if not 0 <= peer < self.world_size:
            raise ValueError(f"invalid relay peer rank {peer}")
        ri = (seq - 1) % self.num_rounds
        return (peer * self._relay_num_groups + gid) * self.num_rounds + ri

    def _relay_flag_reached(self, kind: int, peer: int, gid: int, seq: int) -> bool:
        self._control_check()
        slot = self._relay_flag_slot(peer, gid, seq)
        buf = (
            self._relay_data_buf
            if kind == self._RELAY_DATA_KIND
            else self._relay_ack_buf
        )
        return int(buf[slot].item()) >= seq

    def _poll_relay_flag(
        self,
        kind: int,
        peer: int,
        gid: int,
        seq: int,
        *,
        timeout_s: float = 600.0,
    ) -> None:
        t0 = time.time()
        while not self._relay_flag_reached(kind, peer, gid, seq):
            if time.time() - t0 > timeout_s:
                label = "DATA" if kind == self._RELAY_DATA_KIND else "ACK"
                raise TimeoutError(
                    f"wbridge rank {self._rank}: relay {label} timeout peer={peer} "
                    f"group={gid} seq={seq}"
                )
            time.sleep(1e-4)

    def _relay_emit(self, kind: int, peer: int, gid: int, seq: int) -> None:
        """Publish one group-specific DATA or RECV-drained ACK sequence."""
        if kind not in (self._RELAY_DATA_KIND, self._RELAY_ACK_KIND):
            raise ValueError(f"invalid relay flag kind {kind}")
        self._flag_reaper_check()
        token = self._encode_relay_token(gid, seq)
        tcp_control = getattr(self, "_tcp_control", None)
        if tcp_control is not None and tcp_control.has_peer(peer):
            tcp_control.send(kind, peer, token)
            return

        slot = self._relay_flag_slot(peer, gid, seq)
        src = (
            self._relay_data_src
            if kind == self._RELAY_DATA_KIND
            else self._relay_ack_src
        )
        dst_base = (
            self._relay_data_dst[peer]
            if kind == self._RELAY_DATA_KIND
            else self._relay_ack_dst[peer]
        )
        src[slot] = seq
        remote_slot = self._relay_flag_slot(self._rank, gid, seq)
        src_addr = src.data_ptr() + slot * self._FLAG_ITEMSIZE
        dst_addr = dst_base + remote_slot * self._FLAG_ITEMSIZE
        with self._flag_submit_lock:
            handle = self.engine.write_async(
                self._relay_peer_session[peer],
                [src_addr],
                [dst_addr],
                [self._FLAG_ITEMSIZE],
            )
        if handle is not None:
            self._flag_reaper_ensure()
            assert self._flag_reaper_q is not None
            self._flag_reaper_q.put((handle, kind, peer, token))

    def _tcp_flag_landed(self, kind: int, peer: int, seq: int) -> None:
        self._control_flag_landed("tcp", kind, peer, seq)

    def _control_check(self) -> None:
        tcp_control = getattr(self, "_tcp_control", None)
        if tcp_control is not None:
            tcp_control.check()

    def _post_flag_async(self, kind: int, peer: int, seq: int) -> None:
        """Submit one control word directly and return without waiting for remote completion."""
        self._flag_reaper_check()
        wt, ri = divmod(seq - 1, self.num_rounds)
        op = ("ctl_ack", "ctl_ready", "ctl_cons")[kind]
        tcp_control = getattr(self, "_tcp_control", None)
        if tcp_control is not None and tcp_control.has_peer(peer):
            started = time.perf_counter() if self._ctlp else None
            self._trace_state(
                "tcp_ctl_flag_submit",
                kind=kind,
                peer=peer,
                seq=seq,
                round=ri,
            )
            with gantt_span("ctl", self._rank, wt, op, ri):
                with gantt_span("ctl", self._rank, wt, f"{op}_submit", ri):
                    tcp_control.send(kind, peer, seq)
            self._trace_state(
                "tcp_ctl_flag_submitted",
                kind=kind,
                peer=peer,
                seq=seq,
                round=ri,
            )
            if started is not None:
                self._ctl_acc(
                    ("w_ack", "w_ready", "w_cons")[kind],
                    time.perf_counter() - started,
                    0,
                )
            return
        if not hasattr(self, "_flag_submit_lock"):
            self._flag_submit_lock = threading.Lock()
        if kind == 0:  # trainer DATA-ready or receiver ACK
            slot, slot_ri = self._flag_message_slot(self.flag_slot_of[peer], seq)
            src, sess, dst_base = (
                self._flag_src,
                self.peer_session[peer],
                self._flag_dst[peer],
            )
        elif kind == 1:  # rollout external READY
            slot, slot_ri = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
            src = self._repl_flag_src
            sess, dst_base = self._repl_peer_session[peer], self._repl_flag_dst[peer]
        else:  # rollout external CONS
            slot, slot_ri = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
            src = self._repl_cons_src
            sess, dst_base = self._repl_peer_session[peer], self._repl_cons_dst[peer]
        assert slot_ri == ri
        started = time.perf_counter() if self._ctlp else None
        self._trace_state("ctl_flag_submit", kind=kind, peer=peer, seq=seq, slot=slot)
        with gantt_span("ctl", self._rank, wt, op, ri):
            with gantt_span("ctl", self._rank, wt, f"{op}_submit", ri):
                with self._flag_submit_lock:
                    src[slot] = seq
                    src_ptr = src.data_ptr() + slot * self._FLAG_ITEMSIZE
                    dst_ptr = dst_base + ri * self._FLAG_ITEMSIZE
                    handle = self.engine.write_async(
                        sess,
                        [src_ptr],
                        [dst_ptr],
                        [self._FLAG_ITEMSIZE],
                    )
        self._trace_state(
            "ctl_flag_submitted", kind=kind, peer=peer, seq=seq, slot=slot
        )
        if started is not None:
            self._ctl_acc(
                ("w_ack", "w_ready", "w_cons")[kind], time.perf_counter() - started, 0
            )
        if handle is not None:
            self._flag_reaper_ensure()
            assert self._flag_reaper_q is not None
            self._flag_reaper_q.put((handle, kind, peer, seq))

    def _flag_emit(self, kind: int, peer: int, seq: int) -> None:
        """Publish ACK/DATA-ready, READY, or CONS with no shared writer queue or completion wait."""
        if kind == 0:
            self._write_flag(peer, seq)
        elif kind == 1:
            self._write_repl_flag(peer, seq)
        else:
            self._write_repl_cons_flag(peer, seq)

    def _write_flag(self, peer: int, seq: int) -> None:
        """Asynchronously write *seq* into its exclusive peer/round DATA-ready or ACK word."""
        self._post_flag_async(0, peer, seq)

    def _poll_flag(self, peer: int, seq: int, *, timeout_s: float = 600.0) -> None:
        """Spin until *peer* has written a flag >= *seq* into our incoming slot for it."""
        slot, _ = self._flag_message_slot(self.flag_slot_of[peer], seq)
        buf = self._flag_buf
        have = int(buf[slot].item())
        self._trace_state("wait_wire_flag", peer=peer, want=seq, have=have)
        if self._ctlp:
            _t = time.perf_counter()
            _imm = 1 if int(buf[slot].item()) >= seq else 0
        t0 = time.time()
        warned = 0.0
        while int(buf[slot].item()) < seq:
            self._control_check()
            time.sleep(1e-4)
            el = time.time() - t0
            if el > timeout_s:
                raise TimeoutError(
                    f"wbridge rank {self._rank}: waited {el:.0f}s for peer {peer} "
                    f"flag (want>={seq}, have {int(buf[slot].item())})"
                )
            if el - warned >= 30.0:
                warned = el
                logger.warning(
                    "wbridge rank %d: waiting %.0fs for peer %d flag (want>=%d, have %d)",
                    self._rank,
                    el,
                    peer,
                    seq,
                    int(buf[slot].item()),
                )
        self._trace_state(
            "wait_wire_flag_done", peer=peer, want=seq, have=int(buf[slot].item())
        )
        if self._ctlp:
            self._ctl_acc("p_recv", time.perf_counter() - _t, _imm)

    def _flag_reached(self, peer: int, seq: int) -> bool:
        """Non-blocking counterpart of :meth:`_poll_flag` for scheduler-side readiness checks.

        Incoming sequence flags live in pinned CPU memory, so peeking one does not synchronize or otherwise
        touch CUDA.  The sender writes the flag only after its bulk transfer completes; consequently a true
        result carries the same data-before-flag guarantee as the blocking poll.
        """
        self._control_check()
        slot, _ = self._flag_message_slot(self.flag_slot_of[peer], seq)
        return int(self._flag_buf[slot].item()) >= seq

    def _write_repl_flag(self, peer: int, seq: int) -> None:
        """One-sided write of *seq* into class-peer *peer*'s incoming repl-flag slot for this rank."""
        if peer in getattr(self, "_repl_peer_local_flags", {}):
            self._trace_state("write_ready_flag_local", peer=peer, seq=seq)
            self._repl_local_flags.publish_ready(seq)
            return
        self._post_flag_async(1, peer, seq)

    def _repl_flag_reached(self, peer: int, seq: int) -> bool:
        """Non-blockingly test one replica peer's READY sequence.

        Internal peers use a node-local mmap word; external peers use the existing pinned CPU flag slot.
        This lets one E+C dispatcher scan every internal dependency and service peers in actual readiness
        order instead of blocking on an arbitrary fixed peer order.
        """
        local = getattr(self, "_repl_peer_local_flags", {}).get(peer)
        if local is not None:
            return local.ready() >= seq
        self._control_check()
        slot, _ = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
        return int(self._repl_flag_buf[slot].item()) >= seq

    def _topo_slot_ready_reached(
        self, owner: int, source: int, slot: int, seq: int
    ) -> bool:
        """Test READY for ``source -> owner.grecv[source, slot]``."""
        local = self._repl_peer_local_flags.get(owner)
        channel = self._topo_peer_slot_channel.get(owner, {}).get((source, slot))
        if local is None or channel is None:
            raise RuntimeError(
                f"missing topology slot READY mapping owner={owner} source={source} slot={slot}"
            )
        return local.ready(channel) >= seq

    def _publish_topo_slot_ready(
        self,
        source: int,
        slot: int,
        readers: tuple[int, ...],
        seq: int,
        stream: torch.cuda.Stream | None = None,
    ) -> None:
        """Publish one DOFF source slot to all same-node readers.

        The CUDA event is per reader (IPC import requirement); the CPU sequence is one shared channel, so
        reader fan-out costs one coherent store rather than one control-plane message per consumer. ``stream``
        is the source's offload stream; publishing the CPU word after enqueueing these records lets readers
        wait for the exact D2D-copy generation without a host synchronization.
        """
        key = (source, slot)
        channel = self._topo_local_slot_channel.get(key)
        if channel is None:
            raise RuntimeError(
                f"missing local topology slot channel for external source {source} slot={slot}"
            )
        events = self._topo_slot_ready_event.get(key, {})
        for reader in readers:
            event = events.get(reader)
            if event is None:
                raise RuntimeError(
                    f"missing topology slot event source={source} slot={slot} reader={reader}"
                )
            if stream is None:
                event.record()
            else:
                event.record(stream)
        self._repl_local_flags.publish_ready(seq, channel)

    def _write_topo_slot_cons_flag(
        self, owner: int, source: int, slot: int, seq: int
    ) -> None:
        """Commit this reader's completed consume of one owner/external-source slot."""
        local = self._repl_peer_local_flags.get(owner)
        channel = self._topo_peer_slot_channel.get(owner, {}).get((source, slot))
        if local is None or channel is None:
            raise RuntimeError(
                f"missing topology slot commit mapping owner={owner} source={source} slot={slot}"
            )
        local.publish_consumed(self._repl_peer_slot_of_me[owner], seq, channel)

    def _topo_slot_cons_flag_reached(
        self, reader: int, source: int, slot: int, seq: int
    ) -> bool:
        """Test whether one local reader committed this worker's external slot."""
        channel = self._topo_local_slot_channel.get((source, slot))
        if channel is None:
            raise RuntimeError(
                f"missing local topology slot channel for external source {source} slot={slot}"
            )
        return (
            self._repl_local_flags.consumed(
                self._repl_flag_slot_of[reader],
                channel,
            )
            >= seq
        )

    def _poll_repl_flag(self, peer: int, seq: int, *, timeout_s: float = 600.0) -> None:
        """Spin until class-peer *peer* has written a repl-flag >= *seq* into our slot for it."""
        local = getattr(self, "_repl_peer_local_flags", {}).get(peer)
        buf = self._repl_flag_buf
        if local is not None:
            read = local.ready
        else:
            slot, _ = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
            read = lambda: int(buf[slot].item())
        have = read()
        self._trace_state("wait_ready_flag", peer=peer, want=seq, have=have)
        if self._ctlp:
            _t = time.perf_counter()
            _imm = 1 if read() >= seq else 0
        t0 = time.time()
        warned = 0.0
        while read() < seq:
            if not local:
                self._control_check()
            time.sleep(1e-4)
            el = time.time() - t0
            if el > timeout_s:
                raise TimeoutError(
                    f"wbridge rank {self._rank}: waited {el:.0f}s for repl peer {peer} "
                    f"flag (want>={seq}, have {read()})"
                )
            if el - warned >= 30.0:
                warned = el
                logger.warning(
                    "wbridge rank %d: waiting %.0fs for repl peer %d flag (want>=%d, have %d)",
                    self._rank,
                    el,
                    peer,
                    seq,
                    read(),
                )
        self._trace_state("wait_ready_flag_done", peer=peer, want=seq, have=read())
        if self._ctlp:
            self._ctl_acc("p_ready", time.perf_counter() - _t, _imm)

    def _write_repl_cons_flag(self, peer: int, seq: int) -> None:
        """One-sided write of *seq* into class-peer *peer*'s incoming repl-CONSUMED-flag slot for this rank.
        Signals that this rank's internal-consume kernel finished reading the peer's generation, or that its
        own external ingress generation is fully consumed, so the protected source/destination may be reused."""
        local = getattr(self, "_repl_peer_local_flags", {}).get(peer)
        if local is not None:
            self._trace_state("write_consumed_flag_local", peer=peer, seq=seq)
            # This rank occupies this slot in the peer's incoming replica-peer ordering.
            local.publish_consumed(self._repl_peer_slot_of_me[peer], seq)
            return
        self._post_flag_async(2, peer, seq)

    def _repl_cons_flag_reached(self, peer: int, seq: int) -> bool:
        """Non-blockingly test one peer's consumed/release sequence."""
        if peer in getattr(self, "_repl_peer_local_flags", {}):
            return self._repl_local_flags.consumed(self._repl_flag_slot_of[peer]) >= seq
        self._control_check()
        slot, _ = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
        return int(self._repl_cons_buf[slot].item()) >= seq

    def _poll_repl_cons_flag(
        self, peer: int, seq: int, *, timeout_s: float = 600.0
    ) -> None:
        """Spin until class-peer *peer* has consumed round >= *seq* (its repl-consumed-flag for us)."""
        buf = self._repl_cons_buf
        local = peer in getattr(self, "_repl_peer_local_flags", {})
        if local:
            peer_slot = self._repl_flag_slot_of[peer]
            read = lambda: self._repl_local_flags.consumed(peer_slot)
        else:
            slot, _ = self._flag_message_slot(self._repl_flag_slot_of[peer], seq)
            read = lambda: int(buf[slot].item())
        have = read()
        self._trace_state("wait_consumed_flag", peer=peer, want=seq, have=have)
        if self._ctlp:
            _t = time.perf_counter()
            _imm = 1 if read() >= seq else 0
        t0 = time.time()
        warned = 0.0
        while read() < seq:
            if not local:
                self._control_check()
            time.sleep(1e-4)
            el = time.time() - t0
            if el > timeout_s:
                raise TimeoutError(
                    f"wbridge rank {self._rank}: waited {el:.0f}s for repl peer {peer} "
                    f"consumed-flag (want>={seq}, have {read()})"
                )
            if el - warned >= 30.0:
                warned = el
                logger.warning(
                    "wbridge rank %d: waiting %.0fs for repl peer %d consumed-flag (want>=%d, have %d)",
                    self._rank,
                    el,
                    peer,
                    seq,
                    read(),
                )
        self._trace_state("wait_consumed_flag_done", peer=peer, want=seq, have=read())
        if self._ctlp:
            self._ctl_acc("p_cons", time.perf_counter() - _t, _imm)
