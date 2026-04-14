# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-ignore-all-errors

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from typing import Optional

from monarch.actor import Actor, endpoint
from monarch.config import configured
from monarch.remotemount.fast_pack import (  # noqa: F401
    block_hashes,
    CHUNK_SIZE,
    HASH_BLOCK_SIZE,
    load_pack_index,
    pack_directory_chunked,
    save_pack_index,
)

logger: logging.Logger = logging.getLogger(__name__)

CACHE_DIR = "/tmp/monarch_remotemount_cache"


def _point_to_key(point: dict) -> str:
    if not point:
        return ""
    return "_".join(f"{k}_{v}" for k, v in point.items())


def prepare_mount_point(path: str) -> None:
    """Create the mount point directory, recovering from dead FUSE mounts.

    ``os.makedirs(exist_ok=True)`` raises ``FileExistsError`` when the path
    exists but is a stale FUSE mount (``os.path.isdir`` returns False on
    dead mounts).  Detect this case, unmount, and retry.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        # May be a dead FUSE mount — try to clean it up.
        result = subprocess.run(
            ["fusermount3", "-u", "-z", path], capture_output=True, text=True
        )
        if result.returncode != 0:
            # Not a FUSE mount or unmount failed — try plain umount.
            subprocess.run(["umount", "-l", path], capture_output=True)
        os.makedirs(path, exist_ok=True)


def _resolve_path(path: str) -> str:
    """Replace ``$SUBDIR`` with this actor's mesh-coordinate key, if present."""
    if "$SUBDIR" not in path:
        return path
    from monarch.actor import context

    rank = context().actor_instance.rank
    return path.replace("$SUBDIR", _point_to_key(dict(rank)))


def classify_workers(
    client_hashes: list[str],
    client_total_size: int,
    worker_states: list[tuple[list[str], int]],
) -> tuple[list[int], dict[int, list[int] | None]]:
    """Classify workers as fresh, partial, or stale.

    Args:
        client_hashes: list of block hash strings from client
        client_total_size: total packed data size on client
        worker_states: list of (remote_hashes, remote_size) tuples

    Returns:
        (fresh_ranks, worker_dirty) where:
        - fresh_ranks: list of rank indices that are up-to-date
        - worker_dirty: dict {rank: list[int] | None} — dirty block
          indices for partial workers, or None for stale workers
    """
    fresh_ranks = []
    worker_dirty = {}
    for rank, (remote_hashes, remote_size) in enumerate(worker_states):
        if remote_hashes == client_hashes and remote_size == client_total_size:
            fresh_ranks.append(rank)
        elif remote_hashes:
            # Partial: compare overlapping blocks, mark new/changed as dirty.
            min_blocks = min(len(remote_hashes), len(client_hashes))
            dirty = [
                i for i in range(min_blocks) if remote_hashes[i] != client_hashes[i]
            ]
            # Any blocks beyond the old count are new and need transfer.
            dirty.extend(range(min_blocks, len(client_hashes)))
            # If size changed, the last overlapping block likely changed
            # (partial block at the boundary may have different content).
            if remote_size != client_total_size and min_blocks > 0:
                last = min_blocks - 1
                if last not in dirty:
                    dirty.append(last)
                    dirty.sort()
            worker_dirty[rank] = dirty
        else:
            worker_dirty[rank] = None
    return fresh_ranks, worker_dirty


class FUSEActor(Actor):
    def __init__(self, chunk_size, backend="slurm"):
        self.chunk_size = chunk_size
        self.backend = backend
        self.meta = None
        self.chunks = []
        self._chunk_storage = None
        self._chunk_offsets = None
        self._next_chunk_idx = 0
        self._block_hashes = []
        self._total_size = 0
        self._fuse_handle = None
        self._cache_path = None
        self._pack_index = {}
        # Dirty block indices accumulated since the last refresh.
        # None means a full buffer copy is needed (initial mount or full transfer).
        self._pending_dirty_blocks: list[int] | None = None

    def _alloc_storage(self, total_size, truncate=False):
        """Allocate or resize chunk storage (file-backed or anonymous mmap).

        Args:
            total_size: desired buffer size in bytes.
            truncate: if True, wipe existing data (O_TRUNC). If False,
                preserve existing cached blocks during resize.
        """
        import mmap as _mmap

        self._chunk_storage_mv = None
        self.chunks = []
        self._chunk_offsets = None

        if self._cache_path:
            flags = os.O_RDWR | os.O_CREAT | (os.O_TRUNC if truncate else 0)
            fd = os.open(self._cache_path, flags, 0o600)
            os.ftruncate(fd, total_size)
            if self._chunk_storage is not None:
                self._chunk_storage.close()
            self._chunk_storage = _mmap.mmap(fd, total_size)
            os.close(fd)
        else:
            if self._chunk_storage is not None:
                self._chunk_storage.close()
            self._chunk_storage = _mmap.mmap(
                -1, total_size, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS
            )

        self._chunk_storage_mv = memoryview(self._chunk_storage)
        self._total_size = total_size

        # Build chunks list for mount().
        self._chunk_offsets = []
        remaining = total_size
        off = 0
        while remaining > 0:
            sz = min(remaining, self.chunk_size)
            self._chunk_offsets.append((off, sz))
            self.chunks.append(self._chunk_storage_mv[off : off + sz])
            off += sz
            remaining -= sz
        self._next_chunk_idx = len(self._chunk_offsets)

    @endpoint
    def try_load_cache(self, cache_key):
        """Load cached chunk data from a previous run, if available.

        Sets ``_cache_path`` so that subsequent ``init_chunk_storage`` and
        ``collect_shards`` calls use file-backed mmap. If a cache file
        already exists, mmaps it and computes block hashes so the client
        can classify this worker as fresh or partial.
        """
        os.makedirs(CACHE_DIR, exist_ok=True)
        self._cache_path = os.path.join(CACHE_DIR, cache_key)

        if not os.path.exists(self._cache_path):
            return

        size = os.path.getsize(self._cache_path)
        if size == 0:
            return

        self._alloc_storage(size)
        self._block_hashes = block_hashes(self._chunk_storage_mv)
        self._pack_index = load_pack_index(self._cache_path + ".index") or {}

        print(
            f"[CACHE] Loaded {self._cache_path}: "
            f"{size // (1024**2)}MiB, "
            f"{len(self._block_hashes)} block hashes",
            file=sys.stderr,
            flush=True,
        )

    @endpoint
    def set_meta(self, meta):
        self.meta = meta

    @endpoint
    def init_chunk_storage(self, chunk_sizes):
        self._alloc_storage(sum(chunk_sizes), truncate=True)
        # Override uniform chunks with caller-specified sizes.
        self._chunk_offsets = []
        self.chunks = []
        offset = 0
        for size in chunk_sizes:
            self._chunk_offsets.append((offset, size))
            self.chunks.append(self._chunk_storage_mv[offset : offset + size])
            offset += size
        self._next_chunk_idx = 0
        # TODO(cpuhrsch): Re-enable EFA init after testing on SLURM.
        # EFA requires explicit initialization of its manager actor on each
        # worker (EfaManagerActor) before workers can register destination
        # buffers for RDMA transfers. Unlike ibverbs, which lazily initializes.
        # Code was:
        #   if self.backend == "slurm":
        #       from monarch.rdma import is_rdma_available
        #       if not is_rdma_available():
        #           from monarch._src.rdma.rdma import _ensure_init_efa_manager
        #           _ensure_init_efa_manager().block_on()

    @endpoint
    def ensure_storage(self, total_size):
        """Ensure storage is allocated at the given size."""
        if self._chunk_storage is not None and self._total_size == total_size:
            return
        self._alloc_storage(total_size)

    @endpoint
    def get_blocks_rdma_buffer(self, block_indices, total_size):
        """Pack specified blocks into a contiguous staging buffer, return RDMABuffer.

        Used for leader→peer fan-out: the leader packs its blocks and
        peers read from the returned RDMABuffer via ibverbs RDMA.
        """
        import mmap as _mmap

        from monarch.rdma import RDMABuffer

        staging_needed = sum(
            min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            for bi in block_indices
        )

        staging = _mmap.mmap(
            -1, staging_needed, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS
        )
        staging_mv = memoryview(staging)

        pos = 0
        for bi in block_indices:
            block_size = min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            offset = bi * HASH_BLOCK_SIZE
            staging_mv[pos : pos + block_size] = self._chunk_storage_mv[  # noqa: E203
                offset : offset + block_size
            ]
            pos += block_size

        return RDMABuffer(staging_mv[:staging_needed])

    def _receive_rdma_impl(self, rdma_buffer, block_indices, total_size, timeout=300):
        """Shared implementation for receive_rdma and receive_and_relay_rdma."""
        import mmap as _mmap

        if self._chunk_storage is None or self._total_size != total_size:
            self._alloc_storage(total_size)

        staging_size = sum(
            min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            for bi in block_indices
        )

        # RDMA memory registration fails on file-backed MAP_SHARED pages.
        # Always use anonymous mmap for the RDMA destination.
        staging = _mmap.mmap(-1, staging_size, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS)
        staging_mv = memoryview(staging)

        t0 = time.time()
        rdma_buffer.read_into(staging_mv[:staging_size], timeout=timeout).get()
        t1 = time.time()

        # Scatter from staging into chunk storage at correct offsets.
        pos = 0
        for bi in block_indices:
            block_size = min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            offset = bi * HASH_BLOCK_SIZE
            self._chunk_storage_mv[offset : offset + block_size] = staging_mv[  # noqa: E203
                pos : pos + block_size
            ]
            pos += block_size
        # Accumulate for atomic application during refresh_mount.
        if self._pending_dirty_blocks is not None:
            self._pending_dirty_blocks.extend(block_indices)
        # If _pending_dirty_blocks is None a full copy is already scheduled.

        t2 = time.time()
        gbs = (staging_size / 1e9) / max(t1 - t0, 1e-9)
        logger.debug(
            "receive_rdma %sMiB in %.2fs (%.1f GB/s), scatter=%.3fs",
            staging_size // (1024**2),
            t1 - t0,
            gbs,
            t2 - t1,
        )
        return staging_mv[:staging_size]

    @endpoint
    def receive_rdma(self, rdma_buffer, block_indices, total_size, timeout=300):
        """Receive dirty blocks from client via RDMA read.

        The client creates an RDMABuffer containing all dirty blocks
        packed contiguously. This endpoint reads the data and scatters
        blocks to their correct offsets in chunk storage.
        """
        self._receive_rdma_impl(rdma_buffer, block_indices, total_size, timeout)

    @endpoint
    def receive_and_relay_rdma(
        self, rdma_buffer, block_indices, total_size, timeout=300
    ):
        """Receive dirty blocks and return an RDMABuffer for fan-out to peers.

        Like receive_rdma, but returns an RDMABuffer wrapping the received
        staging buffer. Peers can read from it via ibverbs, eliminating the
        redundant gather step that get_blocks_rdma_buffer would perform.
        """
        from monarch.rdma import RDMABuffer

        staging_mv = self._receive_rdma_impl(
            rdma_buffer, block_indices, total_size, timeout
        )
        return RDMABuffer(staging_mv)

    def _do_refresh(self, new_block_hashes=None, total_size=0, pack_index=None):
        """Swap metadata and chunk data into the running FUSE filesystem."""
        t0 = time.time()

        if self._cache_path and self._chunk_storage is not None:
            self._chunk_storage.flush()
        t_flush = time.time()

        if pack_index is not None and self._cache_path:
            self._pack_index = pack_index
            save_pack_index(self._cache_path + ".index", pack_index)
        t_index = time.time()

        # Compute dirty ranges from accumulated pending blocks.
        # None means a full buffer copy is needed (initial mount).
        if self._pending_dirty_blocks is None:
            dirty_ranges = [(0, self._total_size)] if self._total_size > 0 else []
        else:
            dirty_ranges = [
                (
                    bi * HASH_BLOCK_SIZE,
                    min(HASH_BLOCK_SIZE, self._total_size - bi * HASH_BLOCK_SIZE),
                )
                for bi in self._pending_dirty_blocks
            ]
        self._pending_dirty_blocks = []

        chunk_buf = (
            self._chunk_storage_mv
            if self._chunk_storage_mv is not None
            else memoryview(b"")
        )
        # Atomically apply chunk patches and swap metadata under one write lock.
        self._fuse_handle.refresh(
            self.meta, chunk_buf, dirty_ranges, self._total_size, self.chunk_size
        )
        t_fuse = time.time()

        self._block_hashes = new_block_hashes or []
        self._total_size = total_size

        return {
            "flush": t_flush - t0,
            "save_index": t_index - t_flush,
            "fuse_refresh": t_fuse - t_index,
            "total": t_fuse - t0,
        }

    @endpoint
    def mount(self, mount_point, new_block_hashes=None, total_size=0, pack_index=None):
        """Mount an empty FUSE filesystem and populate it via refresh."""
        mount_point = _resolve_path(mount_point)
        from monarch._rust_bindings.monarch_extension.chunked_fuse import (
            mount_chunked_fuse,
        )

        now = time.time()
        empty_meta = {
            "/": {
                "attr": {
                    "st_atime": now,
                    "st_ctime": now,
                    "st_gid": os.getgid(),
                    "st_mode": 0o40755,
                    "st_mtime": now,
                    "st_nlink": 2,
                    "st_size": 4096,
                    "st_uid": os.getuid(),
                },
                "children": [],
            }
        }
        self._fuse_handle = mount_chunked_fuse(
            empty_meta, [], self.chunk_size, mount_point
        )
        # Block transfers ran while _fuse_handle was None, so no dirty blocks
        # were accumulated. Signal _do_refresh to do a full buffer copy.
        self._pending_dirty_blocks = None
        if self.meta is not None:
            self._do_refresh(new_block_hashes, total_size, pack_index)

    @endpoint
    def refresh_mount(self, new_block_hashes=None, total_size=0, pack_index=None):
        """Refresh FUSE mount data without unmounting.

        Atomically swaps metadata and chunk data in the running FUSE
        filesystem. Open file handles remain valid and subsequent reads
        see the new data.
        """
        if self._fuse_handle is None:
            raise RuntimeError("no active mount to refresh")
        return self._do_refresh(new_block_hashes, total_size, pack_index)

    @endpoint
    def get_block_hashes(self):
        """Return per-block hashes and total size of the mounted data."""
        return (self._block_hashes, self._total_size)

    @endpoint
    def recompute_block_hashes(self):
        """Recompute block hashes from the current chunk storage."""
        return block_hashes(self._chunk_storage_mv)

    @endpoint
    def get_pack_index(self):
        """Return the pack index for append-only packing."""
        return self._pack_index

    @endpoint
    def mkdir(self, path):
        """Create a directory on the worker."""
        prepare_mount_point(_resolve_path(path))

    @endpoint
    def unmount(self, mount_point):
        """Unmount a FUSE filesystem.

        Returns (status, detail) where status is one of:
          "ok"          — unmounted successfully
          "not_mounted" — path was not a mountpoint (nothing to unmount)
          "busy"        — mountpoint is in use by another process
          "error"       — unexpected failure
        """
        mount_point = _resolve_path(mount_point)
        check = subprocess.run(
            ["mountpoint", "-q", mount_point],
            capture_output=True,
        )
        if check.returncode != 0:
            return "not_mounted", ""

        result = subprocess.run(
            ["fusermount3", "-u", mount_point], capture_output=True, text=True
        )
        if result.returncode == 0:
            return "ok", ""
        if "busy" in result.stderr.lower():
            return "busy", result.stderr.strip()
        return "error", result.stderr.strip()


class MountHandler:
    def __init__(
        self,
        host_mesh,
        sourcepath: str,
        mntpoint: Optional[str] = None,
        chunk_size=None,
        backend: str = "slurm",
        num_parallel_streams: int = 8,
        transfer_mode: str = "rdma",
        cert_path: Optional[str] = None,
        tls_port: int = 0,
    ):
        import warnings

        if cert_path is not None:
            warnings.warn(
                "cert_path is deprecated and ignored. "
                "TLS is now configured via HYPERACTOR_TLS_CERT, "
                "HYPERACTOR_TLS_KEY, and HYPERACTOR_TLS_CA env vars.",
                DeprecationWarning,
                stacklevel=2,
            )
        if tls_port != 0:
            warnings.warn(
                "tls_port is deprecated and ignored. "
                "Port binding is handled by the transport layer automatically.",
                DeprecationWarning,
                stacklevel=2,
            )
        if transfer_mode != "rdma":
            raise ValueError(
                "transfer_mode must be 'rdma'; 'rust_tls' and 'actor' were removed "
                "because RDMA already falls back to TCP when ibverbs is unavailable"
            )

        self.sourcepath = os.path.abspath(sourcepath)
        if mntpoint is None:
            mntpoint = self.sourcepath
        self.mntpoint = os.path.abspath(mntpoint)
        self.fuse_actors = None
        self.host_mesh = host_mesh
        self.procs = None
        self.chunk_size = chunk_size
        self.backend = backend
        if num_parallel_streams < 1:
            raise ValueError(
                f"num_parallel_streams must be >= 1, got {num_parallel_streams}"
            )
        self.num_parallel_streams = num_parallel_streams
        self._staging_mv = None
        self._pack_shm_path = None
        self._mounted = False

    def _sync(self):
        """Pack source, diff against workers, transfer dirty blocks, refresh FUSE.

        Shared by open() and refresh(). Expects self.fuse_actors to be
        initialized and, for refresh(), an active mount.
        """
        t_start = time.time()

        flat_actors = self.fuse_actors.flatten("rank")
        hashes_future = self.fuse_actors.get_block_hashes.call()
        index_future = self.fuse_actors.get_pack_index.call()

        index_result = index_future.get()
        t_index_done = time.time()
        print(
            f"_sync(): get_pack_index RPC took {t_index_done - t_start:.2f}s",
            file=sys.stderr,
            flush=True,
        )
        previous_index = next(
            (idx for _, idx in index_result if idx and idx.get("files")),
            None,
        )

        meta, self._staging_mv, chunks, client_hashes, new_pack_index = (
            pack_directory_chunked(
                self.sourcepath, self.chunk_size, previous_index=previous_index
            )
        )
        staging_mv = self._staging_mv
        client_total_size = len(staging_mv) if staging_mv is not None else 0

        t_pack_done = time.time()

        result = hashes_future.get()
        t_hashes_done = time.time()
        print(
            f"_sync(): get_block_hashes RPC wait {t_hashes_done - t_pack_done:.2f}s "
            f"(overlapped with pack)",
            file=sys.stderr,
            flush=True,
        )
        worker_states = [
            (remote_hashes, remote_size)
            for _point, (remote_hashes, remote_size) in result
        ]
        fresh_ranks, worker_dirty = classify_workers(
            client_hashes, client_total_size, worker_states
        )

        t_classify_done = time.time()
        print(
            f"_sync(): classify_workers {t_classify_done - t_hashes_done:.2f}s",
            file=sys.stderr,
            flush=True,
        )

        # No-op shortcut: all workers already have the right data and
        # are mounted — nothing to transfer, refresh, or remount.
        if not worker_dirty and self._mounted:
            t_done = time.time()
            print(
                f"_sync(): all workers up-to-date, nothing to do. "
                f"total={t_done - t_start:.2f}s",
                file=sys.stderr,
                flush=True,
            )
            return

        self.fuse_actors.set_meta.call(meta).get()

        t_meta_done = time.time()
        print(
            f"_sync(): set_meta RPC {t_meta_done - t_classify_done:.2f}s",
            file=sys.stderr,
            flush=True,
        )

        all_blocks = list(range(len(client_hashes)))
        dirty_blocks: set[int] = set()
        for _rank, d in worker_dirty.items():
            if d is None:
                dirty_blocks = set(all_blocks)
                break
            dirty_blocks.update(d)
        sorted_dirty = sorted(dirty_blocks)

        target_ranks = sorted(worker_dirty.keys())
        is_full_transfer = len(sorted_dirty) == len(all_blocks)

        if sorted_dirty and target_ranks:
            print(
                f"_sync(): {len(sorted_dirty)}/{len(client_hashes)} blocks dirty "
                f"across {len(target_ranks)} workers"
                f"{' (full transfer)' if is_full_transfer else ''}",
                file=sys.stderr,
                flush=True,
            )
            self._transfer_fanout(
                flat_actors,
                target_ranks,
                sorted_dirty,
                client_total_size,
                full_transfer=is_full_transfer,
            )

        t_transfer_done = time.time()
        print(
            f"_sync(): transfer {t_transfer_done - t_meta_done:.2f}s",
            file=sys.stderr,
            flush=True,
        )

        # Mount or refresh after transfer succeeds — mounting before
        # transfer would leak a FUSE mount if the transfer fails
        # (open() raises before __enter__ completes, so close() never runs).
        if self._mounted:
            if os.environ.get("MONARCH_SKIP_REFRESH_MOUNT"):
                print(
                    "_sync(): skipping refresh_mount (MONARCH_SKIP_REFRESH_MOUNT set)",
                    file=sys.stderr,
                    flush=True,
                )
            else:
                refresh_results = self.fuse_actors.refresh_mount.call(
                    client_hashes, client_total_size, new_pack_index
                ).get()
                t_refresh_done = time.time()
                for _point, timings in refresh_results:
                    if timings:
                        timings_str = ", ".join(
                            f"{k}={v:.2f}s" for k, v in timings.items()
                        )
                        print(
                            f"_sync(): refresh_mount remote breakdown: {timings_str}",
                            file=sys.stderr,
                            flush=True,
                        )
                print(
                    f"_sync(): refresh_mount RPC {t_refresh_done - t_transfer_done:.2f}s",
                    file=sys.stderr,
                    flush=True,
                )
        else:
            self.fuse_actors.mount.call(
                self.mntpoint, client_hashes, client_total_size, new_pack_index
            ).get()
            t_refresh_done = time.time()
            print(
                f"_sync(): mount RPC {t_refresh_done - t_transfer_done:.2f}s",
                file=sys.stderr,
                flush=True,
            )
            self._mounted = True

        t_done = time.time()

        print(
            f"_sync() timings: "
            f"index_rpc={t_index_done - t_start:.2f}s, "
            f"pack={t_pack_done - t_index_done:.2f}s "
            f"({client_total_size / (1024**2):.0f}MiB), "
            f"hashes_rpc={t_hashes_done - t_pack_done:.2f}s, "
            f"classify={t_classify_done - t_hashes_done:.2f}s, "
            f"set_meta={t_meta_done - t_classify_done:.2f}s, "
            f"transfer={t_transfer_done - t_meta_done:.2f}s, "
            f"refresh={t_done - t_transfer_done:.2f}s, "
            f"total={t_done - t_start:.2f}s"
        )

    def open(self):
        # Reuse existing actors if available (preserves block hashes
        # and pack index for incremental update checks).
        if self.fuse_actors is None:
            self.procs = self.host_mesh.spawn_procs()
            self.fuse_actors = self.procs.spawn(
                "FUSEActor", FUSEActor, self.chunk_size, self.backend
            )
            self.fuse_actors.mkdir.call(self.mntpoint).get()

            import xxhash

            cache_key = xxhash.xxh64(
                (self.sourcepath + ":" + self.mntpoint).encode()
            ).hexdigest()
            self.fuse_actors.try_load_cache.call(cache_key).get()

        with configured(rdma_tcp_fallback_parallelism=self.num_parallel_streams):
            self._sync()
        return self

    def _transfer_fanout(
        self,
        flat_actors: object,
        target_ranks: list[int],
        dirty_blocks: list[int],
        total_size: int,
        full_transfer: bool = False,
    ) -> None:
        """Transfer dirty blocks: client → leaders via RDMABuffer, leaders → peers via RDMA.

        The client sends to leader(s) using RDMABuffer (TCP fallback on
        client since it typically lacks ibverbs). Leaders then fan out to
        peers using RDMABuffer over ibverbs for maximum throughput.

        When peers exist, leaders use receive_and_relay_rdma which returns
        the received staging buffer as an RDMABuffer — eliminating the
        redundant gather that get_blocks_rdma_buffer would perform.

        When full_transfer is True (cold start — all blocks dirty), the
        client skips the gather copy and wraps _staging_mv directly since
        the packed buffer already contains all blocks contiguously.
        """
        from monarch.rdma import RDMABuffer

        t0 = time.time()

        total_bytes = sum(
            min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            for bi in dirty_blocks
        )

        if full_transfer:
            # Cold start: all blocks dirty, _staging_mv already has them
            # contiguously in order. Skip the gather copy.
            rdma_buf = RDMABuffer(self._staging_mv[:total_bytes])
        else:
            import mmap as _mmap

            # Partial update: gather dirty blocks into a contiguous staging buffer.
            staging = _mmap.mmap(
                -1, total_bytes, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS
            )
            staging_mv = memoryview(staging)
            pos = 0
            for bi in dirty_blocks:
                block_size = min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
                offset = bi * HASH_BLOCK_SIZE
                dst_block = slice(pos, pos + block_size)
                src_block = slice(offset, offset + block_size)
                staging_mv[dst_block] = self._staging_mv[src_block]
                pos += block_size
            rdma_buf = RDMABuffer(staging_mv[:total_bytes])

        t_setup = time.time()

        # Pick leader(s) — transfer from client to leaders first.
        num_leaders = min(4, len(target_ranks))
        leader_ranks = target_ranks[:num_leaders]
        peer_ranks = target_ranks[num_leaders:]

        # Step 1: Client → leader(s) via RDMABuffer (TCP fallback).
        # If there are peers, leaders use receive_and_relay_rdma which
        # returns an RDMABuffer for fan-out (avoids redundant gather).
        leader_relay_futures = {}
        leader_futures = {}
        for rank in leader_ranks:
            worker = flat_actors.slice(rank=rank)
            if peer_ranks:
                leader_relay_futures[rank] = worker.receive_and_relay_rdma.call(
                    rdma_buf, dirty_blocks, total_size
                )
            else:
                leader_futures[rank] = worker.receive_rdma.call(
                    rdma_buf, dirty_blocks, total_size
                )

        if not peer_ranks:
            for rank, f in leader_futures.items():
                f.get()
                elapsed = time.time() - t_setup
                gbs = (total_bytes / 1e9) / max(elapsed, 1e-9)
                print(
                    f"  leader rank={rank}: {total_bytes // (1024**2)}MiB "
                    f"done at {elapsed:.1f}s ({gbs:.1f} GB/s)",
                    file=sys.stderr,
                    flush=True,
                )
            return

        # Step 2: Ensure peers have storage allocated (overlaps with leader transfer).
        ensure_futures = []
        for rank in peer_ranks:
            ensure_futures.append(
                flat_actors.slice(rank=rank).ensure_storage.call(total_size)
            )
        for f in ensure_futures:
            f.get()

        # Distribute peers round-robin across leaders.
        leader_peer_groups: dict[int, list[int]] = {r: [] for r in leader_ranks}
        for i, peer_rank in enumerate(peer_ranks):
            leader_rank = leader_ranks[i % num_leaders]
            leader_peer_groups[leader_rank].append(peer_rank)

        # Step 3: Collect leader RDMABuffers and fan out to peers.
        # Each leader's receive_and_relay_rdma returns an RDMABuffer
        # wrapping the staging buffer it received into — no re-gather needed.
        peer_futures = []
        for leader_rank, relay_f in leader_relay_futures.items():
            leader_rdma_result = relay_f.get()
            elapsed = time.time() - t_setup
            gbs = (total_bytes / 1e9) / max(elapsed, 1e-9)
            print(
                f"  leader rank={leader_rank}: {total_bytes // (1024**2)}MiB "
                f"done at {elapsed:.1f}s ({gbs:.1f} GB/s)",
                file=sys.stderr,
                flush=True,
            )
            leader_rdma_buf = [v for _, v in leader_rdma_result][0]
            for peer_rank in leader_peer_groups[leader_rank]:
                worker = flat_actors.slice(rank=peer_rank)
                peer_futures.append(
                    worker.receive_rdma.call(leader_rdma_buf, dirty_blocks, total_size)
                )

        t_leaders = time.time()
        print(
            f"Client → {num_leaders} leader(s): "
            f"{total_bytes // (1024**2)}MiB in {t_leaders - t_setup:.1f}s",
            file=sys.stderr,
            flush=True,
        )

        for f in peer_futures:
            f.get()

        t_done = time.time()
        peer_gbs = (total_bytes * len(peer_ranks) / 1e9) / max(t_done - t_leaders, 1e-9)
        print(
            f"{num_leaders} leader(s) → {len(peer_ranks)} peer(s) via RDMA: "
            f"{total_bytes // (1024**2)}MiB in {t_done - t_leaders:.1f}s "
            f"({peer_gbs:.1f} GB/s), "
            f"total={t_done - t0:.1f}s",
            file=sys.stderr,
            flush=True,
        )

    def close(self) -> None:
        """Unmount FUSE but keep actors alive for incremental updates."""
        if self.fuse_actors is not None:
            result = self.fuse_actors.unmount.call(self.mntpoint).get()
            for _point, (status, detail) in result:
                if status not in ("ok", "not_mounted"):
                    logger.warning(f"unmount failed ({status}): {detail}")
        self._mounted = False

    def refresh(self, sourcepath: str):
        """Re-pack source directory and refresh all running mounts in-place.

        Unlike close()+open(), this does not unmount the FUSE filesystem.
        Open file handles remain valid; subsequent reads see the updated
        data.

        Args:
            sourcepath: Must match the sourcepath used in open(). Requiring
                the caller to pass it again prevents accidentally refreshing
                a mount with a forgotten or wrong source directory.
        """
        if sourcepath != self.sourcepath:
            raise ValueError(
                f"sourcepath mismatch: refresh called with {sourcepath!r} "
                f"but mount was opened with {self.sourcepath!r}"
            )
        if not self._mounted or self.fuse_actors is None:
            raise RuntimeError("no active mount to refresh; call open() first")
        with configured(rdma_tcp_fallback_parallelism=self.num_parallel_streams):
            self._sync()

    def __enter__(self) -> "MountHandler":
        self.open()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        self.close()
        return False  # Don't suppress exceptions


def remotemount(
    host_mesh: object,
    sourcepath: str,
    mntpoint: Optional[str] = None,
    chunk_size: Optional[int] = None,
    backend: str = "slurm",
    num_parallel_streams: int = 8,
    transfer_mode: str = "rdma",
    cert_path: Optional[str] = None,
    tls_port: int = 0,
) -> MountHandler:
    """Mount a local directory on remote hosts via RDMA transfer and FUSE.

    Uses RDMABuffer for data transfer — supports ibverbs (hardware RDMA)
    with automatic TCP fallback when ibverbs is unavailable.

    Args:
        num_parallel_streams: Number of parallel TCP streams for the
            RDMABuffer TCP fallback path (when ibverbs is unavailable).
        transfer_mode: Must be "rdma". RDMABuffer uses ibverbs when
            available and automatically falls back to TCP otherwise.
        cert_path: Deprecated, ignored. TLS is now configured via
            HYPERACTOR_TLS_CERT, HYPERACTOR_TLS_KEY, and
            HYPERACTOR_TLS_CA env vars.
        tls_port: Deprecated, ignored. Port binding is handled by the
            transport layer automatically.
    """
    if chunk_size is None:
        chunk_size = CHUNK_SIZE
    return MountHandler(
        host_mesh,
        sourcepath,
        mntpoint,
        chunk_size,
        backend,
        num_parallel_streams,
        transfer_mode,
        cert_path,
        tls_port,
    )
