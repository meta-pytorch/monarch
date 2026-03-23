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
import time
from typing import Optional

from monarch.actor import Actor, endpoint
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
RDMA_PARALLEL_TLS_THRESHOLD = (
    8  # blocks <= this: TLS to all workers; above: RDMA fan-out
)


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
        self._tls_receiver = None
        self._pack_index = {}
        self._rdma_src_staging = None  # staging for get_blocks_rdma_buffer (source)
        self._rdma_src_staging_mv = None
        self._rdma_dst_staging = None  # staging for replace_blocks (destination)
        self._rdma_dst_staging_mv = None

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

        logger.info(
            f"[CACHE] Loaded {self._cache_path}: "
            f"{size // (1024**2)}MiB, "
            f"{len(self._block_hashes)} block hashes"
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
        # buffers for RDMA transfers. Unlike ibverbs which lazily initializes.
        # Code was:
        #   if self.backend == "slurm":
        #       from monarch.rdma import is_rdma_available
        #       if not is_rdma_available():
        #           from monarch._src.rdma.rdma import _ensure_init_efa_manager
        #           _ensure_init_efa_manager().block_on()

    @endpoint
    def fetch_chunk_rdma(self, rdma_buffer, chunk_size: int, timeout: int = 300):
        """Receive RDMABuffer (works with both ibverbs and EFA) and read from it."""
        import mmap as _mmap

        idx = self._next_chunk_idx

        # Copy into pre-allocated mmap via RDMA (ibverbs or TCP fallback).
        offset, _ = self._chunk_offsets[idx]
        dst_mv = self._chunk_storage_mv[offset : offset + chunk_size]
        t1 = time.time()

        if self._cache_path:
            # RDMA memory registration fails on file-backed MAP_SHARED pages.
            # Receive into an anonymous buffer, then copy to the cache file.
            # Don't explicitly close the anonymous mmap — it can raise
            # BufferError if the Rust RDMABuffer still holds a reference.
            anon = _mmap.mmap(-1, chunk_size, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS)
            anon_mv = memoryview(anon)
            rdma_buffer.read_into(anon_mv, timeout=timeout).get()
            dst_mv[:] = anon_mv
        else:
            rdma_buffer.read_into(dst_mv, timeout=timeout).get()

        t2 = time.time()
        self.chunks.append(dst_mv)
        self._next_chunk_idx += 1
        gbs = (chunk_size / 1e9) / max(t2 - t1, 1e-9)
        logger.info(
            f"[WORKER] fetch_chunk {idx}: {chunk_size / (1024**2):.0f}MiB "
            f"in {t2 - t1:.3f}s ({gbs:.1f} GB/s)"
        )

    @endpoint
    def fanout_chunk_rdma(
        self, peer_actors, chunk_size: int, chunk_idx: int = -1, timeout: int = 300
    ):
        """Fan out a chunk to peer workers via RDMA.

        Args:
            peer_actors: Mesh of peer FUSEActors to receive the chunk.
            chunk_size: Size of the chunk in bytes.
            chunk_idx: Which chunk to fan out. Defaults to -1 (last received).
            timeout: RDMA timeout in seconds.
        """
        import mmap

        from monarch.rdma import RDMABuffer

        t0 = time.time()
        idx = chunk_idx if chunk_idx >= 0 else self._next_chunk_idx - 1
        offset, _ = self._chunk_offsets[idx]
        src_mv = self._chunk_storage_mv[offset : offset + chunk_size]

        # RDMA memory registration (ibv_reg_mr) can fail on file-backed
        # MAP_SHARED mmap pages.  Copy into an anonymous buffer so the
        # RDMABuffer always uses anonymous memory.
        anon = None
        if self._cache_path:
            anon = mmap.mmap(-1, chunk_size, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
            anon_mv = memoryview(anon)
            anon_mv[:] = src_mv
            rdma_buffer = RDMABuffer(anon_mv)
        else:
            rdma_buffer = RDMABuffer(src_mv)
        flat_peers = peer_actors.flatten("rank")
        t1 = time.time()
        futures = []
        for rank in range(len(flat_peers)):
            peer = flat_peers.slice(rank=rank)
            futures.append(peer.fetch_chunk_rdma.call(rdma_buffer, chunk_size, timeout))
        t2 = time.time()
        for f in futures:
            f.get()
        t3 = time.time()

        # Anonymous mmap is reclaimed on GC; explicit close() can raise
        # BufferError if the Rust RDMABuffer still holds a reference.

        n = len(flat_peers)
        gbs = (chunk_size * n / 1e9) / max(t3 - t1, 1e-9)
        logger.info(
            f"[WORKER] fanout_chunk {idx}: setup={t1 - t0:.3f}s, "
            f"dispatch={t2 - t1:.3f}s, wait={t3 - t2:.3f}s, "
            f"total={t3 - t0:.3f}s ({gbs:.1f} GB/s aggregate, {n} peers)"
        )

    @endpoint
    def get_blocks_rdma_buffer(self, block_indices, total_size):
        """Copy multiple blocks into a contiguous staging buffer, return RDMABuffer.

        Blocks are packed contiguously (no gaps). The caller MUST wait for
        all peers to finish reading before calling this again.
        """
        import mmap as _mmap

        from monarch.rdma import RDMABuffer

        staging_needed = 0
        for bi in block_indices:
            staging_needed += min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)

        if (
            self._rdma_src_staging is None
            or len(self._rdma_src_staging) < staging_needed
        ):
            self._rdma_src_staging = _mmap.mmap(
                -1, staging_needed, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS
            )
            self._rdma_src_staging_mv = memoryview(self._rdma_src_staging)

        pos = 0
        for bi in block_indices:
            block_size = min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            offset = bi * HASH_BLOCK_SIZE
            self._rdma_src_staging_mv[pos : pos + block_size] = self._chunk_storage_mv[
                offset : offset + block_size
            ]
            pos += block_size

        return RDMABuffer(self._rdma_src_staging_mv[:staging_needed])

    @endpoint
    def ensure_storage(self, total_size):
        """Ensure storage is allocated at the given size for RDMA reception."""
        if self._chunk_storage is not None and self._total_size == total_size:
            return
        self._alloc_storage(total_size)

    @endpoint
    def replace_blocks(
        self,
        block_indices,
        total_size,
        rdma_buffer,
        staging_size: int,
        timeout: int = 300,
    ):
        """Overwrite multiple hash blocks from a single contiguous RDMA buffer.

        The rdma_buffer contains blocks packed contiguously (matching the
        order in block_indices). This reduces RDMA round-trips vs per-block.
        """
        import mmap as _mmap

        if self._rdma_dst_staging is None or len(self._rdma_dst_staging) < staging_size:
            self._rdma_dst_staging = _mmap.mmap(
                -1, staging_size, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS
            )
            self._rdma_dst_staging_mv = memoryview(self._rdma_dst_staging)

        staging_slice = self._rdma_dst_staging_mv[:staging_size]
        rdma_buffer.read_into(staging_slice, timeout=timeout).get()

        pos = 0
        for bi in block_indices:
            block_size = min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            offset = bi * HASH_BLOCK_SIZE
            self._chunk_storage_mv[offset : offset + block_size] = staging_slice[
                pos : pos + block_size
            ]
            pos += block_size

    @endpoint
    def mount(self, mount_point, new_block_hashes=None, total_size=0, pack_index=None):
        from monarch._rust_bindings.monarch_extension.chunked_fuse import (
            mount_chunked_fuse,
        )

        # Flush mmap to disk so the cache file persists across actor restarts.
        if self._cache_path and self._chunk_storage is not None:
            self._chunk_storage.flush()

        # Persist pack index alongside cached data.
        if pack_index is not None and self._cache_path:
            self._pack_index = pack_index
            save_pack_index(self._cache_path + ".index", pack_index)

        self._fuse_handle = mount_chunked_fuse(
            self.meta,
            self.chunks,
            self.chunk_size,
            mount_point,
        )
        self._block_hashes = new_block_hashes or []
        self._total_size = total_size
        return 0

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
    def prepare_receiver(self, num_streams, total_size):
        """Create a Rust TLS receiver and return its address."""
        from monarch._rust_bindings.monarch_extension.tls_receiver import TlsReceiver

        if self._chunk_storage is None or self._total_size != total_size:
            self._alloc_storage(total_size)

        self._tls_receiver = TlsReceiver(num_streams)
        return (
            self._tls_receiver.addr,
            self._tls_receiver.tls_hostname,
            self._cache_path or "",
        )

    @endpoint
    def receive_blocks(self):
        """Block until the TLS receiver has finished receiving all blocks."""
        if self._tls_receiver is None:
            raise RuntimeError("prepare_receiver() not called")
        self._tls_receiver.wait(self._chunk_storage_mv)
        self._tls_receiver = None
        return True

    @endpoint
    def receive_block(self, block_idx: int, data: bytes, total_size: int):
        """Receive a single block via actor message passing.

        Allocates storage on first call. Slower than TLS but works
        without custom TLS certs (e.g. GitHub CI, local testing).
        """
        if self._chunk_storage is None or self._total_size != total_size:
            self._alloc_storage(total_size)

        offset = block_idx * HASH_BLOCK_SIZE
        self._chunk_storage_mv[offset : offset + len(data)] = data

    @endpoint
    def mkdir(self, path):
        """Create a directory on the worker."""
        os.makedirs(path, exist_ok=True)

    @endpoint
    def unmount(self, mount_point):
        """Unmount a FUSE filesystem. Returns (returncode, stderr)."""
        result = subprocess.run(
            ["fusermount3", "-u", mount_point], capture_output=True, text=True
        )
        return result.returncode, result.stderr


class MountHandler:
    def __init__(
        self,
        host_mesh,
        sourcepath: str,
        mntpoint: Optional[str] = None,
        chunk_size=None,
        backend: str = "slurm",
        num_parallel_streams: int = 8,
        transfer_mode: str = "rust_tls",
    ):
        self.sourcepath = sourcepath
        if mntpoint is None:
            mntpoint = sourcepath
        self.mntpoint = mntpoint
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
        if transfer_mode not in ("rust_tls", "actor"):
            raise ValueError(
                f"transfer_mode must be 'rust_tls' or 'actor', got {transfer_mode!r}"
            )
        self.transfer_mode = transfer_mode
        self._staging_mv = None
        self._pack_shm_path = None

    def open(self):
        t_open_start = time.time()

        # Reuse existing actors if available (preserves block hashes
        # and pack index for incremental update checks).
        if self.fuse_actors is None:
            self.procs = self.host_mesh.spawn_procs(per_host={"gpus": 1})
            self.fuse_actors = self.procs.spawn(
                "FUSEActor", FUSEActor, self.chunk_size, self.backend
            )
            self.fuse_actors.mkdir.call(self.mntpoint).get()

            import xxhash

            cache_key = xxhash.xxh64(
                (self.sourcepath + ":" + self.mntpoint).encode()
            ).hexdigest()
            self.fuse_actors.try_load_cache.call(cache_key).get()

        t_actors_ready = time.time()

        # Fire RPCs before packing so the network round-trips overlap
        # with the CPU-bound walk+pack+hash step.
        flat_actors = self.fuse_actors.flatten("rank")
        num_workers = len(flat_actors)
        hashes_future = self.fuse_actors.get_block_hashes.call()
        index_future = self.fuse_actors.get_pack_index.call()

        # Get pack index from workers (first non-empty).
        # This is small JSON so the wait is fast.
        index_result = index_future.get()
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

        # Collect worker hashes (should already be available after packing).
        result = hashes_future.get()
        worker_states = [
            (remote_hashes, remote_size)
            for _point, (remote_hashes, remote_size) in result
        ]
        fresh_ranks, worker_dirty = classify_workers(
            client_hashes, client_total_size, worker_states
        )

        t_classify_done = time.time()

        # Always send metadata so newly spawned actors (which loaded
        # block data from the persistent cache) have filesystem layout.
        self.fuse_actors.set_meta.call(meta).get()

        t_meta_done = time.time()

        if not worker_dirty:
            self.fuse_actors.mount.call(
                self.mntpoint, client_hashes, client_total_size, new_pack_index
            ).get()
            t_mount_done = time.time()
            logger.info(
                f"All {num_workers} workers up-to-date — skipping transfer, re-mounting. "
                f"Timings: actors={t_actors_ready - t_open_start:.2f}s, "
                f"pack+hash={t_pack_done - t_actors_ready:.2f}s "
                f"({client_total_size / (1024**2):.0f}MiB), "
                f"classify={t_classify_done - t_pack_done:.2f}s, "
                f"set_meta={t_meta_done - t_classify_done:.2f}s, "
                f"mount={t_mount_done - t_meta_done:.2f}s, "
                f"total={t_mount_done - t_open_start:.2f}s"
            )
            return self

        n_partial = sum(1 for v in worker_dirty.values() if v is not None)
        n_stale = sum(1 for v in worker_dirty.values() if v is None)
        logger.info(
            f"{len(fresh_ranks)} fresh, {n_partial} partial, "
            f"{n_stale} stale out of {num_workers} workers"
        )

        # Unmount workers that need updating.
        for rank in worker_dirty:
            result = flat_actors.slice(rank=rank).unmount.call(self.mntpoint).get()
            for _point, (rc, stderr) in result:
                if rc != 0:
                    logger.warning(
                        f"fusermount3 -u failed on rank {rank} (rc={rc}): {stderr.strip()}"
                    )

        t_unmount_done = time.time()

        # Compute dirty blocks: union of all non-fresh workers.
        # Stale workers (None) need all blocks; partial workers need their list.
        all_blocks = list(range(len(client_hashes)))
        dirty_blocks: set[int] = set()
        for _rank, d in worker_dirty.items():
            if d is None:
                dirty_blocks = set(all_blocks)
                break
            dirty_blocks.update(d)
        sorted_dirty = sorted(dirty_blocks)

        target_ranks = sorted(worker_dirty.keys())

        if sorted_dirty and target_ranks:
            logger.info(
                f"{len(sorted_dirty)}/{len(client_hashes)} blocks dirty "
                f"across {len(target_ranks)} workers"
            )
            self._transfer_fanout(
                flat_actors, target_ranks, sorted_dirty, client_total_size
            )

        t_transfer_done = time.time()

        # Remount all workers (fresh ones for metadata update).
        self.fuse_actors.mount.call(
            self.mntpoint, client_hashes, client_total_size, new_pack_index
        ).get()

        t_mount_done = time.time()

        logger.info(
            f"open() timings: actors={t_actors_ready - t_open_start:.2f}s, "
            f"pack+hash={t_pack_done - t_actors_ready:.2f}s "
            f"({client_total_size / (1024**2):.0f}MiB), "
            f"classify={t_classify_done - t_pack_done:.2f}s, "
            f"set_meta={t_meta_done - t_classify_done:.2f}s, "
            f"unmount={t_unmount_done - t_meta_done:.2f}s, "
            f"transfer={t_transfer_done - t_unmount_done:.2f}s, "
            f"mount={t_mount_done - t_transfer_done:.2f}s, "
            f"total={t_mount_done - t_open_start:.2f}s"
        )
        return self

    def _transfer_blocks_actor(self, fuse_actor, dirty_blocks, total_size):
        """Transfer dirty blocks via actor message passing.

        Sends each block as a bytes argument to the receive_block endpoint.
        Slower than Rust TLS but works without custom TLS certs.
        """
        if not dirty_blocks:
            return

        t_start = time.time()
        total_bytes = 0
        for bi in dirty_blocks:
            offset = bi * HASH_BLOCK_SIZE
            size = min(HASH_BLOCK_SIZE, total_size - offset)
            block_data = bytes(self._staging_mv[offset : offset + size])
            fuse_actor.receive_block.call(bi, block_data, total_size).get()
            total_bytes += size
        t_done = time.time()

        elapsed = max(t_done - t_start, 1e-9)
        gbs = (total_bytes / 1e9) / elapsed
        logger.info(
            f"Actor block transfer ({len(dirty_blocks)} blocks): "
            f"{total_bytes // (1024**2)}MiB in {elapsed:.1f}s ({gbs:.1f} GB/s)"
        )

    def _transfer_blocks_rust_tls(self, fuse_actor, dirty_blocks, total_size):
        """Transfer dirty blocks to a single worker using Rust TLS.

        Sends blocks directly from ``self._staging_mv`` (the buffer produced
        by ``pack_directory_chunked``) so no second pack step is needed.

        Flow:
          1. Worker: prepare_receiver() → creates TlsReceiver, returns address
          2. Client: send_blocks_from_buffer() → parallel TLS connections
          3. Worker: receive_blocks() → waits for all data
        """
        if not dirty_blocks:
            return

        from monarch._rust_bindings.monarch_extension.tls_sender import (
            send_blocks_from_buffer,
        )

        num_streams = self.num_parallel_streams

        total_bytes = sum(
            min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            for bi in dirty_blocks
        )

        # 1. Start receiver on worker (returns address, tls_hostname, cache path).
        t_start = time.time()
        result = fuse_actor.prepare_receiver.call(num_streams, total_size).get()
        addr, tls_hostname, cache_path = [v for _, v in result][0]
        addresses = [addr] * num_streams

        # 2. Fire receive_blocks (non-blocking) so worker starts waiting.
        recv_future = fuse_actor.receive_blocks.call()

        # 3. Send blocks directly from the staging buffer.
        t_setup = time.time()
        send_blocks_from_buffer(
            self._staging_mv,
            total_size,
            dirty_blocks,
            addresses,
            cache_path,
            tls_hostname=tls_hostname,
        )
        t_send = time.time()

        # 4. Wait for receiver to finish.
        recv_future.get()
        t_done = time.time()

        gbs = (total_bytes / 1e9) / max(t_send - t_setup, 1e-9)
        logger.info(
            f"Rust TLS block transfer ({len(dirty_blocks)} blocks, "
            f"{num_streams} streams): {total_bytes // (1024**2)}MiB "
            f"in {t_send - t_setup:.1f}s ({gbs:.1f} GB/s), "
            f"setup={t_setup - t_start:.2f}s, "
            f"total={t_done - t_start:.1f}s"
        )

    def _transfer_fanout(
        self,
        flat_actors: object,
        target_ranks: list[int],
        dirty_blocks: list[int],
        total_size: int,
    ) -> None:
        """Transfer dirty blocks: TLS to leader, RDMA fan-out to peers.

        Small incremental transfers (≤ RDMA_PARALLEL_TLS_THRESHOLD blocks)
        go directly via TLS to each worker in parallel — faster than any
        RDMA round-trip for small payloads.

        Large transfers use TLS to the leader, then a single RDMA fan-out
        to all peers (one staging + one RDMA read per peer).
        """
        t0 = time.time()

        total_bytes = sum(
            min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            for bi in dirty_blocks
        )

        # Use multiple leaders for higher initial fan-out.
        # TLS from client to each leader in parallel, then tree fan-out.
        num_leaders = min(4, len(target_ranks))
        leader_ranks = target_ranks[:num_leaders]
        peer_ranks = target_ranks[num_leaders:]

        transfer_fn = (
            self._transfer_blocks_actor
            if self.transfer_mode == "actor"
            else self._transfer_blocks_rust_tls
        )

        # Small incremental: TLS/actor to every worker in parallel (no RDMA).
        if len(dirty_blocks) <= RDMA_PARALLEL_TLS_THRESHOLD or not peer_ranks:
            workers = [(rank, flat_actors.slice(rank=rank)) for rank in target_ranks]

            # Ensure all workers have storage allocated.
            ensure_futures = []
            for _rank, worker in workers:
                ensure_futures.append(worker.ensure_storage.call(total_size))
            for f in ensure_futures:
                f.get()

            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=len(workers)) as pool:
                futures = {
                    pool.submit(transfer_fn, worker, dirty_blocks, total_size): rank
                    for rank, worker in workers
                }
                for future in futures:
                    future.result()  # raises on first failure

            t_done = time.time()
            logger.info(
                f"Parallel transfer to {len(target_ranks)} workers: "
                f"{total_bytes // (1024**2)}MiB "
                f"({len(dirty_blocks)} blocks) in {t_done - t0:.1f}s"
            )
            return

        # Large transfer: TLS to leaders, then chunked tree RDMA fan-out.

        # Step 1: TLS transfer to all leaders in parallel.
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=num_leaders) as pool:
            tls_futures = {
                pool.submit(
                    transfer_fn,
                    flat_actors.slice(rank=r),
                    dirty_blocks,
                    total_size,
                ): r
                for r in leader_ranks
            }
            for f in tls_futures:
                f.result()
        t_tls = time.time()

        # Step 2: Ensure peers have storage allocated.
        for rank in peer_ranks:
            flat_actors.slice(rank=rank).ensure_storage.call(total_size).get()

        # Step 3: Chunked pipelined tree RDMA fan-out.
        # Concurrent reads from the same RDMABuffer are not safe
        # (see disabled test_rdma_buffer_read_into_concurrent in
        # test_rdma_unit.py: "TODO: fix concurrency issues").
        # Tree fan-out avoids this: each source sends to at most ONE
        # destination per round. Source and destination use SEPARATE
        # staging buffers (_rdma_src_staging vs _rdma_dst_staging),
        # so a node CAN be both source and destination simultaneously.
        #
        # Chunking splits the payload into ~1GB pieces. Once a node
        # receives chunk 0, it can forward chunk 0 while receiving
        # chunk 1. Latency ≈ (tree_depth + num_chunks - 1) × chunk_time.
        rdma_chunk_blocks = max(1, (1024 * 1024 * 1024) // HASH_BLOCK_SIZE)
        block_chunks = [
            dirty_blocks[i : i + rdma_chunk_blocks]
            for i in range(0, len(dirty_blocks), rdma_chunk_blocks)
        ]
        num_chunks = len(block_chunks)
        chunk_byte_sizes = [
            sum(
                min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE) for bi in blocks
            )
            for blocks in block_chunks
        ]

        chunk_owners: dict[int, set[int]] = {
            c: set(leader_ranks) for c in range(num_chunks)
        }
        completed_peers: set[int] = set()

        while len(completed_peers) < len(peer_ranks):
            # Schedule: each source sends one chunk to one destination.
            # Sources may not send two chunks at once (overwrites staging).
            # A node CAN be src for chunk A and dst for chunk B in the
            # same round because they use separate staging buffers.
            src_used: set[int] = set()
            dst_used: set[int] = set()
            pairs: list[tuple[int, int, int]] = []
            for chunk_idx in range(num_chunks):
                for src_rank in list(chunk_owners[chunk_idx]):
                    if src_rank in src_used:
                        continue
                    dst_rank = None
                    for c in peer_ranks:
                        if (
                            c not in completed_peers
                            and c not in chunk_owners[chunk_idx]
                            and c not in dst_used
                        ):
                            dst_rank = c
                            break
                    if dst_rank is None:
                        continue
                    src_used.add(src_rank)
                    dst_used.add(dst_rank)
                    pairs.append((src_rank, dst_rank, chunk_idx))

            if not pairs:
                break

            # Fire all staging calls in parallel (different sources).
            staging_futures = []
            for src_rank, dst_rank, chunk_idx in pairs:
                src = flat_actors.slice(rank=src_rank)
                staging_futures.append(
                    (
                        src_rank,
                        dst_rank,
                        chunk_idx,
                        src.get_blocks_rdma_buffer.call(
                            block_chunks[chunk_idx], total_size
                        ),
                    )
                )

            # Collect staging results and fire transfers.
            transfer_futures = []
            for _src_rank, dst_rank, chunk_idx, staging_f in staging_futures:
                rdma_result = staging_f.get()
                rdma_buf = [v for _, v in rdma_result][0]
                dst = flat_actors.slice(rank=dst_rank)
                transfer_futures.append(
                    (
                        dst_rank,
                        chunk_idx,
                        dst.replace_blocks.call(
                            block_chunks[chunk_idx],
                            total_size,
                            rdma_buf,
                            chunk_byte_sizes[chunk_idx],
                        ),
                    )
                )

            for dst_rank, chunk_idx, f in transfer_futures:
                f.get()
                chunk_owners[chunk_idx].add(dst_rank)
                if all(dst_rank in chunk_owners[c] for c in range(num_chunks)):
                    completed_peers.add(dst_rank)

        t_rdma = time.time()
        tls_gbs = (total_bytes / 1e9) / max(t_tls - t0, 1e-9)
        rdma_gbs = (total_bytes * len(peer_ranks) / 1e9) / max(t_rdma - t_tls, 1e-9)
        logger.info(
            f"TLS→{num_leaders} leaders: {total_bytes // (1024**2)}MiB in {t_tls - t0:.1f}s "
            f"({tls_gbs:.1f} GB/s), "
            f"RDMA fan-out: {len(dirty_blocks)} blocks to {len(peer_ranks)} "
            f"peers in {t_rdma - t_tls:.1f}s ({rdma_gbs:.1f} GB/s)"
        )

    def close(self) -> None:
        """Unmount FUSE but keep actors alive for incremental updates."""
        if self.fuse_actors is not None:
            result = self.fuse_actors.unmount.call(self.mntpoint).get()
            for _point, (rc, stderr) in result:
                if rc != 0:
                    logger.warning(f"fusermount3 -u failed (rc={rc}): {stderr.strip()}")

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
    transfer_mode: str = "rust_tls",
) -> MountHandler:
    """Mount a local directory on remote hosts via RDMA transfer and FUSE.

    Args:
        transfer_mode: "rust_tls" (default) uses custom Rust TLS sender/receiver
            for maximum throughput. "actor" uses monarch's built-in actor message
            passing — slower but works without Meta TLS certs (e.g. CI, local testing).
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
    )
