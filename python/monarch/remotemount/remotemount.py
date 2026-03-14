# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import logging
import os
import subprocess
import time
from typing import Optional

from monarch._rust_bindings.monarch_extension.fast_pack import (
    pack_files_to_shm as _c_pack_files_to_shm,
    pack_files_with_offsets as _c_pack_files,
)
from monarch.actor import Actor, endpoint

logger = logging.getLogger(__name__)

CHUNK_SIZE = (1024 * 1024 * 1024) * 8
HASH_BLOCK_SIZE = 64 * 1024 * 1024  # 64MB blocks for incremental diffing
CACHE_DIR = "/tmp/monarch_remotemount_cache"


def block_hashes(data_mv, block_size=HASH_BLOCK_SIZE):
    """Compute xxhash per block of a packed memoryview."""
    import xxhash

    hashes = []
    for i in range(0, len(data_mv), block_size):
        hashes.append(xxhash.xxh64(bytes(data_mv[i : i + block_size])).hexdigest())
    return hashes


def classify_workers(client_hashes, client_total_size, worker_states):
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
        elif remote_size == client_total_size and len(remote_hashes) == len(
            client_hashes
        ):
            dirty = [
                i
                for i, (a, b) in enumerate(zip(client_hashes, remote_hashes))
                if a != b
            ]
            worker_dirty[rank] = dirty
        else:
            worker_dirty[rank] = None
    return fresh_ranks, worker_dirty


def pack_directory_chunked(source_path, chunk_size=None, use_shm=False):
    """Walk a directory, pack all files into contiguous mmap chunks.

    Returns (fs_metadata, staging_mv, chunks, shm_path, block_hashes_list)
    where:
    - fs_metadata: dict mapping virtual paths to stat/offset metadata
    - staging_mv: memoryview over the packed data
    - chunks: list of chunk-sized memoryview slices
    - shm_path: path to named pack file if use_shm=True, else None
    - block_hashes_list: list of xxh64 hex digest strings per 100 MB block
    """
    if chunk_size is None:
        chunk_size = CHUNK_SIZE

    fs_metadata = {}
    file_list = []

    # Tracks the virtual address of the filesystem
    current_global_offset = 0

    source_path = os.path.abspath(source_path)

    for root, dirs, files in os.walk(source_path):
        rel_path = root[len(source_path) :]
        if rel_path == "":
            rel_path = "/"

        # Directory Metadata
        st = os.stat(root)
        fs_metadata[rel_path] = {
            "attr": {
                key: getattr(st, key)
                for key in (
                    "st_atime",
                    "st_ctime",
                    "st_gid",
                    "st_mode",
                    "st_mtime",
                    "st_nlink",
                    "st_size",
                    "st_uid",
                )
            },
            "children": dirs + files,
        }

        for f in files:
            full_path = os.path.join(root, f)
            virtual_path = (rel_path + "/" + f) if rel_path != "/" else ("/" + f)

            lst = os.lstat(full_path)
            is_symlink = (lst.st_mode & 0o170000) == 0o120000

            if is_symlink:
                fs_metadata[virtual_path] = {
                    "attr": {
                        key: getattr(lst, key)
                        for key in (
                            "st_atime",
                            "st_ctime",
                            "st_gid",
                            "st_mode",
                            "st_mtime",
                            "st_nlink",
                            "st_size",
                            "st_uid",
                        )
                    },
                    "link_target": os.readlink(full_path),
                }
            else:
                file_len = lst.st_size
                attr = {
                    key: getattr(lst, key)
                    for key in (
                        "st_atime",
                        "st_ctime",
                        "st_gid",
                        "st_mode",
                        "st_mtime",
                        "st_nlink",
                        "st_size",
                        "st_uid",
                    )
                }
                attr["st_size"] = file_len

                fs_metadata[virtual_path] = {
                    "attr": attr,
                    "global_offset": current_global_offset,
                    "file_len": file_len,
                }

                file_list.append((full_path, current_global_offset, file_len))
                # Tracks the virtual address of the filesystem
                current_global_offset += file_len

    total_size = current_global_offset
    logger.info(f"Packing {total_size // (1024**2)}MiB, {len(file_list)} files")

    if total_size == 0:
        return fs_metadata, None, [], None, []

    if use_shm:
        staging_mv, shm_path, hashes = _c_pack_files_to_shm(file_list, total_size)
    else:
        staging_mv, hashes = _c_pack_files(file_list, total_size)
        shm_path = None
    chunks = [
        staging_mv[i : i + chunk_size] for i in range(0, len(staging_mv), chunk_size)
    ]

    return fs_metadata, staging_mv, chunks, shm_path, list(hashes)


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

    @endpoint
    def try_load_cache(self, cache_key):
        """Load cached chunk data from a previous run, if available.

        Sets ``_cache_path`` so that subsequent ``init_chunk_storage`` and
        ``collect_shards`` calls use file-backed mmap. If a cache file
        already exists, mmaps it and computes block hashes so the client
        can classify this worker as fresh or partial.
        """
        import mmap

        os.makedirs(CACHE_DIR, exist_ok=True)
        self._cache_path = os.path.join(CACHE_DIR, cache_key)

        try:
            if os.path.exists(self._cache_path):
                size = os.path.getsize(self._cache_path)
                if size > 0:
                    fd = os.open(self._cache_path, os.O_RDWR)
                    try:
                        self._chunk_storage = mmap.mmap(fd, size)
                    finally:
                        os.close(fd)
                    self._chunk_storage_mv = memoryview(self._chunk_storage)
                    self._total_size = size
                    self._block_hashes = block_hashes(self._chunk_storage_mv)

                    # Build chunks list so mount() works with cached data.
                    self.chunks = []
                    self._chunk_offsets = []
                    remaining = size
                    off = 0
                    idx = 0
                    while remaining > 0:
                        sz = min(remaining, self.chunk_size)
                        self._chunk_offsets.append((off, sz))
                        self.chunks.append(self._chunk_storage_mv[off : off + sz])
                        off += sz
                        remaining -= sz
                        idx += 1
                    self._next_chunk_idx = idx

                    logger.info(
                        f"[CACHE] Loaded {self._cache_path}: "
                        f"{size // (1024**2)}MiB, "
                        f"{len(self._block_hashes)} block hashes"
                    )
        except Exception:
            logger.warning(
                f"[CACHE] Failed to load cache {self._cache_path}, "
                "will do full transfer",
                exc_info=True,
            )
            self._block_hashes = []
            self._total_size = 0

    @endpoint
    def set_meta(self, meta):
        self.meta = meta

    @endpoint
    def init_chunk_storage(self, chunk_sizes):
        import mmap

        # Reset state from any previous transfer.
        self.chunks = []
        self._next_chunk_idx = 0

        total_size = sum(chunk_sizes)
        if self._cache_path:
            fd = os.open(self._cache_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.ftruncate(fd, total_size)
                self._chunk_storage = mmap.mmap(fd, total_size)
            finally:
                os.close(fd)
        else:
            self._chunk_storage = mmap.mmap(
                -1, total_size, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS
            )
        self._chunk_storage_mv = memoryview(self._chunk_storage)
        self._chunk_offsets = []
        offset = 0
        for size in chunk_sizes:
            self._chunk_offsets.append((offset, size))
            offset += size
        self._next_chunk_idx = 0

        # EFA requires explicit initialization of its manager actor on each worker.
        # Unlike ibverbs which lazily initializes its RDMA context when creating
        # buffers, EFA uses an actor-based approach (EfaManagerActor) that must be
        # spawned before workers can register destination buffers for RDMA transfers.
        # Only needed on SLURM (EFA), not MAST (ibverbs).
        if self.backend == "slurm":
            from monarch.rdma import is_rdma_available

            if not is_rdma_available():
                try:
                    logger.debug("[WORKER] init_chunk_storage: starting EFA init")
                    from monarch._src.rdma.rdma import _ensure_init_efa_manager

                    _ensure_init_efa_manager().block_on()
                    logger.debug("[WORKER] init_chunk_storage: EFA init complete")
                except ImportError:
                    logger.debug(
                        "[WORKER] init_chunk_storage: EFA APIs not available, skipping"
                    )

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
        gbps = (chunk_size * 8.0 / 1e9) / max(t2 - t1, 1e-9)
        logger.info(
            f"[WORKER] fetch_chunk {idx}: {chunk_size / (1024**2):.0f}MiB "
            f"in {t2 - t1:.3f}s ({gbps:.1f} Gbps)"
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
        gbps = (chunk_size * n * 8.0 / 1e9) / max(t3 - t1, 1e-9)
        logger.info(
            f"[WORKER] fanout_chunk {idx}: setup={t1 - t0:.3f}s, "
            f"dispatch={t2 - t1:.3f}s, wait={t3 - t2:.3f}s, "
            f"total={t3 - t0:.3f}s ({gbps:.1f} Gbps aggregate, {n} peers)"
        )

    @endpoint
    def replace_block(
        self, block_idx: int, rdma_buffer, block_size: int, timeout: int = 300
    ):
        """Overwrite a single hash block in existing storage via RDMA."""
        import mmap as _mmap

        offset = block_idx * HASH_BLOCK_SIZE
        dst_mv = self._chunk_storage_mv[offset : offset + block_size]

        if self._cache_path:
            # Don't explicitly close the anonymous mmap — it can raise
            # BufferError if the Rust RDMABuffer still holds a reference.
            anon = _mmap.mmap(-1, block_size, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS)
            anon_mv = memoryview(anon)
            rdma_buffer.read_into(anon_mv, timeout=timeout).get()
            dst_mv[:] = anon_mv
        else:
            rdma_buffer.read_into(dst_mv, timeout=timeout).get()

    @endpoint
    def mount(self, mount_point, new_block_hashes=None, total_size=0):
        import json

        from monarch._rust_bindings.monarch_extension.chunked_fuse import (
            mount_chunked_fuse,
        )

        # Flush mmap to disk so the cache file persists across actor restarts.
        if self._cache_path and self._chunk_storage is not None:
            try:
                self._chunk_storage.flush()
            except Exception:
                pass

        self._fuse_handle = mount_chunked_fuse(
            json.dumps(self.meta),
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
    def get_cache_path(self):
        """Return the cache file path for this worker."""
        return self._cache_path

    @endpoint
    def prepare_receiver(self, num_streams, total_size):
        """Create a Rust TLS receiver and return its address.

        Allocates chunk storage (file-backed or anonymous mmap) and creates
        a TlsReceiver that will write received blocks directly into it.
        """
        import mmap

        from monarch._rust_bindings.monarch_extension.tls_receiver import TlsReceiver

        # Allocate storage if not already present.
        if self._chunk_storage is None or self._total_size != total_size:
            if self._cache_path:
                fd = os.open(
                    self._cache_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600
                )
                try:
                    os.ftruncate(fd, total_size)
                    self._chunk_storage = mmap.mmap(fd, total_size)
                finally:
                    os.close(fd)
            else:
                self._chunk_storage = mmap.mmap(
                    -1, total_size, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS
                )
            self._chunk_storage_mv = memoryview(self._chunk_storage)
            self._total_size = total_size

            # Build chunks list for mount().
            self.chunks = []
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

        self._tls_receiver = TlsReceiver(num_streams)
        return self._tls_receiver.addr

    @endpoint
    def receive_blocks(self):
        """Block until the TLS receiver has finished receiving all blocks."""
        if self._tls_receiver is None:
            raise RuntimeError("prepare_receiver() not called")
        self._tls_receiver.wait(self._chunk_storage_mv)
        self._tls_receiver = None
        return True

    @endpoint
    def run_commands(self, commands):
        result = subprocess.run(commands, capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr


class MountHandler:
    def __init__(
        self,
        host_mesh,
        sourcepath: str,
        mntpoint: Optional[str] = None,
        chunk_size=None,
        backend: str = "slurm",
        num_parallel_streams: int = 8,
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
        self._staging_mv = None

    def open(self):
        t_open_start = time.time()

        # Reuse existing actors if available (preserves block hashes
        # for incremental update checks).
        if self.fuse_actors is None:
            self.procs = self.host_mesh.spawn_procs(per_host={"gpus": 1})
            self.fuse_actors = self.procs.spawn(
                "FUSEActor", FUSEActor, self.chunk_size, self.backend
            )
            self.fuse_actors.run_commands.call(["mkdir", "-p", self.mntpoint]).get()

            import xxhash

            cache_key = xxhash.xxh64(
                (self.sourcepath + ":" + self.mntpoint).encode()
            ).hexdigest()
            self.fuse_actors.try_load_cache.call(cache_key).get()

        t_actors_ready = time.time()

        # Fire get_block_hashes before packing so the network round-trip
        # overlaps with the CPU-bound pack+hash step.
        flat_actors = self.fuse_actors.flatten("rank")
        num_workers = len(flat_actors)
        hashes_future = self.fuse_actors.get_block_hashes.call()

        meta, self._staging_mv, chunks, self._pack_shm_path, client_hashes = (
            pack_directory_chunked(self.sourcepath, self.chunk_size, use_shm=True)
        )
        staging_mv = self._staging_mv
        client_total_size = len(staging_mv) if staging_mv is not None else 0

        t_pack_done = time.time()

        # Collect worker hashes (should already be available after packing).
        try:
            result = hashes_future.get()
            worker_states = [
                (remote_hashes, remote_size)
                for _point, (remote_hashes, remote_size) in result
            ]
            fresh_ranks, worker_dirty = classify_workers(
                client_hashes, client_total_size, worker_states
            )
        except Exception as e:
            logger.info(f"Block hash query failed: {e}")
            fresh_ranks = []
            worker_dirty = {rank: None for rank in range(num_workers)}

        t_classify_done = time.time()

        # Always send metadata so newly spawned actors (which loaded
        # block data from the persistent cache) have filesystem layout.
        self.fuse_actors.set_meta.call(meta).get()

        t_meta_done = time.time()

        if not worker_dirty:
            self.fuse_actors.mount.call(
                self.mntpoint, client_hashes, client_total_size
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
            try:
                flat_actors.slice(rank=rank).run_commands.call(
                    ["fusermount3", "-u", self.mntpoint]
                ).get()
            except Exception:
                pass

        t_unmount_done = time.time()

        # Update each non-fresh worker.
        stale_ranks = [r for r, d in worker_dirty.items() if d is None]
        partial_ranks = [r for r, d in worker_dirty.items() if d is not None]

        for rank in partial_ranks:
            actor = flat_actors.slice(rank=rank)
            dirty = worker_dirty[rank]
            logger.info(
                f"Worker {rank}: {len(dirty)}/{len(client_hashes)} blocks dirty"
            )
            self._transfer_blocks_rust_tls(actor, dirty, client_total_size)

        t_partial_done = time.time()

        if stale_ranks and staging_mv is not None:
            # Rust TLS to leader, RDMA fan-out to peers (if any).
            self._transfer_fanout(
                flat_actors, stale_ranks, staging_mv, chunks, client_total_size
            )

            # Clean up the client-side pack file after all transfers.
            if self._pack_shm_path is not None:
                try:
                    os.unlink(self._pack_shm_path)
                except OSError:
                    pass
                self._pack_shm_path = None

        t_transfer_done = time.time()

        # Remount all workers (fresh ones for metadata update).
        self.fuse_actors.mount.call(
            self.mntpoint, client_hashes, client_total_size
        ).get()

        t_mount_done = time.time()

        logger.info(
            f"open() timings: actors={t_actors_ready - t_open_start:.2f}s, "
            f"pack+hash={t_pack_done - t_actors_ready:.2f}s "
            f"({client_total_size / (1024**2):.0f}MiB), "
            f"classify={t_classify_done - t_pack_done:.2f}s, "
            f"set_meta={t_meta_done - t_classify_done:.2f}s, "
            f"unmount={t_unmount_done - t_meta_done:.2f}s, "
            f"partial={t_partial_done - t_unmount_done:.2f}s, "
            f"transfer={t_transfer_done - t_partial_done:.2f}s, "
            f"mount={t_mount_done - t_transfer_done:.2f}s, "
            f"total={t_mount_done - t_open_start:.2f}s"
        )
        return self

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

        # Get cache path from the FUSEActor.
        cache_result = fuse_actor.get_cache_path.call().get()
        cache_path = [v for _, v in cache_result][0]
        if cache_path is None:
            cache_path = ""

        total_bytes = sum(
            min(HASH_BLOCK_SIZE, total_size - bi * HASH_BLOCK_SIZE)
            for bi in dirty_blocks
        )

        # 1. Start receiver on worker.
        t_start = time.time()
        addr_result = fuse_actor.prepare_receiver.call(num_streams, total_size).get()
        addr = [v for _, v in addr_result][0]
        addresses = [addr] * num_streams

        # 2. Fire receive_blocks (non-blocking) so worker starts waiting.
        recv_future = fuse_actor.receive_blocks.call()

        # 3. Send blocks directly from the staging buffer.
        t_setup = time.time()
        send_blocks_from_buffer(
            self._staging_mv, total_size, dirty_blocks, addresses, cache_path
        )
        t_send = time.time()

        # 4. Wait for receiver to finish.
        recv_future.get()
        t_done = time.time()

        gbps = (total_bytes * 8.0 / 1e9) / max(t_send - t_setup, 1e-9)
        logger.info(
            f"Rust TLS block transfer ({len(dirty_blocks)} blocks, "
            f"{num_streams} streams): {total_bytes // (1024**2)}MiB "
            f"in {t_send - t_setup:.1f}s ({gbps:.1f} Gbps), "
            f"setup={t_setup - t_start:.2f}s, "
            f"total={t_done - t_start:.1f}s"
        )

    def _transfer_fanout(
        self, flat_actors, stale_ranks, staging_mv, chunks, total_size
    ):
        """Transfer data to leader via Rust TLS, then fan out to peers via RDMA.

        Phase 1: Send full payload to stale_ranks[0] using Rust TLS.
        Phase 2: Leader fans out each chunk to all other stale workers via RDMA.

        Metadata must already be sent to all workers before calling this.
        """
        leader_rank = stale_ranks[0]
        peer_ranks = stale_ranks[1:]
        leader = flat_actors.slice(rank=leader_rank)

        chunk_sizes = [len(c) for c in chunks]
        all_blocks = list(range((total_size + HASH_BLOCK_SIZE - 1) // HASH_BLOCK_SIZE))

        # Phase 1: Rust TLS transfer to leader.
        t0 = time.time()
        self._transfer_blocks_rust_tls(leader, all_blocks, total_size)
        t1 = time.time()

        # Phase 2: RDMA fan-out from leader to peers (skip if single worker).
        if peer_ranks:
            for rank in peer_ranks:
                flat_actors.slice(rank=rank).init_chunk_storage.call(chunk_sizes).get()

            # Build a mesh of peer FUSEActors for the fan-out call.
            if peer_ranks == list(range(peer_ranks[0], peer_ranks[-1] + 1)):
                peer_mesh = flat_actors.slice(
                    rank=slice(peer_ranks[0], peer_ranks[-1] + 1)
                )
            else:
                peer_mesh = flat_actors.slice(rank=peer_ranks[0])
                for rank in peer_ranks[1:]:
                    peer_mesh = peer_mesh.concat(flat_actors.slice(rank=rank))

            for i, chunk in enumerate(chunks):
                leader.fanout_chunk_rdma.call(peer_mesh, len(chunk), chunk_idx=i).get()

        t2 = time.time()
        n_peers = len(peer_ranks)
        total_elapsed = t2 - t0
        gbps = (total_size * n_peers * 8.0 / 1e9) / max(t2 - t1, 1e-9)
        logger.info(
            f"Fan-out: {total_size // (1024**2)}MiB to {n_peers + 1} workers "
            f"in {total_elapsed:.1f}s (TLS={t1 - t0:.1f}s, "
            f"RDMA={t2 - t1:.1f}s, {gbps:.1f} Gbps aggregate)"
        )

    def close(self):
        """Unmount FUSE but keep actors alive for incremental updates."""
        if self.fuse_actors is not None:
            try:
                self.fuse_actors.run_commands.call(
                    ["fusermount3", "-u", self.mntpoint]
                ).get()
            except Exception:
                pass

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Don't suppress exceptions


def remotemount(
    host_mesh,
    sourcepath: str,
    mntpoint: Optional[str] = None,
    chunk_size=None,
    backend: str = "slurm",
    num_parallel_streams: int = 8,
):
    """Mount a local directory on remote hosts via RDMA transfer and FUSE."""
    if chunk_size is None:
        chunk_size = CHUNK_SIZE
    return MountHandler(
        host_mesh, sourcepath, mntpoint, chunk_size, backend, num_parallel_streams
    )
