# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import cloudpickle
import logging
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from monarch.actor import Actor, endpoint
from remotemount._fast_pack import pack_files_with_offsets as _c_pack_files

from typing import Optional

logger = logging.getLogger(__name__)

CHUNK_SIZE = (1024 * 1024 * 1024) * 8


def pack_directory_chunked(source_path, chunk_size=None):
    if chunk_size is None:
        chunk_size = CHUNK_SIZE

    fs_metadata = {}
    file_list = []

    # Tracks the virtual address of the filesystem
    current_global_offset = 0

    source_path = os.path.abspath(source_path)

    for root, dirs, files in os.walk(source_path):
        rel_path = root[len(source_path):]
        if rel_path == '': rel_path = '/'

        # Directory Metadata
        st = os.stat(root)
        fs_metadata[rel_path] = {
            'attr': dict((key, getattr(st, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode',
                'st_mtime', 'st_nlink', 'st_size', 'st_uid'
            )),
            'children': dirs + files
        }

        for f in files:
            full_path = os.path.join(root, f)
            virtual_path = (rel_path + '/' + f) if rel_path != '/' else ('/' + f)

            lst = os.lstat(full_path)
            is_symlink = (lst.st_mode & 0o170000) == 0o120000

            if is_symlink:
                fs_metadata[virtual_path] = {
                    'attr': dict((key, getattr(lst, key)) for key in (
                        'st_atime', 'st_ctime', 'st_gid', 'st_mode',
                        'st_mtime', 'st_nlink', 'st_size', 'st_uid'
                    )),
                    'link_target': os.readlink(full_path)
                }
            else:
                file_len = lst.st_size
                attr = dict((key, getattr(lst, key)) for key in (
                    'st_atime', 'st_ctime', 'st_gid', 'st_mode',
                    'st_mtime', 'st_nlink', 'st_size', 'st_uid'
                ))
                attr['st_size'] = file_len

                fs_metadata[virtual_path] = {
                    'attr': attr,
                    'global_offset': current_global_offset,
                    'file_len': file_len
                }

                file_list.append((full_path, current_global_offset, file_len))
                # Tracks the virtual address of the filesystem
                current_global_offset += file_len

    total_size = current_global_offset
    logger.info(f"Packing {total_size // (1024**2)}MiB, {len(file_list)} files")

    if total_size == 0:
        return fs_metadata, []

    staging_mv = _c_pack_files(file_list, total_size)
    chunks = [
        staging_mv[i:i + chunk_size]
        for i in range(0, len(staging_mv), chunk_size)
    ]

    return fs_metadata, chunks


class FUSEActor(Actor):

    def __init__(self, chunk_size, use_rdma=False, backend="slurm"):
        self.chunk_size = chunk_size
        self.use_rdma = use_rdma
        self.backend = backend
        self.meta = None
        self.chunks = []
        self._chunk_storage = None
        self._chunk_offsets = None
        self._next_chunk_idx = 0

    @endpoint
    def set_meta(self, meta):
        self.meta = meta

    @endpoint
    def append_chunk(self, chunk):
        self.chunks.append(chunk)

    @endpoint
    def init_chunk_storage(self, chunk_sizes):
        import mmap
        total_size = sum(chunk_sizes)
        self._chunk_storage = mmap.mmap(-1, total_size, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
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
        if self.use_rdma and self.backend == "slurm":
            from monarch.rdma import is_rdma_available
            if not is_rdma_available():
                try:
                    logger.debug("[WORKER] init_chunk_storage: starting EFA init")
                    from monarch._src.rdma.rdma import _ensure_init_efa_manager
                    _ensure_init_efa_manager().block_on()
                    logger.debug("[WORKER] init_chunk_storage: EFA init complete")
                except ImportError:
                    # EFA APIs not available in this Monarch build (e.g., MAST variant)
                    # This shouldn't happen if is_rdma_available() is working correctly
                    logger.debug("[WORKER] init_chunk_storage: EFA APIs not available, skipping")

    @endpoint
    def fetch_chunk_rdma(self, rdma_buffer, chunk_size: int, timeout: int = 60):
        """Receive RDMABuffer (works with both ibverbs and EFA) and read from it."""
        logger.debug(f"[WORKER] fetch_chunk_rdma: starting chunk {self._next_chunk_idx}")
        offset, _ = self._chunk_offsets[self._next_chunk_idx]
        dst_mv = self._chunk_storage_mv[offset:offset + chunk_size]
        logger.debug("[WORKER] fetch_chunk_rdma: calling read_into")
        rdma_buffer.read_into(dst_mv, timeout=timeout).get()
        logger.debug("[WORKER] fetch_chunk_rdma: read_into complete")
        self.chunks.append(dst_mv)
        self._next_chunk_idx += 1

    @endpoint
    def mount(self, mount_point):
        # Only set FUSE_LIBRARY_PATH on mast (slurm sets it via job script)
        if self.backend == "mast":
            os.environ["FUSE_LIBRARY_PATH"] = "/packages/monarch_default_workspace/conda/lib/libfuse.so.2.9.9"
        from fuse import FUSE, FuseOSError, Operations
        import errno
        from itertools import count

        class ChunkedFS(Operations):
            __slots__ = ('metadata', 'chunks', 'chunk_size', 'fh_map', '_fh_counter')

            def __init__(self, metadata, chunks, chunk_size):
                self.metadata = metadata
                self.chunks = chunks
                self.chunk_size = chunk_size
                self.fh_map = {}
                self._fh_counter = count()

            def getattr(self, path, fh=None):
                meta = self.metadata.get(path)
                if meta is None:
                    raise FuseOSError(errno.ENOENT)
                return meta['attr']

            def readlink(self, path):
                meta = self.metadata.get(path)
                if meta is None:
                    raise FuseOSError(errno.ENOENT)
                link_target = meta.get('link_target')
                if link_target is None:
                    raise FuseOSError(errno.EINVAL)
                return link_target

            def access(self, path, mode):
                if path not in self.metadata:
                    raise FuseOSError(errno.ENOENT)

            def readdir(self, path, fh):
                meta = self.metadata.get(path)
                if meta is None:
                    raise FuseOSError(errno.ENOENT)
                yield '.'
                yield '..'
                children = meta.get('children')
                if children:
                    yield from children

            def open(self, path, flags):
                if path not in self.metadata:
                    raise FuseOSError(errno.ENOENT)
                fh = next(self._fh_counter)
                self.fh_map[fh] = (path, flags)
                return fh

            def release(self, path, fh):
                self.fh_map.pop(fh, None)

            def read(self, path, length, offset, fh):
                meta = self.metadata.get(path)
                if meta is None:
                    raise FuseOSError(errno.ENOENT)

                assert self.fh_map[fh][0] == path

                global_offset = meta.get('global_offset')
                if global_offset is None:
                    return b''

                chunk_size = self.chunk_size
                chunks = self.chunks
                num_chunks = len(chunks)

                start_pos = global_offset + offset
                start_chunk_idx = start_pos // chunk_size

                if start_chunk_idx >= num_chunks:
                    return b''

                # Fast path: single-chunk read (common case)
                offset_in_chunk = start_pos % chunk_size
                first_chunk = chunks[start_chunk_idx]
                available_in_first = len(first_chunk) - offset_in_chunk

                if length <= available_in_first:
                    return bytes(
                        memoryview(first_chunk)[offset_in_chunk:offset_in_chunk + length]
                    )

                # Multi-chunk read
                pieces = [memoryview(first_chunk)[offset_in_chunk:]]
                remaining = length - available_in_first
                chunk_idx = start_chunk_idx + 1

                while remaining > 0 and chunk_idx < num_chunks:
                    chunk = chunks[chunk_idx]
                    chunk_len = len(chunk)

                    if chunk_len == 0:
                        break

                    if remaining >= chunk_len:
                        pieces.append(memoryview(chunk))
                        remaining -= chunk_len
                    else:
                        pieces.append(memoryview(chunk)[:remaining])
                        remaining = 0

                    chunk_idx += 1

                return b''.join(pieces)

        operations = ChunkedFS(self.meta, self.chunks, self.chunk_size)
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(FUSE, operations, mount_point, foreground=True, nothreads=True)

        start_time = time.time()
        while True:
            # Check if the thread crashed immediately (e.g. Permission Denied)
            if future.done():
                # Calling .result() will raise the exception that killed the thread
                future.result()
                raise RuntimeError("FUSE thread exited unexpectedly.")
            # Check if the OS sees the mount
            if os.path.ismount(mount_point):
                break
            if time.time() - start_time > 50:
                raise TimeoutError("Timed out waiting for FUSE mount.")
            time.sleep(0.1)
        self.future = future
        self.executor = executor
        return 0

    @endpoint
    def run_commands(self, commands):
        result = subprocess.run(commands,
                                capture_output=True,
                                text=True)
        return result.returncode, result.stdout, result.stderr


class MountHandler:
    def __init__(self,
                 host_mesh,
                 sourcepath: str,
                 mntpoint: Optional[str] = None,
                 chunk_size=None,
                 use_rdma: bool = False,
                 backend: str = "slurm"):
        self.sourcepath = sourcepath
        if mntpoint is None:
            mntpoint = sourcepath
        self.mntpoint = mntpoint
        self.fuse_actors = None
        self.host_mesh = host_mesh
        self.procs = None
        self.chunk_size = chunk_size
        self.use_rdma = use_rdma
        self.backend = backend

    def open(self):
        assert self.fuse_actors is None
        self.procs = self.host_mesh.spawn_procs(per_host={"gpus": 1})
        self.fuse_actors = self.procs.spawn("FUSEActor", FUSEActor, self.chunk_size, self.use_rdma, self.backend)
        self.fuse_actors.run_commands.call(["mkdir", "-p", self.mntpoint]).get()
        meta, chunks = pack_directory_chunked(self.sourcepath, self.chunk_size)
        logger.info(f"Sending chunks and remotely mounting {self.sourcepath} under {self.mntpoint}")
        logger.info(f"Sending meta")
        self.fuse_actors.set_meta.call(meta).get()
        if self.use_rdma:
            self._transfer_rdma(chunks)
        else:
            for chunk in chunks:
                logger.info(f"Sending chunk of size {len(chunk) // (1024**2)}MiB")
                self.fuse_actors.append_chunk.call(chunk).get()
        logger.info(f"Starting remote mount points")
        self.fuse_actors.mount.call(self.mntpoint).get()
        return self

    def _transfer_rdma(self, chunks):
        """Transfer chunks using RDMA (works with both ibverbs and EFA).

        With the actor-based EFA implementation, both backends now use the same
        pattern: create RDMABuffer, send it to workers, workers read from it.
        The RDMABuffer is picklable for both backends.
        """
        from monarch.rdma import RDMABuffer, is_rdma_available
        try:
            from monarch.rdma import is_efa_available
        except ImportError:
            is_efa_available = lambda: False

        # Determine backend
        if is_rdma_available():
            backend = "ibverbs"
        elif is_efa_available():
            backend = "efa"
        else:
            raise RuntimeError("No RDMA backend available (neither ibverbs nor EFA)")

        num_workers = len(self.fuse_actors)
        total_bytes = sum(len(c) for c in chunks)
        chunk_sizes = [len(c) for c in chunks]
        self.fuse_actors.init_chunk_storage.call(chunk_sizes).get()

        chunk_rdma_buffers = [RDMABuffer(chunk) for chunk in chunks]
        flat_actors = self.fuse_actors.flatten("rank")

        transfer_start = time.time()
        all_futures = []
        for i, chunk in enumerate(chunks):
            rdma_buffer = chunk_rdma_buffers[i]
            for worker_idx in range(num_workers):
                worker_actor = flat_actors.slice(rank=worker_idx)
                future = worker_actor.fetch_chunk_rdma.call(rdma_buffer, len(chunk))
                all_futures.append(future)

        for future in all_futures:
            future.get()

        transfer_elapsed = time.time() - transfer_start
        per_host_gbps = (total_bytes * 8.0 / 1e9) / transfer_elapsed if transfer_elapsed > 0 else 0
        logger.info(f"RDMA transfer ({backend}): {total_bytes // (1024**2)}MiB in {transfer_elapsed:.1f}s ({per_host_gbps:.1f} Gbps/host, {num_workers} hosts)")

    def close(self):
        if self.fuse_actors is not None:
            self.fuse_actors.run_commands.call(["umount", self.mntpoint]).get()
            self.fuse_actors = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Don't suppress exceptions


def remotemount(host_mesh,
                sourcepath: str,
                mntpoint: Optional[str] = None,
                chunk_size=None,
                use_rdma: bool = False,
                backend: str = "slurm"):
    if chunk_size is None:
        chunk_size = CHUNK_SIZE
    return MountHandler(host_mesh, sourcepath, mntpoint, chunk_size, use_rdma, backend)


# Register for pickle-by-value so classes are serialized to remote workers
import remotemount.remotemount as _this_module
cloudpickle.register_pickle_by_value(_this_module)
