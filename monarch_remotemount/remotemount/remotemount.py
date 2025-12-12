# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import sys
import torch
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from monarch.actor import Actor, current_rank, endpoint
from monarch.actor import this_host
from monarch.actor import HostMesh

from monarch.job import SlurmJob

from typing import Optional

logger = logging.getLogger(__name__)

CHUNK_SIZE = (1024 * 1024 * 1024) * 8


def pack_directory_chunked(source_path):
    logger.info(f"Packing '{source_path}' into {CHUNK_SIZE//(2**20)}MiB chunks...")

    fs_metadata = {}

    all_full_file_paths = []

    # Tracks the virtual address of the filesystem
    current_global_offset = 0

    source_path = os.path.abspath(source_path)

    logger.info("Creating metadata dictionary")

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
            all_full_file_paths.append(full_path)

            virtual_path = (rel_path + '/' + f) if rel_path != '/' else ('/' + f)

            # Read file
            with open(full_path, 'rb') as fo:
                # file_bytes = fo.read()
                file_len = os.fstat(fo.fileno()).st_size
                # file_len = len(file_bytes)

            # Store metadata pointing to the current global offset
            st = os.stat(full_path)
            attr = dict((key, getattr(st, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode',
                'st_mtime', 'st_nlink', 'st_size', 'st_uid'
            ))
            attr['st_size'] = file_len

            fs_metadata[virtual_path] = {
                'attr': attr,
                'global_offset': current_global_offset,
                'file_len': file_len
            }
            # Tracks the virtual address of the filesystem
            current_global_offset += file_len

    logger.info("Concat file contents into a single buffer")

    def read_file_with_index(args):
        idx, path = args
        with open(path, 'rb') as f:
            return idx, f.read()

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = dict(executor.map(read_file_with_index, enumerate(all_full_file_paths)))

    staging_buffer = b''.join(results[i] for i in range(len(results)))

    logger.info("Creating chunks")

    mv = memoryview(staging_buffer)
    chunks = [
        mv[i:i + CHUNK_SIZE]
        for i in range(0, len(staging_buffer), CHUNK_SIZE)
    ]

    logger.info(f"Packed into {len(chunks)} bytearrays.")
    if len(chunks) > 0:
        logger.info(f"Chunk sizes: {[len(c) // (1024**2) for c in chunks]}MiB")

    return fs_metadata, chunks


# Define our actor
class FUSEActor(Actor):

    def __init__(self, chunk_size):
        self.chunk_size = chunk_size
        self.meta = None
        self.chunks = []

    @endpoint
    def set_meta(self, meta):
        self.meta = meta

    @endpoint
    def append_chunk(self, chunk):
        self.chunks.append(chunk)

    @endpoint
    def len_chunks(self):
        return len(self.chunks)

    @endpoint
    def mount(self, mount_point):

        from fuse import FUSE, FuseOSError, Operations
        import errno
        import functools
        import errno
        from itertools import count
        from fuse import FuseOSError, Operations


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
                 host_mesh: HostMesh,
                 sourcepath: str,
                 mntpoint: Optional[str] = None):
        self.host_mesh = host_mesh
        self.sourcepath = sourcepath
        if mntpoint is None:
            mntpoint = sourcepath
        self.mntpoint = mntpoint
        self.fuse_actors = None
        self.procs = self.host_mesh.spawn_procs()

    def open(self):
        assert self.fuse_actors is None
        self.fuse_actors = self.procs.spawn("FUSEActor", FUSEActor, CHUNK_SIZE)
        self.fuse_actors.run_commands.call(["mkdir", "-p", self.mntpoint]).get()
        meta, chunks = pack_directory_chunked(self.sourcepath)
        logger.info(f"Sending chunks and remotely mounting {self.sourcepath} under {self.mntpoint}")
        logger.info(f"Sending meta")
        self.fuse_actors.set_meta.call(meta).get()
        for chunk in chunks:
            logger.info(f"Sending chunk of size {len(chunk) // (1024**2)}MiB")
            self.fuse_actors.append_chunk.call(chunk).get()
        logger.info(f"Starting remote mount points")
        self.fuse_actors.mount.call(self.mntpoint).get()
        return self

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


def remotemount(host_mesh: HostMesh,
                sourcepath: str,
                mntpoint: Optional[str] = None):
    return MountHandler(host_mesh, sourcepath, mntpoint)
