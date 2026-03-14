# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import logging
import os
import time

import cloudpickle
from monarch._rust_bindings.monarch_extension.fast_pack import (
    pack_files_to_shm as _c_pack_files_to_shm,
    pack_files_with_offsets as _c_pack_files,
)
from monarch.actor import Actor, endpoint

logger = logging.getLogger(__name__)

CHUNK_SIZE = (1024 * 1024 * 1024) * 8
HASH_BLOCK_SIZE = 64 * 1024 * 1024  # 64MB blocks for incremental diffing


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
    - block_hashes_list: list of xxh64 hex digest strings per block
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


def _make_server_ssl_context():
    """Create a TLS server context using MetaTLS certificates.

    Uses the well-known Meta cert paths so SSLWall allows the
    cross-DC connection (it inspects egress and RSTs non-TLS traffic).
    """
    import ssl

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    # server.pem is a combined cert+key PEM file.
    ctx.load_cert_chain(
        certfile="/var/facebook/x509_identities/server.pem",
        keyfile="/var/facebook/x509_identities/server.pem",
    )
    ctx.load_verify_locations(cafile="/var/facebook/rootcanal/ca.pem")
    # Don't require client certs — SSLWall only checks that TLS is used.
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _make_client_ssl_context():
    """Create a TLS client context using MetaTLS certificates."""
    import ssl

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(cafile="/var/facebook/rootcanal/ca.pem")
    # Load client cert for mutual TLS (some firewalls may require it).
    ctx.load_cert_chain(
        certfile="/var/facebook/x509_identities/server.pem",
        keyfile="/var/facebook/x509_identities/server.pem",
    )
    # The cert CN may not match the FQDN we connect to, so disable
    # hostname verification. The CA check is sufficient for SSLWall.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


class TransferShardActor(Actor):
    """Receives data blocks via persistent TLS connection.

    Each instance runs in its own process and opens its own TLS listener.
    The sender connects directly to this listener, bypassing the
    hyperactor host_agent relay that bottlenecks all actor messages
    through a single TCP connection per host pair. Multiple blocks are
    sent over a single persistent connection to avoid per-block TLS
    handshake overhead. TLS is required because SSLWall RSTs raw TCP
    connections that cross datacenters.
    """

    @endpoint
    def start_persistent_receiver(self, cache_path, total_size):
        """Start a TLS listener that receives multiple blocks over one connection.

        The sender sends a stream of (offset_u64, size_u64, data) tuples.
        A size of 0 signals completion. All data is written into the
        cache file at the specified offsets.
        """
        import mmap
        import socket
        import struct
        import threading

        t0 = time.time()
        fd = os.open(cache_path, os.O_RDWR)
        mm = mmap.mmap(fd, total_size)
        os.close(fd)
        self._persistent_mm = mm
        self._persistent_mv = memoryview(mm)
        self._recv_error = None
        self._recv_done = threading.Event()

        ssl_ctx = _make_server_ssl_context()

        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
        sock.bind(("::", 0))
        sock.listen(1)
        port = sock.getsockname()[1]

        import socket as _socket

        hostname = _socket.getfqdn()
        addr = f"{hostname}:{port}"
        t1 = time.time()

        def _recv_exact(conn, n):
            buf = bytearray(n)
            pos = 0
            while pos < n:
                chunk = conn.recv(n - pos)
                if not chunk:
                    return None
                buf[pos : pos + len(chunk)] = chunk
                pos += len(chunk)
            return bytes(buf)

        def _receive_loop():
            try:
                conn, _ = sock.accept()
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
                conn = ssl_ctx.wrap_socket(conn, server_side=True)
                blocks = 0

                while True:
                    header = _recv_exact(conn, 16)
                    if header is None:
                        break
                    offset, size = struct.unpack("!QQ", header)
                    if size == 0:
                        break

                    mv = self._persistent_mv[offset : offset + size]
                    received = 0
                    while received < size:
                        n = conn.recv_into(mv[received:])
                        if n == 0:
                            self._recv_error = (
                                f"short read at offset {offset}: "
                                f"got {received}, expected {size}"
                            )
                            return
                        received += n
                    blocks += 1

                conn.close()
                logger.info(f"[SHARD] persistent receiver: {blocks} blocks received")
            except Exception as e:
                self._recv_error = str(e)
            finally:
                sock.close()
                self._recv_done.set()

        self._recv_thread = threading.Thread(target=_receive_loop, daemon=True)
        self._recv_thread.start()

        logger.info(
            f"[SHARD] start_persistent_receiver: "
            f"listening on {addr}, setup={t1 - t0:.3f}s"
        )
        return addr

    @endpoint
    def wait_for_data(self):
        """Block until the receiver thread has finished."""
        self._recv_done.wait()
        if self._recv_error:
            raise RuntimeError(f"receive failed: {self._recv_error}")
        return True


class SenderShardActor(Actor):
    """Client-side actor for parallel data sending via direct TLS.

    Each instance runs in its own process and connects directly to
    a TransferShardActor's TLS listener, bypassing the hyperactor
    host_agent relay. This gives each sender-receiver pair its own
    independent TLS connection with its own congestion window.
    """

    @endpoint
    def connect_persistent(self, dest_addr, shm_path, total_size):
        """Establish a persistent TLS connection to a receiver.

        The connection is kept open so multiple blocks can be pushed
        without re-handshaking TLS for each one.
        """
        import mmap
        import socket

        t0 = time.time()
        fd = os.open(shm_path, os.O_RDONLY)
        self._persistent_mm = mmap.mmap(
            fd, total_size, mmap.MAP_PRIVATE, mmap.PROT_READ
        )
        os.close(fd)
        self._persistent_mv = memoryview(self._persistent_mm)

        host, port_str = dest_addr.rsplit(":", 1)
        port = int(port_str)

        ssl_ctx = _make_client_ssl_context()

        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)
        sock.settimeout(300)
        sock.connect((host, port))
        self._persistent_conn = ssl_ctx.wrap_socket(sock, server_hostname=host)
        t1 = time.time()

        logger.info(
            f"[SENDER] connect_persistent to {dest_addr}: connect+tls={t1 - t0:.3f}s"
        )

    @endpoint
    def push_block(self, offset, size):
        """Send one block over the persistent connection.

        Sends a 16-byte header (offset_u64, size_u64) followed by data.
        """
        import struct

        header = struct.pack("!QQ", offset, size)
        self._persistent_conn.sendall(header)
        self._persistent_conn.sendall(self._persistent_mv[offset : offset + size])

    @endpoint
    def push_done(self):
        """Send zero-size sentinel and close the persistent connection."""
        import struct

        header = struct.pack("!QQ", 0, 0)
        self._persistent_conn.sendall(header)
        try:
            raw = self._persistent_conn.unwrap()
            raw.close()
        except Exception:
            self._persistent_conn.close()
        del self._persistent_mv
        self._persistent_mm.close()


# Register for pickle-by-value so classes are serialized to remote workers
import monarch.remotemount.remotemount as _this_module  # noqa: E402

cloudpickle.register_pickle_by_value(_this_module)
