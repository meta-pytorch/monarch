# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Persistent low-latency TCP transport for WeightBridge sequence flags.

The bulk data plane uses the selected one-sided RDMA backend. Reusing that data
plane for an 8-byte ACK/READY/CONS word can give control messages the tail latency
of a busy RDMA submission queue. This module instead opens one full-duplex
connection per inter-node worker pair on the host-network address gathered during
connection setup. Connections are established once and carry fixed 16-byte
records for the lifetime of the endpoint.

TCP is reliable and ordered, while WeightBridge sequence values are monotonic.
There is therefore no controller queue, response, or completion handshake here:
``send`` returns when the record has been copied into the kernel socket buffer and
the peer's blocking receive loop publishes it into the existing flag word.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from collections.abc import Callable, Mapping


class TcpControlTransport:
    """One persistent full-duplex TCP socket for each remote endpoint rank."""

    _HELLO = struct.Struct("!I")
    _MESSAGE = struct.Struct("!B7xQ")  # kind + padding + uint64 sequence

    def __init__(
        self,
        rank: int,
        host: str,
        on_message: Callable[[int, int, int], None],
    ) -> None:
        self.rank = int(rank)
        self.host = host
        self._on_message = on_message
        self._cv = threading.Condition()
        self._sockets: dict[int, socket.socket] = {}
        self._send_locks: dict[int, threading.Lock] = {}
        self._recv_threads: dict[int, threading.Thread] = {}
        self._expected: set[int] = set()
        self._errors: list[BaseException] = []
        self._closed = False

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, 0))
        listener.listen(128)
        listener.settimeout(0.2)
        self._listener = listener
        self.endpoint = (host, int(listener.getsockname()[1]))
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name=f"wbridge-tcp-ctl-accept-{rank}",
            daemon=True,
        )
        self._accept_thread.start()

    @staticmethod
    def _tune(sock: socket.socket) -> None:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Linux ACKs small control records promptly.  TCP_NODELAY is the important
        # portable setting; QUICKACK is best-effort and need not exist elsewhere.
        quickack = getattr(socket, "TCP_QUICKACK", None)
        if quickack is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, quickack, 1)
            except OSError:
                pass

    @staticmethod
    def _recv_exact(sock: socket.socket, buf: bytearray) -> bool:
        view = memoryview(buf)
        pos = 0
        while pos < len(buf):
            got = sock.recv_into(view[pos:])
            if got == 0:
                return False
            pos += got
        return True

    def _record_error(self, error: BaseException) -> None:
        with self._cv:
            if not self._closed:
                self._errors.append(error)
                self._cv.notify_all()

    def _register(self, peer: int, sock: socket.socket) -> None:
        self._tune(sock)
        with self._cv:
            if self._closed:
                sock.close()
                return
            if peer in self._sockets:
                sock.close()
                return
            self._sockets[peer] = sock
            self._send_locks[peer] = threading.Lock()
            thread = threading.Thread(
                target=self._recv_loop,
                args=(peer, sock),
                name=f"wbridge-tcp-ctl-recv-{peer}",
                daemon=True,
            )
            self._recv_threads[peer] = thread
            thread.start()
            self._cv.notify_all()

    def _accept_loop(self) -> None:
        while True:
            with self._cv:
                if self._closed:
                    return
            try:
                sock, _addr = self._listener.accept()
            except socket.timeout:
                continue
            except OSError as error:
                with self._cv:
                    if self._closed:
                        return
                self._record_error(error)
                return
            try:
                hello = bytearray(self._HELLO.size)
                if not self._recv_exact(sock, hello):
                    raise ConnectionError(
                        "TCP control peer closed during rank handshake"
                    )
                peer = int(self._HELLO.unpack(hello)[0])
                self._register(peer, sock)
            except BaseException as error:  # noqa: BLE001 - surfaced through check()
                sock.close()
                self._record_error(error)

    def _recv_loop(self, peer: int, sock: socket.socket) -> None:
        record = bytearray(self._MESSAGE.size)
        try:
            while self._recv_exact(sock, record):
                kind, seq = self._MESSAGE.unpack(record)
                self._on_message(int(kind), peer, int(seq))
            raise ConnectionError(f"TCP control peer {peer} closed")
        except BaseException as error:  # noqa: BLE001 - surfaced through check()
            self._record_error(error)

    def configure(
        self,
        peers: set[int] | list[int] | tuple[int, ...],
        endpoints: Mapping[int, tuple[str, int]],
        *,
        timeout_s: float = 120.0,
    ) -> None:
        """Connect lower ranks to higher ranks, then wait for every pair.

        Every listener is bound before endpoint metadata is gathered.  Choosing
        exactly one initiator per unordered rank pair avoids duplicate sockets
        while still allowing both directions to publish over the connection.
        """
        expected = {int(peer) for peer in peers}
        with self._cv:
            self._expected = expected
        deadline = time.monotonic() + timeout_s
        for peer in sorted(p for p in expected if self.rank < p):
            endpoint = endpoints.get(peer)
            if endpoint is None:
                raise RuntimeError(
                    f"peer {peer} did not publish a TCP control endpoint"
                )
            while True:
                self.check()
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._tune(sock)
                try:
                    # Explicit source binding keeps this channel on the host-network
                    # interface even on nodes exposing many fabric-facing addresses.
                    sock.bind((self.host, 0))
                    sock.settimeout(min(1.0, max(0.05, deadline - time.monotonic())))
                    sock.connect((str(endpoint[0]), int(endpoint[1])))
                    sock.settimeout(None)
                    sock.sendall(self._HELLO.pack(self.rank))
                    self._register(peer, sock)
                    break
                except OSError:
                    sock.close()
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"rank {self.rank}: timed out connecting TCP control peer {peer} at {endpoint}"
                        )
                    time.sleep(0.01)

        with self._cv:
            while not expected <= self._sockets.keys() and not self._errors:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    missing = sorted(expected - self._sockets.keys())
                    raise TimeoutError(
                        f"rank {self.rank}: timed out awaiting TCP control peers {missing}"
                    )
                self._cv.wait(min(remaining, 0.2))
        self.check()

    @property
    def peers(self) -> set[int]:
        with self._cv:
            return set(self._expected)

    def has_peer(self, peer: int) -> bool:
        with self._cv:
            return peer in self._sockets

    def send(self, kind: int, peer: int, seq: int) -> None:
        """Publish one fixed-size record without a transport completion wait."""
        self.check()
        # 0..2 are the legacy ACK/READY/CONS channels. 3/4 carry replica-group relay DATA/ACK
        # tokens; their uint64 payload encodes both group id and the ordinary monotonic sequence.
        if not 0 <= kind <= 4:
            raise ValueError(f"invalid TCP control kind {kind}")
        with self._cv:
            sock = self._sockets.get(peer)
            lock = self._send_locks.get(peer)
        if sock is None or lock is None:
            raise RuntimeError(f"TCP control peer {peer} is not connected")
        record = self._MESSAGE.pack(kind, seq)
        try:
            # A lock is local to one peer.  It prevents byte interleaving when that
            # peer receives READY and CONS from different progress threads without
            # serializing unrelated destinations.
            with lock:
                sock.sendall(record)
        except OSError as error:
            self._record_error(error)
            raise RuntimeError(f"TCP control send to peer {peer} failed") from error

    def check(self) -> None:
        with self._cv:
            error = self._errors[0] if self._errors else None
        if error is not None:
            raise RuntimeError(
                f"rank {self.rank}: TCP control transport failed"
            ) from error

    def close(self) -> None:
        with self._cv:
            if self._closed:
                return
            self._closed = True
            sockets = list(self._sockets.values())
            threads = list(self._recv_threads.values())
            self._cv.notify_all()
        try:
            self._listener.close()
        except OSError:
            pass
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self._accept_thread.join(timeout=2.0)
        for thread in threads:
            thread.join(timeout=2.0)
