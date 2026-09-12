# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Loopback tests for the persistent host-network control transport."""

import threading
import time

from wbridge.backend.tcp_control import TcpControlTransport


def test_tcp_control_full_duplex_fixed_records():
    landed = {0: [], 1: []}
    cv = threading.Condition()

    def callback(rank):
        def receive(kind, peer, seq):
            with cv:
                landed[rank].append((kind, peer, seq))
                cv.notify_all()

        return receive

    left = TcpControlTransport(0, "127.0.0.1", callback(0))
    right = TcpControlTransport(1, "127.0.0.1", callback(1))
    try:
        # Rank 0 is the unique connector. Rank 1's accept loop is live before
        # configure(), so setup does not require an auxiliary controller thread.
        left.configure({1}, {1: right.endpoint}, timeout_s=2.0)
        right.configure({0}, {0: left.endpoint}, timeout_s=2.0)

        left.send(0, 1, 17)
        right.send(1, 0, 19)
        left.send(2, 1, 23)
        with cv:
            cv.wait_for(
                lambda: len(landed[0]) == 1 and len(landed[1]) == 2,
                timeout=2.0,
            )
        assert landed[0] == [(1, 1, 19)]
        assert landed[1] == [(0, 0, 17), (2, 0, 23)]
        left.check()
        right.check()
    finally:
        left.close()
        right.close()


def test_tcp_control_peers_do_not_share_send_lock():
    received = []
    cv = threading.Condition()

    def callback(kind, peer, seq):
        with cv:
            received.append((kind, peer, seq, time.perf_counter()))
            cv.notify_all()

    root = TcpControlTransport(0, "127.0.0.1", lambda *_args: None)
    one = TcpControlTransport(1, "127.0.0.1", callback)
    two = TcpControlTransport(2, "127.0.0.1", callback)
    try:
        root.configure({1, 2}, {1: one.endpoint, 2: two.endpoint}, timeout_s=2.0)
        one.configure({0}, {0: root.endpoint}, timeout_s=2.0)
        two.configure({0}, {0: root.endpoint}, timeout_s=2.0)

        a = threading.Thread(target=root.send, args=(0, 1, 31))
        b = threading.Thread(target=root.send, args=(0, 2, 37))
        a.start()
        b.start()
        a.join(timeout=2.0)
        b.join(timeout=2.0)
        with cv:
            cv.wait_for(lambda: len(received) == 2, timeout=2.0)
        assert {(kind, peer, seq) for kind, peer, seq, _t in received} == {
            (0, 0, 31),
            (0, 0, 37),
        }
    finally:
        root.close()
        one.close()
        two.close()
