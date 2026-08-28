# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""CPU-only coverage for endpoint-lifetime topology completion waiters."""

import queue
import time

from wbridge.backend.receiver import WeightReceiver


class _FakeEngine:
    def __init__(self):
        self.waited = []

    def wait(self, bids):
        self.waited.extend(bids)


def test_topology_outbound_waiter_is_reused_and_preserves_peer_generation_order():
    receiver = WeightReceiver.__new__(WeightReceiver)
    receiver._rank = 8
    receiver.engine = _FakeEngine()
    receiver._trace_state = lambda *_args, **_kwargs: None
    flags = []
    receiver._flag_emit = lambda kind, peer, seq: flags.append((kind, peer, seq))

    peer = 16
    receiver._ensure_topo_out_waiters({peer})
    thread = receiver._topo_out_wait_threads[peer]
    results = queue.Queue()
    for ri, seq, bid in ((0, 11, "round-0"), (1, 12, "round-1")):
        receiver._topo_out_wait_queues[peer].put(
            (3, ri, seq, bid, time.time(), results)
        )

    assert results.get(timeout=1) == (0, peer, None)
    assert results.get(timeout=1) == (1, peer, None)
    assert receiver.engine.waited == ["round-0", "round-1"]
    assert flags == [(1, peer, 11), (1, peer, 12)]

    receiver._ensure_topo_out_waiters({peer})
    assert receiver._topo_out_wait_threads[peer] is thread
    receiver._topo_out_wait_queues[peer].put(None)
    thread.join(timeout=1)
    assert not thread.is_alive()
