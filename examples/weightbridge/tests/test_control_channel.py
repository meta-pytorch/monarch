# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for the rank0-mediated ZMQ control star (:class:`ControlChannel`).

Torch/GPU-free: exercises the per-round rendezvous directly. Skipped where ``zmq`` is unavailable.
Also runnable as a script (``python tests/test_control_channel.py``) for reliable multiprocessing spawn.
"""

import json
import multiprocessing as mp
import tempfile
import time

import pytest

pytest.importorskip("zmq")
import zmq  # noqa: E402
from wbridge.backend.control_channel import (  # noqa: E402
    CONNECT_REQUEST,
    ControlChannel,
    EMPTY,
    RECEIVE_REQUEST,
    STARTUP_REGISTER_TIMEOUT_ENV,
    startup_register_timeout_s,
    WORKER_REGISTER,
)

N_ROUNDS = 8


def test_rank0_uses_shared_startup_registration_timeout(monkeypatch):
    """The launcher timeout covers the peer hub as well as the coordinator."""
    monkeypatch.setenv(STARTUP_REGISTER_TIMEOUT_ENV, "14400")
    observed = []
    monkeypatch.setattr(
        ControlChannel,
        "_await_registration",
        lambda self, timeout_s: observed.append(timeout_s),
    )

    ch0 = ControlChannel(_new_ipc(), 0, 2)
    ch0.close()

    assert startup_register_timeout_s() == 14400.0
    assert observed == [14400.0]


def _new_ipc() -> str:
    return f"ipc://{tempfile.NamedTemporaryFile(delete=False).name}"


def _controller(ctx, ipc):
    """A mock WeightReceiverController ROUTER that refuses to silently drop (ROUTER_MANDATORY)."""
    sock = ctx.socket(zmq.ROUTER)
    sock.setsockopt(zmq.ROUTER_MANDATORY, 1)
    sock.bind(ipc)
    return sock


def _send_routable(ctrl, ident: bytes, obj: dict, tries: int = 300) -> None:
    """Send to ``ident``, retrying until the peer's DEALER is connected (handshake done)."""
    data = json.dumps(obj).encode()
    for _ in range(tries):
        try:
            ctrl.send_multipart([ident, data])
            return
        except zmq.error.ZMQError:
            time.sleep(0.02)
    raise RuntimeError(f"could not route to {ident!r}: peer never connected")


def _recv_register(ctrl, timeout_ms: int = 5000) -> int:
    """Consume receiver rank 0's WORKER_REGISTER (sent at :class:`ControlChannel` init); return num_workers."""
    ctrl.setsockopt(zmq.RCVTIMEO, timeout_ms)
    msg = json.loads(ctrl.recv_multipart()[1].decode())
    assert msg["type"] == WORKER_REGISTER, f"expected WORKER_REGISTER first, got {msg}"
    return int(msg["num_workers"])


def _drain_acks(ctrl, timeout_ms: int = 1000) -> int:
    ctrl.setsockopt(zmq.RCVTIMEO, timeout_ms)
    n = 0
    while True:
        try:
            frames = ctrl.recv_multipart()
        except zmq.Again:
            return n
        assert json.loads(frames[1].decode())["status"] == "ack"
        n += 1


def _peer_proc(ipc: str, rank: int, num_workers: int, n_rounds: int, out_q) -> None:
    ch = ControlChannel(ipc, rank, num_workers)
    seq = [ch.step().get("type") for _ in range(n_rounds)]
    ch.close()
    out_q.put((rank, seq))


def test_star_lockstep_and_ordering():
    """All ranks observe the identical decision sequence; connect precedes receive; one ack per msg."""
    ipc = _new_ipc()
    num_workers = 4
    ctx = zmq.Context()
    ctrl = _controller(ctx, ipc)

    spawn = mp.get_context(
        "spawn"
    )  # avoid inheriting the parent's zmq context via fork
    out_q = spawn.Queue()
    peers = [
        spawn.Process(target=_peer_proc, args=(ipc, r, num_workers, N_ROUNDS, out_q))
        for r in range(1, num_workers)
    ]
    for p in peers:
        p.start()

    # rank0 lives here; its ctor blocks until all peers register (also lets its controller
    # DEALER finish the handshake, so the ROUTER can route to it).
    ch0 = ControlChannel(ipc, 0, num_workers)
    assert (
        _recv_register(ctrl) == num_workers
    )  # rank 0 reports its worker count to the coordinator at init

    inject = {
        1: {
            "type": CONNECT_REQUEST,
            "rank": 8,
            "world_size": 12,
            "sender_world_size": 8,
            "init_method": "tcp://x:1",
            "group_name": "wbridge",
        },
        4: {"type": RECEIVE_REQUEST},
    }
    r0_seq = []
    for i in range(N_ROUNDS):
        if i in inject:
            _send_routable(ctrl, b"worker-0", inject[i])
            time.sleep(
                0.05
            )  # let it reach rank0's DEALER before its non-blocking drain
        r0_seq.append(ch0.step().get("type"))
    ch0.close()

    assert _drain_acks(ctrl) == len(inject)

    results = {}
    for _ in peers:
        rank, seq = out_q.get(timeout=15)
        results[rank] = seq
    for p in peers:
        p.join(timeout=10)

    for rank, seq in results.items():
        assert seq == r0_seq, f"rank {rank} desynced: {seq} != {r0_seq}"
    assert CONNECT_REQUEST in r0_seq and RECEIVE_REQUEST in r0_seq
    assert r0_seq.index(CONNECT_REQUEST) < r0_seq.index(RECEIVE_REQUEST)

    ctrl.close(linger=0)
    ctx.term()


def test_single_worker_no_peers():
    """num_workers=1: rank0 alone, no registration wait, decisions still flow from the controller."""
    ipc = _new_ipc()
    ctx = zmq.Context()
    ctrl = _controller(ctx, ipc)

    ch0 = ControlChannel(ipc, 0, 1)
    assert (
        _recv_register(ctrl) == 1
    )  # rank 0 reports its worker count to the coordinator at init
    assert ch0.step().get("type") == EMPTY  # nothing queued -> empty

    _send_routable(ctrl, b"worker-0", {"type": RECEIVE_REQUEST})
    got = EMPTY
    for _ in range(
        50
    ):  # drain rounds until the receive decision lands (delivery is async)
        got = ch0.step().get("type")
        if got == RECEIVE_REQUEST:
            break
        time.sleep(0.02)
    assert got == RECEIVE_REQUEST
    assert _drain_acks(ctrl) == 1

    ch0.close()
    ctrl.close(linger=0)
    ctx.term()


if __name__ == "__main__":
    # Standalone runner (reliable multiprocessing 'spawn' without pytest collection quirks).
    test_star_lockstep_and_ordering()
    print("PASS test_star_lockstep_and_ordering")
    test_single_worker_no_peers()
    print("PASS test_single_worker_no_peers")
    print("ALL RENDEZVOUS TESTS PASSED")
