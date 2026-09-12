# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""CPU-only state-machine tests for the data-driven receiver KICK/GO protocol."""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

torch = pytest.importorskip("torch")

from wbridge.backend.control_channel import (  # noqa: E402
    EMPTY,
    RECEIVE_KICK,
    RECEIVE_REQUEST,
)
from wbridge.backend.receiver import WeightReceiver  # noqa: E402
from wbridge.backend.sender import _receiver_rank_has_input  # noqa: E402
from wbridge.utils.data import ShardSpec  # noqa: E402


def _receiver(round_peers: list[list[int]]) -> WeightReceiver:
    r = WeightReceiver.__new__(WeightReceiver)
    r.router = SimpleNamespace(
        local_rounds=[(None, {p: object() for p in peers}) for peers in round_peers]
    )
    r.num_rounds = len(round_peers)
    r._epoch = 0
    peers = sorted({p for ps in round_peers for p in ps})
    r.flag_slot_of = {p: i for i, p in enumerate(peers)}
    r._flag_buf = torch.zeros(max(len(peers), 1) * r.num_rounds, dtype=torch.int64)
    r._connected = True
    r._rs_cv = threading.Condition()
    r._rs_err = None
    r._rs_kicked = -1
    r._rs_staged = -1
    r._rs_gpu_ready = -1
    return r


def test_first_active_input_requires_every_contributing_sender():
    r = _receiver([[], [3, 5]])
    assert r._first_active_round() == 1
    assert not r._sender_input_ready(0)

    # epoch 0, global round 1 => sequence 2. One contributor is not sufficient.
    slot3, _ = r._flag_message_slot(r.flag_slot_of[3], 2)
    slot5, _ = r._flag_message_slot(r.flag_slot_of[5], 2)
    r._flag_buf[slot3] = 2
    assert not r._sender_input_ready(0)
    r._flag_buf[slot5] = 2
    assert r._sender_input_ready(0)

    # A prior epoch's sequence cannot wake the next update (epoch 1 wants sequence 4).
    assert not r._sender_input_ready(1)
    r._flag_buf[:] = 4
    assert r._sender_input_ready(1)


def test_direct_rank0_go_is_driven_by_gpu_ingress_flag():
    r = _receiver([[2]])
    assert r._rank0_decision(None, staged=False)["type"] == EMPTY
    slot, _ = r._flag_message_slot(r.flag_slot_of[2], 1)
    r._flag_buf[slot] = 1
    decision = r._rank0_decision(None, staged=False)
    assert decision == {"type": RECEIVE_REQUEST, "epoch": 0}


def test_rs_cpu_flag_kicks_then_local_gpu_flag_goes():
    r = _receiver([[2]])
    assert r._rank0_decision(None, staged=True)["type"] == EMPTY

    # Trainer->CPU data-before-flag starts the background receive, but is not GO.
    slot, _ = r._flag_message_slot(r.flag_slot_of[2], 1)
    r._flag_buf[slot] = 1
    assert r._rank0_decision(None, staged=True) == {"type": RECEIVE_KICK, "epoch": 0}
    assert r._rank0_decision(None, staged=True)["type"] == EMPTY

    # Only the completed CPU->GPU first-round flag authorizes rank 0's GO.
    r._rs_staged = 0
    assert r._rank0_decision(None, staged=True)["type"] == EMPTY
    r._rs_gpu_ready = 0
    assert r._rank0_decision(None, staged=True) == {"type": RECEIVE_REQUEST, "epoch": 0}


def test_zero_input_rank_accepts_legacy_doorbell_only_as_fallback():
    r = _receiver([[], []])
    fallback = {"type": RECEIVE_REQUEST}
    assert r._rank0_decision(None, staged=False)["type"] == EMPTY
    assert r._rank0_decision(fallback, staged=False) == {
        "type": RECEIVE_REQUEST,
        "epoch": 0,
    }
    assert r._rank0_decision(fallback, staged=True) == {
        "type": RECEIVE_KICK,
        "epoch": 0,
    }


def test_rs_worker_finishes_all_cpu_receive_before_first_h2d():
    r = _receiver([[2]])
    r._rank = 9
    r._stop = False
    r._rs_go = 0
    order: list[str] = []

    def receive_all(epoch: int) -> None:
        assert epoch == 0
        order.append("cpu_all")

    def stage_first(epoch: int) -> None:
        assert epoch == 0
        assert order == ["cpu_all"]
        order.append("first_h2d")
        r._stop = True

    r._receive_to_cpu = receive_all
    r._rs_stage_first_to_gpu = stage_first
    r._trace_state = lambda *_args, **_kwargs: None

    th = threading.Thread(target=r._recv_worker)
    th.start()
    th.join(timeout=5)
    assert not th.is_alive()
    assert order == ["cpu_all", "first_h2d"]
    assert r._rs_staged == 0
    assert r._rs_gpu_ready == 0


def test_zero_input_fallback_classification_uses_deduplicated_specs():
    src = ShardSpec({"a": [[(0, 8, 8)]]})
    hit = ShardSpec({"a": [[(2, 4, 8)]]})
    miss = ShardSpec({"b": [[(0, 1, 1)]]})
    router = SimpleNamespace(send_specs=[src], recv_specs=[hit, miss])
    assert _receiver_rank_has_input(router, 0)
    assert not _receiver_rank_has_input(router, 1)
