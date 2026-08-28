# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import threading
import time

import torch
from wbridge.backend.receiver import WeightReceiver
from wbridge.backend.router import _relay_round_predecessors, WBEndpoint, WeightRouter
from wbridge.utils.data import ShardSpec


def _spec(**entries):
    return ShardSpec(
        {
            name: [[(left, right, width), (0, 4, 4)]]
            for name, (left, right, width) in entries.items()
        }
    )


def test_relay_selects_one_owner_per_node_and_preserves_full_group(monkeypatch):
    monkeypatch.setenv("WBRIDGE_DEDUP_PAIR_BYTES", "0")
    senders = [
        _spec(weight=(0, 4, 8)),
        _spec(weight=(4, 8, 8)),
    ]
    receivers = [_spec(weight=(0, 8, 8)) for _ in range(4)]
    router = WeightRouter(
        0,
        2,
        senders + receivers,
        {"weight": torch.float32},
        global_rounds=[{"weight"}],
    )
    placement = {
        0: "trainer",
        1: "trainer",
        2: "node-a",
        3: "node-a",
        4: "node-b",
        5: "node-b",
    }

    assert router.configure_relay(placement)
    assert len(router._relay_groups) == 1
    group = router.relay_group(0)
    assert group["members"] == (0, 1, 2, 3)
    assert group["owners"] == (0, 2)
    assert group["chain"] == (2, 4)
    assert group["owner_of"] == {0: 0, 1: 0, 2: 2, 3: 2}
    assert group["local_readers"] == {0: (0, 1), 2: (2, 3)}
    assert group["round_specs"][0].nbytes(router.dtype_spec) == 8 * 4 * 4
    assert {
        rank: spec.nbytes(router.dtype_spec)
        for rank, spec in group["trainer_specs"][0].items()
    } == {0: 4 * 4 * 4, 1: 4 * 4 * 4}


def test_relay_coalesces_names_by_replica_membership(monkeypatch):
    monkeypatch.setenv("WBRIDGE_DEDUP_PAIR_BYTES", "0")
    sender = _spec(a=(0, 4, 4), b=(0, 4, 4))
    receivers = [
        _spec(a=(0, 4, 4), b=(0, 4, 4)),
        _spec(a=(0, 4, 4)),
        _spec(a=(0, 4, 4), b=(0, 4, 4)),
        _spec(a=(0, 4, 4)),
    ]
    router = WeightRouter(
        0,
        1,
        [sender] + receivers,
        {"a": torch.float32, "b": torch.float32},
        global_rounds=[{"a"}, {"b"}],
    )
    placement = {0: "trainer", 1: "node-a", 2: "node-a", 3: "node-b", 4: "node-b"}

    assert router.configure_relay(placement)
    by_members = {group["members"]: group for group in router._relay_groups}
    assert set(by_members) == {(0, 1, 2, 3), (0, 2)}
    assert by_members[(0, 1, 2, 3)]["names"] == ("a",)
    assert by_members[(0, 2)]["names"] == ("b",)
    assert by_members[(0, 2)]["chain"] == (1, 3)
    assert not by_members[(0, 2)]["round_specs"][0].entries
    assert by_members[(0, 2)]["round_specs"][1].entries


def test_relay_inf_piggybacks_wide_group_onto_existing_replica_groups(monkeypatch):
    monkeypatch.setenv("WBRIDGE_DEDUP_PAIR_BYTES", "inf")
    sender = _spec(a=(0, 4, 4), b=(0, 4, 4), c=(0, 4, 4))
    receivers = [
        _spec(a=(0, 4, 4), b=(0, 4, 4)),
        _spec(a=(0, 4, 4), c=(0, 4, 4)),
        _spec(a=(0, 4, 4), b=(0, 4, 4)),
        _spec(a=(0, 4, 4), c=(0, 4, 4)),
    ]
    router = WeightRouter(
        0,
        1,
        [sender] + receivers,
        {"a": torch.float32, "b": torch.float32, "c": torch.float32},
        global_rounds=[{"a", "b", "c"}],
    )
    placement = {
        0: "trainer",
        1: "node-a",
        2: "node-a",
        3: "node-b",
        4: "node-b",
    }

    assert router.configure_relay(placement)
    by_members = {group["members"]: group for group in router._relay_groups}
    assert set(by_members) == {(0, 2), (1, 3)}
    assert set(by_members[(0, 2)]["names"]) == {"a", "b"}
    assert set(by_members[(1, 3)]["names"]) == {"a", "c"}


def test_relay_control_token_roundtrip_and_slot():
    endpoint = WBEndpoint.__new__(WBEndpoint)
    endpoint._relay_num_groups = 9
    endpoint.num_rounds = 16
    endpoint.world_size = 24

    token = endpoint._encode_relay_token(7, 12345)
    assert endpoint._decode_relay_token(token) == (7, 12345)
    assert endpoint._relay_flag_slot(11, 7, 12345) == (
        (11 * 9 + 7) * 16 + (12345 - 1) % 16
    )


def test_relay_round_dependencies_order_operations_and_reuse_exact_parity():
    assert _relay_round_predecessors([0, 1, 2, 4, 5, 8], depth=2) == {
        0: (None, None),
        1: (0, None),
        2: (1, 0),
        4: (2, 2),
        5: (4, 1),
        8: (5, 4),
    }


def test_relay_rollout_rdma_bytes_matches_depth2_owner_layout(monkeypatch):
    monkeypatch.setenv("WBRIDGE_DEDUP_PAIR_BYTES", "0")
    senders = [
        _spec(weight=(0, 4, 8)),
        _spec(weight=(4, 8, 8)),
    ]
    receivers = [_spec(weight=(0, 8, 8)) for _ in range(4)]
    router = WeightRouter(
        0,
        2,
        senders + receivers,
        {"weight": torch.float32},
        global_rounds=[{"weight"}],
    )
    placement = {
        0: "trainer",
        1: "trainer",
        2: "node-a",
        3: "node-a",
        4: "node-b",
        5: "node-b",
    }

    # One 128-byte canonical payload. Each node owner now registers only depth-2 PREP: 128+1 bytes because
    # the unused parity retains the implementation's one-byte allocation. DOFF is non-RDMA. Every rollout
    # worker registers four 6-rank x 1-group x 1-round int64 control banks (192 bytes).
    assert router.relay_rollout_rdma_bytes(placement) == [321, 192, 321, 192]


def test_relay_downstream_engine_bootstraps_from_predecessor_data():
    spec = _spec(weight=(0, 8, 8))
    group = {
        "chain": (8, 16),
        "round_specs": [spec, ShardSpec({})],
        "trainer_specs": [{0: spec}, {}],
    }

    class _Router:
        @staticmethod
        def relay_group(gid):
            assert gid == 3
            return group

    receiver = WeightReceiver.__new__(WeightReceiver)
    receiver._relay_enabled = True
    receiver._relay_owned_gids = (3,)
    receiver.router = _Router()
    receiver._rank = 16
    receiver.num_rounds = 2
    observed = []

    def reached(kind, peer, gid, seq):
        observed.append((kind, peer, gid, seq))
        return True

    receiver._relay_flag_reached = reached

    assert receiver._sender_input_ready(epoch=4)
    assert observed == [(receiver._RELAY_DATA_KIND, 8, 3, 9)]


def test_relay_local_exit_ignores_downstream_and_other_local_readers():
    owner_states = {
        (0, 0): {
            "relay_done": True,
            "local_consumed": False,
            "prep_free": False,
            "upstream_done": False,
        },
    }
    consume_states = {(0, 0): {"done": True}}

    assert WeightReceiver._relay_local_exit_ready(owner_states, consume_states)
    owner_states[(0, 0)]["relay_done"] = False
    assert not WeightReceiver._relay_local_exit_ready(owner_states, consume_states)
    owner_states[(0, 0)]["relay_done"] = True
    consume_states[(0, 0)]["done"] = False
    assert not WeightReceiver._relay_local_exit_ready(owner_states, consume_states)


def test_relay_retirement_polls_tasks_independently_and_preserves_delivery():
    receiver = WeightReceiver.__new__(WeightReceiver)
    receiver._rank = 10
    receiver._relay_local_readers = {}
    receiver._relay_local_channel = {}
    receiver._relay_local_slot_of = {}
    receiver._relay_retire_lock = threading.Lock()
    receiver._relay_retire_q = None
    receiver._relay_retire_thread = None
    receiver._relay_retire_errors = []
    receiver._relay_prep_released = {}
    receiver._relay_doff_released = {}
    receiver._relay_doff_depth = 1

    ready = {(21, 1, 8)}
    observed = []
    observed_cv = threading.Condition()

    def reached(_kind, peer, gid, seq):
        return (peer, gid, seq) in ready

    def emit(kind, peer, gid, seq):
        with observed_cv:
            observed.append((kind, peer, gid, seq))
            observed_cv.notify_all()

    receiver._relay_flag_reached = reached
    receiver._relay_emit = emit
    receiver._trace_state = lambda *_args, **_kwargs: None

    def wait_for(count):
        deadline = time.monotonic() + 2.0
        with observed_cv:
            while len(observed) < count:
                remaining = deadline - time.monotonic()
                assert remaining > 0
                observed_cv.wait(remaining)

    try:
        receiver._defer_relay_retirement(
            [
                {
                    "wt": 0,
                    "gid": 0,
                    "ri": 0,
                    "seq": 7,
                    "succ": 20,
                    "upstream_peers": (3,),
                    "doff_released": False,
                },
                {
                    "wt": 0,
                    "gid": 1,
                    "ri": 1,
                    "seq": 8,
                    "succ": 21,
                    "upstream_peers": (4, 5),
                    "doff_released": False,
                },
            ]
        )
        wait_for(2)
        assert observed == [
            (receiver._RELAY_DATA_KIND, 4, 1, 8),
            (receiver._RELAY_DATA_KIND, 5, 1, 8),
        ]
        assert receiver._relay_doff_was_released(0, 0, 7)
        assert receiver._relay_doff_was_released(1, 1, 8)

        ready.add((20, 0, 7))
        wait_for(3)
        assert observed[-1] == (receiver._RELAY_DATA_KIND, 3, 0, 7)
    finally:
        receiver._stop_relay_retire_worker()
