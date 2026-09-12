# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Offline unit tests for the tensor-dedup split ingress/exchange layout planner.

Pure, no-GPU accounting: exercises the per-tensor receiver-dedup primitives and the arena LAYOUT PLANNER
(:meth:`WeightRouter.arena_layout`) — stable per-sender RECV lanes, parity-slotted PREP offsets, and the shared
GRECV bank — that the runtime consumes to allocate isolated trainer-ingress and rollout-exchange buffers.
Only ``torch`` (for dtype item sizes) is needed, not CUDA.

Run: ``python tests/test_arena_planner.py`` or ``pytest tests/test_arena_planner.py``.
"""

import os

import pytest

torch = pytest.importorskip("torch")

from wbridge.backend import router as R  # noqa: E402
from wbridge.backend.receiver import _doff_source_predecessors  # noqa: E402
from wbridge.backend.router import (  # noqa: E402
    _arena_peer_predecessors,
    _arena_recv_total_bytes,
    _arena_slot_offset,
    _arena_slot_predecessors,
    _arena_total_bytes,
    _canonical_dtype_spec,
    _dedup_specs,
    _doff_arena_layout,
    _even_round_soft_target,
    _ipc_event_readers,
    _merge_recv_prep_layout,
    _packed_copy_spans,
    _shards_canonical_key,
    WBEndpoint,
    WeightRouter,
)
from wbridge.backend.sender import _recv_lane_predecessors  # noqa: E402
from wbridge.utils.data import (  # noqa: E402
    LoadSpec,
    shard_shape,
    shards_numel,
    ShardSpec,
    split_shard_evenly,
)

F32 = torch.float32


# --------------------------------------------------------------------------- helpers
def d1(l: int, r: int, n: int):
    """One 1-D shard covering ``[l, r)`` of a size-``n`` axis (single-shard :class:`Shards`)."""
    return [[(l, r, n)]]


def dtypes(*names):
    return {n: F32 for n in names}


def make_router(send_specs, recv_specs, dtype_spec, *, cap, rank=0):
    """Construct a pure :class:`WeightRouter` with a forced round cap (per-tensor recv dedup is always on).

    ``RECEIVER_ROUND_CAP_BYTES`` (module global) is read during ``__init__``; set it before construction and
    restore after (the router caches ``global_rounds``, so a post-construction restore is safe). These tests
    assert the natural per-tensor classes, so they explicitly disable the production-default consolidation.
    No CUDA / dist is touched."""
    old_cap = R.RECEIVER_ROUND_CAP_BYTES
    old_pair = os.environ.get("WBRIDGE_DEDUP_PAIR_BYTES")
    R.RECEIVER_ROUND_CAP_BYTES = cap
    os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = "0"
    try:
        return WeightRouter(
            rank, len(send_specs), list(send_specs) + list(recv_specs), dtype_spec
        )
    finally:
        R.RECEIVER_ROUND_CAP_BYTES = old_cap
        if old_pair is None:
            os.environ.pop("WBRIDGE_DEDUP_PAIR_BYTES", None)
        else:
            os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = old_pair


def _expected_split_axis(shape, k):
    """Mirror :func:`split_shard_evenly`'s axis choice: outermost axis with extent >= k, else the longest."""
    return next(
        (i for i in range(len(shape)) if shape[i] >= k),
        max(range(len(shape)), key=lambda i: shape[i]),
    )


def _disjoint(regions):
    """True iff a list of ``(off, nb)`` byte regions are pairwise disjoint (empty regions ignored)."""
    ivs = sorted((o, o + n) for o, n in regions if n > 0)
    return all(ivs[i][1] <= ivs[i + 1][0] for i in range(len(ivs) - 1))


def _unique(regions):
    """Physical regions in first-seen order (multiple peer send entries may intentionally alias)."""
    return list(dict.fromkeys(regions))


def _bind_fake_topology_endpoint(rt, rl, ip, *, depth=2):
    """Resolve one pure router plan against fake CPU/remote addresses (no CUDA, dist, or RDMA)."""
    sw = rt.sender_ws
    rank = sw + rl
    layout, stride, peers = rt.arena_layout(rl, depth=depth)
    endpoint = WBEndpoint.__new__(WBEndpoint)
    endpoint.router = rt
    endpoint.dtype_spec = rt.dtype_spec
    endpoint._topo_structure_ok = True
    endpoint._recv_depth = depth
    endpoint._arena_layout = layout
    endpoint._arena_S = stride
    endpoint._doff_depth = 1
    endpoint._doff_layout, _doff_bytes, endpoint._doff_S = _doff_arena_layout(
        layout,
        stride,
        endpoint._doff_depth,
    )
    endpoint._arena = torch.zeros(
        max(_arena_total_bytes(layout, depth, stride), 1), dtype=torch.uint8
    )
    endpoint._repl_peers = [sw + p for p in peers]
    endpoint._repl_same_node = {sw + p for p in peers if ip[sw + p] == ip[rank]}
    endpoint._peer_ip = ip
    endpoint._local_ip = ip[rank]
    endpoint._arena_peer_dst = {}
    for p in peers:
        peer_layout, _peer_stride, _ = rt.arena_layout(p, depth=depth)
        remote_base = 10_000_000 * (p + 1)
        endpoint._arena_peer_dst[sw + p] = [
            remote_base + rd["grecv"][rl][0] if rl in rd["grecv"] else None
            for rd in peer_layout
        ]
    assert endpoint._resolve_topo_exchange(
        rank=rank,
        sw=sw,
        rl=rl,
        nr_rounds=len(rt.global_rounds),
    )
    return endpoint, layout, stride, peers


def test_depth_buffered_arena_slot_offset():
    """Odd rounds must select slot 1 at depth 2, then wrap without changing the region-relative offset."""
    stride, off = 4096, 137
    assert [_arena_slot_offset(off, ri, 2, stride) for ri in range(6)] == [
        off,
        stride + off,
        off,
        stride + off,
        off,
        stride + off,
    ]
    assert [_arena_slot_offset(off, ri, 1, stride) for ri in range(4)] == [off] * 4


def test_cuda_ipc_events_are_scoped_to_same_node_readers():
    """A 256-rank topology must not materialize every slot event for all 255 replica peers."""
    rank = 136
    peers = [peer for peer in range(256) if peer != rank]
    peer_ip = {peer: f"node-{peer // 8}" for peer in range(256)}
    readers = _ipc_event_readers(peers, peer_ip, peer_ip[rank], enabled=True)

    assert readers == [137, 138, 139, 140, 141, 142, 143]
    assert len(readers) * (1 + 32) == 231
    assert len(peers) * (1 + 32) == 8415
    assert _ipc_event_readers(peers, peer_ip, peer_ip[rank], enabled=False) == []


def test_doff_layout_is_fixed_depth_and_source_exclusive():
    rounds = [
        {"grecv": {2: (1000, 7), 5: (2000, 31)}},
        {"grecv": {2: (3000, 19), 5: (4000, 11)}},
        {"grecv": {2: (5000, 3)}},
    ]
    layout, total, stride = _doff_arena_layout(rounds, prep_stride=101, depth=1)
    assert stride == total == 101 + 19 + 31
    assert {rd["own"] for rd in layout} == {(0, 101)}
    # Source 2 owns its max-19-byte region; source 5 starts after it and can never overlap despite different
    # per-round payload lengths.
    assert [rd["grecv"].get(2) for rd in layout] == [(101, 7), (101, 19), (101, 3)]
    assert [rd["grecv"].get(5) for rd in layout] == [(120, 31), (120, 11), None]

    depth2, total2, stride2 = _doff_arena_layout(rounds, prep_stride=101, depth=2)
    assert stride2 == stride and total2 == 2 * stride
    assert depth2[0]["grecv"][2][0] == depth2[2]["grecv"][2][0]
    assert depth2[1]["grecv"][2][0] == stride + 101


def test_depth_buffered_slot_predecessor_uses_global_round_parity():
    rounds = [0, 2, 3, 6]
    assert _arena_slot_predecessors(rounds, 2) == {0: None, 2: 0, 3: None, 6: 2}
    assert _arena_slot_predecessors(rounds, 1) == {0: None, 2: 0, 3: 2, 6: 3}


def test_doff_source_predecessors_are_per_source_and_slot():
    ext_recv = [(8,), (9,), (8, 9), (), (8,)]
    pred = _doff_source_predecessors([0, 1, 2, 4], ext_recv, depth=2, rank=7)

    assert pred == [
        {7: (-1, 4), 8: (-1, 4)},
        {7: (-1, 1), 9: (-1, 1)},
        {7: (0, 0), 8: (0, 0), 9: (-1, 2)},
        {},
        {7: (0, 2), 8: (0, 2)},
    ]


def test_merged_recv_prep_layout_relocates_only_absolute_grecv():
    rounds = [
        {"recv_stride": 120, "own": (7, 80), "grecv": {3: (200, 20)}},
        {"recv_stride": 120, "own": (9, 70), "grecv": {4: (220, 30)}},
    ]
    # Original PREP bank is [0, 2*100); merged bank is [0, 2*120), so GRECV shifts by 40.
    assert _merge_recv_prep_layout(rounds, depth=2, prep_stride=100) == 120
    assert [rd["own"] for rd in rounds] == [(7, 80), (9, 70)]
    assert rounds[0]["grecv"] == {3: (240, 20)}
    assert rounds[1]["grecv"] == {4: (260, 30)}
    assert _arena_total_bytes(rounds, 2, 120) == 290


def test_grecv_predecessor_is_per_peer_and_parity():
    rounds = [
        {"send": {1: (10, 4), 2: (20, 8)}},
        {"send": {2: (30, 8)}},
        {"send": {1: (40, 4), 3: (50, 0)}},
        {"send": {2: (60, 8), 3: (70, 2)}},
    ]
    assert _arena_peer_predecessors(rounds) == [
        {1: (-1, 2), 2: (-1, 3)},
        {2: (0, 0)},
        {1: (0, 0)},
        {2: (0, 1), 3: (-1, 3)},
    ]
    assert _arena_peer_predecessors(rounds, depth=2) == [
        {1: (-1, 2), 2: (-1, 0)},
        {2: (-1, 3)},
        {1: (0, 0)},
        {2: (0, 1), 3: (-1, 3)},
    ]


def test_even_round_soft_headroom_avoids_runt_round():
    """An exact-average soft gate can strand a hard-cap-feasible tensor in an extra round."""
    rt = object.__new__(WeightRouter)
    rt.sender_ws = rt.receiver_ws = 2
    names = [f"T{i}" for i in range(5)]
    rt.dtype_spec = {n: F32 for n in names}
    name_send = dict(zip(names, ([7, 17], [6, 27], [7, 32], [16, 25], [27, 8])))
    name_recv = dict(zip(names, ([19, 5], [31, 2], [32, 7], [12, 29], [1, 34])))
    hard = 100
    inf = float("inf")

    minimum = rt._pack_rounds(name_send, name_recv, hard, hard, [inf, inf], [inf, inf])
    assert len(minimum) == 2
    send_tot = [sum(name_send[n][i] for n in names) for i in range(2)]
    recv_tot = [sum(name_recv[n][i] for n in names) for i in range(2)]
    exact_send = [-(-total // 2) for total in send_tot]
    exact_recv = [-(-total // 2) for total in recv_tot]
    assert (
        len(rt._pack_rounds(name_send, name_recv, hard, hard, exact_send, exact_recv))
        == 3
    )

    soft_send = [_even_round_soft_target(total, 2, hard) for total in send_tot]
    soft_recv = [_even_round_soft_target(total, 2, hard) for total in recv_tot]
    balanced = rt._pack_rounds(name_send, name_recv, hard, hard, soft_send, soft_recv)
    assert len(balanced) == 2
    for round_names in balanced:
        assert max(sum(name_send[n][i] for n in round_names) for i in range(2)) <= hard
        assert max(sum(name_recv[n][i] for n in round_names) for i in range(2)) <= hard


def test_explicit_round_mode_produces_exact_balanced_count(monkeypatch):
    names = tuple(f"w{i:02d}" for i in range(12))
    send = [ShardSpec({name: d1(0, 120, 120) for name in names})]
    recv = [ShardSpec({name: d1(0, 120, 120) for name in names}) for _ in range(2)]
    monkeypatch.setenv("WBRIDGE_NUM_ROUNDS", "3")
    monkeypatch.delenv("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", raising=False)

    rt = WeightRouter(0, len(send), send + recv, dtypes(*names))
    assert len(rt.global_rounds) == 3
    assert [len(round_names) for round_names in rt.global_rounds] == [4, 4, 4]
    assert set().union(*rt.global_rounds) == set(names)


def test_rollout_rdma_cap_binary_search_selects_smallest_fitting_round_count(
    monkeypatch,
):
    names = tuple(f"w{i:02d}" for i in range(12))
    send = [ShardSpec({name: d1(0, 120, 120) for name in names})]
    recv = [ShardSpec({name: d1(0, 120, 120) for name in names}) for _ in range(2)]
    specs = send + recv
    dtype = dtypes(*names)
    peer_ip = {0: "trainer", 1: "rollout-a", 2: "rollout-b"}
    monkeypatch.delenv("WBRIDGE_NUM_ROUNDS", raising=False)
    monkeypatch.delenv("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", raising=False)

    probe = WeightRouter(0, len(send), specs, dtype)
    name_send, name_recv = probe._name_rank_bytes()
    probe.global_rounds = probe._pack_exact_rounds(name_send, name_recv, 2)
    peak_two = max(probe.rollout_rdma_bytes(peer_ip))
    probe.global_rounds = probe._pack_exact_rounds(name_send, name_recv, 3)
    peak_three = max(probe.rollout_rdma_bytes(peer_ip))
    assert peak_three < peak_two

    monkeypatch.setenv("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", str(peak_three))
    capped = WeightRouter(0, len(send), specs, dtype, peer_ip=peer_ip)
    assert len(capped.global_rounds) == 3
    assert max(capped.rollout_rdma_bytes(peer_ip)) <= peak_three


def test_round_planner_modes_are_mutually_exclusive(monkeypatch):
    send = [ShardSpec({"w": d1(0, 16, 16)})]
    recv = [ShardSpec({"w": d1(0, 16, 16)})]
    monkeypatch.setenv("WBRIDGE_NUM_ROUNDS", "1")
    monkeypatch.setenv("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", "4096")
    with pytest.raises(ValueError, match="mutually exclusive"):
        WeightRouter(0, 1, send + recv, dtypes("w"), peer_ip={0: "t", 1: "r"})


def test_direct_same_node_routes_full_receiver_and_allocates_no_exchange_payload(
    monkeypatch,
):
    send = [
        ShardSpec({"w": d1(0, 16, 16)}),
        ShardSpec({"w": d1(0, 16, 16)}),
    ]
    recv = [
        ShardSpec({"w": d1(0, 16, 16)}),
        ShardSpec({"w": d1(0, 16, 16)}),
    ]
    monkeypatch.delenv("WBRIDGE_NUM_ROUNDS", raising=False)
    monkeypatch.setenv("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", "4096")
    peer_ip = {0: "node", 1: "node", 2: "node", 3: "node"}

    sender = WeightRouter(
        0,
        len(send),
        send + recv,
        dtypes("w"),
        peer_ip=peer_ip,
        direct_same_node=True,
    )
    receiver = WeightRouter(
        len(send),
        len(send),
        send + recv,
        dtypes("w"),
        peer_ip=peer_ip,
        direct_same_node=True,
    )

    assert len(sender.global_rounds) == 1
    assert sender.recv_specs == recv
    assert sender.recv_tensor_classes()["w"] == [[0], [1]]
    full, overlaps = receiver.local_rounds[0]
    assert full.nbytes(dtypes("w")) == 16 * torch.empty((), dtype=F32).element_size()
    assert sum(spec.nbytes(dtypes("w")) for spec in overlaps.values()) == full.nbytes(
        dtypes("w")
    )
    layout, stride, peers = receiver.arena_layout(0, depth=2)
    assert stride == 0 and peers == []
    assert _arena_recv_total_bytes(layout, 2) == 0
    assert _arena_total_bytes(layout, 2, stride) == 0


def test_name_rank_bytes_bitmap_matches_dense_reference(monkeypatch):
    """Bitmap name filtering must preserve exact per-rank bytes, including disjoint same-name shards."""
    names = ("left", "right", "shared", "absent")
    send = [
        ShardSpec({"left": d1(0, 4, 8), "shared": d1(0, 8, 8)}),
        ShardSpec(
            {
                "left": d1(4, 8, 8),
                "right": d1(4, 8, 8),
                "shared": d1(0, 8, 8),
            }
        ),
    ]
    recv = [
        ShardSpec({"left": d1(4, 8, 8), "shared": d1(0, 4, 8)}),
        ShardSpec({"right": d1(4, 8, 8), "shared": d1(4, 8, 8)}),
    ]
    monkeypatch.setenv("WBRIDGE_DEDUP_PAIR_BYTES", "0")
    router = WeightRouter(
        0, len(send), send + recv, dtypes(*names), global_rounds=[set(names)]
    )

    expected_send = {name: [0] * router.sender_ws for name in names}
    expected_recv = {name: [0] * router.receiver_ws for name in names}
    for si, send_spec in enumerate(router.send_specs):
        for ri, recv_spec in enumerate(router.recv_specs):
            overlap = ShardSpec.compute_overlap(send_spec, recv_spec)
            for name, shards in overlap.entries.items():
                nb = R.shards_nbytes(shards, router.dtype_spec[name])
                expected_send[name][si] += nb
                expected_recv[name][ri] += nb

    assert router._name_rank_bytes() == (expected_send, expected_recv)


def test_rollout_rdma_reuses_round_invariant_overlap_cache(monkeypatch):
    names = ("a", "b", "c", "d")
    send = [
        ShardSpec({name: d1(0, 16, 16) for name in names}),
        ShardSpec({name: d1(0, 16, 16) for name in names}),
    ]
    recv = [
        ShardSpec({name: d1(0, 16, 16) for name in names}),
        ShardSpec({name: d1(0, 16, 16) for name in names}),
    ]
    monkeypatch.setenv("WBRIDGE_DEDUP_PAIR_BYTES", "0")
    router = WeightRouter(
        0,
        len(send),
        send + recv,
        dtypes(*names),
        global_rounds=[set(names)],
    )
    peer_ip = {0: "trainer-a", 1: "trainer-b", 2: "rollout-a", 3: "rollout-b"}

    original = ShardSpec.compute_overlap
    overlap_calls = 0

    def counted_overlap(*args, **kwargs):
        nonlocal overlap_calls
        overlap_calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(ShardSpec, "compute_overlap", staticmethod(counted_overlap))
    router.rollout_rdma_bytes(peer_ip)
    first_probe_calls = overlap_calls
    assert first_probe_calls > 0

    router.global_rounds = [{"a", "b"}, {"c", "d"}]
    router.rollout_rdma_bytes(peer_ip)
    assert overlap_calls == first_probe_calls
    assert router._trainer_peer_counts_cache == [1, 1]


def test_independent_rank_planning_is_deterministic(monkeypatch):
    names = tuple(f"w{i:02d}" for i in range(12))
    send = [ShardSpec({name: d1(0, 120, 120) for name in reversed(names)})]
    recv = [
        ShardSpec({name: d1(0, 120, 120) for name in names}),
        ShardSpec({name: d1(0, 120, 120) for name in reversed(names)}),
    ]
    canonical = _canonical_dtype_spec(
        [
            {name: F32 for name in reversed(names)},
            {name: F32 for name in names},
        ]
    )
    assert list(canonical) == sorted(names)

    monkeypatch.setenv("WBRIDGE_DEDUP_PAIR_BYTES", "0")
    monkeypatch.delenv("WBRIDGE_NUM_ROUNDS", raising=False)
    monkeypatch.delenv("WBRIDGE_ROLLOUT_RDMA_CAP_BYTES", raising=False)
    old_cap = R.RECEIVER_ROUND_CAP_BYTES
    R.RECEIVER_ROUND_CAP_BYTES = 1600
    try:
        sender = WeightRouter(0, len(send), send + recv, dict(canonical))
        receiver = WeightRouter(2, len(send), send + recv, dict(canonical))
    finally:
        R.RECEIVER_ROUND_CAP_BYTES = old_cap

    assert [sorted(round_names) for round_names in sender.global_rounds] == [
        sorted(round_names) for round_names in receiver.global_rounds
    ]


# ------------------------------------------------------------------ 1. split_shard_evenly partition
def test_split_shard_evenly_partition():
    cases = [
        ([(0, 12, 12)], "1D exact"),
        ([(0, 10, 10)], "1D remainder"),
        ([(3, 11, 16)], "1D offset range"),
        ([(0, 4, 4), (0, 6, 6)], "2D split outer"),
        ([(0, 1, 1), (0, 6, 6)], "2D outer<k -> split axis1"),
        ([(0, 8, -8)], "1D transposed (negative w)"),
    ]
    for shard, label in cases:
        shape = shard_shape(shard)
        for k in (1, 2, 3, 8):
            subs = [split_shard_evenly(shard, k, j) for j in range(k)]
            present = [s for s in subs if s is not None]
            assert present, f"{label} k={k}: all sub-shards empty"
            if k == 1:
                assert present == [list(shard)], f"{label} k=1 not identity"
                continue
            ax = _expected_split_axis(shape, k)
            # w (incl. sign) preserved everywhere; non-split dims unchanged.
            for s in present:
                for dpos, (dim_s, dim_o) in enumerate(zip(s, shard)):
                    assert dim_s[2] == dim_o[2], (
                        f"{label} k={k}: w changed on dim {dpos}"
                    )
                    if dpos != ax:
                        assert (dim_s[0], dim_s[1]) == (dim_o[0], dim_o[1]), (
                            f"{label} k={k}: non-split dim {dpos} changed"
                        )
            # Split axis: contiguous, disjoint, covers [l, r) exactly.
            ivs = sorted((s[ax][0], s[ax][1]) for s in present)
            assert ivs[0][0] == shard[ax][0] and ivs[-1][1] == shard[ax][1], (
                f"{label} k={k}: coverage gap"
            )
            for i in range(len(ivs) - 1):
                assert ivs[i][1] == ivs[i + 1][0], (
                    f"{label} k={k}: not contiguous/disjoint"
                )
            # None only past the remainder (empty parts are the trailing j's).
            first_none = next((j for j, s in enumerate(subs) if s is None), k)
            assert all(s is None for s in subs[first_none:]), (
                f"{label} k={k}: a None precedes a present part"
            )


# ------------------------------------------------------------------ 2. _dedup_specs (per-tensor)
def test_dedup_specs_partition_and_determinism():
    # recv0,1 identical {A,B}; recv2,3 identical {A}.
    # Per-tensor: A held identically by ALL four -> class {0,1,2,3}; B held by {0,1}.
    specs = [
        ShardSpec({"A": d1(0, 100, 100), "B": d1(0, 40, 40)}),
        ShardSpec({"A": d1(0, 100, 100), "B": d1(0, 40, 40)}),
        ShardSpec({"A": d1(0, 100, 100)}),
        ShardSpec({"A": d1(0, 100, 100)}),
    ]

    ded = _dedup_specs(specs)
    # Determinism: identical output on a second call.
    assert [s.entries for s in _dedup_specs(specs)] == [s.entries for s in ded]

    # Per-tensor class sub-shards partition the original (disjoint + full cover => equal total numel).
    for name in ("A", "B"):
        holders = [i for i, s in enumerate(specs) if name in s.entries]
        classes: dict = {}
        for i in holders:
            classes.setdefault(_shards_canonical_key(specs[i][name]), []).append(i)
        for members in classes.values():
            orig = shards_numel(specs[members[0]][name])
            got = sum(
                shards_numel(ded[i][name]) for i in members if name in ded[i].entries
            )
            assert got == orig, f"{name}: class {members} numel {got} != {orig}"

    # A is 4-way replicated -> per-tensor dedup gives each holder a QUARTER of it.
    assert shards_numel(ded[0]["A"]) == 25

    # k=1 identity: a tensor held by exactly one worker is unchanged.
    solo = _dedup_specs([ShardSpec({"Z": d1(0, 7, 7)}), ShardSpec({"A": d1(0, 5, 5)})])
    assert solo[0]["Z"] == [[(0, 7, 7)]]


# ------------------------------------------------------------------ 3. recv_tensor_classes
def test_recv_tensor_classes():
    send = [ShardSpec({"A": d1(0, 100, 100), "B": d1(0, 40, 40)})]
    recv = [
        ShardSpec({"A": d1(0, 100, 100), "B": d1(0, 40, 40)}),
        ShardSpec({"A": d1(0, 100, 100), "B": d1(0, 40, 40)}),
        ShardSpec({"A": d1(0, 100, 100)}),
        ShardSpec({"A": d1(0, 100, 100)}),
    ]
    rt = make_router(send, recv, dtypes("A", "B"), cap=10**9)
    classes = rt.recv_tensor_classes()
    assert classes["A"] == [[0, 1, 2, 3]], classes["A"]  # all hold identical full A
    assert classes["B"] == [[0, 1]], classes["B"]  # only 0,1 hold B
    # A member's slice index is its position in its class; a non-holder is in no class.
    assert rt._arena_class_of(classes, "A", 2) == [0, 1, 2, 3]
    assert rt._arena_class_of(classes, "B", 3) == []


# ------------------------------------------------------------------ 4. arena_layout invariants
def test_arena_layout_invariants():
    # 2 senders hold 6 full tensors. recv0,1 hold Ti[0:400]; recv2,3 hold Ti[400:800] => per-tensor class
    # {0,1} and {2,3} (size 2 each) => an all-gather peer every round.
    names = [f"T{i}" for i in range(6)]
    send = [ShardSpec({n: d1(0, 800, 800) for n in names}) for _ in range(2)]
    recv = []
    for lo, hi in ((0, 400), (0, 400), (400, 800), (400, 800)):
        recv.append(ShardSpec({n: d1(lo, hi, 800) for n in names}))
    rt = make_router(send, recv, dtypes(*names), cap=1700)
    assert len(rt.global_rounds) >= 2, (
        f"need >=2 rounds to exercise cross-round coexistence, got {len(rt.global_rounds)}"
    )

    for rl in range(rt.receiver_ws):
        rounds, S, peers = rt.arena_layout(rl)
        total = _arena_total_bytes(rounds, 1, S)
        recv_total = _arena_recv_total_bytes(rounds, 1)
        assert rounds and S > 0
        my = rt.recv_specs[rl]
        for ri, rd in enumerate(rounds):
            s2r, prep, base = rd["s2r"], rd["prep"], rd["prep_base"]
            recv_regs = list(rd["recv"].values())
            prep_regs = _unique([rd["own"], *rd["send"].values()])
            grecv_regs = list(rd["grecv"].values())

            # Isolated RECV: per-sender lanes are disjoint/bounded by recv_stride; their CURRENT prefixes sum
            # to s2r == my deduped receive this round (padding between stable lanes is not charged to s2r).
            assert _disjoint(recv_regs), f"rl{rl} r{ri}: RECV overlap"
            assert all(0 <= o and o + n <= rd["recv_stride"] for o, n in recv_regs), (
                f"rl{rl} r{ri}: RECV out of isolated slot"
            )
            assert sum(n for _, n in recv_regs) == s2r
            round_names = sorted(n for n in rt.global_rounds[ri] if n in my.entries)
            assert s2r == my.subset(set(round_names)).nbytes(rt.dtype_spec), (
                f"rl{rl} r{ri}: s2r != my deduped recv (senders don't partition)"
            )
            assert rd["own"][1] == s2r, (
                "own must equal the assembled-canonical (== s2r) bytes"
            )

            # PREP zone: union(own, unique-send) is disjoint, top-anchored, and sums to prep.
            assert _disjoint(prep_regs), f"rl{rl} r{ri}: PREP overlap"
            assert all(base <= o and o + n <= S for o, n in prep_regs), (
                f"rl{rl} r{ri}: PREP out of slot"
            )
            assert sum(n for _, n in prep_regs) == prep
            assert base == S - prep
            assert rd["agh"] == prep + sum(n for _, n in grecv_regs)

            # GRECV is outside all rollout PREP parity slots, with current-round views bounded by the bank.
            assert _disjoint(grecv_regs), f"rl{rl} r{ri}: GRECV peer slots overlap"
            assert all(S <= o and o + n <= total for o, n in grecv_regs), (
                f"rl{rl} r{ri}: GRECV out of bank"
            )
            assert all(0 <= o and o + n <= S for o, n in prep_regs)

        # A peer has one stable base over all rounds, reserved at its maximum round size.
        peer_slots = []
        for p in peers:
            entries = [rd["grecv"][p] for rd in rounds if p in rd["grecv"]]
            assert len({off for off, _ in entries}) == 1
            peer_slots.append((entries[0][0], max(nb for _, nb in entries)))
        assert _disjoint(peer_slots)
        assert total == S + sum(nb for _, nb in peer_slots)

        # PREP and RECV are separate allocations. S is only the largest PREP payload; RECV has its own stride.
        assert S == max(rd["prep"] for rd in rounds)
        assert recv_total == rounds[0]["recv_stride"]
        assert len({rd["recv_stride"] for rd in rounds}) == 1

        # At a fixed parity, a trainer's lane base is stable across every round in which it contributes.
        for si in range(rt.sender_ws):
            entries = [rd["recv"][si] for rd in rounds if si in rd["recv"]]
            assert len({off for off, _ in entries}) <= 1
        assert peers, f"rl{rl}: expected AGH peers"

    assert rt.arena_layout(0) == rt.arena_layout(0), "arena_layout not deterministic"


def test_arena_layout_isolates_skipped_depth2_recv_from_live_prep():
    """A skipped round does not create a RECV/PREP alias when a parity is reused.

    Receiver 0 has a large PREP in round 0, skips round 1, and receives D in round 2. The old unified arena
    had to co-pack RECV(2) beside still-live PREP(0). They now belong to separate allocations: PREP stride is
    max(prep), while the trainer's round-0/round-2 RECV lane has one stable base in the isolated ingress slot.
    """
    send = [
        ShardSpec(
            {
                "A": d1(0, 300, 300),
                "B": d1(0, 300, 300),
                "C": d1(0, 300, 300),
                "D": d1(0, 500, 500),
                "X": d1(0, 10, 10),
            }
        )
    ]
    recv = [
        ShardSpec(
            {
                "A": d1(0, 300, 300),
                "B": d1(0, 300, 300),
                "C": d1(0, 300, 300),
                "D": d1(0, 500, 500),
            }
        ),
        ShardSpec({"A": d1(0, 300, 300), "B": d1(0, 300, 300), "X": d1(0, 10, 10)}),
        ShardSpec({"B": d1(0, 300, 300), "C": d1(0, 300, 300)}),
    ]
    rt = make_router(send, recv, dtypes("A", "B", "C", "D", "X"), cap=10**9)
    rt.global_rounds = [{"A", "B", "C"}, {"X"}, {"D"}]
    rt.local_rounds = rt.compute_local_rounds()

    rounds, S, _ = rt.arena_layout(0, depth=2)
    assert [ri for ri, rd in enumerate(rounds) if rd["s2r"]] == [0, 2]
    assert rounds[0]["prep"] > rounds[0]["s2r"]
    assert S == max(rd["prep"] for rd in rounds)
    assert rounds[0]["recv"][0][0] == rounds[2]["recv"][0][0]
    assert _arena_recv_total_bytes(rounds, 2) == 2 * rounds[0]["recv_stride"]


def test_isolated_recv_lanes_prevent_three_round_cross_sender_overwrite():
    """Regression for the exact 8-trainer/16-rollout corruption found in the 11-group integration test.

    Worker 11's round-0 half slice comes from trainer 3; its round-2 wide slice comes from trainer 5. Both
    rounds use parity zero. The old compact RECV layout assigned both writes offset zero, while trainer 5's
    round-0 ACK set did not include worker 11. Stable sender lanes must give those two writes disjoint offsets.
    """
    names = ("half", "pair", "wide")
    send = [ShardSpec({name: d1(0, 160, 160) for name in names}) for _ in range(8)]
    recv = []
    for worker in range(16):
        lane = worker % 8
        half = 0 if lane < 4 else 1
        recv.append(
            ShardSpec(
                {
                    "half": d1(half * 80, (half + 1) * 80, 160),
                    "pair": d1(lane * 20, (lane + 1) * 20, 160),
                    "wide": d1(0, 160, 160),
                }
            )
        )
    rt = make_router(send, recv, dtypes(*names), cap=40)
    assert [sorted(names) for names in rt.global_rounds] == [
        ["half"],
        ["pair"],
        ["wide"],
    ]

    worker = 11
    rounds, _S, _peers = rt.arena_layout(worker, depth=2)
    assert rounds[0]["recv"] == {3: (0, 40)}
    assert rounds[2]["recv"] == {5: (40, 40)}
    assert rounds[0]["recv_stride"] == 80
    assert _disjoint([rounds[0]["recv"][3], rounds[2]["recv"][5]])

    # Demonstrate why the former global-parity sender gate could not protect this receiver.
    sender5 = make_router(send, recv, dtypes(*names), cap=40, rank=5)
    r0_peers = set(sender5.local_rounds[0][1])
    r2_peers = set(sender5.local_rounds[2][1])
    worker_global = len(send) + worker
    assert worker_global not in r0_peers
    assert worker_global in r2_peers


def test_sender_ingress_gate_tracks_destination_and_parity():
    """A skipped use of one destination must not be hidden by another receiver's newer parity generation."""
    pred, last = _recv_lane_predecessors(
        [[10, 11], [12], [11], [12], [10]],
        depth=2,
    )
    assert pred == [
        {10: None, 11: None},
        {12: None},
        {11: 0},
        {12: 1},
        {10: 0},
    ]
    assert last == {(10, 0): 4, (11, 0): 2, (12, 1): 3}


@pytest.mark.skipif(
    not torch.cuda.is_available(), reason="fused CopyPlan execution requires CUDA"
)
def test_fused_prepare_reads_isolated_ingress_after_future_round_lands():
    """Land round 2 before assembling round 0 and prove the fused kernel still reads round 0's sender lane."""
    names = ("t0", "t1", "t2")
    send = [ShardSpec({name: d1(0, 20, 20) for name in names}) for _ in range(2)]
    recv_spec = ShardSpec(
        {"t0": d1(0, 10, 20), "t1": d1(0, 10, 20), "t2": d1(10, 20, 20)}
    )
    rt = make_router(send, [recv_spec], dtypes(*names), cap=10**9, rank=2)
    rt.global_rounds = [{name} for name in names]
    rt.local_rounds = rt.compute_local_rounds()
    layout, stride, peers = rt.arena_layout(0, depth=2)
    assert not peers

    endpoint = WBEndpoint.__new__(WBEndpoint)
    endpoint.router = rt
    endpoint._rank = 2
    endpoint._recv_depth = 2
    endpoint._arena_layout = layout
    endpoint._arena_S = stride
    endpoint._recv_S = layout[0]["recv_stride"]
    endpoint._arena = torch.zeros(
        max(_arena_total_bytes(layout, 2, stride), 1),
        dtype=torch.uint8,
        device="cuda",
    )
    endpoint._recv_arena = torch.zeros(
        max(_arena_recv_total_bytes(layout, 2), 1),
        dtype=torch.uint8,
        device="cuda",
    )
    endpoint._repl_peers = []
    endpoint._topo_ok = False
    endpoint.receiver_staging = False
    endpoint.dtype_spec = dtypes(*names)

    entries = {}
    endpoint.wksd = {}
    expected = {}
    for ni, (name, shards) in enumerate(recv_spec):
        numel = shards_numel(shards)
        entries[name] = {name: [(shards[0], [(0, numel, numel)])]}
        endpoint.wksd[name] = torch.full((numel,), -1, dtype=F32, device="cuda")
        left, right, _width = shards[0][0]
        expected[name] = torch.arange(left, right, dtype=F32, device="cuda") + ni * 1000
    endpoint.load_spec = LoadSpec(entries)
    endpoint._build_arena_plans()

    # Populate every ingress round first. In particular r2 lands in parity 0 before r0's fused prepare runs.
    # The original compact ingress put both writes at parity-0 offset zero and deterministically corrupted t0.
    for ri, name in enumerate(names):
        rd = layout[ri]
        assert len(rd["recv"]) == 1
        _si, (off, nb) = next(iter(rd["recv"].items()))
        absolute = _arena_slot_offset(off, ri, 2, endpoint._recv_S)
        endpoint._recv_arena[absolute : absolute + nb].view(F32).copy_(expected[name])

    for ri, name in enumerate(names):
        endpoint._arena_prepare[ri].run()
        endpoint._arena_consume[ri].run()
        torch.cuda.synchronize()
        torch.testing.assert_close(endpoint.wksd[name], expected[name], rtol=0, atol=0)


# ------------------------------------------------------------------ 5. adversarial: peer set varies per round
def test_arena_layout_adversarial_varying_peers():
    # 4 receivers A,B,C,D = 0,1,2,3. Four tensors, each held (identically, fully) by a DIFFERENT pair, so
    # each tensor's per-tensor class is exactly that pair and different rounds pull in different peers.
    tens = {"tAB": (0, 1), "tAC": (0, 2), "tBD": (1, 3), "tCD": (2, 3)}
    send = [ShardSpec({t: d1(0, 600, 600) for t in tens}) for _ in range(2)]
    recv = [
        ShardSpec({t: d1(0, 600, 600) for t, pair in tens.items() if rc in pair})
        for rc in range(4)
    ]
    rt = make_router(
        send, recv, dtypes(*tens), cap=2000
    )  # 1 tensor (deduped 300*4=1200B) per receiver/round

    classes = rt.recv_tensor_classes()
    assert classes == {
        "tAB": [[0, 1]],
        "tAC": [[0, 2]],
        "tBD": [[1, 3]],
        "tCD": [[2, 3]],
    }, classes

    layouts = {rl: rt.arena_layout(rl)[0] for rl in range(4)}
    gr = rt.global_rounds

    # Reciprocity: for a tensor shared by pair (a,b), in the round holding it, a.send[b] feeds b.grecv[a].
    for t, (a, b) in tens.items():
        ri = next(i for i, s in enumerate(gr) if t in s)
        la, lb = layouts[a][ri], layouts[b][ri]
        assert b in la["send"] and a in lb["grecv"], f"{t}: peers not wired"
        assert la["send"][b][1] == lb["grecv"][a][1], f"{t}: a.send != b.grecv"
        assert la["grecv"][b][1] == lb["send"][a][1], f"{t}: a.grecv != b.send"

    # Peer set VARIES across rounds for receiver 0 (peer 1 in tAB's round, peer 2 in tAC's round).
    r0_peers = [set(rd["send"]) for rd in layouts[0]]
    assert {1} in r0_peers and {2} in r0_peers, r0_peers

    # Offsets valid for every receiver; RECV is isolated, PREP is slotted, GRECV is in the shared bank.
    for rl in range(4):
        rounds, S, _ = rt.arena_layout(rl)
        total = _arena_total_bytes(rounds, 1, S)
        for rd in rounds:
            recv_regs = list(rd["recv"].values())
            prep_regs = _unique([rd["own"], *rd["send"].values()])
            grecv_regs = list(rd["grecv"].values())
            assert (
                _disjoint(recv_regs) and _disjoint(prep_regs) and _disjoint(grecv_regs)
            )
            assert all(0 <= o and o + n <= rd["recv_stride"] for o, n in recv_regs)
            assert all(0 <= o and o + n <= S for o, n in prep_regs)
            assert all(S <= o and o + n <= total for o, n in grecv_regs)


def test_depth2_uses_two_grecv_parity_slots():
    names = [f"T{i}" for i in range(6)]
    send = [ShardSpec({n: d1(0, 800, 800) for n in names}) for _ in range(2)]
    recv = [ShardSpec({n: d1(0, 400, 800) for n in names}) for _ in range(4)]
    rt = make_router(send, recv, dtypes(*names), cap=1700)

    rounds, S, peers = rt.arena_layout(0, depth=2)
    total = _arena_total_bytes(rounds, 2, S)
    shared_by_parity = sum(
        max(
            (
                rd["grecv"].get(p, (0, 0))[1]
                for ri, rd in enumerate(rounds)
                if ri % 2 == slot
            ),
            default=0,
        )
        for p in peers
        for slot in range(2)
    )
    assert total == 2 * S + shared_by_parity
    assert shared_by_parity > 0
    for p in peers:
        offsets = {
            slot: {
                rd["grecv"][p][0]
                for ri, rd in enumerate(rounds)
                if ri % 2 == slot and p in rd["grecv"]
            }
            for slot in range(2)
        }
        assert all(len(slot_offsets) <= 1 for slot_offsets in offsets.values())
        active = [
            next(iter(slot_offsets))
            for slot_offsets in offsets.values()
            if slot_offsets
        ]
        assert len(active) == 2 and len(set(active)) == 2


def test_grecv_peer_slots_are_distinct_and_sized_per_parity():
    send = [ShardSpec({"small": d1(0, 40, 40), "large": d1(0, 100, 100)})]
    recv = [
        ShardSpec({"small": d1(0, 40, 40), "large": d1(0, 100, 100)}) for _ in range(2)
    ]
    rt = make_router(send, recv, dtypes("small", "large"), cap=10**9)
    rt.global_rounds = [{"small"}, {"large"}]
    rt.local_rounds = rt.compute_local_rounds()

    rounds, S, peers = rt.arena_layout(0, depth=2)
    assert peers == [1]
    slots = [rd["grecv"][1] for rd in rounds]
    assert slots[0][0] != slots[1][0]
    assert slots[0][1] < slots[1][1]
    assert _arena_total_bytes(rounds, 2, S) == 2 * S + sum(nb for _, nb in slots)


# ------------------------------------------------------------------ 6. sender-target size agreement
def test_arena_sender_target_agreement():
    """The arena RECV nb a sender writes to == the (sender, receiver) compute_overlap nb — the cross-wire
    size the tensor-mode sender uses (`_fuse_sizes`). This is the runtime invariant _setup_rdma_buffers relies
    on so a sender writes exactly into its RECV sub-region."""
    names = [f"T{i}" for i in range(6)]
    send = [ShardSpec({n: d1(0, 800, 800) for n in names}) for _ in range(2)]
    recv = [
        ShardSpec({n: d1(lo, hi, 800) for n in names})
        for lo, hi in ((0, 400), (0, 400), (400, 800), (400, 800))
    ]
    rt = make_router(send, recv, dtypes(*names), cap=1700)
    for rl in range(rt.receiver_ws):
        rounds, _S, _ = rt.arena_layout(rl)
        my = rt.recv_specs[rl]
        for ri, rd in enumerate(rounds):
            round_names = set(n for n in rt.global_rounds[ri] if n in my.entries)
            for si in range(rt.sender_ws):
                ov = ShardSpec.compute_overlap(rt.send_specs[si], my).subset(
                    set(round_names)
                )
                assert rd["recv"].get(si, (0, 0))[1] == ov.nbytes(rt.dtype_spec), (
                    f"rl{rl} r{ri} si{si}: arena recv nb != compute_overlap nb"
                )


# ------------------------------------------------------------------ 7. dtype-view alignment (uniform dtype)
def test_arena_dtype_alignment():
    """Every region offset is itemsize-aligned for a UNIFORM-dtype spec, so `arena[off:].view(dtype)` (what
    `_carve_named`/`fuse_copy_pairs` do) is valid — the 30B is bf16 so this holds. Mixed-dtype models would
    need offset padding across the whole wire layout (a pre-existing `_carve_named` constraint, not arena-specific)."""
    names = [f"T{i}" for i in range(6)]
    send = [ShardSpec({n: d1(0, 800, 800) for n in names}) for _ in range(2)]
    recv = [
        ShardSpec({n: d1(lo, hi, 800) for n in names})
        for lo, hi in ((0, 400), (0, 400), (400, 800), (400, 800))
    ]
    it = torch.bfloat16.itemsize
    rt = make_router(send, recv, {n: torch.bfloat16 for n in names}, cap=1700)
    for rl in range(rt.receiver_ws):
        rounds, S, _ = rt.arena_layout(rl)
        total = _arena_total_bytes(rounds, 1, S)
        recv_total = _arena_recv_total_bytes(rounds, 1)
        assert S % it == 0
        assert total % it == 0
        assert recv_total % it == 0
        for rd in rounds:
            regs = list(rd["recv"].values()) + [
                rd["own"],
                *rd["send"].values(),
                *rd["grecv"].values(),
            ]
            for off, nb in regs:
                assert off % it == 0 and nb % it == 0, (
                    f"unaligned region ({off},{nb}) for itemsize {it}"
                )


# ------------------------------------------------------------------ 8. wide (c=8) class reciprocity
def test_arena_wide_class_reciprocity():
    """A c=8 tensor (all 8 receivers hold it identically) + a c=2 tensor + a c=1 tensor — the tiny
    router-gate case that forces the m>2 all-to-all. Reciprocity `a.send[p].nb == p.grecv[a].nb` and
    shared-set symmetry hold across the wide fan-out; receiver 0 sees all 7 others as peers."""
    G, P, solo = "gate", "pairAB", "solo"
    send = [
        ShardSpec({G: d1(0, 64, 64), P: d1(0, 256, 256), solo: d1(0, 64, 64)})
        for _ in range(2)
    ]
    recv = []
    for rc in range(8):
        e = {G: d1(0, 64, 64)}  # G: all 8 hold it identically -> class {0..7}
        if rc in (0, 1):
            e[P] = d1(0, 256, 256)  # P: receivers 0,1 -> class {0,1}
        if rc == 0:
            e[solo] = d1(0, 64, 64)  # solo: receiver 0 only -> class {0}
        recv.append(ShardSpec(e))
    rt = make_router(send, recv, dtypes(G, P, solo), cap=10**9)
    classes = rt.recv_tensor_classes()
    assert classes[G] == [[0, 1, 2, 3, 4, 5, 6, 7]]
    assert classes[P] == [[0, 1]]
    assert classes[solo] == [[0]]
    layouts = {rl: rt.arena_layout(rl) for rl in range(8)}
    assert layouts[0][2] == [1, 2, 3, 4, 5, 6, 7], (
        "receiver 0 must all-gather with all 7 others via G"
    )
    # Deduplicate sends by logical payload before considering own aliasing: receiver 0 sends {G,P} to peer
    # 1 and {G} to peers 2..7, hence exactly two physical send regions (neither is own because own has solo).
    assert len(rt.global_rounds) == 1
    r0 = layouts[0][0][0]
    assert len(set(r0["send"].values())) == 2
    assert r0["own"] not in set(r0["send"].values())
    for a in range(8):
        for ri, rd_a in enumerate(layouts[a][0]):
            rn_a = sorted(
                n for n in rt.global_rounds[ri] if n in rt.recv_specs[a].entries
            )
            for p in rd_a["send"]:
                rd_b = layouts[p][0][ri]
                assert a in rd_b["grecv"], (
                    f"a{a}->p{p} r{ri}: not reciprocated in grecv"
                )
                assert rd_a["send"][p][1] == rd_b["grecv"][a][1], (
                    f"a{a} p{p} r{ri}: send/grecv nb mismatch"
                )
                rn_b = sorted(
                    n for n in rt.global_rounds[ri] if n in rt.recv_specs[p].entries
                )
                assert rt._arena_shared(a, p, rn_a, classes) == rt._arena_shared(
                    p, a, rn_b, classes
                )


def test_topology_external_only_one_worker_per_node_is_eligible():
    """Two one-worker rollout replicas have a valid external-only topology (empty internal phase)."""
    send = [ShardSpec({"weight": d1(0, 64, 64)})]
    recv = [ShardSpec({"weight": d1(0, 64, 64)}) for _ in range(2)]
    rt = make_router(send, recv, dtypes("weight"), cap=10**9)
    ip = {0: "trainer", 1: "node-a", 2: "node-b"}

    assert rt.configure_topology(ip)
    assert len(rt._topology_groups) == 1
    assert rt.topology_plan(0, 0) == {
        "external": {1: ("weight",)},
        "pull": {},
        "peers": (1,),
        "internal": (),
    }


def test_topology_external_reuse_waits_two_rounds_back_at_depth2():
    names = tuple(f"w{i}" for i in range(4))
    send = [ShardSpec({name: d1(0, 100, 100) for name in names})]
    recv = [ShardSpec({name: d1(0, 100, 100) for name in names}) for _ in range(2)]
    rt = make_router(send, recv, dtypes(*names), cap=200)
    assert len(rt.global_rounds) == 4
    ip = {0: "trainer", 1: "node-a", 2: "node-b"}
    assert rt.configure_topology(ip)

    endpoint, layout, _stride, _peers = _bind_fake_topology_endpoint(rt, 0, ip, depth=2)
    peer = 2
    assert endpoint._topo_peer_predecessors == [
        {peer: (-1, 2)},
        {peer: (-1, 3)},
        {peer: (0, 0)},
        {peer: (0, 1)},
    ]
    offsets = [rd["grecv"][1][0] for rd in layout]
    assert offsets[0] == offsets[2]
    assert offsets[1] == offsets[3]
    assert offsets[0] != offsets[1]


def test_topology_multi_group_16_workers_11_groups():
    """Hand-crafted two-node topology: 1 wide + 8 rank-pair + 2 half-GPU groups.

    Every receiver belongs to three overlapping groups.  The exact external column is packed once per
    cross-node peer, while a local peer can require multiple disjoint own/grecv reads because its unrelated
    rank-pair tensor lies between the selected half/wide tensors in packed-name order.
    """
    names = ("half", "pair", "wide")
    send = [ShardSpec({name: d1(0, 160, 160) for name in names})]
    recv = []
    for worker in range(16):
        lane = worker % 8
        half = 0 if lane < 4 else 1
        recv.append(
            ShardSpec(
                {
                    "half": d1(half * 80, (half + 1) * 80, 160),
                    "pair": d1(lane * 20, (lane + 1) * 20, 160),
                    "wide": d1(0, 160, 160),
                }
            )
        )
    rt = make_router(send, recv, dtypes(*names), cap=10**9)
    sw = rt.sender_ws
    ip = {0: "trainer"}
    ip.update(
        {sw + worker: ("node-a" if worker < 8 else "node-b") for worker in range(16)}
    )

    assert rt.configure_topology(ip)
    assert len(rt._topology_groups) == 11
    assert sum(grp["names"] == ("wide",) for grp in rt._topology_groups) == 1
    assert sum(grp["names"] == ("pair",) for grp in rt._topology_groups) == 8
    assert sum(grp["names"] == ("half",) for grp in rt._topology_groups) == 2

    plan = rt.topology_plan(0, 0)
    assert plan["external"] == {8: names}
    assert plan["internal"] == tuple(range(1, 8))
    assert plan["peers"] == tuple(range(1, 16))
    routes1 = {
        (kind, source): route_names for kind, source, route_names in plan["pull"][1]
    }
    assert routes1 == {
        ("grecv", 9): ("half", "wide"),
        ("own", 1): ("half", "wide"),
    }
    routes4 = {
        (kind, source): route_names for kind, source, route_names in plan["pull"][4]
    }
    assert routes4 == {("grecv", 12): ("wide",), ("own", 4): ("wide",)}

    layout, stride, peers = rt.arena_layout(0, depth=2)
    assert peers == list(range(1, 16))
    assert set(layout[0]["grecv"]) == {8}, (
        "only the cross-node ingress column gets GRECV storage"
    )
    assert layout[0]["grecv_names"] == {8: names}
    # In this aligned example all three groups choose the same cross-node lane, so the exact packed payload
    # aliases both the generic peer payload and own rather than reserving another PREP buffer.
    assert layout[0]["topo_send"][8] == layout[0]["send"][8] == layout[0]["own"]

    # Bind the symbolic plan to fake CPU arena addresses.  This exercises the runtime resolver without CUDA
    # or RDMA and proves that one local pair really produces multiple direct copy spans.
    endpoint, _layout, _stride, _peers = _bind_fake_topology_endpoint(rt, 0, ip)
    assert endpoint._topo_ext_send_peers_by_round == [(sw + 8,)]
    assert endpoint._topo_ext_recv_peers_by_round == [(sw + 8,)]
    assert endpoint._topo_int_peers_by_round == [tuple(sw + p for p in range(1, 8))]
    assert endpoint._topo_int_readers_by_round == [tuple(sw + p for p in range(1, 8))]
    assert endpoint._topo_peer_predecessors[0][sw + 8] == (-1, 0)
    assert len(endpoint._topo_ext_xfer[sw + 8][0]) == 1
    # Logical own and GRECV routes retain their exact source identity. The own descriptor joins the first
    # external slot at bind time, while that slot can be consumed and released independently of other slots.
    peer_sources = endpoint._topo_internal_consume_src[sw + 1][0]
    assert len(peer_sources) == 2
    assert {source for source, *_rest in peer_sources} == {None, sw + 9}


def test_topology_overlapping_groups_can_choose_different_external_columns():
    """One grecv slot may be populated partly by a direct peer and partly through a local intermediary."""
    wide, diag = "a_wide", "z_diag"
    send = [ShardSpec({wide: d1(0, 160, 160), diag: d1(0, 160, 160)})]
    recv = []
    for worker in range(4):
        diag_half = 0 if worker in (0, 3) else 1
        recv.append(
            ShardSpec(
                {
                    wide: d1(0, 160, 160),
                    diag: d1(diag_half * 80, (diag_half + 1) * 80, 160),
                }
            )
        )
    ip = {0: "trainer", 1: "node-a", 2: "node-a", 3: "node-b", 4: "node-b"}

    # In one round, worker 0's wide column goes to worker 2 while its diagonal group goes to worker 3.
    # Worker 3's generic payload contains both names, but only `diag` is direct; `wide` arrives internally.
    rt = make_router(send, recv, dtypes(wide, diag), cap=10**9)
    assert rt.configure_topology(ip)
    plan = rt.topology_plan(0, 0)
    assert plan["external"] == {2: (wide,), 3: (diag,)}
    layout, _stride, _peers = rt.arena_layout(0, depth=2)
    assert layout[0]["grecv_names"] == {2: (wide,), 3: (diag,)}
    assert layout[0]["topo_send"][2] == layout[0]["send"][2]
    assert layout[0]["topo_send"][3] != layout[0]["send"][3]
    assert layout[0]["topo_send"][3][1] * 2 == layout[0]["send"][3][1]
    endpoint, _layout, _stride, _peers = _bind_fake_topology_endpoint(rt, 0, ip)
    assert endpoint._topo_ext_send_peers_by_round == [(3, 4)]
    assert endpoint._topo_ext_recv_peers_by_round == [(3, 4)]
    assert endpoint._topo_int_peers_by_round == [(2,)]
    assert endpoint._topo_int_readers_by_round == [(2,)]
    assert endpoint._topo_peer_predecessors[0] == {3: (-1, 0), 4: (-1, 0)}
    assert len(endpoint._topo_internal_consume_src[2][0]) == 2

    # Split the names into two rounds. Worker 3 receives `wide` through an intermediary in round 0, then
    # receives `diag` directly from worker 0 in round 1. Internal consume has no local staging slot, so the
    # direct compact GRECV slot's predecessor is its own final direct use in the prior epoch—not round 0.
    rt2 = make_router(send, recv, dtypes(wide, diag), cap=160)
    assert rt2.configure_topology(ip)
    ri_wide = next(ri for ri, names in enumerate(rt2.global_rounds) if names == {wide})
    ri_diag = next(ri for ri, names in enumerate(rt2.global_rounds) if names == {diag})
    assert ri_wide < ri_diag
    assert rt2.topology_plan(0, ri_wide)["external"] == {2: (wide,)}
    assert rt2.topology_plan(0, ri_diag)["external"] == {3: (diag,)}
    endpoint2, _layout, _stride, _peers = _bind_fake_topology_endpoint(rt2, 0, ip)
    assert endpoint2._topo_peer_predecessors[ri_diag][4] == (-1, ri_diag)


def test_packed_copy_spans_scatter_selected_names_and_coalesce_neighbors():
    dtype = dtypes("a", "b", "c")
    full = ShardSpec({name: d1(0, 8, 8) for name in ("a", "b", "c")})
    compact_ac = ShardSpec({name: d1(0, 8, 8) for name in ("a", "c")})

    # Compact source -> gapped destination needs two spans.
    assert _packed_copy_spans(
        compact_ac, full, ("a", "c"), dtype, src_base=100, dst_base=200
    ) == [
        (100, 200, 32),
        (132, 264, 32),
    ]
    # Adjacent names in both layouts merge into one transfer span.
    assert _packed_copy_spans(
        full, full, ("a", "b"), dtype, src_base=100, dst_base=200
    ) == [
        (100, 200, 64),
    ]


def test_arena_identical_full_peer_sends_alias_own():
    """Deduplicate peer sends first, then use the canonical own bytes as that full-payload source."""
    name = "shared"
    send = [ShardSpec({name: d1(0, 400, 400)})]
    recv = [ShardSpec({name: d1(0, 400, 400)}) for _ in range(4)]
    rt = make_router(send, recv, dtypes(name), cap=10**9)

    rounds, _S, peers = rt.arena_layout(0, depth=2)
    assert peers == [1, 2, 3]
    rd = next(rd for rd in rounds if rd["s2r"])
    assert len(rd["send"]) == 3  # protocol remains per-peer
    assert set(rd["send"].values()) == {
        rd["own"]
    }  # storage is the canonical payload itself
    assert rd["prep"] == rd["own"][1]
    assert rd["agh"] == rd["own"][1] + sum(nb for _, nb in rd["grecv"].values())


if __name__ == "__main__":
    test_split_shard_evenly_partition()
    print("PASS test_split_shard_evenly_partition")
    test_dedup_specs_partition_and_determinism()
    print("PASS test_dedup_specs_partition_and_determinism")
    test_recv_tensor_classes()
    print("PASS test_recv_tensor_classes")
    test_arena_layout_invariants()
    print("PASS test_arena_layout_invariants")
    test_arena_layout_isolates_skipped_depth2_recv_from_live_prep()
    print("PASS test_arena_layout_isolates_skipped_depth2_recv_from_live_prep")
    test_isolated_recv_lanes_prevent_three_round_cross_sender_overwrite()
    print("PASS test_isolated_recv_lanes_prevent_three_round_cross_sender_overwrite")
    test_sender_ingress_gate_tracks_destination_and_parity()
    print("PASS test_sender_ingress_gate_tracks_destination_and_parity")
    test_arena_layout_adversarial_varying_peers()
    print("PASS test_arena_layout_adversarial_varying_peers")
    test_arena_sender_target_agreement()
    print("PASS test_arena_sender_target_agreement")
    test_arena_dtype_alignment()
    print("PASS test_arena_dtype_alignment")
    test_arena_wide_class_reciprocity()
    print("PASS test_arena_wide_class_reciprocity")
    test_arena_identical_full_peer_sends_alias_own()
    print("PASS test_arena_identical_full_peer_sends_alias_own")
    print("ALL ARENA PLANNER TESTS PASSED")
