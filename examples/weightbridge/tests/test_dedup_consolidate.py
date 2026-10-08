# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for the dedup group consolidation pass (router.consolidate_groups).

Threshold is now a float (0 = off, inf = most aggressive). The overlap guard means a group is only
decomposed when ANOTHER multi-worker group overlaps it (shares a worker); a standalone group is left
intact (covering it by singletons would destroy its dedup) — this also terminates threshold=inf.
"""

import math

from wbridge.backend.router import consolidate_groups

MB = 1024 * 1024


def _partition_ok(nat, out):
    """Output must be a partition-refinement of the natural classes: same coverage, each worker once, and
    every sub-group a subset of some natural class of that tensor."""
    for name, subs in out.items():
        flat = [w for sg in subs for w in sg]
        assert len(flat) == len(set(flat)), f"{name}: worker repeated in {subs}"
        nat_workers = {w for c in nat[name] for w in c}
        assert set(flat) == nat_workers, f"{name}: coverage {subs} != {nat[name]}"
        for sg in subs:
            assert any(set(sg) <= set(c) for c in nat[name]), (
                f"{name}: {sg} not a subset of any class"
            )


def test_high_traffic_noop():
    # {0,1,2,3} @ 200 MB -> per-pair 2*200/4 = 100 MB > 20 MB threshold -> untouched.
    nat = {"big": [[0, 1, 2, 3]]}
    cb = {("big", (0, 1, 2, 3)): 200 * MB}
    assert consolidate_groups(nat, cb, 20 * MB) == {"big": [[0, 1, 2, 3]]}


def test_threshold_zero_noop():
    nat = {"t": [[0, 1, 2, 3]]}
    cb = {("t", (0, 1, 2, 3)): 1 * MB}
    assert consolidate_groups(nat, cb, 0) == {"t": [[0, 1, 2, 3]]}


def test_wide_small_standalone_intact():
    # Small wide group with NO other overlapping group -> the overlap guard leaves it INTACT (covering it by
    # singletons would destroy its dedup). (Before the overlap guard this was singletonized.)
    nat = {"s": [[0, 1, 2, 3]]}
    cb = {("s", (0, 1, 2, 3)): 1 * MB}
    out = consolidate_groups(nat, cb, 20 * MB)
    assert out == {"s": [[0, 1, 2, 3]]}
    _partition_ok(nat, out)


def test_piggyback_onto_existing_group():
    # big={0,1}@200MB keeps its high-traffic edge; small={0,1,2,3}@1MB overlaps it, so it dissolves,
    # piggybacking {0,1} and sending {2},{3} directly.
    nat = {"big": [[0, 1]], "small": [[0, 1, 2, 3]]}
    cb = {("big", (0, 1)): 200 * MB, ("small", (0, 1, 2, 3)): 1 * MB}
    out = consolidate_groups(nat, cb, 20 * MB)
    assert out["big"] == [[0, 1]]
    assert out["small"] == [[0, 1], [2], [3]]
    _partition_ok(nat, out)


def test_piggyback_prefers_larger_subgroup():
    # A size-3 existing group is preferred (worker-count desc) over splitting into singletons.
    nat = {"z": [[0, 1, 2]], "small": [[0, 1, 2, 3]]}
    cb = {("z", (0, 1, 2)): 300 * MB, ("small", (0, 1, 2, 3)): 1 * MB}
    out = consolidate_groups(nat, cb, 20 * MB)
    assert out["small"] == [[0, 1, 2], [3]]
    _partition_ok(nat, out)


def test_determinism_and_termination():
    nat = {"a": [[0, 1, 2, 3]], "b": [[1, 2]], "c": [[0, 1, 2, 3, 4, 5]]}
    cb = {
        ("a", (0, 1, 2, 3)): 1 * MB,
        ("b", (1, 2)): 300 * MB,
        ("c", (0, 1, 2, 3, 4, 5)): 2 * MB,
    }
    o1 = consolidate_groups(nat, cb, 20 * MB)
    o2 = consolidate_groups(nat, cb, 20 * MB)
    assert o1 == o2  # deterministic (and it returned -> terminated)
    _partition_ok(nat, o1)


def test_multiclass_disjoint_classes_intact():
    # A partially-sharded tensor (two disjoint classes). The low-traffic class (1,3) does NOT overlap the
    # other class (0,2), so the overlap guard leaves BOTH intact.
    nat = {"tp": [[0, 2], [1, 3]]}
    cb = {("tp", (0, 2)): 300 * MB, ("tp", (1, 3)): 1 * MB}
    out = consolidate_groups(nat, cb, 20 * MB)
    assert out["tp"] == [[0, 2], [1, 3]]
    _partition_ok(nat, out)


def test_float_threshold_and_inf():
    nat = {"t": [[0, 1, 2, 3]]}
    cb = {("t", (0, 1, 2, 3)): 5 * MB}
    assert consolidate_groups(nat, cb, 0.0) == {"t": [[0, 1, 2, 3]]}  # 0.0 -> off
    assert consolidate_groups(nat, cb, math.inf) == {
        "t": [[0, 1, 2, 3]]
    }  # standalone stays intact at inf


def test_inf_terminates_and_preserves_standalone():
    # threshold=inf: an OVERLAPPED group decomposes; STANDALONE groups stay intact; the loop terminates.
    nat = {"big": [[0, 1]], "small": [[0, 1, 2, 3]], "solo": [[4, 5, 6, 7]]}
    cb = {
        ("big", (0, 1)): 200 * MB,
        ("small", (0, 1, 2, 3)): 1 * MB,
        ("solo", (4, 5, 6, 7)): 1 * MB,
    }
    out = consolidate_groups(nat, cb, math.inf)  # returns => terminated
    assert out["small"] == [[0, 1], [2], [3]]  # overlaps big -> decomposed
    assert out["big"] == [[0, 1]]  # standalone after small dissolves -> intact
    assert out["solo"] == [[4, 5, 6, 7]]  # never overlapped -> intact
    _partition_ok(nat, out)


def test_one_group_per_worker():
    # 4 engines x 4 ranks = 16 receivers. Expert tensors are size-4 same-rank classes {r,r+4,r+8,r+12};
    # a dense tensor is the size-16 all-class. At inf the dense folds onto the (partitioning) expert groups,
    # so each worker ends up in exactly ONE multi-worker group, minimizing topology fan-out.
    experts = {f"e{r}": [[r, r + 4, r + 8, r + 12]] for r in range(4)}
    dense = {"dense": [list(range(16))]}
    nat = {**experts, **dense}
    cb = {("dense", tuple(range(16))): 1 * MB}
    for r in range(4):
        cb[(f"e{r}", (r, r + 4, r + 8, r + 12))] = 100 * MB
    out = consolidate_groups(nat, cb, math.inf)
    wg: dict[int, set] = {}
    for subs in out.values():
        for sg in subs:
            if len(sg) >= 2:
                for w in sg:
                    wg.setdefault(w, set()).add(tuple(sg))
    assert all(len(g) == 1 for g in wg.values()), (
        wg
    )  # one multi-worker group per worker
    assert all(
        len(sg) in (1, 4) for sg in out["dense"]
    )  # dense dissolved onto size-4 expert groups
    _partition_ok(nat, out)
