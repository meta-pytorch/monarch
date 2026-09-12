# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for WeightRouter.topo_subgroups (topology-aware 1-worker-per-node partition)."""

from wbridge.backend.router import WeightRouter


def test_equal_n_two_nodes():
    subs, ok = WeightRouter.topo_subgroups(
        [4, 5, 6, 7], {4: "A", 5: "A", 6: "B", 7: "B"}
    )
    assert ok
    assert subs == [[4, 6], [5, 7]]  # subgroup j = j-th worker (by rank) on each node


def test_three_nodes():
    ip = {0: "A", 1: "A", 2: "B", 3: "B", 4: "C", 5: "C"}
    subs, ok = WeightRouter.topo_subgroups([0, 1, 2, 3, 4, 5], ip)
    assert ok
    assert subs == [[0, 2, 4], [1, 3, 5]]


def test_unequal_n_fails():
    subs, ok = WeightRouter.topo_subgroups([4, 5, 6], {4: "A", 5: "A", 6: "B"})
    assert not ok and subs == []


def test_missing_ip_fails():
    subs, ok = WeightRouter.topo_subgroups(
        [4, 5, 6, 7], {4: "A", 5: "A", 6: "B"}
    )  # 7's ip missing
    assert not ok


def test_one_per_node_ok():
    # n=1 is the external-only topology used by two one-node rollout replicas.
    subs, ok = WeightRouter.topo_subgroups([4, 6], {4: "A", 6: "B"})
    assert ok and subs == [[4, 6]]


def test_determinism_order_independent():
    ip = {4: "A", 5: "A", 6: "B", 7: "B"}
    a, _ = WeightRouter.topo_subgroups([7, 6, 5, 4], ip)  # unsorted input
    b, _ = WeightRouter.topo_subgroups([4, 5, 6, 7], ip)
    assert a == b == [[4, 6], [5, 7]]
