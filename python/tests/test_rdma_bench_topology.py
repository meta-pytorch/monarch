#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Tests for the multi-host RDMA benchmark's topology module.

``bench_topology`` imports only the standard library, so these run in
milliseconds with no cluster, no GPU, and no torch. They are the only
automated coverage of the routing logic: a wrongly-wired peer is exactly what
the RDMA layer cannot catch, because an ``RDMAAction`` deliberately does not
track remote address ranges.
"""

from __future__ import annotations

import bench_topology as bt
import pytest


GB: int = 1000**3
MB: int = 1000**2


def _all_patterns_edges(num_hosts: int) -> dict[str, list[tuple[int, int]]]:
    return {p: bt.host_edges(p, num_hosts) for p in bt.PATTERNS}


def test_host_edges_two_hosts() -> None:
    assert _all_patterns_edges(2) == {
        "p2p": [(0, 1)],
        "fan-out": [(0, 1)],
        "fan-in": [(1, 0)],
        "all-to-all": [(0, 1), (1, 0)],
        "ring": [(0, 1), (1, 0)],
    }


def test_host_edges_four_hosts() -> None:
    edges = _all_patterns_edges(4)
    assert edges["p2p"] == [(0, 1)], "p2p ignores the extra hosts"
    assert edges["fan-out"] == [(0, 1), (0, 2), (0, 3)]
    assert edges["fan-in"] == [(1, 0), (2, 0), (3, 0)]
    assert edges["ring"] == [(0, 1), (1, 2), (2, 3), (3, 0)]
    assert len(edges["all-to-all"]) == 12, "n(n-1) for n=4"


def test_one_host_is_the_loopback_self_edge() -> None:
    for pattern in bt.PATTERNS:
        assert bt.host_edges(pattern, 1) == [(0, 0)], pattern


@pytest.mark.parametrize("num_hosts", [2, 3, 4, 8])
def test_all_to_all_is_complete_and_has_no_self_edges(num_hosts: int) -> None:
    edges = bt.host_edges("all-to-all", num_hosts)
    assert len(edges) == num_hosts * (num_hosts - 1)
    assert len(set(edges)) == len(edges)
    assert all(src != dst for src, dst in edges)


@pytest.mark.parametrize("num_hosts", [2, 3, 4, 8])
def test_ring_is_a_single_cycle_covering_every_host(num_hosts: int) -> None:
    edges = bt.host_edges("ring", num_hosts)
    assert len(edges) == num_hosts
    assert {src for src, _ in edges} == set(range(num_hosts))
    assert {dst for _, dst in edges} == set(range(num_hosts))

    # Walking successors from 0 must visit every host exactly once and return.
    successor = dict(edges)
    seen = []
    host = 0
    for _ in range(num_hosts):
        seen.append(host)
        host = successor[host]
    assert sorted(seen) == list(range(num_hosts)), "one cycle, not several"
    assert host == 0


@pytest.mark.parametrize("num_hosts", [2, 3, 4, 8])
def test_fan_out_and_fan_in_are_transposes(num_hosts: int) -> None:
    out = bt.host_edges("fan-out", num_hosts)
    into = bt.host_edges("fan-in", num_hosts)
    assert sorted((dst, src) for src, dst in out) == sorted(into)


def test_host_edges_rejects_bad_input() -> None:
    with pytest.raises(ValueError, match="unknown pattern 'torus'"):
        bt.host_edges("torus", 4)
    with pytest.raises(ValueError, match="num_hosts must be at least 1"):
        bt.host_edges("ring", 0)
    # The message names the valid set so a typo is self-correcting.
    with pytest.raises(ValueError, match="all-to-all"):
        bt.host_edges("alltoall", 4)


def test_expand_lanes_same_stays_on_one_ordinal() -> None:
    edges = bt.expand_lanes((0, 1), 4)
    assert len(edges) == 4
    assert all(e.src.lane == e.dst.lane for e in edges)
    assert edges[0] == bt.Edge(bt.Slot(0, 0), bt.Slot(1, 0))


def test_expand_lanes_shifted_crosses_ordinals() -> None:
    edges = bt.expand_lanes((0, 1), 4, bt.SHIFTED, shift=1)
    assert len(edges) == 4
    assert all(e.src.lane != e.dst.lane for e in edges)
    assert {e.dst.lane for e in edges} == {0, 1, 2, 3}, "still a permutation"

    wrapped = bt.expand_lanes((0, 1), 4, bt.SHIFTED, shift=3)
    assert bt.Edge(bt.Slot(0, 1), bt.Slot(1, 0)) in wrapped


def test_expand_lanes_all_is_the_full_product() -> None:
    edges = bt.expand_lanes((0, 1), 4, bt.ALL)
    assert len(edges) == 16
    assert len(set(edges)) == 16


def test_expand_lanes_rejects_a_shift_that_is_really_same() -> None:
    with pytest.raises(ValueError, match="multiple of procs_per_host"):
        bt.expand_lanes((0, 1), 4, bt.SHIFTED, shift=4)
    with pytest.raises(ValueError, match="multiple of procs_per_host"):
        bt.expand_lanes((0, 1), 4, bt.SHIFTED, shift=0)
    # One proc per host leaves no ordinal to shift to.
    with pytest.raises(ValueError, match="multiple of procs_per_host"):
        bt.expand_lanes((0, 1), 1, bt.SHIFTED, shift=1)


def test_expand_lanes_rejects_bad_input() -> None:
    with pytest.raises(ValueError, match="unknown pairing 'diagonal'"):
        bt.expand_lanes((0, 1), 4, "diagonal")
    with pytest.raises(ValueError, match="procs_per_host must be at least 1"):
        bt.expand_lanes((0, 1), 0)


def test_edges_are_canonical() -> None:
    topo = bt.build_topology("all-to-all", 4, 4, bt.ALL)
    assert topo.edges == tuple(sorted(set(topo.edges)))
    assert len(topo.edges) == 12 * 16


@pytest.mark.parametrize("pattern", bt.PATTERNS)
@pytest.mark.parametrize("pairing", bt.PAIRINGS)
def test_every_in_edge_comes_from_a_distinct_peer(pattern: str, pairing: str) -> None:
    """What makes keying incoming pools by the sending peer unambiguous."""
    topo = bt.build_topology(pattern, 4, 4, pairing)
    for slot in topo.slots():
        peers = [edge.src for edge in topo.in_edges(slot)]
        assert len(set(peers)) == len(peers) == topo.in_degree(slot), slot


@pytest.mark.parametrize("pattern", bt.PATTERNS)
@pytest.mark.parametrize("pairing", bt.PAIRINGS)
def test_degree_conservation(pattern: str, pairing: str) -> None:
    topo = bt.build_topology(pattern, 4, 4, pairing)
    slots = topo.slots()
    assert sum(topo.out_degree(s) for s in slots) == len(topo.edges)
    assert sum(topo.in_degree(s) for s in slots) == len(topo.edges)


def test_degree_follows_the_initiator() -> None:
    topo = bt.build_topology("fan-out", 5, 2)
    root, leaf = bt.Slot(0, 0), bt.Slot(3, 0)

    assert topo.degree(root, bt.WRITE) == 4, "the root pushes to every leaf"
    assert topo.degree(root, bt.READ) == 0, "nothing is pushed to the root"
    assert topo.degree(leaf, bt.READ) == 1, "each leaf pulls from the root"
    assert topo.degree(leaf, bt.WRITE) == 0

    assert topo.initiators(bt.WRITE) == (bt.Slot(0, 0), bt.Slot(0, 1))
    assert len(topo.initiators(bt.READ)) == 8, "4 leaf hosts x 2 procs"
    assert topo.max_degree(bt.WRITE) == 4
    assert topo.max_degree(bt.READ) == 1


def test_fan_in_mirrors_fan_out() -> None:
    topo = bt.build_topology("fan-in", 5, 2)
    root = bt.Slot(0, 0)
    assert topo.degree(root, bt.READ) == 4, "the root pulls from every leaf"
    assert topo.degree(root, bt.WRITE) == 0
    assert topo.initiators(bt.READ) == (bt.Slot(0, 0), bt.Slot(0, 1))


def test_all_to_all_and_ring_degrees() -> None:
    mesh = bt.build_topology("all-to-all", 8, 8)
    ring = bt.build_topology("ring", 8, 8)
    for direction in bt.DIRECTIONS:
        assert mesh.max_degree(direction) == 7
        assert ring.max_degree(direction) == 1
        assert len(mesh.initiators(direction)) == 64
        assert len(ring.initiators(direction)) == 64


def test_directions_are_validated() -> None:
    topo = bt.build_topology("ring", 4, 2)
    with pytest.raises(ValueError, match="unknown direction 'send'"):
        topo.initiators("send")
    with pytest.raises(ValueError, match="unknown direction"):
        topo.degree(bt.Slot(0, 0), "recv")


def test_unused_hosts() -> None:
    assert bt.unused_hosts(bt.build_topology("p2p", 8, 4)) == (2, 3, 4, 5, 6, 7)
    assert bt.unused_hosts(bt.build_topology("ring", 8, 4)) == ()


def test_allocation_only_charges_for_roles_a_slot_has() -> None:
    topo = bt.build_topology("fan-out", 4, 2)

    root = bt.allocation_for(topo, bt.Slot(0, 0), ops=3)
    assert root == bt.SlotAllocation(sends=True, receives_from=(), ops=3)
    assert root.tensors == 3, "the root never receives, so it has no incoming pool"

    leaf = bt.allocation_for(topo, bt.Slot(2, 0), ops=3)
    assert leaf == bt.SlotAllocation(sends=False, receives_from=(bt.Slot(0, 0),), ops=3)
    assert leaf.tensors == 3, "a leaf never sends, so it has no outgoing pool"

    mesh = bt.build_topology("all-to-all", 8, 8)
    every_other_host = bt.allocation_for(mesh, bt.Slot(0, 0), ops=1)
    assert every_other_host.tensors == 8, "1 outgoing + 7 incoming"
    assert len(set(every_other_host.receives_from)) == 7

    loop = bt.allocation_for(bt.build_topology("p2p", 1, 2), bt.Slot(0, 0), ops=2)
    assert loop == bt.SlotAllocation(
        sends=True, receives_from=(bt.Slot(0, 0),), ops=2
    ), "the self-edge makes a slot its own peer"

    with pytest.raises(ValueError, match="concurrent_ops must be at least 1"):
        bt.allocation_for(topo, bt.Slot(0, 0), ops=0)


def test_max_ops_per_action_scales_with_concurrent_ops() -> None:
    mesh = bt.build_topology("all-to-all", 8, 8)
    assert mesh.max_degree(bt.WRITE) == 7, "edges, not ops"
    assert bt.max_ops_per_action(mesh, bt.WRITE, ops=1) == 7
    assert bt.max_ops_per_action(mesh, bt.WRITE, ops=4) == 28

    ring = bt.build_topology("ring", 8, 8)
    assert bt.max_ops_per_action(ring, bt.READ, ops=4) == 4


def test_memory_plan_charges_each_pool_to_its_own_device() -> None:
    """All four memory-kind configs, at more than one concurrent op.

    Every slot of an 8-host all-to-all sends and receives, so with 2 ops it
    holds 2 outgoing tensors and 2 in each of 7 incoming pools: 16 in all. The
    per-host totals are eight times a slot's, one per proc.
    """
    topo = bt.build_topology("all-to-all", 8, 8)
    slot = bt.Slot(0, 0)
    kwargs = {"ops": 2, "payload_bytes": GB}

    gpu = bt.memory_footprint(topo, **kwargs, source_on_gpu=True, dest_on_gpu=True)
    assert gpu.buffers[slot] == 16, "2 outgoing + 7 incoming pools of 2"
    assert gpu.device_bytes[slot] == 16 * GB
    assert gpu.host_bytes_per_host[0] == 0

    cpu = bt.memory_footprint(topo, **kwargs, source_on_gpu=False, dest_on_gpu=False)
    assert cpu.buffers[slot] == 16, "the same tensors, charged elsewhere"
    assert cpu.device_bytes[slot] == 0
    assert cpu.host_bytes_per_host[0] == 128 * GB, "16 GB on each of 8 procs"

    cpu2gpu = bt.memory_footprint(topo, **kwargs, source_on_gpu=False, dest_on_gpu=True)
    assert cpu2gpu.device_bytes[slot] == 14 * GB, "the incoming pools only"
    assert cpu2gpu.host_bytes_per_host[0] == 16 * GB, "one outgoing pool per proc"

    gpu2cpu = bt.memory_footprint(topo, **kwargs, source_on_gpu=True, dest_on_gpu=False)
    assert gpu2cpu.device_bytes[slot] == 2 * GB, "the outgoing pool only"
    assert gpu2cpu.host_bytes_per_host[0] == 112 * GB


def test_check_memory_names_the_binding_budget_and_a_payload_that_fits() -> None:
    topo = bt.build_topology("all-to-all", 8, 8)
    budgets = {"max_device_bytes": 40 * GB, "max_host_bytes": 256 * GB}

    ok = bt.memory_footprint(
        topo, ops=1, payload_bytes=GB, source_on_gpu=True, dest_on_gpu=True
    )
    bt.check_memory(topo, ok, **budgets)

    # 8 concurrent ops puts 64 GB on one card.
    too_big = bt.memory_footprint(
        topo, ops=8, payload_bytes=GB, source_on_gpu=True, dest_on_gpu=True
    )
    with pytest.raises(ValueError) as excinfo:
        bt.check_memory(topo, too_big, **budgets)
    message = str(excinfo.value)
    assert "device memory" in message
    assert "in-degree is 7" in message
    # The slot holds 64 payload-sized tensors: 8 outgoing, plus 8 in each of the
    # 7 incoming pools. Its footprint is therefore 64 payloads, so the payload
    # that exactly fills the 40 GB budget is 40/64 GB = 625 MB.
    assert "625.00 MB" in message

    # The cpu config trips the per-host budget while every card stays empty.
    host_bound = bt.memory_footprint(
        topo, ops=6, payload_bytes=GB, source_on_gpu=False, dest_on_gpu=False
    )
    assert host_bound.device_bytes[bt.Slot(0, 0)] == 0
    with pytest.raises(ValueError, match="pinned host memory across its 8 procs"):
        bt.check_memory(topo, host_bound, **budgets)


def test_check_memory_catches_the_all_pairing_blowup() -> None:
    topo = bt.build_topology("all-to-all", 8, 8, bt.ALL)
    assert topo.in_degree(bt.Slot(0, 0)) == 56, "7 peer hosts x 8 lanes"
    footprint = bt.memory_footprint(
        topo, ops=1, payload_bytes=GB, source_on_gpu=True, dest_on_gpu=True
    )
    with pytest.raises(ValueError, match="in-degree is 56"):
        bt.check_memory(
            topo, footprint, max_device_bytes=40 * GB, max_host_bytes=256 * GB
        )


def test_byte_counts() -> None:
    topo = bt.build_topology("ring", 4, 8)
    assert bt.bytes_per_iteration(topo, ops=2, payload_bytes=MB) == 32 * 2 * MB
    assert (
        bt.initiator_bytes(topo, bt.Slot(0, 0), bt.WRITE, ops=2, payload_bytes=MB)
        == 2 * MB
    )

    mesh = bt.build_topology("all-to-all", 8, 8)
    assert bt.bytes_per_iteration(mesh, ops=1, payload_bytes=GB) == 448 * GB
    assert (
        bt.initiator_bytes(mesh, bt.Slot(3, 2), bt.READ, ops=1, payload_bytes=GB)
        == 7 * GB
    )


def _string_values(topo: bt.Topology, ops: int) -> dict[bt.Slot, bt.SlotValues]:
    """Stand-in for the RDMA buffers the actors would expose. Routing treats
    handles as opaque, so strings exercise it exactly."""
    values = {}
    for slot in topo.slots():
        allocation = bt.allocation_for(topo, slot, ops=ops)
        values[slot] = bt.SlotValues(
            outgoing=tuple(
                f"{slot}:out{op}" for op in range(ops if allocation.sends else 0)
            ),
            incoming={
                peer: tuple(f"{slot}:in[{peer}].{op}" for op in range(ops))
                for peer in allocation.receives_from
            },
        )
    return values


def test_plan_for_write_targets_the_pool_the_peer_reserved_for_it() -> None:
    topo = bt.build_topology("all-to-all", 3, 1)
    values = _string_values(topo, ops=2)
    plans = bt.plan_for(topo, bt.WRITE, values, ops=2)

    slot = bt.Slot(0, 0)
    assert set(plans) == set(topo.slots())
    assert plans[slot].pull == (), "a write plan issues no reads"

    expected = []
    for edge in topo.out_edges(slot):
        pool = values[edge.dst].incoming[slot]
        expected.extend(
            bt.PushOp(remote=pool[op], outgoing_index=op) for op in range(2)
        )
    assert plans[slot].push == tuple(expected)
    assert plans[slot].ops == 4, "2 out-edges x 2 ops"

    # Every writer into one destination must hit a different pool, which is the
    # invariant the RDMA layer cannot check: it does not track remote ranges,
    # so two initiators sharing a remote buffer would corrupt it silently.
    destination = bt.Slot(2, 0)
    remotes = [
        op.remote
        for edge in topo.in_edges(destination)
        for op in plans[edge.src].push
        if op.remote.startswith(str(destination))
    ]
    assert len(remotes) == len(set(remotes)) == 4


def test_plan_for_read_targets_the_peer_shared_outgoing_pool() -> None:
    topo = bt.build_topology("all-to-all", 3, 1)
    values = _string_values(topo, ops=2)
    plans = bt.plan_for(topo, bt.READ, values, ops=2)

    slot = bt.Slot(1, 0)
    assert plans[slot].push == (), "a read plan issues no writes"

    expected = []
    for edge in topo.in_edges(slot):
        outgoing = values[edge.src].outgoing
        expected.extend(
            bt.PullOp(remote=outgoing[op], peer=edge.src, op_index=op)
            for op in range(2)
        )
    assert plans[slot].pull == tuple(expected)

    # Every read lands somewhere different: overlapping local write claims in
    # one action are a hard error in the RDMA layer.
    landings = {(op.peer, op.op_index) for op in plans[slot].pull}
    assert len(landings) == plans[slot].ops


@pytest.mark.parametrize("pattern", bt.PATTERNS)
@pytest.mark.parametrize("direction", bt.DIRECTIONS)
def test_plan_op_count_matches_degree(pattern: str, direction: str) -> None:
    topo = bt.build_topology(pattern, 4, 2)
    values = _string_values(topo, ops=3)
    for slot, plan in bt.plan_for(topo, direction, values, ops=3).items():
        assert plan.ops == topo.degree(slot, direction) * 3, slot


def test_plan_for_the_self_edge_wires_a_slot_to_itself() -> None:
    topo = bt.build_topology("p2p", 1, 2)
    values = _string_values(topo, ops=1)
    slot = bt.Slot(0, 1)

    write = bt.plan_for(topo, bt.WRITE, values, ops=1)[slot]
    assert write.push == (bt.PushOp(remote=f"{slot}:in[{slot}].0", outgoing_index=0),)

    read = bt.plan_for(topo, bt.READ, values, ops=1)[slot]
    assert read.pull == (bt.PullOp(remote=f"{slot}:out0", peer=slot, op_index=0),)


def test_plan_for_rejects_a_bad_direction() -> None:
    topo = bt.build_topology("ring", 4, 2)
    with pytest.raises(ValueError, match="unknown direction"):
        bt.plan_for(topo, "push", _string_values(topo, ops=1), ops=1)


def _digests(
    topo: bt.Topology, ops: int, *, transferred: bool
) -> dict[bt.Slot, bt.SlotValues]:
    """Digests as they would look before (``transferred=False``) and after a
    successful round. An incoming pool either mirrors the peer that fills it or
    still holds the zero-fill."""
    digests = {}
    for slot in topo.slots():
        allocation = bt.allocation_for(topo, slot, ops=ops)
        digests[slot] = bt.SlotValues(
            outgoing=tuple(
                f"digest-{slot}-{op}" for op in range(ops if allocation.sends else 0)
            ),
            incoming={
                peer: (
                    tuple(f"digest-{peer}-{op}" for op in range(ops))
                    if transferred
                    else tuple("zero" for _ in range(ops))
                )
                for peer in allocation.receives_from
            },
        )
    return digests


@pytest.mark.parametrize("pattern", bt.PATTERNS)
def test_compare_digests_accepts_a_clean_round(pattern: str) -> None:
    topo = bt.build_topology(pattern, 4, 2)
    checked, mismatches = bt.compare_digests(topo, _digests(topo, 2, transferred=True))
    assert mismatches == []
    assert checked == len(topo.edges) * 2


@pytest.mark.parametrize("pattern", bt.PATTERNS)
def test_negative_control_every_pair_mismatches_before_a_transfer(
    pattern: str,
) -> None:
    topo = bt.build_topology(pattern, 4, 2)
    checked, mismatches = bt.compare_digests(topo, _digests(topo, 2, transferred=False))
    assert len(mismatches) == checked, "the check must be able to fail"


def test_compare_digests_catches_two_swapped_incoming_pools() -> None:
    topo = bt.build_topology("all-to-all", 4, 1)
    digests = _digests(topo, 1, transferred=True)

    slot = bt.Slot(2, 0)
    held = digests[slot]
    first, second = sorted(held.incoming)[:2]
    swapped = dict(held.incoming)
    swapped[first], swapped[second] = swapped[second], swapped[first]
    digests[slot] = bt.SlotValues(outgoing=held.outgoing, incoming=swapped)

    _checked, mismatches = bt.compare_digests(topo, digests)
    assert len(mismatches) == 2, "both swapped edges now disagree"
    assert str(slot) in mismatches[0]
    assert "op 0" in mismatches[0]


def test_compare_digests_catches_an_untouched_incoming_pool() -> None:
    topo = bt.build_topology("ring", 4, 1)
    digests = _digests(topo, 2, transferred=True)
    slot = bt.Slot(1, 0)
    held = digests[slot]
    peer = next(iter(held.incoming))
    digests[slot] = bt.SlotValues(
        outgoing=held.outgoing, incoming={peer: ("zero", "zero")}
    )
    _checked, mismatches = bt.compare_digests(topo, digests)
    assert len(mismatches) == 2


def test_phase_of() -> None:
    # The cold iteration takes the place of the first ramp iteration, so a run
    # is `warmup` + `warm` iterations long whichever run it is.
    assert [bt.phase_of(0, i, warmup=3) for i in range(5)] == [
        bt.COLD_QP,
        bt.RAMP,
        bt.RAMP,
        bt.WARM,
        bt.WARM,
    ]
    # Only the very first iteration of all is cold: every later run opens on a
    # discarded iteration, since its queue pairs are already up.
    assert [bt.phase_of(1, i, warmup=3) for i in range(5)] == [
        bt.RAMP,
        bt.RAMP,
        bt.RAMP,
        bt.WARM,
        bt.WARM,
    ]
    # With no ramp at all the cold iteration takes a warm slot instead, so the
    # first run reports one fewer warm iteration than the others.
    assert [bt.phase_of(0, i, warmup=0) for i in range(3)] == [
        bt.COLD_QP,
        bt.WARM,
        bt.WARM,
    ]
    assert [bt.phase_of(2, i, warmup=0) for i in range(3)] == [
        bt.WARM,
        bt.WARM,
        bt.WARM,
    ]
    assert bt.RAMP not in bt.PHASES, "ramp samples are discarded, never reported"


def test_describe_mentions_the_shape() -> None:
    line = bt.describe(
        bt.build_topology("fan-out", 4, 8), bt.WRITE, ops=2, payload_bytes=GB
    )
    assert "fan-out" in line
    assert "4x8 procs" in line
    assert "24 edges" in line
    assert "same lanes" in line
    assert "max ops/action=6" in line, "3 out-edges x 2 concurrent ops"
    assert "48.00 GB per iteration" in line
