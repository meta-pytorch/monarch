#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Transfer topologies for the multi-host RDMA benchmark.

A benchmark configuration is a directed graph whose vertices are procs. A
:py:class:`Slot` is one proc, identified by its logical host index and its
*lane* -- the proc's ordinal within its host, which for a GPU run is also its
CUDA device ordinal. An :py:class:`Edge` means bytes move from the source
slot's memory into the destination slot's memory. The graph is built in two
independent steps: :py:func:`host_edges` turns a pattern name and a host count
into a host graph, and :py:func:`expand_lanes` realizes each host edge as proc
edges under a lane-pairing rule.

Which side *initiates* the ibverbs ops is a separate axis. Under ``"write"``
the source pushes into the destination's buffers; under ``"read"`` the
destination pulls from the source's buffers. Both move the same bytes over the
same edges, so a pattern and a direction compose freely.

Memory follows from the graph, and :py:class:`SlotValues` is its shape. A slot
that sends allocates one *outgoing* pool of ``concurrent_ops`` tensors shared by
all of its out-edges, which is legal because an ``RDMAAction`` treats a
write-from-local as a *read* claim on local memory and merges overlapping read
claims. A slot that receives allocates one *incoming* pool per peer that sends
to it, which is mandatory: an ``RDMAAction`` does not track remote ranges, so
two initiators writing one remote buffer would corrupt it silently. Since an
edge is exactly a pair of slots, incoming pools are keyed by the sending peer
and no separate index is needed. :py:func:`allocation_for` is the single source
of truth for what a slot allocates, so what the actors allocate and what
:py:func:`plan_memory` charges them for cannot drift.

This module imports only the standard library. It holds no monarch, torch, or
RDMA state, and every function in it is pure, so the whole benchmark's
topology, routing, resource, and validation logic is testable without a
cluster. Buffer handles are opaque: :py:func:`plan_for` and
:py:func:`compare_digests` never look inside them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


P2P: str = "p2p"
FAN_OUT: str = "fan-out"
FAN_IN: str = "fan-in"
ALL_TO_ALL: str = "all-to-all"
RING: str = "ring"
PATTERNS: tuple[str, ...] = (P2P, FAN_OUT, FAN_IN, ALL_TO_ALL, RING)

READ: str = "read"
WRITE: str = "write"
DIRECTIONS: tuple[str, ...] = (READ, WRITE)

# How a host edge becomes proc edges. `same` pairs lane k with lane k, so a
# transfer never leaves one pair of device ordinals. `shifted` pairs lane k with
# lane k+shift, and `all` pairs every lane with every lane; both cross ordinals,
# so whether they can connect at all depends on the cluster's NIC topology.
SAME: str = "same"
SHIFTED: str = "shifted"
ALL: str = "all"
PAIRINGS: tuple[str, ...] = (SAME, SHIFTED, ALL)

COLD_QP: str = "cold_qp"
RAMP: str = "ramp"
WARM: str = "warm"

# The phases the benchmark reports. `RAMP` is deliberately absent: those
# iterations are discarded so that settling never lands in a warm percentile.
PHASES: tuple[str, ...] = (COLD_QP, WARM)


@dataclass(frozen=True, order=True)
class Slot:
    """One benchmark proc: its logical host index and its lane within that host."""

    host: int
    lane: int

    def __str__(self) -> str:
        return f"h{self.host}/l{self.lane}"


@dataclass(frozen=True, order=True)
class Edge:
    """One directed flow. Bytes move src to dst whichever side initiates."""

    src: Slot
    dst: Slot

    def __str__(self) -> str:
        return f"{self.src} -> {self.dst}"


def host_edges(pattern: str, num_hosts: int) -> list[tuple[int, int]]:
    """The directed host graph for ``pattern`` over ``num_hosts`` hosts.

    One host yields the loopback self-edge, whatever the pattern, which is how
    the benchmark expresses a same-host transfer.

    Above one host, ``p2p`` always uses hosts 0 and 1 and leaves the rest idle.
    That is intentional: it keeps p2p available as a baseline in a sweep that
    provisions enough hosts for the other patterns.
    """
    if pattern not in PATTERNS:
        raise ValueError(f"unknown pattern {pattern!r}; expected one of {PATTERNS}")
    if num_hosts < 1:
        raise ValueError(f"num_hosts must be at least 1, got {num_hosts}")
    if num_hosts == 1:
        return [(0, 0)]
    match pattern:
        case "p2p":
            return [(0, 1)]
        case "fan-out":
            return [(0, dst) for dst in range(1, num_hosts)]
        case "fan-in":
            return [(src, 0) for src in range(1, num_hosts)]
        case "all-to-all":
            return [
                (src, dst)
                for src in range(num_hosts)
                for dst in range(num_hosts)
                if src != dst
            ]
        case _:
            return [(src, (src + 1) % num_hosts) for src in range(num_hosts)]


def expand_lanes(
    host_edge: tuple[int, int],
    procs_per_host: int,
    pairing: str = SAME,
    shift: int = 1,
) -> list[Edge]:
    """Realize one host edge as proc edges under a lane-pairing rule."""
    if pairing not in PAIRINGS:
        raise ValueError(f"unknown pairing {pairing!r}; expected one of {PAIRINGS}")
    if procs_per_host < 1:
        raise ValueError(f"procs_per_host must be at least 1, got {procs_per_host}")
    src_host, dst_host = host_edge
    lanes = range(procs_per_host)
    if pairing == ALL:
        return [
            Edge(Slot(src_host, src), Slot(dst_host, dst))
            for src in lanes
            for dst in lanes
        ]
    if pairing == SAME:
        return [Edge(Slot(src_host, lane), Slot(dst_host, lane)) for lane in lanes]
    if shift % procs_per_host == 0:
        raise ValueError(
            f"shift {shift} is a multiple of procs_per_host {procs_per_host}, so "
            f"{SHIFTED!r} pairing would silently be {SAME!r}"
        )
    return [
        Edge(Slot(src_host, lane), Slot(dst_host, (lane + shift) % procs_per_host))
        for lane in lanes
    ]


@dataclass(frozen=True)
class Topology:
    """A benchmark configuration's proc graph, with its per-slot edge indices.

    Build with :py:func:`build_topology` rather than directly, so ``edges`` is
    sorted and deduplicated.
    """

    pattern: str
    num_hosts: int
    procs_per_host: int
    pairing: str
    shift: int
    edges: tuple[Edge, ...]

    _out: dict[Slot, tuple[Edge, ...]] = field(init=False, repr=False, compare=False)
    _in: dict[Slot, tuple[Edge, ...]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        out: dict[Slot, list[Edge]] = {}
        incoming: dict[Slot, list[Edge]] = {}
        for edge in self.edges:
            out.setdefault(edge.src, []).append(edge)
            incoming.setdefault(edge.dst, []).append(edge)
        object.__setattr__(self, "_out", {s: tuple(e) for s, e in out.items()})
        object.__setattr__(self, "_in", {s: tuple(e) for s, e in incoming.items()})

    def slots(self) -> tuple[Slot, ...]:
        """Every slot the graph touches, sorted. Idle procs are absent."""
        return tuple(sorted(set(self._out) | set(self._in)))

    def out_edges(self, slot: Slot) -> tuple[Edge, ...]:
        """The edges ``slot`` sends on, sorted."""
        return self._out.get(slot, ())

    def in_edges(self, slot: Slot) -> tuple[Edge, ...]:
        """The edges ``slot`` receives on, sorted."""
        return self._in.get(slot, ())

    def out_degree(self, slot: Slot) -> int:
        return len(self.out_edges(slot))

    def in_degree(self, slot: Slot) -> int:
        return len(self.in_edges(slot))

    def initiators(self, direction: str) -> tuple[Slot, ...]:
        """The slots that issue ibverbs ops: sources under write, destinations
        under read."""
        _check_direction(direction)
        return tuple(sorted(self._out if direction == WRITE else self._in))

    def degree(self, slot: Slot, direction: str) -> int:
        """How many edges ``slot`` initiates on under ``direction``.

        Also the number of active, polling queue-pair actors the slot's proc
        drives, since a queue pair is created lazily by the initiator and the
        target's mirror is never polled.
        """
        _check_direction(direction)
        return self.out_degree(slot) if direction == WRITE else self.in_degree(slot)

    def max_degree(self, direction: str) -> int:
        """The largest number of edges any one slot initiates on."""
        return max((self.degree(s, direction) for s in self.slots()), default=0)


def build_topology(
    pattern: str,
    num_hosts: int,
    procs_per_host: int,
    pairing: str = SAME,
    shift: int = 1,
) -> Topology:
    """Build the proc graph for one benchmark configuration."""
    edges = tuple(
        sorted(
            {
                edge
                for host_edge in host_edges(pattern, num_hosts)
                for edge in expand_lanes(host_edge, procs_per_host, pairing, shift)
            }
        )
    )
    return Topology(
        pattern=pattern,
        num_hosts=num_hosts,
        procs_per_host=procs_per_host,
        pairing=pairing,
        shift=shift,
        edges=edges,
    )


def _check_direction(direction: str) -> None:
    if direction not in DIRECTIONS:
        raise ValueError(
            f"unknown direction {direction!r}; expected one of {DIRECTIONS}"
        )


@dataclass(frozen=True)
class SlotAllocation:
    """What one slot must allocate.

    ``sends`` is false for a slot with no out-edges -- a fan-out leaf -- which
    therefore pays for its incoming pools only. A sending slot shares one
    outgoing pool across every out-edge, so one is always enough.
    """

    sends: bool
    receives_from: tuple[Slot, ...]
    ops: int

    @property
    def tensors(self) -> int:
        """Total tensors, each of which is registered as one RDMA buffer."""
        return (self.ops if self.sends else 0) + len(self.receives_from) * self.ops


def allocation_for(topo: Topology, slot: Slot, *, ops: int) -> SlotAllocation:
    """What ``slot`` allocates. Independent of direction, so one allocation
    serves both a read pass and a write pass."""
    if ops < 1:
        raise ValueError(f"concurrent_ops must be at least 1, got {ops}")
    return SlotAllocation(
        sends=topo.out_degree(slot) > 0,
        receives_from=tuple(edge.src for edge in topo.in_edges(slot)),
        ops=ops,
    )


def max_ops_per_action(topo: Topology, direction: str, *, ops: int) -> int:
    """The most ops any one initiator batches into a single ``RDMAAction``:
    one per out-edge (or in-edge) per concurrent op."""
    return topo.max_degree(direction) * ops


@dataclass(frozen=True)
class MemoryPlan:
    """One configuration's footprint, split by memory kind.

    Device memory is charged per slot because each slot owns one GPU; host
    memory is charged per host because the procs on a host share it. Unused
    hosts and slots have no entry in the plan.
    """

    payload_bytes: int
    buffers: dict[Slot, int]
    device_bytes: dict[Slot, int]
    host_bytes_per_host: dict[int, int]

    @property
    def total_bytes(self) -> int:
        return sum(self.device_bytes.values()) + sum(self.host_bytes_per_host.values())


def plan_memory(
    topo: Topology,
    *,
    ops: int,
    payload_bytes: int,
    source_on_gpu: bool,
    dest_on_gpu: bool,
) -> MemoryPlan:
    """Charge every slot's tensors to device or host memory by their role."""
    buffers: dict[Slot, int] = {}
    device: dict[Slot, int] = {}
    host: dict[int, int] = {slot.host: 0 for slot in topo.slots()}
    for slot in topo.slots():
        allocation = allocation_for(topo, slot, ops=ops)
        outgoing = (ops if allocation.sends else 0) * payload_bytes
        incoming = len(allocation.receives_from) * ops * payload_bytes
        buffers[slot] = allocation.tensors
        device[slot] = (outgoing if source_on_gpu else 0) + (
            incoming if dest_on_gpu else 0
        )
        host[slot.host] += (0 if source_on_gpu else outgoing) + (
            0 if dest_on_gpu else incoming
        )
    return MemoryPlan(
        payload_bytes=payload_bytes,
        buffers=buffers,
        device_bytes=device,
        host_bytes_per_host=host,
    )


def check_memory(
    topo: Topology,
    plan: MemoryPlan,
    *,
    max_device_bytes: int,
    max_host_bytes: int,
) -> None:
    """Raise before anything is provisioned if the footprint will not fit.

    Names the binding slot or host, its in-degree, and the largest payload
    that would fit, so a bad flag combination fails on the client in
    milliseconds instead of part-way into a multi-host allocation.
    """
    slot, device_bytes = max(plan.device_bytes.items(), key=lambda kv: kv[1])
    if device_bytes > max_device_bytes:
        raise ValueError(
            f"{topo.pattern}: slot {slot} needs {_size(device_bytes)} of device "
            f"memory, over the {_size(max_device_bytes)} budget. Its in-degree is "
            f"{topo.in_degree(slot)}, its out-degree is {topo.out_degree(slot)}, "
            f"and the payload is {_size(plan.payload_bytes)}; the largest payload "
            "that fits is "
            f"{_size(_fitting_payload(plan.payload_bytes, device_bytes, max_device_bytes))}"
        )
    host, host_bytes = max(plan.host_bytes_per_host.items(), key=lambda kv: kv[1])
    if host_bytes > max_host_bytes:
        raise ValueError(
            f"{topo.pattern}: host {host} needs {_size(host_bytes)} of pinned host "
            f"memory across its {topo.procs_per_host} procs, over the "
            f"{_size(max_host_bytes)} budget. The payload is "
            f"{_size(plan.payload_bytes)}; the largest payload that fits is "
            f"{_size(_fitting_payload(plan.payload_bytes, host_bytes, max_host_bytes))}"
        )


def _fitting_payload(payload_bytes: int, needed: int, budget: int) -> int:
    """The largest payload that would not exceed ``budget``.

    A configuration's footprint is proportional to its payload, so scaling
    ``payload_bytes`` by ``budget / needed`` lands on the budget.
    """
    return payload_bytes * budget // needed


def _size(num_bytes: int) -> str:
    """Bytes in the largest decimal unit that keeps the number legible."""
    for unit, scale in (("TB", 10**12), ("GB", 10**9), ("MB", 10**6), ("KB", 10**3)):
        if num_bytes >= scale:
            return f"{num_bytes / scale:.2f} {unit}"
    return f"{num_bytes} B"


def bytes_per_iteration(topo: Topology, *, ops: int, payload_bytes: int) -> int:
    """Bytes the whole graph moves in one iteration -- the numerator of the
    aggregate throughput."""
    return len(topo.edges) * ops * payload_bytes


def initiator_bytes(
    topo: Topology, slot: Slot, direction: str, *, ops: int, payload_bytes: int
) -> int:
    """Bytes one initiator moves in one iteration."""
    return topo.degree(slot, direction) * ops * payload_bytes


@dataclass(frozen=True)
class SlotValues:
    """One value per tensor one slot holds, grouped by role and keyed by peer.

    ``outgoing`` covers the pool every out-edge sends from; ``incoming`` holds
    one pool per peer that sends to this slot. The values are opaque: RDMA
    buffers when the driver collects them from the actors, digests when it
    collects them to check integrity.
    """

    outgoing: tuple[Any, ...]
    incoming: dict[Slot, tuple[Any, ...]]


@dataclass(frozen=True)
class PushOp:
    """Write ``outgoing[outgoing_index]`` into a remote buffer."""

    remote: Any
    outgoing_index: int


@dataclass(frozen=True)
class PullOp:
    """Read a remote buffer into ``incoming[peer][op_index]``."""

    remote: Any
    peer: Slot
    op_index: int


@dataclass(frozen=True)
class InitiatorPlan:
    """Every op one slot issues in one iteration.

    A flat list, so the actor's timed loop reads this and nothing else: it
    never learns what a pattern, a direction, or a topology is.
    """

    push: tuple[PushOp, ...] = ()
    pull: tuple[PullOp, ...] = ()

    @property
    def ops(self) -> int:
        return len(self.push) + len(self.pull)


def plan_for(
    topo: Topology,
    direction: str,
    values: Mapping[Slot, SlotValues],
    *,
    ops: int,
) -> dict[Slot, InitiatorPlan]:
    """Route every edge to the ops its initiator must issue.

    Under ``write`` an initiator targets the peer's incoming pool *reserved for
    it*; under ``read`` it targets the peer's shared outgoing pool. Slots that
    initiate nothing are absent from the result.
    """
    _check_direction(direction)
    return {
        slot: (
            _push_plan(topo, slot, values, ops)
            if direction == WRITE
            else _pull_plan(topo, slot, values, ops)
        )
        for slot in topo.initiators(direction)
    }


def _push_plan(
    topo: Topology, slot: Slot, values: Mapping[Slot, SlotValues], ops: int
) -> InitiatorPlan:
    push: list[PushOp] = []
    for edge in topo.out_edges(slot):
        incoming = values[edge.dst].incoming[edge.src]
        if len(incoming) != ops:
            raise ValueError(
                f"{edge}: incoming pool has {len(incoming)} tensors, expected {ops}"
            )
        push.extend(PushOp(remote=incoming[op], outgoing_index=op) for op in range(ops))
    return InitiatorPlan(push=tuple(push))


def _pull_plan(
    topo: Topology, slot: Slot, values: Mapping[Slot, SlotValues], ops: int
) -> InitiatorPlan:
    pull: list[PullOp] = []
    for edge in topo.in_edges(slot):
        outgoing = values[edge.src].outgoing
        if len(outgoing) != ops:
            raise ValueError(
                f"{edge}: outgoing pool has {len(outgoing)} tensors, expected {ops}"
            )
        pull.extend(
            PullOp(remote=outgoing[op], peer=edge.src, op_index=op) for op in range(ops)
        )
    return InitiatorPlan(pull=tuple(pull))


def compare_digests(
    topo: Topology, digests: Mapping[Slot, SlotValues]
) -> tuple[int, list[str]]:
    """Compare each edge's incoming pool against the pool it was sent from.

    Returns ``(pairs_checked, mismatches)``. After a round every pair must
    match. Before any transfer every pair must *mismatch*, which is the
    negative control that proves the comparison is wired to the right slots.
    """
    checked = 0
    mismatches: list[str] = []
    for edge in topo.edges:
        outgoing = digests[edge.src].outgoing
        incoming = digests[edge.dst].incoming[edge.src]
        if len(outgoing) != len(incoming):
            raise ValueError(
                f"{edge}: sender has {len(outgoing)} digests, receiver has {len(incoming)}"
            )
        for op, (want, got) in enumerate(zip(outgoing, incoming)):
            checked += 1
            if want != got:
                mismatches.append(f"{edge} op {op}: sent={want} received={got}")
    return checked, mismatches


def phase_of(run_index: int, iteration: int, warmup: int) -> str:
    """Which reporting phase an iteration belongs to.

    Iteration 0 of the first run is ``cold_qp``: it pays the lazy queue-pair
    handshake to every peer. A run's first ``warmup`` iterations are ``ramp``
    and are discarded; the rest are ``warm``.
    """
    if run_index == 0 and iteration == 0:
        return COLD_QP
    return RAMP if iteration < warmup else WARM


def describe(topo: Topology, direction: str, *, ops: int, payload_bytes: int) -> str:
    """One line naming the graph and what each initiator will do on it."""
    return (
        f"{topo.pattern}: {topo.num_hosts}x{topo.procs_per_host} procs, "
        f"{len(topo.edges)} edges, {topo.pairing} lanes, {direction}; "
        f"initiators={len(topo.initiators(direction))}, "
        f"max ops/action={max_ops_per_action(topo, direction, ops=ops)}, "
        f"{_size(bytes_per_iteration(topo, ops=ops, payload_bytes=payload_bytes))} "
        f"per iteration"
    )


def unused_hosts(topo: Topology) -> Sequence[int]:
    """Hosts the job provisions that this pattern never touches."""
    touched = {slot.host for slot in topo.slots()}
    return tuple(host for host in range(topo.num_hosts) if host not in touched)
