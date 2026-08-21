#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
End-to-end tests for the multi-host RDMA benchmark's participant actor.

These drive the sequence the driver will -- ``setup``, ``wire``,
``execute_iteration``, ``digest`` -- against real registered buffers
under ibverbs and tcp.

A single host is enough. The ``shifted`` and ``all`` lane pairings turn a
self-edge into genuine proc-to-proc edges, and ``all`` gives every slot several
peers, which is where per-peer routing can go wrong.
"""

import os

# More efficient RDMA support for CUDA tensors.
os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")

import bench_peer  # noqa: E402
import bench_topology as bt  # noqa: E402
import pytest  # noqa: E402
import torch  # noqa: E402
from monarch.actor import this_host  # noqa: E402
from monarch.config import get_global_config  # noqa: E402
from proc_mesh_test_utils import stop_all_proc_meshes  # noqa: E402, F401
from rdma_test_utils import rdma_backends  # noqa: E402


_LANES = 2
_OPS = 2
_PAYLOAD_BYTES = 4096
_SEED = 7

# The benchmark's four memory configurations, named as the CLI names them.
DEVICE_VARIANTS: list[tuple[bool, bool]] = [
    (False, False),
    (True, True),
    (False, True),
    (True, False),
]
DEVICE_IDS = ["cpu", "gpu", "cpu2gpu", "gpu2cpu"]


def _slot_of(point) -> bt.Slot:
    """Which slot a value in a cast's result came from."""
    host = point["hosts"] if "hosts" in point.extent.labels else 0
    return bt.Slot(host, point["lanes"])


def _spawn(lanes: int = _LANES):
    procs = this_host().spawn_procs(per_host={"lanes": lanes})
    return procs.spawn("peer", bench_peer.Peer)


async def _drive(peers, topo, direction, *, source_on_gpu=False, dest_on_gpu=False):
    """Allocate and register everywhere, then route and install the plans."""
    allocations = {
        slot: bt.allocation_for(topo, slot, ops=_OPS) for slot in topo.slots()
    }
    results = await peers.setup.call(
        allocations, _PAYLOAD_BYTES, _SEED, source_on_gpu, dest_on_gpu
    )
    buffers = {_slot_of(point): value[1] for point, value in results.items()}
    await peers.wire.call(bt.plan_for(topo, direction, buffers, ops=_OPS))
    return results


async def _compare(peers, topo):
    digests = {
        _slot_of(point): value
        for point, value in (
            await peers.digest.call(bench_peer.VERIFY_FULL, _PAYLOAD_BYTES)
        ).items()
    }
    return bt.compare_digests(topo, digests)


def _skip_without_cuda(source_on_gpu: bool, dest_on_gpu: bool) -> None:
    if (source_on_gpu or dest_on_gpu) and not torch.cuda.is_available():
        pytest.skip("CUDA not available")


@rdma_backends
async def test_a_peer_reports_the_transport_its_configuration_selects() -> None:
    # The rdma_backends decorator skips the test if rdma_disable_ibverbs == False
    # and ibverbs is unavailable, and always enables tcp fallback when ibverbs is
    # disabled, so this computation for `expected` is correct.
    expected = "tcp" if get_global_config()["rdma_disable_ibverbs"] else "ibverbs"
    peers = _spawn(lanes=1)

    reported = {value for _point, value in (await peers.transport.call()).items()}

    assert reported == {expected}


@rdma_backends
@pytest.mark.parametrize("direction", bt.DIRECTIONS)
@pytest.mark.parametrize(
    ("source_on_gpu", "dest_on_gpu"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_every_edge_delivers_its_own_bytes(
    direction, source_on_gpu, dest_on_gpu
) -> None:
    """Each slot ends up holding exactly what its peers sent it.

    Under ``all`` pairing every slot both sends and receives on several edges,
    so this fails if any op is routed to the wrong peer's memory -- the failure
    the RDMA layer cannot detect for itself.
    """
    _skip_without_cuda(source_on_gpu, dest_on_gpu)
    topo = bt.build_topology("p2p", 1, _LANES, bt.ALL)
    peers = _spawn()
    await _drive(
        peers, topo, direction, source_on_gpu=source_on_gpu, dest_on_gpu=dest_on_gpu
    )

    # Nothing has moved yet, so every pair must disagree. Without this the test
    # would still pass if the comparison were somehow against itself.
    checked, mismatches = await _compare(peers, topo)
    assert checked == len(topo.edges) * _OPS
    assert len(mismatches) == checked, "incoming tensors start zeroed"

    samples = await peers.execute_iteration.call()
    assert all(sample is not None for _point, sample in samples.items())

    _checked, mismatches = await _compare(peers, topo)
    assert mismatches == []


@rdma_backends
@pytest.mark.parametrize("pairing", bt.PAIRINGS)
async def test_every_lane_pairing_routes_correctly(pairing) -> None:
    """``same`` keeps a slot talking to itself; ``shifted`` and ``all`` cross
    proc boundaries, which is what makes this more than a loopback test."""
    topo = bt.build_topology("p2p", 1, _LANES, pairing)
    crosses = [edge for edge in topo.edges if edge.src != edge.dst]
    assert bool(crosses) == (pairing != bt.SAME), "the pairing does what it says"

    peers = _spawn()
    await _drive(peers, topo, bt.WRITE)
    await peers.execute_iteration.call()

    _checked, mismatches = await _compare(peers, topo)
    assert mismatches == []


@rdma_backends
async def test_a_proc_outside_the_topology_stays_idle() -> None:
    """A pattern need not use every proc the job provisioned. Those procs are
    absent from the mappings, allocate nothing, and report no measurement."""
    topo = bt.build_topology("p2p", 1, _LANES, bt.ALL)
    peers = _spawn(lanes=_LANES + 2)
    await _drive(peers, topo, bt.WRITE)

    samples = {
        _slot_of(point): sample
        for point, sample in (await peers.execute_iteration.call()).items()
    }
    assert set(topo.slots()) < set(samples), "more procs than the topology uses"
    for slot, sample in samples.items():
        if slot in topo.slots():
            assert sample is not None and sample.slot == slot
        else:
            assert sample is None, f"{slot} initiates nothing"

    _checked, mismatches = await _compare(peers, topo)
    assert mismatches == []


@rdma_backends
async def test_a_run_can_be_repeated_against_fresh_buffers() -> None:
    """What repeating a run relies on: setup reports its registration cost and
    can be called again, releasing the previous buffers as it goes."""
    topo = bt.build_topology("p2p", 1, _LANES, bt.ALL)
    peers = _spawn()

    for _run in range(2):
        results = await _drive(peers, topo, bt.WRITE)
        for _point, (register_ms, buffers) in results.items():
            assert register_ms > 0.0, "registering real buffers takes measurable time"
            expected = _OPS * (1 + _LANES)
            assert len(list(buffers.flat())) == expected, "1 outgoing + 2 incoming"
        await peers.execute_iteration.call()
        _checked, mismatches = await _compare(peers, topo)
        assert mismatches == []


def test_fill_seeds_are_unique_per_run_and_slot() -> None:
    """What makes a wrongly-routed transfer visible at all: two slots sharing a
    seed would fill identical bytes, and a digest comparison would accept the
    mix-up."""
    seeds = [
        bench_peer._fill_seed(run, bt.Slot(host, lane))
        for run in range(8)
        for host in range(16)
        for lane in range(8)
    ]
    assert len(set(seeds)) == len(seeds)
    assert all(0 <= seed <= 0x7FFF_FFFF for seed in seeds), "a valid torch seed"
