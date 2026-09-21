# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Manual 3-node integration test for overlapping topology-aware replication groups.

Launch 8 tasks/node: node 0 is eight trainer senders; nodes 1 and 2 are the 16 rollout receivers.
The receiver layout is the hand-crafted 11-group case used by ``test_arena_planner``:

* one tensor replicated by all 16 workers;
* one tensor with eight same-rank pairs across rollout nodes; and
* one tensor with two half-GPU groups.

This driver uses real :class:`WeightSender` / :class:`WeightReceiver` data paths but bypasses their
application control adapters. It is intentionally not pytest-collected; it requires 24 GPUs on 3 nodes.
"""

from __future__ import annotations

import os
import queue
import threading

import torch
import torch.distributed as dist
from wbridge.backend.receiver import WeightReceiver
from wbridge.backend.sender import WeightSender
from wbridge.utils.data import LoadSpec, shards_numel, ShardSpec


SENDERS = 8
RECEIVER_NODES = int(os.environ.get("WBRIDGE_TEST_RECEIVER_NODES", "2"))
if RECEIVER_NODES < 2:
    raise ValueError(
        f"WBRIDGE_TEST_RECEIVER_NODES must be at least 2, got {RECEIVER_NODES}"
    )
RECEIVERS = 8 * RECEIVER_NODES
WORLD = SENDERS + RECEIVERS
NAME_COPIES = int(os.environ.get("WBRIDGE_TEST_NAME_COPIES", "1"))
if NAME_COPIES <= 0:
    raise ValueError(f"WBRIDGE_TEST_NAME_COPIES must be positive, got {NAME_COPIES}")
NAMES = tuple(
    kind if NAME_COPIES == 1 else f"{kind}.{index:03d}"
    for kind in ("half", "pair", "wide")
    for index in range(NAME_COPIES)
)
WIDTH = int(os.environ.get("WBRIDGE_TEST_WIDTH", "160"))
if WIDTH <= 0 or WIDTH % 8:
    raise ValueError(
        f"WBRIDGE_TEST_WIDTH must be a positive multiple of 8, got {WIDTH}"
    )


def name_dtype(name: str) -> torch.dtype:
    """Optional mixed wire layout matching GLM-5's BF16 weights and FP32 vectors."""
    if os.environ.get("WBRIDGE_TEST_MIXED_DTYPE") == "1":
        return torch.float32 if name.startswith("pair") else torch.bfloat16
    return torch.float32


def d1(left: int, right: int, width: int):
    return [[(left, right, width)]]


def receiver_spec(worker: int) -> ShardSpec:
    lane = worker % 8
    half = 0 if lane < 4 else 1
    entries = {}
    for name in NAMES:
        if name.startswith("half"):
            entries[name] = d1(
                half * (WIDTH // 2),
                (half + 1) * (WIDTH // 2),
                WIDTH,
            )
        elif name.startswith("pair"):
            entries[name] = d1(
                lane * (WIDTH // 8),
                (lane + 1) * (WIDTH // 8),
                WIDTH,
            )
        else:
            assert name.startswith("wide")
            entries[name] = d1(0, WIDTH, WIDTH)
    return ShardSpec(entries)


def sender_spec() -> ShardSpec:
    return ShardSpec({name: d1(0, WIDTH, WIDTH) for name in NAMES})


def identity_side(spec: ShardSpec, *, sender: bool):
    """Build a LoadSpec mapping each global source shard to one flat local model tensor."""
    entries = {}
    wksd = {}
    for ni, (name, shards) in enumerate(spec):
        assert len(shards) == 1
        numel = shards_numel(shards)
        local = [(0, numel, numel)]
        entries[name] = {name: [(shards[0], local)]}
        if sender:
            left = shards[0][0][0]
            # Build the value from the integer global coordinate before casting.  A floating-point
            # ``arange(0, N)[left:]`` is not bitwise equivalent to ``arange(left, N)`` once ``left``
            # exceeds 2**24, so the old oracle produced false mismatches in large-offset tests.
            values = torch.arange(
                left,
                left + numel,
                dtype=torch.int32,
                device="cuda",
            )
            values.remainder_(251).add_(ni * 1000)
            wksd[name] = values.to(name_dtype(name))
        else:
            wksd[name] = torch.full(
                (numel,),
                -1,
                dtype=name_dtype(name),
                device="cuda",
            )
    return LoadSpec(entries), wksd


def make_sender(
    rank: int, spec: ShardSpec, load_spec: LoadSpec, wksd: dict
) -> WeightSender:
    endpoint = WeightSender.__new__(WeightSender)
    endpoint.cuda_device = f"cuda:{torch.cuda.current_device()}"
    endpoint.rank = rank
    endpoint.shard_spec = spec
    endpoint.dtype_spec = {}  # learned from receivers in the metadata gather
    endpoint.load_spec = load_spec
    endpoint.wksd = wksd
    endpoint.sender_staging = False
    endpoint.receiver_staging = False
    endpoint.connected = False
    endpoint._offload_ev = {}
    endpoint._fallback_receive_engines = []
    return endpoint


def start_sender(endpoint: WeightSender) -> None:
    """The normal connect() starts these after endpoint metadata setup; this test calls setup directly."""
    endpoint.connected = True
    endpoint._coord = []
    endpoint.receiver_urls = []
    endpoint._sq = queue.Queue()
    endpoint._cv = threading.Condition()
    endpoint._completed = set()
    endpoint._werr = []
    endpoint._drained_count = 0
    endpoint._offload_ev = {}
    endpoint._flag_reaper_ensure()
    endpoint._send_thread = threading.Thread(
        target=endpoint._send_worker,
        name="wbridge-test-send",
        daemon=True,
    )
    endpoint._send_thread.start()


def make_receiver(
    worker: int, spec: ShardSpec, load_spec: LoadSpec, wksd: dict
) -> WeightReceiver:
    endpoint = WeightReceiver.__new__(WeightReceiver)
    endpoint.cuda_device = f"cuda:{torch.cuda.current_device()}"
    endpoint.rank = worker
    endpoint.shard_spec = spec
    endpoint.dtype_spec = {name: name_dtype(name) for name in NAMES}
    endpoint.load_spec = load_spec
    endpoint.wksd = wksd
    endpoint.sender_staging = False
    endpoint.receiver_staging = False
    return endpoint


def check_receiver(worker: int, wksd: dict) -> None:
    full = receiver_spec(worker)
    for ni, (name, shards) in enumerate(full):
        left, right, _width = shards[0][0]
        expected = torch.arange(left, right, dtype=torch.int32, device="cuda")
        expected.remainder_(251).add_(ni * 1000)
        expected = expected.to(name_dtype(name))
        if not torch.equal(wksd[name], expected):
            mismatch_count = int(torch.count_nonzero(wksd[name] != expected).item())
            print(
                f"VALUE_MISMATCH worker={worker} name={name} "
                f"mismatches={mismatch_count}/{expected.numel()} "
                f"actual_head={wksd[name][:8].cpu().tolist()} "
                f"expected_head={expected[:8].cpu().tolist()} "
                f"actual_tail={wksd[name][-8:].cpu().tolist()} "
                f"expected_tail={expected[-8:].cpu().tolist()}",
                flush=True,
            )
            raise AssertionError(
                f"worker {worker} tensor {name} mismatch: "
                f"{mismatch_count}/{expected.numel()} elements"
            )


def main() -> None:
    # torchrun marks its own MASTER_PORT store as agent-owned. This test deliberately creates a second,
    # independent TCPStore at WBRIDGE_TEST_PORT; without clearing the marker PyTorch assumes an agent already
    # owns that second port too, so every rank waits for a server that was never created.
    os.environ.pop("TORCHELASTIC_USE_AGENT_STORE", None)
    rank = (
        int(os.environ["RANK"])
        if "RANK" in os.environ
        else int(os.environ.get("WBRIDGE_TEST_RANK_BASE", "0"))
        + int(os.environ["SLURM_PROCID"])
    )
    local_rank = int(os.environ.get("LOCAL_RANK", os.environ.get("SLURM_LOCALID", "0")))
    assert 0 <= rank < WORLD
    # CUDA-IPC import needs every local peer GPU visible, matching the production Ray/SGLang actors.
    # CI may opt into sharing one allocated GPU among all eight local ranks; this preserves the process/node
    # topology and CUDA-IPC/RDMA protocols while avoiding a full-node lease. Production verification leaves the
    # switch off and requires all eight GPUs to be visible.
    visible = torch.cuda.device_count()
    shared_gpu = os.environ.get("WBRIDGE_TEST_SHARED_GPU") == "1"
    if not shared_gpu:
        assert visible >= 8, (rank, local_rank, os.environ.get("CUDA_VISIBLE_DEVICES"))
    assert visible >= 1
    torch.cuda.set_device(local_rank % visible)

    is_sender = rank < SENDERS
    worker = rank - SENDERS
    spec = sender_spec() if is_sender else receiver_spec(worker)
    load_spec, wksd = identity_side(spec, sender=is_sender)
    endpoint = (
        make_sender(rank, spec, load_spec, wksd)
        if is_sender
        else make_receiver(worker, spec, load_spec, wksd)
    )

    endpoint.set_up_connection(
        protocol="efa",
        init_method=(
            f"tcp://{os.environ['MASTER_ADDR']}:"
            f"{os.environ.get('WBRIDGE_TEST_PORT', os.environ.get('MASTER_PORT', '62991'))}"
        ),
        world_size=WORLD,
        rank=rank,
        sender_world_size=SENDERS,
        group_name="wbridge-multigroup-integration",
    )
    if is_sender:
        start_sender(endpoint)
    else:
        print(
            f"MULTI_GROUP_CONFIG worker={worker} topo_ok={endpoint._topo_ok} "
            f"groups={len(endpoint.router._topology_groups)} rounds={endpoint.num_rounds} "
            f"ext_send={endpoint._topo_ext_send_peers_by_round} "
            f"ext_recv={endpoint._topo_ext_recv_peers_by_round} "
            f"internal_consume={endpoint._topo_int_peers_by_round}",
            flush=True,
        )

    dist.barrier(group=endpoint.group)
    for _ in range(int(os.environ.get("WBRIDGE_TEST_ITERS", "2"))):
        if is_sender:
            event = endpoint.send()
            if event is not None:
                event.synchronize()
            endpoint.wait_send_complete()
        else:
            endpoint._receive_weights(False)
            check_receiver(worker, wksd)
        dist.barrier(group=endpoint.group)

    if not is_sender and os.environ.get("WBRIDGE_TOPO_EXCHANGE", "1") == "1":
        assert endpoint._topo_ok
        assert len(endpoint.router._topology_groups) == 11
    ok = torch.tensor(1, dtype=torch.int64)
    dist.all_reduce(ok, group=endpoint.group)
    if rank == 0:
        print(
            "MULTI_GROUP_TOPOLOGY_INTEGRATION_PASS "
            f"world={WORLD} senders={SENDERS} receivers={RECEIVERS} "
            f"receiver_nodes={RECEIVER_NODES} rounds={endpoint.num_rounds}",
            flush=True,
        )

    dist.barrier(group=endpoint.group)
    if is_sender:
        endpoint._sq.put(None)
        endpoint._send_thread.join(timeout=5)
    endpoint._teardown()


if __name__ == "__main__":
    main()
