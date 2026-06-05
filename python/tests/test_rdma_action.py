# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""End-to-end tests for ``monarch.rdma.RDMAAction``."""

import os

# Required to enable RDMA support for CUDA tensors.
os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")

import pytest  # noqa: E402
import torch  # noqa: E402
from monarch.actor import Actor, endpoint, this_host  # noqa: E402
from monarch.rdma import RDMAAction, RDMABuffer  # noqa: E402
from proc_mesh_test_utils import stop_all_proc_meshes  # noqa: E402, F401
from rdma_test_utils import rdma_backends  # noqa: E402


TIMEOUT = 60


def _make_seed_tensor(seed: int, size: int, device: str) -> torch.Tensor:
    generator = torch.Generator(device=device).manual_seed(seed)
    return torch.rand(size, generator=generator, dtype=torch.float32, device=device)


class BufferHost(Actor):
    def __init__(
        self, num_buffers: int, size: int, seed_base: int, device: str
    ) -> None:
        super().__init__()
        self.device = device
        self.tensors = [
            _make_seed_tensor(seed_base + i, size, device) for i in range(num_buffers)
        ]
        self.buffers: list[RDMABuffer] = []

    @endpoint
    async def create_buffers(self) -> list[RDMABuffer]:
        self.buffers = [RDMABuffer(t) for t in self.tensors]
        return self.buffers

    @endpoint
    async def get_tensors(self) -> list[torch.Tensor]:
        return list(self.tensors)


class ActionClient(Actor):
    def __init__(self, num_slots: int, size: int, device: str) -> None:
        super().__init__()
        self.device = device
        self.slots = [
            torch.zeros(size, dtype=torch.float32, device=device)
            for _ in range(num_slots)
        ]

    @endpoint
    async def get_slots(self) -> list[torch.Tensor]:
        return list(self.slots)

    @endpoint
    async def submit_empty_is_noop(self) -> None:
        await RDMAAction().submit(timeout=TIMEOUT)

    @endpoint
    async def read_all_with_submit(self, buffers: list[RDMABuffer]) -> None:
        action = RDMAAction()
        for i, buffer in enumerate(buffers):
            action.read_remote(self.slots[i], buffer)
        await action.submit(timeout=TIMEOUT)

    @endpoint
    async def read_resubmit(
        self, buffers: list[RDMABuffer]
    ) -> tuple[list[torch.Tensor], list[torch.Tensor]]:
        action = RDMAAction()
        for i, buffer in enumerate(buffers):
            action.read_remote(self.slots[i], buffer)
        await action.submit(timeout=TIMEOUT)
        # Snapshot before zeroing so the second submit has something to fill in.
        first = [s.clone() for s in self.slots[: len(buffers)]]
        for slot in self.slots[: len(buffers)]:
            slot.zero_()
        await action.submit(timeout=TIMEOUT)
        second = [s.clone() for s in self.slots[: len(buffers)]]
        return first, second

    @endpoint
    async def write_from_seeded(
        self, buffers: list[RDMABuffer], seeds: list[int]
    ) -> None:
        size = self.slots[0].numel()
        for i, seed in enumerate(seeds):
            self.slots[i] = _make_seed_tensor(seed, size, self.device)
        action = RDMAAction()
        for i, buffer in enumerate(buffers):
            action.write_remote(buffer, self.slots[i])
        await action.submit(timeout=TIMEOUT)

    @endpoint
    async def mixed_read_write(
        self,
        read_buffers: list[RDMABuffer],
        write_buffers: list[RDMABuffer],
        write_seeds: list[int],
    ) -> None:
        size = self.slots[0].numel()
        for i, seed in enumerate(write_seeds):
            self.slots[len(read_buffers) + i] = _make_seed_tensor(
                seed, size, self.device
            )
        action = RDMAAction()
        for i, buffer in enumerate(read_buffers):
            action.read_remote(self.slots[i], buffer)
        for i, buffer in enumerate(write_buffers):
            action.write_remote(buffer, self.slots[len(read_buffers) + i])
        await action.submit(timeout=TIMEOUT)

    @endpoint
    async def submit_validates_size_eagerly(self, buffer: RDMABuffer) -> bool:
        oversized = torch.zeros(
            buffer.size() + 32, dtype=torch.uint8, device=self.device
        )
        action = RDMAAction()
        try:
            action.write_remote(buffer, oversized)
        except ValueError:
            return True
        return False

    @endpoint
    async def overlapping_writes_into_local_error(self, buffer: RDMABuffer) -> bool:
        # Two `read_remote`s onto overlapping local destinations both write
        # the same local memory: the new local-side claim algorithm must
        # flag this as a race.
        size = buffer.size()
        slot = torch.zeros(size, dtype=torch.uint8, device=self.device)
        action = RDMAAction()
        action.read_remote(slot, buffer)
        try:
            action.read_remote(slot, buffer)
        except ValueError:
            return True
        return False

    @endpoint
    async def overlapping_reads_from_local_ok(
        self, buffer_a: RDMABuffer, buffer_b: RDMABuffer
    ) -> None:
        # Two `write_remote`s using the same local memory range both *read*
        # local memory — that is safe, and the new algorithm must let it
        # through (the old python helper would have errored).
        size = min(buffer_a.size(), buffer_b.size())
        slot = torch.zeros(size, dtype=torch.uint8, device=self.device)
        action = RDMAAction()
        action.write_remote(buffer_a, slot)
        action.write_remote(buffer_b, slot)
        await action.submit(timeout=TIMEOUT)


async def _spawn_host_and_client(
    *,
    num_buffers: int,
    size: int,
    host_device: str,
    client_device: str,
    seed_base: int = 0,
    num_slots: int | None = None,
) -> tuple[BufferHost, ActionClient]:
    host_proc = this_host().spawn_procs(per_host={"processes": 1})
    client_proc = this_host().spawn_procs(per_host={"processes": 1})
    host = host_proc.spawn(
        "buffer_host", BufferHost, num_buffers, size, seed_base, host_device
    )
    client = client_proc.spawn(
        "action_client",
        ActionClient,
        num_slots or num_buffers,
        size,
        client_device,
    )
    return host, client


def _device_variants() -> list[tuple[str, str]]:
    variants: list[tuple[str, str]] = [("cpu", "cpu")]
    if torch.cuda.is_available():
        variants.extend([("cuda", "cuda"), ("cpu", "cuda"), ("cuda", "cpu")])
    return variants


DEVICE_VARIANTS = _device_variants()
DEVICE_IDS = [f"{h}->{c}" for h, c in DEVICE_VARIANTS]


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_empty_action_is_noop(host_device, client_device) -> None:
    host, client = await _spawn_host_and_client(
        num_buffers=1, size=16, host_device=host_device, client_device=client_device
    )
    # Drain host init by calling a cheap endpoint. Without this, the
    # host actor may still be running its `__init__` (which on CUDA can
    # take seconds to allocate the seed tensor + initialize the CUDA
    # context) when pytest exits, racing the actor cleanup timeout.
    await host.get_tensors.call_one()
    await client.submit_empty_is_noop.call_one()
    # Empty submit must not touch client slots: they were initialized to zeros
    # and should remain so after a no-op submit.
    slots = await client.get_slots.call_one()
    for i, slot in enumerate(slots):
        assert torch.equal(slot, torch.zeros_like(slot)), (
            f"slot {i} was mutated by an empty RDMAAction.submit"
        )


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_reads_all_buffers(host_device, client_device) -> None:
    size = 64
    num_buffers = 5
    seed_base = 1000
    host, client = await _spawn_host_and_client(
        num_buffers=num_buffers,
        size=size,
        host_device=host_device,
        client_device=client_device,
        seed_base=seed_base,
    )
    buffers = await host.create_buffers.call_one()
    await client.read_all_with_submit.call_one(buffers)
    host_tensors = await host.get_tensors.call_one()
    client_tensors = await client.get_slots.call_one()
    # Pin both sides to the seed values: catches direction bugs (a read
    # mistakenly behaving as a write would mutate the host).
    expected_seeds = [
        _make_seed_tensor(seed_base + i, size, host_device).cpu()
        for i in range(num_buffers)
    ]
    for i in range(num_buffers):
        assert torch.equal(host_tensors[i].cpu(), expected_seeds[i]), (
            f"host buffer {i} was mutated by the read"
        )
        assert torch.equal(client_tensors[i].cpu(), expected_seeds[i]), (
            f"client slot {i} does not match the host's seed bytes"
        )


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_can_be_called_twice(host_device, client_device) -> None:
    size = 32
    num_buffers = 3
    seed_base = 2000
    host, client = await _spawn_host_and_client(
        num_buffers=num_buffers,
        size=size,
        host_device=host_device,
        client_device=client_device,
        seed_base=seed_base,
    )
    buffers = await host.create_buffers.call_one()
    first, second = await client.read_resubmit.call_one(buffers)
    host_tensors = await host.get_tensors.call_one()
    expected_seeds = [
        _make_seed_tensor(seed_base + i, size, host_device).cpu()
        for i in range(num_buffers)
    ]
    for i in range(num_buffers):
        assert torch.equal(host_tensors[i].cpu(), expected_seeds[i]), (
            f"host buffer {i} was mutated across the two reads"
        )
        assert torch.equal(first[i].cpu(), expected_seeds[i]), (
            f"first attempt slot {i} does not match the seed"
        )
        assert torch.equal(second[i].cpu(), expected_seeds[i]), (
            f"second attempt slot {i} does not match the seed"
        )


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_writes_all_buffers(host_device, client_device) -> None:
    size = 32
    num_buffers = 4
    host, client = await _spawn_host_and_client(
        num_buffers=num_buffers,
        size=size,
        host_device=host_device,
        client_device=client_device,
        seed_base=3000,
    )
    buffers = await host.create_buffers.call_one()
    seeds = [40, 41, 42, 43]
    await client.write_from_seeded.call_one(buffers, seeds)
    host_tensors = await host.get_tensors.call_one()
    client_slots = await client.get_slots.call_one()
    # Pin both sides to the write seeds: catches a write-misbehaving-as-read
    # (the host would still hold its original `seed_base` bytes).
    expected_writes = [
        _make_seed_tensor(seed, size, client_device).cpu() for seed in seeds
    ]
    for i in range(num_buffers):
        assert torch.equal(host_tensors[i].cpu(), expected_writes[i]), (
            f"host buffer {i} does not match the write seed"
        )
        assert torch.equal(client_slots[i].cpu(), expected_writes[i]), (
            f"client slot {i} no longer matches its write seed"
        )


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_mixes_reads_and_writes(host_device, client_device) -> None:
    size = 32
    num_reads = 2
    num_writes = 2
    seed_base = 4000
    host, client = await _spawn_host_and_client(
        num_buffers=num_reads + num_writes,
        size=size,
        host_device=host_device,
        client_device=client_device,
        seed_base=seed_base,
        num_slots=num_reads + num_writes,
    )
    buffers = await host.create_buffers.call_one()
    write_seeds = [77, 78]

    await client.mixed_read_write.call_one(
        buffers[:num_reads], buffers[num_reads:], write_seeds
    )

    client_tensors = await client.get_slots.call_one()
    host_after = await host.get_tensors.call_one()

    expected_host_seeds = [
        _make_seed_tensor(seed_base + i, size, host_device).cpu()
        for i in range(num_reads + num_writes)
    ]
    expected_writes = [
        _make_seed_tensor(s, size, client_device).cpu() for s in write_seeds
    ]

    # Reads: host stays at its seed; client received the host's seed bytes.
    for i in range(num_reads):
        assert torch.equal(host_after[i].cpu(), expected_host_seeds[i]), (
            f"read-source buffer {i} was modified by the action"
        )
        assert torch.equal(client_tensors[i].cpu(), expected_host_seeds[i]), (
            f"read slot {i} does not match the host's seed"
        )
    # Writes: host now holds the client's write seed; client still holds the seed it wrote.
    for j in range(num_writes):
        assert torch.equal(host_after[num_reads + j].cpu(), expected_writes[j]), (
            f"write-target buffer {num_reads + j} does not match the write seed"
        )
        assert torch.equal(client_tensors[num_reads + j].cpu(), expected_writes[j]), (
            f"client write slot {j} no longer matches its write seed"
        )


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_validation_errors_surface(host_device, client_device) -> None:
    size = 32
    host, client = await _spawn_host_and_client(
        num_buffers=1,
        size=size,
        host_device=host_device,
        client_device=client_device,
        num_slots=2,
    )
    (buffer,) = await host.create_buffers.call_one()
    caught = await client.submit_validates_size_eagerly.call_one(buffer)
    assert caught, "Expected ValueError from RDMAAction.write_remote size check"


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_overlapping_local_writes_are_a_race(host_device, client_device) -> None:
    # Two `read_remote`s onto the same local destination are concurrent writes —
    # the bug-fixed claim algorithm must flag this as a race.
    size = 32
    host, client = await _spawn_host_and_client(
        num_buffers=1, size=size, host_device=host_device, client_device=client_device
    )
    (buffer,) = await host.create_buffers.call_one()
    caught = await client.overlapping_writes_into_local_error.call_one(buffer)
    assert caught, "Expected ValueError from two overlapping read_remote claims"


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_overlapping_local_reads_are_ok(host_device, client_device) -> None:
    # Two `write_remote`s from the same local source both *read* local memory —
    # safe; today's python helper would have errored, the new one merges.
    # Uses two distinct remote buffers so the test exercises the local-side
    # read-merge without two writes landing on the same remote target.
    size = 32
    host, client = await _spawn_host_and_client(
        num_buffers=2, size=size, host_device=host_device, client_device=client_device
    )
    buffer_a, buffer_b = await host.create_buffers.call_one()
    await client.overlapping_reads_from_local_ok.call_one(buffer_a, buffer_b)
