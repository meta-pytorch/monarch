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
            action.read_into(buffer, self.slots[i])
        await action.submit(timeout=TIMEOUT)

    @endpoint
    async def read_resubmit(
        self, buffers: list[RDMABuffer]
    ) -> tuple[list[torch.Tensor], list[torch.Tensor]]:
        action = RDMAAction()
        for i, buffer in enumerate(buffers):
            action.read_into(buffer, self.slots[i])
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
            action.write_from(buffer, self.slots[i])
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
            action.read_into(buffer, self.slots[i])
        for i, buffer in enumerate(write_buffers):
            action.write_from(buffer, self.slots[len(read_buffers) + i])
        await action.submit(timeout=TIMEOUT)

    @endpoint
    async def submit_validates_size_eagerly(self, buffer: RDMABuffer) -> bool:
        oversized = torch.zeros(
            buffer.size() + 32, dtype=torch.uint8, device=self.device
        )
        action = RDMAAction()
        try:
            action.write_from(buffer, oversized)
        except ValueError:
            return True
        return False


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
    _, client = await _spawn_host_and_client(
        num_buffers=1, size=16, host_device=host_device, client_device=client_device
    )
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
    host, client = await _spawn_host_and_client(
        num_buffers=num_buffers,
        size=size,
        host_device=host_device,
        client_device=client_device,
        seed_base=1000,
    )
    buffers = await host.create_buffers.call_one()
    await client.read_all_with_submit.call_one(buffers)
    host_tensors = await host.get_tensors.call_one()
    client_tensors = await client.get_slots.call_one()
    for i in range(num_buffers):
        assert torch.equal(client_tensors[i], host_tensors[i]), (
            f"slot {i} does not match source buffer bit-for-bit"
        )


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_can_be_called_twice(host_device, client_device) -> None:
    size = 32
    num_buffers = 3
    host, client = await _spawn_host_and_client(
        num_buffers=num_buffers,
        size=size,
        host_device=host_device,
        client_device=client_device,
        seed_base=2000,
    )
    buffers = await host.create_buffers.call_one()
    first, second = await client.read_resubmit.call_one(buffers)
    host_tensors = await host.get_tensors.call_one()
    for i in range(num_buffers):
        assert torch.equal(first[i], host_tensors[i]), (
            f"first attempt slot {i} mismatch"
        )
        assert torch.equal(second[i], host_tensors[i]), (
            f"second attempt slot {i} mismatch"
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
    for i in range(num_buffers):
        assert torch.equal(host_tensors[i], client_slots[i]), (
            f"host buffer {i} does not match bytes the client wrote"
        )


@rdma_backends
@pytest.mark.parametrize(
    ("host_device", "client_device"), DEVICE_VARIANTS, ids=DEVICE_IDS
)
async def test_submit_mixes_reads_and_writes(host_device, client_device) -> None:
    size = 32
    num_reads = 2
    num_writes = 2
    host, client = await _spawn_host_and_client(
        num_buffers=num_reads + num_writes,
        size=size,
        host_device=host_device,
        client_device=client_device,
        seed_base=4000,
        num_slots=num_reads + num_writes,
    )
    buffers = await host.create_buffers.call_one()
    host_before = await host.get_tensors.call_one()
    write_seeds = [77, 78]

    await client.mixed_read_write.call_one(
        buffers[:num_reads], buffers[num_reads:], write_seeds
    )

    client_tensors = await client.get_slots.call_one()
    for i in range(num_reads):
        assert torch.equal(client_tensors[i], host_before[i]), (
            f"read slot {i} did not capture the pre-action host bytes"
        )

    host_after = await host.get_tensors.call_one()
    for i in range(num_reads):
        assert torch.equal(host_after[i], host_before[i]), (
            f"read-source buffer {i} was modified by the action"
        )
    for j in range(len(write_seeds)):
        assert torch.equal(host_after[num_reads + j], client_tensors[num_reads + j]), (
            f"write-target buffer {num_reads + j} mismatch"
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
    assert caught, "Expected ValueError from RDMAAction.write_from size check"
