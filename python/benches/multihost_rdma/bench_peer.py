#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
The multi-host RDMA benchmark's participant actor.

One :py:class:`Peer` runs in each proc, and works out for itself which
:py:class:`~bench_topology.Slot` it is from its own position in its mesh. It
allocates and registers its tensors, installs whatever ops the driver routed to
it, and executes one iteration per call. It knows nothing about patterns,
directions, runs, or phases: the driver hands it an
:py:class:`~bench_topology.InitiatorPlan`, a flat list of ops over local keys
and remote handles, and the timed path reads that and nothing else.

Because every proc knows its own slot, the driver addresses them all at once:
:py:meth:`Peer.setup` and :py:meth:`Peer.wire` take a mapping keyed by slot and
each proc takes its own entry, so a configuration of any size is two casts
rather than two calls per proc. A slot the pattern never touches is simply
absent from the mapping, and that proc allocates nothing.

The actor also holds **no peer actor handles**. ``read_remote`` and
``write_remote`` need only the remote ``RDMABuffer``, so nothing on the timed
path calls into another actor. That matters because the Python actor dispatch
loop is serial -- an actor blocked inside one endpoint cannot service a call to
another -- so a ring or an all-to-all, where every participant both sends and
receives, would otherwise be a deadlock waiting to happen.

Setup is two casts rather than one because an initiator's ops name its peers'
buffers: every participant must finish :py:meth:`Peer.setup` before any of them
can be given a plan by :py:meth:`Peer.wire`.
"""

from __future__ import annotations

import time
from typing import Any, Mapping

import torch
import xxhash
from bench_stats import Sample
from bench_topology import InitiatorPlan, Slot, SlotAllocation, SlotValues
from monarch.actor import Actor, context, endpoint
from monarch.rdma import get_rdma_backend, RDMAAction, RDMABuffer


VERIFY_OFF: str = "off"
VERIFY_SAMPLED: str = "sampled"
VERIFY_FULL: str = "full"
VERIFY_MODES: tuple[str, ...] = (VERIFY_OFF, VERIFY_SAMPLED, VERIFY_FULL)


class Peer(Actor):
    """One benchmark participant, holding one slot's tensors and op list."""

    def __init__(self) -> None:
        # `Instance.rank` is where this proc sits in the mesh it was spawned
        # into. A mesh over a single host carries no hosts dimension at all,
        # which is the same thing as sitting at index zero of one.
        rank = context().actor_instance.rank
        host = rank["hosts"] if "hosts" in rank.extent.labels else 0
        self.slot: Slot = Slot(host, rank["lanes"])
        # Byte tensors, because an RDMA op moves opaque bytes. A slot's outgoing
        # tensors live on the source device and its incoming ones on the
        # destination device.
        self.tensors: SlotValues[torch.Tensor] = _nothing()
        self.buffers: SlotValues[RDMABuffer] = _nothing()
        self.plan: InitiatorPlan = InitiatorPlan()

    @endpoint
    async def backend(self) -> str:
        """The RDMA backend this proc actually resolved.

        Checked against the requested transport so a silent TCP fallback fails
        loudly instead of showing up as a hundred-fold outlier.
        """
        return get_rdma_backend()

    @endpoint
    async def setup(
        self,
        allocations: Mapping[Slot, SlotAllocation],
        payload_bytes: int,
        seed: int,
        source_on_gpu: bool,
        dest_on_gpu: bool,
    ) -> tuple[float, SlotValues[RDMABuffer]]:
        """Allocate this slot's tensors, fill them, and register them.

        Returns the milliseconds registration alone took, which the driver
        reports as ``register_ms``, along with the buffers it needs in order to
        route everyone's ops.

        The order is deliberate. Filling happens before registration so that it
        stays out of the measurement, and every tensor is allocated before any
        is registered: with expandable segments enabled and mlx5dv supported,
        interleaving would make each new tensor incur a fresh registration and
        risk falling back to the dmabuf path.
        """
        await self._reset()
        allocation = allocations.get(self.slot)
        if allocation is None:
            return 0.0, self.buffers
        outgoing_device = _device(source_on_gpu, self.slot.lane)
        incoming_device = _device(dest_on_gpu, self.slot.lane)
        self.tensors = SlotValues(
            outgoing=tuple(
                torch.empty(payload_bytes, dtype=torch.uint8, device=outgoing_device)
                for _ in range(allocation.ops if allocation.sends else 0)
            ),
            incoming={
                peer: tuple(
                    torch.empty(
                        payload_bytes, dtype=torch.uint8, device=incoming_device
                    )
                    for _ in range(allocation.ops)
                )
                for peer in allocation.receives_from
            },
        )
        self._fill(seed)

        started = time.perf_counter()
        self.buffers = self.tensors.map(RDMABuffer)
        return _ms(time.perf_counter() - started), self.buffers

    def _fill(self, seed: int) -> None:
        """Give the outgoing tensors random bytes and zero the incoming ones.

        The bytes only have to be distinctive: a digest comparison checks that
        what arrived matches what was sent, so content unique to this slot is
        what makes a transfer routed to the wrong peer visible. Zeroing the
        incoming tensors is what makes a transfer that never happened visible,
        and it is what a pre-transfer digest comparison relies on to fail.
        """
        for tensor in self.tensors.outgoing:
            generator = torch.Generator(device=tensor.device)
            generator.manual_seed(_fill_seed(seed, self.slot))
            tensor.random_(generator=generator)
        for group in self.tensors.incoming.values():
            for tensor in group:
                tensor.zero_()

    @endpoint
    async def wire(self, plans: Mapping[Slot, InitiatorPlan]) -> None:
        """Install the ops this slot will issue."""
        self.plan = plans.get(self.slot, InitiatorPlan())

    @endpoint
    async def execute_iteration(self) -> Sample | None:
        """Build one ``RDMAAction`` from the installed plan, submit it, and wait
        for every op in it to complete.

        ``None`` on a proc that initiates nothing, so the driver can cast to
        every proc and keep only the measurements that mean something.
        """
        if not self.plan.ops:
            return None
        started = time.perf_counter()
        action = RDMAAction()
        for op in self.plan.push:
            action.write_remote(op.remote, self.tensors.outgoing[op.outgoing_index])
        for op in self.plan.pull:
            action.read_remote(self.tensors.incoming[op.peer][op.op_index], op.remote)
        built = time.perf_counter()
        await action.submit()
        finished = time.perf_counter()
        return Sample(
            slot=self.slot,
            build_ms=_ms(built - started),
            submit_ms=_ms(finished - built),
        )

    @endpoint
    async def digest(self, mode: str, window_bytes: int) -> SlotValues[str]:
        """Digest every tensor, in the same shape as the buffers.

        ``full`` hashes every byte. ``sampled`` hashes a window at each end and
        one in the middle, which is far cheaper than pulling gigabytes per proc
        back across PCIe and still catches a wrong peer or a short transfer.
        """
        assert mode in (VERIFY_SAMPLED, VERIFY_FULL), f"cannot digest in mode {mode!r}"
        return self.tensors.map(lambda tensor: _digest(tensor, mode, window_bytes))

    @endpoint
    async def reset(self) -> None:
        """Release every registered buffer and forget every tensor."""
        await self._reset()

    async def _reset(self) -> None:
        """The body of :py:meth:`reset`, callable from inside the actor: an
        endpoint is not a plain method."""
        for buffer in self.buffers.flat():
            await buffer.drop()
        self.buffers = _nothing()
        self.tensors = _nothing()
        self.plan = InitiatorPlan()


def _nothing() -> SlotValues[Any]:
    """A slot holding no tensors yet, or none at all."""
    return SlotValues(outgoing=(), incoming={})


def _ms(seconds: float) -> float:
    return seconds * 1000.0


def _device(use_gpu: bool, lane: int) -> str:
    return f"cuda:{lane}" if use_gpu else "cpu"


def _fill_seed(seed: int, slot: Slot) -> int:
    """A generator seed unique to one ``(run seed, slot)``.

    Two slots sharing a seed would fill identical bytes, and a transfer routed
    to the wrong peer would then pass a digest comparison.
    """
    return (seed * 1_000_003 + slot.host * 1_009 + slot.lane) & 0x7FFF_FFFF


def _digest(tensor: torch.Tensor, mode: str, window_bytes: int) -> str:
    """An xxh64 digest of a tensor's bytes, over all of them or three windows."""
    total = tensor.numel()
    if mode == VERIFY_FULL:
        windows = [tensor]
    else:
        width = min(max(window_bytes, 1), total)
        middle = max((total - width) // 2, 0)
        windows = [
            tensor[:width],
            tensor[middle : middle + width],
            tensor[total - width :],
        ]
    digest = xxhash.xxh64()
    for window in windows:
        digest.update(window.cpu().numpy().tobytes())
    return digest.hexdigest()
