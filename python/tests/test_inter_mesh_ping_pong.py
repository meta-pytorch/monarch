# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Test demonstrating cross-mesh communication via serialized ActorMesh
references.

This is the Python equivalent of the Rust test `test_inter_mesh_ping_pong`
in hyperactor_mesh/src/reference.rs. It shows how two separate ProcMeshes
can communicate by passing ActorMesh references in messages.

Key points:
- Two separate ProcMeshes are created (separate allocations)
- Each mesh spawns actors that hold a reference to their own mesh
- Actors send their mesh reference to actors in the other mesh
- The receiving actor uses the deserialized reference to send back
- This demonstrates how "different Monarch contexts" can communicate
"""

from dataclasses import dataclass
from typing import Any

import pytest
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint


@dataclass
class PingPongMessage:
    """
    Message containing the sender's mesh reference.

    When this message crosses process boundaries, the sender_mesh field
    is serialized (pickled). The receiving actor deserializes it and
    can use it to send messages back to the sender's mesh.
    """

    ttl: int
    sender_mesh: Any  # ActorMesh reference (not publicly exported)


class PingPongActor(Actor):
    """
    Actor that participates in ping-pong communication across meshes.

    Each actor knows its own mesh reference and uses received mesh
    references to send replies.
    """

    def __init__(self) -> None:
        self.my_mesh_ref: Any = None
        self.received_count: int = 0

    @endpoint
    async def set_mesh_ref(self, mesh_ref: Any) -> None:
        """Initialize this actor with a reference to its own mesh."""
        self.my_mesh_ref = mesh_ref

    @endpoint
    async def ping(self, msg: PingPongMessage) -> None:
        """
        Handle a ping message.

        If ttl > 0, send a reply back using the sender's mesh reference.
        The sender_mesh in the message was serialized when sent and
        deserialized here - demonstrating cross-mesh reference passing.
        """
        self.received_count += 1

        if msg.ttl == 0:
            return

        # Use the RECEIVED mesh reference to send back to the other mesh
        assert self.my_mesh_ref is not None
        reply = PingPongMessage(
            ttl=msg.ttl - 1,
            sender_mesh=self.my_mesh_ref,
        )
        await msg.sender_mesh.ping.call(reply)

    @endpoint
    async def get_received_count(self) -> int:
        """Return how many ping messages this actor received."""
        return self.received_count


@pytest.mark.timeout(60)
async def test_inter_mesh_ping_pong() -> None:
    """
    Test that two separate ProcMeshes can communicate by passing
    ActorMesh references in messages.

    This demonstrates:
    1. Creating two separate ProcMeshes (different allocations)
    2. Spawning actors on each mesh
    3. Passing ActorMesh references in messages between meshes
    4. Using deserialized mesh references to send replies
    """
    host = this_host()

    # Create TWO SEPARATE ProcMeshes. Each spawns separate OS processes
    # with their own Monarch context (global router, etc.).
    ping_procs = host.spawn_procs(per_host={"gpus": 1})
    pong_procs = host.spawn_procs(per_host={"gpus": 1})

    ping_mesh = ping_procs.spawn("ping", PingPongActor)
    pong_mesh = pong_procs.spawn("pong", PingPongActor)

    # Initialize actors with references to their own meshes.
    # These refs will be serialized when sent to the other mesh.
    await ping_mesh.set_mesh_ref.call(ping_mesh)
    await pong_mesh.set_mesh_ref.call(pong_mesh)

    # Start the ping-pong: ping initiates by sending to pong.
    # The message contains ping_mesh as the reply-to reference.
    # Flow: pong receives ttl=10, replies to ping with ttl=9,
    #       ping receives ttl=9, replies to pong with ttl=8, etc.
    initial_ttl = 10
    initial_msg = PingPongMessage(ttl=initial_ttl, sender_mesh=ping_mesh)
    await pong_mesh.ping.call(initial_msg)

    ping_count = await ping_mesh.get_received_count.call_one()
    pong_count = await pong_mesh.get_received_count.call_one()

    # pong receives first (ttl=10,8,6,4,2,0) = 6 messages
    # ping receives replies (ttl=9,7,5,3,1) = 5 messages
    assert pong_count == 6
    assert ping_count == 5


@pytest.mark.timeout(60)
async def test_inter_mesh_ping_pong_multiple_ranks() -> None:
    """
    Test cross-mesh communication with multiple ranks per mesh.

    Verifies that ActorMesh references work correctly when the mesh
    contains multiple actors. Uses a single round-trip to avoid
    complexity from broadcast semantics.
    """
    host = this_host()

    ping_procs = host.spawn_procs(per_host={"gpus": 2})
    pong_procs = host.spawn_procs(per_host={"gpus": 2})

    ping_mesh = ping_procs.spawn("ping", PingPongActor)
    pong_mesh = pong_procs.spawn("pong", PingPongActor)

    await ping_mesh.set_mesh_ref.call(ping_mesh)
    await pong_mesh.set_mesh_ref.call(pong_mesh)

    # Single round-trip to keep it simple:
    # ttl=1: pong receives, sends ttl=0 to ping
    # ttl=0: ping receives, done
    initial_msg = PingPongMessage(ttl=1, sender_mesh=ping_mesh)
    await pong_mesh.ping.call(initial_msg)

    # Verify message counts with broadcast semantics:
    # - Initial broadcast to pong_mesh: 2 pong actors each receive 1 message
    # - Each pong actor broadcasts ttl=0 to ping_mesh: 2 ping actors each
    #   receive 2 messages (one from each pong actor)
    ping_counts = await ping_mesh.get_received_count.call()
    pong_counts = await pong_mesh.get_received_count.call()

    total_ping = sum(ping_counts.values())
    total_pong = sum(pong_counts.values())

    assert total_pong == 2, "each pong actor receives initial broadcast"
    assert total_ping == 4, "each ping actor receives reply from each pong actor"
