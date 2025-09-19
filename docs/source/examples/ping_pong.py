# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Monarch Actor API: Ping Pong Example
====================================

This example demonstrates the basics of Monarch's Actor/endpoint API, which provides
a generic interface for distributed computing. We'll cover:

- Creating and spawning actors in process meshes
- Calling endpoints on actors
- Actor-to-actor communication with a ping-pong example
"""

# %%
# Hello World
# -----------
# Actors are spawned in Process meshes via the `monarch.actor` API. For those familiar with
# distributed systems, it can be helpful to think of each Actor as a server with endpoints
# that can be called.

import asyncio

from monarch.actor import Actor, current_rank, endpoint, proc_mesh

NUM_ACTORS = 4


class ToyActor(Actor):
    def __init__(self):
        self.rank = current_rank().rank

    @endpoint
    async def hello_world(self, msg):
        print(f"Identity: {self.rank}, {msg=}")


# Note: Meshes can be also be created on different nodes, but we're ignoring that in this example
async def create_toy_actors():
    local_proc_mesh = proc_mesh(gpus=NUM_ACTORS)
    # This spawns 4 instances of 'ToyActor'
    toy_actor = await local_proc_mesh.spawn("toy_actor", ToyActor)
    return toy_actor, local_proc_mesh


# %%
# Once actors are spawned, we can call all of them simultaneously with `Actor.endpoint.call`
async def call_all_actors(toy_actor):
    await toy_actor.hello_world.call("hey there, from script!!")


# %%
# We can also specify a single actor using the 'slice' API
async def call_specific_actors(toy_actor):
    futures = []
    for idx in range(NUM_ACTORS):
        actor_instance = toy_actor.slice(gpus=idx)
        futures.append(
            actor_instance.hello_world.call_one(
                f"Here's an arbitrary unique value: {idx}"
            )
        )

    # conveniently, we can still schedule & gather them in parallel using asyncio
    await asyncio.gather(*futures)


# %%
# Ping Pong
# ---------
# Not only is it possible to call endpoints from a 'main' function, but actors have
# the useful property of being able to communicate with one another.


class ExampleActor(Actor):
    def __init__(self, actor_name):
        self.actor_name = actor_name

    @endpoint
    async def init(self, other_actor):
        self.other_actor = other_actor
        self.other_actor_pair = other_actor.slice(**current_rank())
        self.identity = current_rank().rank

    @endpoint
    async def send(self, msg):
        await self.other_actor_pair.recv.call(
            f"Sender ({self.actor_name}:{self.identity}) {msg=}"
        )

    @endpoint
    async def recv(self, msg):
        print(f"Pong!, Receiver ({self.actor_name}:{self.identity}) received msg {msg}")


async def create_ping_pong_actors():
    # Spawn two different Actors in different meshes, with two instances each
    local_mesh_0 = proc_mesh(gpus=2)
    actor_0 = await local_mesh_0.spawn(
        "actor_0",
        ExampleActor,
        "actor_0",  # this arg is passed to ExampleActor.__init__
    )

    local_mesh_1 = proc_mesh(gpus=2)
    actor_1 = await local_mesh_1.spawn(
        "actor_1",
        ExampleActor,
        "actor_1",  # this arg is passed to ExampleActor.__init__
    )

    return actor_0, actor_1, local_mesh_0, local_mesh_1


# %%
# Initialize each actor with references to each other
async def init_ping_pong(actor_0, actor_1):
    await asyncio.gather(
        actor_0.init.call(actor_1),
        actor_1.init.call(actor_0),
    )


# %%
# Send messages between actors
async def send_ping_pong(actor_0, actor_1):
    # Actor 0 sends to Actor 1
    await actor_0.send.call("Ping")

    # Actor 1 sends to Actor 0
    await actor_1.send.call("Ping")


# %%
# Main function to run the example
async def main():
    # Hello World example
    toy_actor, toy_mesh = await create_toy_actors()
    await call_all_actors(toy_actor)
    await call_specific_actors(toy_actor)

    # Ping Pong example
    actor_0, actor_1, mesh_0, mesh_1 = await create_ping_pong_actors()
    await init_ping_pong(actor_0, actor_1)
    await send_ping_pong(actor_0, actor_1)

    print("Example completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
