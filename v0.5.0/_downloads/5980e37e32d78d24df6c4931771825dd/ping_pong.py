# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Monarch Actor API: Ping Pong
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

from monarch.actor import Actor, current_rank, endpoint, this_host

NUM_ACTORS = 4


class ToyActor(Actor):
    def __init__(self):
        self.rank = current_rank().rank

    @endpoint
    def hello_world(self, msg):
        print(f"Identity: {self.rank}, {msg=}")


# %%
# Note: Meshes can be also be created on different nodes, but we're ignoring that in this example

local_proc_mesh = this_host().spawn_procs(per_host={"gpus": NUM_ACTORS})
# This spawns 4 instances of 'ToyActor'
toy_actor = local_proc_mesh.spawn("toy_actor", ToyActor)


# %%
# Once actors are spawned, we can call all of them simultaneously with `Actor.endpoint.call`

toy_actor.hello_world.call("hey there, from script!!").get()


# %%
# We can also specify a single actor using the 'slice' API

futures = []
for idx in range(NUM_ACTORS):
    actor_instance = toy_actor.slice(gpus=idx)
    futures.append(
        actor_instance.hello_world.call_one(f"Here's an arbitrary unique value: {idx}")
    )

# Wait for all futures to complete
for fut in futures:
    fut.get()


# %%
# Ping Pong
# ---------
# Not only is it possible to call endpoints from a 'main' function, but actors have
# the useful property of being able to communicate with one another.


class ExampleActor(Actor):
    def __init__(self, actor_name):
        self.actor_name = actor_name

    @endpoint
    def init(self, other_actor):
        self.other_actor = other_actor
        self.other_actor_pair = other_actor.slice(**current_rank())
        self.identity = current_rank().rank

    @endpoint
    def send(self, msg):
        self.other_actor_pair.recv.call(
            f"Sender ({self.actor_name}:{self.identity}) {msg=}"
        ).get()

    @endpoint
    def recv(self, msg):
        print(f"Pong!, Receiver ({self.actor_name}:{self.identity}) received msg {msg}")


# %%
# Spawn two different Actors in different meshes, with two instances each

local_mesh_0 = this_host().spawn_procs(per_host={"gpus": 2})
actor_0 = local_mesh_0.spawn(
    "actor_0",
    ExampleActor,
    "actor_0",  # this arg is passed to ExampleActor.__init__
)

local_mesh_1 = this_host().spawn_procs(per_host={"gpus": 2})
actor_1 = local_mesh_1.spawn(
    "actor_1",
    ExampleActor,
    "actor_1",  # this arg is passed to ExampleActor.__init__
)


# %%
# Initialize each actor with references to each other

actor_0.init.call(actor_1).get()
actor_1.init.call(actor_0).get()


# %%
# Send messages between actors

# Actor 0 sends to Actor 1
actor_0.send.call("Ping").get()

# Actor 1 sends to Actor 0
actor_1.send.call("Ping").get()

print("Example completed successfully!")
