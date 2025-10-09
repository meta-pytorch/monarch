# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Getting Started
===============

This guide introduces the core concepts of Monarch, a framework for building
multi-machine training programs using actors. We'll cover:

- Defining actors with endpoint functions
- Spawning actors locally and across multiple hosts and processes
- Sending messages and organizing actors into meshes
- The supervision tree for fault tolerance
- Distributed tensors and RDMA capabilities
"""

from assets import show_svg

# %%
# Defining an Actor
# -----------------
# At its core, Monarch uses actors as a way to create multi-machine training programs.
# Actors are Python objects that expose a number of endpoint functions. These functions
# can be called by other actors in the system and their responses gathered asynchronously.
#
# Let's start by defining a simple actor:

from monarch.actor import Actor, endpoint, this_proc


class Counter(Actor):
    def __init__(self, initial_value: int):
        self.value = initial_value

    @endpoint
    def increment(self) -> None:
        self.value += 1

    @endpoint
    def get_value(self) -> int:
        return self.value


# %%
# Message Flow Diagram
# ===================
# Here's how messages flow between actors:

show_svg("message_flow.svg")
