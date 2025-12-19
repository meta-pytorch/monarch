# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Debugging Python Actors with pdb in Monarch
============================================

Monarch supports ``pdb`` debugging for python actor meshes. This guide demonstrates
how to debug distributed actors using Python's built-in debugger with breakpoints,
step-through debugging, and interactive debugging sessions. It includes:

- Setting up actors with breakpoints for debugging
- Accessing the Monarch debugger and listing active breakpoints
- Attaching to specific actors and using pdb commands
- Broadcasting commands to multiple actors
- Post-mortem debugging capabilities
"""

# %%
# Defining an Actor with Breakpoints
# -----------------------------------
# To debug an actor, simply define your python actor and insert typical breakpoints
# in the relevant endpoint that you want to debug using Python's built-in ``breakpoint()``.
#
# **Note: Currently, debug sessions are indexed by actor name + rank. This means that
# concurrently debugging two actor endpoints in the same actor instance is not supported
# at the moment.**

from monarch.actor import Actor, current_rank, endpoint, this_host


def _bad_rank():
    raise ValueError("bad rank")


def _debugee_actor_internal(rank):
    if rank % 4 == 0:
        rank += 1
        return rank
    elif rank % 4 == 1:
        rank += 2
        return rank
    elif rank % 4 == 2:
        rank += 3
        _bad_rank()
    elif rank % 4 == 3:
        rank += 4
        return rank


class DebugeeActor(Actor):
    @endpoint
    async def to_debug(self):
        rank = current_rank().rank
        breakpoint()  # noqa
        return _debugee_actor_internal(rank)


# %%
# Setting Up the Debug Session
# ----------------------------
# The monarch debug server listens for breakpoints at a TCP address
# determined by these environment variables:
#
# - ``MONARCH_DEBUG_SERVER_HOST`` (default ``localhost``, must be on the root client host where you run your monarch program)
# - ``MONARCH_DEBUG_SERVER_PORT`` (default ``27000``)
#
# Simply run your monarch program as usual with the desired values for host and port.

if __name__ == "__main__":
    # Create a mesh with 4 "hosts" and 4 gpus per "host"
    process_mesh = this_host().spawn_procs(per_host={"host": 4, "gpu": 4})

    # Spawn the actor you want to debug on the mesh
    debugee_mesh = process_mesh.spawn("debugee", DebugeeActor)

    # Call the endpoint you want to debug
    print(debugee_mesh.to_debug.call().get())


# %%
# Using the Monarch Debugger
# ---------------------------
# To access the debugger, from a separate terminal with conda activated and monarch installed, run:
#
# .. code-block:: sh
#
#     monarch debug
#
# There are two optional flags:
#
# - ``--host``: the value of ``MONARCH_DEBUG_SERVER_HOST`` in your program (same default as before)
# - ``--port``: the value of ``MONARCH_DEBUG_SERVER_PORT`` in your program (same default as before)
#
# You should then see this:
#
# .. code-block:: text
#
#     ************************ MONARCH DEBUGGER ************************
#     Enter 'help' for a list of commands.
#     Enter 'list' to show all active breakpoints.
#
#     monarch_dbg>
#
# Enter ``list``, and you should see a table showing all actors in your system
# that are currently stopped at a breakpoint, along with basic information
# about each breakpoint including actor name, rank, coordinates, hostname,
# function, and line number.


# %%
# Attaching to a Specific Actor
# -----------------------------
# From the ``monarch_dbg>`` prompt, you can dive into a specific actor/breakpoint
# using the ``attach`` command, specifying the *name* and *rank* of the actor:
#
# .. code-block:: text
#
#     monarch_dbg> attach debugee 13
#     Attached to debug session for rank 13 (your.host.com)
#     > /path/to/debugging.py(16)to_debug()
#     -> rank = _debugee_actor_internal(rank)
#     (Pdb)
#
# From here, you can send arbitrary pdb commands to the attached actor:
#
# .. code-block:: text
#
#     (Pdb) s
#     --Call--
#     > /path/to/debugging.py(20)_debugee_actor_internal()
#     -> def _debugee_actor_internal(rank):
#     (Pdb) n
#     > /path/to/debugging.py(21)_debugee_actor_internal()
#     -> if rank % 4 == 0:
#     (Pdb) rank
#     13
#
# The debugger will automatically detach when the endpoint completes, but you
# can detach early using the ``detach`` command.


# %%
# Casting Commands to Multiple Actors
# ------------------------------------
# You can send ``pdb`` commands to multiple actors on the same actor mesh at once
# using the ``cast`` command. The usage is:
#
# .. code-block:: text
#
#     monarch_dbg> cast <actor_name> ranks(<ranks>) <pdb_command>
#
# There are several ways to specify ranks:
#
# - ``ranks(<rank>)``: sends a command to a single rank without attaching
# - ``ranks(<r1>,<r2>,<r3>)``: sends to comma-separated list of ranks
# - ``ranks(<r_start>:<r_stop>:<r_step>)``: like python list indexing syntax
# - ``ranks(<dim1>=<...>, <dim2>=<...>)``: sends to specified coordinates
#
# Example commands:
#
# .. code-block:: text
#
#     monarch_dbg> cast debugee ranks(0,1) n                 # casts `n` to ranks 0 and 1
#     monarch_dbg> cast debugee ranks(2:7:2) s               # casts `s` to ranks 2, 4 and 6
#     monarch_dbg> cast debugee ranks(host=2:4, gpus=1:3) c  # casts `c` to ranks where host dimension is 2 or 3 and gpu dimension is 1 or 2


# %%
# Post-Mortem Debugging
# ---------------------
# If an actor endpoint raises an error after a breakpoint has been hit,
# execution will stop where the error was raised to allow for post-mortem
# debugging. This is currently enabled by default and requires that the
# endpoint already hit a breakpoint to access post-mortem debugging.
#
# In the example above, rank 2 will hit the ``_bad_rank()`` function which
# raises a ValueError, allowing you to inspect the state at the point
# of failure.


# %%
# Continuing Execution
# --------------------
# To allow execution to continue, from the ``monarch_dbg>`` prompt,
# simply enter ``c`` or ``continue``. This will clear any non-hardcoded
# breakpoints and cast the "``continue``" ``pdb`` command to all ranks
# currently stopped at a breakpoint.
