# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Namespace server script for cross-process testing.

This script spawns an actor mesh and registers it to the SMC namespace,
then waits for a signal to exit. It's used in conjunction with namespace_client.py
to test cross-process namespace functionality.

Usage:
    buck run //monarch/python/tests:namespace_server -- \
        --tier smc.canaryservice.test.monarch_test123 \
        --actor-name my_test_actors \
        --initial-value 42
"""

import asyncio
import signal
import sys
from typing import NoReturn

import click
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.allocator import LocalAllocator
from monarch._src.actor.host_mesh import _bootstrap_cmd, HostMesh
from monarch._src.actor.namespace import configure_namespace, NamespacePersistence
from monarch.actor import Actor, endpoint


class NamespaceServerActor(Actor):
    """Actor for cross-process namespace testing."""

    def __init__(self, value: int) -> None:
        self._value = value
        self._call_count = 0

    @endpoint
    def get_value(self) -> int:
        return self._value

    @endpoint
    def set_value(self, value: int) -> None:
        self._value = value

    @endpoint
    def increment_and_get(self) -> int:
        self._call_count += 1
        return self._call_count

    @endpoint
    def get_call_count(self) -> int:
        return self._call_count


async def run_server(
    tier: str,
    namespace_name: str,
    actor_name: str,
    initial_value: int,
    num_actors: int,
) -> NoReturn:
    """Run the namespace server."""
    # Configure the SMC namespace
    configure_namespace(
        NamespacePersistence.SMC,
        name=namespace_name,
        tier=tier,
    )

    # Create a host mesh with a known name
    host_mesh_name = f"{actor_name}_host"
    host = HostMesh.allocate_nonblocking(
        host_mesh_name,
        Extent([], []),
        LocalAllocator(),
        bootstrap_cmd=_bootstrap_cmd(),
    )

    # Spawn a proc mesh
    proc_mesh_name = f"{actor_name}_proc"
    proc = host.spawn_procs(per_host={"workers": num_actors}, name=proc_mesh_name)

    # Spawn the actor mesh with the specified name
    actors = proc.spawn(actor_name, NamespaceServerActor, initial_value)

    # Wait for actors to be initialized
    result = await actors.get_value.call()
    assert all(v == initial_value for v in result.values())

    # Print ready signal to stdout (flush to ensure it's seen immediately)
    print("READY", flush=True)
    print(f"Actor mesh '{actor_name}' registered with {num_actors} actors", flush=True)
    print(f"Host mesh: {host_mesh_name}", flush=True)
    print(f"Proc mesh: {proc_mesh_name}", flush=True)
    print(f"Initial value: {initial_value}", flush=True)
    print("Waiting for input to shutdown...", flush=True)

    # Set up signal handler for graceful shutdown
    shutdown_event: asyncio.Event = asyncio.Event()

    def signal_handler(signum: int, frame: object) -> None:
        print(f"\nReceived signal {signum}, shutting down...", flush=True)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Wait for either stdin input or signal
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    async def wait_for_stdin() -> None:
        await loop.run_in_executor(None, sys.stdin.readline)
        shutdown_event.set()

    stdin_task = asyncio.create_task(wait_for_stdin())

    try:
        await shutdown_event.wait()
    finally:
        stdin_task.cancel()
        try:
            await stdin_task
        except asyncio.CancelledError:
            pass

    print("Server shutting down...", flush=True)
    # Exit cleanly - the actor mesh will be unregistered on cleanup
    sys.exit(0)


@click.command()
@click.option(
    "--tier",
    required=True,
    help="SMC tier name for the namespace",
)
@click.option(
    "--namespace-name",
    default="monarch",
    help="Namespace name (default: monarch)",
)
@click.option(
    "--actor-name",
    required=True,
    help="Name for the actor mesh",
)
@click.option(
    "--initial-value",
    default=42,
    type=int,
    help="Initial value for the actors (default: 42)",
)
@click.option(
    "--num-actors",
    default=2,
    type=int,
    help="Number of actors to spawn (default: 2)",
)
def main(
    tier: str,
    namespace_name: str,
    actor_name: str,
    initial_value: int,
    num_actors: int,
) -> None:
    """Start the namespace server with an actor mesh."""
    asyncio.run(run_server(tier, namespace_name, actor_name, initial_value, num_actors))


if __name__ == "__main__":
    main()
