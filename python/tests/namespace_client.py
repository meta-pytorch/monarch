# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Namespace client script for cross-process testing.

This script loads an actor mesh from the SMC namespace and makes endpoint calls.
It's used in conjunction with namespace_server.py to test cross-process namespace
functionality.

Usage:
    buck run //monarch/python/tests:namespace_client -- \
        --tier smc.canaryservice.test.monarch_test123 \
        --actor-name my_test_actors
"""

import asyncio

import click
from monarch._src.actor.actor_mesh import ActorMesh
from monarch._src.actor.namespace import (
    configure_namespace,
    load,
    MeshKind,
    NamespacePersistence,
)
from monarch.actor import Actor, endpoint


# Actor class must match the one in namespace_server.py
# In production, this would be in a shared library
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


async def run_client(
    tier: str,
    namespace_name: str,
    actor_name: str,
    test_value: int,
) -> None:
    """Run the namespace client."""
    # Configure the SMC namespace (same tier as server)
    configure_namespace(
        NamespacePersistence.SMC,
        name=namespace_name,
        tier=tier,
    )

    print(f"Loading actor mesh '{actor_name}' from namespace...", flush=True)

    # Load the actor mesh from the namespace
    actors: ActorMesh[NamespaceServerActor] = await load(
        MeshKind.Actor,
        actor_name,
        NamespaceServerActor,
    )

    print(f"Successfully loaded actor mesh: {type(actors)}", flush=True)

    # Test 1: Get initial values
    print("\n=== Test 1: Get initial values ===", flush=True)
    # pyre-ignore[16]: Pyre doesn't understand that @endpoint methods become Endpoint objects on ActorMesh
    values = await actors.get_value.call()
    print(f"Initial values: {values}", flush=True)
    for idx, val in values.items():
        print(f"  Actor {idx}: value={val}", flush=True)

    # Test 2: Set new values
    print("\n=== Test 2: Set new values ===", flush=True)
    # pyre-ignore[16]: Pyre doesn't understand that @endpoint methods become Endpoint objects on ActorMesh
    await actors.set_value.call(test_value)
    print(f"Set all actors to value={test_value}", flush=True)

    # Verify the values were set
    # pyre-ignore[16]: Pyre doesn't understand that @endpoint methods become Endpoint objects on ActorMesh
    values = await actors.get_value.call()
    print(f"New values: {values}", flush=True)
    assert all(v == test_value for v in values.values()), "Value mismatch!"
    print("✓ All values updated correctly", flush=True)

    # Test 3: Increment and get call count
    print("\n=== Test 3: Test increment_and_get ===", flush=True)
    for i in range(3):
        # pyre-ignore[16]: Pyre doesn't understand that @endpoint methods become Endpoint objects on ActorMesh
        counts = await actors.increment_and_get.call()
        print(f"Iteration {i + 1}: {counts}", flush=True)

    # Test 4: Get final call counts
    print("\n=== Test 4: Get final call counts ===", flush=True)
    # pyre-ignore[16]: Pyre doesn't understand that @endpoint methods become Endpoint objects on ActorMesh
    call_counts = await actors.get_call_count.call()
    print(f"Final call counts: {call_counts}", flush=True)
    for idx, count in call_counts.items():
        print(f"  Actor {idx}: called {count} times", flush=True)
        assert count == 3, f"Expected 3 calls, got {count}"

    print("\n✓ All tests passed!", flush=True)
    print("SUCCESS", flush=True)  # Signal for test harness


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
    help="Name of the actor mesh to load",
)
@click.option(
    "--test-value",
    default=100,
    type=int,
    help="Value to set in the test (default: 100)",
)
def main(
    tier: str,
    namespace_name: str,
    actor_name: str,
    test_value: int,
) -> None:
    """Load actor mesh from namespace and run tests."""
    asyncio.run(run_client(tier, namespace_name, actor_name, test_value))


if __name__ == "__main__":
    main()
