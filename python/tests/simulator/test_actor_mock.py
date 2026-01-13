# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

import pytest
from monarch._src.actor.actor_mesh import Actor
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.mock import _actor_registry, get_actor_class, patch_actor


class OriginalActor(Actor):
    """A simple actor class for testing."""

    def __init__(self) -> None:
        self.name = "original"

    @endpoint
    async def get_name(self) -> str:
        return self.name

    @endpoint
    async def compute(self, x: int) -> int:
        return x * 2


class MockActor(Actor):
    """Mock actor for testing."""

    def __init__(self) -> None:
        self.name = "mock"

    @endpoint
    async def get_name(self) -> str:
        return self.name

    @endpoint
    async def compute(self, x: int) -> int:
        return x * 10


class AnotherMockActor(Actor):
    """Another mock actor for testing multiple patches."""

    def __init__(self) -> None:
        self.name = "another_mock"

    @endpoint
    async def get_name(self) -> str:
        return self.name


class InnerActor(Actor):
    """Actor to be mocked when spawned from another actor."""

    def __init__(self) -> None:
        self.actor_type = "real"

    @endpoint
    def get_actor_type(self) -> str:
        return self.actor_type


class MockInnerActor(Actor):
    """Mock version of InnerActor."""

    def __init__(self) -> None:
        self.actor_type = "mock"

    @endpoint
    def get_actor_type(self) -> str:
        return self.actor_type


class TestActorMock(unittest.TestCase):
    def setUp(self) -> None:
        # Clear registry between tests
        _actor_registry.clear()

    def test_patch_actor_as_context_manager(self) -> None:
        # Setup: Verify original actor is used initially
        original_class = get_actor_class(OriginalActor)
        self.assertEqual(original_class, OriginalActor)

        # Execute: Use patch_actor as context manager
        with patch_actor(OriginalActor, MockActor):
            # Assert: Mock actor is returned during patch
            patched_class = get_actor_class(OriginalActor)
            self.assertEqual(patched_class, MockActor)

        # Assert: Original actor is restored after exiting context
        restored_class = get_actor_class(OriginalActor)
        self.assertEqual(restored_class, OriginalActor)

    def test_patch_actor_as_sync_decorator(self) -> None:
        # Setup: Define a function to be decorated
        @patch_actor(OriginalActor, MockActor)
        def test_function() -> type[Actor]:
            return get_actor_class(OriginalActor)

        # Execute: Call the decorated function
        result = test_function()

        # Assert: Mock actor was used inside the decorated function
        self.assertEqual(result, MockActor)

        # Assert: Original actor is restored after function execution
        restored_class = get_actor_class(OriginalActor)
        self.assertEqual(restored_class, OriginalActor)

    def test_multiple_patches_context_manager(self) -> None:
        # Setup: Create a second dummy actor class for testing
        class SecondOriginalActor(Actor):
            pass

        # Execute: Use multiple patches
        with (
            patch_actor(OriginalActor, MockActor),
            patch_actor(SecondOriginalActor, AnotherMockActor),
        ):
            # Assert: Both actors are patched
            self.assertEqual(get_actor_class(OriginalActor), MockActor)
            self.assertEqual(get_actor_class(SecondOriginalActor), AnotherMockActor)

        # Assert: Both actors are restored
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)
        self.assertEqual(get_actor_class(SecondOriginalActor), SecondOriginalActor)

    def test_sequential_patches(self) -> None:
        # Setup: Verify original state
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)

        # Execute: Use first patch
        with patch_actor(OriginalActor, MockActor):
            # Assert: First patch is active
            self.assertEqual(get_actor_class(OriginalActor), MockActor)

        # Assert: Original actor is restored after first patch
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)

        # Execute: Use second patch
        with patch_actor(OriginalActor, AnotherMockActor):
            # Assert: Second patch is active
            self.assertEqual(get_actor_class(OriginalActor), AnotherMockActor)

        # Assert: Original actor is restored after second patch
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)

    def test_nested_patches_same_actor(self) -> None:
        """Test that nested patches of the same actor work correctly.

        This tests the fix for the KeyError issue when an actor is already patched.
        The inner patch should temporarily override the outer patch, and when the
        inner patch exits, the outer patch should be restored (not the original).
        """
        # Verify original state
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)

        with patch_actor(OriginalActor, MockActor):
            # First patch is active
            self.assertEqual(get_actor_class(OriginalActor), MockActor)

            with patch_actor(OriginalActor, AnotherMockActor):
                # Second (inner) patch is active
                self.assertEqual(get_actor_class(OriginalActor), AnotherMockActor)

            # After inner patch exits, first patch should be restored
            self.assertEqual(get_actor_class(OriginalActor), MockActor)

        # After all patches exit, original should be restored
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)

    def test_patch_unknown_actor_returns_original(self) -> None:
        # Setup: Create an actor class that is not in registry
        class UnknownActor(Actor):
            pass

        # Execute: Get actor class for unknown actor
        result = get_actor_class(UnknownActor)

        # Assert: Returns the original class when not found in registry
        self.assertEqual(result, UnknownActor)

    def test_patch_actor_exit_removes_registry_entry(self) -> None:
        # Setup: Verify registry is empty
        self.assertEqual(len(_actor_registry), 0)

        # Execute: Use patch and verify registry behavior
        with patch_actor(OriginalActor, MockActor):
            # Assert: Registry contains the patch (using class as key, not __name__)
            self.assertIn(OriginalActor, _actor_registry)
            self.assertEqual(_actor_registry[OriginalActor], MockActor)

        # Assert: Registry entry is cleaned up after exit
        self.assertNotIn(OriginalActor, _actor_registry)
        self.assertEqual(len(_actor_registry), 0)

    def test_patch_actor_exception_cleanup(self) -> None:
        # Setup: Verify initial state
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)
        self.assertEqual(len(_actor_registry), 0)

        # Execute: Use patch that raises exception
        try:
            with patch_actor(OriginalActor, MockActor):
                # Assert: Patch is active
                self.assertEqual(get_actor_class(OriginalActor), MockActor)
                self.assertIn(OriginalActor, _actor_registry)
                # Raise exception to test cleanup
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Assert: Original actor is restored even after exception
        self.assertEqual(get_actor_class(OriginalActor), OriginalActor)
        # Assert: Registry is cleaned up even after exception
        self.assertNotIn(OriginalActor, _actor_registry)
        self.assertEqual(len(_actor_registry), 0)


class TestActorMockAsync(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        # Clear registry between tests
        _actor_registry.clear()

    async def test_patch_actor_as_async_decorator(self) -> None:
        # Setup: Define an async function to be decorated
        @patch_actor(OriginalActor, MockActor)
        async def async_test_function() -> type[Actor]:
            return get_actor_class(OriginalActor)

        # Execute: Call the decorated async function
        result = await async_test_function()

        # Assert: Mock actor was used inside the decorated async function
        self.assertEqual(result, MockActor)

        # Assert: Original actor is restored after function execution
        restored_class = get_actor_class(OriginalActor)
        self.assertEqual(restored_class, OriginalActor)

    async def test_patch_actor_as_async_context_manager(self) -> None:
        # Setup: Verify original actor is used initially
        original_class = get_actor_class(OriginalActor)
        self.assertEqual(original_class, OriginalActor)

        # Execute: Use patch_actor as async context manager
        async with patch_actor(OriginalActor, MockActor):
            # Assert: Mock actor is returned during patch
            patched_class = get_actor_class(OriginalActor)
            self.assertEqual(patched_class, MockActor)

        # Assert: Original actor is restored after exiting context
        restored_class = get_actor_class(OriginalActor)
        self.assertEqual(restored_class, OriginalActor)


class TestMockRegistryPropagation(unittest.TestCase):
    def setUp(self) -> None:
        # Clear registry between tests
        _actor_registry.clear()

    def test_mock_registry_state_get_and_set(self) -> None:
        """
        Test that mock registry state can be captured and restored.

        This is the core mechanism for propagating mocks to remote processes:
        1. Capture registry state with get_actor_mock_registry_state()
        2. Transfer state to remote process (via SetupActor)
        3. Restore state with set_actor_mock_registry_state()
        """
        from monarch._src.actor.mock import (
            get_actor_mock_registry_state,
            set_actor_mock_registry_state,
        )

        # Setup: Verify registry is initially empty
        self.assertEqual(len(_actor_registry), 0)
        initial_state = get_actor_mock_registry_state()
        self.assertEqual(len(initial_state), 0)

        # Execute: Patch an actor and capture the state
        with patch_actor(InnerActor, MockInnerActor):
            # Verify the patch is active
            self.assertEqual(get_actor_class(InnerActor), MockInnerActor)

            # Capture the registry state (this is what would be sent to remote process)
            captured_state = get_actor_mock_registry_state()
            self.assertEqual(len(captured_state), 1)
            # Registry now uses class objects as keys, not __name__ strings
            self.assertIn(InnerActor, captured_state)
            self.assertEqual(captured_state[InnerActor], MockInnerActor)

        # After exiting the context, the registry should be empty
        self.assertEqual(len(_actor_registry), 0)
        self.assertEqual(get_actor_class(InnerActor), InnerActor)

        # Execute: Restore the captured state (simulating what SetupActor does)
        set_actor_mock_registry_state(captured_state)

        # Assert: The mock should now be active again
        self.assertEqual(get_actor_class(InnerActor), MockInnerActor)
        self.assertEqual(len(_actor_registry), 1)

        # Cleanup
        _actor_registry.clear()


class OuterActor(Actor):
    """Actor that spawns InnerActor from within a subprocess."""

    @endpoint
    def spawn_inner_and_get_type(self) -> str:
        """Spawn InnerActor using this_proc() and return its type."""
        from monarch._src.actor.host_mesh import this_proc

        inner = this_proc().spawn("inner", InnerActor)
        return inner.get_actor_type.call_one().get()


class TestMockPropagationEndToEnd(unittest.TestCase):
    """
    End-to-end test for mock propagation to subprocesses.

    This test verifies the reviewer's requirement:
    "To test, spawn an actor in a sub-process and have that actor
    spawn the to-be-mocked actor."
    """

    def setUp(self) -> None:
        # Clear registry between tests
        _actor_registry.clear()

    # pyre-ignore[56]: Pyre cannot infer type of pytest.mark.timeout decorator
    @pytest.mark.timeout(60)
    def test_outer_actor_spawns_mocked_inner_actor(self) -> None:
        """
        Test that when OuterActor (in subprocess) spawns InnerActor,
        the mocked version is used.

        Flow:
        1. Client patches InnerActor with MockInnerActor
        2. Client spawns OuterActor on a ProcMesh (runs in subprocess)
        3. OuterActor spawns InnerActor via this_proc().spawn()
        4. InnerActor should be MockInnerActor (returns "mock" not "real")
        """
        from monarch._src.actor.host_mesh import create_local_host_mesh

        with patch_actor(InnerActor, MockInnerActor):
            # Create a host mesh and spawn processes
            host = create_local_host_mesh()
            proc_mesh = host.spawn_procs(name="test_proc")

            # Spawn OuterActor in the subprocess
            outer = proc_mesh.spawn("outer", OuterActor)

            # OuterActor spawns InnerActor and returns its type
            # If mock propagation works, this should return "mock"
            # If mock propagation fails, this would return "real"
            actor_type = outer.spawn_inner_and_get_type.call_one().get()

            self.assertEqual(
                actor_type,
                "mock",
                "InnerActor should be mocked when spawned from OuterActor in subprocess",
            )


if __name__ == "__main__":
    unittest.main()
