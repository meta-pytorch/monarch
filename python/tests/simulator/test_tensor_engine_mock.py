# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from monarch._src.actor.actor_mesh import Actor
from monarch._src.actor.mock import (
    _tensor_engine_registry,
    get_tensor_engine_factory,
    get_tensor_engine_mock_registry_state,
    patch_tensor_engine,
    set_tensor_engine_mock_registry_state,
    TensorEngineFactory,
)

if TYPE_CHECKING:
    from monarch._src.actor.proc_mesh import DeviceMesh, ProcMesh


class TestActor(Actor):
    """A simple actor class for testing tensor engine mocking."""

    pass


class AnotherTestActor(Actor):
    """Another actor class for testing multiple patches."""

    pass


def mock_tensor_engine_factory(proc_mesh: "ProcMesh") -> "DeviceMesh":
    """A mock tensor engine factory for testing."""
    return MagicMock()


def another_mock_factory(proc_mesh: "ProcMesh") -> "DeviceMesh":
    """Another mock factory for testing multiple patches."""
    return MagicMock()


class TestTensorEngineMock(unittest.TestCase):
    def setUp(self) -> None:
        # Clear registry between tests
        _tensor_engine_registry.clear()

    def test_patch_tensor_engine_as_context_manager(self) -> None:
        # Setup: Verify no factory is registered initially
        factory = get_tensor_engine_factory(TestActor)
        self.assertIsNone(factory)

        # Execute: Use patch_tensor_engine as context manager
        with patch_tensor_engine(TestActor, mock_tensor_engine_factory):
            # Assert: Mock factory is returned during patch
            patched_factory = get_tensor_engine_factory(TestActor)
            self.assertEqual(patched_factory, mock_tensor_engine_factory)

        # Assert: Factory is removed after exiting context
        restored_factory = get_tensor_engine_factory(TestActor)
        self.assertIsNone(restored_factory)

    def test_patch_tensor_engine_as_sync_decorator(self) -> None:
        # Setup: Define a function to be decorated
        @patch_tensor_engine(TestActor, mock_tensor_engine_factory)
        def test_function() -> TensorEngineFactory | None:
            return get_tensor_engine_factory(TestActor)

        # Execute: Call the decorated function
        result = test_function()

        # Assert: Mock factory was used inside the decorated function
        self.assertEqual(result, mock_tensor_engine_factory)

        # Assert: Factory is removed after function execution
        restored_factory = get_tensor_engine_factory(TestActor)
        self.assertIsNone(restored_factory)

    def test_patch_tensor_engine_default_factory(self) -> None:
        # Execute: Use patch_tensor_engine without providing a factory
        with patch_tensor_engine(TestActor):
            # Assert: A default factory is registered
            factory = get_tensor_engine_factory(TestActor)
            self.assertIsNotNone(factory)

        # Assert: Factory is removed after exiting context
        restored_factory = get_tensor_engine_factory(TestActor)
        self.assertIsNone(restored_factory)

    def test_multiple_patches_context_manager(self) -> None:
        # Execute: Use multiple patches
        with (
            patch_tensor_engine(TestActor, mock_tensor_engine_factory),
            patch_tensor_engine(AnotherTestActor, another_mock_factory),
        ):
            # Assert: Both actors have factories registered
            self.assertEqual(
                get_tensor_engine_factory(TestActor), mock_tensor_engine_factory
            )
            self.assertEqual(
                get_tensor_engine_factory(AnotherTestActor), another_mock_factory
            )

        # Assert: Both factories are removed
        self.assertIsNone(get_tensor_engine_factory(TestActor))
        self.assertIsNone(get_tensor_engine_factory(AnotherTestActor))

    def test_sequential_patches(self) -> None:
        # Setup: Verify no factory initially
        self.assertIsNone(get_tensor_engine_factory(TestActor))

        # Execute: Use first patch
        with patch_tensor_engine(TestActor, mock_tensor_engine_factory):
            # Assert: First patch is active
            self.assertEqual(
                get_tensor_engine_factory(TestActor), mock_tensor_engine_factory
            )

        # Assert: Factory removed after first patch
        self.assertIsNone(get_tensor_engine_factory(TestActor))

        # Execute: Use second patch
        with patch_tensor_engine(TestActor, another_mock_factory):
            # Assert: Second patch is active
            self.assertEqual(get_tensor_engine_factory(TestActor), another_mock_factory)

        # Assert: Factory removed after second patch
        self.assertIsNone(get_tensor_engine_factory(TestActor))

    def test_patch_unknown_actor_returns_none(self) -> None:
        # Setup: Create an actor class that is not in registry
        class UnknownActor(Actor):
            pass

        # Execute: Get factory for unknown actor
        result = get_tensor_engine_factory(UnknownActor)

        # Assert: Returns None when not found in registry
        self.assertIsNone(result)

    def test_patch_tensor_engine_exit_removes_registry_entry(self) -> None:
        # Setup: Verify registry is empty
        self.assertEqual(len(_tensor_engine_registry), 0)

        # Execute: Use patch and verify registry behavior
        with patch_tensor_engine(TestActor, mock_tensor_engine_factory):
            # Assert: Registry contains the patch (using class as key, not __name__)
            self.assertIn(TestActor, _tensor_engine_registry)
            self.assertEqual(
                _tensor_engine_registry[TestActor],
                mock_tensor_engine_factory,
            )

        # Assert: Registry entry is cleaned up after exit
        self.assertNotIn(TestActor, _tensor_engine_registry)
        self.assertEqual(len(_tensor_engine_registry), 0)

    def test_patch_tensor_engine_exception_cleanup(self) -> None:
        # Setup: Verify initial state
        self.assertIsNone(get_tensor_engine_factory(TestActor))
        self.assertEqual(len(_tensor_engine_registry), 0)

        # Execute: Use patch that raises exception
        try:
            with patch_tensor_engine(TestActor, mock_tensor_engine_factory):
                # Assert: Patch is active
                self.assertEqual(
                    get_tensor_engine_factory(TestActor), mock_tensor_engine_factory
                )
                self.assertIn(TestActor, _tensor_engine_registry)
                # Raise exception to test cleanup
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Assert: Factory is removed even after exception
        self.assertIsNone(get_tensor_engine_factory(TestActor))
        # Assert: Registry is cleaned up even after exception
        self.assertNotIn(TestActor, _tensor_engine_registry)
        self.assertEqual(len(_tensor_engine_registry), 0)


class TestTensorEngineMockAsync(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        # Clear registry between tests
        _tensor_engine_registry.clear()

    async def test_patch_tensor_engine_as_async_decorator(self) -> None:
        # Setup: Define an async function to be decorated
        @patch_tensor_engine(TestActor, mock_tensor_engine_factory)
        async def async_test_function() -> TensorEngineFactory | None:
            return get_tensor_engine_factory(TestActor)

        # Execute: Call the decorated async function
        result = await async_test_function()

        # Assert: Mock factory was used inside the decorated async function
        self.assertEqual(result, mock_tensor_engine_factory)

        # Assert: Factory is removed after function execution
        restored_factory = get_tensor_engine_factory(TestActor)
        self.assertIsNone(restored_factory)

    async def test_patch_tensor_engine_as_async_context_manager(self) -> None:
        # Setup: Verify no factory initially
        factory = get_tensor_engine_factory(TestActor)
        self.assertIsNone(factory)

        # Execute: Use patch_tensor_engine as async context manager
        async with patch_tensor_engine(TestActor, mock_tensor_engine_factory):
            # Assert: Mock factory is returned during patch
            patched_factory = get_tensor_engine_factory(TestActor)
            self.assertEqual(patched_factory, mock_tensor_engine_factory)

        # Assert: Factory is removed after exiting context
        restored_factory = get_tensor_engine_factory(TestActor)
        self.assertIsNone(restored_factory)


class TestTensorEngineMockRegistryPropagation(unittest.TestCase):
    def setUp(self) -> None:
        # Clear registry between tests
        _tensor_engine_registry.clear()

    def test_tensor_engine_mock_registry_state_get_and_set(self) -> None:
        """
        Test that tensor engine mock registry state can be captured and restored.

        This is the core mechanism for propagating mocks to remote processes:
        1. Capture registry state with get_tensor_engine_mock_registry_state()
        2. Transfer state to remote process (via SetupActor)
        3. Restore state with set_tensor_engine_mock_registry_state()
        """
        # Setup: Verify registry is initially empty
        self.assertEqual(len(_tensor_engine_registry), 0)
        initial_state = get_tensor_engine_mock_registry_state()
        self.assertEqual(len(initial_state), 0)

        # Execute: Patch a tensor engine and capture the state
        with patch_tensor_engine(TestActor, mock_tensor_engine_factory):
            # Verify the patch is active
            self.assertEqual(
                get_tensor_engine_factory(TestActor), mock_tensor_engine_factory
            )

            # Capture the registry state (this is what would be sent to remote process)
            captured_state = get_tensor_engine_mock_registry_state()
            self.assertEqual(len(captured_state), 1)
            # Registry now uses class objects as keys, not __name__ strings
            self.assertIn(TestActor, captured_state)
            self.assertEqual(captured_state[TestActor], mock_tensor_engine_factory)

        # After exiting the context, the registry should be empty
        self.assertEqual(len(_tensor_engine_registry), 0)
        self.assertIsNone(get_tensor_engine_factory(TestActor))

        # Execute: Restore the captured state (simulating what SetupActor does)
        set_tensor_engine_mock_registry_state(captured_state)

        # Assert: The mock should now be active again
        self.assertEqual(
            get_tensor_engine_factory(TestActor), mock_tensor_engine_factory
        )
        self.assertEqual(len(_tensor_engine_registry), 1)

        # Cleanup
        _tensor_engine_registry.clear()


if __name__ == "__main__":
    unittest.main()
