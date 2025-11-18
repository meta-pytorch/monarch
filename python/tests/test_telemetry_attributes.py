# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import MagicMock

from monarch._src.actor.telemetry import Counter, UpDownCounter


class CounterAttributesTest(unittest.TestCase):
    """Tests to verify that Counter.add() correctly passes attributes to the underlying Rust implementation."""

    def test_counter_add_passes_attributes(self) -> None:
        """Test that Counter.add() accepts and processes attributes correctly."""
        # Setup: Create a counter instance
        counter = Counter("test_counter")

        # Execute: Call add with attributes (should not raise an error)
        # Note: We can't directly verify the Rust implementation receives the attributes,
        # but we can verify the method accepts them and the conversion logic works
        try:
            counter.add(1, attributes={"method": "test_method", "actor_count": 5})
            # If we get here without exception, the attributes were processed
            success = True
        except Exception:
            success = False

        # Assert: Verify the call succeeded
        self.assertTrue(
            success, "Counter.add() should accept and process attributes without error"
        )

    def test_counter_add_with_none_attributes(self) -> None:
        """Test that Counter.add() works correctly when attributes is None."""
        # Setup: Create a counter instance
        counter = Counter("test_counter_none")

        # Execute: Call add without attributes (should not raise an error)
        try:
            counter.add(1, attributes=None)
            success = True
        except Exception:
            success = False

        # Assert: Verify the call succeeded
        self.assertTrue(
            success, "Counter.add() should work correctly when attributes is None"
        )

    def test_counter_add_increments_value(self) -> None:
        """Test that Counter.add() correctly increments the counter value."""
        # Setup: Create a counter instance and mock the inner Rust object
        counter = Counter("test_counter_increment")
        mock_inner = MagicMock()
        counter.inner = mock_inner

        # Execute: Add values to the counter multiple times
        # This tests that the method correctly converts and passes values to the Rust implementation
        counter.add(5, attributes={"method": "test"})
        counter.add(10, attributes={"method": "test"})
        counter.add(3, attributes={"method": "test"})

        # Assert: Verify the inner Rust implementation was called with correct values
        self.assertEqual(mock_inner.add.call_count, 3)
        # Verify first call: add(5, attributes={"method": "test"})
        self.assertEqual(mock_inner.add.call_args_list[0][0][0], 5)
        self.assertEqual(
            mock_inner.add.call_args_list[0][1]["attributes"], {"method": "test"}
        )
        # Verify second call: add(10, attributes={"method": "test"})
        self.assertEqual(mock_inner.add.call_args_list[1][0][0], 10)
        self.assertEqual(
            mock_inner.add.call_args_list[1][1]["attributes"], {"method": "test"}
        )
        # Verify third call: add(3, attributes={"method": "test"})
        self.assertEqual(mock_inner.add.call_args_list[2][0][0], 3)
        self.assertEqual(
            mock_inner.add.call_args_list[2][1]["attributes"], {"method": "test"}
        )


class UpDownCounterAttributesTest(unittest.TestCase):
    """Tests to verify that UpDownCounter.add() correctly passes attributes to the underlying Rust implementation."""

    def test_updowncounter_add_passes_attributes(self) -> None:
        """Test that UpDownCounter.add() accepts and processes attributes correctly."""
        # Setup: Create an updowncounter instance
        counter = UpDownCounter("test_updowncounter")

        # Execute: Call add with attributes (should not raise an error)
        try:
            counter.add(1, attributes={"method": "test_method", "actor_count": 5})
            success = True
        except Exception:
            success = False

        # Assert: Verify the call succeeded
        self.assertTrue(
            success,
            "UpDownCounter.add() should accept and process attributes without error",
        )

    def test_updowncounter_add_with_none_attributes(self) -> None:
        """Test that UpDownCounter.add() works correctly when attributes is None."""
        # Setup: Create an updowncounter instance
        counter = UpDownCounter("test_updowncounter_none")

        # Execute: Call add without attributes (should not raise an error)
        try:
            counter.add(1, attributes=None)
            success = True
        except Exception:
            success = False

        # Assert: Verify the call succeeded
        self.assertTrue(
            success,
            "UpDownCounter.add() should work correctly when attributes is None",
        )

    def test_updowncounter_add_changes_value(self) -> None:
        """Test that UpDownCounter.add() correctly changes the counter value."""
        # Setup: Create an updowncounter instance and mock the inner Rust object
        counter = UpDownCounter("test_updowncounter_change")
        mock_inner = MagicMock()
        counter.inner = mock_inner

        # Execute: Add positive and negative values to the counter
        # This tests that the method correctly converts and passes values to the Rust implementation
        counter.add(10, attributes={"method": "test"})
        counter.add(-5, attributes={"method": "test"})
        counter.add(3, attributes={"method": "test"})
        counter.add(-2, attributes={"method": "test"})

        # Assert: Verify the inner Rust implementation was called with correct values
        self.assertEqual(mock_inner.add.call_count, 4)
        # Verify first call: add(10, attributes={"method": "test"})
        self.assertEqual(mock_inner.add.call_args_list[0][0][0], 10)
        self.assertEqual(
            mock_inner.add.call_args_list[0][1]["attributes"], {"method": "test"}
        )
        # Verify second call: add(-5, attributes={"method": "test"})
        self.assertEqual(mock_inner.add.call_args_list[1][0][0], -5)
        self.assertEqual(
            mock_inner.add.call_args_list[1][1]["attributes"], {"method": "test"}
        )
        # Verify third call: add(3, attributes={"method": "test"})
        self.assertEqual(mock_inner.add.call_args_list[2][0][0], 3)
        self.assertEqual(
            mock_inner.add.call_args_list[2][1]["attributes"], {"method": "test"}
        )
        # Verify fourth call: add(-2, attributes={"method": "test"})
        self.assertEqual(mock_inner.add.call_args_list[3][0][0], -2)
        self.assertEqual(
            mock_inner.add.call_args_list[3][1]["attributes"], {"method": "test"}
        )
