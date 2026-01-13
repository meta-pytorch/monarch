# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase

import pytest
from monarch._rust_bindings.monarch_hyperactor.shape import Shape, Slice
from monarch._src.actor.shape import ShapeExt


class TestShapeSlicing(TestCase):
    def test_shape_at_removes_dimension(self) -> None:
        """Test that at() removes dimensions and updates offset
        correctly."""

        slice_obj = Slice(offset=0, sizes=[2, 3, 4], strides=[12, 4, 1])
        shape = Shape(["batch", "height", "width"], slice_obj)

        # Test removing first dimension
        result = shape.at("batch", 1)
        self.assertEqual(result.labels, ["height", "width"])
        self.assertEqual(result.ndslice.sizes, [3, 4])
        self.assertEqual(result.ndslice.strides, [4, 1])
        self.assertEqual(result.ndslice.offset, 12)  # 1 * 12

        # Test removing middle dimension
        result = shape.at("height", 2)
        self.assertEqual(result.labels, ["batch", "width"])
        self.assertEqual(result.ndslice.sizes, [2, 4])
        self.assertEqual(result.ndslice.strides, [12, 1])
        self.assertEqual(result.ndslice.offset, 8)  # 2 * 4

        # Test removing last dimension
        result = shape.at("width", 3)
        self.assertEqual(result.labels, ["batch", "height"])
        self.assertEqual(result.ndslice.sizes, [2, 3])
        self.assertEqual(result.ndslice.strides, [12, 4])
        self.assertEqual(result.ndslice.offset, 3)  # 3  * 1

    def test_shape_select_keeps_dimension(self) -> None:
        """Test that select() keeps dimensions but changes sizes."""

        slice_obj = Slice.new_row_major([4, 6])
        shape = Shape(["rows", "cols"], slice_obj)

        # Test range selection
        result = shape.select("rows", slice(1, 3))
        self.assertEqual(result.labels, ["rows", "cols"])
        self.assertEqual(result.ndslice.sizes, [2, 6])  # 3-1=2 rows
        self.assertEqual(result.ndslice.offset, 6)  # 1 * 6

        # Test step selection
        result = shape.select("cols", slice(0, 6, 2))
        self.assertEqual(result.labels, ["rows", "cols"])
        self.assertEqual(result.ndslice.sizes, [4, 3])  # every 2nd col = 3 cols
        self.assertEqual(result.ndslice.strides, [6, 2])  # stride becomes 2

    def test_shape_slice_mixed_operations(self) -> None:
        """Test mixing at() and select() operations."""

        slice_obj = Slice.new_row_major([2, 3, 4])
        shape = Shape(["batch", "height", "width"], slice_obj)

        # Chain operations: select then at
        result = shape.select("width", slice(1, 4)).at("batch", 0)
        self.assertEqual(result.labels, ["height", "width"])
        self.assertEqual(result.ndslice.sizes, [3, 3])

        # Chain operations: at then select
        result = shape.at("height", 1).select("width", slice(2, 4))
        self.assertEqual(result.labels, ["batch", "width"])
        self.assertEqual(result.ndslice.sizes, [2, 2])

    def test_shape_slice_errors(self) -> None:
        """Test error conditions."""
        slice_obj = Slice.new_row_major([2, 3])
        shape = Shape(["rows", "cols"], slice_obj)

        # Test invalid label
        with self.assertRaises(ValueError):
            shape.at("nonexistent", 0)

        # Test index out of range
        with self.assertRaises(ValueError):
            shape.at("rows", 5)

        # Test negative index (Python-Rust boundary issue)
        with self.assertRaises(OverflowError):  # Changed from ValueError
            shape.at("rows", -1)

    def test_shape_slice_comprehensive(self) -> None:
        """Comprehensive test of slice() method."""

        slice_obj = Slice.new_row_major([4, 5, 6])
        shape = Shape(["a", "b", "c"], slice_obj)

        # Test integer selection (removes dimensions)
        result = ShapeExt.slice(shape, a=1, c=2)
        self.assertEqual(result.labels, ["b"])
        self.assertEqual(result.ndslice.sizes, [5])

        # Test slice selection (keeps dimensions)
        result = ShapeExt.slice(shape, b=slice(1, 4), c=slice(0, 6, 2))
        self.assertEqual(result.labels, ["a", "b", "c"])
        self.assertEqual(result.ndslice.sizes, [4, 3, 3])

        # Test mixed selection
        result = ShapeExt.slice(shape, a=2, b=slice(1, 3))
        self.assertEqual(result.labels, ["b", "c"])
        self.assertEqual(result.ndslice.sizes, [2, 6])
