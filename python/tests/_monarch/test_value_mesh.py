# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase

from monarch._rust_bindings.monarch_hyperactor.shape import Shape, Slice
from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh


class TestValueMesh(TestCase):
    def test_construct(self) -> None:
        shape = Shape(["n"], Slice.new_row_major([3]))
        vm = ValueMesh(shape, [10, 20, 30])
        self.assertIsInstance(vm, ValueMesh)

    def test_len_and_values(self) -> None:
        shape = Shape(["n"], Slice.new_row_major([3]))
        vm = ValueMesh(shape, [10, 20, 30])
        self.assertEqual(len(vm), 3)
        self.assertEqual(vm.values(), [10, 20, 30])

    def test_get_in_bounds(self) -> None:
        shape = Shape(["n"], Slice.new_row_major([3]))
        vm = ValueMesh(shape, [10, 20, 30])
        self.assertEqual(vm.get(0), 10)
        self.assertEqual(vm.get(1), 20)
        self.assertEqual(vm.get(2), 30)

    def test_get_out_of_range(self) -> None:
        shape = Shape(["n"], Slice.new_row_major([3]))
        vm = ValueMesh(shape, [10, 20, 30])
        with self.assertRaisesRegex(ValueError, "out of range"):
            vm.get(3)

    def test_from_indexed_ok(self) -> None:
        shape = Shape(["n"], Slice.new_row_major([3]))
        vm = ValueMesh.from_indexed(shape, [(2, "c"), (0, "a"), (1, "b")])
        self.assertEqual(len(vm), 3)
        self.assertEqual(vm.values(), ["a", "b", "c"])
        # Check via `get()`.
        self.assertEqual(vm.get(0), "a")
        self.assertEqual(vm.get(1), "b")
        self.assertEqual(vm.get(2), "c")

    def test_from_indexed_duplicate_last_write_wins(self) -> None:
        shape = Shape(["n"], Slice.new_row_major([3]))
        # Rank 1 written twice; last write should win.
        vm = ValueMesh.from_indexed(shape, [(0, 7), (1, 8), (1, 88), (2, 9)])
        self.assertEqual(vm.values(), [7, 88, 9])

    def test_from_indexed_missing_rank_is_error(self) -> None:
        shape = Shape(["n"], Slice.new_row_major([3]))
        # Missing rank 2
        with self.assertRaisesRegex(ValueError, r"expected\s+3.*contains\s+2"):
            ValueMesh.from_indexed(shape, [(0, 10), (1, 20)])
