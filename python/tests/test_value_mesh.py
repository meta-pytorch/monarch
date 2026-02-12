# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import pickle

from monarch._rust_bindings.monarch_hyperactor.shape import Shape, Slice
from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh


class TestValueMeshConstruction:
    def test_basic_construction(self) -> None:
        shape = Shape(["rank"], Slice(offset=0, sizes=[4], strides=[1]))
        values = ["a", "b", "c", "d"]
        vm = ValueMesh(shape, values)
        assert len(vm) == 4

    def test_2d_construction(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)
        assert len(vm) == 6


class TestValueMeshAccess:
    def test_values(self) -> None:
        shape = Shape(["rank"], Slice(offset=0, sizes=[4], strides=[1]))
        values = ["a", "b", "c", "d"]
        vm = ValueMesh(shape, values)
        assert list(vm.values()) == ["a", "b", "c", "d"]

    def test_item_with_coords(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = ["00", "01", "02", "10", "11", "12"]
        vm = ValueMesh(shape, values)
        assert vm.item(x=0, y=0) == "00"
        assert vm.item(x=0, y=2) == "02"
        assert vm.item(x=1, y=1) == "11"

    def test_item_no_args_singleton(self) -> None:
        # After slicing to a single element, item() with no args should work
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = ["00", "01", "02", "10", "11", "12"]
        vm = ValueMesh(shape, values)
        # Slice to single element
        singleton = vm.slice(x=1, y=2)
        assert singleton.item() == "12"


class TestValueMeshIteration:
    def test_items(self) -> None:
        shape = Shape(["rank"], Slice(offset=0, sizes=[3], strides=[1]))
        values = ["a", "b", "c"]
        vm = ValueMesh(shape, values)
        items = list(vm.items())
        assert len(items) == 3
        # Each item is (Point, value)
        assert items[0][1] == "a"
        assert items[1][1] == "b"
        assert items[2][1] == "c"

    def test_iter(self) -> None:
        shape = Shape(["rank"], Slice(offset=0, sizes=[3], strides=[1]))
        values = ["a", "b", "c"]
        vm = ValueMesh(shape, values)
        # __iter__ returns items() which is a list - iterate with for loop
        items = []
        for item in vm:
            items.append(item)
        assert len(items) == 3
        assert items[0][1] == "a"


class TestValueMeshPickle:
    def test_pickle_roundtrip(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 2], strides=[2, 1]))
        values = ["a", "b", "c", "d"]
        vm = ValueMesh(shape, values)

        pickled = pickle.dumps(vm)
        vm2 = pickle.loads(pickled)

        assert len(vm2) == 4
        assert list(vm2.values()) == ["a", "b", "c", "d"]
        assert vm2.item(x=0, y=0) == "a"
        assert vm2.item(x=1, y=1) == "d"


class TestValueMeshRepr:
    def test_repr(self) -> None:
        shape = Shape(
            ["x", "y", "z"], Slice(offset=0, sizes=[2, 2, 2], strides=[4, 2, 1])
        )
        values = ["000", "001", "010", "011", "100", "101", "110", "111"]
        vm = ValueMesh(shape, values)
        r = repr(vm)
        expected = """\
ValueMesh({x: 2, y: 2, z: 2}):
(
  ({'x': 0/2, 'y': 0/2, 'z': 0/2}, '000'),
  ({'x': 0/2, 'y': 0/2, 'z': 1/2}, '001'),
  ({'x': 0/2, 'y': 1/2, 'z': 0/2}, '010'),
  ({'x': 0/2, 'y': 1/2, 'z': 1/2}, '011'),
  ({'x': 1/2, 'y': 0/2, 'z': 0/2}, '100'),
  ({'x': 1/2, 'y': 0/2, 'z': 1/2}, '101'),
  ({'x': 1/2, 'y': 1/2, 'z': 0/2}, '110'),
  ({'x': 1/2, 'y': 1/2, 'z': 1/2}, '111'),
)"""
        assert r == expected


class TestValueMeshMeshTrait:
    def test_shape_property(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)
        assert list(vm.shape.labels) == ["x", "y"]

    def test_extent_property(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)
        assert vm.extent["x"] == 2
        assert vm.extent["y"] == 3

    def test_sizes(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)
        assert vm.sizes == {"x": 2, "y": 3}

    def test_size(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)
        assert vm.size() == 6
        assert vm.size("x") == 2
        assert vm.size("y") == 3

    def test_slice(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = ["00", "01", "02", "10", "11", "12"]
        vm = ValueMesh(shape, values)

        # Slice x=1 -> keeps all y values for x=1
        sliced = vm.slice(x=1)
        assert len(sliced) == 3
        assert list(sliced.values()) == ["10", "11", "12"]

    def test_flatten(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)

        flat = vm.flatten("rank")
        assert len(flat) == 6
        assert list(flat.shape.labels) == ["rank"]
        assert list(flat.values()) == list(range(6))

    def test_rename(self) -> None:
        shape = Shape(["x", "y"], Slice(offset=0, sizes=[2, 3], strides=[3, 1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)

        renamed = vm.rename(x="rows", y="cols")
        assert list(renamed.shape.labels) == ["rows", "cols"]
        assert list(renamed.values()) == list(range(6))

    def test_split(self) -> None:
        # Create a 1D mesh with 6 elements
        shape = Shape(["rank"], Slice(offset=0, sizes=[6], strides=[1]))
        values = list(range(6))
        vm = ValueMesh(shape, values)

        # Split rank=6 into x=2, y=3
        split = vm.split(rank=("x", "y"), y=3)
        assert list(split.shape.labels) == ["x", "y"]
        assert split.sizes == {"x": 2, "y": 3}
        assert list(split.values()) == list(range(6))
