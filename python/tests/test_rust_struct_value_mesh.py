# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Empirical proof that @rust_struct works for ValueMesh:

A Rust #[pyfunction] creates and returns a Rust ValueMesh object.
The @rust_struct decorator in actor_mesh.py patches Python MeshTrait methods
onto the Rust class at import time.
We verify those Python-only methods are callable on Rust-returned objects.

This proves that a Rust function returning ValueMesh will have all
Python extension methods available, with no Python wrapper layer needed.
"""

from __future__ import annotations

from monarch._rust_bindings.monarch_hyperactor.value_mesh import _make_test_value_mesh

# Import ValueMesh from actor_mesh — this triggers the @rust_struct patching
from monarch._src.actor.actor_mesh import ValueMesh
from monarch._src.actor.mpsc import Receiver


class TestRustStructValueMesh:
    """Prove that a Rust function returning ValueMesh has Python-patched methods."""

    def test_rust_returned_object_has_python_methods(self) -> None:
        """Core proof: Rust creates the object, Python methods are available."""
        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))

        # These methods exist ONLY because @rust_struct patched them on:
        assert vm.sizes == {"x": 2, "y": 3}
        assert vm.size() == 6
        assert vm.size("x") == 2

    def test_rust_returned_is_same_type(self) -> None:
        """The Rust-returned object is the exact same type as ValueMesh."""
        vm = _make_test_value_mesh(["rank"], [3], ["a", "b", "c"])
        assert type(vm) is ValueMesh
        assert type(vm).__name__ == "ValueMesh"

    def test_flatten_on_rust_object(self) -> None:
        """Python flatten() works on Rust-returned object."""
        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))
        flat = vm.flatten("rank")
        assert len(flat) == 6
        assert list(flat._shape.labels) == ["rank"]

    def test_rename_on_rust_object(self) -> None:
        """Python rename() works on Rust-returned object."""
        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))
        renamed = vm.rename(x="rows", y="cols")
        assert list(renamed._shape.labels) == ["rows", "cols"]

    def test_split_on_rust_object(self) -> None:
        """Python split() works on Rust-returned object."""
        vm = _make_test_value_mesh(["rank"], [6], list(range(6)))
        split = vm.split(rank=("x", "y"), y=3)
        assert list(split._shape.labels) == ["x", "y"]
        assert split.sizes == {"x": 2, "y": 3}

    def test_items_on_rust_object(self) -> None:
        """Python items() works on Rust-returned object."""
        vm = _make_test_value_mesh(["rank"], [3], ["a", "b", "c"])
        items = list(vm.items())
        assert len(items) == 3
        assert items[0][1] == "a"
        assert items[2][1] == "c"

    def test_iter_on_rust_object(self) -> None:
        """Python __iter__() works on Rust-returned object."""
        vm = _make_test_value_mesh(["rank"], [3], ["a", "b", "c"])
        collected = list(vm)
        assert len(collected) == 3

    def test_no_wrapper_layer(self) -> None:
        """The object is a direct Rust object, not wrapped in Python."""
        vm = _make_test_value_mesh(["rank"], [2], [1, 2])
        # Rust PyO3 objects reject arbitrary attribute assignment
        try:
            vm._some_random_attr = "fail"  # pyre-ignore[16]
            raise AssertionError("Should not set arbitrary attrs on Rust object")
        except AttributeError:
            pass  # Confirms Rust object, not Python wrapper

    def test_rust_methods_still_work(self) -> None:
        """Original Rust methods (values, __len__) still work after patching."""
        vm = _make_test_value_mesh(["rank"], [4], ["a", "b", "c", "d"])
        assert len(vm) == 4
        assert list(vm.values()) == ["a", "b", "c", "d"]

    def test_new_with_shape_identity(self) -> None:
        """_new_with_shape with the same shape preserves values."""
        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))
        vm2 = vm._new_with_shape(vm._shape)
        assert list(vm2.values()) == list(range(6))

    def test_new_with_shape_via_slice(self) -> None:
        """_new_with_shape remaps values correctly when slicing."""
        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))
        # Slice to x=1 -> should get values [3, 4, 5]
        sliced = vm.slice(x=1)
        assert list(sliced.values()) == [3, 4, 5]
        assert sliced.sizes == {"y": 3}

    def test_new_with_shape_via_rename_preserves_values(self) -> None:
        """Renaming doesn't change value order."""
        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))
        renamed = vm.rename(x="a", y="b")
        assert list(renamed.values()) == list(range(6))

    def test_item_access(self) -> None:
        """item() accesses values by named coordinates."""
        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))
        assert vm.item(x=0, y=0) == 0
        assert vm.item(x=1, y=2) == 5

    def test_pickle_roundtrip(self) -> None:
        """ValueMesh can be pickled and unpickled."""
        import pickle

        vm = _make_test_value_mesh(["x", "y"], [2, 3], list(range(6)))
        data = pickle.dumps(vm)
        vm2 = pickle.loads(data)
        assert type(vm2) is ValueMesh
        assert list(vm2.values()) == list(range(6))
        assert vm2.sizes == {"x": 2, "y": 3}

    def test_value_mesh_runtime_subscript(self) -> None:
        """ValueMesh[T] works at runtime (not just under TYPE_CHECKING)."""
        # This must not raise TypeError: type 'ValueMesh' is not subscriptable
        alias = ValueMesh[int]
        assert alias is ValueMesh

    def test_receiver_runtime_subscript(self) -> None:
        """Receiver[T] works at runtime (not just under TYPE_CHECKING)."""
        alias = Receiver[int]
        assert alias is Receiver
