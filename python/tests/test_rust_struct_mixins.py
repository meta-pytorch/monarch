# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Tests for the base-class mixin feature of @rust_struct.

Uses a dedicated Rust PyO3 TestStruct with ABC mixins to verify that
concrete methods from base classes are patched onto the Rust class,
abstract methods are skipped, and isinstance() checks work — just as
they do for real @rust_struct usage like ValueMesh(MeshTrait).
"""

from __future__ import annotations

import pytest
from monarch._rust_bindings.monarch_hyperactor.testing import _make_test_struct
from monarch._src.actor.python_extension_methods import PatchRustClass
from monarch._src.actor.testing import PlainMixin, TestStruct, TraitMixin


class TestShouldPatch:
    """Unit tests for PatchRustClass._should_patch."""

    def test_new_method_should_patch(self) -> None:
        class Target:
            pass

        patcher = PatchRustClass(Target)
        assert patcher._should_patch("foo", lambda self: None)

    def test_existing_method_should_not_patch(self) -> None:
        class Target:
            def foo(self) -> None:
                pass

        patcher = PatchRustClass(Target)
        assert not patcher._should_patch("foo", lambda self: None)

    def test_object_default_dunder_should_patch(self) -> None:
        class Target:
            pass

        patcher = PatchRustClass(Target)
        # __repr__ comes from object, so it should be overridable
        assert patcher._should_patch("__repr__", lambda self: "hi")

    def test_non_callable_should_not_patch(self) -> None:
        class Target:
            pass

        patcher = PatchRustClass(Target)
        assert not patcher._should_patch("x", 67)

    def test_property_should_patch(self) -> None:
        class Target:
            pass

        patcher = PatchRustClass(Target)
        assert patcher._should_patch("x", property(lambda self: 1))


class TestMixinPatching:
    """Tests for mixin patching using @rust_struct with a real Rust struct.

    TestStruct is a Rust #[pyclass] decorated with @rust_struct and
    inheriting from TraitMixin (ABC) and PlainMixin (ABC).
    _make_test_struct creates instances entirely from Rust.
    """

    def test_concrete_mixin_methods_are_patched(self) -> None:
        """Concrete methods from ABC mixin are available on Rust objects."""
        obj = _make_test_struct(67)
        assert obj.concrete_from_trait() == "from_trait"

    def test_abstract_mixin_methods_are_skipped(self) -> None:
        """Abstract methods from mixin are not patched as stubs."""
        obj = _make_test_struct(67)
        # abstract_method is abstract on TraitMixin but concrete in TestStruct body
        assert obj.abstract_method() == "implemented_67"

    def test_rust_methods_not_overridden_by_mixins(self) -> None:
        """Rust-implemented methods survive mixin patching."""
        obj = _make_test_struct(67)
        assert obj.rust_method() == 67
        assert obj.shared_method() == "from_rust"

    def test_python_only_method_patched(self) -> None:
        """Python-only methods from the class body are patched onto Rust class."""
        obj = _make_test_struct(10)
        assert obj.python_only() == "python_10"

    def test_multiple_mixins_applied_in_order(self) -> None:
        """Methods from multiple mixins are applied; first mixin wins on overlap."""
        obj = _make_test_struct(1)
        assert obj.plain_method() == "from_plain"  # from PlainMixin
        # overlap is on both TraitMixin and PlainMixin; TraitMixin listed first
        assert obj.overlap() == "from_trait"

    def test_mixin_property_is_patched(self) -> None:
        """Properties from mixin are patched onto the Rust class."""
        obj = _make_test_struct(1)
        assert obj.trait_prop == 67

    def test_abc_mixin_registers_isinstance(self) -> None:
        """ABC mixins are registered so isinstance() checks work."""
        obj = _make_test_struct(1)
        assert isinstance(obj, TraitMixin)
        assert isinstance(obj, PlainMixin)

    def test_non_abc_mixin_raises_type_error(self) -> None:
        """Non-ABC mixins are rejected with a clear error."""

        class NotABC:
            def method(self) -> str:
                return "plain"

        RustClass = type("C", (), {"__module__": "test.mod", "__qualname__": "C"})
        PythonClass = type(
            "C", (NotABC,), {"__module__": "test.mod", "__qualname__": "C"}
        )
        patcher = PatchRustClass(RustClass)
        with pytest.raises(TypeError, match="must inherit from ABC"):
            patcher(PythonClass)

    def test_rust_returned_is_same_type(self) -> None:
        """The Rust-returned object is the exact same type as TestStruct."""
        obj = _make_test_struct(1)
        assert type(obj) is TestStruct
