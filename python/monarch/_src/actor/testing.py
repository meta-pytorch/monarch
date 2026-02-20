# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Test-only @rust_struct class with mixin inheritance.

Mirrors the real pattern (e.g. ValueMesh + MeshTrait) using a minimal
Rust PyO3 TestStruct to verify mixin patching behavior.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from monarch._src.actor.python_extension_methods import rust_struct


class TraitMixin(ABC):
    """ABC mixin with abstract + concrete methods, like MeshTrait."""

    @abstractmethod
    def abstract_method(self) -> str: ...

    def concrete_from_trait(self) -> str:
        return "from_trait"

    def overlap(self) -> str:
        return "from_trait"

    @property
    def trait_prop(self) -> int:
        return 67


class PlainMixin(ABC):
    """Non-abstract ABC mixin — isinstance() works via register()."""

    @abstractmethod
    def abstract_for_lint(self) -> None:
        """Abstract method to satisfy B024 lint (ABC must have abstract methods)."""
        ...

    def plain_method(self) -> str:
        return "from_plain"

    def overlap(self) -> str:
        return "from_plain"


@rust_struct("monarch_hyperactor::testing::TestStruct")
class TestStruct(TraitMixin, PlainMixin):
    """Rust struct with mixin inheritance, exactly like real usage."""

    def __init__(self, value: int) -> None: ...

    # Stubs for Rust-implemented methods (type-checker only):
    # fn rust_method(&self) -> i64 { self.value }
    def rust_method(self) -> int: ...
    # fn shared_method(&self) -> String { "from_rust".to_string() }
    def shared_method(self) -> str: ...

    # Concrete implementation of TraitMixin's abstract method:
    def abstract_method(self) -> str:
        return f"implemented_{self.rust_method()}"

    # Python-only method that calls into Rust:
    def python_only(self) -> str:
        return f"python_{self.rust_method()}"

    # Implement PlainMixin's abstract method (required for ABC):
    def abstract_for_lint(self) -> None:
        pass
