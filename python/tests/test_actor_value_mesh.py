# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

from typing import Any, cast, Iterator, List, Sequence, Tuple, TYPE_CHECKING

import monarch._src.actor.actor_mesh as actor_mesh
import pytest

if TYPE_CHECKING:
    # Type-only import to satisfy the constructor annotation; we won't
    # import the Rust type at runtime.
    from monarch._rust_bindings.monarch_hyperactor.shape import Shape as HyShape


# These tests target ValueMesh._new_with_shape. The goal is to verify the
# remapping logic:
#
#   - A Shape exposes .ranks(), which are **global ranks** (absolute,
#     offset/stride aware).
#   - ValueMesh stores values internally by **local index** (position
#     in the current shape's rank order).
#   - _new_with_shape builds a mapping {global_rank -> local_index}
#     and uses it to reorder values into the new shape's global rank
#     order.
#
# Invariant: each global rank maps to exactly one local index, and the
# remapping is just applying that mapping in the target rank order.
#
# HyValueMesh is patched with a fake so the Rust extension isn't
# exercised. This keeps the suite minimal and focused strictly on the
# remapping logic.


class FakeShape:
    """Minimal shape exposing just .ranks() for these tests."""

    _ranks: List[int]

    def __init__(self, ranks: Sequence[int]) -> None:
        self._ranks = list(ranks)

    def ranks(self) -> Iterator[int]:
        return iter(self._ranks)


class FakeHyValueMesh:
    """Simple backing store with .get(i) and .values() like the Rust binding."""

    _shape: Any
    _values: List[Any]

    def __init__(self, shape: Any, values: Sequence[Any]) -> None:
        self._shape = shape
        self._values = list(values)

    def get(self, i: int) -> Any:
        return self._values[i]

    def values(self) -> List[Any]:
        return list(self._values)


@pytest.fixture(autouse=True)
def patch_hyvaluemesh(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patch HyValueMesh → FakeHyValueMesh so ValueMesh()
    # constructs our simple fake during tests.
    monkeypatch.setattr(actor_mesh, "HyValueMesh", FakeHyValueMesh)


def make_vm(
    ranks: Sequence[int],
    values: Sequence[Any],
) -> Tuple[actor_mesh.ValueMesh[Any], FakeShape]:
    """Helper: build a ValueMesh with a fake shape and values."""
    shape = FakeShape(ranks)
    shape_typed = cast("HyShape", shape)
    vals: List[Any] = list(values)  # satisfy List[R] requirement
    vm: actor_mesh.ValueMesh[Any] = (
        actor_mesh.ValueMesh(  # pyre-ignore[6]: test double for Shape
            shape_typed, vals
        )
    )
    return vm, shape


def test_remap_identity() -> None:
    # When the target shape has the same global ranks, the values
    # should remain in the same order.
    vm, _ = make_vm([0, 1, 2, 3], ["a", "b", "c", "d"])
    target = FakeShape([0, 1, 2, 3])
    vm2 = vm._new_with_shape(cast("HyShape", target))  # pyre-ignore[6]
    assert vm2._hy.values() == ["a", "b", "c", "d"]


def test_remap_permutation() -> None:
    # A permutation of global ranks should permute the values
    # accordingly.
    vm, _ = make_vm([0, 1, 2, 3], ["a", "b", "c", "d"])
    target = FakeShape([2, 0, 3, 1])
    vm2 = vm._new_with_shape(cast("HyShape", target))  # pyre-ignore[6]
    assert vm2._hy.values() == ["c", "a", "d", "b"]


def test_remap_noncontiguous_global_ranks() -> None:
    # Non-contiguous ranks (simulating offset/stride) remap correctly.
    vm, _ = make_vm([10, 12, 14, 16], ["p", "q", "r", "s"])
    target = FakeShape([16, 10, 14, 12])
    vm2 = vm._new_with_shape(cast("HyShape", target))  # pyre-ignore[6]
    assert vm2._hy.values() == ["s", "p", "r", "q"]


def test_remap_missing_rank_raises() -> None:
    # If the target shape requests a rank not present in the current
    # shape, _new_with_shape should raise a KeyError.
    vm, _ = make_vm([4, 5, 7], ["x", "y", "z"])
    target = FakeShape([4, 6, 7])  # 6 is not present
    with pytest.raises(KeyError):
        _ = vm._new_with_shape(cast("HyShape", target))  # pyre-ignore[6]
