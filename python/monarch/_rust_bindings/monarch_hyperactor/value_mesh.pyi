# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, final, Iterable, Tuple

from monarch._rust_bindings.monarch_hyperactor.shape import Shape

@final
class ValueMesh:
    """Mesh holding values per rank.

    See also monarch._src.actor.actor_mesh.ValueMesh for the full
    @rust_struct definition with MeshTrait methods.
    """

    def __init__(self, shape: Shape, values: list[Any]) -> None: ...
    def __len__(self) -> int: ...
    def values(self) -> list[Any]: ...
    def get(self, rank: int) -> Any: ...
    @staticmethod
    def from_indexed(shape: Shape, pairs: Iterable[Tuple[int, Any]]) -> "ValueMesh": ...

def _make_test_value_mesh(
    labels: list[str], sizes: list[int], values: list[Any]
) -> Any: ...
