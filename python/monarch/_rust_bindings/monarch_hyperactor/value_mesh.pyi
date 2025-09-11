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
    """Mesh holding values per rank."""

    def __init__(self, shape: Shape, values: list[Any]) -> None: ...
    def __len__(self) -> int: ...
    def values(self) -> list[Any]: ...
    def get(self, rank: int) -> Any: ...
    @staticmethod
    def from_indexed(shape: Shape, pairs: Iterable[Tuple[int, Any]]) -> "ValueMesh": ...
