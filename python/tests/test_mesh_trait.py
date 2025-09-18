# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Iterable

from monarch._src.actor.shape import MeshTrait, NDSlice, Shape, Slice


class Mesh(MeshTrait):
    """
    A simple implementor of MeshTrait.
    """

    def __init__(self, shape: Shape, values: list[int]) -> None:
        self._shape = shape
        self._values = values

    def _new_with_shape(self, shape: Shape) -> "Mesh":
        return Mesh(shape, self._values)

    @property
    def _ndslice(self) -> NDSlice:
        return self._shape.ndslice

    @property
    def _labels(self) -> Iterable[str]:
        return self._shape.labels


def test_len() -> None:
    s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
    shape = Shape(["label0", "label1"], s)

    mesh = Mesh(shape, [1, 2, 3, 4, 5, 6])
    assert 6 == len(mesh)
