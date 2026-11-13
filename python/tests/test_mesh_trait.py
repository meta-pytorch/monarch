# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Iterable

import pytest
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
    # pyrefly: ignore [bad-override]
    def _labels(self) -> Iterable[str]:
        return self._shape.labels


def test_len() -> None:
    s = Slice(offset=0, sizes=[2, 3], strides=[3, 1])
    shape = Shape(["label0", "label1"], s)

    mesh = Mesh(shape, [1, 2, 3, 4, 5, 6])
    assert 6 == len(mesh)


def test_flatten_contiguous_with_size_one_dim() -> None:
    # A 2x4 mesh sliced to replicas=1:2, gpus=0:2 keeps the stride of the
    # size-1 "replicas" dimension, but its ranks (4, 5) are contiguous.
    s = Slice(offset=0, sizes=[2, 4], strides=[4, 1])
    mesh = Mesh(Shape(["replicas", "gpus"], s), list(range(8)))
    sub = mesh.slice(replicas=slice(1, 2), gpus=slice(0, 2))

    flat = sub.flatten("rank")
    assert list(flat._ndslice) == [4, 5]
    assert flat._ndslice.strides == [1]


def test_flatten_sparse_mesh_still_rejected() -> None:
    # gpus=0:1 leaves ranks 0 and 4, which are not contiguous.
    s = Slice(offset=0, sizes=[2, 4], strides=[4, 1])
    mesh = Mesh(Shape(["replicas", "gpus"], s), list(range(8)))
    sub = mesh.slice(gpus=slice(0, 1))

    with pytest.raises(ValueError, match="cannot flatten sparse mesh"):
        sub.flatten("rank")
