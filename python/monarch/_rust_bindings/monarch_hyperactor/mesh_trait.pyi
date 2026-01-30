# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Protocol defining the MeshTrait interface.

This protocol is defined in _rust_bindings to be used as the common interface
for both Rust-backed (ValueMesh) and Python (MeshTrait) mesh implementations.
Use this Protocol in function signatures to accept either implementation.
"""

from typing import Any, Protocol, Sequence, Tuple, Union

from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape, Slice
from typing_extensions import Self

class MeshTraitProtocol(Protocol):
    """
    Protocol defining the mesh interface.

    Both ValueMesh (Rust) and MeshTrait (Python) satisfy this protocol.
    Use this in function signatures to accept any mesh implementation.

    Required properties/methods that implementations must provide:
    - `_ndslice`: The underlying n-dimensional slice
    - `_labels`: Tuple of dimension labels
    - `_new_with_shape`: Factory method to create new instances with a different shape
    """

    @property
    def _ndslice(self) -> Slice: ...
    @property
    def _labels(self) -> Tuple[str, ...]: ...
    def _new_with_shape(self, shape: Shape) -> Self: ...
    def slice(self, **kwargs: Any) -> Self:
        """Select along named dimensions. Integer values remove
        dimensions, slice objects keep dimensions but restrict them.

        Examples: mesh.slice(batch=3, gpu=slice(2, 6))
        """
        ...

    def split(self, **kwargs: Any) -> Self:
        """
        Returns a new mesh with some dimensions split.

        Example: mesh.split(host=('dp', 'pp'), pp=16)
        """
        ...

    def flatten(self, name: str) -> Self:
        """
        Returns a new mesh with all dimensions flattened into a single dimension
        with the given name.
        """
        ...

    def rename(self, **kwargs: Any) -> Self:
        """
        Returns a new mesh with some dimensions renamed.

        Example: mesh.rename(host='dp', gpu='tp')
        """
        ...

    def size(self, dim: Union[None, str, Sequence[str]] = None) -> int:
        """
        Returns the number of elements of the subset of mesh asked for.
        If dim is None, returns the total number of elements in the mesh.
        """
        ...

    @property
    def sizes(self) -> dict[str, int]:
        """Dictionary mapping dimension labels to their sizes."""
        ...

    @property
    def extent(self) -> Extent:
        """The extent of this mesh."""
        ...

    def __len__(self) -> int: ...
