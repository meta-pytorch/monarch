# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import itertools
import operator
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, Sequence, Tuple, Union

from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape, Slice
from typing_extensions import Self

NDSlice = Slice

Slices = Slice | list[Slice]


def iter_ranks(ranks: Slices) -> Generator[int, None, None]:
    if isinstance(ranks, list):
        seen = set()
        for slice_ in ranks:
            for rank in slice_:
                if rank not in seen:
                    seen.add(rank)
                    yield rank
    else:
        yield from ranks


class ShapeExt:
    """Extension methods for Shape that add higher-level
    functionality."""

    @staticmethod
    def slice(shape: Shape, **kwargs: Any) -> Shape:
        """Select along named dimensions. Integer values remove
        dimensions, slice objects keep dimensions but restrict them.

        Examples: ShapeExt.slice(shape, batch=3, gpu=slice(2, 6))
        """
        for label, selector in kwargs.items():
            if label not in shape.labels:
                raise TypeError(f"Shape does not have dimension labeled {label!r}")
            if isinstance(selector, slice):
                shape = shape.select(label, selector)
            else:
                if (
                    selector < 0
                    or selector >= shape.ndslice.sizes[shape.labels.index(label)]
                ):
                    raise IndexError("index out of range")
                shape = shape.at(label, selector)
        return shape


class MeshTrait(ABC):
    """
    Mesh interface. Implemented via Shape.
    """

    @property
    @abstractmethod
    def _ndslice(self) -> NDSlice: ...

    @property
    @abstractmethod
    def _labels(self) -> Tuple[str, ...]: ...

    # mesh trait guarentees that its own calls to _new_with_shape
    # will only ever select a shape that is a subspace of the
    # current _ndslice.
    @abstractmethod
    def _new_with_shape(self, shape: Shape) -> Self: ...

    def slice(self, **kwargs: Any) -> Self:
        """Select along named dimensions. Integer values remove
        dimensions, slice objects keep dimensions but restrict them.

        Examples: mesh.slice(batch=3, gpu=slice(2, 6))
        """
        shape = Shape(list(self._labels), self._ndslice)
        return self._new_with_shape(ShapeExt.slice(shape, **kwargs))

    def split(self, **kwargs: Any) -> Self:
        """
        Returns a new device mesh with some dimensions of this mesh split.
        For instance, this call splits the host dimension into dp and pp dimensions,
        The size of 'pp' is specified and the dimension size is derived from it:

            new_mesh = mesh.split(host=('dp', 'pp'), gpu=('tp','cp'), pp=16, cp=2)

        Dimensions not specified will remain unchanged.
        """
        splits: Dict[str, Sequence[str]] = {}
        size_constraints: Dict[str, int] = {}
        for key, value in kwargs.items():
            if key in self._labels:
                if isinstance(value, str):
                    raise ValueError(
                        f"expected a sequence of dimensions, but got '{value}'"
                    )
                splits[key] = value
            else:
                if not isinstance(value, int):
                    raise ValueError(
                        f"'{key}' is not an existing dim. Expected an integer size constraint on a new dim."
                    )
                size_constraints[key] = value

        names = []
        sizes = []
        strides = []
        ndslice = self._ndslice
        for name, size, stride in zip(self._labels, ndslice.sizes, ndslice.strides):
            to_names = splits.get(name, (name,))
            total_size = 1
            unknown_size_name = None
            for to_name in to_names:
                if to_name in size_constraints:
                    total_size *= size_constraints[to_name]
                elif unknown_size_name is None:
                    unknown_size_name = to_name
                else:
                    raise ValueError(
                        f"Cannot infer size of {to_names} because both {to_name} and {unknown_size_name} have unknown size. Specify at least one as argument, e.g. {to_name}=4"
                    )
            if unknown_size_name is not None:
                inferred_size, m = divmod(size, total_size)
                if m != 0:
                    to_sizes = tuple(
                        (
                            size_constraints[to_name]
                            if to_name in size_constraints
                            else "?"
                        )
                        for to_name in to_names
                    )
                    raise ValueError(
                        f"Dimension '{name}' of size {size} is not evenly divided by {to_names!r} with sizes {to_sizes!r}"
                    )
                size_constraints[unknown_size_name] = inferred_size
            elif total_size != size:
                to_sizes = tuple(size_constraints[to_name] for to_name in to_names)
                raise ValueError(
                    f"Dimension '{name}' of size {size} is not evenly divided by {to_names!r} with sizes {to_sizes!r}"
                )
            new_sizes = [size_constraints.pop(to_name) for to_name in to_names]
            new_strides_reversed = tuple(
                itertools.accumulate(reversed(new_sizes), operator.mul, initial=stride)
            )
            sizes.extend(new_sizes)
            strides.extend(reversed(new_strides_reversed[:-1]))
            for name in to_names:
                if name in names:
                    raise ValueError(f"Duplicate dimension name '{name}'")
            names.extend(to_names)
        if size_constraints:
            raise ValueError(
                f"unused size constraints: {tuple(size_constraints.keys())}"
            )
        return self._new_with_shape(
            Shape(names, NDSlice(offset=ndslice.offset, sizes=sizes, strides=strides))
        )

    def flatten(self, name: str) -> Self:
        """
        Returns a new device mesh with all dimensions flattened into a single dimension
        with the given name.

        Currently this supports only dense meshes: that is, all ranks must be contiguous
        in the mesh.
        """
        ndslice = self._ndslice
        dense_strides = tuple(
            itertools.accumulate(reversed(ndslice.sizes), operator.mul, initial=1)
        )
        dense_strides, total_size = (
            list(reversed(dense_strides[:-1])),
            dense_strides[-1],
        )
        if dense_strides != ndslice.strides:
            raise ValueError(
                f"cannot flatten sparse mesh: {ndslice.strides=} != {dense_strides=}"
            )

        return self._new_with_shape(
            Shape(
                [name], NDSlice(offset=ndslice.offset, sizes=[total_size], strides=[1])
            )
        )

    def rename(self, **kwargs: Any) -> Self:
        """
        Returns a new device mesh with some of dimensions renamed.
        Dimensions not mentioned are retained:

            new_mesh = mesh.rename(host='dp', gpu='tp')
        """
        return self.split(**{k: (v,) for k, v in kwargs.items()})

    def size(self, dim: Union[None, str, Sequence[str]] = None) -> int:
        """
        Returns the number of elements (total) of the subset of mesh asked for.
        If dims is None, returns the total number of devices in the mesh.
        """

        if dim is None:
            dim = self._labels
        if isinstance(dim, str):
            if dim not in self._labels:
                raise KeyError(f"{self} does not have dimension {repr(dim)}")
            return self._ndslice.sizes[self._labels.index(dim)]
        else:
            p = 1
            for d in dim:
                p *= self.size(d)
            return p

    @property
    def sizes(self) -> dict[str, int]:
        return dict(zip(self._labels, self._ndslice.sizes))

    @property
    def extent(self) -> "Extent":
        return Extent(self._labels, self._ndslice.sizes)

    def __len__(self) -> int:
        return len(self._ndslice)


__all__ = ["NDSlice", "Shape", "MeshTrait"]
