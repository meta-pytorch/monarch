# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import (
    Any,
    final,
    Generic,
    Iterator,
    List,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Point, Shape, Slice

R = TypeVar("R")

@final
class ValueMesh(Generic[R]):
    """
    A mesh that holds the result of an endpoint invocation.

    ValueMesh is returned when calling `.get()` on a Future from an endpoint
    invocation on an ActorMesh or ProcMesh, or by awaiting the Future directly.
    It contains the return values from all actors in the mesh, organized by
    their coordinates.

    Iteration:
        The most efficient way to iterate over a ValueMesh is using `.items()`,
        which yields (point, value) tuples:

        >>> for point, result in value_mesh.items():
        ...     rank = point["hosts"] * gpus_per_host + point["gpus"]
        ...     print(f"Rank {rank}: {result}")

        You can also iterate over just values:

        >>> for result in value_mesh.values():
        ...     process(result)

    Accessing specific values:
        Use `.item()` to extract a single value from a singleton mesh:

        >>> single_value = value_mesh.slice(hosts=0, gpus=0).item()

        Or with keyword arguments for multi-dimensional access:

        >>> value = value_mesh.item(hosts=0, gpus=0)

    Mesh operations:
        ValueMesh supports the same operations as other MeshTrait types:

        - `.slice(**coords)`: Select a subset of the mesh
        - `.flatten(name)`: Flatten all dimensions into one
        - `.split(**kwargs)`: Split dimensions into sub-dimensions
        - `.rename(**kwargs)`: Rename dimensions
        - `.size(dim)`: Get the size of dimension(s)
        - `.sizes`: Dictionary of dimension label to size

    Examples:
        >>> # Sync API - Get results from all actors
        >>> results = actor_mesh.endpoint.call(arg).get()
        >>> for point, result in results.items():
        ...     print(f"Actor at {point}: {result}")
        >>>
        >>> # Async API - Await the future directly
        >>> results = await actor_mesh.endpoint.call(arg)
        >>> for point, result in results.items():
        ...     print(f"Actor at {point}: {result}")
        >>>
        >>> # Access a specific actor's result
        >>> result_0 = results.item(hosts=0, gpus=0)
        >>>
        >>> # Flatten and iterate
        >>> for point, result in results.flatten("rank").items():
        ...     print(f"Rank {point.rank}: {result}")
    """

    def __init__(self, shape: Shape, values: List[R]) -> None: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[Tuple[Point, R]]: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: Any) -> bool: ...
    def values(self) -> List[R]: ...
    def get(self, rank: int) -> R: ...
    def item(self, **kwargs: int) -> R: ...
    def items(self) -> List[Tuple[Point, R]]: ...

    # Shape accessors (required by MeshTraitProtocol)
    @property
    def shape(self) -> Shape: ...
    @property
    def _ndslice(self) -> Slice: ...
    @property
    def _labels(self) -> Tuple[str, ...]: ...
    @property
    def extent(self) -> Extent: ...
    @property
    def sizes(self) -> dict[str, int]: ...

    # MeshTrait operations (required by MeshTraitProtocol)
    def _new_with_shape(self, shape: Shape) -> "ValueMesh[R]": ...
    def slice(self, **kwargs: Any) -> "ValueMesh[R]": ...
    def split(self, **kwargs: Any) -> "ValueMesh[R]": ...
    def flatten(self, name: str) -> "ValueMesh[R]": ...
    def rename(self, **kwargs: Any) -> "ValueMesh[R]": ...
    def size(self, dim: Union[None, str, Sequence[str]] = None) -> int: ...
    @staticmethod
    def from_indexed(shape: Shape, pairs: List[Tuple[int, R]]) -> "ValueMesh[R]": ...

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.mesh_trait import MeshTraitProtocol
    def _assert_implements_protocol(x: MeshTraitProtocol) -> None: ...
    def _check_valuemesh_satisfies_protocol(v: ValueMesh[Any]) -> None:
        _assert_implements_protocol(v)
