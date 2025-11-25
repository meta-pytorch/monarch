# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Callable, final

@final
class Ref:
    """
    A reference to a value that exists on the worker and is used by other
    actors such as controller, client etc to reference the value.
    TODO: This is used for all types of values like tensors, streams, pipes etc.
    But should be split into separate types for each of them.

    Args:
    - `id`: The id of the value on the worker.
    """

    def __init__(self, id: int) -> None: ...
    @property
    def id(self) -> int:
        """The id of the value on the worker."""
        ...

    def __repr__(self) -> str: ...
    def __lt__(self, other: Ref) -> bool: ...
    def __le__(self, other: Ref) -> bool: ...
    def __eq__(self, value: Ref) -> bool: ...
    def __ne__(self, value: Ref) -> bool: ...
    def __gt__(self, other: Ref) -> bool: ...
    def __ge__(self, other: Ref) -> bool: ...
    def __hash__(self) -> int: ...
    def __getnewargs_ex__(self) -> tuple[tuple, dict]: ...

@final
class StreamRef:
    """
    A reference to a stream that exists on the worker and is used by other
    actors such as controller, client etc to reference it.

    Args:
    - `id`: The id of the stream on the worker.
    """

    def __init__(self, *, id: int) -> None: ...
    @property
    def id(self) -> int:
        """The id of the stream on the worker."""
        ...

    def __repr__(self) -> str: ...
    def __lt__(self, other: Ref) -> bool: ...
    def __le__(self, other: Ref) -> bool: ...
    def __eq__(self, value: Ref) -> bool: ...
    def __ne__(self, value: Ref) -> bool: ...
    def __gt__(self, other: Ref) -> bool: ...
    def __ge__(self, other: Ref) -> bool: ...
    def __hash__(self) -> int: ...

@final
class FunctionPath:
    """
    The fully qualified path to a function on the worker.

    Args:
    - `path`: The path to the function eg. `builtins.range`
    """

    def __init__(self, *, path: str) -> None: ...
    @property
    def path(self) -> str:
        """The path to the function."""
        ...

    def __repr__(self) -> str: ...
    def resolve(self) -> Callable[..., object]:
        """Resolve the function path to a callable."""
        ...

@final
class Cloudpickle:
    """
    A serialized function to run remotely.

    Args:
    - `func`: The function wrap
    """

    def __init__(self, *, bytes: bytes) -> None: ...
    def __repr__(self) -> str: ...
    def resolve(self) -> Callable[..., object]:
        """Resolve the live function to a callable."""
        ...

ResolvableFunction = FunctionPath | Cloudpickle
