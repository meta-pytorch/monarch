# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from datetime import timedelta
from typing import Dict, final, Optional

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from typing_extensions import Self

class Alloc:
    """
    An alloc represents an allocation of procs. Allocs are returned by
    one of the allocator implementations, such as `ProcessAllocator` or
    `LocalAllocator`.
    """
    def reshape(self, extent: Dict[str, int]) -> Alloc:
        """
        Reshape the alloc to a different shape before creating the proc mesh.
        The number of elements in the new alloc must be the same.
        """
        ...

@final
class AllocConstraints:
    def __init__(self, match_labels: Optional[dict[str, str]] = None) -> None:
        """
        Create a new alloc constraints.

        Arguments:
        - `match_labels`: A dictionary of labels to match. If a label is present
                in the dictionary, the alloc must have that label and its value
                must match the value in the dictionary.
        """
        ...

    @property
    def match_labels(self) -> dict[str, str]:
        """
        The labels to match.
        """
        ...

@final
class AllocSpec:
    def __init__(self, constraints: AllocConstraints, **kwargs: int) -> None:
        """
        Initialize a shape with the provided dimension-size pairs.
        For example, `AllocSpec(constraints, replica=2, host=3, gpu=8)` creates a
        shape with 2 replicas with 3 hosts each, each of which in turn
        has 8 GPUs.
        """
        ...

    @property
    def extent(self) -> Dict[str, int]:
        """
        Size of requested alloc.
        """
        ...

    @property
    def constraints(self) -> AllocConstraints:
        """
        The AllocConstraints used to create this AllocSpec.
        """
        ...

class AllocatorBase:
    def allocate_nonblocking(self, spec: AllocSpec) -> PythonTask[Alloc]:
        """
        Allocate a process according to the provided spec.

        Arguments:
        - `spec`: The spec to allocate according to.
        """
        ...

class ProcessAllocatorBase(AllocatorBase):
    def __init__(
        self,
        program: str,
        args: Optional[list[str]] = None,
        envs: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Create a new process allocator.

        Arguments:
        - `program`: The program for each process to run. Must be a hyperactor
                    bootstrapped program.
        - `args`: The arguments to pass to the program.
        - `envs`: The environment variables to set for the program.
        """
        ...

class LocalAllocatorBase(AllocatorBase):
    pass

class SimAllocatorBase(AllocatorBase):
    pass

class RemoteAllocatorBase(AllocatorBase):
    def __new__(
        cls,
        world_id: str,
        initializer: "monarch._src.actor.allocator.RemoteAllocInitializer",  # pyre-ignore[11]
        heartbeat_interval: timedelta = timedelta(seconds=5),
    ) -> Self:
        """
        Create a new (client-side) allocator instance that submits allocation requests to
        remote hosts that are running hyperactor's RemoteProcessAllocator.

        Arguments:
        - `world_id`: The world id to use for the remote allocator.
        - `initializer`: Returns the server addresses to send allocation requests to.
        - `heartbeat_interval`: Heartbeat interval used to maintain health status of remote hosts.
        """
        ...
