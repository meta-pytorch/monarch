# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, final

from monarch._rust_bindings.monarch_hyperactor.alloc import Alloc
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Region

@final
class HostMesh:
    @classmethod
    def allocate_nonblocking(
        self,
        instance: Instance,
        alloc: Alloc,
        name: str,
        bootstrap_command: BootstrapCommand | None,
    ) -> PythonTask["HostMesh"]:
        """
        Allocate a host mesh according to the provided alloc.

        Arguments:
        - `instance`: The actor instance used to allocate the mesh.
        - `alloc`: The alloc to allocate according to.
        - `name`: Name of the mesh.
        - `bootstrap_command`: Override the bootstrap command used to bootstrap procs.
        """
        ...

    def spawn_nonblocking(
        self,
        instance: Instance,
        name: str,
        per_host: Extent,
    ) -> PythonTask[ProcMesh]:
        """
        Spawn a new actor on this mesh.

        Arguments:
        - `instance`: The instance to use to spawn the mesh.
        - `name`: Name of the proc mesh
        - `per_host`: Extent describing the shape of the proc mesh on each host.
        """
        ...

    def sliced(self, region: Region) -> "HostMesh":
        """
        Slice this mesh into a new mesh with the given region.

        Arguments:
        - `region`: The region to slice the mesh into.
        """
        ...

    @property
    def region(self) -> Region:
        """
        The region of the mesh.
        """
        ...

    def __reduce__(self) -> Any: ...
    def __eq__(self, other: "HostMesh") -> bool: ...
    def shutdown(self, instance: Instance) -> PythonTask[None]:
        """
        Shutdown the hosts in this mesh. This will throw an exception if this object
        is backed by a reference to a mesh rather than an owned mesh.

        Arguments:
        - `instance`: The instance to use to shutdown the mesh.
        """
        ...

@final
class BootstrapCommand:
    def __init__(
        self,
        program: str,
        arg0: str | None,
        args: list[str],
        env: dict[str, str],
    ) -> None:
        """
        Bootstrap command specification.

        Arguments:
        - `program`: The program to execute.
        - `arg0`: Optionally, the program's arg0. If not provided, the program's name will be used.
        - `args`: List of command line arguments.
        - `env`: Environment variables as key-value pairs.
        """
        ...

    def __repr__(self) -> str: ...

def bootstrap_host(
    bootstrap_cmd: BootstrapCommand | None,
) -> PythonTask[tuple[HostMesh, ProcMesh, Instance]]:
    """
    Bootstrap a host mesh in this process, returning the host mesh,
    proc mesh, and client instance.

    Arguments:
    - `bootstrap_cmd`: The bootstrap command to use to bootstrap the host.
    """
    ...

def shutdown_local_host_mesh() -> PythonTask[None]:
    """
    Shutdown the local host mesh created by bootstrap_host().

    Sends ShutdownHost message to the local host mesh agent with:
    - timeout: 10 seconds grace period before SIGTERM escalation
    - max_in_flight: 16 concurrent child terminations

    Raises:
        RuntimeError: If no local host mesh exists (bootstrap_host not called)
    """
    ...
