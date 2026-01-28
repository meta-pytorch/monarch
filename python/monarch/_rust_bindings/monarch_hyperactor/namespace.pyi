# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum
from typing import final, Union

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.host_mesh import HostMesh
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

@final
class MeshKind(Enum):
    """The kind of mesh (host, proc, or actor)."""

    Host: "MeshKind"
    Proc: "MeshKind"
    Actor: "MeshKind"

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

@final
class Namespace:
    """
    A read-only namespace for looking up meshes.

    This class only exposes read operations (get, contains) and does not
    allow registration or unregistration of meshes.
    """

    @property
    def name(self) -> str:
        """Get the namespace name."""
        ...

    def contains(self, kind: MeshKind, name: str) -> PythonTask[bool]:
        """
        Check if a mesh exists in the namespace.

        Args:
            kind: The mesh kind (MeshKind.Host, MeshKind.Proc, or MeshKind.Actor)
            name: The mesh name

        Returns:
            A PythonTask that resolves to True if the mesh exists, False otherwise
        """
        ...

    def get(
        self, kind: MeshKind, name: str
    ) -> PythonTask[Union[HostMesh, ProcMesh, PythonActorMesh]]:
        """
        Get a mesh from the namespace.

        Args:
            kind: The mesh kind (MeshKind.Host, MeshKind.Proc, or MeshKind.Actor)
            name: The mesh name

        Returns:
            A PythonTask that resolves to HostMesh, ProcMesh, or PythonActorMesh
            depending on kind

        Raises:
            KeyError: If the mesh is not found
        """
        ...

    def __repr__(self) -> str: ...

def create_in_memory_namespace(name: str) -> Namespace:
    """
    Create an in-memory namespace for testing.

    Args:
        name: The namespace name (e.g., "my.namespace")

    Returns:
        A Namespace instance backed by in-memory storage
    """
    ...
