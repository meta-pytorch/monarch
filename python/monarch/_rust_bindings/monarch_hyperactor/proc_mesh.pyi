# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, final, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import Actor
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.alloc import Alloc
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Region

@final
class ProcMesh:
    @classmethod
    def allocate_nonblocking(
        self, instance: Instance, alloc: Alloc, name: str
    ) -> PythonTask["ProcMesh"]:
        """
        Allocate a process mesh according to the provided alloc.
        Returns when the mesh is fully allocated.

        Arguments:
        - `instance`: The actor instance used to allocate the mesh.
        - `alloc`: The alloc to allocate according to.
        - `name`: Name of the mesh.
        """
        ...

    def spawn_nonblocking(
        self,
        instance: Instance,
        name: str,
        actor: Any,
        supervision_display_name: str | None = None,
    ) -> PythonTask[PythonActorMesh]:
        """
        Spawn a new actor on this mesh.

        Arguments:
        - `instance`: The actor instance that will own the returned actor mesh.
        - `name`: Name of the actor.
        - `actor`: The type of the actor that will be spawned.
        - `supervision_display_name`: The name of the actor to display in supervision. If not None, this
            will be used instead of the fully qualified name of the actor.
        """
        ...

    @staticmethod
    def spawn_async(
        proc_mesh: Shared["ProcMesh"],
        instance: Instance,
        name: str,
        actor: Type["Actor"],
        emulated: bool,
        supervision_display_name: str | None = None,
    ) -> PythonActorMesh: ...
    @property
    def region(self) -> Region:
        """
        The region of the mesh.
        """
        ...

    def stop_nonblocking(self, instance: Instance, reason: str) -> PythonTask[None]:
        """
        Stop the proc mesh.
        """
        ...

    def __repr__(self) -> str: ...
    def sliced(self, region: Region) -> "ProcMesh":
        """
        Returns a new mesh that is a slice of this mesh with the given region.
        """
        ...
