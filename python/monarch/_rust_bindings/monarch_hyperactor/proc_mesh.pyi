# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, AsyncIterator, final, Literal, overload, Type, TYPE_CHECKING

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
    def allocate_nonblocking(self, alloc: Alloc) -> PythonTask["ProcMesh"]:
        """
        Allocate a process mesh according to the provided alloc.
        Returns when the mesh is fully allocated.

        Arguments:
        - `alloc`: The alloc to allocate according to.
        """
        ...

    def spawn_nonblocking(
        self,
        name: str,
        actor: Any,
    ) -> PythonTask[PythonActorMesh]:
        """
        Spawn a new actor on this mesh.

        Arguments:
        - `name`: Name of the actor.
        - `actor`: The type of the actor that will be spawned.
        """
        ...

    @staticmethod
    def spawn_async(
        proc_mesh: Shared["ProcMesh"], name: str, actor: Type["Actor"]
    ) -> PythonActorMesh: ...
    async def monitor(self) -> ProcMeshMonitor:
        """
        Returns a supervision monitor for this mesh.
        """
        ...

    @property
    def client(self) -> Instance:
        """
        A client that can be used to communicate with individual
        actors in the mesh, and also to create ports that can be
        broadcast across the mesh)
        """
        ...

    @property
    def region(self) -> Region:
        """
        The region of the mesh.
        """
        ...

    def stop_nonblocking(self) -> PythonTask[None]:
        """
        Stop the proc mesh.
        """
        ...

    def __repr__(self) -> str: ...

@final
class ProcMeshMonitor:
    def __aiter__(self) -> AsyncIterator["ProcEvent"]:
        """
        Returns an async iterator for this monitor.
        """
        ...

    async def __anext__(self) -> "ProcEvent":
        """
        Returns the next proc event in the proc mesh.
        """
        ...

@final
class ProcEvent:
    @final
    class Stopped:
        """
        A Stopped event.
        """

        ...

    @final
    class Crashed:
        """
        A Crashed event.
        """

        ...
