# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import AsyncIterator, final, Type

from monarch._rust_bindings.hyperactor_extension.alloc import Alloc
from monarch._rust_bindings.monarch_hyperactor.actor import Actor
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.mailbox import Mailbox
from monarch._rust_bindings.monarch_hyperactor.shape import Shape

@final
class ProcMesh:
    @classmethod
    async def allocate_nonblocking(self, alloc: Alloc) -> ProcMesh:
        """
        Allocate a process mesh according to the provided alloc.
        Returns when the mesh is fully allocated.

        Arguments:
        - `alloc`: The alloc to allocate according to.
        """
        ...

    @classmethod
    def allocate_blocking(self, alloc: Alloc) -> ProcMesh:
        """
        Allocate a process mesh according to the provided alloc.
        Blocks until the mesh is fully allocated.

        Arguments:
        - `alloc`: The alloc to allocate according to.
        """
        ...

    async def spawn_nonblocking(self, name: str, actor: Type[Actor]) -> PythonActorMesh:
        """
        Spawn a new actor on this mesh.

        Arguments:
        - `name`: Name of the actor.
        - `actor`: The type of the actor that will be spawned.
        """
        ...

    async def spawn_blocking(self, name: str, actor: Type[Actor]) -> PythonActorMesh:
        """
        Spawn a new actor on this mesh. Blocks until the actor is fully spawned.

        Arguments:
        - `name`: Name of the actor.
        - `actor`: The type of the actor that will be spawned.
        """
        ...

    async def monitor(self) -> ProcMeshMonitor:
        """
        Returns a supervision monitor for this mesh.
        """
        ...

    @property
    def client(self) -> Mailbox:
        """
        A client that can be used to communicate with individual
        actors in the mesh, and also to create ports that can be
        broadcast across the mesh)
        """
        ...

    @property
    def shape(self) -> Shape:
        """
        The shape of the mesh.
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
