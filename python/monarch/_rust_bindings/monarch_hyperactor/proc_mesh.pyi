# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from typing import Any, final, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import (
        Actor,
        ActorKind,
        PythonMessage,
    )
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.handle import Handle
from monarch._rust_bindings.monarch_hyperactor.pickle import PendingMessage
from monarch._rust_bindings.monarch_hyperactor.shape import Region

@final
class ProcMesh:
    @staticmethod
    def sliced_handle(proc_mesh: Handle[ProcMesh], region: Region) -> Handle[ProcMesh]:
        """Return a Handle for a non-owning view of the procs selected by region.

        Wait for the input proc mesh if it is still pending, then select those
        procs. This does not spawn new procs. Creation or slicing errors are
        raised when the returned Handle is observed.
        """
        ...

    @staticmethod
    def spawn_async(
        proc_mesh: Handle["ProcMesh"],
        instance: Instance,
        mesh_base_name: str,
        actor: Type["Actor"],
        init_message: PendingMessage,
        kind: ActorKind,
        emulated: bool,
        supervision_display_name: str | None = None,
    ) -> PythonActorMesh:
        """Spawn an actor mesh on the procs supplied by the input Handle.

        Normally return a PythonActorMesh immediately, while creation is still
        pending. Native code waits for the proc mesh and resolves pending
        references in the initialization message before spawning the actors.

        The actor type is serialized and the initialization message is consumed
        during this call. With emulated=True, wait for mesh creation before
        returning instead.
        """
        ...

    @property
    def region(self) -> Region:
        """
        The region of the mesh.
        """
        ...

    def stop_nonblocking(self, instance: Instance, reason: str) -> Handle[None]:
        """
        Start stopping the proc mesh and return an observer for completion.
        """
        ...

    def __repr__(self) -> str: ...
    def sliced(self, region: Region) -> "ProcMesh":
        """
        Returns a new mesh that is a slice of this mesh with the given region.
        """
        ...
