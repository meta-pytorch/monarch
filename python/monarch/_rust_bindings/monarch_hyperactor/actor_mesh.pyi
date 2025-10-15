# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final, Optional, Protocol

from monarch._rust_bindings.monarch_hyperactor.actor import PythonMessage
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Region
from typing_extensions import Self

class ActorMeshProtocol(Protocol):
    """
    Protocol defining the common interface for actor mesh and mesh ref.
    """

    def cast(
        self,
        message: PythonMessage,
        selection: str,
        instance: Instance,
    ) -> None: ...
    def new_with_region(self, region: Region) -> Self: ...
    def supervision_event(
        self, instance: Instance
    ) -> "Optional[Shared[Exception]]": ...
    def stop(self, instance: Instance) -> PythonTask[None]: ...
    def initialized(self) -> PythonTask[None]: ...

@final
class PythonActorMesh(ActorMeshProtocol):
    pass

@final
class ActorSupervisionEvent:
    @property
    def actor_id(self) -> ActorId:
        """
        The actor id of the actor.
        """
        ...

    @property
    def actor_status(self) -> str:
        """
        Detailed actor status.
        """
        ...
