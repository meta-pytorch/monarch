# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, AsyncIterator, final, NoReturn, Optional, Protocol, Tuple

from monarch._rust_bindings.monarch_hyperactor.actor import PythonMessage
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    Mailbox,
    OncePortReceiver,
    PortReceiver,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.selection import Selection
from monarch._rust_bindings.monarch_hyperactor.shape import Shape
from typing_extensions import Self

class ActorMeshProtocol(Protocol):
    """
    Protocol defining the common interface for actor mesh, mesh ref and _ActorMeshRefImpl.
    """

    def cast(
        self,
        message: PythonMessage,
        selection: str,
        mailbox: Mailbox,
    ) -> None: ...
    def new_with_shape(self, shape: Shape) -> Self: ...
    def supervision_event(self) -> "Optional[Shared[Exception]]": ...
    def stop(self) -> PythonTask[None]: ...
    def initialized(self) -> PythonTask[None]: ...

@final
class PythonActorMesh(ActorMeshProtocol):
    """
    A Python actor mesh that forwards to the rust trait implementation.
    """

    def get(self, rank: int) -> "ActorId | None":
        """
        Get the actor id for the actor at the given rank.
        """
        ...

    @property
    def client(self) -> Mailbox:
        """
        The mailbox client for this actor mesh.
        """
        ...

    @property
    def stopped(self) -> bool:
        """
        If the mesh has been stopped.
        """
        ...

    def __reduce__(self) -> Tuple[Any, Any]:
        """
        Support for pickle serialization.
        """
        ...

    @staticmethod
    def from_bytes(bytes: bytes) -> "PythonActorMesh":
        """
        Deserialize a PythonActorMesh from bytes.
        """
        ...

class PythonActorMeshImpl:
    """
    Implementation of a Python actor mesh with supervision monitoring.
    """

    def get_supervision_event(self) -> "ActorSupervisionEvent | None":
        """
        Returns supervision event if there is any.
        """
        ...

    def get(self, rank: int) -> "ActorId | None":
        """
        Get the actor id for the actor at the given rank.
        """
        ...

    def stop(self) -> PythonTask[None]:
        """
        Stop all actors that are part of this mesh.
        Using this mesh after stop() is called will raise an Exception.
        """
        ...

    def supervision_event(self) -> "Optional[Shared[Exception]]":
        """
        Get supervision events for this actor mesh.
        """
        ...

    @property
    def stopped(self) -> bool:
        """
        If the mesh has been stopped.
        """
        ...

@final
class ActorSupervisionEvent:
    """
    Event representing an actor supervision failure.
    """

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

    def __repr__(self) -> str:
        """
        String representation of the supervision event.
        """
        ...
