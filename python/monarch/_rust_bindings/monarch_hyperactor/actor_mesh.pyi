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

    @property
    def region(self) -> Region:
        """Get the region of the mesh."""
        ...

    def get(self, rank: int) -> Optional[ActorId]:
        """Get the actor id at the given rank."""
        ...

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
    # Starts supervision monitoring for future uses of "supervision_event".
    def start_supervision(
        self, instance: Instance, supervision_display_name: str
    ) -> None: ...
    def stop(self, instance: Instance, reason: str) -> PythonTask[None]: ...
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

def hold_gil_for_test(delay_secs: float, hold_secs: float) -> None:
    """
    Test utility that holds the GIL for a specified duration.

    This spawns a background thread that waits for `delay_secs` before
    acquiring the Python GIL, then holds it for `hold_secs`.

    Args:
        delay_secs: Seconds to wait before acquiring the GIL
        hold_secs: Seconds to hold the GIL
    """
    ...
