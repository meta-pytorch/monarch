# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final, Optional

from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.pytokio import Shared

@final
class SupervisionError(RuntimeError):
    """
    Custom exception for supervision-related errors in monarch_hyperactor.
    """

    endpoint: str | None  # Settable attribute

# TODO: Make this an exception subclass
@final
class MeshFailure:
    """
    Contains details about a failure on a mesh. This can be from an ActorMesh,
    ProcMesh, or HostMesh.
    The __str__ of this failure will provide the origin resource (actor, proc, host)
    of the failure along with the reason.
    """
    @property
    def mesh(self) -> object: ...
    def report(self) -> str:
        """
        User-readable error report for this particular failure.
        """
        ...

@final
class Supervisor:
    """
    A wrapper for supervision functionality.

    This provides a concrete type that can be passed to endpoint collectors
    to monitor actor health during endpoint operations.
    """

    ...

    def supervision_event_task(
        self, instance: Instance
    ) -> Optional[Shared[Exception]]: ...
