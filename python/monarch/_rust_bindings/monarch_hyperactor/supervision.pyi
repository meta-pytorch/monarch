# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final, Optional

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import ActorSupervisionEvent
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.pytokio import Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Point

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
    @property
    def mesh_name(self) -> str | None:
        """Optional user-facing display name for diagnostics. Use
        ``mesh_id`` for identity comparisons."""
        ...

    @property
    def mesh_id(self) -> str | None:
        """Stable internal mesh ID. Compare this with a mesh object's ``id``."""
        ...

    @property
    def coordinate(self) -> Point | None:
        """The coordinate of the point in the mesh where the failure occurred.
        For single-point failures, this is the coordinate of the failed point.
        For whole-mesh failures, this is None."""
        ...

    @property
    def event(self) -> ActorSupervisionEvent:
        """The interior ActorSupervisionEvent that caused this mesh failure.
        Provides access to the actor_id, display_name, actor_status, etc."""
        ...

    def report(self) -> str:
        """
        User-readable error report for this particular failure.
        """
        ...
