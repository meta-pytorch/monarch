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
    @property
    def mesh_name(self) -> str | None:
        """The user-facing mesh base name, if the source supervision
        event carried structured attribution."""
        ...

    @property
    def actor_class(self) -> str | None:
        """Structured actor-class token of the failing actor, if the
        source supervision event carried structured attribution. For
        Python-spawned actors this is typically ``module.qualname``."""
        ...

    @property
    def actor_display_name(self) -> str | None:
        """Rendered display name of the failing actor, if the source
        supervision event carried structured attribution."""
        ...

    @property
    def rank(self) -> int | None:
        """Mesh rank of the failing actor, if the source supervision
        event carried structured attribution."""
        ...

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
    def mesh_name(self) -> str:
        """The name of the mesh that this failure occurred on. Can be compared
        to existing meshes names to determine identity"""
        ...

    def report(self) -> str:
        """
        User-readable error report for this particular failure.
        """
        ...
