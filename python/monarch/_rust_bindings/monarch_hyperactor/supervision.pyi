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
    """Custom exception for supervision-related errors in
    monarch_hyperactor.

    Structured attribution fields (``mesh_name``, ``actor_class``,
    ``actor_display_name``, ``rank``) are populated from
    ``ActorSupervisionEvent.attribution`` upstream. Consumers should
    read these attributes directly instead of parsing the rendered
    message or ``__str__``. These fields are populated only on the
    supervision paths that have been wired to carry attribution; on
    other paths they may be ``None``.
    """

    endpoint: str | None  # Settable attribute

    @property
    def mesh_name(self) -> str | None: ...
    @property
    def actor_class(self) -> str | None: ...
    @property
    def actor_display_name(self) -> str | None: ...
    @property
    def rank(self) -> int | None: ...

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
