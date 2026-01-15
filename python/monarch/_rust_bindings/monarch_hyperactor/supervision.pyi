# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final, Optional

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
