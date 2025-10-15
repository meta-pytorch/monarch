# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final

@final
class SupervisionError(RuntimeError):
    """
    Custom exception for supervision-related errors in monarch_hyperactor.
    """

    ...

# TODO: Make this an exception subclass
@final
class MeshFailure:
    """
    Raise this if you cannot handle the supervision event and you want
    to propagate it to the parent actor.
    """
    @property
    def mesh(self) -> object: ...
