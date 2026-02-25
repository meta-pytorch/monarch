# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from enum import Enum
from typing import Any, final, List, Optional, Tuple, Union

from monarch._rust_bindings.monarch_extension.tensor_worker import Ref
from monarch._rust_bindings.monarch_hyperactor.proc import Serialized

@final
class Node:
    """
    Notify the controller of the dependencies for a worker operation with the
    same seq. It is the responsibility of the caller to ensure the seq is unique
    and strictly increasing and matches the right message. This will be used by
    the controller for history / data dependency tracking.

    Args:
    - `seq`: Sequence number of the message that will be sent to the workers.
    - `defs`: References to the values that the operation defines.
    - `uses`: References to the values that the operation uses.
    - `future`: Reference to the future that the operation returns.
    """

    def __init__(
        self,
        *,
        seq: int,
        defs: List[Ref],
        uses: List[Ref],
    ) -> None: ...
    @property
    def seq(self) -> int:
        """Sequence number of the message that will be sent to the workers."""
        ...

    @property
    def defs(self) -> List[Ref]:
        """References to the values that the operation defines."""
        ...

    @property
    def uses(self) -> List[Ref]:
        """References to the values that the operation uses."""
        ...

    def serialize(self) -> Serialized:
        """Serialize the message into a Serialized object."""
        ...

    @staticmethod
    def from_serialized(serialized: Serialized) -> Node:
        """Deserialize the message from a Serialized object."""
        ...
