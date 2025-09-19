# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final, Union

@final
class DebuggerAction:
    """Enum representing actions for the debugger communication between worker and client."""

    class Paused:
        """
        Sent from worker to client to indicate that the worker has entered
        a pdb debugging session.
        """

        pass

    class Attach:
        """
        Sent from client to worker to indicate that the client has started
        the debugging session.
        """

        pass

    class Detach:
        """Sent to client or to worker to end the debugging session."""

        pass

    class Write:
        """Sent to client or to worker to write bytes to receiver's stdout."""

        def __init__(self, bytes: bytes) -> None: ...

    class Read:
        """Sent from worker to client to read bytes from client's stdin."""

        def __init__(self, requested_size: int) -> None: ...
        @property
        def requested_size(self) -> int:
            """Get the number of bytes to read from stdin."""
            ...

DebuggerActionType = Union[
    DebuggerAction.Paused,
    DebuggerAction.Attach,
    DebuggerAction.Detach,
    DebuggerAction.Read,
    DebuggerAction.Write,
]
