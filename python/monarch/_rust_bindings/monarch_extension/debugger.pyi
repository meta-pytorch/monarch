# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final, Optional, Union

from monarch._rust_bindings.monarch_hyperactor.proc import Serialized
from monarch._rust_bindings.monarch_messages.debugger import (
    DebuggerAction,
    DebuggerActionType,
)

@final
class DebuggerMessage:
    """A message for debugger communication between worker and client."""

    def __init__(self, action: DebuggerActionType) -> None:
        """
        Create a new DebuggerMessage.

        Arguments:
            action: The debugger action to include in the message.
        """
        ...

    @property
    def action(self) -> DebuggerActionType:
        """Get the debugger action contained in this message."""
        ...

    def serialize(self) -> Serialized:
        """
        Serialize this message for transmission.

        Returns:
            A serialized representation of this message.
        """
        ...

@final
class PdbActor:
    """An actor for interacting with PDB debugging sessions."""

    def __init__(self) -> None:
        """Create a new PdbActor."""
        ...

    def send(self, action: DebuggerActionType) -> None:
        """
        Send a debugger action to the worker.

        Arguments:
            action: The debugger action to send.
        """
        ...

    def receive(self) -> Optional[DebuggerActionType]:
        """
        Receive a debugger action from the worker.

        Returns:
            A DebuggerAction if one is available, or None if no action is available.
        """
        ...

    def drain_and_stop(self) -> None:
        """
        Drain any remaining messages and stop the actor.
        """
        ...

def get_bytes_from_write_action(action: DebuggerAction.Write) -> bytes:
    """
    Extract the bytes from the provided write action.
    """
    ...
