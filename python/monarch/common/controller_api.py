# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
from typing import Any, List, NamedTuple, Optional, Protocol, Sequence, Union

from monarch._rust_bindings.monarch_extension.client import (  # @manual=//monarch/monarch_extension:monarch_extension
    DebuggerMessage,
    LogLevel,
)
from monarch._src.actor.shape import NDSlice
from monarch.common.invocation import DeviceException, RemoteException, Seq
from monarch.common.reference import Ref
from monarch.common.tensor import Tensor


class LogMessage(NamedTuple):
    level: LogLevel
    message: str


class MessageResult(NamedTuple):
    """
    Message result given a seq id of an invocation.
    """

    seq: Seq
    result: Any
    error: Optional[RemoteException | DeviceException] = None


class TController(Protocol):
    """
    Controller APIs
    """

    # =======================================================
    # === APIs for the client to call into the controller ===
    # =======================================================

    def send(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        msg: NamedTuple,
    ) -> None:
        """
        Send a message to a set of ranks.
        """
        ...

    def drop_refs(self, refs: Sequence[Ref]) -> None:
        """
        Mark references as never being used again
        """
        ...

    # TODO: there are a few things to do to clean up the API:
    # 2. no need to depend on Tensors, a Referenceable; a Ref is enough.
    # 3. support mutates as another input parameter.
    def node(
        self, seq: Seq, defs: Sequence["Tensor"], uses: Sequence["Tensor"]
    ) -> None:
        """
        Create an invocation node given a sequence id. The node provides what tensors it defines,
        what tensors it uses, and what tensors it mutates.
        """
        ...

    # ==============================================================
    # == APIs for the client to read response from the controller ==
    # ==============================================================

    # TODO: remove timeout parameter; instead, return a future that can wait on a timeout
    def next_message(
        self, timeout: Optional[float]
    ) -> Optional[MessageResult | LogMessage]:
        """
        Read a message given a timeout in seconds. Returns a message output given the seq of an invocation.
        The output could be the returned value or an exception.
        If the returned message is None, it means there is no message to read within the given timeout.
        If timeout is None, it means no timeout (infinite).
        """
        ...

    def stop_mesh(self) -> None:
        """Stop the system."""
        ...

    def drain_and_stop(self) -> List[MessageResult | LogMessage | DebuggerMessage]:
        """Drain all the messages in the controller upon shutdown."""
        ...
