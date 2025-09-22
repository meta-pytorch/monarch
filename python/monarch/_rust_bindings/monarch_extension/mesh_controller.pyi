# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from traceback import FrameSummary
from typing import List, NamedTuple, Sequence, Tuple, Union

from monarch._rust_bindings.monarch_extension import client
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortId
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh

from monarch._rust_bindings.monarch_hyperactor.shape import Slice as NDSlice

class _Controller:
    def __init__(self) -> None: ...
    def node(
        self,
        seq: int,
        defs: Sequence[object],
        uses: Sequence[object],
        port: Tuple[PortId, NDSlice] | None,
        tracebacks: List[List[FrameSummary]],
    ) -> None: ...
    def drop_refs(self, refs: Sequence[object]) -> None: ...
    def send(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        msg: NamedTuple,
    ) -> None: ...
    def _drain_and_stop(
        self,
    ) -> List[client.LogMessage | client.WorkerResponse | client.DebuggerMessage]: ...
    def sync_at_exit(self, port: PortId) -> None:
        """
        Controller waits until all nodes that were added are complete, then replies on the
        given port. The port will get an exception if there was a known error that was not reported
        to any future.
        """
        ...

    @property
    def broker_id(self) -> Tuple[str, int]: ...
