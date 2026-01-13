# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
from collections import deque
from typing import (
    cast,
    Generator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Union,
)

import torch
from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
)
from monarch._src.actor.shape import iter_ranks, NDSlice, Slices as Ranks
from monarch.common import messages
from monarch.common.controller_api import DebuggerMessage, LogMessage, MessageResult
from monarch.common.device_mesh import no_mesh
from monarch.common.invocation import Invocation, RemoteException, Seq
from monarch.common.reference import Ref
from monarch.common.tree import flatten

if TYPE_CHECKING:
    from monarch.common.tensor import Tensor

logger = logging.getLogger(__name__)


class History:
    def __init__(self, N):
        self.first_uncompleted_ident = [0 for _ in range(N)]
        self.min_first_uncompleted_ident = 0
        self.invocations = deque[Invocation]()

    def _invocation(
        self,
        seq: Seq,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
    ):
        r = Invocation(seq)
        for t in uses:
            u = t._invocation
            assert u is not None
            u.add_user(r)
        for t in defs:
            t._invocation = r
        return r

    def ident(
        self,
        seq: Seq,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
    ):
        invocation = self._invocation(seq, defs, uses)
        self.invocations.append(invocation)

    def propagate_failure(self, seq, traceback_index, exception, worker_frames):
        invocation = self.invocations[seq - self.min_first_uncompleted_ident]
        remote_exception = RemoteException(
            seq,
            exception,
            traceback_index,
            None,
            worker_frames,
            ActorId.from_string("unknown[0].unknown[0]"),
        )
        worklist = deque((invocation,))
        while worklist:
            invocation = worklist.popleft()
            if invocation.fail(remote_exception):
                worklist.extend(invocation.users)

    def rank_completed(
        self, rank, first_uncompleted_ident
    ) -> Generator[MessageResult, None, None]:
        # advance what our last completed action was, and
        # trim the list of tracebacks if we no longer need them.
        prev = self.first_uncompleted_ident[rank]
        self.first_uncompleted_ident[rank] = first_uncompleted_ident
        if prev == self.min_first_uncompleted_ident:
            self.min_first_uncompleted_ident = min(self.first_uncompleted_ident)
            for seq in range(prev, self.min_first_uncompleted_ident):
                invocation = self.invocations.popleft()
                assert seq == invocation.seq
                result, error = invocation.complete()
                yield MessageResult(
                    seq=seq,
                    result=result,
                    error=error,
                )

    def future_completed(self, ident, value):
        invocation = self.invocations[ident - self.min_first_uncompleted_ident]
        invocation.fut_value = value


class MockController:
    def __init__(self, world_size: int, verbose: bool = True):
        self.history = History(world_size)
        self.world_size = world_size
        self.responses = deque[MessageResult | LogMessage | DebuggerMessage]()
        self.exited = False
        self.verbose = verbose

    @property
    def gpu_per_host(self) -> int:
        return self.world_size

    def send(self, ranks: Union[NDSlice, List[NDSlice]], msg: NamedTuple) -> None:
        attr = getattr(self, type(msg).__name__, None)
        if self.verbose:
            logger.info(
                "MockController: %s %s %s", str(ranks), str(type(msg)), str(msg)
            )

        if attr is not None:
            attr(ranks, msg)

    def next_message(
        self, timeout: Optional[float]
    ) -> Optional[MessageResult | LogMessage]:
        return (
            cast(Optional[MessageResult | LogMessage], self.responses.popleft())
            if len(self.responses) > 0
            else None
        )

    def stop_mesh(self) -> None:
        pass

    def drain_and_stop(self) -> List[MessageResult | LogMessage | DebuggerMessage]:
        if not self.exited:
            raise RuntimeError("Got drain_and_stop but exited is not True")
        r = list(self.responses)
        self.responses.clear()
        return r

    def drop_refs(self, refs: Sequence[Ref]) -> None:
        """
        noop as this is used for the Rust controller to know when to gc invocations_for_ref for failed invocations
        """
        pass

    def node(
        self, seq: Seq, defs: Sequence["Tensor"], uses: Sequence["Tensor"]
    ) -> None:
        self.history.ident(seq, defs, uses)

    # Below are the messages that should be executed on "workers".
    def CommandGroup(self, ranks: Ranks, msg: messages.CommandGroup):
        for command in msg.commands:
            self.send(ranks, command)

    def RequestStatus(self, ranks: Ranks, msg: messages.RequestStatus):
        for rank in iter_ranks(ranks):
            for r in self.history.rank_completed(rank, msg.ident + 1):
                self.responses.append(r)

    def SendValue(self, ranks: Ranks, msg: messages.SendValue):
        dtensors, unflatten = flatten(
            (msg.args, msg.kwargs), lambda x: isinstance(x, torch.Tensor)
        )
        fake_args, _fake_kwargs = unflatten(d._fake for d in dtensors)
        if msg.function is not None:
            fake_result = None
        else:
            fake_result = fake_args[0]

        if msg.destination is None:
            # If the destination is the controller, we need to send back an actual
            # tensor, not a fake tensor because the rest operations are likely to
            # be data dependent (e.g., losses.item()).
            # Note that this also means that if the controller are going to branch
            # out the execution, the execution path is going to diverge from the
            # actual workload.
            with no_mesh.activate():
                tensors, unflatten = flatten(
                    fake_result, lambda x: isinstance(x, torch.Tensor)
                )

                def create_zeros(t: torch.Tensor) -> torch.Tensor:
                    """Create zeros tensor, falling back to CPU if CUDA fails."""
                    try:
                        return torch.zeros(
                            t.size(),
                            dtype=t.dtype,
                            device=t.device,
                            requires_grad=False,
                        )
                    except Exception:
                        # CUDA not functional, use CPU instead
                        return torch.zeros(
                            t.size(),
                            dtype=t.dtype,
                            device="cpu",
                            requires_grad=False,
                        )

                fake_result = unflatten(create_zeros(t) for t in tensors)
            for _ in iter_ranks(ranks):
                self.responses.append(
                    self.history.future_completed(msg.ident, fake_result)
                )

    def Exit(self, ranks: Ranks, msg: messages.Exit):
        self.exited = True
