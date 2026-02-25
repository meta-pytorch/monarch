# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
from collections import deque
from typing import Generator, Sequence, TYPE_CHECKING

from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
)
from monarch.common.controller_api import MessageResult
from monarch.common.invocation import Invocation, RemoteException, Seq

if TYPE_CHECKING:
    from monarch.common.tensor import Tensor


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
