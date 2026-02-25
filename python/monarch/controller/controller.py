# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
import traceback
from collections import deque
from typing import Generator, List, NamedTuple, Optional, Sequence, Tuple, Union

from monarch._rust_bindings.monarch_extension.client import (  # @manual=//monarch/monarch_extension:monarch_extension
    DebuggerMessage,
)
from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
)
from monarch._src.actor.shape import NDSlice
from monarch.common import messages
from monarch.common.controller_api import LogMessage, MessageResult
from monarch.common.invocation import DeviceException, Seq
from monarch.common.reference import Ref
from monarch.common.tensor import Tensor
from monarch.controller import debugger

from .backend import Backend
from .history import History

logger = logging.getLogger(__name__)


class Controller:
    def __init__(self, backend: Backend):
        self._backend = backend
        self._history = History(backend.world_size)
        self._messages = deque()

        self.exited = {}
        self.active_debugger: Optional[Tuple[int, int]] = None
        self.pending_debugger_sessions: deque[Tuple[int, int]] = deque()
        # for current active session
        self.pending_debugger_messages: deque[messages.DebuggerMessage] = deque()

    def send(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        msg: NamedTuple,
    ) -> None:
        self._backend.send(ranks, msg)

    def next_message(
        self, timeout: Optional[float]
    ) -> Optional[MessageResult | LogMessage]:
        if len(self._messages) == 0:
            self._messages.extend(self._read_messages(timeout))
        return self._messages.popleft() if len(self._messages) > 0 else None

    def drop_refs(self, refs: Sequence[Ref]) -> None:
        """
        noop as this is used for the Rust controller to know when to gc invocations_for_ref for failed invocations
        """
        pass

    def _read_messages(
        self, timeout: Optional[float]
    ) -> Generator[MessageResult, None, None]:
        # XXX - how can we avoid always requesting status when waiting on futures?
        #       we need to figure out what submesh we need to hear from before a future
        #       is considered 'good'. This means not just waiting for the future value
        #       but also for signal that any failures that could invalidate the future have
        #       not happened. We could do better if tensors/collectives had an invalid bit
        #       that we propagate. In real uses fetches might lag behind anyway so we would not
        #       have to send out so many requests for current status.
        for rank, value in self._backend.recvready(timeout):
            yield from self._handle_message(rank, value)

    def drain_and_stop(self) -> List[MessageResult | LogMessage | DebuggerMessage]:
        messages = []
        while self._messages:
            messages.append(self._messages.popleft())
        while len(self.exited) < self._backend.world_size:
            messages.extend(self._read_messages(None))
        return messages

    def stop_mesh(self) -> None:
        pass

    def node(
        self,
        seq: Seq,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
    ) -> None:
        self._history.ident(seq, defs, uses)

    def _handle_message(self, sender, value) -> Generator[MessageResult, None, None]:
        yield from getattr(self, value.__class__.__name__)(sender, *value)

    def ProcessExited(self, proc, result) -> Generator[MessageResult, None, None]:
        if result != 0:
            # XXX - this should start the failure recovery process
            raise RuntimeError("Unexpected worker process exit")
        self.exited[proc] = result
        yield from []

    def ProcessStarted(self, proc, pid) -> Generator[MessageResult, None, None]:
        yield from []

    def FetchResult(self, proc, ident, value) -> Generator[MessageResult, None, None]:
        self._history.future_completed(ident, value)
        yield from []

    def RemoteFunctionFailed(
        self,
        proc,
        failing_ident,
        traceback_index,
        exception: Exception,
        worker_frames: List[traceback.FrameSummary],
    ) -> Generator[MessageResult, None, None]:
        self._history.propagate_failure(
            failing_ident, traceback_index, exception, worker_frames
        )
        yield from self._history.rank_completed(proc, failing_ident)

    def InternalException(
        self,
        proc,
        exception: Exception,
        worker_frames: List[traceback.FrameSummary],
    ) -> Generator[MessageResult, None, None]:
        yield MessageResult(
            seq=0,  # will not be used
            result=None,
            error=DeviceException(
                exception,
                worker_frames,
                ActorId.from_string("unknown[0].unknown[0]"),
                message="A worker experienced an internal error.",
            ),
        )

    def RemoteGeneratorFailed(
        self,
        proc,
        exception: Exception,
        frames: List[traceback.FrameSummary],
    ) -> Generator[MessageResult, None, None]:
        yield MessageResult(
            seq=0,  # will not be used
            result=None,
            error=DeviceException(
                exception=exception,
                frames=frames,
                source_actor_id=ActorId.from_string("unknown[0].unknown[0]"),
                message="A remote generator failed.",
            ),
        )

    def Status(
        self, proc, first_uncompleted_ident
    ) -> Generator[MessageResult, None, None]:
        yield from self._history.rank_completed(proc, first_uncompleted_ident)

    def DebuggerMessage(
        self, proc, stream_id: int, action
    ) -> Generator[MessageResult, None, None]:
        if action == "paused":
            self.pending_debugger_sessions.append((proc, stream_id))
        else:
            assert self.active_debugger == (proc, stream_id)
            self.pending_debugger_messages.append(action)

        if self.active_debugger is None:
            yield from self._run_debugger_loop()

    def _run_debugger_loop(self) -> Generator[MessageResult, None, None]:
        # debug loop
        while self.pending_debugger_sessions:
            yield from self._run_debugger_session(
                *self.pending_debugger_sessions.popleft()
            )

    def _run_debugger_session(
        self, proc_id: int, stream_id: int
    ) -> Generator[MessageResult, None, None]:
        debugger.write(f"pdb attached to rank {proc_id}, stream {stream_id}\n")
        self.active_debugger = (proc_id, stream_id)
        try:
            rank = NDSlice(offset=proc_id, sizes=[], strides=[])
            self.send(rank, messages.DebuggerMessage(stream_id, "attach"))
            while True:
                while not self.pending_debugger_messages:
                    # todo: eventually we should timeout
                    yield from self._read_messages(None)
                message = self.pending_debugger_messages.popleft()
                match message:
                    case "detach":
                        break
                    case messages.DebuggerRead(requested):
                        self.send(
                            rank,
                            messages.DebuggerMessage(
                                stream_id,
                                messages.DebuggerWrite(debugger.read(requested)),
                            ),
                        )
                    case messages.DebuggerWrite(payload):
                        debugger.write(payload.decode())
                    case other:
                        raise RuntimeError(f"unexpected debugger message: {other}")
        finally:
            self.active_debugger = None
            self.pending_debugger_messages.clear()
