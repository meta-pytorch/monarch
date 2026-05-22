# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import bdb
import io
import logging
import pdb  # noqa
import sys
from typing import Optional, TYPE_CHECKING

from monarch.common import messages

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .worker import Stream, Worker

_orig_set_trace = pdb.set_trace


def _set_trace(*, header=None):
    from .worker import _tls

    stream = _tls.stream
    if stream is None:
        _orig_set_trace(header=header)
    # pyrefly: ignore [bad-argument-type]
    ds = PdbWrapper(stream, header)
    ds.set_trace()


class PdbWrapper(pdb.Pdb):
    def __init__(self, stream: "Stream", header: Optional[str]):
        self.stream = stream
        self.worker: "Worker" = self.stream.worker
        self.header = header
        # pyre-ignore
        super().__init__(stdout=WriteWrapper(self), stdin=ReadWrapper.create(self))
        self._first = True

    def setup(self, *args, **kwargs):
        r = super().setup(*args, **kwargs)
        if self._first:
            self._first = False
            # when we enter the debugger, we want to present the user's stack frame
            # not the nested one inside session.run. This means that the local
            # variables are what gets printed, etc. To do this
            # we first execute up 2 to get to that frame.
            # pyrefly: ignore [bad-argument-type]
            self.do_up(2)
        return r

    def set_continue(self) -> None:
        r = super().set_continue()
        if not self.breaks:
            # no more breakpoints so this debugger will not
            # be used again, and we detach from the controller io.
            self._send("detach")
            # break cycle with itself before we exit
            self.stdin = sys.stdin
            self.stdout = sys.stdout
        return r

    def _send(self, action):
        self.worker.schedule(
            lambda: self.worker.q.send(messages.DebuggerMessage(self.stream.id, action))
        )

    # pyrefly: ignore [bad-override]
    def set_trace(self):
        self._send("paused")
        message = self.stream.debugger_queue.get()
        # we give the controller the option to ignore this request to debug
        # by issuing a "detach" message immediately.
        match message:
            case "attach":
                pass
            case "detach":
                return
            case other:
                raise RuntimeError(f"unexpected debugger message {other}")
        if self.header:
            self.message(self.header)
        super().set_trace()


class ReadWrapper(io.RawIOBase):
    def __init__(self, session: "PdbWrapper"):
        self.session = session

    def readinto(self, b):
        self.session._send(messages.DebuggerRead(len(b)))
        response = self.session.stream.debugger_queue.get()
        if response == "detach":
            # this gets injected by the worker event loop to
            # get the worker thread to exit on an Exit command.
            raise bdb.BdbQuit
        assert isinstance(response, messages.DebuggerWrite) and len(
            response.payload
        ) <= len(b)
        b[: len(response.payload)] = response.payload
        return len(response.payload)

    def readable(self) -> bool:
        return True

    @classmethod
    def create(cls, session: "PdbWrapper"):
        return io.TextIOWrapper(io.BufferedReader(cls(session)))


class WriteWrapper:
    def __init__(self, session: "PdbWrapper"):
        self.session = session

    def writable(self) -> bool:
        return True

    def write(self, s: str):
        self.session._send(messages.DebuggerWrite(s.encode()))

    def flush(self):
        pass
