# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import bdb
import inspect
import io
import linecache
import os
import pdb  # noqa
import socket
import sys
from contextlib import contextmanager
from dataclasses import dataclass

from typing import Dict, TYPE_CHECKING

from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._src.actor.sync_state import fake_sync_state

if TYPE_CHECKING:
    from monarch._src.actor.debugger.debugger import DebugController


@dataclass
class DebuggerWrite:
    payload: bytes
    function: str | None
    lineno: int | None


@contextmanager
def _debug_controller_request_ctx():
    try:
        with fake_sync_state():
            yield
    except Exception as e:
        raise bdb.BdbQuit from e


class PdbWrapper(pdb.Pdb):
    def __init__(
        self,
        rank: int,
        coords: Dict[str, int],
        actor_id: ActorId,
        controller: "DebugController",
        header: str | None = None,
    ):
        self.rank = rank
        self.coords = coords
        self.header = header
        self.actor_id = actor_id
        self.controller = controller
        # pyre-ignore
        super().__init__(stdout=WriteWrapper(self), stdin=ReadWrapper.create(self))

    def set_trace(self, frame=None):
        with _debug_controller_request_ctx():
            self.controller.debugger_session_start.call_one(
                self.rank,
                self.coords,
                socket.getfqdn(socket.gethostname()),
                self.actor_id.actor_name,
            ).get()
        if self.header:
            self.message(self.header)
        super().set_trace(frame)

    def do_clear(self, arg):
        if not arg:
            # Sending `clear` without any argument specified will
            # request confirmation from the user using the `input` function,
            # which bypasses our ReadWrapper and causes a hang on the client.
            # To avoid this, we just clear all breakpoints instead without
            # confirmation.
            super().clear_all_breaks()
        else:
            super().do_clear(arg)

    def lookupmodule(self, filename):
        filename = super().lookupmodule(filename)
        if (
            filename is not None
            and not os.path.exists(filename)
            and filename not in linecache.cache
        ):
            from monarch._src.actor.actor_mesh import ActorError
            from monarch._src.actor.source_loader import load_remote_source

            try:
                with fake_sync_state():
                    source = load_remote_source(filename)
                    if source:
                        linecache.cache[filename] = (
                            len(source),
                            None,
                            source.splitlines(keepends=True),
                            filename,
                        )
            except ActorError as e:
                self.error(f"Failed querying root client host for source code: {e}")
        return filename

    def end_debug_session(self):
        with _debug_controller_request_ctx():
            self.controller.debugger_session_end.call_one(
                self.actor_id.actor_name, self.rank
            ).get()
        # Once the debug client actor is notified of the session being over,
        # we need to prevent any additional requests being sent for the session
        # by redirecting stdin and stdout.
        self.stdin = sys.stdin
        self.stdout = sys.stdout

    def post_mortem(self, exc_tb):
        with _debug_controller_request_ctx():
            self.controller.debugger_session_start.call_one(
                self.rank,
                self.coords,
                socket.getfqdn(socket.gethostname()),
                self.actor_id.actor_name,
            ).get()

        # See builtin implementation of pdb.post_mortem() for reference.
        self.reset()
        self.interaction(None, exc_tb)


class ReadWrapper(io.RawIOBase):
    def __init__(self, session: "PdbWrapper"):
        self.session = session

    def readinto(self, b):
        with _debug_controller_request_ctx():
            response = self.session.controller.debugger_read.call_one(
                self.session.actor_id.actor_name, self.session.rank, len(b)
            ).get()
            if response == "detach":
                # this gets injected by the worker event loop to
                # get the worker thread to exit on an Exit command.
                raise bdb.BdbQuit
            assert isinstance(response, DebuggerWrite) and len(response.payload) <= len(
                b
            )
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
        function = None
        lineno = None
        if self.session.curframe is not None:
            # pyre-ignore
            function = f"{inspect.getmodulename(self.session.curframe.f_code.co_filename)}.{self.session.curframe.f_code.co_name}"
            # pyre-ignore
            lineno = self.session.curframe.f_lineno
        with _debug_controller_request_ctx():
            self.session.controller.debugger_write.call_one(
                self.session.actor_id.actor_name,
                self.session.rank,
                DebuggerWrite(
                    s.encode(),
                    function,
                    lineno,
                ),
            ).get()

    def flush(self):
        pass
