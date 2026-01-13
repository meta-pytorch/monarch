# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import asyncio
from dataclasses import dataclass
from typing import Dict, Generator, List, Optional, Tuple

from monarch._src.actor.debugger.debug_command import RanksType
from monarch._src.actor.debugger.debug_io import DebugIO, DebugIOError
from monarch._src.actor.debugger.pdb_wrapper import DebuggerWrite


@dataclass
class DebugSessionInfo:
    actor_name: str
    rank: int
    coords: Dict[str, int]
    hostname: str
    function: str | None
    lineno: int | None

    def __lt__(self, other: "DebugSessionInfo") -> bool:
        if self.actor_name < other.actor_name:
            return True
        elif self.actor_name == other.actor_name:
            return self.rank < other.rank
        else:
            return False


class DebugSession:
    """Represents a single session with a remote debugger."""

    def __init__(
        self, rank: int, coords: Dict[str, int], hostname: str, actor_name: str
    ) -> None:
        self.rank = rank
        self.coords = coords
        self.hostname = hostname
        self.actor_name = actor_name
        self._active = False
        self._message_queue: asyncio.Queue[str | Tuple[str, DebuggerWrite]] = (
            asyncio.Queue()
        )
        self._task: Optional[asyncio.Task[None]] = None
        self._pending_send_to_actor: asyncio.Queue[bytes] = asyncio.Queue()
        self._outputs_since_last_input: List[DebuggerWrite] = []
        self._function_lineno: Optional[Tuple[str, int]] = None
        self._need_read = False

    async def _event_loop(
        self,
        debug_io: DebugIO,
        line: Optional[str] = None,
        suppress_output: bool = False,
    ) -> None:
        if not suppress_output:
            # If the user had previously attached to this debug session,
            # then it would have printed various messages from the
            # message queue. When the user re-attaches, we want to
            # print out all of the output that was printed since the
            # last command sent to this session.
            if len(self._outputs_since_last_input) > 0:
                await debug_io.output(
                    f"<last pdb output for {self.actor_name} {self.rank} follows>\n"
                )
            for output in self._outputs_since_last_input:
                await debug_io.output(output.payload.decode())

        while True:
            # When the user inputs "detach", it uses up a "read" message
            # without actually responding to the actor being debugged. We
            # can't manually reinsert the "read" message into the message queue,
            # so instead the self._need_read flag indicates there's an additional
            # "read" that we need to respond to.
            if self._need_read:
                self._need_read = False
                message = "read"
            else:
                message = await self._message_queue.get()
            if message == "detach":
                # Return to the main outer debug loop.
                break
            elif message == "read":
                try:
                    break_after = False
                    if line is not None:
                        break_after = True
                    else:
                        line = await debug_io.input()
                    if line == "detach":
                        self._need_read = True
                        break
                    else:
                        await self._pending_send_to_actor.put((line + "\n").encode())
                        # Cancel safety: don't clear the previous outputs until we know
                        # the actor will receive the input.
                        self._outputs_since_last_input = []
                        line = None
                        if break_after:
                            break
                except (DebugIOError, asyncio.CancelledError):
                    # See earlier comment about this flag. If either of the awaits inside
                    # the try block is cancelled, we need to redo the read without actually
                    # reinserting "read" into the message queue.
                    self._need_read = True
                    raise
            elif message[0] == "write":
                output = message[1]
                assert isinstance(output, DebuggerWrite)
                # If the user sees this output but then detaches from the session,
                # its useful to store all outputs since the last input so that
                # they can be printed again when the user re-attaches.
                self._outputs_since_last_input.append(output)
                if not suppress_output:
                    await debug_io.output(output.payload.decode())

        if not suppress_output:
            await debug_io.output(
                f"Detaching from debug session for {self.actor_name} {self.rank} ({self.hostname})\n"
            )

    def get_info(self) -> DebugSessionInfo:
        function = lineno = None
        if self._function_lineno is not None:
            function, lineno = self._function_lineno
        return DebugSessionInfo(
            self.actor_name, self.rank, self.coords, self.hostname, function, lineno
        )

    async def attach(
        self,
        debug_io: DebugIO,
        line: Optional[str] = None,
        suppress_output: bool = False,
    ) -> None:
        self._active = True
        if not suppress_output:
            await debug_io.output(
                f"Attached to debug session for {self.actor_name} {self.rank} ({self.hostname})\n"
            )
        self._task = asyncio.create_task(
            self._event_loop(debug_io, line, suppress_output)
        )
        await self._task
        if not suppress_output:
            await debug_io.output(
                f"Detached from debug session for {self.actor_name} {self.rank} ({self.hostname})\n"
            )
        self._active = False

    async def detach(self) -> None:
        if self._active:
            await self._message_queue.put("detach")

    async def debugger_read(self, size: int) -> DebuggerWrite:
        await self._message_queue.put("read")
        input_data = await self._pending_send_to_actor.get()
        if len(input_data) > size:
            input_data = input_data[:size]
        return DebuggerWrite(input_data, None, None)

    async def debugger_write(self, write: DebuggerWrite) -> None:
        if write.function is not None and write.lineno is not None:
            self._function_lineno = (write.function, write.lineno)
        await self._message_queue.put(("write", write))


class DebugSessions:
    def __init__(self) -> None:
        self._sessions: Dict[str, Dict[int, DebugSession]] = {}

    def insert(self, session: DebugSession) -> None:
        if session.actor_name not in self._sessions:
            self._sessions[session.actor_name] = {session.rank: session}
        elif session.rank not in self._sessions[session.actor_name]:
            self._sessions[session.actor_name][session.rank] = session
        else:
            raise ValueError(
                f"Debug session for rank {session.rank} already exists for actor {session.actor_name}"
            )

    def remove(self, actor_name: str, rank: int) -> DebugSession:
        if actor_name not in self._sessions:
            raise ValueError(f"No debug sessions for actor {actor_name}")
        elif rank not in self._sessions[actor_name]:
            raise ValueError(f"No debug session for rank {rank} for actor {actor_name}")
        session = self._sessions[actor_name].pop(rank)
        if len(self._sessions[actor_name]) == 0:
            del self._sessions[actor_name]
        return session

    def get(self, actor_name: str, rank: int) -> DebugSession:
        if actor_name not in self._sessions:
            raise ValueError(f"No debug sessions for actor {actor_name}")
        elif rank not in self._sessions[actor_name]:
            raise ValueError(f"No debug session for rank {rank} for actor {actor_name}")
        return self._sessions[actor_name][rank]

    def iter(
        self, selection: Optional[Tuple[str, Optional[RanksType]]]
    ) -> Generator[DebugSession, None, None]:
        if selection is None:
            for sessions in self._sessions.values():
                for session in sessions.values():
                    yield session
            return
        actor_name, ranks = selection
        if actor_name not in self._sessions:
            return
        sessions = self._sessions[actor_name]
        if ranks is None:
            for session in sessions.values():
                yield session
        elif isinstance(ranks, int):
            if ranks in sessions:
                yield sessions[ranks]
        elif isinstance(ranks, list):
            for rank in ranks:
                if rank in sessions:
                    yield sessions[rank]
        elif isinstance(ranks, dict):
            dims = ranks
            for session in sessions.values():
                include_rank = True
                for dim, ranks in dims.items():
                    if dim not in session.coords:
                        include_rank = False
                        break
                    elif (
                        isinstance(ranks, range) or isinstance(ranks, list)
                    ) and session.coords[dim] not in ranks:
                        include_rank = False
                        break
                    elif isinstance(ranks, int) and session.coords[dim] != ranks:
                        include_rank = False
                        break
                if include_rank:
                    yield session
        elif isinstance(ranks, range):
            for rank, session in sessions.items():
                if rank in ranks:
                    yield session

    def info(self) -> List[DebugSessionInfo]:
        session_info = []
        for sessions in self._sessions.values():
            for session in sessions.values():
                session_info.append(session.get_info())
        return session_info

    def __len__(self) -> int:
        return sum(len(sessions) for sessions in self._sessions.values())

    def __contains__(self, item: Tuple[str, int]) -> bool:
        actor_name, rank = item
        return actor_name in self._sessions and rank in self._sessions[actor_name]
