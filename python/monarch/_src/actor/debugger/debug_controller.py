# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import asyncio
import functools
from typing import Dict, List, Optional, Tuple

from monarch._src.actor.actor_mesh import Actor
from monarch._src.actor.debugger.debug_command import (
    Attach,
    Cast,
    Continue,
    DebugCommand,
    Help,
    ListCommand,
    Quit,
    RanksType,
)
from monarch._src.actor.debugger.debug_io import (
    DebugCliIO,
    DebugIO,
    DebugIOError,
    DebugStdIO,
)
from monarch._src.actor.debugger.debug_session import (
    DebugSession,
    DebugSessionInfo,
    DebugSessions,
)
from monarch._src.actor.debugger.pdb_wrapper import DebuggerWrite
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.proc_mesh import get_or_spawn_controller
from monarch._src.actor.sync_state import fake_sync_state
from monarch.tools.debug_env import (
    _get_debug_server_host,
    _get_debug_server_port,
    _get_debug_server_protocol,
)
from pyre_extensions import none_throws
from tabulate import tabulate


class DebugController(Actor):
    """
    Single actor for both remote debuggers and users to talk to.

    Handles multiple sessions simultanesouly
    """

    def __init__(self) -> None:
        self.sessions = DebugSessions()
        self._task_lock = asyncio.Lock()
        self._task: Optional[asyncio.Task[None]] = None
        self._debug_io: DebugIO = DebugStdIO()
        self._server: asyncio.Future[asyncio.Server] = asyncio.Future()
        self._server_task: asyncio.Task[None] = asyncio.create_task(self._serve())

    async def _serve(self) -> None:
        try:
            if (proto := _get_debug_server_protocol()) != "tcp":
                raise NotImplementedError(
                    f"Network protocol {proto} not yet supported."
                )
            server = await asyncio.start_server(
                self._handle_client,
                _get_debug_server_host(),
                _get_debug_server_port(),
            )
            async with server:
                self._server.set_result(server)
                await server.serve_forever()
        except Exception as e:
            if self._server.done():
                self._server = asyncio.Future()
            self._server.set_exception(e)
            raise

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # Make sure only one external debug process can
        # be attached at a time. If a new request is
        # received, the current task is cancelled.
        async with self._task_lock:
            if self._task is not None:
                self._task.cancel()
                try:
                    await none_throws(self._task)
                except (DebugIOError, asyncio.CancelledError):
                    pass
            self._debug_io = DebugCliIO(reader, writer)
            self._task = asyncio.create_task(self._enter())

    @endpoint
    async def wait_pending_session(self) -> None:
        while len(self.sessions) == 0:
            await asyncio.sleep(1)

    @endpoint
    async def list(self, print_output: bool = True) -> List[DebugSessionInfo]:
        session_info = sorted(self.sessions.info())
        if print_output:
            await self._debug_io.output(
                tabulate(
                    (
                        (
                            info.actor_name,
                            info.rank,
                            info.coords,
                            info.hostname,
                            info.function,
                            info.lineno,
                        )
                        for info in session_info
                    ),
                    headers=[
                        "Actor Name",
                        "Rank",
                        "Coords",
                        "Hostname",
                        "Function",
                        "Line No.",
                    ],
                    tablefmt="grid",
                )
                + "\n"
            )
        return session_info

    async def _enter(self) -> None:
        await asyncio.sleep(0.5)
        await self._debug_io.output(
            "\n\n************************ MONARCH DEBUGGER ************************\n"
        )
        await self._debug_io.output("Enter 'help' for a list of commands.\n")
        await self._debug_io.output("Enter 'list' to show all active breakpoints.\n\n")

        while True:
            try:
                user_input = await self._debug_io.input("monarch_dbg> ")
                if not user_input.strip():
                    continue
                command = await DebugCommand.parse(self._debug_io, user_input)
                if isinstance(command, Help):
                    await self._debug_io.output("monarch_dbg commands:\n")
                    await self._debug_io.output(
                        "\tattach <actor_name> <rank> - attach to a debug session\n"
                    )
                    await self._debug_io.output("\tlist - list all debug sessions\n")
                    await self._debug_io.output(
                        "\tquit - exit the debugger, leaving all sessions in place\n"
                    )
                    await self._debug_io.output(
                        "\tcast <actor_name> ranks(...) <command> - send a command to a set of ranks on the specified actor mesh.\n"
                        "\t\tThe value inside ranks(...) can be a single rank (ranks(1)),\n"
                        "\t\ta list of ranks (ranks(1,4,6)), a range of ranks (ranks(start?:stop?:step?)),\n"
                        "\t\tor a dict of dimensions (ranks(dim1=1:5:2,dim2=3, dim4=(3,6))).\n"
                    )
                    await self._debug_io.output(
                        "\tcontinue - clear all breakpoints and tell all ranks to continue\n"
                    )
                    await self._debug_io.output("\thelp - print this help message\n")
                elif isinstance(command, Attach):
                    await self.sessions.get(command.actor_name, command.rank).attach(
                        self._debug_io
                    )
                elif isinstance(command, ListCommand):
                    # pyre-ignore
                    await self.list._method(self)
                elif isinstance(command, Continue):
                    await self._cast_input_and_wait("clear")
                    await self._cast_input_and_wait("c")
                elif isinstance(command, Quit):
                    await self._debug_io.quit()
                    return
                elif isinstance(command, Cast):
                    await self._cast_input_and_wait(
                        command.command, (command.actor_name, command.ranks)
                    )
            except (DebugIOError, asyncio.CancelledError):
                raise
            except Exception as e:
                await self._debug_io.output(f"Error processing command: {e}\n")

    async def _cast_input_and_wait(
        self,
        command: str,
        selection: Optional[Tuple[str, Optional[RanksType]]] = None,
    ) -> None:
        tasks = []
        for session in self.sessions.iter(selection):
            tasks.append(session.attach(self._debug_io, command, suppress_output=True))
        await asyncio.gather(*tasks)

    ##########################################################################
    # Debugger APIs
    #
    # These endpoints are called by the remote debuggers to establish sessions
    # and communicate with them.
    @endpoint
    async def debugger_session_start(
        self, rank: int, coords: Dict[str, int], hostname: str, actor_name: str
    ) -> None:
        # Good enough for now to ensure that if the server for processing
        # user interactions never starts, then the rank being debugged will
        # fail instead of hanging indefinitely with no way to send it commands.
        # Of course this isn't sufficient to handle the case where the server
        # fails after the rank's debug session has successfully started.
        # TODO: implement a heartbeat to prevent pdb sessions from hanging.
        await self._server
        # Create a session if it doesn't exist
        if (actor_name, rank) not in self.sessions:
            self.sessions.insert(DebugSession(rank, coords, hostname, actor_name))

    @endpoint
    async def debugger_session_end(self, actor_name: str, rank: int) -> None:
        """Detach from the current debug session."""
        await self.sessions.remove(actor_name, rank).detach()

    @endpoint
    async def debugger_read(
        self, actor_name: str, rank: int, size: int
    ) -> DebuggerWrite | str:
        """Read from the debug session for the given rank."""
        return await self.sessions.get(actor_name, rank).debugger_read(size)

    @endpoint
    async def debugger_write(
        self, actor_name: str, rank: int, write: DebuggerWrite
    ) -> None:
        """Write to the debug session for the given rank."""
        await self.sessions.get(actor_name, rank).debugger_write(write)


# Cached so that we don't have to call out to the root client every time,
# which may be on a different host.
@functools.cache
def debug_controller() -> DebugController:
    with fake_sync_state():
        return get_or_spawn_controller("debug_controller", DebugController).get()
