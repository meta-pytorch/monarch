# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import asyncio
import functools
import inspect
import logging
import os
import sys
from abc import abstractmethod
from dataclasses import dataclass
from typing import cast, Dict, Generator, List, Optional, Tuple, Union

from monarch._src.actor.actor_mesh import Actor, context, DebugContext
from monarch._src.actor.debugger.pdb_wrapper import DebuggerWrite, PdbWrapper
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.proc_mesh import get_or_spawn_controller
from monarch._src.actor.sync_state import fake_sync_state
from pyre_extensions import none_throws
from tabulate import tabulate


logger = logging.getLogger(__name__)

_MONARCH_DEBUG_SERVER_HOST_ENV_VAR = "MONARCH_DEBUG_SERVER_HOST"
_MONARCH_DEBUG_SERVER_HOST_DEFAULT = "localhost"
_MONARCH_DEBUG_SERVER_PORT_ENV_VAR = "MONARCH_DEBUG_SERVER_PORT"
_MONARCH_DEBUG_SERVER_PORT_DEFAULT = "27000"
_MONARCH_DEBUG_SERVER_PROTOCOL_ENV_VAR = "MONARCH_DEBUG_SERVER_PROTOCOL"
_MONARCH_DEBUG_SERVER_PROTOCOL_DEFAULT = "tcp"
_MONARCH_DEBUG_POST_MORTEM_ENABLED_ENV_VAR = "MONARCH_DEBUG_POST_MORTEM_ENABLED"
_MONARCH_DEBUG_POST_MORTEM_ENABLED_DEFAULT = "0"


def _get_debug_server_host():
    return os.environ.get(
        _MONARCH_DEBUG_SERVER_HOST_ENV_VAR, _MONARCH_DEBUG_SERVER_HOST_DEFAULT
    )


def _get_debug_server_port():
    return os.environ.get(
        _MONARCH_DEBUG_SERVER_PORT_ENV_VAR, _MONARCH_DEBUG_SERVER_PORT_DEFAULT
    )


def _get_debug_server_protocol():
    return os.environ.get(
        _MONARCH_DEBUG_SERVER_PROTOCOL_ENV_VAR, _MONARCH_DEBUG_SERVER_PROTOCOL_DEFAULT
    )


class DebugIO:
    @abstractmethod
    async def input(self, prompt: str = "") -> str: ...

    @abstractmethod
    async def output(self, msg: str) -> None: ...

    @abstractmethod
    async def quit(self) -> None: ...


class DebugStdIO(DebugIO):
    async def input(self, prompt: str = "") -> str:
        return await asyncio.to_thread(input, prompt)

    async def output(self, msg: str) -> None:
        sys.stdout.write(msg)
        sys.stdout.flush()

    async def quit(self) -> None:
        pass


class DebugIOError(RuntimeError):
    def __init__(self):
        super().__init__("Error encountered during debugger I/O operation.")


class DebugCliIO(DebugIO):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer

    async def input(self, prompt: str = "") -> str:
        try:
            await self.output(prompt)
            msg = (await self._reader.readline()).decode()
            # Incomplete read due to EOF
            if not msg.endswith("\n"):
                raise RuntimeError("Unexpected end of input.")
            # Strip the newline to be consistent with the behavior of input()
            return msg.strip("\n")
        except Exception as e:
            raise DebugIOError() from e

    async def output(self, msg: str) -> None:
        try:
            self._writer.write(msg.encode())
            await self._writer.drain()
        except Exception as e:
            raise DebugIOError() from e

    async def quit(self) -> None:
        await self.output("Quitting debug session...\n")
        self._writer.close()
        await self._writer.wait_closed()


@dataclass
class DebugSessionInfo:
    actor_name: str
    rank: int
    coords: Dict[str, int]
    hostname: str
    function: str | None
    lineno: int | None

    def __lt__(self, other):
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
    ):
        self.rank = rank
        self.coords = coords
        self.hostname = hostname
        self.actor_name = actor_name
        self._active = False
        self._message_queue = asyncio.Queue()
        self._task = None
        self._pending_send_to_actor = asyncio.Queue()
        self._outputs_since_last_input = []
        self._function_lineno = None
        self._need_read = False

    async def _event_loop(self, debug_io: DebugIO, line=None, suppress_output=False):
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

    def get_info(self):
        function = lineno = None
        if self._function_lineno is not None:
            function, lineno = self._function_lineno
        return DebugSessionInfo(
            self.actor_name, self.rank, self.coords, self.hostname, function, lineno
        )

    async def attach(self, debug_io: DebugIO, line=None, suppress_output=False):
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

    async def detach(self):
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


RanksType = Union[int, List[int], range, Dict[str, Union[range, List[int], int]]]


class DebugSessions:
    def __init__(self):
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


_debug_input_parser = None


# Wrap the parser in a function so that jobs don't have to import lark
# unless they want to use the debugger.
def _get_debug_input_parser():
    global _debug_input_parser
    if _debug_input_parser is None:
        from lark import Lark

        _debug_input_parser = Lark(
            """
            rank_list: INT "," INT ("," INT)*
            start: INT?
            stop: INT?
            step: INT?
            rank_range: start ":" stop (":" step)?
            dim: CNAME "=" (rank_range | "(" rank_list ")" | INT)
            dims: dim ("," dim)*
            ranks: "ranks(" (dims | rank_range | rank_list | INT) ")"
            pdb_command: /\\w+.*/
            actor_name: /[-_a-zA-Z0-9]+/
            cast: "cast" _WS actor_name ranks pdb_command
            help: "h" | "help"
            attach: ("a" | "attach") _WS actor_name INT
            cont: "c" | "continue"
            quit: "q" | "quit"
            list: "l" | "list"
            command: attach | list | cast | help | cont | quit

            _WS: WS+

            %import common.INT
            %import common.CNAME
            %import common.WS
            %ignore WS
            """,
            start="command",
        )
    return _debug_input_parser


_debug_input_transformer = None


# Wrap the transformer in a function so that jobs don't have to import lark
# unless they want to use the debugger.
def _get_debug_input_transformer():
    global _debug_input_transformer
    if _debug_input_transformer is None:
        from lark import Transformer
        from lark.lexer import Token

        class _IntoDebugCommandTransformer(Transformer):
            def rank_list(self, items: List[Token]) -> List[int]:
                return [int(item.value) for item in items]

            def start(self, items: List[Token]) -> int:
                if len(items) == 0:
                    return 0
                return int(items[0].value)

            def stop(self, items: List[Token]) -> int:
                if len(items) == 0:
                    return sys.maxsize
                return int(items[0].value)

            def step(self, items: List[Token]) -> int:
                if len(items) == 0:
                    return 1
                return int(items[0].value)

            def rank_range(self, items: List[int]) -> range:
                return range(*items)

            def dim(
                self, items: Tuple[Token, Union[range, List[int], Token]]
            ) -> Tuple[str, Union[range, List[int], int]]:
                if isinstance(items[1], range):
                    return (items[0].value, cast(range, items[1]))
                elif isinstance(items[1], list):
                    return (items[0].value, cast(List[int], items[1]))
                else:
                    return (items[0].value, int(cast(Token, items[1]).value))

            def dims(
                self, items: List[Tuple[str, Union[range, List[int], int]]]
            ) -> Dict[str, Union[range, List[int], int]]:
                return {dim[0]: dim[1] for dim in items}

            def ranks(self, items: List[Union[RanksType, Token]]) -> RanksType:
                if isinstance(items[0], Token):
                    return int(cast(Token, items[0]).value)
                return cast(RanksType, items[0])

            def pdb_command(self, items: List[Token]) -> str:
                return items[0].value

            def actor_name(self, items: List[Token]) -> str:
                return items[0].value

            def help(self, _items: List[Token]) -> "Help":
                return Help()

            def attach(self, items: Tuple[str, Token]) -> "Attach":
                return Attach(items[0], int(items[1].value))

            def cont(self, _items: List[Token]) -> "Continue":
                return Continue()

            def quit(self, _items: List[Token]) -> "Quit":
                return Quit()

            def cast(self, items: Tuple[str, RanksType, str]) -> "Cast":
                return Cast(*items)

            def list(self, items: List[Token]) -> "ListCommand":
                return ListCommand()

            def command(self, items: List["DebugCommand"]) -> "DebugCommand":
                return items[0]

        _debug_input_transformer = _IntoDebugCommandTransformer()
    return _debug_input_transformer


class DebugCommand:
    @staticmethod
    async def parse(debug_io: DebugIO, line: str) -> Union["DebugCommand", None]:
        try:
            tree = _get_debug_input_parser().parse(line)
            return _get_debug_input_transformer().transform(tree)
        except Exception as e:
            await debug_io.output(f"Error parsing input: {e}\n")
            return None


@dataclass
class Attach(DebugCommand):
    actor_name: str
    rank: int


@dataclass
class ListCommand(DebugCommand):
    pass


@dataclass
class Quit(DebugCommand):
    pass


@dataclass
class Help(DebugCommand):
    pass


@dataclass
class Continue(DebugCommand):
    pass


@dataclass
class Cast(DebugCommand):
    actor_name: str
    ranks: RanksType
    command: str


class DebugController(Actor):
    """
    Single actor for both remote debuggers and users to talk to.

    Handles multiple sessions simultanesouly
    """

    def __init__(self) -> None:
        self.sessions = DebugSessions()
        self._task_lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._debug_io: DebugIO = DebugStdIO()
        self._server = asyncio.Future()
        self._server_task = asyncio.create_task(self._serve())

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
    async def wait_pending_session(self):
        while len(self.sessions) == 0:
            await asyncio.sleep(1)

    @endpoint
    async def list(self, print_output=True) -> List[DebugSessionInfo]:
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

    @endpoint
    def post_mortem_enabled(self) -> bool:
        return (
            os.environ.get(
                _MONARCH_DEBUG_POST_MORTEM_ENABLED_ENV_VAR,
                _MONARCH_DEBUG_POST_MORTEM_ENABLED_DEFAULT,
            )
            == "1"
        )


# Cached so that we don't have to call out to the root client every time,
# which may be on a different host.
@functools.cache
def debug_controller() -> DebugController:
    with fake_sync_state():
        return get_or_spawn_controller("debug_controller", DebugController).get()


def remote_breakpointhook() -> None:
    frame = inspect.currentframe()
    assert frame is not None
    frame = frame.f_back
    assert frame is not None

    ctx = context()
    rank = ctx.message_rank
    pdb_wrapper = PdbWrapper(
        rank.rank,
        {k: rank[k] for k in rank},
        ctx.actor_instance.actor_id,
        debug_controller(),
    )
    DebugContext.set(DebugContext(pdb_wrapper))
    pdb_wrapper.set_trace(frame)


@functools.cache
def _post_mortem_enabled() -> bool:
    # If the _controller_controller isn't set, then we can't
    # query the debug controller, so we can't do post-mortem debugging.
    if context().actor_instance._controller_controller is None:
        return False
    with fake_sync_state():
        return debug_controller().post_mortem_enabled.call_one().get()
