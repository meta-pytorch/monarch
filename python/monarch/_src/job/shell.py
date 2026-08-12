# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import contextlib
import errno
import fcntl
import os
import signal
import struct
import sys
import termios
import tty
from typing import AsyncIterator, Callable, Iterator, NamedTuple, TypeAlias

from monarch.actor import (
    Actor,
    Channel,
    context,
    endpoint,
    HostMesh,
    Port,
    PortReceiver,
)


class _ShellInputData(NamedTuple):
    data: bytes


class _ShellInputWindowSize(NamedTuple):
    window_size: tuple[int, int]


class _ShellInputClose(NamedTuple):
    pass


_ShellInput: TypeAlias = _ShellInputData | _ShellInputWindowSize | _ShellInputClose


class _ShellOutputData(NamedTuple):
    data: bytes


class _ShellOutputReturncode(NamedTuple):
    returncode: int


class _ShellOutputError(NamedTuple):
    error: str


_ShellOutput: TypeAlias = _ShellOutputData | _ShellOutputReturncode | _ShellOutputError


_CHUNK_SIZE = 16 * 1024
_PROCESS_GRACE_SECONDS = 1


def _set_window_size(fd: int, window_size: tuple[int, int]) -> None:
    rows, columns = window_size
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, columns, 0, 0))


def _window_size(fd: int) -> tuple[int, int]:
    try:
        size = os.get_terminal_size(fd)
    except OSError:
        return 24, 80
    return size.lines, size.columns


def _rank_env() -> dict[str, str]:
    rank = context().actor_instance.rank
    return {
        **{f"MONARCH_RANK_{key}": str(value) for key, value in dict(rank).items()},
        **{
            f"MONARCH_SIZE_{key}": str(value)
            for key, value in zip(rank.extent.keys(), rank.extent.sizes)
        },
    }


def _normalize_returncode(returncode: int) -> int:
    return returncode if returncode >= 0 else min(128 - returncode, 255)


def _process_groups(master_fd: int, process: asyncio.subprocess.Process) -> set[int]:
    groups = {process.pid}
    with contextlib.suppress(OSError):
        groups.add(os.tcgetpgrp(master_fd))
    groups.discard(os.getpgrp())
    return {group for group in groups if group > 1}


def _signal_process_groups(groups: set[int], sig: signal.Signals) -> None:
    for group in groups:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(group, sig)


def _live_process_groups(groups: set[int]) -> set[int]:
    live = set()
    for group in groups:
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            continue
        except PermissionError:
            pass
        live.add(group)
    return live


async def _stop_process(
    master_fd: int,
    process: asyncio.subprocess.Process,
) -> None:
    groups = _process_groups(master_fd, process)
    _signal_process_groups(groups, signal.SIGHUP)
    if process.returncode is None:
        try:
            await asyncio.wait_for(process.wait(), timeout=_PROCESS_GRACE_SECONDS)
        except asyncio.TimeoutError:
            pass

    live = _live_process_groups(groups)
    if live:
        _signal_process_groups(live, signal.SIGTERM)
        await asyncio.sleep(_PROCESS_GRACE_SECONDS)
        _signal_process_groups(_live_process_groups(live), signal.SIGKILL)

    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()


class _ShellActor(Actor):
    def __init__(self) -> None:
        self._process: asyncio.subprocess.Process | None = None
        self._master_fd: int | None = None
        self._session_task: asyncio.Task[None] | None = None

    async def _read_output(
        self,
        master_fd: int,
        process: asyncio.subprocess.Process,
        output: Port[_ShellOutput],
    ) -> None:
        async with _read_stream(master_fd) as reader:
            while True:
                try:
                    data = await reader.read(_CHUNK_SIZE)
                except OSError as error:
                    if error.errno == errno.EIO:
                        break
                    raise
                if not data:
                    break
                output.send(_ShellOutputData(data))
        returncode = await process.wait()
        output.send(_ShellOutputReturncode(_normalize_returncode(returncode)))

    async def _write_input(
        self,
        receiver: PortReceiver[_ShellInput],
        master_fd: int,
        process: asyncio.subprocess.Process,
    ) -> None:
        async with _write_stream(master_fd) as writer:
            while process.returncode is None:
                message = await receiver.recv()
                match message:
                    case _ShellInputClose():
                        await _stop_process(master_fd, process)
                        return
                    case _ShellInputWindowSize(window_size):
                        _set_window_size(master_fd, window_size)
                    case _ShellInputData(data):
                        writer.write(data)
                        await writer.drain()

    async def _run_session(
        self,
        receiver: PortReceiver[_ShellInput],
        master_fd: int,
        process: asyncio.subprocess.Process,
        output: Port[_ShellOutput],
    ) -> None:
        input_task = asyncio.create_task(
            self._write_input(
                receiver,
                master_fd,
                process,
            ),
            name="monarch-shell-input",
        )
        output_task = asyncio.create_task(
            self._read_output(master_fd, process, output),
            name="monarch-shell-output",
        )
        try:
            done, _ = await asyncio.wait(
                (input_task, output_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if output_task in done:
                await output_task
                return
            await input_task
            await output_task
        except Exception as error:
            with contextlib.suppress(Exception):
                output.send(_ShellOutputError(str(error)))
            await _stop_process(master_fd, process)
        finally:
            input_task.cancel()
            output_task.cancel()
            await asyncio.gather(input_task, output_task, return_exceptions=True)

    @endpoint
    async def start(
        self,
        output: Port[_ShellOutput],
        env: dict[str, str] | None,
        workdir: str | None,
        client_cwd: str,
        terminal: str,
        window_size: tuple[int, int],
    ) -> Port[_ShellInput]:
        if self._process is not None:
            raise RuntimeError("shell is already running")

        input_port, input_receiver = Channel[_ShellInput].open()
        master_fd, slave_fd = os.openpty()
        _set_window_size(master_fd, window_size)
        child_env = {**os.environ, **_rank_env(), **(env or {}), "TERM": terminal}
        effective_workdir = workdir or (
            client_cwd if os.path.isdir(client_cwd) else None
        )
        shell_executable = child_env.get("SHELL", "/bin/bash")
        try:
            process = await asyncio.create_subprocess_exec(
                "/usr/bin/setsid",
                "--ctty",
                shell_executable,
                "-i",
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                cwd=effective_workdir,
                env=child_env,
            )
        except (OSError, ValueError):
            os.close(master_fd)
            raise
        finally:
            os.close(slave_fd)

        self._process = process
        self._master_fd = master_fd
        self._session_task = asyncio.create_task(
            self._run_session(input_receiver, master_fd, process, output),
            name="monarch-shell-session",
        )
        return input_port

    async def __cleanup__(self, exc: Exception | None) -> None:
        process = self._process
        if process is not None and process.returncode is None:
            assert self._master_fd is not None
            await _stop_process(self._master_fd, process)
        if self._session_task is not None:
            self._session_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._session_task
            self._session_task = None
        if self._master_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._master_fd)
            self._master_fd = None


@contextlib.contextmanager
def _raw_terminal(fd: int) -> Iterator[None]:
    if not os.isatty(fd):
        yield
        return

    attributes = termios.tcgetattr(fd)
    tty.setraw(fd)
    try:
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, attributes)


@contextlib.contextmanager
def _nonblocking(fd: int) -> Iterator[None]:
    blocking = os.get_blocking(fd)
    os.set_blocking(fd, False)
    try:
        yield
    finally:
        os.set_blocking(fd, blocking)


@contextlib.contextmanager
def _resize_handler(notify: Callable[[], None]) -> Iterator[None]:
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signal.SIGWINCH)
    try:
        loop.add_signal_handler(signal.SIGWINCH, notify)
    except (RuntimeError, ValueError):
        yield
        return
    try:
        yield
    finally:
        loop.remove_signal_handler(signal.SIGWINCH)
        signal.signal(signal.SIGWINCH, previous)


@contextlib.asynccontextmanager
async def _read_stream(fd: int) -> AsyncIterator[asyncio.StreamReader]:
    loop = asyncio.get_running_loop()
    pipe = os.fdopen(os.dup(fd), "rb", buffering=0)
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    try:
        transport, _ = await loop.connect_read_pipe(lambda: protocol, pipe)
    except BaseException:
        pipe.close()
        raise
    try:
        yield reader
    finally:
        transport.close()


@contextlib.asynccontextmanager
async def _write_stream(fd: int) -> AsyncIterator[asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    pipe = os.fdopen(os.dup(fd), "wb", buffering=0)
    protocol = asyncio.streams.FlowControlMixin(loop=loop)
    try:
        transport, _ = await loop.connect_write_pipe(lambda: protocol, pipe)
    except BaseException:
        pipe.close()
        raise
    writer = asyncio.StreamWriter(transport, protocol, None, loop)
    try:
        yield writer
    finally:
        writer.close()
        with contextlib.suppress(
            BrokenPipeError, ConnectionResetError, NotImplementedError
        ):
            await writer.wait_closed()


async def _forward_terminal(
    input_port: Port[_ShellInput],
    output_receiver: PortReceiver[_ShellOutput],
    stdin_fd: int,
    stdout_fd: int,
) -> int:
    async def forward_input(reader: asyncio.StreamReader) -> None:
        try:
            while data := await reader.read(_CHUNK_SIZE):
                input_port.send(_ShellInputData(data))
        except BaseException:
            with contextlib.suppress(Exception):
                input_port.send(_ShellInputClose())
            raise
        else:
            input_port.send(_ShellInputClose())

    async def forward_output(writer: asyncio.StreamWriter) -> int:
        writer.write(b"\r")
        await writer.drain()
        while True:
            message = await output_receiver.recv()
            match message:
                case _ShellOutputError(error):
                    raise RuntimeError(error)
                case _ShellOutputData(data):
                    writer.write(data)
                    await writer.drain()
                case _ShellOutputReturncode(returncode):
                    return returncode

    def resize() -> None:
        input_port.send(_ShellInputWindowSize(_window_size(stdin_fd)))

    with (
        _raw_terminal(stdin_fd),
        _nonblocking(stdin_fd),
        _nonblocking(stdout_fd),
        _resize_handler(resize),
    ):
        async with (
            _read_stream(stdin_fd) as reader,
            _write_stream(stdout_fd) as writer,
        ):
            input_task = asyncio.create_task(
                forward_input(reader),
                name="monarch-shell-client-input",
            )
            output_task = asyncio.create_task(
                forward_output(writer),
                name="monarch-shell-client-output",
            )
            try:
                done, _ = await asyncio.wait(
                    (input_task, output_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if input_task in done:
                    await input_task
                return await output_task
            finally:
                input_task.cancel()
                output_task.cancel()
                await asyncio.gather(
                    input_task,
                    output_task,
                    return_exceptions=True,
                )


async def _shell(
    host_mesh_singleton: HostMesh,
    env: dict[str, str] | None,
    workdir: str | None,
) -> int:
    stdin_fd = sys.stdin.fileno()
    stdout_fd = sys.stdout.fileno()
    output_port, output_receiver = Channel[_ShellOutput].open()
    procs = host_mesh_singleton.spawn_procs()
    try:
        actor = procs.spawn("shell", _ShellActor)
        input_port = await actor.start.call_one(
            output_port,
            env,
            workdir,
            os.getcwd(),
            os.environ.get("TERM", "xterm-256color"),
            _window_size(stdin_fd),
        )
        return await _forward_terminal(
            input_port,
            output_receiver,
            stdin_fd,
            stdout_fd,
        )
    finally:
        await procs.stop()


def shell(
    host_mesh_singleton: HostMesh,
    *,
    env: dict[str, str] | None = None,
    workdir: str | None = None,
) -> int:
    """Run an interactive shell on a singleton host mesh.

    The Python actors establish two native Monarch channels, then terminal
    input and output travel directly through their Rust ports.

    Args:
        host_mesh_singleton: A HostMesh containing exactly one host.
        env: Extra environment variables for the remote shell.
        workdir: Working directory on the remote host. If omitted, the
            client's working directory is used when it exists remotely.

    Returns:
        The remote shell's exit status.
    """
    if host_mesh_singleton.size() != 1:
        raise ValueError(
            "shell requires a singleton HostMesh; slice the mesh to one host first"
        )

    return asyncio.run(_shell(host_mesh_singleton, env, workdir))
