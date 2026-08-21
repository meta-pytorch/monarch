# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Point-to-point TCP port forwarding between process meshes."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from typing import NamedTuple

from monarch.actor import (
    Actor,
    ActorError,
    Channel,
    current_rank,
    endpoint,
    Point,
    Port,
    PortReceiver,
    ProcMesh,
)

_HOST = "127.0.0.1"
_READ_SIZE = 64 * 1024
_CLOSE_TIMEOUT = 1

_Port = int | Callable[[Point], int]
_Address = str | Callable[[Point], str]


class _Data(NamedTuple):
    data: bytes


class _Eof(NamedTuple):
    pass


class _Abort(NamedTuple):
    pass


_Message = _Data | _Eof | _Abort


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=_CLOSE_TIMEOUT)
    except OSError:
        writer.transport.abort()


def _send_abort(channel: Port[_Message]) -> None:
    try:
        channel.send(_Abort())
    except RuntimeError:
        pass


async def _send_socket(reader: asyncio.StreamReader, channel: Port[_Message]) -> None:
    while data := await reader.read(_READ_SIZE):
        channel.send(_Data(data))
    channel.send(_Eof())


async def _receive_channel(
    channel: PortReceiver[_Message],
    writer: asyncio.StreamWriter,
    received_eof: asyncio.Event,
) -> None:
    while True:
        message = await channel.recv()
        if isinstance(message, _Data):
            writer.write(message.data)
            await writer.drain()
        elif isinstance(message, _Eof):
            if not received_eof.is_set():
                if writer.can_write_eof():
                    writer.write_eof()
                    await writer.drain()
                else:
                    writer.close()
                received_eof.set()
        elif isinstance(message, _Abort):
            writer.transport.abort()
            raise ConnectionAbortedError
        else:
            raise RuntimeError("received unexpected port-forward message")


async def _wait_for_eofs(
    send_task: asyncio.Task[None], received_eof: asyncio.Event
) -> None:
    await send_task
    await received_eof.wait()


async def _relay_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    send: Port[_Message],
    receive: PortReceiver[_Message],
) -> None:
    received_eof = asyncio.Event()
    send_task = asyncio.create_task(_send_socket(reader, send))
    receive_task = asyncio.create_task(_receive_channel(receive, writer, received_eof))
    closed_task = asyncio.create_task(_wait_for_eofs(send_task, received_eof))
    tasks = [
        send_task,
        receive_task,
        closed_task,
    ]
    try:
        done, _ = await asyncio.wait(
            (receive_task, closed_task), return_when=asyncio.FIRST_COMPLETED
        )
        await (receive_task if receive_task in done else closed_task)
    except asyncio.CancelledError:
        _send_abort(send)
        writer.transport.abort()
        raise
    except (ActorError, OSError, RuntimeError):
        _send_abort(send)
        writer.transport.abort()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await _close_writer(writer)


class _Connections:
    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[None]] = set()

    def add(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        send: Port[_Message],
        receive: PortReceiver[_Message],
    ) -> None:
        task = asyncio.create_task(_relay_connection(reader, writer, send, receive))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()


class _DestinationPortForwardActor(Actor):
    def __init__(self) -> None:
        self._destination_port: int | None = None
        self._connections = _Connections()

    @endpoint
    async def configure(self, destination_port: _Port) -> None:
        self._destination_port = _resolve_port(
            "dst_port", destination_port, current_rank()
        )

    @endpoint
    async def open(self, send: Port[_Message]) -> Port[_Message]:
        destination_port = self._destination_port
        if destination_port is None:
            raise RuntimeError("port forward destination is not configured")
        reader, writer = await asyncio.open_connection(_HOST, destination_port)
        try:
            source_send, receive = Channel[_Message].open()
            self._connections.add(reader, writer, send, receive)
            return source_send
        except (Exception, asyncio.CancelledError):
            await _close_writer(writer)
            raise

    async def __cleanup__(self, exc: Exception | None) -> None:
        try:
            await self._connections.close()
        except Exception:
            if exc is None:
                raise


class _SourcePortForwardActor(Actor):
    def __init__(self) -> None:
        self._destination: _DestinationPortForwardActor | None = None
        self._connections = _Connections()
        self._server: asyncio.AbstractServer | None = None

    @endpoint
    async def start(
        self,
        destination: _DestinationPortForwardActor,
        source_address: _Address,
    ) -> None:
        self._destination = destination.slice(**dict(current_rank()))
        address, port = _resolve_address(source_address, current_rank())
        self._server = await asyncio.start_server(
            self._accept, address, port, reuse_port=True
        )

    async def _accept(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        destination = self._destination
        if destination is None:
            await _close_writer(writer)
            return
        try:
            send, receive = Channel[_Message].open()
            source_send = await destination.open.call_one(send)
            self._connections.add(reader, writer, source_send, receive)
        except (Exception, asyncio.CancelledError):
            await _close_writer(writer)
            raise

    async def _shutdown(self) -> None:
        failure: Exception | None = None
        server, self._server = self._server, None
        if server is not None:
            try:
                server.close()
                await server.wait_closed()
            except Exception as error:
                failure = error
        try:
            await self._connections.close()
        except Exception as error:
            if failure is None:
                failure = error
        if failure is not None:
            raise failure

    async def __cleanup__(self, exc: Exception | None) -> None:
        try:
            await self._shutdown()
        except Exception:
            if exc is None:
                raise


async def _stop_forward_actors(
    source: _SourcePortForwardActor | None,
    destination: _DestinationPortForwardActor | None,
) -> list[BaseException]:
    stops = [
        actor.stop().as_asyncio()
        for actor in (source, destination)
        if actor is not None
    ]
    results = await asyncio.gather(*stops, return_exceptions=True)
    return [result for result in results if isinstance(result, BaseException)]


class PortForwarder(Actor):
    """Local actor that owns point-to-point port forwards.

    Spawn one on the local process, call :meth:`forward` for each desired
    forward, and stop the actor to close every forward it started.
    """

    @endpoint
    async def forward(
        self,
        dst_proc_mesh: ProcMesh,
        dst_port: int | Callable[[Point], int],
        src_proc_mesh: ProcMesh,
        src_address: str | Callable[[Point], str],
    ) -> None:
        """Start a pointwise TCP port forward between two process meshes.

        The process meshes must have the same dimension labels and sizes. Each
        source process listens on the ``"address:port"`` returned by
        ``src_address`` and forwards connections to ``127.0.0.1:dst_port`` on
        the destination process at the same mesh coordinate. ``dst_port`` and
        ``src_address`` may be functions of the corresponding actor's
        :class:`~monarch.actor.Point`.

        Args:
            dst_proc_mesh: Processes that can reach the destination service.
            dst_port: Loopback TCP port of the destination service, or a function
                that returns one for each destination point.
            src_proc_mesh: Processes that expose the forwarded port.
            src_address: ``"address:port"`` on which each source process listens,
                or a function that returns one for each source point.

        Raises:
            ActorError: Wrapping a ``ValueError`` if the mesh shapes differ or a
                static address or port is invalid. Per-point values are validated
                on their respective actors.
        """
        if dst_proc_mesh.extent != src_proc_mesh.extent:
            raise ValueError(
                "destination and source process meshes must have the same shape: "
                f"{dst_proc_mesh.extent} != {src_proc_mesh.extent}"
            )
        if not callable(dst_port):
            _validate_port("dst_port", dst_port)
        if not callable(src_address):
            _parse_address(src_address)

        source: _SourcePortForwardActor | None = None
        destination: _DestinationPortForwardActor | None = None
        try:
            suffix = uuid.uuid4().hex
            source = src_proc_mesh.spawn(
                f"port_forward_source_{suffix}", _SourcePortForwardActor
            )
            destination = dst_proc_mesh.spawn(
                f"port_forward_destination_{suffix}",
                _DestinationPortForwardActor,
            )
            await destination.configure.call(dst_port)
            await source.start.call(destination, src_address)
        except Exception as setup_error:
            cleanup_failures = await _stop_forward_actors(source, destination)
            if cleanup_failures:
                details = "; ".join(
                    f"{type(failure).__name__}: {failure}"
                    for failure in cleanup_failures
                )
                setup_error.add_note(f"port-forward setup cleanup failed: {details}")
            raise


def _validate_port(name: str, port: int) -> None:
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError(f"{name} must be an integer from 1 through 65535")


def _resolve_port(name: str, port: _Port, point: Point) -> int:
    resolved = port(point) if callable(port) else port
    _validate_port(name, resolved)
    return resolved


def _parse_address(address: str) -> tuple[str, int]:
    if not isinstance(address, str):
        raise ValueError("src_address must resolve to a string in 'address:port' form")
    host, separator, port_text = address.rpartition(":")
    if not separator or not host:
        raise ValueError("src_address must use 'address:port' form")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        port = int(port_text)
    except ValueError as error:
        raise ValueError("src_address port must be an integer") from error
    _validate_port("src_address port", port)
    return host, port


def _resolve_address(address: _Address, point: Point) -> tuple[str, int]:
    return _parse_address(address(point) if callable(address) else address)
