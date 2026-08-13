# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for point-to-point TCP port forwarding."""

import asyncio
import socket
import time

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import ActorError, ProcMesh, this_host, this_proc
from monarch.job import PortForwarder

_HOST = "127.0.0.1"


def _unused_port() -> int:
    return _unused_ports(1)[0]


def _unused_ports(count: int) -> list[int]:
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind((_HOST, 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


async def _echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def _round_trip(port: int, payload: bytes) -> bytes:
    reader, writer = await asyncio.open_connection(_HOST, port)
    try:
        writer.write(payload)
        writer.write_eof()
        await writer.drain()
        response = await reader.readexactly(len(payload))
        assert await reader.read() == b""
        return response
    finally:
        writer.close()
        await writer.wait_closed()


async def _read_identity(port: int) -> bytes:
    reader, writer = await asyncio.open_connection(_HOST, port)
    try:
        return await reader.readexactly(1)
    finally:
        writer.close()
        await writer.wait_closed()


async def _forward_between_ranks(
    proc_mesh: ProcMesh, destination_port: int
) -> tuple[int, PortForwarder]:
    source_port = _unused_port()
    forwarder = this_proc().spawn("port_forwarder", PortForwarder)
    await forwarder.forward.call_one(
        proc_mesh.slice(procs=0),
        destination_port,
        proc_mesh.slice(procs=1),
        f"{_HOST}:{source_port}",
    )
    return source_port, forwarder


async def _cleanup(
    forwarder: PortForwarder,
    proc_mesh: ProcMesh,
    *servers: asyncio.AbstractServer,
) -> None:
    await forwarder.stop()
    for server in servers:
        server.close()
        await server.wait_closed()
    await proc_mesh.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_round_trip_and_stop() -> None:
    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    server = await asyncio.start_server(_echo, _HOST, 0)
    destination_port = server.sockets[0].getsockname()[1]
    source_port, forwarder = await _forward_between_ranks(proc_mesh, destination_port)
    try:
        payload = bytes(range(256)) * 1024
        assert await _round_trip(source_port, payload) == payload

        await forwarder.stop()
        await forwarder.stop()
        with pytest.raises(OSError):
            await asyncio.open_connection(_HOST, source_port)
    finally:
        await _cleanup(forwarder, proc_mesh, server)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forwarder_owns_multiple_forwards() -> None:
    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    server = await asyncio.start_server(_echo, _HOST, 0)
    destination_port = server.sockets[0].getsockname()[1]
    source_ports = _unused_ports(2)
    forwarder = this_proc().spawn("port_forwarder", PortForwarder)
    try:
        for source_port in source_ports:
            await forwarder.forward.call_one(
                proc_mesh.slice(procs=0),
                destination_port,
                proc_mesh.slice(procs=1),
                f"{_HOST}:{source_port}",
            )
        for source_port in source_ports:
            assert await _round_trip(source_port, b"hello") == b"hello"

        await forwarder.stop()
        for source_port in source_ports:
            with pytest.raises(OSError):
                await asyncio.open_connection(_HOST, source_port)
    finally:
        await _cleanup(forwarder, proc_mesh, server)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_setup_failure_closes_partial_meshes() -> None:
    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    destination_port, *source_ports = _unused_ports(3)
    blocker = socket.socket()
    blocker.bind((_HOST, source_ports[1]))
    blocker.listen()
    forwarder = this_proc().spawn("port_forwarder", PortForwarder)
    try:
        try:
            with pytest.raises(ActorError):
                await forwarder.forward.call_one(
                    proc_mesh,
                    destination_port,
                    proc_mesh,
                    lambda point: f"{_HOST}:{source_ports[point['procs']]}",
                )
        finally:
            blocker.close()

        for source_port in source_ports:
            with socket.socket() as probe:
                probe.bind((_HOST, source_port))
    finally:
        await _cleanup(forwarder, proc_mesh)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_waits_for_destination_initialization() -> None:
    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    destination_port, *source_ports = _unused_ports(3)
    forwarder = this_proc().spawn("port_forwarder", PortForwarder)
    try:
        with pytest.raises(ActorError):
            await forwarder.forward.call_one(
                proc_mesh,
                lambda point: destination_port if point["procs"] == 0 else 0,
                proc_mesh,
                lambda point: f"{_HOST}:{source_ports[point['procs']]}",
            )

        for source_port in source_ports:
            with socket.socket() as probe:
                probe.bind((_HOST, source_port))
    finally:
        await _cleanup(forwarder, proc_mesh)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_multi_rank_concurrent_connections() -> None:
    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    source_ports = _unused_ports(2)

    async def identify(
        rank: int, _reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.write(bytes([rank]))
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    servers = [
        await asyncio.start_server(
            lambda reader, writer, rank=rank: identify(rank, reader, writer),
            _HOST,
            0,
        )
        for rank in range(2)
    ]
    destination_ports = [server.sockets[0].getsockname()[1] for server in servers]
    forwarder = this_proc().spawn("port_forwarder", PortForwarder)
    await forwarder.forward.call_one(
        proc_mesh,
        lambda point: destination_ports[point["procs"]],
        proc_mesh,
        lambda point: f"{_HOST}:{source_ports[point['procs']]}",
    )
    try:
        responses = await asyncio.gather(
            *(_read_identity(source_ports[rank]) for rank in range(2) for _ in range(4))
        )
        assert responses == [b"\x00"] * 4 + [b"\x01"] * 4
    finally:
        await _cleanup(forwarder, proc_mesh, *servers)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_shared_source_address() -> None:
    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    server = await asyncio.start_server(_echo, _HOST, 0)
    source_port = _unused_port()
    forwarder = this_proc().spawn("port_forwarder", PortForwarder)
    await forwarder.forward.call_one(
        proc_mesh,
        server.sockets[0].getsockname()[1],
        proc_mesh,
        f"{_HOST}:{source_port}",
    )
    try:
        payloads = [bytes([index]) * (64 * 1024) for index in range(1, 9)]
        assert (
            await asyncio.gather(
                *(_round_trip(source_port, payload) for payload in payloads)
            )
            == payloads
        )
    finally:
        await _cleanup(forwarder, proc_mesh, server)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_preserves_data_under_backpressure() -> None:
    received: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def slow_sink(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        total = 0
        while data := await reader.read(64 * 1024):
            total += len(data)
            await asyncio.sleep(0.002)
        received.set_result(total)
        writer.close()
        await writer.wait_closed()

    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    server = await asyncio.start_server(slow_sink, _HOST, 0)
    source_port, forwarder = await _forward_between_ranks(
        proc_mesh, server.sockets[0].getsockname()[1]
    )
    try:
        reader, writer = await asyncio.open_connection(_HOST, source_port)
        payload_size = 32 * 1024 * 1024
        writer.write(b"x" * payload_size)
        writer.write_eof()
        await writer.drain()
        assert await asyncio.wait_for(received, timeout=30) == payload_size
        assert await reader.read() == b""
        writer.close()
        await writer.wait_closed()
    finally:
        await _cleanup(forwarder, proc_mesh, server)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_propagates_abrupt_disconnect() -> None:
    connected = asyncio.Event()
    disconnected = asyncio.Event()

    async def stream(
        _reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        connected.set()
        try:
            while True:
                writer.write(b"x" * (64 * 1024))
                await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()
            disconnected.set()

    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    server = await asyncio.start_server(stream, _HOST, 0)
    source_port, forwarder = await _forward_between_ranks(
        proc_mesh, server.sockets[0].getsockname()[1]
    )
    try:
        _reader, writer = await asyncio.open_connection(_HOST, source_port)
        await asyncio.wait_for(connected.wait(), timeout=5)
        writer.transport.abort()
        await asyncio.wait_for(disconnected.wait(), timeout=5)
    finally:
        await _cleanup(forwarder, proc_mesh, server)


@pytest.mark.timeout(30)
@isolate_in_subprocess
async def test_port_forward_stop_with_backpressured_connection() -> None:
    connected = asyncio.Event()
    release = asyncio.Event()

    async def do_not_read(
        _reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        connected.set()
        await release.wait()
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass

    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    server = await asyncio.start_server(do_not_read, _HOST, 0)
    source_port, forwarder = await _forward_between_ranks(
        proc_mesh, server.sockets[0].getsockname()[1]
    )
    _reader, writer = await asyncio.open_connection(_HOST, source_port)
    drain_task: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(connected.wait(), timeout=5)
        writer.write(b"x" * (16 * 1024 * 1024))
        drain_task = asyncio.create_task(writer.drain())
        await asyncio.sleep(0.5)
        started = time.monotonic()
        await asyncio.wait_for(forwarder.stop().as_asyncio(), timeout=10)
        assert time.monotonic() - started < 10
    finally:
        release.set()
        if drain_task is not None:
            drain_task.cancel()
            await asyncio.gather(drain_task, return_exceptions=True)
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
        await _cleanup(forwarder, proc_mesh, server)


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_port_forward_rejects_different_shapes() -> None:
    proc_mesh = this_host().spawn_procs(per_host={"procs": 2})
    forwarder = this_proc().spawn("port_forwarder", PortForwarder)
    try:
        with pytest.raises(ActorError, match="must have the same shape"):
            await forwarder.forward.call_one(
                proc_mesh, 1234, proc_mesh.slice(procs=0), f"{_HOST}:5678"
            )
    finally:
        await forwarder.stop()
        await proc_mesh.stop()
