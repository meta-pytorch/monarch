# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for the telemetry socket ingest helpers."""

import socket
import struct
from pathlib import Path

import monarch._src.job.telemetry_ingest as ti
import pyarrow as pa
import pytest

pytest_plugins = ["socket_test_utils"]


def _recv_exact(conn: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            break
        data += chunk
    return data


def test_encode_frame_layout() -> None:
    frame = ti.encode_frame("example", pa.RecordBatch.from_pylist([{"value": 1}]))
    (name_len,) = struct.unpack(">H", frame[:2])
    assert frame[2 : 2 + name_len] == b"example"
    (payload_len,) = struct.unpack(">I", frame[2 + name_len : 6 + name_len])
    payload = frame[6 + name_len :]
    assert len(payload) == payload_len
    assert pa.ipc.open_stream(payload).read_all().num_rows == 1


def test_frame_writer_reconnects(unix_socket_path: str) -> None:
    path = unix_socket_path
    writer = ti.FrameWriter(path)

    first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    first.bind(path)
    first.listen(1)
    writer.send(b"one")
    conn, _ = first.accept()
    assert _recv_exact(conn, 3) == b"one"
    conn.close()
    first.close()
    Path(path).unlink()

    # The peer and the socket path are gone; the cached connection fails.
    with pytest.raises(OSError):
        for _ in range(10):
            writer.send(b"lost")

    second = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    second.bind(path)
    second.listen(1)
    writer.send(b"two")
    conn, _ = second.accept()
    assert _recv_exact(conn, 3) == b"two"
    conn.close()
    second.close()


@pytest.mark.parametrize("table", ["", "x" * (ti.MAX_TABLE_NAME_LEN + 1)])
def test_encode_frame_rejects_invalid_table_names(table: str) -> None:
    with pytest.raises(ValueError, match="table name length"):
        ti.encode_frame(table, pa.RecordBatch.from_pylist([{"value": 1}]))


def test_encode_frame_rejects_oversized_payload(monkeypatch) -> None:
    monkeypatch.setattr(ti, "MAX_FRAME_LEN", 1)
    with pytest.raises(ValueError, match="exceeds ingest limit"):
        ti.encode_frame("example", pa.RecordBatch.from_pylist([{"value": 1}]))


def test_writer_resends_initial_frame_after_connect_failure(
    unix_socket_path: str,
) -> None:
    writer = ti.FrameWriter(unix_socket_path, timeout=1, initial_frame=b"metadata")
    with pytest.raises(OSError):
        writer.send(b"result")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(unix_socket_path)
        server.listen(1)
        writer.send(b"result")
        connection, _ = server.accept()
        with connection:
            assert _recv_exact(connection, 14) == b"metadataresult"
        writer.close()
        writer.send(b"second")
        connection, _ = server.accept()
        with connection:
            assert _recv_exact(connection, 14) == b"metadatasecond"
    finally:
        writer.close()
        server.close()


def test_frame_writer_import_does_not_require_arrow(monkeypatch) -> None:
    import importlib
    import sys

    monkeypatch.setitem(sys.modules, "pyarrow", None)
    reloaded = importlib.reload(ti)
    writer = reloaded.FrameWriter("/tmp/unused-telemetry.sock")
    assert writer._sock is None
