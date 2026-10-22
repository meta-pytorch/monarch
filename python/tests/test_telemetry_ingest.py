# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Unit tests for the telemetry socket ingest helpers."""

import socket
import struct

import monarch._src.job.telemetry_ingest as ti
import pyarrow as pa
import pytest


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


def test_frame_writer_reconnects(tmp_path) -> None:
    path = str(tmp_path / "t.sock")
    writer = ti.FrameWriter(path)

    first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    first.bind(path)
    first.listen(1)
    writer.send(b"one")
    conn, _ = first.accept()
    assert _recv_exact(conn, 3) == b"one"
    conn.close()
    first.close()
    (tmp_path / "t.sock").unlink()

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
