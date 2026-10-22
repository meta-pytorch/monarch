# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Python producer helpers for host-local telemetry socket ingest.

Each host's active `TelemetryActor` binds `telemetry.sock` and ingests framed
Arrow IPC batches into its tables. `encode_frame` builds one frame and
`FrameWriter` sends frames over the socket. Socket ingest accepts only tables
registered in Rust, and a batch's schema must match the registered schema
exactly.
"""

from __future__ import annotations

import socket
import struct

import pyarrow as pa


def encode_frame(table: str, batch: pa.RecordBatch) -> bytes:
    """Encode one batch as a telemetry socket frame.

    The frame is a `u16` big-endian table-name length, the UTF-8 table name, a
    `u32` big-endian payload length, and an Arrow IPC stream holding the batch.
    This layout must match the Rust decoder (`read_table_name` and
    `read_payload` in `monarch_distributed_telemetry/src/socket_ingest.rs`);
    change both together.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes()
    name = table.encode()
    return (
        struct.pack(">H", len(name)) + name + struct.pack(">I", len(payload)) + payload
    )


class FrameWriter:
    """Unix-socket writer for telemetry frames that reconnects after errors."""

    def __init__(self, socket_path: str) -> None:
        self._socket_path = socket_path
        self._sock: socket.socket | None = None

    def send(self, frame: bytes) -> None:
        """Send one frame, connecting first if needed.

        On `OSError` the connection is closed and the error re-raised; the next
        call reconnects.
        """
        try:
            if self._sock is None:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._sock = sock
                sock.connect(self._socket_path)
            self._sock.sendall(frame)
        except OSError:
            self._close()
            raise

    def _close(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            sock.close()
