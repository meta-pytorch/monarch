# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Python producer helpers for host-local telemetry socket ingest.

Each host's active `TelemetryActor` binds `telemetry.sock` and ingests framed
Arrow IPC batches into its tables. `encode_frame` builds one frame and
`FrameWriter` sends frames over the socket. Socket ingest accepts only tables
registered in Rust, and a batch's schema must match the registered schema
exactly.
"""

from __future__ import annotations

import os
import socket
import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


# Socket protocol limits defined by monarch_telemetry_schema in Rust.
MAX_FRAME_LEN = 16 * 1024 * 1024
MAX_TABLE_NAME_LEN = 256


def telemetry_socket_dir(apply_id: str) -> str:
    """Return the host-local telemetry socket directory for an apply id."""
    return os.path.join("/tmp", f"monarch_{apply_id}")


def telemetry_socket_path(apply_id: str) -> str:
    """Return the host-local telemetry socket path for an apply id."""
    return os.path.join(telemetry_socket_dir(apply_id), "telemetry.sock")


def encode_frame(table: str, batch: pa.RecordBatch) -> bytes:
    """Encode one batch as a telemetry socket frame.

    The frame is a `u16` big-endian table-name length, the UTF-8 table name, a
    `u32` big-endian payload length, and an Arrow IPC stream holding the batch.
    This layout must match the Rust decoder (`read_table_name` and
    `read_payload` in `monarch_distributed_telemetry/src/socket_ingest.rs`);
    change both together.
    """
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes()
    name = table.encode()
    if not name or len(name) > MAX_TABLE_NAME_LEN:
        raise ValueError("invalid telemetry table name length")
    if len(payload) > MAX_FRAME_LEN:
        raise ValueError("telemetry frame exceeds ingest limit")
    return (
        struct.pack(">H", len(name)) + name + struct.pack(">I", len(payload)) + payload
    )


class FrameWriter:
    """Unix-socket writer for telemetry frames that reconnects after errors."""

    def __init__(
        self,
        socket_path: str,
        *,
        timeout: float | None = None,
        initial_frame: bytes = b"",
    ) -> None:
        self._socket_path = socket_path
        self._sock: socket.socket | None = None
        self._timeout = timeout
        self._initial_frame = initial_frame

    def send(self, frame: bytes) -> None:
        """Send one frame, connecting first if needed.

        On `OSError` the connection is closed and the error re-raised; the next
        call reconnects. Each connection first sends `initial_frame`, if supplied.
        Calls and `close` must be serialized by the caller.
        """
        try:
            if self._sock is None:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._sock = sock
                if self._timeout is not None:
                    sock.settimeout(self._timeout)
                sock.connect(self._socket_path)
                if self._initial_frame:
                    sock.sendall(self._initial_frame)
            self._sock.sendall(frame)
        except OSError:
            self.close()
            raise

    def close(self) -> None:
        """Close the current connection; a later send will reconnect."""
        sock = self._sock
        self._sock = None
        if sock is not None:
            sock.close()
