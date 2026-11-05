# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Command outcomes emitted through distributed telemetry's Arrow IPC socket.

One invocation groups all meshes/ranks. Metadata and compact outcomes remain
for the sidecar lifetime. Terminal outcomes remain history rather than actor
liveness. Reporting is best effort and cannot change command exit codes.
Command arguments are retained as plaintext; pass credentials through environment
variables or files instead of arguments.
"""

from __future__ import annotations

import hashlib
import json
import logging
import queue
import re
import shlex
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from monarch._src.job import execution_config
from monarch._src.job.execution_config import normalize_exit_code
from monarch._src.job.telemetry_ingest import (
    encode_frame,
    FrameWriter,
    telemetry_socket_path,
)

logger: logging.Logger = logging.getLogger(__name__)
HEARTBEAT_SECS = execution_config.HEARTBEAT_SECS
HEARTBEAT_TIMEOUT_US = execution_config.HEARTBEAT_TIMEOUT_US


def execution_schemas() -> dict[str, Any]:
    """Arrow schemas matching monarch_telemetry_schema::execution_tables."""
    import pyarrow as pa

    def field(name: str, type_: Any, nullable: bool = False) -> Any:
        return pa.field(name, type_, nullable=nullable)

    return {
        "executions": pa.schema(
            [
                field("id", pa.string()),
                field("timestamp_us", pa.int64()),
                field("command", pa.string()),
                field("workdir", pa.string(), True),
                field("output_dir", pa.string(), True),
            ]
        ),
        "execution_status_events": pa.schema(
            [
                field("execution_id", pa.string()),
                field("timestamp_us", pa.int64()),
                field("status", pa.string()),
                field("returncode", pa.int32(), True),
                field("error", pa.string(), True),
            ]
        ),
        "execution_rank_results": pa.schema(
            [
                field("id", pa.string()),
                field("execution_id", pa.string()),
                field("timestamp_us", pa.int64()),
                field("mesh", pa.string()),
                field("rank", pa.string()),
                field("flat_rank", pa.int64(), True),
                field("coordinates", pa.string(), True),
                field("host_rank", pa.int64(), True),
                field("returncode", pa.int32()),
            ]
        ),
    }


class ExecutionRecorder:
    """Best-effort recorder for one CLI command invocation.

    Delivery is lossy, like the other telemetry producers: one daemon thread
    sends heartbeats and queued rank frames, and a frame that cannot be queued
    or sent is dropped and counted. The next send reconnects and resends the
    command metadata. Socket writes are not collector acknowledgments.
    Command outcomes describe observed process exits, independently of
    rank-frame delivery; missing rank telemetry is reported in the final status.
    """

    _QUEUE_LIMIT = 4096
    _FINISH_TIMEOUT_SECS = 2.0

    def __init__(
        self,
        apply_id: Any,
        cmd: list[str],
        workdir: str | None,
        output_dir: str | None,
        script: str | None = None,
        script_text: str | None = None,
    ) -> None:
        self.id: str | None = None
        self._writer: FrameWriter | None = None
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._stop = threading.Event()
        self._reporter: threading.Thread | None = None
        self._queue: queue.Queue[bytes | None] = queue.Queue(self._QUEUE_LIMIT)
        self._observed_rank_count = 0
        self._dropped_ranks = 0
        self._finished = False
        self._terminal: tuple[str, int | None, str | None] | None = None
        self._warned = False
        if not isinstance(apply_id, str) or not re.fullmatch(
            r"[a-zA-Z0-9_-]+", apply_id
        ):
            return
        self._path = Path(telemetry_socket_path(apply_id))
        if not self._path.exists():
            return
        try:
            self._schemas = execution_schemas()
            self.id = uuid.uuid4().hex
            command = shlex.join(cmd)
            if script is not None:
                digest = hashlib.sha256((script_text or "").encode()).hexdigest()
                command = f"bash --script {shlex.quote(script)} [sha256:{digest}]"
            metadata = self._frame(
                "executions",
                {
                    "id": self.id,
                    "timestamp_us": time.time_ns() // 1000,
                    "command": command,
                    "workdir": workdir,
                    "output_dir": output_dir,
                },
            )
            self._writer = FrameWriter(
                str(self._path), timeout=1, initial_frame=metadata
            )
            self._reporter = threading.Thread(
                target=self._run, name="monarch-execution-reporter", daemon=True
            )
            self._reporter.start()
        except Exception:
            self.id = None
            self._disable()
            logger.warning("Could not initialize execution recording", exc_info=True)

    def _frame(self, table: str, row: dict[str, Any]) -> bytes:
        import pyarrow as pa

        batch = pa.RecordBatch.from_pylist([row], schema=self._schemas[table])
        return encode_frame(table, batch)

    def _send(self, frame: bytes) -> bool:
        writer = self._writer
        if writer is None:
            return False
        try:
            writer.send(frame)
            return True
        except OSError:
            if not self._warned:
                self._warned = True
                logger.warning(
                    "Execution telemetry unavailable; dropping frames until the "
                    "collector accepts connections",
                    exc_info=True,
                )
            return False

    def _drop_rank(self) -> None:
        with self._lock:
            self._dropped_ranks += 1

    def _send_rank(self, frame: bytes | None) -> None:
        if frame is not None and not self._send(frame):
            self._drop_rank()

    def _status(
        self, status: str, returncode: int | None = None, error: str | None = None
    ) -> None:
        self._send(
            self._frame(
                "execution_status_events",
                {
                    "execution_id": self.id,
                    "timestamp_us": time.time_ns() // 1000,
                    "status": status,
                    "returncode": returncode,
                    "error": error,
                },
            )
        )

    def _disable(self) -> None:
        self._done.set()
        self._stop.set()
        if self._reporter is not None and self._reporter.ident is not None:
            self._reporter.join(timeout=self._FINISH_TIMEOUT_SECS)

    def _report_running(self) -> None:
        try:
            self._status("running")
        except Exception:
            logger.warning("Execution heartbeat was not recorded", exc_info=True)

    def _run(self) -> None:
        try:
            self._report_running()
            next_heartbeat = time.monotonic() + HEARTBEAT_SECS
            while not self._done.is_set():
                try:
                    timeout = max(0.0, next_heartbeat - time.monotonic())
                    self._send_rank(self._queue.get(timeout=timeout))
                except queue.Empty:
                    pass
                if not self._done.is_set() and time.monotonic() >= next_heartbeat:
                    self._report_running()
                    next_heartbeat = time.monotonic() + HEARTBEAT_SECS
        except Exception:
            logger.warning("Execution reporting stopped", exc_info=True)
        finally:
            try:
                if self._terminal is not None:
                    self._complete()
            except Exception:
                logger.warning("Execution completion was not recorded", exc_info=True)
            finally:
                if self._writer is not None:
                    self._writer.close()

    def record_rank(self, mesh: str, rank: Any, result: dict[str, Any]) -> None:
        if self.id is None:
            return
        with self._lock:
            if self._finished:
                return
            self._observed_rank_count += 1
        try:
            identity = rank if isinstance(rank, dict) and "flat_rank" in rank else {}
            coordinates = identity.get("coordinates")
            frame = self._frame(
                "execution_rank_results",
                {
                    "id": uuid.uuid4().hex,
                    "execution_id": self.id,
                    "timestamp_us": time.time_ns() // 1000,
                    "mesh": mesh,
                    "rank": str(identity.get("label", rank)),
                    "flat_rank": identity.get("flat_rank"),
                    "coordinates": json.dumps(coordinates, sort_keys=True)
                    if coordinates is not None
                    else None,
                    "host_rank": identity.get("host_rank"),
                    "returncode": normalize_exit_code(int(result.get("returncode", 1))),
                },
            )
            self._queue.put_nowait(frame)
        except queue.Full:
            self._drop_rank()
        except Exception:
            self._drop_rank()
            logger.warning("Execution rank outcome was not recorded", exc_info=True)

    def finish(
        self, returncode: int | None, error: BaseException | None = None
    ) -> None:
        """Report the CLI outcome with a bounded wait for the sender thread."""
        try:
            with self._lock:
                if self.id is None or self._finished:
                    return
                # An aggregate zero without observed process results is not an actual exit.
                status = (
                    (
                        "interrupted"
                        if isinstance(error, (KeyboardInterrupt, SystemExit))
                        else "failed"
                    )
                    if error
                    else "unknown"
                    if not self._observed_rank_count
                    else "succeeded"
                    if returncode == 0
                    else "failed"
                )
                message = f"{type(error).__name__}: {error}"[:4096] if error else None
                self._terminal = (status, returncode, message)
                self._finished = True
            self._done.set()
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                pass
            if self._reporter is not None:
                self._reporter.join(timeout=self._FINISH_TIMEOUT_SECS)
                self._stop.set()
        except Exception:
            self._disable()
            logger.warning("Execution completion was not recorded", exc_info=True)

    def _complete(self) -> None:
        unsent = 0
        while True:
            try:
                frame = self._queue.get_nowait()
            except queue.Empty:
                break
            if frame is None:
                continue
            if self._stop.is_set():
                unsent += 1
            else:
                self._send_rank(frame)
        assert self._terminal is not None
        status, returncode, message = self._terminal
        with self._lock:
            missing = self._dropped_ranks + unsent
        if missing:
            message = ((message + "; ") if message else "") + (
                f"Telemetry incomplete: {missing} rank reports unavailable"
            )
        self._status(status, returncode, message)
