# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Exercise real Arrow socket framing and execution reporting failure paths."""

import shutil
import socket
import struct
import threading
import time
import uuid
from pathlib import Path

import pyarrow as pa
import pytest
from monarch._src.job import execution_history as history, telemetry_ingest as ingest


def wait_until(predicate):
    deadline = time.monotonic() + 3
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.01)


@pytest.fixture
def collector():
    apply_id = uuid.uuid4().hex
    root = Path("/tmp") / f"monarch_{apply_id}"
    root.mkdir(mode=0o700)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(root / "telemetry.sock"))
    server.listen()
    frames = []
    done = threading.Event()

    def read_exact(stream, count):
        chunks = bytearray()
        while len(chunks) < count:
            data = stream.recv(count - len(chunks))
            if not data:
                return None
            chunks.extend(data)
        return bytes(chunks)

    def read():
        try:
            connection, _ = server.accept()
            with connection:
                while True:
                    length = read_exact(connection, 2)
                    if length is None:
                        break
                    table = read_exact(
                        connection, struct.unpack("!H", length)[0]
                    ).decode()
                    size = struct.unpack("!I", read_exact(connection, 4))[0]
                    payload = read_exact(connection, size)
                    reader = pa.ipc.open_stream(payload)
                    batches = list(reader)
                    assert len(batches) == 1
                    frames.append((table, batches[0]))
        finally:
            done.set()

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    yield apply_id, frames, done
    server.close()
    shutil.rmtree(root)


def test_arrow_frames_capture_rank_failure_and_finish_after_heartbeats(
    collector, monkeypatch
):
    apply_id, frames, done = collector
    monkeypatch.setattr(history, "HEARTBEAT_SECS", 0.01)
    recorder = history.ExecutionRecorder(apply_id, ["python", "train.py"], None, None)
    recorder.record_rank(
        "workers",
        {"host": 0, "gpu": 0},
        {"returncode": 0, "stdout": "hello", "stderr": ""},
    )
    recorder.record_rank(
        "workers",
        {"host": 0, "gpu": 1},
        {"returncode": -9, "stdout": "", "stderr": "failed"},
    )
    time.sleep(0.04)
    recorder.finish(137)
    recorder.finish(0)
    assert done.wait(3)
    events = [
        row
        for table, batch in frames
        if table == "execution_status_events"
        for row in batch.to_pylist()
    ]
    assert len(events) >= 3
    assert events[-1]["status"] == "failed"
    assert events[-1]["returncode"] == 137
    assert all(row["status"] == "running" for row in events[:-1])
    ranks = [
        row
        for table, batch in frames
        if table == "execution_rank_results"
        for row in batch.to_pylist()
    ]
    assert [r["returncode"] for r in ranks] == [0, 137]
    assert {row["execution_id"] for row in ranks} == {recorder.id}


def test_repeated_finish_preserves_the_terminal_result(collector, monkeypatch):
    apply_id, frames, done = collector
    monkeypatch.setattr(history, "HEARTBEAT_SECS", 3600)
    recorder = history.ExecutionRecorder(apply_id, ["true"], None, None)
    recorder.record_rank("workers", "rank0", {"returncode": 0})
    recorder.finish(0)
    recorder.finish(None, KeyboardInterrupt())
    assert done.wait(3)
    statuses = [
        row["status"]
        for table, batch in frames
        if table == "execution_status_events"
        for row in batch.to_pylist()
    ]
    assert statuses == ["running", "succeeded"]


def test_late_rank_after_interrupt_does_not_reopen_recording(collector):
    apply_id, frames, done = collector
    recorder = history.ExecutionRecorder(apply_id, ["sleep", "10"], None, None)
    recorder.finish(None, KeyboardInterrupt())
    assert done.wait(3)
    recorder.record_rank("workers", "rank0", {"returncode": 0})
    assert recorder._writer._sock is None
    assert recorder._observed_rank_count == 0
    assert recorder._queue.empty()
    assert all(table != "execution_rank_results" for table, _ in frames)


def test_aggregate_zero_without_process_results_never_claims_success(collector):
    from unittest.mock import MagicMock

    from monarch._src.job import job

    apply_id, frames, done = collector
    recorder = history.ExecutionRecorder(apply_id, ["true"], None, None)
    hosts = MagicMock()
    procs = hosts.spawn_procs.return_value
    procs.spawn.return_value.run.call.return_value.get.return_value = []
    result = job.exec_command(
        hosts,
        ["true"],
        _result_callback=lambda rank, result: recorder.record_rank(
            "workers", rank, result
        ),
    ).get()
    assert result == 0  # Aggregate default, not an observed process exit.
    procs.stop.assert_called_once()
    recorder.finish(result)
    assert done.wait(3)
    assert frames[-1][1].to_pylist()[0]["status"] == "unknown"
    assert not any(table == "execution_rank_results" for table, _ in frames)


@pytest.mark.parametrize("returncode,status", [(0, "succeeded"), (7, "failed")])
def test_observed_exit_survives_loss_of_all_rank_telemetry(
    collector, monkeypatch, returncode, status
):
    apply_id, frames, done = collector
    recorder = history.ExecutionRecorder(apply_id, ["command"], None, None)
    send = recorder._send

    def drop_rank(frame):
        size = struct.unpack("!H", frame[:2])[0]
        if frame[2 : 2 + size] == b"execution_rank_results":
            return False
        return send(frame)

    monkeypatch.setattr(recorder, "_send", drop_rank)
    recorder.record_rank("workers", "rank0", {"returncode": returncode})
    recorder.finish(returncode)
    assert done.wait(3)
    assert not any(table == "execution_rank_results" for table, _ in frames)
    outcome = frames[-1][1].to_pylist()[0]
    assert outcome["status"] == status
    assert outcome["returncode"] == returncode
    assert outcome["error"] == "Telemetry incomplete: 1 rank reports unavailable"


@pytest.mark.parametrize(
    "error,status",
    [(ValueError("failed"), "failed"), (KeyboardInterrupt(), "interrupted")],
)
def test_exception_outcome_is_retained_without_raising(collector, error, status):
    apply_id, frames, done = collector
    recorder = history.ExecutionRecorder(apply_id, ["python", "train.py"], None, None)
    recorder.finish(None, error)
    assert done.wait(3)
    assert frames[-1][1].to_pylist()[0]["status"] == status


def test_rank_telemetry_excludes_output_and_keeps_script_identity(collector):
    apply_id, frames, done = collector
    recorder = history.ExecutionRecorder(
        apply_id,
        ["bash", "-c", "script body"],
        None,
        None,
        script="workspace/train.sh",
        script_text="script body",
    )
    recorder.record_rank(
        "workers",
        "rank0",
        {"returncode": 0, "stdout": "x" * 100000, "stderr": "😀" * 20000},
    )
    recorder.finish(0)
    assert done.wait(3)
    import hashlib

    assert (
        frames[0][1].to_pylist()[0]["command"]
        == "bash --script workspace/train.sh [sha256:"
        + hashlib.sha256(b"script body").hexdigest()
        + "]"
    )
    result = next(
        batch.to_pylist()[0]
        for table, batch in frames
        if table == "execution_rank_results"
    )
    assert result["returncode"] == 0
    assert set(result) == {
        "id",
        "execution_id",
        "timestamp_us",
        "mesh",
        "rank",
        "flat_rank",
        "coordinates",
        "host_rank",
        "returncode",
    }


def test_disabled_or_invalid_telemetry_does_not_add_services():
    for apply_id in [None, "../invalid", uuid.uuid4().hex]:
        recorder = history.ExecutionRecorder(apply_id, ["ls"], None, None)
        assert recorder.id is None
        recorder.record_rank("workers", "rank0", {"returncode": 0})
        recorder.finish(0)


@pytest.mark.parametrize("coordinates", [None, {}, {"hosts": 0}])
def test_partial_rank_identity_keeps_outcome_and_available_coordinates(
    collector, coordinates
):
    apply_id, frames, done = collector
    recorder = history.ExecutionRecorder(apply_id, ["command"], None, None)
    identity = {"flat_rank": 0}
    if coordinates is not None:
        identity["coordinates"] = coordinates
    recorder.record_rank("workers", identity, {"returncode": 7, "stdout": "result"})
    recorder.finish(7)
    assert done.wait(3)
    rank = next(
        batch.to_pylist()[0]
        for table, batch in frames
        if table == "execution_rank_results"
    )
    import json

    assert rank["flat_rank"] == 0
    assert rank["returncode"] == 7
    assert rank["coordinates"] == (
        json.dumps(coordinates) if coordinates is not None else None
    )
    assert frames[-1][1].to_pylist()[0]["error"] is None


def test_collector_connection_failure_keeps_reporting_optional(monkeypatch):
    from unittest.mock import MagicMock

    connection = MagicMock()
    closed = threading.Event()
    connection.close.side_effect = closed.set
    connection.connect.side_effect = OSError("collector unavailable")
    monkeypatch.setattr(history.Path, "exists", lambda self: True)
    monkeypatch.setattr(ingest.socket, "socket", lambda *args: connection)
    recorder = history.ExecutionRecorder(uuid.uuid4().hex, ["ls"], None, None)
    assert recorder.id is not None  # identity survives transient connection failure
    assert closed.wait(3)
    connection.close.assert_called_once()
    recorder.finish(0)
    assert recorder._stop.is_set()


def test_reporting_error_does_not_mask_command_cleanup(collector, monkeypatch):
    apply_id, _, done = collector
    recorder = history.ExecutionRecorder(apply_id, ["true"], None, None)
    wait_until(lambda: len(collector[1]) >= 2)

    def fail(*args):
        raise OSError("collector disconnected")

    monkeypatch.setattr(recorder, "_frame", fail)
    recorder.record_rank("workers", "0", {"returncode": 7})
    recorder.finish(None, RuntimeError("cleanup failed"))
    assert done.wait(3)


@pytest.mark.parametrize("table", tuple(history.execution_schemas()))
def test_execution_scans_include_worker_collector_records(table):
    from unittest.mock import MagicMock

    from monarch._src.job.telemetry_actor import TelemetryActor

    actor = TelemetryActor("query-root-test", 0)
    actor._scanner = MagicMock()
    actor._scanner.scan.return_value = 2
    worker = MagicMock()
    worker.scan.call.return_value.get.return_value = [("rank0", 3)]
    actor._worker_collectors = {"workers": worker}
    assert TelemetryActor.scan._method(actor, "dest", table, None, None, None) == 5
    worker.scan.call.assert_called_once()


def test_slow_collector_does_not_block_rank_callback(monkeypatch):
    from unittest.mock import MagicMock

    blocked, release, recorded = threading.Event(), threading.Event(), threading.Event()
    connection = MagicMock()
    writes = 0

    def send(frame):
        nonlocal writes
        writes += 1
        if writes == 3:
            blocked.set()
            assert release.wait(5)

    connection.sendall.side_effect = send
    monkeypatch.setattr(history.Path, "exists", lambda _: True)
    monkeypatch.setattr(ingest.socket, "socket", lambda *args: connection)
    monkeypatch.setattr(history, "HEARTBEAT_SECS", 0.01)
    recorder = history.ExecutionRecorder(uuid.uuid4().hex, ["true"], None, None)

    def report():
        recorder.record_rank("workers", "rank0", {"returncode": 0})
        recorded.set()

    callback = threading.Thread(target=report)
    try:
        assert blocked.wait(3)
        callback.start()
        assert recorded.wait(1), "socket I/O blocked the command result callback"
    finally:
        release.set()
        if callback.ident is not None:
            callback.join(3)
        recorder.finish(0)


@pytest.mark.parametrize("blocked_table", ["executions", "execution_rank_results"])
@pytest.mark.parametrize(
    "returncode,error,status",
    [
        (0, None, "succeeded"),
        (7, None, "failed"),
        (None, KeyboardInterrupt(), "interrupted"),
    ],
)
def test_finish_wait_is_bounded_when_collector_stalls(
    monkeypatch, blocked_table, returncode, error, status
):
    from unittest.mock import MagicMock

    blocked, release = threading.Event(), threading.Event()
    connection = MagicMock()
    frames = []

    def send(frame):
        name_len = struct.unpack("!H", frame[:2])[0]
        if frame[2 : 2 + name_len].decode() == blocked_table:
            blocked.set()
            assert release.wait(5)
        frames.append(frame)

    connection.sendall.side_effect = send
    monkeypatch.setattr(history.Path, "exists", lambda _: True)
    monkeypatch.setattr(ingest.socket, "socket", lambda *args: connection)
    monkeypatch.setattr(history.ExecutionRecorder, "_FINISH_TIMEOUT_SECS", 0.01)
    recorder = history.ExecutionRecorder(uuid.uuid4().hex, ["true"], None, None)
    try:
        recorder.record_rank("workers", "rank0", {"returncode": 0})
        assert blocked.wait(3)
        recorder.finish(returncode, error)
        assert recorder._stop.is_set()
        assert recorder._reporter.is_alive()
    finally:
        release.set()
        recorder._reporter.join(3)
    assert not recorder._reporter.is_alive()
    assert recorder._queue.empty()
    terminal_frame = frames[-1]
    name_len = struct.unpack("!H", terminal_frame[:2])[0]
    assert terminal_frame[2 : 2 + name_len] == b"execution_status_events"
    outcome = (
        pa.ipc.open_stream(terminal_frame[6 + name_len :]).read_all().to_pylist()[0]
    )
    assert outcome["status"] == status
    assert outcome["returncode"] == returncode


def test_rank_queue_stays_bounded_while_sender_is_blocked(monkeypatch):
    from unittest.mock import MagicMock

    blocked, release = threading.Event(), threading.Event()
    connection = MagicMock()

    def send(frame):
        blocked.set()
        assert release.wait(5)

    connection.sendall.side_effect = send
    monkeypatch.setattr(history.Path, "exists", lambda _: True)
    monkeypatch.setattr(ingest.socket, "socket", lambda *args: connection)
    monkeypatch.setattr(history.ExecutionRecorder, "_QUEUE_LIMIT", 8)
    recorder = history.ExecutionRecorder(uuid.uuid4().hex, ["true"], None, None)
    try:
        assert blocked.wait(3)
        for i in range(20):
            recorder.record_rank("workers", f"rank{i}", {"returncode": 0})
        assert recorder._queue.qsize() == 8
        assert recorder._dropped_ranks == 12
        assert recorder._observed_rank_count == 20
    finally:
        release.set()
        recorder.finish(0)
    assert recorder._queue.empty()


def test_failed_send_drops_frame_and_next_send_reconnects(monkeypatch, caplog):
    from unittest.mock import MagicMock

    first, second = MagicMock(), MagicMock()
    frames = []
    writes = 0

    def first_send(frame):
        nonlocal writes
        writes += 1
        if writes == 3:  # Metadata and the first heartbeat succeeded.
            raise BrokenPipeError("collector went away")
        frames.append(frame)

    first.sendall.side_effect = first_send
    second.sendall.side_effect = frames.append
    sockets = iter([first, second])
    monkeypatch.setattr(history.Path, "exists", lambda _: True)
    monkeypatch.setattr(ingest.socket, "socket", lambda *args: next(sockets))
    monkeypatch.setattr(history, "HEARTBEAT_SECS", 3600)
    recorder = history.ExecutionRecorder(uuid.uuid4().hex, ["true"], None, None)
    wait_until(lambda: writes == 2)
    with caplog.at_level("WARNING", logger=history.__name__):
        recorder.record_rank("workers", "rank0", {"returncode": 0})
        wait_until(lambda: recorder._dropped_ranks == 1)
        recorder.finish(0)
    rows = []
    for frame in frames:
        size = struct.unpack("!H", frame[:2])[0]
        table = frame[2 : 2 + size].decode()
        rows.extend(
            (table, row)
            for row in pa.ipc.open_stream(frame[6 + size :]).read_all().to_pylist()
        )
    metadata = [row for table, row in rows if table == "executions"]
    assert len(metadata) == 2 and metadata[0] == metadata[1]
    assert not [row for table, row in rows if table == "execution_rank_results"]
    assert rows[-1][1]["status"] == "succeeded"
    assert rows[-1][1]["error"] == "Telemetry incomplete: 1 rank reports unavailable"
    assert sum("telemetry unavailable" in r.message for r in caplog.records) == 1
