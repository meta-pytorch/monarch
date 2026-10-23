# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for the host metrics samplers."""

import socket
import struct
import threading
import time

import monarch._src.job.host_metrics_actor as hma
import pyarrow as pa
import pytest
from monarch._rust_bindings.monarch_distributed_telemetry import (
    _register_telemetry_schemas,
)
from monarch._rust_bindings.monarch_distributed_telemetry.database_scanner import (
    DatabaseScanner,
)


def _registered_schema(table: str) -> pa.Schema:
    scanner = DatabaseScanner(0, retention_secs=0)
    _register_telemetry_schemas(scanner)
    return pa.ipc.open_stream(bytes(scanner.schema_for(table))).schema


def _host_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(
        [hma.sample_host_metrics("h")], schema=hma.HOST_METRICS_SCHEMA
    )


def _ingest(batch: pa.RecordBatch) -> None:
    scanner = DatabaseScanner(0, retention_secs=0)
    _register_telemetry_schemas(scanner)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    scanner.ingest_snapshot_batch(hma.HOST_METRICS_TABLE, sink.getvalue().to_pybytes())


class _CountingStop(threading.Event):
    """Records each wait timeout and sets itself after `waits` waits."""

    def __init__(self, waits: int) -> None:
        super().__init__()
        self.timeouts: list[float] = []
        self._waits = waits

    def wait(self, timeout: float | None = None) -> bool:
        assert timeout is not None
        self.timeouts.append(timeout)
        if len(self.timeouts) >= self._waits:
            self.set()
        return self.is_set()


def _recv_exact(conn: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            break
        data += chunk
    return data


def test_host_metrics_schema_matches_registered() -> None:
    assert hma.HOST_METRICS_SCHEMA == _registered_schema("host_metrics")


def test_sample_host_metrics_builds_batch() -> None:
    row = hma.sample_host_metrics("h")
    batch = pa.RecordBatch.from_pylist([row], schema=hma.HOST_METRICS_SCHEMA)
    assert batch.num_rows == 1
    _ingest(batch)
    assert row["hostname"] == "h"
    memory_total = row["memory_total_bytes"]
    assert isinstance(memory_total, int) and memory_total > 0
    cpu_count = row["cpu_count"]
    assert isinstance(cpu_count, int) and cpu_count >= 1
    disk_root_free = row["disk_root_free_bytes"]
    assert isinstance(disk_root_free, int) and disk_root_free > 0


def test_sample_host_metrics_without_disk_counters(monkeypatch) -> None:
    monkeypatch.setattr(hma.psutil, "disk_io_counters", lambda: None)
    row = hma.sample_host_metrics("h")
    assert row["disk_read_bytes"] is None
    assert row["disk_write_bytes"] is None
    pa.RecordBatch.from_pylist([row], schema=hma.HOST_METRICS_SCHEMA)


def test_sample_host_metrics_without_cpu_count(monkeypatch) -> None:
    monkeypatch.setattr(hma.psutil, "cpu_count", lambda: None)
    row = hma.sample_host_metrics("h")
    assert row["cpu_count"] is None
    _ingest(pa.RecordBatch.from_pylist([row], schema=hma.HOST_METRICS_SCHEMA))


@pytest.mark.parametrize(
    "path, column",
    [("/", "disk_root_free_bytes"), ("/tmp", "disk_tmp_free_bytes")],
)
def test_sample_host_metrics_when_disk_usage_fails(
    monkeypatch, path: str, column: str
) -> None:
    disk_usage = hma.psutil.disk_usage

    def failing_disk_usage(p: str) -> object:
        if p == path:
            raise OSError("unavailable")
        return disk_usage(p)

    monkeypatch.setattr(hma.psutil, "disk_usage", failing_disk_usage)
    row = hma.sample_host_metrics("h")
    assert row[column] is None
    _ingest(pa.RecordBatch.from_pylist([row], schema=hma.HOST_METRICS_SCHEMA))


def test_run_sampler_loop_jitters_wait() -> None:
    stop = _CountingStop(50)
    hma.run_sampler_loop(lambda: None, 10.0, stop)
    assert len(stop.timeouts) == 50
    assert len(set(stop.timeouts)) > 1
    assert all(5.0 <= t <= 15.0 for t in stop.timeouts), stop.timeouts


def test_run_sampler_loop_clamps_jitter(monkeypatch) -> None:
    draws = iter([100.0, -100.0])
    monkeypatch.setattr(hma.random, "gauss", lambda mu, sigma: next(draws))
    stop = _CountingStop(2)
    hma.run_sampler_loop(lambda: None, 10.0, stop)
    assert stop.timeouts == [15.0, 5.0]


def test_run_sampler_loop_survives_failing_tick() -> None:
    calls = []
    stop = threading.Event()

    def tick() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        if len(calls) >= 3:
            stop.set()

    thread = threading.Thread(target=hma.run_sampler_loop, args=(tick, 0.01, stop))
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(calls) >= 3


def test_start_is_idempotent(tmp_path) -> None:
    actor = hma.HostMetricsActor(str(tmp_path / "missing.sock"), 60.0)
    assert actor._start_impl()
    first = actor._thread
    assert first is not None
    assert first.daemon
    assert first.is_alive()
    assert actor._start_impl()
    assert actor._thread is first


def test_first_cpu_sample_is_measured_on_the_sampler_thread(
    monkeypatch, tmp_path
) -> None:
    # Like psutil 7, state is per thread, and a call with no earlier call on
    # its thread, or right after one, measures nothing and returns 0.0.
    last_call: dict[int | None, float] = {}

    def cpu_percent(interval: float | None = None) -> float:
        tid = threading.current_thread().ident
        now = time.monotonic()
        previous = last_call.get(tid)
        last_call[tid] = now
        if previous is None or now - previous < 0.01:
            return 0.0
        return 42.0

    monkeypatch.setattr(hma.psutil, "cpu_percent", cpu_percent)
    path = str(tmp_path / "t.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.settimeout(5)
    server.bind(path)
    server.listen(1)
    actor = hma.HostMetricsActor(path, 0.05)
    try:
        assert actor._start_impl()
        conn, _ = server.accept()
        conn.settimeout(5)
        (name_len,) = struct.unpack(">H", _recv_exact(conn, 2))
        _recv_exact(conn, name_len)
        (payload_len,) = struct.unpack(">I", _recv_exact(conn, 4))
        batch = pa.ipc.open_stream(_recv_exact(conn, payload_len)).read_all()
        assert batch.column("cpu_percent").to_pylist() == [42.0]
        conn.close()
    finally:
        actor._stop.set()
        server.close()
