# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Host resource samplers for distributed telemetry.

Samplers are host-local telemetry producers. Each runs in the same proc as the
host's active `TelemetryActor`, samples on a daemon thread, and writes one
framed Arrow IPC batch per tick to the host's `telemetry.sock`, the ingestion
path every other producer uses. Rust owns the table schemas
(`monarch_telemetry_schema::host_tables`); the schemas here mirror them and
must match exactly, because socket ingest rejects any mismatch.
"""

from __future__ import annotations

import logging
import random
import socket
import threading
import time
from typing import Callable

import psutil
import pyarrow as pa
from monarch._src.job.telemetry_ingest import encode_frame, FrameWriter
from monarch.actor import Actor, endpoint

logger: logging.Logger = logging.getLogger(__name__)

HOST_METRICS_TABLE: str = "host_metrics"

HOST_METRICS_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("timestamp_us", pa.int64(), nullable=False),
        pa.field("hostname", pa.utf8(), nullable=False),
        pa.field("cpu_percent", pa.float64(), nullable=False),
        pa.field("cpu_count", pa.uint64()),
        pa.field("load_avg_1m", pa.float64(), nullable=False),
        pa.field("load_avg_5m", pa.float64(), nullable=False),
        pa.field("load_avg_15m", pa.float64(), nullable=False),
        pa.field("memory_total_bytes", pa.uint64(), nullable=False),
        pa.field("memory_available_bytes", pa.uint64(), nullable=False),
        pa.field("memory_used_bytes", pa.uint64(), nullable=False),
        pa.field("swap_used_bytes", pa.uint64(), nullable=False),
        pa.field("net_rx_bytes", pa.uint64(), nullable=False),
        pa.field("net_tx_bytes", pa.uint64(), nullable=False),
        pa.field("disk_read_bytes", pa.uint64()),
        pa.field("disk_write_bytes", pa.uint64()),
        pa.field("disk_root_free_bytes", pa.uint64()),
        pa.field("disk_tmp_free_bytes", pa.uint64()),
    ]
)


def sample_host_metrics(hostname: str) -> dict[str, object]:
    """Sample one `host_metrics` row for this host."""
    memory = psutil.virtual_memory()
    net = psutil.net_io_counters()
    disk = psutil.disk_io_counters()
    load_1m, load_5m, load_15m = psutil.getloadavg()
    return {
        "timestamp_us": time.time_ns() // 1000,
        "hostname": hostname,
        "cpu_percent": psutil.cpu_percent(None),
        "cpu_count": psutil.cpu_count(),
        "load_avg_1m": load_1m,
        "load_avg_5m": load_5m,
        "load_avg_15m": load_15m,
        "memory_total_bytes": memory.total,
        "memory_available_bytes": memory.available,
        "memory_used_bytes": memory.used,
        "swap_used_bytes": psutil.swap_memory().used,
        "net_rx_bytes": net.bytes_recv,
        "net_tx_bytes": net.bytes_sent,
        "disk_read_bytes": disk.read_bytes if disk is not None else None,
        "disk_write_bytes": disk.write_bytes if disk is not None else None,
        "disk_root_free_bytes": _free_bytes("/"),
        "disk_tmp_free_bytes": _free_bytes("/tmp"),
    }


def _free_bytes(path: str) -> int | None:
    try:
        return psutil.disk_usage(path).free
    except OSError:
        return None


def _jittered(interval_secs: float) -> float:
    wait = random.gauss(interval_secs, 0.1 * interval_secs)
    return min(max(wait, 0.5 * interval_secs), 1.5 * interval_secs)


def run_sampler_loop(
    tick: Callable[[], None], interval_secs: float, stop: threading.Event
) -> None:
    """Call `tick` about every `interval_secs` until `stop` is set.

    Each wait is drawn from a normal distribution around `interval_secs` with
    a 10% standard deviation, clamped to half and one and a half intervals.
    The first call comes one wait after the loop starts. A failing tick
    is logged and retried on the next interval: the first failure of a streak
    logs a warning and recovery logs once at info level. The loop logs an
    error if it ever exits while `stop` is unset.
    """
    failing = False
    try:
        while not stop.wait(_jittered(interval_secs)):
            try:
                tick()
            except Exception:
                if not failing:
                    logger.warning("telemetry sampler tick failed", exc_info=True)
                failing = True
            else:
                if failing:
                    logger.info("telemetry sampler recovered")
                failing = False
    finally:
        if not stop.is_set():
            logger.error("telemetry sampler loop exited")


class Sampler(Actor):
    """Base actor for a host-local sampler that writes one telemetry table.

    Subclasses set `table` and `schema` and implement `_sample`. They may
    override `_prepare` to decline running on hosts where sampling is not
    possible, and `_prime` to set up state on the sampler thread before the
    first sample.
    """

    _is_system_actor: bool = True

    table: str
    schema: pa.Schema

    def __init__(self, socket_path: str, interval_secs: float) -> None:
        self._writer = FrameWriter(socket_path)
        self._interval_secs = interval_secs
        self._thread: threading.Thread | None = None
        # Never set: samplers run for the life of their proc.
        self._stop = threading.Event()

    @endpoint
    def start(self) -> bool:
        """Start sampling; return whether this sampler runs on this host."""
        return self._start_impl()

    def _start_impl(self) -> bool:
        if self._thread is not None:
            return True
        if not self._prepare():
            return False
        thread = threading.Thread(
            target=self._run,
            name=f"{self.table}_sampler",
            daemon=True,
        )
        thread.start()
        self._thread = thread
        return True

    def _run(self) -> None:
        self._prime()
        run_sampler_loop(self._tick, self._interval_secs, self._stop)

    def _prepare(self) -> bool:
        return True

    def _prime(self) -> None:
        pass

    def _sample(self) -> list[dict[str, object]]:
        raise NotImplementedError

    def _tick(self) -> None:
        rows = self._sample()
        if not rows:
            return
        batch = pa.RecordBatch.from_pylist(rows, schema=self.schema)
        self._writer.send(encode_frame(self.table, batch))


class HostMetricsActor(Sampler):
    """Samples host CPU, load, memory, swap, network, and disk usage."""

    table: str = HOST_METRICS_TABLE
    schema: pa.Schema = HOST_METRICS_SCHEMA

    def __init__(self, socket_path: str, interval_secs: float) -> None:
        super().__init__(socket_path, interval_secs)
        self._hostname: str = socket.gethostname()

    def _prime(self) -> None:
        # psutil keeps `cpu_percent(None)` state per thread; this first call on
        # the sampler thread sets the baseline the first sample measures
        # against.
        psutil.cpu_percent(None)

    def _sample(self) -> list[dict[str, object]]:
        return [sample_host_metrics(self._hostname)]
