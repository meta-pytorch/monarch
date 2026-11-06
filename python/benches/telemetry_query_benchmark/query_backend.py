# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Execution backends for the telemetry query benchmark."""

from __future__ import annotations

import shutil
import uuid
from contextlib import suppress
from typing import Any, cast, Literal, Protocol

import pyarrow as pa
from monarch._src.job._telemetry_query_client import QueryEngineClient
from monarch._src.job.telemetry_actor import telemetry_socket_dir, TelemetryActor
from monarch.actor import context, current_rank, endpoint, HostMesh, shutdown_context
from monarch.distributed_telemetry.engine import QueryEngine
from monarch.job import ProcessJob, TelemetryConfig
from monarch.python.benches.telemetry_query_benchmark import pyspy_fixture

BackendName = Literal["query-engine", "sidecar-http"]
QueryRows = list[dict[str, object]]
QueryData = dict[str, object] | pa.Table
_QUERY_TIMEOUT_SEC = 60.0


class QueryBackend(Protocol):
    """A fixture store and query execution boundary."""

    def preload(self, shape: pyspy_fixture.DatasetShape) -> None:
        """Load the deterministic fixture."""
        ...

    def query(self, sql: str) -> QueryData:
        """Execute SQL and return the backend-native result."""
        ...

    def rows(self, data: QueryData) -> QueryRows:
        """Materialize rows outside the timed query interval."""
        ...

    def close(self) -> None:
        """Release resources owned by this backend."""
        ...


class _SidecarHttpBackend:
    def __init__(self) -> None:
        job = ProcessJob({"hosts": 1}).enable_telemetry(
            TelemetryConfig(
                retention_secs=0,
                include_dashboard=False,
                dashboard_port=0,
            )
        )
        try:
            state = job.state(cached_path=None)
            if state.telemetry_url is None:
                raise RuntimeError("telemetry query URL is unavailable")
            self._client = QueryEngineClient(
                state.telemetry_url,
                _QUERY_TIMEOUT_SEC,
            )
        except Exception:
            with suppress(Exception):
                job.kill()
            raise
        self._job = job

    def preload(self, shape: pyspy_fixture.DatasetShape) -> None:
        payload = pyspy_fixture.make_dump(shape)
        for dump_index in range(shape.dumps):
            response = self._client.store_pyspy_dump(
                pyspy_fixture.dump_id(dump_index),
                f"proc[{dump_index}]",
                payload,
            )
            if response.get("status") != "ok":
                raise RuntimeError(f"py-spy preload failed: {response!r}")

    def query(self, sql: str) -> QueryData:
        return cast(dict[str, object], self._client.query(sql))

    def rows(self, data: QueryData) -> QueryRows:
        if isinstance(data, pa.Table):
            raise RuntimeError("sidecar HTTP returned an Arrow table")
        rows = data.get("rows")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise RuntimeError(f"query returned malformed rows: {rows!r}")
        return cast(QueryRows, rows)

    def close(self) -> None:
        self._job.kill()


def _worker_apply_id(apply_id: str, rank: int) -> str:
    return f"{apply_id}-worker{rank}"


class _WorkerCollector(TelemetryActor):
    """Worker collector with its own store, as on a separate host.

    Local worker processes share one machine, so each rank takes its own socket
    namespace to activate instead of deferring to the first collector.
    """

    def __init__(self, apply_id: str, *args: Any) -> None:
        super().__init__(_worker_apply_id(apply_id, current_rank().rank), *args)


class _RootCollector(TelemetryActor):
    @endpoint
    def start_workers(self, host_mesh: HostMesh) -> Any:
        """Start one worker collector per host of `host_mesh`."""
        return self._start_worker_collector(
            host_mesh,
            _WorkerCollector,
            self._apply_id,
            "telemetry_benchmark_workers",
        )


class _QueryEngineBackend:
    def __init__(self, workers: int) -> None:
        self._apply_id = f"query_benchmark_{uuid.uuid4().hex}"
        self._socket_dirs: list[str] = [
            telemetry_socket_dir(self._apply_id),
            *(
                telemetry_socket_dir(_worker_apply_id(self._apply_id, rank))
                for rank in range(workers)
            ),
        ]
        self._job: ProcessJob | None = None
        try:
            proc_mesh = context().actor_instance.proc_mesh
            actor: Any = proc_mesh.spawn(
                "telemetry_query_benchmark",
                _RootCollector,
                self._apply_id,
                0,
            )
            if not actor.activate.call_one().get():
                raise RuntimeError("telemetry collector activation failed")
            self._collectors: list[Any] = [actor]
            if workers > 0:
                job = ProcessJob({"hosts": workers})
                self._job = job
                hosts = job.state(cached_path=None).hosts
                worker_mesh = actor.start_workers.call_one(hosts).get()
                if worker_mesh is None:
                    raise RuntimeError("worker telemetry collectors did not activate")
                self._collectors += [
                    worker_mesh.slice(hosts=rank) for rank in range(workers)
                ]
            self._engine = QueryEngine(actor)
        except Exception:
            self._release()
            raise

    def preload(self, shape: pyspy_fixture.DatasetShape) -> None:
        payload = pyspy_fixture.make_dump(shape)
        for dump_index in range(shape.dumps):
            collector = self._collectors[dump_index % len(self._collectors)]
            stored = collector.store_pyspy_dump.call_one(
                pyspy_fixture.dump_id(dump_index),
                f"proc[{dump_index}]",
                payload,
            ).get()
            if not stored:
                raise RuntimeError("py-spy preload failed")

    def query(self, sql: str) -> QueryData:
        return self._engine.query(sql)

    def rows(self, data: QueryData) -> QueryRows:
        if not isinstance(data, pa.Table):
            raise RuntimeError("direct query engine returned JSON rows")
        return cast(QueryRows, data.to_pylist())

    def close(self) -> None:
        self._release()

    def _release(self) -> None:
        try:
            with suppress(TimeoutError):
                shutdown_context().get(timeout=5.0)
        finally:
            if self._job is not None:
                with suppress(Exception):
                    self._job.kill()
            for socket_dir in self._socket_dirs:
                shutil.rmtree(socket_dir, ignore_errors=True)


def create_backend(
    name: BackendName,
    workers: int = 0,
) -> QueryBackend:
    """Create a benchmark backend.

    `workers` adds that many local worker collectors to the `query-engine`
    backend. Each holds a share of the fixture, so queries fan out across them.
    """
    if name == "query-engine":
        return _QueryEngineBackend(workers)
    if workers:
        raise ValueError("worker collectors require the query-engine backend")
    return _SidecarHttpBackend()
