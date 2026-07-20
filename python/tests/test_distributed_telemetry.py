# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Tests for distributed telemetry with automatic callback registration."""

import json
import os
import shutil
import time
import types
import unittest.mock
import uuid
from collections import Counter
from typing import Any, cast

import monarch._src.job.telemetry_actor as job_telemetry_actor
import monarch.actor
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch._src.actor.actor_mesh import Actor, ActorMesh, current_rank
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.proc_mesh import (
    _proc_mesh_spawn_callbacks,
    SetupActor,
    unregister_proc_mesh_spawn_callback,
)
from monarch.actor import span
from monarch.job import MeshAdminConfig, ProcessJob, TelemetryConfig
from scoped_state import scoped_state


class WorkerActor(Actor):
    """Simple test actor with a no-op ping endpoint."""

    @endpoint
    def ping(self) -> None:
        pass

    @endpoint
    def emit_trace(self, name: str) -> None:
        """Emit a named user span."""
        with span(name):
            pass


class SenderActor(Actor):
    """Actor that sends messages to another actor mesh."""

    @endpoint
    def send_ping(self, target: WorkerActor) -> None:
        """Cast to the target actor mesh from within this actor."""
        target.ping.call().get()


class TelemetryWorkerActor(Actor):
    """Worker that exercises multi-hop actor messaging."""

    @endpoint
    def start(self, coordinator: Any) -> None:
        coordinator.request.call_one(current_rank().rank).get()

    @endpoint
    def reply(self) -> None:
        pass


class TelemetryCoordinatorActor(Actor):
    """Coordinator that replies to the requesting worker."""

    def __init__(self, workers: Any) -> None:
        self.workers = workers

    @endpoint
    def request(self, rank: int) -> None:
        self.workers.slice(replica=rank).reply.broadcast()


class TelemetryFailureActor(Actor):
    """Actor whose fire-and-forget endpoint fails the actor."""

    @endpoint
    def fail(self) -> None:
        raise RuntimeError("telemetry failure")


def _telemetry_config(**kwargs: Any) -> TelemetryConfig:
    kwargs.setdefault("dashboard_port", 0)
    return TelemetryConfig(**kwargs)


def _sidecar_telemetry_config(**kwargs: Any) -> TelemetryConfig:
    kwargs.setdefault("retention_secs", 0)
    return _telemetry_config(**kwargs)


def _assert_sidecar(state) -> None:
    assert state.query_engine is None
    assert state.query_engine_client is not None


def _sidecar_query_rows(state, sql: str) -> list[dict[str, Any]]:
    client = state.query_engine_client
    assert client is not None
    return client.query(sql).get("rows", [])


def _rows_to_pydict(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not rows:
        return {}
    return {column: [row.get(column) for row in rows] for column in rows[0].keys()}


def _pydict_to_rows(columns: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not columns:
        return []
    names = list(columns)
    return [
        dict(zip(names, values, strict=True))
        for values in zip(*(columns[name] for name in names), strict=True)
    ]


def _query(
    state, sql: str, *, min_rows: int = 1, timeout_secs: float = 20.0
) -> dict[str, list[Any]]:
    deadline = time.monotonic() + timeout_secs
    rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        rows = _sidecar_query_rows(state, sql)
        if len(rows) >= min_rows:
            break
        time.sleep(0.2)
    return _rows_to_pydict(rows)


def _store_pyspy_dump(
    state, dump_id: str, proc_ref: str, pyspy_result_json: str
) -> dict[str, Any]:
    client = state.query_engine_client
    assert client is not None
    return client.store_pyspy_dump(dump_id, proc_ref, pyspy_result_json)


def _new_apply_id() -> str:
    return f"test_{uuid.uuid4().hex}"


def _remove_socket_dir(apply_id: str) -> None:
    shutil.rmtree(
        job_telemetry_actor.telemetry_socket_dir(apply_id), ignore_errors=True
    )


_TELEMETRY_WORKER_MESH = "telemetry_worker"
_TELEMETRY_COORDINATOR_MESH = "telemetry_coordinator"
_TELEMETRY_FAILURE_MESH = "telemetry_failure"


def _start_telemetry_workload(state) -> None:
    hosts = state.hosts
    worker_procs = hosts.spawn_procs(
        per_host={"replica": 2}, name="telemetry_worker_procs"
    )
    coordinator_proc = hosts.spawn_procs(name=_TELEMETRY_COORDINATOR_MESH)
    workers = worker_procs.spawn(_TELEMETRY_WORKER_MESH, TelemetryWorkerActor)
    coordinator = coordinator_proc.spawn(
        _TELEMETRY_COORDINATOR_MESH, TelemetryCoordinatorActor, workers
    )
    workers.initialized.get()
    coordinator.initialized.get()
    workers.start.broadcast(coordinator)


def _start_failed_actor(state) -> tuple[int, int]:
    monarch.actor.unhandled_fault_hook = lambda failure: None
    failure_procs = state.hosts.spawn_procs(name="telemetry_failure_procs")
    failure_actor = failure_procs.spawn(_TELEMETRY_FAILURE_MESH, TelemetryFailureActor)
    failure_actor.initialized.get()
    actor_id = _query(
        state,
        "SELECT a.id FROM actors a JOIN meshes m ON a.mesh_id = m.id "
        f"WHERE m.given_name = '{_TELEMETRY_FAILURE_MESH}'",
    )["id"][0]

    failure_actor.fail.broadcast()
    _query(
        state,
        "SELECT id FROM actor_status_events "
        f"WHERE actor_id = {actor_id} AND new_status = 'Failed'",
    )
    message_id = _query(
        state,
        "SELECT msg.id FROM messages msg "
        "JOIN actors a ON msg.to_actor_id = a.id "
        "JOIN meshes m ON a.mesh_id = m.id "
        f"WHERE m.given_name = '{_TELEMETRY_FAILURE_MESH}' "
        "AND msg.endpoint = 'fail'",
    )["id"][0]
    return actor_id, message_id


@pytest.mark.timeout(30)
def test_telemetry_actor_starts_local_socket_collector() -> None:
    apply_id = _new_apply_id()
    _remove_socket_dir(apply_id)
    try:
        actor = job_telemetry_actor.TelemetryActor(apply_id, retention_secs=0)
        with (
            unittest.mock.patch.object(
                job_telemetry_actor,
                "current_rank",
                return_value=types.SimpleNamespace(rank=0),
            ),
            unittest.mock.patch.object(
                job_telemetry_actor,
                "_start_socket_ingest",
            ) as start_ingest,
        ):
            assert actor._activate_impl()
            assert actor._scanner is not None
            # Second call is a no-op.
            assert actor._activate_impl()

        socket_dir = job_telemetry_actor.telemetry_socket_dir(apply_id)
        socket_path = job_telemetry_actor.telemetry_socket_path(apply_id)
        assert os.stat(socket_dir).st_mode & 0o777 == 0o700
        start_ingest.assert_called_once()
        assert start_ingest.call_args.args[1] == socket_path
    finally:
        _remove_socket_dir(apply_id)


@pytest.mark.timeout(30)
def test_telemetry_actor_reports_live_collector_activation_failure() -> None:
    apply_id = _new_apply_id()
    _remove_socket_dir(apply_id)
    try:
        actor = job_telemetry_actor.TelemetryActor(apply_id, retention_secs=0)
        with (
            unittest.mock.patch.object(
                job_telemetry_actor,
                "current_rank",
                return_value=types.SimpleNamespace(rank=0),
            ),
            unittest.mock.patch.object(
                job_telemetry_actor,
                "_start_socket_ingest",
                side_effect=RuntimeError(
                    "telemetry socket already has a live collector"
                ),
            ),
        ):
            assert not actor._activate_impl()
        assert actor._scanner is None
        with pytest.raises(RuntimeError, match="not an active telemetry collector"):
            actor._scanner_or_raise()
    finally:
        _remove_socket_dir(apply_id)


@pytest.mark.timeout(30)
def test_telemetry_actor_reports_activation_failure() -> None:
    apply_id = _new_apply_id()
    _remove_socket_dir(apply_id)
    try:
        actor = job_telemetry_actor.TelemetryActor(apply_id, retention_secs=0)
        with (
            unittest.mock.patch.object(
                job_telemetry_actor,
                "current_rank",
                return_value=types.SimpleNamespace(rank=0),
            ),
            unittest.mock.patch.object(
                job_telemetry_actor,
                "_start_socket_ingest",
                side_effect=RuntimeError("boom"),
            ),
        ):
            assert not actor._activate_impl()
        assert actor._scanner is None
    finally:
        _remove_socket_dir(apply_id)


@pytest.fixture
def cleanup_callbacks():
    """Fixture to clean up any callbacks registered during tests."""
    initial_callbacks = list(_proc_mesh_spawn_callbacks)
    initial_startup_functions = list(SetupActor._startup_functions)
    yield
    # Remove any callbacks added during the test
    callbacks_to_remove = [
        cb for cb in _proc_mesh_spawn_callbacks if cb not in initial_callbacks
    ]
    for cb in callbacks_to_remove:
        unregister_proc_mesh_spawn_callback(cb)
    # Remove any startup functions added during the test
    startup_to_remove = [
        fn
        for fn in SetupActor._startup_functions
        if fn not in initial_startup_functions
    ]
    for fn in startup_to_remove:
        SetupActor._startup_functions.remove(fn)


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_actors_table() -> None:
    """Test that the actors table is populated when actors are spawned."""
    # Spawn some worker actors - this should trigger notify_actor_created
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2})
        workers = worker_procs.spawn("test_worker", WorkerActor)
        workers.initialized.get()

        # Query the actors table to verify actors were recorded
        result_dict = _query(
            state,
            "SELECT a.* FROM actors a "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'test_worker'",
        )

        # We should have at least some actors recorded
        # (the exact count depends on internal actors created)
        actor_count = len(result_dict.get("id", []))
        assert actor_count > 0, f"Expected at least one actor, got {actor_count}"

        # Verify the schema has the expected columns
        expected_columns = {
            "id",
            "timestamp_us",
            "mesh_id",
            "rank",
            "full_name",
            "display_name",
        }
        actual_columns = set(result_dict.keys())
        assert expected_columns == actual_columns, (
            f"Expected columns {expected_columns}, got {actual_columns}"
        )

        # Verify full_name is populated with canonical actor identifiers.
        full_names = result_dict.get("full_name", [])
        assert all(full_names), (
            f"Expected non-empty full_name values, got: {full_names}"
        )

        # Verify display_name carries the user-facing supervision name.
        display_names = result_dict.get("display_name", [])
        has_test_worker = any(
            name is not None and "test_worker" in name for name in display_names
        )
        assert has_test_worker, (
            f"Expected to find 'test_worker' in actor display names, got: {display_names}"
        )

        # Verify that the bootstrap client actor is recorded with display_name "<root>".
        result_dict = _query(state, "SELECT display_name FROM actors")
        root_display_names = result_dict.get("display_name", [])
        assert "<root>" in root_display_names, (
            f"Expected bootstrap client actor with display_name '<root>', got: {root_display_names}"
        )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_telemetry_workload_actor_topology() -> None:
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        _start_telemetry_workload(state)

        result = _query(
            state,
            "SELECT a.id, a.timestamp_us, a.rank, a.full_name, "
            "m.given_name, m.class FROM actors a "
            "JOIN meshes m ON a.mesh_id = m.id "
            f"WHERE m.given_name IN ('{_TELEMETRY_WORKER_MESH}', "
            f"'{_TELEMETRY_COORDINATOR_MESH}')",
            min_rows=4,
        )
        rows = _pydict_to_rows(result)
        assert len(rows) == 4, rows
        assert Counter(row["class"] for row in rows) == {
            "Proc": 1,
            "Python<TelemetryWorkerActor>": 2,
            "Python<TelemetryCoordinatorActor>": 1,
        }

        workers = [
            row for row in rows if row["class"] == "Python<TelemetryWorkerActor>"
        ]
        assert sorted(row["rank"] for row in workers) == [0, 1]
        assert [
            row["rank"]
            for row in rows
            if row["class"] == "Python<TelemetryCoordinatorActor>"
        ] == [0]
        assert len({row["id"] for row in rows}) == 4
        assert all(row["timestamp_us"] > 0 for row in rows)
        assert all(ActorAddr.from_string(row["full_name"]) for row in rows)


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_meshes_table() -> None:
    """Test that the meshes table is populated when actor meshes are spawned."""
    # Spawn some worker actors - this should trigger notify_mesh_created
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2})
        workers = worker_procs.spawn("test_mesh_worker", WorkerActor)
        workers.initialized.get()

        # Query the meshes table to verify actor meshes were recorded
        result_dict = _query(
            state,
            "SELECT * FROM meshes WHERE given_name = 'test_mesh_worker'",
        )

        # We should have at least some actor meshes recorded
        mesh_count = len(result_dict.get("id", []))
        assert mesh_count > 0, f"Expected at least one actor mesh, got {mesh_count}"

        # Verify the schema has the expected columns
        expected_columns = {
            "id",
            "timestamp_us",
            "class",
            "given_name",
            "full_name",
            "shape_json",
            "parent_mesh_id",
            "parent_view_json",
        }
        actual_columns = set(result_dict.keys())
        assert expected_columns == actual_columns, (
            f"Expected columns {expected_columns}, got {actual_columns}"
        )

        # Verify given_name is the user-provided name (not the full name with UUID suffix)
        given_names = result_dict.get("given_name", [])
        full_names = result_dict.get("full_name", [])
        assert "test_mesh_worker" in given_names, (
            f"Expected exact 'test_mesh_worker' in given_names, got: {given_names}"
        )
        for gn, fn in zip(given_names, full_names):
            if gn == "test_mesh_worker":
                # full_name includes a UUID suffix, so it should differ from given_name
                assert fn != gn, (
                    f"Expected full_name to differ from given_name, but both are '{gn}'"
                )
                assert fn.startswith("test_mesh_worker"), (
                    f"Expected full_name to start with 'test_mesh_worker', got: {fn}"
                )

        # Verify parent_view_json is populated (serialized Region from ndslice)
        parent_views = result_dict.get("parent_view_json", [])
        for name, view in zip(given_names, parent_views):
            if name == "test_mesh_worker":
                assert view is not None, (
                    f"Expected parent_view_json to be populated for '{name}', got None"
                )
                parsed_view = json.loads(view)
                # Region serializes as {"labels": [...], "slice": {"offset": ..., "sizes": [...], "strides": [...]}}
                assert "slice" in parsed_view, (
                    f"Expected parent_view_json to contain 'slice' key (ndslice Region), got: {parsed_view}"
                )
                assert "labels" in parsed_view, (
                    f"Expected parent_view_json to contain 'labels' key, got: {parsed_view}"
                )

        # Verify shape_json describes the actor mesh's shape (serialized Extent from ndslice)
        shape_jsons = result_dict.get("shape_json", [])
        for name, shape in zip(given_names, shape_jsons):
            if name == "test_mesh_worker":
                assert shape is not None and shape != "", (
                    f"Expected shape_json to be populated for '{name}', got '{shape}'"
                )
                parsed_shape = json.loads(shape)
                # Extent serializes as {"inner": {"labels": [...], "sizes": [...]}}
                assert "inner" in parsed_shape, (
                    f"Expected shape_json to contain 'inner' key (ndslice Extent), got: {parsed_shape}"
                )
                labels = parsed_shape["inner"]["labels"]
                sizes = parsed_shape["inner"]["sizes"]
                assert "workers" in labels, (
                    f"Expected shape_json labels to contain 'workers', got: {labels}"
                )
                workers_idx = labels.index("workers")
                assert sizes[workers_idx] == 2, (
                    f"Expected 2 workers in shape, got: {sizes[workers_idx]}"
                )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_telemetry_workload_mesh_topology() -> None:
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        _start_telemetry_workload(state)

        rows = _pydict_to_rows(
            _query(
                state,
                "SELECT id, timestamp_us, class, given_name, shape_json, "
                "parent_mesh_id FROM meshes "
                f"WHERE given_name IN ('{_TELEMETRY_WORKER_MESH}', "
                f"'{_TELEMETRY_COORDINATOR_MESH}')",
                min_rows=3,
            )
        )
        assert len(rows) == 3, rows
        assert Counter(row["class"] for row in rows) == {
            "Proc": 1,
            "Python<TelemetryWorkerActor>": 1,
            "Python<TelemetryCoordinatorActor>": 1,
        }
        assert len({row["id"] for row in rows}) == 3
        assert all(row["timestamp_us"] > 0 for row in rows)

        worker_mesh = next(
            row for row in rows if row["class"] == "Python<TelemetryWorkerActor>"
        )
        worker_shape = json.loads(worker_mesh["shape_json"])["inner"]
        assert worker_shape == {
            "labels": ["hosts", "replica"],
            "sizes": [1, 2],
        }

        coordinator_proc = next(row for row in rows if row["class"] == "Proc")
        coordinator_actor_mesh = next(
            row for row in rows if row["class"] == "Python<TelemetryCoordinatorActor>"
        )
        assert coordinator_actor_mesh["parent_mesh_id"] == coordinator_proc["id"]


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_proc_mesh_in_meshes_table() -> None:
    """Test that ProcMesh creation is recorded in the meshes table with class 'Proc'."""
    # Spawn a named proc mesh — this should emit a mesh event with class "Proc"
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="proc_mesh_test")
        workers = worker_procs.spawn("proc_mesh_test_worker", WorkerActor)
        workers.initialized.get()

        # Query meshes with class "Proc"
        result_dict = _query(
            state,
            "SELECT given_name, full_name, class, shape_json, parent_mesh_id, parent_view_json "
            "FROM meshes WHERE class = 'Proc' AND given_name = 'proc_mesh_test'",
        )

        # Verify our named proc mesh appears with the correct given_name.
        # The bootstrap path also emits a "local" proc mesh, so filter for ours.
        given_names = result_dict.get("given_name", [])
        assert "proc_mesh_test" in given_names, (
            f"Expected 'proc_mesh_test' in given_names, got: {given_names}"
        )

        # Verify full_name differs from given_name (includes UUID suffix)
        full_names = result_dict.get("full_name", [])
        for gn, fn in zip(given_names, full_names):
            if gn == "proc_mesh_test":
                assert fn != gn, (
                    f"Expected full_name to differ from given_name, but both are '{gn}'"
                )
                assert fn.startswith("proc_mesh_test"), (
                    f"Expected full_name to start with 'proc_mesh_test', got: {fn}"
                )

        # Verify shape_json is populated for the proc mesh
        shape_jsons = result_dict.get("shape_json", [])
        for gn, shape in zip(given_names, shape_jsons):
            if gn == "proc_mesh_test":
                assert shape is not None and shape != "", (
                    f"Expected shape_json to be populated for '{gn}', got '{shape}'"
                )
                parsed_shape = json.loads(shape)
                assert "inner" in parsed_shape, (
                    f"Expected shape_json to contain 'inner' key (ndslice Extent), got: {parsed_shape}"
                )
                labels = parsed_shape["inner"]["labels"]
                sizes = parsed_shape["inner"]["sizes"]
                assert "workers" in labels, (
                    f"Expected shape_json labels to contain 'workers', got: {labels}"
                )
                workers_idx = labels.index("workers")
                assert sizes[workers_idx] == 2, (
                    f"Expected 2 workers in shape, got: {sizes[workers_idx]}"
                )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_actors_join_meshes_on_mesh_id(cleanup_callbacks) -> None:
    """Test that actors.mesh_id matches meshes.id, enabling joins."""
    # Spawn actors — this populates both the actors and meshes tables
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2})
        workers = worker_procs.spawn("join_test_worker", WorkerActor)
        workers.initialized.get()

        # Join actors with meshes on mesh_id = id
        result_dict = _query(
            state,
            """SELECT a.full_name AS actor_name,
                      a.mesh_id,
                      a.rank,
                      m.given_name AS mesh_name,
                      m.class AS mesh_class
               FROM actors a
               INNER JOIN meshes m ON a.mesh_id = m.id
               WHERE m.given_name = 'join_test_worker'
               ORDER BY a.rank""",
            min_rows=2,
        )

        # The join should produce results — if mesh_id doesn't match, this is empty
        joined_count = len(result_dict.get("actor_name", []))
        assert joined_count > 0, (
            "Expected actors to join with meshes on mesh_id, but got 0 rows. "
            "This means actors.mesh_id does not match any meshes.id."
        )

        # Every joined row should reference our mesh name
        mesh_names = result_dict.get("mesh_name", [])
        assert all("join_test_worker" in name for name in mesh_names), (
            f"Expected all joined rows to reference 'join_test_worker', got: {mesh_names}"
        )
        actor_names = result_dict.get("actor_name", [])
        assert all(actor_names), (
            f"Expected canonical actor names to be populated, got: {actor_names}"
        )

        # With 2 workers, we should see 2 joined rows
        assert joined_count == 2, (
            f"Expected 2 joined rows for 2 workers, got: {joined_count}"
        )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_all_actors_in_proc_mesh(cleanup_callbacks) -> None:
    """Test that all actor meshes within a proc mesh have actors in the actors table."""
    # Spawn a named proc mesh and user actors
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="workers_procs")
        workers = worker_procs.spawn("worker_actors", WorkerActor)
        workers.initialized.get()

        # Get the proc mesh entry so we can filter child meshes by parent_mesh_id
        proc_dict = _query(
            state,
            "SELECT id FROM meshes WHERE class = 'Proc' AND given_name = 'workers_procs'",
        )
        proc_ids = proc_dict.get("id", [])
        assert len(proc_ids) == 1, f"Expected exactly 1 proc mesh, got {len(proc_ids)}"
        proc_mesh_id = proc_ids[0]

        # ProcAgent actors have mesh_id pointing directly to the proc mesh
        proc_agents = _query(
            state,
            f"SELECT DISTINCT id FROM actors WHERE mesh_id = {proc_mesh_id}",
            min_rows=2,
        )
        proc_agents_count = len(proc_agents.get("id", []))
        assert proc_agents_count == 2, (
            f"Expected 2 ProcAgent actors, got {proc_agents_count}"
        )

        # Query all child actor meshes of this proc mesh
        child_dict = _query(
            state,
            f"SELECT id, class, given_name FROM meshes WHERE parent_mesh_id = {proc_mesh_id}",
            min_rows=4,
        )
        child_classes = child_dict.get("class", [])
        child_names = child_dict.get("given_name", [])
        child_ids = child_dict.get("id", [])

        assert set(child_names) == {
            "worker_actors",
            "logger",
            "setup",
        }

        # For every child actor mesh, verify that actors exist in the actors table
        for mesh_id, mesh_class, mesh_name in zip(
            child_ids, child_classes, child_names
        ):
            actor_dict = _query(
                state,
                f"SELECT DISTINCT id, rank, full_name, display_name "
                f"FROM actors WHERE mesh_id = {mesh_id}",
                min_rows=2,
            )
            actor_count = len(actor_dict.get("id", []))

            # Each mesh on a 2-worker proc mesh should have exactly 2 actors
            assert actor_count == 2, (
                f"Expected 2 actors for mesh '{mesh_name}' (class={mesh_class}), "
                f"got {actor_count}: {actor_dict}"
            )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_all_actors_in_host_mesh(cleanup_callbacks) -> None:
    """Test that all actor meshes within a proc mesh have actors in the actors table."""
    # Spawn a named proc mesh and user actors
    with scoped_state(
        ProcessJob({"hosts": 2}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="workers_procs")
        workers = worker_procs.spawn("worker_actors", WorkerActor)
        workers.initialized.get()

        # Get the hosts mesh entry so we can filter child meshes by parent_mesh_id
        host_mesh_result = _query(
            state,
            "SELECT hosts.id FROM meshes hosts "
            "JOIN meshes proc ON proc.parent_mesh_id = hosts.id "
            "WHERE hosts.class = 'Host' "
            "AND hosts.given_name = 'hosts' "
            "AND proc.given_name = 'workers_procs'",
        )
        host_mesh_ids = host_mesh_result.get("id", [])
        assert len(host_mesh_ids) == 1, (
            f"Expected exactly 1 hosts mesh, got {len(host_mesh_ids)}"
        )
        host_mesh_id = host_mesh_ids[0]

        # HostAgent actors have mesh_id pointing directly to the host mesh
        host_agents = _query(
            state, f"SELECT DISTINCT id FROM actors WHERE mesh_id = {host_mesh_id}"
        )
        host_agents_count = len(host_agents.get("id", []))
        assert host_agents_count > 0, (
            f"Expected HostAgent actors, got {host_agents_count}"
        )

        # Query all proc meshes of this hosts mesh
        proc_dict = _query(
            state,
            f"SELECT id, class, given_name FROM meshes WHERE parent_mesh_id = {host_mesh_id}",
        )
        proc_given_names = set(proc_dict.get("given_name", []))
        assert "workers_procs" in proc_given_names

        # Query all child actor meshes of this hosts mesh
        child_dict = _query(
            state,
            f"""
            SELECT m.id, m.class, m.given_name
            FROM meshes m
            INNER JOIN meshes proc ON m.parent_mesh_id = proc.id
            INNER JOIN meshes hosts ON proc.parent_mesh_id = hosts.id
            WHERE hosts.id = {host_mesh_id}
              AND proc.given_name = 'workers_procs'
            """,
            min_rows=4,
        )
        child_classes = child_dict.get("class", [])
        child_names = child_dict.get("given_name", [])
        child_ids = child_dict.get("id", [])

        assert set(child_names) == {
            "worker_actors",
            "logger",
            "setup",
        }

        # For every child actor mesh, verify that actors exist in the actors table
        for mesh_id, mesh_class, mesh_name in zip(
            child_ids, child_classes, child_names
        ):
            actor_dict = _query(
                state,
                f"SELECT DISTINCT id FROM actors WHERE mesh_id = {mesh_id}",
                min_rows=4,
            )
            actor_count = len(actor_dict.get("id", []))
            assert actor_count == 4, (
                f"Expected 4 actors for mesh '{mesh_name}' (class={mesh_class}), "
                f"got {actor_count}"
            )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_actor_status_events_table() -> None:
    """Test that the actor_status_events table is populated when actors change status."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2})
        workers = worker_procs.spawn("status_test_worker", WorkerActor)
        workers.initialized.get()

        result_dict = _query(state, "SELECT * FROM actor_status_events")
        expected_columns = {
            "id",
            "timestamp_us",
            "actor_id",
            "new_status",
            "reason",
        }
        actual_columns = set(result_dict.keys())
        assert expected_columns == actual_columns, (
            f"Expected columns {expected_columns}, got {actual_columns}"
        )

        worker_rows = _pydict_to_rows(
            _query(
                state,
                "SELECT ase.actor_id, ase.new_status, ase.reason "
                "FROM actor_status_events ase "
                "JOIN actors a ON ase.actor_id = a.id "
                "JOIN meshes m ON a.mesh_id = m.id "
                "WHERE m.given_name = 'status_test_worker'",
                min_rows=6,
            )
        )
        statuses_by_actor = {
            actor_id: {
                row["new_status"] for row in worker_rows if row["actor_id"] == actor_id
            }
            for actor_id in {row["actor_id"] for row in worker_rows}
        }
        assert len(statuses_by_actor) == 2
        assert all(
            statuses == {"Created", "Initializing", "Idle"}
            for statuses in statuses_by_actor.values()
        ), statuses_by_actor
        assert all(row["reason"] is None for row in worker_rows)

        client_rows = _query(
            state,
            "SELECT ase.id FROM actor_status_events ase "
            "JOIN actors a ON ase.actor_id = a.id "
            "WHERE a.display_name = '<root>' AND ase.new_status = 'Client'",
        )
        assert client_rows["id"]

        valid_statuses = {
            "Unknown",
            "Created",
            "Initializing",
            "Client",
            "Idle",
            "Processing",
            "Stopping",
            "Stopped",
            "Failed",
        }
        new_statuses = set(
            _query(state, "SELECT DISTINCT new_status FROM actor_status_events")[
                "new_status"
            ]
        )
        assert new_statuses.issubset(valid_statuses), (
            f"Found unexpected status values: {new_statuses - valid_statuses}"
        )
        assert "Unknown" not in new_statuses


@pytest.mark.timeout(120)
@isolate_in_subprocess
@pytest.mark.parametrize("required_status", ["Created", "Processing"])
def test_user_actor_status_lifecycle(required_status: str) -> None:
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        _start_telemetry_workload(state)

        _query(
            state,
            "SELECT msg.id FROM messages msg "
            "JOIN actors a ON msg.to_actor_id = a.id "
            "JOIN meshes m ON a.mesh_id = m.id "
            f"WHERE m.given_name IN ('{_TELEMETRY_WORKER_MESH}', "
            f"'{_TELEMETRY_COORDINATOR_MESH}') "
            "AND msg.endpoint IN ('start', 'request', 'reply')",
            min_rows=6,
        )
        actors = _query(
            state,
            "SELECT a.id FROM actors a JOIN meshes m ON a.mesh_id = m.id "
            f"WHERE m.given_name IN ('{_TELEMETRY_WORKER_MESH}', "
            f"'{_TELEMETRY_COORDINATOR_MESH}') AND m.class LIKE 'Python<%'",
            min_rows=3,
        )["id"]
        actor_ids = ", ".join(str(actor_id) for actor_id in actors)
        required_count = 3 if required_status == "Created" else 6
        _query(
            state,
            "SELECT ase.id FROM actor_status_events ase "
            "JOIN actors a ON ase.actor_id = a.id "
            "JOIN meshes m ON a.mesh_id = m.id "
            f"WHERE m.given_name IN ('{_TELEMETRY_WORKER_MESH}', "
            f"'{_TELEMETRY_COORDINATOR_MESH}') "
            f"AND ase.new_status = '{required_status}'",
            min_rows=required_count,
        )
        if required_status == "Processing":
            _query(
                state,
                "SELECT ase.id FROM actor_status_events ase "
                "WHERE ase.new_status = 'Idle' "
                f"AND ase.actor_id IN ({actor_ids})",
                min_rows=9,
            )
        status_rows = _pydict_to_rows(
            _query(
                state,
                "SELECT actor_id, timestamp_us, new_status, reason "
                "FROM actor_status_events "
                f"WHERE actor_id IN ({actor_ids})",
                min_rows=9,
            )
        )

        statuses_by_actor = {
            actor_id: {
                row["new_status"] for row in status_rows if row["actor_id"] == actor_id
            }
            for actor_id in actors
        }
        missing = {
            actor_id: statuses
            for actor_id, statuses in statuses_by_actor.items()
            if required_status not in statuses
        }
        assert not missing, missing
        if required_status == "Processing":
            for actor_id in actors:
                processing_at = max(
                    row["timestamp_us"]
                    for row in status_rows
                    if row["actor_id"] == actor_id and row["new_status"] == "Processing"
                )
                idle_at = max(
                    row["timestamp_us"]
                    for row in status_rows
                    if row["actor_id"] == actor_id and row["new_status"] == "Idle"
                )
                assert idle_at >= processing_at


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_actor_status_events_failed_actor() -> None:
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        actor_id, _ = _start_failed_actor(state)

        rows = _pydict_to_rows(
            _query(
                state,
                "SELECT new_status, reason FROM actor_status_events "
                f"WHERE actor_id = {actor_id}",
                min_rows=7,
            )
        )
        assert {row["new_status"] for row in rows} == {
            "Created",
            "Initializing",
            "Idle",
            "Processing",
            "Stopping",
            "Failed",
        }, rows
        failed_reason = next(
            row["reason"] for row in rows if row["new_status"] == "Failed"
        )
        assert "telemetry failure" in failed_reason


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_sliced_vs_full_view_rank(cleanup_callbacks) -> None:
    """Test that rank and parent_view_json are correct for sliced and full actor meshes."""
    # Spawn 3 workers so we can slice a subset
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 3}, name="rank_test_procs"
        )

        # Full view: spawn on the unsliced proc mesh (all 3 workers)
        full_actors = worker_procs.spawn("full_view_actor", WorkerActor)
        full_actors.initialized.get()

        # Sliced view: take workers 1..3 (indices 1 and 2)
        sliced_procs = worker_procs.slice(workers=slice(1, 3))
        sliced_actors = sliced_procs.spawn("sliced_view_actor", WorkerActor)
        sliced_actors.initialized.get()

        # -- Verify full-view actor mesh --
        full_mesh_dict = _query(
            state,
            "SELECT id, shape_json, parent_view_json FROM meshes "
            "WHERE given_name = 'full_view_actor'",
        )
        assert len(full_mesh_dict["id"]) == 1, (
            f"Expected 1 full_view_actor mesh, got {len(full_mesh_dict['id'])}"
        )
        full_mesh_id = full_mesh_dict["id"][0]

        # parent_view_json for full view should have offset 0
        full_view = json.loads(full_mesh_dict["parent_view_json"][0])
        assert full_view["slice"]["offset"] == 0, (
            f"Expected full view offset=0, got {full_view['slice']['offset']}"
        )
        # Full view should cover all 3 workers
        workers_label_idx = full_view["labels"].index("workers")
        assert full_view["slice"]["sizes"][workers_label_idx] == 3, (
            f"Expected full view size=3, got {full_view['slice']['sizes'][workers_label_idx]}"
        )

        # Actors in the full mesh should have ranks 0, 1, 2
        full_actors_result = _query(
            state,
            f"SELECT rank FROM actors WHERE mesh_id = {full_mesh_id} ORDER BY rank",
            min_rows=3,
        )
        full_ranks = full_actors_result["rank"]
        assert full_ranks == [0, 1, 2], f"Expected ranks [0, 1, 2], got {full_ranks}"

        # -- Verify sliced-view actor mesh --
        sliced_mesh_dict = _query(
            state,
            "SELECT id, shape_json, parent_view_json FROM meshes "
            "WHERE given_name = 'sliced_view_actor'",
        )
        assert len(sliced_mesh_dict["id"]) == 1, (
            f"Expected 1 sliced_view_actor mesh, got {len(sliced_mesh_dict['id'])}"
        )
        sliced_mesh_id = sliced_mesh_dict["id"][0]

        # parent_view_json for sliced view should have offset > 0 (starts at worker 1)
        sliced_view = json.loads(sliced_mesh_dict["parent_view_json"][0])
        assert sliced_view["slice"]["offset"] > 0, (
            f"Expected sliced view offset > 0, got {sliced_view['slice']['offset']}"
        )
        # Sliced view should cover 2 workers
        workers_label_idx = sliced_view["labels"].index("workers")
        assert sliced_view["slice"]["sizes"][workers_label_idx] == 2, (
            f"Expected sliced view size=2, got {sliced_view['slice']['sizes'][workers_label_idx]}"
        )

        # Actors in the sliced mesh should have ranks 0, 1 (0-indexed within the slice)
        sliced_actors_result = _query(
            state,
            f"SELECT rank FROM actors WHERE mesh_id = {sliced_mesh_id} ORDER BY rank",
            min_rows=2,
        )
        sliced_ranks = sliced_actors_result["rank"]
        assert sliced_ranks == [0, 1], f"Expected ranks [0, 1], got {sliced_ranks}"


@pytest.mark.timeout(120)
@isolate_in_subprocess
@pytest.mark.parametrize(
    "send_path, expected_view_labels",
    [
        # call() targets the full mesh — view Region has ["hosts", "workers"]
        ("call", ["hosts", "workers"]),
        # call_one() on a sliced single worker — workers dim collapsed, only ["hosts"]
        ("call_one", ["hosts"]),
        # broadcast() targets the full mesh — view Region has ["hosts", "workers"]
        ("broadcast", ["hosts", "workers"]),
        # choose() selects a single actor — scalar (0-dim) Region
        ("choose", []),
    ],
)
def test_sent_messages_table(
    cleanup_callbacks, send_path: str, expected_view_labels: list
) -> None:
    """Test that sent_messages are logged with correct view/shape for each send path.

    All send paths (call, call_one, broadcast, choose) go through
    cast_with_selection in actor_mesh.rs, which calls notify_sent_message
    with a SentMessageEvent containing:
      - sender_actor_id: hash of the sending actor's ActorId
      - actor_mesh_id: hash of the target (ProcMeshId, ActorMeshId)
      - view_json: serialized ndslice::Region of the current view
      - shape_json: serialized ndslice::Shape (converted from the Region)
    """
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 2})
        mesh_name = f"sent_msg_{send_path}_worker"
        workers = worker_procs.spawn(mesh_name, WorkerActor)
        workers.initialized.get()

        for _ in range(42):
            if send_path == "call":
                workers.ping.call().get()
            elif send_path == "call_one":
                workers.slice(workers=0).ping.call_one().get()
            elif send_path == "broadcast":
                workers.ping.broadcast()
            elif send_path == "choose":
                workers.ping.choose().get()

        # Verify the schema matches the shared SentMessage telemetry row.
        # (only check once, for the "call" path)
        if send_path == "call":
            result = _query(
                state,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'sent_messages' ORDER BY ordinal_position",
            )
            required_columns = {
                "id",
                "timestamp_us",
                "sender_actor_id",
                "actor_mesh_id",
                "view_json",
                "shape_json",
            }
            column_names = set(result.get("column_name", []))
            assert required_columns.issubset(column_names), column_names

        # Verify 42 sent_messages join with the correct mesh
        joined = _query(
            state,
            "SELECT COUNT(*) AS cnt FROM sent_messages sm JOIN meshes m "
            f"ON sm.actor_mesh_id = m.id WHERE m.given_name = '{mesh_name}' "
            "HAVING COUNT(*) = 42",
        )
        joined_count = joined["cnt"][0]
        assert joined_count == 42, (
            f"Expected 42 sent_messages via {send_path}, got {joined_count}"
        )

        actor_joined_dict = _query(
            state,
            "SELECT COUNT(DISTINCT sm.id) AS message_count, "
            "COUNT(DISTINCT a.id) AS actor_count "
            "FROM sent_messages sm "
            "JOIN actors a ON sm.actor_mesh_id = a.mesh_id "
            "JOIN meshes m ON a.mesh_id = m.id "
            f"WHERE m.given_name = '{mesh_name}' "
            "HAVING COUNT(DISTINCT sm.id) = 42 AND COUNT(DISTINCT a.id) = 2",
        )
        joined_message_count = actor_joined_dict["message_count"][0]
        joined_actor_count = actor_joined_dict["actor_count"][0]
        assert joined_message_count == 42, (
            "Expected sent_messages.actor_mesh_id to join actors.mesh_id for "
            f"{send_path}, got {joined_message_count} messages"
        )
        assert joined_actor_count == 2, (
            f"Expected 2 target actors for {send_path}, got {joined_actor_count}"
        )

        # Verify view_json (ndslice Region) and shape_json (ndslice Shape).
        # Region serializes as {"labels": [...], "slice": {"offset": ..., "sizes": [...], "strides": [...]}}.
        # Shape is Region converted via Region::into::<Shape>, same serialization format.
        mesh = _query(state, f"SELECT id FROM meshes WHERE given_name = '{mesh_name}'")
        mesh_id = mesh["id"][0]
        msgs_dict = _query(
            state,
            f"SELECT view_json, shape_json FROM sent_messages "
            f"WHERE actor_mesh_id = {mesh_id} LIMIT 1",
        )
        view = json.loads(msgs_dict["view_json"][0])
        shape = json.loads(msgs_dict["shape_json"][0])

        assert view["labels"] == expected_view_labels, (
            f"Expected {send_path}() view labels={expected_view_labels}, got {view['labels']}"
        )
        assert shape["labels"] == expected_view_labels, (
            f"Expected {send_path}() shape labels={expected_view_labels}, got {shape['labels']}"
        )

        # For paths that target the full mesh, verify workers size=2
        if "workers" in expected_view_labels:
            workers_idx = view["labels"].index("workers")
            assert view["slice"]["sizes"][workers_idx] == 2, (
                f"Expected {send_path}() view workers size=2, "
                f"got {view['slice']['sizes'][workers_idx]}"
            )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_telemetry_workload_sent_message_fanout() -> None:
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        _start_telemetry_workload(state)

        rows = _pydict_to_rows(
            _query(
                state,
                "SELECT sm.id, sm.timestamp_us, m.given_name "
                "FROM sent_messages sm JOIN meshes m ON sm.actor_mesh_id = m.id "
                f"WHERE m.given_name IN ('{_TELEMETRY_WORKER_MESH}', "
                f"'{_TELEMETRY_COORDINATOR_MESH}') "
                "AND m.class LIKE 'Python<%'",
                min_rows=5,
            )
        )
        assert len(rows) == 5, rows
        assert Counter(row["given_name"] for row in rows) == {
            _TELEMETRY_WORKER_MESH: 3,
            _TELEMETRY_COORDINATOR_MESH: 2,
        }
        assert len({row["id"] for row in rows}) == 5
        assert all(row["timestamp_us"] > 0 for row in rows)


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_messages_table(cleanup_callbacks) -> None:
    """Test that the messages table is populated when messages are received."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="msg_workers_procs"
        )
        workers = worker_procs.spawn("msg_test_worker", WorkerActor)
        workers.initialized.get()

        # Send several messages to trigger telemetry
        for _ in range(5):
            workers.ping.call().get()

        # Verify schema
        result = _query(
            state,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'messages' ORDER BY ordinal_position",
        )
        column_names = result.get("column_name", [])
        assert column_names == [
            "id",
            "timestamp_us",
            "from_actor_id",
            "to_actor_id",
            "endpoint",
            "port_index",
        ], f"Unexpected columns: {column_names}"

        # Verify rows exist
        result_dict = _query(state, "SELECT * FROM messages")
        row_count = len(result_dict.get("id", []))
        assert row_count > 0, f"Expected messages, got {row_count}"

        # Verify to_actor_id joins with actors table (receiver is a known actor)
        joined = _query(
            state,
            "SELECT m.id, m.from_actor_id FROM messages m "
            "JOIN actors a ON m.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'msg_test_worker'",
            min_rows=10,
        )
        joined_count = len(joined.get("id", []))
        # 5 casts x 2 workers = 10 messages received by msg_test_worker actors
        assert joined_count == 10, (
            f"Expected 10 messages received by msg_test_worker, got {joined_count}"
        )

        sent = _query(
            state,
            "SELECT sm.sender_actor_id FROM sent_messages sm "
            "JOIN meshes mesh ON sm.actor_mesh_id = mesh.id "
            "WHERE mesh.given_name = 'msg_test_worker'",
            min_rows=5,
        )
        assert len(sent["sender_actor_id"]) == 5, sent
        expected_sender_ids = set(sent["sender_actor_id"])
        assert len(expected_sender_ids) == 1, sent
        assert set(joined["from_actor_id"]) == expected_sender_ids, {
            "expected_sender_ids": expected_sender_ids,
            "received_sender_ids": set(joined["from_actor_id"]),
        }

        ports_by_actor = _query(
            state,
            "SELECT m.to_actor_id, COUNT(*) AS message_count, "
            "COUNT(DISTINCT m.port_index) AS port_count "
            "FROM messages m "
            "JOIN actors a ON m.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'msg_test_worker' "
            "AND m.endpoint = 'ping' AND m.port_index IS NOT NULL "
            "GROUP BY m.to_actor_id "
            "ORDER BY m.to_actor_id",
            min_rows=2,
        )
        assert len(ports_by_actor["to_actor_id"]) == 2, ports_by_actor
        assert ports_by_actor["message_count"] == [5, 5], ports_by_actor
        assert ports_by_actor["port_count"] == [1, 1], ports_by_actor


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_telemetry_workload_message_fanout() -> None:
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        _start_telemetry_workload(state)

        rows = _pydict_to_rows(
            _query(
                state,
                "SELECT msg.endpoint, target.given_name "
                "FROM messages msg "
                "JOIN actors receiver ON msg.to_actor_id = receiver.id "
                "JOIN meshes target ON receiver.mesh_id = target.id "
                f"WHERE target.given_name IN ('{_TELEMETRY_WORKER_MESH}', "
                f"'{_TELEMETRY_COORDINATOR_MESH}') "
                "AND msg.endpoint IN ('start', 'request', 'reply')",
                min_rows=6,
            )
        )
        assert len(rows) == 6, rows
        assert Counter(row["endpoint"] for row in rows) == {
            "start": 2,
            "request": 2,
            "reply": 2,
        }
        assert Counter(row["given_name"] for row in rows) == {
            _TELEMETRY_WORKER_MESH: 4,
            _TELEMETRY_COORDINATOR_MESH: 2,
        }


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_messages_endpoint(cleanup_callbacks) -> None:
    """Test that the messages table endpoint column is populated with the method name."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="ep_workers_procs"
        )
        workers = worker_procs.spawn("ep_test_worker", WorkerActor)
        workers.initialized.get()

        # Call the "ping" endpoint
        for _ in range(3):
            workers.ping.call().get()

        result_dict = _query(
            state,
            "SELECT m.endpoint FROM messages m "
            "JOIN actors a ON m.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'ep_test_worker' AND m.endpoint IS NOT NULL",
            min_rows=6,
        )
        endpoints = result_dict.get("endpoint", [])

        # 3 casts x 2 workers = 6 messages, all with endpoint "ping"
        assert len(endpoints) == 6, (
            f"Expected 6 messages with endpoint, got {len(endpoints)}"
        )
        assert all(ep == "ping" for ep in endpoints), (
            f"Expected all endpoints to be 'ping', got {set(endpoints)}"
        )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_message_status_events_table() -> None:
    """Test the complete status lifecycle for successful received messages."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        _start_telemetry_workload(state)

        result = _query(
            state,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'message_status_events' ORDER BY ordinal_position",
        )
        column_names = result.get("column_name", [])
        assert column_names == [
            "id",
            "timestamp_us",
            "message_id",
            "status",
        ], f"Unexpected columns: {column_names}"

        messages = _pydict_to_rows(
            _query(
                state,
                "SELECT msg.id, msg.timestamp_us FROM messages msg "
                "JOIN actors receiver ON msg.to_actor_id = receiver.id "
                "JOIN meshes target ON receiver.mesh_id = target.id "
                f"WHERE target.given_name IN ('{_TELEMETRY_WORKER_MESH}', "
                f"'{_TELEMETRY_COORDINATOR_MESH}') "
                "AND msg.endpoint IN ('start', 'request', 'reply')",
                min_rows=6,
            )
        )
        assert len(messages) == 6, messages
        message_timestamps = {
            message["id"]: message["timestamp_us"] for message in messages
        }
        message_ids = ", ".join(str(message_id) for message_id in message_timestamps)

        events = _pydict_to_rows(
            _query(
                state,
                "SELECT id, timestamp_us, message_id, status "
                "FROM message_status_events "
                f"WHERE message_id IN ({message_ids})",
                min_rows=18,
            )
        )
        assert len(events) == 18, events
        assert len({event["id"] for event in events}) == 18
        assert Counter(event["status"] for event in events) == {
            "queued": 6,
            "active": 6,
            "complete": 6,
        }
        assert {event["message_id"] for event in events} == set(message_timestamps)

        for message_id, message_timestamp in message_timestamps.items():
            message_events = [
                event for event in events if event["message_id"] == message_id
            ]
            assert Counter(event["status"] for event in message_events) == {
                "queued": 1,
                "active": 1,
                "complete": 1,
            }
            timestamps = {
                event["status"]: event["timestamp_us"] for event in message_events
            }
            assert timestamps["active"] == message_timestamp
            assert timestamps["complete"] >= timestamps["active"]


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_message_status_events_failed_handler() -> None:
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        _, message_id = _start_failed_actor(state)

        events = _pydict_to_rows(
            _query(
                state,
                "SELECT status, timestamp_us FROM message_status_events "
                f"WHERE message_id = {message_id}",
                min_rows=3,
            )
        )
        statuses = {event["status"] for event in events}
        assert "failed" in statuses and "complete" not in statuses, events


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_sent_messages_with_sliced_mesh(cleanup_callbacks) -> None:
    """Test that sent_messages view_json/shape_json reflect sliced vs full actor mesh casts."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 4}, name="sm_slice_procs")

        # Spawn actors on the full proc mesh
        actors = worker_procs.spawn("sm_actors", WorkerActor)
        actors.initialized.get()

        # Cast to the full actor mesh (all 4 workers)
        actors.ping.call().get()

        # Slice the actor mesh and cast to the slice (workers 1..3, i.e. 2 workers)
        sliced_actors = actors.slice(workers=slice(1, 3))
        sliced_actors.ping.call().get()

        # Both casts target the same actor mesh, so actor_mesh_id is the same.
        # The view_json distinguishes full vs sliced.
        mesh = _query(state, "SELECT id FROM meshes WHERE given_name = 'sm_actors'")
        mesh_id = mesh["id"][0]

        _query(
            state,
            f"SELECT COUNT(*) AS cnt FROM sent_messages "
            f"WHERE actor_mesh_id = {mesh_id} HAVING COUNT(*) = 2",
        )
        msgs_dict = _query(
            state,
            f"SELECT view_json, shape_json FROM sent_messages "
            f"WHERE actor_mesh_id = {mesh_id} ORDER BY timestamp_us",
            min_rows=2,
        )
        assert len(msgs_dict["view_json"]) == 2, (
            f"Expected 2 sent messages, got {len(msgs_dict['view_json'])}"
        )

        # First cast: full mesh (all 4 workers)
        full_view = json.loads(msgs_dict["view_json"][0])
        workers_idx = full_view["labels"].index("workers")
        assert full_view["slice"]["sizes"][workers_idx] == 4, (
            f"Expected full view size=4, got {full_view['slice']['sizes'][workers_idx]}"
        )

        # Second cast: sliced mesh (2 workers, offset > 0)
        sliced_view = json.loads(msgs_dict["view_json"][1])
        workers_idx = sliced_view["labels"].index("workers")
        assert sliced_view["slice"]["sizes"][workers_idx] == 2, (
            f"Expected sliced view size=2, got {sliced_view['slice']['sizes'][workers_idx]}"
        )
        assert sliced_view["slice"]["offset"] > 0, (
            f"Expected sliced view offset > 0, got {sliced_view['slice']['offset']}"
        )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_sent_messages_sender_actor_id(cleanup_callbacks) -> None:
    """Test that sender_actor_id identifies the actor that initiated the cast,
    not the target actor, when one actor casts to another actor mesh."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="sender_test_procs"
        )

        # Spawn target actors on the full proc mesh
        targets = worker_procs.spawn("target_workers", WorkerActor)
        targets.initialized.get()

        # Spawn a single sender actor on worker 0
        sender = worker_procs.slice(workers=0).spawn("sender_actor", SenderActor)
        sender.initialized.get()

        # SenderActor casts to the target actor mesh from within its endpoint
        sender.send_ping.call_one(targets).get()

        # Find the sent_messages row targeting the "target_workers" mesh
        target_mesh = _query(
            state, "SELECT id FROM meshes WHERE given_name = 'target_workers'"
        )
        target_mesh_id = target_mesh["id"][0]

        msgs_dict = _query(
            state,
            f"SELECT sender_actor_id FROM sent_messages "
            f"WHERE actor_mesh_id = {target_mesh_id}",
        )
        assert len(msgs_dict["sender_actor_id"]) > 0, (
            "Expected at least one sent message targeting 'target_workers'"
        )

        # The sender_actor_id should match an actor in the "sender_actor" mesh,
        # not an actor in the "target_workers" mesh.
        sender_mesh = _query(
            state, "SELECT id FROM meshes WHERE given_name = 'sender_actor'"
        )
        sender_mesh_id = sender_mesh["id"][0]

        sender_actors = _query(
            state,
            f"SELECT id, display_name FROM actors WHERE mesh_id = {sender_mesh_id}",
        )
        sender_actor_ids = set(sender_actors["id"])

        target_actors = _query(
            state, f"SELECT id FROM actors WHERE mesh_id = {target_mesh_id}"
        )
        target_actor_ids = set(target_actors["id"])

        for sender_id in msgs_dict["sender_actor_id"]:
            assert sender_id in sender_actor_ids, (
                f"sender_actor_id {sender_id} should be a sender actor, "
                f"not a target actor. sender_actor_ids={sender_actor_ids}, "
                f"target_actor_ids={target_actor_ids}"
            )
            assert sender_id not in target_actor_ids, (
                f"sender_actor_id {sender_id} should NOT be a target actor"
            )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_query_after_stopping_proc_mesh(cleanup_callbacks) -> None:
    """Test that query still works after a user-spawned actor's proc mesh is stopped."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="stop_test_procs"
        )

        # Spawn and initialize a user actor
        workers = worker_procs.spawn("stop_test_worker", WorkerActor)
        workers.initialized.get()

        # Send messages to the workers so the messages table is populated
        # on the child processes (notify_message fires on the receiver's process).
        workers.ping.call().get()

        # Verify the actor appears in the actors table before stopping
        result = _query(
            state,
            "SELECT a.id FROM actors a "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'stop_test_worker'",
        )
        pre_stop_count = len(result.get("id", []))
        assert pre_stop_count > 0, "Expected stop_test_worker actors before stopping"

        # Verify received messages exist before stopping. The messages table is
        # populated on the child process via notify_message, so these records
        # come from the child scanner.
        pre_stop_msgs = _query(
            state,
            "SELECT m.id FROM messages m "
            "JOIN actors a ON m.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'stop_test_worker'",
            min_rows=2,
        )
        pre_stop_msg_count = len(pre_stop_msgs.get("id", []))
        assert pre_stop_msg_count > 0, (
            "Expected received messages for stop_test_worker before stopping"
        )

        # Stop the proc mesh — this kills both user actors AND telemetry actors on it.
        # The coordinator's _children list still references the dead telemetry actors.
        worker_procs.stop().get()

        # Query should still work after the proc mesh is stopped.
        # The distributed telemetry scan must handle stopped children gracefully.
        result_dict = _query(state, "SELECT * FROM actors")
        actor_count = len(result_dict.get("id", []))
        assert actor_count > 0, (
            f"Expected actors in query result after stopping proc mesh, got {actor_count}"
        )

        # The stopped actor should still appear in historical data since
        # it's event was emitted from the root client process.
        matching_actors = _query(
            state,
            "SELECT a.id FROM actors a "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'stop_test_worker'",
        )
        post_stop_count = len(matching_actors.get("id", []))
        assert post_stop_count > 0, (
            "Expected stop_test_worker actors to remain queryable after stop"
        )

        # Sidecar ingestion stores received messages in the sidecar collector,
        # so rows remain queryable after the worker proc mesh stops.
        post_stop_msgs = _query(
            state,
            "SELECT COUNT(*) AS cnt FROM messages m "
            "JOIN actors a ON m.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'stop_test_worker' "
            f"HAVING COUNT(*) = {pre_stop_msg_count}",
        )
        post_stop_msg_count = post_stop_msgs["cnt"][0]
        assert post_stop_msg_count == pre_stop_msg_count, (
            f"Expected {pre_stop_msg_count} received messages after stopping proc mesh, "
            f"got {post_stop_msg_count} (was {pre_stop_msg_count} before stop)"
        )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_query_after_stopping_actor_mesh(cleanup_callbacks) -> None:
    """Test that stopping a user ActorMesh does not affect telemetry queries.

    Stopping an ActorMesh is a user-initiated action that does not trigger
    __supervise__ on the telemetry coordinator. The telemetry actors on the
    ProcMesh remain alive, so all data (including process-local tables like
    messages) is still queryable.
    """
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="actor_stop_test_procs"
        )

        # Spawn and initialize a user actor
        workers = worker_procs.spawn("actor_stop_worker", WorkerActor)
        workers.initialized.get()

        # Send messages so the messages table is populated on child processes
        workers.ping.call().get()

        # Verify received messages exist before stopping
        pre_stop_msgs = _query(
            state,
            "SELECT m.id FROM messages m "
            "JOIN actors a ON m.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'actor_stop_worker'",
            min_rows=2,
        )
        pre_stop_msg_count = len(pre_stop_msgs.get("id", []))
        assert pre_stop_msg_count > 0, (
            "Expected received messages for actor_stop_worker before stopping"
        )
        pre_stop_statuses = _query(
            state,
            "SELECT mse.id FROM message_status_events mse "
            "JOIN messages msg ON mse.message_id = msg.id "
            "JOIN actors a ON msg.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'actor_stop_worker'",
            min_rows=6,
        )
        pre_stop_status_count = len(pre_stop_statuses["id"])

        # Stop only the user ActorMesh, not the ProcMesh.
        # The telemetry actors on the ProcMesh remain alive.
        cast(ActorMesh[WorkerActor], workers).stop().get()

        status_rows = _pydict_to_rows(
            _query(
                state,
                "SELECT ase.actor_id, ase.new_status, ase.reason "
                "FROM actor_status_events ase "
                "JOIN actors a ON ase.actor_id = a.id "
                "JOIN meshes m ON a.mesh_id = m.id "
                "WHERE m.given_name = 'actor_stop_worker'",
                min_rows=14,
            )
        )
        statuses_by_actor = {
            actor_id: {
                row["new_status"] for row in status_rows if row["actor_id"] == actor_id
            }
            for actor_id in {row["actor_id"] for row in status_rows}
        }
        assert all(
            statuses
            == {"Created", "Initializing", "Idle", "Processing", "Stopping", "Stopped"}
            for statuses in statuses_by_actor.values()
        ), statuses_by_actor
        assert all(
            row["reason"] for row in status_rows if row["new_status"] == "Stopped"
        )

        sent_after_stop = _query(
            state,
            "SELECT sm.id FROM sent_messages sm "
            "JOIN meshes m ON sm.actor_mesh_id = m.id "
            "WHERE m.given_name = 'actor_stop_worker'",
        )
        sent_after_stop_count = len(sent_after_stop["id"])

        monarch.actor.unhandled_fault_hook = lambda failure: None
        with pytest.raises(SupervisionError, match="stopped"):
            workers.ping.call().get()

        failed_send = _query(
            state,
            "SELECT sm.id FROM sent_messages sm "
            "JOIN meshes m ON sm.actor_mesh_id = m.id "
            "WHERE m.given_name = 'actor_stop_worker'",
            min_rows=sent_after_stop_count + 1,
        )
        assert len(failed_send["id"]) == sent_after_stop_count + 1

        client_processing = _query(
            state,
            "SELECT ase.id FROM actor_status_events ase "
            "JOIN actors a ON ase.actor_id = a.id "
            "WHERE a.display_name = '<root>' "
            "AND ase.new_status = 'Processing'",
        )
        assert client_processing["id"]

        # Query should still work — the telemetry children are unaffected
        result_dict = _query(state, "SELECT * FROM actors")
        actor_count = len(result_dict.get("id", []))
        assert actor_count > 0, (
            f"Expected actors after stopping user ActorMesh, got {actor_count}"
        )

        # The stopped actor should still appear in the actors table
        matching_actors = _query(
            state,
            "SELECT a.id FROM actors a "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'actor_stop_worker'",
        )
        post_stop_count = len(matching_actors.get("id", []))
        assert post_stop_count > 0, (
            "Expected actor_stop_worker actors to remain queryable after stop"
        )

        # Unlike stopping a ProcMesh, received messages are NOT lost because
        # the telemetry actors and their scanners are still alive.
        post_stop_msgs = _query(
            state,
            "SELECT m.id FROM messages m "
            "JOIN actors a ON m.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'actor_stop_worker'",
            min_rows=pre_stop_msg_count,
        )
        post_stop_msg_count = len(post_stop_msgs.get("id", []))
        assert post_stop_msg_count == pre_stop_msg_count, (
            f"Expected {pre_stop_msg_count} received messages after stopping ActorMesh, "
            f"got {post_stop_msg_count} (data should be preserved)"
        )
        post_stop_statuses = _query(
            state,
            "SELECT mse.id FROM message_status_events mse "
            "JOIN messages msg ON mse.message_id = msg.id "
            "JOIN actors a ON msg.to_actor_id = a.id "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'actor_stop_worker'",
            min_rows=pre_stop_status_count,
        )
        assert len(post_stop_statuses["id"]) == pre_stop_status_count


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_store_pyspy_dump_and_query(cleanup_callbacks) -> None:
    """Store a py-spy dump via the sidecar API, query it back via SQL."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)

        pyspy_json = json.dumps(
            {
                "Ok": {
                    "pid": 1234,
                    "binary": "python3",
                    "stack_traces": [
                        {
                            "pid": 1234,
                            "thread_id": 1,
                            "thread_name": "MainThread",
                            "os_thread_id": 100,
                            "active": True,
                            "owns_gil": True,
                            "frames": [
                                {
                                    "name": "stalling_fn",
                                    "filename": "app.py",
                                    "module": "app",
                                    "short_filename": "app.py",
                                    "line": 10,
                                    "locals": [
                                        {
                                            "name": "x",
                                            "addr": 100,
                                            "arg": True,
                                            "repr": "42",
                                        },
                                        {
                                            "name": "y",
                                            "addr": 200,
                                            "arg": False,
                                            "repr": None,
                                        },
                                    ],
                                    "is_entry": False,
                                },
                                {
                                    "name": "main",
                                    "filename": "app.py",
                                    "module": "app",
                                    "short_filename": "app.py",
                                    "line": 5,
                                    "locals": [
                                        {
                                            "name": "z",
                                            "addr": 300,
                                            "arg": True,
                                            "repr": "'hello'",
                                        },
                                    ],
                                    "is_entry": True,
                                },
                            ],
                        }
                    ],
                    "warnings": [],
                }
            }
        )

        _store_pyspy_dump(state, "dump-1", "proc[0]", pyspy_json)

        result_dict = _query(
            state,
            "SELECT name, line FROM pyspy_frames "
            "WHERE dump_id = 'dump-1' ORDER BY frame_depth",
            min_rows=2,
        )
        assert len(result_dict["name"]) == 2
        assert result_dict["name"] == ["stalling_fn", "main"]

        # Query local variables
        locals_dict = _query(
            state,
            "SELECT name, addr, arg, repr, frame_depth FROM pyspy_local_variables "
            "WHERE dump_id = 'dump-1' ORDER BY frame_depth, name",
            min_rows=3,
        )
        assert len(locals_dict["name"]) == 3
        assert locals_dict["name"] == ["x", "y", "z"]
        assert locals_dict["addr"] == [100, 200, 300]
        assert locals_dict["arg"] == [True, False, True]
        assert locals_dict["repr"] == ["42", None, "'hello'"]
        assert locals_dict["frame_depth"] == [0, 0, 1]


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_pyspy_tables_in_information_schema(cleanup_callbacks) -> None:
    """py-spy tables are visible in information_schema."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        result = _query(
            state,
            "SELECT table_name FROM information_schema.tables ORDER BY table_name",
        )
        table_names = result.get("table_name", [])
        assert "pyspy_dumps" in table_names
        assert "pyspy_stack_traces" in table_names
        assert "pyspy_frames" in table_names
        assert "pyspy_local_variables" in table_names


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_store_pyspy_dump_with_child_proc_ref(cleanup_callbacks) -> None:
    """store_pyspy_dump stores data with a child proc_ref."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="pyspy_route_procs"
        )
        workers = worker_procs.spawn("pyspy_route_worker", WorkerActor)
        workers.initialized.get()

        # Discover child proc_refs by parsing canonical ActorAddr strings for
        # non-local ProcAgent actors. display_name is reserved for user-facing
        # names, so the canonical full_name is the stable source of system actor
        # identity.
        proc_agents = _query(state, "SELECT full_name FROM actors")
        proc_agent_names = proc_agents.get("full_name", [])
        child_proc_refs = [
            str(actor_id.proc_id)
            for row in proc_agent_names
            if (actor_id := ActorAddr.from_string(row)).label
            in {"proc_agent", "_proc_agent"}
            and actor_id.proc_label != "local"
        ]
        assert len(child_proc_refs) > 0, f"Expected child proc_refs, got: {proc_agents}"
        child_proc_ref = child_proc_refs[0]

        pyspy_json = json.dumps(
            {
                "Ok": {
                    "pid": 9999,
                    "binary": "python3",
                    "stack_traces": [
                        {
                            "pid": 9999,
                            "thread_id": 1,
                            "thread_name": "MainThread",
                            "os_thread_id": 200,
                            "active": True,
                            "owns_gil": True,
                            "frames": [
                                {
                                    "name": "child_fn",
                                    "filename": "child.py",
                                    "module": "child",
                                    "short_filename": "child.py",
                                    "line": 42,
                                    "locals": [],
                                    "is_entry": True,
                                }
                            ],
                        }
                    ],
                    "warnings": [],
                }
            }
        )

        # Store a pyspy dump targeting the child proc_ref on the root actor.
        result = _store_pyspy_dump(state, "child-dump-1", child_proc_ref, pyspy_json)
        assert result["status"] == "ok"

        # The dump should be queryable via distributed scan.
        frames_dict = _query(
            state, "SELECT name, line FROM pyspy_frames WHERE dump_id = 'child-dump-1'"
        )
        assert frames_dict["name"] == ["child_fn"]
        assert frames_dict["line"] == [42]

        # Verify the dump's proc_ref is stored correctly.
        dumps = _query(
            state, "SELECT proc_ref FROM pyspy_dumps WHERE dump_id = 'child-dump-1'"
        )
        assert dumps["proc_ref"] == [child_proc_ref]


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_store_pyspy_dump_with_unknown_proc_ref(cleanup_callbacks) -> None:
    """store_pyspy_dump stores data even for unknown proc_ref values."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="pyspy_fallback_procs"
        )
        workers = worker_procs.spawn("pyspy_fallback_worker", WorkerActor)
        workers.initialized.get()

        # Trigger child spawning.
        _query(state, "SELECT COUNT(*) AS cnt FROM actors HAVING COUNT(*) > 0")

        pyspy_json = json.dumps(
            {
                "Ok": {
                    "pid": 7777,
                    "binary": "python3",
                    "stack_traces": [
                        {
                            "pid": 7777,
                            "thread_id": 1,
                            "thread_name": "MainThread",
                            "os_thread_id": 300,
                            "active": True,
                            "owns_gil": False,
                            "frames": [
                                {
                                    "name": "orphan_fn",
                                    "filename": "orphan.py",
                                    "module": "orphan",
                                    "short_filename": "orphan.py",
                                    "line": 99,
                                    "locals": [],
                                    "is_entry": True,
                                }
                            ],
                        }
                    ],
                    "warnings": [],
                }
            }
        )

        # Store with a proc_ref that doesn't exist in the tree.
        result = _store_pyspy_dump(
            state, "orphan-dump-1", "nonexistent.proc[999]", pyspy_json
        )
        assert result["status"] == "ok"

        # The dump should be queryable (stored on root).
        frames_dict = _query(
            state, "SELECT name, line FROM pyspy_frames WHERE dump_id = 'orphan-dump-1'"
        )
        assert frames_dict["name"] == ["orphan_fn"]
        assert frames_dict["line"] == [99]

        # Verify proc_ref is preserved even though it didn't match any proc.
        dumps = _query(
            state, "SELECT proc_ref FROM pyspy_dumps WHERE dump_id = 'orphan-dump-1'"
        )
        assert dumps["proc_ref"] == ["nonexistent.proc[999]"]


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_json_columns_are_valid_json() -> None:
    """Test that all view_json and shape_json columns contain valid JSON."""
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)

        # Spawn actors and send messages to populate all tables that have JSON columns:
        # - meshes: shape_json, parent_view_json
        # - sent_messages: view_json, shape_json
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="json_test_procs"
        )
        workers = worker_procs.spawn("json_test_worker", WorkerActor)
        workers.initialized.get()

        # Send messages to populate sent_messages
        workers.ping.call().get()

        # -- Verify meshes.shape_json --
        result_dict = _query(state, "SELECT given_name, shape_json FROM meshes")
        for name, shape in zip(result_dict["given_name"], result_dict["shape_json"]):
            assert shape is not None and shape != "", (
                f"meshes.shape_json is empty for mesh '{name}'"
            )
            try:
                json.loads(shape)
            except json.JSONDecodeError as e:
                raise AssertionError(
                    f"meshes.shape_json is not valid JSON for mesh '{name}': {shape!r}"
                ) from e

        # -- Verify meshes.parent_view_json (nullable) --
        result_dict = _query(
            state,
            "SELECT given_name, parent_view_json FROM meshes "
            "WHERE parent_view_json IS NOT NULL",
        )
        for name, view in zip(
            result_dict["given_name"], result_dict["parent_view_json"]
        ):
            try:
                json.loads(view)
            except json.JSONDecodeError as e:
                raise AssertionError(
                    f"meshes.parent_view_json is not valid JSON for mesh '{name}': {view!r}"
                ) from e

        # -- Verify sent_messages.view_json --
        result_dict = _query(state, "SELECT id, view_json FROM sent_messages")
        assert len(result_dict["id"]) > 0, "Expected sent_messages rows"
        for msg_id, view in zip(result_dict["id"], result_dict["view_json"]):
            assert view is not None and view != "", (
                f"sent_messages.view_json is empty for id={msg_id}"
            )
            try:
                json.loads(view)
            except json.JSONDecodeError as e:
                raise AssertionError(
                    f"sent_messages.view_json is not valid JSON for id={msg_id}: {view!r}"
                ) from e

        # -- Verify sent_messages.shape_json --
        result_dict = _query(state, "SELECT id, shape_json FROM sent_messages")
        for msg_id, shape in zip(result_dict["id"], result_dict["shape_json"]):
            assert shape is not None and shape != "", (
                f"sent_messages.shape_json is empty for id={msg_id}"
            )
            try:
                json.loads(shape)
            except json.JSONDecodeError as e:
                raise AssertionError(
                    f"sent_messages.shape_json is not valid JSON for id={msg_id}: {shape!r}"
                ) from e


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_per_table_row_retention(cleanup_callbacks) -> None:
    """Test that time-based retention deletes old rows from message tables."""

    # Use a 1-second retention window so rows expire quickly.
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(
            _sidecar_telemetry_config(retention_secs=1)
        ),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 8}, name="worker_procs")
        workers = worker_procs.spawn("workers", WorkerActor)
        workers.initialized.get()

        for _ in range(50):
            workers.ping.call().get()

        # Verify events exist before retention kicks in.
        _query(state, "SELECT id FROM message_status_events LIMIT 1")
        before = _query(
            state,
            "SELECT COUNT(*) AS cnt FROM message_status_events HAVING COUNT(*) > 0",
        )
        before_count = before["cnt"][0]
        assert before_count > 0, "Expected message_status_events rows before retention"

        # Wait for the 1-second retention window to expire, then query again.
        # The query triggers flush(), which applies retention and trims old rows.
        time.sleep(2)

        after = _query(
            state,
            f"SELECT COUNT(*) AS cnt FROM message_status_events HAVING COUNT(*) < {before_count}",
        )
        after_count = after["cnt"][0]
        assert after_count < before_count, (
            f"Expected fewer rows after retention, got {after_count} vs {before_count}"
        )


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_scan_timeout_on_dead_child(cleanup_callbacks) -> None:
    """Test that scan completes with partial results when a child times out.

    Stops a child proc mesh and patches the scan timeout to a short value,
    then verifies the query completes within a bounded time instead of hanging.
    """
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(
            per_host={"workers": 2}, name="timeout_test_procs"
        )

        workers = worker_procs.spawn("timeout_test_worker", WorkerActor)
        workers.initialized.get()
        workers.ping.call().get()

        # Verify data exists before stopping
        result = _query(
            state,
            "SELECT a.id FROM actors a "
            "JOIN meshes mesh ON a.mesh_id = mesh.id "
            "WHERE mesh.given_name = 'timeout_test_worker'",
        )
        pre_count = len(result.get("id", []))
        assert pre_count > 0, "Expected timeout_test_worker actors before stopping"

        # Stop the proc mesh to kill child telemetry actors
        worker_procs.stop().get()

        # Patch the timeout to a short value so the test doesn't wait 10s
        with unittest.mock.patch.object(
            job_telemetry_actor, "_SCAN_WORKER_TIMEOUT_SECS", 1.0
        ):
            start = time.monotonic()
            result_dict = _query(state, "SELECT * FROM actors")
            elapsed = time.monotonic() - start

            # The query should complete — not hang forever
            actor_count = len(result_dict.get("id", []))
            assert actor_count > 0, (
                f"Expected actors in result after child timeout, got {actor_count}"
            )

            # Should complete well within the test timeout (60s).
            # With a 1s scan timeout, expect completion in a few seconds.
            assert elapsed < 15, (
                f"Query took {elapsed:.1f}s — expected it to complete quickly "
                f"with 1s child scan timeout"
            )


# --- Snapshot integration tests ---
#
# These tests verify that introspection snapshot tables are
# pre-registered into the telemetry sidecar query surface and that
# periodic capture populates them through the sidecar query path.


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_snapshot_schemas_pre_registered(cleanup_callbacks) -> None:
    """Snapshot table schemas are always present in the query surface.

    Even with default config (no periodic timer), the 11 snapshot
    tables should be visible in information_schema and queryable
    with 0 rows. This ensures the query schema does not depend on
    whether periodic snapshots are enabled.

    SI-1 (discoverable), SI-6 (unconditional schemas); see snapshot
    integration invariants in monarch_introspection_snapshot::integration.
    """
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)
        result = _query(
            state,
            "SELECT table_name FROM information_schema.tables ORDER BY table_name",
        )
        table_names = result.get("table_name", [])

        expected_snapshot_tables = [
            "actor_failures",
            "actor_inbound_orderings",
            "actor_nodes",
            "children",
            "host_nodes",
            "nodes",
            "ordering_sessions",
            "proc_nodes",
            "resolution_errors",
            "root_nodes",
            "snapshots",
        ]
        for table in expected_snapshot_tables:
            assert table in table_names, (
                f"snapshot table '{table}' should be pre-registered"
            )

        # All snapshot tables should be queryable with 0 rows.
        for table in expected_snapshot_tables:
            count_result = _query(
                state, f"SELECT COUNT(*) AS cnt FROM {table} HAVING COUNT(*) = 0"
            )
            cnt = count_result["cnt"][0]
            assert cnt == 0, (
                f"'{table}' should have 0 rows before any capture, got {cnt}"
            )


@pytest.mark.timeout(180)
@isolate_in_subprocess
def test_snapshot_periodic_capture_populates_tables(cleanup_callbacks) -> None:
    """Periodic snapshots become queryable through the live query path.

    With periodic capture enabled, the timer fires and the full
    snapshot relational model (nodes, children, subtype tables)
    becomes queryable through the telemetry sidecar. The test
    verifies this by tracing the ancestry of a known actor through
    the snapshot tables using a recursive CTE.

    SI-1 (discoverable), SI-2 (queryable); see snapshot integration
    invariants in monarch_introspection_snapshot::integration.
    """
    with scoped_state(
        ProcessJob({"hosts": 1})
        .enable_telemetry(_sidecar_telemetry_config(snapshot_interval_secs=5))
        .enable_admin(
            MeshAdminConfig(
                # Use an ephemeral admin port so concurrent --stress-runs
                # replicas do not contend on the default fixed mesh-admin
                # port.
                admin_addr="[::]:0",
            )
        ),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)

        # Spawn a worker so the mesh has content to snapshot.
        hosts = state.hosts
        worker_procs = hosts.spawn_procs(per_host={"workers": 1}, name="snap_procs")
        workers = worker_procs.spawn("snap_worker", WorkerActor)
        workers.initialized.get()

        # --- Relational coherence proof ---
        #
        # Find the snap_worker actor whose direct proc parent is
        # snap_procs, from the most recent snapshot containing one.
        # This proves the full snapshot model (nodes, children,
        # actor_nodes, proc_nodes, host_nodes, root_nodes) is
        # populated and relationally coherent through the live
        # query path.

        # Find a non-system actor whose direct proc parent is snap_procs.
        # Snapshot node_id now stores canonical actor refs, so key off the
        # actor's proc ancestry and system bit instead of name substrings.
        # If the first snapshot was captured before the worker spawned,
        # wait for a capture containing the worker.
        snap_worker_query = (
            "SELECT a.node_id AS actor_node_id, a.snapshot_id AS snapshot_id,"
            " pn.proc_name AS proc_name"
            " FROM actor_nodes a"
            " JOIN children ch ON ch.snapshot_id = a.snapshot_id AND ch.child_id = a.node_id"
            " JOIN nodes p ON p.snapshot_id = ch.snapshot_id AND p.node_id = ch.parent_id AND p.node_kind = 'proc'"
            " JOIN proc_nodes pn ON pn.snapshot_id = p.snapshot_id AND pn.node_id = p.node_id"
            " JOIN snapshots s ON s.snapshot_id = a.snapshot_id"
            " WHERE a.is_system = false"
            " AND pn.proc_name LIKE 'snap_procs%'"
            " ORDER BY s.snapshot_ts DESC"
            " LIMIT 1"
        )
        rows = _query(state, snap_worker_query, timeout_secs=60.0)
        actor_ids = rows.get("actor_node_id", [])
        assert len(actor_ids) >= 1, (
            "expected non-system actor on snap_procs in snapshot"
        )
        actor_node_id = actor_ids[0]
        snapshot_id = rows["snapshot_id"][0]
        assert rows["proc_name"][0].startswith("snap_procs")

        # --- Ancestry coherence: actor → proc → host → root ---
        #
        # Walk up from the selected actor through children/nodes
        # to verify the full snapshot graph is connected.
        ancestor_rows = _query(
            state,
            f"""
            WITH RECURSIVE ancestors AS (
                SELECT ch.parent_id AS node_id, 1 AS depth
                FROM children ch
                WHERE ch.snapshot_id = '{snapshot_id}'
                  AND ch.child_id = '{actor_node_id}'
                UNION ALL
                SELECT ch.parent_id, a.depth + 1
                FROM ancestors a
                JOIN children ch
                  ON ch.snapshot_id = '{snapshot_id}'
                 AND ch.child_id = a.node_id
                WHERE a.depth < 10
            )
            SELECT DISTINCT a.node_id, n.node_kind
            FROM ancestors a
            LEFT JOIN nodes n
              ON n.snapshot_id = '{snapshot_id}'
             AND n.node_id = a.node_id
        """,
        )
        ancestor_kinds = set(ancestor_rows.get("node_kind", []))
        ancestor_ids = ancestor_rows.get("node_id", [])

        assert "proc" in ancestor_kinds, (
            f"expected a proc ancestor for {actor_node_id}, "
            f"got kinds={ancestor_kinds}, ids={ancestor_ids}"
        )
        assert "host" in ancestor_kinds or any(
            "root" in str(nid) for nid in ancestor_ids
        ), (
            f"expected host or root ancestor for {actor_node_id}, "
            f"got kinds={ancestor_kinds}, ids={ancestor_ids}"
        )


@pytest.mark.timeout(60)
def test_public_custom_trace_is_queryable(cleanup_callbacks) -> None:
    trace_name = f"custom_trace_{uuid.uuid4().hex}"
    with scoped_state(
        ProcessJob({"hosts": 1}).enable_telemetry(_sidecar_telemetry_config()),
        cached_path=None,
    ) as state:
        _assert_sidecar(state)

        workers = state.hosts.spawn_procs(per_host={"workers": 1}).spawn(
            "custom_trace_worker", WorkerActor
        )
        workers.initialized.get()
        workers.emit_trace.call(trace_name).get()

        result = _query(
            state,
            "SELECT name, fields_json FROM spans "
            "WHERE name = 'python_user_span' "
            f"AND fields_json LIKE '%{trace_name}%'",
        )

        assert result.get("name") == ["python_user_span"]
        fields = json.loads(result["fields_json"][0])
        assert fields["name"] == trace_name
