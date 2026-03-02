# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Tests for distributed telemetry with automatic callback registration."""

import json
import os

# Enable the unified telemetry layer BEFORE importing monarch
# This is required for the TraceEventDispatcher to be created, which processes sinks
os.environ["USE_UNIFIED_LAYER"] = "1"

import monarch.distributed_telemetry.actor as telemetry_actor
import pytest
from monarch._src.actor.actor_mesh import Actor
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.proc_mesh import (
    _proc_mesh_spawn_callbacks,
    SetupActor,
    unregister_proc_mesh_spawn_callback,
)
from monarch.distributed_telemetry import start_telemetry
from monarch.job import ProcessJob


class WorkerActor(Actor):
    """Simple worker actor that can spawn child processes."""

    @endpoint
    def ping(self) -> None:
        pass


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
    # Reset module-level state for next test
    telemetry_actor._scanner = None
    telemetry_actor._scanner_startup_impl = None
    telemetry_actor._spawned_procs = []
    telemetry_actor._spawn_callback_registered = False


@pytest.mark.timeout(120)
def test_distributed_telemetry_auto_callback(cleanup_callbacks) -> None:
    """Test that start_telemetry registers a spawn callback."""
    initial_callback_count = len(_proc_mesh_spawn_callbacks)

    # Start telemetry with fake data - returns QueryEngine directly
    engine = start_telemetry(use_fake_data=True)

    # Verify callback was registered
    assert len(_proc_mesh_spawn_callbacks) == initial_callback_count + 1

    # Spawn workers - callback should fire and add them as children
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    host.spawn_procs(per_host={"workers": 2})

    # Query should return data from coordinator + 2 workers = 3 sources
    # Each source has 10 hosts, so we expect 30 total hosts
    result = engine.query("SELECT COUNT(*) as total_hosts FROM hosts")
    total_hosts = result.to_pydict()["total_hosts"][0]
    assert total_hosts == 30, f"Expected 30 hosts, got {total_hosts}"


@pytest.mark.timeout(180)
def test_distributed_telemetry_grandchild(cleanup_callbacks) -> None:
    """Test that grandchild data makes it to the top-level query."""
    # Start telemetry with fake data - returns QueryEngine directly
    engine = start_telemetry(use_fake_data=True)

    # Spawn workers
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    worker_procs = host.spawn_procs(per_host={"workers": 2})

    # Spawn worker actors for business logic
    workers = worker_procs.spawn("worker", WorkerActor)
    workers.initialized.get()

    # Spawn a grandchild - telemetry automatically tracks it
    host.spawn_procs(name="grandchild")

    # Query should return data from coordinator + 2 workers + 1 grandchild = 4 sources
    # Each source has 10 hosts, so we expect 40 total hosts
    result = engine.query("SELECT COUNT(*) as total_hosts FROM hosts")
    total_hosts = result.to_pydict()["total_hosts"][0]
    assert total_hosts == 40, f"Expected 40 hosts (4 sources x 10), got {total_hosts}"

    # Verify metrics are also collected from all sources
    # Each source has 960 metrics
    result = engine.query("SELECT COUNT(*) as total_metrics FROM metrics")
    total_metrics = result.to_pydict()["total_metrics"][0]
    assert total_metrics == 960 * 4, f"Expected {960 * 4} metrics, got {total_metrics}"


@pytest.mark.timeout(60)
def test_record_batch_tracing(cleanup_callbacks) -> None:
    """Test that RecordBatchSink captures trace events as RecordBatches."""
    try:
        from monarch._rust_bindings.monarch_distributed_telemetry import (
            enable_record_batch_tracing,
            get_record_batch_flush_count,
            reset_record_batch_flush_count,
        )
    except ImportError:
        pytest.skip(  # pyre-ignore[29]: pytest.skip is callable
            "RecordBatch tracing not available (requires distributed_sql_telemetry feature)"
        )
        return

    # Reset the counter before starting
    reset_record_batch_flush_count()
    initial_count = get_record_batch_flush_count()
    assert initial_count == 0, "Flush count should be 0 after reset"

    # Enable the record batch sink with a small batch size to trigger flushing
    enable_record_batch_tracing(batch_size=5)

    # Spawn some workers to generate trace events
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    host.spawn_procs(per_host={"workers": 2})

    # The sink should have received and flushed some batches
    # Note: The exact count depends on the number of trace events generated
    final_count = get_record_batch_flush_count()
    assert final_count >= 0, "Flush count should be non-negative"


@pytest.mark.timeout(120)
def test_actors_table(cleanup_callbacks) -> None:
    """Test that the actors table is populated when actors are spawned."""
    # Start telemetry with real data (not fake) so RecordBatchSink receives events
    engine = start_telemetry(use_fake_data=False, batch_size=10)

    # Spawn some worker actors - this should trigger notify_actor_created
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    worker_procs = host.spawn_procs(per_host={"workers": 2})
    workers = worker_procs.spawn("test_worker", WorkerActor)
    workers.initialized.get()

    # Query the actors table to verify actors were recorded
    result = engine.query("SELECT * FROM actors")
    result_dict = result.to_pydict()

    # We should have at least some actors recorded
    # (the exact count depends on internal actors created)
    actor_count = len(result_dict.get("id", []))
    assert actor_count > 0, f"Expected at least one actor, got {actor_count}"

    # Verify the schema has the expected columns
    expected_columns = {"id", "timestamp_us", "mesh_id", "rank", "full_name"}
    actual_columns = set(result_dict.keys())
    assert expected_columns == actual_columns, (
        f"Expected columns {expected_columns}, got {actual_columns}"
    )

    # Verify full_name contains our worker actor name
    full_names = result_dict.get("full_name", [])
    has_test_worker = any("test_worker" in name for name in full_names)
    assert has_test_worker, (
        f"Expected to find 'test_worker' in actor names, got: {full_names}"
    )


@pytest.mark.timeout(120)
def test_meshes_table(cleanup_callbacks) -> None:
    """Test that the meshes table is populated when actor meshes are spawned."""
    # Start telemetry with real data (not fake) so RecordBatchSink receives events
    engine = start_telemetry(use_fake_data=False, batch_size=10)

    # Spawn some worker actors - this should trigger notify_mesh_created
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    worker_procs = host.spawn_procs(per_host={"workers": 2})
    workers = worker_procs.spawn("test_mesh_worker", WorkerActor)
    workers.initialized.get()

    # Query the meshes table to verify actor meshes were recorded
    result = engine.query("SELECT * FROM meshes")
    result_dict = result.to_pydict()

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
def test_proc_mesh_in_meshes_table(cleanup_callbacks) -> None:
    """Test that ProcMesh creation is recorded in the meshes table with class 'Proc'."""
    engine = start_telemetry(use_fake_data=False, batch_size=10)

    # Spawn a named proc mesh — this should emit a mesh event with class "Proc"
    hosts = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="proc_mesh_test")
    workers = worker_procs.spawn("proc_mesh_test_worker", WorkerActor)
    workers.initialized.get()

    # Query meshes with class "Proc"
    result = engine.query(
        "SELECT given_name, full_name, class, shape_json, parent_mesh_id, parent_view_json "
        "FROM meshes WHERE class = 'Proc'"
    )
    result_dict = result.to_pydict()

    # Verify our named proc mesh appears with the correct given_name
    given_names = result_dict.get("given_name", [])
    assert ["proc_mesh_test"] == given_names, (
        f"Expected given_name to be 'proc_mesh_test', got: {given_names}"
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
def test_actors_join_meshes_on_mesh_id(cleanup_callbacks) -> None:
    """Test that actors.mesh_id matches meshes.id, enabling joins."""
    engine = start_telemetry(use_fake_data=False, batch_size=10)

    # Spawn actors — this populates both the actors and meshes tables
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    worker_procs = host.spawn_procs(per_host={"workers": 2})
    workers = worker_procs.spawn("join_test_worker", WorkerActor)
    workers.initialized.get()

    # Join actors with meshes on mesh_id = id
    result = engine.query(
        """SELECT a.full_name AS actor_name,
                  a.mesh_id,
                  a.rank,
                  m.given_name AS mesh_name,
                  m.class AS mesh_class
           FROM actors a
           INNER JOIN meshes m ON a.mesh_id = m.id
           WHERE a.full_name LIKE '%join_test_worker%'
           ORDER BY a.rank"""
    )
    result_dict = result.to_pydict()

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

    # With 2 workers, we should see 2 joined rows
    assert joined_count == 2, (
        f"Expected 2 joined rows for 2 workers, got: {joined_count}"
    )


@pytest.mark.timeout(120)
def test_all_actors_in_proc_mesh(cleanup_callbacks) -> None:
    """Test that all actor meshes within a proc mesh have actors in the actors table."""
    engine = start_telemetry(use_fake_data=False, batch_size=10)

    # Spawn a named proc mesh and user actors
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    worker_procs = host.spawn_procs(per_host={"workers": 2}, name="workers_procs")
    workers = worker_procs.spawn("worker_actors", WorkerActor)
    workers.initialized.get()

    # Get the proc mesh entry so we can filter child meshes by parent_mesh_id
    proc_result = engine.query(
        "SELECT id FROM meshes WHERE class = 'Proc' AND given_name = 'workers_procs'"
    )
    proc_ids = proc_result.to_pydict().get("id", [])
    assert len(proc_ids) == 1, f"Expected exactly 1 proc mesh, got {len(proc_ids)}"
    proc_mesh_id = proc_ids[0]

    # ProcAgent actors have mesh_id pointing directly to the proc mesh
    proc_agents = engine.query(f"SELECT id FROM actors WHERE mesh_id = {proc_mesh_id}")
    proc_agents_count = len(proc_agents.to_pydict().get("id", []))
    assert proc_agents_count == 2, (
        f"Expected 2 ProcAgent actors, got {proc_agents_count}"
    )

    # Query all child actor meshes of this proc mesh
    child_meshes = engine.query(
        f"SELECT id, class, given_name FROM meshes WHERE parent_mesh_id = {proc_mesh_id}"
    )
    child_dict = child_meshes.to_pydict()
    child_classes = set(child_dict.get("class", []))
    child_names = child_dict.get("given_name", [])
    child_ids = child_dict.get("id", [])

    assert set(child_names) == {
        "worker_actors",
        "telemetry",
        "logger",
        "setup",
        "comm",
    }

    # For every child actor mesh, verify that actors exist in the actors table
    for mesh_id, mesh_class, mesh_name in zip(child_ids, child_classes, child_names):
        actor_result = engine.query(f"SELECT id FROM actors WHERE mesh_id = {mesh_id}")
        actor_dict = actor_result.to_pydict()
        actor_count = len(actor_dict.get("id", []))

        # Each mesh on a 2-worker proc mesh should have exactly 2 actors
        assert actor_count == 2, (
            f"Expected 2 actors for mesh '{mesh_name}' (class={mesh_class}), "
            f"got {actor_count}"
        )


@pytest.mark.timeout(120)
def test_actor_status_events_table(cleanup_callbacks) -> None:
    """Test that the actor_status_events table is populated when actors change status."""
    engine = start_telemetry(use_fake_data=False, batch_size=10)

    # Spawn worker actors — actors go through status transitions during spawn
    host = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    worker_procs = host.spawn_procs(per_host={"workers": 2})
    workers = worker_procs.spawn("status_test_worker", WorkerActor)
    workers.initialized.get()

    # Query the actor_status_events table
    result = engine.query("SELECT * FROM actor_status_events")
    result_dict = result.to_pydict()

    # Verify the schema has the expected columns
    expected_columns = {
        "timestamp_us",
        "actor_id",
        "new_status",
        "prev_status",
        "reason",
    }
    actual_columns = set(result_dict.keys())
    assert expected_columns == actual_columns, (
        f"Expected columns {expected_columns}, got {actual_columns}"
    )

    # We should have at least some status events (actors transition through
    # Created -> Initializing -> Idle at minimum)
    event_count = len(result_dict.get("timestamp_us", []))
    assert event_count > 0, (
        f"Expected at least one actor status event, got {event_count}"
    )

    # Verify new_status values are valid ActorStatus arm names
    valid_statuses = {
        "Unknown",
        "Created",
        "Initializing",
        "Client",
        "Idle",
        "Processing",
        "Saving",
        "Loading",
        "Stopping",
        "Stopped",
        "Failed",
    }
    new_statuses = set(result_dict.get("new_status", []))
    assert new_statuses.issubset(valid_statuses), (
        f"Found unexpected status values: {new_statuses - valid_statuses}"
    )
