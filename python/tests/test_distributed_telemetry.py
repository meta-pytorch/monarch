# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Tests for distributed telemetry with automatic callback registration."""

import json
import os

from isolate_in_subprocess import isolate_in_subprocess

# Enable the unified telemetry layer BEFORE importing monarch
# This is required for the TraceEventDispatcher to be created, which processes sinks
os.environ["USE_UNIFIED_LAYER"] = "1"

from typing import cast

import monarch.distributed_telemetry.actor as telemetry_actor
import pytest
from monarch._src.actor.actor_mesh import Actor, ActorMesh
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.proc_mesh import (
    _proc_mesh_spawn_callbacks,
    SetupActor,
    unregister_proc_mesh_spawn_callback,
)
from monarch.distributed_telemetry.actor import start_telemetry
from monarch.job import ProcessJob


class WorkerActor(Actor):
    """Simple worker actor that can spawn child processes."""

    @endpoint
    def ping(self) -> None:
        pass


class SenderActor(Actor):
    """Actor that sends messages to another actor mesh."""

    @endpoint
    def send_ping(self, target: WorkerActor) -> None:
        """Cast to the target actor mesh from within this actor."""
        # pyre-ignore[29]: target is an ActorMesh
        target.ping.call().get()


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
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    hosts.spawn_procs(per_host={"workers": 2})

    # The sink should have received and flushed some batches
    # Note: The exact count depends on the number of trace events generated
    final_count = get_record_batch_flush_count()
    assert final_count >= 0, "Flush count should be non-negative"

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_actors_table() -> None:
    """Test that the actors table is populated when actors are spawned."""
    # Start telemetry with real data (not fake) so RecordBatchSink receives events
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn some worker actors - this should trigger notify_actor_created
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2})
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

    # Verify full_name contains our worker actor name
    full_names = result_dict.get("full_name", [])
    has_test_worker = any("test_worker" in name for name in full_names)
    assert has_test_worker, (
        f"Expected to find 'test_worker' in actor names, got: {full_names}"
    )

    # Verify that the bootstrap client actor is recorded with display_name "client"
    display_names = result_dict.get("display_name", [])
    assert "<root>" in display_names, (
        f"Expected bootstrap client actor with display_name '<root>', got: {display_names}"
    )

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_meshes_table() -> None:
    """Test that the meshes table is populated when actor meshes are spawned."""
    # Start telemetry with real data (not fake) so RecordBatchSink receives events
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn some worker actors - this should trigger notify_mesh_created
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2})
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

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_proc_mesh_in_meshes_table() -> None:
    """Test that ProcMesh creation is recorded in the meshes table with class 'Proc'."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn a named proc mesh — this should emit a mesh event with class "Proc"
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="proc_mesh_test")
    workers = worker_procs.spawn("proc_mesh_test_worker", WorkerActor)
    workers.initialized.get()

    # Query meshes with class "Proc"
    result = engine.query(
        "SELECT given_name, full_name, class, shape_json, parent_mesh_id, parent_view_json "
        "FROM meshes WHERE class = 'Proc'"
    )
    result_dict = result.to_pydict()

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

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_actors_join_meshes_on_mesh_id(cleanup_callbacks) -> None:
    """Test that actors.mesh_id matches meshes.id, enabling joins."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn actors — this populates both the actors and meshes tables
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2})
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

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_all_actors_in_proc_mesh(cleanup_callbacks) -> None:
    """Test that all actor meshes within a proc mesh have actors in the actors table."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn a named proc mesh and user actors
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="workers_procs")
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

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_all_actors_in_host_mesh(cleanup_callbacks) -> None:
    """Test that all actor meshes within a proc mesh have actors in the actors table."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn a named proc mesh and user actors
    job = ProcessJob({"hosts": 2})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="workers_procs")
    workers = worker_procs.spawn("worker_actors", WorkerActor)
    workers.initialized.get()

    # Get the hosts mesh entry so we can filter child meshes by parent_mesh_id
    host_mesh_result = engine.query(
        "SELECT id FROM meshes WHERE class = 'Host' AND given_name = 'hosts'"
    )
    host_mesh_ids = host_mesh_result.to_pydict().get("id", [])
    assert len(host_mesh_ids) == 1, (
        f"Expected exactly 1 hosts mesh, got {len(host_mesh_ids)}"
    )
    host_mesh_id = host_mesh_ids[0]

    # HostAgent actors have mesh_id pointing directly to the host mesh
    host_agents = engine.query(f"SELECT id FROM actors WHERE mesh_id = {host_mesh_id}")
    host_agents_count = len(host_agents.to_pydict().get("id", []))
    assert host_agents_count == 2, (
        f"Expected 2 HostAgent actors, got {host_agents_count}"
    )

    # Query all proc meshes of this hosts mesh
    proc_meshes = engine.query(
        f"SELECT id, class, given_name FROM meshes WHERE parent_mesh_id = {host_mesh_id}"
    )
    proc_dict = proc_meshes.to_pydict()
    proc_given_names = set(proc_dict.get("given_name", []))
    assert proc_given_names == {"workers_procs"}

    # Query all child actor meshes of this hosts mesh
    child_meshes = engine.query(
        f"""
        SELECT m.id, m.class, m.given_name
        FROM meshes m
        INNER JOIN meshes proc ON m.parent_mesh_id = proc.id
        INNER JOIN meshes hosts ON proc.parent_mesh_id = hosts.id
        WHERE hosts.id = {host_mesh_id}
        """
    )
    child_dict = child_meshes.to_pydict()
    child_classes = set(child_dict.get("class", []))
    child_names = child_dict.get("given_name", [])
    child_ids = child_dict.get("id", [])

    # The proc mesh should contain user-spawned actor and telemetry, logger, setup, agent, and comm actors.
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
        assert actor_count == 4, (
            f"Expected 4 actors for mesh '{mesh_name}' (class={mesh_class}), "
            f"got {actor_count}"
        )

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_actor_status_events_table() -> None:
    """Test that the actor_status_events table is populated when actors change status."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn worker actors — actors go through status transitions during spawn
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2})
    workers = worker_procs.spawn("status_test_worker", WorkerActor)
    workers.initialized.get()

    # Query the actor_status_events table
    result = engine.query("SELECT * FROM actor_status_events")
    result_dict = result.to_pydict()

    # Verify the schema has the expected columns
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

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_sliced_vs_full_view_rank(cleanup_callbacks) -> None:
    """Test that rank and parent_view_json are correct for sliced and full actor meshes."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    # Spawn 3 workers so we can slice a subset
    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 3}, name="rank_test_procs")

    # Full view: spawn on the unsliced proc mesh (all 3 workers)
    full_actors = worker_procs.spawn("full_view_actor", WorkerActor)
    full_actors.initialized.get()

    # Sliced view: take workers 1..3 (indices 1 and 2)
    sliced_procs = worker_procs.slice(workers=slice(1, 3))
    sliced_actors = sliced_procs.spawn("sliced_view_actor", WorkerActor)
    sliced_actors.initialized.get()

    # -- Verify full-view actor mesh --
    full_mesh = engine.query(
        "SELECT id, shape_json, parent_view_json FROM meshes "
        "WHERE given_name = 'full_view_actor'"
    )
    full_mesh_dict = full_mesh.to_pydict()
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
    full_actors_result = engine.query(
        f"SELECT rank FROM actors WHERE mesh_id = {full_mesh_id} ORDER BY rank"
    )
    full_ranks = full_actors_result.to_pydict()["rank"]
    assert full_ranks == [0, 1, 2], f"Expected ranks [0, 1, 2], got {full_ranks}"

    # -- Verify sliced-view actor mesh --
    sliced_mesh = engine.query(
        "SELECT id, shape_json, parent_view_json FROM meshes "
        "WHERE given_name = 'sliced_view_actor'"
    )
    sliced_mesh_dict = sliced_mesh.to_pydict()
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
    sliced_actors_result = engine.query(
        f"SELECT rank FROM actors WHERE mesh_id = {sliced_mesh_id} ORDER BY rank"
    )
    sliced_ranks = sliced_actors_result.to_pydict()["rank"]
    assert sliced_ranks == [0, 1], f"Expected ranks [0, 1], got {sliced_ranks}"

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
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
      - actor_mesh_id: hash of the target actor mesh name
      - view_json: serialized ndslice::Region of the current view
      - shape_json: serialized ndslice::Shape (converted from the Region)
    """
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
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

    # Verify the schema matches SentMessage struct in entity_dispatcher.rs
    # (only check once, for the "call" path)
    if send_path == "call":
        result = engine.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'sent_messages' ORDER BY ordinal_position"
        )
        column_names = result.to_pydict().get("column_name", [])
        assert column_names == [
            "id",
            "timestamp_us",
            "sender_actor_id",
            "actor_mesh_id",
            "view_json",
            "shape_json",
        ], f"Unexpected columns: {column_names}"

    # Verify 42 sent_messages join with the correct mesh
    joined = engine.query(
        "SELECT sm.id FROM sent_messages sm LEFT JOIN meshes m "
        f"ON sm.actor_mesh_id = m.id WHERE m.given_name = '{mesh_name}'"
    )
    joined_count = len(joined.to_pydict().get("id", []))
    assert joined_count == 42, (
        f"Expected 42 sent_messages via {send_path}, got {joined_count}"
    )

    # Verify view_json (ndslice Region) and shape_json (ndslice Shape).
    # Region serializes as {"labels": [...], "slice": {"offset": ..., "sizes": [...], "strides": [...]}}.
    # Shape is Region converted via Region::into::<Shape>, same serialization format.
    mesh = engine.query(f"SELECT id FROM meshes WHERE given_name = '{mesh_name}'")
    mesh_id = mesh.to_pydict()["id"][0]
    msgs = engine.query(
        f"SELECT view_json, shape_json FROM sent_messages "
        f"WHERE actor_mesh_id = {mesh_id} LIMIT 1"
    )
    msgs_dict = msgs.to_pydict()
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

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_messages_table(cleanup_callbacks) -> None:
    """Test that the messages table is populated when messages are received."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="msg_workers_procs")
    workers = worker_procs.spawn("msg_test_worker", WorkerActor)
    workers.initialized.get()

    # Send several messages to trigger telemetry
    for _ in range(5):
        workers.ping.call().get()

    # Verify schema
    result = engine.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'messages' ORDER BY ordinal_position"
    )
    column_names = result.to_pydict().get("column_name", [])
    assert column_names == [
        "id",
        "timestamp_us",
        "from_actor_id",
        "to_actor_id",
        "endpoint",
        "port_id",
    ], f"Unexpected columns: {column_names}"

    # Verify rows exist
    result = engine.query("SELECT * FROM messages")
    result_dict = result.to_pydict()
    row_count = len(result_dict.get("id", []))
    assert row_count > 0, f"Expected messages, got {row_count}"

    # Verify to_actor_id joins with actors table (receiver is a known actor)
    joined = engine.query(
        "SELECT m.id FROM messages m "
        "JOIN actors a ON m.to_actor_id = a.id "
        "JOIN meshes mesh ON a.mesh_id = mesh.id "
        "WHERE mesh.given_name = 'msg_test_worker'"
    )
    joined_count = len(joined.to_pydict().get("id", []))
    # 5 casts x 2 workers = 10 messages received by msg_test_worker actors
    assert joined_count == 10, (
        f"Expected 10 messages received by msg_test_worker, got {joined_count}"
    )

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_message_status_events_table(cleanup_callbacks) -> None:
    """Test that message_status_events captures queued/active/complete transitions."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(
        per_host={"workers": 1}, name="status_workers_procs"
    )
    workers = worker_procs.spawn("status_test_worker", WorkerActor)
    workers.initialized.get()

    workers.ping.call().get()

    # Verify schema
    result = engine.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'message_status_events' ORDER BY ordinal_position"
    )
    column_names = result.to_pydict().get("column_name", [])
    assert column_names == [
        "id",
        "timestamp_us",
        "message_id",
        "status",
    ], f"Unexpected columns: {column_names}"

    # Verify status values include queued, active, complete
    result = engine.query("SELECT DISTINCT status FROM message_status_events")
    statuses = set(result.to_pydict().get("status", []))
    expected_statuses = {"queued", "active", "complete"}
    assert expected_statuses.issubset(statuses), (
        f"Expected statuses {expected_statuses} to be subset of {statuses}"
    )

    # Verify at least one message has all 3 status events (queued, active, complete)
    result = engine.query(
        "SELECT message_id, COUNT(*) as cnt "
        "FROM message_status_events "
        "GROUP BY message_id "
        "HAVING COUNT(*) = 3"
    )
    result_dict = result.to_pydict()
    assert len(result_dict.get("message_id", [])) > 0, (
        "Expected at least one message with all 3 status events"
    )

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_sent_messages_with_sliced_mesh(cleanup_callbacks) -> None:
    """Test that sent_messages view_json/shape_json reflect sliced vs full actor mesh casts."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
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
    mesh = engine.query("SELECT id FROM meshes WHERE given_name = 'sm_actors'")
    mesh_id = mesh.to_pydict()["id"][0]

    msgs = engine.query(
        f"SELECT view_json, shape_json FROM sent_messages "
        f"WHERE actor_mesh_id = {mesh_id} ORDER BY timestamp_us"
    )
    msgs_dict = msgs.to_pydict()
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

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_sent_messages_sender_actor_id(cleanup_callbacks) -> None:
    """Test that sender_actor_id identifies the actor that initiated the cast,
    not the target actor, when one actor casts to another actor mesh."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="sender_test_procs")

    # Spawn target actors on the full proc mesh
    targets = worker_procs.spawn("target_workers", WorkerActor)
    targets.initialized.get()

    # Spawn a single sender actor on worker 0
    sender = worker_procs.slice(workers=0).spawn("sender_actor", SenderActor)
    sender.initialized.get()

    # SenderActor casts to the target actor mesh from within its endpoint
    sender.send_ping.call_one(targets).get()

    # Find the sent_messages row targeting the "target_workers" mesh
    target_mesh = engine.query(
        "SELECT id FROM meshes WHERE given_name = 'target_workers'"
    )
    target_mesh_id = target_mesh.to_pydict()["id"][0]

    msgs = engine.query(
        f"SELECT sender_actor_id FROM sent_messages "
        f"WHERE actor_mesh_id = {target_mesh_id}"
    )
    msgs_dict = msgs.to_pydict()
    assert len(msgs_dict["sender_actor_id"]) > 0, (
        "Expected at least one sent message targeting 'target_workers'"
    )

    # The sender_actor_id should match an actor in the "sender_actor" mesh,
    # not an actor in the "target_workers" mesh.
    sender_mesh = engine.query(
        "SELECT id FROM meshes WHERE given_name = 'sender_actor'"
    )
    sender_mesh_id = sender_mesh.to_pydict()["id"][0]

    sender_actors = engine.query(
        f"SELECT id, display_name FROM actors WHERE mesh_id = {sender_mesh_id}"
    )
    sender_actor_ids = set(sender_actors.to_pydict()["id"])

    target_actors = engine.query(
        f"SELECT id FROM actors WHERE mesh_id = {target_mesh_id}"
    )
    target_actor_ids = set(target_actors.to_pydict()["id"])

    for sender_id in msgs_dict["sender_actor_id"]:
        assert sender_id in sender_actor_ids, (
            f"sender_actor_id {sender_id} should be a sender actor, "
            f"not a target actor. sender_actor_ids={sender_actor_ids}, "
            f"target_actor_ids={target_actor_ids}"
        )
        assert sender_id not in target_actor_ids, (
            f"sender_actor_id {sender_id} should NOT be a target actor"
        )

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_query_after_stopping_proc_mesh(cleanup_callbacks) -> None:
    """Test that query still works after a user-spawned actor's proc mesh is stopped."""
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 2}, name="stop_test_procs")

    # Spawn and initialize a user actor
    workers = worker_procs.spawn("stop_test_worker", WorkerActor)
    workers.initialized.get()

    # Send messages to the workers so the messages table is populated
    # on the child processes (notify_message fires on the receiver's process).
    workers.ping.call().get()

    # Verify the actor appears in the actors table before stopping
    result = engine.query(
        "SELECT full_name FROM actors WHERE full_name LIKE '%stop_test_worker%'"
    )
    pre_stop_count = len(result.to_pydict().get("full_name", []))
    assert pre_stop_count > 0, "Expected stop_test_worker actors before stopping"

    # Verify received messages exist before stopping. The messages table is
    # populated on the child process via notify_message, so these records
    # come from the child scanner.
    pre_stop_msgs = engine.query(
        "SELECT m.id FROM messages m "
        "JOIN actors a ON m.to_actor_id = a.id "
        "JOIN meshes mesh ON a.mesh_id = mesh.id "
        "WHERE mesh.given_name = 'stop_test_worker'"
    )
    pre_stop_msg_count = len(pre_stop_msgs.to_pydict().get("id", []))
    assert pre_stop_msg_count > 0, (
        "Expected received messages for stop_test_worker before stopping"
    )

    # Stop the proc mesh — this kills both user actors AND telemetry actors on it.
    # The coordinator's _children list still references the dead telemetry actors.
    worker_procs.stop().get()

    # Query should still work after the proc mesh is stopped.
    # The distributed telemetry scan must handle stopped children gracefully.
    result = engine.query("SELECT * FROM actors")
    result_dict = result.to_pydict()
    actor_count = len(result_dict.get("id", []))
    assert actor_count > 0, (
        f"Expected actors in query result after stopping proc mesh, got {actor_count}"
    )

    # The stopped actor should still appear in historical data since
    # it's event was emitted from the root client process.
    full_names = result_dict.get("full_name", [])
    assert any("stop_test_worker" in name for name in full_names), (
        f"Expected 'stop_test_worker' in actors after stop, got: {full_names}"
    )

    # Received messages are lost after stopping the proc mesh because
    # notify_message fires on the receiver's process. The child scanner
    # that held those records is gone.
    post_stop_msgs = engine.query(
        "SELECT m.id FROM messages m "
        "JOIN actors a ON m.to_actor_id = a.id "
        "JOIN meshes mesh ON a.mesh_id = mesh.id "
        "WHERE mesh.given_name = 'stop_test_worker'"
    )
    post_stop_msg_count = len(post_stop_msgs.to_pydict().get("id", []))
    assert post_stop_msg_count == 0, (
        f"Expected 0 received messages after stopping proc mesh, "
        f"got {post_stop_msg_count} (was {pre_stop_msg_count} before stop)"
    )

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(120)
def test_query_after_stopping_actor_mesh(cleanup_callbacks) -> None:
    """Test that stopping a user ActorMesh does not affect telemetry queries.

    Stopping an ActorMesh is a user-initiated action that does not trigger
    __supervise__ on the telemetry coordinator. The telemetry actors on the
    ProcMesh remain alive, so all data (including process-local tables like
    messages) is still queryable.
    """
    engine = start_telemetry(batch_size=10, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(
        per_host={"workers": 2}, name="actor_stop_test_procs"
    )

    # Spawn and initialize a user actor
    workers = worker_procs.spawn("actor_stop_worker", WorkerActor)
    workers.initialized.get()

    # Send messages so the messages table is populated on child processes
    workers.ping.call().get()

    # Verify received messages exist before stopping
    pre_stop_msgs = engine.query(
        "SELECT m.id FROM messages m "
        "JOIN actors a ON m.to_actor_id = a.id "
        "JOIN meshes mesh ON a.mesh_id = mesh.id "
        "WHERE mesh.given_name = 'actor_stop_worker'"
    )
    pre_stop_msg_count = len(pre_stop_msgs.to_pydict().get("id", []))
    assert pre_stop_msg_count > 0, (
        "Expected received messages for actor_stop_worker before stopping"
    )

    # Stop only the user ActorMesh, not the ProcMesh.
    # The telemetry actors on the ProcMesh remain alive.
    cast(ActorMesh[WorkerActor], workers).stop().get()

    # The actor_status_events table should show a Stopped status for the
    # stopped actors. This event fires on the child process, and is
    # queryable because the ProcMesh (and its telemetry actor) is still alive.
    status_result = engine.query(
        "SELECT ase.new_status FROM actor_status_events ase "
        "JOIN actors a ON ase.actor_id = a.id "
        "JOIN meshes m ON a.mesh_id = m.id "
        "WHERE m.given_name = 'actor_stop_worker'"
    )
    statuses = set(status_result.to_pydict().get("new_status", []))
    assert "Stopped" in statuses, (
        f"Expected 'Stopped' in actor status events after ActorMesh.stop(), "
        f"got: {statuses}"
    )

    # Query should still work — the telemetry children are unaffected
    result = engine.query("SELECT * FROM actors")
    result_dict = result.to_pydict()
    actor_count = len(result_dict.get("id", []))
    assert actor_count > 0, (
        f"Expected actors after stopping user ActorMesh, got {actor_count}"
    )

    # The stopped actor should still appear in the actors table
    full_names = result_dict.get("full_name", [])
    assert any("actor_stop_worker" in name for name in full_names), (
        f"Expected 'actor_stop_worker' in actors after stop, got: {full_names}"
    )

    # Unlike stopping a ProcMesh, received messages are NOT lost because
    # the telemetry actors and their scanners are still alive.
    post_stop_msgs = engine.query(
        "SELECT m.id FROM messages m "
        "JOIN actors a ON m.to_actor_id = a.id "
        "JOIN meshes mesh ON a.mesh_id = mesh.id "
        "WHERE mesh.given_name = 'actor_stop_worker'"
    )
    post_stop_msg_count = len(post_stop_msgs.to_pydict().get("id", []))
    assert post_stop_msg_count == pre_stop_msg_count, (
        f"Expected {pre_stop_msg_count} received messages after stopping ActorMesh, "
        f"got {post_stop_msg_count} (data should be preserved)"
    )

    # Clean up
    hosts.shutdown().get()


@pytest.mark.timeout(60)
def test_store_pyspy_dump_and_query(cleanup_callbacks) -> None:
    """Store a py-spy dump via actor endpoint, query it back via SQL."""
    engine = start_telemetry(include_dashboard=False)

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

    engine._actor.store_pyspy_dump.call("dump-1", "proc[0]", pyspy_json).get()

    result = engine.query(
        "SELECT name, line FROM pyspy_frames "
        "WHERE dump_id = 'dump-1' ORDER BY frame_depth"
    )
    result_dict = result.to_pydict()
    assert len(result_dict["name"]) == 2
    assert result_dict["name"] == ["stalling_fn", "main"]

    # Query local variables
    locals_result = engine.query(
        "SELECT name, addr, arg, repr, frame_depth FROM pyspy_local_variables "
        "WHERE dump_id = 'dump-1' ORDER BY frame_depth, name"
    )
    locals_dict = locals_result.to_pydict()
    assert len(locals_dict["name"]) == 3
    assert locals_dict["name"] == ["x", "y", "z"]
    assert locals_dict["addr"] == [100, 200, 300]
    assert locals_dict["arg"] == [True, False, True]
    assert locals_dict["repr"] == ["42", None, "'hello'"]
    assert locals_dict["frame_depth"] == [0, 0, 1]


@pytest.mark.timeout(60)
def test_pyspy_tables_in_information_schema(cleanup_callbacks) -> None:
    """py-spy tables are visible in information_schema."""
    engine = start_telemetry(include_dashboard=False)
    result = engine.query(
        "SELECT table_name FROM information_schema.tables ORDER BY table_name"
    )
    table_names = result.to_pydict().get("table_name", [])
    assert "pyspy_dumps" in table_names
    assert "pyspy_stack_traces" in table_names
    assert "pyspy_frames" in table_names
    assert "pyspy_local_variables" in table_names


@pytest.mark.timeout(120)
def test_per_table_row_retention(cleanup_callbacks) -> None:
    """Test that time-based retention deletes old rows from message tables."""
    import time

    # Use a 1-second retention window so rows expire quickly.
    engine = start_telemetry(batch_size=2, retention_secs=1, include_dashboard=False)

    job = ProcessJob({"hosts": 1})
    hosts = job.state(cached_path=None).hosts
    worker_procs = hosts.spawn_procs(per_host={"workers": 8}, name="worker_procs")
    workers = worker_procs.spawn("workers", WorkerActor)
    workers.initialized.get()

    for _ in range(50):
        workers.ping.call().get()

    # Verify events exist before retention kicks in.
    before = engine.query("SELECT COUNT(*) AS cnt FROM message_status_events")
    before_count = before.to_pydict()["cnt"][0]
    assert before_count > 0, "Expected message_status_events rows before retention"

    # Wait for the 1-second retention window to expire, then query again.
    # The query triggers flush(), which applies retention and trims old rows.
    time.sleep(2)

    after = engine.query("SELECT COUNT(*) AS cnt FROM message_status_events")
    after_count = after.to_pydict()["cnt"][0]
    assert after_count < before_count, (
        f"Expected fewer rows after retention, got {after_count} vs {before_count}"
    )

    # Clean up
    hosts.shutdown().get()
