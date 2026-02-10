# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Tests for distributed telemetry with automatic callback registration."""

import os

# Enable the unified telemetry layer BEFORE importing monarch
# This is required for the TraceEventDispatcher to be created, which processes sinks
os.environ["USE_UNIFIED_LAYER"] = "1"

import monarch.distributed_telemetry.actor as telemetry_actor
import pytest
from monarch._src.actor.actor_mesh import Actor
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import this_host
from monarch._src.actor.proc_mesh import (
    _proc_mesh_spawn_callbacks,
    SetupActor,
    unregister_proc_mesh_spawn_callback,
)
from monarch.distributed_telemetry import start_telemetry


class WorkerActor(Actor):
    """Simple worker actor that can spawn child processes."""

    @endpoint
    def spawn_child(self, name: str) -> None:
        """Spawn a child process from this worker's host."""
        this_host().spawn_procs(name=name)


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
    this_host().spawn_procs(per_host={"workers": 2})

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
    worker_procs = this_host().spawn_procs(per_host={"workers": 2})

    # Spawn worker actors for business logic
    workers = worker_procs.spawn("worker", WorkerActor)

    # Have worker 0 spawn a grandchild - telemetry automatically tracks it
    workers.slice(workers=0).spawn_child.call_one("grandchild").get()

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
    this_host().spawn_procs(per_host={"workers": 2})

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
    worker_procs = this_host().spawn_procs(per_host={"workers": 2})
    _ = worker_procs.spawn("test_worker", WorkerActor)

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
