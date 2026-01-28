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

import pytest
from monarch._src.actor.actor_mesh import Actor
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import this_host
from monarch._src.actor.proc_mesh import (
    _proc_mesh_spawn_callbacks,
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
    yield
    # Remove any callbacks added during the test
    callbacks_to_remove = [
        cb for cb in _proc_mesh_spawn_callbacks if cb not in initial_callbacks
    ]
    for cb in callbacks_to_remove:
        unregister_proc_mesh_spawn_callback(cb)


@pytest.mark.timeout(120)
def test_distributed_telemetry_auto_callback(cleanup_callbacks) -> None:
    """Test that start_telemetry registers a spawn callback."""
    initial_callback_count = len(_proc_mesh_spawn_callbacks)

    # Start telemetry with fake data - returns QueryEngine directly
    engine = start_telemetry(use_fake_data=True).get()

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
    engine = start_telemetry(use_fake_data=True).get()

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
    from monarch._rust_bindings.monarch_extension.distributed_telemetry import (
        enable_record_batch_tracing,
    )

    # Enable the record batch sink with a small batch size to trigger flushing
    enable_record_batch_tracing(batch_size=5)

    # Spawn some workers to generate trace events
    this_host().spawn_procs(per_host={"workers": 2})

    # The RecordBatchSink should print RecordBatches to stdout
    # This test just verifies the function can be called without error
    # Visual inspection of stdout will show the RecordBatch output
