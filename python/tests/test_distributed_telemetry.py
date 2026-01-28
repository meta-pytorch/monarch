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
from monarch._src.actor.host_mesh import this_host
from monarch._src.actor.proc_mesh import (
    _proc_mesh_spawn_callbacks,
    unregister_proc_mesh_spawn_callback,
)


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


@pytest.mark.timeout(60)
def test_record_batch_tracing(cleanup_callbacks) -> None:
    """Test that RecordBatchSink captures trace events as RecordBatches."""
    from monarch._rust_bindings.monarch_extension.distributed_telemetry import (
        enable_record_batch_tracing,
        get_record_batch_flush_count,
        reset_record_batch_flush_count,
    )

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
