#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Distributed Telemetry with Real Tracing Data.

This example demonstrates querying real tracing data collected from actors.
Unlike the hello_world example which uses fake demo data, this example:

1. Starts telemetry with use_fake_data=False
2. Spawns actors that do work (generating real tracing events)
3. Queries the spans, span_events, events, and actors tables

The RecordBatchSink automatically captures tracing events and stores them
in queryable Arrow tables. Actor creation events are captured separately
via the ActorEventSink.

Usage:
    python distributed_telemetry_real_data.py
"""

import os
import time

# Enable unified telemetry layer before importing monarch
os.environ["USE_UNIFIED_LAYER"] = "true"

import pyarrow as pa
from monarch.actor import Actor, endpoint, this_host
from monarch.distributed_telemetry import start_telemetry


NUM_WORKERS = 3


class ComputeActor(Actor):
    """Actor that does some work to generate tracing events."""

    @endpoint
    def compute(self, iterations: int) -> int:
        """Do some computation, generating trace events."""
        total = 0
        for i in range(iterations):
            total += i * i
        return total

    def _do_nested_work(self, depth: int) -> str:
        """Internal method for nested work."""
        if depth <= 0:
            return "done"
        time.sleep(0.01)  # Small delay to ensure distinct timestamps
        return f"depth_{depth}_" + self._do_nested_work(depth - 1)

    @endpoint
    def nested_work(self, depth: int) -> str:
        """Do nested work to generate hierarchical spans."""
        return self._do_nested_work(depth)

    @endpoint
    def spawn_child_work(self) -> None:
        """Spawn a child process to do work."""
        child_procs = this_host().spawn_procs(name="child_worker")
        child_actors = child_procs.spawn("child_compute", ComputeActor)
        # pyre-ignore[29]: child_actors is an ActorMesh
        child_actors.compute.call(100).get()


def print_table(table: pa.Table, max_rows: int = 20) -> None:
    """Pretty print a PyArrow table."""
    if table.num_rows == 0:
        print("(empty result)")
        return
    df = table.to_pandas()
    if len(df) > max_rows:
        print(df.head(max_rows).to_string())
        print(f"... +{len(df) - max_rows} more rows")
    else:
        print(df.to_string())


def main() -> None:
    print("Distributed Telemetry - Real Tracing Data Demo")
    print("=" * 50)
    print()

    # Start telemetry with real data collection (no fake data)
    print("Starting telemetry with real data collection...")
    engine = start_telemetry(use_fake_data=False)

    # Spawn worker processes - telemetry automatically tracks them
    print(f"Spawning {NUM_WORKERS} worker processes...")
    worker_procs = this_host().spawn_procs(per_host={"workers": NUM_WORKERS})

    # Spawn compute actors
    print("Spawning compute actors...")
    workers = worker_procs.spawn("compute", ComputeActor)

    # Do some work to generate tracing events
    print("Doing computation work...")
    # pyre-ignore[29]: workers is an ActorMesh
    results = workers.compute.call(1000).get()
    print(f"Computation results: {list(results)}")

    print("Doing nested work...")
    # pyre-ignore[29]: workers is an ActorMesh
    nested_results = workers.nested_work.call(3).get()
    print(f"Nested work results: {list(nested_results)}")

    # Have worker 0 spawn a child and do work
    print("Having worker 0 spawn a child process...")
    workers.slice(workers=0).spawn_child_work.call_one().get()

    # Give a moment for all trace events to be flushed
    print("Waiting for trace events to flush...")
    time.sleep(1.0)

    print()
    print("Querying real telemetry data...")
    print("-" * 50)
    print()

    # Query the real telemetry tables
    queries = [
        # Show table schemas first
        (
            "Schema of 'spans' table",
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_name = 'spans'
               ORDER BY ordinal_position""",
        ),
        (
            "Schema of 'span_events' table",
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_name = 'span_events'
               ORDER BY ordinal_position""",
        ),
        (
            "Schema of 'events' table",
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_name = 'events'
               ORDER BY ordinal_position""",
        ),
        (
            "Schema of 'actors' table",
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_name = 'actors'
               ORDER BY ordinal_position""",
        ),
        (
            "Schema of 'actor_meshes' table",
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_name = 'actor_meshes'
               ORDER BY ordinal_position""",
        ),
        # Show available spans
        ("Count of spans", "SELECT COUNT(*) as total_spans FROM spans"),
        (
            "Count of span events",
            "SELECT COUNT(*) as total_span_events FROM span_events",
        ),
        ("Count of events", "SELECT COUNT(*) as total_events FROM events"),
        ("Count of actors", "SELECT COUNT(*) as total_actors FROM actors"),
        (
            "Count of actor meshes",
            "SELECT COUNT(*) as total_actor_meshes FROM actor_meshes",
        ),
        # Show span details
        (
            "Spans by target",
            """SELECT target, COUNT(*) as count
               FROM spans
               GROUP BY target
               ORDER BY count DESC
               LIMIT 10""",
        ),
        # Show span names
        (
            "Span names",
            """SELECT name, level, COUNT(*) as count
               FROM spans
               GROUP BY name, level
               ORDER BY count DESC
               LIMIT 10""",
        ),
        # Show span events (enter/exit/close)
        (
            "Span event types",
            """SELECT event_type, COUNT(*) as count
               FROM span_events
               GROUP BY event_type
               ORDER BY event_type""",
        ),
        # Show trace events by level
        (
            "Events by level",
            """SELECT level, COUNT(*) as count
               FROM events
               GROUP BY level
               ORDER BY count DESC""",
        ),
        # Sample of actual spans
        (
            "Sample spans",
            """SELECT id, name, target, level, timestamp_us
               FROM spans
               ORDER BY timestamp_us DESC
               LIMIT 10""",
        ),
        # Sample of actual events
        (
            "Sample events",
            """SELECT name, target, level, timestamp_us, thread_name
               FROM events
               ORDER BY timestamp_us DESC
               LIMIT 10""",
        ),
        # Sample of actors
        (
            "Sample actors",
            """SELECT id, mesh_id, rank, full_name, timestamp_us
               FROM actors
               ORDER BY timestamp_us DESC
               LIMIT 10""",
        ),
        # Actors by name pattern
        (
            "Actors by name",
            """SELECT full_name, rank
               FROM actors
               ORDER BY full_name""",
        ),
        # Sample of actor meshes
        (
            "Sample actor meshes",
            """SELECT id, class, given_name, full_name, shape_json, parent_view_json, timestamp_us
               FROM actor_meshes
               ORDER BY timestamp_us DESC
               LIMIT 10""",
        ),
        # Actor meshes by name pattern
        (
            "Actor meshes by name",
            """SELECT given_name, class, shape_json, parent_view_json
               FROM actor_meshes
               ORDER BY given_name""",
        ),
        # Actor status events schema
        (
            "Schema of 'actor_status_events' table",
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_name = 'actor_status_events'
               ORDER BY ordinal_position""",
        ),
        # Actor status events by status
        (
            "Actor status transitions",
            """SELECT new_status, COUNT(*) as count
               FROM actor_status_events
               GROUP BY new_status
               ORDER BY count DESC""",
        ),
        # Actor status events joined with actors
        (
            "Actor status timeline",
            """SELECT a.full_name, s.new_status, s.prev_status, s.reason
               FROM actor_status_events s
               JOIN actors a ON s.actor_id = a.full_name
               ORDER BY s.timestamp_us""",
        ),
    ]

    for title, sql in queries:
        print(f">>> {title}")
        # Clean up multi-line SQL for display
        display_sql = " ".join(sql.split())
        print(f"sql> {display_sql}")

        try:
            start = time.time()
            table = engine.query(sql)
            elapsed = time.time() - start

            print_table(table)
            print(
                f"\n({table.num_rows} row{'s' if table.num_rows != 1 else ''} in {elapsed:.3f}s)"
            )
        except Exception as e:
            print(f"Error: {e}")
        print()

    print("Demo complete!")


if __name__ == "__main__":
    main()
