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
    buck2 run //monarch/examples:distributed_telemetry_real_data
    buck2 run //monarch/examples:distributed_telemetry_real_data -- --summary
"""

import argparse
import os
import time

# Enable unified telemetry layer before importing monarch
os.environ["USE_UNIFIED_LAYER"] = "true"

import pyarrow as pa
from monarch.actor import Actor, endpoint
from monarch.distributed_telemetry import start_telemetry
from monarch.job import ProcessJob


NUM_WORKERS = 4  # Match MAST total: WORKERS_PER_HOST * MAST_HOSTS


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
    def spawn_child_work(self) -> int:
        """Spawn a child process and do work in it, generating more trace events."""
        from monarch.actor import this_host

        child_procs = this_host().spawn_procs(name="child_worker")
        child = child_procs.spawn("child_compute", ComputeActor)
        # pyre-ignore[29]: child is an ActorMesh
        return child.compute.call_one(100).get()


def print_table(table: pa.Table, max_rows: int = 50) -> None:
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


# Shared queries for telemetry tables
QUERIES = [
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
        "Schema of 'meshes' table",
        """SELECT column_name, data_type, is_nullable
           FROM information_schema.columns
           WHERE table_name = 'meshes'
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
        "Count of meshes",
        "SELECT COUNT(*) as total_meshes FROM meshes",
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
    # Sample of meshes
    (
        "Sample meshes",
        """SELECT id, class, given_name, full_name, shape_json, parent_view_json, timestamp_us
           FROM meshes
           ORDER BY timestamp_us DESC
           LIMIT 10""",
    ),
    # Meshes by name pattern
    (
        "meshes by name",
        """SELECT given_name, class, shape_json, parent_view_json
           FROM meshes
           ORDER BY given_name""",
    ),
    # Find all actors in a proc mesh.
    # Regular actors: actor -> actor mesh (mesh_id) -> proc mesh (parent_mesh_id)
    # ProcMeshAgent actors: actor -> proc mesh (mesh_id) directly
    (
        "Actors in each proc mesh",
        """SELECT pm.given_name AS proc_mesh_name,
                  am.given_name AS actor_mesh_name,
                  a.full_name AS actor_name,
                  a.rank
           FROM actors a
           JOIN meshes am ON a.mesh_id = am.id
           JOIN meshes pm ON am.parent_mesh_id = pm.id
           WHERE pm.class = 'Proc'
           UNION ALL
           SELECT pm.given_name AS proc_mesh_name,
                  pm.given_name AS actor_mesh_name,
                  a.full_name AS actor_name,
                  a.rank
           FROM actors a
           JOIN meshes pm ON a.mesh_id = pm.id
           WHERE pm.class = 'Proc'
           ORDER BY proc_mesh_name, actor_mesh_name, rank""",
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
    (
        "Actors by class",
        """SELECT class, count(*) as cnt
           FROM meshes
           GROUP BY class""",
    ),
    # All actors detail
    (
        "All actors detail",
        """SELECT full_name, mesh_id, rank
           FROM actors
           ORDER BY full_name""",
    ),
    # Meshes detail
    (
        "Meshes detail",
        """SELECT given_name, class, shape_json
           FROM meshes""",
    ),
]


def print_summary(title: str, table: pa.Table) -> None:
    """Print a diff-friendly summary of a query result."""
    df = table.to_pandas()
    print(f">>> {title}: {table.num_rows} rows")
    # For grouped queries with a 'count'/'cnt' column, list each group
    count_col = None
    for col in ("count", "cnt"):
        if col in df.columns:
            count_col = col
            break
    if count_col is not None:
        # Find the grouping column(s) — everything except the count column
        group_cols = [c for c in df.columns if c != count_col]
        if group_cols:
            sorted_df = df.sort_values(group_cols).reset_index(drop=True)
            for _, row in sorted_df.iterrows():
                key = ", ".join(str(row[c]) for c in group_cols)
                print(f"  {key}: {row[count_col]}")


def run_queries(engine, summary: bool = False) -> None:
    """Run all telemetry queries against the engine."""
    for title, sql in QUERIES:
        try:
            start = time.time()
            table = engine.query(sql)
            if summary:
                print_summary(title, table)
                continue
        except Exception as e:
            if summary:
                print(f">>> {title}: Error: {e}")
                continue
            print(f">>> {title}")
            print(f"Error: {e}")
            print()
            continue

        print(f">>> {title}")
        # Clean up multi-line SQL for display
        display_sql = " ".join(sql.split())
        print(f"sql> {display_sql}")

        elapsed = time.time() - start

        print_table(table)
        print(
            f"\n({table.num_rows} row{'s' if table.num_rows != 1 else ''} in {elapsed:.3f}s)"
        )
        print()


def run_workload(procs, summary=False, spawn_child=None):
    """Shared workload: start telemetry, spawn actors, run work, and query.

    Args:
        procs: The proc mesh to spawn actors on.
        summary: If True, print summary output instead of full tables.
        spawn_child: Optional callable(actors) to spawn a child process and do work.
    """
    print("Starting telemetry with real data collection...")
    engine = start_telemetry(use_fake_data=False)

    print("Spawning compute actors...")
    # pyre-ignore[29]: procs is a ProcMesh
    actors = procs.spawn("compute", ComputeActor)

    print("Doing computation work...")
    # pyre-ignore[29]: actors is an ActorMesh
    results = actors.compute.call(1000).get()
    print(f"Computation results: {list(results)}")

    print("Doing nested work...")
    # pyre-ignore[29]: actors is an ActorMesh
    nested_results = actors.nested_work.call(3).get()
    print(f"Nested work results: {list(nested_results)}")

    if spawn_child is not None:
        spawn_child(actors)

    # Give a moment for all trace events to be flushed
    print("Waiting for trace events to flush...")
    time.sleep(1.0)

    print()
    print("Querying real telemetry data...")
    print("-" * 50)
    print()

    run_queries(engine, summary=summary)

    print("Demo complete!")


def main(summary: bool = False) -> None:
    print("Distributed Telemetry - Real Tracing Data Demo")
    print("=" * 50)
    print()

    # Spawn worker processes - telemetry automatically tracks them
    print(f"Spawning {NUM_WORKERS} worker processes...")
    hosts = ProcessJob({"hosts": 1}).state(cached_path=None).hosts
    procs = hosts.spawn_procs(per_host={"workers": NUM_WORKERS}, name="workers")

    def spawn_child(_actors):
        print("Spawning a child process...")
        child_procs = hosts.spawn_procs(name="child_worker")
        child_actors = child_procs.spawn("child_compute", ComputeActor)
        # pyre-ignore[29]: child_actors is an ActorMesh
        child_actors.compute.call(100).get()

    run_workload(procs, summary, spawn_child=spawn_child)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args()

    main(summary=args.summary)
