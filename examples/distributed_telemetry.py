#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Distributed Telemetry with Real Tracing Data.

This example demonstrates querying real tracing data collected from actors:

1. Starts telemetry
2. Spawns actors that do work (generating real tracing events)
3. Queries the spans, span_events, events, and actors tables

The RecordBatchSink automatically captures tracing events and stores them
in queryable Arrow tables. Actor creation events are captured separately
via the ActorEventSink.

Usage:
    buck2 run //monarch/examples:distributed_telemetry
    buck2 run //monarch/examples:distributed_telemetry -- --summary
    buck2 run //monarch/examples:distributed_telemetry -- --interactive

To browse the dashboard interactively, use --interactive. This pauses after
actors are spawned so you can open the dashboard in a browser:
    buck2 run //monarch/examples:distributed_telemetry -- --interactive
    # Then SSH-tunnel: ssh -L 8265:localhost:8265 <devserver>
    # Open http://localhost:8265, press Ctrl+C to continue to queries.
"""

import argparse
import os
import time

import pyarrow as pa
from monarch.actor import Actor, endpoint
from monarch.distributed_telemetry.actor import start_telemetry
from monarch.job import ProcessJob, TelemetryConfig


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


class StoppingActor(Actor):
    """Actor that stops itself with a reason from within an endpoint."""

    @endpoint
    def do_work_then_stop(self) -> str:
        """Do some work, then stop the actor with a reason."""
        from monarch.actor import context

        context().actor_instance.stop("finished processing batch")
        return "stopped"


class FailingActor(Actor):
    """Actor that aborts itself, demonstrating failure telemetry."""

    @endpoint
    def fail(self) -> None:
        from monarch.actor import context

        context().actor_instance.abort("intentional failure for demo")


class SenderActor(Actor):
    """Actor that sends messages to another actor mesh.

    The sent_messages table records this actor's ID as sender_actor_id,
    joinable with actors.id to retrieve display_name.
    """

    @endpoint
    def send_compute(self, target: ComputeActor, iterations: int) -> int:
        """Send a compute request to the target actor mesh."""
        # pyre-ignore[29]: target is an ActorMesh
        return sum(target.compute.call(iterations).get().values())


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
        "Actors by full_name",
        """SELECT id, full_name
           FROM actors
           ORDER BY full_name""",
    ),
    # Actors by name pattern
    (
        "Actors by display_name",
        """SELECT id, display_name
           FROM actors
           WHERE display_name IS NOT NULL
           ORDER BY display_name""",
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
    # Find all prochmesh in each host mesh
    (
        "Proc mesh in each host mesh",
        """SELECT hm.given_name AS host_mesh_name,
                      pm.given_name AS proc_mesh_name,
                      pm.id AS proc_mesh_id
               FROM meshes pm
               INNER JOIN meshes hm ON pm.parent_mesh_id = hm.id
               WHERE hm.class = 'Host' AND pm.class = 'Proc'
               ORDER BY hm.given_name, pm.given_name""",
    ),
    # Find all actors in a proc mesh.
    # Regular actors: actor -> actor mesh (mesh_id) -> proc mesh (parent_mesh_id)
    # ProcAgent actors: actor -> proc mesh (mesh_id) directly
    # HostAgent actors: actor -> host mesh (mesh_id) directly
    (
        "Actors in each host mesh",
        """SELECT hm.given_name AS host_mesh_name,
                  pm.given_name AS proc_mesh_name,
                  am.given_name AS actor_mesh_name,
                  a.full_name AS actor_name,
                   a.rank
           FROM actors a
           JOIN meshes am ON a.mesh_id = am.id
           JOIN meshes pm ON am.parent_mesh_id = pm.id
           JOIN meshes hm ON pm.parent_mesh_id = hm.id
           WHERE hm.class = 'Host'
           UNION ALL
           SELECT hm.given_name AS host_mesh_name,
                  pm.given_name AS proc_mesh_name,
                  pm.given_name AS actor_mesh_name,
                  a.full_name AS actor_name,
                  a.rank
           FROM actors a
           JOIN meshes pm ON a.mesh_id = pm.id
           JOIN meshes hm ON pm.parent_mesh_id = hm.id
           WHERE pm.class = 'Proc' AND hm.class = 'Host'
           UNION ALL
           SELECT hm.given_name AS host_mesh_name,
                  hm.given_name AS proc_mesh_name,
                  hm.given_name AS actor_mesh_name,
                  a.full_name AS actor_name,
                  a.rank
           FROM actors a
           JOIN meshes hm ON a.mesh_id = hm.id
           WHERE hm.class = 'Host'
           ORDER BY host_mesh_name, proc_mesh_name, actor_mesh_name, rank""",
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
        """SELECT a.full_name, s.new_status, s.reason
           FROM actor_status_events s
           JOIN actors a ON s.actor_id = a.id
           ORDER BY s.timestamp_us""",
    ),
    # Show stop/failure reasons for terminal actors
    (
        "Actor stop and failure reasons",
        """SELECT a.full_name, s.new_status, s.reason
           FROM actor_status_events s
           JOIN actors a ON s.actor_id = a.id
           WHERE s.new_status IN ('Stopped', 'Failed')
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
    (
        "Count of sent messages",
        "SELECT COUNT(*) as total_sent_messages FROM sent_messages",
    ),
    (
        "Sent messages to 'compute' actor mesh",
        """SELECT sm.id, sm.timestamp_us, a.display_name AS sender, m.given_name AS mesh_name
           FROM sent_messages sm
           LEFT JOIN meshes m ON sm.actor_mesh_id = m.id
           LEFT JOIN actors a ON sm.sender_actor_id = a.id
           WHERE m.given_name = 'compute'
           ORDER BY sm.timestamp_us DESC""",
    ),
    (
        "Sample sent messages",
        """SELECT sm.timestamp_us, a.display_name AS sender,
                  sm.view_json, sm.shape_json
           FROM sent_messages sm
           JOIN actors a ON sm.sender_actor_id = a.id
           ORDER BY sm.timestamp_us DESC
           LIMIT 10""",
    ),
    (
        "Received Messages",
        """SELECT m.id, m.timestamp_us, m.port_id,
                  sender.full_name AS from_actor, receiver.full_name AS to_actor,
           FROM messages m
           LEFT JOIN actors sender ON m.from_actor_id = sender.id
           LEFT JOIN actors receiver ON m.to_actor_id = receiver.id
           ORDER BY m.timestamp_us
           LIMIT 10""",
    ),
    (
        "Messages received by 'compuate' actor mesh sent from 'sender' actor mesh",
        """SELECT m.id, m.timestamp_us,
                  sender.display_name AS from_actor, receiver.display_name AS to_actor,
                  m.port_id
           FROM messages m
           JOIN actors sender ON m.from_actor_id = sender.id
           JOIN actors receiver ON m.to_actor_id = receiver.id
           JOIN meshes sm ON sender.mesh_id = sm.id
           JOIN meshes rm ON receiver.mesh_id = rm.id
           WHERE sm.given_name = 'sender' AND rm.given_name = 'compute'
           ORDER BY m.timestamp_us DESC""",
    ),
    (
        "Message Status Events",
        "SELECT * FROM message_status_events ORDER BY timestamp_us LIMIT 10",
    ),
    (
        "Messages by endpoint",
        """SELECT m.endpoint, COUNT(*) as cnt
           FROM messages m
           WHERE m.endpoint IS NOT NULL
           GROUP BY m.endpoint
           ORDER BY cnt DESC""",
    ),
    (
        "Lifecycle: sender -> compute messages",
        """SELECT sender.display_name AS from_actor,
                  receiver.display_name AS to_actor,
                  m.endpoint,
                  mse.status,
                  mse.timestamp_us
           FROM messages m
           INNER JOIN message_status_events mse ON m.id = mse.message_id
           LEFT JOIN actors sender ON m.from_actor_id = sender.id
           LEFT JOIN actors receiver ON m.to_actor_id = receiver.id
           LEFT JOIN meshes sm ON sender.mesh_id = sm.id
           LEFT JOIN meshes rm ON receiver.mesh_id = rm.id
           WHERE sm.given_name = 'sender' AND rm.given_name = 'compute'
           ORDER BY m.id, mse.timestamp_us""",
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


def run_workload(job, summary=False, interactive=False):
    """Run the full telemetry demo: spawn actors, run work, query, and shut down.

    Args:
        job: JobTrait whose state has a "workers" HostMesh.
            If the job was created with ``telemetry=TelemetryConfig()``, the
            query engine is available via ``state.query_engine`` and
            ``start_telemetry()`` does not need to be called separately.
        summary: If True, print summary output instead of full tables.
        interactive: If True, pause after setup so the dashboard can be browsed.
    """
    print("=" * 50)
    print()

    state = job.state(cached_path=None)

    # Use engine from JobState if available (telemetry configured on job),
    # otherwise fall back to manual start_telemetry() for backward compat.
    engine = state.query_engine
    if engine is None:
        engine, _ = start_telemetry()

    hosts = state.hosts

    procs = hosts.spawn_procs(per_host={"workers": 2}, name="workers")

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

    print("Spawning sender actor for actor-to-actor messaging...")
    # pyre-ignore[29]: procs is a ProcMesh
    sender = procs.slice(hosts=0, workers=0).spawn("sender", SenderActor)

    print("Sending from sender actor to compute actors...")
    # pyre-ignore[29]: sender is an ActorMesh
    result = sender.send_compute.call_one(actors, 42).get()
    print(f"Sender-to-compute result: {result}")

    print("Spawning a child process...")
    # pyre-ignore[29]: actors is an ActorMesh
    actors.slice(hosts=0, workers=0).spawn_child_work.call_one().get()

    print("Stopping sender actor...")
    sender.stop().get()

    # Issue a warm-up query to ensure telemetry children are spawned on
    # the workers.  The coordinator lazily spawns telemetry actors via
    # _spawn_missing_children() during the first scan; without this query
    # the telemetry actors would not exist when the abort fires, so the
    # Failed status event would not be captured by the distributed scan.
    # TODO: Remove this once we have a better way to ensure the telemetry actors are spawned.
    engine.query("SELECT COUNT(*) FROM actors")

    print("Spawning an actor that stops itself with a reason...")
    # pyre-ignore[29]: procs is a ProcMesh
    stopper = procs.slice(hosts=0, workers=0).spawn("stopper", StoppingActor)
    # pyre-ignore[29]: stopper is an ActorMesh
    result = stopper.do_work_then_stop.call_one().get()
    print(f"Stopper result before stopping: {result}")

    print("Spawning an actor that fails...")
    # pyre-ignore[29]: procs is a ProcMesh
    failer = procs.slice(hosts=0, workers=0).spawn("failer", FailingActor)
    try:
        # pyre-ignore[29]: failer is an ActorMesh
        failer.fail.call_one().get()
        time.sleep(2.0)
    except KeyboardInterrupt:
        pass  # Expected: the abort propagates an error back

    if interactive:
        import signal

        dashboard_url = os.environ.get("MONARCH_DASHBOARD_URL", "http://localhost:8265")
        print(f"\nDashboard at {dashboard_url}")
        print("Press Ctrl+C to continue to queries...")
        try:
            signal.pause()
        except KeyboardInterrupt:
            print()

    print()
    print("Querying real telemetry data...")
    print("-" * 50)
    print()

    run_queries(engine, summary=summary)

    print("Demo complete!")

    hosts.shutdown().get()


def main(summary: bool = False, interactive: bool = False) -> None:
    run_workload(
        ProcessJob({"hosts": 2}).enable_telemetry(TelemetryConfig()),
        summary=summary,
        interactive=interactive,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", action="store_true")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Pause after setup so the dashboard can be browsed",
    )
    args = parser.parse_args()

    main(summary=args.summary, interactive=args.interactive)
