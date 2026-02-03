#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Distributed SQL Query Demo using Monarch.

This example demonstrates distributed SQL queries using the three-component
architecture:
1. DatabaseScanner (Rust): Local MemTable operations
2. DistributedTelemetryActor (Python): Orchestrates children
3. QueryEngine (Rust): DataFusion query execution

The telemetry system automatically tracks all spawned ProcMeshes. Just call
start_telemetry() before spawning any procs.

Usage:
    python distributed_telemetry_hello_world.py
"""

import time

import pyarrow as pa
from monarch.actor import Actor, endpoint, this_host
from monarch.distributed_telemetry import start_telemetry


NUM_WORKERS = 3


class WorkerActor(Actor):
    """Simple worker actor that can spawn child processes."""

    @endpoint
    def spawn_child(self, name: str) -> None:
        """Spawn a child process from this worker's host."""
        this_host().spawn_procs(name=name)


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
    print("Distributed SQL Query Demo (Monarch)")
    print("=====================================")
    print()

    # Start telemetry with fake demo data - returns a QueryEngine directly
    print("Starting telemetry with fake demo data...")
    engine = start_telemetry(use_fake_data=True)

    # Spawn worker processes - telemetry automatically tracks them.
    print(f"Spawning {NUM_WORKERS} worker processes...")
    worker_procs = this_host().spawn_procs(per_host={"workers": NUM_WORKERS})

    # Spawn worker actors for business logic
    print("Spawning worker actors...")
    workers = worker_procs.spawn("worker", WorkerActor)

    # Have worker 0 spawn a grandchild - telemetry automatically tracks it too.
    print("Having worker 0 spawn a grandchild process...")
    workers.slice(workers=0).spawn_child.call_one("grandchild").get()

    print("Registered tables with DataFusion")
    print(f"Data from coordinator + {NUM_WORKERS} workers + 1 grandchild")
    print()

    # Demo queries
    queries = [
        # Count total rows to verify all sources contribute
        "SELECT COUNT(*) as total_hosts FROM hosts",
        "SELECT COUNT(*) as total_metrics FROM metrics",
        # Sample data from hosts table
        "SELECT * FROM hosts ORDER BY host_id LIMIT 10",
        # Aggregation across all sources
        """SELECT h.datacenter, ROUND(AVG(m.value), 2) as avg_cpu
           FROM metrics m
           JOIN hosts h ON m.host_id = h.host_id
           WHERE m.metric_name = 'cpu_usage'
           GROUP BY h.datacenter
           ORDER BY h.datacenter""",
        """SELECT h.hostname, h.os, ROUND(AVG(m.value), 2) as avg_memory
           FROM metrics m
           JOIN hosts h ON m.host_id = h.host_id
           WHERE m.metric_name = 'memory_usage'
           GROUP BY h.hostname, h.os
           ORDER BY avg_memory DESC
           LIMIT 5""",
    ]

    for sql in queries:
        # Clean up multi-line SQL for display
        display_sql = " ".join(sql.split())
        print(f"sql> {display_sql}")

        start = time.time()
        table = engine.query(sql)
        elapsed = time.time() - start

        print_table(table)
        print(
            f"\n({table.num_rows} row{'s' if table.num_rows != 1 else ''} in {elapsed:.3f}s)"
        )
        print()

    print("Demo complete!")


if __name__ == "__main__":
    main()
