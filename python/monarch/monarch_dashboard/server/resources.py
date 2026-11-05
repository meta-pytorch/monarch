# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Bounded, physical-host resource inventory and recent samples."""

import time
from typing import Any

from . import db

HOST_LIMIT = 1000
GPU_LIMIT = 4096
HISTORY_SECONDS = 300
SAMPLES_PER_DEVICE = 601
MAX_HISTORY_ROWS = 8192
MAX_RATE_GAP_SECONDS = 10


def _latest(table: str, partition: str, limit: int) -> tuple[list[dict], bool]:
    rows = db.raw_query(
        f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER ("
        f"PARTITION BY {partition} ORDER BY timestamp_us DESC) AS rn "
        f"FROM {table}) ranked WHERE rn = 1 ORDER BY {partition} LIMIT {limit + 1}"
    )
    for row in rows:
        row.pop("rn", None)
    return rows[:limit], len(rows) > limit


def inventory() -> dict[str, Any]:
    """Latest readings, deduplicated by physical hostname and GPU UUID."""
    tables = set(db._get_adapter().table_names())
    hosts, host_truncated = (
        _latest("host_metrics", "hostname", HOST_LIMIT)
        if "host_metrics" in tables
        else ([], False)
    )
    gpus, gpu_truncated = (
        _latest("gpu_metrics", "hostname, gpu_uuid", GPU_LIMIT)
        if "gpu_metrics" in tables
        else ([], False)
    )
    registered = []
    if {"host_nodes", "snapshots"}.issubset(tables):
        registered = db.raw_query(
            "SELECT DISTINCT hostname FROM host_nodes WHERE snapshot_id = "
            "(SELECT snapshot_id FROM snapshots ORDER BY snapshot_ts DESC LIMIT 1) "
            f"AND hostname IS NOT NULL ORDER BY hostname LIMIT {HOST_LIMIT + 1}"
        )
    by_host = {
        r["hostname"]: {"hostname": r["hostname"], "latest": r, "gpus": []}
        for r in hosts
    }
    for row in registered:
        name = row["hostname"]
        by_host.setdefault(name, {"hostname": name, "latest": None, "gpus": []})
    for gpu in gpus:
        name = gpu["hostname"]
        by_host.setdefault(name, {"hostname": name, "latest": None, "gpus": []})[
            "gpus"
        ].append(gpu)
    names = sorted(by_host)
    return {
        "generated_at_us": time.time_ns() // 1000,
        "hosts": [by_host[name] for name in names[:HOST_LIMIT]],
        "truncated": host_truncated
        or gpu_truncated
        or len(names) > HOST_LIMIT
        or len(registered) > HOST_LIMIT,
        "host_metrics_available": "host_metrics" in tables,
        "gpu_metrics_available": "gpu_metrics" in tables,
    }


def _history(table: str, hostname: str, partition: str) -> tuple[list[dict], bool]:
    rows = db._query(
        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER ("
        f"PARTITION BY {partition} ORDER BY timestamp_us DESC) AS rn "
        f"FROM {table} WHERE hostname = ?) ranked "
        f"WHERE rn <= {SAMPLES_PER_DEVICE + 1} "
        f"ORDER BY {partition}, timestamp_us DESC LIMIT {MAX_HISTORY_ROWS + 1}",
        (hostname,),
    )
    return rows[:MAX_HISTORY_ROWS], len(rows) > MAX_HISTORY_ROWS


def history(hostname: str) -> dict[str, Any]:
    """Five-minute window; retain one predecessor for counter deltas."""
    end_us = time.time_ns() // 1000
    start_us = end_us - HISTORY_SECONDS * 1_000_000
    tables = set(db._get_adapter().table_names())
    hosts, host_truncated = (
        _history("host_metrics", hostname, "hostname")
        if "host_metrics" in tables
        else ([], False)
    )
    gpus, gpu_truncated = (
        _history("gpu_metrics", hostname, "gpu_uuid")
        if "gpu_metrics" in tables
        else ([], False)
    )
    capped = any(
        r["rn"] > SAMPLES_PER_DEVICE and r["timestamp_us"] >= start_us
        for r in hosts + gpus
    )
    hosts = [r for r in hosts if r["rn"] <= SAMPLES_PER_DEVICE]
    gpus = [r for r in gpus if r["rn"] <= SAMPLES_PER_DEVICE]
    hosts.sort(key=lambda r: r["timestamp_us"])
    previous = None
    for row in hosts:
        row.pop("rn", None)
        dt = (
            (row["timestamp_us"] - previous["timestamp_us"]) / 1_000_000
            if previous
            else 0
        )
        for counter in (
            "net_rx_bytes",
            "net_tx_bytes",
            "disk_read_bytes",
            "disk_write_bytes",
        ):
            value = row.get(counter)
            before = previous.get(counter) if previous else None
            row[counter.removesuffix("_bytes") + "_bytes_per_sec"] = (
                (value - before) / dt
                if value is not None
                and before is not None
                and 0 < dt <= MAX_RATE_GAP_SECONDS
                and value >= before
                else None
            )
        previous = row
    for row in gpus:
        row.pop("rn", None)
    return {
        "hostname": hostname,
        "start_us": start_us,
        "end_us": end_us,
        "hosts": [r for r in hosts if start_us <= r["timestamp_us"] <= end_us],
        "gpus": sorted(
            [r for r in gpus if start_us <= r["timestamp_us"] <= end_us],
            key=lambda r: r["timestamp_us"],
        ),
        "truncated": capped or host_truncated or gpu_truncated,
        "max_gap_seconds": MAX_RATE_GAP_SECONDS,
    }
