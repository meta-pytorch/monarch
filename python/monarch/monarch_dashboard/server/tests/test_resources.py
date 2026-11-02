# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from monarch.monarch_dashboard.server import db, resources


class ResourcesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = self.tmp.name + "/metrics.db"
        db.init(self.path)
        self.conn = sqlite3.connect(self.path)
        self.addCleanup(self.conn.close)
        self.conn.executescript(
            "CREATE TABLE host_metrics (timestamp_us INTEGER, hostname TEXT, "
            "net_rx_bytes INTEGER, net_tx_bytes INTEGER, disk_read_bytes INTEGER, disk_write_bytes INTEGER);"
            "CREATE TABLE gpu_metrics (timestamp_us INTEGER, hostname TEXT, gpu_uuid TEXT, utilization_percent REAL);"
            "CREATE TABLE snapshots (snapshot_id TEXT, snapshot_ts INTEGER);"
            "CREATE TABLE host_nodes (snapshot_id TEXT, hostname TEXT);"
        )

    def sample(self, seconds, counter, hostname="worker's-host"):
        self.conn.execute(
            "INSERT INTO host_metrics VALUES (?, ?, ?, ?, ?, ?)",
            (seconds * 1_000_000, hostname, counter, counter, None, counter),
        )
        self.conn.commit()

    def history(self, now=400):
        with patch.object(resources.time, "time_ns", return_value=now * 1_000_000_000):
            return resources.history("worker's-host")

    def test_rates_use_predecessor_and_actual_elapsed_time(self):
        self.sample(99, 100)
        self.sample(101, 500)
        self.sample(104, 1100)
        rows = self.history()["hosts"]
        self.assertEqual([r["timestamp_us"] for r in rows], [101_000_000, 104_000_000])
        self.assertEqual([r["net_rx_bytes_per_sec"] for r in rows], [200, 200])
        self.assertTrue(all(r["disk_read_bytes_per_sec"] is None for r in rows))

    def test_resets_duplicates_and_gaps_are_unavailable(self):
        for sec, value in [(101, 100), (102, 50), (102, 50), (120, 900), (121, 1000)]:
            self.sample(sec, value)
        rows = self.history()["hosts"]
        self.assertEqual(
            [r["net_tx_bytes_per_sec"] for r in rows], [None, None, None, None, 100]
        )

    def test_inventory_deduplicates_hosts_and_keeps_missing_hosts(self):
        self.sample(101, 100)
        self.sample(102, 200)
        self.conn.executescript(
            "INSERT INTO snapshots VALUES ('old', 1), ('new', 2);"
            "INSERT INTO host_nodes VALUES ('old', 'gone'), ('new', 'missing'), ('new', 'missing');"
            "INSERT INTO gpu_metrics VALUES (101, 'gpu-host', 'uuid', NULL), (102, 'gpu-host', 'uuid', NULL);"
        )
        result = resources.inventory()
        hosts = {h["hostname"]: h for h in result["hosts"]}
        self.assertEqual(set(hosts), {"worker's-host", "missing", "gpu-host"})
        self.assertEqual(hosts["worker's-host"]["latest"]["net_rx_bytes"], 200)
        self.assertIsNone(hosts["missing"]["latest"])
        self.assertEqual(len(hosts["gpu-host"]["gpus"]), 1)
        self.assertIsNone(hosts["gpu-host"]["gpus"][0]["utilization_percent"])

    def test_history_cap_is_reported_and_preserves_recent_samples(self):
        for second in range(10):
            self.sample(second, second)
        with patch.object(resources, "SAMPLES_PER_DEVICE", 3):
            result = self.history(now=10)
        self.assertTrue(result["truncated"])
        self.assertEqual(
            [r["timestamp_us"] for r in result["hosts"]],
            [7_000_000, 8_000_000, 9_000_000],
        )

    def test_missing_tables_and_other_hosts(self):
        self.sample(102, 200, "other-host")
        self.assertEqual(self.history()["hosts"], [])
        self.conn.executescript("DROP TABLE host_metrics; DROP TABLE gpu_metrics;")
        result = resources.inventory()
        self.assertEqual(result["hosts"], [])
        self.assertFalse(result["host_metrics_available"])
        self.assertFalse(result["gpu_metrics_available"])

    def test_inventory_limits_are_reported(self):
        self.sample(102, 200, "a")
        self.sample(102, 200, "b")
        with patch.object(resources, "HOST_LIMIT", 1):
            result = resources.inventory()
        self.assertTrue(result["truncated"])
        self.assertEqual([h["hostname"] for h in result["hosts"]], ["a"])
