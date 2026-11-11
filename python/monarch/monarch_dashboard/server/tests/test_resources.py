# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch

import pyarrow as pa
from monarch.monarch_dashboard.fake_data.store import ArrowAdapter
from monarch.monarch_dashboard.server import db, resources


class ResourcesTest(unittest.TestCase):
    def setUp(self):
        schemas = {
            "host_metrics": [
                ("timestamp_us", pa.int64()),
                ("hostname", pa.string()),
                ("net_rx_bytes", pa.int64()),
                ("net_tx_bytes", pa.int64()),
                ("disk_read_bytes", pa.int64()),
                ("disk_write_bytes", pa.int64()),
            ],
            "gpu_metrics": [
                ("timestamp_us", pa.int64()),
                ("hostname", pa.string()),
                ("gpu_uuid", pa.string()),
                ("utilization_percent", pa.float64()),
            ],
            "snapshots": [("snapshot_id", pa.string()), ("snapshot_ts", pa.int64())],
            "host_nodes": [("snapshot_id", pa.string()), ("hostname", pa.string())],
        }
        self.data = ArrowAdapter(
            {
                name: pa.Table.from_pylist([], schema=pa.schema(fields))
                for name, fields in schemas.items()
            }
        )
        db.set_adapter(self.data)

    def sample(self, seconds, counter, hostname="worker's-host"):
        self.data.append_rows(
            "host_metrics",
            [
                {
                    "timestamp_us": seconds * 1_000_000,
                    "hostname": hostname,
                    "net_rx_bytes": counter,
                    "net_tx_bytes": counter,
                    "disk_read_bytes": None,
                    "disk_write_bytes": counter,
                }
            ],
        )

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
        self.data.append_rows(
            "snapshots",
            [
                {"snapshot_id": "old", "snapshot_ts": 1},
                {"snapshot_id": "new", "snapshot_ts": 2},
            ],
        )
        self.data.append_rows(
            "host_nodes",
            [
                {"snapshot_id": "old", "hostname": "gone"},
                {"snapshot_id": "new", "hostname": "missing"},
                {"snapshot_id": "new", "hostname": "missing"},
            ],
        )
        self.data.append_rows(
            "gpu_metrics",
            [
                {
                    "timestamp_us": 101,
                    "hostname": "gpu-host",
                    "gpu_uuid": "uuid",
                    "utilization_percent": None,
                },
                {
                    "timestamp_us": 102,
                    "hostname": "gpu-host",
                    "gpu_uuid": "uuid",
                    "utilization_percent": None,
                },
            ],
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
        self.data.remove_table("host_metrics")
        self.data.remove_table("gpu_metrics")
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
