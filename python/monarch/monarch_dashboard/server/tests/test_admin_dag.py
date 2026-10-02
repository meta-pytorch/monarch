# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for snapshot-backed dashboard topology construction."""

import unittest
from unittest.mock import patch

from monarch.monarch_dashboard.server import admin_dag


class AdminDagTest(unittest.TestCase):
    def test_resolution_error_node_does_not_hide_live_sibling(self) -> None:
        root = "root"
        host = "host:host_agent.service@tcp://host:1"
        live_proc = "worker<live>@worker<live>.tcp://host:1"
        stale_proc = "anon<stale>@anon<stale>.tcp://host:1"
        actor = "trainer<actor>.worker<live>@worker<live>.tcp://host:1"

        def query(sql: str, _params=()):
            if " FROM nodes " in sql:
                return [
                    {"node_id": root, "node_kind": "root"},
                    {"node_id": host, "node_kind": "host"},
                    {"node_id": live_proc, "node_kind": "proc"},
                    {"node_id": stale_proc, "node_kind": "error"},
                    {"node_id": actor, "node_kind": "actor"},
                ]
            if " FROM children " in sql:
                return [
                    {
                        "parent_id": root,
                        "child_id": host,
                        "is_system": False,
                        "child_sort_key": 0,
                    },
                    {
                        "parent_id": host,
                        "child_id": stale_proc,
                        "is_system": False,
                        "child_sort_key": 0,
                    },
                    {
                        "parent_id": host,
                        "child_id": live_proc,
                        "is_system": False,
                        "child_sort_key": 1,
                    },
                    {
                        "parent_id": live_proc,
                        "child_id": actor,
                        "is_system": False,
                        "child_sort_key": 0,
                    },
                ]
            if " FROM host_nodes " in sql:
                return [{"node_id": host, "addr": "tcp://host:1"}]
            if " FROM proc_nodes " in sql:
                return [
                    {
                        "node_id": live_proc,
                        "proc_name": "worker",
                        "is_poisoned": False,
                        "failed_actor_count": 0,
                    }
                ]
            if " FROM actor_nodes " in sql:
                return [
                    {
                        "node_id": actor,
                        "actor_status": "idle",
                        "is_system": False,
                    }
                ]
            # Telemetry enrichment and message edges are optional for this
            # topology-only regression test.
            return []

        with (
            patch.object(admin_dag.db, "_query_one", return_value={"snapshot_id": "s"}),
            patch.object(admin_dag.db, "_query", side_effect=query),
        ):
            result = admin_dag.build_admin_dag()

        entity_ids = {node["entity_id"] for node in result["nodes"]}
        self.assertEqual(entity_ids, {host, live_proc, actor})
        self.assertNotIn(stale_proc, entity_ids)
        self.assertFalse(result["snapshot_pending"])
        self.assertTrue(result["snapshot_partial"])
        self.assertEqual(result["resolution_error_count"], 1)
        self.assertTrue(
            all(
                edge["source_id"] in {node["id"] for node in result["nodes"]}
                and edge["target_id"] in {node["id"] for node in result["nodes"]}
                for edge in result["edges"]
            )
        )

    def test_no_snapshot_is_pending_not_partial(self) -> None:
        with patch.object(admin_dag.db, "_query_one", return_value=None):
            result = admin_dag.build_admin_dag()

        self.assertEqual(
            result,
            {
                "nodes": [],
                "edges": [],
                "snapshot_pending": True,
                "snapshot_partial": False,
                "resolution_error_count": 0,
            },
        )


if __name__ == "__main__":
    unittest.main()
