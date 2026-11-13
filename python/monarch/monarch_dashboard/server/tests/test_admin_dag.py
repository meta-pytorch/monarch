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
        host = "host:host_agent.service<2>@tcp://host:1"
        live_proc = "worker<live>@worker<live>.tcp://host:1"
        stale_proc = "anon<stale>@anon<stale>.tcp://host:1"
        actor = "logger_controller<actor>.worker<live>@worker<live>.tcp://host:1"
        system = "ordinary<system>.worker<live>@worker<live>.tcp://host:1"

        def query(sql: str, _params=()):
            if " FROM nodes " in sql:
                return [
                    {"node_id": root, "node_kind": "root"},
                    {"node_id": host, "node_kind": "host"},
                    {"node_id": live_proc, "node_kind": "proc"},
                    {"node_id": stale_proc, "node_kind": "error"},
                    {"node_id": actor, "node_kind": "actor"},
                    {"node_id": system, "node_kind": "actor"},
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
                        "child_id": system,
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
                return [
                    {
                        "node_id": host,
                        "addr": "tcp://host:1",
                        "hostname": "physical-host",
                    }
                ]
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
                    {"node_id": system, "actor_status": "idle", "is_system": True},
                    {
                        "node_id": actor,
                        "actor_status": "idle",
                        "is_system": False,
                    },
                ]
            # Telemetry enrichment and message edges are optional for this
            # topology-only regression test.
            return []

        with (
            patch.object(admin_dag.db, "_query_one", return_value={"snapshot_id": "s"}),
            patch.object(admin_dag.db, "_query", side_effect=query),
        ):
            result = admin_dag.build_admin_dag()
            unfiltered = admin_dag.build_admin_dag(hide_system=False)

        self.assertIn(system, {node["entity_id"] for node in unfiltered["nodes"]})
        self.assertEqual(
            next(n for n in result["nodes"] if n["entity_id"] == host)["hostname"],
            "physical-host",
        )
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

    def test_sidecar_host_is_labeled_and_not_the_controller(self) -> None:
        hosts = {
            "controller": "unix:@controller",
            "sidecar": "unix:@sidecar",
        }
        host_ref = {
            name: f"host:host_agent.service<{name}>@{addr}"
            for name, addr in hosts.items()
        }
        proc_ref = {name: f"local<{name}>@{addr}" for name, addr in hosts.items()}
        client_ref = {
            name: f"client.local<{name}>@{addr}" for name, addr in hosts.items()
        }

        def query(sql: str, _params=()):
            if " FROM nodes " in sql:
                return [{"node_id": "root", "node_kind": "root"}] + [
                    {"node_id": ref, "node_kind": kind}
                    for name in hosts
                    for ref, kind in (
                        (host_ref[name], "host"),
                        (proc_ref[name], "proc"),
                        (client_ref[name], "actor"),
                    )
                ]
            if " FROM children " in sql:
                return [
                    {
                        "parent_id": parent,
                        "child_id": child,
                        "is_system": child == client_ref[name],
                        "child_sort_key": 0,
                    }
                    for name in hosts
                    for parent, child in (
                        ("root", host_ref[name]),
                        (host_ref[name], proc_ref[name]),
                        (proc_ref[name], client_ref[name]),
                    )
                ]
            if " FROM host_nodes " in sql:
                return [
                    {"node_id": host_ref[name], "addr": addr, "hostname": "devvm"}
                    for name, addr in hosts.items()
                ]
            if " FROM actor_nodes " in sql:
                return [
                    {
                        "node_id": client_ref[name],
                        "actor_status": "idle",
                        "is_system": True,
                    }
                    for name in hosts
                ]
            return []

        def host_labels(hide_system: bool) -> dict:
            with (
                patch.object(
                    admin_dag.db, "_query_one", return_value={"snapshot_id": "s"}
                ),
                patch.object(admin_dag.db, "_query", side_effect=query),
            ):
                result = admin_dag.build_admin_dag(
                    hide_system=hide_system, sidecar_addr=hosts["sidecar"]
                )
            return {
                n["entity_id"]: n["label"]
                for n in result["nodes"]
                if n["tier"] == "host"
            }

        self.assertEqual(
            host_labels(hide_system=True),
            {host_ref["controller"]: "@controller (controller)"},
        )
        self.assertEqual(
            host_labels(hide_system=False),
            {
                host_ref["controller"]: "@controller (controller)",
                host_ref["sidecar"]: "Job sidecar",
            },
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
