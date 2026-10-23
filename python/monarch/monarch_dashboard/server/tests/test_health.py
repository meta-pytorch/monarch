# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Regression coverage for health after successful iterative executions."""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from monarch.monarch_dashboard.fake_data.generate import generate
from monarch.monarch_dashboard.server import db


class DashboardHealthTest(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self._path = Path(self._directory.name) / "health.db"
        generate(str(self._path))
        db.init(str(self._path))
        self._event_id = 0
        with sqlite3.connect(self._path) as connection:
            # These lifecycle tests treat every fixture actor as user workload.
            # Agent membership is exercised separately below.
            connection.execute(
                "UPDATE meshes SET class='TestWorkload' WHERE class IN ('Host', 'Proc')"
            )
            connection.execute("DELETE FROM actor_status_events")
        self._actors = db.list_actors()

    def _status(self, actor_id: int, status: str, reason: str | None = None):
        self._event_id += 1
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                "INSERT INTO actor_status_events "
                "(id, timestamp_us, actor_id, new_status, reason) VALUES (?, ?, ?, ?, ?)",
                (self._event_id, self._event_id * 1_000_000, actor_id, status, reason),
            )

    def test_completed_executions_and_normal_transitions_are_healthy(self):
        for actor in self._actors:
            self._status(actor["id"], "Idle")
        for actor in self._actors[1:]:
            for status in (
                "Created",
                "Initializing",
                "Loading",
                "Processing",
                "Saving",
                "Stopping",
                "Stopped",
            ):
                self._status(
                    actor["id"],
                    status,
                    "requested stop" if status == "Stopped" else None,
                )
                summary = db.get_summary()
                self.assertEqual(summary["health_status"], "healthy", status)
                self.assertEqual(summary["errors"]["failed_actors"], [])
        self.assertEqual(summary["actor_counts"]["total"], len(self._actors))
        self.assertEqual(
            len(summary["errors"]["stopped_actors"]), len(self._actors) - 1
        )
        self.assertEqual(
            summary["errors"]["stopped_actors"][0]["reason"], "requested stop"
        )
        self.assertNotIn("health_score", summary)

    def test_failure_is_not_diluted_by_healthy_or_stopped_actors(self):
        for actor in self._actors:
            self._status(actor["id"], "Idle")
        self._status(self._actors[0]["id"], "Failed", "handler failed")
        summary = db.get_summary()
        self.assertEqual(summary["health_status"], "unhealthy")
        for actor in self._actors[2:]:
            self._status(actor["id"], "Stopped", "stopped by client")
        summary = db.get_summary()
        self.assertEqual(summary["health_status"], "unhealthy")
        self.assertEqual(len(summary["errors"]["failed_actors"]), 1)
        self.assertEqual(
            summary["errors"]["failed_actors"][0]["reason"], "handler failed"
        )
        self.assertEqual(summary["actor_counts"]["by_status"]["failed"], 1)

    def test_failure_takes_precedence_over_missing_telemetry(self):
        self._status(self._actors[0]["id"], "Failed", "handler failed")
        summary = db.get_summary()
        self.assertEqual(summary["health_status"], "unhealthy")
        self.assertEqual(summary["health_coverage"]["unknown"], len(self._actors) - 1)

    def test_absent_or_stopped_work_retains_coverage(self):
        summary = db.get_summary()
        self.assertEqual(summary["health_status"], "unknown")
        self.assertEqual(summary["health_coverage"]["known"], 0)
        self.assertEqual(summary["health_coverage"]["unknown"], len(self._actors))
        for actor in self._actors:
            self._status(actor["id"], "Stopped", "requested stop")
        summary = db.get_summary()
        self.assertEqual(summary["health_status"], "inactive")
        self.assertEqual(summary["health_coverage"]["stopped"], len(self._actors))

    def test_partial_and_unrecognized_states_are_unknown(self):
        for actor in self._actors:
            self._status(actor["id"], "Stopping")
        for status in ("Unknown", "FutureStatus", ""):
            self._status(self._actors[0]["id"], status)
            summary = db.get_summary()
            self.assertEqual(summary["health_status"], "unknown", status)
            self.assertEqual(summary["health_coverage"]["known"], len(self._actors) - 1)
            self.assertEqual(summary["health_coverage"]["unknown"], 1)

    def test_latest_normal_state_recovers_reported_failure(self):
        for actor in self._actors:
            self._status(actor["id"], "Idle")
        self._status(self._actors[0]["id"], "Failed", "handler failed")
        self.assertEqual(db.get_summary()["health_status"], "unhealthy")
        self._status(self._actors[0]["id"], "Initializing")
        summary = db.get_summary()
        self.assertEqual(summary["health_status"], "healthy")
        self.assertEqual(summary["errors"]["failed_actors"], [])

    def _snapshot_tables(self):
        with sqlite3.connect(self._path) as connection:
            connection.executescript(
                "CREATE TABLE snapshots (snapshot_id TEXT, snapshot_ts INTEGER);"
                "CREATE TABLE actor_nodes (snapshot_id TEXT, node_id TEXT, is_system BOOLEAN);"
            )

    def _snapshot_role(self, snapshot_id, timestamp, actor, flag):
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                "INSERT INTO snapshots VALUES (?, ?)", (snapshot_id, timestamp)
            )
            connection.execute(
                "INSERT INTO actor_nodes VALUES (?, ?, ?)",
                (snapshot_id, actor["full_name"], flag),
            )

    def test_roles_come_from_snapshot_flags_not_names(self):
        self._snapshot_tables()
        for actor in self._actors:
            self._status(actor["id"], "Idle")
        actor = self._actors[0]
        for name in ("logger", "controller", "SetupActor", "telemetry", "HostAgent"):
            with sqlite3.connect(self._path) as connection:
                connection.execute(
                    "UPDATE actors SET full_name=? WHERE id=?", (name, actor["id"])
                )
            actor["full_name"] = name
            self._status(actor["id"], "Failed")
            self._snapshot_role(name, self._event_id, actor, False)
            self.assertFalse(db.list_actors()[0]["is_system"])
            self.assertEqual(db.get_summary()["health_status"], "unhealthy", name)
        # A neutral name with an explicit flag is still a system actor.
        self._snapshot_role("system", self._event_id + 1, actor, True)
        self.assertTrue(db.list_actors()[0]["is_system"])
        self.assertEqual(db.get_summary()["health_status"], "healthy")
        # Exclusion from workload health does not erase reported failures.
        self.assertEqual(len(db.get_summary()["errors"]["failed_actors"]), 1)

    def test_missing_empty_and_unpublished_snapshots_do_not_guess_roles(self):
        self._status(self._actors[0]["id"], "Failed")
        self.assertIsNone(db.list_actors()[0]["is_system"])
        self._snapshot_tables()
        self.assertIsNone(db.list_actors()[0]["is_system"])
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                "INSERT INTO actor_nodes VALUES (?, ?, ?)",
                ("not-published", self._actors[0]["full_name"], True),
            )
        self.assertIsNone(db.list_actors()[0]["is_system"])
        self.assertEqual(db.get_summary()["health_status"], "unhealthy")

    def test_latest_role_survives_omission_and_does_not_duplicate_actors(self):
        self._snapshot_tables()
        actor = self._actors[0]
        self._snapshot_role("old", 100, actor, False)
        self._snapshot_role("new", 200, actor, True)
        self._snapshot_role("other", 300, self._actors[1], False)
        rows = db.list_actors()
        self.assertEqual(len(rows), len(self._actors))
        self.assertTrue(rows[0]["is_system"])
        self.assertFalse(rows[1]["is_system"])
        self.assertIsNone(rows[2]["is_system"])

    def test_agent_mesh_membership_classifies_short_lived_agents_without_snapshots(
        self,
    ):
        for actor in self._actors:
            self._status(actor["id"], "Idle")
        agent = self._actors[0]
        for mesh_class in ("Host", "Proc"):
            with sqlite3.connect(self._path) as connection:
                connection.execute(
                    "UPDATE meshes SET class=? WHERE id=?",
                    (mesh_class, agent["mesh_id"]),
                )
                connection.execute(
                    "DELETE FROM actor_status_events WHERE actor_id=?", (agent["id"],)
                )
            self.assertTrue(db.list_actors()[0]["is_system"], mesh_class)
            self.assertEqual(db.get_summary()["health_status"], "healthy", mesh_class)
        # An explicit snapshot flag wins over membership metadata.
        self._snapshot_tables()
        self._snapshot_role("explicit", 1, agent, False)
        self.assertFalse(db.list_actors()[0]["is_system"])
        self.assertEqual(db.get_summary()["health_status"], "unknown")
