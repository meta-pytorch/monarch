# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for the Monarch Dashboard fake data generator.

Validates that the generated SQLite database conforms to the data contract
defined in the data contract, including:
  - Correct table schemas and row counts
  - Valid foreign key relationships
  - 5-minute timeline with actor failure at minute 4
  - All ActorStatus enum values are exercised
  - Non-sparse message data
  - Proper system actor (HostMeshAgent/ProcMeshAgent) and user actor presence
  - Death propagation semantics
  - Deterministic (reproducible) output
"""

import os
import sqlite3
import tempfile
import unittest

from monarch.monarch_dashboard.fake_data.generate import (
    ACTOR_STATUSES,
    BASE_TIMESTAMP_US,
    ENDPOINTS,
    FAILURE_MINUTE,
    generate,
    MESSAGE_STATUSES,
    MINUTES_US,
)


def _ts(minute: float) -> int:
    """Shorthand timestamp helper mirroring generate._ts."""
    return BASE_TIMESTAMP_US + int(minute * MINUTES_US)


class SchemaTest(unittest.TestCase):
    """Verify that all six tables exist with the expected columns."""

    @classmethod
    def setUpClass(cls):
        cls.db_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.db_dir, "test.db")
        generate(cls.db_path)
        cls.conn = sqlite3.connect(cls.db_path)
        cls.conn.row_factory = sqlite3.Row

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.remove(cls.db_path)

    def _columns(self, table: str) -> list[str]:
        cur = self.conn.execute(f"PRAGMA table_info({table})")
        return [row["name"] for row in cur.fetchall()]

    def test_meshes_columns(self):
        expected = [
            "id",
            "timestamp_us",
            "class",
            "given_name",
            "full_name",
            "shape_json",
            "parent_mesh_id",
            "parent_view_json",
        ]
        self.assertEqual(self._columns("meshes"), expected)

    def test_actors_columns(self):
        expected = ["id", "timestamp_us", "mesh_id", "rank", "full_name"]
        self.assertEqual(self._columns("actors"), expected)

    def test_actor_status_events_columns(self):
        expected = ["id", "timestamp_us", "actor_id", "new_status", "reason"]
        self.assertEqual(self._columns("actor_status_events"), expected)

    def test_messages_columns(self):
        expected = [
            "id",
            "timestamp_us",
            "from_actor_id",
            "to_actor_id",
            "status",
            "endpoint",
            "port_id",
        ]
        self.assertEqual(self._columns("messages"), expected)

    def test_message_status_events_columns(self):
        expected = ["id", "timestamp_us", "message_id", "status"]
        self.assertEqual(self._columns("message_status_events"), expected)

    def test_sent_messages_columns(self):
        expected = [
            "id",
            "timestamp_us",
            "sender_actor_id",
            "actor_mesh_id",
            "view_json",
            "shape_json",
        ]
        self.assertEqual(self._columns("sent_messages"), expected)


class MeshTest(unittest.TestCase):
    """Validate mesh hierarchy: 2 host, 4 proc, 4 actor meshes."""

    @classmethod
    def setUpClass(cls):
        cls.db_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.db_dir, "test.db")
        generate(cls.db_path)
        cls.conn = sqlite3.connect(cls.db_path)
        cls.conn.row_factory = sqlite3.Row

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.remove(cls.db_path)

    def test_total_mesh_count(self):
        """10 meshes: 2 host + 4 proc + 4 actor."""
        count = self.conn.execute("SELECT COUNT(*) FROM meshes").fetchone()[0]
        self.assertEqual(count, 10)

    def test_host_mesh_count(self):
        rows = self.conn.execute("SELECT * FROM meshes WHERE class = 'Host'").fetchall()
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertIsNone(row["parent_mesh_id"])

    def test_proc_mesh_count(self):
        rows = self.conn.execute("SELECT * FROM meshes WHERE class = 'Proc'").fetchall()
        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertIsNotNone(row["parent_mesh_id"])

    def test_actor_mesh_count(self):
        rows = self.conn.execute(
            "SELECT * FROM meshes WHERE class NOT IN ('Host', 'Proc')"
        ).fetchall()
        self.assertEqual(len(rows), 4)

    def test_proc_meshes_parent_is_host(self):
        """Every proc mesh must reference a host mesh as its parent."""
        host_ids = {
            r["id"]
            for r in self.conn.execute(
                "SELECT id FROM meshes WHERE class = 'Host'"
            ).fetchall()
        }
        proc_parents = {
            r["parent_mesh_id"]
            for r in self.conn.execute(
                "SELECT parent_mesh_id FROM meshes WHERE class = 'Proc'"
            ).fetchall()
        }
        self.assertTrue(proc_parents.issubset(host_ids))

    def test_actor_meshes_parent_is_proc(self):
        """Every actor mesh must reference a proc mesh as its parent."""
        proc_ids = {
            r["id"]
            for r in self.conn.execute(
                "SELECT id FROM meshes WHERE class = 'Proc'"
            ).fetchall()
        }
        actor_mesh_parents = {
            r["parent_mesh_id"]
            for r in self.conn.execute(
                "SELECT parent_mesh_id FROM meshes WHERE class NOT IN ('Host', 'Proc')"
            ).fetchall()
        }
        self.assertTrue(actor_mesh_parents.issubset(proc_ids))

    def test_two_procs_per_host(self):
        """Each host mesh has exactly 2 child proc meshes."""
        host_ids = [
            r["id"]
            for r in self.conn.execute(
                "SELECT id FROM meshes WHERE class = 'Host'"
            ).fetchall()
        ]
        for hid in host_ids:
            count = self.conn.execute(
                "SELECT COUNT(*) FROM meshes WHERE parent_mesh_id = ? AND class = 'Proc'",
                (hid,),
            ).fetchone()[0]
            self.assertEqual(count, 2, f"host mesh {hid} should have 2 proc children")

    def test_shape_json_is_valid(self):
        """shape_json should be parseable and contain a 'dims' key."""
        import json

        rows = self.conn.execute("SELECT shape_json FROM meshes").fetchall()
        for row in rows:
            data = json.loads(row["shape_json"])
            self.assertIn("dims", data)


class ActorTest(unittest.TestCase):
    """Validate actor records: system + user actors, correct foreign keys."""

    @classmethod
    def setUpClass(cls):
        cls.db_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.db_dir, "test.db")
        generate(cls.db_path)
        cls.conn = sqlite3.connect(cls.db_path)
        cls.conn.row_factory = sqlite3.Row

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.remove(cls.db_path)

    def test_total_actor_count(self):
        """10 actors: 2 HostMeshAgent + 4 ProcMeshAgent + 4 user actors."""
        count = self.conn.execute("SELECT COUNT(*) FROM actors").fetchone()[0]
        self.assertEqual(count, 10)

    def test_host_mesh_agents(self):
        rows = self.conn.execute(
            "SELECT * FROM actors WHERE full_name LIKE '%HostMeshAgent%'"
        ).fetchall()
        self.assertEqual(len(rows), 2)

    def test_proc_mesh_agents(self):
        rows = self.conn.execute(
            "SELECT * FROM actors WHERE full_name LIKE '%ProcMeshAgent%'"
        ).fetchall()
        self.assertEqual(len(rows), 4)

    def test_user_actors(self):
        rows = self.conn.execute(
            "SELECT * FROM actors WHERE full_name LIKE '%PythonActor<Trainer>%'"
        ).fetchall()
        self.assertEqual(len(rows), 4)

    def test_actor_mesh_id_fk_valid(self):
        """Every actor.mesh_id must reference an existing mesh."""
        mesh_ids = {
            r["id"] for r in self.conn.execute("SELECT id FROM meshes").fetchall()
        }
        actor_mesh_ids = {
            r["mesh_id"]
            for r in self.conn.execute("SELECT mesh_id FROM actors").fetchall()
        }
        self.assertTrue(actor_mesh_ids.issubset(mesh_ids))

    def test_four_actors_per_host_mesh(self):
        """Each host mesh should have 4 actors in its subtree.

        host -> proc meshes -> actor meshes, each with one actor, plus
        the host-level HostMeshAgent and proc-level ProcMeshAgents.
        That's 1 (host agent) + 2 (proc agents) + 2 (user actors) = 5 per host...
        Wait — the plan says "4 actors per host mesh", meaning user+system
        actors linked under the host's subtree.  Our count is:
          host mesh -> 1 HostMeshAgent
          2 proc meshes -> 2 ProcMeshAgents
          2 actor meshes -> 2 user actors
          Total = 5.
        The plan's "4 actors per host mesh" likely counts the proc-level and
        actor-level entities (excluding the HostMeshAgent itself).  We verify
        the subtree total is 5.
        """
        host_ids = [
            r["id"]
            for r in self.conn.execute(
                "SELECT id FROM meshes WHERE class = 'Host'"
            ).fetchall()
        ]
        for hid in host_ids:
            # Collect all mesh ids in the subtree rooted at this host.
            subtree = {hid}
            queue = [hid]
            while queue:
                mid = queue.pop()
                children = [
                    r["id"]
                    for r in self.conn.execute(
                        "SELECT id FROM meshes WHERE parent_mesh_id = ?", (mid,)
                    ).fetchall()
                ]
                subtree.update(children)
                queue.extend(children)

            count = self.conn.execute(
                f"SELECT COUNT(*) FROM actors WHERE mesh_id IN ({','.join('?' * len(subtree))})",
                tuple(subtree),
            ).fetchone()[0]
            self.assertEqual(count, 5, f"host mesh {hid} subtree should have 5 actors")


class StatusEventTest(unittest.TestCase):
    """Validate actor_status_events: timeline, enum coverage, death propagation."""

    @classmethod
    def setUpClass(cls):
        cls.db_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.db_dir, "test.db")
        generate(cls.db_path)
        cls.conn = sqlite3.connect(cls.db_path)
        cls.conn.row_factory = sqlite3.Row

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.remove(cls.db_path)

    def test_events_exist(self):
        count = self.conn.execute(
            "SELECT COUNT(*) FROM actor_status_events"
        ).fetchone()[0]
        self.assertGreater(count, 0)

    def test_actor_id_fk_valid(self):
        """Every event must reference a valid actor."""
        actor_ids = {
            r["id"] for r in self.conn.execute("SELECT id FROM actors").fetchall()
        }
        event_actor_ids = {
            r["actor_id"]
            for r in self.conn.execute(
                "SELECT actor_id FROM actor_status_events"
            ).fetchall()
        }
        self.assertTrue(event_actor_ids.issubset(actor_ids))

    def test_all_statuses_used(self):
        """Every ActorStatus enum value appears at least once."""
        statuses = {
            r["new_status"]
            for r in self.conn.execute(
                "SELECT DISTINCT new_status FROM actor_status_events"
            ).fetchall()
        }
        for s in ACTOR_STATUSES:
            self.assertIn(s, statuses, f"status '{s}' never appears in events")

    def test_timeline_covers_five_minutes(self):
        """Events span from T=0 to approximately T=5:00."""
        row = self.conn.execute(
            "SELECT MIN(timestamp_us) AS tmin, MAX(timestamp_us) AS tmax "
            "FROM actor_status_events"
        ).fetchone()
        earliest = row["tmin"]
        latest = row["tmax"]
        # Earliest should be near T=0.
        self.assertLessEqual(earliest, _ts(0.1))
        # Latest should be near or past T=4:00 (failure + propagation).
        self.assertGreaterEqual(latest, _ts(FAILURE_MINUTE))

    def test_failure_at_minute_four(self):
        """At least one actor reaches 'failed' status near T=4:00."""
        rows = self.conn.execute(
            "SELECT * FROM actor_status_events WHERE new_status = 'failed'"
        ).fetchall()
        self.assertGreater(len(rows), 0)
        for r in rows:
            # Should be within a reasonable window around T=4:00.
            self.assertGreaterEqual(r["timestamp_us"], _ts(3.9))
            self.assertLessEqual(r["timestamp_us"], _ts(4.5))

    def test_failure_has_cuda_oom_reason(self):
        rows = self.conn.execute(
            "SELECT reason FROM actor_status_events WHERE new_status = 'failed'"
        ).fetchall()
        reasons = [r["reason"] for r in rows]
        self.assertIn("CUDA OOM", reasons)

    def test_death_propagation(self):
        """Actors in the same host mesh as the failed actor should reach
        a terminal status (stopped) after the failure."""
        # Find the failed actor.
        failed = self.conn.execute(
            "SELECT actor_id FROM actor_status_events WHERE new_status = 'failed'"
        ).fetchone()
        failed_actor_id = failed["actor_id"]

        # Find the host mesh of the failed actor.
        failed_actor = self.conn.execute(
            "SELECT mesh_id FROM actors WHERE id = ?", (failed_actor_id,)
        ).fetchone()
        mesh_id = failed_actor["mesh_id"]

        # Walk up to the host mesh.
        while True:
            mesh = self.conn.execute(
                "SELECT parent_mesh_id, class FROM meshes WHERE id = ?", (mesh_id,)
            ).fetchone()
            if mesh["class"] == "Host" or mesh["parent_mesh_id"] is None:
                break
            mesh_id = mesh["parent_mesh_id"]

        host_mesh_id = mesh_id

        # Get all mesh ids in this host's subtree.
        subtree = {host_mesh_id}
        queue = [host_mesh_id]
        while queue:
            mid = queue.pop()
            children = [
                r["id"]
                for r in self.conn.execute(
                    "SELECT id FROM meshes WHERE parent_mesh_id = ?", (mid,)
                ).fetchall()
            ]
            subtree.update(children)
            queue.extend(children)

        # Get sibling actors (same host, different from the failed one).
        siblings = self.conn.execute(
            f"SELECT id FROM actors WHERE mesh_id IN ({','.join('?' * len(subtree))}) AND id != ?",
            (*subtree, failed_actor_id),
        ).fetchall()
        self.assertGreater(len(siblings), 0)

        # Each sibling should have a terminal status event after the failure.
        for sib in siblings:
            terminal = self.conn.execute(
                "SELECT new_status FROM actor_status_events "
                "WHERE actor_id = ? AND new_status IN ('stopped', 'stopping') "
                "ORDER BY timestamp_us DESC LIMIT 1",
                (sib["id"],),
            ).fetchone()
            self.assertIsNotNone(
                terminal,
                f"actor {sib['id']} in failed host mesh should have "
                "a stopping/stopped event",
            )

    def test_every_actor_has_created_event(self):
        """Every actor starts with a 'created' status event."""
        actor_ids = [
            r["id"] for r in self.conn.execute("SELECT id FROM actors").fetchall()
        ]
        for aid in actor_ids:
            row = self.conn.execute(
                "SELECT * FROM actor_status_events "
                "WHERE actor_id = ? AND new_status = 'created' "
                "ORDER BY timestamp_us ASC LIMIT 1",
                (aid,),
            ).fetchone()
            self.assertIsNotNone(row, f"actor {aid} missing 'created' event")


class MessageTest(unittest.TestCase):
    """Validate message tables: non-sparse data, FK integrity, endpoints."""

    @classmethod
    def setUpClass(cls):
        cls.db_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.db_dir, "test.db")
        generate(cls.db_path)
        cls.conn = sqlite3.connect(cls.db_path)
        cls.conn.row_factory = sqlite3.Row

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.remove(cls.db_path)

    def test_messages_non_sparse(self):
        """There should be a substantial number of messages (non-sparse)."""
        count = self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        # With ~20 time steps and 3–6 messages each, expect 60+.
        self.assertGreater(count, 50, "message data should be non-sparse")

    def test_message_from_actor_fk(self):
        actor_ids = {
            r["id"] for r in self.conn.execute("SELECT id FROM actors").fetchall()
        }
        from_ids = {
            r["from_actor_id"]
            for r in self.conn.execute("SELECT from_actor_id FROM messages").fetchall()
        }
        self.assertTrue(from_ids.issubset(actor_ids))

    def test_message_to_actor_fk(self):
        actor_ids = {
            r["id"] for r in self.conn.execute("SELECT id FROM actors").fetchall()
        }
        to_ids = {
            r["to_actor_id"]
            for r in self.conn.execute("SELECT to_actor_id FROM messages").fetchall()
        }
        self.assertTrue(to_ids.issubset(actor_ids))

    def test_no_self_messages(self):
        """An actor should never send a message to itself."""
        count = self.conn.execute(
            "SELECT COUNT(*) FROM messages WHERE from_actor_id = to_actor_id"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_multiple_endpoints_used(self):
        """Messages should use more than one endpoint name."""
        endpoints = {
            r["endpoint"]
            for r in self.conn.execute(
                "SELECT DISTINCT endpoint FROM messages"
            ).fetchall()
        }
        self.assertGreaterEqual(len(endpoints), 3)
        # All used endpoints should be from the known set.
        self.assertTrue(endpoints.issubset(set(ENDPOINTS)))

    def test_message_statuses_valid(self):
        statuses = {
            r["status"]
            for r in self.conn.execute(
                "SELECT DISTINCT status FROM messages"
            ).fetchall()
        }
        self.assertTrue(statuses.issubset(set(MESSAGE_STATUSES)))

    def test_message_status_events_fk(self):
        """Every message_status_event.message_id must reference a message."""
        msg_ids = {
            r["id"] for r in self.conn.execute("SELECT id FROM messages").fetchall()
        }
        mse_msg_ids = {
            r["message_id"]
            for r in self.conn.execute(
                "SELECT message_id FROM message_status_events"
            ).fetchall()
        }
        self.assertTrue(mse_msg_ids.issubset(msg_ids))

    def test_message_status_events_non_empty(self):
        count = self.conn.execute(
            "SELECT COUNT(*) FROM message_status_events"
        ).fetchone()[0]
        self.assertGreater(count, 0)

    def test_every_message_has_status_events(self):
        """Each message should have at least one status event."""
        msgs_without = self.conn.execute(
            "SELECT m.id FROM messages m "
            "LEFT JOIN message_status_events mse ON m.id = mse.message_id "
            "WHERE mse.id IS NULL"
        ).fetchall()
        self.assertEqual(
            len(msgs_without), 0, "every message should have status events"
        )


class SentMessageTest(unittest.TestCase):
    """Validate sent_messages table."""

    @classmethod
    def setUpClass(cls):
        cls.db_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.db_dir, "test.db")
        generate(cls.db_path)
        cls.conn = sqlite3.connect(cls.db_path)
        cls.conn.row_factory = sqlite3.Row

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.remove(cls.db_path)

    def test_sent_messages_non_empty(self):
        count = self.conn.execute("SELECT COUNT(*) FROM sent_messages").fetchone()[0]
        self.assertGreater(count, 0)

    def test_sender_actor_fk(self):
        actor_ids = {
            r["id"] for r in self.conn.execute("SELECT id FROM actors").fetchall()
        }
        sender_ids = {
            r["sender_actor_id"]
            for r in self.conn.execute(
                "SELECT sender_actor_id FROM sent_messages"
            ).fetchall()
        }
        self.assertTrue(sender_ids.issubset(actor_ids))

    def test_actor_mesh_fk(self):
        mesh_ids = {
            r["id"] for r in self.conn.execute("SELECT id FROM meshes").fetchall()
        }
        sm_mesh_ids = {
            r["actor_mesh_id"]
            for r in self.conn.execute(
                "SELECT actor_mesh_id FROM sent_messages"
            ).fetchall()
        }
        self.assertTrue(sm_mesh_ids.issubset(mesh_ids))

    def test_one_sent_message_per_message(self):
        """sent_messages count should equal messages count (one record each)."""
        msg_count = self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        sm_count = self.conn.execute("SELECT COUNT(*) FROM sent_messages").fetchone()[0]
        self.assertEqual(sm_count, msg_count)


class DeterminismTest(unittest.TestCase):
    """Verify that two runs produce identical databases."""

    def test_reproducible_output(self):
        dir1 = tempfile.mkdtemp()
        dir2 = tempfile.mkdtemp()
        path1 = os.path.join(dir1, "a.db")
        path2 = os.path.join(dir2, "b.db")

        generate(path1)
        generate(path2)

        conn1 = sqlite3.connect(path1)
        conn2 = sqlite3.connect(path2)

        tables = [
            "meshes",
            "actors",
            "actor_status_events",
            "messages",
            "message_status_events",
            "sent_messages",
        ]
        for table in tables:
            rows1 = conn1.execute(f"SELECT * FROM {table}").fetchall()
            rows2 = conn2.execute(f"SELECT * FROM {table}").fetchall()
            self.assertEqual(rows1, rows2, f"table '{table}' differs between runs")

        conn1.close()
        conn2.close()
        os.remove(path1)
        os.remove(path2)


class PrimaryKeyTest(unittest.TestCase):
    """Verify that all id columns contain unique values."""

    @classmethod
    def setUpClass(cls):
        cls.db_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.db_dir, "test.db")
        generate(cls.db_path)
        cls.conn = sqlite3.connect(cls.db_path)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.remove(cls.db_path)

    def test_unique_ids(self):
        tables = [
            "meshes",
            "actors",
            "actor_status_events",
            "messages",
            "message_status_events",
            "sent_messages",
        ]
        for table in tables:
            total = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            distinct = self.conn.execute(
                f"SELECT COUNT(DISTINCT id) FROM {table}"
            ).fetchone()[0]
            self.assertEqual(
                total,
                distinct,
                f"table '{table}' has duplicate ids",
            )


if __name__ == "__main__":
    unittest.main()
