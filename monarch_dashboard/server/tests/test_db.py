# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for the SQL query layer (server.db).

Each test class generates a fresh SQLite database, initialises db.init(),
and validates that the query functions return correct results.
"""

import os
import tempfile
import unittest

from monarch.monarch_dashboard.fake_data.generate import generate
from monarch.monarch_dashboard.server import db


class _DbTestBase(unittest.TestCase):
    """Shared setup: generate a temp database and point the db module at it."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        cls._db_path = os.path.join(cls._tmpdir, "test.db")
        generate(cls._db_path)
        db.init(cls._db_path)

    @classmethod
    def tearDownClass(cls):
        os.remove(cls._db_path)


# ---------------------------------------------------------------------------
# Mesh queries
# ---------------------------------------------------------------------------


class ListMeshesTest(_DbTestBase):
    def test_returns_all_meshes(self):
        meshes = db.list_meshes()
        self.assertEqual(len(meshes), 10)

    def test_filter_by_class_host(self):
        meshes = db.list_meshes(mesh_class="Host")
        self.assertEqual(len(meshes), 2)
        for m in meshes:
            self.assertEqual(m["class"], "Host")

    def test_filter_by_class_proc(self):
        meshes = db.list_meshes(mesh_class="Proc")
        self.assertEqual(len(meshes), 4)

    def test_filter_by_nonexistent_class(self):
        meshes = db.list_meshes(mesh_class="DoesNotExist")
        self.assertEqual(len(meshes), 0)

    def test_rows_are_dicts(self):
        meshes = db.list_meshes()
        self.assertIsInstance(meshes[0], dict)
        self.assertIn("id", meshes[0])
        self.assertIn("class", meshes[0])

    def test_ordered_by_id(self):
        meshes = db.list_meshes()
        ids = [m["id"] for m in meshes]
        self.assertEqual(ids, sorted(ids))


class GetMeshTest(_DbTestBase):
    def test_existing_mesh(self):
        mesh = db.get_mesh(1)
        self.assertIsNotNone(mesh)
        self.assertEqual(mesh["id"], 1)
        self.assertEqual(mesh["class"], "Host")

    def test_nonexistent_mesh(self):
        mesh = db.get_mesh(9999)
        self.assertIsNone(mesh)


class GetMeshChildrenTest(_DbTestBase):
    def test_host_has_proc_children(self):
        children = db.get_mesh_children(1)
        self.assertEqual(len(children), 2)
        for c in children:
            self.assertEqual(c["class"], "Proc")
            self.assertEqual(c["parent_mesh_id"], 1)

    def test_proc_has_actor_mesh_children(self):
        children = db.get_mesh_children(3)
        self.assertEqual(len(children), 1)
        self.assertNotEqual(children[0]["class"], "Proc")

    def test_leaf_has_no_children(self):
        children = db.get_mesh_children(7)
        self.assertEqual(len(children), 0)


# ---------------------------------------------------------------------------
# Actor queries
# ---------------------------------------------------------------------------


class ListActorsTest(_DbTestBase):
    def test_returns_all_actors(self):
        actors = db.list_actors()
        self.assertEqual(len(actors), 10)

    def test_filter_by_mesh_id(self):
        actors = db.list_actors(mesh_id=1)
        self.assertGreater(len(actors), 0)
        for a in actors:
            self.assertEqual(a["mesh_id"], 1)

    def test_filter_by_nonexistent_mesh(self):
        actors = db.list_actors(mesh_id=9999)
        self.assertEqual(len(actors), 0)


class GetActorTest(_DbTestBase):
    def test_existing_actor(self):
        actor = db.get_actor(1)
        self.assertIsNotNone(actor)
        self.assertEqual(actor["id"], 1)
        expected_keys = {
            "id",
            "timestamp_us",
            "mesh_id",
            "rank",
            "full_name",
        }
        self.assertTrue(expected_keys.issubset(set(actor.keys())))

    def test_no_status_fields(self):
        actor = db.get_actor(1)
        self.assertNotIn("latest_status", actor)
        self.assertNotIn("status_timestamp_us", actor)

    def test_nonexistent_actor(self):
        actor = db.get_actor(9999)
        self.assertIsNone(actor)


class GetActorLatestStatusTest(_DbTestBase):
    def test_returns_status_fields(self):
        status = db.get_actor_latest_status(1)
        self.assertIsNotNone(status)
        self.assertIn("latest_status", status)
        self.assertIn("status_timestamp_us", status)

    def test_status_value_populated(self):
        status = db.get_actor_latest_status(1)
        self.assertIsNotNone(status["latest_status"])
        self.assertIsNotNone(status["status_timestamp_us"])

    def test_nonexistent_actor(self):
        status = db.get_actor_latest_status(9999)
        self.assertIsNone(status)


# ---------------------------------------------------------------------------
# Status event queries
# ---------------------------------------------------------------------------


class ListActorStatusEventsTest(_DbTestBase):
    def test_all_events(self):
        events = db.list_actor_status_events()
        self.assertGreater(len(events), 0)

    def test_filter_by_actor_id(self):
        events = db.list_actor_status_events(actor_id=1)
        self.assertGreater(len(events), 0)
        for e in events:
            self.assertEqual(e["actor_id"], 1)

    def test_ordered_by_timestamp(self):
        events = db.list_actor_status_events(actor_id=1)
        timestamps = [e["timestamp_us"] for e in events]
        self.assertEqual(timestamps, sorted(timestamps))


# ---------------------------------------------------------------------------
# Message queries
# ---------------------------------------------------------------------------


class ListMessagesTest(_DbTestBase):
    def test_all_messages(self):
        msgs = db.list_messages()
        self.assertGreater(len(msgs), 50)

    def test_filter_by_from_actor_id(self):
        msgs = db.list_messages(from_actor_id=1)
        for m in msgs:
            self.assertEqual(m["from_actor_id"], 1)

    def test_filter_by_to_actor_id(self):
        msgs = db.list_messages(to_actor_id=2)
        for m in msgs:
            self.assertEqual(m["to_actor_id"], 2)

    def test_filter_by_both(self):
        all_msgs = db.list_messages()
        # Pick a real sender/receiver pair from the data.
        pair = next((m["from_actor_id"], m["to_actor_id"]) for m in all_msgs)
        filtered = db.list_messages(from_actor_id=pair[0], to_actor_id=pair[1])
        for m in filtered:
            self.assertEqual(m["from_actor_id"], pair[0])
            self.assertEqual(m["to_actor_id"], pair[1])


class GetActorMessagesTest(_DbTestBase):
    def test_returns_sent_and_received(self):
        # Pick an actor that appears as both sender and receiver.
        all_msgs = db.list_messages()
        senders = {m["from_actor_id"] for m in all_msgs}
        receivers = {m["to_actor_id"] for m in all_msgs}
        both = senders & receivers
        self.assertGreater(len(both), 0)
        actor_id = next(iter(both))

        msgs = db.get_actor_messages(actor_id)
        self.assertGreater(len(msgs), 0)
        for m in msgs:
            self.assertTrue(
                m["from_actor_id"] == actor_id or m["to_actor_id"] == actor_id
            )


# ---------------------------------------------------------------------------
# Message status event queries
# ---------------------------------------------------------------------------


class ListMessageStatusEventsTest(_DbTestBase):
    def test_all_events(self):
        events = db.list_message_status_events()
        self.assertGreater(len(events), 0)

    def test_filter_by_message_id(self):
        first_msg = db.list_messages()[0]
        events = db.list_message_status_events(message_id=first_msg["id"])
        self.assertGreater(len(events), 0)
        for e in events:
            self.assertEqual(e["message_id"], first_msg["id"])


# ---------------------------------------------------------------------------
# Sent message queries
# ---------------------------------------------------------------------------


class ListSentMessagesTest(_DbTestBase):
    def test_all_sent_messages(self):
        sm = db.list_sent_messages()
        self.assertGreater(len(sm), 0)

    def test_filter_by_sender(self):
        all_sm = db.list_sent_messages()
        sender = all_sm[0]["sender_actor_id"]
        filtered = db.list_sent_messages(sender_actor_id=sender)
        self.assertGreater(len(filtered), 0)
        for s in filtered:
            self.assertEqual(s["sender_actor_id"], sender)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class InitErrorTest(unittest.TestCase):
    def test_query_before_init_raises(self):
        """Calling a query before db.init() should raise RuntimeError."""
        import monarch.monarch_dashboard.server.db as fresh_db

        saved = fresh_db._db_path
        try:
            fresh_db._db_path = None
            with self.assertRaises(RuntimeError):
                fresh_db.list_meshes()
        finally:
            fresh_db._db_path = saved


# ---------------------------------------------------------------------------
# Summary / aggregate queries
# ---------------------------------------------------------------------------


class GetSummaryTest(_DbTestBase):
    def test_returns_dict(self):
        summary = db.get_summary()
        self.assertIsInstance(summary, dict)

    def test_top_level_keys(self):
        summary = db.get_summary()
        for key in (
            "mesh_counts",
            "actor_counts",
            "message_counts",
            "errors",
            "timeline",
            "health_score",
        ):
            self.assertIn(key, summary)

    def test_mesh_counts(self):
        summary = db.get_summary()
        mc = summary["mesh_counts"]
        self.assertEqual(mc["total"], 10)
        self.assertIn("by_class", mc)
        self.assertEqual(mc["by_class"]["Host"], 2)
        self.assertEqual(mc["by_class"]["Proc"], 4)

    def test_actor_counts(self):
        summary = db.get_summary()
        ac = summary["actor_counts"]
        self.assertEqual(ac["total"], 10)
        self.assertIn("by_status", ac)
        # All 10 actors should be represented across statuses.
        total_by_status = sum(ac["by_status"].values())
        self.assertEqual(total_by_status, 10)

    def test_actor_status_includes_failed(self):
        summary = db.get_summary()
        by_status = summary["actor_counts"]["by_status"]
        self.assertIn("failed", by_status)
        self.assertGreaterEqual(by_status["failed"], 1)

    def test_message_counts(self):
        summary = db.get_summary()
        mc = summary["message_counts"]
        self.assertGreater(mc["total"], 50)
        self.assertIn("by_status", mc)
        self.assertIn("by_endpoint", mc)
        self.assertIn("delivery_rate", mc)
        self.assertGreater(mc["delivery_rate"], 0)
        self.assertLessEqual(mc["delivery_rate"], 1.0)

    def test_message_by_endpoint_has_entries(self):
        summary = db.get_summary()
        by_ep = summary["message_counts"]["by_endpoint"]
        self.assertGreater(len(by_ep), 0)
        # All endpoint counts should sum to total.
        self.assertEqual(sum(by_ep.values()), summary["message_counts"]["total"])

    def test_errors_failed_actors(self):
        summary = db.get_summary()
        fa = summary["errors"]["failed_actors"]
        self.assertIsInstance(fa, list)
        self.assertGreaterEqual(len(fa), 1)
        for a in fa:
            self.assertIn("actor_id", a)
            self.assertIn("full_name", a)
            self.assertIn("reason", a)
            self.assertIn("timestamp_us", a)

    def test_errors_stopped_actors(self):
        summary = db.get_summary()
        sa = summary["errors"]["stopped_actors"]
        self.assertIsInstance(sa, list)
        self.assertGreaterEqual(len(sa), 1)

    def test_errors_failed_messages_count(self):
        summary = db.get_summary()
        self.assertIsInstance(summary["errors"]["failed_messages"], int)

    def test_timeline(self):
        summary = db.get_summary()
        tl = summary["timeline"]
        self.assertIn("start_us", tl)
        self.assertIn("end_us", tl)
        self.assertIn("failure_onset_us", tl)
        self.assertGreater(tl["start_us"], 0)
        self.assertGreater(tl["end_us"], tl["start_us"])
        self.assertIsNotNone(tl["failure_onset_us"])
        self.assertGreater(tl["total_status_events"], 0)
        self.assertGreater(tl["total_message_events"], 0)

    def test_health_score_range(self):
        summary = db.get_summary()
        score = summary["health_score"]
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)

    def test_health_score_reflects_failures(self):
        # With failed/stopped actors, health should be below 100.
        summary = db.get_summary()
        self.assertLess(summary["health_score"], 100)


if __name__ == "__main__":
    unittest.main()
