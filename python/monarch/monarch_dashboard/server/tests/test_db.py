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
        self.assertGreater(len(meshes), 0)

    def test_rows_are_dicts(self):
        meshes = db.list_meshes()
        self.assertIsInstance(meshes[0], dict)
        self.assertIn("id", meshes[0])
        self.assertIn("class", meshes[0])
        self.assertIn("given_name", meshes[0])

    def test_ordered_by_id(self):
        meshes = db.list_meshes()
        ids = [m["id"] for m in meshes]
        self.assertEqual(ids, sorted(ids))

    def test_filter_by_class(self):
        hosts = db.list_meshes(class_filter="Host")
        self.assertEqual(len(hosts), 2)
        for m in hosts:
            self.assertEqual(m["class"], "Host")

    def test_filter_by_class_proc(self):
        procs = db.list_meshes(class_filter="Proc")
        self.assertGreaterEqual(len(procs), 4)
        for m in procs:
            self.assertEqual(m["class"], "Proc")

    def test_filter_by_parent_mesh_id(self):
        # Get first host mesh id
        hosts = db.list_meshes(class_filter="Host")
        host_id = hosts[0]["id"]
        children = db.list_meshes(parent_mesh_id=host_id)
        self.assertGreater(len(children), 0)
        for m in children:
            self.assertEqual(m["parent_mesh_id"], host_id)

    def test_filter_nonexistent_class(self):
        meshes = db.list_meshes(class_filter="Nonexistent")
        self.assertEqual(len(meshes), 0)


class GetMeshTest(_DbTestBase):
    def test_existing_mesh(self):
        mesh = db.get_mesh(1)
        self.assertIsNotNone(mesh)
        self.assertEqual(mesh["id"], 1)
        self.assertIn("class", mesh)
        self.assertIn("full_name", mesh)

    def test_nonexistent_mesh(self):
        mesh = db.get_mesh(9999)
        self.assertIsNone(mesh)


class GetMeshChildrenTest(_DbTestBase):
    def test_host_mesh_has_children(self):
        hosts = db.list_meshes(class_filter="Host")
        host_id = hosts[0]["id"]
        children = db.get_mesh_children(host_id)
        self.assertGreater(len(children), 0)
        for c in children:
            self.assertEqual(c["parent_mesh_id"], host_id)

    def test_nonexistent_mesh_returns_empty(self):
        children = db.get_mesh_children(9999)
        self.assertEqual(len(children), 0)


# ---------------------------------------------------------------------------
# Actor queries
# ---------------------------------------------------------------------------


class ListActorsTest(_DbTestBase):
    def test_returns_all_actors(self):
        actors = db.list_actors()
        self.assertGreater(len(actors), 0)

    def test_filter_by_mesh_id(self):
        # Get an actor mesh
        meshes = db.list_meshes()
        actor_mesh = next(
            m for m in meshes if m["class"] != "Host" and m["class"] != "Proc"
        )
        actors = db.list_actors(mesh_id=actor_mesh["id"])
        self.assertGreater(len(actors), 0)
        for a in actors:
            self.assertEqual(a["mesh_id"], actor_mesh["id"])

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

    def test_includes_latest_status(self):
        """get_actor_messages should JOIN latest message status."""
        all_msgs = db.list_messages()
        actor_id = all_msgs[0]["from_actor_id"]
        msgs = db.get_actor_messages(actor_id)
        self.assertGreater(len(msgs), 0)
        for m in msgs:
            self.assertIn("latest_status", m)
            # Every message in fake data has status events, so status should be set.
            self.assertIsNotNone(m["latest_status"])


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

        saved = fresh_db._adapter
        try:
            fresh_db._adapter = None
            with self.assertRaises(RuntimeError):
                fresh_db.list_meshes()
        finally:
            fresh_db._adapter = saved


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
            "hierarchy_counts",
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
        self.assertGreater(mc["total"], 0)

    def test_hierarchy_counts(self):
        summary = db.get_summary()
        hc = summary["hierarchy_counts"]
        self.assertEqual(hc["host_meshes"], 2)
        self.assertGreaterEqual(hc["proc_meshes"], 4)
        self.assertGreater(hc["actor_meshes"], 0)

    def test_actor_counts(self):
        summary = db.get_summary()
        ac = summary["actor_counts"]
        self.assertGreater(ac["total"], 0)
        self.assertIn("by_status", ac)
        # All actors should be represented across statuses.
        total_by_status = sum(ac["by_status"].values())
        self.assertEqual(total_by_status, ac["total"])

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


class RegionMappingTest(unittest.TestCase):
    """Tests for ndslice Region → proc rank mapping helpers."""

    def test_parse_region_valid(self):
        region_json = '{"labels": ["workers"], "slice": {"offset": 2, "sizes": [3], "strides": [1]}}'
        result = db._parse_region(region_json)
        self.assertEqual(result, (2, [3], [1]))

    def test_parse_region_none(self):
        self.assertIsNone(db._parse_region(None))
        self.assertIsNone(db._parse_region(""))

    def test_parse_region_invalid_json(self):
        self.assertIsNone(db._parse_region("not json"))

    def test_parse_region_missing_slice(self):
        self.assertIsNone(db._parse_region('{"labels": ["x"]}'))

    def test_1d_contiguous(self):
        # offset=2, sizes=[3], strides=[1] → ranks 2, 3, 4
        self.assertEqual(db._actor_rank_to_proc_rank(0, 2, [3], [1]), 2)
        self.assertEqual(db._actor_rank_to_proc_rank(1, 2, [3], [1]), 3)
        self.assertEqual(db._actor_rank_to_proc_rank(2, 2, [3], [1]), 4)

    def test_1d_strided(self):
        # offset=1, sizes=[2], strides=[2] → ranks 1, 3
        self.assertEqual(db._actor_rank_to_proc_rank(0, 1, [2], [2]), 1)
        self.assertEqual(db._actor_rank_to_proc_rank(1, 1, [2], [2]), 3)

    def test_2d(self):
        # 2D: offset=0, sizes=[2, 3], strides=[3, 1]
        # Row-major: (0,0)=0, (0,1)=1, (0,2)=2, (1,0)=3, (1,1)=4, (1,2)=5
        self.assertEqual(db._actor_rank_to_proc_rank(0, 0, [2, 3], [3, 1]), 0)
        self.assertEqual(db._actor_rank_to_proc_rank(1, 0, [2, 3], [3, 1]), 1)
        self.assertEqual(db._actor_rank_to_proc_rank(2, 0, [2, 3], [3, 1]), 2)
        self.assertEqual(db._actor_rank_to_proc_rank(3, 0, [2, 3], [3, 1]), 3)
        self.assertEqual(db._actor_rank_to_proc_rank(5, 0, [2, 3], [3, 1]), 5)

    def test_2d_with_offset(self):
        # offset=6, sizes=[2, 3], strides=[3, 1] → 6,7,8,9,10,11
        self.assertEqual(db._actor_rank_to_proc_rank(0, 6, [2, 3], [3, 1]), 6)
        self.assertEqual(db._actor_rank_to_proc_rank(5, 6, [2, 3], [3, 1]), 11)

    def test_proc_ranks_for_region(self):
        ranks = db._proc_ranks_for_region(2, [3], [1])
        self.assertEqual(ranks, {2, 3, 4})

    def test_proc_ranks_for_region_2d(self):
        ranks = db._proc_ranks_for_region(0, [2, 3], [3, 1])
        self.assertEqual(ranks, {0, 1, 2, 3, 4, 5})


class DagRegionEdgesTest(_DbTestBase):
    """Test that get_dag_data uses parent_view_json for proc→actor_mesh edges."""

    def test_dag_has_proc_unit_to_actor_mesh_edges(self):
        dag = db.get_dag_data()
        hier_edges = [
            e
            for e in dag["edges"]
            if e["type"] == "hierarchy"
            and e["source_id"].startswith("proc_unit-")
            and e["target_id"].startswith("actor_mesh-")
        ]
        # Fake data has 4 proc meshes × 1 actor mesh each = 4 edges
        self.assertGreaterEqual(len(hier_edges), 4)


if __name__ == "__main__":
    unittest.main()
