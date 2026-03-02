# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for the Flask API route handlers.

Uses Flask's test client to exercise every endpoint, verifying status codes,
JSON structure, filtering, and 404 behaviour.
"""

import os
import tempfile
import unittest

from monarch.monarch_dashboard.fake_data.generate import generate
from monarch.monarch_dashboard.server.app import create_app


class _RouteTestBase(unittest.TestCase):
    """Shared setup: create app with a temp database and a test client."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        cls._db_path = os.path.join(cls._tmpdir, "test.db")
        generate(cls._db_path)
        cls.app = create_app(cls._db_path)
        cls.app.config["TESTING"] = True
        cls.client = cls.app.test_client()

    @classmethod
    def tearDownClass(cls):
        os.remove(cls._db_path)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthTest(_RouteTestBase):
    def test_health_returns_ok(self):
        resp = self.client.get("/api/health")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["status"], "ok")


# ---------------------------------------------------------------------------
# Meshes
# ---------------------------------------------------------------------------


class MeshRoutesTest(_RouteTestBase):
    def test_list_meshes(self):
        resp = self.client.get("/api/meshes")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(len(data), 10)

    def test_list_meshes_filter_class(self):
        resp = self.client.get("/api/meshes?class=Host")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(len(data), 2)
        for m in data:
            self.assertEqual(m["class"], "Host")

    def test_list_meshes_filter_proc(self):
        resp = self.client.get("/api/meshes?class=Proc")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.get_json()), 4)

    def test_get_mesh(self):
        resp = self.client.get("/api/meshes/1")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["id"], 1)
        self.assertEqual(data["class"], "Host")

    def test_get_mesh_not_found(self):
        resp = self.client.get("/api/meshes/9999")
        self.assertEqual(resp.status_code, 404)

    def test_get_mesh_children(self):
        resp = self.client.get("/api/meshes/1/children")
        self.assertEqual(resp.status_code, 200)
        children = resp.get_json()
        self.assertEqual(len(children), 2)
        for c in children:
            self.assertEqual(c["parent_mesh_id"], 1)

    def test_get_mesh_children_not_found(self):
        resp = self.client.get("/api/meshes/9999/children")
        self.assertEqual(resp.status_code, 404)

    def test_get_mesh_children_leaf(self):
        resp = self.client.get("/api/meshes/7/children")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.get_json()), 0)

    def test_mesh_json_keys(self):
        resp = self.client.get("/api/meshes/1")
        data = resp.get_json()
        expected_keys = {
            "id",
            "timestamp_us",
            "class",
            "given_name",
            "full_name",
            "shape_json",
            "parent_mesh_id",
            "parent_view_json",
        }
        self.assertEqual(set(data.keys()), expected_keys)


# ---------------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------------


class ActorRoutesTest(_RouteTestBase):
    def test_list_actors(self):
        resp = self.client.get("/api/actors")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.get_json()), 10)

    def test_list_actors_filter_mesh_id(self):
        resp = self.client.get("/api/actors?mesh_id=1")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertGreater(len(data), 0)
        for a in data:
            self.assertEqual(a["mesh_id"], 1)

    def test_get_actor(self):
        resp = self.client.get("/api/actors/1")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["id"], 1)
        self.assertIn("latest_status", data)
        self.assertIn("status_timestamp_us", data)

    def test_get_actor_not_found(self):
        resp = self.client.get("/api/actors/9999")
        self.assertEqual(resp.status_code, 404)

    def test_actor_json_keys(self):
        resp = self.client.get("/api/actors/1")
        data = resp.get_json()
        expected_keys = {
            "id",
            "timestamp_us",
            "mesh_id",
            "rank",
            "full_name",
            "latest_status",
            "status_timestamp_us",
        }
        self.assertEqual(set(data.keys()), expected_keys)


# ---------------------------------------------------------------------------
# Actor status events
# ---------------------------------------------------------------------------


class ActorStatusEventsRoutesTest(_RouteTestBase):
    def test_get_actor_status_events(self):
        resp = self.client.get("/api/actors/1/status_events")
        self.assertEqual(resp.status_code, 200)
        events = resp.get_json()
        self.assertGreater(len(events), 0)
        for e in events:
            self.assertEqual(e["actor_id"], 1)

    def test_status_events_not_found_actor(self):
        resp = self.client.get("/api/actors/9999/status_events")
        self.assertEqual(resp.status_code, 404)

    def test_status_events_ordered(self):
        resp = self.client.get("/api/actors/1/status_events")
        events = resp.get_json()
        timestamps = [e["timestamp_us"] for e in events]
        self.assertEqual(timestamps, sorted(timestamps))


# ---------------------------------------------------------------------------
# Actor messages
# ---------------------------------------------------------------------------


class ActorMessagesRoutesTest(_RouteTestBase):
    def test_get_actor_messages(self):
        # Pick an actor that participates in messaging.
        all_msgs = self.client.get("/api/messages").get_json()
        actor_id = all_msgs[0]["from_actor_id"]
        resp = self.client.get(f"/api/actors/{actor_id}/messages")
        self.assertEqual(resp.status_code, 200)
        msgs = resp.get_json()
        self.assertGreater(len(msgs), 0)
        for m in msgs:
            self.assertTrue(
                m["from_actor_id"] == actor_id or m["to_actor_id"] == actor_id
            )

    def test_actor_messages_not_found(self):
        resp = self.client.get("/api/actors/9999/messages")
        self.assertEqual(resp.status_code, 404)


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class MessageRoutesTest(_RouteTestBase):
    def test_list_all_messages(self):
        resp = self.client.get("/api/messages")
        self.assertEqual(resp.status_code, 200)
        self.assertGreater(len(resp.get_json()), 50)

    def test_filter_from_actor_id(self):
        all_msgs = self.client.get("/api/messages").get_json()
        fid = all_msgs[0]["from_actor_id"]
        resp = self.client.get(f"/api/messages?from_actor_id={fid}")
        self.assertEqual(resp.status_code, 200)
        for m in resp.get_json():
            self.assertEqual(m["from_actor_id"], fid)

    def test_filter_to_actor_id(self):
        all_msgs = self.client.get("/api/messages").get_json()
        tid = all_msgs[0]["to_actor_id"]
        resp = self.client.get(f"/api/messages?to_actor_id={tid}")
        self.assertEqual(resp.status_code, 200)
        for m in resp.get_json():
            self.assertEqual(m["to_actor_id"], tid)

    def test_message_json_keys(self):
        resp = self.client.get("/api/messages")
        msg = resp.get_json()[0]
        expected_keys = {
            "id",
            "timestamp_us",
            "from_actor_id",
            "to_actor_id",
            "status",
            "endpoint",
            "port_id",
        }
        self.assertEqual(set(msg.keys()), expected_keys)


# ---------------------------------------------------------------------------
# Message status events
# ---------------------------------------------------------------------------


class MessageStatusEventsRoutesTest(_RouteTestBase):
    def test_list_all(self):
        resp = self.client.get("/api/message_status_events")
        self.assertEqual(resp.status_code, 200)
        self.assertGreater(len(resp.get_json()), 0)

    def test_filter_by_message_id(self):
        msgs = self.client.get("/api/messages").get_json()
        mid = msgs[0]["id"]
        resp = self.client.get(f"/api/message_status_events?message_id={mid}")
        self.assertEqual(resp.status_code, 200)
        events = resp.get_json()
        self.assertGreater(len(events), 0)
        for e in events:
            self.assertEqual(e["message_id"], mid)


# ---------------------------------------------------------------------------
# Sent messages
# ---------------------------------------------------------------------------


class SentMessagesRoutesTest(_RouteTestBase):
    def test_list_all(self):
        resp = self.client.get("/api/sent_messages")
        self.assertEqual(resp.status_code, 200)
        self.assertGreater(len(resp.get_json()), 0)

    def test_filter_by_sender(self):
        all_sm = self.client.get("/api/sent_messages").get_json()
        sid = all_sm[0]["sender_actor_id"]
        resp = self.client.get(f"/api/sent_messages?sender_actor_id={sid}")
        self.assertEqual(resp.status_code, 200)
        for s in resp.get_json():
            self.assertEqual(s["sender_actor_id"], sid)

    def test_sent_message_json_keys(self):
        resp = self.client.get("/api/sent_messages")
        sm = resp.get_json()[0]
        expected_keys = {
            "id",
            "timestamp_us",
            "sender_actor_id",
            "actor_mesh_id",
            "view_json",
            "shape_json",
        }
        self.assertEqual(set(sm.keys()), expected_keys)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


class SummaryRoutesTest(_RouteTestBase):
    def test_summary_returns_200(self):
        resp = self.client.get("/api/summary")
        self.assertEqual(resp.status_code, 200)

    def test_summary_top_level_keys(self):
        resp = self.client.get("/api/summary")
        data = resp.get_json()
        for key in (
            "mesh_counts",
            "actor_counts",
            "message_counts",
            "errors",
            "timeline",
            "health_score",
        ):
            self.assertIn(key, data)


if __name__ == "__main__":
    unittest.main()
