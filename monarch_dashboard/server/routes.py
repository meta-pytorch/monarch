# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""API route definitions for the Monarch Dashboard.

Registers a Flask Blueprint with all REST endpoints for querying meshes,
actors, status events, messages, and sent messages.  Every handler returns
JSON and uses standard HTTP status codes (200, 404).
"""

from flask import Blueprint, jsonify, request

from . import db

api = Blueprint("api", __name__, url_prefix="/api")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@api.route("/health")
def health():
    """Simple liveness check."""
    return jsonify({"status": "ok"})


@api.route("/summary")
def summary():
    """Aggregate metrics for the summary dashboard."""
    return jsonify(db.get_summary())


# ---------------------------------------------------------------------------
# Meshes
# ---------------------------------------------------------------------------


@api.route("/meshes")
def list_meshes():
    """List all meshes.  Optional query param: ?class=Host"""
    mesh_class = request.args.get("class")
    return jsonify(db.list_meshes(mesh_class))


@api.route("/meshes/<int:mesh_id>")
def get_mesh(mesh_id):
    """Get a single mesh by id."""
    mesh = db.get_mesh(mesh_id)
    if mesh is None:
        return jsonify({"error": "mesh not found"}), 404
    return jsonify(mesh)


@api.route("/meshes/<int:mesh_id>/children")
def get_mesh_children(mesh_id):
    """Get child meshes of a given mesh."""
    parent = db.get_mesh(mesh_id)
    if parent is None:
        return jsonify({"error": "mesh not found"}), 404
    return jsonify(db.get_mesh_children(mesh_id))


# ---------------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------------


@api.route("/actors")
def list_actors():
    """List all actors.  Optional query param: ?mesh_id=3"""
    mesh_id = request.args.get("mesh_id", type=int)
    return jsonify(db.list_actors(mesh_id))


@api.route("/actors/<int:actor_id>")
def get_actor(actor_id):
    """Get a single actor by id, including its latest status."""
    actor = db.get_actor(actor_id)
    if actor is None:
        return jsonify({"error": "actor not found"}), 404
    status = db.get_actor_latest_status(actor_id)
    if status:
        actor.update(status)
    else:
        actor["latest_status"] = None
        actor["status_timestamp_us"] = None
    return jsonify(actor)


@api.route("/actors/<int:actor_id>/status_events")
def get_actor_status_events(actor_id):
    """Get the status event history for an actor."""
    actor = db.get_actor(actor_id)
    if actor is None:
        return jsonify({"error": "actor not found"}), 404
    return jsonify(db.list_actor_status_events(actor_id))


@api.route("/actors/<int:actor_id>/messages")
def get_actor_messages(actor_id):
    """Get all messages where the actor is sender or receiver."""
    actor = db.get_actor(actor_id)
    if actor is None:
        return jsonify({"error": "actor not found"}), 404
    return jsonify(db.get_actor_messages(actor_id))


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@api.route("/messages")
def list_messages():
    """List messages.  Optional: ?from_actor_id=1&to_actor_id=2"""
    from_id = request.args.get("from_actor_id", type=int)
    to_id = request.args.get("to_actor_id", type=int)
    return jsonify(db.list_messages(from_id, to_id))


# ---------------------------------------------------------------------------
# Message status events
# ---------------------------------------------------------------------------


@api.route("/message_status_events")
def list_message_status_events():
    """List message status events.  Optional: ?message_id=5"""
    message_id = request.args.get("message_id", type=int)
    return jsonify(db.list_message_status_events(message_id))


# ---------------------------------------------------------------------------
# Sent messages
# ---------------------------------------------------------------------------


@api.route("/sent_messages")
def list_sent_messages():
    """List sent messages.  Optional: ?sender_actor_id=1"""
    sender_id = request.args.get("sender_actor_id", type=int)
    return jsonify(db.list_sent_messages(sender_id))
