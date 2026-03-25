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

from typing import Any

from flask import Blueprint, jsonify, request

from . import db

api = Blueprint("api", __name__, url_prefix="/api")

# Monarch uses 64-bit IDs which can exceed JavaScript's Number.MAX_SAFE_INTEGER.
# We always serialize ID fields as strings for type consistency on the frontend.


def _sanitize_for_js(obj: Any, _key: str | None = None) -> Any:
    """Recursively convert ID fields to strings for JavaScript safety.

    Any dict value whose key is ``"id"`` or ends with ``"_id"`` is
    stringified, regardless of magnitude.  This keeps the frontend
    ``EntityId`` type a simple ``string`` rather than ``number | string``.
    """
    if isinstance(obj, bool):
        return obj
    if (
        isinstance(obj, int)
        and _key is not None
        and (_key == "id" or _key.endswith("_id"))
    ):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _sanitize_for_js(v, _key=k) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_js(item) for item in obj]
    return obj


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
    return jsonify(_sanitize_for_js(db.get_summary()))


@api.route("/dag")
def dag():
    """Classified nodes and edges for the DAG visualization."""
    try:
        return jsonify(_sanitize_for_js(db.get_dag_data()))
    except Exception as exc:
        return jsonify({"error": str(exc), "nodes": [], "edges": []}), 500


# ---------------------------------------------------------------------------
# Meshes
# ---------------------------------------------------------------------------


@api.route("/meshes")
def list_meshes():
    """List meshes.  Optional: ?class=Host&parent_mesh_id=1&exclude_classes=Host,Proc"""
    class_filter = request.args.get("class", type=str)
    parent_mesh_id = request.args.get("parent_mesh_id", type=int)
    exclude_raw = request.args.get("exclude_classes", type=str)
    exclude_classes = exclude_raw.split(",") if exclude_raw else None
    return jsonify(
        _sanitize_for_js(
            db.list_meshes(
                class_filter=class_filter,
                parent_mesh_id=parent_mesh_id,
                exclude_classes=exclude_classes,
            )
        )
    )


@api.route("/meshes/<int:mesh_id>")
def get_mesh(mesh_id):
    """Get a single mesh by id."""
    mesh = db.get_mesh(mesh_id)
    if mesh is None:
        return jsonify({"error": "mesh not found"}), 404
    return jsonify(_sanitize_for_js(mesh))


@api.route("/meshes/<int:mesh_id>/children")
def get_mesh_children(mesh_id):
    """Get child meshes of a given mesh.  Optional: ?mesh_class=Proc&exclude_classes=Host,Proc"""
    parent = db.get_mesh(mesh_id)
    if parent is None:
        return jsonify({"error": "mesh not found"}), 404
    mesh_class = request.args.get("mesh_class", type=str)
    exclude_raw = request.args.get("exclude_classes", type=str)
    exclude_classes = exclude_raw.split(",") if exclude_raw else None
    return jsonify(
        _sanitize_for_js(
            db.get_mesh_children(
                mesh_id, mesh_class=mesh_class, exclude_classes=exclude_classes
            )
        )
    )


# ---------------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------------


@api.route("/actors")
def list_actors():
    """List all actors.  Optional: ?mesh_id=1"""
    mesh_id = request.args.get("mesh_id", type=int)
    return jsonify(_sanitize_for_js(db.list_actors(mesh_id=mesh_id)))


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
    return jsonify(_sanitize_for_js(actor))


@api.route("/actors/<int:actor_id>/status_events")
def get_actor_status_events(actor_id):
    """Get the status event history for an actor."""
    actor = db.get_actor(actor_id)
    if actor is None:
        return jsonify({"error": "actor not found"}), 404
    return jsonify(_sanitize_for_js(db.list_actor_status_events(actor_id)))


@api.route("/actors/<int:actor_id>/messages")
def get_actor_messages(actor_id):
    """Get all messages where the actor is sender or receiver."""
    actor = db.get_actor(actor_id)
    if actor is None:
        return jsonify({"error": "actor not found"}), 404
    return jsonify(_sanitize_for_js(db.get_actor_messages(actor_id)))


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@api.route("/messages")
def list_messages():
    """List messages.  Optional: ?from_actor_id=1&to_actor_id=2"""
    from_id = request.args.get("from_actor_id", type=int)
    to_id = request.args.get("to_actor_id", type=int)
    return jsonify(_sanitize_for_js(db.list_messages(from_id, to_id)))


# ---------------------------------------------------------------------------
# Message status events
# ---------------------------------------------------------------------------


@api.route("/message_status_events")
def list_message_status_events():
    """List message status events.  Optional: ?message_id=5"""
    message_id = request.args.get("message_id", type=int)
    return jsonify(_sanitize_for_js(db.list_message_status_events(message_id)))


# ---------------------------------------------------------------------------
# Sent messages
# ---------------------------------------------------------------------------


@api.route("/sent_messages")
def list_sent_messages():
    """List sent messages.  Optional: ?sender_actor_id=1"""
    sender_id = request.args.get("sender_actor_id", type=int)
    return jsonify(_sanitize_for_js(db.list_sent_messages(sender_id)))


# ---------------------------------------------------------------------------
# SQL query
# ---------------------------------------------------------------------------


@api.route("/query", methods=["POST"])
def query():
    """Execute an arbitrary SQL query against the DataFusion engine."""
    data = request.get_json()
    if not data or "sql" not in data:
        return jsonify({"error": "missing 'sql' in request body"}), 400
    sql = data["sql"]
    try:
        rows = db.raw_query(sql)
        return jsonify({"rows": rows})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


# ---------------------------------------------------------------------------
# Py-spy dump storage
# ---------------------------------------------------------------------------


@api.route("/pyspy_dump", methods=["POST"])
def pyspy_dump():
    """Store a py-spy dump result in the DataFusion pyspy tables."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "missing request body"}), 400
    dump_id = data.get("dump_id")
    proc_ref = data.get("proc_ref")
    pyspy_result_json = data.get("pyspy_result_json")
    if not all([dump_id, proc_ref, pyspy_result_json]):
        return jsonify(
            {"error": "missing dump_id, proc_ref, or pyspy_result_json"}
        ), 400
    try:
        db.store_pyspy_dump(dump_id, proc_ref, pyspy_result_json)
        return jsonify({"status": "ok"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
