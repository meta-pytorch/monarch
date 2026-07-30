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
from monarch._rust_bindings.monarch_extension.snapshot_integration import (
    _snapshot_table_names,
)

from . import db
from .admin_dag import build_admin_dag
from .cache import cached
from .pyspy_client import capture_pyspy_dump, mesh_admin_base_url
from .system_actors import get_system_actor_names

api = Blueprint("api", __name__, url_prefix="/api")

_SNAPSHOT_TABLE_NAMES = tuple(_snapshot_table_names())
_SNAPSHOT_TABLE_NAME_SET = frozenset(_SNAPSHOT_TABLE_NAMES)

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
    return jsonify(cached("summary", lambda: _sanitize_for_js(db.get_summary())))


@api.route("/message-stats")
def message_stats():
    """Consolidated, cached message metrics: handler lifecycle, per-endpoint
    volume, and the distinct actor pairs for the topology overlay.

    The frontend reads all message-table aggregates from this one cached
    endpoint rather than posting raw per-poll SQL. A raw client scan of
    ``message_status_events`` is streamed back through the telemetry scanner as
    one message per batch, each recorded as a ``queued`` event, so polling raw
    scans of that table inflates the table being read (a self-amplifying loop).
    One cached server-side aggregate keeps observer load bounded — the ids are
    already JS-safe strings, so no sanitize pass is needed.
    """
    return jsonify(cached("message-stats", db.get_message_stats))


@api.route("/message-activity")
def message_activity():
    """Cached message-throughput histogram + window for the activity panel.

    Returns a small fixed-size histogram (buckets + span + total) instead of the
    full message list, so the panel's poll never streams the whole `messages`
    table back through the telemetry scanner (which would inflate
    `message_status_events`)."""
    return jsonify(cached("message-activity", db.get_message_activity))


@api.route("/dag")
def dag():
    """Classified nodes and edges for the DAG visualization.

    When snapshot tables are populated (periodic snapshot capture),
    uses the 4-tier snapshot hierarchy — same structure as the TUI:
    Host → Proc → Actor.  System actors are filtered using the
    snapshot ``is_system`` flag and a name-based heuristic.

    Until the first snapshot is captured (cold start), returns an empty
    DAG with ``snapshot_pending: true`` so the frontend can show a
    "waiting for first snapshot" state.

    Optional: ?hide_system=true (default) to filter system actors.
    """
    hide_system = request.args.get("hide_system", "true").lower() != "false"
    cache_key = f"dag:hide_system={hide_system}"
    try:

        def _compute_dag():
            result = build_admin_dag(hide_system=hide_system)
            if result.get("nodes"):
                return _sanitize_for_js(
                    {
                        "nodes": result["nodes"],
                        "edges": result["edges"],
                    }
                )

            # No snapshot captured yet (cold start — up to the first snapshot
            # interval). The frontend shows a "waiting for first snapshot"
            # state. We no longer fall back to the telemetry-SQL DAG: it
            # produced a divergent second node shape (explicit *_mesh container
            # nodes) that mesh-view collapse mishandled, and snapshots are
            # always configured whenever the dashboard is enabled.
            return {"nodes": [], "edges": [], "snapshot_pending": True}

        return jsonify(cached(cache_key, _compute_dag))
    except Exception as exc:
        return jsonify({"error": str(exc), "nodes": [], "edges": []}), 500


@api.route("/system-actors")
def list_system_actors():
    """Return the set of system actor names from the Mesh Admin API."""
    names = get_system_actor_names()
    return jsonify({"system_actors": sorted(names), "count": len(names)})


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
    cache_key = f"meshes:{class_filter}:{parent_mesh_id}:{exclude_raw}"
    return jsonify(
        cached(
            cache_key,
            lambda: _sanitize_for_js(
                db.list_meshes(
                    class_filter=class_filter,
                    parent_mesh_id=parent_mesh_id,
                    exclude_classes=exclude_classes,
                )
            ),
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
    cache_key = f"actors:mesh_id={mesh_id}"
    return jsonify(
        cached(cache_key, lambda: _sanitize_for_js(db.list_actors(mesh_id=mesh_id)))
    )


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


@api.route("/pyspy/capture", methods=["POST"])
def pyspy_capture():
    """Trigger an on-demand py-spy stack dump for a proc.

    Body: ``{"proc_ref": "<proc entity_id from /api/dag>"}``. py-spy is
    proc-level, so this profiles the whole process (all actors sharing it).
    Proxies to the Mesh Admin ``GET /v1/pyspy/{proc_ref}`` and returns the
    structured ``PySpyResult`` verbatim under ``result``.
    """
    data = request.get_json(silent=True) or {}
    proc_ref = data.get("proc_ref")
    if not proc_ref:
        return jsonify({"error": "missing 'proc_ref' in request body"}), 400
    try:
        result = capture_pyspy_dump(proc_ref)
        return jsonify({"proc_ref": proc_ref, "result": result})
    except Exception as exc:
        return jsonify({"error": str(exc), "admin_url": mesh_admin_base_url()}), 502


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


# ---------------------------------------------------------------------------
# Snapshot ingest
# ---------------------------------------------------------------------------


@api.route("/ingest_snapshot/<table_name>", methods=["POST"])
def ingest_snapshot(table_name: str):
    """Store one snapshot Arrow IPC stream in the sidecar collector."""
    if table_name not in _SNAPSHOT_TABLE_NAME_SET:
        return jsonify({"error": f"not a snapshot table: {table_name}"}), 400

    try:
        db.ingest_snapshot_batch(table_name, request.get_data())
        return jsonify({"status": "ok"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
