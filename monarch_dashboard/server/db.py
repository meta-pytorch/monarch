# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""SQL query layer for the Monarch Dashboard.

Provides read-only access to the fake (or real) SQLite telemetry database.
Each public function executes a single query and returns a list of row dicts.
All SQL is parameterised to prevent injection.
"""

import sqlite3
from typing import Any

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

_db_path: str | None = None


def init(db_path: str) -> None:
    """Set the database path used by all subsequent queries."""
    global _db_path
    _db_path = db_path


def _connect() -> sqlite3.Connection:
    """Open a read-only connection to the configured database."""
    if _db_path is None:
        raise RuntimeError("db.init() must be called before querying")
    conn = sqlite3.connect(_db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    """Execute *sql* with *params* and return all rows as dicts."""
    conn = _connect()
    try:
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _query_one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    """Execute *sql* and return the first row as a dict, or None."""
    rows = _query(sql, params)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# Mesh queries
# ---------------------------------------------------------------------------


def list_meshes(mesh_class: str | None = None) -> list[dict[str, Any]]:
    """Return all meshes, optionally filtered by class."""
    if mesh_class:
        return _query("SELECT * FROM meshes WHERE class = ? ORDER BY id", (mesh_class,))
    return _query("SELECT * FROM meshes ORDER BY id")


def get_mesh(mesh_id: int) -> dict[str, Any] | None:
    """Return a single mesh by id."""
    return _query_one("SELECT * FROM meshes WHERE id = ?", (mesh_id,))


def get_mesh_children(mesh_id: int) -> list[dict[str, Any]]:
    """Return child meshes whose parent_mesh_id equals *mesh_id*."""
    return _query(
        "SELECT * FROM meshes WHERE parent_mesh_id = ? ORDER BY id", (mesh_id,)
    )


# ---------------------------------------------------------------------------
# Actor queries
# ---------------------------------------------------------------------------


def list_actors(mesh_id: int | None = None) -> list[dict[str, Any]]:
    """Return all actors, optionally filtered by mesh_id."""
    if mesh_id is not None:
        return _query("SELECT * FROM actors WHERE mesh_id = ? ORDER BY id", (mesh_id,))
    return _query("SELECT * FROM actors ORDER BY id")


def get_actor(actor_id: int) -> dict[str, Any] | None:
    """Return a single actor by id (without status)."""
    return _query_one("SELECT * FROM actors WHERE id = ?", (actor_id,))


def get_actor_latest_status(actor_id: int) -> dict[str, Any] | None:
    """Return the latest status for an actor, or None if no events exist.

    Returns a dict with ``latest_status`` and ``status_timestamp_us`` keys,
    ready to be merged into an actor dict.
    """
    row = _query_one(
        "SELECT new_status AS latest_status, "
        "timestamp_us AS status_timestamp_us "
        "FROM actor_status_events WHERE actor_id = ? "
        "ORDER BY timestamp_us DESC LIMIT 1",
        (actor_id,),
    )
    return row


# ---------------------------------------------------------------------------
# Actor status event queries
# ---------------------------------------------------------------------------


def list_actor_status_events(
    actor_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return status events, optionally filtered by actor_id."""
    if actor_id is not None:
        return _query(
            "SELECT * FROM actor_status_events WHERE actor_id = ? "
            "ORDER BY timestamp_us",
            (actor_id,),
        )
    return _query("SELECT * FROM actor_status_events ORDER BY timestamp_us")


# ---------------------------------------------------------------------------
# Message queries
# ---------------------------------------------------------------------------


def list_messages(
    from_actor_id: int | None = None,
    to_actor_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return messages with optional sender/receiver filters."""
    clauses: list[str] = []
    params: list[Any] = []
    if from_actor_id is not None:
        clauses.append("from_actor_id = ?")
        params.append(from_actor_id)
    if to_actor_id is not None:
        clauses.append("to_actor_id = ?")
        params.append(to_actor_id)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return _query(f"SELECT * FROM messages{where} ORDER BY timestamp_us", tuple(params))


def get_actor_messages(actor_id: int) -> list[dict[str, Any]]:
    """Return all messages where the actor is sender or receiver."""
    return _query(
        "SELECT * FROM messages "
        "WHERE from_actor_id = ? OR to_actor_id = ? "
        "ORDER BY timestamp_us",
        (actor_id, actor_id),
    )


# ---------------------------------------------------------------------------
# Message status event queries
# ---------------------------------------------------------------------------


def list_message_status_events(
    message_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return message status events, optionally filtered by message_id."""
    if message_id is not None:
        return _query(
            "SELECT * FROM message_status_events WHERE message_id = ? "
            "ORDER BY timestamp_us",
            (message_id,),
        )
    return _query("SELECT * FROM message_status_events ORDER BY timestamp_us")


# ---------------------------------------------------------------------------
# Sent message queries
# ---------------------------------------------------------------------------


def list_sent_messages(
    sender_actor_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return sent messages, optionally filtered by sender_actor_id."""
    if sender_actor_id is not None:
        return _query(
            "SELECT * FROM sent_messages WHERE sender_actor_id = ? "
            "ORDER BY timestamp_us",
            (sender_actor_id,),
        )
    return _query("SELECT * FROM sent_messages ORDER BY timestamp_us")


# ---------------------------------------------------------------------------
# Summary / aggregate queries
# ---------------------------------------------------------------------------


def get_summary() -> dict[str, Any]:
    """Return aggregate metrics for the summary dashboard.

    Performs multiple aggregate queries in a single connection for efficiency.
    """
    conn = _connect()
    try:
        # -- Mesh counts --
        total_meshes = conn.execute("SELECT COUNT(*) FROM meshes").fetchone()[0]
        by_class_rows = conn.execute(
            "SELECT class, COUNT(*) AS cnt FROM meshes GROUP BY class ORDER BY class"
        ).fetchall()
        mesh_by_class = {row["class"]: row["cnt"] for row in by_class_rows}

        # -- Actor counts --
        total_actors = conn.execute("SELECT COUNT(*) FROM actors").fetchone()[0]

        # Latest status per actor (subquery picks the most recent event).
        actor_status_rows = conn.execute(
            "SELECT sub.new_status, COUNT(*) AS cnt FROM ("
            "  SELECT ase.actor_id, ase.new_status"
            "  FROM actor_status_events ase"
            "  INNER JOIN ("
            "    SELECT actor_id, MAX(timestamp_us) AS max_ts"
            "    FROM actor_status_events GROUP BY actor_id"
            "  ) latest ON ase.actor_id = latest.actor_id"
            "    AND ase.timestamp_us = latest.max_ts"
            ") sub GROUP BY sub.new_status ORDER BY sub.new_status"
        ).fetchall()
        actor_by_status = {row["new_status"]: row["cnt"] for row in actor_status_rows}

        # -- Message counts --
        total_messages = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

        msg_status_rows = conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM messages GROUP BY status ORDER BY status"
        ).fetchall()
        msg_by_status = {row["status"]: row["cnt"] for row in msg_status_rows}

        msg_endpoint_rows = conn.execute(
            "SELECT endpoint, COUNT(*) AS cnt FROM messages "
            "GROUP BY endpoint ORDER BY endpoint"
        ).fetchall()
        msg_by_endpoint = {row["endpoint"]: row["cnt"] for row in msg_endpoint_rows}

        delivered = msg_by_status.get("delivered", 0)
        delivery_rate = (
            round(delivered / total_messages, 3) if total_messages > 0 else 0.0
        )

        # -- Error details --
        failed_actors = [
            dict(row)
            for row in conn.execute(
                "SELECT ase.actor_id, a.full_name, ase.reason, ase.timestamp_us, a.mesh_id "
                "FROM actor_status_events ase "
                "JOIN actors a ON ase.actor_id = a.id "
                "INNER JOIN ("
                "  SELECT actor_id, MAX(timestamp_us) AS max_ts "
                "  FROM actor_status_events GROUP BY actor_id"
                ") latest ON ase.actor_id = latest.actor_id "
                "  AND ase.timestamp_us = latest.max_ts "
                "WHERE ase.new_status = 'failed' "
                "ORDER BY ase.timestamp_us"
            ).fetchall()
        ]

        stopped_actors = [
            dict(row)
            for row in conn.execute(
                "SELECT ase.actor_id, a.full_name, ase.reason, ase.timestamp_us, a.mesh_id "
                "FROM actor_status_events ase "
                "JOIN actors a ON ase.actor_id = a.id "
                "INNER JOIN ("
                "  SELECT actor_id, MAX(timestamp_us) AS max_ts "
                "  FROM actor_status_events GROUP BY actor_id"
                ") latest ON ase.actor_id = latest.actor_id "
                "  AND ase.timestamp_us = latest.max_ts "
                "WHERE ase.new_status = 'stopped' "
                "ORDER BY ase.timestamp_us"
            ).fetchall()
        ]

        failed_messages = msg_by_status.get("failed", 0)

        # -- Timeline --
        time_range = conn.execute(
            "SELECT MIN(timestamp_us) AS start_us, MAX(timestamp_us) AS end_us "
            "FROM actor_status_events"
        ).fetchone()
        start_us = time_range["start_us"] if time_range else 0
        end_us = time_range["end_us"] if time_range else 0

        failure_onset_row = conn.execute(
            "SELECT MIN(timestamp_us) AS ts FROM actor_status_events "
            "WHERE new_status = 'failed'"
        ).fetchone()
        failure_onset_us = (
            failure_onset_row["ts"]
            if failure_onset_row and failure_onset_row["ts"]
            else None
        )

        total_status_events = conn.execute(
            "SELECT COUNT(*) FROM actor_status_events"
        ).fetchone()[0]
        total_message_events = conn.execute(
            "SELECT COUNT(*) FROM message_status_events"
        ).fetchone()[0]

        # -- Health score (0-100) --
        # Weighted by status: idle=100, processing=80, unknown/client=50,
        # transitional=30, stopped=20, failed=0
        weights = {
            "idle": 100,
            "processing": 80,
            "client": 50,
            "unknown": 50,
            "created": 30,
            "initializing": 30,
            "saving": 30,
            "loading": 30,
            "stopping": 30,
            "stopped": 20,
            "failed": 0,
        }
        total_weight = 0
        actor_count_with_status = 0
        for status, count in actor_by_status.items():
            w = weights.get(status, 50)
            total_weight += w * count
            actor_count_with_status += count
        health_score = (
            round(total_weight / actor_count_with_status)
            if actor_count_with_status > 0
            else 100
        )

        return {
            "mesh_counts": {
                "total": total_meshes,
                "by_class": mesh_by_class,
            },
            "actor_counts": {
                "total": total_actors,
                "by_status": actor_by_status,
            },
            "message_counts": {
                "total": total_messages,
                "by_status": msg_by_status,
                "by_endpoint": msg_by_endpoint,
                "delivery_rate": delivery_rate,
            },
            "errors": {
                "failed_actors": failed_actors,
                "stopped_actors": stopped_actors,
                "failed_messages": failed_messages,
            },
            "timeline": {
                "start_us": start_us,
                "end_us": end_us,
                "failure_onset_us": failure_onset_us,
                "total_status_events": total_status_events,
                "total_message_events": total_message_events,
            },
            "health_score": health_score,
        }
    finally:
        conn.close()
