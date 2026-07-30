# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""SQL query layer for the Monarch Dashboard.

Defines a DBAdapter interface with two implementations:
  - SQLiteAdapter: local dev/testing with a SQLite file (fake or real data).
  - QueryEngineAdapter (separate module): production, wraps Monarch's
    DataFusion QueryEngine for live telemetry.

Module-level functions (init, _query, etc.) provide backward compatibility
by delegating to a module-level SQLiteAdapter instance.
"""

import sqlite3
from abc import ABC, abstractmethod
from typing import Any


# ---------------------------------------------------------------------------
# Abstract adapter interface
# ---------------------------------------------------------------------------


class DBAdapter(ABC):
    """Interface for dashboard data access.

    Implementations must support SQL queries returning rows as dicts.
    The SQL passed to ``query`` is always fully formatted — no placeholders.
    """

    @abstractmethod
    def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute *sql* and return rows as dicts."""
        ...

    @abstractmethod
    def table_names(self) -> list[str]:
        """Return the names of available tables."""
        ...

    def query_one(self, sql: str) -> dict[str, Any] | None:
        """Execute *sql* and return the first row, or None."""
        rows = self.query(sql)
        return rows[0] if rows else None

    def store_pyspy_dump(  # noqa: B027
        self, dump_id: str, proc_ref: str, pyspy_result_json: str
    ) -> None:
        """Store a py-spy dump result. No-op by default."""
        pass

    def ingest_snapshot_batch(self, table_name: str, arrow_ipc_bytes: bytes) -> None:
        """Store one snapshot Arrow IPC stream. Unsupported by default."""
        raise NotImplementedError("snapshot ingest unavailable")


# ---------------------------------------------------------------------------
# SQLite adapter — local dev/testing
# ---------------------------------------------------------------------------


class SQLiteAdapter(DBAdapter):
    """LOCAL DEV/TESTING: reads from a SQLite database file.

    For production use with the live Monarch telemetry stack,
    use QueryEngineAdapter instead.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        # WAL mode allows concurrent readers without blocking on writes.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def query(self, sql: str) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(sql).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def table_names(self) -> list[str]:
        return [
            r["name"]
            for r in self.query("SELECT name FROM sqlite_master WHERE type='table'")
        ]


# ---------------------------------------------------------------------------
# Module-level backward compatibility
# ---------------------------------------------------------------------------

_adapter: DBAdapter | None = None


def init(db_path: str) -> None:
    """Initialise with a SQLite database path (backward-compatible entry point)."""
    global _adapter
    _adapter = SQLiteAdapter(db_path)


def set_adapter(adapter: DBAdapter) -> None:
    """Replace the module-level adapter (e.g. with a QueryEngineAdapter)."""
    global _adapter
    _adapter = adapter


def _get_adapter() -> DBAdapter:
    if _adapter is None:
        raise RuntimeError("db.init() or db.set_adapter() must be called first")
    return _adapter


def raw_query(sql: str) -> list[dict[str, Any]]:
    """Execute a raw SQL query (no placeholder substitution)."""
    return _get_adapter().query(sql)


def store_pyspy_dump(dump_id: str, proc_ref: str, pyspy_result_json: str) -> None:
    """Store a py-spy dump result via the current adapter."""
    _get_adapter().store_pyspy_dump(dump_id, proc_ref, pyspy_result_json)


def ingest_snapshot_batch(table_name: str, arrow_ipc_bytes: bytes) -> None:
    """Store one snapshot Arrow IPC stream via the current adapter."""
    _get_adapter().ingest_snapshot_batch(table_name, arrow_ipc_bytes)


def _sql_literal(value: Any) -> str:
    """Convert a Python value to a SQL literal string for placeholder substitution."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    # String: escape single quotes by doubling them.
    s = str(value).replace("'", "''")
    return f"'{s}'"


def _format_sql(sql: str, params: tuple) -> str:
    """Replace ``?`` placeholders in *sql* with literal values from *params*."""
    if not params:
        return sql
    parts = sql.split("?")
    if len(parts) - 1 != len(params):
        raise ValueError(f"Expected {len(parts) - 1} params, got {len(params)}")
    result = parts[0]
    for i, param in enumerate(params):
        result += _sql_literal(param) + parts[i + 1]
    return result


def _query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    """Execute *sql* with *params* and return all rows as dicts.

    Placeholders (``?``) are substituted with literal values before the query
    is forwarded to the adapter, so the adapter only ever sees fully-formed SQL.
    """
    return _get_adapter().query(_format_sql(sql, params))


def _query_one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    """Execute *sql* and return the first row as a dict, or None."""
    return _get_adapter().query_one(_format_sql(sql, params))


def _dedup_rows(rows: list[dict[str, Any]], key: str = "id") -> list[dict[str, Any]]:
    """Deduplicate rows by *key*, keeping the first occurrence."""
    seen: set = set()
    result: list[dict[str, Any]] = []
    for r in rows:
        val = r.get(key)
        if val not in seen:
            seen.add(val)
            result.append(r)
    return result


# Reusable SQL fragments for latest-status subqueries.

# Latest status per entity via a single-pass window function (ROW_NUMBER over
# one ordered scan). A MAX(timestamp) self-join is RACY against a
# concurrently-appended table: its two scans can observe different snapshots, so
# for a fast-updating entity the `timestamp = max_ts` equality finds no row and
# the entity drops out entirely — surfacing as a null status (→ spurious
# "unknown", which dinged the health score). ROW_NUMBER reads the table once, so
# it always returns exactly one row per entity. Column names (actor_id/message_id,
# new_status/status, max_ts) are kept identical so callers are unaffected.
_LATEST_ACTOR_STATUS_SQL = (
    "SELECT actor_id, new_status, max_ts FROM ("
    "   SELECT actor_id, new_status, timestamp_us AS max_ts,"
    "     ROW_NUMBER() OVER ("
    "       PARTITION BY actor_id ORDER BY timestamp_us DESC"
    "     ) AS rn"
    "   FROM actor_status_events"
    " ) t WHERE rn = 1"
)

_LATEST_MSG_STATUS_SQL = (
    "SELECT message_id, status FROM ("
    "   SELECT message_id, status,"
    "     ROW_NUMBER() OVER ("
    "       PARTITION BY message_id ORDER BY timestamp_us DESC"
    "     ) AS rn"
    "   FROM message_status_events"
    " ) t WHERE rn = 1"
)

# Deduped handler lifecycle: reduce each message's events to one terminal/current
# state with precedence complete > failed > active > queued (so
# queued -> active -> failed counts as `failed`, not still-in-flight).
#
# Grounded in the `messages` table (real handler-dispatched messages), NOT the
# raw `message_status_events` set. The telemetry query transport itself emits a
# `queued` status event per once-port QueryResponse post but never a `messages`
# row, so counting distinct message_ids in message_status_events would fold that
# self-instrumentation flood into "queued" (hundreds of thousands of phantom
# messages). The LEFT JOIN keeps only genuine application messages.
_MSG_LIFECYCLE_SQL = (
    "SELECT state, COUNT(*) AS n FROM ("
    " SELECT m.id, CASE"
    " WHEN MAX(CASE WHEN LOWER(e.status) = 'complete' THEN 1 ELSE 0 END) = 1 THEN 'complete'"
    " WHEN MAX(CASE WHEN LOWER(e.status) = 'failed' THEN 1 ELSE 0 END) = 1 THEN 'failed'"
    " WHEN MAX(CASE WHEN LOWER(e.status) = 'active' THEN 1 ELSE 0 END) = 1 THEN 'active'"
    " ELSE 'queued' END AS state"
    " FROM messages m LEFT JOIN message_status_events e ON e.message_id = m.id"
    " GROUP BY m.id"
    " ) t GROUP BY state"
)


# ---------------------------------------------------------------------------
# Mesh queries
# ---------------------------------------------------------------------------


def list_meshes(
    class_filter: str | None = None,
    parent_mesh_id: int | None = None,
    exclude_classes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return meshes, optionally filtered by class and/or parent_mesh_id.

    ``exclude_classes`` removes meshes whose class is in the given list
    (applied in Python to work with both SQLite and DataFusion).
    Results are deduplicated by mesh id.
    """
    clauses: list[str] = []
    params: list[Any] = []
    if class_filter is not None:
        clauses.append("class = ?")
        params.append(class_filter)
    if parent_mesh_id is not None:
        clauses.append("parent_mesh_id = ?")
        params.append(parent_mesh_id)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = _query(f"SELECT * FROM meshes{where} ORDER BY id", tuple(params))
    if exclude_classes:
        rows = [r for r in rows if r.get("class") not in exclude_classes]
    return _dedup_rows(rows)


def get_mesh(mesh_id: int) -> dict[str, Any] | None:
    """Return a single mesh by id."""
    return _query_one("SELECT * FROM meshes WHERE id = ?", (mesh_id,))


def get_mesh_children(
    mesh_id: int,
    mesh_class: str | None = None,
    exclude_classes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return child meshes of *mesh_id* (where parent_mesh_id = mesh_id).

    Optionally filter by ``mesh_class`` or exclude specific classes.
    Results are deduplicated by mesh id.
    """
    rows = _query(
        "SELECT * FROM meshes WHERE parent_mesh_id = ? ORDER BY id", (mesh_id,)
    )
    # Exclude self-referencing meshes (e.g. Proc "local" with same id as
    # its parent Host "local" in DataFusion).
    rows = [r for r in rows if r["id"] != mesh_id]
    if mesh_class is not None:
        rows = [r for r in rows if r.get("class") == mesh_class]
    if exclude_classes:
        rows = [r for r in rows if r.get("class") not in exclude_classes]
    return _dedup_rows(rows)


# ---------------------------------------------------------------------------
# Actor queries
# ---------------------------------------------------------------------------


def list_actors(mesh_id: int | None = None) -> list[dict[str, Any]]:
    """Return all actors with latest_status and mesh_class, optionally filtered."""
    base = (
        "SELECT a.*, m.class AS mesh_class,"
        " m.given_name AS mesh_name,"
        " latest.new_status AS latest_status,"
        " latest.max_ts AS status_timestamp_us"
        " FROM actors a"
        " LEFT JOIN meshes m ON a.mesh_id = m.id"
        f" LEFT JOIN ({_LATEST_ACTOR_STATUS_SQL}) latest"
        " ON a.id = latest.actor_id"
    )
    if mesh_id is not None:
        rows = _query(f"{base} WHERE a.mesh_id = ? ORDER BY a.id", (mesh_id,))
    else:
        rows = _query(f"{base} ORDER BY a.id")
    rows = _dedup_rows(rows)
    # Normalise status to lowercase (DataFusion emits PascalCase, fake data lowercase).
    for r in rows:
        if r.get("latest_status"):
            r["latest_status"] = r["latest_status"].lower()
    return rows


def get_actor(actor_id: int) -> dict[str, Any] | None:
    """Return a single actor by id (base fields only, no status JOIN)."""
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
    if row and row.get("latest_status"):
        row["latest_status"] = row["latest_status"].lower()
    return row


# ---------------------------------------------------------------------------
# Actor status event queries
# ---------------------------------------------------------------------------


# Cap for the per-actor drill-in history (status events, messages). Unlike the
# message tables, actor_status_events is unbounded (one event per
# Idle<->Processing flip), so an actor accumulates thousands of events; the
# detail drawer only needs recent history. This bounds the payload/scan for the
# drill-in — bounding the underlying table is the core fix (paste P2440431201).
_DRILL_LIMIT = 200


def list_actor_status_events(
    actor_id: int | None = None,
    limit: int = _DRILL_LIMIT,
) -> list[dict[str, Any]]:
    """Return the *most recent* ``limit`` status events (re-sorted ascending for
    display), optionally filtered by actor_id. Capped because
    ``actor_status_events`` is unbounded — see ``_DRILL_LIMIT``."""
    if actor_id is not None:
        return _query(
            "SELECT * FROM ("
            " SELECT * FROM actor_status_events WHERE actor_id = ?"
            " ORDER BY timestamp_us DESC LIMIT ?"
            " ) t ORDER BY timestamp_us",
            (actor_id, limit),
        )
    return _query(
        "SELECT * FROM ("
        " SELECT * FROM actor_status_events ORDER BY timestamp_us DESC LIMIT ?"
        " ) t ORDER BY timestamp_us",
        (limit,),
    )


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


def get_actor_messages(
    actor_id: int, limit: int = _DRILL_LIMIT
) -> list[dict[str, Any]]:
    """Return the actor's *most recent* ``limit`` messages (re-sorted ascending
    for display), with latest status. Capped for the same reason as
    ``list_actor_status_events`` — see ``_DRILL_LIMIT``."""
    rows = _query(
        "SELECT * FROM ("
        " SELECT m.*, latest.status AS latest_status"
        " FROM messages m"
        f" LEFT JOIN ({_LATEST_MSG_STATUS_SQL}) latest"
        " ON m.id = latest.message_id"
        " WHERE m.from_actor_id = ? OR m.to_actor_id = ?"
        " ORDER BY m.timestamp_us DESC LIMIT ?"
        " ) t ORDER BY timestamp_us",
        (actor_id, actor_id, limit),
    )
    for r in rows:
        if r.get("latest_status"):
            r["latest_status"] = r["latest_status"].lower()
    return rows


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
    """Return aggregate metrics for the summary dashboard."""

    def _count(sql: str) -> int:
        row = _query_one(sql)
        return list(row.values())[0] if row else 0

    # -- Mesh counts by class (deduplicate by id since DataFusion can have
    # multiple rows for the same mesh id with different classes) --
    all_meshes = _query("SELECT id, class FROM meshes")
    _unique_meshes = _dedup_rows(all_meshes)
    total_meshes = len(_unique_meshes)
    host_meshes = sum(1 for m in _unique_meshes if m["class"] == "Host")
    proc_meshes = sum(1 for m in _unique_meshes if m["class"] == "Proc")
    actor_meshes = sum(1 for m in _unique_meshes if m["class"] not in ("Host", "Proc"))

    # -- Actor counts --
    total_actors = _count("SELECT COUNT(*) AS n FROM actors")

    # Count actors by latest status entirely in SQL — one (status, n) row per
    # status, not one row per actor. The inner GROUP BY collapses same-timestamp
    # ties to a single status per actor. Normalise to lowercase so both fake
    # data ("idle") and real DataFusion telemetry ("Idle") match.
    actor_status_rows = _query(
        "SELECT new_status, COUNT(*) AS n FROM ("
        "  SELECT actor_id, MIN(new_status) AS new_status"
        f"  FROM ({_LATEST_ACTOR_STATUS_SQL}) GROUP BY actor_id"
        " ) t GROUP BY new_status"
    )
    actor_by_status: dict[str, int] = {}
    for row in actor_status_rows:
        s = (row["new_status"] or "unknown").lower()
        actor_by_status[s] = actor_by_status.get(s, 0) + int(row["n"] or 0)

    # -- Message counts --
    total_messages = _count("SELECT COUNT(*) AS n FROM messages")

    # Count messages by latest status entirely in SQL. Pulling one row per
    # message here would stream the whole table back through the telemetry
    # scanner and re-record each batch as a `queued` event (a self-amplifying
    # loop); the aggregate returns only a handful of (status, n) rows. The inner
    # GROUP BY collapses same-timestamp ties to a single status per message.
    msg_status_rows = _query(
        "SELECT status, COUNT(*) AS n FROM ("
        "  SELECT message_id, MIN(status) AS status"
        f"  FROM ({_LATEST_MSG_STATUS_SQL}) GROUP BY message_id"
        " ) t GROUP BY status"
    )
    msg_by_status: dict[str, int] = {}
    for row in msg_status_rows:
        s = (row["status"] or "unknown").lower()
        msg_by_status[s] = msg_by_status.get(s, 0) + int(row["n"] or 0)

    msg_endpoint_rows = _query(
        "SELECT endpoint, COUNT(*) AS cnt FROM messages "
        "GROUP BY endpoint ORDER BY endpoint"
    )
    msg_by_endpoint = {
        (row["endpoint"] or "(none)"): row["cnt"] for row in msg_endpoint_rows
    }

    completed = msg_by_status.get("complete", 0)
    delivery_rate = (
        min(1.0, round(completed / total_messages, 3)) if total_messages > 0 else 0.0
    )

    # -- Error details --
    # Use LOWER() so both fake data ("failed") and real telemetry ("Failed") match.
    # Single query for both failed and stopped actors.
    _error_actor_sql = (
        "SELECT ase.actor_id, a.full_name, ase.reason, ase.timestamp_us, a.mesh_id,"
        " ase.new_status"
        " FROM actor_status_events ase"
        " JOIN actors a ON ase.actor_id = a.id"
        f" INNER JOIN ({_LATEST_ACTOR_STATUS_SQL}) latest"
        " ON ase.actor_id = latest.actor_id"
        "   AND ase.timestamp_us = latest.max_ts"
        " WHERE LOWER(ase.new_status) IN ('failed', 'stopped')"
        " ORDER BY ase.timestamp_us"
    )
    error_actors = _query(_error_actor_sql)
    failed_actors = []
    stopped_actors = []
    for r in error_actors:
        status = (r.pop("new_status", None) or "").lower()
        if status == "failed":
            failed_actors.append(r)
        elif status == "stopped":
            stopped_actors.append(r)

    # Hyperactor telemetry doesn't track message delivery failures.
    # Actor failures from undeliverable messages are already surfaced in
    # failed_actors above (the failure reason contains the delivery error).
    failed_messages = 0

    # -- Timeline (single query instead of four) --
    timeline_row = _query_one(
        "SELECT MIN(timestamp_us) AS start_us,"
        " MAX(timestamp_us) AS end_us,"
        " MIN(CASE WHEN LOWER(new_status) = 'failed'"
        "   THEN timestamp_us END) AS failure_onset_us,"
        " COUNT(*) AS total_status_events"
        " FROM actor_status_events"
    )
    start_us = timeline_row["start_us"] if timeline_row else 0
    end_us = timeline_row["end_us"] if timeline_row else 0
    failure_onset_us = (
        timeline_row["failure_onset_us"]
        if timeline_row and timeline_row["failure_onset_us"]
        else None
    )
    total_status_events = timeline_row["total_status_events"] if timeline_row else 0

    total_message_events = _count("SELECT COUNT(*) AS n FROM message_status_events")

    # -- Health score (0-100) --
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
        },
        "hierarchy_counts": {
            "host_meshes": host_meshes,
            "proc_meshes": proc_meshes,
            "actor_meshes": actor_meshes,
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


def get_message_stats() -> dict[str, Any]:
    """Consolidated message metrics for the overview + topology overlay.

    Computes the deduped handler lifecycle, per-endpoint volume, and the
    distinct actor->actor pairs in ONE server-side aggregate so the browser can
    read them from a single cached endpoint instead of issuing its own raw
    per-poll SQL. A raw client scan of ``message_status_events`` is streamed
    back through the telemetry scanner as one message per batch, and each of
    those is itself recorded as a ``queued`` event — so a browser that polls raw
    scans of that table inflates the very table it reads (a self-amplifying
    loop). Keeping the scan server-side and behind the route cache bounds it.
    """
    # Deduped handler lifecycle: one terminal/current state per message.
    lifecycle = {"queued": 0, "active": 0, "completed": 0, "failed": 0}
    _state_key = {"complete": "completed", "failed": "failed", "active": "active"}
    for row in _query(_MSG_LIFECYCLE_SQL):
        key = _state_key.get(str(row.get("state") or ""), "queued")
        lifecycle[key] = int(row.get("n") or 0)

    # Per-endpoint volume + completed count, deduped by message id.
    endpoint_rows = _query(
        "SELECT m.endpoint AS endpoint,"
        " COUNT(DISTINCT m.id) AS total,"
        " COUNT(DISTINCT CASE WHEN LOWER(e.status) = 'complete' THEN m.id END) AS completed"
        " FROM messages m"
        " LEFT JOIN message_status_events e ON e.message_id = m.id"
        " GROUP BY m.endpoint ORDER BY total DESC"
    )
    endpoints = [
        {
            "endpoint": row.get("endpoint") or "(none)",
            "total": int(row.get("total") or 0),
            "completed": int(row.get("completed") or 0),
        }
        for row in endpoint_rows
    ]

    # Distinct actor->actor pairs for the topology message overlay. CAST to
    # string to survive JS bigint precision (ids exceed Number.MAX_SAFE_INTEGER).
    pair_rows = _query(
        "SELECT DISTINCT CAST(from_actor_id AS VARCHAR) AS f,"
        " CAST(to_actor_id AS VARCHAR) AS t FROM messages"
    )
    pairs = [[str(row.get("f")), str(row.get("t"))] for row in pair_rows]

    return {"lifecycle": lifecycle, "endpoints": endpoints, "pairs": pairs}


def get_message_activity(num_buckets: int = 44) -> dict[str, Any]:
    """Message-throughput histogram + window, computed server-side.

    Returns the message time span, total count, and a fixed-size bucket
    histogram — a small, bounded result — so the activity panel never polls the
    full `messages` list. Streaming that list back through the telemetry scanner
    would re-record each batch as a `queued` event, inflating the very tables
    the dashboard reads (a self-amplifying loop).
    """
    span = _query_one(
        "SELECT MIN(timestamp_us) AS start_us,"
        " MAX(timestamp_us) AS end_us,"
        " COUNT(*) AS total FROM messages"
    )
    start = span["start_us"] if span and span["start_us"] is not None else 0
    end = span["end_us"] if span and span["end_us"] is not None else 0
    total = int(span["total"]) if span and span["total"] is not None else 0

    buckets = [0] * num_buckets
    if end > start and total > 0:
        width = end - start
        # Bucket index = floor((ts - start) * num_buckets / span). CAST guards
        # against engines that promote integer division to float. GROUP BY the
        # index so only ~num_buckets rows come back, never the raw rows.
        rows = _query(
            "SELECT b, COUNT(*) AS n FROM ("
            "  SELECT CAST((timestamp_us - ?) * ? / ? AS BIGINT) AS b FROM messages"
            " ) t GROUP BY b ORDER BY b",
            (start, num_buckets, width),
        )
        for row in rows:
            b = int(row["b"] or 0)
            b = min(num_buckets - 1, max(0, b))
            buckets[b] += int(row["n"] or 0)

    return {"start_us": start, "end_us": end, "total": total, "buckets": buckets}
