# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""SQL query layer for the Monarch Dashboard.

Provides read-only access to the fake (or real) SQLite telemetry database.
Each public function executes a single query and returns a list of row dicts.
All SQL is parameterised to prevent injection.

The data model uses two core tables:
  - meshes: all mesh types (Host, Proc, actor meshes) differentiated by class
  - actors: all actors including system agents (HostAgent, ProcAgent)
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


def list_meshes(
    class_filter: str | None = None,
    parent_mesh_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return meshes, optionally filtered by class and/or parent_mesh_id."""
    clauses: list[str] = []
    params: list[Any] = []
    if class_filter is not None:
        clauses.append("class = ?")
        params.append(class_filter)
    if parent_mesh_id is not None:
        clauses.append("parent_mesh_id = ?")
        params.append(parent_mesh_id)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return _query(f"SELECT * FROM meshes{where} ORDER BY id", tuple(params))


def get_mesh(mesh_id: int) -> dict[str, Any] | None:
    """Return a single mesh by id."""
    return _query_one("SELECT * FROM meshes WHERE id = ?", (mesh_id,))


def get_mesh_children(mesh_id: int) -> list[dict[str, Any]]:
    """Return child meshes of *mesh_id* (where parent_mesh_id = mesh_id)."""
    return _query(
        "SELECT * FROM meshes WHERE parent_mesh_id = ? ORDER BY id", (mesh_id,)
    )


# ---------------------------------------------------------------------------
# Actor queries
# ---------------------------------------------------------------------------


def list_actors(mesh_id: int | None = None) -> list[dict[str, Any]]:
    """Return all actors with latest_status and mesh_class, optionally filtered."""
    base = (
        "SELECT a.*, m.class AS mesh_class, "
        "m.given_name AS mesh_name, "
        "latest.new_status AS latest_status, "
        "latest.max_ts AS status_timestamp_us "
        "FROM actors a "
        "LEFT JOIN meshes m ON a.mesh_id = m.id "
        "LEFT JOIN ("
        "  SELECT ase.actor_id, ase.new_status, sub.max_ts "
        "  FROM actor_status_events ase "
        "  INNER JOIN ("
        "    SELECT actor_id, MAX(timestamp_us) AS max_ts "
        "    FROM actor_status_events GROUP BY actor_id"
        "  ) sub ON ase.actor_id = sub.actor_id "
        "    AND ase.timestamp_us = sub.max_ts"
        ") latest ON a.id = latest.actor_id"
    )
    if mesh_id is not None:
        return _query(f"{base} WHERE a.mesh_id = ? ORDER BY a.id", (mesh_id,))
    return _query(f"{base} ORDER BY a.id")


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


def get_dag_data() -> dict[str, Any]:
    """Return classified nodes and edges for the DAG visualization.

    Fetches all meshes, actors (with latest status), and messages in a single
    connection, then builds the 6-tier graph structure server-side:

      host_mesh -> host_unit -> proc_mesh -> proc_unit -> actor_mesh -> actor

    Status propagation: if a host_unit has a terminal status (failed/stopped/
    stopping), all proc_units and actors under that host inherit it.

    Returns ``{"nodes": [...], "edges": [...]}``.
    """
    conn = _connect()
    try:
        meshes = [
            dict(r) for r in conn.execute("SELECT * FROM meshes ORDER BY id").fetchall()
        ]

        # Actors with latest status via JOIN.
        actor_rows = conn.execute(
            "SELECT a.*, latest.new_status AS latest_status "
            "FROM actors a LEFT JOIN ("
            "  SELECT ase.actor_id, ase.new_status, sub.max_ts "
            "  FROM actor_status_events ase "
            "  INNER JOIN ("
            "    SELECT actor_id, MAX(timestamp_us) AS max_ts "
            "    FROM actor_status_events GROUP BY actor_id"
            "  ) sub ON ase.actor_id = sub.actor_id "
            "    AND ase.timestamp_us = sub.max_ts"
            ") latest ON a.id = latest.actor_id "
            "ORDER BY a.id"
        ).fetchall()
        actors = [dict(r) for r in actor_rows]

        messages = [
            dict(r)
            for r in conn.execute(
                "SELECT from_actor_id, to_actor_id FROM messages ORDER BY id"
            ).fetchall()
        ]
    finally:
        conn.close()

    # -- Classify meshes --
    host_meshes = [m for m in meshes if m["class"] == "Host"]
    proc_meshes = [m for m in meshes if m["class"] == "Proc"]
    actor_meshes = [m for m in meshes if m["class"] not in ("Host", "Proc")]

    # -- Classify actors by name pattern --
    host_agents_by_mesh: dict[int, list[dict]] = {}
    proc_agents_by_mesh: dict[int, list[dict]] = {}
    regular_actors: list[dict] = []

    for a in actors:
        if "HostAgent" in a["full_name"]:
            host_agents_by_mesh.setdefault(a["mesh_id"], []).append(a)
        elif "ProcAgent" in a["full_name"]:
            proc_agents_by_mesh.setdefault(a["mesh_id"], []).append(a)
        else:
            regular_actors.append(a)

    # -- Build parent -> children mesh map --
    mesh_children: dict[int, list[dict]] = {}
    for m in meshes:
        pid = m["parent_mesh_id"]
        if pid is not None:
            mesh_children.setdefault(pid, []).append(m)

    # -- Build mesh -> regular actors map --
    mesh_actors: dict[int, list[dict]] = {}
    for a in regular_actors:
        mesh_actors.setdefault(a["mesh_id"], []).append(a)

    # -- Actor statuses --
    actor_statuses: dict[int, str] = {}
    for a in actors:
        actor_statuses[a["id"]] = (a.get("latest_status") or "unknown").lower()

    # -- Terminal status propagation --
    terminal = {"stopped", "failed", "stopping"}

    def host_terminal_status(host_mesh_id: int) -> str | None:
        for agent in host_agents_by_mesh.get(host_mesh_id, []):
            s = actor_statuses.get(agent["id"], "unknown")
            if s in terminal:
                return s
        return None

    proc_to_host: dict[int, int] = {}
    for pm in proc_meshes:
        if pm["parent_mesh_id"] is not None:
            proc_to_host[pm["id"]] = pm["parent_mesh_id"]

    def _short(name: str) -> str:
        return name.rsplit("/", 1)[-1]

    # -- Build nodes --
    nodes: list[dict[str, Any]] = []

    for m in host_meshes:
        nodes.append(
            {
                "id": f"host_mesh-{m['id']}",
                "entity_id": m["id"],
                "tier": "host_mesh",
                "label": _short(m["given_name"]),
                "subtitle": "Host Mesh",
                "status": "n/a",
            }
        )

    for hm in host_meshes:
        for agent in host_agents_by_mesh.get(hm["id"], []):
            nodes.append(
                {
                    "id": f"host_unit-{agent['id']}",
                    "entity_id": agent["id"],
                    "tier": "host_unit",
                    "label": _short(agent["full_name"]),
                    "subtitle": "Host",
                    "status": actor_statuses.get(agent["id"], "unknown"),
                }
            )

    for m in proc_meshes:
        nodes.append(
            {
                "id": f"proc_mesh-{m['id']}",
                "entity_id": m["id"],
                "tier": "proc_mesh",
                "label": _short(m["given_name"]),
                "subtitle": "Proc Mesh",
                "status": "n/a",
            }
        )

    for pm in proc_meshes:
        host_id = proc_to_host.get(pm["id"])
        t_host = host_terminal_status(host_id) if host_id is not None else None
        for agent in proc_agents_by_mesh.get(pm["id"], []):
            own = actor_statuses.get(agent["id"], "unknown")
            nodes.append(
                {
                    "id": f"proc_unit-{agent['id']}",
                    "entity_id": agent["id"],
                    "tier": "proc_unit",
                    "label": _short(agent["full_name"]),
                    "subtitle": "Proc",
                    "status": t_host if t_host else own,
                }
            )

    for m in actor_meshes:
        nodes.append(
            {
                "id": f"actor_mesh-{m['id']}",
                "entity_id": m["id"],
                "tier": "actor_mesh",
                "label": _short(m["given_name"]),
                "subtitle": "Actor Mesh",
                "status": "n/a",
            }
        )

    for a in regular_actors:
        parent_mesh = next((m for m in meshes if m["id"] == a["mesh_id"]), None)
        parent_proc_id = parent_mesh["parent_mesh_id"] if parent_mesh else None
        host_id = (
            proc_to_host.get(parent_proc_id) if parent_proc_id is not None else None
        )
        t_host = host_terminal_status(host_id) if host_id is not None else None
        nodes.append(
            {
                "id": f"actor-{a['id']}",
                "entity_id": a["id"],
                "tier": "actor",
                "label": _short(a["full_name"]),
                "subtitle": f"rank {a['rank']}",
                "status": t_host if t_host else actor_statuses.get(a["id"], "unknown"),
            }
        )

    # -- Build edges --
    edges: list[dict[str, Any]] = []

    # Host mesh -> host unit
    for hm in host_meshes:
        for agent in host_agents_by_mesh.get(hm["id"], []):
            edges.append(
                {
                    "id": f"hier-host_mesh-{hm['id']}-host_unit-{agent['id']}",
                    "source_id": f"host_mesh-{hm['id']}",
                    "target_id": f"host_unit-{agent['id']}",
                    "type": "hierarchy",
                }
            )

    # Host unit -> proc mesh
    for pm in proc_meshes:
        if pm["parent_mesh_id"] is None:
            continue
        for agent in host_agents_by_mesh.get(pm["parent_mesh_id"], []):
            edges.append(
                {
                    "id": f"hier-host_unit-{agent['id']}-proc_mesh-{pm['id']}",
                    "source_id": f"host_unit-{agent['id']}",
                    "target_id": f"proc_mesh-{pm['id']}",
                    "type": "hierarchy",
                }
            )

    # Proc mesh -> proc unit
    for pm in proc_meshes:
        for agent in proc_agents_by_mesh.get(pm["id"], []):
            edges.append(
                {
                    "id": f"hier-proc_mesh-{pm['id']}-proc_unit-{agent['id']}",
                    "source_id": f"proc_mesh-{pm['id']}",
                    "target_id": f"proc_unit-{agent['id']}",
                    "type": "hierarchy",
                }
            )

    # Proc unit -> actor mesh
    for am in actor_meshes:
        if am["parent_mesh_id"] is None:
            continue
        for agent in proc_agents_by_mesh.get(am["parent_mesh_id"], []):
            edges.append(
                {
                    "id": f"hier-proc_unit-{agent['id']}-actor_mesh-{am['id']}",
                    "source_id": f"proc_unit-{agent['id']}",
                    "target_id": f"actor_mesh-{am['id']}",
                    "type": "hierarchy",
                }
            )

    # Actor mesh -> actor
    for a in regular_actors:
        edges.append(
            {
                "id": f"hier-actor_mesh-{a['mesh_id']}-actor-{a['id']}",
                "source_id": f"actor_mesh-{a['mesh_id']}",
                "target_id": f"actor-{a['id']}",
                "type": "hierarchy",
            }
        )

    # Message edges (deduplicated by actor pair).
    # Map actor_id -> node_id so messages reference the correct node prefix
    # (host_unit/proc_unit/actor rather than always "actor-").
    actor_node_id: dict[int, str] = {}
    for n in nodes:
        if n["tier"] in ("host_unit", "proc_unit", "actor"):
            actor_node_id[n["entity_id"]] = n["id"]

    seen: set[str] = set()
    for m in messages:
        src = actor_node_id.get(m["from_actor_id"])
        tgt = actor_node_id.get(m["to_actor_id"])
        if not src or not tgt:
            continue
        key = f"{m['from_actor_id']}-{m['to_actor_id']}"
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            {
                "id": f"msg-{m['from_actor_id']}-{m['to_actor_id']}",
                "source_id": src,
                "target_id": tgt,
                "type": "message",
            }
        )

    return {"nodes": nodes, "edges": edges}


def get_summary() -> dict[str, Any]:
    """Return aggregate metrics for the summary dashboard.

    Performs multiple aggregate queries in a single connection for efficiency.
    """
    conn = _connect()
    try:
        # -- Mesh counts by class --
        total_meshes = conn.execute("SELECT COUNT(*) FROM meshes").fetchone()[0]
        host_meshes = conn.execute(
            "SELECT COUNT(*) FROM meshes WHERE class = 'Host'"
        ).fetchone()[0]
        proc_meshes = conn.execute(
            "SELECT COUNT(*) FROM meshes WHERE class = 'Proc'"
        ).fetchone()[0]
        actor_meshes = conn.execute(
            "SELECT COUNT(*) FROM meshes WHERE class != 'Host' AND class != 'Proc'"
        ).fetchone()[0]

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
    finally:
        conn.close()
