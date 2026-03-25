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

import json
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


# ---------------------------------------------------------------------------
# ndslice Region → proc rank mapping
# ---------------------------------------------------------------------------


def _parse_region(
    parent_view_json: str | None,
) -> tuple[int, list[int], list[int]] | None:
    """Parse a serialized ndslice Region into (offset, sizes, strides).

    Accepts the real DataFusion format::

        {"labels": ["workers"], "slice": {"offset": 0, "sizes": [2], "strides": [1]}}

    Returns None if *parent_view_json* is null or unparseable.
    """
    if not parent_view_json:
        return None
    try:
        parsed = json.loads(parent_view_json)
    except (json.JSONDecodeError, TypeError):
        return None
    sl = parsed.get("slice")
    if not sl or "offset" not in sl or "sizes" not in sl or "strides" not in sl:
        return None
    return (sl["offset"], sl["sizes"], sl["strides"])


def _child_rank_to_parent_rank(
    child_rank: int,
    offset: int,
    sizes: list[int],
    strides: list[int],
) -> int:
    """Map a child mesh rank to a parent mesh rank via the parent Region.

    A child mesh is spawned on a view (Region) of the parent mesh.  Children
    are enumerated in row-major order over the Region.  Given the Region
    R = (offset, sizes, strides), child rank *r* maps to::

        parent_rank = offset + Σ_{k} i_k · strides_k

    where (i_0, ..., i_{d-1}) is the row-major decomposition of *r* over
    *sizes*.  O(d) per call where d = len(sizes).
    """
    parent_rank = offset
    remainder = child_rank
    for k in range(len(sizes)):
        suffix = 1
        for j in range(k + 1, len(sizes)):
            suffix *= sizes[j]
        i_k = (remainder // suffix) % sizes[k]
        parent_rank += i_k * strides[k]
        remainder %= suffix
    return parent_rank


def _parent_ranks_for_region(
    offset: int,
    sizes: list[int],
    strides: list[int],
) -> set[int]:
    """Return the set of all parent mesh ranks covered by a Region.  O(|R|)."""
    total = 1
    for s in sizes:
        total *= s
    return {_child_rank_to_parent_rank(r, offset, sizes, strides) for r in range(total)}


# Reusable SQL fragments for latest-status subqueries.

_LATEST_ACTOR_STATUS_SQL = (
    "SELECT ase.actor_id, ase.new_status, sub.max_ts"
    " FROM actor_status_events ase"
    " INNER JOIN ("
    "   SELECT actor_id, MAX(timestamp_us) AS max_ts"
    "   FROM actor_status_events GROUP BY actor_id"
    " ) sub ON ase.actor_id = sub.actor_id"
    "   AND ase.timestamp_us = sub.max_ts"
)

_LATEST_MSG_STATUS_SQL = (
    "SELECT mse.message_id, mse.status"
    " FROM message_status_events mse"
    " INNER JOIN ("
    "   SELECT message_id, MAX(timestamp_us) AS max_ts"
    "   FROM message_status_events GROUP BY message_id"
    " ) sub ON mse.message_id = sub.message_id"
    "   AND mse.timestamp_us = sub.max_ts"
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
    """Return all messages where the actor is sender or receiver, with latest status."""
    rows = _query(
        "SELECT m.*, latest.status AS latest_status"
        " FROM messages m"
        f" LEFT JOIN ({_LATEST_MSG_STATUS_SQL}) latest"
        " ON m.id = latest.message_id"
        " WHERE m.from_actor_id = ? OR m.to_actor_id = ?"
        " ORDER BY m.timestamp_us",
        (actor_id, actor_id),
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


def get_dag_data() -> dict[str, Any]:
    """Return classified nodes and edges for the DAG visualization.

    Fetches all meshes, actors (with latest status), and messages in a single
    connection, then builds the 6-tier graph structure server-side:

      host_mesh -> host_unit -> proc_mesh -> proc_unit -> actor_mesh -> actor

    Returns ``{"nodes": [...], "edges": [...]}``.
    """
    meshes = _query("SELECT * FROM meshes ORDER BY id")

    # Actors with latest status via JOIN.
    actors = _query(
        "SELECT a.*, latest.new_status AS latest_status"
        " FROM actors a"
        f" LEFT JOIN ({_LATEST_ACTOR_STATUS_SQL}) latest"
        " ON a.id = latest.actor_id"
        " ORDER BY a.id"
    )

    messages = _query("SELECT from_actor_id, to_actor_id FROM messages ORDER BY id")

    # -- Index meshes by id --
    mesh_by_id: dict[int, dict] = {m["id"]: m for m in meshes}

    # -- Classify meshes --
    host_meshes = [m for m in meshes if m["class"] == "Host"]
    proc_meshes = [m for m in meshes if m["class"] == "Proc"]
    actor_meshes = [m for m in meshes if m["class"] not in ("Host", "Proc")]

    # -- Classify actors by name pattern --
    host_agents_by_mesh: dict[int, list[dict]] = {}
    proc_agents_by_mesh: dict[int, list[dict]] = {}
    regular_actors: list[dict] = []

    for a in actors:
        name_lower = a["full_name"].lower()
        if "hostagent" in name_lower or "host_agent" in name_lower:
            host_agents_by_mesh.setdefault(a["mesh_id"], []).append(a)
        elif "procagent" in name_lower or "proc_agent" in name_lower:
            proc_agents_by_mesh.setdefault(a["mesh_id"], []).append(a)
        else:
            regular_actors.append(a)

    # -- Actor statuses --
    actor_statuses: dict[int, str] = {}
    for a in actors:
        actor_statuses[a["id"]] = (a.get("latest_status") or "unknown").lower()

    def _leaf_name(name: str) -> str:
        """Extract the last segment from a hierarchical name.

        Handles both fake data (``/`` separators) and real data (``,`` separators).
        """
        return name.rsplit("/", 1)[-1].rsplit(",", 1)[-1]

    # -- Build nodes --
    nodes: list[dict[str, Any]] = []

    for m in host_meshes:
        nodes.append(
            {
                "id": f"host_mesh-{m['id']}",
                "entity_id": m["id"],
                "tier": "host_mesh",
                "label": _leaf_name(m["given_name"]),
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
                    "label": _leaf_name(agent["full_name"]),
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
                "label": _leaf_name(m["given_name"]),
                "subtitle": "Proc Mesh",
                "status": "n/a",
            }
        )

    for pm in proc_meshes:
        for agent in proc_agents_by_mesh.get(pm["id"], []):
            nodes.append(
                {
                    "id": f"proc_unit-{agent['id']}",
                    "entity_id": agent["id"],
                    "tier": "proc_unit",
                    "label": _leaf_name(agent["full_name"]),
                    "subtitle": "Proc",
                    "status": actor_statuses.get(agent["id"], "unknown"),
                }
            )

    for m in actor_meshes:
        nodes.append(
            {
                "id": f"actor_mesh-{m['id']}",
                "entity_id": m["id"],
                "tier": "actor_mesh",
                "label": _leaf_name(m["given_name"]),
                "subtitle": "Actor Mesh",
                "status": "n/a",
            }
        )

    for a in regular_actors:
        nodes.append(
            {
                "id": f"actor-{a['id']}",
                "entity_id": a["id"],
                "tier": "actor",
                "label": _leaf_name(a["full_name"]),
                "subtitle": f"rank {a['rank']}",
                "status": actor_statuses.get(a["id"], "unknown"),
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
    # Use parent_view_json (ndslice Region) on the proc mesh to determine
    # which host ranks it covers, then connect only matching host agents.
    # Same pattern as proc_unit -> actor_mesh linking below.
    for pm in proc_meshes:
        if pm["parent_mesh_id"] is None:
            continue
        host_agents = host_agents_by_mesh.get(pm["parent_mesh_id"], [])
        region = _parse_region(pm.get("parent_view_json"))
        if region is not None and host_agents:
            covered_ranks = _parent_ranks_for_region(*region)
            matching = [a for a in host_agents if a.get("rank") in covered_ranks]
            targets = matching if matching else host_agents
        else:
            targets = host_agents
        for agent in targets:
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
    # Use parent_view_json (ndslice Region) to determine which proc ranks
    # the actor mesh spans, then connect only the matching proc agents.
    # Falls back to connecting all proc agents if parent_view_json is absent.
    for am in actor_meshes:
        if am["parent_mesh_id"] is None:
            continue
        proc_agents = proc_agents_by_mesh.get(am["parent_mesh_id"], [])
        region = _parse_region(am.get("parent_view_json"))
        if region is not None and proc_agents:
            covered_ranks = _parent_ranks_for_region(*region)
            matching = [a for a in proc_agents if a.get("rank") in covered_ranks]
            # Fall back to all agents if no rank matches (e.g. rank data missing).
            targets = matching if matching else proc_agents
        else:
            targets = proc_agents
        for agent in targets:
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

    # Latest status per actor — deduplicate in Python to handle cases where
    # multiple events share the same max timestamp.
    actor_latest_rows = _query(
        f"SELECT actor_id, new_status FROM ({_LATEST_ACTOR_STATUS_SQL})"
    )
    # Keep first occurrence per actor_id.  Normalise to lowercase so both
    # fake data ("idle") and real DataFusion telemetry ("Idle") match.
    actor_by_status: dict[str, int] = {}
    for row in _dedup_rows(actor_latest_rows, key="actor_id"):
        s = (row["new_status"] or "unknown").lower()
        actor_by_status[s] = actor_by_status.get(s, 0) + 1

    # -- Message counts --
    total_messages = _count("SELECT COUNT(*) AS n FROM messages")

    # Latest status per message — deduplicate in Python.
    msg_latest_rows = _query(
        f"SELECT message_id, status FROM ({_LATEST_MSG_STATUS_SQL})"
    )
    msg_by_status: dict[str, int] = {}
    for row in _dedup_rows(msg_latest_rows, key="message_id"):
        s = (row["status"] or "unknown").lower()
        msg_by_status[s] = msg_by_status.get(s, 0) + 1

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
    _error_actor_sql = (
        "SELECT ase.actor_id, a.full_name, ase.reason, ase.timestamp_us, a.mesh_id"
        " FROM actor_status_events ase"
        " JOIN actors a ON ase.actor_id = a.id"
        f" INNER JOIN ({_LATEST_ACTOR_STATUS_SQL}) latest"
        " ON ase.actor_id = latest.actor_id"
        "   AND ase.timestamp_us = latest.max_ts"
        " WHERE LOWER(ase.new_status) = ?"
        " ORDER BY ase.timestamp_us"
    )
    failed_actors = _query(_error_actor_sql, ("failed",))
    stopped_actors = _query(_error_actor_sql, ("stopped",))

    # Hyperactor telemetry doesn't track message delivery failures.
    # Actor failures from undeliverable messages are already surfaced in
    # failed_actors above (the failure reason contains the delivery error).
    failed_messages = 0

    # -- Timeline --
    time_range = _query_one(
        "SELECT MIN(timestamp_us) AS start_us, MAX(timestamp_us) AS end_us "
        "FROM actor_status_events"
    )
    start_us = time_range["start_us"] if time_range else 0
    end_us = time_range["end_us"] if time_range else 0

    failure_onset_row = _query_one(
        "SELECT MIN(timestamp_us) AS ts FROM actor_status_events "
        "WHERE LOWER(new_status) = 'failed'"
    )
    failure_onset_us = (
        failure_onset_row["ts"]
        if failure_onset_row and failure_onset_row["ts"]
        else None
    )

    total_status_events = _count("SELECT COUNT(*) AS n FROM actor_status_events")
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
