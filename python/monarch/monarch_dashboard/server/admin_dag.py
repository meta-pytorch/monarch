# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Build a DAG from snapshot tables.

This mirrors the TUI's tree hierarchy (Root → Host → Proc → Actor)
rather than the telemetry SQL layer's 6-tier mesh hierarchy.  The
result is a simpler, more readable graph.

Topology comes from the snapshot tables (``nodes``, ``children``,
``host_nodes``, ``proc_nodes``, ``actor_nodes``), populated by
periodic snapshot capture.  Messages and telemetry actor IDs come
from the telemetry SQL tables (``messages``, ``actors``, ``meshes``).

System actors are filtered using the ``is_system`` flag from the
``actor_nodes`` / ``children`` snapshot tables, plus a name-based
heuristic for infrastructure actors that aren't flagged.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set

from . import db

logger = logging.getLogger(__name__)

# Known system/infrastructure actor name patterns.
# These actors are spawned by Monarch internals, not by user code.
_SYSTEM_NAME_PATTERNS = re.compile(
    r"(telemetry|setup[-_]|SetupActor|comm[-_]|CommActor|"
    r"logger[-_]|LoggerActor|log_client|MeshAdminAgent|HostAgent|ProcAgent|"
    r"host_agent|proc_agent|mesh_admin|controller_controller|"
    r"proc_mesh_controller|actor_mesh_controller)",
    re.IGNORECASE,
)


def _is_client_actor(ref: str) -> bool:
    """The root client actor (client[0]) is the user's entrypoint.

    It is marked ``is_system`` by the admin API and listed in
    ``system_children``, but it sends all user-visible messages
    (e.g. accumulate_gradients, get_status) to user actors. Keeping
    it in the DAG preserves message edge visibility.
    """
    return ref.endswith(",client[0]")


def _is_system_by_name(label: str) -> bool:
    """Heuristic: check if a node label looks like a system/infra actor."""
    return bool(_SYSTEM_NAME_PATTERNS.search(label))


def _derive_label(node_id: str, node_kind: str, addr: str = "") -> str:
    """Derive a short display label from a snapshot node.

    For hosts, uses the ``addr`` field so each host is visually
    distinguishable.  For procs and actors, extracts the name
    component from the node_id string.
    """
    if node_kind == "host" and addr:
        # Strip transport prefix (e.g. "tcp:" or "metatls:") -> IP:port
        if ":" in addr:
            addr = addr.split(":", 1)[1]
        return addr

    # node_id for actors/procs is the last comma-separated component
    return node_id.rsplit(",", 1)[-1] or node_id


def _extract_status(
    node_kind: str,
    actor_status: str = "",
    is_poisoned: bool = False,
    failed_actor_count: int = 0,
) -> str:
    """Derive a status string from snapshot node fields."""
    if node_kind == "actor" and actor_status:
        return actor_status.split(":")[0].strip().lower()
    if node_kind == "proc":
        if is_poisoned:
            return "failed"
        if failed_actor_count > 0:
            return "stopping"
        return "idle"
    if node_kind == "host":
        return "idle"
    return "n/a"


_LATEST_SNAPSHOT_SQL = (
    "SELECT snapshot_id FROM snapshots ORDER BY snapshot_ts DESC LIMIT 1"
)


def build_admin_dag(hide_system: bool = True) -> Dict[str, Any]:
    """Build a 4-tier DAG (Host → Proc → Actor) from snapshot tables.

    Returns ``{"nodes": [...], "edges": [...]}``.
    """
    try:
        snap_row = db._query_one(_LATEST_SNAPSHOT_SQL)
    except Exception:
        logger.debug("Could not query snapshots table", exc_info=True)
        return {"nodes": [], "edges": []}

    if not snap_row:
        return {"nodes": [], "edges": []}

    snap_id = snap_row["snapshot_id"]

    # Load all snapshot data for the latest snapshot in bulk.
    try:
        all_nodes = db._query(
            "SELECT node_id, node_kind FROM nodes WHERE snapshot_id = ?",
            (snap_id,),
        )
        all_children = db._query(
            "SELECT parent_id, child_id, is_system, child_sort_key"
            " FROM children WHERE snapshot_id = ?"
            " ORDER BY parent_id, child_sort_key",
            (snap_id,),
        )
        host_rows = db._query(
            "SELECT node_id, addr FROM host_nodes WHERE snapshot_id = ?",
            (snap_id,),
        )
        proc_rows = db._query(
            "SELECT node_id, proc_name, is_poisoned, failed_actor_count"
            " FROM proc_nodes WHERE snapshot_id = ?",
            (snap_id,),
        )
        actor_rows = db._query(
            "SELECT node_id, actor_status, is_system"
            " FROM actor_nodes WHERE snapshot_id = ?",
            (snap_id,),
        )
    except Exception:
        logger.debug("Could not load snapshot data", exc_info=True)
        return {"nodes": [], "edges": []}

    # Index snapshot data.
    node_kinds: Dict[str, str] = {n["node_id"]: n["node_kind"] for n in all_nodes}
    host_info: Dict[str, Dict] = {h["node_id"]: h for h in host_rows}
    proc_info: Dict[str, Dict] = {p["node_id"]: p for p in proc_rows}
    actor_info: Dict[str, Dict] = {a["node_id"]: a for a in actor_rows}

    # Build parent → children map and system_children set per parent.
    children_map: Dict[str, List[str]] = {}
    system_children_map: Dict[str, Set[str]] = {}
    for c in all_children:
        parent = c["parent_id"]
        child = c["child_id"]
        children_map.setdefault(parent, []).append(child)
        if c["is_system"]:
            system_children_map.setdefault(parent, set()).add(child)

    # BFS walk: Root → Host → Proc → Actor (same structure as old HTTP walk).
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    visited: Set[str] = set()
    ref_to_node_id: Dict[str, str] = {}
    controller_host_ref: Optional[str] = None

    queue: List[tuple] = [("root", None)]

    while queue:
        ref, parent_ref = queue.pop(0)
        if ref in visited:
            continue
        visited.add(ref)

        ntype = node_kinds.get(ref)
        if ntype is None:
            continue

        child_refs = children_map.get(ref, [])
        system_children = system_children_map.get(ref, set())

        # Skip root node itself — just enqueue its children.
        if ntype == "root":
            for child_ref in child_refs:
                if hide_system and child_ref in system_children:
                    continue
                queue.append((child_ref, None))
            continue

        # Derive label for this node.
        if ntype == "host":
            h = host_info.get(ref, {})
            label = _derive_label(ref, ntype, addr=h.get("addr", ""))
        else:
            label = _derive_label(ref, ntype)

        # Filter system actors — both snapshot is_system flag and name heuristic.
        # The root client actor is exempt.
        if hide_system and not _is_client_actor(ref):
            if ntype == "actor":
                a = actor_info.get(ref, {})
                if a.get("is_system", False):
                    continue
                if _is_system_by_name(label):
                    continue

        # Build node data.
        tier_map = {"host": "host", "proc": "proc", "actor": "actor"}
        tier = tier_map.get(ntype, "actor")

        if ntype == "actor":
            a = actor_info.get(ref, {})
            status = _extract_status(ntype, actor_status=a.get("actor_status", ""))
        elif ntype == "proc":
            p = proc_info.get(ref, {})
            status = _extract_status(
                ntype,
                is_poisoned=p.get("is_poisoned", False),
                failed_actor_count=p.get("failed_actor_count", 0),
            )
        else:
            status = _extract_status(ntype)

        node_id = f"{tier}-{ref}"
        ref_to_node_id[ref] = node_id

        node_data: Dict[str, Any] = {
            "id": node_id,
            "entity_id": ref,
            "tier": tier,
            "label": label,
            "subtitle": tier.capitalize(),
            "status": status,
        }
        nodes.append(node_data)

        if parent_ref is not None and parent_ref in ref_to_node_id:
            parent_node_id = ref_to_node_id[parent_ref]
            edges.append(
                {
                    "id": f"hier-{parent_node_id}-{node_id}",
                    "source_id": parent_node_id,
                    "target_id": node_id,
                    "type": "hierarchy",
                }
            )

        # Detect the controller host by looking for the root client
        # actor (client[0]) in proc children. Only the controller
        # process has this actor; worker procs do not.
        if ntype == "proc" and any(c.endswith(",client[0]") for c in child_refs):
            controller_host_ref = parent_ref

        # Enqueue children.
        for child_ref in child_refs:
            if (
                hide_system
                and child_ref in system_children
                and not _is_client_actor(child_ref)
            ):
                continue
            queue.append((child_ref, ref))

    # Tag the controller host with "(controller)" in its label.
    if controller_host_ref is not None:
        for n in nodes:
            if n["tier"] == "host" and n["entity_id"] == controller_host_ref:
                n["label"] += " (controller)"
                break

    # Resolve telemetry actor IDs so the detail panel can query messages.
    try:
        tel_actors = db._query("SELECT id, full_name FROM actors")
        if tel_actors:
            name_to_tel_id: Dict[str, int] = {
                a["full_name"]: a["id"] for a in tel_actors
            }
            for n in nodes:
                if n["tier"] == "actor":
                    tel_id = name_to_tel_id.get(n["entity_id"])
                    if tel_id is not None:
                        n["telemetry_actor_id"] = tel_id
    except Exception:
        logger.debug("Could not resolve telemetry actor IDs", exc_info=True)

    # Resolve mesh names (host mesh, proc mesh, actor mesh) from telemetry.
    _resolve_mesh_names(nodes)

    # Prune procs that have no actor children after system filtering.
    if hide_system:
        actor_node_ids = {n["id"] for n in nodes if n["tier"] == "actor"}
        procs_with_actors: Set[str] = set()
        for e in edges:
            if e["type"] == "hierarchy" and e["target_id"] in actor_node_ids:
                procs_with_actors.add(e["source_id"])
        proc_node_ids = {n["id"] for n in nodes if n["tier"] == "proc"}
        empty_procs = proc_node_ids - procs_with_actors
        if empty_procs:
            nodes = [n for n in nodes if n["id"] not in empty_procs]
            edges = [
                e
                for e in edges
                if e["source_id"] not in empty_procs
                and e["target_id"] not in empty_procs
            ]

    # Add message edges from telemetry.
    _add_message_edges(nodes, edges)

    return {"nodes": nodes, "edges": edges}


def _resolve_mesh_names(nodes: List[Dict[str, Any]]) -> None:
    try:
        rows = db._query(
            "SELECT a.full_name, m.full_name AS mesh_full_name"
            " FROM actors a"
            " LEFT JOIN meshes m ON a.mesh_id = m.id"
        )
        if not rows:
            return

        fname_to_mesh: Dict[str, str] = {}
        for r in rows:
            mname = r.get("mesh_full_name")
            if mname:
                fname_to_mesh[r["full_name"]] = mname

        for n in nodes:
            entity = n["entity_id"]
            bare = entity[5:] if entity.startswith("host:") else entity
            if bare in fname_to_mesh:
                n["mesh_name"] = fname_to_mesh[bare]
                continue
            agent_name = bare + ",proc_agent[0]"
            if agent_name in fname_to_mesh:
                n["mesh_name"] = fname_to_mesh[agent_name]

    except Exception:
        logger.debug("Could not resolve mesh names from telemetry", exc_info=True)


def _add_message_edges(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]
) -> None:
    """Overlay message edges from the telemetry SQL layer."""
    try:
        messages = db._query("SELECT DISTINCT from_actor_id, to_actor_id FROM messages")
        if not messages:
            return

        actors = db._query("SELECT id, full_name FROM actors")
        if not actors:
            return

        actor_id_to_name: Dict[int, str] = {a["id"]: a["full_name"] for a in actors}

        entity_to_node_id: Dict[str, str] = {}
        for n in nodes:
            if n["tier"] == "actor":
                entity_to_node_id[n["entity_id"]] = n["id"]

        def _match_node(telemetry_name: str) -> Optional[str]:
            return entity_to_node_id.get(telemetry_name)

        seen: Set[str] = set()
        for m in messages:
            src_name = actor_id_to_name.get(m["from_actor_id"], "")
            tgt_name = actor_id_to_name.get(m["to_actor_id"], "")

            src_node = _match_node(src_name)
            tgt_node = _match_node(tgt_name)

            if src_node and tgt_node and src_node != tgt_node:
                edge_key = f"{src_node}-{tgt_node}"
                if edge_key not in seen:
                    seen.add(edge_key)
                    edges.append(
                        {
                            "id": f"msg-{edge_key}",
                            "source_id": src_node,
                            "target_id": tgt_node,
                            "type": "message",
                        }
                    )
        logger.info(
            "message edge matching: %d messages, %d telemetry actors, "
            "%d DAG actor nodes, %d edges added",
            len(messages),
            len(actors),
            len(entity_to_node_id),
            len(seen),
        )
    except Exception as exc:
        logger.debug("Could not add message edges: %s", exc)
