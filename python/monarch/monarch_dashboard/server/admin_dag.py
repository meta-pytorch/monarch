# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Build a DAG by walking the Mesh Admin API.

This mirrors the TUI's tree hierarchy (Root → Host → Proc → Actor)
rather than the telemetry SQL layer's 6-tier mesh hierarchy.  The
result is a simpler, more readable graph.

System actors are filtered using the ``system_children`` list and
``is_system`` flag from the Admin API, plus a name-based heuristic
for infrastructure actors that aren't flagged (e.g. telemetry, setup).
"""

import logging
import os
import re
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from . import db

logger = logging.getLogger(__name__)

# Cache the built DAG to avoid re-walking on every poll.
_dag_cache: Optional[Dict[str, Any]] = None
_dag_cache_time: float = 0.0
_DAG_CACHE_TTL = 2.0  # seconds

# Max walk depth: Root(0) -> Host(1) -> Proc(2) -> Actor(3).
_MAX_TREE_DEPTH = 4

# Known system/infrastructure actor name patterns.
# These actors are spawned by Monarch internals, not by user code.
_SYSTEM_NAME_PATTERNS = re.compile(
    r"(telemetry|setup[-_]|SetupActor|comm[-_]|CommActor|"
    r"logger[-_]|LoggerActor|log_client|MeshAdminAgent|HostAgent|ProcAgent|"
    r"host_agent|proc_agent|mesh_admin|controller_controller|"
    r"proc_mesh_controller|actor_mesh_controller|client\[)",
    re.IGNORECASE,
)


def _get_admin_url() -> Optional[str]:
    return os.environ.get("MONARCH_ADMIN_URL")


def configure_tls(session: requests.Session) -> None:
    """Configure TLS client certs on a requests session.

    Mirrors the cert detection logic in Rust's ``try_tls_connector``
    (``hyperactor/src/channel/net.rs``):

    1. **OSS** — ``HYPERACTOR_TLS_CA``, ``HYPERACTOR_TLS_CERT``,
       ``HYPERACTOR_TLS_KEY`` environment variables.
    2. **Meta** — ``/var/facebook/rootcanal/ca.pem`` (CA),
       ``/var/facebook/x509_identities/server.pem`` (cert + key).
    3. **Fallback** — no TLS configuration (plain HTTP only).
    """
    # OSS: explicit env vars take priority.
    ca = os.environ.get("HYPERACTOR_TLS_CA")
    cert = os.environ.get("HYPERACTOR_TLS_CERT")
    key = os.environ.get("HYPERACTOR_TLS_KEY")
    if ca and os.path.isfile(ca):
        session.verify = ca
        if cert and key and os.path.isfile(cert) and os.path.isfile(key):
            session.cert = (cert, key)
        return

    # Meta: well-known certificate paths.
    meta_ca = "/var/facebook/rootcanal/ca.pem"
    meta_cert = "/var/facebook/x509_identities/server.pem"
    if os.path.isfile(meta_ca) and os.path.isfile(meta_cert):
        session.verify = meta_ca
        session.cert = (meta_cert, meta_cert)  # PEM contains both cert and key
        return

    # No certs found; HTTPS requests will fail and fall back to the
    # telemetry SQL layer in build_admin_dag.
    logger.debug("No TLS certs found for admin API; HTTPS will not work")


def _fetch_node(
    session: requests.Session, admin_url: str, ref: str
) -> Optional[Dict[str, Any]]:
    """Fetch a single node from the admin API."""
    encoded = urllib.parse.quote(ref, safe="")
    try:
        resp = session.get(f"{admin_url}/v1/{encoded}", timeout=2.0)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        logger.debug("Failed to fetch admin node %s", ref, exc_info=True)
    return None


def _extract_status(props: Dict[str, Any]) -> str:
    """Extract a status string from node properties.

    Actors have an explicit ``actor_status`` field.  Procs derive status
    from ``is_poisoned`` and ``failed_actor_count``.  Hosts derive status
    from their child procs (resolved later — default to "idle" here).
    """
    # Actor: explicit status.
    actor = props.get("Actor", {})
    if isinstance(actor, dict) and "actor_status" in actor:
        status = actor["actor_status"]
        if isinstance(status, str):
            return status.split(":")[0].strip().lower() if status else "unknown"

    # Proc: derive from health fields.
    proc = props.get("Proc", {})
    if isinstance(proc, dict):
        if proc.get("is_poisoned"):
            return "failed"
        if proc.get("failed_actor_count", 0) > 0:
            return "stopping"
        return "idle"

    # Host: healthy by default (no failure fields on Host).
    host = props.get("Host", {})
    if isinstance(host, dict):
        return "idle"

    return "n/a"


def _extract_is_system(props: Dict[str, Any]) -> bool:
    """Check if a node is a system actor via the API flag."""
    actor = props.get("Actor", {})
    return isinstance(actor, dict) and actor.get("is_system", False)


def _extract_system_children(props: Dict[str, Any]) -> Set[str]:
    """Get system_children refs from Root/Host/Proc properties."""
    result: Set[str] = set()
    for variant_data in props.values():
        if isinstance(variant_data, dict):
            for sc in variant_data.get("system_children", []):
                result.add(sc)
    return result


def _node_type(props: Dict[str, Any]) -> str:
    for key in ("Root", "Host", "Proc", "Actor", "Error"):
        if key in props:
            return key.lower()
    return "unknown"


def _is_system_by_name(label: str) -> bool:
    """Heuristic: check if a node label looks like a system/infra actor."""
    return bool(_SYSTEM_NAME_PATTERNS.search(label))


def _derive_label(payload: Dict[str, Any]) -> str:
    """Derive a short display label from a node payload."""
    identity = payload.get("identity", "")
    if "ActorId" in identity:
        inner = identity.split("(", 1)[-1].rstrip(")")
        parts = inner.split(",")
        if len(parts) >= 3:
            return f"{parts[1].strip()}[{parts[2].strip()}]"
        return inner
    if "ProcId" in identity:
        inner = identity.split("(", 1)[-1].rstrip(")")
        parts = inner.split(",")
        if len(parts) >= 2:
            return f"{parts[0].strip()}[{parts[1].strip()}]"
        return inner
    return identity.rsplit("/", 1)[-1].rsplit(",", 1)[-1] or identity


def build_admin_dag(hide_system: bool = True) -> Dict[str, Any]:
    """Walk the Admin API and build a DAG matching the TUI hierarchy.

    Returns ``{"nodes": [...], "edges": [...]}``.
    """
    global _dag_cache, _dag_cache_time

    admin_url = _get_admin_url()
    if not admin_url:
        return {"nodes": [], "edges": []}

    now = time.monotonic()
    cached = _dag_cache
    if cached is not None and now - _dag_cache_time < _DAG_CACHE_TTL:
        if cached.get("_hide_system") == hide_system:
            return cached

    session = requests.Session()
    configure_tls(session)
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    visited: Set[str] = set()
    ref_to_node_id: Dict[str, str] = {}

    queue: List[Tuple[str, Optional[str], int]] = [("root", None, 0)]

    while queue:
        ref, parent_ref, depth = queue.pop(0)
        if ref in visited:
            continue
        visited.add(ref)

        payload = _fetch_node(session, admin_url, ref)
        if payload is None:
            continue

        props = payload.get("properties", {})
        children_refs = payload.get("children", [])
        ntype = _node_type(props)

        # Skip root node itself.
        if ntype == "root":
            system_children = _extract_system_children(props) if hide_system else set()
            for child_ref in children_refs:
                if hide_system and child_ref in system_children:
                    continue
                queue.append((child_ref, None, depth))
            continue

        # Filter system actors — both API flag and name heuristic.
        label = _derive_label(payload)
        if hide_system:
            if _extract_is_system(props):
                continue
            if ntype == "actor" and _is_system_by_name(label):
                continue

        system_children = _extract_system_children(props) if hide_system else set()

        tier_map = {"host": "host", "proc": "proc", "actor": "actor"}
        tier = tier_map.get(ntype, "actor")
        status = _extract_status(props)

        node_id = f"{tier}-{ref}"
        ref_to_node_id[ref] = node_id

        nodes.append(
            {
                "id": node_id,
                "entity_id": ref,
                "tier": tier,
                "label": label,
                "subtitle": tier.capitalize(),
                "status": status,
            }
        )

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

        if depth < _MAX_TREE_DEPTH:
            for child_ref in children_refs:
                if hide_system and child_ref in system_children:
                    continue
                queue.append((child_ref, ref, depth + 1))

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

    # Prune procs that have no actor children after system filtering.
    if hide_system:
        actor_node_ids = {n["id"] for n in nodes if n["tier"] == "actor"}
        # Find which procs have at least one actor child via edges.
        procs_with_actors: Set[str] = set()
        for e in edges:
            if e["type"] == "hierarchy" and e["target_id"] in actor_node_ids:
                procs_with_actors.add(e["source_id"])
        # Remove procs with no actors and their edges.
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

    result = {"nodes": nodes, "edges": edges, "_hide_system": hide_system}
    _dag_cache = result
    _dag_cache_time = now
    return result


def _add_message_edges(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]
) -> None:
    """Overlay message edges from the telemetry SQL layer.

    Matching strategy: both the admin API references (entity_id) and
    the telemetry actor full_names use the same format:
        unix:@HASH,PROC_NAME,ACTOR_NAME[RANK]

    So we match telemetry full_name against admin entity_id directly.
    For each telemetry actor, if its full_name is a substring of (or
    equal to) an admin node's entity_id, that's a match.
    """
    try:
        messages = db._query("SELECT DISTINCT from_actor_id, to_actor_id FROM messages")
        if not messages:
            return

        actors = db._query("SELECT id, full_name FROM actors")
        if not actors:
            return

        # Map telemetry actor numeric ID -> full_name.
        actor_id_to_name: Dict[int, str] = {a["id"]: a["full_name"] for a in actors}

        # Build lookup: telemetry full_name -> admin node_id.
        # Admin entity_ids are the full reference strings which contain
        # the same actor identifier as the telemetry full_name.
        # Strategy: for each admin actor node, its entity_id IS the same
        # reference string as the telemetry full_name.
        entity_to_node_id: Dict[str, str] = {}
        for n in nodes:
            if n["tier"] == "actor":
                entity_to_node_id[n["entity_id"]] = n["id"]

        def _match_node(telemetry_name: str) -> Optional[str]:
            # Direct match: telemetry name == admin entity_id.
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
    except Exception as exc:
        logger.debug("Could not add message edges: %s", exc)
