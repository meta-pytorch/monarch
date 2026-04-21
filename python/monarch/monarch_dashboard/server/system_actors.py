# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Discover system actors by walking the Mesh Admin API.

The Mesh Admin API exposes ``system_children`` on Root/Host/Proc nodes
and ``is_system`` on Actor nodes.  This module walks the admin tree and
collects:

  1. System actor ``full_name`` strings (actors with ``is_system: true``)
  2. System proc/mesh names (references listed in ``system_children``)

The dashboard uses these to prune entire system subtrees from the DAG,
not just individual actors.  Results are cached with a TTL.
"""

import logging
import os
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional, Set

import requests

logger = logging.getLogger(__name__)


@dataclass
class SystemInfo:
    """Collected system actor/mesh classification from the admin API."""

    # Actor full_names that are system actors.
    actor_names: Set[str] = field(default_factory=set)
    # References (opaque strings) that appear in system_children.
    system_refs: Set[str] = field(default_factory=set)


# Cache.
_cache: SystemInfo = SystemInfo()
_cache_time: float = 0.0
_CACHE_TTL_SECS = 10.0


def _get_admin_url() -> Optional[str]:
    """Read the admin URL from the environment (set by the host application)."""
    return os.environ.get("MONARCH_ADMIN_URL")


def _walk_admin_tree(admin_url: str) -> SystemInfo:
    """Walk the admin API tree and collect system classification info."""
    from .admin_dag import configure_tls

    info = SystemInfo()
    session = requests.Session()
    configure_tls(session)
    visited: Set[str] = set()
    queue = ["root"]

    while queue:
        ref = queue.pop(0)
        if ref in visited:
            continue
        visited.add(ref)

        encoded = urllib.parse.quote(ref, safe="")
        try:
            resp = session.get(f"{admin_url}/v1/{encoded}", timeout=2.0)
            if resp.status_code != 200:
                continue
            payload = resp.json()
        except Exception:
            logger.debug("Failed to fetch node %s", ref, exc_info=True)
            continue

        props = payload.get("properties", {})
        children = payload.get("children", [])

        for variant_name, variant_data in props.items():
            if not isinstance(variant_data, dict):
                continue

            # Actor: check is_system flag.
            if variant_name == "Actor" and variant_data.get("is_system"):
                name = variant_data.get("full_name") or payload.get("identity", "")
                if name:
                    info.actor_names.add(name)

            # Root/Host/Proc: collect system_children refs.
            for sc in variant_data.get("system_children", []):
                info.system_refs.add(sc)

        # Queue children for traversal.
        for child_ref in children:
            queue.append(child_ref)

    # Also resolve system refs to get their actor names.
    for ref in info.system_refs:
        if ref in visited:
            continue
        encoded = urllib.parse.quote(ref, safe="")
        try:
            resp = session.get(f"{admin_url}/v1/{encoded}", timeout=2.0)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            props = payload.get("properties", {})
            actor_data = props.get("Actor", {})
            name = actor_data.get("full_name") or payload.get("identity", "")
            if name:
                info.actor_names.add(name)
        except Exception:
            logger.debug("Failed to resolve system ref %s", ref, exc_info=True)
            continue

    logger.debug(
        "Admin API: %d system actors, %d system refs",
        len(info.actor_names),
        len(info.system_refs),
    )
    return info


def get_system_info() -> SystemInfo:
    """Return system classification info, with caching.

    Returns an empty SystemInfo if the admin URL is not configured or
    unreachable.
    """
    global _cache, _cache_time

    now = time.monotonic()
    if now - _cache_time < _CACHE_TTL_SECS and (
        _cache.actor_names or _cache.system_refs
    ):
        return _cache

    admin_url = _get_admin_url()
    if not admin_url:
        return _cache

    try:
        info = _walk_admin_tree(admin_url)
        _cache = info
        _cache_time = now
    except Exception:
        logger.warning("Failed to walk admin API for system actors", exc_info=True)

    return _cache


def get_system_actor_names() -> Set[str]:
    """Convenience: return just the set of system actor full_names."""
    return get_system_info().actor_names
