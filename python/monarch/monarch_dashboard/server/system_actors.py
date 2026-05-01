# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Discover system actors from snapshot tables and name heuristics.

System actor classification uses two complementary filters:

  1. **Snapshot flag** — the ``actor_nodes`` table (populated by periodic
     snapshot capture) carries an ``is_system`` flag set by actors that
     call ``set_system()`` in their ``init()`` (e.g. proc_agent, comm,
     host_agent, mesh_admin).
  2. **Name heuristic** — a regex catches infrastructure actors that do
     *not* call ``set_system()`` but are still Monarch internals (e.g.
     telemetry, setup, controller_controller).

Both filters are applied to the telemetry ``actors`` table, and results
are unioned.  The root client actor (``client[0]``) is always excluded.

Results are cached with a short TTL to avoid redundant queries.
"""

import logging
import re
import time
from typing import Set

logger = logging.getLogger(__name__)

# Known system/infrastructure actor name patterns.
# Applied to the leaf name (last comma-separated component of full_name),
# matching the old admin_dag._derive_label behavior.
_SYSTEM_NAME_PATTERNS = re.compile(
    r"(telemetry|setup[-_]|SetupActor|comm[-_]|CommActor|"
    r"logger[-_]|LoggerActor|log_client|MeshAdminAgent|HostAgent|ProcAgent|"
    r"host_agent|proc_agent|mesh_admin|controller_controller|"
    r"proc_mesh_controller|actor_mesh_controller)",
    re.IGNORECASE,
)

_cache: Set[str] = set()
_cache_time: float = 0.0
_CACHE_TTL_SECS = 10.0

_LATEST_SNAPSHOT_SYSTEM_ACTORS_SQL = (
    "SELECT a.node_id FROM actor_nodes a"
    " INNER JOIN ("
    "   SELECT snapshot_id FROM snapshots ORDER BY snapshot_ts DESC LIMIT 1"
    " ) latest ON a.snapshot_id = latest.snapshot_id"
    " WHERE a.is_system = true"
)

_ALL_TELEMETRY_ACTORS_SQL = "SELECT full_name FROM actors"


def get_system_actor_names() -> Set[str]:
    """Return system actor names for DAG filtering.

    Combines two filters:
      1. ``is_system`` flag from snapshot ``actor_nodes`` table.
      2. Name heuristic (``_SYSTEM_NAME_PATTERNS``) applied to the leaf
         name of each actor in the telemetry ``actors`` table.

    The root client actor (``client[0]``) is excluded — it is the
    user's entrypoint and the source of all user-visible messages.

    Returns an empty set if tables are not yet populated or queries fail.
    """
    global _cache, _cache_time

    now = time.monotonic()
    if now - _cache_time < _CACHE_TTL_SECS and _cache:
        return _cache

    from . import db

    names: Set[str] = set()

    # Filter 1: actors with is_system=true from snapshot tables.
    try:
        rows = db._query(_LATEST_SNAPSHOT_SYSTEM_ACTORS_SQL)
        for r in rows:
            names.add(r["node_id"])
    except Exception:
        logger.debug("Could not query snapshot tables for system actors", exc_info=True)

    # Filter 2: name heuristic on telemetry actors table.
    try:
        rows = db._query(_ALL_TELEMETRY_ACTORS_SQL)
        for r in rows:
            full_name = r["full_name"]
            leaf = full_name.rsplit(",", 1)[-1]
            if _SYSTEM_NAME_PATTERNS.search(leaf):
                names.add(full_name)
    except Exception:
        logger.debug(
            "Could not query telemetry actors for name heuristic", exc_info=True
        )

    # TODO(matthewzhang): Replace the snapshot + name heuristic approach
    # with ``SELECT full_name FROM actors WHERE is_system = true`` once
    # the ``is_system`` column on the telemetry ``actors`` table
    # correctly classifies all system actors (including Python-spawned
    # ones like setup, telemetry, controller_controller). See D102645433.

    # Exclude the root client actor.
    names = {n for n in names if not n.endswith(",client[0]")}

    _cache = names
    _cache_time = now
    logger.debug("System actors: %d (snapshot + heuristic)", len(names))

    return _cache
