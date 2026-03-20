# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Deterministic fake data generator for the Monarch Dashboard.

Produces a SQLite database with realistic Monarch telemetry data spanning
a 5-minute window. The topology follows the real Monarch hierarchy:

    meshes table (all mesh types differentiated by class):
      Host meshes   (class="Host", parent_mesh_id=NULL)
        -> Proc meshes  (class="Proc", parent_mesh_id=host_mesh.id)
          -> Actor meshes (class="Python<Trainer>" etc., parent_mesh_id=proc_mesh.id)

    actors table (all actors including system agents):
      HostAgent  (mesh_id -> host mesh)
      ProcAgent  (mesh_id -> proc mesh)
      Regular actors (mesh_id -> actor mesh)

Sizing (deterministic):
  - 2 host meshes
  - 2 proc meshes per host mesh (4 total)
  - 1 actor mesh per proc mesh (4 total)
  - 1 HostAgent per host mesh (2 total)
  - 1 ProcAgent per proc mesh (4 total)
  - 1 user actor per actor mesh (4 total)
  - 10 actors total

The generated data exercises all dashboard features:
  - Full mesh hierarchy via parent_mesh_id
  - System actors (HostAgent, ProcAgent) and user actors
  - Complete ActorStatus lifecycle with failure at T=4:00
  - Non-sparse message traffic across multiple endpoints
  - Death propagation from a failed actor to its host mesh siblings

Usage:
    python generate.py [--output PATH]

The default output is fake_data.db in the same directory as this script.
"""

import argparse
import json
import os
import random
import sqlite3
from importlib.resources import files

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEED = 42
"""RNG seed -- keeps every run reproducible."""

BASE_TIMESTAMP_US = 1_700_000_000_000_000
"""Epoch anchor in microseconds (approx 2023-11-14). All timestamps are
relative offsets from this value."""

MINUTES_US = 60 * 1_000_000
"""One minute in microseconds."""

TIMELINE_DURATION_MINUTES = 5
"""Total simulated window length."""

FAILURE_MINUTE = 4
"""Minute at which the designated actor fails."""

ACTOR_STATUSES = [
    "unknown",
    "created",
    "initializing",
    "client",
    "idle",
    "processing",
    "saving",
    "loading",
    "stopping",
    "stopped",
    "failed",
]
"""All valid ActorStatus enum values (lowercase display form)."""

TERMINAL_STATUSES = {"stopped", "failed"}
"""Statuses where is_terminal() returns true."""

ENDPOINTS = [
    "train_step",
    "aggregate_gradients",
    "checkpoint",
    "broadcast_params",
    "sync_state",
]
"""Endpoint names used in message traffic."""

MESSAGE_STATUSES = ["queued", "active", "complete"]
"""Message lifecycle statuses emitted by hyperactor telemetry (mailbox.rs, proc.rs)."""

# ---------------------------------------------------------------------------
# Hierarchy naming pools
# ---------------------------------------------------------------------------

_ACTOR_MESH_CLASSES = [
    "Python<Trainer>",
    "Python<DataLoader>",
    "Python<Aggregator>",
]

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meshes (
    id              INTEGER PRIMARY KEY,
    timestamp_us    INTEGER NOT NULL,
    class           TEXT    NOT NULL,
    given_name      TEXT    NOT NULL,
    full_name       TEXT    NOT NULL,
    shape_json      TEXT    NOT NULL,
    parent_mesh_id  INTEGER,
    parent_view_json TEXT,
    FOREIGN KEY (parent_mesh_id) REFERENCES meshes(id)
);

CREATE TABLE IF NOT EXISTS actors (
    id              INTEGER PRIMARY KEY,
    timestamp_us    INTEGER NOT NULL,
    mesh_id         INTEGER NOT NULL,
    rank            INTEGER NOT NULL,
    full_name       TEXT    NOT NULL,
    display_name    TEXT,
    FOREIGN KEY (mesh_id) REFERENCES meshes(id)
);

CREATE TABLE IF NOT EXISTS actor_status_events (
    id              INTEGER PRIMARY KEY,
    timestamp_us    INTEGER NOT NULL,
    actor_id        INTEGER NOT NULL,
    new_status      TEXT    NOT NULL,
    reason          TEXT,
    FOREIGN KEY (actor_id) REFERENCES actors(id)
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY,
    timestamp_us    INTEGER NOT NULL,
    from_actor_id   INTEGER NOT NULL,
    to_actor_id     INTEGER NOT NULL,
    endpoint        TEXT,
    port_id         INTEGER,
    FOREIGN KEY (from_actor_id) REFERENCES actors(id),
    FOREIGN KEY (to_actor_id)   REFERENCES actors(id)
);

CREATE TABLE IF NOT EXISTS message_status_events (
    id              INTEGER PRIMARY KEY,
    timestamp_us    INTEGER NOT NULL,
    message_id      INTEGER NOT NULL,
    status          TEXT    NOT NULL,
    FOREIGN KEY (message_id) REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS sent_messages (
    id              INTEGER PRIMARY KEY,
    timestamp_us    INTEGER NOT NULL,
    sender_actor_id INTEGER NOT NULL,
    actor_mesh_id   INTEGER NOT NULL,
    view_json       TEXT    NOT NULL,
    shape_json      TEXT    NOT NULL,
    FOREIGN KEY (sender_actor_id) REFERENCES actors(id),
    FOREIGN KEY (actor_mesh_id)   REFERENCES meshes(id)
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts(minute: float, offset_us: int = 0) -> int:
    """Return an absolute timestamp for *minute* into the simulation."""
    return BASE_TIMESTAMP_US + int(minute * MINUTES_US) + offset_us


class _IdSeq:
    """Monotonically increasing ID generator, starting at 1."""

    def __init__(self) -> None:
        self._next = 1

    def __call__(self) -> int:
        val = self._next
        self._next += 1
        return val


# ---------------------------------------------------------------------------
# Hierarchy generation
# ---------------------------------------------------------------------------


def _generate_hierarchy() -> tuple[
    list[dict],
    list[dict],
    dict[int, int],
    int,
    int,
    str,
]:
    """Build the 2-table Monarch hierarchy (meshes + actors).

    Returns:
        (meshes, actors, actor_to_host_mesh, failed_host_mesh_id,
         trigger_actor_id, failed_host_name)
    """
    ts = _ts(0)

    mesh_seq = _IdSeq()
    actor_seq = _IdSeq()

    meshes: list[dict] = []
    actors: list[dict] = []
    # Maps actor_id -> host_mesh_id for death propagation
    actor_to_host_mesh: dict[int, int] = {}

    failed_host_mesh_id: int | None = None
    trigger_actor_id: int | None = None
    failed_host_name: str = ""

    for h_idx in range(2):
        host_mesh_id = mesh_seq()
        host_given = f"host_mesh_{h_idx}"
        host_full = host_given
        meshes.append(
            {
                "id": host_mesh_id,
                "timestamp_us": ts,
                "class": "Host",
                "given_name": host_given,
                "full_name": host_full,
                "shape_json": json.dumps({"dims": [1]}),
                "parent_mesh_id": None,
                "parent_view_json": None,
            }
        )

        # Designate the second host mesh as failing.
        if h_idx == 1:
            failed_host_mesh_id = host_mesh_id
            failed_host_name = host_full

        # Create HostAgent actor for this host mesh.
        hma_id = actor_seq()
        hma_full = f"{host_full}/HostAgent[0]"
        actors.append(
            {
                "id": hma_id,
                "timestamp_us": ts,
                "mesh_id": host_mesh_id,
                "rank": 0,
                "full_name": hma_full,
            }
        )
        actor_to_host_mesh[hma_id] = host_mesh_id

        # First actor in failing host mesh is the trigger.
        if host_mesh_id == failed_host_mesh_id and trigger_actor_id is None:
            trigger_actor_id = hma_id

        # Proc meshes under this host mesh (deterministic: 2 per host).
        n_proc_meshes = 2
        for pm_idx in range(n_proc_meshes):
            proc_mesh_id = mesh_seq()
            pm_given = f"proc_mesh_{pm_idx}"
            pm_full = f"{host_full}/{pm_given}"
            meshes.append(
                {
                    "id": proc_mesh_id,
                    "timestamp_us": ts,
                    "class": "Proc",
                    "given_name": pm_given,
                    "full_name": pm_full,
                    "shape_json": json.dumps({"dims": [1]}),
                    "parent_mesh_id": host_mesh_id,
                    "parent_view_json": json.dumps({"offset": [0], "sizes": [1]}),
                }
            )

            # Create ProcAgent actor for this proc mesh.
            pma_id = actor_seq()
            pma_full = f"{pm_full}/ProcAgent[0]"
            actors.append(
                {
                    "id": pma_id,
                    "timestamp_us": ts,
                    "mesh_id": proc_mesh_id,
                    "rank": 0,
                    "full_name": pma_full,
                }
            )
            actor_to_host_mesh[pma_id] = host_mesh_id

            if host_mesh_id == failed_host_mesh_id and trigger_actor_id is None:
                trigger_actor_id = pma_id

            # Actor meshes under this proc mesh (deterministic: 1 per proc).
            n_actor_meshes = 1
            for _am_idx in range(n_actor_meshes):
                am_class = _ACTOR_MESH_CLASSES[pm_idx % len(_ACTOR_MESH_CLASSES)]
                actor_mesh_id = mesh_seq()
                am_given = am_class
                am_full = f"{pm_full}/{am_class}"
                meshes.append(
                    {
                        "id": actor_mesh_id,
                        "timestamp_us": ts,
                        "class": am_class,
                        "given_name": am_given,
                        "full_name": am_full,
                        "shape_json": json.dumps({"dims": [2]}),
                        "parent_mesh_id": proc_mesh_id,
                        "parent_view_json": json.dumps({"offset": [0], "sizes": [1]}),
                    }
                )

                # Regular actors in this actor mesh (deterministic: 1 per mesh).
                n_actors = 1
                actor_type = am_class.replace("Python<", "PythonActor<")
                for rank in range(n_actors):
                    aid = actor_seq()
                    actor_full = f"{am_full}/{actor_type}[{rank}]"
                    actors.append(
                        {
                            "id": aid,
                            "timestamp_us": ts,
                            "mesh_id": actor_mesh_id,
                            "rank": rank,
                            "full_name": actor_full,
                        }
                    )
                    actor_to_host_mesh[aid] = host_mesh_id

                    if host_mesh_id == failed_host_mesh_id and trigger_actor_id is None:
                        trigger_actor_id = aid

    assert failed_host_mesh_id is not None
    assert trigger_actor_id is not None

    return (
        meshes,
        actors,
        actor_to_host_mesh,
        failed_host_mesh_id,
        trigger_actor_id,
        failed_host_name,
    )


# ---------------------------------------------------------------------------
# Status event generation
# ---------------------------------------------------------------------------


def _generate_status_events(
    actors: list[dict],
    actor_to_host_mesh: dict[int, int],
    failed_host_mesh_id: int,
    trigger_actor_id: int,
    failed_host_name: str,
    rng: random.Random,
) -> list[dict]:
    """Produce actor_status_events covering the full 5-min timeline.

    Lifecycle per actor:
      T=0:00  created
      T=0:05  initializing
      T=0:15  idle
      T=0:20-3:50  cycle idle/processing (with occasional saving/loading)
      T=4:00  failure actor -> failed; others in same host mesh -> stopping
      T=4:10  remaining host mesh actors -> stopped
      Actors in other host meshes continue idle/processing until T=5:00

    Every valid ActorStatus value is used at least once across the full set.
    """
    next_id = _IdSeq()
    events: list[dict] = []

    # Track which special statuses we still need to emit.
    needed = {"client", "unknown", "saving", "loading"}

    for idx, actor in enumerate(actors):
        host_mesh_id = actor_to_host_mesh[actor["id"]]
        in_failed_host = host_mesh_id == failed_host_mesh_id
        is_trigger = actor["id"] == trigger_actor_id

        # --- Early lifecycle (all actors) ---
        events.append(
            {
                "id": next_id(),
                "timestamp_us": _ts(0, offset_us=idx * 100),
                "actor_id": actor["id"],
                "new_status": "created",
                "reason": None,
            }
        )
        events.append(
            {
                "id": next_id(),
                "timestamp_us": _ts(0.083, offset_us=idx * 100),  # ~5 s
                "actor_id": actor["id"],
                "new_status": "initializing",
                "reason": None,
            }
        )
        events.append(
            {
                "id": next_id(),
                "timestamp_us": _ts(0.25, offset_us=idx * 100),  # ~15 s
                "actor_id": actor["id"],
                "new_status": "idle",
                "reason": None,
            }
        )

        # --- Inject rare statuses on specific actors ---
        if "client" in needed and idx == 0:
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(0.30),
                    "actor_id": actor["id"],
                    "new_status": "client",
                    "reason": "client-managed mailbox",
                }
            )
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(0.32),
                    "actor_id": actor["id"],
                    "new_status": "idle",
                    "reason": None,
                }
            )
            needed.discard("client")

        if "unknown" in needed and idx == 1:
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(0.30),
                    "actor_id": actor["id"],
                    "new_status": "unknown",
                    "reason": "status probe timeout",
                }
            )
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(0.32),
                    "actor_id": actor["id"],
                    "new_status": "idle",
                    "reason": None,
                }
            )
            needed.discard("unknown")

        # --- Steady-state cycling T=0:20 - T=3:50 ---
        t = 0.333  # ~20 s in minutes
        cycle_statuses = ["idle", "processing"]
        cycle_idx = 0

        # Inject saving/loading once each on designated actors.
        inject_saving = "saving" in needed and idx == 2
        inject_loading = "loading" in needed and idx == 3
        saving_done = False
        loading_done = False

        while t < 3.833:  # ~3:50
            status = cycle_statuses[cycle_idx % 2]
            cycle_idx += 1

            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(t, offset_us=idx * 50),
                    "actor_id": actor["id"],
                    "new_status": status,
                    "reason": None,
                }
            )

            # Inject saving after first processing block.
            if inject_saving and not saving_done and status == "processing":
                t += rng.uniform(0.05, 0.1)
                events.append(
                    {
                        "id": next_id(),
                        "timestamp_us": _ts(t),
                        "actor_id": actor["id"],
                        "new_status": "saving",
                        "reason": "periodic checkpoint",
                    }
                )
                t += 0.03
                events.append(
                    {
                        "id": next_id(),
                        "timestamp_us": _ts(t),
                        "actor_id": actor["id"],
                        "new_status": "idle",
                        "reason": None,
                    }
                )
                saving_done = True
                needed.discard("saving")

            if inject_loading and not loading_done and status == "idle" and t > 1.0:
                t += rng.uniform(0.05, 0.1)
                events.append(
                    {
                        "id": next_id(),
                        "timestamp_us": _ts(t),
                        "actor_id": actor["id"],
                        "new_status": "loading",
                        "reason": "restoring from checkpoint",
                    }
                )
                t += 0.03
                events.append(
                    {
                        "id": next_id(),
                        "timestamp_us": _ts(t),
                        "actor_id": actor["id"],
                        "new_status": "idle",
                        "reason": None,
                    }
                )
                loading_done = True
                needed.discard("loading")

            t += rng.uniform(0.15, 0.35)

        # --- Failure sequence at T=4:00 (failing host mesh only) ---
        if is_trigger:
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(4.0),
                    "actor_id": actor["id"],
                    "new_status": "failed",
                    "reason": "CUDA OOM",
                }
            )
        elif in_failed_host:
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(4.0, offset_us=5_000_000),  # T=4:05
                    "actor_id": actor["id"],
                    "new_status": "stopping",
                    "reason": f"death propagation from {failed_host_name}",
                }
            )
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(4.167),  # T=4:10
                    "actor_id": actor["id"],
                    "new_status": "stopped",
                    "reason": f"death propagation from {failed_host_name}",
                }
            )
        else:
            # Actors in healthy host meshes keep running.
            t_end = 4.0
            while t_end < 5.0:
                status = cycle_statuses[cycle_idx % 2]
                cycle_idx += 1
                events.append(
                    {
                        "id": next_id(),
                        "timestamp_us": _ts(t_end, offset_us=idx * 50),
                        "actor_id": actor["id"],
                        "new_status": status,
                        "reason": None,
                    }
                )
                t_end += rng.uniform(0.15, 0.35)

    return events


# ---------------------------------------------------------------------------
# Message generation
# ---------------------------------------------------------------------------


def _generate_messages(
    actors: list[dict],
    rng: random.Random,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Generate messages, message_status_events, and sent_messages.

    Produces non-sparse traffic: each communicating actor pair has multiple
    messages across different endpoints.  Messages flow both within the same
    mesh and across meshes.

    Returns (messages, message_status_events, sent_messages).
    """
    msg_id = _IdSeq()
    mse_id = _IdSeq()
    sm_id = _IdSeq()

    messages: list[dict] = []
    msg_status_events: list[dict] = []
    sent_messages: list[dict] = []

    actors_by_id = {a["id"]: a for a in actors}
    actor_ids = [a["id"] for a in actors]

    t = 0.333  # Start messages at ~20 s
    while t < 3.833:
        # Each time step: pick several random (sender, receiver) pairs.
        n_msgs = rng.randint(3, 6)
        for _ in range(n_msgs):
            sender_id = rng.choice(actor_ids)
            receiver_id = rng.choice([a for a in actor_ids if a != sender_id])
            endpoint = rng.choice(ENDPOINTS)

            mid = msg_id()
            ts_msg = _ts(t, offset_us=rng.randint(0, 500_000))

            final_status = "complete"

            messages.append(
                {
                    "id": mid,
                    "timestamp_us": ts_msg,
                    "from_actor_id": sender_id,
                    "to_actor_id": receiver_id,
                    "endpoint": endpoint,
                    "port_id": rng.randint(1, 100),
                }
            )

            # Message status events: queued -> active -> complete.
            for step_idx, step_status in enumerate(["queued", "active", final_status]):
                msg_status_events.append(
                    {
                        "id": mse_id(),
                        "timestamp_us": ts_msg + step_idx * 50_000,
                        "message_id": mid,
                        "status": step_status,
                    }
                )

            # Sent message record.
            sender_actor = actors_by_id[sender_id]
            sent_messages.append(
                {
                    "id": sm_id(),
                    "timestamp_us": ts_msg,
                    "sender_actor_id": sender_id,
                    "actor_mesh_id": sender_actor["mesh_id"],
                    "view_json": '{"offset": [0], "sizes": [1]}',
                    "shape_json": '{"dims": [1]}',
                }
            )

        t += rng.uniform(0.1, 0.25)

    return messages, msg_status_events, sent_messages


# ---------------------------------------------------------------------------
# Database writer
# ---------------------------------------------------------------------------


def _insert_rows(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict],
) -> None:
    """Bulk-insert *rows* into *table* using the dict keys as columns."""
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
    conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows])


def generate_fake_data(db_path: str | None = None) -> str:
    """Generate the fake SQLite database and return the file path.

    Args:
        db_path: Destination file path.  Defaults to ``fake_data.db`` in the
            same directory as this script.

    Returns:
        Absolute path to the generated database file.
    """
    if db_path is None:
        db_path = str(files("monarch.monarch_dashboard.fake_data") / "fake_data.db")

    rng = random.Random(SEED)

    # Build the full hierarchy.
    (
        meshes,
        actors,
        actor_to_host_mesh,
        failed_host_mesh_id,
        trigger_actor_id,
        failed_host_name,
    ) = _generate_hierarchy()

    # Generate events.
    status_events = _generate_status_events(
        actors,
        actor_to_host_mesh,
        failed_host_mesh_id,
        trigger_actor_id,
        failed_host_name,
        rng,
    )
    messages, msg_status_events, sent_messages = _generate_messages(actors, rng)

    # Write to SQLite.
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        _insert_rows(conn, "meshes", meshes)
        _insert_rows(conn, "actors", actors)
        _insert_rows(conn, "actor_status_events", status_events)
        _insert_rows(conn, "messages", messages)
        _insert_rows(conn, "message_status_events", msg_status_events)
        _insert_rows(conn, "sent_messages", sent_messages)
        conn.commit()
    finally:
        conn.close()

    return os.path.abspath(db_path)


# Backward compatibility alias.
generate = generate_fake_data


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate fake Monarch telemetry data."
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output SQLite file path (default: fake_data.db beside this script)",
    )
    args = parser.parse_args()
    path = generate_fake_data(args.output)
    print(f"Generated fake data: {path}")


if __name__ == "__main__":
    main()
