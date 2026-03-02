# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Deterministic fake data generator for the Monarch Dashboard.

Produces a SQLite database with realistic Monarch telemetry data spanning
a 5-minute window. The topology includes 2 host meshes, 2 procs per host,
and 4 actors per host mesh (1 system ProcMeshAgent + 1 user actor per proc).

The generated data exercises all dashboard features:
  - Full mesh hierarchy (host -> proc -> actor meshes)
  - System actors (HostMeshAgent, ProcMeshAgent) and user actors
  - Complete ActorStatus lifecycle with failure at T=4:00
  - Non-sparse message traffic across multiple endpoints
  - Death propagation from a failed actor to its mesh siblings

Usage:
    python generate.py [--output PATH]

The default output is fake_data.db in the same directory as this script.
"""

import argparse
import os
import random
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEED = 42
"""RNG seed — keeps every run reproducible."""

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

MESSAGE_STATUSES = ["queued", "sent", "delivered", "failed"]
"""Possible message lifecycle statuses."""

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
    status          TEXT    NOT NULL,
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
# Mesh generation
# ---------------------------------------------------------------------------


def _generate_meshes() -> list[dict]:
    """Build the full mesh hierarchy.

    Returns a list of row dicts ready for INSERT.  The topology is:

        host_mesh_0  (id=1)
          proc_mesh_0_0  (id=3)   ->  actor_mesh_0_0  (id=7)
          proc_mesh_0_1  (id=4)   ->  actor_mesh_0_1  (id=8)
        host_mesh_1  (id=2)
          proc_mesh_1_0  (id=5)   ->  actor_mesh_1_0  (id=9)
          proc_mesh_1_1  (id=6)   ->  actor_mesh_1_1  (id=10)
    """
    ts = _ts(0)
    meshes: list[dict] = []

    # Host meshes (ids 1–2)
    for h in range(2):
        meshes.append(
            {
                "id": h + 1,
                "timestamp_us": ts,
                "class": "Host",
                "given_name": f"host_mesh_{h}",
                "full_name": f"/host_mesh_{h}",
                "shape_json": '{"dims": [2]}',
                "parent_mesh_id": None,
                "parent_view_json": None,
            }
        )

    # Proc meshes (ids 3–6)
    proc_id = 3
    for h in range(2):
        for p in range(2):
            meshes.append(
                {
                    "id": proc_id,
                    "timestamp_us": ts,
                    "class": "Proc",
                    "given_name": f"proc_mesh_{h}_{p}",
                    "full_name": f"/host_mesh_{h}/proc_mesh_{h}_{p}",
                    "shape_json": '{"dims": [1]}',
                    "parent_mesh_id": h + 1,
                    "parent_view_json": f'{{"offset": [{p}], "sizes": [1]}}',
                }
            )
            proc_id += 1

    # Actor meshes (ids 7–10), one per proc mesh
    actor_mesh_id = 7
    proc_id = 3
    for h in range(2):
        for p in range(2):
            meshes.append(
                {
                    "id": actor_mesh_id,
                    "timestamp_us": ts,
                    "class": "Python<Trainer>",
                    "given_name": f"actor_mesh_{h}_{p}",
                    "full_name": (
                        f"/host_mesh_{h}/proc_mesh_{h}_{p}/actor_mesh_{h}_{p}"
                    ),
                    "shape_json": '{"dims": [1]}',
                    "parent_mesh_id": proc_id,
                    "parent_view_json": '{"offset": [0], "sizes": [1]}',
                }
            )
            actor_mesh_id += 1
            proc_id += 1

    return meshes


# ---------------------------------------------------------------------------
# Actor generation
# ---------------------------------------------------------------------------


def _generate_actors(meshes: list[dict]) -> list[dict]:
    """Create actors for each mesh.

    For each host mesh: 1 HostMeshAgent (rank 0).
    For each proc mesh: 1 ProcMeshAgent (rank 0).
    For each actor mesh: 1 PythonActor<Trainer> user actor (rank 0).

    This yields 2 + 4 + 4 = 10 actors total.
    """
    ts = _ts(0)
    next_id = _IdSeq()
    actors: list[dict] = []

    mesh_by_id = {m["id"]: m for m in meshes}

    for m in meshes:
        cls = m["class"]
        if cls == "Host":
            actors.append(
                {
                    "id": next_id(),
                    "timestamp_us": ts,
                    "mesh_id": m["id"],
                    "rank": 0,
                    "full_name": f"{m['full_name']}/HostMeshAgent[0]",
                }
            )
        elif cls == "Proc":
            actors.append(
                {
                    "id": next_id(),
                    "timestamp_us": ts,
                    "mesh_id": m["id"],
                    "rank": 0,
                    "full_name": f"{m['full_name']}/ProcMeshAgent[0]",
                }
            )
        else:
            # User actor mesh
            actors.append(
                {
                    "id": next_id(),
                    "timestamp_us": ts,
                    "mesh_id": m["id"],
                    "rank": 0,
                    "full_name": f"{m['full_name']}/PythonActor<Trainer>[0]",
                }
            )

    return actors


# ---------------------------------------------------------------------------
# Status event generation
# ---------------------------------------------------------------------------


def _is_in_failed_host(actor: dict, meshes_by_id: dict) -> bool:
    """Return True if the actor belongs to host_mesh_1 (the failing mesh)."""
    mesh = meshes_by_id[actor["mesh_id"]]
    # Walk up to the host mesh.
    while mesh["parent_mesh_id"] is not None:
        mesh = meshes_by_id[mesh["parent_mesh_id"]]
    return mesh["id"] == 2  # host_mesh_1


def _is_failure_actor(actor: dict, meshes_by_id: dict) -> bool:
    """The actor in proc_mesh_1_1's actor mesh is the one that fails first."""
    mesh = meshes_by_id[actor["mesh_id"]]
    return mesh["given_name"] == "actor_mesh_1_1"


def _generate_status_events(
    actors: list[dict],
    meshes: list[dict],
    rng: random.Random,
) -> list[dict]:
    """Produce actor_status_events covering the full 5-min timeline.

    Lifecycle per actor:
      T=0:00  created
      T=0:05  initializing
      T=0:15  idle
      T=0:20–3:50  cycle idle/processing (with occasional saving/loading)
      T=4:00  failure actor -> failed; others in host_mesh_1 -> stopping
      T=4:10  remaining host_mesh_1 actors -> stopped
      host_mesh_0 actors continue idle/processing until T=5:00

    Every valid ActorStatus value is used at least once across the full set.
    """
    next_id = _IdSeq()
    events: list[dict] = []
    meshes_by_id = {m["id"]: m for m in meshes}

    # Track which special statuses we still need to emit.
    needed = {"client", "unknown", "saving", "loading"}

    for idx, actor in enumerate(actors):
        in_failed_host = _is_in_failed_host(actor, meshes_by_id)
        is_trigger = _is_failure_actor(actor, meshes_by_id)

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

        # --- Steady-state cycling T=0:20 – T=3:50 ---
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

        # --- Failure sequence at T=4:00 (host_mesh_1 only) ---
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
                    "reason": "death propagation from proc_mesh_1_1",
                }
            )
            events.append(
                {
                    "id": next_id(),
                    "timestamp_us": _ts(4.167),  # T=4:10
                    "actor_id": actor["id"],
                    "new_status": "stopped",
                    "reason": "death propagation from proc_mesh_1_1",
                }
            )
        else:
            # host_mesh_0 actors keep running.
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
    meshes: list[dict],
    rng: random.Random,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Generate messages, message_status_events, and sent_messages.

    Produces non-sparse traffic: each communicating actor pair has multiple
    messages across different endpoints.  Messages flow both within the same
    proc mesh (local) and across proc meshes (remote).

    Returns (messages, message_status_events, sent_messages).
    """
    msg_id = _IdSeq()
    mse_id = _IdSeq()
    sm_id = _IdSeq()

    messages: list[dict] = []
    msg_status_events: list[dict] = []
    sent_messages: list[dict] = []

    meshes_by_id = {m["id"]: m for m in meshes}

    # Build actor pairs. We generate messages between every ordered pair of
    # actors that belong to actor meshes (user actors) or proc meshes (system).
    # For simplicity, every actor can message every other actor.
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

            final_status = rng.choices(
                ["delivered", "delivered", "delivered", "failed"],
                weights=[5, 5, 5, 1],
            )[0]

            messages.append(
                {
                    "id": mid,
                    "timestamp_us": ts_msg,
                    "from_actor_id": sender_id,
                    "to_actor_id": receiver_id,
                    "status": final_status,
                    "endpoint": endpoint,
                    "port_id": rng.randint(1, 100),
                }
            )

            # Message status events: queued -> sent -> delivered (or failed).
            for step_idx, step_status in enumerate(["queued", "sent", final_status]):
                msg_status_events.append(
                    {
                        "id": mse_id(),
                        "timestamp_us": ts_msg + step_idx * 50_000,
                        "message_id": mid,
                        "status": step_status,
                    }
                )

            # Sent message record.
            sender_actor = next(a for a in actors if a["id"] == sender_id)
            sender_mesh = meshes_by_id[sender_actor["mesh_id"]]
            sent_messages.append(
                {
                    "id": sm_id(),
                    "timestamp_us": ts_msg,
                    "sender_actor_id": sender_id,
                    "actor_mesh_id": sender_actor["mesh_id"],
                    "view_json": '{"offset": [0], "sizes": [1]}',
                    "shape_json": sender_mesh["shape_json"],
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


def generate(db_path: str | None = None) -> str:
    """Generate the fake SQLite database and return the file path.

    Args:
        db_path: Destination file path.  Defaults to ``fake_data.db`` in the
            same directory as this script.

    Returns:
        Absolute path to the generated database file.
    """
    if db_path is None:
        db_path = str(Path(__file__).parent / "fake_data.db")

    rng = random.Random(SEED)

    # Build all data sets.
    meshes = _generate_meshes()
    actors = _generate_actors(meshes)
    status_events = _generate_status_events(actors, meshes, rng)
    messages, msg_status_events, sent_messages = _generate_messages(actors, meshes, rng)

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
    path = generate(args.output)
    print(f"Generated fake data: {path}")


if __name__ == "__main__":
    main()
