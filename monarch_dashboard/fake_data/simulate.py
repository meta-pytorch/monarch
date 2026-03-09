# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Continuous fake data simulator for the Monarch Dashboard.

Unlike ``generate.py`` which produces a static database with pre-computed
timestamps, this script writes data with **real wall-clock timestamps** so
the dashboard can display live-updating state.

At a configurable time (default ~4.5 minutes) the designated host mesh
terminates, demonstrating downward-only death propagation:
  host mesh stops → proc meshes stop → actor meshes stop → actors stop/fail

Usage:
    python fake_data/simulate.py [--db PATH] [--interval SECONDS] [--failure-at SECONDS]
"""

import argparse
import json
import os
import random
import signal
import sqlite3
import time
from pathlib import Path

from generate import _ACTOR_MESH_CLASSES, _insert_rows, ENDPOINTS, SCHEMA_SQL

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_us() -> int:
    """Current wall-clock time in microseconds."""
    return time.time_ns() // 1000


class _IdSeq:
    """Monotonically increasing ID generator, starting at 1."""

    def __init__(self) -> None:
        self._next = 1

    def __call__(self) -> int:
        val = self._next
        self._next += 1
        return val


# ---------------------------------------------------------------------------
# Hierarchy builder (real timestamps, deterministic topology)
# ---------------------------------------------------------------------------


def _build_hierarchy() -> dict:
    """Build the 2-table Monarch hierarchy using real timestamps.

    Deterministic topology matching generate.py:
      2 host meshes, 2 proc meshes each, 1 actor mesh each, 1 user actor each.
      Total: 10 meshes, 10 actors (2 HostAgent + 4 ProcAgent + 4 user).

    Returns a dict with all hierarchy rows plus bookkeeping structures
    needed by the simulation loop.
    """
    ts = _now_us()

    mesh_seq = _IdSeq()
    actor_seq = _IdSeq()

    meshes: list[dict] = []
    actors: list[dict] = []
    actor_to_host_mesh: dict[int, int] = {}

    failed_host_mesh_id: int | None = None
    host_trigger_id: int | None = None
    actor_trigger_id: int | None = None
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

        # Second host mesh is the failure target.
        if h_idx == 1:
            failed_host_mesh_id = host_mesh_id
            failed_host_name = host_full

        # HostAgent actor for this host mesh.
        hma_id = actor_seq()
        actors.append(
            {
                "id": hma_id,
                "timestamp_us": ts,
                "mesh_id": host_mesh_id,
                "rank": 0,
                "full_name": f"{host_full}/HostAgent[0]",
            }
        )
        actor_to_host_mesh[hma_id] = host_mesh_id

        if host_mesh_id == failed_host_mesh_id and host_trigger_id is None:
            host_trigger_id = hma_id

        # 2 proc meshes per host mesh.
        for pm_idx in range(2):
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

            # ProcAgent actor for this proc mesh.
            pma_id = actor_seq()
            actors.append(
                {
                    "id": pma_id,
                    "timestamp_us": ts,
                    "mesh_id": proc_mesh_id,
                    "rank": 0,
                    "full_name": f"{pm_full}/ProcAgent[0]",
                }
            )
            actor_to_host_mesh[pma_id] = host_mesh_id

            # 1 actor mesh per proc mesh.
            am_class = _ACTOR_MESH_CLASSES[pm_idx % len(_ACTOR_MESH_CLASSES)]
            actor_mesh_id = mesh_seq()
            am_full = f"{pm_full}/{am_class}"
            meshes.append(
                {
                    "id": actor_mesh_id,
                    "timestamp_us": ts,
                    "class": am_class,
                    "given_name": am_class,
                    "full_name": am_full,
                    "shape_json": json.dumps({"dims": [2]}),
                    "parent_mesh_id": proc_mesh_id,
                    "parent_view_json": json.dumps({"offset": [0], "sizes": [1]}),
                }
            )

            # 1 user actor per actor mesh.
            actor_type = am_class.replace("Python<", "PythonActor<")
            aid = actor_seq()
            actors.append(
                {
                    "id": aid,
                    "timestamp_us": ts,
                    "mesh_id": actor_mesh_id,
                    "rank": 0,
                    "full_name": f"{am_full}/{actor_type}[0]",
                }
            )
            actor_to_host_mesh[aid] = host_mesh_id

            if host_mesh_id == failed_host_mesh_id and actor_trigger_id is None:
                actor_trigger_id = aid

    assert failed_host_mesh_id is not None
    assert host_trigger_id is not None
    assert actor_trigger_id is not None

    return {
        "meshes": meshes,
        "actors": actors,
        "actor_to_host_mesh": actor_to_host_mesh,
        "failed_host_mesh_id": failed_host_mesh_id,
        "host_trigger_id": host_trigger_id,
        "actor_trigger_id": actor_trigger_id,
        "failed_host_name": failed_host_name,
    }


# ---------------------------------------------------------------------------
# Simulation loop
# ---------------------------------------------------------------------------


def _run_simulation(
    db_path: str,
    interval: float,
    failure_at: float,
    host_failure: bool = False,
) -> None:
    """Run the continuous simulation until interrupted.

    Failure modes:
      - Default: trigger actor hits CUDA OOM → status = "failed" (actor only)
      - --host-failure: additionally, all actors in the same host mesh get
        "stopping" then "stopped" events (downward propagation from host)
    """

    rng = random.Random()

    # -- Build hierarchy ------------------------------------------------
    hierarchy = _build_hierarchy()

    actors = hierarchy["actors"]
    actor_to_host = hierarchy["actor_to_host_mesh"]
    failed_host_id = hierarchy["failed_host_mesh_id"]
    failed_host_name = hierarchy["failed_host_name"]

    # Pick trigger: HostAgent for host failure, regular actor otherwise.
    trigger_actor_id = (
        hierarchy["host_trigger_id"] if host_failure else hierarchy["actor_trigger_id"]
    )

    actor_ids = [a["id"] for a in actors]

    # -- Open DB --------------------------------------------------------
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA_SQL)

    # Insert hierarchy rows.
    _insert_rows(conn, "meshes", hierarchy["meshes"])
    _insert_rows(conn, "actors", hierarchy["actors"])
    conn.commit()

    # -- ID sequences for event tables ----------------------------------
    event_seq = _IdSeq()
    msg_seq = _IdSeq()
    mse_seq = _IdSeq()
    sm_seq = _IdSeq()

    # -- Actor state tracking -------------------------------------------
    # Emit initial "created" -> "initializing" -> "idle" for every actor.
    init_events: list[dict] = []
    actor_state: dict[int, str] = {}

    ts_created = _now_us()
    for a in actors:
        aid = a["id"]
        init_events.append(
            {
                "id": event_seq(),
                "timestamp_us": ts_created,
                "actor_id": aid,
                "new_status": "created",
                "reason": None,
            }
        )

    ts_init = ts_created + 500_000  # +0.5 s
    for a in actors:
        aid = a["id"]
        init_events.append(
            {
                "id": event_seq(),
                "timestamp_us": ts_init,
                "actor_id": aid,
                "new_status": "initializing",
                "reason": None,
            }
        )

    ts_idle = ts_init + 1_000_000  # +1 s
    for a in actors:
        aid = a["id"]
        init_events.append(
            {
                "id": event_seq(),
                "timestamp_us": ts_idle,
                "actor_id": aid,
                "new_status": "idle",
                "reason": None,
            }
        )
        actor_state[aid] = "idle"

    _insert_rows(conn, "actor_status_events", init_events)
    conn.commit()

    n_actors_total = len(actor_ids)
    print(
        f"Simulator started: {n_actors_total} actors, "
        f"failure at {failure_at:.0f}s, tick every {interval:.1f}s"
    )
    print(f"Database: {os.path.abspath(db_path)}")

    # -- Graceful shutdown via SIGINT ------------------------------------
    shutting_down = False

    def _handle_sigint(sig: int, frame: object) -> None:
        nonlocal shutting_down
        shutting_down = True

    signal.signal(signal.SIGINT, _handle_sigint)

    # -- Main loop ------------------------------------------------------
    start_time = time.monotonic()
    tick = 0
    failure_triggered = False
    dead_actors: set[int] = set()

    try:
        while not shutting_down:
            tick += 1
            elapsed = time.monotonic() - start_time
            now = _now_us()

            new_events: list[dict] = []
            new_messages: list[dict] = []
            new_msg_events: list[dict] = []
            new_sent: list[dict] = []

            # -- Failure event ----------------------------------------------
            if not failure_triggered and elapsed >= failure_at:
                failure_triggered = True

                # 1. Trigger actor fails with CUDA OOM.
                new_events.append(
                    {
                        "id": event_seq(),
                        "timestamp_us": now,
                        "actor_id": trigger_actor_id,
                        "new_status": "failed",
                        "reason": "CUDA OOM",
                    }
                )
                actor_state[trigger_actor_id] = "failed"
                dead_actors.add(trigger_actor_id)

                # 2. If --host-failure: cascade downward through the host mesh.
                #    All siblings in the same host mesh → stopping → stopped.
                if host_failure:
                    for aid in actor_ids:
                        if aid == trigger_actor_id:
                            continue
                        if actor_to_host[aid] == failed_host_id:
                            new_events.append(
                                {
                                    "id": event_seq(),
                                    "timestamp_us": now + 100_000,
                                    "actor_id": aid,
                                    "new_status": "stopping",
                                    "reason": f"death propagation from {failed_host_name}",
                                }
                            )
                            new_events.append(
                                {
                                    "id": event_seq(),
                                    "timestamp_us": now + 500_000,
                                    "actor_id": aid,
                                    "new_status": "stopped",
                                    "reason": f"death propagation from {failed_host_name}",
                                }
                            )
                            actor_state[aid] = "stopped"
                            dead_actors.add(aid)

                mode = (
                    "HOST FAILURE (downward propagation)"
                    if host_failure
                    else "ACTOR FAILURE (single actor)"
                )
                print(f"  [tick {tick}] {mode} — {len(dead_actors)} actors dead")

            # -- Transition healthy actors ------------------------------
            live_actors = [a for a in actor_ids if a not in dead_actors]

            if live_actors:
                n_transitions = rng.randint(2, max(3, len(live_actors) // 3))
                to_transition = rng.sample(
                    live_actors, min(n_transitions, len(live_actors))
                )

                for aid in to_transition:
                    cur = actor_state[aid]
                    if cur == "idle":
                        new_status = "processing"
                    elif cur == "processing":
                        new_status = "idle"
                    else:
                        continue

                    actor_state[aid] = new_status
                    new_events.append(
                        {
                            "id": event_seq(),
                            "timestamp_us": now + rng.randint(0, 100_000),
                            "actor_id": aid,
                            "new_status": new_status,
                            "reason": None,
                        }
                    )

            # -- Generate messages between live actors ------------------
            if len(live_actors) >= 2:
                n_msgs = rng.randint(2, 4)
                for _ in range(n_msgs):
                    sender_id = rng.choice(live_actors)
                    receiver_id = rng.choice([a for a in live_actors if a != sender_id])
                    endpoint = rng.choice(ENDPOINTS)
                    mid = msg_seq()
                    ts_msg = now + rng.randint(0, 500_000)

                    final_status = rng.choices(
                        ["delivered", "failed"],
                        weights=[15, 1],
                    )[0]

                    sender_actor = next(a for a in actors if a["id"] == sender_id)

                    new_messages.append(
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

                    for step_idx, step_status in enumerate(
                        ["queued", "sent", final_status]
                    ):
                        new_msg_events.append(
                            {
                                "id": mse_seq(),
                                "timestamp_us": ts_msg + step_idx * 50_000,
                                "message_id": mid,
                                "status": step_status,
                            }
                        )

                    new_sent.append(
                        {
                            "id": sm_seq(),
                            "timestamp_us": ts_msg,
                            "sender_actor_id": sender_id,
                            "mesh_id": sender_actor["mesh_id"],
                            "view_json": '{"offset": [0], "sizes": [1]}',
                            "shape_json": '{"dims": [1]}',
                        }
                    )

            # -- Write to DB --------------------------------------------
            _insert_rows(conn, "actor_status_events", new_events)
            _insert_rows(conn, "messages", new_messages)
            _insert_rows(conn, "message_status_events", new_msg_events)
            _insert_rows(conn, "sent_messages", new_sent)
            conn.commit()

            status_summary = (
                f"live={len(live_actors)} dead={len(dead_actors)} "
                f"events={len(new_events)} msgs={len(new_messages)}"
            )
            print(f"  [tick {tick}] {elapsed:6.1f}s  {status_summary}")

            time.sleep(interval)

    finally:
        conn.close()
        print(f"\nSimulator stopped after {tick} ticks.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a continuous Monarch fake data simulator."
    )
    parser.add_argument(
        "--db",
        default=str(Path(__file__).parent / "fake_data.db"),
        help="SQLite database path (default: fake_data/fake_data.db)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between ticks (default: 1.0)",
    )
    parser.add_argument(
        "--failure-at",
        type=float,
        default=270.0,
        help="Seconds until failure event (default: 270 = 4.5 minutes)",
    )
    parser.add_argument(
        "--host-failure",
        action="store_true",
        help="Cascade failure to entire host mesh (downward propagation)",
    )
    args = parser.parse_args()
    _run_simulation(args.db, args.interval, args.failure_at, args.host_failure)


if __name__ == "__main__":
    main()
