# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Execution history, pagination and fleet isolation regression coverage."""

import sqlite3
import time
from pathlib import Path

import pytest
from monarch._src.job.execution_history import execution_schemas
from monarch.monarch_dashboard.fake_data.generate import generate
from monarch.monarch_dashboard.server import db
from monarch.monarch_dashboard.server.app import create_app


@pytest.fixture
def history(tmp_path):
    path = tmp_path / "dashboard.db"
    generate(str(path))
    with sqlite3.connect(path) as conn:
        for table, schema in execution_schemas().items():
            columns = ",".join(
                f"{field.name} {'TEXT' if str(field.type) == 'string' else 'INTEGER'}"
                for field in schema
            )
            conn.execute(f"CREATE TABLE {table} ({columns})")
    app = create_app(db.SQLiteAdapter(str(path)))
    app.testing = True
    return path, app.test_client()


def insert(path: Path, table: str, row: dict):
    if table == "execution_rank_results":
        row = {
            "flat_rank": None,
            "coordinates": None,
            "host_rank": None,
            **row,
        }
    with sqlite3.connect(path) as conn:
        conn.execute(
            f"INSERT INTO {table} ({','.join(row)}) VALUES ({','.join('?' for _ in row)})",
            tuple(row.values()),
        )


def execution(path, number, status="succeeded", timestamp=None, command="ls", code=0):
    identifier = f"{number:032x}"
    timestamp = timestamp if timestamp is not None else time.time_ns() // 1000
    insert(
        path,
        "executions",
        {"id": identifier, "timestamp_us": timestamp, "command": command},
    )
    insert(
        path,
        "execution_status_events",
        {
            "execution_id": identifier,
            "timestamp_us": timestamp + 1,
            "status": status,
            "returncode": code if status != "running" else None,
        },
    )
    return identifier


def own_actor(path, actor_id, execution_id):
    """Give exactly one fixture actor an explicit execution proc parent."""
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO meshes(id,timestamp_us,class,given_name,full_name,shape_json,parent_mesh_id) VALUES(100000,0,'Proc',?,?,'{}',1)",
            (f"exec_{execution_id}", f"exec_{execution_id}"),
        )
        conn.execute(
            "INSERT INTO meshes(id,timestamp_us,class,given_name,full_name,shape_json,parent_mesh_id) VALUES(100001,0,'BashActor','BashActor','BashActor','{}',100000)"
        )
        conn.execute("UPDATE actors SET mesh_id=100001 WHERE id=?", (actor_id,))


def test_failure_survives_successful_ls_and_independent_running_job(history):
    path, client = history
    failed = execution(path, 1, "failed", command="python train.py", code=7)
    success = execution(path, 2, command="ls")
    active = execution(path, 3, "running", command="python distributed.py")
    result = client.get("/api/executions").get_json()
    assert result["counts"] == {"failed": 1, "running": 1, "succeeded": 1}
    assert result["retained_failures"] == 1
    assert result["needs_attention"] == 1
    assert [x["id"] for x in result["active"]] == [active]
    assert {x["id"] for x in result["history"]} == {failed, success}
    assert next(x for x in result["history"] if x["id"] == failed)["returncode"] == 7


def test_pagination_does_not_drop_equal_timestamp_commands(history):
    path, client = history
    timestamp = time.time_ns() // 1000
    identifiers = {execution(path, i, timestamp=timestamp) for i in range(27)}
    observed = []
    url = "/api/executions?limit=5"
    while url:
        page = client.get(url).get_json()
        observed.extend(x["id"] for x in page["history"])
        url = (
            f"/api/executions?limit=5&before={page['next_before']}"
            if page["next_before"]
            else None
        )
    assert set(observed) == identifiers
    assert len(observed) == len(identifiers)


@pytest.mark.parametrize("missing_table", list(execution_schemas()))
def test_missing_command_table_disables_history_and_details(history, missing_table):
    path, client = history
    identifier = execution(path, 1)
    with sqlite3.connect(path) as conn:
        conn.execute(f"DROP TABLE {missing_table}")
    response = client.get("/api/executions")
    assert response.status_code == 200
    assert response.get_json()["available"] is False
    assert client.get(f"/api/executions/{identifier}").status_code == 404


def test_search_treats_sql_and_wildcards_as_command_text(history):
    path, client = history
    target = execution(path, 1, command="printf '100%_done'; select * from actors")
    execution(path, 2, command="printf 100Xdone")
    assert [
        x["id"]
        for x in client.get(
            "/api/executions", query_string={"search": "100%_done"}
        ).get_json()["history"]
    ] == [target]
    assert not client.get(
        "/api/executions", query_string={"search": "' OR 1=1 --"}
    ).get_json()["history"]


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=101",
        "limit=no",
        "before=12",
        "before=1:bad",
        "before=9999999999999999999:" + "a" * 32,
        "status=running",
    ],
)
def test_invalid_pagination_is_rejected(history, query):
    assert history[1].get("/api/executions?" + query).status_code == 400


def test_lost_heartbeat_is_unknown_and_terminal_outcome_wins_timestamp_tie(history):
    path, client = history
    stale = execution(path, 1, "running", timestamp=time.time_ns() // 1000 - 50_000_000)
    finished = execution(path, 2, "running")
    with sqlite3.connect(path) as conn:
        timestamp = conn.execute(
            "SELECT timestamp_us FROM execution_status_events WHERE execution_id=?",
            (finished,),
        ).fetchone()[0]
    insert(
        path,
        "execution_status_events",
        {
            "execution_id": finished,
            "timestamp_us": timestamp,
            "status": "succeeded",
            "returncode": 0,
        },
    )
    results = {x["id"]: x for x in client.get("/api/executions").get_json()["history"]}
    assert results[stale]["status"] == "unknown"
    assert "may still be running" in results[stale]["error"]
    assert results[finished]["status"] == "succeeded"


def test_large_active_list_and_rank_details_are_bounded(history):
    path, client = history
    for i in range(120):
        execution(path, i, "running")
    page = client.get("/api/executions").get_json()
    assert page["counts"]["running"] == 120
    assert len(page["active"]) == 100
    identifier = page["active"][0]["id"]
    for i in range(205):
        insert(
            path,
            "execution_rank_results",
            {
                "id": f"{i:032x}",
                "execution_id": identifier,
                "timestamp_us": i,
                "mesh": "workers",
                "rank": f"{i:03}",
                "returncode": 7 if i == 204 else 0,
            },
        )
    detail = client.get("/api/executions/" + identifier).get_json()
    assert detail["rank_count"] == 205
    assert len(detail["ranks"]) == 100
    assert "stdout" not in detail["ranks"][0]
    last = client.get("/api/executions/" + identifier + "?rank_offset=200").get_json()
    assert len(last["ranks"]) == 5
    assert last["ranks"][-1]["returncode"] == 7


def test_unavailable_history_is_explicit(tmp_path):
    path = tmp_path / "old.db"
    generate(str(path))
    client = create_app(db.SQLiteAdapter(str(path))).test_client()
    assert client.get("/api/executions").get_json()["available"] is False
    assert client.get("/api/executions/missing").status_code == 404


@pytest.mark.parametrize("expired", [False, True])
def test_missing_or_expired_metadata_preserves_failed_actor_in_fleet(history, expired):
    path, _ = history
    actors = [a for a in db.list_actors() if a["is_system"] is not True]
    identifier = f"{1:032x}"
    own_actor(path, actors[0]["id"], identifier)
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM actor_status_events")
    insert(
        path,
        "actor_status_events",
        {
            "id": 1,
            "timestamp_us": 1,
            "actor_id": actors[0]["id"],
            "new_status": "Failed",
            "reason": "unreported command actor failure",
        },
    )
    insert(
        path,
        "actor_status_events",
        {"id": 2, "timestamp_us": 2, "actor_id": actors[1]["id"], "new_status": "Idle"},
    )
    if expired:
        execution(path, 1, "failed", code=7)
        assert (
            next(a for a in db.list_actors() if a["id"] == actors[0]["id"])[
                "execution_id"
            ]
            == identifier
        )
        with sqlite3.connect(path) as conn:
            conn.execute("DELETE FROM executions")
    summary = db.get_summary()
    assert summary["health_status"] == "unhealthy"
    assert len(summary["errors"]["failed_actors"]) == 1
    assert (
        next(a for a in db.list_actors() if a["id"] == actors[0]["id"])["execution_id"]
        is None
    )


@pytest.mark.parametrize(
    "identifier,suffix",
    [("a" * 32, ""), ("a" * 32, "-0"), ("a" * 32, "-15"), ("g" * 32, "-15")],
)
@pytest.mark.parametrize("status", ["Failed", "Stopped"])
def test_execution_actor_history_is_separate_from_fleet(
    history, identifier, suffix, status
):
    path, _ = history
    actors = [a for a in db.list_actors() if a["is_system"] is not True]
    if identifier != "g" * 32:
        insert(
            path, "executions", {"id": identifier, "timestamp_us": 1, "command": "test"}
        )
        own_actor(path, actors[0]["id"], identifier)
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM actor_status_events")
        conn.execute(
            "UPDATE actors SET full_name=? WHERE id=?",
            (
                f"bashactor<actor>.exec_{identifier}{suffix}<proc>@tcp://worker",
                actors[0]["id"],
            ),
        )
    insert(
        path,
        "actor_status_events",
        {
            "id": 1,
            "timestamp_us": 1,
            "actor_id": actors[0]["id"],
            "new_status": status,
            "reason": "exec ended",
        },
    )
    insert(
        path,
        "actor_status_events",
        {"id": 2, "timestamp_us": 2, "actor_id": actors[1]["id"], "new_status": "Idle"},
    )
    if identifier == "g" * 32:
        summary = db.get_summary()
        assert summary["health_status"] == (
            "unhealthy" if status == "Failed" else "unknown"
        )
        error_key = "failed_actors" if status == "Failed" else "stopped_actors"
        assert len(summary["errors"][error_key]) == 1
        assert (
            next(a for a in db.list_actors() if a["id"] == actors[0]["id"])[
                "execution_id"
            ]
            is None
        )
        return
    summary = db.get_summary()
    assert summary["health_status"] == "unknown"
    assert summary["health_coverage"]["unknown"] == len(actors) - 2
    assert summary["errors"]["failed_actors"] == []
    assert summary["errors"]["stopped_actors"] == []
    assert (
        next(a for a in db.list_actors() if a["id"] == actors[0]["id"])["execution_id"]
        == identifier
    )
    assert (
        db.list_actor_status_events(actor_id=actors[0]["id"])[0]["new_status"] == status
    )
    insert(
        path,
        "actor_status_events",
        {
            "id": 3,
            "timestamp_us": 3,
            "actor_id": actors[1]["id"],
            "new_status": "Failed",
            "reason": "live worker failed",
        },
    )
    assert db.get_summary()["health_status"] == "unhealthy"
    assert len(db.get_summary()["errors"]["failed_actors"]) == 1


@pytest.mark.parametrize(
    "outcome,code", [("succeeded", 0), ("failed", 7), ("interrupted", 130)]
)
def test_known_outcome_survives_late_running_and_unknown(history, outcome, code):
    path, client = history
    now = time.time_ns() // 1000
    identifier = execution(path, 1, outcome, timestamp=now - 100, code=code)
    for status, timestamp in [
        ("running", now),
        ("unknown", now + 1),
    ]:
        insert(
            path,
            "execution_status_events",
            {
                "execution_id": identifier,
                "timestamp_us": timestamp,
                "status": status,
                "error": "outcome unavailable",
            },
        )
    page = client.get("/api/executions").get_json()
    assert page["counts"] == {outcome: 1}
    assert not page["active"]
    assert page["history"][0]["status"] == outcome
    detail = client.get("/api/executions/" + identifier).get_json()
    assert detail["status"] == outcome
    assert detail["returncode"] == code
    assert detail["finished_us"] == now - 99
    assert detail["error"] is None


@pytest.mark.parametrize("status", ["succeeded", "running"])
def test_owned_runtime_failure_remains_visible_with_any_command_outcome(
    history, status
):
    path, client = history
    identifier = execution(path, 1, status)
    actor = db.list_actors()[0]
    own_actor(path, actor["id"], identifier)
    insert(
        path,
        "actor_status_events",
        {
            "id": 900001,
            "timestamp_us": time.time_ns() // 1000 + 100,
            "actor_id": actor["id"],
            "new_status": "Failed",
            "reason": "runtime crashed",
        },
    )
    page = client.get("/api/executions").get_json()
    assert page["counts"] == {status: 1}
    assert page["needs_attention"] == page["runtime_faulted"] == 1
    if status == "succeeded":
        assert not page["active"]
        assert page["history"][0]["runtime_faults"] == 1
    else:
        assert not page["history"]
        assert page["active"][0]["runtime_faults"] == 1
    detail = client.get("/api/executions/" + identifier).get_json()
    assert detail["status"] == status
    assert detail["returncode"] == (None if status == "running" else 0)
    assert detail["runtime_actor_failures"][0]["reason"] == "runtime crashed"
    assert detail["runtime_actor_failures"][0]["actor_id"] == str(actor["id"])
