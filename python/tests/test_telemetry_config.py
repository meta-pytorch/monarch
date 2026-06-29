# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Unit tests for telemetry hosted by the job sidecar."""

import json
import os
import pickle
import shutil
import socket
import threading
import urllib.request
import uuid
from typing import cast
from unittest.mock import MagicMock, patch

import monarch._src.job.job_sidecar as js
import monarch._src.job.telemetry_config as tc
import pytest
from monarch._src.job.process_guard import _Shutdown, _wait_for_socket
from monarch._src.job.telemetry_actor import telemetry_socket_dir, telemetry_socket_path
from monarch.job import TelemetryConfig


def _new_apply_id() -> str:
    return f"test_{uuid.uuid4().hex}"


def _remove_socket_dir(apply_id: str) -> None:
    shutil.rmtree(telemetry_socket_dir(apply_id), ignore_errors=True)


def _post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10.0) as response:
        return json.loads(response.read().decode("utf-8"))


def _cfg_dict(**overrides):
    base = {"retention_secs": 0, "include_dashboard": False, "dashboard_port": 0}
    base.update(overrides)
    return base


class _FakeTelemetryHandle:
    """Stand-in for `_TelemetryHandle` so the job sidecar command loop can be
    driven without bootstrapping a real actor system / dashboard."""

    def __init__(self) -> None:
        self.calls = []
        self.shutdown_called = False

    def open_or_refresh(self, host_meshes, config):
        self.calls.append((host_meshes, config))
        return {
            "telemetry_url": "http://telemetry",
            "dashboard_url": "http://dashboard",
            "socket_path": "/tmp/fake.sock",
        }

    def shutdown(self) -> None:
        self.shutdown_called = True


# ── job sidecar command loop ──────────────────────────────────────────────────


@pytest.mark.timeout(30)
def test_run_job_sidecar_survives_broken_connection(tmp_path) -> None:
    """A connection that sends garbage (or breaks mid-request) must drop only
    that connection, not tear down the job sidecar."""
    socket_path = str(tmp_path / "cmd.sock")
    fake = _FakeTelemetryHandle()

    with patch.object(tc, "_TelemetryHandle", return_value=fake):
        thread = threading.Thread(
            target=js._run_job_sidecar,
            args=(socket_path,),
            daemon=True,
        )
        thread.start()
        try:
            _wait_for_socket(socket_path, timeout=10.0)

            # 1) Unparseable pickle: pickle.load raises (not EOFError), which
            #    must be caught at the connection level, not kill the loop.
            bad = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            bad.connect(socket_path)
            bad.sendall(b"\xff")  # invalid pickle opcode → UnpicklingError
            bad.close()

            # 2) A valid request still gets served → proves the job sidecar lived.
            good = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            good.connect(socket_path)
            good.sendall(
                pickle.dumps(
                    js.TelemetryRequest(
                        apply_id="apply",
                        config={},
                        host_meshes={},
                    )
                )
            )
            response = pickle.load(good.makefile("rb"))
            good.close()

            assert response["telemetry_url"] == "http://telemetry"
            assert len(fake.calls) == 1
        finally:
            stop = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                stop.connect(socket_path)
                stop.sendall(pickle.dumps(_Shutdown()))
                stop.close()
            except OSError:
                pass
            thread.join(timeout=10)

    assert not thread.is_alive(), "job sidecar did not exit on _Shutdown"
    assert fake.shutdown_called


# ── _TelemetryHandle.open_or_refresh (bootstrap-once + drift) ─────────────────


def test_open_or_refresh_bootstraps_once_and_ignores_drift() -> None:
    handle = tc._TelemetryHandle("apply")
    bootstrap_calls = []

    def fake_bootstrap(config) -> None:
        bootstrap_calls.append(config)
        handle._dashboard_info = {"local_url": "http://local", "url": "http://ext"}

    with patch.object(handle, "_bootstrap", side_effect=fake_bootstrap):
        expected = {
            "telemetry_url": "http://local",
            "dashboard_url": "http://ext",
            "socket_path": telemetry_socket_path("apply"),
        }

        # First call bootstraps and returns the urls.
        assert handle.open_or_refresh({}, _cfg_dict()) == expected
        assert len(bootstrap_calls) == 1

        # Same config: reuse, no re-bootstrap.
        assert handle.open_or_refresh({}, _cfg_dict()) == expected
        assert len(bootstrap_calls) == 1

        # Drifted config: still no re-bootstrap, first config retained.
        assert handle.open_or_refresh({}, _cfg_dict(retention_secs=600)) == expected
        assert len(bootstrap_calls) == 1
        assert handle._first_config is not None
        assert handle._first_config.retention_secs == 0


def test_open_or_refresh_raises_when_not_open() -> None:
    """If bootstrap fails to stand up the dashboard, open_or_refresh surfaces
    a clear error rather than returning a malformed response."""
    handle = tc._TelemetryHandle("apply")
    with patch.object(handle, "_bootstrap", lambda config: None):
        with pytest.raises(RuntimeError, match="not open"):
            handle.open_or_refresh({}, _cfg_dict())


def test_open_or_refresh_spawns_telemetry_actors_once_with_no_active_workers() -> None:
    handle = tc._TelemetryHandle("apply")

    def fake_bootstrap(config) -> None:
        handle._dashboard_info = {"local_url": "http://local", "url": "http://ext"}

    with (
        patch.object(handle, "_bootstrap", side_effect=fake_bootstrap),
        patch.object(handle, "_spawn_telemetry_actors", return_value=[]) as spawn,
    ):
        handle.open_or_refresh({"hosts": MagicMock()}, _cfg_dict())
        handle.open_or_refresh({"hosts": MagicMock()}, _cfg_dict())

    spawn.assert_called_once()
    assert handle._worker_proc_meshes == []


def test_spawn_telemetry_actors_registers_workers_and_stops_on_shutdown() -> None:
    """Spawning creates a collector mesh, registers it with the client
    collector, retains it, and stops its proc mesh on shutdown."""
    actor_mesh = MagicMock()
    proc_mesh = MagicMock()
    host_mesh = MagicMock()
    client_actor = MagicMock()
    handle = tc._TelemetryHandle("fanout_test")
    handle._client_actor = client_actor

    proc_mesh.spawn.return_value = actor_mesh
    host_mesh.spawn_procs.return_value = proc_mesh
    actor_mesh.activate.call.return_value.get.return_value = [(0, True)]

    worker_proc_meshes = handle._spawn_telemetry_actors(
        {
            "hosts": host_mesh,
        },
        TelemetryConfig(
            retention_secs=0,
            include_dashboard=False,
            dashboard_port=0,
        ),
    )

    host_mesh.spawn_procs.assert_called_once_with(name="telemetry_hosts")
    proc_mesh.spawn.assert_called_once_with(
        "TelemetryActor",
        tc.TelemetryActor,
        "fanout_test",
        0,
    )
    actor_mesh.activate.call.return_value.get.assert_called_once_with()
    client_actor.set_worker_collector_meshes.call_one.assert_called_once_with(
        [actor_mesh]
    )
    client_actor.set_worker_collector_meshes.call_one.return_value.get.assert_called_once_with()

    assert worker_proc_meshes == [proc_mesh]
    handle._worker_proc_meshes = worker_proc_meshes
    with patch.object(tc, "shutdown_context") as shutdown_context:
        handle.shutdown()
    proc_mesh.stop.assert_called_once_with("telemetry shutdown")
    proc_mesh.stop.return_value.get.assert_called_once_with()
    shutdown_context.return_value.get.assert_called_once_with(timeout=5.0)


def test_spawn_telemetry_actors_drops_inactive_worker_collectors() -> None:
    actor_mesh = MagicMock()
    proc_mesh = MagicMock()
    host_mesh = MagicMock()
    client_actor = MagicMock()
    handle = tc._TelemetryHandle("fanout_test")
    handle._client_actor = client_actor

    proc_mesh.spawn.return_value = actor_mesh
    host_mesh.spawn_procs.return_value = proc_mesh
    actor_mesh.activate.call.return_value.get.return_value = [(0, False)]

    worker_proc_meshes = handle._spawn_telemetry_actors(
        {
            "hosts": host_mesh,
        },
        TelemetryConfig(
            retention_secs=0,
            include_dashboard=False,
            dashboard_port=0,
        ),
    )

    client_actor.set_worker_collector_meshes.call_one.assert_called_once_with([])
    assert worker_proc_meshes == []
    proc_mesh.stop.assert_called_once_with("telemetry collector inactive")
    proc_mesh.stop.return_value.get.assert_called_once_with()


def test_spawn_telemetry_actors_noops_when_setup_raises() -> None:
    host_mesh = MagicMock()
    client_actor = MagicMock()
    handle = tc._TelemetryHandle("fanout_test")
    handle._client_actor = client_actor

    host_mesh.spawn_procs.side_effect = RuntimeError("boom")

    worker_proc_meshes = handle._spawn_telemetry_actors(
        {
            "hosts": host_mesh,
        },
        TelemetryConfig(
            retention_secs=0,
            include_dashboard=False,
            dashboard_port=0,
        ),
    )

    host_mesh.spawn_procs.assert_called_once_with(name="telemetry_hosts")
    client_actor.set_worker_collector_meshes.call_one.assert_called_once_with([])
    client_actor.set_worker_collector_meshes.call_one.return_value.get.assert_called_once_with()
    assert worker_proc_meshes == []


@pytest.mark.parametrize("failure_point", ["spawn", "activate"])
def test_spawn_telemetry_actors_stops_partially_started_proc_mesh(
    failure_point: str,
) -> None:
    actor_mesh = MagicMock()
    proc_mesh = MagicMock()
    host_mesh = MagicMock()
    client_actor = MagicMock()
    sidecar = tc._TelemetryHandle("fanout_test")
    sidecar._client_actor = client_actor

    host_mesh.spawn_procs.return_value = proc_mesh
    if failure_point == "spawn":
        proc_mesh.spawn.side_effect = RuntimeError("spawn boom")
    else:
        proc_mesh.spawn.return_value = actor_mesh
        actor_mesh.activate.call.return_value.get.side_effect = RuntimeError(
            "activate boom"
        )

    worker_proc_meshes = sidecar._spawn_telemetry_actors(
        {
            "hosts": host_mesh,
        },
        TelemetryConfig(
            retention_secs=0,
            include_dashboard=False,
            dashboard_port=0,
        ),
    )

    host_mesh.spawn_procs.assert_called_once_with(name="telemetry_hosts")
    client_actor.set_worker_collector_meshes.call_one.assert_called_once_with([])
    client_actor.set_worker_collector_meshes.call_one.return_value.get.assert_called_once_with()
    assert worker_proc_meshes == []
    proc_mesh.stop.assert_called_once_with("telemetry collector startup failed")
    proc_mesh.stop.return_value.get.assert_called_once_with()


def test_ensure_open_requires_apply_id() -> None:
    tel = tc.Telemetry(TelemetryConfig())
    with pytest.raises(RuntimeError, match="apply_id"):
        tel.ensure_open(cast(str, None))


def test_ensure_open_reraises_sidecar_error() -> None:
    apply_id = _new_apply_id()
    tel = tc.Telemetry(TelemetryConfig())
    try:
        with patch.object(tc, "create_job_sidecar") as create_sidecar:
            create_sidecar.return_value.send.return_value.get.return_value = {
                "error": "boom"
            }
            with pytest.raises(RuntimeError, match="boom"):
                tel.ensure_open(apply_id)
    finally:
        _remove_socket_dir(apply_id)


# ── End-to-end: real job sidecar subprocess ───────────────────────────────────


@pytest.mark.timeout(120)
def test_job_sidecar_hosts_telemetry_query_api() -> None:
    """Launch the real job sidecar process, open telemetry over the command
    socket, and confirm the data socket is bound and the query API answers."""
    apply_id = _new_apply_id()
    socket_dir = telemetry_socket_dir(apply_id)
    _remove_socket_dir(apply_id)
    os.makedirs(socket_dir, mode=0o700)
    try:
        response = tc.Telemetry(
            TelemetryConfig(
                retention_secs=0,
                include_dashboard=False,
                dashboard_port=0,
            )
        ).ensure_open(
            apply_id,
            host_meshes={},
        )
        assert isinstance(response, dict)
        telemetry_url = response["telemetry_url"]
        socket_path = response["socket_path"]
        assert isinstance(telemetry_url, str)
        assert socket_path == telemetry_socket_path(apply_id)

        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.connect(socket_path)
        finally:
            client.close()

        query_response = _post_json(
            f"{telemetry_url}/api/query",
            {"sql": "SELECT COUNT(*) AS count FROM spans"},
        )
        assert "rows" in query_response
    finally:
        js.stop_job_sidecar(apply_id)
        _remove_socket_dir(apply_id)
