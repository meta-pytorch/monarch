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
import urllib.error
import urllib.request
import uuid
from typing import cast
from unittest.mock import call, MagicMock, patch

import monarch._src.job.job_sidecar as js
import monarch._src.job.telemetry_config as tc
import pytest
from monarch._src.job.process_guard import _Shutdown, _wait_for_socket
from monarch._src.job.telemetry_actor import telemetry_socket_dir, telemetry_socket_path
from monarch.config import ChannelTransport, configured
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


def _post_bytes(
    url: str,
    payload: bytes,
    content_type: str = "application/octet-stream",
) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": content_type},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10.0) as response:
        return json.loads(response.read().decode("utf-8"))


def _snapshot_ipc(snapshot_id: str) -> bytes:
    import pyarrow as pa
    import pyarrow.ipc as ipc

    schema = pa.schema(
        [
            pa.field("snapshot_id", pa.string(), nullable=False),
            pa.field("snapshot_ts", pa.int64(), nullable=False),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([snapshot_id], type=pa.string()),
            pa.array([123456], type=pa.int64()),
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


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

    def open_or_refresh(self, host_meshes, config, spawn_worker_collectors=True):
        self.calls.append((host_meshes, config, spawn_worker_collectors))
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
        handle._dashboard_info = {
            "api_url": "http://api",
            "local_url": "http://local",
            "url": "http://ext",
        }

    with patch.object(handle, "_bootstrap", side_effect=fake_bootstrap):
        expected = {
            "telemetry_url": "http://api",
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


@pytest.mark.parametrize("spawn_worker_collectors", [True, False])
def test_open_or_refresh_starts_worker_collectors_once(
    spawn_worker_collectors: bool,
) -> None:
    handle = tc._TelemetryHandle("apply")
    host_mesh = MagicMock()
    client_actor = MagicMock()

    def fake_bootstrap(config) -> None:
        handle._client_actor = client_actor
        handle._dashboard_info = {
            "api_url": "http://api",
            "local_url": "http://local",
            "url": "http://ext",
        }

    with patch.object(handle, "_bootstrap", side_effect=fake_bootstrap):
        handle.open_or_refresh(
            {"hosts": host_mesh},
            _cfg_dict(),
            spawn_worker_collectors,
        )
        handle.open_or_refresh(
            {"hosts": host_mesh},
            _cfg_dict(),
            spawn_worker_collectors,
        )

    client_actor.start_worker_collector.call_one.assert_called_once_with(
        host_mesh,
        spawn_worker_collectors,
    )
    client_actor.start_worker_collector.call_one.return_value.get.assert_called_once_with()


def test_shutdown_stops_worker_collectors() -> None:
    client_actor = MagicMock()
    handle = tc._TelemetryHandle("fanout_test")
    handle._client_actor = client_actor

    with patch.object(tc, "shutdown_context") as shutdown_context:
        handle.shutdown()
    client_actor.stop_worker_collectors.call_one.assert_called_once_with()
    client_actor.stop_worker_collectors.call_one.return_value.get.assert_called_once_with(
        timeout=5.0
    )
    shutdown_context.return_value.get.assert_called_once_with(timeout=5.0)


def test_start_worker_collector_drops_inactive_actor() -> None:
    actor_mesh = MagicMock()
    proc_mesh = MagicMock()
    host_mesh = MagicMock()
    telemetry_actor = tc.TelemetryActor("fanout_test", 0)

    proc_mesh.spawn.return_value = actor_mesh
    host_mesh.spawn_procs.return_value = proc_mesh
    actor_mesh.activate.call.return_value.get.return_value = [(0, False)]

    worker_collector_mesh = telemetry_actor._start_worker_collector(
        host_mesh,
        tc.TelemetryActor,
        "fanout_test",
        "telemetry_hosts",
    )

    assert worker_collector_mesh is None
    assert telemetry_actor._worker_proc_meshes == [proc_mesh]
    actor_mesh.stop.assert_called_once_with("telemetry collector inactive")
    actor_mesh.stop.return_value.get.assert_called_once_with(timeout=5.0)


def test_start_worker_collector_retains_active_actor() -> None:
    actor_mesh = MagicMock()
    proc_mesh = MagicMock()
    host_mesh = MagicMock()
    telemetry_actor = tc.TelemetryActor("fanout_test", 0)

    proc_mesh.spawn.return_value = actor_mesh
    host_mesh.spawn_procs.return_value = proc_mesh
    actor_mesh.activate.call.return_value.get.return_value = [(0, True)]
    actor_mesh.id = "active"

    assert (
        telemetry_actor._start_worker_collector(
            host_mesh,
            tc.TelemetryActor,
            "fanout_test",
            "telemetry_hosts",
        )
        is actor_mesh
    )
    assert telemetry_actor._worker_proc_meshes == [proc_mesh]
    assert telemetry_actor._worker_collectors == {"active": actor_mesh}


def test_telemetry_actor_supervision_prunes_only_matching_worker() -> None:
    telemetry_actor = tc.TelemetryActor("fanout_test", 0)
    failed = MagicMock()
    healthy = MagicMock()
    telemetry_actor._worker_collectors = {
        "failed": failed,
        "healthy": healthy,
    }

    failure = MagicMock(mesh_id="failed")
    assert telemetry_actor.__supervise__(failure)
    assert telemetry_actor._worker_collectors == {"healthy": healthy}

    assert telemetry_actor.__supervise__(failure)
    assert telemetry_actor.__supervise__(MagicMock(mesh_id="unknown"))
    assert telemetry_actor._worker_collectors == {"healthy": healthy}


def test_telemetry_actor_suppresses_undeliverable_worker_message() -> None:
    telemetry_actor = tc.TelemetryActor("fanout_test", 0)

    # A message the query root sent to a now-dead worker collector bounces back
    # as undeliverable. Returning True suppresses escalation so the query root
    # is not torn down.
    assert telemetry_actor._handle_undeliverable_message(MagicMock())


def test_open_or_refresh_continues_after_worker_setup_failure() -> None:
    failed_host_mesh = MagicMock()
    healthy_host_mesh = MagicMock()
    client_actor = MagicMock()
    handle = tc._TelemetryHandle("fanout_test")

    def fake_bootstrap(config) -> None:
        handle._client_actor = client_actor
        handle._dashboard_info = {
            "api_url": "http://api",
            "local_url": "http://local",
            "url": "http://ext",
        }

    client_actor.start_worker_collector.call_one.return_value.get.side_effect = (
        RuntimeError("boom"),
        None,
    )

    with patch.object(handle, "_bootstrap", side_effect=fake_bootstrap):
        handle.open_or_refresh(
            {
                "failed": failed_host_mesh,
                "healthy": healthy_host_mesh,
            },
            _cfg_dict(),
        )

    assert client_actor.start_worker_collector.call_one.call_args_list == [
        call(failed_host_mesh, True),
        call(healthy_host_mesh, True),
    ]
    assert client_actor.start_worker_collector.call_one.return_value.get.call_count == 2


@pytest.mark.parametrize("failure_point", ["spawn", "activate"])
def test_start_worker_collector_stops_partially_started_proc_mesh(
    failure_point: str,
) -> None:
    actor_mesh = MagicMock()
    proc_mesh = MagicMock()
    host_mesh = MagicMock()
    telemetry_actor = tc.TelemetryActor("fanout_test", 0)

    host_mesh.spawn_procs.return_value = proc_mesh
    if failure_point == "spawn":
        proc_mesh.spawn.side_effect = RuntimeError("spawn boom")
    else:
        proc_mesh.spawn.return_value = actor_mesh
        actor_mesh.activate.call.return_value.get.side_effect = RuntimeError(
            "activate boom"
        )

    with pytest.raises(RuntimeError):
        telemetry_actor._start_worker_collector(
            host_mesh,
            tc.TelemetryActor,
            "fanout_test",
            "telemetry_hosts",
        )

    host_mesh.spawn_procs.assert_called_once_with(name="telemetry_hosts")
    proc_mesh.stop.assert_called_once_with("telemetry collector startup failed")
    proc_mesh.stop.return_value.get.assert_called_once_with(timeout=5.0)


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


@pytest.mark.parametrize(
    ("transport", "expected"),
    [
        (ChannelTransport.MetaTlsWithIpV6, "metatls(IpV6)"),
        (ChannelTransport.MetaTlsWithHostname, "metatls(Hostname)"),
        ("unix", "unix"),
        ("tcp://127.0.0.1:1234", "tcp://127.0.0.1:1234"),
    ],
)
def test_sidecar_transport_from_runtime_serializes_config_transport(
    transport: object,
    expected: str,
) -> None:
    with patch.object(
        js,
        "get_runtime_config",
        return_value={"default_transport": transport},
    ):
        assert js.sidecar_transport_from_runtime() == expected


def test_sidecar_transport_from_runtime_reads_configured_bind_spec() -> None:
    with configured(default_transport=ChannelTransport.MetaTlsWithIpV6):
        assert js.sidecar_transport_from_runtime() == "metatls(IpV6)"


def test_configure_sidecar_transport_enables_parent_transport() -> None:
    with patch.object(js, "enable_transport") as enable_transport:
        js.configure_sidecar_transport("metatls")

    enable_transport.assert_called_once_with("metatls")


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

        snapshot_id = f"snapshot_{uuid.uuid4().hex}"
        ingest_response = _post_bytes(
            f"{telemetry_url}/api/ingest_snapshot/snapshots",
            _snapshot_ipc(snapshot_id),
            "application/vnd.apache.arrow.stream",
        )
        assert ingest_response == {"status": "ok"}
        snapshot_response = _post_json(
            f"{telemetry_url}/api/query",
            {
                "sql": (
                    "SELECT snapshot_id FROM snapshots "
                    f"WHERE snapshot_id = '{snapshot_id}'"
                )
            },
        )
        assert snapshot_response["rows"] == [{"snapshot_id": snapshot_id}]

        with pytest.raises(urllib.error.HTTPError) as error:
            _post_bytes(
                f"{telemetry_url}/api/ingest_snapshot/spans",
                b"not-arrow",
                "application/vnd.apache.arrow.stream",
            )
        assert error.value.code == 400
    finally:
        js.stop_job_sidecar(apply_id)
        _remove_socket_dir(apply_id)
