# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Per-job sidecar command server.

The sidecar process is keyed by a job ``apply_id`` and kept alive by ``OnceDaemon``.
"""

import logging
import os
import pickle
import socket
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any, TypedDict

from monarch._rust_bindings.monarch_hyperactor.channel import BindSpec
from monarch._rust_bindings.monarch_hyperactor.host_mesh import PyMeshAdminRef
from monarch._src.actor.host_mesh import _add_admin_hosts, _spawn_admin
from monarch._src.job.once_daemon import find_daemon, OnceDaemon
from monarch.actor import attach, context, enable_transport, HostMesh
from monarch.config import get_runtime_config

logger: logging.Logger = logging.getLogger(__name__)

_JOB_SIDECAR_WORKER_MODULE = "monarch._src.job._job_sidecar_worker"
_MOUNT_CLEAR_TIMEOUT_S = 30.0
_SIDECAR_EXIT_GRACE_S = 2.0

try:
    from __manifest__ import fbmake  # noqa

    _IN_PAR: bool = bool(fbmake.get("par_style"))
except ImportError:
    _IN_PAR = False


def job_sidecar_lock_path(apply_id: str) -> str:
    """Return the lock path for the per-job sidecar process."""
    return f"/tmp/monarch_job_sidecar_{apply_id}.lock"


def spawn_module(
    lock_path: str,
    config_key: object,
    module_name: str,
    runtime_transport: str | None = None,
    attach_to: str | None = None,
    process_name: str | None = None,
    module_args: list[str] | None = None,
) -> OnceDaemon:
    """Launch a Python module as a ``OnceDaemon``-managed background process."""
    env = (
        {"HYPERACTOR_PROCESS_NAME": process_name} if process_name is not None else None
    )
    if _IN_PAR:
        command = [sys.argv[0]]
        env = {**(env or {}), "PAR_MAIN_OVERRIDE": module_name}
    else:
        if not sys.executable:
            raise RuntimeError("no python executable available")
        command = [sys.executable, "-m", module_name]
    if runtime_transport is not None:
        command.extend(["--runtime-transport", runtime_transport])
    if attach_to is not None:
        command.extend(["--attach-to", attach_to])
    if module_args is not None:
        command.extend(module_args)
    return OnceDaemon.create(lock_path, config_key, command, env=env)


def create_job_sidecar(
    apply_id: str,
    attach_to: str | None = None,
) -> OnceDaemon:
    """Ensure the per-job sidecar process is running and return its handle."""
    runtime_transport = sidecar_transport_from_runtime()
    # Keyed on the route too: a sidecar attached through a gateway that has
    # since moved is replaced rather than left talking to a dead address.
    return spawn_module(
        job_sidecar_lock_path(apply_id),
        (apply_id, runtime_transport, attach_to),
        _JOB_SIDECAR_WORKER_MODULE,
        runtime_transport=runtime_transport,
        attach_to=attach_to,
        process_name="job_sidecar",
    )


def get_job_sidecar(apply_id: str) -> OnceDaemon:
    """Return an existing job sidecar, creating an unattached one if absent."""
    return find_job_sidecar(apply_id) or create_job_sidecar(apply_id)


def find_job_sidecar(apply_id: str) -> OnceDaemon | None:
    """Return a handle to the per-job sidecar process if it exists."""
    return find_daemon(job_sidecar_lock_path(apply_id))


def stop_job_sidecar(apply_id: str) -> None:
    """Close mounts, then shut down the per-job sidecar for an apply id.

    Mount cleanup is an acknowledged operation so an unmount failure leaves the
    sidecar alive and retryable instead of destroying the only process holding
    the mount handle and leaking a stale filesystem. A sidecar that has already
    died holds no mounts, so losing the connection to it counts as stopped.
    """
    daemon = find_job_sidecar(apply_id)
    if daemon is None:
        return
    try:
        response = daemon.send(ClearMountsRequest()).get(timeout=_MOUNT_CLEAR_TIMEOUT_S)
        raise_for_sidecar_error(response, "clear job mounts")
        daemon.shutdown()
    except (OSError, EOFError):
        # The sidecar shares the client's process group, so a terminal Ctrl-C
        # can kill it while we are talking to it.
        if not daemon.exited(timeout=_SIDECAR_EXIT_GRACE_S):
            raise


def ensure_job_admin(
    apply_id: str,
    host_meshes: dict[str, HostMesh],
    client_host: HostMesh,
    admin_addr: str,
    telemetry_url: str | None,
    snapshot_interval_secs: float | None,
) -> str:
    """Return the job's mesh admin URL, starting the admin in the sidecar if needed.

    The sidecar also lists this process's ``client_host`` until the process
    exits. Raises if the admin cannot start, for example because its address is
    taken.
    """
    response = (
        get_job_sidecar(apply_id)
        .send(
            AdminRequest(
                apply_id=apply_id,
                host_meshes=host_meshes,
                client_host=client_host,
                client_pid=os.getpid(),
                admin_addr=admin_addr,
                telemetry_url=telemetry_url,
                snapshot_interval_secs=snapshot_interval_secs,
            )
        )
        .get()
    )
    if not isinstance(response, dict):
        raise RuntimeError(f"unexpected mesh admin response: {response!r}")
    if isinstance(response.get("error"), str):
        raise RuntimeError(response["error"])
    for warning in response["warnings"]:
        logger.warning("mesh admin: %s", warning)
    return response["admin_url"]


def raise_for_sidecar_error(response: object, operation: str) -> None:
    """Raise when a job-sidecar response reports an operation failure."""
    if isinstance(response, dict) and isinstance(response.get("error"), str):
        raise RuntimeError(f"job sidecar failed to {operation}:\n{response['error']}")
    if response != "ok":
        raise RuntimeError(
            f"job sidecar returned an unexpected response while attempting to "
            f"{operation}: {response!r}"
        )


def sidecar_transport_from_runtime() -> str | None:
    """Return the Runtime-layer default transport for sidecar startup."""
    transport = get_runtime_config().get("default_transport")
    if transport is None:
        return None
    if isinstance(transport, str):
        return transport
    return str(BindSpec(transport))


def configure_sidecar_transport(transport: str | None) -> None:
    """Enable sidecar startup transport before actor context bootstrap."""
    if transport is not None:
        enable_transport(transport)


@dataclass
class ClearMountsRequest:
    """Clear the job sidecar's mount state."""


@dataclass
class MountsRequest:
    """Refresh the job sidecar's mount state."""

    mounts: Any
    host_meshes: dict[str, HostMesh]


@dataclass
class TelemetryRequest:
    """Open or refresh the job sidecar's telemetry state."""

    apply_id: str
    config: dict[str, object]
    host_meshes: dict[str, HostMesh]
    spawn_worker_collectors: bool = True


@dataclass
class TelemetryInfoRequest:
    """Return the existing telemetry endpoint without starting it."""

    apply_id: str


@dataclass
class AdminRequest:
    """Start the job's mesh admin, or return the URL of the running one."""

    apply_id: str
    host_meshes: dict[str, HostMesh]
    client_host: HostMesh
    client_pid: int
    admin_addr: str
    telemetry_url: str | None
    # None when the client has no telemetry config, so no snapshot opinion.
    snapshot_interval_secs: float | None


class AdminResponse(TypedDict):
    """The job sidecar's reply to an `AdminRequest`."""

    admin_url: str
    warnings: list[str]


@dataclass(frozen=True)
class _RunningAdmin:
    """The job's mesh admin, started by the first `AdminRequest`."""

    url: str
    addr: str
    telemetry_url: str | None
    ref: PyMeshAdminRef


def _telemetry_mismatch(admin: _RunningAdmin, request: AdminRequest) -> str | None:
    """Warn when the client's telemetry is not the one the admin started with."""
    if request.telemetry_url is None or request.telemetry_url == admin.telemetry_url:
        return None
    if admin.telemetry_url is None:
        return (
            "started without telemetry, so it cannot query the dashboard; "
            "kill the job to link it"
        )
    return f"uses telemetry at {admin.telemetry_url}; kill the job to change it"


class _JobSidecarState:
    """State owned by the background job sidecar process."""

    def __init__(self) -> None:
        self._mounts_handle: Any | None = None
        self._mounts_key: bytes | None = None
        self._telemetry_handle: Any | None = None
        self._admin: _RunningAdmin | None = None
        self._snapshot_interval_secs: float | None = None
        self._apply_id: str | None = None

    def _ensure_apply_id(self, apply_id: str) -> None:
        if self._apply_id is None:
            self._apply_id = apply_id
        elif self._apply_id != apply_id:
            raise RuntimeError(f"job sidecar already owns apply id {self._apply_id}")

    def handle_mounts(self, request: MountsRequest) -> str:
        from monarch._src.job.mount_config import MountOpenError

        # The request carries declarative mount config. Compare that serialized
        # config so edited mounts replace the live handles, while unchanged
        # config refreshes existing remote mounts in-place.
        # @lint-ignore PYTHONPICKLEISBAD
        mounts_key = pickle.dumps(request.mounts)
        t0 = time.time()
        if self._mounts_handle is not None and mounts_key != self._mounts_key:
            _dbg("replacing mounts")
            self.clear_mounts()

        if self._mounts_handle is None:
            _dbg("initialising mounts")
            try:
                self._mounts_handle = request.mounts.open(request.host_meshes)
            except MountOpenError as error:
                self._mounts_handle = error.handle
                raise
            self._mounts_key = mounts_key
            _dbg(f"mounts opened in {time.time() - t0:.2f}s")
            return "ok"

        _dbg("refreshing mounts")
        self._mounts_handle.refresh()
        _dbg(f"refresh complete in {time.time() - t0:.2f}s")
        return "ok"

    def clear_mounts(self) -> str:
        """Close any live mounts and reset mount state."""
        if self._mounts_handle is not None:
            # A failed close may already have removed some mounts.
            self._mounts_key = None
            self._mounts_handle.close()
            self._mounts_handle = None
        return "ok"

    def handle_telemetry(self, request: TelemetryRequest) -> object:
        from monarch._src.job.telemetry_config import _TelemetryHandle

        self._ensure_apply_id(request.apply_id)
        if self._telemetry_handle is None:
            self._telemetry_handle = _TelemetryHandle(request.apply_id)

        return self._telemetry_handle.open_or_refresh(
            request.host_meshes,
            request.config,
            spawn_worker_collectors=request.spawn_worker_collectors,
        )

    def handle_telemetry_info(self, request: TelemetryInfoRequest) -> object:
        if self._apply_id != request.apply_id or self._telemetry_handle is None:
            raise RuntimeError("distributed telemetry is not running for this job")
        return self._telemetry_handle.info()

    def handle_admin(self, request: AdminRequest) -> AdminResponse:
        """Start the mesh admin once; later requests reuse it."""
        self._ensure_apply_id(request.apply_id)
        admin = self._admin
        if admin is None:
            url, ref = _spawn_admin(
                list(request.host_meshes.values()),
                admin_addr=request.admin_addr,
                telemetry_url=request.telemetry_url,
            ).get()
            admin = self._admin = _RunningAdmin(
                url, request.admin_addr, request.telemetry_url, ref
            )
            # The dashboard's py-spy client reads the admin URL from here.
            os.environ["MONARCH_ADMIN_URL"] = url
        elif request.admin_addr != admin.addr:
            raise RuntimeError(
                f"mesh admin is already running at {admin.url}; "
                "kill the job to change its address"
            )
        warnings = [
            warning
            for warning in (
                self._add_client_host(admin, request),
                _telemetry_mismatch(admin, request),
                self._ensure_snapshots(admin, request),
            )
            if warning is not None
        ]
        return {"admin_url": admin.url, "warnings": warnings}

    def _add_client_host(
        self, admin: _RunningAdmin, request: AdminRequest
    ) -> str | None:
        """List the requesting client, so actors in its process stay visible.

        Best-effort: a failure returns a warning instead of failing `state()`.
        """
        try:
            _add_admin_hosts(admin.ref, [request.client_host], request.client_pid).get()
        except Exception as error:
            _dbg(traceback.format_exc())
            return f"this client is not listed in the admin topology: {error}"
        return None

    def _ensure_snapshots(
        self, admin: _RunningAdmin, request: AdminRequest
    ) -> str | None:
        """Start snapshots once telemetry is live. Best-effort: a failed start
        returns a warning and is retried on the next request.

        Also returns a warning when the request asks for a different interval
        than the running snapshots, which cannot be changed or stopped.
        """
        requested = request.snapshot_interval_secs
        if requested is None:
            return None
        running = self._snapshot_interval_secs
        if running is not None:
            if requested != running:
                return (
                    f"snapshots keep running every {running}s; kill the job to "
                    f"apply snapshot_interval_secs={requested}"
                )
            return None
        if request.telemetry_url is None or requested <= 0:
            return None
        from monarch._rust_bindings.monarch_extension.snapshot_integration import (
            _start_periodic_snapshots_http,
        )

        try:
            _start_periodic_snapshots_http(
                base_url=request.telemetry_url,
                admin_ref=admin.ref,
                instance=context().actor_instance._as_rust(),
                interval_secs=requested,
            )
        except Exception as error:
            _dbg(traceback.format_exc())
            return f"snapshots failed to start; will retry on the next state(): {error}"
        self._snapshot_interval_secs = requested
        return None

    def shutdown(self) -> None:
        self.clear_mounts()
        if self._telemetry_handle is not None:
            self._telemetry_handle.shutdown()
            self._telemetry_handle = None
        self._apply_id = None


def _dbg(msg: str) -> None:
    print(f"[job_sidecar pid={os.getpid()}] {msg}", file=sys.stderr, flush=True)


def _run_job_sidecar(
    socket_path: str,
    runtime_transport: str | None = None,
    attach_to: str | None = None,
) -> None:
    """Run in the child process: bind socket then serve refresh/shutdown requests."""
    import signal as _signal

    # Signal handlers are process-global and only legal from the main thread.
    if threading.current_thread() is threading.main_thread():
        _signal.signal(_signal.SIGINT, _signal.SIG_DFL)
        _signal.signal(_signal.SIGTERM, _signal.SIG_DFL)

    configure_sidecar_transport(runtime_transport)
    if attach_to is not None:
        attach(attach_to)

    from monarch._src.job.once_daemon import _Shutdown

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)
    server.listen(5)
    _dbg(f"listening on {socket_path}")

    state = _JobSidecarState()

    while True:
        try:
            conn, _ = server.accept()
        except OSError:
            break
        try:
            f = conn.makefile("rb")
            while True:
                try:
                    # @lint-ignore PYTHONPICKLEISBAD
                    msg = pickle.load(f)
                except EOFError:
                    break  # client disconnected (e.g. readiness probe)
                if isinstance(msg, _Shutdown):
                    _dbg("received shutdown")
                    state.shutdown()
                    conn.close()
                    server.close()
                    try:
                        os.unlink(socket_path)
                    except OSError:
                        pass
                    _dbg("exiting")
                    return
                try:
                    if isinstance(msg, MountsRequest):
                        response = state.handle_mounts(msg)
                    elif isinstance(msg, ClearMountsRequest):
                        response = state.clear_mounts()
                    elif isinstance(msg, TelemetryRequest):
                        response = state.handle_telemetry(msg)
                    elif isinstance(msg, TelemetryInfoRequest):
                        try:
                            response = state.handle_telemetry_info(msg)
                        except Exception as error:
                            # Preserve sidecar context in its log, not in CLI errors.
                            _dbg(traceback.format_exc())
                            response = {"error": str(error)}
                    elif isinstance(msg, AdminRequest):
                        try:
                            response = state.handle_admin(msg)
                        except Exception as error:
                            _dbg(traceback.format_exc())
                            response = {"error": str(error)}
                    else:
                        raise RuntimeError(f"unexpected job sidecar request: {msg!r}")
                except Exception:
                    _dbg(
                        "ERROR during job sidecar operation:\n" + traceback.format_exc()
                    )
                    response = {"error": traceback.format_exc()}
                # @lint-ignore PYTHONPICKLEISBAD
                conn.sendall(pickle.dumps(response))
        except Exception:
            _dbg("ERROR handling connection:\n" + traceback.format_exc())
        finally:
            try:
                conn.close()
            except Exception:
                pass

    server.close()
    _dbg("exiting")
