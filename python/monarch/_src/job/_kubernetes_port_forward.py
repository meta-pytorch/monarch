# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Allocation-scoped ``kubectl port-forward`` for out-of-cluster clients.

Each allocation gets one forward, run by its own daemon process and keyed by
``apply_id`` like the job sidecar. Every client process (scripts, ``monarch
exec``) and the job sidecar reuse it, so nothing starts ``kubectl`` per run.
``job.kill()`` releases it, and it exits on its own if ``kubectl`` does (for
example, when the pod is gone).
"""

import argparse
import logging
import os
import pickle
import re
import select
import shutil
import socket
import subprocess
import threading
from dataclasses import dataclass
from types import TracebackType
from typing import TextIO

from monarch._src.job.job_sidecar import spawn_module
from monarch._src.job.once_daemon import _Shutdown, find_daemon

logger: logging.Logger = logging.getLogger(__name__)

# Seconds to wait for `kubectl port-forward` to report it is ready before giving
# up, so a silently hung forward cannot stall job initialization indefinitely.
START_TIMEOUT_SECONDS: int = 30
# Keep both the graceful and forced waits inside the actor runtime's roughly
# two-second aggregate atexit budget.
STOP_TIMEOUT_SECONDS: float = 0.1
_REQUEST_TIMEOUT_SECONDS: float = 5.0
# Gateway connections the socket queues. Each request is answered at once, so
# only clients starting simultaneously ever wait here.
_LISTEN_BACKLOG: int = 5
# How often the accept loop wakes to check whether `kubectl` has exited (for
# example, because the pod is gone), bounding how long a dead forward lingers.
_KUBECTL_POLL_INTERVAL_SECONDS: float = 0.5
_WORKER_MODULE = "monarch._src.job._kubernetes_port_forward"
_ADDRESS_REQUEST = "address"


@dataclass(frozen=True)
class PortForwardSpec:
    """Inputs that identify one Kubernetes forwarding endpoint."""

    namespace: str
    pod_name: str
    remote_port: int
    kubeconfig: str


class KubectlPortForward:
    """A running ``kubectl port-forward``; closing it stops ``kubectl``."""

    def __init__(self, process: subprocess.Popen[str], address: str) -> None:
        self.process = process
        self.address = address

    def alive(self) -> bool:
        return self.process.poll() is None

    def close(self) -> None:
        """Terminate ``kubectl``, killing it if it lingers."""
        process = self.process
        if process.poll() is not None:
            return
        try:
            process.terminate()
            process.wait(timeout=STOP_TIMEOUT_SECONDS)
            return
        except subprocess.TimeoutExpired:
            pass
        except OSError:
            logger.warning(
                "failed to terminate or reap kubectl port-forward; attempting kill",
                exc_info=True,
            )
        try:
            process.kill()
            process.wait(timeout=STOP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            logger.warning("kubectl port-forward did not exit after being killed")
        except OSError:
            logger.warning("failed to kill or reap kubectl port-forward", exc_info=True)

    def __enter__(self) -> "KubectlPortForward":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def start_port_forward(spec: PortForwardSpec) -> KubectlPortForward:
    """Start ``kubectl port-forward`` to the pod and return the running forward."""
    if shutil.which("kubectl") is None:
        raise RuntimeError(
            "kubectl is required for out-of-cluster port forwarding but was not found in PATH"
        )
    command = [
        "kubectl",
        "port-forward",
        "--namespace",
        spec.namespace,
        f"pod/{spec.pod_name}",
        f":{spec.remote_port}",
        "--kubeconfig",
        spec.kubeconfig,
    ]
    process = subprocess.Popen(
        command,
        # kubectl prompts for credentials it lacks; fail instead of waiting.
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.stdout is None or process.stderr is None:
        raise RuntimeError(
            f"failed to open output for kubectl port-forward to pod {spec.pod_name}"
        )

    # kubectl prints "Forwarding from ..." to stdout once the tunnel is up.
    # Guard the blocking read so a silently hung forward cannot stall startup.
    ready, _, _ = select.select([process.stdout], [], [], START_TIMEOUT_SECONDS)
    if not ready:
        process.kill()
        process.wait()
        raise RuntimeError(
            f"kubectl port-forward to pod {spec.pod_name} did not start within "
            f"{START_TIMEOUT_SECONDS}s"
        )

    first_line = process.stdout.readline()
    if not first_line:
        # kubectl closed stdout without announcing readiness. Terminate it and
        # drain stderr with a deadline so a process that closed stdout while
        # still running cannot block us.
        process.terminate()
        try:
            _, stderr_output = process.communicate(timeout=START_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            _, stderr_output = process.communicate()
        raise RuntimeError(
            f"kubectl port-forward produced no output for pod {spec.pod_name}: "
            f"{stderr_output}"
        )

    match = re.search(r"Forwarding from (?:127\.0\.0\.1|\[::1\]):(\d+) ->", first_line)
    if not match:
        process.kill()
        process.wait()
        raise RuntimeError(
            "could not parse local port from kubectl output for pod "
            f"{spec.pod_name}: {first_line}"
        )

    # kubectl keeps logging per connection; keep the pipes from filling up.
    for stream in (process.stdout, process.stderr):
        threading.Thread(target=_drain, args=(stream,), daemon=True).start()
    return KubectlPortForward(process, f"tcp://127.0.0.1:{int(match.group(1))}")


def allocation_port_forward_lock_path(apply_id: str) -> str:
    """Return the local lock path for an allocation's shared gateway."""
    return f"/tmp/monarch_kubernetes_gateway_{apply_id}.lock"


def ensure_allocation_port_forward(apply_id: str, spec: PortForwardSpec) -> str:
    """Return the address of the allocation's port-forward, starting it if needed."""
    daemon = spawn_module(
        allocation_port_forward_lock_path(apply_id),
        spec,
        _WORKER_MODULE,
        process_name="kubernetes_gateway",
        module_args=[
            "--namespace",
            spec.namespace,
            "--pod-name",
            spec.pod_name,
            "--remote-port",
            str(spec.remote_port),
            "--kubeconfig",
            spec.kubeconfig,
        ],
    )
    response = daemon.send(_ADDRESS_REQUEST).get()
    if not isinstance(response, str):
        raise RuntimeError(f"unexpected Kubernetes gateway response: {response!r}")
    return response


def stop_allocation_port_forward(apply_id: str) -> None:
    """Stop an allocation-scoped port-forward if one is running."""
    daemon = find_daemon(allocation_port_forward_lock_path(apply_id))
    if daemon is not None:
        daemon.shutdown()


def _drain(stream: TextIO) -> None:
    for _line in stream:
        pass


def _serve(spec: PortForwardSpec, socket_path: str) -> None:
    with start_port_forward(spec) as forward:
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(socket_path)
            server.listen(_LISTEN_BACKLOG)
            server.settimeout(_KUBECTL_POLL_INTERVAL_SECONDS)
            while forward.alive():
                try:
                    conn, _ = server.accept()
                except TimeoutError:
                    continue
                with conn:
                    conn.settimeout(_REQUEST_TIMEOUT_SECONDS)
                    try:
                        # @lint-ignore PYTHONPICKLEISBAD
                        message = pickle.load(conn.makefile("rb"))
                    except Exception:
                        # A disconnect, stalled client, or malformed request only
                        # costs that client; the forward is shared by the allocation.
                        # Debug level: OnceDaemon's readiness probe connects and
                        # closes without sending a request.
                        logger.debug("dropped gateway client", exc_info=True)
                        continue
                    if isinstance(message, _Shutdown):
                        return
                    response = (
                        forward.address
                        if message == _ADDRESS_REQUEST
                        else {
                            "error": f"unexpected Kubernetes gateway request: {message!r}"
                        }
                    )
                    try:
                        # @lint-ignore PYTHONPICKLEISBAD
                        conn.sendall(pickle.dumps(response))
                    except OSError:
                        continue  # The client left before reading its reply.
        finally:
            server.close()
            try:
                os.unlink(socket_path)
            except FileNotFoundError:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run one allocation's shared `kubectl port-forward` and serve its local "
            "address to Monarch clients. Launched by OnceDaemon via "
            "`ensure_allocation_port_forward`, not meant to be run by hand."
        )
    )
    parser.add_argument(
        "--namespace", required=True, help="Kubernetes namespace of the worker pod."
    )
    parser.add_argument(
        "--pod-name", required=True, help="Worker pod to forward to, e.g. `mesh-0`."
    )
    parser.add_argument(
        "--remote-port",
        required=True,
        type=int,
        help="Monarch worker port inside the pod.",
    )
    parser.add_argument(
        "--kubeconfig", required=True, help="Path of the kubeconfig kubectl uses."
    )
    parser.add_argument(
        "socket_path",
        help="Unix socket to serve address requests on (supplied by OnceDaemon).",
    )
    parser.add_argument(
        "lock_fd",
        type=int,
        help="Inherited lock file descriptor held for the process lifetime "
        "(supplied by OnceDaemon).",
    )
    args = parser.parse_args()
    _serve(
        PortForwardSpec(
            namespace=args.namespace,
            pod_name=args.pod_name,
            remote_port=args.remote_port,
            kubeconfig=args.kubeconfig,
        ),
        args.socket_path,
    )


if __name__ == "__main__":
    main()
