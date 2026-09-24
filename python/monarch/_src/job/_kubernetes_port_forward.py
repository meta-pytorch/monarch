# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""``kubectl port-forward`` processes, including allocation-scoped ones.

A process-local forward lives as long as the client that started it. An
allocation-scoped forward runs under its own guarded process, so sidecars
attached through it keep their route after the client exits; ``job.kill()``
releases it.
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
from typing import TextIO

from monarch._src.job.job_sidecar import spawn_module
from monarch._src.job.process_guard import _Shutdown, find_process

logger: logging.Logger = logging.getLogger(__name__)

# Seconds to wait for `kubectl port-forward` to report it is ready before giving
# up, so a silently hung forward cannot stall job initialization indefinitely.
START_TIMEOUT_SECONDS: int = 30
# Keep both the graceful and forced waits inside the actor runtime's roughly
# two-second aggregate atexit budget.
STOP_TIMEOUT_SECONDS: float = 0.1
_REQUEST_TIMEOUT_SECONDS: float = 5.0
_WORKER_MODULE = "monarch._src.job._kubernetes_port_forward"
_ADDRESS_REQUEST = "address"


@dataclass(frozen=True)
class PortForwardSpec:
    """Inputs that identify one Kubernetes forwarding endpoint."""

    namespace: str
    pod_name: str
    remote_port: int
    kubeconfig: str | None


def start_port_forward(spec: PortForwardSpec) -> tuple[subprocess.Popen[str], str]:
    """Start ``kubectl port-forward`` to the pod; return it and its local address."""
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
    ]
    if spec.kubeconfig is not None:
        command.extend(["--kubeconfig", spec.kubeconfig])
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
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
    return process, f"tcp://127.0.0.1:{int(match.group(1))}"


def stop_port_forward(process: subprocess.Popen[str]) -> None:
    """Terminate a ``kubectl port-forward`` process, killing it if it lingers."""
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


def allocation_port_forward_lock_path(apply_id: str) -> str:
    """Return the local guard path for an allocation's shared gateway."""
    return f"/tmp/monarch_kubernetes_gateway_{apply_id}.lock"


def ensure_allocation_port_forward(apply_id: str, spec: PortForwardSpec) -> str:
    """Return the stable address of an allocation-scoped port-forward."""
    guard = spawn_module(
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
            *(["--kubeconfig", spec.kubeconfig] if spec.kubeconfig is not None else []),
        ],
    )
    response = guard.send(_ADDRESS_REQUEST).get()
    if not isinstance(response, str):
        raise RuntimeError(f"unexpected Kubernetes gateway response: {response!r}")
    return response


def stop_allocation_port_forward(apply_id: str) -> None:
    """Stop an allocation-scoped port-forward if one is running."""
    guard = find_process(allocation_port_forward_lock_path(apply_id))
    if guard is not None:
        guard.shutdown()


def _drain(stream: TextIO) -> None:
    for _line in stream:
        pass


def _serve(spec: PortForwardSpec, socket_path: str) -> None:
    process, address = start_port_forward(spec)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(socket_path)
        server.listen(5)
        server.settimeout(0.5)
        while process.poll() is None:
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
                    continue
                if isinstance(message, _Shutdown):
                    return
                response = (
                    address
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
        stop_port_forward(process)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--pod-name", required=True)
    parser.add_argument("--remote-port", required=True, type=int)
    parser.add_argument("--kubeconfig")
    parser.add_argument("socket_path")
    parser.add_argument("lock_fd", type=int)
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
