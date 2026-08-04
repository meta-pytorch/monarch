#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""MAST entry point for the minimonarch smoke test.

This runs once on *every* task of the MAST job. The behaviour is:

  * Every task starts a worker (``smoke.py --worker``) that serves a quic
    listener on all IPv6 interfaces.
  * Rank 0 *additionally* starts the root (``smoke.py --root ...``), which dials
    every host in the task group, times the connect + round-trip, and exits with
    the smoke test's status.

Host discovery uses the MAST-provided environment:

  * ``MAST_HPC_TASK_GROUP_HOSTNAMES`` — comma-separated FQDNs of every task host
    (includes this host).
  * ``TW_TASK_ID`` / ``HOSTNAME`` — used to decide which task is rank 0. MAST may
    order the host list differently from ``TW_TASK_ID``, so we identify rank 0 by
    matching our own hostname against ``hostnames[0]`` and fall back to
    ``TW_TASK_ID == "0"``.

We never connect by DNS name: each peer FQDN is resolved to a concrete IPv6
address with ``getaddrinfo`` and the root dials ``[<ipv6>]:<port>``.

The package layout (see build_mast.py) places ``smoke.py``, the compiled
``minimonarch*.so`` and a ``certs/`` directory all next to this file, so the
worker/root subprocesses import the extension from here and pick up TLS material
automatically.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("SMOKE_PORT", "26600"))
# How many worker processes to run on each host, on sequential ports
# PORT, PORT+1, ... This lets you scale the worker count without more machines.
WORKERS_PER_HOST = int(os.environ.get("SMOKE_WORKERS_PER_HOST", "1"))
# Seconds rank 0 waits before dialing, giving every worker time to bind.
STARTUP_GRACE = float(os.environ.get("SMOKE_STARTUP_GRACE", "15"))


def task_hosts() -> list[str]:
    """Every host FQDN in this task group, in MAST's order (includes self)."""
    raw = os.environ.get("MAST_HPC_TASK_GROUP_HOSTNAMES", "")
    return [h for h in raw.split(",") if h]


def is_rank0() -> bool:
    """True on the one task that should also run the root.

    Pinned to ``TW_TASK_ID == "0"`` so the root is *always* task 0 and its logs
    can be fetched with a single-task query (``tw log <job>/0``) — critical at
    scale, where a job-wide log fan-out across thousands of tasks trips
    Logarithm's per-user query rate limit. The root dials the full host list from
    ``MAST_HPC_TASK_GROUP_HOSTNAMES`` regardless of which task/host it runs on, so
    nothing requires it to be ``hosts[0]``; any single task works.
    """
    return os.environ.get("TW_TASK_ID", "0") == "0"


def resolve_ipv6(host: str, port: int) -> str:
    """Resolve ``host`` to a single IPv6 address (we connect by IP, never DNS)."""
    # MAST supplies task FQDNs to this OSS smoke test.
    # ast-grep-ignore: python/python-dns-deps
    infos = socket.getaddrinfo(host, port, socket.AF_INET6, socket.SOCK_DGRAM)
    if not infos:
        raise RuntimeError(f"no IPv6 address for {host}")
    return infos[0][4][0]


def _child_env() -> dict[str, str]:
    # Make the bundled extension importable from the package directory.
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = HERE + (os.pathsep + existing if existing else "")
    return env


def main() -> None:
    python = sys.executable
    smoke = os.path.join(HERE, "smoke.py")
    env = _child_env()
    hosts = task_hosts()
    rank0 = is_rank0()

    print(
        f"[bootstrap] host={socket.getfqdn()} rank0={rank0} "
        f"task_id={os.environ.get('TW_TASK_ID')} hosts={len(hosts)} "
        f"port={PORT} workers_per_host={WORKERS_PER_HOST}",
        flush=True,
    )

    # Every task runs WORKERS_PER_HOST workers on sequential ports.
    workers = [
        subprocess.Popen(
            [python, smoke, "--worker", "--port", str(PORT + i), "--bind", "::"],
            cwd=HERE,
            env=env,
        )
        for i in range(WORKERS_PER_HOST)
    ]

    if not rank0:
        # Plain worker task: serve until the workers shut down (on the root's
        # death notice) or the job is torn down. Fail if any worker fails.
        codes = [w.wait() for w in workers]
        sys.exit(max(codes) if codes else 0)

    # Rank 0 also runs the root. Give every worker a moment to bind first. The
    # root dials every (host, port) pair across the whole job.
    time.sleep(STARTUP_GRACE)
    addrs = [
        f"[{resolve_ipv6(h, PORT)}]:{PORT + i}"
        for h in hosts
        for i in range(WORKERS_PER_HOST)
    ]
    print(f"[bootstrap] root dialing {len(addrs)} workers", flush=True)

    # Pass addresses via a file, not argv: at tens of thousands of workers the
    # combined command line exceeds the OS ARG_MAX limit (E2BIG). Write it to a
    # writable temp dir — the fbpkg install dir (HERE) is a read-only mount.
    addr_file = os.path.join(tempfile.gettempdir(), "mm_root_hosts.txt")
    with open(addr_file, "w") as f:
        f.write("\n".join(addrs))
    rc = subprocess.call([python, smoke, "--root-file", addr_file], cwd=HERE, env=env)
    print(f"[bootstrap] root finished with code {rc}", flush=True)

    # The smoke test is done; tear down our local workers and exit with its status.
    for w in workers:
        w.terminate()
    for w in workers:
        try:
            w.wait(timeout=10)
        except subprocess.TimeoutExpired:
            w.kill()
    sys.exit(rc)


if __name__ == "__main__":
    main()
