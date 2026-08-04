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

import math
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
# Direct+ring rounds the root runs at each sweep size (passed as --min-rounds).
SMOKE_ROUNDS = int(os.environ.get("SMOKE_ROUNDS", "10"))
# Cap the ring into independent chains of at most this many workers, each returning
# to the root on its own. Bounds ring latency and blast radius at scale (0 = one
# chain of every worker).
SMOKE_CHAIN_LENGTH = int(os.environ.get("SMOKE_CHAIN_LENGTH", "512"))
# After the final (largest) sweep size, idle the whole connected set with no data
# for this many ms (should exceed the heartbeat timeout), then verify every link
# survived — a heartbeat-only liveness check the back-to-back sweep never does.
# 0 (default) disables it. Passed to smoke.py as --settle-ms.
SMOKE_SETTLE_MS = float(os.environ.get("SMOKE_SETTLE_MS", "0"))
# Coordination port (0 = disabled). When set, rank 0 does NOT read the worker list
# from this job's env; instead it serves a coordination listener on this port and
# waits for an external controller to send the worker addresses. This is how one
# logical run spans multiple MAST jobs (e.g. across regions): the controller unions
# every job's hosts and injects them into the one root it picks. See smoke.py's
# --wait-for-addresses / run_root_coordinated.
SMOKE_COORD_PORT = int(os.environ.get("SMOKE_COORD_PORT", "0"))
# Max sweep size (worker count actually exercised). 0 ⇒ sweep every launched worker.
# Set this BELOW the launched worker count (hosts * WORKERS_PER_HOST) to over-provision:
# the root builds the sweep from the first SMOKE_TARGET workers that connect and
# backfills from the reserve when borrowed (preemptible) hosts drop. See smoke.py's
# --target / run_root.
SMOKE_TARGET = int(os.environ.get("SMOKE_TARGET", "0"))
# Network transport for the worker serve / root join, passed to smoke.py's
# --transport (default quic). The worker and root must agree, so it is read once
# here and applied to both. Validated against smoke.py's choices.
SMOKE_TRANSPORT = os.environ.get("SMOKE_TRANSPORT", "quic")
if SMOKE_TRANSPORT not in ("quic", "tcp"):
    raise SystemExit(
        f"SMOKE_TRANSPORT must be 'quic' or 'tcp', got {SMOKE_TRANSPORT!r}"
    )


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
            [
                python,
                smoke,
                "--worker",
                "--port",
                str(PORT + i),
                "--bind",
                "::",
                "--transport",
                SMOKE_TRANSPORT,
            ],
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

    # Rank 0 also runs the root, in one of two modes.
    if SMOKE_COORD_PORT:
        # Coordinated multi-job mode: don't read this job's env host list at all.
        # Serve a coordination listener and let an external controller supply the
        # full (possibly multi-job / multi-region) worker list. MM_QUIC_MAX_DIRECT_
        # CHILDREN, if it matters, is set globally by the controller via --env, since
        # a single job cannot know the cross-job total.
        print(
            f"[bootstrap] root: coordinated mode, serving coord port "
            f"{SMOKE_COORD_PORT}, {SMOKE_ROUNDS} rounds/size, "
            f"chain-length {SMOKE_CHAIN_LENGTH}",
            flush=True,
        )
        root_cmd = [
            python,
            smoke,
            "--wait-for-addresses",
            str(SMOKE_COORD_PORT),
            "--min-rounds",
            str(SMOKE_ROUNDS),
            "--chain-length",
            str(SMOKE_CHAIN_LENGTH),
            "--settle-ms",
            str(SMOKE_SETTLE_MS),
            "--transport",
            SMOKE_TRANSPORT,
            *(["--target", str(SMOKE_TARGET)] if SMOKE_TARGET > 0 else []),
        ]
        root_env = env
    else:
        # Env-based single-job mode. Give every worker a moment to bind, then sweep
        # the addresses derived from this job's task-group hostnames.
        time.sleep(STARTUP_GRACE)

        # The root is the one actor that heartbeats the whole fleet, so it is the
        # only place MM_QUIC_MAX_DIRECT_CHILDREN matters (leaf workers have no
        # children). Size the keeper set to sqrt(total workers): the root keeps
        # ~sqrt(N) children direct and delegates the rest, balanced, onto them, so
        # both the keeper count and each keeper's delegated load grow as sqrt(N).
        total_workers = len(hosts) * WORKERS_PER_HOST
        # The root heartbeats the *swept* (connected) set, which is SMOKE_TARGET when
        # over-provisioning, else every launched worker. Size the keeper set off that.
        swept = SMOKE_TARGET if SMOKE_TARGET > 0 else total_workers
        max_direct = max(1, round(math.sqrt(swept)))
        root_env = dict(env)
        root_env["MM_QUIC_MAX_DIRECT_CHILDREN"] = str(max_direct)
        print(
            f"[bootstrap] root MM_QUIC_MAX_DIRECT_CHILDREN={max_direct} "
            f"(sqrt of {swept} swept workers; {total_workers} launched)",
            flush=True,
        )
        addrs = [
            f"[{resolve_ipv6(h, PORT)}]:{PORT + i}"
            for h in hosts
            for i in range(WORKERS_PER_HOST)
        ]
        # Pass addresses via a file, not argv: at tens of thousands of workers the
        # combined command line exceeds the OS ARG_MAX limit (E2BIG). Write it to a
        # writable temp dir — the fbpkg install dir (HERE) is a read-only mount.
        addr_file = os.path.join(tempfile.gettempdir(), "mm_root_hosts.txt")
        with open(addr_file, "w") as f:
            f.write("\n".join(addrs))
        print(
            f"[bootstrap] root: perf sweep 2,4,8,..,{len(addrs)}, {SMOKE_ROUNDS} "
            f"rounds/size, chain-length {SMOKE_CHAIN_LENGTH}",
            flush=True,
        )
        root_cmd = [
            python,
            smoke,
            "--root-file",
            addr_file,
            "--min-rounds",
            str(SMOKE_ROUNDS),
            "--chain-length",
            str(SMOKE_CHAIN_LENGTH),
            "--settle-ms",
            str(SMOKE_SETTLE_MS),
            "--transport",
            SMOKE_TRANSPORT,
            *(["--target", str(SMOKE_TARGET)] if SMOKE_TARGET > 0 else []),
        ]

    rc = subprocess.call(root_cmd, cwd=HERE, env=root_env)
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
