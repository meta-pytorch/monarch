#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Run the minimonarch smoke test locally, on one host, with delegated heartbeats.

This is the single-host analogue of ``mast_bootstrap.py``: it starts N worker
processes on sequential localhost ports and one root process that dials them all.
It is meant for exercising the *delegated* heartbeat path without a cluster:

  * ``--max-direct`` (``MM_QUIC_MAX_DIRECT_CHILDREN``) is set well below the worker
    count, so the root cannot heartbeat every worker directly and must delegate the
    excess onto that many sibling workers (cover hosts), balanced across them.
  * ``--hb-interval-ms`` / ``--hb-timeout-ms`` speed the heartbeat cadence and
    timeout way down (defaults 300 / 1200) so a run of a few seconds spans many
    heartbeat cycles.
  * The root pings in a loop for ``--duration`` seconds with ``--send-interval``
    (default 2s, > the timeout) between rounds, so between rounds *no data flows*
    and the links survive only if heartbeats — direct and delegated — keep them
    alive. A broken delegated heartbeat would sever a worker and fail the run.

Usage (from the python/ dir so `minimonarch` is importable, e.g. via `uv run`):

    uv run python ../mast/local_smoke.py --workers 8 --duration 12
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SMOKE = os.path.join(HERE, "smoke.py")


def child_env(args: argparse.Namespace) -> dict[str, str]:
    """Environment for the worker/root children: heartbeat + delegation tunables."""
    env = dict(os.environ)
    env["MM_QUIC_MAX_DIRECT_CHILDREN"] = str(args.max_direct)
    env["MM_QUIC_HEARTBEAT_INTERVAL_MS"] = str(args.hb_interval_ms)
    env["MM_QUIC_HEARTBEAT_TIMEOUT_MS"] = str(args.hb_timeout_ms)
    if args.debug:
        env["MM_QUIC_DEBUG"] = "1"
    return env


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workers", type=int, default=8, help="worker count (default: 8)"
    )
    parser.add_argument(
        "--port", type=int, default=26700, help="base port (default: 26700)"
    )
    parser.add_argument(
        "--max-direct",
        type=int,
        default=2,
        help="MM_QUIC_MAX_DIRECT_CHILDREN: children the root keeps direct; the rest "
        "are delegated onto them, balanced (default: 2). Keep < --workers to force "
        "delegation.",
    )
    parser.add_argument("--hb-interval-ms", type=int, default=300)
    parser.add_argument("--hb-timeout-ms", type=int, default=1200)
    parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="direct+ring rounds the root runs at each sweep size (default: 5)",
    )
    parser.add_argument(
        "--chain-length",
        type=int,
        default=0,
        help="cap the ring into chains of at most this many workers (default: 0 = "
        "one chain of all workers)",
    )
    parser.add_argument("--startup-grace", type=float, default=2.0)
    parser.add_argument(
        "--debug", action="store_true", help="set MM_QUIC_DEBUG=1 (MM_HB logs)"
    )
    parser.add_argument(
        "--logdir",
        default=None,
        help="if set, each worker/root writes its own stdout+stderr file here "
        "(untangles the interleaved multi-process output)",
    )
    args = parser.parse_args()

    env = child_env(args)
    ports = [args.port + i for i in range(args.workers)]

    def out_for(name: str):
        """Per-process output file (or None → inherit our stdout/stderr)."""
        if not args.logdir:
            return None
        os.makedirs(args.logdir, exist_ok=True)
        return open(os.path.join(args.logdir, f"{name}.log"), "w")

    print(
        f"[local] {args.workers} workers on ::1 ports {ports[0]}..{ports[-1]}; "
        f"max_direct={args.max_direct} "
        f"hb={args.hb_interval_ms}/{args.hb_timeout_ms}ms "
        f"sweep rounds={args.rounds}",
        flush=True,
    )

    worker_outs = [out_for(f"worker-{p}") for p in ports]
    workers = [
        subprocess.Popen(
            # Each worker binds all interfaces (default) and auto-advertises its own
            # routable IPv6 as a dialable @tag, so the root can delegate its heartbeat
            # to a sibling (siblings dial each other at that advertised address).
            [
                sys.executable,
                SMOKE,
                "--worker",
                "--port",
                str(p),
            ],
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT if out else None,
        )
        for p, out in zip(ports, worker_outs)
    ]
    try:
        time.sleep(args.startup_grace)  # let every worker bind before dialing
        addrs = [f"[::1]:{p}" for p in ports]
        root_out = out_for("root")
        rc = subprocess.call(
            [
                sys.executable,
                SMOKE,
                "--root",
                *addrs,
                "--min-rounds",
                str(args.rounds),
                "--chain-length",
                str(args.chain_length),
                "--timeout",
                "5",
                "--connect-timeout",
                "30",
            ],
            env=env,
            stdout=root_out,
            stderr=subprocess.STDOUT if root_out else None,
        )
        print(f"[local] root finished with code {rc}", flush=True)
        sys.exit(rc)
    finally:
        for w in workers:
            w.terminate()
        for w in workers:
            try:
                w.wait(timeout=10)
            except subprocess.TimeoutExpired:
                w.kill()


if __name__ == "__main__":
    main()
