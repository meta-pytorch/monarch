#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Driver for the big-message vs. heartbeat stress (see bench.py).

Spawns three *separate* processes — root, child-a, child-b — so their event loops
run on independent interpreters and never contend on the GIL (the whole point is to
actually saturate the QUIC links, which a single-process GIL-bound run could not).

It forces delegation and a short heartbeat window via the environment:

  * ``MM_QUIC_MAX_DIRECT_CHILDREN=1`` — the root keeps a direct, delegates b onto a,
    so b's heartbeat rides the a<->b side channels.
  * ``MM_QUIC_HEARTBEAT_TIMEOUT_MS`` — kept *below* a single big message's transfer
    time (bench.py measures and reports the actual transfer time so you can confirm).

Usage (from the python/ dir so the extension is importable, e.g. via uv):

    uv run python ../big_messages/run.py --msg-mb 1024 --duration 20
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(HERE, "bench.py")
TEST_CERTS = os.path.join(HERE, "..", "test_certs")


def child_env(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    env["MM_QUIC_MAX_DIRECT_CHILDREN"] = str(args.max_direct)
    env["MM_QUIC_HEARTBEAT_INTERVAL_MS"] = str(args.hb_interval_ms)
    env["MM_QUIC_HEARTBEAT_TIMEOUT_MS"] = str(args.hb_timeout_ms)
    env["BENCH_MSG_BYTES"] = str(args.msg_mb * 1024 * 1024)
    env["BENCH_POOL"] = str(args.pool)
    # Certs: reuse the repo's fixture set unless already pointed elsewhere.
    for var, pem in (
        ("MM_QUIC_CERT", "cert.pem"),
        ("MM_QUIC_KEY", "key.pem"),
        ("MM_QUIC_CA", "ca.pem"),
    ):
        env.setdefault(var, os.path.join(TEST_CERTS, pem))
    if args.debug:
        env["MM_QUIC_DEBUG"] = "1"
    return env


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--msg-mb", type=int, default=1024, help="message size (MiB)")
    parser.add_argument(
        "--pool",
        type=int,
        default=1,
        help="big buffers each sender keeps in flight per destination (default 1); "
        ">1 saturates the stream more but uses proportionally more memory",
    )
    parser.add_argument("--duration", type=float, default=20.0, help="hammer seconds")
    parser.add_argument(
        "--max-direct",
        type=int,
        default=1,
        help="MM_QUIC_MAX_DIRECT_CHILDREN (default 1: b is delegated onto a)",
    )
    parser.add_argument("--hb-interval-ms", type=int, default=250)
    parser.add_argument(
        "--hb-timeout-ms",
        type=int,
        default=1000,
        help="keep below a single message's transfer time (bench.py reports it)",
    )
    parser.add_argument("--port-a", type=int, default=26810)
    parser.add_argument("--port-b", type=int, default=26811)
    parser.add_argument("--startup-grace", type=float, default=2.0)
    parser.add_argument("--debug", action="store_true", help="MM_QUIC_DEBUG=1")
    parser.add_argument(
        "--logdir", default=None, help="write each process's output to its own file"
    )
    args = parser.parse_args()

    env = child_env(args)
    a_addr = f"[::1]:{args.port_a}"
    b_addr = f"[::1]:{args.port_b}"

    def out_for(name: str):
        if not args.logdir:
            return None
        os.makedirs(args.logdir, exist_ok=True)
        return open(os.path.join(args.logdir, f"{name}.log"), "w")

    print(
        f"[bench] msg={args.msg_mb} MiB pool={args.pool} duration={args.duration}s "
        f"max_direct={args.max_direct} hb={args.hb_interval_ms}/{args.hb_timeout_ms}ms "
        f"a={a_addr} b={b_addr}",
        flush=True,
    )

    def spawn(role_args: list[str], name: str) -> subprocess.Popen:
        out = out_for(name)
        return subprocess.Popen(
            [sys.executable, BENCH, *role_args],
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT if out else None,
        )

    # Workers first (they serve); then the root (it dials).
    a = spawn(
        ["--role", "a", "--port", str(args.port_a), "--peer-port", str(args.port_b)],
        "child-a",
    )
    b = spawn(["--role", "b", "--port", str(args.port_b)], "child-b")
    try:
        time.sleep(args.startup_grace)
        root = spawn(
            [
                "--role",
                "root",
                "--a-addr",
                a_addr,
                "--b-addr",
                b_addr,
                "--duration",
                str(args.duration),
            ],
            "root",
        )
        rc = root.wait()
        print(f"[bench] root finished with code {rc}", flush=True)
        sys.exit(rc)
    finally:
        for w in (a, b):
            w.terminate()
        for w in (a, b):
            try:
                w.wait(timeout=10)
            except subprocess.TimeoutExpired:
                w.kill()


if __name__ == "__main__":
    main()
