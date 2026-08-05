#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Multi-machine smoke test for minimonarch over QUIC (TLS-encrypted).

Topology: a single root node connects to N worker nodes. Each worker *serves*
a quic:// listener; the root *joins* (connects out to) every worker. Once all
workers are connected the root sends one message to each worker, and each
worker replies with its own identity. Every phase is timed.

Worker IPs are not known until runtime and we never use DNS: the root is given
the worker addresses directly on the command line (or, on MAST, via an env var)
and dials them by IPv6 address.

Usage:

    # On each worker machine (binds all interfaces on PORT):
    python smoke.py --worker --port 26600

    # On the root machine, given the worker addresses:
    python smoke.py --root "[2401:db00::1]:26600" "[2401:db00::2]:26600"

TLS: the quic transport reads its material from MM_QUIC_CERT / MM_QUIC_KEY /
MM_QUIC_CA. The client verifies the server certificate against the CA for a
*fixed* server name ("monarch-mini", baked into the transport), so a single
shared cert/key/ca set works on every machine regardless of its IP -- no
per-host certificate signing and no CA private key distribution is required.
See ``ensure_quic_certs`` and the README section in this file's module docstring.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import os
import socket
import sys
import time

import minimonarch
from minimonarch import Actor

ba = minimonarch.bytearray

# The fixed server name the transport's TLS client verifies against; the shipped
# leaf cert carries it as a SAN. Documented here so the cert story is discoverable
# from Python, but not otherwise used by this script.
SERVER_NAME = "monarch-mini"

# Sentinel prefix a worker registers as its connection-failure message. When the
# root tears down (clean exit or crash) the worker's parent link severs and
# minimonarch delivers [_DOWN, root_ident, reason]; the worker uses the prefix to
# tell that death notice apart from a normal ping and shut itself down.
_DOWN = b"__worker_down__"

# Short hostname, included in every log line so multi-host logs are easy to read.
_HOST = socket.gethostname()


def log(msg: str) -> None:
    """Print a flushed, wall-clock-timestamped, host-tagged log line.

    The timestamp (with milliseconds) and host let us correlate events across
    machines when debugging at scale; clocks are NTP-synced closely enough for
    sub-second ordering within a short run.
    """
    ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"{ts} {_HOST} {msg}", flush=True)


def ensure_quic_certs(certs_dir: str | None) -> None:
    """Point the quic transport at a cert/key/ca set via its env vars.

    If MM_QUIC_CERT / MM_QUIC_KEY / MM_QUIC_CA are already set we leave them
    alone. Otherwise we search ``certs_dir`` (if given), then a ``certs``
    directory next to this script, then the repo's ``test_certs`` directory, and
    use the first that holds all three PEM files.
    """
    needed = ("MM_QUIC_CERT", "MM_QUIC_KEY", "MM_QUIC_CA")
    if all(os.environ.get(v) for v in needed):
        return

    here = os.path.dirname(os.path.abspath(__file__))
    candidates: list[str] = []
    if certs_dir:
        candidates.append(certs_dir)
    candidates.append(os.path.join(here, "certs"))
    candidates.append(os.path.join(here, "..", "test_certs"))

    for d in candidates:
        cert = os.path.join(d, "cert.pem")
        key = os.path.join(d, "key.pem")
        ca = os.path.join(d, "ca.pem")
        if os.path.exists(cert) and os.path.exists(key) and os.path.exists(ca):
            os.environ["MM_QUIC_CERT"] = cert
            os.environ["MM_QUIC_KEY"] = key
            os.environ["MM_QUIC_CA"] = ca
            log(f"[certs] using TLS material from {os.path.abspath(d)}")
            return

    raise SystemExit(
        "no quic certs found: set MM_QUIC_CERT/MM_QUIC_KEY/MM_QUIC_CA or pass "
        f"--certs-dir (looked in: {', '.join(candidates)})"
    )


def worker_ident(port: int) -> bytes:
    """A unique, human-readable identity for this worker process."""
    return f"worker-{socket.gethostname()}-{port}".encode()


def _failure_notice(parts: list) -> tuple[str, str]:
    """Decode a failure notice the root receives when a connection to a worker
    severs. The root joined each worker with no failure prefix, so minimonarch
    delivers ``[worker_ident, reason]`` (unlike the worker, which registered the
    ``_DOWN`` prefix). Returns (worker, reason) as strings for logging."""
    who = bytes(parts[0]).decode(errors="replace") if parts else "?"
    reason = bytes(parts[1]).decode(errors="replace") if len(parts) > 1 else "?"
    return who, reason


def _report_progress(label: str, done: int, total: int, step: int) -> None:
    """Print a ``label`` progress line at most ~11 times for the whole loop.

    Only prints on a decile boundary (every ``step``) or on the final item, so
    the number of prints stays bounded as the host count ramps up and does not
    skew the timed phase.
    """
    if done % step == 0 or done == total:
        pct = 100 * done // total
        log(f"[root]   {label} {done}/{total} ({pct}%)")


async def run_worker(port: int, bind: str) -> None:
    """Serve a quic listener, answer round-trips, and exit when the root dies.

    The worker is the *child* of the root: it serves (listens) and the root
    joins (dials). On each established link the worker learns the root's identity
    from the establishment hello, then echoes back its own identity so the root
    can report exactly who replied.

    The worker registers ``_DOWN`` as its failure prefix, so when the root sends
    its explicit "context shutdown" notice the severed parent link delivers
    ``[_DOWN, root_ident, reason]``. The worker then calls ``minimonarch.close()``
    to tear its own context down gracefully — which sends its shutdown notice back
    to the root, the response the root waits for before it closes that connection.
    """
    ident = worker_ident(port)
    tag = f"[worker :{port}]"
    url = f"quic://[{bind}]:{port}"
    me = Actor(ident)
    me.serve(url, "child", failure=[ba(_DOWN)])
    # pid lets us map the Rust writer's MM_HB heartbeat-send lines (tagged by pid)
    # back to this worker's port when correlating send vs. receive at scale.
    log(f"{tag} serving {url} pid={os.getpid()}")

    # Establishment hello: [self_ident, root_ident].
    hello = await me.next()
    root_ident = bytes(hello[1])
    log(f"{tag} connected to root {root_ident.decode()}")

    # Answer every message the root sends with our identity. A death notice for
    # the root (the registered _DOWN failure) ends the process.
    while True:
        msg = await me.next()
        if msg and bytes(msg[0]) == _DOWN:
            reason = bytes(msg[2]).decode() if len(msg) > 2 else "unknown"
            log(f"{tag} root {root_ident.decode()} gone ({reason}); shutting down")
            # Graceful close: replies to the root's shutdown so it can close
            # promptly instead of waiting out its ack timeout.
            minimonarch.close()
            return
        log(f"{tag} got message; replying")
        me.send(root_ident, [ba(b"hello from " + ident)])


async def run_root(hosts: list[str], timeout: float, connect_timeout: float) -> int:
    """Dial every worker, time connection + round-trip, and report.

    ``connect_timeout`` bounds the connect phase (longer, since with hundreds of
    hosts an occasional one is slow to boot); ``timeout`` bounds each reply.
    Returns a process exit code (0 on full success).
    """
    root = Actor(b"root")
    try:
        # --- Phase 1: connect to all workers ---------------------------------
        total = len(hosts)
        step = max(1, total // 10)
        log(f"[root] joining {total} worker(s)...")
        t_join = time.monotonic()
        for host in hosts:
            root.join(f"quic://{host}", "parent")

        # Every failure notice the root receives (a severed connection to a
        # worker) is printed as it arrives so we can see exactly which workers
        # dropped and why. `failures` keeps a running total across both phases.
        workers: list[bytes] = []
        failures = 0
        # Loop until every worker has connected. A message is either an
        # establishment hello ([b"root", worker_ident]) or a failure notice
        # ([worker_ident, reason]); only hellos count toward `total`, failures
        # are printed and tallied.
        while len(workers) < total:
            try:
                msg = await asyncio.wait_for(root.next(), connect_timeout)
            except asyncio.TimeoutError:
                log(
                    f"[root] ERROR: only {len(workers)}/{total} workers "
                    f"connected within {connect_timeout:.0f}s ({failures} failures)"
                )
                return 1
            if len(msg) >= 2 and bytes(msg[0]) == b"root":
                # Establishment hello: record who connected. Progress is printed
                # only at decile boundaries (bounded prints) so it stays cheap.
                workers.append(bytes(msg[1]))
                _report_progress("connected", len(workers), total, step)
            else:
                who, reason = _failure_notice(msg)
                failures += 1
                log(f"[root] FAILURE (connect) {who}: {reason}")
        connect_ms = (time.monotonic() - t_join) * 1e3
        log(f"[root] all {len(workers)} workers connected in {connect_ms:.1f} ms")

        # --- Phase 2: one message to each worker, await each reply -----------
        t_send = time.monotonic()
        for wid in workers:
            root.send(wid, [ba(b"ping")])
        log(f"[root] sent ping to {len(workers)} workers")

        # Each connected worker yields exactly one terminal message: a reply
        # ([b"hello from ..."]) or a failure notice ([worker_ident, reason]) if
        # its connection dropped after connecting. Consume that many messages,
        # counting replies (progress at decile boundaries) and printing every
        # failure as it arrives.
        replies = 0
        reply_failures = 0
        for _ in range(len(workers)):
            try:
                msg = await asyncio.wait_for(root.next(), timeout)
            except asyncio.TimeoutError:
                log(
                    f"[root] ERROR: only {replies}/{len(workers)} replies within "
                    f"{timeout:.0f}s ({failures + reply_failures} failures)"
                )
                return 1
            if msg and bytes(msg[0]).startswith(b"hello from "):
                replies += 1
                _report_progress("replies", replies, len(workers), step)
            else:
                who, reason = _failure_notice(msg)
                reply_failures += 1
                log(f"[root] FAILURE (reply) {who}: {reason}")
        failures += reply_failures
        roundtrip_ms = (time.monotonic() - t_send) * 1e3

        log("[root] --- summary ---")
        log(f"[root] connect:    {connect_ms:.1f} ms ({len(workers)} workers)")
        log(f"[root] round-trip: {roundtrip_ms:.1f} ms ({replies} replies)")
        log(f"[root] failures:   {failures}")
        # Success only if every worker replied and nothing severed along the way.
        return 0 if replies == len(workers) and failures == 0 else 1
    finally:
        # Gracefully tear down the context *inside the event loop*: this flushes
        # an "actor destroyed" death notice to every worker before the loop
        # thread stops, so workers shut down promptly instead of waiting out the
        # quic heartbeat timeout. Runs on success, timeout, and error paths.
        minimonarch.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--root",
        nargs="+",
        metavar="HOST",
        help="run as the root and connect to these worker addresses "
        "(e.g. '[::1]:26600')",
    )
    mode.add_argument(
        "--root-file",
        metavar="PATH",
        help="run as the root, reading worker addresses (one per line) from PATH. "
        "Use this instead of --root at high host counts: tens of thousands of "
        "addresses on argv exceed the OS ARG_MAX limit.",
    )
    mode.add_argument(
        "--worker", action="store_true", help="run as a worker (serve a listener)"
    )
    parser.add_argument(
        "--port", type=int, default=26600, help="worker listen port (default: 26600)"
    )
    parser.add_argument(
        "--bind",
        default="::",
        help="worker bind address (default: '::' = all IPv6 interfaces)",
    )
    parser.add_argument(
        "--certs-dir", default=None, help="directory holding cert.pem/key.pem/ca.pem"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="root: seconds to wait for each reply (default: 60)",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=180.0,
        help="root: seconds to wait for all workers to connect (default: 180); "
        "larger than --timeout because with many hosts one can be slow to boot",
    )
    args = parser.parse_args()

    ensure_quic_certs(args.certs_dir)

    if args.worker:
        asyncio.run(run_worker(args.port, args.bind))
    else:
        if args.root_file:
            with open(args.root_file) as f:
                hosts = [line.strip() for line in f if line.strip()]
        else:
            hosts = args.root
        sys.exit(asyncio.run(run_root(hosts, args.timeout, args.connect_timeout)))


if __name__ == "__main__":
    main()
