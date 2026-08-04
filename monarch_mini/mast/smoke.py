#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Multi-machine smoke test for minimonarch over QUIC (TLS-encrypted).

Topology: a single root node connects to N worker nodes. Each worker *serves*
a quic:// listener; the root *joins* (connects out to) every worker. Once all
workers are connected each timed round exercises two message patterns:

  * *direct*: the root sends every worker a ping and each replies with its own
    identity (a star of independent round-trips through the root).
  * *ring*: the root wires each worker to its successor (the last worker back to
    the root), then injects a token at the first worker carrying an integer count.
    Each worker logs the count it saw and forwards count+1 to its next hop, so the
    token threads the whole ring hop-by-hop and returns to the root as the worker
    count. This exercises worker-to-worker routing, not just root round-trips.

Every phase is timed. Every message carries its type as a header part (see the
``H_*`` constants) so dispatch is an exact header match, never a payload sniff.

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

# Message-type headers: part[0] of every application message. Dispatch is an exact
# match on this header, so we never sniff a payload to learn a message's type.
# H_FAIL is special: it is the failure *prefix* both the worker's serve and the
# root's join register, so minimonarch delivers a severed link as
# [H_FAIL, dead_ident, reason] — the same shape as our own messages.
H_FAIL = b"h:fail"  # [H_FAIL, dead_ident, reason]   a connection severed
H_PING = b"h:ping"  # [H_PING]                        root -> worker, direct round
H_REPLY = b"h:reply"  # [H_REPLY, worker_ident]       worker -> root, direct reply
H_NEXT = b"h:next"  # [H_NEXT, next_hop_ident]         root -> worker, ring wiring
H_RING = b"h:ring"  # [H_RING, count]                 circulating ring token (count
#                                                      is a decimal-ascii integer)

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


def _advertised_host() -> str:
    """This worker's own routable IPv6 address, for the dialable gateway ``@tag``.

    Siblings need a concrete address to reach us for delegated heartbeats, and it
    must be the *same* address the root reaches us at (the root dials each peer by
    resolving its FQDN to IPv6 — see ``resolve_ipv6`` in ``mast_bootstrap.py``).
    Resolving our own FQDN yields exactly that address, so a sibling's side channel
    lands on the same listener the root uses.
    """
    host = socket.getfqdn()
    # MAST peers need this task's concrete FQDN address; this is not service discovery.
    # ast-grep-ignore: python/python-dns-deps
    infos = socket.getaddrinfo(host, None, socket.AF_INET6, socket.SOCK_DGRAM)
    if not infos:
        raise RuntimeError(f"no IPv6 address for {host}")
    return infos[0][4][0]


def worker_ident(port: int) -> bytes:
    """A unique, human-readable identity for this worker process.

    The ident always carries a dialable gateway ``@tag`` (``...@[<host>]:<port>``)
    built from this worker's own routable IPv6 (see ``_advertised_host``) so a
    *sibling* worker can open a side channel to us — which is what lets the root
    delegate our heartbeat to a sibling.
    """
    return (
        f"worker-{socket.gethostname()}-{port}@[{_advertised_host()}]:{port}".encode()
    )


def _failure_notice(parts: list) -> tuple[str, str]:
    """Decode an ``[H_FAIL, dead_ident, reason]`` notice into (who, reason) strings.

    Both the worker's serve and the root's join register ``H_FAIL`` as their failure
    prefix, so a severed link arrives with the dead ident at ``parts[1]`` and the
    reason at ``parts[2]`` — the header at ``parts[0]`` has already been matched by
    the caller."""
    who = bytes(parts[1]).decode(errors="replace") if len(parts) > 1 else "?"
    reason = bytes(parts[2]).decode(errors="replace") if len(parts) > 2 else "?"
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

    The worker registers ``H_FAIL`` as its failure prefix, so when the root sends
    its explicit "context shutdown" notice the severed parent link delivers
    ``[H_FAIL, root_ident, reason]``. The worker then calls ``minimonarch.close()``
    to tear its own context down gracefully — which sends its shutdown notice back
    to the root, the response the root waits for before it closes that connection.

    Beyond the direct ping/reply, the worker participates in the ring: an ``H_NEXT``
    message wires its successor, and each ``H_RING`` token is logged and forwarded
    (count+1) to that successor. Ring routing is plain destination routing — a token
    to a sibling or the root climbs to the common ancestor (the root) and back down.
    """
    ident = worker_ident(port)
    tag = f"[worker :{port}]"
    url = f"quic://[{bind}]:{port}"
    me = Actor(ident)
    me.serve(url, "child", failure=[ba(H_FAIL)])
    # pid lets us map the Rust writer's MM_HB heartbeat-send lines (tagged by pid)
    # back to this worker's port when correlating send vs. receive at scale.
    log(f"{tag} serving {url} pid={os.getpid()}")

    # Establishment hello: [self_ident, root_ident].
    hello = await me.next()
    root_ident = bytes(hello[1])
    log(f"{tag} connected to root {root_ident.decode()}")

    # Our successor in the ring, learned from the root's H_NEXT wiring. The root
    # wires the whole ring before injecting any token, so this is set before the
    # first H_RING arrives.
    next_hop: bytes | None = None

    # Dispatch on the message header. A death notice for the root (the registered
    # H_FAIL failure) ends the process.
    while True:
        msg = await me.next()
        header = bytes(msg[0])
        if header == H_FAIL:
            reason = bytes(msg[2]).decode() if len(msg) > 2 else "unknown"
            log(f"{tag} root {root_ident.decode()} gone ({reason}); shutting down")
            # Graceful close: replies to the root's shutdown so it can close
            # promptly instead of waiting out its ack timeout.
            minimonarch.close()
            return
        if header == H_PING:
            log(f"{tag} ping; replying")
            me.send(root_ident, [ba(H_REPLY), ba(ident)])
        elif header == H_NEXT:
            next_hop = bytes(msg[1])
            log(f"{tag} ring wired: next hop = {next_hop.decode()}")
        elif header == H_RING:
            count = int(bytes(msg[1]))
            log(f"{tag} ring: had count {count}")
            assert next_hop is not None, "ring token arrived before H_NEXT wiring"
            me.send(next_hop, [ba(H_RING), ba(str(count + 1).encode())])
        else:
            log(f"{tag} unexpected header {header!r}; ignoring")


async def _direct_round(root: Actor, workers: list[bytes], timeout: float) -> int:
    """Send one ping to every worker, await each reply, return the failure count.

    Each connected worker yields exactly one terminal message per round: a reply
    (``[H_REPLY, worker_ident]``) or a failure notice (``[H_FAIL, worker_ident,
    reason]``) if its connection dropped (e.g. a heartbeat timeout severed it).
    Raises ``asyncio.TimeoutError`` if a worker neither replies nor fails within
    ``timeout`` — the signal that a connection silently stalled.
    """
    for wid in workers:
        root.send(wid, [ba(H_PING)])
    failures = 0
    for _ in range(len(workers)):
        msg = await asyncio.wait_for(root.next(), timeout)
        if bytes(msg[0]) == H_REPLY:
            continue
        who, reason = _failure_notice(msg)
        failures += 1
        log(f"[root] FAILURE (reply) {who}: {reason}")
    return failures


async def _ring_round(
    root: Actor, workers: list[bytes], timeout: float, heads: list[int]
) -> int:
    """Inject a token at each chain head and wait for every chain to return.

    ``heads`` are the chain-start indices. Each token's count starts at 0; workers
    log the count they saw and forward count+1 to their wired next hop, which the
    root set to itself at each chain boundary. A chain of ``L`` workers therefore
    returns ``[H_RING, L]`` to the root, so the counts across all chains sum to the
    worker count. The root expects exactly ``len(heads)`` replies; a severed link
    substitutes an ``[H_FAIL, ...]`` notice (or the await times out if a token
    stalled). Returns the failure count.
    """
    for h in heads:
        root.send(workers[h], [ba(H_RING), ba(b"0")])
    total = 0
    failures = 0
    for _ in range(len(heads)):
        msg = await asyncio.wait_for(root.next(), timeout)
        if bytes(msg[0]) == H_RING:
            total += int(bytes(msg[1]))
            continue
        who, reason = _failure_notice(msg)
        log(f"[root] FAILURE (ring) {who}: {reason}")
        failures += 1
    # Every worker must have been visited exactly once across all chains.
    if not failures and total != len(workers):
        log(f"[root] ERROR: ring visited {total} workers, expected {len(workers)}")
        return 1
    return failures


async def run_root(
    hosts: list[str],
    timeout: float,
    connect_timeout: float,
    duration: float = 0.0,
    send_interval: float = 1.0,
    chain_length: int = 0,
    end_sleep: float = 0.0,
) -> int:
    """Dial every worker, time connection + round-trip, and report.

    ``connect_timeout`` bounds the connect phase (longer, since with hundreds of
    hosts an occasional one is slow to boot); ``timeout`` bounds each reply.

    If ``duration`` > 0 the ping/reply phase repeats for that many wall-clock
    seconds, one round every ``send_interval`` seconds, instead of a single round.
    A ``send_interval`` larger than the heartbeat timeout leaves gaps with *no*
    data traffic, so the connections stay up only if heartbeats (direct and
    delegated) keep them alive — which is exactly what this exercises.

    ``chain_length`` caps the ring into independent chains of at most that many
    workers (``<= 0`` = a single chain of all workers). Each chain returns to the
    root on its own, so at scale ring latency is bounded by the chain length rather
    than the whole worker count, and one broken link fails only its own chain.

    ``end_sleep`` > 0 adds a final heartbeat-only settling window after the last
    round: the root stops sending data and simply watches for that long, so links
    survive purely on heartbeats. Sizing it above the heartbeat timeout confirms
    steady-state heartbeating holds and keeps a teardown-race severance from being
    misread as a real failure. Any message arriving in the window is a failure.

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
            # Register H_FAIL as the failure prefix so a severed worker link is
            # delivered as [H_FAIL, worker_ident, reason] — a header we dispatch on
            # instead of sniffing the payload.
            root.join(f"quic://{host}", "parent", failure=[ba(H_FAIL)])

        # Every failure notice the root receives (a severed connection to a
        # worker) is printed as it arrives so we can see exactly which workers
        # dropped and why. `failures` keeps a running total across both phases.
        workers: list[bytes] = []
        failures = 0
        # Loop until every worker has connected. A message is either an
        # establishment hello ([b"root", worker_ident]) or a failure notice
        # ([H_FAIL, worker_ident, reason]); only hellos count toward `total`,
        # failures are printed and tallied.
        while len(workers) < total:
            try:
                msg = await asyncio.wait_for(root.next(), connect_timeout)
            except asyncio.TimeoutError:
                log(
                    f"[root] ERROR: only {len(workers)}/{total} workers "
                    f"connected within {connect_timeout:.0f}s ({failures} failures)"
                )
                return 1
            if bytes(msg[0]) == b"root":
                # Establishment hello ([b"root", worker_ident]): record who
                # connected. Progress is printed only at decile boundaries
                # (bounded prints) so it stays cheap.
                workers.append(bytes(msg[1]))
                _report_progress("connected", len(workers), total, step)
            else:
                who, reason = _failure_notice(msg)
                failures += 1
                log(f"[root] FAILURE (connect) {who}: {reason}")
        connect_ms = (time.monotonic() - t_join) * 1e3
        log(f"[root] all {len(workers)} workers connected in {connect_ms:.1f} ms")

        # Wire the ring once, capped into chains of at most `cap` workers: a
        # worker's next hop is the root (not its normal successor) at each chain
        # boundary and at the very end. `heads` are the chain-start indices where
        # the root injects a token. Done before any token is injected so every
        # worker knows its next hop before a token can reach it.
        cap = chain_length if chain_length > 0 else len(workers)
        heads = list(range(0, len(workers), cap))
        for i, wid in enumerate(workers):
            boundary = (i + 1) % cap == 0 or i + 1 == len(workers)
            nxt = b"root" if boundary else workers[i + 1]
            root.send(wid, [ba(H_NEXT), ba(nxt)])
        log(
            f"[root] wired {len(heads)} chain(s) of <= {cap} over {len(workers)} workers"
        )

        # --- Phase 2: each round runs a direct pass then a ring pass, once or
        # repeatedly for `duration` seconds --
        t_send = time.monotonic()
        deadline = t_send + max(duration, 0.0)
        rounds = 0
        try:
            while True:
                rounds += 1
                t_direct = time.monotonic()
                failures += await _direct_round(root, workers, timeout)
                if failures:
                    break
                direct_ms = (time.monotonic() - t_direct) * 1e3
                t_ring = time.monotonic()
                failures += await _ring_round(root, workers, timeout, heads)
                if failures:
                    break
                ring_ms = (time.monotonic() - t_ring) * 1e3
                log(
                    f"[root]   round {rounds} ok ({len(workers)} workers): "
                    f"direct {direct_ms:.1f} ms, ring {ring_ms:.1f} ms "
                    f"({len(heads)} chain(s))"
                )
                if time.monotonic() >= deadline:
                    break
                # Idle gap: no data flows, so only heartbeats keep the links up.
                await asyncio.sleep(send_interval)
        except asyncio.TimeoutError:
            log(
                f"[root] ERROR: a worker stalled (no reply within {timeout:.0f}s) "
                f"in round {rounds} ({failures} failures so far)"
            )
            return 1
        roundtrip_ms = (time.monotonic() - t_send) * 1e3

        # Final heartbeat-only settling window: with the root no longer sending
        # data, links survive purely on heartbeats. Idling here (>= the heartbeat
        # timeout) before teardown confirms steady-state heartbeating holds and
        # avoids attributing a teardown-race severance to a real failure. Any
        # message that arrives in this window is a failure notice.
        if end_sleep > 0 and failures == 0:
            log(f"[root] idle {end_sleep:.1f}s (heartbeat-only) before teardown...")
            idle_deadline = time.monotonic() + end_sleep
            while True:
                remaining = idle_deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(root.next(), remaining)
                except asyncio.TimeoutError:
                    break
                who, reason = _failure_notice(msg)
                failures += 1
                log(f"[root] FAILURE (idle) {who}: {reason}")
            log(f"[root] idle window done ({failures} failures)")

        log("[root] --- summary ---")
        log(f"[root] connect:    {connect_ms:.1f} ms ({len(workers)} workers)")
        log(f"[root] round-trip: {roundtrip_ms:.1f} ms ({rounds} round(s))")
        log(f"[root] failures:   {failures}")
        # Success only if nothing severed across every round.
        return 0 if failures == 0 else 1
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
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="root: repeat the ping/reply phase for this many seconds (default: 0 "
        "= a single round). Use a value well over the heartbeat timeout to keep the "
        "run alive across many heartbeat cycles.",
    )
    parser.add_argument(
        "--send-interval",
        type=float,
        default=1.0,
        help="root: seconds between ping rounds when --duration > 0 (default: 1). "
        "Set larger than the heartbeat timeout so heartbeats, not data, hold the "
        "links open between rounds.",
    )
    parser.add_argument(
        "--chain-length",
        type=int,
        default=0,
        help="root: cap the ring into independent chains of at most this many "
        "workers, each returning to the root on its own (default: 0 = one chain of "
        "all workers). Bounds ring latency and blast radius at large worker counts.",
    )
    parser.add_argument(
        "--end-sleep",
        type=float,
        default=0.0,
        help="root: after the last round, idle for this many seconds in a "
        "heartbeat-only window before tearing down (default: 0 = none). Set above "
        "the heartbeat timeout to confirm heartbeats hold with no data flowing and "
        "avoid teardown-race false failures.",
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
        sys.exit(
            asyncio.run(
                run_root(
                    hosts,
                    args.timeout,
                    args.connect_timeout,
                    args.duration,
                    args.send_interval,
                    args.chain_length,
                    args.end_sleep,
                )
            )
        )


if __name__ == "__main__":
    main()
