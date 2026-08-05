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
import pickle
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
# Coordination headers (multi-job orchestration): an external controller joins the
# root's coordination listener and hands it the worker list, so the root need not
# read it from the environment. See ``run_root_coordinated``.
H_ADDRS = b"h:addrs"  # controller -> root: [H_ADDRS, pickled list[str]] one chunk
H_ADDRS_END = b"h:addrs-end"  # controller -> root: [H_ADDRS_END]  all chunks sent
H_ACK = b"h:ack"  # root -> controller: [H_ACK]         addresses received
H_DONE = b"h:done"  # root -> controller: [H_DONE, code]  sweep finished (ascii code)

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
    min_rounds: int = 1,
    chain_length: int = 0,
    close_ctx: bool = True,
    settle_ms: float = 0.0,
) -> int:
    """Performance sweep: grow the connected set 2, 4, 8, ..., N and time each size.

    Instead of one connect to every worker, the root connects in *doubling batches*:
    it joins two workers, then two more, then four more, ... so the connected total
    climbs 2, 4, 8, 16, ... up to the whole fleet. After each batch it re-wires the
    ring over all currently-connected workers and runs ``min_rounds`` direct+ring
    rounds back-to-back — with **no inter-round pause** and **no heartbeat-only
    settle window** (every phase except the heartbeat pause) — reporting the connect
    time for the batch and the direct/ring round-trip at that size.

    This measures small-scale message performance across a whole range of sizes from
    within a single large job, so a scaling curve (2..N) comes out of one allocation
    rather than one job per size. Returns a process exit code (0 on full success).
    """
    root = Actor(b"root")
    rounds = max(min_rounds, 1)
    try:
        total = len(hosts)
        # Doubling milestones 2, 4, 8, ... below the total, with the total itself as
        # the final size (so the full fleet is always measured, power of two or not).
        sizes: list[int] = []
        n = 2
        while n < total:
            sizes.append(n)
            n *= 2
        if total >= 1 and (not sizes or sizes[-1] != total):
            sizes.append(total)
        log(f"[sweep] sizes {sizes} of {total} workers, {rounds} round(s) each")

        workers: list[bytes] = []
        joined = 0
        for size in sizes:
            # Join only the next batch to grow the connected set to `size`.
            batch = hosts[joined:size]
            t_join = time.monotonic()
            for host in batch:
                root.join(f"quic://{host}", "parent", failure=[ba(H_FAIL)])
            joined = size
            while len(workers) < size:
                try:
                    msg = await asyncio.wait_for(root.next(), connect_timeout)
                except asyncio.TimeoutError:
                    log(f"[sweep] ERROR: only {len(workers)}/{size} workers connected")
                    return 1
                if bytes(msg[0]) == b"root":
                    workers.append(bytes(msg[1]))
                else:
                    who, reason = _failure_notice(msg)
                    log(f"[sweep] FAILURE (connect) {who}: {reason}")
                    return 1
            connect_ms = (time.monotonic() - t_join) * 1e3

            # Re-wire the ring over the whole current set (new workers included), so
            # every worker knows its next hop before a token can reach it.
            cap = chain_length if chain_length > 0 else len(workers)
            heads = list(range(0, len(workers), cap))
            for i, wid in enumerate(workers):
                boundary = (i + 1) % cap == 0 or i + 1 == len(workers)
                nxt = b"root" if boundary else workers[i + 1]
                root.send(wid, [ba(H_NEXT), ba(nxt)])

            # Rounds back-to-back: no inter-round sleep, no end-sleep. Track the best
            # (min) and mean of each phase across the rounds.
            direct_best = ring_best = float("inf")
            direct_sum = ring_sum = 0.0
            try:
                for _ in range(rounds):
                    t_d = time.monotonic()
                    if await _direct_round(root, workers, timeout):
                        log(f"[sweep] FAILURE in direct at size {size}")
                        return 1
                    d = (time.monotonic() - t_d) * 1e3
                    t_r = time.monotonic()
                    if await _ring_round(root, workers, timeout, heads):
                        log(f"[sweep] FAILURE in ring at size {size}")
                        return 1
                    r = (time.monotonic() - t_r) * 1e3
                    direct_best, direct_sum = min(direct_best, d), direct_sum + d
                    ring_best, ring_sum = min(ring_best, r), ring_sum + r
            except asyncio.TimeoutError:
                log(f"[sweep] ERROR: a worker stalled at size {size}")
                return 1
            # Optional heartbeat-only settle: idle the whole connected set with *no
            # data* for longer than the heartbeat timeout, so a link survives only if
            # its heartbeats (direct or delegated) keep flowing. Then one direct+ring
            # round confirms every link is still alive. This is what the back-to-back
            # rounds above deliberately do not test. Run it only once, at the final
            # (largest) size — it costs a full timeout window, so doing it per size
            # would dominate the whole sweep's runtime.
            if settle_ms > 0 and size == sizes[-1]:
                await asyncio.sleep(settle_ms / 1000.0)
                try:
                    if await _direct_round(root, workers, timeout):
                        log(f"[sweep] FAILURE after settle (direct) at size {size}")
                        return 1
                    if await _ring_round(root, workers, timeout, heads):
                        log(f"[sweep] FAILURE after settle (ring) at size {size}")
                        return 1
                except asyncio.TimeoutError:
                    log(f"[sweep] ERROR: a worker stalled after settle at size {size}")
                    return 1
                log(f"[sweep] size={size:>6}  settled {settle_ms:.0f}ms OK")
            log(
                f"[sweep] size={size:>6}  connect(+{len(batch)})={connect_ms:9.1f} ms  "
                f"direct min={direct_best:8.2f} avg={direct_sum / rounds:8.2f} ms  "
                f"ring min={ring_best:8.2f} avg={ring_sum / rounds:8.2f} ms  "
                f"({len(heads)} chain(s))"
            )
        log("[sweep] done (0 failures)")
        return 0
    finally:
        # In coordinated mode the caller keeps the context alive to report back to
        # its controller, so it closes the context itself.
        if close_ctx:
            minimonarch.close()


async def run_root_coordinated(
    coord_port: int,
    bind: str,
    timeout: float,
    connect_timeout: float,
    min_rounds: int = 1,
    chain_length: int = 0,
    settle_ms: float = 0.0,
) -> int:
    """Root variant that waits for a controller to hand it the worker addresses.

    Rather than reading the worker list from ``--root``/``--root-file`` (i.e. from
    a single MAST job's task-group env), the root serves a coordination listener on
    ``coord_port`` and waits for a controller to join and send an ``H_ADDRS`` message
    carrying every worker address. This lets one logical run span *multiple* MAST
    jobs that the scheduler cannot place as one job (e.g. across regions): an
    external controller (a devserver running minimonarch) gathers the union of all
    jobs' hosts and injects it here. The root acks, runs the normal sweep against
    those addresses as a fresh ``b"root"`` actor, then reports the exit code back to
    the controller before tearing the context down.
    """
    coord = Actor(b"coord")
    coord.serve(f"quic://[{bind}]:{coord_port}", "child", failure=[ba(H_FAIL)])
    log(f"[coord] serving quic://[{bind}]:{coord_port}; waiting for controller...")

    # Establishment hello: [self_ident, controller_ident].
    hello = await coord.next()
    controller = bytes(hello[1])
    log(f"[coord] controller joined: {controller.decode(errors='replace')}")

    # Accumulate the address list, delivered as one or more pickled chunks (kept
    # under a few MB each) terminated by H_ADDRS_END, or abort on controller failure.
    hosts: list[str] = []
    while True:
        msg = await coord.next()
        header = bytes(msg[0])
        if header == H_ADDRS:
            # This private MAST smoke channel exchanges only trusted controller data.
            # @lint-ignore PYTHONPICKLEISBAD
            hosts.extend(pickle.loads(bytes(msg[1])))
            continue
        if header == H_ADDRS_END:
            break
        if header == H_FAIL:
            who, reason = _failure_notice(msg)
            log(f"[coord] controller {who} gone before addresses ({reason}); aborting")
            minimonarch.close()
            return 1
        log(f"[coord] unexpected header {header!r} before addresses; ignoring")
    log(f"[coord] received {len(hosts)} worker addresses; acking")
    coord.send(controller, [ba(H_ACK)])

    # Run the normal sweep as a fresh root actor, but keep the context alive so we
    # can report completion to the controller afterward.
    rc = await run_root(
        hosts,
        timeout,
        connect_timeout,
        min_rounds,
        chain_length,
        close_ctx=False,
        settle_ms=settle_ms,
    )

    log(f"[coord] sweep finished rc={rc}; notifying controller and closing")
    coord.send(controller, [ba(H_DONE), ba(str(rc).encode())])
    # close() flushes already-posted messages (the H_DONE) before tearing down.
    minimonarch.close()
    return rc


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
        "--wait-for-addresses",
        type=int,
        metavar="PORT",
        help="run as the root but WAIT for an external controller to supply the "
        "worker addresses over a coordination actor served on PORT, instead of "
        "reading them from --root/--root-file (i.e. from a single job's env). Used "
        "to span multiple MAST jobs: a controller gathers every job's hosts and "
        "sends them here. See run_root_coordinated.",
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
        "--chain-length",
        type=int,
        default=0,
        help="root: cap the ring into independent chains of at most this many "
        "workers, each returning to the root on its own (default: 0 = one chain of "
        "all workers). Bounds ring latency and blast radius at large worker counts.",
    )
    parser.add_argument(
        "--min-rounds",
        type=int,
        default=1,
        help="root: run this many direct+ring rounds at each sweep size "
        "(default: 1). More rounds give a min/avg per size.",
    )
    parser.add_argument(
        "--settle-ms",
        type=float,
        default=0.0,
        help="root: after each sweep size, idle the connected set with no data for "
        "this many ms (should exceed the heartbeat timeout), then verify every link "
        "survived. 0 (default) disables. Isolates heartbeat-only liveness failures.",
    )
    args = parser.parse_args()

    ensure_quic_certs(args.certs_dir)

    if args.worker:
        asyncio.run(run_worker(args.port, args.bind))
    elif args.wait_for_addresses is not None:
        sys.exit(
            asyncio.run(
                run_root_coordinated(
                    args.wait_for_addresses,
                    args.bind,
                    args.timeout,
                    args.connect_timeout,
                    args.min_rounds,
                    args.chain_length,
                    args.settle_ms,
                )
            )
        )
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
                    args.min_rounds,
                    args.chain_length,
                    settle_ms=args.settle_ms,
                )
            )
        )


if __name__ == "__main__":
    main()
