#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Big-message vs. heartbeat stress: one role per process (root, a, b).

Topology (see ``run.py`` for how the three processes are spawned):

    root ── quic ──> child-a        (a is a direct heartbeat *keeper*)
    root ── quic ──> child-b        (b is *delegated* to a: b heartbeats a over a
                                      gateway side channel, a acks over another)
    child-a ── side channel ──> child-b   (a routes messages to b gateway-to-gateway)

With ``MM_QUIC_MAX_DIRECT_CHILDREN=1`` the root keeps exactly one child direct (a)
and delegates the other (b) onto a, so b's liveness rides the a<->b side channels
instead of its direct link to the root.

The point: **hammer huge messages on every data path while the heartbeat timeout is
shorter than a single message's transfer time**, and check that nothing severs.
Heartbeats ride their own QUIC stream, so a multi-second message must not starve
them:

  * root<->a  — big messages on the connection's *data* stream vs. the direct
    heartbeat on its *heartbeat* stream.
  * a->b      — big messages on the a->b side channel's *message* stream vs. a's
    delegate *acks* to b on that same channel's *heartbeat* stream.
  * b->a      — big messages on the b->a side channel's *message* stream vs. b's
    delegate *beats* to a on that channel's *heartbeat* stream.

Every process bounces each big DATA message straight back to its sender, so a small
pool of buffers ping-pongs forever with zero per-message allocation — keeping the
streams continuously saturated for far longer than the heartbeat timeout. Any
severed monitored link (delivered as an ``H_FAIL`` failure notice) fails the run.
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

# part[0] headers (exact-match dispatch).
H_FAIL = b"h:fail"  # [H_FAIL, dead_ident, reason]  a monitored link severed
H_CFG = b"h:cfg"  # [H_CFG]                        root -> worker: start hammering
H_DATA = b"h:data"  # [H_DATA, from_ident, payload] a big message; bounce to from
H_STOP = b"h:stop"  # [H_STOP]                       root -> worker: wind down

_HOST = socket.gethostname()


def log(msg: str) -> None:
    ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"{ts} {_HOST} {msg}", flush=True)


def ident(name: str, addr: str) -> bytes:
    """Ident for a worker: ``name@addr`` where ``addr`` (e.g. ``[::1]:26810``) is
    the worker's own gateway ``@tag`` — the dialable address the root reaches it at
    and a sibling opens a side channel to for delegated heartbeats."""
    return f"{name}@{addr}".encode()


def _require_certs() -> None:
    if not all(
        os.environ.get(v) for v in ("MM_QUIC_CERT", "MM_QUIC_KEY", "MM_QUIC_CA")
    ):
        raise SystemExit("MM_QUIC_CERT/MM_QUIC_KEY/MM_QUIC_CA must be set (see run.py)")


async def run_worker(name: str, addr: str, peer_ident: bytes | None) -> int:
    """Serve a quic listener as a child of the root, bounce every big DATA message
    back to its sender, and — once the root says go (``H_CFG``) — seed the pool of
    big buffers this worker originates (to the root, and to its peer if given).

    Returns a process exit code: 0 on a clean wind-down, 1 if a monitored link
    severed (a heartbeat failure under message load — the thing we are testing).
    """
    _require_certs()
    me_ident = ident(name, addr)
    tag = f"[{name}]"
    me = Actor(me_ident)
    me.serve(f"quic://{addr}", "child", failure=[ba(H_FAIL)])
    log(f"{tag} serving quic://{addr} pid={os.getpid()}")

    # Establishment hello: [self_ident, root_ident]. The root is a fixed name.
    hello = await me.next()
    root_ident = bytes(hello[1])
    log(f"{tag} established with root {root_ident.decode(errors='replace')}")

    msg_bytes = int(os.environ["BENCH_MSG_BYTES"])
    pool = int(os.environ["BENCH_POOL"])
    bounced = 0
    rc = 0
    start = time.monotonic()
    while True:
        msg = await me.next()
        header = bytes(msg[0])
        if header == H_DATA:
            # Bounce the payload straight back to whoever sent it — reusing the
            # received buffer (moved back out), so steady state never allocates.
            sender = bytes(msg[1])
            payload = msg[2]
            bounced += len(payload)
            me.send(sender, [ba(H_DATA), ba(me_ident), payload])
        elif header == H_CFG:
            # Go: originate this worker's own hammer. Seed `pool` big buffers to the
            # root, and `pool` to the peer (a -> b) if we have one. Each seeded
            # buffer then ping-pongs forever via the bounce above.
            log(
                f"{tag} go: seeding {pool} x {msg_bytes} B to root"
                + (" and peer" if peer_ident else "")
            )
            for _ in range(pool):
                me.send(root_ident, [ba(H_DATA), ba(me_ident), ba(msg_bytes)])
                if peer_ident:
                    me.send(peer_ident, [ba(H_DATA), ba(me_ident), ba(msg_bytes)])
        elif header == H_STOP:
            log(f"{tag} stop; bounced {bounced / 1e9:.2f} GB")
            break
        elif header == H_FAIL:
            who = bytes(msg[1]).decode(errors="replace") if len(msg) > 1 else "?"
            reason = bytes(msg[2]).decode(errors="replace") if len(msg) > 2 else "?"
            # The root's shutdown notice severs our parent link cleanly at the end;
            # that is expected. Any *other* sever (or a sever mid-run) is a failure.
            log(f"{tag} FAIL link to {who} severed: {reason}")
            if who != root_ident.decode(errors="replace"):
                rc = 1
            break
        else:
            log(f"{tag} unexpected header {header!r}; ignoring")

    elapsed = max(time.monotonic() - start, 1e-9)
    log(
        f"{tag} done rc={rc}: {bounced / 1e9:.2f} GB over {elapsed:.1f}s "
        f"= {bounced / elapsed / 1e9:.2f} GB/s"
    )
    minimonarch.close()
    return rc


async def run_root(a_addr: str, b_addr: str, duration: float) -> int:
    """Join a and b, measure one big message's transfer time (to confirm it exceeds
    the heartbeat timeout), then reflect big messages for `duration` seconds while
    watching for any severed link. Returns 0 iff no link severed."""
    _require_certs()
    msg_bytes = int(os.environ["BENCH_MSG_BYTES"])
    hb_timeout_s = int(os.environ["MM_QUIC_HEARTBEAT_TIMEOUT_MS"]) / 1000.0
    a_ident = ident("child-a", a_addr)
    b_ident = ident("child-b", b_addr)

    root = Actor(b"root")
    root.join(f"quic://{a_addr}", "parent", failure=[ba(H_FAIL)])
    root.join(f"quic://{b_addr}", "parent", failure=[ba(H_FAIL)])

    # Two establishment hellos ([b"root", worker_ident]), one per child.
    established = set()
    for _ in range(2):
        hello = await root.next()
        established.add(bytes(hello[1]))
    log(
        f"[root] both children established: "
        f"{sorted(i.decode(errors='replace') for i in established)}"
    )

    # Probe: one big round-trip to a, timed, so we can report the single-message
    # transfer time and confirm it is longer than the heartbeat timeout (the whole
    # premise — a beat must survive a transfer it is shorter than).
    t0 = time.perf_counter()
    root.send(a_ident, [ba(H_DATA), ba(b"root"), ba(msg_bytes)])
    probe = await root.next()
    assert bytes(probe[0]) == H_DATA, f"probe returned {bytes(probe[0])!r}"
    rt = time.perf_counter() - t0
    one_way = rt / 2.0
    log(
        f"[root] single {msg_bytes / 1e6:.0f} MB message: round-trip {rt * 1e3:.0f} ms, "
        f"one-way ~{one_way * 1e3:.0f} ms; heartbeat timeout {hb_timeout_s * 1e3:.0f} ms"
    )
    if one_way <= hb_timeout_s:
        log(
            f"[root] WARNING: one-way transfer ({one_way * 1e3:.0f} ms) <= heartbeat "
            f"timeout ({hb_timeout_s * 1e3:.0f} ms); increase --msg-mb or lower "
            f"--hb-timeout-ms so a single transfer outlasts a beat window"
        )

    # Go. a hammers root + b; b hammers root. Everyone (including us) bounces.
    root.send(a_ident, [ba(H_CFG)])
    root.send(b_ident, [ba(H_CFG)])
    log(
        f"[root] hammering for {duration:.0f}s "
        f"(hb timeout {hb_timeout_s * 1e3:.0f} ms — links must survive on beats alone)"
    )

    deadline = time.monotonic() + duration
    from_a = from_b = 0
    failures: list[str] = []
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            msg = await asyncio.wait_for(root.next(), remaining)
        except asyncio.TimeoutError:
            break
        header = bytes(msg[0])
        if header == H_DATA:
            sender = bytes(msg[1])
            payload = msg[2]
            if sender == a_ident:
                from_a += len(payload)
            elif sender == b_ident:
                from_b += len(payload)
            root.send(sender, [ba(H_DATA), ba(b"root"), payload])
        elif header == H_FAIL:
            who = bytes(msg[1]).decode(errors="replace") if len(msg) > 1 else "?"
            reason = bytes(msg[2]).decode(errors="replace") if len(msg) > 2 else "?"
            log(f"[root] FAILURE link to {who} severed under load: {reason}")
            failures.append(f"{who}: {reason}")
            break  # a sever is terminal — the thing we are guarding against
        else:
            log(f"[root] unexpected header {header!r}; ignoring")

    ran = duration - max(deadline - time.monotonic(), 0.0)
    ran = max(ran, 1e-9)
    log(
        f"[root] reflected: root<->a {from_a / 1e9:.2f} GB "
        f"({from_a / ran / 1e9:.2f} GB/s), root<->b {from_b / 1e9:.2f} GB "
        f"({from_b / ran / 1e9:.2f} GB/s) over {ran:.1f}s"
    )

    # Wind the workers down (best effort) before tearing our context down.
    for wid in (a_ident, b_ident):
        root.send(wid, [ba(H_STOP)])

    if failures:
        log(
            f"[root] RESULT: FAIL — {len(failures)} link(s) severed under message "
            f"load: {'; '.join(failures)}"
        )
        rc = 1
    else:
        log(
            "[root] RESULT: PASS — no link severed; heartbeats survived continuous "
            "big-message saturation on every data/side-channel stream"
        )
        rc = 0
    minimonarch.close()
    return rc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=["root", "a", "b"])
    parser.add_argument("--port", type=int, help="worker (a/b) listen port")
    parser.add_argument("--peer-port", type=int, help="a: child-b's port (a -> b)")
    parser.add_argument("--a-addr", help="root: child-a address, e.g. [::1]:26810")
    parser.add_argument("--b-addr", help="root: child-b address")
    parser.add_argument("--duration", type=float, default=20.0, help="root: seconds")
    parser.add_argument("--bind", default="::1", help="worker bind host")
    args = parser.parse_args()

    if args.role == "root":
        sys.exit(asyncio.run(run_root(args.a_addr, args.b_addr, args.duration)))
    else:
        addr = f"[{args.bind}]:{args.port}"
        peer = (
            ident("child-b", f"[{args.bind}]:{args.peer_port}")
            if args.peer_port
            else None
        )
        name = "child-a" if args.role == "a" else "child-b"
        sys.exit(asyncio.run(run_worker(name, addr, peer)))


if __name__ == "__main__":
    main()
