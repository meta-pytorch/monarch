# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Subprocess worker for the minimonarch round-trip benchmarks.

The orchestrator (``run_bench.py``) launches one or more of these per topology.
Exactly one of them plays the *sender* role: it builds its slice of the
topology, warms the path up, then measures all four metrics across every
message size and prints the results as a single JSON line on stdout. The other
roles (parent / host / receiver / process managers) just stand up their part of
the tree and either echo (receiver) or idle (structural actors) until the
orchestrator kills them.

Usage:
    python bench_worker.py <config-json>

where ``<config-json>`` is the JSON produced by the orchestrator describing the
topology, this process's role, the urls to use, and the workload parameters.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import minimonarch
from minimonarch import Actor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_common import RECEIVER, RESULT_MARKER, SENDER  # noqa: E402

ba = minimonarch.bytearray

# A small, fixed payload used only to warm the path up before timing.
_WARMUP_SIZE = 64


# ---------------------------------------------------------------------------
# Receiver: echo every data message straight back to its return id.
# ---------------------------------------------------------------------------
async def _echo_forever(actor: Actor) -> None:
    """Echo loop: each data message is ``[return_id, payload...]``; send the
    payload parts straight back to ``return_id``. The payload bytearrays are
    *moved* back into the reply, so no payload bytes are copied here."""
    while True:
        parts = await actor.next()
        return_id = parts[0].tobytes()
        actor.send(return_id, parts[1:])


async def _consume_hellos(actor: Actor, n: int) -> None:
    """Drain the ``n`` connection-established (``[self, other]``) messages that
    a freshly connected actor receives, so later ``next()`` calls only ever see
    data messages."""
    for _ in range(n):
        await actor.next()


# ---------------------------------------------------------------------------
# Sender: warm up, then measure.
# ---------------------------------------------------------------------------
async def _warmup(sender: Actor, deadline_s: float) -> None:
    """Resend a probe to the receiver until it echoes back, establishing that
    the full s -> r -> s path is live, then drain any duplicate echoes that the
    retries may have produced so timed iterations start from a clean queue."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    while True:
        sender.send(RECEIVER, [ba(SENDER), ba(_WARMUP_SIZE)])
        try:
            await asyncio.wait_for(sender.next(), 0.5)
            break
        except asyncio.TimeoutError:
            if loop.time() - start > deadline_s:
                raise TimeoutError("benchmark path never became reachable")
    while True:  # drain duplicates
        try:
            await asyncio.wait_for(sender.next(), 0.1)
        except asyncio.TimeoutError:
            break


async def _measure_latency(
    sender: Actor, size: int, iters: int, monitor: bool
) -> list[float]:
    """Return ``iters`` samples of round-trip latency in microseconds. One
    payload buffer ping-pongs so there is no per-iteration payload allocation.
    When ``monitor`` is set, each round trip is wrapped in a
    ``monitor(r)`` / ``cancel()`` pair and that cost is included in the sample."""
    payload = ba(size)
    # Prime: one untimed round trip to settle the buffer reuse.
    sender.send(RECEIVER, [ba(SENDER), payload])
    payload = (await sender.next())[0]

    samples: list[float] = []
    for _ in range(iters):
        t0 = time.perf_counter()
        handle = sender.monitor(RECEIVER, failure=[ba(b"F")]) if monitor else None
        sender.send(RECEIVER, [ba(SENDER), payload])
        msg = await sender.next()
        if handle is not None:
            handle.cancel()
        t1 = time.perf_counter()
        payload = msg[0]
        samples.append((t1 - t0) * 1e6)
    return samples


async def _measure_throughput(
    sender: Actor, size: int, reps: int, n: int, cap_bytes: int, monitor: bool
) -> list[dict[str, float]]:
    """Return ``reps`` throughput samples. Each rep fires ``n`` messages
    back-to-back, then awaits ``n`` replies; ``n`` is capped so n*size stays
    under ``cap_bytes``. When ``monitor`` is set, every message gets its own
    ``monitor(r)`` before send and its handle is cancelled after the replies."""
    n = max(1, min(n, cap_bytes // max(size, 1)))
    bufs = [ba(size) for _ in range(n)]

    samples: list[dict[str, float]] = []
    for _ in range(reps):
        handles = []
        t0 = time.perf_counter()
        for i in range(n):
            if monitor:
                handles.append(sender.monitor(RECEIVER, failure=[ba(b"F")]))
            sender.send(RECEIVER, [ba(SENDER), bufs[i]])
        replies = [await sender.next() for _ in range(n)]
        for handle in handles:
            handle.cancel()
        t1 = time.perf_counter()

        bufs = [r[0] for r in replies]  # reuse echoed buffers next rep
        elapsed = t1 - t0
        samples.append(
            {
                "n": float(n),
                "elapsed_s": elapsed,
                "msgs_per_s": n / elapsed if elapsed else 0.0,
                "gb_per_s": (n * size / elapsed) / 1e9 if elapsed else 0.0,
            }
        )
    return samples


async def _run_sender(sender: Actor, cfg: dict) -> dict:
    """Run all four metrics across all sizes and return the results dict."""
    await _warmup(sender, deadline_s=60.0)

    sizes: list[int] = cfg["sizes"]
    results: dict[str, dict] = {}

    for size in sizes:
        results.setdefault("latency", {})[size] = await _measure_latency(
            sender, size, cfg["latency_iters"], monitor=False
        )
        results.setdefault("monitor_latency", {})[size] = await _measure_latency(
            sender, size, cfg["latency_iters"], monitor=True
        )
        results.setdefault("throughput", {})[size] = await _measure_throughput(
            sender,
            size,
            cfg["throughput_reps"],
            cfg["throughput_n"],
            cfg["throughput_cap_bytes"],
            monitor=False,
        )
        results.setdefault("monitor_throughput", {})[size] = await _measure_throughput(
            sender,
            size,
            cfg["throughput_reps"],
            cfg["throughput_n"],
            cfg["throughput_cap_bytes"],
            monitor=True,
        )
    return {"topology": cfg["topology"], "results": results}


# ---------------------------------------------------------------------------
# Topology setup. Each role builds its actors and either measures (sender),
# echoes (receiver), or idles (structural actors) until killed.
# ---------------------------------------------------------------------------
async def _idle_forever() -> None:
    while True:
        await asyncio.sleep(3600)


async def role_sender_inproc(cfg: dict) -> dict:
    """Topology (a): one process hosts parent p plus inproc children s and r.
    r's echo loop runs as a background task while s measures."""
    parent = Actor(b"p")
    sender = Actor(SENDER)
    receiver = Actor(RECEIVER)

    parent.serve("inproc://bench-p-s", "parent")
    sender.join("inproc://bench-p-s", "child")
    parent.serve("inproc://bench-p-r", "parent")
    receiver.join("inproc://bench-p-r", "child")

    await _consume_hellos(parent, 2)
    await _consume_hellos(sender, 1)
    await _consume_hellos(receiver, 1)

    echo = asyncio.ensure_future(_echo_forever(receiver))
    try:
        return await _run_sender(sender, cfg)
    finally:
        echo.cancel()


async def role_parent(cfg: dict) -> None:
    """Topology (b): the common parent p, serving both children over unix."""
    parent = Actor(b"p")
    parent.serve(cfg["urls"]["sender"], "parent")
    parent.serve(cfg["urls"]["receiver"], "parent")
    await _idle_forever()


async def role_receiver(cfg: dict) -> None:
    """Topology (b): receiver r, a unix child of p, echoing forever."""
    receiver = Actor(RECEIVER)
    receiver.join(cfg["urls"]["receiver"], "child")
    await _consume_hellos(receiver, 1)
    await _echo_forever(receiver)


async def role_sender(cfg: dict) -> dict:
    """Topology (b): sender s, a unix child of p."""
    sender = Actor(SENDER)
    sender.join(cfg["urls"]["sender"], "child")
    await _consume_hellos(sender, 1)
    return await _run_sender(sender, cfg)


async def role_host(cfg: dict) -> None:
    """Topology (c): host manager h (root), serving both process managers."""
    host = Actor(b"h")
    host.serve(cfg["urls"]["p0"], "parent")
    host.serve(cfg["urls"]["p1"], "parent")
    await _idle_forever()


async def role_pm_receiver(cfg: dict) -> None:
    """Topology (c): process manager p1 (unix child of h) plus its inproc
    child receiver r, which echoes forever."""
    pm = Actor(b"p1")
    pm.join(cfg["urls"]["p1"], "child")
    await _consume_hellos(pm, 1)

    receiver = Actor(RECEIVER)
    pm.serve("inproc://bench-p1-r", "parent")
    receiver.join("inproc://bench-p1-r", "child")
    await _consume_hellos(pm, 1)
    await _consume_hellos(receiver, 1)

    await _echo_forever(receiver)


async def role_pm_sender(cfg: dict) -> dict:
    """Topology (c): process manager p0 (unix child of h) plus its inproc child
    sender s, which measures."""
    pm = Actor(b"p0")
    pm.join(cfg["urls"]["p0"], "child")
    await _consume_hellos(pm, 1)

    sender = Actor(SENDER)
    pm.serve("inproc://bench-p0-s", "parent")
    sender.join("inproc://bench-p0-s", "child")
    await _consume_hellos(pm, 1)
    await _consume_hellos(sender, 1)

    return await _run_sender(sender, cfg)


_ROLES = {
    "sender": role_sender_inproc,  # remapped below per topology
    "parent": role_parent,
    "receiver": role_receiver,
    "host": role_host,
    "pm_receiver": role_pm_receiver,
    "pm_sender": role_pm_sender,
}


def _resolve_role(cfg: dict):
    """``sender`` means the inproc all-in-one role for topology (a) but the
    unix-child role for topology (b); disambiguate on the topology key."""
    role = cfg["role"]
    if role == "sender":
        return role_sender_inproc if cfg["topology"] == "inproc" else role_sender
    return _ROLES[role]


async def _main(cfg: dict) -> None:
    coro = _resolve_role(cfg)(cfg)
    out = await coro
    if out is not None:  # a sender role produced results
        sys.stdout.write(RESULT_MARKER + json.dumps(out) + "\n")
        sys.stdout.flush()
    minimonarch.close()


if __name__ == "__main__":
    config = json.loads(sys.argv[1])
    asyncio.run(_main(config))
