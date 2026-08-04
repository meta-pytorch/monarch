# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""net_parallel_bench.py — does the net-data plane actually run crypto in parallel?

Verifies the `MM_NET_DATA_THREADS` change: with it set, the per-connection message
writer/reader coroutines run on a multi-threaded runtime, so the CPU work of
processing many streams (framing + TLS crypto) should spread across cores instead of
serializing on the single `monarch-mini` command-loop thread.

How it measures (no guessing from wall-clock alone): each process reads its own
per-thread CPU time from `/proc/self/task/<tid>/{comm,stat}` before and after a blast
and attributes the delta by thread name. The tokio worker threads are named
`mm-net-data`; the command loop (and, for quic, quinn's endpoint driver) run on
`monarch-mini`. The telling number is **CPU-seconds on the data threads / wall-seconds**
during the blast: > 1 means several data threads ran at once, i.e. genuine parallelism.

Topology: two processes joined over `scheme://127.0.0.1:port`, one connection per
port. The client (child) blasts `msgs` payloads of `size` bytes to the server (parent)
on each of `conns` independent connections; the server decrypts them. Both processes
report their own per-thread CPU, so we see sender (encrypt) and receiver (decrypt)
parallelism separately.

Run the default matrix (tcp/quic x 1/N threads) from this directory:

    uv run python net_parallel_bench.py

or one cell (it re-execs itself as the two subprocesses):

    uv run python net_parallel_bench.py --scheme tcp --threads 8 --conns 8
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from collections import defaultdict

import minimonarch
from minimonarch import Actor

ba = minimonarch.bytearray

# Thread names (tokio sets these) whose CPU we attribute the networking work to.
DATA_THREAD = "mm-net-data"  # multi-threaded data-plane workers (the parallel target)
LOOP_THREAD = "monarch-mini"  # the single command loop + quic endpoint driver
_CLK_TCK = os.sysconf("SC_CLK_TCK")
_RESULT_MARKER = "BENCH_RESULT "


def sample_thread_cpu() -> dict[str, float]:
    """CPU seconds (utime+stime) consumed so far, summed by thread name.

    Reads every thread of this process from `/proc/self/task`. A name may cover
    several threads (the `mm-net-data` pool), so values are summed per name.
    """
    totals: dict[str, float] = defaultdict(float)
    task_dir = "/proc/self/task"
    for tid in os.listdir(task_dir):
        try:
            with open(f"{task_dir}/{tid}/comm") as f:
                name = f.read().strip()
            with open(f"{task_dir}/{tid}/stat") as f:
                stat = f.read()
        except FileNotFoundError:
            continue  # thread exited between listdir and read
        # comm can contain spaces/parens; fields after the ")" are space-separated,
        # with utime/stime at positions 14/15 (1-indexed) of the whole line.
        fields = stat[stat.rfind(")") + 2 :].split()
        utime, stime = int(fields[11]), int(fields[12])
        totals[name] += (utime + stime) / _CLK_TCK
    return dict(totals)


def cpu_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    names = set(before) | set(after)
    return {n: after.get(n, 0.0) - before.get(n, 0.0) for n in names}


def url(scheme: str, port: int) -> str:
    return f"{scheme}://127.0.0.1:{port}"


def free_ports(n: int, scheme: str) -> list[int]:
    """`n` distinct free ports for the scheme's socket type (UDP for quic, TCP else).

    Bound all at once then released, so the returned ports are distinct; serve/join
    retry, so the brief unbound gap before the transport rebinds is harmless.
    """
    kind = socket.SOCK_DGRAM if scheme == "quic" else socket.SOCK_STREAM
    socks = [socket.socket(socket.AF_INET, kind) for _ in range(n)]
    try:
        for s in socks:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def _emit_result(payload: dict[str, object]) -> None:
    # A single machine-readable line the driver greps out of stdout.
    print(_RESULT_MARKER + json.dumps(payload), flush=True)


async def _recv_windowed(actor: Actor, peer: bytes, msgs: int, window: int) -> int:
    """Receive `msgs` payloads in `window`-sized rounds, acking each round.

    The per-window ack lets the sender bound its outstanding (unsent) buffers, so a
    large total volume never has to be resident at once.
    """
    total = 0
    got = 0
    while got < msgs:
        n = min(window, msgs - got)
        for _ in range(n):
            parts = await actor.next()
            total += sum(len(p) for p in parts)
        got += n
        actor.send(peer, [ba(b"ack")])
    return total


async def run_server(scheme: str, ports: list[int], msgs: int, window: int) -> None:
    """Serve one parent per port, receive `msgs` blasts on each, ack, and report CPU."""
    actors = [Actor(f"srv{i}".encode()) for i in range(len(ports))]
    for actor, port in zip(actors, ports):
        actor.serve(url(scheme, port), "parent")
    peers = [bytes((await actor.next())[1]) for actor in actors]  # [self, peer]

    base = sample_thread_cpu()
    t0 = time.perf_counter()
    received = await asyncio.gather(
        *(_recv_windowed(a, p, msgs, window) for a, p in zip(actors, peers))
    )
    wall = time.perf_counter() - t0

    _emit_result(
        {
            "who": "server",
            "wall_s": wall,
            "bytes": sum(received),
            "cpu": cpu_delta(base, sample_thread_cpu()),
        }
    )
    minimonarch.close()


async def _blast_windowed(
    actor: Actor, peer: bytes, msgs: int, size: int, window: int
) -> None:
    payload = bytes(size)  # contents are irrelevant to crypto/throughput cost
    sent = 0
    while sent < msgs:
        n = min(window, msgs - sent)
        for _ in range(n):
            actor.send(peer, [ba(payload)])  # ba(bytes) copies; send moves it out
        sent += n
        await actor.next()  # the server's ack for this window (bounds our backlog)


async def run_client(
    scheme: str, ports: list[int], msgs: int, size: int, window: int
) -> None:
    """Join one child per port, blast `msgs` payloads on each, and report CPU."""
    actors = [Actor(f"cli{i}".encode()) for i in range(len(ports))]
    for actor, port in zip(actors, ports):
        actor.join(url(scheme, port), "child")
    peers = [bytes((await actor.next())[1]) for actor in actors]

    base = sample_thread_cpu()
    t0 = time.perf_counter()
    await asyncio.gather(
        *(_blast_windowed(a, p, msgs, size, window) for a, p in zip(actors, peers))
    )
    wall = time.perf_counter() - t0

    _emit_result(
        {
            "who": "client",
            "wall_s": wall,
            "bytes": msgs * size * len(ports),
            "cpu": cpu_delta(base, sample_thread_cpu()),
        }
    )
    minimonarch.close()


def _child_env(threads: int) -> dict[str, str]:
    env = os.environ.copy()
    env["MM_NET_DATA_THREADS"] = str(threads)
    certs = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test_certs"))
    env["MM_QUIC_CERT"] = os.path.join(certs, "cert.pem")
    env["MM_QUIC_KEY"] = os.path.join(certs, "key.pem")
    env["MM_QUIC_CA"] = os.path.join(certs, "ca.pem")
    return env


def _spawn(role: str, scheme: str, ports: list[int], args: argparse.Namespace, env):
    csv = ",".join(str(p) for p in ports)
    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        "--role",
        role,
        "--scheme",
        scheme,
        "--ports",
        csv,
        "--msgs",
        str(args.msgs),
        "--size",
        str(args.size),
        "--window",
        str(args.window),
    ]
    return subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, text=True)


def _parse_result(output: str) -> dict[str, object]:
    for line in output.splitlines():
        if line.startswith(_RESULT_MARKER):
            return json.loads(line[len(_RESULT_MARKER) :])
    raise RuntimeError(f"no result line in subprocess output:\n{output}")


def _run_cell(scheme: str, threads: int, args: argparse.Namespace) -> dict[str, object]:
    """Run one (scheme, threads) cell across two subprocesses; return their reports."""
    env = _child_env(threads)
    ports = free_ports(args.conns, scheme)
    server = _spawn("server", scheme, ports, args, env)
    client = _spawn("client", scheme, ports, args, env)
    try:
        client_out, _ = client.communicate(timeout=args.timeout)
        server_out, _ = server.communicate(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        server.kill()
        client.kill()
        raise
    if client.returncode != 0 or server.returncode != 0:
        raise RuntimeError(
            f"subprocess failed (server rc={server.returncode}, client rc={client.returncode})\n"
            f"--- server ---\n{server_out}\n--- client ---\n{client_out}"
        )
    return {"server": _parse_result(server_out), "client": _parse_result(client_out)}


def _summarize(who: dict[str, object], total_mb: float) -> dict[str, float]:
    wall = float(who["wall_s"])
    cpu = who["cpu"]
    data = float(cpu.get(DATA_THREAD, 0.0))
    loop = float(cpu.get(LOOP_THREAD, 0.0))
    return {
        "mb_per_s": total_mb / wall if wall else 0.0,
        "data_cpu_s": data,
        "loop_cpu_s": loop,
        # Avg data-thread cores busy over the window: > 1 ⇒ genuine parallelism.
        "data_parallelism": data / wall if wall else 0.0,
    }


def _print_report(args: argparse.Namespace, cells: dict) -> None:
    total_mb = args.conns * args.msgs * args.size / 1e6
    print(
        f"\nnet_parallel_bench: conns={args.conns} msgs/conn={args.msgs} "
        f"size={args.size}B total={total_mb:.0f}MB\n"
        f"'data_par' = CPU-seconds on {DATA_THREAD} threads / wall-seconds "
        f"(>1 means crypto ran on several cores at once)\n"
    )
    header = f"{'scheme':6} {'thr':>3} {'side':6} {'MB/s':>8} {'data_cpu_s':>11} {'loop_cpu_s':>11} {'data_par':>9}"
    print(header)
    print("-" * len(header))
    for (scheme, threads), report in cells.items():
        for side in ("server", "client"):
            s = _summarize(report[side], total_mb)
            print(
                f"{scheme:6} {threads:>3} {side:6} {s['mb_per_s']:>8.1f} "
                f"{s['data_cpu_s']:>11.2f} {s['loop_cpu_s']:>11.2f} {s['data_parallelism']:>9.2f}"
            )
    print()


def _run_driver(args: argparse.Namespace) -> None:
    schemes = args.scheme.split(",") if args.scheme else ["tcp", "quic"]
    thread_counts = [int(t) for t in args.matrix_threads.split(",")]
    cells: dict = {}
    for scheme in schemes:
        for threads in thread_counts:
            print(f"running {scheme} threads={threads} ...", file=sys.stderr)
            cells[(scheme, threads)] = _run_cell(scheme, threads, args)
    _print_report(args, cells)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=["server", "client"], default=None)
    parser.add_argument("--scheme", default=None, help="csv of tcp,quic (driver)")
    parser.add_argument("--ports", default=None, help="csv of ports (subprocess)")
    parser.add_argument("--conns", type=int, default=8)
    parser.add_argument("--msgs", type=int, default=256, help="messages per connection")
    parser.add_argument("--size", type=int, default=256 * 1024, help="payload bytes")
    parser.add_argument(
        "--window", type=int, default=32, help="unacked messages in flight per conn"
    )
    parser.add_argument("--threads", type=int, default=8, help="single-cell threads")
    parser.add_argument("--matrix-threads", default="1,8", help="driver thread sweep")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    if args.role is None:
        # Driver mode. A single --scheme (no matrix override) still sweeps threads.
        _run_driver(args)
        return

    ports = [int(p) for p in args.ports.split(",")]
    if args.role == "server":
        asyncio.run(run_server(args.scheme, ports, args.msgs, args.window))
    else:
        asyncio.run(run_client(args.scheme, ports, args.msgs, args.size, args.window))


if __name__ == "__main__":
    main()
