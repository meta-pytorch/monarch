#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Drive ONE logical minimonarch smoke run across MULTIPLE MAST jobs from a devserver.

MAST cannot place a single job across regions (its allocators are regional), so a
run bigger than one region's free capacity has to be split into several independent
jobs. This orchestrator stitches them back into one run from *outside* MAST:

  1. Schedule N MAST jobs (via ``build_mast.py``), each in coordinated mode
     (``SMOKE_COORD_PORT`` set) and optionally pinned to its own region. Every job's
     rank-0 task serves a minimonarch *coordination* listener and waits — it does NOT
     read the worker list from its own env. (Every job serves such a root; all but
     one go unused.)
  2. Poll ``mast get-status`` until every job is RUNNING and has published all its
     task hostnames.
  3. Gather the union of every job's worker addresses (``[<ipv6>]:<port>`` for each
     host x each worker port).
  4. Locally (here, on the devserver) join the *chosen* job's coordination root over
     minimonarch and send it that full address list. The root acks, spins up the
     real ``b"root"`` actor, and runs the normal sweep against every worker across
     all jobs — genuinely cross-job (and cross-region) connectivity.
  5. Wait for the root to report completion, then kill every job.

This is deliberately a hack: the "extra" roots are wasted, and control-plane traffic
(the devserver <-> root channel) rides the same QUIC transport as the test.

Run under the built minimonarch (e.g. ``.venv/bin/python``); it shells out to
``python3`` for ``build_mast.py`` and to ``mast`` for status/kill.

Usage (small test):

    .venv/bin/python ../mast/multi_region.py --hosts-per-job 2 --workers-per-host 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pickle
import re
import socket
import subprocess
import sys
import time

import minimonarch
from minimonarch import Actor

ba = minimonarch.bytearray

HERE = os.path.dirname(os.path.abspath(__file__))
BUILD_MAST = os.path.join(HERE, "build_mast.py")
TEST_CERTS = os.path.join(HERE, "..", "test_certs")

# Must match smoke.py's coordination headers.
H_FAIL = b"h:fail"
H_ADDRS = b"h:addrs"
H_ADDRS_END = b"h:addrs-end"
H_ACK = b"h:ack"
H_DONE = b"h:done"

# Cap each address message at ~4MB so a huge fleet's list is split across chunks
# rather than one oversized message.
MAX_MSG_BYTES = 4 * 1024 * 1024


def chunk_addrs(addrs: list[str]) -> list[list[str]]:
    """Split addresses into sublists whose pickled size stays under MAX_MSG_BYTES.

    Packs by a conservative per-address byte estimate (length + pickle overhead) so
    a single pickled chunk never approaches the cap; no per-add re-pickling.
    """
    chunks: list[list[str]] = []
    cur: list[str] = []
    cur_bytes = 64  # pickle framing overhead
    budget = MAX_MSG_BYTES - 4096  # margin for framing / header part
    for a in addrs:
        sz = len(a.encode()) + 16  # element bytes + generous pickle overhead
        if cur and cur_bytes + sz > budget:
            chunks.append(cur)
            cur, cur_bytes = [], 64
        cur.append(a)
        cur_bytes += sz
    if cur:
        chunks.append(cur)
    return chunks


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{ts} [multi-region] {msg}", flush=True)


def ensure_certs() -> None:
    """Point the local minimonarch transport at the shipped test certs if unset."""
    if all(os.environ.get(v) for v in ("MM_QUIC_CERT", "MM_QUIC_KEY", "MM_QUIC_CA")):
        return
    for name, var in (
        ("cert.pem", "MM_QUIC_CERT"),
        ("key.pem", "MM_QUIC_KEY"),
        ("ca.pem", "MM_QUIC_CA"),
    ):
        path = os.path.join(TEST_CERTS, name)
        if not os.path.exists(path):
            raise SystemExit(f"missing cert {path}; set MM_QUIC_* env vars instead")
        os.environ[var] = path


def resolve_ipv6(host: str, port: int) -> str:
    """Resolve ``host`` to a single IPv6 address (we connect by IP, never DNS)."""
    # MAST supplies task FQDNs to this OSS smoke test.
    # ast-grep-ignore: python/python-dns-deps
    infos = socket.getaddrinfo(host, port, socket.AF_INET6, socket.SOCK_DGRAM)
    if not infos:
        raise RuntimeError(f"no IPv6 address for {host}")
    return infos[0][4][0]


def schedule_job(
    args: argparse.Namespace,
    coord_port: int,
    region: str | None,
    package: str | None,
) -> tuple[str, str]:
    """Schedule one coordinated MAST job via build_mast.py.

    Returns ``(job_name, fbpkg_id)``. If ``package`` is given the fbpkg build is
    skipped and that package reused (so all jobs share one identical binary).
    """
    cmd = [
        "python3",
        BUILD_MAST,
        "--hosts",
        str(args.hosts_per_job),
        "--workers-per-host",
        str(args.workers_per_host),
        "--port",
        str(args.port),
        "--cluster",
        args.cluster,
        "--env",
        f"SMOKE_COORD_PORT={coord_port}",
        "--env",
        f"SMOKE_PORT={args.port}",
        "--launch",
    ]
    for kv in args.env:
        cmd += ["--env", kv]
    if region:
        cmd += ["--region", region]
    if package:
        cmd += ["--skip-build", "--package", package]
    log(f"scheduling job (region={region or 'auto'}): {' '.join(cmd)}")
    out = subprocess.run(cmd, capture_output=True, text=True)
    sys.stdout.write(out.stdout)
    if out.returncode != 0:
        sys.stderr.write(out.stderr)
        raise SystemExit(f"build_mast.py failed (region={region})")
    job_name = _parse_field(out.stdout, "job name:")
    fbpkg = _parse_field(out.stdout, "fbpkg:")
    if not job_name:
        raise SystemExit(
            f"could not parse job name from build_mast output:\n{out.stdout}"
        )
    return job_name, fbpkg


def _parse_field(text: str, label: str) -> str:
    for line in text.splitlines():
        if line.strip().startswith(label):
            return line.split(label, 1)[1].strip()
    return ""


def job_status(job: str) -> dict:
    out = subprocess.check_output(
        ["mast", "get-status", job, "--output", "json"],
        stderr=subprocess.DEVNULL,
    )
    return json.loads(out)


def job_state(status: dict) -> str:
    """Top-level job state string (RUNNING / PENDING / DEAD / ...), best-effort."""
    found = ["?"]

    def walk(o: object) -> None:
        if isinstance(o, dict):
            if "state" in o and isinstance(o["state"], str) and found[0] == "?":
                found[0] = o["state"]
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(status)
    return found[0]


def task_hostnames(status: dict) -> dict[int, str]:
    """Map task-id -> hostname for every task instance that has one, from status."""
    out: dict[int, str] = {}

    def walk(o: object) -> None:
        if isinstance(o, dict):
            tid = o.get("taskInstanceIdentifier")
            host = o.get("hostname")
            if isinstance(tid, str) and isinstance(host, str) and host:
                m = re.search(r"/(\d+)(?::\d+)?$", tid)
                if m:
                    out[int(m.group(1))] = host
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(status)
    return out


def wait_for_hosts(job: str, expected: int, timeout: float) -> dict[int, str]:
    """Poll until ``job`` is RUNNING with ``expected`` task hostnames published."""
    deadline = time.monotonic() + timeout
    while True:
        status = job_status(job)
        state = job_state(status)
        hosts = task_hostnames(status)
        if state in ("DEAD", "FAILED", "SHUTTING_DOWN"):
            raise SystemExit(f"job {job} entered {state} before running")
        log(f"  {job}: state={state} hosts={len(hosts)}/{expected}")
        if len(hosts) >= expected:
            return hosts
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out waiting for {job} ({len(hosts)}/{expected})")
        time.sleep(10)


def kill_job(job: str) -> None:
    try:
        subprocess.run(
            ["mast", "kill", job, "--comment", "multi_region orchestrator cleanup"],
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
        log(f"killed {job}")
    except Exception as e:  # best-effort cleanup
        log(f"failed to kill {job}: {e}")


async def coordinate(root_ip: str, coord_port: int, addrs: list[str]) -> int:
    """Join the chosen root, send it the full address list, await completion."""
    ctrl = Actor(b"controller")
    url = f"quic://[{root_ip}]:{coord_port}"
    log(f"joining coordination root {url} ...")
    ctrl.join(url, "parent", failure=[ba(H_FAIL)])

    # Establishment hello: [self_ident, root_ident].
    hello = await asyncio.wait_for(ctrl.next(), timeout=180)
    root_ident = bytes(hello[1])
    log(f"connected to root {root_ident.decode(errors='replace')}")

    chunks = chunk_addrs(addrs)
    log(f"sending {len(addrs)} worker addresses in {len(chunks)} chunk(s) (<=4MB each)")
    for ch in chunks:
        # This private MAST smoke channel exchanges only trusted controller data.
        # @lint-ignore PYTHONPICKLEISBAD
        ctrl.send(root_ident, [ba(H_ADDRS), ba(pickle.dumps(ch))])
    ctrl.send(root_ident, [ba(H_ADDRS_END)])

    # Ack, then the sweep runs (can be long), then completion.
    msg = await asyncio.wait_for(ctrl.next(), timeout=120)
    if bytes(msg[0]) != H_ACK:
        log(f"unexpected reply (wanted ack): {bytes(msg[0])!r}")
        return 1
    log("root acked; running sweep (waiting for done)...")

    while True:
        msg = await ctrl.next()
        header = bytes(msg[0])
        if header == H_DONE:
            rc = int(bytes(msg[1])) if len(msg) > 1 else 0
            log(f"root reported done, rc={rc}")
            return rc
        if header == H_FAIL:
            log("root connection failed before reporting done")
            return 1
        log(f"unexpected message {header!r}; ignoring")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hosts-per-job", type=int, default=2, help="hosts per MAST job (default: 2)"
    )
    parser.add_argument(
        "--workers-per-host", type=int, default=1, help="workers per host (default: 1)"
    )
    parser.add_argument("--port", type=int, default=26600, help="base worker port")
    parser.add_argument(
        "--coord-port", type=int, default=26599, help="root coordination port"
    )
    parser.add_argument(
        "--cluster", default="CPUTrainingWorkloads", help="MAST cluster"
    )
    parser.add_argument(
        "--regions",
        default="",
        help="comma-separated region per job (e.g. 'pnb,atn'); empty = auto for all. "
        "The number of jobs is len(regions) if given, else --jobs.",
    )
    parser.add_argument(
        "--jobs", type=int, default=2, help="number of jobs when --regions is empty"
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra env var forwarded to every job (repeatable)",
    )
    parser.add_argument(
        "--start-timeout",
        type=float,
        default=900.0,
        help="seconds to wait for each job to reach RUNNING with hosts (default: 900)",
    )
    parser.add_argument(
        "--keep-jobs",
        action="store_true",
        help="do not kill the jobs on exit (for debugging)",
    )
    args = parser.parse_args()

    ensure_certs()

    regions: list[str | None]
    if args.regions.strip():
        regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    else:
        regions = [None] * args.jobs
    if len(regions) < 1:
        raise SystemExit("need at least one job")

    jobs: list[str] = []
    fbpkg: str | None = None
    try:
        # 1. Schedule every job (first one builds the fbpkg, the rest reuse it).
        for region in regions:
            job_name, built = schedule_job(args, args.coord_port, region, fbpkg)
            jobs.append(job_name)
            fbpkg = fbpkg or built
        log(f"scheduled {len(jobs)} job(s): {jobs}")

        # 2 + 3. Wait for every job's hosts, then union all worker addresses.
        all_addrs: list[str] = []
        root_ip: str | None = None
        for i, job in enumerate(jobs):
            hosts = wait_for_hosts(job, args.hosts_per_job, args.start_timeout)
            if i == 0:
                # The chosen root is job 0's task 0.
                root_host = hosts[min(hosts)] if 0 not in hosts else hosts[0]
                root_ip = resolve_ipv6(root_host, args.coord_port)
                log(f"chosen root = {job} task0 host {root_host} -> [{root_ip}]")
            for host in hosts.values():
                ip = resolve_ipv6(host, args.port)
                for w in range(args.workers_per_host):
                    all_addrs.append(f"[{ip}]:{args.port + w}")
        log(f"gathered {len(all_addrs)} worker addresses across {len(jobs)} job(s)")
        assert root_ip is not None

        # 4 + 5. Coordinate the run, then report.
        rc = asyncio.run(coordinate(root_ip, args.coord_port, all_addrs))
        log(f"=== run finished, rc={rc} ===")
        sys.exit(rc)
    finally:
        minimonarch.close()
        if not args.keep_jobs:
            for job in jobs:
                kill_job(job)


if __name__ == "__main__":
    main()
