# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Checks the UDP check in `slurm/check_udp.py` and its launcher `run_udp.sh`.

Workers run `check_udp.check` in forked child processes on loopback addresses
127.0.0.1 and 127.0.0.2, with discovery, budgets and sends injected below the
command line. A test owns its children: it waits for each with a bound, kills
any still running and reaps them all. The launcher runs under bash against a
stand-in `srun`. None of this tests Slurm or address discovery on a cluster.
"""

import contextlib
import dataclasses
import errno
import ipaddress
import json
import multiprocessing
import os
import shutil
import signal
import socket
import stat
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any

import check_udp
import pytest
import slurm_network

_ADDRESSES = ("127.0.0.1", "127.0.0.2")
_WAIT = 30.0
_FORK = multiprocessing.get_context("fork")


def _ctx(slot: int) -> check_udp.Context:
    return check_udp.Context("4242", slot, f"node-{slot}", 1000)


def _run_id(rendezvous: Path) -> str:
    return f"4242/{rendezvous.parent.name}/{rendezvous.name}"


def _worker(
    conn: Connection,
    slot: int,
    rendezvous: Path,
    ready_budget: float,
    exchange_budget: float,
    hide_peer_for: float,
    drop_reply: int | None,
) -> None:
    signal.signal(signal.SIGTERM, check_udp._terminate)
    # Monotonic times, comparable across these processes: when this worker
    # first sent, and when it could first see its peer's record.
    timing: dict[str, Any] = {"hidden": 0, "first_send": None, "visible_at": None}
    first_seen: float | None = None

    def read(path: Path) -> bytes | None:
        nonlocal first_seen
        data = slurm_network.read_record(path)
        if data is None:
            return None
        now = time.monotonic()
        first_seen = now if first_seen is None else first_seen
        if now - first_seen < hide_peer_for:
            timing["hidden"] += 1
            return None
        timing["visible_at"] = now
        return data

    def sendto(sock: socket.socket, data: bytes, address: check_udp.Address) -> None:
        if timing["first_send"] is None:
            timing["first_send"] = time.monotonic()
        msg = json.loads(data)
        if msg["kind"] == "reply" and msg["seq"] == drop_reply:
            return
        sock.sendto(data, address)

    try:
        outcome = check_udp.check(
            _ctx(slot),
            rendezvous,
            discover=lambda: ("lo", ipaddress.IPv4Address(_ADDRESSES[slot])),
            ready_budget=ready_budget,
            exchange_budget=exchange_budget,
            read=read,
            sendto=sendto,
        )
    except check_udp.CheckError as e:
        conn.send(("fail", e.phase, str(e), timing))
        return
    conn.send(("pass", dataclasses.asdict(outcome), timing))


Start = Callable[..., tuple[BaseProcess, Connection]]


@contextlib.contextmanager
def _workers() -> Iterator[Start]:
    procs: list[BaseProcess] = []

    def start(
        slot: int,
        rendezvous: Path,
        *,
        ready_budget: float = 10.0,
        exchange_budget: float = 10.0,
        hide_peer_for: float = 0.0,
        drop_reply: int | None = None,
    ) -> tuple[BaseProcess, Connection]:
        parent, child = _FORK.Pipe(duplex=False)
        proc = _FORK.Process(
            target=_worker,
            args=(
                child,
                slot,
                rendezvous,
                ready_budget,
                exchange_budget,
                hide_peer_for,
                drop_reply,
            ),
        )
        proc.start()
        child.close()
        procs.append(proc)
        return proc, parent

    try:
        yield start
    finally:
        deadline = time.monotonic() + _WAIT
        for proc in procs:
            proc.join(max(0.0, deadline - time.monotonic()))
            if proc.is_alive():
                proc.kill()
                proc.join(_WAIT)
        assert all(not proc.is_alive() for proc in procs)


def _result(conn: Connection) -> tuple[Any, ...]:
    assert conn.poll(_WAIT), "worker sent no result"
    return conn.recv()


def _rebindable(address: tuple[str, int]) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(address)


def _attempt(tmp_path: Path) -> Path:
    attempt = tmp_path / "attempt.abc123"
    attempt.mkdir(mode=0o700)
    return attempt


def _await_record(path: Path) -> dict[str, Any]:
    deadline = time.monotonic() + _WAIT
    while not path.exists():
        assert time.monotonic() < deadline, f"no {path}"
        time.sleep(0.02)
    return json.loads(path.read_bytes())


@contextlib.contextmanager
def _test_peer(rendezvous: Path) -> Iterator[socket.socket]:
    """A peer for slot 0 that the test drives by hand, on 127.0.0.2."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((_ADDRESSES[1], 0))
        ready = slurm_network.Ready(
            _run_id(rendezvous),
            1,
            "node-1",
            1000,
            ipaddress.IPv4Address(_ADDRESSES[1]),
            sock.getsockname()[1],
        )
        slurm_network.publish(rendezvous, slurm_network.ready_name(1), ready.to_json())
        yield sock


def _recv_frame(sock: socket.socket) -> dict[str, Any]:
    sock.settimeout(_WAIT)
    data, _ = sock.recvfrom(1024)
    return json.loads(data)


def test_pair_passes(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start:
        procs = [start(slot, attempt) for slot in (0, 1)]
        results = [_result(conn) for _, conn in procs]
        for proc, _ in procs:
            proc.join(_WAIT)
            assert proc.exitcode == 0
    for slot, result in enumerate(results):
        assert result[0] == "pass", result
        outcome = result[1]
        assert isinstance(outcome, dict)
        assert (outcome["requests"], outcome["replies"]) == (3, 3)
        assert outcome["address"][0] == _ADDRESSES[slot]
        assert outcome["peer_address"][0] == _ADDRESSES[1 - slot]
        record = json.loads((attempt / slurm_network.result_name(slot)).read_bytes())
        assert record["outcome"] == "pass"
        assert record["run"] == _run_id(attempt)
        _rebindable(tuple(outcome["address"]))


def test_dropped_reply_fails_the_worker_waiting_for_it(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start:
        _, conn0 = start(0, attempt, exchange_budget=1.0)
        _, conn1 = start(1, attempt, exchange_budget=1.0, drop_reply=1)
        result0 = _result(conn0)
        _result(conn1)
    assert result0[:2] == ("fail", "exchange"), result0
    assert "3/3 requests and 2/3 replies" in str(result0[2])
    assert not (attempt / slurm_network.result_name(0)).exists()


def test_missing_peer_fails_in_readiness(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start:
        _, conn = start(0, attempt, ready_budget=0.5)
        result = _result(conn)
    assert result[:2] == ("fail", "readiness"), result
    assert "no ready-1.json after 0.5 s" in str(result[2])


def test_late_peer_view_within_the_exchange_budget_passes(tmp_path: Path) -> None:
    # Worker 1 can't see worker 0's record for 1.5 s after it appears, while
    # worker 0 has entered the exchange and sent its requests. Scaled to a 3 s
    # exchange budget, the former 15 s of 90 s is 0.5 s.
    attempt = _attempt(tmp_path)
    with _workers() as start:
        _, conn0 = start(0, attempt, exchange_budget=3.0)
        _, conn1 = start(1, attempt, exchange_budget=3.0, hide_peer_for=1.5)
        result0, result1 = _result(conn0), _result(conn1)
    assert result0[0] == "pass" and result1[0] == "pass", (result0, result1)
    first_send, visible_at = result0[-1]["first_send"], result1[-1]["visible_at"]
    assert result1[-1]["hidden"] > 0
    assert first_send is not None and visible_at is not None
    assert first_send < visible_at
    assert visible_at - first_send > 0.5


def test_missing_route_table_fails_in_discovery(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with pytest.raises(check_udp.CheckError, match="read .*absent") as raised:
        check_udp.check(
            _ctx(0),
            attempt,
            discover=lambda: slurm_network.discover(tmp_path / "absent"),
        )
    assert raised.value.phase == "discovery"
    assert list(attempt.iterdir()) == []


def test_socket_failure_during_discovery_fails_in_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempt = _attempt(tmp_path)
    route = tmp_path / "route"
    route.write_text(
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n"
        "eth0\t00000000\t0105800A\t0003\t0\t0\t0\t00000000\t0\t0\t0\n"
    )

    def no_socket(*args: object, **kwargs: object) -> socket.socket:
        raise OSError(errno.EMFILE, os.strerror(errno.EMFILE))

    monkeypatch.setattr(slurm_network.socket, "socket", no_socket)
    with pytest.raises(
        check_udp.CheckError, match="interface eth0: .*open files"
    ) as raised:
        check_udp.check(
            _ctx(0), attempt, discover=lambda: slurm_network.discover(route)
        )
    assert raised.value.phase == "discovery"
    assert list(attempt.iterdir()) == []


def test_unbindable_address_fails_in_discovery(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with pytest.raises(check_udp.CheckError, match="bind 192.0.2.1") as raised:
        check_udp.check(
            _ctx(0),
            attempt,
            discover=lambda: ("eth0", ipaddress.IPv4Address("192.0.2.1")),
        )
    assert raised.value.phase == "discovery"
    assert list(attempt.iterdir()) == []


def test_frame_from_another_address_fails(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start, _test_peer(attempt) as peer:
        _, conn = start(0, attempt)
        ready = _await_record(attempt / slurm_network.ready_name(0))
        _recv_frame(peer)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as other:
            other.bind(("127.0.0.3", 0))
            other.sendto(
                check_udp.frame(_run_id(attempt), 1, "request", 0),
                (str(ready["ip"]), int(ready["port"])),
            )
            result = _result(conn)
    assert result[:2] == ("fail", "exchange"), result
    assert "frame from 127.0.0.3" in str(result[2])


def test_duplicate_and_foreign_frames_do_not_count(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start, _test_peer(attempt) as peer:
        _, conn = start(0, attempt)
        ready = _await_record(attempt / slurm_network.ready_name(0))
        worker = (str(ready["ip"]), int(ready["port"]))
        requests = [_recv_frame(peer) for _ in range(3)]
        assert sorted(int(r["seq"]) for r in requests) == [0, 1, 2]
        run = _run_id(attempt)
        peer.sendto(check_udp.frame("4242/attempt.other", 1, "request", 0), worker)
        peer.sendto(b"not a frame", worker)
        for seq in (0, 0, 1, 2):
            peer.sendto(check_udp.frame(run, 1, "request", seq), worker)
        replies = [_recv_frame(peer) for _ in range(4)]
        for seq in range(3):
            peer.sendto(check_udp.frame(run, 1, "reply", seq), worker)
        result = _result(conn)
    assert result[0] == "pass", result
    outcome = result[1]
    assert isinstance(outcome, dict)
    assert (outcome["requests"], outcome["replies"]) == (3, 3)
    assert sorted(int(r["seq"]) for r in replies) == [0, 0, 1, 2]
    assert all(r["kind"] == "reply" for r in replies)


def test_foreign_traffic_does_not_extend_the_exchange(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start, _test_peer(attempt) as peer:
        _, conn = start(0, attempt, exchange_budget=0.5)
        ready = _await_record(attempt / slurm_network.ready_name(0))
        worker = (str(ready["ip"]), int(ready["port"]))
        _recv_frame(peer)
        entered = time.monotonic()
        while not conn.poll(0.02):
            assert time.monotonic() - entered < _WAIT
            peer.sendto(check_udp.frame("4242/attempt.other", 1, "request", 0), worker)
        elapsed = time.monotonic() - entered
        result = _result(conn)
    assert result[:2] == ("fail", "exchange"), result
    assert "0/3 requests and 0/3 replies" in str(result[2])
    assert elapsed < 2.5


def test_peer_exit_ends_the_survivor_within_its_budget(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start:
        proc1, _ = start(1, attempt, ready_budget=_WAIT)
        ready1 = _await_record(attempt / slurm_network.ready_name(1))
        proc1.kill()
        proc1.join(_WAIT)
        _, conn0 = start(0, attempt, exchange_budget=0.5)
        result0 = _result(conn0)
    assert result0[:2] == ("fail", "exchange"), result0
    ready0 = json.loads((attempt / slurm_network.ready_name(0)).read_bytes())
    _rebindable((str(ready0["ip"]), int(ready0["port"])))
    _rebindable((str(ready1["ip"]), int(ready1["port"])))


def test_terminated_worker_exits_and_closes_its_socket(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _workers() as start:
        proc, conn = start(0, attempt, ready_budget=_WAIT)
        ready = _await_record(attempt / slurm_network.ready_name(0))
        assert proc.pid is not None
        os.kill(proc.pid, signal.SIGTERM)
        proc.join(_WAIT)
        assert proc.exitcode == 128 + signal.SIGTERM
        with pytest.raises(EOFError):
            conn.recv()
    _rebindable((str(ready["ip"]), int(ready["port"])))


_ENV = {
    "SLURM_JOB_ID": "4242",
    "SLURM_PROCID": "1",
    "SLURM_NODEID": "1",
    "SLURM_NTASKS": "2",
    "SLURM_JOB_NUM_NODES": "2",
    "SLURMD_NODENAME": "cpu-2",
}


def test_two_node_job_step_is_accepted() -> None:
    assert check_udp.context(_ENV, 1000) == check_udp.Context("4242", 1, "cpu-2", 1000)


@pytest.mark.parametrize(
    ("change", "uid", "error"),
    [
        ({"SLURM_JOB_ID": ""}, 1000, "SLURM_JOB_ID is not set"),
        ({"SLURMD_NODENAME": ""}, 1000, "SLURMD_NODENAME is not set"),
        ({}, 0, "running as root"),
        ({"SLURM_JOB_NUM_NODES": "1"}, 1000, "SLURM_JOB_NUM_NODES is 1"),
        ({"SLURM_NTASKS": "4"}, 1000, "SLURM_NTASKS is 4"),
        ({"SLURM_PROCID": "2", "SLURM_NODEID": "2"}, 1000, "SLURM_PROCID is 2"),
        ({"SLURM_NODEID": "0"}, 1000, "one task per node"),
    ],
)
def test_invalid_job_step_fails(change: dict[str, str], uid: int, error: str) -> None:
    with pytest.raises(check_udp.CheckError, match=error) as raised:
        check_udp.context(_ENV | change, uid)
    assert raised.value.phase == "context"


_SRUN = """#!/bin/bash
previous=
for a in "$@"; do
  case $a in --output=*) out=${a#--output=} ;; esac
  [ "$previous" != --family ] || family=$a
  previous=$a
done
printf 'worker log\\n' > "${out//%t/0}"
printf '%s\\n' "$@" >> "$SRUN_ARGS"
printf '%s\\n' "$family" >> "$SRUN_FAMILIES"
if [ "$family" = 6 ]; then exit "$IPV6_STATUS"; else exit "$IPV4_STATUS"; fi
"""


def _launch_setup(
    tmp_path: Path, ipv6_status: int = 7, ipv4_status: int = 7, srun_body: str = _SRUN
) -> tuple[Path, dict[str, str]]:
    # sbatch runs a spooled copy, so the test does too.
    spool = tmp_path / "spool"
    spool.mkdir()
    script = spool / "slurm_script"
    shutil.copy(os.environ["RUN_UDP"], script)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    srun = bin_dir / "srun"
    srun.write_text(srun_body)
    srun.chmod(0o755)
    env = os.environ | {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "HOME": str(tmp_path / "home"),
        "SLURM_JOB_ID": "4242",
        "SRUN_ARGS": str(tmp_path / "srun-args"),
        "SRUN_FAMILIES": str(tmp_path / "srun-families"),
        "IPV6_STATUS": str(ipv6_status),
        "IPV4_STATUS": str(ipv4_status),
    }
    return script, env


def _launcher(
    tmp_path: Path, *args: str, ipv6_status: int = 7, ipv4_status: int = 7
) -> subprocess.CompletedProcess[str]:
    script, env = _launch_setup(tmp_path, ipv6_status, ipv4_status)
    return subprocess.run(
        ["bash", str(script), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=_WAIT,
    )


def _tools(tmp_path: Path) -> Path:
    tools = tmp_path / "tools"
    tools.mkdir()
    shutil.copy(check_udp.__file__, tools / "check_udp.py")
    shutil.copy(slurm_network.__file__, tools / "slurm_network.py")
    return tools


def test_launcher_reports_both_failed_steps_and_keeps_the_logs(tmp_path: Path) -> None:
    tools = _tools(tmp_path)
    run = _launcher(tmp_path, str(tools))
    assert run.returncode == 1, run.stderr
    args = (tmp_path / "srun-args").read_text().splitlines()
    attempt = Path(args[args.index("--rendezvous") + 1])
    assert attempt.parent.parent == tmp_path / "home" / "chr-slurm-checks" / "4242"
    assert stat.S_IMODE(attempt.stat().st_mode) == 0o700
    assert "--kill-on-bad-exit=1" in args
    assert args[args.index("-u") - 1 : args.index("-u") + 2] == [
        "python3",
        "-u",
        str(tools / "check_udp.py"),
    ]
    assert (attempt / "worker-0.out").read_text() == "worker log\n"
    assert f"logs: {attempt.parent}" in run.stdout
    summary = json.loads((attempt.parent / "summary.json").read_text())
    assert summary["selected"] is None
    assert [a["exit_code"] for a in summary["attempts"]] == [7, 7]
    assert (tmp_path / "srun-families").read_text().splitlines() == ["6", "4"]
    assert (attempt.parent / "ipv4" / "worker-0.out").read_text() == "worker log\n"


@pytest.mark.parametrize("which", ["relative", "missing"])
def test_launcher_rejects_a_bad_tools_directory(tmp_path: Path, which: str) -> None:
    tools = "slurm" if which == "relative" else str(tmp_path / "absent")
    run = _launcher(tmp_path, tools)
    assert run.returncode == 2
    assert not (tmp_path / "srun-args").exists()


def test_launcher_prefers_ipv6_and_does_not_run_ipv4(tmp_path: Path) -> None:
    tools = _tools(tmp_path)
    result = _launcher(tmp_path, str(tools), ipv6_status=0)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "srun-families").read_text().splitlines() == ["6"]
    summaries = list(
        (tmp_path / "home" / "chr-slurm-checks" / "4242").glob("attempt.*/summary.json")
    )
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text())
    assert summary["selected"] == "IPv6"
    assert summary["attempts"] == [
        {"family": "IPv6", "exit_code": 0, "outcome": "pass", "directory": "ipv6"}
    ]
    assert not (summaries[0].parent / "ipv4").exists()
    assert "PASS IPv6; IPv4 not tested" in result.stdout


def test_launcher_reports_ipv6_failure_when_ipv4_passes(tmp_path: Path) -> None:
    tools = _tools(tmp_path)
    result = _launcher(tmp_path, str(tools), ipv6_status=1, ipv4_status=0)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "srun-families").read_text().splitlines() == ["6", "4"]
    attempt = next((tmp_path / "home" / "chr-slurm-checks" / "4242").iterdir())
    summary = json.loads((attempt / "summary.json").read_text())
    assert summary["selected"] == "IPv4"
    assert [a["outcome"] for a in summary["attempts"]] == ["fail", "pass"]
    assert [a["exit_code"] for a in summary["attempts"]] == [1, 0]
    assert (attempt / "ipv6" / "worker-0.out").read_text() == "worker log\n"
    assert (attempt / "ipv4" / "worker-0.out").read_text() == "worker log\n"
    assert "PASS IPv4 fallback; IPv6 failed" in result.stdout


def test_launcher_reports_worker_signal_status_and_tries_ipv4(tmp_path: Path) -> None:
    tools = _tools(tmp_path)
    result = _launcher(tmp_path, str(tools), ipv6_status=143, ipv4_status=0)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "srun-families").read_text().splitlines() == ["6", "4"]
    attempt = next((tmp_path / "home" / "chr-slurm-checks" / "4242").iterdir())
    summary = json.loads((attempt / "summary.json").read_text())
    assert summary["selected"] == "IPv4"
    assert [a["exit_code"] for a in summary["attempts"]] == [143, 0]


@contextlib.contextmanager
def _ipv6_sockets() -> Iterator[tuple[socket.socket, socket.socket]]:
    with contextlib.ExitStack() as stack:
        try:
            socks = [
                stack.enter_context(socket.socket(socket.AF_INET6, socket.SOCK_DGRAM))
                for _ in (0, 1)
            ]
            for sock in socks:
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                sock.bind(("::1", 0))
        except OSError as e:
            if e.errno in (
                errno.EADDRNOTAVAIL,
                errno.EAFNOSUPPORT,
                errno.EPROTONOSUPPORT,
            ):
                pytest.skip(f"IPv6 loopback unavailable: {e}")
            raise
        yield socks[0], socks[1]


def test_ipv6_exchange_passes_with_four_field_socket_addresses() -> None:
    results: list[tuple[int, int]] = []
    errors: list[Exception] = []
    with _ipv6_sockets() as socks:

        def worker(slot: int) -> None:
            bound = socks[1 - slot].getsockname()
            try:
                results.append(
                    check_udp.exchange(
                        socks[slot], "ipv6-test", slot, (bound[0], bound[1]), 2
                    )
                )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(slot,)) for slot in (0, 1)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert results == [(3, 3), (3, 3)]


def test_ipv6_exchange_rejects_a_different_source_port() -> None:
    with (
        _ipv6_sockets() as (worker, peer),
        socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as other,
    ):
        other.bind(("::1", 0))
        destination, expected = worker.getsockname(), peer.getsockname()
        other.sendto(check_udp.frame("ipv6-test", 1, "request", 0), destination)
        with pytest.raises(check_udp.CheckError, match="frame from \\[::1\\]"):
            check_udp.exchange(worker, "ipv6-test", 0, (expected[0], expected[1]), 1)


def test_ipv6_worker_rejects_an_ipv4_peer(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    with _ipv6_sockets(), _test_peer(attempt):
        with pytest.raises(
            check_udp.CheckError, match="peer advertised IPv4, expected IPv6"
        ):
            check_udp.check(
                _ctx(0),
                attempt,
                family=6,
                discover=lambda: ("lo", ipaddress.IPv6Address("::1")),
            )
    record = json.loads((attempt / "ready-0.json").read_text())
    assert record["ip"] == "::1"
    assert not (attempt / "result-0.json").exists()


def test_ipv6_result_formats_bracketed_addresses() -> None:
    outcome = check_udp.Outcome(
        "node-0", ("fd00::1", 40000), "node-1", ("fd00::2", 40001), 3, 3, 0, 0
    )
    result = outcome.to_json("run", 0)
    assert result["family"] == "IPv6"
    assert result["address"] == "[fd00::1]:40000"
    assert result["peer"] == "[fd00::2]:40001"


_STUCK_SRUN = '''#!/usr/bin/env python3
import os, signal, socket, subprocess, sys, time
from pathlib import Path
def term(signum, frame):
    marker = os.environ.get('CLEANUP_STARTED')
    if marker:
        Path(marker).write_text('TERM received')
signal.signal(signal.SIGTERM, term)
code = """import os, signal, socket, time
from pathlib import Path
signal.signal(signal.SIGTERM, signal.SIG_IGN)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('127.0.0.1', 0))
Path(os.environ['CHILD_ADDRESS']).write_text(str(sock.getsockname()[1]))
while True: time.sleep(1)
"""
child = subprocess.Popen([sys.executable, '-c', code])
Path(os.environ['STEP_PID']).write_text(str(os.getpid()))
Path(os.environ['CHILD_PID']).write_text(str(child.pid))
while True: time.sleep(1)
'''


def _await_file(path: Path) -> str:
    deadline = time.monotonic() + 5
    while not path.exists():
        assert time.monotonic() < deadline, f"no {path}"
        time.sleep(0.02)
    return path.read_text()


def _await_gone(pid: int) -> None:
    # The direct child is reaped by run_step; orphaned descendants are reaped
    # by the namespace's init, which need not happen before wait returns.
    deadline = time.monotonic() + 5
    while Path(f"/proc/{pid}").exists():
        assert time.monotonic() < deadline, f"process {pid} remains"
        time.sleep(0.02)


def test_cancelling_launcher_kills_its_step_and_descendant(tmp_path: Path) -> None:
    tools = _tools(tmp_path)
    script, env = _launch_setup(tmp_path, srun_body=_STUCK_SRUN)
    env.update(
        {
            "STEP_PID": str(tmp_path / "step-pid"),
            "CHILD_PID": str(tmp_path / "child-pid"),
            "CHILD_ADDRESS": str(tmp_path / "child-address"),
        }
    )
    proc = subprocess.Popen(
        ["bash", str(script), str(tools)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        step_pid = int(_await_file(tmp_path / "step-pid"))
        child_pid = int(_await_file(tmp_path / "child-pid"))
        port = int(_await_file(tmp_path / "child-address"))
        proc.send_signal(signal.SIGTERM)
        time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)  # Cleanup must survive a second signal.
        stdout, stderr = proc.communicate(timeout=15)
        assert proc.returncode == 143, (stdout, stderr)
        _await_gone(step_pid)
        _await_gone(child_pid)
        _rebindable(("127.0.0.1", port))
        attempt = next((tmp_path / "home" / "chr-slurm-checks" / "4242").iterdir())
        assert not (attempt / "ipv4").exists()
        assert "PASS" not in stdout
    finally:
        if (tmp_path / "step-pid").exists():
            with contextlib.suppress(ProcessLookupError):
                os.killpg(int((tmp_path / "step-pid").read_text()), signal.SIGKILL)
        if proc.poll() is None:
            proc.kill()
        proc.communicate(timeout=5)


def test_step_timeout_kills_a_process_that_ignores_term(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tools = _tools(tmp_path)
    _, env = _launch_setup(tmp_path, srun_body=_STUCK_SRUN)
    for name, path in (
        ("STEP_PID", "step-pid"),
        ("CHILD_PID", "child-pid"),
        ("CHILD_ADDRESS", "child-address"),
    ):
        env[name] = str(tmp_path / path)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    directory = tmp_path / "ipv6"
    directory.mkdir()
    start = time.monotonic()
    status = check_udp.run_step(tools, directory, 6, timeout=1)
    assert status == 124
    assert time.monotonic() - start < 12
    assert (tmp_path / "child-address").exists()
    _await_gone(int((tmp_path / "step-pid").read_text()))
    _await_gone(int((tmp_path / "child-pid").read_text()))
    _rebindable(("127.0.0.1", int((tmp_path / "child-address").read_text())))


_TIMED_LAUNCH = """import functools, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import check_udp
check_udp.run_step = functools.partial(check_udp.run_step, timeout=1)
sys.exit(check_udp.main(['--launch', sys.argv[1], '--rendezvous', sys.argv[2]]))
"""


def test_term_received_during_cleanup_prevents_ipv4(tmp_path: Path) -> None:
    tools = _tools(tmp_path)
    _, env = _launch_setup(tmp_path, srun_body=_STUCK_SRUN)
    attempt = _attempt(tmp_path)
    env.update(
        {
            "STEP_PID": str(tmp_path / "step-pid"),
            "CHILD_PID": str(tmp_path / "child-pid"),
            "CHILD_ADDRESS": str(tmp_path / "child-address"),
            "CLEANUP_STARTED": str(tmp_path / "cleanup-started"),
        }
    )
    proc = subprocess.Popen(
        ["python3", "-u", "-c", _TIMED_LAUNCH, str(tools), str(attempt)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        step_pid = int(_await_file(tmp_path / "step-pid"))
        child_pid = int(_await_file(tmp_path / "child-pid"))
        port = int(_await_file(tmp_path / "child-address"))
        # The step has timed out and is inside its cleanup wait before TERM.
        assert _await_file(tmp_path / "cleanup-started") == "TERM received"
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=15)
        assert proc.returncode == 143, (stdout, stderr)
        assert not (attempt / "ipv4").exists()
        assert not (attempt / "summary.json").exists()
        assert "PASS" not in stdout
        _await_gone(step_pid)
        _await_gone(child_pid)
        _rebindable(("127.0.0.1", port))
    finally:
        if (tmp_path / "step-pid").exists():
            with contextlib.suppress(ProcessLookupError):
                os.killpg(int((tmp_path / "step-pid").read_text()), signal.SIGKILL)
        if proc.poll() is None:
            proc.kill()
        proc.communicate(timeout=5)


class _UnreapedSrun:
    """An srun that never exits, as one blocked in the kernel can after SIGKILL."""

    pid = 4242

    def __init__(self, args: list[str], **kwargs: object) -> None:
        self.args = args

    def wait(self, timeout: float | None = None) -> int:
        raise subprocess.TimeoutExpired(self.args, timeout or 0)


def test_unreaped_srun_still_leaves_a_summary_and_tries_ipv4(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    attempt = _attempt(tmp_path)
    signals: list[int] = []
    monkeypatch.setattr(check_udp.subprocess, "Popen", _UnreapedSrun)
    monkeypatch.setattr(check_udp.os, "killpg", lambda pid, sig: signals.append(sig))
    assert check_udp.launch(tmp_path / "tools", attempt) == 1
    summary = json.loads((attempt / "summary.json").read_text())
    assert [a["family"] for a in summary["attempts"]] == ["IPv6", "IPv4"]
    assert [a["exit_code"] for a in summary["attempts"]] == [124, 124]
    assert signals == [signal.SIGTERM, signal.SIGKILL] * 2
    assert capsys.readouterr().err.count("did not exit after SIGKILL") == 2


def test_worker_saves_ipv6_discovery_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempt = _attempt(tmp_path)
    for name, value in _ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(check_udp.os, "getuid", lambda: 1000)
    monkeypatch.setattr(check_udp.signal, "signal", lambda *args: None)

    def unavailable() -> tuple[str, ipaddress.IPv6Address]:
        raise slurm_network.NetworkError("no up IPv6 default route")

    monkeypatch.setattr(slurm_network, "discover_ipv6", unavailable)
    assert check_udp.main(["--family", "6", "--rendezvous", str(attempt)]) == 1
    failure = json.loads((attempt / "failure-1.json").read_text())
    assert failure["family"] == "IPv6"
    assert failure["phase"] == "discovery"
    assert "no up IPv6 default route" in failure["error"]
    assert not (attempt / "result-1.json").exists()


def test_failed_summary_write_cannot_report_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(check_udp, "run_step", lambda *args: 0)
    monkeypatch.setattr(check_udp.signal, "signal", lambda *args: None)

    def no_space(*args: object) -> Path:
        raise OSError(errno.ENOSPC, "no space for summary")

    monkeypatch.setattr(slurm_network, "publish", no_space)
    assert (
        check_udp.main(["--launch", str(tmp_path), "--rendezvous", str(tmp_path)]) == 1
    )
    output = capsys.readouterr()
    assert "PASS" not in output.out
    assert "FAIL launch" in output.err
    assert not (tmp_path / "summary.json").exists()
