# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""A two-node UDP check for Slurm jobs.

`run_udp.sh` tries IPv6 first, then IPv4 if needed, with one job step per family.
Each worker binds an address of its default-route interface, publishes the bound
address in the attempt directory, reads its peer's, and sends the peer three
numbered requests while answering the peer's. A worker passes only when it has
received all three of the peer's requests and all three replies, each from the
peer's published address. Nothing is retransmitted, so a lost datagram fails
the check.
"""

import argparse
import ipaddress
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

import slurm_network

READY_BUDGET = 300.0
EXCHANGE_BUDGET = 90.0
FRAME_MAX = 256

_SEQS = frozenset({0, 1, 2})
_FRAME_FIELDS = frozenset({"v", "run", "sender", "kind", "seq"})

Address = tuple[str, int]
SendTo = Callable[[socket.socket, bytes, Address], object]


class CheckError(Exception):
    """A failed check, with the phase it failed in."""

    def __init__(self, phase: str, message: str) -> None:
        super().__init__(f"{phase}: {message}")
        self.phase = phase


@dataclass(frozen=True)
class Context:
    """This worker's place in the Slurm job."""

    job_id: str
    slot: int
    node: str
    uid: int


@dataclass(frozen=True)
class Outcome:
    """A passed check."""

    node: str
    address: Address
    peer_node: str
    peer_address: Address
    requests: int
    replies: int
    ready_elapsed: float
    exchange_elapsed: float

    def to_json(self, run: str, slot: int) -> dict[str, object]:
        return {
            "v": slurm_network.FORMAT,
            "run": run,
            "slot": slot,
            "node": self.node,
            "outcome": "pass",
            "family": f"IPv{ipaddress.ip_address(self.address[0]).version}",
            "address": address_text(self.address),
            "peer": address_text(self.peer_address),
            "requests": self.requests,
            "replies": self.replies,
            "ready_elapsed": round(self.ready_elapsed, 3),
            "exchange_elapsed": round(self.exchange_elapsed, 3),
        }


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def address_text(address: Address) -> str:
    """Formats an endpoint, with brackets around an IPv6 address."""
    ip, port = address
    return f"[{ip}]:{port}" if ":" in ip else f"{ip}:{port}"


def context(env: Mapping[str, str], uid: int) -> Context:
    """Validates a two-node, two-task job step with one task per node."""

    def var(name: str) -> str:
        value = env.get(name)
        if not value:
            raise CheckError("context", f"{name} is not set")
        return value

    job_id = var("SLURM_JOB_ID")
    procid = var("SLURM_PROCID")
    nodeid = var("SLURM_NODEID")
    ntasks = var("SLURM_NTASKS")
    nnodes = var("SLURM_JOB_NUM_NODES")
    node = var("SLURMD_NODENAME")
    if uid == 0:
        raise CheckError("context", "running as root")
    if nnodes != "2":
        raise CheckError("context", f"SLURM_JOB_NUM_NODES is {nnodes}, expected 2")
    if ntasks != "2":
        raise CheckError("context", f"SLURM_NTASKS is {ntasks}, expected 2")
    if procid not in ("0", "1"):
        raise CheckError("context", f"SLURM_PROCID is {procid}, expected 0 or 1")
    if nodeid != procid:
        raise CheckError(
            "context",
            f"SLURM_NODEID {nodeid} differs from SLURM_PROCID {procid}: "
            "expected one task per node",
        )
    return Context(job_id, int(procid), node, uid)


def send(sock: socket.socket, data: bytes, address: Address) -> None:
    """Sends one datagram; `check` and `exchange` take a replacement for tests."""
    sock.sendto(data, address)


def frame(run: str, sender: int, kind: str, seq: int) -> bytes:
    """Encodes one request or reply."""
    return json.dumps(
        {"v": 1, "run": run, "sender": sender, "kind": kind, "seq": seq},
        separators=(",", ":"),
    ).encode()


def _this_run(data: bytes, run: str) -> dict[str, object] | None:
    """Returns the decoded frame if it belongs to `run`, else None."""
    if len(data) > FRAME_MAX:
        return None
    try:
        obj = json.loads(data.decode())
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(obj, dict) or obj.get("run") != run:
        return None
    return obj


def _valid(msg: dict[str, object], sender: int) -> bool:
    seq = msg.get("seq")
    return (
        set(msg) == _FRAME_FIELDS
        and msg["v"] == 1
        and msg["sender"] == sender
        and msg["kind"] in ("request", "reply")
        and type(seq) is int
        and seq in _SEQS
    )


def exchange(
    sock: socket.socket,
    run: str,
    slot: int,
    peer: Address,
    budget: float,
    *,
    sendto: SendTo = send,
) -> tuple[int, int]:
    """Exchanges three requests and three replies with `peer`, within `budget`.

    Returns the counts of distinct requests and replies received. Traffic from
    another run is ignored and does not extend the budget; a frame of this run
    from any address but `peer`, or with the wrong sender, kind or sequence,
    fails the check. A duplicate request is answered again but counted once.
    """
    deadline = time.monotonic() + budget
    for seq in sorted(_SEQS):
        sendto(sock, frame(run, slot, "request", seq), peer)
    requests: set[int] = set()
    replies: set[int] = set()
    while requests != _SEQS or replies != _SEQS:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise CheckError(
                "exchange",
                f"timed out after {budget:g} s with {len(requests)}/3 requests "
                f"and {len(replies)}/3 replies from {address_text(peer)}",
            )
        sock.settimeout(remaining)
        try:
            data, source = sock.recvfrom(FRAME_MAX + 1)
        except TimeoutError:
            continue
        msg = _this_run(data, run)
        if msg is None:
            continue
        source_address = (source[0], source[1])
        if source_address != peer:
            raise CheckError(
                "exchange",
                f"frame from {address_text(source_address)}, expected {address_text(peer)}",
            )
        if not _valid(msg, 1 - slot):
            raise CheckError("exchange", f"invalid frame from the peer: {msg!r}")
        seq = msg["seq"]
        assert isinstance(seq, int)
        if msg["kind"] == "request":
            sendto(sock, frame(run, slot, "reply", seq), peer)
            requests.add(seq)
        else:
            replies.add(seq)
    return len(requests), len(replies)


def check(
    ctx: Context,
    rendezvous: Path,
    *,
    family: int = 4,
    discover: Callable[[], tuple[str, slurm_network.IPAddress]] | None = None,
    ready_budget: float = READY_BUDGET,
    exchange_budget: float = EXCHANGE_BUDGET,
    read: Callable[[Path], bytes | None] = slurm_network.read_record,
    sendto: SendTo = send,
) -> Outcome:
    """Runs one worker's check through to its published result."""
    run = f"{ctx.job_id}/{rendezvous.parent.name}/{rendezvous.name}"
    peer_slot = 1 - ctx.slot
    if discover is None:
        discover = (
            slurm_network.discover_ipv6 if family == 6 else slurm_network.discover
        )
    try:
        iface, ip = discover()
    except slurm_network.NetworkError as e:
        raise CheckError("discovery", str(e)) from e
    log(f"discovery: {iface} {ip}")
    if ip.version != family:
        raise CheckError("discovery", f"expected IPv{family}, got {ip}")
    try:
        sock = socket.socket(
            socket.AF_INET6 if family == 6 else socket.AF_INET, socket.SOCK_DGRAM
        )
    except OSError as e:
        raise CheckError("discovery", f"socket: {e}") from e
    with sock:
        try:
            if family == 6:
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            sock.bind((str(ip), 0))
        except OSError as e:
            raise CheckError("discovery", f"bind {ip}: {e}") from e
        bound = sock.getsockname()
        address: Address = (bound[0], bound[1])
        me = slurm_network.Ready(run, ctx.slot, ctx.node, ctx.uid, ip, address[1])
        try:
            slurm_network.publish(
                rendezvous, slurm_network.ready_name(ctx.slot), me.to_json()
            )
        except (slurm_network.RecordError, OSError) as e:
            raise CheckError("readiness", f"publish: {e}") from e
        log(
            f"readiness: published {address_text(address)}; waiting for slot {peer_slot}"
        )
        start = time.monotonic()
        try:
            peer = slurm_network.await_ready(
                rendezvous, peer_slot, run, ready_budget, read=read
            )
        except (slurm_network.RecordError, OSError) as e:
            raise CheckError("readiness", str(e)) from e
        if peer.node == ctx.node:
            raise CheckError("readiness", f"peer is on this node, {peer.node}")
        if peer.ip == ip:
            raise CheckError("readiness", f"peer has this worker's address, {ip}")
        if peer.ip.version != family:
            raise CheckError(
                "readiness",
                f"peer advertised IPv{peer.ip.version}, expected IPv{family}",
            )
        ready_elapsed = time.monotonic() - start
        peer_address: Address = (str(peer.ip), peer.port)
        log(
            f"exchange: entered after {ready_elapsed:.1f} s of readiness; "
            f"peer {peer.node} {address_text(peer_address)}"
        )
        start = time.monotonic()
        try:
            requests, replies = exchange(
                sock, run, ctx.slot, peer_address, exchange_budget, sendto=sendto
            )
        except OSError as e:
            raise CheckError("exchange", str(e)) from e
        exchange_elapsed = time.monotonic() - start
    outcome = Outcome(
        ctx.node,
        address,
        peer.node,
        peer_address,
        requests,
        replies,
        ready_elapsed,
        exchange_elapsed,
    )
    try:
        slurm_network.publish(
            rendezvous,
            slurm_network.result_name(ctx.slot),
            outcome.to_json(run, ctx.slot),
        )
    except (slurm_network.RecordError, OSError) as e:
        raise CheckError("result", str(e)) from e
    return outcome


def _terminate(signum: int, stack: FrameType | None) -> None:
    log(f"terminated by signal {signum}")
    raise SystemExit(128 + signum)


def _stop_step(proc: subprocess.Popen[bytes]) -> None:
    """Stops the process group, then propagates any stop requested during cleanup."""
    received: int | None = None

    def remember(signum: int, frame: FrameType | None) -> None:
        nonlocal received
        if received is None:
            received = signum

    handlers = {
        sig: signal.signal(sig, remember)
        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
    }
    try:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            # srun can outlive SIGKILL while blocked in the kernel. Leaving it
            # unreaped lets the caller still record this step and fall back.
            log(f"srun {proc.pid} did not exit after SIGKILL")
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
    if received is not None:
        _terminate(received, None)


def run_step(
    tools: Path,
    directory: Path,
    family: int,
    *,
    timeout: float = READY_BUDGET + EXCHANGE_BUDGET + 30,
) -> int:
    """Runs both workers for one family; cancellation never starts a fallback."""
    command = [
        "srun",
        "--kill-on-bad-exit=1",
        "--nodes=2",
        "--ntasks=2",
        "--ntasks-per-node=1",
        f"--output={directory}/worker-%t.out",
        f"--error={directory}/worker-%t.err",
        "python3",
        "-u",
        str(tools / "check_udp.py"),
        "--family",
        str(family),
        "--rendezvous",
        str(directory),
    ]
    try:
        proc = subprocess.Popen(command, start_new_session=True)
    except OSError as e:
        raise CheckError("launch", str(e)) from e
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        log(f"IPv{family}: Slurm step exceeded its time limit")
        return 124
    finally:
        _stop_step(proc)


def launch(tools: Path, attempt: Path) -> int:
    """Tries IPv6, then IPv4, recording each step and the selected family."""
    attempts: list[dict[str, object]] = []
    selected: str | None = None
    for family in (6, 4):
        directory = attempt / f"ipv{family}"
        directory.mkdir()
        log(f"IPv{family}: starting; logs: {directory}")
        status = run_step(tools, directory, family)
        attempts.append(
            {
                "family": f"IPv{family}",
                "exit_code": status,
                "outcome": "pass" if status == 0 else "fail",
                "directory": directory.name,
            }
        )
        log(f"IPv{family}: {'PASS' if status == 0 else 'FAIL'} (step exit {status})")
        if status == 0:
            selected = f"IPv{family}"
            break
    slurm_network.publish(
        attempt,
        "summary.json",
        {
            "selected": selected,
            "attempts": attempts,
        },
    )
    if selected == "IPv6":
        print("PASS IPv6; IPv4 not tested", flush=True)
    elif selected == "IPv4":
        print("PASS IPv4 fallback; IPv6 failed (see ipv6 logs)", flush=True)
    else:
        print("FAIL IPv6 and IPv4 (see each family's logs)", flush=True)
    return 0 if selected else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rendezvous",
        type=Path,
        required=True,
        help="the shared attempt directory, mounted at the same path on both nodes",
    )
    parser.add_argument("--family", type=int, choices=(4, 6), default=4)
    parser.add_argument(
        "--launch", type=Path, help="run both Slurm steps from this tools directory"
    )
    args = parser.parse_args(argv)
    log(
        f"start: {datetime.now(timezone.utc).isoformat()} pid {os.getpid()} "
        f"{sys.executable} {sys.version.split()[0]}"
    )
    if sys.version_info < (3, 11):
        log("FAIL setup: Python 3.11 or later is required")
        return 1
    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGHUP, _terminate)
    ctx: Context | None = None
    try:
        if args.launch is not None:
            return launch(args.launch, args.rendezvous)
        ctx = context(os.environ, os.getuid())
        log(f"context: job {ctx.job_id} slot {ctx.slot} node {ctx.node} uid {ctx.uid}")
        log(f"family: IPv{args.family}")
        outcome = check(ctx, args.rendezvous, family=args.family)
    except CheckError as e:
        log(f"FAIL {e}")
        if ctx is not None:
            try:
                slurm_network.publish(
                    args.rendezvous,
                    f"failure-{ctx.slot}.json",
                    {
                        "family": f"IPv{args.family}",
                        "node": ctx.node,
                        "phase": e.phase,
                        "error": str(e),
                    },
                )
            except (slurm_network.RecordError, OSError) as report_error:
                log(f"could not save failure: {report_error}")
        return 1
    except OSError as e:
        log(f"FAIL launch: {e}")
        return 1
    print(
        f"PASS IPv{args.family} {outcome.node} {address_text(outcome.address)} "
        f"peer {outcome.peer_node} {address_text(outcome.peer_address)}: "
        f"{outcome.requests}/3 requests, {outcome.replies}/3 replies",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
