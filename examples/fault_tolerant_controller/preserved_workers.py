# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Keep worker state while externally killing supervised controller generations.

Controller is the client shutdown boundary; retained workers belong to Keeper.
Other actors on the joining this_proc() are outside this ownership policy.
Adopting its bootstrap root or process is deferred Monarch core work.

Run ``python examples/fault_tolerant_controller/preserved_workers.py --help``.
See PRESERVED_WORKERS.md for the protocol, topology, and limitations.
"""

import argparse
import asyncio
import json
import os
import random
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast, TypeVar

from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch._src.actor.actor_mesh import ActorMesh
from monarch._src.actor.token import connect, publish
from monarch.actor import (
    Actor,
    ActorError,
    concurrent_endpoint,
    context,
    current_actor_name,
    current_rank,
    endpoint,
    InvalidReferenceReason,
    MeshFailure,
    ProcMesh,
    shutdown_context,
    this_host,
    this_proc,
    TransportFailureReason,
    UndeliverableMessageEnvelope,
    UndeliverableReason,
)
from monarch.config import configure, get_global_config

configure(message_delivery_timeout="2s", data_supervision_timeout="3s")
CALL_TIMEOUT = 15.0
T = TypeVar("T")


@dataclass
class Snapshot:
    """Worker identity, durable-in-process progress, and outstanding requests."""

    pid: int
    actor: str
    rank: int
    ledger: dict[int, int]
    value: int
    active: dict[str, int]
    retired_replies: int


def payload(step: int) -> int:
    """Return the deterministic payload for a logical update, independent of epoch."""
    return random.Random(step).randint(1, 1000)


class Worker(Actor):
    """Own a toy optimizer ledger and tolerate lost controller replies."""

    def __init__(self) -> None:
        """Initialize persistent state that outlives every controller generation."""
        self.rank = current_rank().rank
        self.ledger: dict[int, int] = {}
        self.value = 0
        self.active: dict[str, int] = {}
        self.sessions: set[str] = set()
        self.retired_replies = 0
        self.probe_waiting = False

    @concurrent_endpoint
    async def snapshot(self) -> Snapshot:
        """Read progress without waiting for a deliberately delayed update reply."""
        return Snapshot(
            os.getpid(),
            current_actor_name(),
            self.rank,
            dict(self.ledger),
            self.value,
            dict(self.active),
            self.retired_replies,
        )

    @concurrent_endpoint
    async def register(self, actor: str) -> None:
        """Register the only application actors to which this worker sends replies."""
        self.sessions.add(actor)

    @concurrent_endpoint
    async def apply(self, step: int, value: int, session: str) -> int:
        """Apply each logical step once, including requests with lost replies."""
        request = f"{session}/{uuid.uuid4().hex}"
        self.active[request] = step
        try:
            # Leave both pre-commit and committed-but-unanswered kill windows.
            await asyncio.sleep(0.12 + self.rank * 0.02)
            if step in self.ledger:
                if self.ledger[step] != value:
                    raise ValueError("operation id reused with a different payload")
            else:
                if step != len(self.ledger):
                    raise ValueError("out-of-order update")
                self.ledger[step] = value
                self.value += value
            await asyncio.sleep(0.30)
            return sum(self.ledger[k] for k in range(step + 1))
        finally:
            del self.active[request]

    @concurrent_endpoint
    async def wait_for_failure(self) -> None:
        """Keep a reply outstanding while the observability probe fails the actor."""
        self.probe_waiting = True
        await asyncio.sleep(3600)

    @concurrent_endpoint
    async def probe_ready(self) -> bool:
        """Confirm that the failure probe request is running before the crash."""
        return self.probe_waiting

    @endpoint
    async def crash(self) -> None:
        """Fail this worker actor to verify that supervision remains observable."""
        context().actor_instance.kill("intentional worker failure")

    def _handle_delivery_failure_event(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        # Registration happens before work, so an unannounced crash needs no
        # advance worker notification. Full addresses include the unique ProcId.
        failure = message.root_delivery_failure()
        if failure is None:
            return False
        try:
            registered = str(message.dest().actor_id) in self.sessions
        except ValueError:
            return False
        recipient_lost = failure.invalid_reference_reason in (
            InvalidReferenceReason.ActorStopped,
            InvalidReferenceReason.ActorFailed,
            InvalidReferenceReason.ActorNotExist,
            InvalidReferenceReason.ProcNotExist,
        )
        transport_lost = (
            failure.undeliverable_reason == UndeliverableReason.Transport
            and failure.transport_failure_reason
            in (
                TransportFailureReason.ChannelClosed,
                TransportFailureReason.AckTimedOut,
                TransportFailureReason.DialFailed,
                TransportFailureReason.NoRoute,
                TransportFailureReason.LinkUnavailable,
                TransportFailureReason.ForwarderUnavailable,
            )
        )
        handled = registered and (
            recipient_lost
            or transport_lost
            or failure.undeliverable_reason == UndeliverableReason.PortGone
        )
        if handled:
            self.retired_replies += 1
        return handled


class Controller(Actor):
    """Drive persistent sibling workers from an expendable process."""

    def __init__(self, workers: Worker) -> None:
        """Retain references without becoming the workers' owner."""
        self.workers = workers

    @endpoint
    async def identity(self) -> str:
        """Identify the actual supervised application actor, not its bootstrap root."""
        return current_actor_name()

    @endpoint
    async def run(self, steps: int, hang: bool) -> None:
        """Replay stable logical operations; the worker ledger deduplicates them."""
        if hang:
            await asyncio.sleep(3600)
        snapshots = list((await self.workers.snapshot.call()).values())
        # Some ranks may have committed the interrupted step, while others have
        # not. Replay from the lowest progress; do not discard in-flight work.
        for step in range(min(len(s.ledger) for s in snapshots), steps):
            await self.workers.apply.call(step, payload(step), current_actor_name())


class Keeper(Actor):
    """Own persistent workers and each remote Controller actor as siblings."""

    def __init__(self) -> None:
        """Create workers beneath this keeper's supervision context."""
        self.procs = this_host().spawn_procs(per_host={"replica": 2}, name="retained")
        self.workers = self.procs.spawn("workers", Worker)
        self.controller: Controller | None = None
        self.names: dict[str, int] = {}
        self.failures: set[int] = set()
        self.worker_failure = False
        self.worker_name = ""
        self.expect_worker_failure = False
        self.session = ""
        self.generation = -1

    @endpoint
    async def join(self, procs: ProcMesh, generation: int) -> Controller:
        """Spawn the application controller here, establishing actual ownership."""
        if self.controller is not None:
            raise ValueError("previous controller has not been retired")
        controller = procs.spawn(f"controller_{generation}", Controller, self.workers)
        await controller.initialized
        self.names[await cast(ActorMesh, controller)._name] = generation
        self.session = await controller.identity.call_one()
        await self.workers.register.call(self.session)
        self.generation = generation
        self.controller = controller
        return controller

    @endpoint
    async def state(self) -> tuple[int, list[int], list[Snapshot], bool]:
        """Expose readiness, supervision records, and persistent worker state."""
        self.worker_name = await cast(ActorMesh, self.workers)._name
        snapshots = list((await self.workers.snapshot.call()).values())
        return self.generation, sorted(self.failures), snapshots, self.worker_failure

    @endpoint
    async def release(self, failed: bool) -> None:
        """Allow a new generation only after matching supervision or a clean stop."""
        if failed and self.generation not in self.failures:
            raise ValueError("controller failure has not reached its keeper")
        if self.controller is not None and not failed:
            await self.controller.stop()
        self.controller = None

    @endpoint
    async def replay_probe(self) -> None:
        """Verify old-step replay and changed-payload rejection leave state intact."""
        before = list((await self.workers.snapshot.call()).values())
        result = await self.workers.apply.call(0, payload(0), current_actor_name())
        assert all(value == payload(0) for value in result.values())
        try:
            await self.workers.apply.call(0, payload(0) + 1, current_actor_name())
        except ActorError as error:
            assert isinstance(error.exception, ValueError)
        else:
            raise AssertionError("changed payload was accepted")
        after = list((await self.workers.snapshot.call()).values())
        assert before == after

    @endpoint
    async def worker_probe(self) -> None:
        """Fail worker actors and require their mesh call to raise SupervisionError."""
        pending = self.workers.wait_for_failure.call()
        deadline = time.monotonic() + CALL_TIMEOUT / 2
        while time.monotonic() < deadline:
            if all((await self.workers.probe_ready.call()).values()):
                break
            await asyncio.sleep(0.02)
        else:
            raise TimeoutError("worker probe request did not start")
        self.expect_worker_failure = True
        self.workers.crash.broadcast()
        try:
            await pending
        except SupervisionError:
            return
        raise AssertionError("worker failure was not observable")

    @endpoint
    async def failure_seen(self) -> bool:
        """Report whether the deliberate worker failure reached supervision."""
        return self.worker_failure

    @endpoint
    async def close(self) -> None:
        """Stop all keeper-owned worker processes."""
        await self.procs.stop()

    def __supervise__(self, failure: MeshFailure) -> bool:
        """Handle only owned controller meshes and the deliberate worker probe."""
        name = failure.mesh_name
        if name in self.names:
            self.failures.add(self.names[name])
            print(f"supervised controller generation {self.names[name]}", flush=True)
            return True
        if self.expect_worker_failure and name == self.worker_name:
            self.worker_failure = True
            print("supervised intentional worker failure", flush=True)
            return True
        return False


def call_state(keeper: Keeper) -> tuple[int, list[int], list[Snapshot], bool]:
    """Read keeper status with a bounded endpoint deadline."""
    return keeper.state.call_one().get(timeout=CALL_TIMEOUT)


def wait_until(check: Callable[[], T], deadline: float, description: str) -> T:
    """Poll a bounded condition and report a useful failure on timeout."""
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.02)
    raise TimeoutError(description)


def process_start(pid: int) -> str | None:
    """Return Linux process start time to guard against PID reuse during cleanup."""
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except FileNotFoundError:
        return None


def remaining_workers(run_id: str, identities: dict[int, str]) -> list[int]:
    """Find only live processes with both the captured start time and run marker."""
    remaining = []
    for pid, started in identities.items():
        if process_start(pid) != started:
            continue
        try:
            environment = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
        except FileNotFoundError:
            continue
        if f"PRESERVED_RUN_ID={run_id}".encode() in environment:
            remaining.append(pid)
    return remaining


def controller_main(token: str, generation: int, steps: int, hang: bool) -> None:
    """Offer this process to Keeper, then await its owned application's work."""
    keeper = connect(token, Keeper)
    controller = keeper.join.call_one(this_proc(), generation).get(timeout=CALL_TIMEOUT)
    controller.run.call_one(steps, hang).get(timeout=120)
    keeper.release.call_one(False).get(timeout=CALL_TIMEOUT)
    shutdown_context().get(timeout=CALL_TIMEOUT)


def keeper_main(args: argparse.Namespace) -> None:
    """Crash several generations in-flight, validate replay, and clean up."""
    effective = get_global_config()
    for key, value in (
        ("message_delivery_timeout", "2s"),
        ("data_supervision_timeout", "3s"),
    ):
        if effective[key] != value:
            raise ValueError(
                f"unset the environment override for {key}: expected {value}"
            )
    run_id = uuid.uuid4().hex
    os.environ["PRESERVED_RUN_ID"] = run_id
    rng = random.Random(args.seed)
    keeper = this_proc().spawn("keeper", Keeper)
    process: subprocess.Popen | None = None
    identities: dict[int, str] = {}
    baseline: dict[int, tuple[int, str]] = {}
    killed = []
    deadline = time.monotonic() + args.timeout
    try:
        keeper.initialized.get(timeout=CALL_TIMEOUT)
        publication = publish(keeper)
        initial = call_state(keeper)[2]
        for snapshot in initial:
            started = process_start(snapshot.pid)
            assert started is not None
            identities[snapshot.pid] = started
            assert remaining_workers(run_id, {snapshot.pid: started}) == [
                snapshot.pid
            ], "worker did not inherit the cleanup run marker"
            baseline[snapshot.rank] = (snapshot.pid, snapshot.actor)
        for generation in range(args.kills + 1):
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "controller",
                publication.token,
                str(generation),
                str(args.steps),
            ]
            if args.inject == "hang" and generation == args.kills:
                command.append("hang")
            process = subprocess.Popen(command)
            wait_until(
                lambda generation=generation: call_state(keeper)[0] == generation,
                min(deadline, time.monotonic() + CALL_TIMEOUT),
                "controller did not join",
            )
            if generation < args.kills:
                wait_until(
                    lambda: any(s.active for s in call_state(keeper)[2]),
                    min(deadline, time.monotonic() + CALL_TIMEOUT),
                    "no in-flight request",
                )
                delay = (
                    rng.uniform(0.03, 0.08)
                    if generation % 2 == 0
                    else rng.uniform(0.18, 0.24)
                )
                time.sleep(delay)
                before = call_state(keeper)[2]
                assert any(s.active for s in before), "kill missed in-flight work"
                process.kill()
                assert process.wait(timeout=CALL_TIMEOUT) == -signal.SIGKILL
                # Reaping the old OS process and observing its owned mesh failure
                # both precede admission of a replacement.
                wait_until(
                    lambda generation=generation: generation in call_state(keeper)[1],
                    min(deadline, time.monotonic() + CALL_TIMEOUT),
                    "missing controller supervision",
                )
                wait_until(
                    lambda: all(not s.active for s in call_state(keeper)[2]),
                    min(deadline, time.monotonic() + CALL_TIMEOUT),
                    "old requests did not drain",
                )
                keeper.release.call_one(True).get(timeout=CALL_TIMEOUT)
                record = {
                    "generation": generation,
                    "delay": delay,
                    "committed_in_flight": any(
                        any(k in s.ledger for k in s.active.values()) for s in before
                    ),
                }
                killed.append(record)
                print(json.dumps({"killed": record}), flush=True)
            else:
                try:
                    code = process.wait(
                        timeout=max(
                            0.01,
                            min(args.generation_timeout, deadline - time.monotonic()),
                        )
                    )
                except subprocess.TimeoutExpired:
                    raise TimeoutError("final controller deadline exceeded") from None
                assert code == 0, f"final controller exited {code}"
            state = call_state(keeper)
            assert {s.rank: (s.pid, s.actor) for s in state[2]} == baseline
        final = call_state(keeper)
        expected = {step: payload(step) for step in range(args.steps)}
        assert killed and len(final[1]) == args.kills
        assert all(
            s.ledger == expected and s.value == sum(expected.values()) and not s.active
            for s in final[2]
        )
        # Wait for late failed replies to be classified before probing a worker.
        wait_until(
            lambda: sum(s.retired_replies for s in call_state(keeper)[2]) > 0,
            min(deadline, time.monotonic() + CALL_TIMEOUT),
            "no lost reply observed",
        )
        assert any(k["committed_in_flight"] for k in killed), (
            "no committed lost-reply kill"
        )
        keeper.replay_probe.call_one().get(timeout=CALL_TIMEOUT)
        if args.inject != "skip-worker-fault":
            keeper.worker_probe.call_one().get(timeout=CALL_TIMEOUT)
        wait_until(
            lambda: keeper.failure_seen.call_one().get(timeout=CALL_TIMEOUT),
            min(deadline, time.monotonic() + CALL_TIMEOUT),
            "missing worker supervision",
        )
        report = {
            "result": "PASS",
            "seed": args.seed,
            "kills": killed,
            "workers": [asdict(s) for s in final[2]],
            "value": sum(expected.values()),
        }
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=CALL_TIMEOUT)
        try:
            keeper.close.call_one().get(timeout=CALL_TIMEOUT)
        finally:
            try:
                shutdown_context().get(timeout=CALL_TIMEOUT)
            finally:
                leftover = remaining_workers(run_id, identities)
                for pid in leftover:
                    os.kill(pid, signal.SIGKILL)
                wait_until(
                    lambda: not remaining_workers(run_id, identities),
                    time.monotonic() + CALL_TIMEOUT,
                    "worker cleanup failed",
                )
                if leftover:
                    raise AssertionError(f"emergency cleanup required: {leftover}")
        print("cleanup complete", flush=True)
    print(json.dumps(report), flush=True)


def main() -> None:
    """Parse the public driver options or the private controller bootstrap role."""
    if len(sys.argv) > 1 and sys.argv[1] == "controller":
        controller_main(
            sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), len(sys.argv) > 5
        )
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--kills", type=int, default=4)
    parser.add_argument("--steps", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--generation-timeout", type=float, default=30)
    parser.add_argument("--inject", choices=["hang", "skip-worker-fault"], default="")
    args = parser.parse_args()
    if (
        args.kills < 2
        or args.steps <= args.kills
        or args.timeout <= 0
        or args.generation_timeout <= 0
    ):
        parser.error("require kills >= 2, steps > kills, and timeout > 0")
    keeper_main(args)


if __name__ == "__main__":
    main()
