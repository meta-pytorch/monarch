# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Restart an entire controller subtree from an external complete checkpoint.

Run this file without a role for the keeper; it launches independent Python
controller bootstraps, which join the private token API. The keeper spawns the
application Controller actor on each bootstrap's process. Token join itself
only exchanges references and does not adopt the bootstrap's root actor.
Controller is the client shutdown boundary: all application children are spawned
from its context. Other actors on the joining this_proc() are outside that
boundary; adopting the bootstrap root or process is deferred core work.

The Keeper actor owns each Controller actor. Each Controller owns its trainer
and generator actor AND process meshes. They run on a borrowed keeper host,
whose lifetime is outside the failed subtree. A hard controller-process kill
must therefore stop all old workers through Monarch's remote orphan cleanup.
The keeper verifies their exit before starting a replacement with fresh workers.

Checkpoints live in a keeper-owned directory and contain model, progress, and
all generator RNG state. Crashes occur after uncommitted sampling. Recovery
must reproduce the complete final state of uninterrupted training. See README.md
for the topology, commands, deadlines, and failure injections. Linux only.
"""

import argparse
import importlib
import os
import pickle
import random
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from monarch._src.actor.actor_mesh import ActorMesh
from monarch._src.actor.token import connect, publish
from monarch.actor import (
    Actor,
    current_rank,
    endpoint,
    HostMesh,
    MeshFailure,
    ProcMesh,
    shutdown_context,
    this_host,
    this_proc,
)
from monarch.config import configure, get_global_config

# Short bounded liveness timers keep this CPU-only demonstration practical.
RUNTIME_CONFIG = {
    "message_delivery_timeout": "3s",
    "mesh_orphan_timeout": "8s",
    "data_supervision_timeout": "8s",
    "process_exit_timeout": "3s",
}
CALL_TIMEOUT = 30.0
CLEANUP_TIMEOUT = 45.0
# The fault injection waits through several one-second heartbeat polls. This is
# a startup warmup, not a public readiness guarantee for arbitrary early crashes.
KEEPALIVE_WARMUP = 3.0
TARGET = 10.0
GENERATORS = 2


@dataclass(frozen=True)
class Checkpoint:
    """Complete training state, including the Gaussian generator's cached value."""

    step: int
    weight: float
    rng_states: tuple[Any, ...]


@dataclass(frozen=True)
class ProcessIdentity:
    """Linux pid and start time, which together survive pid reuse checks."""

    pid: int
    start_time: int


@dataclass(frozen=True)
class Options:
    """Keeper-selected options shared with each controller generation."""

    steps: int
    restarts: int
    seed: int
    random_kills: bool
    inject: str


def initial_checkpoint() -> Checkpoint:
    """Return the deterministic initial model and independent generator streams."""
    return Checkpoint(
        0, 0.0, tuple(random.Random(i).getstate() for i in range(GENERATORS))
    )


def reference_checkpoint(steps: int) -> Checkpoint:
    """Compute an uninterrupted reference without actors or subprocesses."""
    rngs = [random.Random(i) for i in range(GENERATORS)]
    weight = 0.0
    for _ in range(steps):
        samples = [rng.gauss(weight, 3.0) for rng in rngs]
        weight = min(samples, key=lambda sample: abs(sample - TARGET))
    return Checkpoint(steps, weight, tuple(rng.getstate() for rng in rngs))


def atomic_write(path: Path, value: object) -> None:
    """Commit one trusted local pickle with atomic rename and durable ordering."""
    temporary = path.with_suffix(".pending")
    with temporary.open("wb") as stream:
        pickle.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def read_checkpoint(path: Path) -> Checkpoint:
    """Read and validate a checkpoint written by this trusted example."""
    with path.open("rb") as stream:
        checkpoint = pickle.load(stream)
    if (
        not isinstance(checkpoint, Checkpoint)
        or len(checkpoint.rng_states) != GENERATORS
    ):
        raise ValueError("incomplete checkpoint")
    for state in checkpoint.rng_states:
        random.Random().setstate(state)
    return checkpoint


def process_identity(pid: int) -> ProcessIdentity | None:
    """Return a live Linux identity; zombies have already exited."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except (FileNotFoundError, ProcessLookupError):
        return None
    if fields[0] == "Z":
        return None
    return ProcessIdentity(pid, int(fields[19]))


def is_alive(identity: ProcessIdentity) -> bool:
    """Check that the same process is still executing, without signaling it."""
    return process_identity(identity.pid) == identity


def run_processes(run_id: str) -> list[ProcessIdentity]:
    """Find only live processes that inherited this run's unguessable marker."""
    found = []
    marker = f"FTC_RUN_ID={run_id}".encode()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if marker not in (entry / "environ").read_bytes().split(b"\0"):
                continue
            identity = process_identity(int(entry.name))
        except (OSError, ProcessLookupError):
            continue
        if identity is not None:
            found.append(identity)
    return found


def wait_for_exit(identities: list[ProcessIdentity], timeout: float) -> None:
    """Require natural worker exit; never force success by sending a signal."""
    deadline = time.monotonic() + timeout
    while any(is_alive(identity) for identity in identities):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"workers survived their controller: {identities}")
        time.sleep(0.1)


class Generator(Actor):
    """Stateful rollout sampler whose whole RNG state must be checkpointed."""

    def __init__(self, states: tuple[Any, ...]):
        """Restore the stream for this generator rank, including gauss cache."""
        self.rank = current_rank().rank
        self.rng = random.Random()
        self.rng.setstate(states[self.rank])

    @endpoint
    def sample(self, weight: float) -> tuple[int, float]:
        """Draw one candidate and return it with a deterministic rank."""
        return self.rank, self.rng.gauss(weight, 3.0)

    @endpoint
    def state(self) -> tuple[int, Any, ProcessIdentity]:
        """Return RNG state and OS identity without advancing the stream."""
        identity = process_identity(os.getpid())
        assert identity is not None
        return self.rank, self.rng.getstate(), identity


class Trainer(Actor):
    """Toy model and training progress, restored together from a checkpoint."""

    def __init__(self, checkpoint: Checkpoint):
        """Restore the next step and model weight."""
        self.step = checkpoint.step
        self.weight = checkpoint.weight

    @endpoint
    def train(self, step: int, samples: list[tuple[int, float]]) -> None:
        """Apply exactly the expected step using rank-stable tie breaking."""
        assert step == self.step
        self.weight = min(sorted(samples), key=lambda item: abs(item[1] - TARGET))[1]
        self.step += 1

    @endpoint
    def state(self) -> tuple[int, float, ProcessIdentity]:
        """Return model, progress, and identity without modifying training."""
        identity = process_identity(os.getpid())
        assert identity is not None
        return self.step, self.weight, identity


class Controller(Actor):
    """Disposable application controller that owns the entire worker subtree."""

    def __init__(self, host: HostMesh, directory: str, generation: int):
        """Borrow the surviving host; spawning later gives this actor ownership."""
        self.host = host
        self.directory = Path(directory)
        self.generation = generation
        self.procs: list[ProcMesh] = []

    @endpoint
    def run(self, options: Options) -> Checkpoint:
        """Restore fresh workers, train, and optionally hard-kill this process."""
        path = self.directory / "checkpoint.pkl"
        checkpoint = read_checkpoint(path)
        trainer_procs = self.host.spawn_procs(name=f"trainer_{self.generation}")
        self.procs.append(trainer_procs)
        trainer = trainer_procs.spawn("trainer", Trainer, checkpoint)
        generator_procs = self.host.spawn_procs(
            per_host={"replica": GENERATORS}, name=f"generators_{self.generation}"
        )
        self.procs.append(generator_procs)
        generators = generator_procs.spawn(
            "generator", Generator, checkpoint.rng_states
        )
        trainer.initialized.get(timeout=CALL_TIMEOUT)
        generators.initialized.get(timeout=CALL_TIMEOUT)
        states = sorted(generators.state.call().get(timeout=CALL_TIMEOUT).values())
        step, weight, trainer_identity = trainer.state.call_one().get(
            timeout=CALL_TIMEOUT
        )
        identities = [trainer_identity] + [state[2] for state in states]
        atomic_write(self.directory / f"workers_{self.generation}.pkl", identities)
        restored = Checkpoint(step, weight, tuple(state[1] for state in states))
        assert restored == checkpoint, "workers did not restore the complete checkpoint"
        print(
            f"[controller {self.generation}] restored step {step}, weight {weight}, "
            f"fresh workers {[identity.pid for identity in identities]}",
            flush=True,
        )
        if self.generation <= options.restarts:
            time.sleep(KEEPALIVE_WARMUP)
        crash_step = step + 2
        if options.random_kills:
            crash_step = step + random.Random(options.seed + self.generation).randint(
                1, 3
            )
        # Leave enough steps for every requested kill and a final successful run.
        assert self.generation > options.restarts or crash_step < options.steps
        for step in range(checkpoint.step, options.steps):
            samples = list(
                generators.sample.call(weight).get(timeout=CALL_TIMEOUT).values()
            )
            if self.generation <= options.restarts and step == crash_step:
                print(
                    f"[controller {self.generation}] SIGKILL after uncommitted sampling at step {step}",
                    flush=True,
                )
                os.kill(os.getpid(), signal.SIGKILL)
            if options.inject == "hang" and self.generation > options.restarts:
                time.sleep(3600)
            trainer.train.call_one(step, samples).get(timeout=CALL_TIMEOUT)
            next_step, weight, _ = trainer.state.call_one().get(timeout=CALL_TIMEOUT)
            states = sorted(generators.state.call().get(timeout=CALL_TIMEOUT).values())
            checkpoint = Checkpoint(
                next_step, weight, tuple(state[1] for state in states)
            )
            atomic_write(path, checkpoint)
        for procs in reversed(self.procs):
            procs.stop().get(timeout=CALL_TIMEOUT)
        return checkpoint


class Keeper(Actor):
    """Surviving application root that supervises controller actors by identity."""

    def __init__(self, host: HostMesh, directory: str, options: Options):
        """Keep durable state and host lifetime outside each controller subtree."""
        self.host = host
        self.directory = directory
        self.options = options
        self.controllers: list[Controller] = []
        self.expected_failures: set[str] = set()
        self.failures: set[str] = set()

    @endpoint
    def join(self, proc: ProcMesh, generation: int) -> Controller:
        """Create an owned Controller on the independently started process."""
        assert generation == len(self.controllers) + 1
        controller = proc.spawn(
            f"controller_{generation}",
            Controller,
            self.host,
            self.directory,
            generation,
        )
        self.controllers.append(controller)
        if generation <= self.options.restarts or self.options.inject == "hang":
            self.expected_failures.add(cast(ActorMesh, controller)._name.get())
        controller.initialized.get(timeout=CALL_TIMEOUT)
        print(
            f"[keeper] joined generation {generation}; spawned supervised Controller",
            flush=True,
        )
        return controller

    def __supervise__(self, failure: MeshFailure) -> bool:
        """Handle only the expected controller mesh, never unrelated failures."""
        name = failure.mesh_name
        if name not in self.expected_failures:
            return False
        print(f"[keeper] supervised controller failure: {name}", flush=True)
        self.failures.add(name)
        return True

    @endpoint
    def failure_count(self) -> int:
        """Report distinct expected controller failures observed by this actor."""
        return len(self.failures)

    @endpoint
    def release(self) -> None:
        """Stop the final successful controller before its bootstrap shuts down."""
        self.controllers[-1].stop().get(timeout=CALL_TIMEOUT)


def executable_command() -> list[str]:
    """Relaunch a Python source file or the same packaged executable."""
    if sys.argv[0].endswith(".py"):
        return [sys.executable, str(Path(__file__).resolve())]
    return [str(Path(sys.argv[0]).resolve())]


def controller_main(token: str, generation: int, options: Options) -> int:
    """Join as a bootstrap client, then run the keeper-owned application actor."""
    keeper = connect(token, Keeper, timeout=CALL_TIMEOUT)
    keeper.initialized.get(timeout=CALL_TIMEOUT)
    controller = keeper.join.call_one(this_proc(), generation).get(timeout=CALL_TIMEOUT)
    controller.run.call_one(options).get(timeout=3600)
    keeper.release.call_one().get(timeout=CALL_TIMEOUT)
    shutdown_context().get(timeout=CALL_TIMEOUT)
    return 0


def await_failure(keeper: Keeper, expected: int) -> None:
    """Require the actual keeper supervision event before a replacement starts."""
    deadline = time.monotonic() + CLEANUP_TIMEOUT
    while keeper.failure_count.call_one().get(timeout=CALL_TIMEOUT) < expected:
        if time.monotonic() >= deadline:
            raise TimeoutError("keeper did not receive controller MeshFailure")
        time.sleep(0.1)


def keeper_main(options: Options, controller_timeout: float) -> int:
    """Run generations serially and reject orphan workers or incomplete recovery."""
    run_id = uuid.uuid4().hex
    os.environ["FTC_RUN_ID"] = run_id
    # Workers import the same module name that disk pickle records. Unlike PAR
    # executables, source scripts need their directory on the worker import path.
    module_directory = str(Path(__file__).resolve().parent)
    os.environ["PYTHONPATH"] = os.pathsep.join(
        [module_directory, os.environ.get("PYTHONPATH", "")]
    )
    # Environment values override configure(); insist on the timers we document.
    configure(**RUNTIME_CONFIG)
    for key, value in RUNTIME_CONFIG.items():
        if get_global_config()[key] != value:
            raise ValueError(
                f"unset the environment override for {key}; expected {value}"
            )
    status = 1
    process: subprocess.Popen | None = None
    print(f"[keeper] pid {os.getpid()}, run {run_id}", flush=True)
    with tempfile.TemporaryDirectory(prefix="monarch-checkpoint-") as directory:
        path = Path(directory) / "checkpoint.pkl"
        atomic_write(path, initial_checkpoint())
        previous: set[ProcessIdentity] = set()
        try:
            keeper = this_proc().spawn(
                "keeper", Keeper, this_host(), directory, options
            )
            keeper.initialized.get(timeout=CALL_TIMEOUT)
            publication = publish(keeper)
            for generation in range(1, options.restarts + 2):
                before = read_checkpoint(path)
                command = executable_command() + [
                    "controller",
                    "--token",
                    publication.token,
                    "--generation",
                    str(generation),
                    "--steps",
                    str(options.steps),
                    "--restarts",
                    str(options.restarts),
                    "--seed",
                    str(options.seed),
                    "--inject",
                    options.inject,
                ]
                if options.random_kills:
                    command.append("--random-kills")
                process = subprocess.Popen(command)
                try:
                    code = process.wait(timeout=controller_timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=CALL_TIMEOUT)
                    raise TimeoutError(
                        f"controller {generation} exceeded its deadline"
                    ) from None
                print(f"[keeper] generation {generation} exited {code}", flush=True)
                peer = publication.next_peer().get(timeout=CALL_TIMEOUT)
                print(
                    f"[keeper] token peer {peer} (bootstrap root, not adopted)",
                    flush=True,
                )
                with (Path(directory) / f"workers_{generation}.pkl").open(
                    "rb"
                ) as stream:
                    identities = pickle.load(stream)
                assert len(identities) == GENERATORS + 1
                assert not previous.intersection(identities), (
                    "replacement reused an old process"
                )
                previous.update(identities)
                wait_for_exit(identities, CLEANUP_TIMEOUT)
                print(
                    f"[keeper] all generation {generation} workers exited naturally",
                    flush=True,
                )
                assert not run_processes(run_id), "unrecorded worker processes remain"
                if generation <= options.restarts:
                    assert code == -signal.SIGKILL, f"expected SIGKILL, got {code}"
                    await_failure(keeper, generation)
                    recovered = read_checkpoint(path)
                    assert recovered.step > before.step, (
                        "controller made no committed progress"
                    )
                    assert recovered == reference_checkpoint(recovered.step), (
                        "checkpoint diverged"
                    )
                    if options.inject == "checkpoint":
                        atomic_write(
                            path,
                            Checkpoint(
                                recovered.step,
                                recovered.weight,
                                initial_checkpoint().rng_states,
                            ),
                        )
                else:
                    assert code == 0, f"final controller failed: {code}"
                    assert read_checkpoint(path) == reference_checkpoint(
                        options.steps
                    ), "final state differs from uninterrupted reference"
            status = 0
        except Exception as error:  # noqa: BLE001 - every failed check needs cleanup
            print(f"[keeper] FAIL: {error}", flush=True)
            traceback.print_exc()
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=CALL_TIMEOUT)
            try:
                shutdown_context().get(timeout=CALL_TIMEOUT)
            except Exception as error:  # noqa: BLE001 - still reap after shutdown errors
                print(f"[keeper] shutdown failed: {error}", flush=True)
                traceback.print_exc()
                status = 1
            # Emergency cleanup is for failed runs only; any use rejects success.
            leftovers = run_processes(run_id)
            if leftovers:
                status = 1
                print(
                    f"[keeper] FAIL: emergency cleanup required for {leftovers}",
                    flush=True,
                )
                for identity in leftovers:
                    if is_alive(identity):
                        try:
                            os.kill(identity.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                wait_for_exit(leftovers, CALL_TIMEOUT)
    if status == 0:
        print(
            f"[keeper] PASS: {options.restarts} recoveries; complete state matches uninterrupted reference",
            flush=True,
        )
    return status


def main() -> None:
    """Parse the keeper CLI or its internal independent-bootstrap role."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "role", nargs="?", choices=["keeper", "controller"], default="keeper"
    )
    parser.add_argument("--steps", type=int, default=10)
    parser.add_argument("--restarts", type=int, default=1)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--random-kills", action="store_true")
    parser.add_argument("--controller-timeout", type=float, default=90)
    parser.add_argument(
        "--inject", choices=["none", "checkpoint", "hang"], default="none"
    )
    parser.add_argument("--token", help=argparse.SUPPRESS)
    parser.add_argument("--generation", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (
        args.restarts < 0
        or args.steps <= 3 * args.restarts
        or args.controller_timeout <= 0
    ):
        parser.error(
            "require restarts >= 0, steps > 3 * restarts, and controller-timeout > 0"
        )
    options = Options(
        args.steps, args.restarts, args.seed, args.random_kills, args.inject
    )
    poll_key = "HYPERACTOR_MESH_SUPERVISION_POLL_FREQUENCY"
    if os.environ.setdefault(poll_key, "1s") != "1s":
        parser.error(f"unset {poll_key}; this example requires one-second polls")
    configure(**RUNTIME_CONFIG)
    if args.role == "controller":
        if args.token is None or args.generation is None:
            parser.error("internal controller role requires token and generation")
        sys.exit(controller_main(args.token, args.generation, options))
    sys.exit(keeper_main(options, args.controller_timeout))


if __name__ == "__main__":
    # A stable module name keeps disk-pickled dataclasses identical in the
    # keeper, external bootstrap, and remote actor deserialization contexts.
    importlib.import_module("fault_tolerant_controller").main()
