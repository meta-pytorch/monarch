# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Restart a crashed controller without losing supervision of its workers.

This is an experimental example, not public API guidance. Python token-join
still lives in the private `monarch._src.actor.token` module; this program uses
it to evaluate a concrete restartable-controller design before that API is
ready for promotion. Token strings grant access to the published actor
endpoints and appear in controller command lines, so run this example only with
trusted processes on a trusted devvm.

A controller drives a toy RL loop with one trainer and two generators. The
controller crashes during step 3; a new controller reconnects to the same
workers, reads the trainer's progress and finishes the run.

The process you launch is the keeper. On one host it creates this topology:

- one trainer worker process containing one `Trainer` actor;
- two generator worker processes containing one `Generator` actor each;
- one controller child process at a time.

The keeper's Monarch root owns both worker-process meshes and actor meshes. It
publishes each actor mesh through a keeper-owned rendezvous token. Each
controller joins those tokens to receive references to the existing trainer
and generator actors, then reads the trainer's progress and drives the toy
loop. Each rendezvous also reports the joining controller's root actor address
through `PublishedToken.next_peer`; the keeper logs those notifications and
checks that both joins came from the same root.

Inside the keeper process, token publication creates this actor hierarchy::

    keeper Hyperactor proc (`this_proc()`)
    `-- root client actor
        |-- trainer rendezvous actor
        `-- generator rendezvous actor

The rendezvous actors are children of the root client. The published trainer
and generator actors remain in the separate worker procs shown above.

`main` selects the process role from the command line. The initial invocation
has no `controller` argument and runs `keeper_main`. The keeper launches the
same executable with `controller` as its first argument, so each child runs
`controller_main` instead.

The same binary runs first as the keeper, then in a new child process for each
controller generation::

    initial keeper process
    main
    `-- keeper_main
        |-- spawn and publish trainer and generator meshes
        `-- run_controller_generations
            |-- launch controller generation 1 process
            |   `-- main -> controller_main -> run_controller_generation
            |       `-- run_training_steps -> SIGKILL controller 1
            `-- launch controller generation 2 process
                `-- main -> controller_main -> run_controller_generation
                    |-- run_training_steps
                    `-- run_post_restart_checks

The run has two intentional failures with different purposes:

1. Controller 1 completes steps 0-2, generates step 3's rollouts, then kills
   itself before the trainer applies them. The keeper starts controller 2,
   which reconnects to the same workers and completes steps 3-5.
2. Controller 2 then kills one generator process. Its next generator-mesh call
   must raise `SupervisionError` instead of hanging, and the keeper must receive
   the generator mesh's `MeshFailure`.

The first failure demonstrates controller restart. The second demonstrates
that supervision still works after the restart.

During the normal run, controller 2 checks that:

- controller 2 reached the same trainer and generator process ids;
- the trainer resumed at step 3 and completed step 5;
- after one generator process died, its next mesh call raised
  `SupervisionError`.

The keeper checks that:

- each controller joined both tokens from the same root actor address;
- it received `MeshFailure` for the generator mesh;
- cleanup left no controller or worker process running.

The normal run exits 0 only if every check passes.

The optional Fault-Tolerant Controller injection environment variable exercises
two failure paths. `FTC_INJECT=check` skips the generator kill, so the
supervision check must fail. `FTC_INJECT=hang` makes controller 2 exceed its
deadline, so the keeper must kill it. Both runs must exit non-zero.

The keeper waits for each controller process to exit before launching the next,
so this run has no controller overlap. Token-join admits multiple joiners,
however, and this example does not fence an old controller that remains alive
when a replacement starts.
"""

import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import cast

import monarch.actor

# `SupervisionError` is not exported from `monarch.actor`.
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError

# Public mesh proxies do not expose the identity used by
# `MeshFailure.mesh_name`.
from monarch._src.actor.actor_mesh import ActorMesh

# Token-join does not yet have a public Python module.
from monarch._src.actor.token import connect, publish, PublishedToken

# Public Monarch APIs.
from monarch.actor import (
    Actor,
    current_rank,
    endpoint,
    MeshFailure,
    ProcMesh,
    shutdown_context,
    this_host,
)
from monarch.config import configure, get_global_config

# Give up on messages to an unreachable process after 5 seconds instead of 30.
MESSAGE_DELIVERY_TIMEOUT_SECONDS = 5
EXPECTED_MESSAGE_DELIVERY_TIMEOUT = f"{MESSAGE_DELIVERY_TIMEOUT_SECONDS}s"
configure(message_delivery_timeout=EXPECTED_MESSAGE_DELIVERY_TIMEOUT)

# Value the toy trainer tries to approach.
TARGET = 10.0
# Number of training steps the two controllers must complete together.
STEPS = 6
# Controller 1 exits after generating rollouts at this step, before
# `Trainer.train`.
CRASH_AT_STEP = 3
# Maximum wait for an ordinary token join or actor call.
CALL_TIMEOUT = 30.0
# This prototype has one original controller and one replacement.
CONTROLLER_GENERATIONS = 2
# Maximum time to wait for `SupervisionError` after killing a generator process.
SUPERVISION_TIMEOUT = 60.0
# Maximum wait for each explicit Monarch stop or shutdown.
SHUTDOWN_DEADLINE = 30.0
# Maximum time the keeper waits for its fault hook to receive the generator's
# `MeshFailure`.
HOOK_DEADLINE = 30.0
# Maximum time the keeper allows a controller process to run before killing it.
CONTROLLER_DEADLINE = 120.0
# Maximum time the keeper waits for a worker to exit after sending `SIGKILL`.
REAP_DEADLINE = 10.0
# Wait through two delivery timeouts before starting the generator-failure test.
QUIET_PERIOD = 2 * MESSAGE_DELIVERY_TIMEOUT_SECONDS + 1

# The RL loop
#
# The toy model is one `weight`. On each step, both generators sample a
# candidate near it, and the trainer keeps the candidate closest to `TARGET`.
# The trainer stores the weight and the next step to apply, so a restarted
# controller can read `progress()` and resume there. Repeating an applied step
# is a no-op. Each rollout includes its generator's pid. Controllers compare
# those pids with the keeper's original list to verify that restart reconnects
# to the existing generator processes instead of replacements.


@dataclass
class Rollout:
    """One generator's candidate weight and process identity."""

    # Generator rank within the actor mesh.
    rank: int
    # OS process serving that generator.
    pid: int
    # Candidate weight drawn around the trainer's current weight.
    sample: float


@dataclass
class Progress:
    """A trainer snapshot used by a restarted controller to resume training."""

    # Next training step the trainer will apply.
    step: int
    # Current toy-model weight.
    weight: float
    # OS process serving the trainer, compared across controllers to verify
    # reconnection.
    pid: int


class Generator(Actor):
    """A worker actor that samples candidate weights for the toy trainer."""

    def __init__(self):
        """Use the mesh rank as both identity and deterministic random seed."""
        self.rank = current_rank().rank
        self.rng = random.Random(self.rank)

    @endpoint
    def generate(self, weight: float) -> Rollout:
        """Return this worker's identity and a normally distributed candidate.

        The distribution is centered on `weight` with standard deviation 3.
        """
        return Rollout(self.rank, os.getpid(), self.rng.gauss(weight, 3.0))


class Trainer(Actor):
    """A worker actor that owns the toy model's weight and next step."""

    def __init__(self):
        """Start with weight zero and step zero as the next step to apply."""
        self.step = 0
        self.weight = 0.0

    @endpoint
    def progress(self) -> Progress:
        """Return this trainer's resumable state and process identity."""
        return Progress(self.step, self.weight, os.getpid())

    @endpoint
    def train(self, step: int, rollouts: list[Rollout]) -> float:
        """Apply one training step when `step` matches the trainer's next step.

        The trainer adopts the rollout sample closest to `TARGET` and advances
        its step. A mismatched step leaves the state unchanged. Return the
        current weight in either case.
        """
        if step == self.step:
            best = min(rollouts, key=lambda r: abs(r.sample - TARGET))
            self.weight = best.sample
            self.step += 1
        return self.weight


# The controller
#
# Controllers are disposable clients. The keeper gives each one tokens for the
# existing trainer and generator meshes, plus their expected process ids.
#
# Controller 1 completes steps 0-2, generates rollouts for step 3, then exits
# before the trainer applies them. Controller 2 reconnects, reads step 3 from
# the trainer, verifies that the worker processes are unchanged and completes
# steps 3-5.
#
# Finally, controller 2 waits for delayed faults from the restart, kills one
# generator process and requires the next generator call to raise
# `SupervisionError` rather than hang.


def log(generation: int, message: str) -> None:
    """Print a controller-attributed message immediately."""
    print(f"[controller {generation}] {message}", flush=True)


def check_failed(generation: int, what: str) -> int:
    """Log a failed controller check and return exit code 1."""
    log(generation, f"check failed: {what}")
    return 1


def kill_generator(generator_pid: int, failure_marker_path: str) -> None:
    """Mark the injected failure, then kill one generator process.

    Args:
        generator_pid: OS process ID reported by the generator selected to fail.
        failure_marker_path: Path created immediately before `SIGKILL`. The
            keeper's fault hook treats any failure observed before it exists as
            unexpected.
    """
    open(failure_marker_path, "w").close()
    os.kill(generator_pid, signal.SIGKILL)


def check_generator_supervision(generators: Generator, weight: float) -> bool:
    """Check that a killed generator is reported through mesh supervision.

    The call must raise `SupervisionError`; a reply, timeout or different
    exception fails the check. If no result arrives within
    `SUPERVISION_TIMEOUT`, `get` raises `TimeoutError`; it does not return a
    timeout value.

    Args:
        generators: Reference to the keeper-owned generator mesh after one
            generator process has been killed.
        weight: Ordinary input required by `Generator.generate`; its value is
            not part of this check.

    Returns:
        Whether the mesh call raised `SupervisionError`.
    """
    try:
        generators.generate.call(weight).get(timeout=SUPERVISION_TIMEOUT)
    except SupervisionError:
        outcome = "SupervisionError"
    except TimeoutError:
        outcome = "TimeoutError"
    except Exception as e:
        outcome = type(e).__name__
    else:
        outcome = "returned"
    print(f"[controller] supervision check: {outcome}", flush=True)
    return outcome == "SupervisionError"


def collect_rollouts(generators: Generator, weight: float) -> list[Rollout]:
    """Call every generator rank and return its replies in rank order.

    `Generator.generate.call` returns a `ValueMesh[Rollout]`; this materializes
    its values and sorts them by each rollout's reported rank.

    Args:
        generators: Reference to the keeper-owned generator mesh.
        weight: Current trainer weight around which each generator samples.

    Returns:
        One rollout per generator rank, ordered by rank.
    """
    return sorted(
        generators.generate.call(weight).get(timeout=CALL_TIMEOUT).values(),
        key=lambda rollout: rollout.rank,
    )


def run_training_steps(
    generation: int,
    trainer: Trainer,
    generators: Generator,
    progress: Progress,
    generator_pids: list[int],
) -> tuple[float, list[Rollout]] | None:
    """Run the remaining training steps against the keeper-owned meshes.

    For each step, call every generator rank, verify that its process ID matches
    the keeper's rank-ordered snapshot, then send the rollouts to the single
    trainer actor. Controller 1 terminates before applying `CRASH_AT_STEP`.

    Args:
        generation: Controller generation number, starting at 1.
        trainer: Connected one-actor `Trainer` mesh.
        generators: Connected `Generator` mesh.
        progress: Trainer snapshot containing the next step and current weight.
        generator_pids: Expected generator process IDs in mesh-rank order.

    Returns:
        The final weight and last rollouts, or `None` when process identity
        differs. Controller 1 normally terminates with `SIGKILL` instead of
        returning.
    """
    weight = progress.weight
    rollouts: list[Rollout] = []
    for step in range(progress.step, STEPS):
        rollouts = collect_rollouts(generators, weight)
        pids = [rollout.pid for rollout in rollouts]
        # Prove this controller reached the keeper's original generators.
        if pids != generator_pids:
            check_failed(generation, f"generator pids {pids}, not {generator_pids}")
            return None
        if generation == 1 and step == CRASH_AT_STEP:
            log(generation, f"crashing during step {step}")
            # Kill only this controller child. The keeper receives a negative
            # exit code and starts the next controller generation. `SIGKILL`
            # never returns, so this generation does not call `Trainer.train`.
            os.kill(os.getpid(), signal.SIGKILL)
        # Send this step's rollouts to the single trainer actor and wait for
        # its updated weight reply.
        weight = trainer.train.call_one(step, rollouts).get(timeout=CALL_TIMEOUT)
        # Keep the generator process identities beside each completed step in
        # the run log, so the two controller generations can be compared.
        shown = ", ".join(str(pid) for pid in pids)
        log(
            generation,
            f"step {step}: weight={weight:.2f} from generators (pids {shown})",
        )
    # The restarted controller uses the last rollout to choose a generator
    # process to kill, then uses the final weight for the call that must fail.
    return weight, rollouts


def run_post_restart_checks(
    generation: int,
    trainer: Trainer,
    generators: Generator,
    weight: float,
    rollouts: list[Rollout],
    failure_marker_path: str,
) -> int:
    """Run the restarted controller's final progress and supervision checks.

    `run_controller_generation` calls this after the restarted controller has
    reconnected and `run_training_steps` has completed steps 3-5. The function
    confirms the trainer's final step, waits for delayed faults from the
    controller restart, marks the failure-injection boundary, kills one
    generator process and verifies that the next generator-mesh call raises
    `SupervisionError`.

    Args:
        generation: Controller generation number used in log messages.
        trainer: Reference returned by `connect` to the keeper-owned one-actor
            `Trainer` mesh.
        generators: Reference returned by `connect` to the keeper-owned
            `Generator` mesh.
        weight: Final model weight passed to `Generator.generate`; its value is
            not under test. The call occurs after one generator process is
            killed and must raise `SupervisionError`.
        rollouts: Last step's rollouts returned by `run_training_steps`; rank
            1's pid selects the generator process to kill.
        failure_marker_path: Keeper-created path received as a controller
            command-line argument. `kill_generator` creates the file
            immediately before `SIGKILL` so the keeper can identify the
            intentionally injected failure.

    Returns:
        Zero when the final step and supervision checks pass, or one when a
        check fails. Under `FTC_INJECT=hang`, the keeper kills this controller
        before the function returns.
    """
    # Finish ordinary training before injecting a worker failure.
    final_step = trainer.progress.call_one().get(timeout=CALL_TIMEOUT).step
    if final_step != STEPS:
        return check_failed(
            generation, f"trainer ended at step {final_step}, not {STEPS}"
        )

    # The controller inherits the keeper's validated fault-injection mode.
    injection = os.environ.get("FTC_INJECT", "")
    if injection == "hang":
        # Exercise the keeper's controller deadline. The keeper sends `SIGKILL`
        # before this sleep finishes, so no later line runs in this process.
        log(generation, "FTC_INJECT=hang: sleeping past the controller deadline")
        time.sleep(CONTROLLER_DEADLINE + 60)

    if injection != "check":
        # In the normal run, leave enough time to classify delayed restart
        # faults as early, then mark and kill generator rank 1.
        log(
            generation,
            f"waiting {QUIET_PERIOD:.0f}s before killing a generator",
        )
        time.sleep(QUIET_PERIOD)
        generator_pid_to_kill = rollouts[1].pid
        kill_generator(generator_pid_to_kill, failure_marker_path)

    # Normal mode must observe `SupervisionError`. `FTC_INJECT=check` leaves
    # both generators alive, so the call returns and deliberately fails here.
    if not check_generator_supervision(generators, weight):
        return 1
    return 0


def run_controller_generation(
    generation: int,
    trainer: Trainer,
    generators: Generator,
    failure_marker_path: str,
    trainer_pid: int,
    generator_pids: list[int],
) -> int:
    """Drive one controller generation against the keeper-owned actor meshes.

    Args:
        generation: Controller generation number, starting at 1. The first
            crashes at `CRASH_AT_STEP`; a restarted controller verifies
            recovery.
        trainer: Connected one-actor `Trainer` mesh published by the keeper.
        generators: Connected `Generator` mesh published by the keeper.
        failure_marker_path: File marking the start of the intentional
            generator-failure check.
        trainer_pid: Trainer process ID recorded by the keeper before any
            controller started.
        generator_pids: Generator process IDs recorded by the keeper in mesh
            rank order.

    Returns:
        Zero when this generation completes its expected checks, or one when a
        check fails. The first generation normally terminates itself with
        `SIGKILL` instead of returning.
    """
    progress = trainer.progress.call_one().get(timeout=CALL_TIMEOUT)
    log(
        generation,
        f"connected to trainer (pid {progress.pid}) at step {progress.step}",
    )
    # Every controller generation must reach the trainer process the keeper
    # spawned before any controller existed.
    if progress.pid != trainer_pid:
        return check_failed(
            generation, f"trainer pid {progress.pid}, not {trainer_pid}"
        )

    is_restarted_controller = generation > 1
    # A restarted controller must see the step that generation 1 generated but
    # never sent to `Trainer.train` before its `SIGKILL`.
    if is_restarted_controller and progress.step != CRASH_AT_STEP:
        return check_failed(generation, f"resumed at step {progress.step}")

    # Resume from the trainer's saved progress. Every rollout must still come
    # from the generator processes recorded by the keeper.
    training_result = run_training_steps(
        generation, trainer, generators, progress, generator_pids
    )
    # `None` means a rollout reported different generator pids; the helper
    # already logged the actual and expected lists.
    if training_result is None:
        return 1
    weight, rollouts = training_result

    # Generation 1 normally dies inside `run_training_steps`; reaching this
    # point means the deliberate crash did not occur.
    if not is_restarted_controller:
        return check_failed(generation, "completed instead of crashing")

    # Only a restarted controller performs the final supervision experiment.
    return run_post_restart_checks(
        generation, trainer, generators, weight, rollouts, failure_marker_path
    )


def controller_main(
    trainer_token: str,
    generators_token: str,
    generation: int,
    failure_marker_path: str,
    trainer_pid: int,
    generator_pids: list[int],
) -> int:
    """Connect to the keeper's meshes and run one controller generation.

    The keeper starts this OS process with both tokens, the generation number,
    its failure-marker path and the worker process ids recorded before any
    controller started. This process joins both tokens, drives training, runs
    the checks assigned to its generation and shuts down only its local Monarch
    context; it never owns or stops the worker meshes.

    Args:
        trainer_token: Opaque token string published by the keeper for the
            trainer mesh.
        generators_token: Opaque token string published by the keeper for the
            generator mesh.
        generation: Controller generation number, starting at 1.
        failure_marker_path: Keeper-created path marking the intentional
            generator failure.
        trainer_pid: Trainer process id recorded by the keeper.
        generator_pids: Generator process ids recorded by the keeper in rank
            order.

    Returns:
        The generation's process exit code.
    """
    log(generation, f"pid {os.getpid()}")
    trainer = connect(trainer_token, Trainer, timeout=CALL_TIMEOUT)
    generators = connect(generators_token, Generator, timeout=CALL_TIMEOUT)
    status = run_controller_generation(
        generation,
        trainer,
        generators,
        failure_marker_path,
        trainer_pid,
        generator_pids,
    )
    start = time.monotonic()
    try:
        shutdown_context().get(timeout=SHUTDOWN_DEADLINE)
    except Exception as e:
        log(generation, f"shutdown failed: {type(e).__name__}: {e}")
        return 1
    log(generation, f"shutdown took {time.monotonic() - start:.1f}s")
    return status


# The keeper
#
# The keeper spawns the trainer and the generators, so it owns them, and
# publishes both meshes. It runs the controller as a child process and
# restarts it only after a crash (a signal); a controller that exits with an
# error has failed a check, and restarting it would only repeat the check.
#
# As the owner, the keeper is told when a worker fails. It records each fault
# and whether the marker file existed when it arrived; controller 2 creates
# the marker just before it kills a generator. The run passes only if the
# generator mesh's failure arrived after the marker and no fault arrived before
# it: neither controller 1's death nor controller 2's reconnection caused one.


class KeeperFaultLog:
    """Thread-safe record of mesh failures delivered to the keeper's root hook.

    The marker separates faults observed before controller 2 intentionally
    kills a generator from faults observed afterward. The expected event must
    name the generator actor mesh and arrive after that boundary.
    """

    def __init__(self, failure_marker_path: str) -> None:
        """Create an empty fault record tied to one failure-marker path."""
        # Controller 2 creates this file immediately before killing a generator.
        self.failure_marker_path = failure_marker_path
        # The root hook can run outside the keeper's main thread.
        self.lock = threading.Lock()
        # Each record is (mesh name, marker existed when the fault arrived).
        self.records: list[tuple[str, bool]] = []
        # Set after the generator actor mesh has been spawned and named.
        self.expected_name: str | None = None
        # Wakes the keeper when the expected post-marker failure arrives.
        self.expected_seen = threading.Event()

    def expect_mesh_failure(self, mesh_name: str) -> None:
        """Set the mesh name expected after the intentional worker kill."""
        self.expected_name = mesh_name

    def record(self, failure: MeshFailure) -> None:
        """Record a root-hook failure and signal when it is expected."""
        after_marker = os.path.exists(self.failure_marker_path)
        print(
            f"[keeper] fault on {failure.mesh_name} (marker={after_marker}): {failure}",
            flush=True,
        )
        with self.lock:
            self.records.append((failure.mesh_name, after_marker))
            if after_marker and failure.mesh_name == self.expected_name:
                self.expected_seen.set()

    def early_faults(self) -> list[str]:
        """Return mesh names for faults observed before the intentional kill."""
        with self.lock:
            return [name for name, after in self.records if not after]


def fail_keeper_check(reason: str) -> int:
    """Log a failed keeper check and return exit code 1."""
    print(f"[keeper] result: FAIL ({reason})", flush=True)
    return 1


def evaluate_keeper_faults(keeper_faults: KeeperFaultLog) -> int:
    """Wait for the expected worker failure and reject any earlier fault.

    Returns:
        Zero only when the expected generator-mesh failure arrived after the
        marker and no fault arrived before it; otherwise one.
    """
    seen = keeper_faults.expected_seen.wait(HOOK_DEADLINE)
    early = keeper_faults.early_faults()
    if not seen:
        return fail_keeper_check("no fault on the generator mesh after the marker")
    if early:
        return fail_keeper_check(f"faults before the marker: {early}")
    print("[keeper] result: PASS", flush=True)
    return 0


def consume_join_notifications(
    generation: int,
    publications: tuple[PublishedToken, PublishedToken],
) -> bool:
    """Log and compare the keeper-side notifications for one controller join.

    Each controller joins the trainer token and generator token from its root
    client actor. `PublishedToken.next_peer` returns that actor's address. Both
    publications must therefore report the same address for this generation.

    Returns:
        Whether both notifications arrived and named the same controller root.
    """
    try:
        trainer_peer = publications[0].next_peer().get(timeout=CALL_TIMEOUT)
        generator_peer = publications[1].next_peer().get(timeout=CALL_TIMEOUT)
    except Exception as error:
        print(
            f"[keeper] controller {generation} join notification failed: "
            f"{type(error).__name__}: {error}",
            flush=True,
        )
        return False

    print(
        f"[keeper] controller {generation} joined trainer as {trainer_peer}",
        flush=True,
    )
    print(
        f"[keeper] controller {generation} joined generators as {generator_peer}",
        flush=True,
    )
    return trainer_peer == generator_peer


# Cleanup
#
# The keeper tears its workers down through Monarch, and as a last resort kills
# any worker still running. An unrelated process can reuse a worker's pid once
# the worker exits, so the keeper records each worker's start time and a run
# ID in its environment, and signals a pid only while both still match.


@dataclass(frozen=True)
class WorkerIdentity:
    """Stable OS-process identity used for safe fallback cleanup."""

    # Process ID reported by the worker when this identity was captured.
    pid: int
    # Linux start time from field 22 of `/proc/<pid>/stat`; unlike a pid, this
    # changes when the operating system reuses a process number.
    start_time: int


def read_process_start_time(pid: int) -> int | None:
    """Read a Linux process's start time from field 22 of `/proc/<pid>/stat`.

    Returns:
        The kernel-clock tick at which the process started, or `None` when the
        process no longer exists or its stat file cannot be read.
    """
    try:
        with open(f"/proc/{pid}/stat") as f:
            stat = f.read()
    except OSError:
        return None
    # Field 22; the fields after the parenthesized command name start at 3.
    return int(stat.rsplit(")", 1)[1].split()[19])


def worker_identity_matches(identity: WorkerIdentity, run_id: str) -> bool:
    """Return whether a pid still denotes the recorded worker process.

    Both its Linux start time and inherited `FTC_RUN_ID` must match. This avoids
    treating an unrelated process that reused the same pid as a worker.
    """
    if read_process_start_time(identity.pid) != identity.start_time:
        return False
    try:
        with open(f"/proc/{identity.pid}/environ", "rb") as f:
            environ = f.read().split(b"\0")
    except OSError:
        return False
    return f"FTC_RUN_ID={run_id}".encode() in environ


def capture_worker_identity(pid: int, run_id: str) -> WorkerIdentity:
    """Capture and log one live worker's pid and Linux start time.

    The worker must have inherited `FTC_RUN_ID`. A missing value aborts setup
    because fallback cleanup could not safely distinguish that worker from a
    later process reusing its pid.
    """
    started = read_process_start_time(pid)
    if started is None:
        raise RuntimeError(f"worker {pid} exited before it was recorded")
    identity = WorkerIdentity(pid, started)
    print(f"[keeper] worker pid {pid} start {started}", flush=True)
    if not worker_identity_matches(identity, run_id):
        raise RuntimeError(f"worker {pid} did not inherit FTC_RUN_ID")
    return identity


def reap_workers(identities: list[WorkerIdentity], run_id: str) -> list[WorkerIdentity]:
    """Kill verified leftover workers and return those still alive at deadline.

    A process is signaled only while `worker_identity_matches` confirms both
    its start time and run id. Each signaled worker gets up to `REAP_DEADLINE`
    to vanish.
    """
    left = []
    for identity in identities:
        if not worker_identity_matches(identity, run_id):
            continue
        try:
            os.kill(identity.pid, signal.SIGKILL)
        except ProcessLookupError:
            continue
        deadline = time.monotonic() + REAP_DEADLINE
        while worker_identity_matches(identity, run_id) and time.monotonic() < deadline:
            time.sleep(0.1)
        if worker_identity_matches(identity, run_id):
            left.append(identity)
    return left


def cleanup_keeper_resources(
    proc_meshes: list[ProcMesh], identities: list[WorkerIdentity], run_id: str
) -> list[WorkerIdentity]:
    """Stop keeper-owned meshes, shut down Monarch and reap verified leftovers.

    Returns:
        Worker identities that still match live processes after all cleanup.
    """
    for procs in proc_meshes:
        try:
            procs.stop().get(timeout=SHUTDOWN_DEADLINE)
        except Exception as e:
            print(
                f"[keeper] stopping procs failed: {type(e).__name__}: {e}", flush=True
            )
    try:
        shutdown_context().get(timeout=SHUTDOWN_DEADLINE)
    except Exception as e:
        print(f"[keeper] shutdown failed: {type(e).__name__}: {e}", flush=True)
    return reap_workers(identities, run_id)


def run_controller_generations(
    publications: tuple[PublishedToken, PublishedToken],
    failure_marker_path: str,
    trainer_pid: int,
    generator_pids: list[int],
    keeper_faults: KeeperFaultLog,
) -> int:
    """Launch controller subprocesses until one succeeds or fails a check.

    Every generation runs this same executable with the `controller` role. A
    negative return code means a signal killed that controller and starts the
    next generation. Zero completes the run; a positive code or timeout fails
    it without another restart.

    Args:
        publications: Trainer and generator publications, in that order. Their
            token strings go to controllers; `next_peer` reports which
            controller root joined each token.
        failure_marker_path: Keeper-created path passed to every controller.
        trainer_pid: Trainer process id recorded before controllers start.
        generator_pids: Generator process ids recorded in mesh-rank order.
        keeper_faults: Keeper-side record used to validate the expected worker
            failure.

    Returns:
        Zero after a successful controller generation and keeper fault checks,
        or one when a controller times out, fails a check or both generations
        crash.
    """
    for generation in range(1, CONTROLLER_GENERATIONS + 1):
        # Relaunch this binary; `main` parses these fields in this exact order.
        command = [
            sys.argv[0],  # This same Buck executable.
            "controller",  # Role selecting `controller_main`.
            publications[0].token,  # Trainer-mesh rendezvous token.
            publications[1].token,  # Generator-mesh rendezvous token.
            str(generation),  # Controller generation number.
            failure_marker_path,  # Created before the intentional worker kill.
            str(trainer_pid),  # Keeper-recorded trainer process id.
            ",".join(str(pid) for pid in generator_pids),  # Rank-ordered pids.
        ]
        code: int | None
        try:
            # The keeper's main thread waits here, while Monarch's runtime keeps
            # serving rendezvous joins and owned-mesh supervision in background.
            code = subprocess.run(command, timeout=CONTROLLER_DEADLINE).returncode
        except subprocess.TimeoutExpired:
            # `subprocess.run` has killed and reaped the overdue controller.
            print(f"[keeper] controller {generation} timed out; killed", flush=True)
            code = None
        if code is not None:
            # Attribute an early controller failure before waiting for join
            # notifications that may never arrive.
            print(f"[keeper] controller {generation} exited with {code}", flush=True)
        # Both joins queued keeper-side notifications while the controller ran.
        # Consume them now and prove they came from one controller root actor.
        if not consume_join_notifications(generation, publications):
            return fail_keeper_check(
                f"controller {generation} did not join both meshes from one root"
            )
        if code is None:
            return fail_keeper_check(f"controller {generation} timed out")
        if code == 0:
            # A successful controller completed its checks; now validate the
            # worker failure observed independently by the keeper.
            return evaluate_keeper_faults(keeper_faults)
        if code > 0:
            # The controller reported a failed check or local shutdown failure.
            return fail_keeper_check(f"controller {generation} failed a check")
        # A negative return code means a signal killed the controller. Continue
        # the loop to start the next generation with the same tokens.
    return fail_keeper_check(
        f"no controller completed after {CONTROLLER_GENERATIONS} generations"
    )


def keeper_main() -> int:
    """Run the long-lived process that owns workers and restarts controllers.

    The keeper spawns the trainer and generator process meshes, records their
    process identities, publishes their actor meshes and launches controller
    generations with the resulting tokens. Its fault hook verifies that worker
    supervision remains active while controllers come and go. Cleanup always
    stops the owned meshes and reaps any worker that Monarch did not stop.

    Returns:
        Zero only when controller restart, supervision and cleanup checks all
        pass; otherwise a non-zero process exit code.
    """
    # Workers inherit this unique run id. Last-resort cleanup checks it and the
    # process start time before killing a recorded pid, so a reused pid cannot
    # identify an unrelated process as one of this run's workers.
    run_id = uuid.uuid4().hex
    os.environ["FTC_RUN_ID"] = run_id
    print(f"[keeper] pid {os.getpid()}", flush=True)

    # Environment configuration outranks `configure`. Refuse to derive the
    # quiet period from five seconds unless five seconds is effective globally.
    effective_delivery_timeout = get_global_config()["message_delivery_timeout"]
    if effective_delivery_timeout != EXPECTED_MESSAGE_DELIVERY_TIMEOUT:
        print(
            "[keeper] effective message_delivery_timeout is "
            f"{effective_delivery_timeout!r}, expected "
            f"{EXPECTED_MESSAGE_DELIVERY_TIMEOUT!r}; unset or correct "
            "HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT",
            flush=True,
        )
        return 2

    # Select a negative-test scenario inherited by controller children. Empty
    # runs normally; `check` omits the generator kill, and `hang` makes
    # controller 2 exceed its deadline.
    injection = os.environ.get("FTC_INJECT", "")
    if injection not in ("", "check", "hang"):
        print(
            f"[keeper] unknown FTC_INJECT={injection!r}; use check or hang",
            flush=True,
        )
        return 2

    # The marker divides faults before and after the intentional generator kill.
    state_dir = tempfile.mkdtemp()
    failure_marker_path = os.path.join(state_dir, "marker")
    keeper_faults = KeeperFaultLog(failure_marker_path)
    # Owned-mesh failures reach the keeper's root hook. Replace the default,
    # which exits, with a recorder that keeps the keeper alive to verify them.
    monarch.actor.unhandled_fault_hook = keeper_faults.record

    # Recorded as soon as each is acquired, so cleanup touches only what exists.
    proc_meshes: list[ProcMesh] = []
    identities: list[WorkerIdentity] = []
    status = 1
    try:
        # `this_host()` has one host rank. Omitting `per_host` therefore spawns
        # exactly one keeper-owned trainer process on this host.
        trainer_procs = this_host().spawn_procs(name="trainer")
        proc_meshes.insert(0, trainer_procs)
        trainer = trainer_procs.spawn("trainer", Trainer)
        trainer.initialized.get()
        trainer_pid = trainer.progress.call_one().get(timeout=CALL_TIMEOUT).pid
        identities.append(capture_worker_identity(trainer_pid, run_id))

        # Add a two-rank `replica` dimension, producing two keeper-owned
        # generator processes on the same host.
        generator_procs = this_host().spawn_procs(
            per_host={"replica": 2}, name="generators"
        )
        proc_meshes.insert(0, generator_procs)
        generators = generator_procs.spawn("generator", Generator)
        generator_mesh_name = cast(ActorMesh, generators)._name.get()
        keeper_faults.expect_mesh_failure(  # Match later mesh failure.
            generator_mesh_name
        )
        generators.initialized.get()
        rollouts = generators.generate.call(0.0).get(timeout=CALL_TIMEOUT).values()
        generator_pids = [r.pid for r in sorted(rollouts, key=lambda r: r.rank)]
        for pid in generator_pids:
            identities.append(capture_worker_identity(pid, run_id))

        # Each call spawns a rendezvous actor under the keeper's root client and
        # returns a `PublishedToken` to the keeper.
        trainer_publication = publish(trainer)
        generator_publication = publish(generators)
        publications = (trainer_publication, generator_publication)
        status = run_controller_generations(
            publications,
            failure_marker_path,
            trainer_pid,
            generator_pids,
            keeper_faults,
        )
    except KeyboardInterrupt:
        print("[keeper] interrupted; cleaning up", flush=True)
        status = 130
    except Exception as e:
        traceback.print_exc()
        status = fail_keeper_check(f"keeper setup failed: {type(e).__name__}: {e}")
    finally:
        # Stop every resource acquired before success or partial setup failure.
        left = cleanup_keeper_resources(proc_meshes, identities, run_id)
        shutil.rmtree(state_dir, ignore_errors=True)
    if left:
        pids = [identity.pid for identity in left]
        print(f"[keeper] result: FAIL (workers left: {pids})", flush=True)
        return 1
    return status


def main() -> None:
    """Select which role drives this invocation's OS process.

    The top-level user invocation has no role argument and runs `keeper_main`.
    The keeper relaunches this same executable with the `controller` role, so
    each new controller child process runs `controller_main` instead.
    """
    args = sys.argv[1:]
    # The user launches the binary without a role; that process is the keeper.
    if not args:
        sys.exit(keeper_main())

    # Only keeper-spawned child invocations use the `controller` role.
    role, *controller_args = args
    if role != "controller":
        raise SystemExit(f"unknown role {role!r}; expected 'controller' or no role")
    if len(controller_args) != 6:
        raise SystemExit(
            "controller role expects trainer token, generator token, generation, "
            "failure marker path, trainer pid and generator pids"
        )

    (
        trainer_token,
        generators_token,
        generation,
        failure_marker_path,
        trainer_pid,
        generator_pids,
    ) = controller_args
    sys.exit(
        controller_main(
            trainer_token,
            generators_token,
            int(generation),
            failure_marker_path,
            int(trainer_pid),
            [int(pid) for pid in generator_pids.split(",")],
        )
    )
