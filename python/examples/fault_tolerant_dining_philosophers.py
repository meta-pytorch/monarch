# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Fault-tolerant Dining Philosophers
===================================

Several philosophers share chopsticks through a waiter actor. A supervisor
restarts a failed actor mesh and sends its replacement to the surviving
actors. Each actor retries a failed remote call after it receives the new
mesh.

Usage::

    buck2 run fbcode//monarch/python/examples:fault_tolerant_dining_philosophers -- \
        --duration 60 \
        --waiter-probability 0.1 \
        --waiter-interval 5 \
        --philosopher-probability 0.2 \
        --philosopher-interval 5

The example repeatedly injects waiter and philosopher failures. Without a
duration, it runs until it is interrupted.

``MeshFailure`` does not currently identify the failed mesh kind, and a process
mesh that observes an actor failure cannot spawn another actor. This example
handles failures from its known actor meshes by replacing their process meshes.
Other failures propagate to the supervisor's owner.

Recovery broadcasts can race with failure notifications. The supervisor ignores
an undeliverable recovery message because the failed owned mesh also reports a
``MeshFailure`` through ``__supervise__``.

Each philosopher persists its meal count in a temporary per-rank file. A
replacement philosopher on the same host restores that count during
initialization. The files exist only for the duration of the program.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import tempfile
import time
from collections.abc import Callable
from enum import auto, Enum
from pathlib import Path
from typing import cast, Generic, ParamSpec, TypeVar

from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.supervision import (
    MeshFailure,
    SupervisionError,
)
from monarch._rust_bindings.monarch_hyperactor.telemetry import instant_event
from monarch._src.actor.actor_mesh import ActorMesh
from monarch._src.actor.endpoint import Endpoint
from monarch._src.actor.telemetry import TracingForwarder
from monarch.actor import (
    Actor,
    concurrent_endpoint,
    current_rank,
    endpoint,
    HostMesh,
    ProcMesh,
)
from monarch.actor.fault_injection import ActorFailurePolicy, inject_actor_failures
from monarch.job import MeshAdminConfig, ProcessJob

logger: logging.Logger = logging.getLogger("fault_tolerant_dining_philosophers")
logger.addHandler(TracingForwarder())
logger.setLevel(logging.INFO)

TActor = TypeVar("TActor", bound=Actor)
P = ParamSpec("P")
R = TypeVar("R")
NUM_PHILOSOPHERS = 5


class RebindableActorMesh(Generic[TActor]):
    def __init__(self, mesh: TActor) -> None:
        self._mesh = mesh
        self._rebound = asyncio.Condition()

    async def rebind(self, replacement: RebindableActorMesh[TActor]) -> None:
        async with self._rebound:
            self._mesh = replacement._mesh
            self._rebound.notify_all()

    async def retry_call_one(
        self,
        select_endpoint: Callable[[TActor], Endpoint[P, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        while True:
            mesh = self._mesh
            endpoint = select_endpoint(mesh)
            try:
                return await endpoint.call_one(*args, **kwargs)
            except SupervisionError:
                async with self._rebound:
                    while self._mesh is mesh:
                        await self._rebound.wait()


class ChopstickStatus(Enum):
    NONE = auto()
    REQUESTED = auto()
    GRANTED = auto()


class Philosopher(Actor):
    """A philosopher that retries calls after its waiter mesh is replaced."""

    def __init__(self, size: int, state_directory: str) -> None:
        self.table_size = size
        self.rank = current_rank().rank
        self.left_status = ChopstickStatus.NONE
        self.right_status = ChopstickStatus.NONE
        self.waiter: RebindableActorMesh[Waiter] | None = None
        self.meals_path = Path(state_directory) / f"philosopher_{self.rank}.meals"
        try:
            self.meals_eaten = int(self.meals_path.read_text())
        except FileNotFoundError:
            self.meals_eaten = 0
        logger.info(
            "philosopher %d restored meal count %d",
            self.rank,
            self.meals_eaten,
        )

    def _chopstick_indices(self) -> tuple[int, int]:
        left = self.rank % self.table_size
        right = (self.rank + 1) % self.table_size
        return left, right

    async def _request_chopsticks(self) -> None:
        waiter = self.waiter
        if waiter is None:
            raise RuntimeError("waiter has not been set")

        left, right = self._chopstick_indices()
        self.left_status = ChopstickStatus.REQUESTED
        self.right_status = ChopstickStatus.REQUESTED
        await waiter.retry_call_one(
            lambda waiter: waiter.request_chopsticks,
            self.rank,
            left,
            right,
        )

    async def _release_chopsticks(self) -> None:
        waiter = self.waiter
        if waiter is None:
            raise RuntimeError("waiter has not been set")

        left, right = self._chopstick_indices()
        self.left_status = ChopstickStatus.NONE
        self.right_status = ChopstickStatus.NONE
        await waiter.retry_call_one(
            lambda waiter: waiter.release_chopsticks,
            left,
            right,
        )

    @endpoint
    async def start(self, waiter: RebindableActorMesh[Waiter]) -> None:
        """Begin the philosopher's lifecycle."""
        self.waiter = waiter
        await self._request_chopsticks()

    @concurrent_endpoint
    async def rebind_waiter(
        self,
        waiter: RebindableActorMesh[Waiter],
    ) -> None:
        current_waiter = self.waiter
        if current_waiter is None:
            self.waiter = waiter
            return

        await current_waiter.rebind(waiter)

    @endpoint
    async def put_down_chopsticks(self) -> None:
        self.left_status = ChopstickStatus.NONE
        self.right_status = ChopstickStatus.NONE
        await self._request_chopsticks()

    @endpoint
    async def grant_chopstick(self, chopstick: int) -> None:
        """Called by the waiter when a chopstick is granted."""
        left, right = self._chopstick_indices()

        if chopstick == left:
            self.left_status = ChopstickStatus.GRANTED
        elif chopstick == right:
            self.right_status = ChopstickStatus.GRANTED

        if (
            self.left_status != ChopstickStatus.GRANTED
            or self.right_status != ChopstickStatus.GRANTED
        ):
            return

        # Reset a progress watchdog here when entering the eating state.
        self.meals_eaten += 1
        self.meals_path.write_text(str(self.meals_eaten))
        logger.info(
            "philosopher %d is eating (meal %d)",
            self.rank,
            self.meals_eaten,
        )
        await asyncio.sleep(1)

        await self._release_chopsticks()

        # Reset a progress watchdog here when entering the thinking state.
        await asyncio.sleep(0.5)

        await self._request_chopsticks()

    @concurrent_endpoint
    async def watch_progress(self) -> None:
        rank = current_rank().rank
        previous_meals_eaten = self.meals_eaten
        while True:
            await asyncio.sleep(4)
            meals_eaten = self.meals_eaten
            if meals_eaten == previous_meals_eaten:
                instant_event(
                    f"[watch progress] philosopher {rank} made no progress within 4 seconds",
                )
            else:
                instant_event(
                    f"[watch progress] philosopher {rank} made progress: "
                    f"{meals_eaten - previous_meals_eaten} meals",
                )
            previous_meals_eaten = meals_eaten


class Waiter(Actor):
    """A waiter that updates its philosopher mesh after replacement."""

    def __init__(
        self,
        philosophers: Philosopher,
        granting_requests: bool = True,
    ) -> None:
        self.philosophers = philosophers
        self.assignments: dict[int, int] = {}
        self.requests: dict[int, int] = {}
        self.pending_requests: dict[int, tuple[int, int]] = {}
        self.granting_requests = granting_requests

    @endpoint
    async def set_philosophers(
        self,
        philosophers: Philosopher,
        waiter: RebindableActorMesh[Waiter],
    ) -> None:
        self.assignments.clear()
        self.requests.clear()
        self.pending_requests.clear()
        self.philosophers = philosophers
        self.philosophers.start.broadcast(waiter)

    def _handle_invalid_reference(
        self,
        message: UndeliverableMessageEnvelope,
    ) -> bool:
        return True

    def _try_grant(self, rank: int, chopstick: int) -> None:
        if chopstick not in self.assignments:
            self.assignments[chopstick] = rank
            logger.info("granted chopstick %d to philosopher %d", chopstick, rank)
            self.philosophers.slice(replica=rank).grant_chopstick.broadcast(chopstick)
        else:
            logger.info("chopstick %d busy, philosopher %d queued", chopstick, rank)
            self.requests[chopstick] = rank

    def _release(self, chopstick: int) -> None:
        self.assignments.pop(chopstick, None)
        if chopstick in self.requests:
            rank = self.requests.pop(chopstick)
            logger.info(
                "chopstick %d released, granting to philosopher %d",
                chopstick,
                rank,
            )
            self._try_grant(rank, chopstick)

    @endpoint
    async def request_chopsticks(self, rank: int, left: int, right: int) -> None:
        if not self.granting_requests:
            self.pending_requests[rank] = (left, right)
            return
        self._try_grant(rank, left)
        self._try_grant(rank, right)

    @endpoint
    async def release_chopsticks(self, left: int, right: int) -> None:
        if not self.granting_requests:
            return
        self._release(left)
        self._release(right)

    @endpoint
    async def start_granting(self) -> None:
        self.granting_requests = True
        pending_requests = list(self.pending_requests.items())
        self.pending_requests.clear()
        for rank, (left, right) in pending_requests:
            self._try_grant(rank, left)
            self._try_grant(rank, right)


class DiningSupervisor(Actor):
    """Restart failed meshes and send replacement references to their peers."""

    def __init__(
        self,
        hosts: HostMesh,
        state_directory: str,
    ) -> None:
        self.hosts = hosts
        self.state_directory = state_directory
        self.retired_mesh_names: set[str] = set()
        self.philosopher_procs = self._spawn_philosopher_procs()
        self.waiter_procs = self._spawn_waiter_procs()
        self.philosophers, self.philosopher_name = self._spawn_philosophers(
            self.philosopher_procs
        )
        self.waiter, self.waiter_name = self._spawn_waiter(
            self.waiter_procs, self.philosophers
        )

        self.philosophers.start.call(RebindableActorMesh(self.waiter)).get()
        self.philosophers.watch_progress.broadcast()

    def _spawn_philosopher_procs(self) -> ProcMesh:
        return self.hosts.spawn_procs(
            per_host={"replica": NUM_PHILOSOPHERS},
            name="philosophers_proc",
        )

    def _spawn_waiter_procs(self) -> ProcMesh:
        return self.hosts.spawn_procs(name="waiter_proc")

    def _spawn_philosophers(
        self,
        procs: ProcMesh,
    ) -> tuple[Philosopher, str]:
        philosophers = procs.spawn(
            "philosophers",
            Philosopher,
            NUM_PHILOSOPHERS,
            self.state_directory,
        )
        actor_mesh = cast(ActorMesh[Actor], philosophers)
        actor_mesh.initialized.get()
        return philosophers, actor_mesh._name.get()

    def _spawn_waiter(
        self,
        procs: ProcMesh,
        philosophers: Philosopher,
        granting_requests: bool = True,
    ) -> tuple[Waiter, str]:
        waiter = procs.spawn("waiter", Waiter, philosophers, granting_requests)
        actor_mesh = cast(ActorMesh[Actor], waiter)
        actor_mesh.initialized.get()
        return waiter, actor_mesh._name.get()

    def _stop_procs(self, procs: ProcMesh, reason: str) -> None:
        try:
            procs.stop(reason).get(timeout=10)
        except Exception as error:
            logger.warning("failed to stop replaced proc mesh: %r", error)

    def _restart_waiters(self) -> None:
        self.retired_mesh_names.add(self.waiter_name)
        self._stop_procs(self.waiter_procs, "restarting waiter")

        self.waiter_procs = self._spawn_waiter_procs()
        self.waiter, self.waiter_name = self._spawn_waiter(
            self.waiter_procs,
            self.philosophers,
            granting_requests=False,
        )

        try:
            self.philosophers.rebind_waiter.call(RebindableActorMesh(self.waiter)).get()
            self.philosophers.put_down_chopsticks.call().get()
        except SupervisionError:
            self._restart_philosophers()

        self.waiter.start_granting.broadcast()

    def _restart_philosophers(self) -> None:
        self.retired_mesh_names.add(self.philosopher_name)
        self._stop_procs(self.philosopher_procs, "restarting philosophers")

        self.philosopher_procs = self._spawn_philosopher_procs()
        self.philosophers, self.philosopher_name = self._spawn_philosophers(
            self.philosopher_procs
        )

        self.philosophers.watch_progress.broadcast()
        self.waiter.set_philosophers.broadcast(
            self.philosophers,
            RebindableActorMesh(self.waiter),
        )

    def _handle_invalid_reference(
        self,
        message: UndeliverableMessageEnvelope,
    ) -> bool:
        return True

    def __supervise__(self, failure: MeshFailure) -> bool:
        if failure.mesh_name == self.philosopher_name:
            self._restart_philosophers()
            return True
        elif failure.mesh_name == self.waiter_name:
            self._restart_waiters()
            return True
        elif failure.mesh_name in self.retired_mesh_names:
            return True
        else:
            return False


def run_dining_philosophers(*, duration_s: float | None = None) -> None:
    with tempfile.TemporaryDirectory(
        prefix="monarch_dining_philosophers_"
    ) as state_directory:
        job = ProcessJob({"hosts": 1})
        job.enable_telemetry(mesh_admin_config=MeshAdminConfig(admin_addr="[::]:43021"))
        try:
            state = job.state(cached_path=None)
            supervisor_proc = state.hosts.spawn_procs(name="dining_supervisor")
            supervisor = supervisor_proc.spawn(
                "dining_supervisor",
                DiningSupervisor,
                state.hosts,
                state_directory,
            )
            cast(ActorMesh[Actor], supervisor).initialized.get()
            if duration_s is None:
                while True:
                    time.sleep(3600)
            else:
                time.sleep(duration_s)
        finally:
            job.kill()


def run_fault_tolerant_example(
    *,
    duration_s: float | None = None,
    waiter_failure_probability: float = 0.3,
    waiter_failure_interval_s: float = 1.0,
    philosopher_failure_probability: float = 0.3,
    philosopher_failure_interval_s: float = 1.0,
) -> None:
    policies: dict[type[Actor], ActorFailurePolicy] = {
        Waiter: ActorFailurePolicy(
            waiter_failure_probability,
            waiter_failure_interval_s,
        ),
        Philosopher: ActorFailurePolicy(
            philosopher_failure_probability,
            philosopher_failure_interval_s,
        ),
    }
    with inject_actor_failures(policies):
        run_dining_philosophers(duration_s=duration_s)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run fault-tolerant dining philosophers with injected failures."
    )
    parser.add_argument("--duration", type=float, help="run duration in seconds")
    parser.add_argument(
        "--waiter-probability",
        type=float,
        default=0.3,
        help="waiter failure probability per interval",
    )
    parser.add_argument(
        "--waiter-interval",
        type=float,
        default=1.0,
        help="seconds between waiter failure checks",
    )
    parser.add_argument(
        "--philosopher-probability",
        type=float,
        default=0.3,
        help="philosopher failure probability per interval",
    )
    parser.add_argument(
        "--philosopher-interval",
        type=float,
        default=1.0,
        help="seconds between philosopher failure checks",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    try:
        run_fault_tolerant_example(
            duration_s=args.duration,
            waiter_failure_probability=args.waiter_probability,
            waiter_failure_interval_s=args.waiter_interval,
            philosopher_failure_probability=args.philosopher_probability,
            philosopher_failure_interval_s=args.philosopher_interval,
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
