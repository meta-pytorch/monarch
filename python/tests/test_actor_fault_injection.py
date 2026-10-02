# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from __future__ import annotations

import asyncio
import unittest
from typing import cast

import pytest
from monarch._src.actor.mock import get_actor_class
from monarch._src.job.process import ProcessJob
from monarch.actor import (
    Actor,
    concurrent_endpoint,
    context,
    endpoint,
    HostMesh,
    MeshFailure,
    ProcMesh,
)
from monarch.actor.fault_injection import ActorFailurePolicy, inject_actor_failures
from scoped_state import scoped_state


class _FailureTargetActor(Actor):
    @endpoint
    async def ping(self) -> str:
        return "alive"

    @endpoint
    async def fail_unexpectedly(self) -> None:
        context().actor_instance.abort("unexpected test failure")


class _NestedSpawnerActor(Actor):
    def __init__(self, hosts: HostMesh) -> None:
        self.hosts = hosts
        self.target_procs: ProcMesh | None = None
        self.failure_report: str | None = None
        self.failure_reported = asyncio.Condition()
        self.failure_is_injected: bool | None = None

    @endpoint
    async def spawn_target(self, fail_unexpectedly: bool = False) -> str:
        self.target_procs = self.hosts.spawn_procs(name="fault_target_proc")
        target = self.target_procs.spawn("fault_target", _FailureTargetActor)

        result = await target.ping.call_one()

        if fail_unexpectedly:
            await target.fail_unexpectedly.call_one()

        return result

    @concurrent_endpoint
    async def wait_for_failure(self) -> tuple[str, bool]:
        async with asyncio.timeout(10.0):
            async with self.failure_reported:
                await self.failure_reported.wait_for(
                    lambda: self.failure_report is not None
                    and self.failure_is_injected is not None
                )
                report = self.failure_report
                is_injected = self.failure_is_injected

        if report is None or is_injected is None:
            raise RuntimeError("failure condition completed without a report")

        return report, is_injected

    async def __supervise__(self, failure: MeshFailure) -> bool:
        async with self.failure_reported:
            self.failure_report = failure.report()
            self.failure_is_injected = failure.is_injected
            self.failure_reported.notify_all()

        return True


class _RecordingSupervisor(Actor):
    def __init__(self) -> None:
        self.handled_failures = 0

    def __supervise__(self, failure: MeshFailure) -> bool:
        self.handled_failures += 1
        return True


class _TestFailure:
    def __init__(self, is_injected: bool) -> None:
        self.is_injected = is_injected


class _OuterObserverActor(Actor):
    def __init__(self, hosts: HostMesh) -> None:
        self.hosts = hosts
        self.supervisor_procs: ProcMesh | None = None
        self.failure_report: str | None = None
        self.failure_is_injected: bool | None = None
        self.failure_reported = asyncio.Condition()

    @endpoint
    async def trigger_unexpected_failure(self) -> str:
        self.supervisor_procs = self.hosts.spawn_procs(name="supervisor_proc")
        supervisor = self.supervisor_procs.spawn(
            "supervisor",
            _NestedSpawnerActor,
            self.hosts,
        )
        return await supervisor.spawn_target.call_one(True)

    @concurrent_endpoint
    async def wait_for_failure(self) -> tuple[str, bool]:
        async with asyncio.timeout(10.0):
            async with self.failure_reported:
                await self.failure_reported.wait_for(
                    lambda: self.failure_report is not None
                    and self.failure_is_injected is not None
                )
                report = self.failure_report
                is_injected = self.failure_is_injected

        if report is None or is_injected is None:
            raise RuntimeError("failure condition completed without a report")
        return report, is_injected

    async def __supervise__(self, failure: MeshFailure) -> bool:
        async with self.failure_reported:
            self.failure_report = failure.report()
            self.failure_is_injected = failure.is_injected
            self.failure_reported.notify_all()

        return True


class SupervisorPolicyTest(unittest.TestCase):
    def test_supervisor_only_handles_injected_failures(self) -> None:
        with inject_actor_failures(
            {_FailureTargetActor: ActorFailurePolicy(0.0)},
            supervisors=(_RecordingSupervisor,),
        ):
            supervisor_type = get_actor_class(_RecordingSupervisor)
            supervisor = cast(_RecordingSupervisor, supervisor_type())
            injected = cast(MeshFailure, _TestFailure(True))
            unexpected = cast(MeshFailure, _TestFailure(False))

            self.assertTrue(supervisor.__supervise__(injected))
            self.assertFalse(supervisor.__supervise__(unexpected))
            self.assertEqual(1, supervisor.handled_failures)


class FaultInjectionTest(unittest.TestCase):
    @pytest.mark.timeout(60)
    def test_failure_timer_reaches_nested_process(self) -> None:
        with inject_actor_failures(
            {_FailureTargetActor: ActorFailurePolicy(1.0, 0.2)},
            supervisors=(_NestedSpawnerActor,),
        ):
            with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
                proc_mesh = state.hosts.spawn_procs(name="fault_owner_proc")
                owner = proc_mesh.spawn(
                    "fault_owner",
                    _NestedSpawnerActor,
                    state.hosts,
                )
                self.assertEqual("alive", owner.spawn_target.call_one().get())

                report, is_injected = owner.wait_for_failure.call_one().get()
                self.assertIn("injected actor failure", report)
                self.assertTrue(is_injected)

    @pytest.mark.timeout(60)
    def test_supervisor_propagates_unexpected_failure(self) -> None:
        with inject_actor_failures(
            {_FailureTargetActor: ActorFailurePolicy(0.0)},
            supervisors=(_NestedSpawnerActor,),
        ):
            with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
                proc_mesh = state.hosts.spawn_procs(name="outer_observer_proc")
                observer = proc_mesh.spawn(
                    "outer_observer",
                    _OuterObserverActor,
                    state.hosts,
                )
                self.assertEqual(
                    "alive",
                    observer.trigger_unexpected_failure.call_one().get(),
                )

                report, is_injected = observer.wait_for_failure.call_one().get()
                self.assertIn("unexpected test failure", report)
                self.assertFalse(is_injected)
