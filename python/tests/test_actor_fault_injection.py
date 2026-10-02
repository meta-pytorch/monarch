# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from __future__ import annotations

import asyncio
import unittest

import pytest
from monarch._src.job.process import ProcessJob
from monarch.actor import (
    Actor,
    concurrent_endpoint,
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


class _NestedSpawnerActor(Actor):
    def __init__(self, hosts: HostMesh) -> None:
        self.hosts = hosts
        self.target_procs: ProcMesh | None = None
        self.failure_report: str | None = None
        self.failure_reported = asyncio.Condition()

    @endpoint
    async def spawn_target(self) -> str:
        self.target_procs = self.hosts.spawn_procs(name="fault_target_proc")
        target = self.target_procs.spawn("fault_target", _FailureTargetActor)
        return await target.ping.call_one()

    @concurrent_endpoint
    async def wait_for_failure(self) -> str:
        async with asyncio.timeout(10.0):
            async with self.failure_reported:
                await self.failure_reported.wait_for(
                    lambda: self.failure_report is not None
                )

        if self.failure_report is None:
            raise RuntimeError("failure condition completed without a report")
        return self.failure_report

    async def __supervise__(self, failure: MeshFailure) -> bool:
        async with self.failure_reported:
            self.failure_report = failure.report()
            self.failure_reported.notify_all()
        return True


class FaultInjectionTest(unittest.TestCase):
    @pytest.mark.timeout(60)
    def test_failure_timer_reaches_nested_process(self) -> None:
        with inject_actor_failures({_FailureTargetActor: ActorFailurePolicy(1.0, 0.2)}):
            with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
                proc_mesh = state.hosts.spawn_procs(name="fault_owner_proc")
                owner = proc_mesh.spawn(
                    "fault_owner",
                    _NestedSpawnerActor,
                    state.hosts,
                )
                self.assertEqual("alive", owner.spawn_target.call_one().get())

                report = owner.wait_for_failure.call_one().get()
                self.assertIn("injected actor failure", report)
