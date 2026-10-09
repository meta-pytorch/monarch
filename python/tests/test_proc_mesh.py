# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from __future__ import annotations

import asyncio
import os
import pathlib
import tempfile
import threading
import time
import warnings
from contextlib import ExitStack
from functools import partial
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import cloudpickle
import monarch.actor
import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.context import Instance as HyInstance
from monarch._rust_bindings.monarch_hyperactor.handle import _new_handle_pair, Handle
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Shape, Slice
from monarch._src.actor.actor_mesh import (
    _client_context,
    Actor,
    ActorMesh,
    context,
    ValueMesh,
)
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import this_host, this_proc
from monarch._src.actor.proc_mesh import (
    get_or_spawn_controller,
    ProcMesh,
    register_proc_mesh_spawn_callback,
    unregister_proc_mesh_spawn_callback,
)
from monarch._src.job.process import ProcessJob
from scoped_state import scoped_state


_proc_rank = -1
_BOOTSTRAP_FAILURE = "stage 3.4 bootstrap failure"


def _successful_bootstrap() -> None:
    return None


def _fail_bootstrap() -> None:
    raise RuntimeError(_BOOTSTRAP_FAILURE)


def _gated_bootstrap(
    entered_path: str,
    release_path: str,
    failure: str | None,
) -> None:
    pathlib.Path(entered_path).touch()
    release = pathlib.Path(release_path)
    deadline = time.monotonic() + 30
    while not release.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("proc bootstrap release was not published")
        time.sleep(0.01)
    if failure is not None:
        raise RuntimeError(failure)


def _wait_for_marker(path: pathlib.Path) -> None:
    # The full target launches many isolated ProcessJobs concurrently, so
    # reaching the remote setup callback can take longer than a focused run.
    deadline = time.monotonic() + 30
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for marker {path}")
        time.sleep(0.01)


async def _wait_for_event(event: threading.Event, message: str) -> None:
    reached = await asyncio.wait_for(
        asyncio.to_thread(event.wait, 30),
        timeout=35,
    )
    assert reached, message


class _PendingActorProbe:
    def __init__(
        self,
        name: str,
        driven: list[str],
        error: BaseException | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.entered = threading.Event()

        async def initialize() -> None:
            driven.append(name)
            self.entered.set()
            if release is not None:
                released = await PythonTask.spawn_blocking(
                    lambda: release.wait(timeout=30)
                )
                if not released:
                    raise TimeoutError("pending actor release was not published")
            if error is not None:
                raise error

        self.initialized = Future._from_coro(initialize())


class _LoggingManagerProbe:
    def __init__(
        self,
        inner: Any,
        phases: list[str],
        label: str,
        error: BaseException | None = None,
        client: object | None = None,
    ) -> None:
        self._inner = inner
        self._phases = phases
        self._label = label
        self._error = error
        self._client = client

    @property
    def _logging_mesh_client(self) -> object:
        self._phases.append(self._label)
        if self._error is not None:
            raise self._error
        if self._client is not None:
            return self._client
        return self._inner._logging_mesh_client

    async def flush_async(self) -> None:
        # As `LoggingManager.flush_async`, reading the client once.
        client: Any = self._logging_mesh_client
        if client is None:
            return
        try:
            await client.flush(context().actor_instance._as_rust())
        except Exception:
            pass


class _StopAbort(BaseException):
    pass


class _ProcStopCallThrough:
    """Gate a real native stop without replacing its behavior."""

    def __init__(self, inner: HyProcMesh, release: threading.Event) -> None:
        self._inner = inner
        self._release = release
        self.entered = threading.Event()
        self.calls = 0

    def stop_nonblocking(self, instance: HyInstance, reason: str) -> Handle[None]:
        self.calls += 1
        self.entered.set()
        handle, completer = _new_handle_pair()

        # The binding starts its stop when called, so call it only after the
        # release.
        def gated() -> None:
            if not self._release.wait(timeout=30):
                completer.set_exception(
                    TimeoutError("proc stop release was not published")
                )
                return
            try:
                self._inner.stop_nonblocking(instance, reason).get()
            except Exception as error:
                completer.set_exception(error)
            else:
                completer.set_result(None)

        threading.Thread(target=gated, daemon=True).start()
        return handle


class TestActor(Actor):
    def __init__(self, initial_value: int = 0):
        self.value = initial_value
        global _proc_rank
        if _proc_rank == -1:
            _proc_rank = context().actor_instance.rank.rank

    @endpoint
    async def get_value(self) -> int:
        return self.value

    @endpoint
    async def set_value(self, value: int) -> None:
        self.value = value

    @endpoint
    async def get_proc_rank(self) -> int:
        return _proc_rank

    @endpoint
    async def spawn_on_this_host(self) -> "TestActor":
        rank = context().actor_instance.rank.rank
        return (
            this_host()
            ._new_with_shape(
                Shape(
                    labels=["hosts"], slice=Slice(offset=rank, sizes=[1], strides=[1])
                )
            )
            .spawn_procs(name=f"test_proc_{rank}", per_host={"gpus": 4})
            .spawn(f"test_{rank}", TestActor, rank)
        )

    @endpoint
    async def get_rank_plus_init_value(self) -> int:
        return context().actor_instance.rank.rank + self.value

    @endpoint
    async def call_on_other_mesh(self, actor: "TestActor") -> ValueMesh[int]:
        return await actor.get_rank_plus_init_value.call()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_proc_mesh_initialization() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(
            name="test_proc",
            bootstrap=_successful_bootstrap,
        )
        assert await proc_mesh.initialized


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_proc_mesh_initialized_fails_when_bootstrap_fails() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        proc_mesh = state.hosts.spawn_procs(bootstrap=_fail_bootstrap)
        with pytest.raises(monarch.actor.ActorError, match=_BOOTSTRAP_FAILURE):
            await proc_mesh.initialized


@pytest.mark.timeout(90)
@isolate_in_subprocess
def test_actor_mesh_readiness_survives_timeout_on_the_same_future() -> None:
    with tempfile.TemporaryDirectory(prefix="monarch_actor_ready_") as directory:
        entered = pathlib.Path(directory) / "entered"
        release = pathlib.Path(directory) / "release"
        with ExitStack() as cleanup:
            state = cleanup.enter_context(
                scoped_state(ProcessJob({"hosts": 1}), cached_path=None)
            )
            cleanup.callback(release.touch)
            proc_mesh = state.hosts.spawn_procs(
                bootstrap=partial(_gated_bootstrap, str(entered), str(release), None)
            )
            _wait_for_marker(entered)
            actor = proc_mesh.spawn("readiness", TestActor, 0)
            readiness = actor.initialized

            # Actor spawn waits for proc setup, which has acknowledged the
            # closed gate. This timeout cannot be a race with completed spawn.
            with pytest.raises(TimeoutError):
                readiness.get(timeout=0.25)
            release.touch()

            assert readiness.get(timeout=30) is None
            assert readiness.get(timeout=30) is None
            assert actor.get_rank_plus_init_value.call_one().get(timeout=30) == 0


@pytest.mark.timeout(90)
@isolate_in_subprocess
def test_proc_mesh_readiness_replays_success_after_discarded_observer() -> None:
    with tempfile.TemporaryDirectory(prefix="monarch_proc_ready_") as directory:
        entered = pathlib.Path(directory) / "entered"
        release = pathlib.Path(directory) / "release"
        with ExitStack() as cleanup:
            state = cleanup.enter_context(
                scoped_state(ProcessJob({"hosts": 1}), cached_path=None)
            )
            cleanup.callback(release.touch)
            proc_mesh = state.hosts.spawn_procs(
                name="test_proc",
                bootstrap=partial(
                    _gated_bootstrap,
                    str(entered),
                    str(release),
                    None,
                ),
            )
            _wait_for_marker(entered)

            discarded = proc_mesh.initialized
            with pytest.raises(TimeoutError):
                discarded.get(timeout=0.25)
            del discarded
            release.touch()

            assert proc_mesh.initialized.get(timeout=30) is True
            assert proc_mesh.initialized.get(timeout=30) is True


@pytest.mark.timeout(90)
@isolate_in_subprocess
def test_proc_mesh_readiness_replays_failure_after_discarded_observer() -> None:
    with tempfile.TemporaryDirectory(prefix="monarch_proc_ready_") as directory:
        entered = pathlib.Path(directory) / "entered"
        release = pathlib.Path(directory) / "release"
        with ExitStack() as cleanup:
            state = cleanup.enter_context(
                scoped_state(ProcessJob({"hosts": 1}), cached_path=None)
            )
            cleanup.callback(release.touch)
            proc_mesh = state.hosts.spawn_procs(
                bootstrap=partial(
                    _gated_bootstrap,
                    str(entered),
                    str(release),
                    _BOOTSTRAP_FAILURE,
                )
            )
            _wait_for_marker(entered)

            discarded = proc_mesh.initialized
            with pytest.raises(TimeoutError):
                discarded.get(timeout=0.25)
            del discarded
            release.touch()

            errors = []
            for _ in range(2):
                with pytest.raises(monarch.actor.ActorError) as excinfo:
                    proc_mesh.initialized.get(timeout=30)
                errors.append(excinfo.value)

            assert type(errors[0]) is monarch.actor.ActorError
            assert type(errors[1]) is monarch.actor.ActorError
            assert str(errors[0]) == str(errors[1])
            assert _BOOTSTRAP_FAILURE in str(errors[0])


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_proc_stop_is_lazy_and_survives_cancelled_observer() -> None:
    job = ProcessJob({"hosts": 1})
    release = threading.Event()
    stop_future: Future[None] | None = None
    try:
        host = job.state(cached_path=None).hosts
        owner = host.spawn_procs(per_host={"gpus": 1})
        await asyncio.wait_for(owner.initialized, timeout=30)
        inner = owner._proc_mesh.poll()
        assert inner is not None

        discarded = owner.stop()
        del discarded
        assert not owner._stopped
        actor = owner.spawn("after_discarded_stop", TestActor, 41)
        assert await asyncio.wait_for(actor.get_value.choose(), 30) == 41

        call_through = _ProcStopCallThrough(inner, release)
        owner._proc_mesh = Shared.from_value(cast(HyProcMesh, call_through))
        stop_future = owner.stop()
        assert not owner._stopped
        assert call_through.calls == 0

        observer = stop_future.as_asyncio()
        await _wait_for_event(
            call_through.entered,
            "proc stop did not reach the native binding",
        )
        assert not owner._stopped
        assert call_through.calls == 1

        assert observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await observer

        release.set()
        assert (
            await asyncio.wait_for(
                asyncio.to_thread(stop_future.get, 30),
                timeout=35,
            )
            is None
        )
        assert await asyncio.wait_for(stop_future, timeout=30) is None
        assert owner._stopped

        assert await asyncio.wait_for(owner.stop(), timeout=30) is None
        assert call_through.calls == 2
        # The two calls prove that both requests reached the binding. Source
        # inspection, not this seam, shows that only the first took the
        # SharedCell and reached the domain ProcMesh::stop operation. It also
        # shows that an overlapping second stop can set _stopped True even if
        # the first domain stop later fails; this test does not create overlap.
    finally:
        release.set()
        try:
            if stop_future is not None:
                try:
                    await asyncio.wait_for(stop_future, timeout=30)
                except (Exception, asyncio.CancelledError):
                    pass
        finally:
            job.kill()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_stop_state_tracks_native_stop_result() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        owner = state.hosts.spawn_procs(per_host={"gpus": 2})
        assert owner.initialized.get(timeout=30) is True
        inner = owner._proc_mesh.poll()
        assert inner is not None
        logging_manager = owner._logging_manager
        assert logging_manager._logging_mesh_client is not None
        proc_ref = owner.slice(gpus=0)
        raw_ref = proc_ref._proc_mesh.poll()
        assert raw_ref is not None
        instance = context().actor_instance._as_rust()

        with pytest.raises(ValueError) as raw_stop_error:
            raw_ref.stop_nonblocking(instance, "test reference rejection")
        assert type(raw_stop_error.value) is ValueError
        assert str(raw_stop_error.value) == (
            "ProcMesh is not owned; must be stopped by an owner"
        )

        with pytest.raises(ValueError) as public_stop_error:
            proc_ref.stop().get(timeout=30)
        assert type(public_stop_error.value) is ValueError
        assert str(public_stop_error.value) == str(raw_stop_error.value)

        assert not proc_ref._stopped
        phases: list[str] = []
        actor_abort = _PendingActorProbe(
            "actor-base-exception",
            phases,
            _StopAbort("actor initialization interrupted"),
        )
        owner._pending_actor_spawns.append(cast(ActorMesh, actor_abort))
        owner._logging_manager = cast(
            Any,
            _LoggingManagerProbe(logging_manager, phases, "logging-unreached"),
        )

        with pytest.raises(_StopAbort, match="actor initialization interrupted"):
            owner.stop().get(timeout=30)
        assert not owner._stopped
        assert owner._pending_actor_spawns == [actor_abort]
        assert inner.region is not None
        assert phases == ["actor-base-exception"]

        owner._pending_actor_spawns.clear()
        pending_actor = _PendingActorProbe("actor", phases)
        owner._pending_actor_spawns.append(cast(ActorMesh, pending_actor))
        owner._logging_manager = cast(
            Any,
            _LoggingManagerProbe(
                logging_manager,
                phases,
                "logging-base-exception",
                _StopAbort("stop interrupted"),
            ),
        )

        with pytest.raises(_StopAbort, match="stop interrupted"):
            owner.stop().get(timeout=30)
        assert not owner._stopped
        # The drain finished, so its spawn is removed although the stop did not.
        assert owner._pending_actor_spawns == []
        assert inner.region is not None
        assert phases == [
            "actor-base-exception",
            "actor",
            "logging-base-exception",
        ]

        owner._logging_manager = cast(
            Any,
            _LoggingManagerProbe(
                logging_manager,
                phases,
                "logging-read-exception",
                RuntimeError("logging client unreadable"),
            ),
        )
        with pytest.raises(RuntimeError, match="logging client unreadable"):
            owner.stop().get(timeout=30)
        assert not owner._stopped
        assert inner.region is not None

        owner._pending_actor_spawns.append(
            cast(ActorMesh, _PendingActorProbe("actor-reason-type", phases))
        )
        owner._logging_manager = cast(
            Any, _LoggingManagerProbe(logging_manager, phases, "logging-reason-type")
        )
        reason_type_stop = owner.stop(cast(Any, 42))
        with pytest.raises(TypeError, match="reason"):
            reason_type_stop.get(timeout=30)
        assert not owner._stopped
        assert inner.region is not None

        owner._pending_actor_spawns.append(
            cast(ActorMesh, _PendingActorProbe("actor-missing-stop", phases))
        )
        owner._logging_manager = cast(
            Any, _LoggingManagerProbe(logging_manager, phases, "logging-missing-stop")
        )
        proc_mesh = owner._proc_mesh
        owner._proc_mesh = cast(Any, Shared.from_value(object()))
        missing_stop = owner.stop()
        with pytest.raises(AttributeError, match="stop_nonblocking"):
            missing_stop.get(timeout=30)
        owner._proc_mesh = proc_mesh
        assert not owner._stopped
        assert inner.region is not None

        owner._logging_manager = cast(
            Any,
            _LoggingManagerProbe(
                logging_manager,
                phases,
                "logging-flush-exception",
                client=object(),
            ),
        )
        assert owner.stop().get(timeout=30) is None
        assert phases == [
            "actor-base-exception",
            "actor",
            "logging-base-exception",
            "logging-read-exception",
            "actor-reason-type",
            "logging-reason-type",
            "actor-missing-stop",
            "logging-missing-stop",
            "logging-flush-exception",
        ]
        assert owner._pending_actor_spawns == []
        assert owner._stopped


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_proc_stop_completes_for_each_observer_kind() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        # A synchronous caller runs the stop on its own thread's loop.
        owner = state.hosts.spawn_procs(per_host={"gpus": 1})
        assert owner.stop().get(timeout=30) is None
        assert owner._stopped

        # An asyncio caller runs it on the awaiting loop.
        owner = state.hosts.spawn_procs(per_host={"gpus": 1})

        async def awaited() -> None:
            assert await asyncio.wait_for(owner.stop(), timeout=30) is None

        asyncio.run(awaited())
        assert owner._stopped

        # A blocking get() inside a running loop, as a synchronous endpoint
        # makes, runs it on a helper thread.
        owner = state.hosts.spawn_procs(per_host={"gpus": 1})

        async def blocking() -> None:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                assert owner.stop().get(timeout=30) is None

        asyncio.run(blocking())
        assert owner._stopped


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_proc_stop_creates_no_python_task_from_a_coroutine() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        owner = state.hosts.spawn_procs(per_host={"gpus": 1})
        assert owner.initialized.get(timeout=30) is True
        with patch.object(Future, "_from_coro", wraps=Future._from_coro) as from_coro:
            assert owner.stop().get(timeout=30) is None
        assert from_coro.call_count == 0
        assert owner._stopped


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_proc_stop_drain_waits_for_spawns_added_meanwhile() -> None:
    driven: list[str] = []
    release_first, release_added = threading.Event(), threading.Event()
    first = _PendingActorProbe("first", driven, release=release_first)
    added = _PendingActorProbe("added", driven, release=release_added)
    owner = SimpleNamespace(_pending_actor_spawns=[first])

    async def drain_while_appending() -> None:
        drain = asyncio.ensure_future(
            ProcMesh._drain_pending_actor_spawns(cast(ProcMesh, owner))
        )
        await _wait_for_event(first.entered, "the drain did not reach its first spawn")
        owner._pending_actor_spawns.append(added)
        release_first.set()
        await _wait_for_event(added.entered, "the drain skipped the added spawn")
        release_added.set()
        await asyncio.wait_for(drain, timeout=30)

    try:
        asyncio.run(drain_while_appending())
    finally:
        release_first.set()
        release_added.set()
    assert driven == ["first", "added"]
    assert owner._pending_actor_spawns == []


class _AppendBeforeRemoval(list[Any]):
    """A pending list that receives one more spawn just before its first
    removal, as when another thread spawns at that moment."""

    def __init__(self, items: list[Any], late: Any) -> None:
        super().__init__(items)
        self._late: Any = late

    def _arrive(self) -> None:
        if self._late is not None:
            late, self._late = self._late, None
            self.append(late)

    def remove(self, value: Any) -> None:
        self._arrive()
        super().remove(value)

    def clear(self) -> None:
        self._arrive()
        super().clear()


class _CountedSpawn:
    """A pending spawn that counts reads of its `initialized` Future, so a test
    can tell when each drain has selected it."""

    def __init__(self, probe: _PendingActorProbe) -> None:
        self._probe = probe
        self.reads = 0

    @property
    def initialized(self) -> Future[None]:
        self.reads += 1
        return self._probe.initialized


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_overlapping_stop_drains_wait_for_a_late_spawn() -> None:
    driven: list[str] = []
    release_first, release_late = threading.Event(), threading.Event()
    first_probe = _PendingActorProbe("first", driven, release=release_first)
    first = _CountedSpawn(first_probe)
    late = _PendingActorProbe("late", driven, release=release_late)
    pending = _AppendBeforeRemoval([first], late)
    owner = SimpleNamespace(_pending_actor_spawns=pending)

    async def overlap() -> None:
        drains = [
            asyncio.ensure_future(
                ProcMesh._drain_pending_actor_spawns(cast(ProcMesh, owner))
            )
            for _ in range(2)
        ]
        # Both drains must have selected the first spawn before it is released,
        # so one of them removes an entry the other has also selected.
        deadline = asyncio.get_running_loop().time() + 30
        while first.reads < 2:
            assert asyncio.get_running_loop().time() < deadline, (
                "both drains did not select the first spawn"
            )
            await asyncio.sleep(0.01)
        assert first.reads == 2
        await _wait_for_event(
            first_probe.entered, "the first spawn did not start initializing"
        )
        release_first.set()
        # The late spawn arrives as one drain removes the first; some drain
        # must wait for it before anything removes it.
        await _wait_for_event(late.entered, "no drain waited for the late spawn")
        assert list(pending) == [late]
        release_late.set()
        await asyncio.wait_for(asyncio.gather(*drains), timeout=30)

    try:
        asyncio.run(overlap())
    finally:
        release_first.set()
        release_late.set()
    assert driven == ["first", "late"]
    assert list(pending) == []


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_proc_mesh_spawn_single_actor() -> None:
    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(name="test_proc")
        actor = proc_mesh.spawn("test_actor", TestActor, 42)
        assert actor.get_value.call_one().get() == 42
        actor.set_value.call_one(43).get()
        assert actor.get_value.call_one().get() == 43


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_proc_mesh_multi_actor() -> None:
    with scoped_state(ProcessJob({"hosts": 4}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(name="test_proc", per_host={"gpus": 3})
        actor = proc_mesh.spawn("test_actor", TestActor, 42)

        proc_ranks = actor.get_proc_rank.call().get()
        assert proc_ranks.extent.labels == ["hosts", "gpus"]
        assert proc_ranks.extent.sizes == [4, 3]
        for i, (point, rank) in enumerate(proc_ranks.items()):
            assert rank == i
            assert point.rank == i


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_proc_mesh_sliced() -> None:
    with scoped_state(ProcessJob({"hosts": 4}), cached_path=None) as state:
        host = state.hosts
        proc_mesh = host.spawn_procs(name="test_proc", per_host={"gpus": 3})
        # Initialize _proc_rank on each actor process
        actor = proc_mesh.spawn("test_actor", TestActor, 42)
        actor.get_proc_rank.call().get()
        hy_proc_mesh = proc_mesh._proc_mesh.block_on()
        release_proc_mesh = threading.Event()

        async def resolve_proc_mesh() -> HyProcMesh:
            await PythonTask.spawn_blocking(release_proc_mesh.wait)
            return hy_proc_mesh

        # Keep the native mesh unresolved so slicing must take the pending path.
        proc_mesh._proc_mesh = PythonTask.from_coroutine(resolve_proc_mesh()).spawn()

        # Hosts 0 and 2, gpus 1 and 2
        slice_shape = Shape(
            labels=["hosts", "gpus"],
            slice=Slice(offset=1, sizes=[2, 2], strides=[6, 1]),
        )
        try:
            assert proc_mesh._proc_mesh.poll() is None
            sliced = proc_mesh._new_with_shape(slice_shape)
            assert sliced._proc_mesh.poll() is None
        finally:
            release_proc_mesh.set()

        actor = sliced.spawn("test_actor_sliced", TestActor, 42)
        proc_ranks = actor.get_proc_rank.call().get()
        assert proc_ranks.extent.labels == ["hosts", "gpus"]
        assert proc_ranks.extent.sizes == [2, 2]
        for (i, (point, rank)), expected_rank in zip(
            enumerate(proc_ranks.items()), [1, 2, 7, 8]
        ):
            assert rank == expected_rank
            assert point.rank == i


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_nested_meshes() -> None:
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        host = state.hosts
        proc = host.spawn_procs(name="proc")
        actor = proc.spawn("actor", TestActor)
        nested = actor.spawn_on_this_host.call().get()
        nested_0 = nested.item(hosts=0)
        nested_1 = nested.item(hosts=1)
        for i, nested in enumerate([nested_0, nested_1]):
            region = cast(
                ProcMesh, cast(ActorMesh[TestActor], nested)._proc_mesh
            )._host_mesh.region
            assert region.labels == ["hosts"]
            assert region.slice() == Slice(offset=i, sizes=[1], strides=[1])
        res_0 = nested_0.slice(gpus=0).call_on_other_mesh.call_one(nested_1).get()
        res_1 = nested_1.slice(gpus=0).call_on_other_mesh.call_one(nested_0).get()
        for point, value in res_0:
            assert value == point.rank + 1
        for point, value in res_1:
            assert value == point.rank


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_pickle_initialized_proc_mesh_in_tokio_thread() -> None:
    monarch.actor.unhandled_fault_hook = lambda failure: None
    with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
        host = state.hosts
        proc = host.spawn_procs(per_host={"gpus": 2})

        async def task():
            cloudpickle.dumps(proc)

        await proc.initialized
        PythonTask.from_coroutine(task()).block_on()

        async def task():
            cloudpickle.dumps(proc.slice(gpus=0, hosts=0))

        PythonTask.from_coroutine(task()).block_on()


class PidActor(Actor):
    @endpoint
    def get_pid(self) -> int:
        return os.getpid()


@pytest.mark.timeout(60)
def test_this_proc_on_root_client_spawns_actor_in_client_os_process() -> None:
    proc = this_proc()
    actor = proc.spawn("pid_actor", PidActor)
    assert actor.get_pid.call_one().get() == os.getpid()


@pytest.mark.timeout(60)
def test_proc_mesh_on_root_client_spawns_actor_in_client_os_process() -> None:
    proc = this_proc()
    actor = proc.spawn("pid_actor", PidActor)
    assert actor.get_pid.call_one().get() == os.getpid()


class PidActorController(Actor):
    @endpoint
    def spawn_pid_actor_with_this_proc(self) -> PidActor:
        return this_proc().spawn("pid", PidActor)

    @endpoint
    def spawn_pid_actor_with_proc_mesh(self) -> PidActor:
        return context().actor_instance.proc_mesh.spawn("pid", PidActor)


@pytest.mark.timeout(60)
def test_this_proc_in_controller_spawns_actor_in_client_os_process() -> None:
    pid_controller = get_or_spawn_controller(
        "pid_test_this_proc_in_controller", PidActorController
    ).get()
    assert (
        pid_controller.spawn_pid_actor_with_this_proc.call_one()
        .get()
        .get_pid.call_one()
        .get()
        == os.getpid()
    )


@pytest.mark.timeout(60)
def test_context_proc_mesh_in_controller_spawns_actor_in_client_os_process() -> None:
    pid_controller = get_or_spawn_controller(
        "pid_test_context_proc_mesh_in_controller", PidActorController
    ).get()
    assert (
        pid_controller.spawn_pid_actor_with_proc_mesh.call_one()
        .get()
        .get_pid.call_one()
        .get()
        == os.getpid()
    )


@pytest.mark.timeout(60)
def test_root_client_does_not_leak_proc_meshes() -> None:
    orig_get_client_context = _client_context.get
    with patch.object(_client_context, "get") as mock_get_client_context:
        mock_get_client_context.side_effect = orig_get_client_context

        def sync_sleep_then_context():
            time.sleep(0.1)
            context()

        threads = []
        for _ in range(100):
            t = threading.Thread(target=sync_sleep_then_context)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        assert mock_get_client_context.call_count == 100


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_spawn_does_not_block_on_proc_mesh_init() -> None:
    async def sleep_then_mesh(pm: Shared[HyProcMesh]) -> HyProcMesh:
        time.sleep(15)
        return await pm

    # Can't use scoped_state here: graceful shutdown blocks on proc_mesh
    # initialization, but this test intentionally replaces _proc_mesh with a
    # sleeping coroutine to verify that spawn() doesn't block.
    job = ProcessJob({"hosts": 1})
    try:
        host = job.state(cached_path=None).hosts
        proc_mesh = host.spawn_procs(name="test_proc")
        proc_mesh._proc_mesh = PythonTask.from_coroutine(
            sleep_then_mesh(proc_mesh._proc_mesh)
        ).spawn()
        assert proc_mesh._proc_mesh.poll() is None
        proc_mesh.spawn("pid", PidActor)
        assert proc_mesh._proc_mesh.poll() is None
    finally:
        job.kill()


@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_raw_proc_mesh_pickle_blocks_on_proc_mesh_init() -> None:
    async def sleep_then_mesh(pm: Shared[HyProcMesh]) -> HyProcMesh:
        time.sleep(15)
        return await pm

    proc_mesh = this_host().spawn_procs(name="test_proc")
    proc_mesh._proc_mesh = PythonTask.from_coroutine(
        sleep_then_mesh(proc_mesh._proc_mesh)
    ).spawn()
    assert proc_mesh._proc_mesh.poll() is None
    cloudpickle.dumps(proc_mesh)
    assert proc_mesh._proc_mesh.poll() is not None


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_actor_spawn_then_immediate_shutdown() -> None:
    with ExitStack() as cleanup:
        job = ProcessJob({"hosts": 1})
        cleanup.callback(job.kill)
        host = job.state(cached_path=None).hosts
        proc_mesh = host.spawn_procs(name="test")
        await proc_mesh.initialized
        logging_manager = proc_mesh._logging_manager
        assert logging_manager._logging_mesh_client is not None

        flush_started = False
        host_flush_called = False
        new_flush_handle = logging_manager._new_flush_handle
        flush_async = logging_manager.flush_async

        def record_flush_start() -> Handle[None]:
            # The returned Handle's flush is already running.
            nonlocal flush_started
            flush_started = True
            return new_flush_handle()

        async def record_flush_async() -> None:
            nonlocal host_flush_called
            host_flush_called = True
            await flush_async()

        # Constructing the first TestActor on a proc imports this module, pytest
        # included, on the actor's event loop. A stop during that import queues
        # the actor's cleanup behind it, where the cleanup deadline can expire
        # and fail the actor (T290443991). This test is about draining pending
        # spawns, so construct one first and start the actor below from the
        # cached module.
        warm_mesh = proc_mesh.spawn("warm_actor", TestActor, 0)
        assert await warm_mesh.get_value.call_one() == 0

        # spawn actor but do NOT await initialized — immediately shutdown
        actor_mesh = proc_mesh.spawn("test_actor", TestActor, 42)
        drain_order: list[str] = []
        proc_mesh._pending_actor_spawns.extend(
            [
                cast(
                    ActorMesh,
                    _PendingActorProbe(
                        "failed",
                        drain_order,
                        RuntimeError("pending actor initialization failed"),
                    ),
                ),
                cast(
                    ActorMesh,
                    _PendingActorProbe("succeeded", drain_order),
                ),
            ]
        )

        with (
            patch.object(logging_manager, "_new_flush_handle", record_flush_start),
            patch.object(
                logging_manager,
                "flush_async",
                record_flush_async,
            ),
        ):
            shutdown_result = await host.shutdown()
            cleanup.pop_all()
            assert shutdown_result is None

            assert host_flush_called

        assert flush_started
        assert drain_order == ["failed", "succeeded"]
        assert await actor_mesh.initialized is None
        assert proc_mesh._pending_actor_spawns == []


@pytest.mark.timeout(60)
def test_proc_mesh_spawn_callback() -> None:
    """Test that registered callbacks are invoked when a ProcMesh is spawned."""
    spawned_meshes: list[ProcMesh] = []

    def callback(pm: ProcMesh) -> None:
        spawned_meshes.append(pm)

    register_proc_mesh_spawn_callback(callback)
    try:
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            host = state.hosts
            proc_mesh = host.spawn_procs(name="test_proc")

            assert len(spawned_meshes) == 1
            assert spawned_meshes[0] is proc_mesh
    finally:
        unregister_proc_mesh_spawn_callback(callback)


@pytest.mark.timeout(60)
def test_proc_mesh_spawn_callback_multiple() -> None:
    """Test that multiple callbacks are all invoked."""
    callback1_meshes: list[ProcMesh] = []
    callback2_meshes: list[ProcMesh] = []

    def callback1(pm: ProcMesh) -> None:
        callback1_meshes.append(pm)

    def callback2(pm: ProcMesh) -> None:
        callback2_meshes.append(pm)

    register_proc_mesh_spawn_callback(callback1)
    register_proc_mesh_spawn_callback(callback2)
    try:
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            host = state.hosts
            proc_mesh = host.spawn_procs(name="test_proc")

            assert len(callback1_meshes) == 1
            assert len(callback2_meshes) == 1
            assert callback1_meshes[0] is proc_mesh
            assert callback2_meshes[0] is proc_mesh
    finally:
        unregister_proc_mesh_spawn_callback(callback1)
        unregister_proc_mesh_spawn_callback(callback2)


@pytest.mark.timeout(60)
def test_proc_mesh_spawn_callback_unregister() -> None:
    """Test that unregistered callbacks are not invoked."""
    spawned_meshes: list[ProcMesh] = []

    def callback(pm: ProcMesh) -> None:
        spawned_meshes.append(pm)

    register_proc_mesh_spawn_callback(callback)
    unregister_proc_mesh_spawn_callback(callback)

    with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
        host = state.hosts
        host.spawn_procs(name="test_proc")

        assert len(spawned_meshes) == 0
