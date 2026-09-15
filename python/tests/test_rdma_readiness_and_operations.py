# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Tests for RDMA initialization, readiness, and public operation boundaries.

These tests cover the owner-backed binding from local and remote actors,
production readiness caching and validation, public buffer operations, when
submit and drop take effect, and backend configuration errors.
"""

import asyncio
import gc
import re
import sys
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import cast

import pytest

if sys.platform != "linux":
    pytest.skip("linux-only", allow_module_level=True)

from isolate_in_subprocess import isolate_in_subprocess
from monarch._src.actor.proc_mesh import ProcMesh
from monarch.actor import Actor, endpoint
from monarch.config import configured
from monarch.rdma import RDMABuffer


_PUBLIC_PATH_INITIAL = b"rdma-public-path-a"
_PUBLIC_PATH_UPDATED = b"rdma-public-path-b"

_SUBMIT_INITIAL = b"rdma-submit-before"
_SUBMIT_UPDATED = b"rdma-submit-after!"
_SUBMIT_RESUBMITTED = b"rdma-submit-second"
_DROP_INITIAL = b"rdma-drop-initial!"


class _RdmaInitProbe(Actor):
    @endpoint
    async def ensure_here(self) -> None:
        from monarch._rust_bindings.rdma import _RdmaManager
        from monarch._src.actor.actor_mesh import context

        instance = context().actor_instance
        proc_mesh = instance.proc_mesh
        assert proc_mesh is not None
        handle = _RdmaManager.ensure_init_rdma_manager_nonblocking(
            proc_mesh._proc_mesh,
            instance,
        )
        assert await handle is None

    @endpoint
    async def ensure_from_nested_child(self) -> None:
        from monarch._src.actor.actor_mesh import context

        proc_mesh = context().actor_instance.proc_mesh
        assert proc_mesh is not None
        child = proc_mesh.spawn("nested_rdma_init_probe", _RdmaInitProbe)
        assert await child.ensure_here.call_one() is None

    @endpoint
    async def exercise_public_paths_with_malformed_target(self) -> tuple[str, str]:
        from monarch._rust_bindings.rdma import RdmaInitError
        from monarch.rdma import RDMAAction

        try:
            RDMABuffer(memoryview(bytearray(b"rdma")))
        except Exception as error:
            assert type(error) is RdmaInitError, (
                f"buffer creation must raise RdmaInitError exactly, got {type(error)}"
            )
            buffer_error = str(error)
        else:
            raise AssertionError(
                "public buffer creation unexpectedly ignored failed readiness"
            )

        try:
            await RDMAAction().submit()
        except Exception as error:
            assert type(error) is RdmaInitError, (
                f"submit must raise RdmaInitError exactly, got {type(error)}"
            )
            submit_error = str(error)
        else:
            raise AssertionError("public submit unexpectedly ignored failed readiness")

        return buffer_error, submit_error


class _RdmaPublicPathProbe(Actor):
    def __init__(self) -> None:
        self.data = bytearray(_PUBLIC_PATH_INITIAL)
        self.buffer: RDMABuffer | None = None

    @endpoint
    async def create_buffer(self) -> RDMABuffer:
        self.buffer = RDMABuffer(memoryview(self.data))
        return self.buffer

    @endpoint
    async def read_and_write(
        self,
        buffer: RDMABuffer,
    ) -> bytes:
        readback = bytearray(len(_PUBLIC_PATH_INITIAL))
        assert await buffer.read_into(memoryview(readback)) is None
        assert await buffer.write_from(memoryview(_PUBLIC_PATH_UPDATED)) is None
        return bytes(readback)

    @endpoint
    async def verify_and_drop(self) -> None:
        assert self.buffer is not None
        assert bytes(self.data) == _PUBLIC_PATH_UPDATED
        assert await self.buffer.drop() is None


class _RdmaDiscardedSubmitOwner(Actor):
    """Holds the target bytes so an independent read can observe them."""

    def __init__(self) -> None:
        self.data = bytearray(_SUBMIT_INITIAL)
        self.buffer: RDMABuffer | None = None

    @endpoint
    async def create_buffer(self) -> RDMABuffer:
        from monarch.rdma import RDMAAction

        # An empty action crosses after_ready but submits no transfer. Use it
        # as an explicit readiness barrier before registering the target.
        assert await RDMAAction().submit() is None
        self.buffer = RDMABuffer(memoryview(self.data))
        return self.buffer

    @endpoint
    async def owner_bytes(self) -> bytes:
        return bytes(self.data)

    @endpoint
    async def release(self) -> None:
        assert self.buffer is not None
        assert await self.buffer.drop() is None


class _RdmaDiscardedSubmitConsumer(Actor):
    """Builds one action, discards its observer, then reuses that action."""

    def __init__(self) -> None:
        self.source = bytearray(_SUBMIT_UPDATED)
        self.action = None

    @endpoint
    async def build_and_discard(self, buffer: RDMABuffer) -> None:
        from monarch.rdma import RDMAAction

        # Prime this proc's readiness with an empty action, which crosses
        # after_ready but submits nothing. The discarded nonempty action below
        # then isolates its own lock and transfer.
        assert await RDMAAction().submit() is None
        self.action = RDMAAction().write_remote(buffer, memoryview(self.source))
        discarded = self.action.submit()
        # The eager Handle owns the transfer independently of this observer.
        del discarded

    @endpoint
    async def submit_same_action(self) -> None:
        assert self.action is not None
        # The forced-TCP write copies every source chunk before sending it to
        # the owner. Observing the complete first payload therefore means this
        # buffer will not be read again by the discarded submit.
        self.source[:] = _SUBMIT_RESUBMITTED
        assert await self.action.submit() is None


class _RdmaDiscardedDropOwner(Actor):
    """Runs the drop witness from the owning actor.

    The release and every barrier request then originate from this actor and
    address the same `RdmaManagerActor` mailbox, which is what makes a later
    request/reply a handler-completion barrier for an earlier release.
    """

    @endpoint
    async def exercise(self) -> str:
        from monarch.config import get_global_config

        assert get_global_config()["enable_dest_actor_reordering_buffer"] is True, (
            "receive-side reordering must be pinned on; without it the barrier "
            "stops ordering the release against the later request"
        )

        data = bytearray(_DROP_INITIAL)
        first = RDMABuffer(memoryview(data))
        barrier: RDMABuffer | None = None
        try:
            discarded = first.drop()
            # The eager Handle owns the release independently of this observer.
            del discarded

            barrier = self._barrier(first, b"rdma-drop-barrier")

            error_message = None
            try:
                await first.read_into(memoryview(bytearray(len(_DROP_INITIAL))))
            except Exception as error:
                assert type(error) is Exception, (
                    f"a released buffer must fail as base Exception, got {type(error)}"
                )
                error_message = str(error)
            if error_message is None:
                raise AssertionError("reading a released registration must fail")
            return error_message
        finally:
            original_error = sys.exc_info()[1]
            if barrier is not None:
                try:
                    await asyncio.wait_for(barrier.drop(), timeout=10)
                except Exception as cleanup_error:
                    if original_error is None:
                        raise
                    if hasattr(original_error, "add_note"):
                        original_error.add_note(
                            f"barrier cleanup also failed: {cleanup_error!r}"
                        )

    def _barrier(self, first: RDMABuffer, payload: bytes) -> RDMABuffer:
        """A request/reply through the same manager, used to order the release."""
        barrier = RDMABuffer(memoryview(bytearray(payload)))
        assert barrier.owner == first.owner, (
            "the barrier must address the same manager as the release; "
            f"{barrier.owner} != {first.owner}"
        )
        return barrier


class _RdmaSettingsProbe(Actor):
    @endpoint
    async def retained_settings(self) -> tuple[bool, bool, str]:
        from monarch.config import get_global_config

        config = get_global_config()
        return (
            config["rdma_disable_ibverbs"],
            config["rdma_allow_tcp_fallback"],
            config["rdma_ibverbs_target"],
        )


class _RdmaDropReadinessOwner(_RdmaSettingsProbe):
    def __init__(self) -> None:
        self.data = bytearray(_DROP_INITIAL)
        self.buffer: RDMABuffer | None = None

    @endpoint
    async def create_buffer(self) -> RDMABuffer:
        self.buffer = RDMABuffer(memoryview(self.data))
        return self.buffer

    @endpoint
    async def still_usable(self) -> bool:
        assert self.buffer is not None
        readback = bytearray(len(_DROP_INITIAL))
        assert await self.buffer.read_into(memoryview(readback)) is None
        return bytes(readback) == _DROP_INITIAL

    @endpoint
    async def release(self) -> None:
        assert self.buffer is not None
        assert await self.buffer.drop() is None


class _RdmaDropReadinessConsumer(_RdmaSettingsProbe):
    @endpoint
    async def drop_and_report(self, buffer: RDMABuffer) -> str:
        from monarch._rust_bindings.rdma import RdmaInitError

        try:
            await buffer.drop()
        except Exception as error:
            assert type(error) is RdmaInitError, (
                f"a failed readiness must surface exactly, got {type(error)}"
            )
            return str(error)
        raise AssertionError("drop must not ignore a failed readiness")


class _RdmaSubmitTimeoutProbe(Actor):
    @endpoint
    async def submit_with_zero_timeout(self) -> str:
        from monarch.rdma import RDMAAction

        data = bytearray(_SUBMIT_INITIAL)
        source = bytearray(_SUBMIT_UPDATED)
        buffer = RDMABuffer(memoryview(data))
        try:
            # An empty action crosses the same readiness gate but returns
            # before the operation deadline. Use it as a separate readiness
            # control for the timeout failure below.
            assert await RDMAAction().submit() is None
            # Keep this action nonempty: native submit returns before checking
            # the deadline for an empty batch, so only a queued operation can
            # reach the zero-deadline guard this test pins.
            action = RDMAAction().write_remote(buffer, memoryview(source))
            try:
                await action.submit(timeout=0)
            except Exception as error:
                assert type(error) is Exception, (
                    f"a post-readiness failure must be base Exception, got {type(error)}"
                )
                return str(error)
            raise AssertionError("a zero submit deadline must fail after readiness")
        finally:
            await buffer.drop()


def test_manager_init_cache_reuses_handle_without_retaining_mesh(monkeypatch) -> None:
    from monarch._src.rdma import rdma as rdma_module

    class FakeMesh:
        _proc_mesh = object()

    class FakeManager:
        calls = 0

        @classmethod
        def ensure_init_rdma_manager_nonblocking(cls, shared, instance):
            cls.calls += 1
            return object()

    class FakeContext:
        actor_instance = object()

    monkeypatch.setattr(rdma_module, "_RdmaManager", FakeManager)
    monkeypatch.setattr(rdma_module, "context", FakeContext)

    with rdma_module._rdma_manager_init_cache_lock:
        initial_size = len(rdma_module._rdma_manager_init_cache)

    mesh = cast(ProcMesh, FakeMesh())
    other_mesh = cast(ProcMesh, FakeMesh())
    first = rdma_module._ensure_init_rdma_manager_on_mesh(mesh)
    second = rdma_module._ensure_init_rdma_manager_on_mesh(mesh)
    other = rdma_module._ensure_init_rdma_manager_on_mesh(other_mesh)
    assert second is first
    assert other is not first
    assert FakeManager.calls == 2

    mesh_ref = weakref.ref(mesh)
    other_mesh_ref = weakref.ref(other_mesh)
    del mesh
    del other_mesh
    gc.collect()
    assert mesh_ref() is None
    assert other_mesh_ref() is None
    with rdma_module._rdma_manager_init_cache_lock:
        assert len(rdma_module._rdma_manager_init_cache) == initial_size


def test_manager_init_cache_concurrent_first_miss_returns_one_handle(
    monkeypatch,
) -> None:
    from monarch._src.rdma import rdma as rdma_module

    class FakeMesh:
        _proc_mesh = object()

    first_creation_started = threading.Event()
    release_first_creation = threading.Event()

    class ContentionTrackingLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self.waiter_started = threading.Event()

        def __enter__(self):
            if self._lock.locked():
                self.waiter_started.set()
            self._lock.acquire()
            return self

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            self._lock.release()

    cache_lock = ContentionTrackingLock()

    class FakeManager:
        calls = 0

        @classmethod
        def ensure_init_rdma_manager_nonblocking(cls, shared, instance):
            cls.calls += 1
            first_creation_started.set()
            assert release_first_creation.wait(timeout=5)
            return object()

    class FakeContext:
        actor_instance = object()

    monkeypatch.setattr(rdma_module, "_RdmaManager", FakeManager)
    monkeypatch.setattr(rdma_module, "context", FakeContext)
    monkeypatch.setattr(rdma_module, "_rdma_manager_init_cache_lock", cache_lock)

    mesh = cast(ProcMesh, FakeMesh())
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(rdma_module._ensure_init_rdma_manager_on_mesh, mesh)
        assert first_creation_started.wait(timeout=5)
        second = pool.submit(rdma_module._ensure_init_rdma_manager_on_mesh, mesh)
        assert cache_lock.waiter_started.wait(timeout=5)
        release_first_creation.set()
        first_handle = first.result(timeout=10)
        second_handle = second.result(timeout=10)

    assert second_handle is first_handle
    assert FakeManager.calls == 1


def test_invalid_submit_timeout_does_not_start_init(monkeypatch) -> None:
    from monarch._src.rdma import rdma as rdma_module

    init_called = False

    def unexpected_init():
        nonlocal init_called
        init_called = True
        raise AssertionError("invalid timeout must be rejected before initialization")

    monkeypatch.setattr(rdma_module, "_ensure_init_rdma_manager", unexpected_init)
    with pytest.raises(OverflowError):
        rdma_module.RDMAAction().submit(timeout=-1)
    assert not init_called


def test_unavailable_backend_does_not_start_init(monkeypatch) -> None:
    from monarch._src.rdma import rdma as rdma_module

    init_called = False

    def unexpected_init():
        nonlocal init_called
        init_called = True
        raise AssertionError("an unavailable backend must be rejected before init")

    monkeypatch.setattr(rdma_module, "get_rdma_backend", lambda: "none")
    monkeypatch.setattr(rdma_module, "_ensure_init_rdma_manager", unexpected_init)

    with pytest.raises(RuntimeError, match="RDMA is not available"):
        rdma_module.RDMABuffer(memoryview(bytearray(b"rdma")))
    assert not init_called


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_public_submit_and_drop_resolve_to_none_after_tcp_init() -> None:
    from monarch.actor import Actor, endpoint, this_host
    from monarch.rdma import RDMAAction, RDMABuffer

    class CpuActor(Actor):
        @endpoint
        async def exercise(self) -> None:
            data = bytearray(b"rdma")
            buffer = RDMABuffer(memoryview(data))
            assert await RDMAAction().submit() is None
            assert await buffer.drop() is None

    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
    ):
        proc = this_host().spawn_procs(per_host={"cpus": 1})
        actor = proc.spawn("cpu_actor", CpuActor)
        assert await actor.exercise.call_one() is None
        await proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_public_paths_propagate_readiness_failure() -> None:
    from monarch.actor import this_host

    with configured(
        rdma_disable_ibverbs=False,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="mlx5_0",
    ):
        proc = this_host().spawn_procs(per_host={"cpus": 1})
        try:
            probe = proc.spawn("rdma_readiness_failure_probe", _RdmaInitProbe)
            (
                buffer_failure,
                submit_failure,
            ) = await probe.exercise_public_paths_with_malformed_target.call_one()
            assert "RDMA_IBVERBS_TARGET" in buffer_failure
            assert "RDMA_IBVERBS_TARGET" in submit_failure
        finally:
            await proc.stop()


@pytest.mark.timeout(90)
@isolate_in_subprocess
async def test_public_rdma_paths() -> None:
    from monarch.actor import this_host

    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
    ):
        producer_proc = this_host().spawn_procs(per_host={"cpus": 1})
        try:
            consumer_proc = this_host().spawn_procs(per_host={"cpus": 1})
            try:
                producer = producer_proc.spawn(
                    "rdma_public_path_producer", _RdmaPublicPathProbe
                )
                consumer = consumer_proc.spawn(
                    "rdma_public_path_consumer", _RdmaPublicPathProbe
                )

                buffer = await producer.create_buffer.call_one()
                readback = await consumer.read_and_write.call_one(buffer)
                assert await producer.verify_and_drop.call_one() is None

                assert readback == _PUBLIC_PATH_INITIAL
            finally:
                await consumer_proc.stop()
        finally:
            await producer_proc.stop()


@pytest.mark.timeout(90)
@isolate_in_subprocess
async def test_discarded_submit_commits_transfer_and_same_action_remains_runnable() -> (
    None
):
    """Discarding the observer does not cancel the submitted transfer."""
    from monarch.actor import this_host

    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
    ):
        # ProcMesh.spawn creates an actor across the whole mesh. Two one-proc
        # meshes keep the owner and consumer as single actors while still
        # exercising a cross-proc transfer, without a multi-proc mesh and
        # explicit rank slices obscuring the witness.
        owner_proc = this_host().spawn_procs(per_host={"cpus": 1})
        try:
            consumer_proc = this_host().spawn_procs(per_host={"cpus": 1})
            try:
                owner = owner_proc.spawn(
                    "rdma_discarded_submit_owner", _RdmaDiscardedSubmitOwner
                )
                consumer = consumer_proc.spawn(
                    "rdma_discarded_submit_consumer", _RdmaDiscardedSubmitConsumer
                )

                buffer = await owner.create_buffer.call_one()
                assert await owner.owner_bytes.call_one() == _SUBMIT_INITIAL
                assert await consumer.build_and_discard.call_one(buffer) is None

                async def wait_for_first_submit() -> bytes:
                    while (
                        first_submit := await owner.owner_bytes.call_one()
                    ) != _SUBMIT_UPDATED:
                        await asyncio.sleep(0.05)
                    return first_submit

                first_submit = await asyncio.wait_for(
                    wait_for_first_submit(), timeout=30
                )
                assert first_submit == _SUBMIT_UPDATED, (
                    "the first submit must finish after its Future is discarded"
                )

                assert await consumer.submit_same_action.call_one() is None
                assert await owner.owner_bytes.call_one() == _SUBMIT_RESUBMITTED, (
                    "the same action must run again with the updated source bytes"
                )

                assert await owner.release.call_one() is None
            finally:
                original_error = sys.exc_info()[1]
                try:
                    await asyncio.wait_for(consumer_proc.stop(), timeout=10)
                except Exception as cleanup_error:
                    if original_error is None:
                        raise
                    # BaseException.add_note() is unavailable on the Python 3.10
                    # OSS test lane. The original failure still takes precedence.
                    if hasattr(original_error, "add_note"):
                        original_error.add_note(
                            f"consumer cleanup also failed: {cleanup_error!r}"
                        )
        finally:
            original_error = sys.exc_info()[1]
            try:
                await asyncio.wait_for(owner_proc.stop(), timeout=10)
            except Exception as cleanup_error:
                if original_error is None:
                    raise
                # BaseException.add_note() is unavailable on the Python 3.10
                # OSS test lane. The original failure still takes precedence.
                if hasattr(original_error, "add_note"):
                    original_error.add_note(
                        f"owner cleanup also failed: {cleanup_error!r}"
                    )


@pytest.mark.timeout(90)
@isolate_in_subprocess
async def test_discarded_drop_commits_release() -> None:
    """Discarding the observer does not cancel the published release."""
    from monarch.actor import this_host

    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
        enable_dest_actor_reordering_buffer=True,
    ):
        proc = this_host().spawn_procs(per_host={"cpus": 1})
        try:
            owner = proc.spawn("rdma_discarded_drop_owner", _RdmaDiscardedDropOwner)
            message = await owner.exercise.call_one()
            assert message.startswith("RdmaAction.submit failed:"), (
                f"the released-buffer read must be a wrapped submit failure: {message}"
            )
            assert re.search(r"\(Tcp\) buffer \d+ not found(?:\n|$)", message), (
                "the cause must name the released registration, not any other "
                f"lookup failure: {message}"
            )
        finally:
            original_error = sys.exc_info()[1]
            try:
                await asyncio.wait_for(proc.stop(), timeout=10)
            except Exception as cleanup_error:
                if original_error is None:
                    raise
                if hasattr(original_error, "add_note"):
                    original_error.add_note(
                        f"proc cleanup also failed: {cleanup_error!r}"
                    )


@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_drop_propagates_readiness_failure_without_releasing_buffer() -> None:
    """A consumer whose readiness fails surfaces the exact RdmaInitError from
    drop, and the owner's registration survives because readiness is awaited
    before the release is published."""
    from monarch.actor import this_host

    owner_proc = None
    consumer_proc = None
    try:
        # This test makes drop() fail during readiness, before it can release
        # the owner's buffer. The owner needs a working TCP backend to register
        # the buffer and later prove that registration remains usable. The
        # consumer needs no available backend so its readiness fails before
        # drop() can publish ReleaseBuffer.
        #
        # configured() changes process-global state, so these roles cannot
        # share one scope. spawn_procs() snapshots that state asynchronously;
        # awaiting each child's initialization inside its scope ensures that
        # it retains the configuration required for its role.
        with configured(
            rdma_disable_ibverbs=True,
            rdma_allow_tcp_fallback=True,
            rdma_ibverbs_target="",
        ):
            owner_proc = this_host().spawn_procs(per_host={"cpus": 1})
            assert await owner_proc.initialized is True
        with configured(
            rdma_disable_ibverbs=True,
            rdma_allow_tcp_fallback=False,
            rdma_ibverbs_target="",
        ):
            consumer_proc = this_host().spawn_procs(per_host={"cpus": 1})
            assert await consumer_proc.initialized is True

        owner = owner_proc.spawn("rdma_drop_readiness_owner", _RdmaDropReadinessOwner)
        consumer = consumer_proc.spawn(
            "rdma_drop_readiness_consumer", _RdmaDropReadinessConsumer
        )

        # Both scopes have restored the parent's ambient configuration. Ask
        # each child what it retained so fixture drift fails here instead of
        # looking like a failure in drop().
        assert await owner.retained_settings.call_one() == (True, True, ""), (
            "the owner proc must retain the configuration active when it spawned"
        )
        assert await consumer.retained_settings.call_one() == (True, False, ""), (
            "the consumer proc must retain its own forced no-backend settings"
        )

        buffer = await owner.create_buffer.call_one()
        message = await consumer.drop_and_report.call_one(buffer)
        assert "no RDMA backend available" in message, (
            f"the readiness cause must survive the drop Future: {message}"
        )

        assert await owner.still_usable.call_one() is True, (
            "a readiness failure must occur before any release is published"
        )
        assert await owner.release.call_one() is None
    finally:
        try:
            if consumer_proc is not None:
                await consumer_proc.stop()
        finally:
            if owner_proc is not None:
                await owner_proc.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_submit_wraps_post_readiness_operation_failure() -> None:
    """A zero submit deadline fails inside the submitted operation, after
    readiness has already succeeded, and is wrapped as a base Exception."""
    from monarch.actor import this_host

    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
    ):
        proc = this_host().spawn_procs(per_host={"cpus": 1})
        try:
            probe = proc.spawn("rdma_submit_timeout_probe", _RdmaSubmitTimeoutProbe)
            message = await probe.submit_with_zero_timeout.call_one()
            assert message.startswith("RdmaAction.submit failed:"), (
                f"the operation failure must carry the submit prefix: {message}"
            )
            assert "tcp submit timed out" in message, (
                f"the cause must be the TCP submit deadline: {message}"
            )
        finally:
            await proc.stop()


# The tests below cover the owner-driven binding
# `_RdmaManager.ensure_init_rdma_manager_nonblocking`, which returns a `Handle`
# and surfaces a typed owner failure as a catchable `RdmaInitError`.


def _ensure_init_handle():
    """Spawn a one-proc mesh and call the Handle-returning binding, returning the
    `Handle` for the caller to await. Must be called inside a live event loop and
    the caller's `configured(...)` context (the owner reads config during init)."""
    from monarch._rust_bindings.rdma import _RdmaManager
    from monarch._src.actor.actor_mesh import context
    from monarch.actor import this_host

    proc_mesh = this_host().spawn_procs(per_host={"cpus": 1})
    return _RdmaManager.ensure_init_rdma_manager_nonblocking(
        proc_mesh._proc_mesh,
        context().actor_instance,
    )


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_ensure_init_returns_handle_that_resolves_over_tcp() -> None:
    """The binding resolves to None once the owner's full post-init() barrier
    completes, driven from the raw Shared[ProcMesh] with no Python-side mesh
    resolution (RMB-1, RMB-4). TCP fallback lets init() succeed without a NIC.
    Observation is non-consuming, so a second await also yields None (HDL-3)."""
    from monarch._rust_bindings.monarch_hyperactor.handle import Handle

    # Pin rdma_ibverbs_target so inherited configuration cannot preempt the
    # intended TCP-fallback branch.
    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
    ):
        handle = _ensure_init_handle()
        assert isinstance(handle, Handle)
        assert await handle is None
        assert await handle is None


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_ensure_init_succeeds_from_remote_actor() -> None:
    """A remote actor initializes through its inherited native client-root
    capability, without any driver-side RDMA initialization (RMB-3)."""
    from monarch.actor import this_host

    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
    ):
        proc_mesh = this_host().spawn_procs(per_host={"processes": 1})
        try:
            probe = proc_mesh.spawn("remote_rdma_init_probe", _RdmaInitProbe)
            assert await probe.ensure_here.call_one() is None
        finally:
            await proc_mesh.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_ensure_init_succeeds_from_nested_actor_on_remote_worker() -> None:
    """A child spawned inside a remote worker retains the capability across a
    second gspawn/ActorSpec environment hop on that worker (RMB-3)."""
    from monarch.actor import this_host

    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="",
    ):
        proc_mesh = this_host().spawn_procs(per_host={"processes": 1})
        try:
            parent = proc_mesh.spawn("parent_rdma_init_probe", _RdmaInitProbe)
            assert await parent.ensure_from_nested_child.call_one() is None
        finally:
            await proc_mesh.stop()


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_ensure_init_observes_its_supplied_shared() -> None:
    """The binding observes the exact Shared it is handed rather than an ambient
    proc mesh: a Shared resolving to a non-ProcMesh value fails the native
    downcast with a TypeError naming the expected ProcMesh type, distinct from
    RdmaInitError (RMB-2, RMB-5)."""
    from monarch._rust_bindings.monarch_hyperactor.pytokio import Shared
    from monarch._rust_bindings.rdma import _RdmaManager
    from monarch._src.actor.actor_mesh import context

    # No configured(...) context: the Shared resolves to a non-ProcMesh value, so the
    # downcast fails before any RDMA config would be read.
    handle = _RdmaManager.ensure_init_rdma_manager_nonblocking(
        Shared.from_value(object()),
        context().actor_instance,
    )
    with pytest.raises(TypeError) as excinfo:
        await handle
    assert "ProcMesh" in str(excinfo.value), (
        f"the downcast failure should name the expected ProcMesh type: {excinfo.value}"
    )


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_ensure_init_raises_rdma_init_error_when_no_backend() -> None:
    """A forced no-backend failure (ibverbs disabled, TCP fallback off) surfaces
    as a catchable native RdmaInitError carrying the cause. Deterministic on
    hosts with or without ibverbs (RMB-4, RMB-5)."""
    from monarch._rust_bindings.rdma import RdmaInitError

    # Pin rdma_ibverbs_target so inherited configuration cannot preempt the
    # forced no-backend branch.
    with configured(
        rdma_disable_ibverbs=True,
        rdma_allow_tcp_fallback=False,
        rdma_ibverbs_target="",
    ):
        handle = _ensure_init_handle()
        with pytest.raises(RdmaInitError) as excinfo:
            await handle
        assert "no RDMA backend available" in str(excinfo.value), (
            f"Actual error: {excinfo.value}"
        )


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_ensure_init_raises_rdma_init_error_on_malformed_target() -> None:
    """A malformed RDMA_IBVERBS_TARGET surfaces as a catchable native
    RdmaInitError, independent of hardware availability (RMB-5)."""
    from monarch._rust_bindings.rdma import RdmaInitError

    with configured(
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="mlx5_0",  # missing the required `nic:` target kind
    ):
        handle = _ensure_init_handle()
        with pytest.raises(RdmaInitError) as excinfo:
            await handle
        assert "RDMA_IBVERBS_TARGET" in str(excinfo.value), (
            f"Actual error: {excinfo.value}"
        )
