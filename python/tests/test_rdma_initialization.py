# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Tests for RDMA manager initialization and its failure surfaces.

These tests cover the legacy fault-hook path, the owner-backed binding from
local and remote actors, and configuration errors around backend fallback.
"""

import sys

import pytest

if sys.platform != "linux":
    pytest.skip("linux-only", allow_module_level=True)

from isolate_in_subprocess import isolate_in_subprocess
from monarch.actor import Actor, endpoint
from monarch.config import configured
from monarch.rdma import is_ibverbs_available


needs_no_rdma = pytest.mark.skipif(
    is_ibverbs_available(),
    reason="RDMA is available, test only runs on systems without RDMA support",
)


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


async def _rdma_manager_init_fault(**config: object) -> str:
    import asyncio

    import monarch.actor
    from monarch._rust_bindings.rdma import _RdmaManager
    from monarch._src.actor.actor_mesh import context
    from monarch._src.actor.future import Future
    from monarch.actor import this_host

    faults = []
    faulted = asyncio.Event()

    def fault_hook(failure):
        faults.append(failure)
        faulted.set()

    monarch.actor.unhandled_fault_hook = fault_hook

    with configured(**config):
        proc_mesh = this_host().spawn_procs(per_host={"cpus": 1})
        await Future(
            coro=_RdmaManager.create_rdma_manager_nonblocking(
                await Future(coro=proc_mesh._proc_mesh.task()),
                context().actor_instance,
            )
        )
        await asyncio.wait_for(faulted.wait(), timeout=15.0)

    assert faults, "Expected a supervision fault, got none"
    return "\n".join(str(failure) for failure in faults)


@needs_no_rdma
@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_rdma_manager_creation_fails_when_unsupported() -> None:
    """RdmaManagerActor creation surfaces a supervision fault when RDMA is unsupported.

    This test only runs on systems where RDMA is not available. If RDMA is
    available, the test is skipped, since we cannot exercise the unsupported
    path.

    With TCP fallback disabled and no NIC backend present, the actor spawns
    successfully but its init fails. The failure is delivered to
    ``unhandled_fault_hook`` as a supervision fault, not raised as an exception
    from the creation call.

    We don't mock here because the failure originates from a cross-language call
    chain (Python -> Rust -> C ibverbs library); a Python mock cannot intercept
    the native device probe.
    """
    failure_message = await _rdma_manager_init_fault(rdma_allow_tcp_fallback=False)
    assert "no RDMA backend available" in failure_message, (
        f"Expected specific failure message not found. Actual fault: {failure_message}"
    )


@pytest.mark.timeout(60)
@isolate_in_subprocess
async def test_malformed_rdma_ibverbs_target_fails_manager_creation() -> None:
    failure_message = await _rdma_manager_init_fault(
        rdma_allow_tcp_fallback=True,
        rdma_ibverbs_target="mlx5_0",  # Missing the required `nic:` target kind.
    )
    assert "RDMA_IBVERBS_TARGET" in failure_message, (
        f"Expected target configuration error not found. Actual fault: {failure_message}"
    )


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
    from monarch._rust_bindings.monarch_hyperactor.pytokio import Handle

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
async def test_ensure_init_succeeds_from_nested_remote_actor() -> None:
    """A nested remote child retains the capability across a second native
    actor-environment hop and reaches the same root-owned service (RMB-3)."""
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
