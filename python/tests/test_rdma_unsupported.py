# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Tests for RDMA functionality when RDMA is not supported.

This file contains tests that are specifically designed to run on systems
where RDMA is NOT available. These tests verify error handling and fallback
behavior when RDMA support is missing.
"""

import sys

import pytest

if sys.platform != "linux":
    pytest.skip("linux-only", allow_module_level=True)

from isolate_in_subprocess import isolate_in_subprocess
from monarch.config import configured
from monarch.rdma import is_ibverbs_available


needs_no_rdma = pytest.mark.skipif(
    is_ibverbs_available(),
    reason="RDMA is available, test only runs on systems without RDMA support",
)


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

    with configured(rdma_allow_tcp_fallback=False):
        proc_mesh = this_host().spawn_procs(per_host={"cpus": 1})

        # Spawning succeeds; the RdmaManagerActor's init then fails because no
        # NIC backend is available and TCP fallback is disabled.
        await Future(
            coro=_RdmaManager.create_rdma_manager_nonblocking(
                await Future(coro=proc_mesh._proc_mesh.task()),
                context().actor_instance,
            )
        )

        # The init failure arrives asynchronously as a supervision fault.
        await asyncio.wait_for(faulted.wait(), timeout=15.0)

    assert len(faults) >= 1, "Expected a supervision fault, got none"
    failure_message = str(faults[0])
    assert "no RDMA backend available" in failure_message, (
        f"Expected specific failure message not found. Actual fault: {failure_message}"
    )
