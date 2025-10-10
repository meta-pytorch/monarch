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

import pytest
from monarch.rdma import is_rdma_available


needs_no_rdma = pytest.mark.skipif(
    is_rdma_available(),
    reason="RDMA is available, test only runs on systems without RDMA support",
)


@needs_no_rdma
@pytest.mark.asyncio
async def test_rdma_manager_creation_fails_when_unsupported():
    """Test that RdmaManagerActor creation fails with correct error when RDMA is not supported.

    This test only runs on systems where RDMA is not available.
    If RDMA is available, the test is skipped since we cannot test the unsupported path.

    Note: We don't use mock here because the error originates from a cross-language call
    chain (Python → Rust → C ibverbs library). Python mocks cannot intercept the native
    ibverbs_supported() function that calls ibv_get_device_list() in the C library.
    """
    from monarch._rust_bindings.rdma import _RdmaManager
    from monarch._src.actor.actor_mesh import context
    from monarch._src.actor.future import Future
    from monarch.actor import this_host

    proc_mesh = this_host().spawn_procs(per_host={"cpus": 1})

    with pytest.raises(Exception) as exc_info:
        await Future(
            coro=_RdmaManager.create_rdma_manager_nonblocking(
                await Future(coro=proc_mesh._proc_mesh.task()),
                context().actor_instance,
            )
        )

    error_message = str(exc_info.value)
    assert (
        "Cannot create RdmaManagerActor because RDMA is not supported on this machine"
        in error_message
    ), f"Expected specific error message not found. Actual error: {error_message}"
