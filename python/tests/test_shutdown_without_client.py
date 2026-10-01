# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import concurrent.futures

import pytest
from monarch._src.actor import actor_mesh as actor_mesh_module
from monarch.actor import shutdown_context


# These tests have to be in their own file so that no other test creates a
# client in the same process first.
def test_shutdown_without_client_creates_no_client() -> None:
    assert actor_mesh_module._client_context.try_get() is None

    assert shutdown_context().get(timeout=30) is None

    assert actor_mesh_module._client_context.try_get() is None
    assert not actor_mesh_module._shutdown_done


def test_shutdown_waits_for_client_bootstrap_in_progress() -> None:
    client_context = actor_mesh_module._client_context
    assert client_context.try_get() is None

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Holding the lock stands in for another thread bootstrapping the client.
        with client_context._lock:
            result = executor.submit(lambda: shutdown_context().get(timeout=30))
            with pytest.raises(concurrent.futures.TimeoutError):
                result.result(timeout=0.5)
        assert result.result(timeout=30) is None

    assert client_context.try_get() is None
    assert not actor_mesh_module._shutdown_done
