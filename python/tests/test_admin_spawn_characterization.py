# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Admin creation starts at the call; observation never owns its native work."""

import asyncio
from unittest.mock import patch

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.handle import _after, _new_handle_pair
from monarch._rust_bindings.monarch_hyperactor.host_mesh import PyMeshAdminRef
from monarch._src.actor import host_mesh as host_mesh_module
from monarch._src.actor.actor_mesh import shutdown_context
from monarch._src.actor.host_mesh import this_host


@pytest.mark.parametrize("observation", ["cancel", "timeout", "drop"])
@pytest.mark.timeout(120)
@isolate_in_subprocess
async def test_spawn_admin_starts_at_call_and_survives_observer_loss(
    observation: str,
) -> None:
    gate, release = _new_handle_pair()
    real_spawn = host_mesh_module._hy_spawn_admin
    calls = []

    def gated_spawn(hosts, instance, admin_addr, telemetry_url):
        calls.append(admin_addr)
        return real_spawn(
            [_after([gate], host) for host in hosts],
            instance,
            admin_addr,
            telemetry_url,
        )

    result = None
    try:
        host = this_host()
        with patch.object(host_mesh_module, "_hy_spawn_admin", gated_spawn):
            with pytest.raises(
                ValueError, match="_spawn_admin requires at least one HostMesh"
            ):
                host_mesh_module._spawn_admin([])
            assert calls == []
            with pytest.raises(Exception, match="invalid admin_addr"):
                host_mesh_module._spawn_admin([host], admin_addr="not-an-address")
            assert calls == ["not-an-address"]

            result = host_mesh_module._spawn_admin([host], admin_addr="[::]:0")
            assert calls == ["not-an-address", "[::]:0"]
            handle = result._as_handle()
            assert handle.poll() is None
            if observation == "timeout":
                with pytest.warns(UserWarning):
                    with pytest.raises(TimeoutError):
                        result.get(timeout=0.01)
            elif observation == "cancel":
                observer = result.as_asyncio()
                assert observer.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await observer
            else:
                del result
                result = None

        release.set_result(None)
        admin_url, admin_ref = await asyncio.wait_for(handle.as_asyncio(), timeout=30)
        assert admin_url
        assert isinstance(admin_ref, PyMeshAdminRef)
        repeated_url, repeated_ref = await handle
        assert repeated_url == admin_url
        assert repeated_ref is admin_ref
    finally:
        try:
            release.set_result(None)
        except RuntimeError:
            pass
        await asyncio.wait_for(shutdown_context().as_asyncio(), timeout=30)
