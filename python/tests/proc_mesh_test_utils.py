# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Shared pytest helpers for tests that spawn ProcMeshes.

Test modules opt in to ``stop_all_proc_meshes`` by importing it; pytest
auto-discovers the fixture in the importing module and runs it around every
test in that module.
"""

import pytest
from monarch.actor import context, ProcMesh
from monarch.config import configured


@pytest.fixture(autouse=True)
async def stop_all_proc_meshes():
    """Tear down ProcMeshes that the test left attached to the root client.

    Only ProcMeshes that were created from Python as direct children of the
    root client are stopped; ProcMeshes nested under other actors, or created
    on the Rust side, are not visible through ``actor_instance._children`` and
    must be cleaned up by their owners.

    The ``configured`` scope caps actor / mesh teardown so a misbehaving test
    cannot hang the suite.
    """
    with configured(stop_actor_timeout="5s", mesh_terminate_timeout="15s"):
        yield
        instance = context().actor_instance
        children = instance._children or []
        for child in children:
            if isinstance(child, ProcMesh):
                await child.stop()
        instance._children = None
