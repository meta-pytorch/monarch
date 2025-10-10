# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from contextlib import contextmanager
from typing import Generator
from unittest import TestCase

import pytest

import torch
from monarch import fetch_shard
from monarch._src.actor.v1 import enabled as v1_enabled
from monarch.common.device_mesh import DeviceMesh
from monarch.sim_mesh import sim_mesh


pytestmark = pytest.mark.skipif(
    not v1_enabled,
    reason="uses deprecated system actor, so might as well only run on v1",
)


@contextmanager
def local_sim_mesh(
    hosts: int = 1,
    # TODO: support multiple gpus in a mesh.
    gpu_per_host: int = 1,
    activate: bool = True,
) -> Generator[DeviceMesh, None, None]:
    dms = sim_mesh(n_meshes=1, hosts=hosts, gpus_per_host=gpu_per_host)
    dm = dms[0]
    try:
        if activate:
            with dm.activate():
                yield dm
        else:
            yield dm
        dm.exit()
    except Exception:
        dm.client._shutdown = True
        raise


# oss_skip: importlib not pulling resource correctly in git CI, needs to be revisited
@pytest.mark.oss_skip
class TestSimBackend(TestCase):
    def test_local_mesh_setup(self):
        with local_sim_mesh():
            t = torch.zeros(3, 4)
            t.add_(1)
            local_t = fetch_shard(t).result()
        # consider support specifying the return value in the mock worker.
        assert local_t is not None
