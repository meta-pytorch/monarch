# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import List

from monarch._src.actor.shape import NDSlice
from monarch.common.client import Client
from monarch.common.device_mesh import DeviceMesh
from monarch.controller.backend import ProcessBackend
from monarch.controller.controller import Controller
from monarch_supervisor import Context, Host


def world_mesh(
    ctx: Context,
    hosts: List[Host],
    gpu_per_host: int,
    _processes=None,
) -> DeviceMesh:
    backend = ProcessBackend(ctx, hosts, gpu_per_host, _processes=_processes)
    client = Client(Controller(backend), backend.world_size, backend.gpu_per_host)
    return DeviceMesh(
        client,
        NDSlice(offset=0, sizes=[len(hosts), gpu_per_host], strides=[gpu_per_host, 1]),
        ("host", "gpu"),
    )
