# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import getpass

import json
import logging
import os
import pathlib

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._src.actor.allocator import RemoteAllocator, TorchXRemoteAllocInitializer

from monarch.actor import ProcMesh
from monarch.tools import commands
from monarch.tools.components import hyperactor
from monarch.tools.config import Config


USER = getpass.getuser()
HOME = pathlib.Path().home()
CWD = os.getcwd()
DEACTIVATE = None

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger: logging.Logger = logging.getLogger(__name__)

# Replace these constants with values specific from your cluster
HOST_TYPE = "aws_g5.12xlarge"
# TODO: Fix torchx so this should not be required from the user.
HOST_MEMORY = 186777


async def get_appdef(num_hosts):
    # similar to Docker image; should contain a conda env in the $img_root/conda/ directory
    # when config.workspace is not None, an ephemeral fbpkg version is created
    # that conda-packs the currently active local conda env AND the directory specified by workspace
    image = "monarch_default_workspace:latest"

    appdef = hyperactor.host_mesh(
        image=image,
        # TODO: For some reason gpu.medium doens't work here
        meshes=[f"mesh0:{num_hosts}:{HOST_TYPE}"],  # mesh_name:num_hosts:host_type
    )
    return appdef


async def get_server_info(appdef):
    jobname = f"monarch-{USER}"

    # TODO: Register this so we don't have to do this every time
    for role in appdef.roles:
        role.resource.memMB = HOST_MEMORY

    config = Config(
        scheduler="slurm",
        appdef=appdef,
        workspace=str(CWD),  # or None to disable building ephemeral,
    )

    server_info = await commands.get_or_create(
        jobname,
        config,
        force_restart=False,
    )
    return server_info


async def create_proc_mesh(num_hosts, appdef, server_info):
    # TODO: why is gpus equal to -1 in server_info?

    num_gpus_per_host = appdef.roles[0].resource.gpu

    logger.info(
        "\n===== Server Info =====\n%s",
        json.dumps(server_info.to_json(), indent=2),
    )

    allocator = RemoteAllocator(
        world_id="foo",
        initializer=TorchXRemoteAllocInitializer(server_info.server_handle),
    )
    alloc = await allocator.allocate(
        AllocSpec(AllocConstraints(), hosts=num_hosts, gpus=num_gpus_per_host)
    )

    proc_mesh = await ProcMesh.from_alloc(alloc)
    return proc_mesh
