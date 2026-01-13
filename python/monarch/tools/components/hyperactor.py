# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import getpass
from typing import Optional

from monarch.tools import mesh_spec
from monarch.tools.config import NOT_SET
from monarch.tools.mesh_spec import mesh_spec_from_str
from torchx import specs

_DEFAULT_MESHES = ["mesh_0:1:gpu.small"]

_USER: str = getpass.getuser()

DEFAULT_NAME: str = f"monarch-{_USER}"


__version__ = "latest"  # TODO get version from monarch.__version_


def host_mesh(
    image: str = f"ghcr.io/meta-pytorch/monarch:{__version__}",  # TODO docker needs to be built and pushed to ghcr
    meshes: list[str] = _DEFAULT_MESHES,
    env: Optional[dict[str, str]] = None,
    port: int = mesh_spec.DEFAULT_REMOTE_ALLOCATOR_PORT,
    program: str = "monarch_bootstrap",  # installed with monarch wheel (as console script)
) -> specs.AppDef:
    """
    Args:
        name: the name of the monarch server job
        image: docker image to run the job on, for slurm, image is the dir the job is run from
        meshes: list of mesh specs of the form "{name}:{num_hosts}:{host_type}"
        env: environment variables to be passed to the main command (e.g. ENV1=v1,ENV2=v2,ENV3=v3)
        port: the port that the remote process allocator runs on (must be reachable from the client)
        program: path to the binary that the remote process allocator spawns on an allocation request
    """

    appdef = specs.AppDef(name=NOT_SET)

    for mesh in [mesh_spec_from_str(mesh) for mesh in meshes]:
        mesh_role = specs.Role(
            name=mesh.name,
            image=image,
            entrypoint="process_allocator",  # run "cargo install monarch_hyperactor" to get this binary
            args=[
                f"--port={port}",
                f"--program={program}",
            ],
            num_replicas=mesh.num_hosts,
            resource=specs.resource(h=mesh.host_type),
            env=env or {},
            port_map={"mesh": port},
        )
        appdef.roles.append(mesh_role)

    return appdef
