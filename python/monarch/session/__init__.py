# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""Monarch Sessions for resource management on multiple hosts."""

import json
import logging

from monarch._src.actor.allocator import RemoteAllocator, TorchXRemoteAllocInitializer
from monarch._src.actor.future import Future
from monarch._src.actor.shape import NDSlice, Shape
from monarch.actor import HostMesh
from monarch.tools import commands
from monarch.tools.config import Config
from monarch.tools.mesh_spec import ServerSpec

logger: logging.Logger = logging.getLogger(__name__)


class MonarchSession:
    """A Monarch session provides high-level APIs for creating and managing Monarch resources.

    A MonarchSession is obtained by calling `connect()` and represents an active connection
    to a monarch cluster. The session provides APIs to create resources like HostMesh.

    Usage:

    .. code-block:: python

        from monarch.session import connect
        from monarch.tools.config import defaults

        config = defaults.config(scheduler="slurm")
        session = await connect("my_session", config)

        # Create a mesh of 4 hosts
        host_mesh = session.host_mesh(num_hosts=4)

        # Spawn procs on the Mesh


    Attributes:
        _alloc: The underlying RemoteAllocator that manages resource allocation

    """

    def __init__(self, allocator: RemoteAllocator):
        """Initialize a MonarchSession with the given allocator.

        Args:
            allocator: RemoteAllocator instance that handles resource allocation
        """
        self._alloc = allocator

    def host_mesh(self, num_hosts: int) -> HostMesh:
        """Create a HostMesh with the specified number of hosts.

        A HostMesh represents a collection of compute hosts that can be used
        for distributed workloads. Each host in the mesh can run multiple
        processes and is suitable for multi-host distributed training or
        computation tasks.

        Args:
            num_hosts: Number of hosts to include in the mesh

        Returns:
            HostMesh: A mesh containing the requested number of hosts

        Example:

        .. code-block:: python

            # Create a mesh with 8 hosts
            mesh = session.host_mesh(num_hosts=8)

            with mesh:
                # Each host can run distributed training processes
                pass
        """
        shape = Shape(["hosts"], NDSlice.new_row_major([num_hosts]))
        return HostMesh(
            shape=shape,
            allocator=self._alloc,
        )


def connect(name: str, config: Config, force_restart: bool = False) -> MonarchSession:
    """Connect to a Monarch server and return a session.

    This establishes a connection to a monarch server by finding an existing
    server with the given name or creating a new one according to the
    provided config. Once connected, it returns a MonarchSession that can
    be used to create and manage distributed compute meshes.

    Usage:

    .. code-block:: python

        from monarch.session import connect
        from monarch.tools.config import defaults

        # Connect to a server on SLURM
        config = defaults.config(scheduler="slurm")
        config.appdef = defaults.component_fn(config.scheduler)()

        session = await connect("my_training_job", config)
        host_mesh = session.host_mesh(num_hosts=4)
        procs = host_mesh.spawn_procs(per_host={"procs": 8})
        # procs is shape {"hosts": 4, "procs": 8}

    Args:
        name: Unique identifier for the monarch server/job. This is used to
            find existing servers or as the job name when creating new ones.
        config: Configuration object containing scheduler settings, workspace
            configuration, and application definition for the server.
        force_restart: If True, restarts the server even if one already exists.

    Returns:
        MonarchSession: An active session connected to the monarch server that
            can be used to create distributed compute meshes.

    Raises:
        RuntimeError: If the server fails to start or becomes unreachable.
        AssertionError: If config.dryrun is True (dryrun not supported for connections).

    Note:
        This function is asynchronous and must be awaited. The underlying server
        creation and readiness checking may take some time depending on the
        scheduler and resource availability.
    """

    async def task() -> ServerSpec:
        return await commands.get_or_create(
            name=name,
            config=config,
            force_restart=force_restart,
        )

    # Allows this to run in both sync/async contexts
    server_info: ServerSpec = Future(coro=task()).get()
    logger.debug("\n=== Server Info ===%s", json.dumps(server_info.to_json(), indent=2))
    allocator = RemoteAllocator(
        world_id=name,
        initializer=TorchXRemoteAllocInitializer(server_info.server_handle),
    )
    return MonarchSession(allocator)
