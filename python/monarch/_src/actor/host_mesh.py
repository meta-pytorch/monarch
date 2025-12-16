# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import warnings
from math import prod

from typing import Callable, Dict, Optional, Tuple, TYPE_CHECKING

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Slice

from monarch._src.actor.actor_mesh import context
from monarch._src.actor.allocator import AllocateMixin, AllocHandle, LocalAllocator
from monarch._src.actor.proc_mesh import (
    _get_bootstrap_args,
    ProcessAllocator,
    ProcMeshV0,
)
from monarch._src.actor.shape import MeshTrait, NDSlice, Shape
from monarch._src.actor.v1 import enabled as v1_enabled
from monarch._src.actor.v1.host_mesh import (
    _bootstrap_cmd,  # noqa: F401
    create_local_host_mesh as create_local_host_mesh_v1,
    host_mesh_from_alloc as host_mesh_from_alloc_v1,
    HostMesh as HostMeshV1,
    hosts_from_config as hosts_from_config_v1,
    this_host as this_host_v1,
    this_proc as this_proc_v1,
)
from monarch.tools.config.workspace import Workspace


def this_host_v0() -> "HostMeshV0":
    """
    The current machine.

    This is just shorthand for looking it up via the context
    """
    host_mesh = context().actor_instance.proc.host_mesh
    assert isinstance(host_mesh, HostMeshV0), "expected v0 HostMesh, got v1 HostMesh"
    return host_mesh


def this_proc_v0() -> "ProcMeshV0":
    """
    The current singleton process that this specific actor is
    running on
    """
    proc = context().actor_instance.proc
    assert isinstance(proc, ProcMeshV0), "expected v1 ProcMesh, got v0 ProcMesh"
    return proc


def create_local_host_mesh_v0() -> "HostMeshV0":
    """
    Create a local host mesh for the current machine.

    Returns:
        HostMesh: A single-host mesh configured for local process allocation.
    """
    cmd, args, env = _get_bootstrap_args()
    return HostMeshV0(Shape.unity(), ProcessAllocator(cmd, args, env))


class HostMeshV0(MeshTrait):
    """
    HostMesh represents a collection of compute hosts that can be used to spawn
    processes and actors. The class requires you to provide your AllocateMixin that
    interfaces with the underlying resource allocator of your choice.
    """

    def __init__(
        self,
        shape: Shape,
        allocator: AllocateMixin,
        alloc_constraints: Optional[AllocConstraints] = None,
    ) -> None:
        warnings.warn(
            (
                "DEPRECATION WARNING: using a deprecated version of HostMesh. This is going be removed imminently. "
                "Make sure you aren't running with `MONARCH_V0_WORKAROUND_DO_NOT_USE=1` to get the new version of "
                "HostMesh."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        self._allocator = allocator
        self._alloc_constraints = alloc_constraints
        self._shape = shape
        self._spawned = 0

    def _alloc(self, hosts: int, gpus: int) -> "AllocHandle":
        spec: AllocSpec = AllocSpec(
            self._alloc_constraints or AllocConstraints(), hosts=hosts, gpus=gpus
        )
        return self._allocator.allocate(spec)

    def spawn_procs(
        self,
        per_host: Optional[Dict[str, int]] = None,
        bootstrap: Optional[Callable[[], None]] = None,
    ) -> "ProcMeshV0":
        """
        Start new processes on this host mesh. By default this starts one proc
        on each host in the mesh. Additional procs can be started using `per_host` to
        specify the local shape, e.g.`
            per_host = {'gpus': 8}
        Will create a proc mesh with an additional 'gpus' dimension.

        `bootstrap` is a function that will be run at startup on each proc and can be used to e.g.
        configure CUDA or NCCL. We guarantee that CUDA has not been initialized before boostrap is called.

        TODO: For now, a new allocator is created for every new ProcMesh.
        """
        if per_host is None:
            per_host = {}
        if self._spawned > 0 and len(self._ndslice) > 1:
            warnings.warn(
                "spawning multiple procs on the same host mesh is kinda fake at the moment, there is no guarentee that the two different spawns will be on shared hosts",
                stacklevel=2,
            )
        self._spawned += 1
        hosts = len(self._ndslice)
        flat_per_host = prod(per_host.values())
        alloc_handle = self._alloc(hosts, flat_per_host)

        new_extent = dict(zip(self._labels, self._ndslice.sizes))

        conflicting_keys = set(per_host.keys()) & set(new_extent.keys())
        if conflicting_keys:
            raise ValueError(
                f"host mesh already has dims {', '.join(sorted(conflicting_keys))}"
            )

        new_extent.update(per_host)
        return ProcMeshV0.from_alloc(alloc_handle.reshape(new_extent), bootstrap)

    @property
    def _ndslice(self) -> NDSlice:
        return self._shape.ndslice

    @property
    def _labels(self) -> Tuple[str, ...]:
        return tuple(self._shape.labels)

    def _new_with_shape(self, shape: Shape) -> "HostMeshV0":
        warnings.warn(
            "Slicing a host mesh is kinda fake at the moment, there is no guarentee that procs in the slice will end up on the corresponding hosts",
            stacklevel=2,
        )
        return HostMeshV0(
            Shape(self._labels, NDSlice.new_row_major(self._ndslice.sizes)),
            self._allocator,
        )

    async def sync_workspace(
        self,
        workspace: Workspace,
        conda: bool = False,
        auto_reload: bool = False,
    ) -> None:
        """
        Sync local code changes to the remote hosts.

        Args:
            workspace: The workspace to sync.
            conda: If True, also sync the currently activated conda env.
            auto_reload: If True, automatically reload the workspace on changes.
        """
        raise NotImplementedError("sync_workspace is not implemented for v0 HostMesh")


def fake_in_process_host_v0() -> "HostMeshV0":
    """
    Create a host mesh for testing and development using a local allocator.

    Returns:
        HostMesh: A host mesh configured with local allocation for in-process use.
    """
    return HostMeshV0(Shape.unity(), LocalAllocator())


def hosts_from_config_v0(name: str) -> HostMeshV0:
    """
    Get the host mesh 'name' from the monarch configuration for the project.

    This config can be modified so that the same code can create meshes from scheduler sources,
    and different sizes etc.

    WARNING: This function is a standin so that our getting_started example code works. The real implementation
    needs an RFC design.
    """

    shape = Shape(["hosts"], NDSlice.new_row_major([2]))
    return HostMeshV0(shape, ProcessAllocator(*_get_bootstrap_args()))


def host_mesh_from_alloc_v0(
    name: str, extent: Extent, allocator: AllocateMixin, constraints: AllocConstraints
) -> HostMeshV0:
    return HostMeshV0(
        Shape(extent.labels, Slice.new_row_major(extent.sizes)),
        allocator,
        constraints,
    )


if v1_enabled or TYPE_CHECKING:
    this_host = this_host_v1
    this_proc = this_proc_v1
    create_local_host_mesh = create_local_host_mesh_v1
    fake_in_process_host = this_host_v1
    HostMesh = HostMeshV1
    hosts_from_config = hosts_from_config_v1
    host_mesh_from_alloc = host_mesh_from_alloc_v1
else:
    this_host = this_host_v0
    this_proc = this_proc_v0
    create_local_host_mesh = create_local_host_mesh_v0
    fake_in_process_host = fake_in_process_host_v0
    HostMesh = HostMeshV0
    hosts_from_config = hosts_from_config_v0
    host_mesh_from_alloc = host_mesh_from_alloc_v0
