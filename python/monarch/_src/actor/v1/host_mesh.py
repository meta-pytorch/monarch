# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Callable, Dict, Optional, Tuple

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Region
from monarch._rust_bindings.monarch_hyperactor.v1.host_mesh import (
    BootstrapCommand,
    HostMesh as HyHostMesh,
)
from monarch._rust_bindings.monarch_hyperactor.v1.proc_mesh import (
    ProcMesh as HyProcMesh,
)

from monarch._src.actor.actor_mesh import context
from monarch._src.actor.allocator import (
    AllocateMixin,
    AllocHandle,
    LocalAllocator,
    ProcessAllocator,
)
from monarch._src.actor.proc_mesh import _get_bootstrap_args, ProcMesh as ProcMeshV0
from monarch._src.actor.shape import MeshTrait, NDSlice, Shape
from monarch._src.actor.v1.proc_mesh import _get_controller_controller, ProcMesh


def _bootstrap_cmd() -> BootstrapCommand:
    cmd, args, bootstrap_env = _get_bootstrap_args()
    return BootstrapCommand(
        cmd,
        None,
        args if args else [],
        bootstrap_env,
    )


def this_host() -> "HostMesh":
    """
    The current machine.

    This is just shorthand for looking it up via the context
    """
    proc = this_proc()
    if proc.host_mesh.is_fake_in_process:
        return create_local_host_mesh("root_host")
    host_mesh = proc.host_mesh
    assert isinstance(host_mesh, HostMesh), "expected v1 HostMesh, got v0 HostMesh"
    return host_mesh


def this_proc() -> "ProcMesh":
    """
    The current singleton process that this specific actor is
    running on
    """
    proc = context().actor_instance.proc
    if isinstance(proc, ProcMeshV0):
        # This case can happen in the client process.
        return _get_controller_controller()[0]
    return proc


def create_local_host_mesh(name: str, extent: Extent | None = None) -> "HostMesh":
    """
    Create a local host mesh for the current machine.

    Args:
        name: The name of the host mesh.
        extent: Optional extent describing the shape of the host mesh.
                If not provided, `Extent(labels=["hosts"], sizes=[1])` is used.
                Other extents allow for local host meshes where each "host" is
                actually just a local process.

    Returns:
        HostMesh: A single-host mesh configured for local process allocation.
    """
    return HostMesh.allocate_nonblocking(
        name,
        extent if extent is not None else Extent(labels=["hosts"], sizes=[1]),
        ProcessAllocator(*_get_bootstrap_args()),
        bootstrap_cmd=_bootstrap_cmd(),
    )


class HostMesh(MeshTrait):
    """
    HostMesh represents a collection of compute hosts that can be used to spawn
    processes and actors.
    """

    def __init__(
        self,
        hy_host_mesh: Shared[HyHostMesh],
        region: Region,
        stream_logs: bool,
        is_fake_in_process: bool,
    ) -> None:
        self._hy_host_mesh = hy_host_mesh
        self._region = region
        self._stream_logs = stream_logs
        self._is_fake_in_process = is_fake_in_process

    @classmethod
    def allocate_nonblocking(
        cls,
        name: str,
        extent: Extent,
        allocator: AllocateMixin,
        alloc_constraints: Optional[AllocConstraints] = None,
        bootstrap_cmd: Optional[BootstrapCommand] = None,
    ) -> "HostMesh":
        spec = AllocSpec(alloc_constraints or AllocConstraints(), **extent)
        alloc: AllocHandle = allocator.allocate(spec)

        async def task() -> HyHostMesh:
            return await HyHostMesh.allocate_nonblocking(
                context().actor_instance._as_rust(),
                await alloc._hy_alloc,
                name,
                bootstrap_cmd,
            )

        return cls(
            PythonTask.from_coroutine(task()).spawn(),
            extent.region,
            alloc.stream_logs,
            isinstance(allocator, LocalAllocator),
        )

    def spawn_procs(
        self,
        per_host: Dict[str, int] | None = None,
        setup: Callable[[], None] | None = None,
        name: str | None = None,
    ) -> "ProcMesh":
        if not per_host:
            per_host = {}

        if not name:
            name = ""

        return self._spawn_nonblocking(
            name, Extent(list(per_host.keys()), list(per_host.values())), setup, True
        )

    def _spawn_nonblocking(
        self,
        name: str,
        per_host: Extent,
        setup: Callable[[], None] | None,
        _attach_controller_controller: bool,
    ) -> "ProcMesh":
        if set(per_host.labels) & set(self._labels):
            # The rust side will catch this too, but this lets us fail fast
            raise ValueError(
                f"per_host labels {per_host.labels} overlap with host labels {self._labels}"
            )

        async def task() -> HyProcMesh:
            hy_host_mesh = await self._hy_host_mesh
            return await hy_host_mesh.spawn_nonblocking(
                context().actor_instance._as_rust(), name, per_host
            )

        return ProcMesh.from_host_mesh(
            self,
            PythonTask.from_coroutine(task()).spawn(),
            Extent(
                self._labels + tuple(per_host.labels),
                self.region.slice().sizes + list(per_host.sizes),
            ).region,
            setup,
            _attach_controller_controller,
        )

    @property
    def _ndslice(self) -> NDSlice:
        return self.region.slice()

    @property
    def _labels(self) -> Tuple[str, ...]:
        return tuple(self.region.labels)

    def _new_with_shape(self, shape: Shape) -> "HostMesh":
        if shape.region == self._region:
            return self

        async def task() -> HyHostMesh:
            hy_host_mesh = await self._hy_host_mesh
            return hy_host_mesh.sliced(shape.region)

        return HostMesh(
            PythonTask.from_coroutine(task()).spawn(),
            shape.region,
            self.stream_logs,
            self.is_fake_in_process,
        )

    @property
    def region(self) -> Region:
        return self._region

    @property
    def stream_logs(self) -> bool:
        return self._stream_logs

    @classmethod
    def _from_initialized_hy_host_mesh(
        cls,
        hy_host_mesh: HyHostMesh,
        region: Region,
        stream_logs: bool,
        is_fake_in_process: bool,
    ) -> "HostMesh":
        async def task() -> HyHostMesh:
            return hy_host_mesh

        return HostMesh(
            PythonTask.from_coroutine(task()).spawn(),
            region,
            stream_logs,
            is_fake_in_process,
        )

    def __reduce_ex__(self, protocol: ...) -> Tuple[Any, Tuple[Any, ...]]:
        return HostMesh._from_initialized_hy_host_mesh, (
            self._hy_host_mesh.block_on(),
            self._region,
            self.stream_logs,
            self.is_fake_in_process,
        )

    @property
    def is_fake_in_process(self) -> bool:
        return self._is_fake_in_process


def fake_in_process_host(name: str) -> "HostMesh":
    """
    Create a host mesh for testing and development using a local allocator.

    Args:
        name: The name of the host mesh.

    Returns:
        HostMesh: A host mesh configured with local allocation for in-process use.
    """
    return HostMesh.allocate_nonblocking(
        name,
        Extent(labels=["hosts"], sizes=[1]),
        LocalAllocator(),
        bootstrap_cmd=_bootstrap_cmd(),
    )
