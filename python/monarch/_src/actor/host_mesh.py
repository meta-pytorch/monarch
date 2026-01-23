# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Tuple

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.host_mesh import (
    BootstrapCommand,
    HostMesh as HyHostMesh,
)
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    PendingPickle,
    PythonTask,
    Shared,
)
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Region
from monarch._src.actor.actor_mesh import _Lazy, context
from monarch._src.actor.allocator import (
    AllocateMixin,
    AllocHandle,
    LocalAllocator,
    ProcessAllocator,
)
from monarch._src.actor.future import Future
from monarch._src.actor.pickle import is_pending_pickle_allowed
from monarch._src.actor.proc_mesh import _get_bootstrap_args, ProcMesh
from monarch._src.actor.shape import MeshTrait, NDSlice, Shape
from monarch.tools.config.workspace import Workspace


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
    return this_proc().host_mesh


def this_proc() -> "ProcMesh":
    """
    The current singleton process that this specific actor is
    running on
    """
    return context().actor_instance.proc


def create_local_host_mesh(
    extent: Optional[Extent] = None, env: Optional[Dict[str, str]] = None
) -> "HostMesh":
    """
    Create a local host mesh for the current machine.

    Args:
        name: The name of the host mesh.
        extent: Optional extent describing the shape of the host mesh.
                If not provided, `Extent(labels=[], sizes=[])` is used.
                Other extents allow for local host meshes where each "host" is
                actually just a local process.

    Returns:
        HostMesh: A single-host mesh configured for local process allocation.
    """

    cmd, args, bootstrap_env = _get_bootstrap_args()
    if env is not None:
        bootstrap_env.update(env)

    return HostMesh.allocate_nonblocking(
        "local_host",
        extent if extent is not None else Extent([], []),
        ProcessAllocator(cmd, args, bootstrap_env),
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
        _code_sync_proc_mesh: Optional["_Lazy[ProcMesh]"],
    ) -> None:
        self._hy_host_mesh = hy_host_mesh
        self._region = region
        self._stream_logs = stream_logs
        self._is_fake_in_process = is_fake_in_process
        self._code_sync_proc_mesh: Optional["_Lazy[ProcMesh]"] = _code_sync_proc_mesh

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

        hm = cls(
            PythonTask.from_coroutine(task()).spawn(),
            extent.region,
            alloc.stream_logs,
            isinstance(allocator, LocalAllocator),
            None,
        )

        hm._code_sync_proc_mesh = _Lazy(lambda: hm.spawn_procs(name="code_sync"))
        return hm

    def spawn_procs(
        self,
        per_host: Dict[str, int] | None = None,
        bootstrap: Callable[[], None] | Callable[[], Awaitable[None]] | None = None,
        name: str | None = None,
    ) -> "ProcMesh":
        if not per_host:
            per_host = {}

        if not name:
            name = "anon"

        return self._spawn_nonblocking(
            name,
            Extent(list(per_host.keys()), list(per_host.values())),
            bootstrap,
            True,
        )

    def _spawn_nonblocking(
        self,
        name: str,
        per_host: Extent,
        setup: Callable[[], None] | Callable[[], Awaitable[None]] | None,
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

        sliced_hy_hm: Shared[HyHostMesh]
        if (hm := self._hy_host_mesh.poll()) is not None:
            sliced_hy_hm = Shared.from_value(hm.sliced(shape.region))
        else:

            async def task() -> HyHostMesh:
                return (await self._hy_host_mesh).sliced(shape.region)

            sliced_hy_hm = PythonTask.from_coroutine(task()).spawn()

        return HostMesh(
            sliced_hy_hm,
            shape.region,
            self.stream_logs,
            self.is_fake_in_process,
            None,
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
        return HostMesh(
            Shared.from_value(hy_host_mesh),
            region,
            stream_logs,
            is_fake_in_process,
            None,
        )

    @classmethod
    def _from_rust(cls, hy_host_mesh: HyHostMesh) -> "HostMesh":
        """
        Create a HostMesh from a Rust HyHostMesh.

        This is used when the host was bootstrapped via bootstrap_host()
        instead of being allocated through an allocator.
        """
        return cls._from_initialized_hy_host_mesh(
            hy_host_mesh,
            hy_host_mesh.region,
            stream_logs=False,
            is_fake_in_process=False,
        )

    def __reduce_ex__(self, protocol: ...) -> Tuple[Any, Tuple[Any, ...]]:
        return HostMesh._from_initialized_hy_host_mesh, (
            self._hy_host_mesh.poll()
            or (
                PendingPickle(self._hy_host_mesh)
                if is_pending_pickle_allowed()
                else self._hy_host_mesh.block_on()
            ),
            self._region,
            self.stream_logs,
            self.is_fake_in_process,
        )

    @property
    def is_fake_in_process(self) -> bool:
        return self._is_fake_in_process

    def __eq__(self, other: "HostMesh") -> bool:
        # Should we include code sync proc mesh?
        return (
            self._initialized_mesh() == other._initialized_mesh()
            and self._region == other._region
            and self.stream_logs == other.stream_logs
            and self.is_fake_in_process == other.is_fake_in_process
        )

    def _initialized_mesh(self) -> HyHostMesh:
        return self._hy_host_mesh.poll() or self._hy_host_mesh.block_on()

    def shutdown(self) -> Future[None]:
        """
        Shutdown the host mesh and all of its processes. It will throw an exception
        if this host mesh is a *reference* rather than *owned*, which can happen
        if this `HostMesh` object was received from a remote actor or if it was
        produced by slicing.

        Returns:
            Future[None]: A future that completes when the host mesh has been shut down.
        """

        async def task() -> None:
            hy_mesh = await self._hy_host_mesh
            await hy_mesh.shutdown(context().actor_instance._as_rust())

        return Future(coro=task())

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
        if self._code_sync_proc_mesh:
            await self._code_sync_proc_mesh.get()._sync_workspace(
                workspace, conda, auto_reload
            )
        else:
            raise RuntimeError(
                "cannot call sync_workspace on a sliced host mesh or one that was sent over an actor endpoint"
            )

    @property
    def initialized(self) -> Future[Literal[True]]:
        """
        Future completes with 'True' when the `HostMesh` has initialized.
        Because `HostMesh` are remote objects, there is no guarentee that the `HostMesh` is
        still usable after this completes, only that at some point in the past it was usable.
        """
        hm: Shared[HyHostMesh] = self._hy_host_mesh

        async def task() -> Literal[True]:
            await hm
            return True

        return Future(coro=task())


def fake_in_process_host() -> "HostMesh":
    """
    Create a host mesh for testing and development using a local allocator.

    Returns:
        HostMesh: A host mesh configured with local allocation for in-process use.
    """
    return HostMesh.allocate_nonblocking(
        "fake_host",
        Extent([], []),
        LocalAllocator(),
        bootstrap_cmd=_bootstrap_cmd(),
    )


def hosts_from_config(name: str) -> HostMesh:
    """
    Get the host mesh 'name' from the monarch configuration for the project.

    This config can be modified so that the same code can create meshes from scheduler sources,
    and different sizes etc.

    WARNING: This function is a standin so that our getting_started example code works. The real implementation
    needs an RFC design.
    """

    return HostMesh.allocate_nonblocking(
        name,
        Extent(["hosts"], [2]),
        ProcessAllocator(*_get_bootstrap_args()),
        bootstrap_cmd=_bootstrap_cmd(),
    )


def host_mesh_from_alloc(
    name: str, extent: Extent, allocator: AllocateMixin, constraints: AllocConstraints
) -> HostMesh:
    return HostMesh.allocate_nonblocking(name, extent, allocator, constraints)
