# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Tuple, Type

from monarch._rust_bindings.monarch_hyperactor.handle import _after, Handle
from monarch._rust_bindings.monarch_hyperactor.host_mesh import (
    _spawn_admin as _hy_spawn_admin,
    BootstrapCommand,
    HostMesh as HyHostMesh,
    PyMeshAdminRef,
)
from monarch._rust_bindings.monarch_hyperactor.pickle import pickle
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Point, Region
from monarch._src.actor.actor_mesh import (
    _Actor,
    _actor_kind,
    _Lazy,
    ActorInitArgs,
    context,
)
from monarch._src.actor.future import _ready_handle, Future
from monarch._src.actor.proc_launcher import ProcLauncher
from monarch._src.actor.proc_mesh import _get_bootstrap_args, ProcMesh
from monarch._src.actor.returns_future import returns_future
from monarch._src.actor.shape import MeshTrait, NDSlice, Shape
from monarch.tools.config.workspace import Workspace


def default_bootstrap_cmd() -> BootstrapCommand:
    """Get the default bootstrap command for the current environment.

    Returns a BootstrapCommand configured with the current Python executable.
    Spawned procs inherit the environment of the host that spawns them; the
    command does not carry the caller's environment. This can be used as a
    base for customization with ``with_env()`` or by modifying its attributes
    directly. In ``with_env()``, a ``None`` value removes a variable from the
    inherited environment; set ``inherit_env = False`` to start from an empty
    environment instead.

    Returns:
        BootstrapCommand: The default bootstrap command.
    """
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


class HostMesh(MeshTrait):
    """
    HostMesh represents a collection of compute hosts that can be used to spawn
    processes and actors.

    Can be used as an async context manager, which will shut down the hosts on
    exit:
    ```
    host_mesh = job.state().hosts
    async with host_mesh:
        # spawn proc meshes, actor meshes, etc.
    # shutdown() is called automatically on exit.
    ```

    If you don't want to shutdown the hosts, you don't need to use it as a
    context manager.
    """

    def __init__(
        self,
        hy_host_mesh: Handle[HyHostMesh],
        region: Region,
        stream_logs: bool,
        is_fake_in_process: bool,
        code_sync_proc_mesh: Optional["_Lazy[ProcMesh]"],
    ) -> None:
        self._inner_host_mesh: Optional[Handle[HyHostMesh]] = hy_host_mesh
        self._region = region
        self._stream_logs = stream_logs
        self._is_fake_in_process = is_fake_in_process
        self._code_sync_proc_mesh: Optional["_Lazy[ProcMesh]"] = code_sync_proc_mesh
        self._pending_spawns: list[Handle[HyProcMesh]] = []
        self._proc_meshes: list["ProcMesh"] = []

    def spawn_procs(
        self,
        per_host: Dict[str, int] | None = None,
        bootstrap: Callable[[], None] | Callable[[], Awaitable[None]] | None = None,
        name: str | None = None,
        proc_bind: list[dict[str, str]] | None = None,
        bootstrap_command: (
            BootstrapCommand | Callable[[Point], BootstrapCommand] | None
        ) = None,
    ) -> "ProcMesh":
        """Spawn a ProcMesh onto this host mesh.

        Args:
            per_host: shape of procs per host, e.g. ``{"gpus": 4}``.
            bootstrap: optional setup callable run on each proc.
            name: optional name for the proc mesh.
            proc_bind: optional per-process CPU/NUMA binding config.
                Length must equal ``math.prod(per_host.values())``.
                Each dict maps binding keys (``cpunodebind``,
                ``membind``, ``physcpubind``, ``cpus``) to values.
            bootstrap_command: optional BootstrapCommand or callable that
                returns a BootstrapCommand for each coordinate. The callable
                receives a ``Point`` (combined coordinate across host and
                per_host dimensions). This allows full customization of the
                bootstrap command per coordinate. The callable runs once per
                rank, in rank order, on the calling thread before spawning
                begins; its errors raise from this call.
        """
        if not per_host:
            per_host = {}

        if not name:
            name = "anon"

        import math

        procs_per_host = math.prod(per_host.values()) if per_host else 1
        if proc_bind is not None and len(proc_bind) != procs_per_host:
            raise ValueError(
                f"proc_bind length ({len(proc_bind)}) must equal "
                f"procs_per_host ({procs_per_host})"
            )

        return self._spawn_nonblocking(
            name,
            Extent(list(per_host.keys()), list(per_host.values())),
            bootstrap,
            True,
            proc_bind,
            bootstrap_command,
        )

    def _spawn_nonblocking(
        self,
        name: str,
        per_host: Extent,
        setup: Callable[[], None] | Callable[[], Awaitable[None]] | None,
        _attach_controller_controller: bool,
        proc_bind: list[dict[str, str]] | None = None,
        bootstrap_command: (
            BootstrapCommand | Callable[[Point], BootstrapCommand] | None
        ) = None,
    ) -> "ProcMesh":
        if set(per_host.labels) & set(self._labels):
            # The rust side will catch this too, but this lets us fail fast
            raise ValueError(
                f"per_host labels {per_host.labels} overlap with host labels {self._labels}"
            )
        # This is checked inside the task as well, but we can pre-emptively raise
        # earlier.
        if self._inner_host_mesh is None:
            raise RuntimeError("HostMesh has already been shut down")

        extent = Extent(
            self._labels + tuple(per_host.labels),
            self.region.slice().sizes + list(per_host.sizes),
        )
        per_rank_bootstrap: BootstrapCommand | list[BootstrapCommand] | None
        if bootstrap_command is None or isinstance(bootstrap_command, BootstrapCommand):
            per_rank_bootstrap = bootstrap_command
        else:
            # Evaluate the callable here, on the caller's thread, so native
            # spawn receives finished commands and never calls back into
            # Python. Ranks follow the combined extent that host spawn uses.
            per_rank_bootstrap = [
                bootstrap_command(Point(rank, extent))
                for rank in range(extent.nelements)
            ]
            if not all(isinstance(cmd, BootstrapCommand) for cmd in per_rank_bootstrap):
                raise TypeError(
                    "per-rank bootstrap callable must return BootstrapCommand"
                )

        spawned = HyHostMesh.spawn_nonblocking(
            self._hy_host_mesh,
            context().actor_instance._as_rust(),
            name,
            per_host,
            proc_bind,
            per_rank_bootstrap,
        )
        self._pending_spawns.append(spawned)

        pm = ProcMesh.from_host_mesh(
            self,
            spawned,
            extent.region,
            setup,
            _attach_controller_controller,
        )
        self._proc_meshes.append(pm)
        return pm

    @property
    def _ndslice(self) -> NDSlice:
        return self.region.slice()

    @property
    def _labels(self) -> Tuple[str, ...]:
        return tuple(self.region.labels)

    def _new_with_shape(self, shape: Shape) -> "HostMesh":
        if shape.region == self._region:
            return self

        sliced_hy_hm: Handle[HyHostMesh]
        if (hm := self._hy_host_mesh.poll()) is not None:
            sliced_hy_hm = _ready_handle(hm.sliced(shape.region))
        else:
            sliced_hy_hm = HyHostMesh.sliced_handle(self._hy_host_mesh, shape.region)

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
            _ready_handle(hy_host_mesh),
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

    def with_python_executable(self, python_executable: str) -> "HostMesh":
        """
        Return a new HostMesh that will use the given Python executable when
        spawning procs. Procs spawned from this mesh will also inherit this
        Python executable when they call ``this_host().spawn_procs(...)``.

        Args:
            python_executable: Path to the Python executable to use.

        Returns:
            A new HostMesh configured to use the specified Python executable.
        """
        _, _, bootstrap_env = _get_bootstrap_args()
        bootstrap_cmd: BootstrapCommand = BootstrapCommand(
            python_executable,
            None,
            ["-m", "monarch._src.actor.bootstrap_main"],
            bootstrap_env,
        )

        host = self._hy_host_mesh
        rebound: Handle[HyHostMesh]
        if (resolved := host.poll()) is not None:
            # Fast path: the host mesh is available, so apply the command here
            # and return a ready Handle, avoiding a Tokio task just for this.
            rebound = _ready_handle(resolved.with_bootstrap(bootstrap_cmd))
        else:
            # Host creation is still pending: Rust waits for it, then applies
            # the launch command. Return its Handle without waiting here.
            rebound = HyHostMesh.with_bootstrap_handle(host, bootstrap_cmd)

        return HostMesh(
            rebound,
            self._region,
            self._stream_logs,
            self._is_fake_in_process,
            None,
        )

    # pyrefly: ignore [invalid-annotation]
    def __reduce_ex__(self, protocol: ...) -> Tuple[Any, Tuple[Any, ...]]:
        # A pending host mesh has no HostMeshRef yet. When mesh-reference
        # collection is active, reserve an out-of-band slot (filled sender-side
        # once the mesh resolves) and reconstruct from the popped mesh;
        # otherwise fall through to the ordinary reduce.
        if self._hy_host_mesh.poll() is None:
            from monarch._rust_bindings.monarch_hyperactor.pickle import (
                reserve_mesh_reference,
            )
            from monarch._src.actor.pickle import _MeshSlot

            if reserve_mesh_reference(self._hy_host_mesh):
                return HostMesh._from_initialized_hy_host_mesh, (
                    _MeshSlot(),
                    self._region,
                    self.stream_logs,
                    self.is_fake_in_process,
                )
        mesh = self._hy_host_mesh.poll()
        if mesh is None:
            mesh = self._hy_host_mesh.get()
        return HostMesh._from_initialized_hy_host_mesh, (
            mesh,
            self._region,
            self.stream_logs,
            self.is_fake_in_process,
        )

    @property
    def is_fake_in_process(self) -> bool:
        return self._is_fake_in_process

    # pyrefly: ignore [bad-override]
    def __eq__(self, other: "HostMesh") -> bool:
        # Should we include code sync proc mesh?
        return (
            self._initialized_mesh() == other._initialized_mesh()
            and self._region == other._region
            and self.stream_logs == other.stream_logs
            and self.is_fake_in_process == other.is_fake_in_process
        )

    def _initialized_mesh(self) -> HyHostMesh:
        return self._hy_host_mesh.poll() or self._hy_host_mesh.get()

    async def _drain_pending_spawns(self) -> None:
        """Wait for every pending proc spawn, including any added while waiting,
        then drain each proc mesh's actor spawns and flush its logs."""
        pending = self._pending_spawns
        while (handle := next(iter(pending), None)) is not None:
            try:
                await handle
            except Exception:
                pass
            # Another drain may have removed it while this one waited. Removal
            # matches by identity in one step, so it cannot drop a different
            # spawn that was never waited for.
            try:
                pending.remove(handle)
            except ValueError:
                pass
        for pm in self._proc_meshes:
            await pm._drain_pending_actor_spawns()
            await pm._logging_manager.flush_async()

    @returns_future
    async def shutdown(self) -> None:
        """
        Shutdown the host mesh and all of its processes. It will throw an exception
        if this host mesh is a *reference* rather than *owned*, which can happen
        if this `HostMesh` object was received from a remote actor or if it was
        produced by slicing.
        After shutting down, the hosts in this mesh will be unusable, and no new
        HostMeshes will be able to connect to them.
        If you want to stop everything on the host but keep them available for
        new clients, use `stop()` instead.

        This is run automatically on __aexit__ when used as an async context manager.

        Returns:
            Future[None]: A future that completes when the host mesh has been shut down.
        """

        await self._drain_pending_spawns()
        hy_mesh = await self._hy_host_mesh
        await hy_mesh.shutdown(context().actor_instance._as_rust())
        # Remove the inner host mesh to clean up associated memory.
        self._inner_host_mesh = None

    @returns_future
    async def stop(self) -> None:
        """
        Stop the host mesh, releasing all resources but keeping worker
        processes alive for reconnection. A new HostMesh can be created that
        points to the same hosts.

        Like `shutdown`, this throws if the host mesh is a reference
        rather than owned.

        Returns:
            Future[None]: A future that completes when the host mesh has been stopped.
        """

        await self._drain_pending_spawns()
        hy_mesh = await self._hy_host_mesh
        await hy_mesh.stop(context().actor_instance._as_rust())

    @returns_future
    async def set_proc_launcher(
        self,
        launcher: Type[ProcLauncher],
        params: dict[str, Any] | None = None,
    ) -> None:
        """
        Install a Python proc launcher on every host of this mesh.

        Calling this method alone does not start installation. Await the
        returned Future or call ``.get()`` to start it and wait for completion.

        Each host constructs ``launcher(params)`` on its system proc. Procs
        spawned on the host afterwards are launched, stopped and reported on
        by that launcher instead of the built-in one. Wait for the returned
        Future before spawning procs on these hosts: a host that has already
        spawned a proc keeps the launcher it used, and installing there fails.
        The launcher stays installed for the lifetime of each host process,
        including across ``stop()`` and later clients.

        Args:
            launcher: A subclass of
                ``monarch._src.actor.proc_launcher.ProcLauncher``.
            params: Picklable configuration passed to the launcher's
                constructor on every host.

        Returns:
            Future[None]: Completes once every host has installed the
            launcher. Raises an error naming each host that failed; hosts
            that succeeded keep the launcher.
        """
        if not (isinstance(launcher, type) and issubclass(launcher, ProcLauncher)):
            raise TypeError(f"{launcher!r} is not a ProcLauncher subclass")
        kind = _actor_kind(launcher)
        init_args = pickle(
            (
                (
                    ActorInitArgs(
                        launcher, None, None, "proc_launcher", None, (params,)
                    ),
                ),
                {},
            ),
            allow_tensor_engine_references=False,
        ).buffer()
        hy_mesh = await self._hy_host_mesh
        await hy_mesh.set_proc_launcher(
            context().actor_instance._as_rust(),
            _Actor,
            init_args,
            kind,
            launcher.reachability.value,
        )

    async def __aenter__(self) -> "HostMesh":
        if self._inner_host_mesh is None:
            raise RuntimeError("HostMesh has already been shut down")
        return self

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        # In case there are multiple nested "async with" statements, we only
        # want it to close once.
        if self._inner_host_mesh is not None:
            await self.shutdown()

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
        Because `HostMesh` are remote objects, there is no guarantee that the `HostMesh` is
        still usable after this completes, only that at some point in the past it was usable.
        """
        ready: Handle[Literal[True]] = _ready_handle(True)
        return Future._from_handle(_after([self._hy_host_mesh], ready))

    @property
    def _hy_host_mesh(self) -> Handle[HyHostMesh]:
        if self._inner_host_mesh is None:
            raise RuntimeError("HostMesh has already been shut down")
        return self._inner_host_mesh


def _spawn_admin(
    host_meshes: list["HostMesh"],
    admin_addr: Optional[str] = None,
    telemetry_url: Optional[str] = None,
) -> "Future[tuple[str, PyMeshAdminRef]]":
    """
    Spawn a MeshAdminAgent aggregating topology across one or more HostMeshes.

    Returns ``(admin_url, admin_ref)`` where ``admin_ref`` is an opaque
    capability token for immediate use only.

    Args:
        host_meshes: One or more HostMeshes whose hosts the admin
            will aggregate for introspection. Must not be empty.
        admin_addr: Optional socket address for the admin HTTP server.
            When ``None``, reads ``MESH_ADMIN_ADDR`` from config.
        telemetry_url: Optional base URL of the Monarch telemetry dashboard.

    Returns:
        Future[tuple[str, PyMeshAdminRef]]: (admin_url, admin_ref).

    Raises:
        ValueError: If host_meshes is empty.
    """
    if not host_meshes:
        raise ValueError("_spawn_admin requires at least one HostMesh")

    return Future._from_handle(
        _hy_spawn_admin(
            [m._hy_host_mesh for m in host_meshes],
            context().actor_instance._as_rust(),
            admin_addr,
            telemetry_url,
        )
    )
