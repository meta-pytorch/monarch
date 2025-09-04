# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
import os
import sys
import threading
import warnings
from contextlib import AbstractContextManager

from functools import cache
from pathlib import Path

from typing import (
    Any,
    Callable,
    cast,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
)
from weakref import WeakValueDictionary

from monarch._rust_bindings.monarch_hyperactor.alloc import (  # @manual=//monarch/monarch_extension:monarch_extension
    Alloc,
    AllocConstraints,
    AllocSpec,
)

from monarch._rust_bindings.monarch_hyperactor.proc_mesh import (
    ProcMesh as HyProcMesh,
    ProcMeshMonitor,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Shape, Slice
from monarch._src.actor.actor_mesh import _Actor, Actor, ActorMesh, context
from monarch._src.actor.allocator import (
    AllocateMixin,
    AllocHandle,
    LocalAllocator,
    ProcessAllocator,
    SimAllocator,
)
from monarch._src.actor.code_sync import (
    CodeSyncMeshClient,
    CodeSyncMethod,
    RemoteWorkspace,
    WorkspaceConfig,
    WorkspaceLocation,
    WorkspaceShape,
)
from monarch._src.actor.device_utils import _local_device_count

from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import DeprecatedNotAFuture, Future
from monarch._src.actor.logging import LoggingManager
from monarch._src.actor.shape import MeshTrait
from monarch.tools.config.environment import CondaEnvironment
from monarch.tools.config.workspace import Workspace
from monarch.tools.utils import conda as conda_utils


@cache
def _has_tensor_engine() -> bool:
    try:
        # Torch is needed for tensor engine
        import torch  # @manual

        # Confirm that rust bindings were built with tensor engine enabled
        from monarch._rust_bindings.rdma import _RdmaManager  # noqa

        return True
    except ImportError:
        logging.warning("Tensor engine is not available on this platform")
        return False


if TYPE_CHECKING:
    Tensor = Any
    DeviceMesh = Any
    from monarch._src.actor.host_mesh import HostMesh


class SetupActor(Actor):
    """
    A helper actor to set up the actor mesh with user defined setup method.
    """

    def __init__(self, env: Callable[[], None]) -> None:
        self._setup_method = env

    @endpoint
    async def setup(self) -> None:
        """
        Call the user defined setup method with the monarch context.
        """
        self._setup_method()


T = TypeVar("T")
try:
    from __manifest__ import fbmake  # noqa

    IN_PAR = bool(fbmake.get("par_style"))
except ImportError:
    IN_PAR = False


# A temporary gate used by the PythonActorMesh/PythonActorMeshRef migration.
# We can use this gate to quickly roll back to using _ActorMeshRefImpl, if we
# encounter any issues with the migration.
#
# This should be removed once we confirm PythonActorMesh/PythonActorMeshRef is
# working correctly in production.
@cache
def _use_standin_mesh() -> bool:
    return os.getenv("USE_STANDIN_ACTOR_MESH", default="0") != "0"


class ProcMeshRef:
    """
    A serializable remote reference to a ProcMesh. The reference is weak: No support
    for refcount'ing. Spawning actors on a ProcMeshRef a stopped or a failed mesh will fail.
    """

    def __init__(self, proc_mesh_id: int) -> None:
        self._proc_mesh_id = proc_mesh_id
        self._host_mesh: Optional["HostMesh"] = None

    @classmethod
    def _fake_proc_mesh(cls, proc_mesh_id: int) -> "ProcMesh":
        return cast(ProcMesh, cls(proc_mesh_id))

    def __getattr__(self, attr: str) -> Any:
        # AttributeError instead of NotImplementedError so that any hasattr calls
        # will properly return False
        raise AttributeError(
            f"NYI: attempting to get ProcMesh attribute `{attr}` on object that's actually a ProcMeshRef"
        )

    def __hash__(self) -> int:
        return hash(self._proc_mesh_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProcMeshRef):
            return False
        return self._proc_mesh_id == other._proc_mesh_id

    @property
    def _proc_mesh(self) -> Shared["HyProcMesh"]:
        return _deref_proc_mesh(self)._proc_mesh


_proc_mesh_lock: threading.Lock = threading.Lock()
_proc_mesh_key: int = 0
_proc_mesh_registry: WeakValueDictionary[ProcMeshRef, "ProcMesh"] = (
    WeakValueDictionary()
)


def _deref_proc_mesh(proc_mesh: ProcMeshRef) -> "ProcMesh":
    if proc_mesh not in _proc_mesh_registry:
        raise ValueError(
            f"ProcMesh with id {proc_mesh._proc_mesh_id} does not exist on host."
        )
    return _proc_mesh_registry[proc_mesh]


class ProcMesh(MeshTrait, DeprecatedNotAFuture):
    """
    A distributed mesh of processes for actor computation.

    ProcMesh represents a collection of processes that can spawn and manage actors.
    It provides the foundation for distributed actor systems by managing process
    allocation, lifecycle, and communication across multiple hosts and devices.

    The ProcMesh supports spawning actors, monitoring process health, logging
    configuration, and code synchronization across distributed processes.
    """

    def __init__(
        self,
        hy_proc_mesh: "Shared[HyProcMesh]",
        shape: Shape,
        _device_mesh: Optional["DeviceMesh"] = None,
    ) -> None:
        self._proc_mesh = hy_proc_mesh
        global _proc_mesh_lock, _proc_mesh_key
        with _proc_mesh_lock:
            self._proc_mesh_id: int = _proc_mesh_key
            _proc_mesh_key += 1
        self._shape = shape
        # until we have real slicing support keep track
        # of whether this is a slice of a real proc_meshg
        self._slice = False
        self._code_sync_client: Optional[CodeSyncMeshClient] = None
        self._logging_manager: LoggingManager = LoggingManager()
        self._maybe_device_mesh: Optional["DeviceMesh"] = _device_mesh
        self._stopped = False
        self._controller_controller: Optional["_ControllerController"] = None
        # current set only for context()'s proc_mesh to be a local host mesh.
        self._host_mesh: Optional["HostMesh"] = None

    @property
    def initialized(self) -> Future[Literal[True]]:
        """
        Future completes with 'True' when the ProcMesh has initialized.
        Because ProcMesh are remote objects, there is no guarentee that the ProcMesh is
        still usable after this completes, only that at some point in the past it was usable.
        """
        pm: Shared[HyProcMesh] = self._proc_mesh

        async def task() -> Literal[True]:
            await pm
            return True

        return Future(coro=task())

    @property
    def host_mesh(self) -> "HostMesh":
        if self._host_mesh is None:
            raise NotImplementedError(
                "NYI complete for release 0.1 (ProcMeshRef knowing its host mesh)"
            )
        return self._host_mesh

    @property
    def _ndslice(self) -> Slice:
        return self._shape.ndslice

    @property
    def _labels(self) -> List[str]:
        return self._shape.labels

    def _new_with_shape(self, shape: Shape) -> "ProcMesh":
        # make sure that if we slice something with unity,
        # we do not lose the ability to spawn on it.
        # remote when spawn is implemented.
        if shape == self._shape:
            return self
        device_mesh = (
            None
            if self._maybe_device_mesh is None
            else self._device_mesh._new_with_shape(shape)
        )
        pm = ProcMesh(self._proc_mesh, shape, _device_mesh=device_mesh)
        pm._slice = True
        return pm

    def spawn(self, name: str, Class: Type[T], *args: Any, **kwargs: Any) -> T:
        """
        Spawn a T-typed actor mesh on the process mesh.

        Args:
        - `name`: The name of the actor.
        - `Class`: The class of the actor to spawn.
        - `args`: Positional arguments to pass to the actor's constructor.
        - `kwargs`: Keyword arguments to pass to the actor's constructor.

        Returns:
        - The actor instance.

        Usage:
            >>> procs: ProcMesh = host_mesh.spawn_procs(per_host={"gpus": 8})
            >>> counters: Counter = procs.spawn("counters", Counter, 0)
        """
        if self._slice:
            raise NotImplementedError("NYI: spawn on slice of a proc mesh.")
        return self._spawn_nonblocking(name, Class, *args, **kwargs)

    @property
    async def _proc_mesh_for_asyncio_fixme(self) -> HyProcMesh:
        """
        Get ProcMesh on the asyncio event stream.
        We should redo this functionality to work on the tokio stream.
        This must be called on the asyncio stream.
        """
        assert asyncio.get_running_loop() is not None
        return await Future(coro=self._proc_mesh.task())

    async def monitor(self) -> ProcMeshMonitor:
        """
        Get a monitor (async iterator) of the proc mesh, it is used to
        monitor the status of the proc mesh. This function can be called at most once.

        Note: This API is experimental and subject to change.

        Example:

        async def monitor_loop(monitor):
            async for event in monitor:
                await handle_exception_event(event)

        # Kick off in background
        asyncio.create_task(monitor_loop(monitor))
        """
        # todo: move monitor to tokio loop
        proc_mesh = await Future(coro=self._proc_mesh.task())
        return await proc_mesh.monitor()

    @classmethod
    def from_alloc(
        self,
        alloc: AllocHandle,
        setup: Callable[[], None] | None = None,
        _attach_controller_controller: bool = True,
    ) -> "ProcMesh":
        """
        Allocate a process mesh according to the provided alloc.
        Returns when the mesh is fully allocated.

        Args:
        - `alloc`: A generator that yields a list of allocations.
        - `setup`: An optional lambda function to configure environment variables on the allocated mesh.
        """

        async def task() -> HyProcMesh:
            return await HyProcMesh.allocate_nonblocking(await alloc._hy_alloc)

        shape = Shape(
            list(alloc._extent.keys()),
            Slice.new_row_major(list(alloc._extent.values())),
        )

        hy_proc_mesh = PythonTask.from_coroutine(task()).spawn()

        pm = ProcMesh(hy_proc_mesh, shape)
        if _attach_controller_controller:
            instance = context().actor_instance
            pm._controller_controller = instance._controller_controller
            instance._add_child(pm)

        async def task(
            pm: "ProcMesh",
            hy_proc_mesh_task: "Shared[HyProcMesh]",
            setup_actor: Optional[SetupActor],
            stream_log_to_client: bool,
        ) -> HyProcMesh:
            hy_proc_mesh = await hy_proc_mesh_task

            await pm._logging_manager.init(hy_proc_mesh, stream_log_to_client)

            if setup_actor is not None:
                await setup_actor.setup.call()

            return hy_proc_mesh

        setup_actor = None
        if setup is not None:
            # If the user has passed the setup lambda, we need to call
            # it here before any of the other actors are spawned so that
            # the environment variables are set up before cuda init.
            setup_actor = pm._spawn_nonblocking_on(
                hy_proc_mesh, "setup", SetupActor, setup
            )

        pm._proc_mesh = PythonTask.from_coroutine(
            task(pm, hy_proc_mesh, setup_actor, alloc.stream_logs)
        ).spawn()

        return pm

    def __repr__(self) -> str:
        return repr(self._proc_mesh)

    def __str__(self) -> str:
        return str(self._proc_mesh)

    def _spawn_nonblocking(
        self, name: str, Class: Type[T], *args: Any, **kwargs: Any
    ) -> T:
        return self._spawn_nonblocking_on(self._proc_mesh, name, Class, *args, **kwargs)

    def to_table(self) -> str:
        return self._device_mesh.to_table()

    def _spawn_nonblocking_on(
        self,
        pm: "Shared[HyProcMesh]",
        name: str,
        Class: Type[T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        if not issubclass(Class, Actor):
            raise ValueError(
                f"{Class} must subclass monarch.service.Actor to spawn it."
            )

        actor_mesh = HyProcMesh.spawn_async(pm, name, _Actor, _use_standin_mesh())
        instance = context().actor_instance
        service = ActorMesh._create(
            Class,
            actor_mesh,
            instance._mailbox,
            self._shape,
            self,
            self._controller_controller,
            *args,
            **kwargs,
        )
        instance._add_child(service)
        return cast(T, service)

    @property
    def _device_mesh(self) -> "DeviceMesh":
        if not _has_tensor_engine():
            raise RuntimeError(
                "DeviceMesh is not available because tensor_engine was not compiled (USE_TENSOR_ENGINE=0)"
            )

        # type: ignore[21]
        from monarch.mesh_controller import spawn_tensor_engine  # @manual

        if self._maybe_device_mesh is None:
            if self._slice:
                raise NotImplementedError(
                    "NYI: activating a proc mesh must first happen on the root proc_mesh until we fix spawning on submeshes."
                )
            # type: ignore[21]
            self._maybe_device_mesh = spawn_tensor_engine(self)
        return self._maybe_device_mesh

    # pyre-ignore
    def activate(self) -> AbstractContextManager:
        return self._device_mesh.activate()

    def rank_tensor(self, dim: str | Sequence[str]) -> "Tensor":
        return self._device_mesh.rank(dim)

    def rank_tensors(self) -> Dict[str, "Tensor"]:
        return self._device_mesh.ranks

    async def sync_workspace(
        self,
        workspace: Workspace,
        conda: bool = False,
        auto_reload: bool = False,
    ) -> None:
        """
        Sync local code changes to the remote processes.

        Args:
            workspace: The workspace to sync.
            conda: If True, also sync the currently activated conda env.
            auto_reload: If True, automatically reload the workspace on changes.
        """
        if self._code_sync_client is None:
            self._code_sync_client = CodeSyncMeshClient.spawn_blocking(
                proc_mesh=await self._proc_mesh_for_asyncio_fixme,
            )

        # TODO(agallagher): We need some way to configure and pass this
        # in -- right now we're assuming the `gpu` dimension, which isn't
        # correct.
        # The workspace shape (i.e. only perform one rsync per host).
        assert set(self._shape.labels).issubset({"gpus", "hosts"})

        workspaces = []
        for src_dir, dst_dir in workspace.dirs.items():
            workspaces.append(
                WorkspaceConfig(
                    local=Path(src_dir),
                    remote=RemoteWorkspace(
                        location=WorkspaceLocation.FromEnvVar(
                            env="WORKSPACE_DIR",
                            relpath=dst_dir,
                        ),
                        shape=WorkspaceShape.shared("gpus"),
                    ),
                    method=CodeSyncMethod.Rsync,
                ),
            )

        # If `conda` is set, also sync the currently activated conda env.
        conda_prefix = conda_utils.active_env_dir()
        if isinstance(workspace.env, CondaEnvironment):
            conda_prefix = workspace.env._conda_prefix

        if conda and conda_prefix is not None:
            conda_prefix = Path(conda_prefix)

            # Resolve top-level symlinks for rsync/conda-sync.
            while conda_prefix.is_symlink():
                conda_prefix = conda_prefix.parent / conda_prefix.readlink()

            workspaces.append(
                WorkspaceConfig(
                    local=conda_prefix,
                    remote=RemoteWorkspace(
                        location=WorkspaceLocation.FromEnvVar(
                            env="CONDA_PREFIX",
                            relpath="",
                        ),
                        shape=WorkspaceShape.shared("gpus"),
                    ),
                    method=CodeSyncMethod.CondaSync,
                ),
            )

        assert self._code_sync_client is not None
        await self._code_sync_client.sync_workspaces(
            workspaces=workspaces,
            auto_reload=auto_reload,
        )

    async def logging_option(
        self,
        stream_to_client: bool = True,
        aggregate_window_sec: int | None = 3,
        level: int = logging.INFO,
    ) -> None:
        """
        Set the logging options for the remote processes

        Args:
            stream_to_client (bool): If True, logs from the remote processes will be streamed to the client.
            Defaults to True.
            aggregate_window_sec (Optional[int]): If not None, logs from the remote processes will be aggregated
            and sent to the client every aggregate_window_sec seconds. Defaults to 3 seconds, meaning no aggregation.
            Error will be thrown if aggregate_window_sec is set and stream_to_client is False.
            level (int): The logging level of the logger. Defaults to logging.INFO.

        Returns:
            None
        """
        await self.initialized

        await self._logging_manager.logging_option(
            stream_to_client=stream_to_client,
            aggregate_window_sec=aggregate_window_sec,
            level=level,
        )

    async def __aenter__(self) -> "ProcMesh":
        if self._stopped:
            raise RuntimeError("`ProcMesh` has already been stopped")
        return self

    def stop(self) -> Future[None]:
        """
        This will stop all processes (and actors) in the mesh and
        release any resources associated with the mesh.
        """
        self._logging_manager.stop()

        async def _stop_nonblocking() -> None:
            await (await self._proc_mesh).stop_nonblocking()
            self._stopped = True

        return Future(coro=_stop_nonblocking())

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        # In case there are multiple nested "async with" statements, we only
        # want it to close once.
        if not self._stopped:
            await self.stop()

    # Finalizer to check if the proc mesh was closed properly.
    def __del__(self) -> None:
        if not self._stopped:
            self._logging_manager.stop()

            warnings.warn(
                f"unstopped ProcMesh {self!r}",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )
            # Cannot call stop here because it is async.

    def __reduce_ex__(self, protocol: ...) -> Tuple[Any, Tuple[Any, ...]]:
        # Ultra-hack. Remote python actors can get a reference to this proc mesh that
        # doesn't have any real functionality, but if they send a request back to the client
        # where the real proc mesh exists, the client can look it up in the proc mesh registry
        # and do something with it.
        global _proc_mesh_registry
        _proc_mesh_registry[ProcMeshRef(self._proc_mesh_id)] = self
        return (ProcMeshRef._fake_proc_mesh, (self._proc_mesh_id,))

    @staticmethod
    def _from_ref(proc_mesh_ref: ProcMeshRef) -> "ProcMesh":
        maybe_proc_mesh = _proc_mesh_registry.get(proc_mesh_ref, None)
        if maybe_proc_mesh is None:
            raise RuntimeError(
                f"ProcMesh with id {proc_mesh_ref._proc_mesh_id} does not exist"
            )
        return maybe_proc_mesh


def local_proc_mesh(*, gpus: Optional[int] = None, hosts: int = 1) -> ProcMesh:
    """
    Create a local process mesh for testing and development.

    This function creates a process mesh using local allocation instead of
    distributed process allocation. Primarily used for testing scenarios.

    Args:
        gpus: Number of GPUs to allocate per host. If None, uses local device count.
        hosts: Number of hosts to allocate. Defaults to 1.

    Returns:
        ProcMesh: A locally allocated process mesh.

    Warning:
        This function is deprecated. Use `fake_in_process_host().spawn_procs()`
        for testing or `this_proc().spawn_procs()` for current process actors.
    """
    warnings.warn(
        "Use monarch._src.actor.host_mesh.fake_in_process_host().spawn_procs for testing. For launching an actor in the current process use this_proc().spawn_procs()",
        DeprecationWarning,
        stacklevel=2,
    )

    return _proc_mesh_from_allocator(
        allocator=LocalAllocator(),
        gpus=gpus,
        hosts=hosts,
    )


def sim_proc_mesh(
    *,
    gpus: int = 1,
    hosts: int = 1,
    racks: int = 1,
    zones: int = 1,
    dcs: int = 1,
    regions: int = 1,
) -> ProcMesh:
    """Create a simulated process mesh for testing distributed scenarios.

    This function creates a process mesh using simulation allocation to test
    distributed behavior without requiring actual remote resources.

    Args:
        gpus: Number of GPUs per host. Defaults to 1.
        hosts: Number of hosts. Defaults to 1.
        racks: Number of racks. Defaults to 1.
        zones: Number of zones. Defaults to 1.
        dcs: Number of data centers. Defaults to 1.
        regions: Number of regions. Defaults to 1.

    Returns:
        ProcMesh: A simulated process mesh with the specified topology.
    """
    spec: AllocSpec = AllocSpec(
        AllocConstraints(),
        hosts=hosts,
        gpus=gpus,
        racks=racks,
        zones=zones,
        dcs=dcs,
        regions=regions,
    )
    alloc = SimAllocator().allocate(spec)
    return ProcMesh.from_alloc(alloc, None, True)


_BOOTSTRAP_MAIN = "monarch._src.actor.bootstrap_main"


def _get_bootstrap_args() -> tuple[str, Optional[list[str]], dict[str, str]]:
    if IN_PAR:
        cmd = sys.argv[0]
        args = None
        env = {
            "PAR_MAIN_OVERRIDE": _BOOTSTRAP_MAIN,
        }
    else:
        cmd = sys.executable
        args = ["-m", _BOOTSTRAP_MAIN]
        env = {}

    return cmd, args, env


async def _hy_proc_mesh_from_alloc_coro(
    alloc: "Shared[Alloc] | PythonTask[Alloc]",
) -> HyProcMesh:
    return await HyProcMesh.allocate_nonblocking(await alloc)


def _proc_mesh_from_allocator(
    *,
    allocator: AllocateMixin,
    gpus: Optional[int],
    hosts: int,
    setup: Callable[[], None] | None = None,
    _attach_controller_controller: bool = True,
) -> ProcMesh:
    if gpus is None:
        gpus = _local_device_count()
    # gpus must come last in this order because
    # test_remote_function_all_gather expects that hosts comes before gpus
    # in the order of the dimensions.
    spec: AllocSpec = AllocSpec(AllocConstraints(), hosts=hosts, gpus=gpus)
    alloc = allocator.allocate(spec)
    return ProcMesh.from_alloc(alloc, setup, _attach_controller_controller)


def proc_mesh(
    *,
    gpus: Optional[int] = None,
    hosts: int = 1,
    env: dict[str, str] | None = None,
    setup: Callable[[], None] | None = None,
) -> ProcMesh:
    """
    Create a distributed process mesh across hosts.

    This function creates a process mesh using distributed process allocation
    across multiple hosts and GPUs. Used for production distributed computing.

    Args:
        gpus: Number of GPUs per host. If None, uses local device count.
        hosts: Number of hosts to allocate. Defaults to 1.
        env: Environment variables to set on remote processes.
        setup: Optional setup function to run on each process at startup.

    Returns:
        ProcMesh: A distributed process mesh with the specified configuration.

    Warning:
        This function is deprecated. Use `this_host().spawn_procs()` with
        appropriate per_host configuration instead.
    """
    warnings.warn(
        "use this_host().spawn_procs(per_host = {'hosts': 2, 'gpus': 3}) instead of monarch.actor.proc_mesh(hosts=2, gpus=3)",
        DeprecationWarning,
        stacklevel=2,
    )

    env = env or {}
    # Todo: Deprecate the env field from the ProcessAllocator
    # The PAR_MAIN_OVERRIDE needs to be passed as an env
    # to the proc mesh construction in rust, so can not be moved to the
    # SetupActor yet
    cmd, args, bootstrap_env = _get_bootstrap_args()
    env.update(bootstrap_env)
    return _proc_mesh_from_allocator(
        allocator=ProcessAllocator(cmd, args, env),
        hosts=hosts,
        gpus=gpus,
        setup=setup,
        _attach_controller_controller=True,
    )


_ActorType = TypeVar("_ActorType", bound=Actor)


class _ControllerController(Actor):
    def __init__(self) -> None:
        self._controllers: Dict[str, Actor] = {}

    # pyre-ignore
    @endpoint
    def get_or_spawn(
        self, name: str, Class: Type[_ActorType], *args: Any, **kwargs: Any
    ) -> _ActorType:
        if name not in self._controllers:
            proc_mesh = _proc_mesh_from_allocator(
                gpus=1,
                hosts=1,
                allocator=LocalAllocator(),
            )
            self._controllers[name] = proc_mesh.spawn(name, Class, *args, **kwargs)
        return cast(_ActorType, self._controllers[name])


_cc_init = threading.Lock()
_cc_proc_mesh: Optional["ProcMesh"] = None
_controller_controller: Optional["_ControllerController"] = None


# Lazy init so that the controller_controller and proc do not produce logs when they aren't used.
# Checking for the controller (when it does not already exist in the MonarchContext) needs a lock,
# otherwise two initializing procs will both try to init resulting in duplicates. The critical
# region is not blocking: it spawns a separate task to do the init, assigns the
# Shared[_ControllerController] from that task to the global and releases the lock.
def _get_controller_controller() -> "Tuple[ProcMesh, _ControllerController]":
    global _controller_controller, _cc_proc_mesh
    with _cc_init:
        if _controller_controller is None:
            alloc = LocalAllocator().allocate(AllocSpec(AllocConstraints()))
            _cc_proc_mesh = ProcMesh.from_alloc(
                alloc, _attach_controller_controller=False
            )
            _controller_controller = _cc_proc_mesh.spawn(
                "controller_controller", _ControllerController
            )
    assert _cc_proc_mesh is not None
    return _cc_proc_mesh, _controller_controller


def get_or_spawn_controller(
    name: str, Class: Type["_ActorType"], *args: Any, **kwargs: Any
) -> Future["_ActorType"]:
    """
    Creates a singleton actor (controller) indexed by name, or if it already exists, returns the
    existing actor.

    Args:
        name (str): The unique name of the actor, used as a key for retrieval.
        Class (Type): The class of the actor to spawn. Must be a subclass of Actor.
        *args (Any): Positional arguments to pass to the actor constructor.
        **kwargs (Any): Keyword arguments to pass to the actor constructor.

    Returns:
        A Future that resolves to a reference to the actor.
    """
    return context().actor_instance._controller_controller.get_or_spawn.call_one(
        name, Class, *args, **kwargs
    )
