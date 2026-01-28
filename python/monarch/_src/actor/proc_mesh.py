# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import importlib
import inspect
import json
import logging
import os
import sys
import warnings
from contextlib import AbstractContextManager
from functools import cache
from pathlib import Path
from typing import (
    Any,
    Awaitable,
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
from urllib.parse import urlparse
from weakref import WeakSet

from monarch._rust_bindings.monarch_hyperactor.actor import MethodSpecifier
from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints
from monarch._rust_bindings.monarch_hyperactor.context import Instance as HyInstance
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    PendingPickle,
    PythonTask,
    Shared,
)
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Region, Shape, Slice
from monarch._src.actor.actor_mesh import (
    _Actor,
    _create_endpoint_message,
    _Lazy,
    Actor,
    ActorInitArgs,
    ActorMesh,
    context,
)
from monarch._src.actor.allocator import AllocHandle, SimAllocator
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
from monarch._src.actor.future import Future
from monarch._src.actor.logging import LoggingManager
from monarch._src.actor.pickle import is_pending_pickle_allowed
from monarch._src.actor.shape import MeshTrait
from monarch.tools.config.environment import CondaEnvironment
from monarch.tools.config.workspace import Workspace
from monarch.tools.utils import conda as conda_utils


@cache
def _has_tensor_engine() -> bool:
    try:
        # Torch is needed for tensor engine
        import torch  # @manual  # noqa: F401

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


logger: logging.Logger = logging.getLogger(__name__)


T = TypeVar("T")
TActor = TypeVar("TActor", bound=Actor)


class SetupActor(Actor):
    """
    A helper actor to set up the actor mesh with user defined setup method.
    Also runs registered startup functions (e.g., for mock propagation).

    This actor uses an async endpoint and wraps synchronous user setup functions
    with fake_sync_state() to properly handle the async context.
    """

    # List of startup functions that are called when spawning a SetupActor.
    # Each function returns Optional[Callable[[], None]] - a callable to run on
    # the remote process. The callable handles its own serialization via __reduce_ex__.
    # Returns None if there's no work to do.
    _startup_functions: List[Callable[[], Optional[Callable[[], None]]]] = []

    @classmethod
    def register_startup_function(
        cls,
        func: Callable[[], Optional[Callable[[], None]]],
    ) -> None:
        """
        Register a startup function.

        The function is called when spawning a SetupActor. It should return:
        - None if there's no work to do
        - A Callable[[], None] that will be run on the remote process.
          The callable is responsible for its own serialization via __reduce_ex__.
        """
        cls._startup_functions.append(func)

    @staticmethod
    def startup_actor_from_setup_function(
        pm: "ProcMesh",
        hy_proc_mesh: "Shared[HyProcMesh]",
        setup: Callable[[], None] | Callable[[], Awaitable[None]] | None,
        run_startup_functions: bool = True,
    ) -> Optional["SetupActor"]:
        """
        Factory method that decides if a SetupActor is needed.

        Creates a SetupActor only if there's work to do: user setup function
        or registered startup functions with work.

        Args:
            pm: The ProcMesh to spawn the actor on.
            hy_proc_mesh: The underlying hyperactor proc mesh.
            setup: Optional user-provided setup function.
            run_startup_functions: Whether to run startup functions. Set to False
                for the root proc mesh to avoid propagating mocks to it.
        """
        # Collect callables from all registered startup functions
        startup_callables: List[Callable[[], None]] = []
        if run_startup_functions:
            for func in SetupActor._startup_functions:
                callable_to_run = func()
                if callable_to_run is not None:
                    startup_callables.append(callable_to_run)

        has_work = setup is not None or bool(startup_callables)

        if not has_work:
            return None

        return pm._spawn_nonblocking_on(
            hy_proc_mesh,
            "setup",
            SetupActor,
            setup,
            startup_callables if startup_callables else None,
        )

    def __init__(
        self,
        user_setup: Callable[[], None] | Callable[[], Awaitable[None]] | None,
        startup_callables: Optional[List[Callable[[], None]]] = None,
    ) -> None:
        self._user_setup = user_setup
        self._startup_callables = startup_callables
        self._is_async: bool = user_setup is not None and inspect.iscoroutinefunction(
            user_setup
        )

    @endpoint
    async def setup(self) -> None:
        """
        Run setup on the remote process:
        1. First run startup callables (always sync, wrapped with fake_sync_state)
        2. Then run user's setup method (sync or async)
        """
        from monarch._src.actor.sync_state import fake_sync_state

        # Run startup callables first (always synchronous)
        # Use local variable so pyre can narrow the type after the None check
        startup_callables = self._startup_callables
        if startup_callables is not None:
            with fake_sync_state():
                for callable_fn in startup_callables:
                    callable_fn()

        # Run user setup
        # Use local variable so pyre can narrow the type after the None check
        user_setup = self._user_setup
        if user_setup is not None:
            if self._is_async:
                # pyre-ignore[12]: user_setup is Awaitable here due to _is_async check
                await user_setup()
            else:
                with fake_sync_state():
                    # pyre-ignore[29]: user_setup is callable here
                    user_setup()


try:
    from __manifest__ import fbmake  # noqa

    IN_PAR = bool(fbmake.get("par_style"))
except ImportError:
    IN_PAR = False


_proc_mesh_registry: WeakSet["ProcMesh"] = WeakSet()

# Callbacks invoked when a new ProcMesh is spawned via from_host_mesh.
# Each callback receives the newly created ProcMesh.
_proc_mesh_spawn_callbacks: List[Callable[["ProcMesh"], None]] = []


def register_proc_mesh_spawn_callback(callback: Callable[["ProcMesh"], None]) -> None:
    """
    Register a callback to be invoked whenever a new ProcMesh is spawned.

    The callback receives the newly created ProcMesh before it is returned
    from from_host_mesh. This allows code to hook into process spawning
    for monitoring, telemetry, or other cross-cutting concerns.

    Args:
        callback: A callable that takes a ProcMesh and returns None.
    """
    _proc_mesh_spawn_callbacks.append(callback)


def unregister_proc_mesh_spawn_callback(callback: Callable[["ProcMesh"], None]) -> None:
    """
    Unregister a previously registered spawn callback.

    Args:
        callback: The callback to remove.

    Raises:
        ValueError: If the callback was not registered.
    """
    _proc_mesh_spawn_callbacks.remove(callback)


def get_active_proc_meshes() -> List["ProcMesh"]:
    """Get a list of all active ProcMesh instances."""
    return list(_proc_mesh_registry)


class ProcMesh(MeshTrait):
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
        host_mesh: "HostMesh",
        region: Region,
        root_region: Region,
        _device_mesh: Optional["DeviceMesh"] = None,
    ) -> None:
        _proc_mesh_registry.add(self)

        self._proc_mesh = hy_proc_mesh
        self._host_mesh = host_mesh
        self._region = region
        self._root_region = root_region
        self._maybe_device_mesh = _device_mesh
        self._stopped = False
        self._logging_manager = LoggingManager()
        self._controller_controller: Optional["_ControllerController"] = None
        self._code_sync_client: Optional[CodeSyncMeshClient] = None

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
        if self.extent.nelements != 1:
            raise NotImplementedError(
                "`ProcMesh.host_mesh` is not yet supported for non-singleton proc meshes."
            )
        return self._host(0)

    @property
    def _ndslice(self) -> Slice:
        return self._region.slice()

    @property
    def _labels(self) -> List[str]:
        return self._region.labels

    def _new_with_shape(self, shape: Shape) -> "ProcMesh":
        if shape == self._region.as_shape():
            return self

        device_mesh = (
            None
            if self._maybe_device_mesh is None
            else self._maybe_device_mesh._new_with_shape(shape)
        )

        sliced_hy_pm: Shared[HyProcMesh]
        if (pm := self._proc_mesh.poll()) is not None:
            sliced_hy_pm = Shared.from_value(pm.sliced(shape.region))
        else:

            async def task() -> HyProcMesh:
                return (await self._proc_mesh).sliced(shape.region)

            sliced_hy_pm = PythonTask.from_coroutine(task()).spawn()

        return ProcMesh(
            sliced_hy_pm,
            self._host_mesh,
            shape.region,
            self._root_region,
            _device_mesh=device_mesh,
        )

    def spawn(
        self, name: str, Class: Type[TActor], *args: Any, **kwargs: Any
    ) -> TActor:
        """
        Spawn a T-typed actor mesh on the process mesh.

        Args:
        - `name`: The name of the actor.
        - `Class`: The class of the actor to spawn.
        - `args`: Positional arguments to pass to the actor's constructor.
        - `kwargs`: Keyword arguments to pass to the actor's constructor.

        Returns:
        - The actor mesh reference typed as T.

        Note:

        The method returns immediately, initializing the underlying actor instances
        asynchronously. Thus, return of this method does not guarantee the actor's
        __init__ has be executed; but rather that __init__ will be executed before the
        first call to the actor's endpoints.

        Nonblocking enhances composition, permitting the user to easily pipeline mesh
        creation, for exmaple to construct complex mesh object graphs without introducing
        additional latency.


        If __init__ fails, the actor will be stopped and a supervision event will
        be raised.
        """
        from monarch._src.actor.mock import get_actor_class

        Class = cast(Type[TActor], get_actor_class(cast(Type[Actor], Class)))
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

    @classmethod
    def from_host_mesh(
        self,
        host_mesh: "HostMesh",
        hy_proc_mesh: "Shared[HyProcMesh]",
        region: Region,
        setup: Callable[[], None] | Callable[[], Awaitable[None]] | None = None,
        _attach_controller_controller: bool = True,
    ) -> "ProcMesh":
        pm = ProcMesh(hy_proc_mesh, host_mesh, region, region, None)

        if _attach_controller_controller:
            instance = context().actor_instance
            pm._controller_controller = instance._controller_controller
            instance._add_child(pm)

        async def task(
            pm: "ProcMesh",
            hy_proc_mesh_task: "Shared[HyProcMesh]",
            setup_actor: "SetupActor | None",
            stream_log_to_client: bool,
        ) -> HyProcMesh:
            hy_proc_mesh = await hy_proc_mesh_task

            await pm._logging_manager.init(hy_proc_mesh, stream_log_to_client)

            # If the user has passed the setup lambda, we need to call
            # it here before any of the other python actors are spawned so
            # that the environment variables are set up before cuda init.
            if setup_actor is not None:
                await setup_actor.setup.call()

            return hy_proc_mesh

        # Use SetupActor factory to handle setup function and startup functions.
        # The SetupActor needs to be spawned outside of `task` for now,
        # since spawning a python actor requires a blocking call to
        # pickle the proc mesh, and we can't do that from the tokio runtime.
        setup_actor = SetupActor.startup_actor_from_setup_function(
            pm, hy_proc_mesh, setup, run_startup_functions=_attach_controller_controller
        )

        pm._proc_mesh = PythonTask.from_coroutine(
            task(pm, hy_proc_mesh, setup_actor, host_mesh.stream_logs)
        ).spawn()

        # Invoke registered spawn callbacks
        for callback in _proc_mesh_spawn_callbacks:
            callback(pm)

        return pm

    def _spawn_nonblocking(
        self, name: str, Class: Type[TActor], *args: Any, **kwargs: Any
    ) -> TActor:
        return self._spawn_nonblocking_on(self._proc_mesh, name, Class, *args, **kwargs)

    def to_table(self) -> str:
        return self._device_mesh.to_table()

    def _spawn_nonblocking_on(
        self,
        pm: "Shared[HyProcMesh]",
        name: str,
        Class: Type[TActor],
        *args: Any,
        **kwargs: Any,
    ) -> TActor:
        if not issubclass(Class, Actor):
            raise ValueError(
                f"{Class} must subclass monarch.service.Actor to spawn it."
            )

        instance = context().actor_instance
        # The default name used has a UUID appended to it that is not useful for debugging.
        # Replace with this more descriptive name.
        supervision_display_name = (
            f"{str(instance)}.<{Class.__module__}.{Class.__name__} {name}>"
        )
        actor_mesh = HyProcMesh.spawn_async(
            pm,
            instance._as_rust(),
            name,
            _Actor,
            emulated=False,
            supervision_display_name=supervision_display_name,
        )
        # Inlined ActorMesh._create implementation
        mesh = ActorMesh(Class, name, actor_mesh, self._region.as_shape(), self)

        # We don't start the supervision polling loop until the first call to
        # supervision_event, which needs an Instance. Initialize here so events
        # can be collected even without any endpoints being awaited.
        supervision_display_name = (
            f"{str(instance)}.<{Class.__module__}.{Class.__name__} {name}>"
        )
        mesh._inner.start_supervision(instance._as_rust(), supervision_display_name)

        # send __init__ message to the mesh to initialize the user defined
        # python actor object.
        message = _create_endpoint_message(
            MethodSpecifier.Init(),
            inspect.signature(Class.__init__),
            (
                ActorInitArgs(
                    cast(Type[Actor], mesh._class),
                    self,
                    self._controller_controller or instance._controller_controller,
                    name,
                    context().actor_instance._as_creator(),
                    args,
                ),
            ),
            kwargs,
            None,
            self,
        )
        mesh._inner.cast(message, "all", instance._as_rust())

        instance._add_child(mesh)
        return cast(TActor, mesh)

    @property
    def _device_mesh(self) -> "DeviceMesh":
        if not _has_tensor_engine():
            raise RuntimeError(
                "DeviceMesh is not available because tensor_engine was not compiled (USE_TENSOR_ENGINE=0)"
            )

        from monarch._src.actor.actor_mesh import context

        if self._maybe_device_mesh is None:
            # Use the actor instance's spawn_tensor_engine method, which handles
            # mock vs real tensor engine decision.
            self._maybe_device_mesh = context().actor_instance.spawn_tensor_engine(self)
        return self._maybe_device_mesh

    # pyre-ignore
    def activate(self) -> AbstractContextManager:
        """
        Activate the device mesh. Operations done from insided this context manager will be
        distributed tensor operations. Each operation will be excuted on each device in the mesh.

        See https://meta-pytorch.org/monarch/generated/examples/distributed_tensors.html for more information


            with mesh.activate():
                t = torch.rand(3, 4, device="cuda")
        """
        return self._device_mesh.activate()

    def rank_tensor(self, dim: str | Sequence[str]) -> "Tensor":
        return self._maybe_device_mesh.rank(dim)

    def rank_tensors(self) -> Dict[str, "Tensor"]:
        return self._maybe_device_mesh.ranks

    async def logging_option(
        self,
        stream_to_client: bool = False,
        aggregate_window_sec: int | None = None,
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

    def stop(self, reason: str = "stopped by client") -> Future[None]:
        """
        This will stop all processes (and actors) in the mesh and
        release any resources associated with the mesh.
        """

        instance = context().actor_instance._as_rust()

        async def _stop_nonblocking(instance: HyInstance) -> None:
            pm = await self._proc_mesh
            await self._logging_manager.flush_async()
            await pm.stop_nonblocking(instance, reason)
            self._stopped = True

        return Future(coro=_stop_nonblocking(instance))

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        # In case there are multiple nested "async with" statements, we only
        # want it to close once.
        if not self._stopped:
            await self.stop()

    @classmethod
    def _from_rust(cls, hy_proc_mesh: HyProcMesh, host_mesh: "HostMesh") -> "ProcMesh":
        """
        Create a HostMesh from a Rust HyProcMesh and its parent HostMesh.
        """
        return cls._from_initialized_hy_proc_mesh(
            hy_proc_mesh,
            host_mesh,
            hy_proc_mesh.region,
            hy_proc_mesh.region,
        )

    @classmethod
    def _from_initialized_hy_proc_mesh(
        cls,
        hy_proc_mesh: HyProcMesh,
        host_mesh: "HostMesh",
        region: Region,
        root_region: Region,
    ) -> "ProcMesh":
        return ProcMesh(
            Shared.from_value(hy_proc_mesh),
            host_mesh,
            region,
            root_region,
        )

    def __reduce_ex__(self, protocol: ...) -> Tuple[Any, Tuple[Any, ...]]:
        return ProcMesh._from_initialized_hy_proc_mesh, (
            self._proc_mesh.poll()
            or (
                PendingPickle(self._proc_mesh)
                if is_pending_pickle_allowed()
                else self._proc_mesh.block_on()
            ),
            self._host_mesh,
            self._region,
            self._root_region,
        )

    def _host(self, proc_rank: int) -> "HostMesh":
        base_proc_rank = self._region.slice().get(proc_rank)
        n_procs = len(self._root_region.slice())
        procs_per_host = n_procs // len(self._host_mesh.region.slice())
        host_rank = base_proc_rank // procs_per_host
        base_host_rank = self._host_mesh.region.slice().get(host_rank)
        return self._host_mesh.slice(
            **self._host_mesh.region.point_of_base_rank(base_host_rank)
        )

    async def sync_workspace(
        self,
        workspace: Workspace,
        conda: bool = False,
        auto_reload: bool = False,
    ) -> None:
        raise NotImplementedError(
            "sync_workspace is not implemented for ProcMesh. Use HostMesh.sync_workspace instead."
        )

    async def _sync_workspace(
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
                client=context().actor_instance,
                proc_mesh=await self._proc_mesh_for_asyncio_fixme,
            )

        # TODO(agallagher): We need some way to configure and pass this
        # in -- right now we're assuming the `gpu` dimension, which isn't
        # correct.
        # The workspace shape (i.e. only perform one rsync per host).
        assert set(self._region.labels).issubset({"gpus", "hosts"})

        workspaces = {}
        for src_dir, dst_dir in workspace.dirs.items():
            local = Path(src_dir)
            workspaces[local] = WorkspaceConfig(
                local=local,
                remote=RemoteWorkspace(
                    location=WorkspaceLocation.FromEnvVar(
                        env="WORKSPACE_DIR",
                        relpath=dst_dir,
                    ),
                    shape=WorkspaceShape.shared("gpus"),
                ),
                method=CodeSyncMethod.Rsync(),
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

            # Build a list of additional paths prefixes to fixup when syncing
            # the conda env.
            conda_prefix_replacements = {}

            # Auto-detect editable installs and implicitly add workspaces for
            # them.
            # NOTE(agallagher): There's sometimes a `python3.1` symlink to
            # `python3.10`, so avoid it.
            (lib_python,) = [
                dirpath
                for dirpath in conda_prefix.glob("lib/python*")
                if not os.path.islink(dirpath)
            ]
            for direct_url in lib_python.glob(
                "site-packages/*.dist-info/direct_url.json"
            ):
                # Parse the direct_url.json to see if it's an editable install
                # (https://packaging.python.org/en/latest/specifications/direct-url/#example-pip-commands-and-their-effect-on-direct-url-json).
                with open(direct_url) as f:
                    info = json.load(f)
                if not info.get("dir_info", {}).get("editable", False):
                    continue

                # Extract the workspace path from the URL (e.g. `file///my/workspace/`).
                url = urlparse(info["url"])
                assert url.scheme == "file", f"expected file:// URL, got {url.scheme}"

                # Get the project name, so we can use it below to create a unique-ish
                # remote directory.
                dist = importlib.metadata.PathDistribution(direct_url.parent)
                name = dist.metadata["Name"]

                local = Path(url.path)

                # Check if we've already defined a workspace for this local path.
                existing = workspaces.get(local)
                if existing is not None:
                    assert existing.method == CodeSyncMethod.Rsync()
                    remote = existing.remote
                else:
                    # Otherwise, add the workspace to the list.
                    remote = RemoteWorkspace(
                        location=WorkspaceLocation.FromEnvVar(
                            env="WORKSPACE_DIR",
                            relpath=f"__editable__.{name}",
                        ),
                        shape=WorkspaceShape.shared("gpus"),
                    )
                    workspaces[local] = WorkspaceConfig(
                        local=local,
                        remote=remote,
                        method=CodeSyncMethod.Rsync(),
                    )

                logging.info(
                    f"Syncing editable install of {name} from {local} (to {remote.location})"
                )

                # Make sure we fixup path prefixes to the editable install.
                conda_prefix_replacements[local] = remote.location

            workspaces[conda_prefix] = WorkspaceConfig(
                local=conda_prefix,
                remote=RemoteWorkspace(
                    location=WorkspaceLocation.FromEnvVar(
                        env="CONDA_PREFIX",
                        relpath="",
                    ),
                    shape=WorkspaceShape.shared("gpus"),
                ),
                method=CodeSyncMethod.CondaSync(conda_prefix_replacements),
            )

        assert self._code_sync_client is not None
        await self._code_sync_client.sync_workspaces(
            instance=context().actor_instance._as_rust(),
            workspaces=list(workspaces.values()),
            auto_reload=auto_reload,
        )

    @classmethod
    def from_alloc(
        self,
        alloc: AllocHandle,
        setup: Callable[[], None] | None = None,
        _attach_controller_controller: bool = True,
    ) -> "ProcMesh":
        warnings.warn(
            (
                "DEPRECATION WARNING: this function will soon be unsupported. "
                "Use `HostMesh.allocate_nonblocking(...).spawn_procs(...)` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )

        from monarch._src.actor.host_mesh import HostMesh

        return HostMesh.allocate_nonblocking(
            "host_mesh_from_alloc",
            Extent(*zip(*alloc._extent.items())),
            alloc._allocator,
            alloc._constraints,
        ).spawn_procs(bootstrap=setup)


class _ControllerController(Actor):
    def __init__(self) -> None:
        self._controllers: Dict[str, Actor] = {}

    # pyre-ignore
    @endpoint
    def get_or_spawn(
        self,
        self_ref: "_ControllerController",  # This is actually an ActorMesh[_ControllerController]
        name: str,
        Class: Type[TActor],
        *args: Any,
        **kwargs: Any,
    ) -> TActor:
        if name not in self._controllers:
            from monarch._src.actor.host_mesh import this_proc

            proc = this_proc()
            proc._controller_controller = self_ref
            self._controllers[name] = proc.spawn(name, Class, *args, **kwargs)
        return cast(TActor, self._controllers[name])


# Lazy init so that the controller_controller and does not produce logs when it isn't used.
# Checking for the controller (when it does not already exist in the MonarchContext) needs a lock,
# otherwise two initializing procs will both try to init resulting in duplicates. The critical
# region is not blocking: it spawns a separate task to do the init, assigns the
# Shared[_ControllerController] from that task to the global and releases the lock.
_controller_controller: _Lazy[_ControllerController] = _Lazy(
    lambda: context().actor_instance.proc_mesh.spawn(
        "controller_controller", _ControllerController
    )
)


def _get_controller_controller() -> "Tuple[ProcMesh, _ControllerController]":
    return context().actor_instance.proc_mesh, _controller_controller.get()


def get_or_spawn_controller(
    name: str, Class: Type[TActor], *args: Any, **kwargs: Any
) -> Future[TActor]:
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
    cc = context().actor_instance._controller_controller
    return cc.get_or_spawn.call_one(cc, name, Class, *args, **kwargs)


def proc_mesh(
    *,
    gpus: Optional[int] = None,
    hosts: int = 1,
    env: dict[str, str] | None = None,
    setup: Callable[[], None] | None = None,
) -> ProcMesh:
    """
    [DEPRECATED] Create a distributed process mesh across hosts.

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
        (
            "DEPRECATION WARNING: this function will soon be unsupported. "
            "Use this_host().spawn_procs(per_host = {'hosts': 2, 'gpus': 3}) "
            "instead of monarch.actor.proc_mesh(hosts=2, gpus=3)."
        ),
        DeprecationWarning,
        stacklevel=2,
    )

    if env is not None and len(env) > 0:
        raise ValueError(
            "`env` is not supported for `proc_mesh(...)`, and you shouldn't be using this function anyway. "
            "Use `this_host().spawn_procs(per_host = {'hosts': ..., 'gpus': ...})` instead."
        )

    from monarch._src.actor.host_mesh import this_host

    return this_host().spawn_procs(
        per_host={"hosts": hosts, "gpus": gpus if gpus else _local_device_count()},
        bootstrap=setup,
    )


def local_proc_mesh(*, gpus: Optional[int] = None, hosts: int = 1) -> ProcMesh:
    """
    [DEPRECATED] Create a local process mesh for testing and development.

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
        (
            "DEPRECATION WARNING: this function will soon be unsupported. "
            "Use monarch._src.actor.host_mesh.fake_in_process_host().spawn_procs "
            "for testing. For launching an actor in the current process use "
            "this_proc().spawn_procs()."
        ),
        DeprecationWarning,
        stacklevel=2,
    )

    from monarch._src.actor.host_mesh import fake_in_process_host

    return fake_in_process_host().spawn_procs(
        per_host={"hosts": hosts, "gpus": gpus if gpus else _local_device_count()},
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
    from monarch._src.actor.host_mesh import HostMesh

    host_mesh = HostMesh.allocate_nonblocking(
        "sim",
        Extent(
            ["regions", "dcs", "zones", "racks", "hosts"],
            [regions, dcs, zones, racks, hosts],
        ),
        SimAllocator(),
        AllocConstraints(),
    )
    return host_mesh.spawn_procs(per_host={"gpus": gpus})


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
