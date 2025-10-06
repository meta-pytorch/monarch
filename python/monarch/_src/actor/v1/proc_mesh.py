# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
import threading
from contextlib import AbstractContextManager

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
from weakref import WeakSet

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Region, Shape, Slice

from monarch._rust_bindings.monarch_hyperactor.v1.proc_mesh import (
    ProcMesh as HyProcMesh,
)
from monarch._src.actor.actor_mesh import _Actor, Actor, ActorMesh, context

from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future
from monarch._src.actor.logging import LoggingManager
from monarch._src.actor.proc_mesh import _has_tensor_engine, SetupActor
from monarch._src.actor.shape import MeshTrait


if TYPE_CHECKING:
    Tensor = Any
    DeviceMesh = Any
    from monarch._src.actor.v1.host_mesh import HostMesh


logger: logging.Logger = logging.getLogger(__name__)


T = TypeVar("T")
TActor = TypeVar("TActor", bound=Actor)


_proc_mesh_registry: WeakSet["ProcMesh"] = WeakSet()


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
        _device_mesh: Optional["DeviceMesh"] = None,
    ) -> None:
        _proc_mesh_registry.add(self)
        self._proc_mesh = hy_proc_mesh
        self._host_mesh = host_mesh
        self._region = region
        self._maybe_device_mesh = _device_mesh
        self._logging_manager = LoggingManager()
        self._controller_controller: Optional["_ControllerController"] = None

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
        return self._host_mesh

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

        async def task() -> HyProcMesh:
            return (await self._proc_mesh).sliced(shape.region)

        return ProcMesh(
            PythonTask.from_coroutine(task()).spawn(),
            self._host_mesh,
            shape.region,
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
        - The actor instance.
        """
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

    async def monitor(self) -> None:
        logger.debug("monitor is not implemented for v1 ProcMesh")

    @classmethod
    def from_host_mesh(
        self,
        host_mesh: "HostMesh",
        hy_proc_mesh: "Shared[HyProcMesh]",
        region: Region,
        setup: Callable[[], None] | None = None,
        _attach_controller_controller: bool = True,
    ) -> "ProcMesh":
        pm = ProcMesh(hy_proc_mesh, host_mesh, region)

        if _attach_controller_controller:
            instance = context().actor_instance
            cc = instance._controller_controller
            if (
                cc is not None
                and cast(ActorMesh[_ControllerController], cc)._class
                is not _ControllerController
            ):
                # This can happen in the client process
                pm._controller_controller = _get_controller_controller()[1]
            else:
                pm._controller_controller = instance._controller_controller  # type: ignore
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
            task(pm, hy_proc_mesh, setup_actor, host_mesh.stream_logs)
        ).spawn()

        return pm

    def __repr__(self) -> str:
        return repr(self._proc_mesh)

    def __str__(self) -> str:
        return str(self._proc_mesh)

    def _spawn_nonblocking(
        self, name: str, Class: Type[TActor], *args: Any, **kwargs: Any
    ) -> TActor:
        return self._spawn_nonblocking_on(self._proc_mesh, name, Class, *args, **kwargs)

    def to_table(self) -> str:
        return self._maybe_device_mesh.to_table()

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
        actor_mesh = HyProcMesh.spawn_async(
            pm, instance._as_rust(), name, _Actor, emulated=False
        )
        service = ActorMesh._create(
            Class,
            actor_mesh,
            self._region.as_shape(),
            self,
            self._controller_controller,
            *args,
            **kwargs,
        )
        instance._add_child(service)
        return cast(TActor, service)

    @property
    def _device_mesh(self) -> "DeviceMesh":
        if not _has_tensor_engine():
            raise RuntimeError(
                "DeviceMesh is not available because tensor_engine was not compiled (USE_TENSOR_ENGINE=0)"
            )

        # type: ignore[21]
        from monarch.mesh_controller import spawn_tensor_engine  # @manual

        if self._maybe_device_mesh is None:
            # type: ignore[21]
            self._maybe_device_mesh = spawn_tensor_engine(self)
        return self._maybe_device_mesh

    # pyre-ignore
    def activate(self) -> AbstractContextManager:
        return self._maybe_device_mesh.activate()

    def rank_tensor(self, dim: str | Sequence[str]) -> "Tensor":
        return self._maybe_device_mesh.rank(dim)

    def rank_tensors(self) -> Dict[str, "Tensor"]:
        return self._maybe_device_mesh.ranks

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
        return self

    def stop(self) -> Future[None]:
        """
        This will stop all processes (and actors) in the mesh and
        release any resources associated with the mesh.
        """

        # FIXME: Actually implement stopping for v1 proc mesh.

        async def _stop_nonblocking() -> None:
            pass

        return Future(coro=_stop_nonblocking())

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        pass

    @classmethod
    def _from_initialized_hy_proc_mesh(
        cls, hy_proc_mesh: HyProcMesh, host_mesh: "HostMesh", region: Region
    ) -> "ProcMesh":
        async def task() -> HyProcMesh:
            return hy_proc_mesh

        return ProcMesh(
            PythonTask.from_coroutine(task()).spawn(),
            host_mesh,
            region,
        )

    def __reduce_ex__(self, protocol: ...) -> Tuple[Any, Tuple[Any, ...]]:
        return ProcMesh._from_initialized_hy_proc_mesh, (
            self._proc_mesh.block_on(),
            self.host_mesh,
            self._region,
        )


class _ControllerController(Actor):
    def __init__(self) -> None:
        self._controllers: Dict[str, Actor] = {}

    # pyre-ignore
    @endpoint
    def get_or_spawn(
        self, name: str, Class: Type[TActor], *args: Any, **kwargs: Any
    ) -> TActor:
        if name not in self._controllers:
            from monarch._src.actor.v1.host_mesh import this_proc

            proc = this_proc()
            proc._controller_controller = _get_controller_controller()[1]
            self._controllers[name] = proc.spawn(name, Class, *args, **kwargs)
        return cast(TActor, self._controllers[name])


_cc_init = threading.Lock()
_cc_proc_mesh: Optional["ProcMesh"] = None
_controller_controller: Optional["_ControllerController"] = None


# Lazy init so that the controller_controller and proc do not produce logs when they aren't used.
# Checking for the controller (when it does not already exist in the MonarchContext) needs a lock,
# otherwise two initializing procs will both try to init resulting in duplicates. The critical
# region is not blocking: it spawns a separate task to do the init, assigns the
# Shared[_ControllerController] from that task to the global and releases the lock.
def _get_controller_controller(
    force_respawn: bool = False,
) -> "Tuple[ProcMesh, _ControllerController]":
    global _controller_controller, _cc_proc_mesh
    with _cc_init:
        if context().is_root_client and (
            _controller_controller is None or force_respawn
        ):
            from monarch._src.actor.v1.host_mesh import fake_in_process_host

            _cc_proc_mesh = fake_in_process_host(
                "controller_controller_host"
            )._spawn_nonblocking(
                name="controller_controller_proc",
                per_host=Extent([], []),
                setup=None,
                _attach_controller_controller=False,
            )
            _controller_controller = _cc_proc_mesh.spawn(
                "controller_controller", _ControllerController
            )
    assert _cc_proc_mesh is not None and _controller_controller is not None
    return _cc_proc_mesh, _controller_controller


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
    if (
        cc is not None
        and cast(ActorMesh[_ControllerController], cc)._class
        is not _ControllerController
    ):
        # This can happen in the client process
        cc = _get_controller_controller()[1]
    return cc.get_or_spawn.call_one(name, Class, *args, **kwargs)
