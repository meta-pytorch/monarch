# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import importlib.resources
import logging
import os
import random
import string
import subprocess
import tempfile
import time
from pathlib import Path
from typing import (
    Callable,
    ContextManager as AbstractContextManager,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
)

from monarch._rust_bindings.monarch_extension.client import (  # @manual=//monarch/monarch_extension:monarch_extension  # @manual=//monarch/monarch_extension:monarch_extension
    ClientActor,
)

from monarch._rust_bindings.monarch_extension.simulator_client import (  # @manual=//monarch/monarch_extension:monarch_extension
    SimulatorClient,
)

from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
    init_proc,
    Proc,
)

from monarch._src.actor.shape import NDSlice
from monarch.common.client import Client
from monarch.common.constants import (
    SIM_MESH_CLIENT_SUPERVISION_UPDATE_INTERVAL,
    SIM_MESH_CLIENT_TIMEOUT,
)
from monarch.common.device_mesh import DeviceMesh
from monarch.common.fake import fake_call
from monarch.common.future import Future, T
from monarch.common.invocation import DeviceException, RemoteException
from monarch.common.messages import Dims
from monarch.controller.rust_backend.controller import RustController
from monarch.rust_backend_mesh import MeshWorld


logger: logging.Logger = logging.getLogger(__name__)


def sim_mesh(n_meshes: int, hosts: int, gpus_per_host: int) -> List[DeviceMesh]:
    """
    Creates a single simulated device mesh with the given number of per host.

    Args:
        n_meshes            : number of device meshes to create.
        hosts               : number of hosts, primarily used for simulating multiple machines locally.
                              Default: 1
        gpus_per_host       : number of gpus per host.
                              Default: the number of GPUs this machine has.
    """
    mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]] = {}
    bootstrap: Bootstrap = Bootstrap(
        n_meshes,
        mesh_world_state,
        world_size=hosts * gpus_per_host,
    )

    client_proc_id = "client[0]"
    client_proc: Proc = init_proc(
        proc_id=client_proc_id,
        bootstrap_addr=bootstrap.client_bootstrap_addr,
        timeout=SIM_MESH_CLIENT_TIMEOUT,  # unused
        supervision_update_interval=SIM_MESH_CLIENT_SUPERVISION_UPDATE_INTERVAL,
        listen_addr=bootstrap.client_listen_addr,
    )
    root_client_actor: ClientActor = ClientActor(
        proc=client_proc, actor_name="root_client"
    )

    dms = []
    for i in range(n_meshes):
        controller_id = ActorId(
            world_name=f"mesh_{i}_controller", rank=0, actor_name="root"
        )
        # Create a new device mesh
        backend_ctrl = RustController(
            proc=client_proc,
            client_actor=ClientActor(client_proc, "backend_controller"),
            controller_id=controller_id,
            worker_world_name=f"mesh_{i}_worker",
        )
        client = Client(backend_ctrl, hosts * gpus_per_host, gpus_per_host)
        dm = SimMesh(
            client,
            NDSlice(offset=0, sizes=[hosts, gpus_per_host], strides=[gpus_per_host, 1]),
            ("host", "gpu"),
            bootstrap._simulator_client,
            f"mesh_{i}_worker",
        )
        dms.append(dm)

    return dms


class OriginalFutureWrapper(Generic[T]):
    result: Callable[
        [
            Future[T],
            float | None,
        ],
        T,
    ] = Future.result
    _set_result: Callable[[Future[T], T], None] = Future._set_result


class SimMesh(DeviceMesh, Generic[T]):
    def __init__(
        self,
        client: "Client",
        processes: "NDSlice",
        names: Dims,
        simulator_client: SimulatorClient,
        mesh_name: str = "default",
    ) -> None:
        super().__init__(client, processes, names, mesh_name)
        self.simulator_client: SimulatorClient = simulator_client

    # monkey patch Future.result and Future._set_result to hook into set_training_script_state_{running,waiting}
    def activate(self) -> AbstractContextManager[DeviceMesh]:
        def sim_result(fut: Future[T], timeout: float | None = None) -> T:
            self.simulator_client.set_training_script_state_waiting()
            return OriginalFutureWrapper.result(fut, timeout)

        def sim_set_result(fut: Future[T], result: T) -> None:
            self.simulator_client.set_training_script_state_running()
            return OriginalFutureWrapper._set_result(fut, result)

        # pyre-ignore
        Future.result = sim_result
        Future._set_result = sim_set_result

        return super().activate()

    # restore Future.result and Future._set_result to their previous values
    def exit(
        self,
        error: Optional[RemoteException | DeviceException | Exception] = None,
    ) -> None:
        self.client.shutdown(True, error)
        # pyre-ignore
        Future.result = OriginalFutureWrapper._result
        Future._set_result = OriginalFutureWrapper._set_result


def _random_id(length: int = 14) -> str:
    """
    A simple random id generator.
    """
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


class Bootstrap:
    def __init__(
        self,
        num_meshes: int,
        mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]],
        world_size: int = 1,
    ) -> None:
        """
        Bootstraps a SimMesh.
        Args:
            num_meshes: int - number of meshes to create.
            mesh_world_state: a state of the meshes. Keys are the MeshWorld and values are boolean indicating if this mesh is active.
        """
        # do a fake call to instantiate ThreadPoolExecutor so we don't block GIL later
        fake_call(lambda: 0)

        env = os.environ.copy()
        self.env: dict[str, str] = env

        self._mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]] = mesh_world_state

        self.bootstrap_addr: str = "sim!unix!@system"
        self.client_listen_addr = "sim!unix!@client"
        self.client_bootstrap_addr = "sim!unix!@client,unix!@system"

        self._simulator_client = SimulatorClient(self.bootstrap_addr, world_size)
        for i in range(num_meshes):
            mesh_name: str = f"mesh_{i}"
            controller_world: str = f"{mesh_name}_controller"
            worker_world: str = f"{mesh_name}_worker"
            controller_id: ActorId = ActorId(
                world_name=controller_world,
                rank=0,
                actor_name="root",
            )
            mesh_world = (worker_world, controller_id)
            self._mesh_world_state[mesh_world] = None
            self.spawn_mesh(mesh_world)
        # sleep for 10 sec for the worker and controller tasks to be spawned and ready.
        time.sleep(10)

    def get_mesh_worlds(self) -> List[MeshWorld]:
        return []

    def kill_mesh(self, mesh_world: MeshWorld) -> None:
        pass

    def spawn_mesh(self, mesh_world: MeshWorld) -> None:
        worker_world, controller_id = mesh_world
        controller_world = controller_id.world_name
        self._simulator_client.spawn_mesh(
            self.bootstrap_addr,
            f"{controller_world}[0].root",
            worker_world,
        )


def _validate_proccesses_end(
    processes: Iterable[subprocess.Popen[bytes]],
    timeout_in_sec: int = 1,
    raise_on_abnormal_exit: bool = True,
) -> list[int]:
    """
    Check if processes have ended properly. Raise errors immediately
    if any process has ended with a non-zero return code.
    Return a list of process indices that have not ended yet.
    """
    running = []
    start_time = time.time()
    for i, process in enumerate(processes):
        try:
            current_time = time.time()
            elapsed_time = current_time - start_time
            # The processes are running in parallel. No need to wait for
            # `timeout_in_sec` for each process. Only count the remaining ones.
            wait_in_sec = max(0, timeout_in_sec - elapsed_time)
            return_code = process.wait(timeout=wait_in_sec)
            if return_code != 0:
                error_message: str = (
                    f"Process[{i}] {process.pid} exited with "
                    f"return code {return_code}. Command:\n "
                    f"{process.args!r}"
                )
                if raise_on_abnormal_exit:
                    raise RuntimeError(error_message)
                else:
                    logger.error(error_message)
        except subprocess.TimeoutExpired:
            running.append(i)

    return running


class PoolDeviceMeshProvider:
    def __init__(
        self,
        hosts_per_mesh: int,
        gpus_per_host: int,
        client_proc: Proc,
        mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]],
        simulator_client: SimulatorClient,
    ) -> None:
        self._hosts_per_mesh = hosts_per_mesh
        self._gpus_per_host = gpus_per_host
        self._client_proc = client_proc
        self._root_client_actor: ClientActor = ClientActor(
            proc=client_proc, actor_name="root_client"
        )
        self._mesh_world_state = mesh_world_state
        self._simulator_client = simulator_client

    def new_mesh(self, timeout_in_sec: Optional[int] = None) -> DeviceMesh:
        mesh_world_to_create = next(
            (
                mesh_world
                for mesh_world, is_created in self._mesh_world_state.items()
                if not is_created
            ),
            None,
        )
        assert mesh_world_to_create is not None, "No mesh world to create"

        worker_world, controller_id = mesh_world_to_create
        # Create a new device mesh
        backend_ctrl = RustController(
            proc=self._client_proc,
            client_actor=ClientActor(self._client_proc, "backend_controller"),
            controller_id=controller_id,
            worker_world_name=worker_world,
        )
        client = Client(
            backend_ctrl,
            self._hosts_per_mesh * self._gpus_per_host,
            self._gpus_per_host,
        )
        dm = SimMesh(
            client,
            NDSlice(
                offset=0,
                sizes=[self._hosts_per_mesh, self._gpus_per_host],
                strides=[self._gpus_per_host, 1],
            ),
            ("host", "gpu"),
            self._simulator_client,
            worker_world,
        )
        self._mesh_world_state[mesh_world_to_create] = dm

        return dm


def sim_mesh_provider(
    num_meshes: int, hosts_per_mesh: int, gpus_per_host: int
) -> Tuple[PoolDeviceMeshProvider, Bootstrap]:
    mesh_world_state = {}
    bootstrap = Bootstrap(num_meshes, mesh_world_state)

    client_proc_id = "client[0]"
    client_proc: Proc = init_proc(
        proc_id=client_proc_id,
        bootstrap_addr=bootstrap.client_bootstrap_addr,
        timeout=SIM_MESH_CLIENT_TIMEOUT,  # unused
        supervision_update_interval=SIM_MESH_CLIENT_SUPERVISION_UPDATE_INTERVAL,
        listen_addr=bootstrap.client_listen_addr,
    )
    dm_provider = PoolDeviceMeshProvider(
        hosts_per_mesh,
        gpus_per_host,
        client_proc,
        mesh_world_state,
        bootstrap._simulator_client,
    )
    return (dm_provider, bootstrap)
